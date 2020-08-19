;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.compute-cluster
  (:require [clojure.data :as data]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [datomic.api :as d]
            [plumbing.core :refer [for-map map-from-keys map-from-vals map-vals]]))

; There's an ugly race where the core cook scheduler can kill a job before it tries to launch it.
; What happens is:
;   1. In launch-matched-tasks, we write instance objects to datomic for everything that matches,
;      we have not submitted these to the compute cluster backends yet.
;   2. A kill command arrives to kill the job. The job is put into completed.
;   3. The monitor-tx-queue happens to notice the job just completed. It sees the instance written in step 1.
;   4. We submit a kill-task to the compute cluster backend.
;   5. Kill task processes. There's not much to do, as there's no task to kill.
;   6. launch-matched-tasks now visits the task and submits it to the compute cluster backend.
;   7. Task executes and is not killed.
;
; At the core the bug is an atomicity bug. The intermediate state of written-to-datomic but not yet sent (via launch-task)
; to the backend. We work around this race by having a lock around of all launch-matched-tasks that contains the database
; update and the submit to kubernetes. We re-use the same lock to wrap kill-task to force an ordering relationship, so
; that kill-task must happen after the write-to-datomic and launch-task have been invoked.
;
; ComputeCluster/kill-task cannot be invoked before we write the task to datomic. If it is invoked after the write to
; datomic, the lock ensures that it won't be acted upon until after launch-task has been invoked on the compute cluster.
;
; So, we must grab this lock before calling kill-task in the compute cluster API. As all of our invocations to it are via
; safe-kill-task, we add the lock there.
(def kill-lock-object (Object.))

(defprotocol ComputeCluster
  (launch-tasks [this pool-name matches process-task-post-launch-fn]
    "Launches the tasks contained in the given matches collection")

  (compute-cluster-name [this]
    "Returns the name of this compute cluster")

  (db-id [this]
    "Get a database entity-id for this compute cluster (used for putting it into a task structure).")

  (initialize-cluster [this pool->fenzo]
    "Initializes the cluster. Returns a channel that will be delivered on when the cluster loses leadership.
     We expect Cook to give up leadership when a compute cluster loses leadership, so leadership is not expected to be regained.
     The channel result will be an exception if an error occurred, or a status message if leadership was lost normally.")

  (kill-task [this task-id]
    "Kill the task with the given task id")

  (decline-offers [this offer-ids]
    "Decline the given offer ids")

  (pending-offers [this pool-name]
    "Retrieve pending offers for the given pool")

  (restore-offers [this pool-name offers]
    "Called when offers are not processed to ensure they're still available.")

  (autoscaling? [this pool-name]
    "Returns true if this compute cluster should autoscale the provided pool to satisfy pending jobs")

  (autoscale! [this pool-name jobs adjust-job-resources-for-pool-fn]
    "Autoscales the provided pool to satisfy the provided pending jobs")

  (use-cook-executor? [this]
    "Returns true if this compute cluster makes use of the Cook executor for running tasks")

  (container-defaults [this]
    "Default values to use for containers launched in this compute cluster")

  (max-tasks-per-host [this]
    "The maximum number of tasks that a given host should run at the same time")

  (num-tasks-on-host [this hostname]
    "The number of tasks currently running on the given hostname")

  (retrieve-sandbox-url-path [this instance-entity]
    "Constructs a URL to query the sandbox directory of the task.
     Users will need to add the file path & offset to their query.
     Refer to the 'Using the output_url' section in docs/scheduler-rest-api.adoc for further details."))

(defn safe-kill-task
  "A safe version of kill task that never throws. This reduces the risk that errors in one compute cluster propagate and cause problems in another compute cluster."
  [{:keys [name] :as compute-cluster} task-id]
  (locking kill-lock-object
    (try
      (kill-task compute-cluster task-id)
      (catch Throwable t
        (log/error t "In compute cluster" name ", error killing task" task-id)))))

(defn kill-task-if-possible
  "If compute cluster is nil, print a warning instead of killing the task. There are cases, in particular,
  lingering tasks, stragglers, or cancelled tasks where the task might outlive the compute cluster it was
  member of. When this occurs, the looked up compute cluster is null and trying to kill via it would cause an NPE,
  when in reality, it's relatively innocuous. So, we have this wrapper to use in those circumstances."
  [compute-cluster task-id]
  (if compute-cluster
    (safe-kill-task compute-cluster task-id)
    (log/warn "Unable to kill task" task-id "because compute-cluster is nil")))

; Internal method
(defn write-compute-cluster
  "Create a missing compute-cluster for one that's not yet in the database."
  [conn compute-cluster]
  (log/info "Installing a new compute cluster in datomic for " compute-cluster)
  (let [tempid (d/tempid :db.part/user)
        result @(d/transact
                  conn
                  [(assoc compute-cluster :db/id tempid)])]
    (d/resolve-tempid (d/db conn) (:tempids result) tempid)))

; Internal variable
(def cluster-name->compute-cluster-atom (atom {}))

(defn register-compute-cluster!
  "Register a compute cluster "
  [compute-cluster]
  (let [compute-cluster-name (compute-cluster-name compute-cluster)]
    (when (contains? @cluster-name->compute-cluster-atom compute-cluster-name)
      (throw (IllegalArgumentException.
               (str "Multiple compute-clusters have the same name: " compute-cluster
                    " and " (@cluster-name->compute-cluster-atom compute-cluster-name)
                    " with name " compute-cluster-name))))
    (log/info "Setting up compute cluster: " compute-cluster)
    (swap! cluster-name->compute-cluster-atom assoc compute-cluster-name compute-cluster)
    nil))

(defn compute-cluster-name->ComputeCluster
  "From the name of a compute cluster, return the object. May return nil if not found."
  [compute-cluster-name]
  (let [result (@cluster-name->compute-cluster-atom compute-cluster-name)]
    (when-not result
      (log/error "Was asked to lookup db-id for" compute-cluster-name "and got nil"))
    result))

(defn get-default-cluster-for-legacy
  "What cluster name to put on for legacy jobs when generating their compute-cluster.
  TODO: Will want this to be configurable when we support multiple mesos clusters."
  []
  {:post [%]} ; Never returns nil.
  (let [first-cluster-name (->> config/config
                                :settings
                                :compute-clusters
                                (map (fn [{:keys [config]}] (:compute-cluster-name config)))
                                first)]
    (compute-cluster-name->ComputeCluster first-cluster-name)))

(defn diff-map-keys
  "Return triple of keys from two maps: [only in left, only in right, in both]"
  [left right]
  (data/diff (set (keys left)) (set (keys right))))

(defn union-map-keys
  "Return the union of the keys from two maps"
  [left right]
  (set/union (keys left) (keys right)))

(defn compute-cluster-config-ent->compute-cluster-config
  "Convert Datomic dynamic cluster configuration entity to an object"
  [{:keys [compute-cluster-config/name
           compute-cluster-config/template
           compute-cluster-config/base-path
           compute-cluster-config/ca-cert
           compute-cluster-config/state
           compute-cluster-config/state-locked?]}]
  {:name name
   :template template
   :base-path base-path
   :ca-cert ca-cert
   :state (case state
            :compute-cluster-config.state/running :running
            :compute-cluster-config.state/draining :draining
            :compute-cluster-config.state/deleted :deleted
            state)
   :state-locked? state-locked?})

(defn compute-cluster-config->compute-cluster-config-ent
  "Convert dynamic cluster configuration to a Datomic entity"
  [{:keys [name template base-path ca-cert state state-locked?]}]
  {:compute-cluster-config/name name
   :compute-cluster-config/template template
   :compute-cluster-config/base-path base-path
   :compute-cluster-config/ca-cert ca-cert
   :compute-cluster-config/state (case state
                                   :running :compute-cluster-config.state/running
                                   :draining :compute-cluster-config.state/draining
                                   :deleted :compute-cluster-config.state/deleted
                                   :state)
   :compute-cluster-config/state-locked? state-locked?})

(defn db-config-ents
  ;TODO is it ok to fail to connect to the db and return empty list?
  "Get the current dynamic cluster configurations from the database"
  [db]
  (let [configs (map #(d/entity db %)
                     (d/q '[:find [?compute-cluster-config ...]
                            :where
                            [?compute-cluster-config :compute-cluster-config/name ?name]]
                          db))]
    (map-from-vals :compute-cluster-config/name configs)))

(defn compute-cluster->compute-cluster-config
  "Calculate dynamic cluster configuration from a compute cluster"
  [{:keys [compute-cluster-config state-atom state-locked?-atom]}]
  {:name (:name compute-cluster-config)
   :template (:template compute-cluster-config)
   :base-path (:base-path compute-cluster-config)
   :ca-cert (:ca-cert compute-cluster-config)
   :state @state-atom
   :state-locked? @state-locked?-atom})

(defn in-mem-configs
  "Get the current in-memory dynamic cluster configurations"
  []
  ; TODO: why can't you call (v :state) on a record????
  (->> (->> @cluster-name->compute-cluster-atom
            (keep (fn [[name cluster]] (when (:state-atom cluster) [name cluster])))
            (#(for-map [[name cluster] %] name cluster)))
       (map-vals compute-cluster->compute-cluster-config)))


(defn compute-current-configs
  "Synthesize the current view of cluster configurations by looking at the current configurations in the database
  and the current configurations in memory. Alert on any inconsistencies. In memory wins on inconsistencies."
  [current-db-configs current-in-mem-configs]
  (let [[only-db-keys only-in-mem-keys both-keys] (diff-map-keys current-db-configs current-in-mem-configs)]
    (doseq [only-db-key only-db-keys]
      (when (not= :deleted (-> only-db-key current-db-configs :state))
        (log/error "In-memory cluster configuration does not match the database. Cluster is only in the database and is not deleted."
                   {:cluster-name only-db-key :cluster (current-db-configs only-db-key)})))
    (doseq [only-in-mem-key only-in-mem-keys]
      (when (not= :deleted (-> only-in-mem-key current-in-mem-configs :state))
        (log/error "In-memory cluster configuration is missing from the database. Cluster is only in memory and is not deleted."
                   {:cluster-name only-in-mem-key :cluster (current-in-mem-configs only-in-mem-key)})))
    (doseq [key both-keys]
      (let [keys-to-keep-synced [:base-path :ca-cert :state]]
        (when (not= (-> key current-db-configs (select-keys keys-to-keep-synced))
                    (-> key current-in-mem-configs (select-keys keys-to-keep-synced)))
          (log/error "Base path, CA cert, or state differ between in-memory and database cluster configurations."
                     {:cluster-name key
                      :in-memory-cluster (current-in-mem-configs key)
                      :db-cluster (current-db-configs key)})))))
  (merge current-in-mem-configs current-db-configs))

(defn get-job-instance-ids-for-cluster-name
  "Get the datomic ids of job instances that are running on the given compute cluster"
  [db cluster-name]
  (d/q '[:find [?job-instance ...]
         :in $ ?cluster-name [?status ...]
         :where
         [?cluster :compute-cluster/cluster-name ?cluster-name]
         [?job-instance :instance/compute-cluster ?cluster]
         [?job-instance :instance/status ?status]]
       db cluster-name [:instance.status/running :instance.status/unknown]))

(defn cluster-state-change-valid?
  "Check that the cluster state transition is valid."
  [db current-state new-state cluster-name]
  (case current-state
    :running (case new-state
               :running true
               :draining true
               :deleted false
               false)
    :draining (case new-state
                :running true
                :draining true
                :deleted (empty? (get-job-instance-ids-for-cluster-name db cluster-name))
                false)
    :deleted (case new-state
               :running false
               :draining false
               :deleted true
               false)
    false))

(defn compute-config-update
  "Add validation info to a dynamic cluster configuration update."
  [db current new force?]
  (assoc (cond
           (not (cluster-state-change-valid? db (:state current) (:state new) (:name current)))
           {:valid? false
            :reason (str "Cluster state transition from " (:state current) " to " (:state new) " is not valid.")}
           force?
           {:valid? true}
           (and (not= (:state current) (:state new)) (:state-locked? current))
           {:valid? false
            :reason (str "Attempting to change cluster state from "
                         (:state current) " to " (:state new) " but not able because it is locked.")}
           (not= (dissoc current :state) (dissoc new :state))
           {:valid? false
            :reason (str "Attempting to change something other than state when force? is false. Diff is "
                         (pr-str (data/diff (dissoc current :state) (dissoc new :state))))}
           :else
           {:valid? true})
    :update? true :config new :changed? (not= current new) :cluster-name (:name new)))

(defn compute-config-insert
  "Add validation info to a new dynamic cluster configuration."
  [new]
  (let [config-from-template ((config/compute-cluster-templates) (:template new))]
    (assoc (cond
             (not config-from-template)
             {:valid? false
              :reason (str "Attempting to create cluster with unknown template: " (:template new))}
             (not (:factory-fn config-from-template))
             {:valid? false
              :reason (str "Template for cluster has no factory-fn: " config-from-template)}
             :else
             {:valid? true})
      :insert? true :config new :changed? true :cluster-name (:name new))))

(defn check-for-unique-constraint-violations
  "Check that the proposed resulting configurations don't collide on fields that should be unique, e.g. :base-path"
  [changes resulting-configs unique-constraint-field]
  (let [constraint-value->clusters (reduce (fn [m [cluster config]]
                                             (update m (config unique-constraint-field) conj cluster))
                                           {} resulting-configs)]
    (->> changes
         (map
           #(let [{:keys [config]} %
                  unique-constraint-value (config unique-constraint-field)
                  keys-for-unique-value (-> unique-constraint-value constraint-value->clusters set)]
              (cond-> %
                (> (count keys-for-unique-value) 1)
                (assoc :valid? false
                       :reason (str unique-constraint-field " is not unique between clusters " keys-for-unique-value))))))))

(defn compute-config-updates
  "Take the current and desired configurations and compute the changes. Alert on invalid changes."
  [db current-configs new-configs force?]
  (let [[deletes-keys inserts-keys updates-keys] (diff-map-keys current-configs new-configs)
        changes (->> (concat
                       (map #(let [current (current-configs %)]
                               (compute-config-update db current (assoc current :state :deleted) force?)) deletes-keys)
                       (map #(compute-config-insert (new-configs %)) inserts-keys)
                       (map #(compute-config-update db (current-configs %) (new-configs %) force?) updates-keys))
                     (filter :changed?))
        resulting-configs (->> changes
                               (filter :valid?)
                               (#(for-map [change %] (:cluster-name change) (:config change)))
                               (merge current-configs))]
    (-> changes
      (check-for-unique-constraint-violations resulting-configs :base-path)
      (check-for-unique-constraint-violations resulting-configs :ca-cert))))

;TODO see if this is ok or need a better way
(def scheduler-promise (promise))
(def exit-code-syncer-state-promise (promise))

(defn add-new-cluster!
  "Add a new cluster from a dynamic cluster config"
  [config]
  (try
    (let [config-from-template ((config/compute-cluster-templates) (:template config))
          factory-fn (:factory-fn config-from-template)
          resolved (cook.util/lazy-load-var factory-fn)
          config (merge (:config config-from-template) config)
          cluster (resolved config {:exit-code-syncer-state @exit-code-syncer-state-promise})]
      (initialize-cluster cluster (:pool-name->fenzo @scheduler-promise)))
    ;TODO insert to db
    {:update-succeeded true}
    (catch Throwable t
      (log/error t "Failed to update cluster" config)
      {:update-succeeded false :error-message (.toString t)})))

(defn update-cluster!
  "Update a cluster with a dynamic cluster config"
  [config]
  ;TODO update in db
  ;TODO  alert if db-id is nil and don't update
  ;TODO update in mem
  {:update-succeeded true})

; TODO make sure anything in "locking" path times out. like db calls
; TODO do alerts in callers of this function on return value
(defn update-dynamic-clusters
  "This function allows adding or updating the current compute cluster configurations. It takes
  in a single configuration and/or a map of configurations with cluser name as the key. Passing in a map of
  configurations implies that these are the only known configurations, and clusters that are missing from this
  map should be removed.
  Only the state of an existing cluster and its configuration can be changed unless force? is set to true."
  [conn new-config new-configs force?]
  {:pre [(= 1 (->> [new-config new-configs] (filter some?) count))]}
  (locking cluster-name->compute-cluster-atom
    (let [db (d/db conn)
          current-db-config-ents (db-config-ents db)
          current-db-configs (map-vals compute-cluster-config-ent->compute-cluster-config current-db-config-ents)
          current-configs (compute-current-configs current-db-configs (in-mem-configs))
          new-configs' (cond-> (or new-configs current-configs) new-config (assoc (:name new-config) new-config))
          updates (compute-config-updates db current-configs new-configs' force?)]
      (log/info "Updating dynamic clusters." {:current-configs current-configs :new-config new-config :new-configs new-configs :force? force? :updates updates})
      (->> updates
           (map #(let [{:keys [valid? insert? update? config]} %]
                   (assoc %
                     :update-result
                     (when valid?
                       (cond
                         insert? (add-new-cluster! config)
                         update? (update-cluster! config))))))
           doall))))