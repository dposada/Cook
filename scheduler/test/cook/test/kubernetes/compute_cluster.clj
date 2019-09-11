(ns cook.test.kubernetes.compute-cluster
  (:require [clojure.core.cache :as cache]
            [clojure.test :refer :all]
            [cook.compute-cluster :as cc]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.compute-cluster :as kcc]
            [cook.kubernetes.controller :as controller]
            [cook.mesos.task :as task]
            [cook.scheduler.scheduler :as sched]
            [cook.test.testutil :as tu]
            [cook.tools :as util]
            [datomic.api :as d])
  (:import (com.netflix.fenzo SimpleAssignmentResult)))

(deftest test-get-or-create-cluster-entity-id
  (let [conn (tu/restore-fresh-database! "datomic:mem://test-get-or-create-cluster-entity-id")]
    (testing "successfully creates clusters"
      (let [eid (kcc/get-or-create-cluster-entity-id conn "test-a")
            entity (d/entity (d/db conn) eid)]
        (is (= "test-a" (:compute-cluster/cluster-name entity)))
        (is (= :compute-cluster.type/kubernetes (:compute-cluster/type entity)))))
    (testing "does not create duplicate clusters"
      (let [eid (kcc/get-or-create-cluster-entity-id conn "test-b")
            eid2 (kcc/get-or-create-cluster-entity-id conn "test-b")]
        (is eid)
        (is eid2)
        (is (= eid eid2))))))

(deftest test-namespace-config
  (tu/setup)
  (let [conn (tu/restore-fresh-database! "datomic:mem://test-namespace-config")
        task-assignment-result-helper (fn [user]
                                        (let [job-id (tu/create-dummy-job conn :user user)
                                              job-ent (d/entity (d/db conn) job-id)]
                                          (-> job-ent
                                              tu/make-task-request
                                              tu/make-task-assignment-result)))
        launched-pod-atom (atom nil)]
    (with-redefs [api/launch-task (fn [api {:keys [launch-pod]}]
                                    (reset! launched-pod-atom launch-pod))]
      (testing "static namespace"
        (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                              (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                              {:kind :static :namespace "cook"} nil)
              task-metadata (task/TaskAssignmentResult->task-metadata (d/db conn)
                                                                      nil
                                                                      compute-cluster
                                                                      (task-assignment-result-helper "testuser"))]

          (cc/launch-tasks compute-cluster [] [task-metadata])
          (is (= "cook" (-> @launched-pod-atom
                            :pod
                            .getMetadata
                            .getNamespace)))))

      (testing "per-user namespace"
        (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                              (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                              {:kind :per-user} nil)
              task-metadata (task/TaskAssignmentResult->task-metadata (d/db conn)
                                                                      nil
                                                                      compute-cluster
                                                                      (task-assignment-result-helper "testuser"))]
          (cc/launch-tasks compute-cluster [] [task-metadata])
          (is (= "testuser" (-> @launched-pod-atom
                            :pod
                            .getMetadata
                            .getNamespace))))))))

(deftest test-generate-offers
  (tu/setup)
  (with-redefs [api/launch-task (constantly nil)]
    (let [conn (tu/restore-fresh-database! "datomic:mem://test-generate-offers")
          compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                          (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                          {:kind :static :namespace "cook"} nil)
          node-name->node {"nodeA" (tu/node-helper "nodeA" 1.0 1000.0)
                           "nodeB" (tu/node-helper "nodeB" 1.0 1000.0)
                           "nodeC" (tu/node-helper "nodeC" 1.0 1000.0)
                           "my.fake.host" (tu/node-helper "my.fake.host" 1.0 1000.0)}
          j1 (tu/create-dummy-job conn :ncpus 0.1)
          j2 (tu/create-dummy-job conn :ncpus 0.2)
          db (d/db conn)
          job-ent-1 (d/entity db j1)
          job-ent-2 (d/entity db j2)
          task-1 (tu/make-task-metadata job-ent-1 db compute-cluster)
          _ (cc/launch-tasks compute-cluster nil [task-1
                                                  (tu/make-task-metadata job-ent-2 db compute-cluster)])
          task-1-id (-> task-1 :task-request :task-id)
          pod-name->pod {{:namespace "cook" :name "podA"} (tu/pod-helper "podA" "nodeA"
                                                                         {:cpus 0.25 :mem 250.0}
                                                                         {:cpus 0.1 :mem 100.0})
                         {:namespace "cook" :name "podB"} (tu/pod-helper "podB" "nodeA"
                                                                         {:cpus 0.25 :mem 250.0})
                         {:namespace "cook" :name "podC"} (tu/pod-helper "podC" "nodeB"
                                                                         {:cpus 1.0 :mem 1100.0})
                         {:namespace "cook" :name task-1-id} (tu/pod-helper task-1-id "my.fake.host"
                                                                            {:cpus 0.1 :mem 10.0})}
          offers (kcc/generate-offers compute-cluster node-name->node pod-name->pod
                                      (controller/starting-namespaced-pod-name->pod compute-cluster))]
      (is (= 4 (count offers)))
      (let [offer (first (filter #(= "nodeA" (:hostname %))
                                 offers))]
        (is (not (nil? offer)))
        (is (= "kubecompute" (:framework-id offer)))
        (is (= {:value "nodeA"} (:slave-id offer)))
        (is (= [{:name "mem" :type :value-scalar :scalar 400.0}
                {:name "cpus" :type :value-scalar :scalar 0.4}
                {:name "disk" :type :value-scalar :scalar 0.0}]
               (:resources offer)))
        (is (:reject-after-match-attempt offer)))

      (let [offer (first (filter #(= "nodeB" (:hostname %))
                                 offers))]
        (is (= {:value "nodeB"} (:slave-id offer)))
        (is (= [{:name "mem" :type :value-scalar :scalar 0.0}
                {:name "cpus" :type :value-scalar :scalar 0.0}
                {:name "disk" :type :value-scalar :scalar 0.0}]
               (:resources offer))))

      (let [offer (first (filter #(= "my.fake.host" (:hostname %)) offers))]
        (is (= [{:name "mem" :type :value-scalar :scalar 980.0}
                {:name "cpus" :type :value-scalar :scalar 0.7}
                {:name "disk" :type :value-scalar :scalar 0.0}]
               (:resources offer)))))))

(deftest determine-expected-state
  ; TODO
  )