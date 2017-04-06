(ns cook.test.mesos.specs
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.test :refer :all]
            [cook.mesos.api :as api]
            [cook.mesos.specs :as specs]
            [cook.test.testutil :refer :all]
            [datomic.api :as d :refer (db)]))

(deftest test-make-job-txn
  (api/make-job-txn {:foo 1
                     :bar 2
                     :baz 3}))

(defn- validate-job
  "Delegates to api/validate-and-munge-job, using the given job and db, the user alice, and
  resource constraints that are guaranteed to not cause api/validate-and-munge-job to throw"
  [job db]
  (let [user "alice"
        task-constraints {:cpus (-> job :cpus inc)
                          :memory-gb (-> job :mem (/ 1024) inc)}
        gpu-enabled? (:gpus job)]
    (api/validate-and-munge-job db user task-constraints gpu-enabled? nil job)))

(deftest test-validate-and-munge-job
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        db (db conn)]
    (doseq [job (gen/sample (s/gen :cook/job) 100)]
      (is (validate-job job db)))))
