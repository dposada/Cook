(ns cook.test.mesos.specs
  (:require [clojure.pprint :as pprint]
            [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.spec.test :as stest]
            [clojure.string :as str]
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

(defn- sample-job
  "Generates a sample of Cook jobs of size n"
  [n]
  (gen/sample (s/gen :cook/job) n))

(deftest test-validate-and-munge-job
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        db (db conn)]
    (doseq [job (sample-job 100)]
      (is (validate-job job db)))))

(deftest test-make-job-txn
  (doseq [job (sample-job 100)]
    (is (api/make-job-txn job))))

(defn summarize-results'
  "Prints the abbrev-results of the given spec-check"
  [spec-check]
  (map (comp #(pprint/write % :stream nil) stest/abbrev-result) spec-check))

(defmacro is-not-failure
  "Asserts that the given spec-check was not a failure"
  [spec-check]
  `(is (nil? (-> ~spec-check first :failure))
       (str/join "\n\n" (summarize-results' ~spec-check))))

(defn- check'
  "Delegates to stest/check with num-tests number of tests"
  [sym num-tests]
  (stest/check sym {:clojure.spec.test.check/opts {:num-tests num-tests}}))

(deftest test-check-make-job-txn
  (is-not-failure (check' `api/make-job-txn 100)))
