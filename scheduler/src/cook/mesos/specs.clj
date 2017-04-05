(ns cook.mesos.specs
  (:require [clojure.spec :as s]
            [cook.mesos.api :as api]
            [clojure.string :as str]
            [clojure.test :refer :all]))

(defn- pos-double?
  "Returns true if n is a positive double"
  [n]
  (and (pos? n) (double? n)))

(defn- valid-port?
  "Returns true if n is an integer in the range [0, 65536]"
  [n]
  #(s/int-in-range? 0 65537 n))

(defn- non-empty-string?
  "Returns true if s is a non-empty string"
  [s]
  (and (string? s) (not (str/blank? s))))

; Helpers
(s/def :cook/string-map (s/map-of non-empty-string? string?))

; Required job fields
(s/def :cook.job/command string?)
(s/def :cook.job/cpus pos-double?)
(s/def :cook.job/max-retries pos-int?)
(s/def :cook.job/max-runtime pos-int?)
(s/def :cook.job/mem pos-double?)
(s/def :cook.job/name (s/and string? api/max-128-characters-and-alphanum?))
(s/def :cook.job/priority #(s/int-in-range? 0 101 %))
(s/def :cook.job/user (s/and string? #(re-matches #"\A[a-z][a-z0-9_-]{0,62}[a-z0-9]\z" %)))
(s/def :cook.job/uuid uuid?)

; Job application
(s/def :cook.application/name (s/and string? api/non-empty-max-128-characters-and-alphanum?))
(s/def :cook.application/version (s/and string? api/non-empty-max-128-characters-and-alphanum?))
(s/def :cook.job/application (s/keys :req-un [:cook.application/name
                                              :cook.application/version]))

; Job container
(s/def :cook.container/type string?)
(s/def :cook.docker/image string?)
(s/def :cook.docker/network string?)
(s/def :cook.docker/force-pull-image boolean?)
(s/def :cook.docker-param/key string?)
(s/def :cook.docker-param/value string?)
(s/def :cook.docker/parameter (s/keys :req-un [:cook.docker-param/key
                                               :cook.docker-param/value]))
(s/def :cook.docker/parameters (s/coll-of :cook.docker/parameter :kind vector?))
(s/def :cook.docker-port-mapping/host-port valid-port?)
(s/def :cook.docker-port-mapping/container-port valid-port?)
(s/def :cook.docker-port-mapping/protocol string?)
(s/def :cook.docker/port-mapping-instance (s/keys :req-un [:cook.docker-port-mapping/host-port
                                                           :cook.docker-port-mapping/container-port]
                                                  :opt-un [:cook.docker-port-mapping/protocol]))
(s/def :cook.docker/port-mapping (s/coll-of :cook.docker/port-mapping-instance :kind vector?))
(s/def :cook.container/docker (s/keys :req-un [:cook.docker/image]
                                      :ope-un [:cook.docker/network
                                               :cook.docker/force-pull-image
                                               :cook.docker/parameters
                                               :cook.docker/port-mapping]))
(s/def :cook.docker-volume/host-path string?)
(s/def :cook.docker-volume/container-path string?)
(s/def :cook.docker-volume/mode string?)
(s/def :cook.docker/volume (s/keys :req-un [:cook.docker-volume/host-path]
                                   :opt-un [:cook.docker-volume/container-path
                                            :cook.docker-volume/mode]))
(s/def :cook.docker/volumes (s/coll-of :cook.docker/volume :kind vector?))
(s/def :cook.job/container (s/keys :req-un [:cook.container/type]
                                   :opt-un [:cook.container/docker
                                            :cook.container/volumes]))

; Job URIs
(s/def :cook.job-uri/value string?)
(s/def :cook.job-uri/executable? boolean?)
(s/def :cook.job-uri/extract? boolean?)
(s/def :cook.job-uri/cache? boolean?)
(s/def :cook.job/uri (s/keys :req-un [:cook.job-uri/value]
                             :opt-un [:cook.job-uri/executable?
                                      :cook.job-uri/extract?
                                      :cook.job-uri/cache?]))
(s/def :cook.job/uris (s/coll-of :cook.job/uri :kind vector?))

; Other optional job fields
(s/def :cook.job/disable-mea-culpa-retries boolean?)
(s/def :cook.job/env :cook/string-map)
(s/def :cook.job/gpus pos-int?)
(s/def :cook.job/group uuid?)
(s/def :cook.job/labels :cook/string-map)
(s/def :cook.job/ports nat-int?)

; Job
(s/def :cook/job
  (s/keys :req-un [:cook.job/command
                   :cook.job/cpus
                   :cook.job/max-retries
                   :cook.job/max-runtime
                   :cook.job/mem
                   :cook.job/name
                   :cook.job/priority
                   :cook.job/user
                   :cook.job/uuid]
          :opt-un [:cook.job/application
                   :cook.job/container
                   :cook.job/disable-mea-culpa-retries
                   :cook.job/env
                   :cook.job/gpus
                   :cook.job/group
                   :cook.job/labels
                   :cook.job/ports
                   :cook.job/uris]))

; Scratch
(deftest test-make-job-txn
  (api/make-job-txn {:foo 1
                     :bar 2
                     :baz 3}))