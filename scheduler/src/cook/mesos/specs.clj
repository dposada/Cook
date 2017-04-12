(ns cook.mesos.specs
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck.generators :as gen']
            [cook.mesos.api :as api]))

; Helpers
(s/def :cook/non-empty-string (s/and string? (complement str/blank?)))
(s/def :cook/string->string (s/map-of :cook/non-empty-string string?))
(s/def :cook/keyword->string (s/map-of keyword? string?))
(s/def :cook/pos-double (s/and double? pos?))
(s/def :cook/pos-number (s/and number? pos?))
(s/def :cook/port (s/int-in 0 65537))

; Required job fields
(s/def :cook.job/command string?)
(s/def :cook.job/cpus :cook/pos-double)
(s/def :cook.job/cpus-request :cook/pos-number)
(s/def :cook.job/max-retries pos-int?)
(s/def :cook.job/max-runtime pos-int?)
(s/def :cook.job/mem :cook/pos-double)
(s/def :cook.job/mem-request :cook/pos-number)
(s/def :cook.job/name (s/and string? api/max-128-characters-and-alphanum?))
(s/def :cook.job/priority (s/int-in 0 101))
(s/def :cook.job/user (s/with-gen
                        (s/and string? #(re-matches #"\A[a-z][a-z0-9_-]{0,62}[a-z0-9]\z" %))
                        #(gen'/string-from-regex #"[a-z][a-z0-9_-]{0,62}[a-z0-9]")))
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
(s/def :cook.docker-port-mapping/host-port :cook/port)
(s/def :cook.docker-port-mapping/container-port :cook/port)
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
(s/def :cook.container-volume/host-path string?)
(s/def :cook.container-volume/container-path string?)
(s/def :cook.container-volume/mode string?)
(s/def :cook.container/volume (s/keys :req-un [:cook.container-volume/host-path]
                                      :opt-un [:cook.container-volume/container-path
                                               :cook.container-volume/mode]))
(s/def :cook.container/volumes (s/coll-of :cook.container/volume :kind vector?))
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
(s/def :cook.job-uri/executable boolean?)
(s/def :cook.job-uri/extract boolean?)
(s/def :cook.job-uri/cache boolean?)
(s/def :cook.job/uri-request (s/keys :req-un [:cook.job-uri/value]
                                     :opt-un [:cook.job-uri/executable
                                              :cook.job-uri/extract
                                              :cook.job-uri/cache]))
(s/def :cook.job/uris (s/coll-of :cook.job/uri :kind vector?))
(s/def :cook.job/uris-request (s/coll-of :cook.job/uri-request :kind vector?))

; Other optional job fields
(s/def :cook.job/disable-mea-culpa-retries boolean?)
(s/def :cook.job/env :cook/string->string)
(s/def :cook.job/env-request :cook/keyword->string)
(s/def :cook.job/gpus (s/int-in 1 Integer/MAX_VALUE))
(s/def :cook.job/group uuid?)
(s/def :cook.job/labels :cook/string->string)
(s/def :cook.job/labels-request :cook/keyword->string)
(s/def :cook.job/ports nat-int?)

; Job
(s/def :cook/job-shared
  (s/keys :req-un [:cook.job/command
                   :cook.job/max-retries
                   :cook.job/name
                   :cook.job/priority
                   :cook.job/uuid]
          :opt-un [:cook.job/application
                   :cook.job/container
                   :cook.job/disable-mea-culpa-retries
                   :cook.job/gpus
                   :cook.job/group
                   :cook.job/ports]))
(s/def :cook/job
  (s/merge :cook/job-shared
           (s/keys :req-un [:cook.job/cpus
                            :cook.job/max-runtime
                            :cook.job/mem
                            :cook.job/user]
                   :opt-un [:cook.job/env
                            :cook.job/labels
                            :cook.job/uris])))
(s/def :cook/job-request
  (s/merge :cook/job-shared
           (s/keys :req-un [:cook.job/cpus-request
                            :cook.job/mem-request]
                   :opt-un [:cook.job/env-request
                            :cook.job/labels-request
                            ;; Make max-runtime optional.
                            ;; It is *not* optional internally but don't want to force users to set it
                            :cook.job/max-runtime
                            :cook.job/uris-request])))

; Datomic datom
(s/def :cook-datom/assertion (s/and keyword? #(= % :db/add)))
(s/def :cook-datom/entity (s/keys :req-un [:cook-datom/part
                                           :cook-datom/idx]))
(s/def :cook/datom (s/or
                     :map (s/keys :req [:db/id])
                     :tuple (s/tuple :cook-datom/assertion :cook-datom/entity keyword? any?)))

; Create-jobs context
(s/def ::api/jobs (s/coll-of :cook/job-request :kind vector?))
(s/def :cook/create-jobs-ctx (s/keys :req [::api/jobs
                                           ::api/groups]))

; Function specs
(s/fdef api/make-job-txn
        :args (s/cat :job :cook/job)
        :ret (s/coll-of :cook/datom))

;(s/fdef api/create-jobs!
;        )