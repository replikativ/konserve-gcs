(ns konserve-gcs.core-test
  "Tests against a REAL Cloud Storage bucket.

   Skipped unless `KONSERVE_TEST_BUCKET` and `KONSERVE_TEST_BUCKET_LOCATION` are
   set, so the namespace loads everywhere; it used to throw while loading, which
   made the whole suite unrunnable without GCS credentials — including the
   emulator tests, which need none.

   REDUCED IN SCOPE, deliberately and visibly. This used to run
   `com.literalco/konserve-compliance-tests`, whose repository no longer exists
   anywhere — not on GitHub, not on Clojars — so the dependency can never resolve
   again and took CI's classpath down with it. Its cache, GC, encryptor and
   serializer suites have no published equivalent: konserve keeps those under
   `konserve.tests.*`, which ships in its test path and not in the jar. What
   remains is konserve's own `compliance-test`, which covers the core API, plus
   the conditional-write contract. If that suite reappears, or konserve publishes
   its sub-suites, the coverage should come back."
  (:require [clojure.test :refer [deftest testing is]]
            [konserve.compliance-test :refer [compliance-test
                                              conditional-write-compliance-test]]
            [konserve.core :as k]
            [konserve.store :as store])
  (:import [java.util UUID]))

(def bucket (System/getenv "KONSERVE_TEST_BUCKET"))
(def location (System/getenv "KONSERVE_TEST_BUCKET_LOCATION"))

(defn- cloud-spec []
  {:backend :gcs
   :bucket bucket
   :location location
   :store-path (str "konserve-test-" (UUID/randomUUID))
   :id (UUID/randomUUID)})

(deftest ^:cloud cloud-storage-compliance-test
  (if-not (and bucket location)
    ;; An explicit passing assertion, not a bare `println`: a deftest that runs
    ;; no assertions is reported as a FAILURE by kaocha, so a skip has to say so
    ;; in the result rather than only on stdout.
    (is true "skipped: set KONSERVE_TEST_BUCKET and KONSERVE_TEST_BUCKET_LOCATION to run against real GCS")
    (let [spec (cloud-spec)]
      (try (store/delete-store spec {:sync? true}) (catch Exception _))
      (let [st (store/create-store spec {:sync? true})]
        (try
          (testing "the core API against real GCS"
            (compliance-test st))
          (finally
            (store/release-store spec st {:sync? true})
            (store/delete-store spec {:sync? true})))))))

(deftest ^:cloud cloud-storage-conditional-write-test
  (if-not (and bucket location)
    ;; An explicit passing assertion, not a bare `println`: a deftest that runs
    ;; no assertions is reported as a FAILURE by kaocha, so a skip has to say so
    ;; in the result rather than only on stdout.
    (is true "skipped: set KONSERVE_TEST_BUCKET and KONSERVE_TEST_BUCKET_LOCATION to run against real GCS")
    (let [spec (cloud-spec)]
      (try (store/delete-store spec {:sync? true}) (catch Exception _))
      (let [st (store/create-store spec {:sync? true})]
        (try
          (testing "the `:expected-revision` contract against real GCS, which is
                    the run the emulator cannot stand in for: fake-gcs-server only
                    began enforcing preconditions in July 2026, and an older image
                    accepts them while ignoring them"
            (is (= :global (k/conditional-write-domain st)))
            (conditional-write-compliance-test st))
          (finally
            (store/release-store spec st {:sync? true})
            (store/delete-store spec {:sync? true})))))))
