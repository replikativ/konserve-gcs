(ns konserve-gcs.emulator-test
  "Tests using local fake-gcs-server emulator.

   Run with: docker-compose up -d
   Then: clojure -X:test"
  (:require [clojure.test :refer [deftest testing is]]
            [konserve.compliance-test :refer [compliance-test
                                              conditional-write-compliance-test]]
            [konserve.impl.storage-layout :as sl]
            [konserve-gcs.core :as gcs]
            [konserve.core :as k]
            [konserve.store :as store])
  (:import [java.util UUID]))

;; Generate random UUIDs for store IDs (required by konserve 0.9.332+)
(defn- random-store-id [] (UUID/randomUUID))

;; Test store IDs - using stable UUIDs for reproducibility
(def sync-store-id "sync-test-store")
(def async-store-id "async-test-store")
(def exists-store-id "exists-test-store")
(def store1-id "store1-test")
(def store2-id "store2-test")

(def emulator-spec
  "Configuration for fake-gcs-server emulator."
  {:location "US-EAST1"
   :bucket "konserve-test"
   :project-id "test-project"  ;; Required by GCS SDK even with emulator
   :host "http://localhost:4443"})

(defn- ensure-bucket
  "Create bucket if it doesn't exist (fake-gcs-server auto-creates on first write,
   but we create explicitly for clarity)."
  [spec]
  (let [client (gcs/cloud-storage-client spec)]
    (when-not (gcs/get-bucket client (:bucket spec))
      (gcs/create-bucket client (:location spec) (:bucket spec)))))

(deftest emulator-conditional-write-test
  (testing "the `:expected-revision` contract against the emulator.

            konserve's shared contract, called rather than restated — a backend
            that restates it drifts. NOTE the emulator must be current: older
            fake-gcs-server images accept `ifGenerationMatch` and ignore it, so a
            stale image turns this suite green while fencing nothing. Verified by
            hand against this one: a stale generation and `ifGenerationMatch=0` on
            an existing object both return 412."
    (let [spec (assoc emulator-spec :backend :gcs
                      :store-path (str "cas-" (UUID/randomUUID))
                      :id (random-store-id))]
      (ensure-bucket spec)
      (try (store/delete-store spec {:sync? true}) (catch Exception _))
      (let [st (store/create-store spec {:sync? true})]
        (try
          (is (= :global (k/conditional-write-domain st))
              "GCS evaluates the precondition, so the reach is every writer anywhere")
          (conditional-write-compliance-test st)
          (finally
            (store/release-store spec st {:sync? true})
            (store/delete-store spec {:sync? true})))))))

(deftest emulator-concurrent-fenced-counter-test
  (testing "concurrent increments converge when the caller fences and retries, and
            no update is lost.

            The contract alone cannot establish this: single-threaded, konserve's
            own `check-revision!` catches a stale token without the storage ever
            being asked to compare anything — measured on konserve-redis, where
            the contract still passed with the CAS replaced by a plain write. Only
            a concurrent test tells an honest fence from a claimed one."
    (let [spec (assoc emulator-spec :backend :gcs
                      :store-path (str "cas-" (UUID/randomUUID))
                      :id (random-store-id))]
      (ensure-bucket spec)
      (try (store/delete-store spec {:sync? true}) (catch Exception _))
      (let [init (store/create-store spec {:sync? true})
            _ (k/assoc-in init [:counter] 0 {:sync? true})
            _ (store/release-store spec init {:sync? true})
            threads 4 per-thread 8
            expected (* threads per-thread)
            conflicts (atom 0)
            unexpected (atom [])
            fs (doall
                (for [_ (range threads)]
                  (future
                    (let [st (store/connect-store spec {:sync? true})]
                      (try
                        (dotimes [_ per-thread]
                          (loop [tries 0]
                            (let [rev (k/revision st :counter {:sync? true})
                                  r (try (k/update-in st [:counter] (fnil inc 0)
                                                      {:sync? true :expected-revision rev})
                                         ::ok
                                         (catch Exception e (or (:type (ex-data e)) ::other)))]
                              (cond
                                (= ::ok r) :done
                                (= :konserve/revision-mismatch r)
                                (do (swap! conflicts inc)
                                    (if (< tries 500)
                                      (recur (inc tries))
                                      (swap! unexpected conj :retries-exhausted)))
                                :else (swap! unexpected conj r)))))
                        (finally (store/release-store spec st {:sync? true})))))))]
        (doseq [f fs] @f)
        (let [fin (store/connect-store spec {:sync? true})]
          (is (empty? @unexpected) (str "unexpected failures: " (pr-str @unexpected)))
          (is (= expected (k/get-in fin [:counter] nil {:sync? true}))
              "every increment must survive")
          (is (pos? @conflicts)
              (str "the threads must actually have contended (" @conflicts "); "
                   "a run with none shows the fence held but not that it was needed"))
          (store/release-store spec fin {:sync? true}))
        (store/delete-store spec {:sync? true})))))

(deftest emulator-compliance-sync-test
  (testing "GCS compliance test with emulator (sync)"
    (ensure-bucket emulator-spec)
    (let [spec (assoc emulator-spec :backend :gcs :store-path sync-store-id :id (random-store-id))
          _     (try (store/delete-store spec {:sync? true}) (catch Exception _))
          s     (store/create-store spec {:sync? true})]
      (compliance-test s)
      (store/release-store spec s {:sync? true})
      (store/delete-store spec {:sync? true}))))

(deftest emulator-compliance-async-test
  (testing "GCS compliance test with emulator (async)"
    (ensure-bucket emulator-spec)
    (let [spec (assoc emulator-spec :backend :gcs :store-path async-store-id :id (random-store-id))
          _     (try (store/delete-store spec {:sync? true}) (catch Exception _))
          s     (store/create-store spec {:sync? true})]
      (compliance-test s)
      (store/release-store spec s {:sync? true})
      (store/delete-store spec {:sync? true}))))

(deftest emulator-store-exists-test
  (testing "store-exists? with marker file"
    (ensure-bucket emulator-spec)
    (let [store-id (random-store-id)
          spec (assoc emulator-spec :backend :gcs :store-path exists-store-id :id store-id)]
      ;; Clean up first
      (try (store/delete-store spec {:sync? true}) (catch Exception _))

      ;; Initially should not exist
      (is (false? (store/store-exists? spec {:sync? true})))

      ;; Create store - should write marker
      (let [s (store/create-store spec {:sync? true})]
        (is (some? s))
        (is (true? (store/store-exists? spec {:sync? true})))

        ;; Should error if we try to create again
        (is (thrown-with-msg? Exception #"already exists"
                              (store/create-store spec {:sync? true})))

        ;; Delete should remove marker
        (store/release-store spec s {:sync? true})
        (store/delete-store spec {:sync? true})
        (is (false? (store/store-exists? spec {:sync? true})))))))

(deftest emulator-multi-store-test
  (testing "multiple stores in same bucket with different IDs"
    (ensure-bucket emulator-spec)
    (let [spec1 (assoc emulator-spec :backend :gcs :store-path store1-id :id (random-store-id))
          spec2 (assoc emulator-spec :backend :gcs :store-path store2-id :id (random-store-id))]

      ;; Clean up
      (try (store/delete-store spec1 {:sync? true}) (catch Exception _))
      (try (store/delete-store spec2 {:sync? true}) (catch Exception _))

      ;; Create both stores
      (let [s1 (store/create-store spec1 {:sync? true})
            s2 (store/create-store spec2 {:sync? true})]

        (is (true? (store/store-exists? spec1 {:sync? true})))
        (is (true? (store/store-exists? spec2 {:sync? true})))

        ;; Write to each
        (k/assoc-in s1 [:key1] "value1" {:sync? true})
        (k/assoc-in s2 [:key2] "value2" {:sync? true})

        ;; Verify isolation
        (is (= "value1" (k/get-in s1 [:key1] nil {:sync? true})))
        (is (nil? (k/get-in s1 [:key2] nil {:sync? true})))

        (is (= "value2" (k/get-in s2 [:key2] nil {:sync? true})))
        (is (nil? (k/get-in s2 [:key1] nil {:sync? true})))

        ;; Clean up
        (store/release-store spec1 s1 {:sync? true})
        (store/release-store spec2 s2 {:sync? true})
        (store/delete-store spec1 {:sync? true})
        (store/delete-store spec2 {:sync? true})

        (is (false? (store/store-exists? spec1 {:sync? true})))
        (is (false? (store/store-exists? spec2 {:sync? true})))))))

(deftest emulator-read-miss-safe-marker-test
  (testing "GCS backing implements PReadMissSafe (io-operation skips the -blob-exists? probe on reads)"
    (ensure-bucket emulator-spec)
    (let [store (gcs/connect-store (assoc emulator-spec :store-path "miss-safe-marker") :opts {:sync? true})]
      (is (satisfies? sl/PReadMissSafe (:backing store))))))
