(ns konserve-gcs.emulator-test
  "Tests using local fake-gcs-server emulator.

   Run with: docker-compose up -d
   Then: clojure -X:test"
  (:require [clojure.test :refer [deftest testing is]]
            [konserve.compliance-test :refer [compliance-test]]
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

(deftest emulator-optimistic-locking-concurrent-test
  (testing "Concurrent updates with optimistic locking - multiple store instances"
    (ensure-bucket emulator-spec)
    (let [store-id (str "optimistic-test-" (UUID/randomUUID))
          spec (assoc emulator-spec
                      :backend :gcs
                      :store-path store-id
                      :id (random-store-id)
                      :config {:optimistic-locking-retries 50})  ;; High retry count for concurrent test
          ;; Clean up first
          _ (try (store/delete-store spec {:sync? true}) (catch Exception _))
          ;; Create initial store to set up counter
          s-init (store/create-store spec {:sync? true})
          _ (k/assoc-in s-init [:counter] 0 {:sync? true})
          _ (store/release-store spec s-init {:sync? true})

          num-threads 5
          increments-per-thread 10
          expected-total (* num-threads increments-per-thread)

          ;; Track retry count and errors
          error-count (atom 0)
          success-count (atom 0)

          ;; Run concurrent updates - each thread connects to the existing store
          futures (doall
                   (for [thread-id (range num-threads)]
                     (future
                       ;; Each thread connects to the same GCS store (separate instance, same data)
                       (let [thread-store (store/connect-store spec {:sync? true})]
                         (try
                           (dotimes [_i increments-per-thread]
                             (try
                               ;; Use truly synchronous mode so retry logic works with try/catch
                               (k/update-in thread-store [:counter] (fnil inc 0) {:sync? true})
                               (swap! success-count inc)
                               (catch Exception e
                                 (swap! error-count inc)
                                 (throw e))))
                           (finally
                             (store/release-store spec thread-store {:sync? true})))))))]

      ;; Wait for all threads to complete
      (doseq [f futures]
        @f)

      ;; Verify final count
      (let [s-final (store/connect-store spec {:sync? true})
            final-count (k/get-in s-final [:counter] nil {:sync? true})]
        (println "Successful updates:" @success-count)
        (println "Errors encountered:" @error-count)
        (is (= expected-total final-count)
            (str "Expected " expected-total " but got " final-count))
        (store/release-store spec s-final {:sync? true}))

      ;; Clean up
      (store/delete-store spec {:sync? true}))))
