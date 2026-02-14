(ns konserve-gcs.core
  (:require [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as impl :refer [PBackingLock -delete-store]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [konserve.store :as store]
            [superv.async :refer [go-try- <?-]]
            [taoensso.timbre :as log :refer [info trace]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.util Arrays]
           [com.google.cloud.storage Blob
            Blob$BlobSourceOption
            BlobId
            BlobInfo Bucket
            BucketInfo
            Storage
            Storage$BlobGetOption
            Storage$BlobListOption
            Storage$BlobSourceOption
            Storage$BlobTargetOption
            Storage$BucketGetOption
            Storage$BucketTargetOption
            Storage$CopyRequest
            StorageException
            StorageOptions]))

(def ^:dynamic *default-bucket* "konserve")
(def ^:dynamic *output-stream-buffer-size* (* 1024 1024))
(def ^:dynamic *deletion-batch-size* 1000)

(defn ^BlobId blob-id
  ([bucket blob-store-path]
   (BlobId/of bucket blob-store-path))
  ([bucket store-path blob-key]
   (BlobId/of bucket (str store-path "/" blob-key))))

(defn write-blob
  [client blob-id bytes]
  (let [blob-info (.build (BlobInfo/newBuilder ^BlobId blob-id))
        opts (into-array Storage$BlobTargetOption [])]
    (.create client ^BlobInfo blob-info #^bytes bytes #^Storage$BlobTargetOption opts)))

(defn read-blob [client blob-id]
  (let [opts (into-array Storage$BlobSourceOption [])]
    (.readAllBytes client blob-id opts)))

(defn read-blob-with-generation
  "Read blob and return map with :data and :generation, or nil if not found.
   Returns nil if blob doesn't exist or was updated during read (stale generation)."
  [^Storage client ^BlobId blob-id]
  (try
    (let [opts (into-array Storage$BlobGetOption [])
          ^Blob blob (.get client blob-id opts)]
      (when blob
        {:data (.getContent blob (into-array Blob$BlobSourceOption []))
         :generation (.getGeneration blob)}))
    (catch StorageException e
      ;; 404 can happen if blob was deleted or generation is stale
      (when-not (= 404 (.getCode e))
        (throw e)))))

(defn write-blob-conditional
  "Write blob with generation match precondition. Returns true on success, false on conflict.
   GCS returns HTTP 412 (Precondition Failed) when generation doesn't match."
  [^Storage client ^BlobId blob-id ^bytes bytes ^Long expected-generation]
  (try
    (let [blob-info (.build (BlobInfo/newBuilder blob-id))
          opts (into-array Storage$BlobTargetOption
                           [(Storage$BlobTargetOption/generationMatch expected-generation)])]
      (.create client blob-info bytes opts)
      true)
    (catch StorageException e
      (if (= 412 (.getCode e))
        false  ; Precondition failed - generation mismatch
        (throw e)))))

(defrecord CloudStorageBlob
           [bucket-store client bucket store-path blob-key data fetched-object generation]
  impl/PBackingBlob
  (-sync [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [{:keys [header meta value]} @data
                       baos (ByteArrayOutputStream. *output-stream-buffer-size*)
              ;; Get generation from bucket's cache (set during read)
                       current-generation (when-let [cache (:generation-cache bucket-store)]
                                            (get @cache blob-key))
                       optimistic-locking-retries (get-in env [:config :optimistic-locking-retries] 0)]
                   (if (and header meta value)
                     (do
                       (.write baos #^bytes header)
                       (.write baos #^bytes meta)
                       (.write baos #^bytes value)
                       (let [bytes (.toByteArray baos)
                             bid (blob-id bucket store-path blob-key)]
                         (if (and (pos? optimistic-locking-retries) current-generation)
                  ;; Use conditional write with generation - throw on conflict for retry
                           (when-not (write-blob-conditional client bid bytes current-generation)
                             (throw (ex-info "Optimistic lock conflict"
                                             {:type :optimistic-lock-conflict
                                              :key blob-key
                                              :generation current-generation})))
                  ;; Regular write without generation check
                           (write-blob client bid bytes)))
                       (.close baos)
                       (reset! data {})
                       (reset! generation nil)
              ;; Clear generation from cache after successful write
                       (when-let [cache (:generation-cache bucket-store)]
                         (swap! cache dissoc blob-key)))
                     (throw (ex-info "Updating a row is only possible if header, meta and value are set." {:data @data})))))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true)))                       ;; May not return nil, otherwise eternal retries
  (-read-header [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
        ;; first access is always to header, after it is cached
                 (when-not @fetched-object
                   (let [optimistic-locking-retries (get-in env [:config :optimistic-locking-retries] 0)
                         bid (blob-id bucket store-path blob-key)
                         response (if (pos? optimistic-locking-retries)
                           ;; Fetch with generation for optimistic locking
                                    (read-blob-with-generation client bid)
                           ;; Regular fetch without generation
                                    {:data (read-blob client bid)
                                     :generation nil})]
            ;; If read fails during optimistic locking (concurrent update), signal conflict
                     (when (and (pos? optimistic-locking-retries) (nil? response))
                       (throw (ex-info "Optimistic lock conflict - blob was concurrently modified during read"
                                       {:type :optimistic-lock-conflict
                                        :key blob-key})))
                     (reset! fetched-object (:data response))
            ;; Store generation in bucket's cache for later use
                     (when (:generation response)
                       (reset! generation (:generation response))
                       (when-let [cache (:generation-cache bucket-store)]
                         (swap! cache assoc blob-key (:generation response))))))
                 (Arrays/copyOfRange ^bytes @fetched-object (int 0) (int impl/header-size)))))
  (-read-meta [_ meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (Arrays/copyOfRange ^bytes @fetched-object (int impl/header-size) (int (+ impl/header-size meta-size))))))
  (-read-value [_ meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [obj ^bytes @fetched-object]
                   (Arrays/copyOfRange obj (int (+ impl/header-size meta-size)) (int (alength obj)))))))
  (-read-binary [_ meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [obj ^bytes @fetched-object]
                   (<?-
                    (locked-cb {:size (- (alength obj) (+ impl/header-size meta-size))
                                :input-stream
                                (ByteArrayInputStream.
                                 (Arrays/copyOfRange obj (int (+ impl/header-size meta-size)) (int (alength obj))))}))))))
  (-write-header [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))
  (-write-meta [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))
  (-write-value [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value))))
  (-write-binary [_ _meta-size blob env]
    ;; TODO offer blob stream
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value blob)))))

(defn ^Boolean delete-blob
  "Delete blob. Returns true if deleted, false if not found."
  [client bucket store-path blob-key]
  (try
    (let [blob-id (blob-id bucket store-path blob-key)
          opts (into-array Storage$BlobSourceOption [])]
      (.delete client blob-id opts))
    (catch StorageException e
      ;; Return false if blob not found (404)
      (if (= 404 (.getCode e))
        false
        (throw e)))))

(defn ^Boolean delete-many-blobs
  [client bucket blob-store-paths]
  (let [blob-ids (map (partial blob-id bucket) blob-store-paths)]
    (.delete client #^BlobId (into-array BlobId blob-ids))))

(defn ^Blob blob-exists?
  [client bucket store-path blob-key]
  (let [blob-id (blob-id bucket store-path blob-key)
        opts (into-array Storage$BlobGetOption [])]
    (.get client blob-id opts)))

(defn ^Blob copy-blob
  "Copy blob from one key to another. Returns nil if source doesn't exist."
  [client bucket store-path from-blob-key to-blob-key]
  (try
    (let [from-blob-id (blob-id bucket store-path from-blob-key)
          to-blob-id (blob-id bucket store-path to-blob-key)
          copy-request (Storage$CopyRequest/of ^BlobId from-blob-id ^BlobId to-blob-id)
          copy-writer (.copy client copy-request)]
      (.getResult copy-writer))
    (catch StorageException e
      ;; Return nil if source blob not found (404)
      (when-not (= 404 (.getCode e))
        (throw e)))))

(defn get-bucket
  [client bucket-name]
  (let [opts (into-array Storage$BucketGetOption [])]
    (.get client bucket-name opts)))

(defn ^Bucket create-bucket [client location bucket]
  (let [bucket-info (-> (BucketInfo/newBuilder bucket)
                        (.setLocation location)
                        (.build))
        opts (into-array Storage$BucketTargetOption [])]
    (.create client bucket-info opts)))

(defn list-objects
  [client bucket store-path]
  (let [bucket (.get client bucket (into-array Storage$BucketGetOption []))
        opts [(Storage$BlobListOption/pageSize 100)
              (Storage$BlobListOption/includeFolders true)
              (Storage$BlobListOption/delimiter "/")
              (Storage$BlobListOption/prefix (str store-path "/"))]
        blobs (.list bucket (into-array Storage$BlobListOption opts))]
    (seq (.iterateAll blobs))))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord CloudStorageBucket [client location bucket store-path generation-cache]
  impl/PBackingStore
  (-create-blob [this blob-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (CloudStorageBlob. this client bucket store-path blob-key (atom {}) (atom nil) (atom nil)))))
  (-delete-blob [_ blob-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (delete-blob client bucket store-path blob-key))))
  (-blob-exists? [_ blob-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (blob-exists? client bucket store-path blob-key))))
  (-copy [_ from-key to-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (copy-blob client bucket store-path from-key to-key))))
  (-atomic-move [_ from-key to-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (copy-blob client bucket store-path from-key to-key)
                 (delete-blob client bucket store-path from-key))))
  (-migratable [_ _key _store-key env]
    (if (:sync? env) nil (go-try- nil)))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-create-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not (get-bucket client bucket)
                   (log/info (str "creating bucket " bucket))
                   (create-bucket client location bucket)))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when (get-bucket client bucket)
                   (let [blobs (list-objects client bucket store-path)
                         keys (filter (fn [key]
                                        (and (.startsWith key store-path)
                                             (or (.endsWith key ".ksv")
                                                 (.endsWith key ".ksv.new")
                                                 (.endsWith key ".ksv.backup"))))
                                      (map #(.getName %) blobs))]
                     (doseq [keys (->> keys
                                       (partition *deletion-batch-size* *deletion-batch-size* []))]
                       (delete-many-blobs client bucket keys)))
                   (.close client)))))
  (-keys [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [blobs (list-objects client bucket store-path)
                       keys (map #(.getName %) blobs)]
                   (->> keys
                        (filter (fn [key]
                                  (and (.startsWith key store-path)
                                       (or (.endsWith key ".ksv")
                                           (.endsWith key ".ksv.new")
                                           (.endsWith key ".ksv.backup")))))
               ;; remove store-id prefix
                        (map #(subs % (inc (count store-path))))))))))

(comment
  {:bucket   "konserve-demo"
   ;;:project-id optional
   :store-path "test-store"
   :location "US-EAST1"})

(defn cloud-storage-client
  "Create a GCS client. Supports :host for emulator endpoint (e.g., 'http://localhost:4443')."
  [{:keys [client project-id host]}]
  (or client
      (let [builder (StorageOptions/newBuilder)]
        (when project-id
          (.setProjectId builder project-id))
        (when host
          (.setHost builder host))
        (.getService (.build builder)))))

(defn spec->store-path
  [{:keys [store-path store-id]}]
  (or store-path store-id
      (throw (Exception. "expected store path in store-spec as :store-path or :store-id"))))

(defn connect-store [spec & {:keys [opts config] :as params}]
  (assert (string? (:bucket spec)))
  (assert (string? (:location spec)))
  (let [client (cloud-storage-client spec)
        store-path (spec->store-path spec)
        backing (CloudStorageBucket. client (:location spec) (:bucket spec) store-path (atom {}))
        ;; Merge spec config and params config with defaults
        merged-config (merge {:sync-blob? true
                              :in-place? true
                              :lock-blob? true}
                             (:config spec)  ;; Config from spec
                             config)         ;; Config from params
        store-config (merge {:opts               opts
                             :config             merged-config
                             :default-serializer :FressianSerializer
                             :buffer-size        (* 1024 1024)}
                            (dissoc params :opts :config))]
    (defaults/connect-default-store backing store-config)))

(defn release [store env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try- (.close ^Storage (:client (:backing store))))))

(defn delete-store [spec & {:keys [opts]}]
  (assert (string? (:bucket spec)))
  (assert (string? (:location spec)))
  (assert (string? (or (:store-path spec) (:store-id spec))))
  (let [complete-opts (merge {:sync? true} opts)
        store-path (spec->store-path spec)
        backing (CloudStorageBucket. (cloud-storage-client spec) (:location spec) (:bucket spec) store-path (atom {}))]
    (-delete-store backing complete-opts)))

;; Marker key for store existence
(def ^:private store-marker-key ".konserve-store-metadata")

(defn- marker-blob-id [bucket store-path]
  (blob-id bucket store-path store-marker-key))

(defn store-exists?
  "Check if a konserve store exists at the given spec."
  [spec & {:keys [opts]}]
  (let [client (cloud-storage-client spec)
        store-path (spec->store-path spec)
        bid (marker-blob-id (:bucket spec) store-path)]
    (some? (.get client bid (into-array Storage$BlobGetOption [])))))

(defn- write-store-marker [client bucket store-path]
  (let [bid (marker-blob-id bucket store-path)
        data (.getBytes (pr-str {:created-at (java.time.Instant/now)}) "UTF-8")]
    (write-blob client bid data)))

(defn- delete-store-marker [client bucket store-path]
  (let [bid (marker-blob-id bucket store-path)]
    (.delete client bid (into-array Storage$BlobSourceOption []))))

;; =============================================================================
;; Multimethod Registration for konserve.store dispatch
;; =============================================================================

(defmethod store/-connect-store :gcs
  [{:keys [bucket location] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [spec (dissoc config :backend)
                     exists (store-exists? spec)]
                 (when-not exists
                   (throw (ex-info (str "GCS store does not exist at: " bucket "/" (spec->store-path spec))
                                   {:bucket bucket :config config})))
                 (<?- (connect-store spec :opts opts))))))

(defmethod store/-create-store :gcs
  [{:keys [bucket location] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [spec (dissoc config :backend)
                     client (cloud-storage-client spec)
                     store-path (spec->store-path spec)
                     exists (store-exists? spec)]
                 (when exists
                   (throw (ex-info (str "GCS store already exists at: " bucket "/" store-path)
                                   {:bucket bucket :config config})))
        ;; Ensure bucket exists
                 (when-not (get-bucket client bucket)
                   (log/info (str "Creating bucket " bucket))
                   (create-bucket client location bucket))
        ;; Write store marker
                 (write-store-marker client bucket store-path)
                 (<?- (connect-store spec :opts opts))))))

(defmethod store/-store-exists? :gcs
  [{:keys [bucket] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [spec (dissoc config :backend)]
                 (store-exists? spec)))))

(defmethod store/-delete-store :gcs
  [{:keys [bucket] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [spec (dissoc config :backend)
                     client (cloud-storage-client spec)
                     store-path (spec->store-path spec)]
        ;; Delete store marker
                 (delete-store-marker client bucket store-path)
        ;; Delete all store files
                 (delete-store spec :opts opts)))))

(defmethod store/-release-store :gcs
  [_config store opts]
  ;; Release respecting caller's sync mode
  (release store opts))
