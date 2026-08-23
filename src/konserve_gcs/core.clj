(ns konserve-gcs.core
  (:require [konserve.impl.defaults :as defaults :refer [absent]]
            [konserve.protocols :as protocols]
            [konserve.impl.storage-layout :as impl
             :refer [PBackingLock PReadMissSafe store-key-not-found-ex -delete-store]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [konserve.store :as store]
            [superv.async :refer [go-try- <?-]]
            [replikativ.logging :as log])
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
  "Read blob and return {:data :generation}, ::not-found if the blob is genuinely
   absent (`.get` returns nil), or nil if it was deleted DURING the read (404 —
   stale generation, i.e. a concurrent-modification conflict). The two absent
   cases are distinguished so PReadMissSafe can turn a genuine miss into
   store-key-not-found while optimistic locking still sees the conflict."
  [^Storage client ^BlobId blob-id]
  ;; RE-READ on a stale generation rather than reporting anything.
  ;;
  ;; `.getContent` is pinned to the generation `.get` returned, so a concurrent
  ;; OVERWRITE — not only a delete — makes it 404 once the old generation is gone.
  ;; Neither answer available at that point is true: calling it a miss makes a key
  ;; that plainly exists read as absent, and calling it a conflict fails an
  ;; ordinary read merely because another writer was active. Both were tried and
  ;; the emulator's concurrency test rejected each in turn.
  ;;
  ;; What the caller wants is the pair (bytes, generation) taken from ONE state,
  ;; and after an overwrite there simply is a newer one to read. So read again.
  ;; Bounded, because a key under constant rewriting should surface as an error
  ;; rather than spin.
  (loop [attempt 0]
    (let [result (try
                   (let [opts (into-array Storage$BlobGetOption [])
                         ^Blob blob (.get client blob-id opts)]
                     (if blob
                       {:data (.getContent blob (into-array Blob$BlobSourceOption []))
                        :generation (.getGeneration blob)}
                       ::not-found))
                   (catch StorageException e
                     ;; 404 here = the generation we resolved is already gone
                     (if (= 404 (.getCode e))
                       ::stale
                       (throw e))))]
      (cond
        (not= ::stale result) result
        (< attempt 10) (recur (inc attempt))
        :else (throw (ex-info (str "Could not read a consistent generation for this object: it was "
                                   "overwritten on every attempt.")
                              {:type :konserve/read-generation-unstable
                               :blob (str blob-id)}))))))

(defn write-blob-conditional
  "Write `bytes` only if the object's generation is still `expected-generation`, or
   — when that is `:absent` — only if there is no object. True on success, false
   when GCS refuses.

   GCS evaluates the precondition itself, which is what makes this backing's
   guarantee `:global`: the comparison and the write are one step against every
   writer anywhere, not merely those sharing a filesystem or a heap.

   Create-if-absent is `ifGenerationMatch(0)`. Zero is GCS's way of saying THE
   OBJECT MUST NOT EXIST, and using it keeps both halves of the contract on one
   mechanism rather than adding a separate existence probe that another writer
   could slip past.

   412 is the refusal in both cases."
  [^Storage client ^BlobId blob-id ^bytes bytes expected-generation]
  (try
    (let [blob-info (.build (BlobInfo/newBuilder blob-id))
          opts (into-array Storage$BlobTargetOption
                           [(Storage$BlobTargetOption/generationMatch
                             (if (= :absent expected-generation) 0 (long expected-generation)))])]
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
              ;; The generation read for this key. `-sync` runs on a DIFFERENT blob
              ;; record than the read did — `update-blob` creates its own — so it
              ;; comes from the bucket-wide cache that `-read-header` fills.
                       current-generation (when-let [cache (:generation-cache bucket-store)]
                                            (get @cache blob-key))
                       expected-revision (:expected-revision env)]
                   (if (and header meta value)
                     (do
                       (.write baos #^bytes header)
                       (.write baos #^bytes meta)
                       (.write baos #^bytes value)
                       (let [bytes (.toByteArray baos)
                             bid (blob-id bucket store-path blob-key)]
                         (if expected-revision
                  ;; FENCED. konserve has already compared the revision it read
                  ;; against the caller's; the generation precondition closes the
                  ;; window BETWEEN that read and this write, which is the half no
                  ;; counter can do. Both together are the compare-and-set.
                  ;;
                  ;; A create-if-absent has no generation to match, and says so.
                           (let [precondition (if (= absent expected-revision)
                                                :absent
                                                current-generation)]
                             (when-not precondition
                    ;; No generation means no read happened, so there is nothing
                    ;; to fence against. REFUSE rather than write unconditionally:
                    ;; the previous code did the latter whenever the cache was
                    ;; cold or `:optimistic-locking-retries` sat at its default,
                    ;; silently withholding the guarantee that was asked for.
                               (throw (ex-info (str "Cannot honour :expected-revision: no generation was read "
                                                    "for this key, so the write cannot be made conditional.")
                                               {:type :konserve/conditional-write-unsupported
                                                :key  blob-key})))
                             (when-not (write-blob-conditional client bid bytes precondition)
                               (throw (ex-info (str "Conditional write rejected: the stored generation is not "
                                                    "the one this value was derived from.")
                                               {:type     :konserve/revision-mismatch
                                                :key      blob-key
                                                :expected expected-revision}))))
                  ;; Unfenced: an ordinary write, exactly as before.
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
                   (let [bid (blob-id bucket store-path blob-key)
                         ;; ALWAYS read the generation. It arrives on the response
                         ;; we already have — `.getGeneration` costs no extra round
                         ;; trip — so gating it on a config knob only left the fence
                         ;; with no token to match against, which is how the old
                         ;; path came to write unconditionally whenever the knob sat
                         ;; at its default.
                         response (read-blob-with-generation client bid)]
                     (cond
                       ;; PReadMissSafe: an absent key throws store-key-not-found-ex,
                       ;; which io-operation converts to the caller's :not-found.
                       (= response ::not-found)
                       (throw (store-key-not-found-ex blob-key))
            ;; Deleted between the `.get` and the `.getContent`. Reported as a
            ;; MISS, which is what an unfenced read got before the generation was
            ;; always fetched — turning it into an error here made an ordinary
            ;; read fail merely because another writer was active, which the
            ;; emulator's own concurrency test caught immediately.
            ;;
            ;; Safe for a fenced write too: konserve then sees the key as absent,
            ;; so a caller holding a real revision is correctly rejected, and a
            ;; create-if-absent falls to `ifGenerationMatch=0`, which GCS
            ;; evaluates against whatever is actually there.
                       ;; `read-blob-with-generation` no longer yields nil: a stale
                       ;; generation is retried there, since the only honest answer
                       ;; is the newer state.
                       (nil? response)
                       (throw (store-key-not-found-ex blob-key))
                       :else
                       (do
                         (reset! fetched-object (:data response))
            ;; Store generation in bucket's cache for later use
                         (when (:generation response)
                           (reset! generation (:generation response))
                           (when-let [cache (:generation-cache bucket-store)]
                             (swap! cache assoc blob-key (:generation response))))))))
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
  ;; GCS evaluates the precondition — `ifGenerationMatch`, checked by GCS itself —
  ;; so konserve adds no mechanism of its own: no sidecar blob, no lock it would
  ;; take. Declared rather than inferred from the domain, since how far a
  ;; guarantee reaches and who evaluates it are separate questions.
  protocols/PSelfConditionalWrite

  protocols/PConditionalWrite
  ;; `:global`. The comparison happens in GCS, so it holds against every writer
  ;; anywhere, not merely those sharing a filesystem or a heap.
  (-conditional-write-domain [_] :global)

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
                   (log/info :konserve.gcs/creating-bucket (str "creating bucket " bucket))
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

;; GCS reads are read-miss-safe: -create-blob only constructs a CloudStorageBlob
;; (no side effect / no materialization), and -read-header throws
;; store-key-not-found-ex on a genuine 404. So io-operation skips the -blob-exists?
;; probe — a read is one object fetch instead of a metadata .get + readAllBytes,
;; and update-in/assoc-in/bassoc drop their probe too.
(extend-type CloudStorageBucket
  PReadMissSafe)

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
        ;; Normalised BEFORE our own serializer default is filled.
        ;;
        ;; This backend already forwarded both `:config` and
        ;; `:default-serializer` correctly -- unlike its siblings, which
        ;; dropped or hardcoded one or the other -- so the only thing wrong
        ;; was emitting the OLD spelling as our default, which trips konserve
        ;; 0.9.369's deprecation warning on every connect, for every caller,
        ;; whatever they passed. A warning nobody can act on is noise and
        ;; drowns the ones they can.
        ;;
        ;; Order is the trap: filling the Fressian default first would let it
        ;; occupy the slot and silently drop a caller's older
        ;; `:default-serializer :BoringSerializer`.
        store-config (-> (dissoc params :opts :config)
                         (assoc :config merged-config)
                         defaults/normalize-store-config
                         (update-in [:config :encoding]
                                    #(merge {:serializer :FressianSerializer} %))
                         (update :buffer-size #(or % (* 1024 1024)))
                         (assoc :opts opts))]
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
                   (log/info :konserve.gcs/creating-bucket (str "Creating bucket " bucket))
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
