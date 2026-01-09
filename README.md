# konserve-gcs

A backend for [konserve](https://github.com/replikativ/konserve) that supports Google [Cloud Storage](https://cloud.google.com/storage).


## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/konserve-gcs/latest-version.svg)](http://clojars.org/io.replikativ/konserve-gcs)

### Configuration

``` clojure
(require '[konserve-gcs.core]  ;; Registers the :gcs backend
         '[konserve.core :as k])

(def config
  {:backend :gcs
   :bucket "my-bucket"
   :location "US-EAST1"
   :store-path "my-store"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"
   ;; Optional:
   :project-id "my-gcp-project"})

(def store (k/create-store config {:sync? true}))
```

For API usage (assoc-in, get-in, delete-store, etc.), see the [konserve documentation](https://github.com/replikativ/konserve).

### Multiple Stores in Same Bucket

GCS supports multiple independent stores within the same bucket by using different `:store-path` values:

``` clojure
;; Store 1
(def store1-config
  {:backend :gcs
   :bucket "my-bucket"
   :location "US-EAST1"
   :store-path "store1"
   :id #uuid "11111111-1111-1111-1111-111111111111"})

;; Store 2 - same bucket, different path
(def store2-config
  {:backend :gcs
   :bucket "my-bucket"
   :location "US-EAST1"
   :store-path "store2"
   :id #uuid "22222222-2222-2222-2222-222222222222"})

(def store1 (k/create-store store1-config {:sync? true}))
(def store2 (k/create-store store2-config {:sync? true}))

;; Each store maintains its own isolated namespace within the bucket
```

### Optimistic Locking for Distributed Updates

konserve-gcs supports optimistic concurrency control using GCS's generation-based conditional writes. This enables safe concurrent updates from multiple machines without distributed locks.

``` clojure
;; Enable optimistic locking with up to 10 retries on conflict
(def config
  {:backend :gcs
   :bucket "my-bucket"
   :location "US-EAST1"
   :store-path "my-store"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"
   :config {:optimistic-locking-retries 10}})

(def store (k/create-store config {:sync? true}))

;; Now update-in is safe across multiple machines!
;; Each machine can run this concurrently:
(k/update-in store [:counter] (fnil inc 0) {:sync? true})
```

**How it works:**
1. When reading a key, konserve-gcs captures the object's generation number
2. When writing, it uses GCS's `generationMatch` precondition with the captured generation
3. If another process modified the object, GCS returns HTTP 412 (Precondition Failed)
4. konserve automatically retries: re-reads the new value, re-applies your update function, and writes again
5. This continues until the write succeeds or max retries is exceeded

This is particularly useful for:
- Counters and metrics aggregation across distributed workers
- Shared configuration that multiple services update
- Any read-modify-write pattern in distributed systems

**Note:** Without optimistic locking enabled, concurrent `update-in` calls from different machines may lose updates (last-write-wins). With optimistic locking, all updates are preserved through automatic retry.

### Notes

Note that you do not need full GCS rights if you manage the bucket outside, i.e.
create it before and delete it after usage from a privileged account. Connection
will otherwise create a bucket and all files created by konserve (with suffix
".ksv", ".ksv.new" or ".ksv.backup") will be deleted by `delete-store`, but the
bucket needs to be separately deleted.

## Authentication

GCS authentication is handled by the Google Cloud SDK. The recommended approaches are:

1. **Application Default Credentials** - Set up with `gcloud auth application-default login`
2. **Service Account** - Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account JSON key file
3. **GCE/GKE** - Automatically uses the instance's service account when running on Google Cloud

See [Google Cloud Authentication documentation](https://cloud.google.com/docs/authentication) for more details.

## Local Development with Emulator

For local development and testing, you can use [fake-gcs-server](https://github.com/fsouza/fake-gcs-server):

```bash
# Start the emulator
docker-compose up -d

# Run tests
./bin/run-unittests

# Or manually connect to the emulator
```

``` clojure
(def emulator-config
  {:backend :gcs
   :bucket "test-bucket"
   :location "US-EAST1"
   :store-path "test-store"
   :project-id "test-project"
   :host "http://localhost:4443"  ;; Emulator endpoint
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"})
```

## Commercial support

We are happy to provide commercial support with
[lambdaforge](https://lambdaforge.io). If you are interested in a particular
feature, please let us know.

## License

Copyright © 2023-2026 Christian Weilbach

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
