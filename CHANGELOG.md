# Changelog

All notable, user-visible changes to konserve-gcs are documented here.

## Unreleased

### Fixed
- **A transient Google 5xx no longer fails a write for good.** Every overwrite
  first copies the old object to `<key>.backup` with a server-side rewrite. The
  Google client library retries only operations that it can prove are safe to
  repeat, and a rewrite with no generation precondition is not one of them, so a
  single `503 We encountered an internal error` failed the write after one
  attempt (`Unretryable error (attempts: 1, maxAttempts: 6)`). In a Datahike
  server that failure shut the writer down for good. The client now uses the
  uniform retry strategy, so these calls retry within Google's retry budget.
  Copying one object to the same backup key twice is harmless.
- **Connecting to a store works with current konserve again.** The GCS backing
  did not implement `-store-exists?`, which konserve now probes on every connect
  (konserve#179). Creating or opening any store failed with
  `AbstractMethodError: CloudStorageBucket._store_exists_QMARK_ is abstract`,
  and Datahike's standalone server, which bundles `konserve-gcs` with konserve
  `0.9.394`, could not create a `:gcs` database. `-store-exists?` now looks for
  the store's marker object, the same one `store-exists?` uses.

### Added
- **Read-miss-safe reads (one object fetch, no metadata probe).** The GCS backing
  implements konserve's `PReadMissSafe` and its read path throws
  `store-key-not-found-ex` on a genuine 404. On a konserve that supports the marker
  the redundant `-blob-exists?` metadata `.get` is dropped, so a read is a single
  object fetch, and read-modify-write ops (`update-in` / `assoc-in` / `bassoc`) skip
  it too. Requires konserve `0.9.354`+.

### Changed
- konserve `0.9.342` → `0.9.354`.
- konserve `0.9.376` → `0.9.394`.
- **Logging: switched from `taoensso.timbre` to `org.replikativ/logging`** — the
  unified logging library used across the konserve backends. Log events are now
  keyword-tagged (e.g. `:konserve.gcs/creating-bucket`).
- CI now runs the emulator compliance suite (sync + async) against a
  `fake-gcs-server` service container, plus a smoke load-check that catches a stale
  konserve pin; the release is gated on both.
