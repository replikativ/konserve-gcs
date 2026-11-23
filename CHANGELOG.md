# Changelog

All notable, user-visible changes to konserve-gcs are documented here.

## Unreleased

### Changed
- **Storage clients are shared, and a store no longer closes its client.** Every
  `store-exists?`, `create-store`, `connect-store` and `delete-store` call built
  its own `Storage` client; `connect-store` through `konserve.store` built two and
  closed neither until release. `cloud-storage-client` now returns one shared
  client per `:project-id` and `:host`, as konserve-s3 does per endpoint.
  `release` and `delete-store` used to close the store's client, including one
  the caller had passed in under `:client`, which then failed under every other
  store using it (a sibling store, or a reconnect). They no longer close anything.
  Call `konserve-gcs.core/shutdown-clients!` on process exit to close the shared
  clients; a client you pass in is yours to close.

### Added
- **Read-miss-safe reads (one object fetch, no metadata probe).** The GCS backing
  implements konserve's `PReadMissSafe` and its read path throws
  `store-key-not-found-ex` on a genuine 404. On a konserve that supports the marker
  the redundant `-blob-exists?` metadata `.get` is dropped, so a read is a single
  object fetch, and read-modify-write ops (`update-in` / `assoc-in` / `bassoc`) skip
  it too. Requires konserve `0.9.354`+.

### Changed
- konserve `0.9.342` → `0.9.354`.
- **Logging: switched from `taoensso.timbre` to `org.replikativ/logging`** — the
  unified logging library used across the konserve backends. Log events are now
  keyword-tagged (e.g. `:konserve.gcs/creating-bucket`).
- CI now runs the emulator compliance suite (sync + async) against a
  `fake-gcs-server` service container, plus a smoke load-check that catches a stale
  konserve pin; the release is gated on both.
