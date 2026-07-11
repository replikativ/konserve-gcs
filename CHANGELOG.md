# Changelog

All notable, user-visible changes to konserve-gcs are documented here.

## Unreleased

### Added
- **Read-miss-safe reads (one object fetch, no metadata probe).** The GCS backing
  implements konserve's `PReadMissSafe` and its read path throws
  `store-key-not-found-ex` on a genuine 404. On a konserve that supports the marker
  the redundant `-blob-exists?` metadata `.get` is dropped, so a read is a single
  object fetch, and read-modify-write ops (`update-in` / `assoc-in` / `bassoc`) skip
  it too. Requires konserve `0.9.354`+.

### Changed
- konserve `0.9.342` → `0.9.354`. Declares `com.taoensso/timbre` explicitly —
  konserve `0.9.353`+ no longer pulls it transitively (it moved to
  `org.replikativ/logging`).
- CI now runs the emulator compliance suite (sync + async) against a
  `fake-gcs-server` service container, plus a smoke load-check that catches a stale
  konserve pin; the release is gated on both.
