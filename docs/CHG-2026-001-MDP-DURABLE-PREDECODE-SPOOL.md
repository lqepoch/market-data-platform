# CHG-2026-001: durable predecode spool

This slice implements the MDP-owned Linux adapter for the broker's frozen two-stage raw-frame
capture port. A successful predecode acknowledgement follows a synchronized append of the exact
frame bytes and source-local identity. A second acknowledgement follows a synchronized append of
the matching decode/finalization summary. The spool does not connect to a provider or Drive and does
not establish entitlement, completeness, or research admission.

The spool uses private owner-only directories and single-link files, an exclusive process lock,
fixed capture and process budgets, and a tracked blocking-worker supervisor. A verified root
directory descriptor anchors lock and capture operations; lock, capture-directory, frame-log, and
directory-sync opens reject symlinks. The lock also rejects hardlinks. Cancellation or ambiguous
I/O poisons the logical sink; capacity remains owned until blocking writes have exited. Existing
capture directories are preserved as unknown and never resumed after process restart. The API is
not wired to a live provider socket or external upload path. The later optional
`offline-capture-synthetic` feature composes the factory with the broker-owned fixed fake-wire
fixture and the LocalTest-only Parquet Pair publisher; it does not accept caller-supplied fixture
bytes or establish provider authority.

The implementation consumes the two-stage sink contract from broker-connectors commit
`537550417d2fbe74343115fa2cc9c158660412f8`. That commit adds the raw capture port and has a small
dependency edge to the already-pinned core `domain` package; no provider REST client or vendor SDK
is added. The corresponding source pin and `Cargo.lock` entries are updated in this change.

The initial spool implementation commit was validated with Rust 1.98.1 and two build jobs on the
shared MDP target:

```sh
CARGO_TARGET_DIR=/root/github/LQEpoch-Platform/worktrees/market-data-platform-chg-2026-001/target \
  CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline -p market-data-platform
```

That baseline run passed 111 unit tests, 5 CLI integration tests, and doc tests. The subsequent
lock-path hardening passed strict formatting, all-target Clippy with warnings denied, and the five
`local_spool_rejects_*` tests, including symlink and hardlink lock targets whose bytes and mtime
remain unchanged. The full post-hardening test suite and supply-chain gates are run for the frozen
head. No validation used provider credentials, called a broker, read a real market payload, or
accessed Google Drive.
