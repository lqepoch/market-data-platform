# CHG-2026-001: durable predecode spool

This slice implements the MDP-owned Linux adapter for the broker's frozen two-stage raw-frame
capture port. A successful predecode acknowledgement follows a synchronized append of the exact
frame bytes and source-local identity. A second acknowledgement follows a synchronized append of
the matching decode/finalization summary. The spool does not connect to a provider or Drive and does
not establish entitlement, completeness, or research admission.

The spool uses private owner-only directories and files, an exclusive process lock, fixed capture
and process budgets, and a tracked blocking-worker supervisor. Cancellation or ambiguous I/O poisons
the logical sink; capacity remains owned until blocking writes have exited. Existing capture
directories are preserved as unknown and never resumed after process restart. The API is not wired
to a provider socket, CLI capture command, Parquet writer, or upload path.

The implementation consumes the two-stage sink contract from broker-connectors commit
`537550417d2fbe74343115fa2cc9c158660412f8`. That commit adds the raw capture port and has a small
dependency edge to the already-pinned core `domain` package; no provider REST client or vendor SDK
is added. The corresponding source pin and `Cargo.lock` entries are updated in this change.

Local validation used Rust 1.98.1 with two build jobs and the shared MDP target:

```sh
CARGO_TARGET_DIR=/root/github/LQEpoch-Platform/worktrees/market-data-platform-chg-2026-001/target \
  CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline -p market-data-platform
```

The final run passed 111 unit tests, 5 CLI integration tests, and doc tests. It did not use provider
credentials, call a broker, read a real market payload, or access Google Drive. The shared target
grew by approximately 1.08 GiB during dependency compilation; no further Cargo build was started in
this slice after that resource threshold was reached.
