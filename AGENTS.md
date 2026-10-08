# Market Data Platform Agent Rules

This crate owns bounded collection orchestration, event normalization, completed-window transforms,
Parquet persistence, immutable archive publication, query, and local replay. It does not own
provider SDKs, REST/WebSocket clients, wire decoding, subscription acknowledgements, or credentials;
those belong to `broker-connectors`.

- Use `market-contracts` as the only owner of event, manifest, uint64 JSON, and schema fingerprint
  contracts. Do not duplicate DTOs, canonical serializers, or schema hashes here.
- Treat provider/feed, entitlement, numeric encoding, source time, receive time, generation, and
  sequence as evidence. Never infer SIP/OPRA entitlement, convert receive time into source time, or
  label binary-float projections as exact source decimals.
- Keep ingestion, aggregation, Parquet reads, query results, queues, gap journals, receipts, and
  uploads within explicit byte, row, worker, and capacity bounds. The process-wide 32-thread budget
  is shared by collection and archive writers. Fail closed on overflow, gaps,
  incomplete windows, conflicting immutable objects, and unknown publication outcomes.
- External archive writes require separately reviewed source-admission evidence. Local-test objects
  are diagnostic only and must keep `local-test:` identities. Never run real OAuth or upload from
  tests or public CI.
- Use argument arrays for subprocesses. Do not log rclone arguments, config paths, remote names,
  root IDs, tokens, or raw provider/subprocess errors.
- Every rclone operation must have a total wall-clock deadline separate from its idle timeout,
  kill and reap on timeout, and consume the shared background-worker permit.
- Run `cargo +1.98.1 fmt --all -- --check`, `CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --offline
  --all-targets -- -D warnings`, and `CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline` for Rust
  changes. Keep shared-schema golden tests and the synthetic CLI replay passing.
- Synchronize README and `docs/ARCHITECTURE.md` whenever the CLI, limits, provenance, archive
  behavior, or validation boundary changes.
