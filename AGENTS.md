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
- Remote query is an operator-only, read-only CLI path. Require an explicit curated/diagnostic
  namespace, verify remote IDs and exact manifest bytes, then verify downloaded SHA-256, size,
  trusted schema fingerprint, footer, and row facts before serving or exporting. Store observations
  only in the private bounded cache receipt; do not add self-hash fields to the shared core
  manifest. A cache hit still rechecks current remote IDs/size/MD5 metadata and local SHA/schema/facts.
  Serialize all cache misses with one cross-process budget lock, hold it through downloads and
  verification, and enforce the observed-size cap while streaming bytes to disk. Expired entries
  must not be treated as fresh or automatically evicted during queries. The local cleanup command
  defaults to report-only and may delete only expired entries whose receipt, exact layout, SHA-256,
  schema, footer, and row facts reverify while holding both dataset and global budget locks. Active,
  unknown, malformed, or incomplete entries must be preserved.
  Never expose rclone configuration to a browser; future UI reads must use an authenticated BFF.
- Use argument arrays for subprocesses. Do not log rclone arguments, config paths, remote names,
  root IDs, tokens, or raw provider/subprocess errors.
- Every rclone operation must have a total wall-clock deadline separate from its idle timeout,
  kill and reap its owned process group on timeout or parent exit (including descendants holding
  inherited pipes), and consume the shared background-worker permit. Fail closed on platforms where
  process-group termination is unsupported.
- Run `cargo +1.98.1 fmt --all -- --check`, `CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --offline
  --all-targets -- -D warnings`, and `CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline` for Rust
  changes. Keep shared-schema golden tests and the synthetic CLI replay passing.
- Synchronize README and `docs/ARCHITECTURE.md` whenever the CLI, limits, provenance, archive
  behavior, or validation boundary changes.
- Supply-chain changes must keep `deny.toml`, `supply-chain/git-source-pins.json`, the committed
  CycloneDX SBOM, license inventory, and `SOURCE-MANIFEST.json` synchronized. Run
  `cargo +1.98.1 deny --all-features check all`, `python3 -m unittest discover -s tests -p
  'test_supply_chain.py'`, and `python3 scripts/supply_chain.py --check`; unknown licenses,
  advisories, sources, or git revisions fail closed.
