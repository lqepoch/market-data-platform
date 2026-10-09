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
- Public PR checks must not use stored repository, organization, provider, or rclone credentials.
  Ordinary verification may use only the ephemeral read-only GitHub token needed to check out source;
  CodeQL may also use its narrowly scoped token to upload SARIF. Never connect to provider feeds,
  invoke real OAuth or rclone configuration, query or publish remote archives, or upload data/test
  artifacts. Pin third-party Actions to full commit SHAs and grant each workflow only its required
  token permissions. Reuse `docs/CI.md` commands instead of duplicating validation logic.
- Remote query is an operator-only, read-only CLI path and an authenticated HTTP path. Require an
  explicit curated/diagnostic namespace, verify remote IDs and exact manifest bytes, then verify
  downloaded SHA-256, size, trusted schema fingerprint, footer, and row facts before serving or
  exporting. Store observations only in the private bounded cache receipt; do not add self-hash
  fields to the shared core manifest. A cache hit still rechecks current remote IDs/size/MD5 metadata
  and local SHA/schema/facts. HTTP emits the existing V1 bar DTO and query summary; its u64 summary
  counts are canonical decimal strings while CLI JSON retains numbers. HTTP must not imply
  DatasetManifestV2 completion evidence. LocalTest HTTP is diagnostic-only and explicitly
  synthetic/unknown. Keep browser reads behind a trusted BFF; never expose cache or rclone config.
- The optional `offline-capture-synthetic` feature may replay only the broker-owned fixed
  `AlpacaOpraTradeV1` MessagePack fixture through its existing runner. Preserve `alpaca/opra` and
  unknown entitlement. Mark Pair manifests `LocalArchive` because they describe exact local finite
  input/readback; include `synthetic-offline-fixture` and the reviewed fixture ID in `input_identity`
  and dataset IDs. The private MDP receipt and CLI report must retain `SYNTHETIC_REPLAY_FIXTURE`,
  `NOT_ASSERTED`, and explicit not-real-OPRA/not-live labeling. `LocalArchive` does not establish
  provider authority, entitlement, Drive durability, or completeness. The fixed freshness clock is
  only a fixture freshness cutoff; received timestamps remain actual runner timestamps. The local
  `FixtureEnd` control marker is never provider or watermark evidence; ordinary EOF, timeout, or
  cancellation cannot complete a capture. Never add arbitrary fixture bytes, provider credentials,
  or a live-capture path to this feature.
- CLI `verify-capture-pair-v2` accepts one explicit `chunk-<sha256>.receipt.json` basename and
  only the existing LocalTest archive/state. Its crate-private reader reuses the Core raw/event
  validators, artifact readback, and bounded Linux Parquet worker. The current-executable worker
  wrapper is CLI-owned, not a general embedded-library reader. Do not scan receipt directories,
  return raw rows, or imply that one verified chunk is a complete capture. Keep entitlement
  `unknown` and source completeness `NOT_ASSERTED`. The verifier is unsupported outside Linux;
  preserve existing `LocalTestTransport::new` behavior on other platforms. Temporary Parquet
  copies use a fresh private `0700` run directory and create-only `0600` files under the caller's
  existing staging root; never create or chmod that root. Serialize readers sharing that root with
  its persistent owner-only `.pair-readback-budget.lock`, acquired before the staging-budget scan
  and held through private run-directory cleanup; lock contention fails closed.
- `archive::LocalRawFrameSpoolFactory` implements the broker's two-stage raw-frame sink on Linux.
  Persist exact payload bytes and the full source-local identity before predecode ACK, then persist
  the matching finalization summary before final ACK. Cancellation, ambiguous I/O, sequence gaps,
  and shutdown poison the logical sink; old process directories remain unknown and are never resumed.
  Keep owner-only `0700` directories and single-link `0600` files and the fixed
  capture/total/frame/identity limits. Open the root, lock, capture directories, and frame logs
  relative to verified directory descriptors with no-follow flags; reject lock symlinks and
  hardlinks before using them. After cancelling and joining subscription tasks, call factory
  `shutdown()`; it closes
  admission and waits for all tracked blocking file operations. This local WAL is not a provider
  connection, Parquet pair receipt, Drive upload, entitlement verifier, or completeness proof.
- The HTTP service accepts only short-lived HS256 `market:read` delegations for the independent
  `lqepoch-market-data` audience. `mdp-terminal` is bound to
  `eqoboard-openterminal`/`MDP_TERMINAL_JWT_SECRET`; `mdp-research` is bound to
  `openterminal-research`/`MDP_RESEARCH_JWT_SECRET`. The keys must differ from each other and all
  Gateway keys. Each BFF signer holds only its matching MDP key; the MDP verifier holds both. Do
  not trust identity headers or `EQO_ACCESS_TOKEN`. Authorize before request-level storage/query
  access. LocalTest profile defaults to loopback; non-loopback requires both independent keys.
- Apply `Cache-Control: no-store` at the outer HTTP router layer to every response, including auth,
  extractor, unmatched-route, and overload responses. Preserve existing cache directives when adding
  `no-store`.
- The HTTP supervisor owns at most two blocking query jobs, bounds its waiting queue and 120-second
  request deadline, and retains capacity until cancellation has killed and reaped any child worker.
  Request drop/timeout and service shutdown must cancel, join, and reap; never detach a worker or
  publish a successful cache receipt after cancellation. Isolated Parquet decoding remains Linux
  only and must fail `Unsupported` elsewhere without inline fallback. Unix service startup registers
  SIGINT and SIGTERM before binding, and either signal follows this same graceful shutdown path;
  registration failure aborts startup. Non-Unix platforms retain Tokio's portable Ctrl-C signal,
  and any signal handler error still stops the listener and joins the supervisor.
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
- `scripts/container_smoke.sh` is an offline LocalTest-only container smoke. It requires an already
  cached immutable Linux runtime image ID in `MDP_RUNTIME_IMAGE_ID`, copies the host-built binary
  after an exact SHA-256 comparison, and must never pull images or build Rust inside Docker. This
  smoke verifies actual SIGINT and SIGTERM exits, including SIGTERM while an authenticated synthetic
  query has an observed Parquet worker and the service confirms supervisor join. It does not validate
  rclone, Drive, or production BFF deployment.
- Synchronize README, `docs/ARCHITECTURE.md`, and `docs/openapi-v1.yaml` whenever the API, CLI,
  limits, provenance, archive
  behavior, or validation boundary changes.
- Supply-chain changes must keep `deny.toml`, `supply-chain/git-source-pins.json`, the committed
  CycloneDX SBOM, license inventory, and `SOURCE-MANIFEST.json` synchronized. Run
  `cargo +1.98.1 deny --all-features check all`, `python3 -m unittest discover -s tests -p
  'test_supply_chain.py'`, and `python3 scripts/supply_chain.py --check`; unknown licenses,
  advisories, sources, or git revisions fail closed.
