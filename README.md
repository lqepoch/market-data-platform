# LQEpoch Market Data Platform

Offline-first Rust pipeline for validating shared market event envelopes, preserving immutable
event Parquet, producing strict one-minute equity trade bars, and publishing readback-verified
objects through a single-writer archive boundary.

`broker-connectors` owns Alpaca SDK/REST/WebSocket transport, MessagePack decoding, typed
subscription ACKs, feed selection, and connection generations. This repository consumes the
versioned `market-contracts` DTOs and pins the read-only `broker-ports` / `alpaca-stream` crates;
it does not create a second Alpaca client. The connector crates are pinned for the upcoming collector
integration but are not yet wired to a runnable live-capture command. Today the CLI accepts
shared-contract JSONL and synthetic input. No live SIP/OPRA feed was connected in this change.

## Status and evidence

- Synthetic replay passes JSONL validation, bounded collection, trade-bar aggregation, Arrow/Parquet
  writes, shared schema fingerprint checks, footer/readback verification, local immutable archive,
  query, and JSONL export.
- Synthetic records are `provider=synthetic`, `feed=synthetic`, `entitlement=unknown`. They are
  diagnostic data and cannot qualify as SIP/OPRA.
- The rclone Google Drive adapter is implemented behind the archive boundary and has fake-transport
  reconciliation tests. Replay and publishing use only `local-test`; the read-only service can be
  configured with an operator rclone config. Real rclone execution, OAuth, remote reads/writes,
  project quota discovery, and Drive upload are **NOT RUN**.
- Core raw MessagePack v1 and correlated event v2 Parquet writers are available for offline local
  diagnostics. Their pair publisher is local-test only; synthetic inputs stay `synthetic/synthetic`
  and do not establish Alpaca provenance. The current broker adapter decodes a frame before placing
  its raw bytes and normalized events on the in-memory ordered channel. MDP does not yet fsync those
  bytes before broker normalization, and no runnable provider capture command is connected.
- SIP/OPRA entitlement and source authorization remain **UNVERIFIED** until trusted operator/provider
  evidence is supplied through the approved source-admission path. Readback proves object bytes, not
  provenance, entitlement, feed completeness, or market-data licensing.
- Trade bars exclude quote events from OHLCV and count them separately. They do not provide NBBO,
  quote-state completeness, or a point-in-time research guarantee. Historical EOF retains the real
  collection `available_at`; it does not backdate availability to the source event time.
- Realtime watermark completion is a separate mode and is not represented by the historical/synthetic
  EOF path.

## Local commands

Use Rust 1.98.1 from `rust-toolchain.toml`.
The shared contracts are pinned to trading-core revision
`0a2eaff08d45e8abc1a0137dab17d5d3ef5553c8` in `Cargo.toml` and `Cargo.lock`. The read-only Alpaca
port and stream crates are pinned to `broker-connectors` revision
`70a8f89378be29a700111c3c6759ae31614efeb5`.

```sh
cargo +1.98.1 run --offline -- synthetic --output /tmp/mdp-demo
cargo +1.98.1 run --offline -- verify \
  --parquet /tmp/mdp-demo/staging/synthetic-2026-10-08-four-bars-parquet-v3-events-v1.parquet \
  --schema market-events-v1
cargo +1.98.1 run --offline -- query-bars \
  --parquet /tmp/mdp-demo/staging/synthetic-2026-10-08-four-bars-parquet-v3-bars-1m-v1.parquet \
  --symbol QQQ --export-jsonl /tmp/mdp-bars.jsonl
cargo +1.98.1 run --offline -- cleanup-staging \
  --state-dir /tmp/mdp-demo/state --staging-dir /tmp/mdp-demo/staging
```

`synthetic` runs the full local workflow and prints a JSON report. It creates immutable output
files, so use a fresh `--output` directory for another run. `verify` accepts `market-events-v1`,
`market-events-v2`, `market-raw-frame-v1`, or `us-equity-trade-bar1m-v1`. The v2 event and raw-frame
schemas must match their exact core schema fingerprints and physical fields. `query-bars` prints
rows as JSONL or writes them to a new export file.
`cleanup-staging` is a dry run unless `--apply` is supplied; it only considers MDP-named temporary
files, preserves live-PID/locked/unresolved-receipt candidates, and never removes published objects
or manifests. On platforms without Linux `/proc` process evidence it conservatively preserves all
PID-owned temporaries.

For the cross-repository adapter fixture, use a separate immutable dataset identity:

```sh
cargo +1.98.1 run --offline -- synthetic --full-session \
  --output /tmp/mdp-full-four-minute-session-parquet-v3
cargo +1.98.1 run --offline -- verify \
  --parquet /tmp/mdp-full-four-minute-session-parquet-v3/staging/synthetic-2026-10-08-full-four-minute-session-parquet-v3-bars-1m-v1.parquet \
  --schema us-equity-trade-bar1m-v1
cargo +1.98.1 run --offline -- synthetic --regular-session \
  --output /tmp/mdp-full-390-minute-session-parquet-v2
cargo +1.98.1 run --offline -- verify \
  --parquet /tmp/mdp-full-390-minute-session-parquet-v2/staging/synthetic-2026-10-08-full-390-minute-session-parquet-v2-bars-1m-v1.parquet \
  --schema us-equity-trade-bar1m-v1
cargo +1.98.1 run --offline -- synthetic --regular-session \
  --session-date 2026-10-07 \
  --output /tmp/mdp-full-390-minute-session-parquet-v2-20261007
cargo +1.98.1 run --offline -- verify \
  --parquet /tmp/mdp-full-390-minute-session-parquet-v2-20261007/staging/synthetic-2026-10-07-full-390-minute-session-parquet-v2-bars-1m-v1.parquet \
  --schema us-equity-trade-bar1m-v1
```

This fixture contains four nonempty minutes in a synthetic session whose UTC interval is
`[2026-10-08T13:30:00Z, 2026-10-08T13:34:00Z)`. The session and requested window are identical to
exercise full-window consumers. It is not an exchange-calendar validation, real market session,
provider history, or research evidence; it remains explicitly synthetic and unauthorized.
`--regular-session` generates 390 nonempty synthetic minutes over a separate six-and-a-half-hour
window for training-pipeline plumbing. `--session-date YYYY-MM-DD` is available only with
`--regular-session`, requires a canonical calendar date, and gives the fixture a new date-scoped
dataset identity. The generator uses a fixed synthetic UTC interval of `[13:30, 20:00)` for every
date. It does not consult an exchange calendar, holiday schedule, early close, or historical provider
API; the date and timestamps only support cross-repository serialization tests. The Oct 7 fixture's
`available_at` is `2026-10-07T20:00:00Z`, before the current clock, but synthetic/unknown provenance
still prevents it from serving as causal market evidence, training input, or promotion evidence.

To replay an existing shared-contract JSONL stream, provide a bounded session config and an explicit
local-test store. The JSONL schema is the flattened Rust/Serde projection from `market-contracts`;
uint64 values such as `generation` and `sequence` are canonical decimal strings.

```sh
cargo +1.98.1 run --offline -- replay-jsonl \
  --input ./input.jsonl \
  --session-config ./session.json \
  --output ./run-output \
  --dataset-id sample-2026-10-08 \
  --local-test-store ./local-test-objects \
  --max-object-bytes 8589934592 \
  --max-manifest-bytes 65536 \
  --max-staging-bytes 34359738368 \
  --upload-queue-capacity 4
```

Example session configuration (synthetic fixture only):

```json
{
  "window": {
    "trade_date": "2026-10-08",
    "session_id": "synthetic-regular-2026-10-08",
    "timezone": "America/New_York",
    "policy_id": "synthetic-session-policy-v1",
    "policy_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "session_start": "2026-10-08T13:30:00Z",
    "session_end_exclusive": "2026-10-08T20:00:00Z",
    "window_start": "2026-10-08T13:30:00Z",
    "window_end_exclusive": "2026-10-08T13:32:00Z",
    "expected_symbols": ["QQQ"]
  },
  "mode": "synthetic_eof",
  "source_is_paged": false,
  "source_pages_exhausted": null,
  "available_at": "2026-10-08T13:32:00Z"
}
```

`historical_eof` requires a non-synthetic provider and explicit `source_is_paged` context. Paged
sources must report `source_pages_exhausted: true`; non-paged sources must report `null`. Output
bars preserve that distinction as `historical_eof_paged` or `historical_eof_nonpaged`. Each requested
symbol must have at least one timestamped trade in the window. Minutes with no observed trade,
including quote-only minutes, are omitted rather than represented by zero-valued bars; every emitted
row for that symbol repeats `window_expected_minutes` and `window_empty_trade_minutes`, so its row
count must equal expected minus empty minutes. The Parquet reader enforces that count and rejects
undeclared omissions or inconsistent per-symbol counts. This describes the finite input only: EOF
and page-exhaustion metadata do not prove provider-side market completeness or entitlement. The
requested window is contained in the caller-supplied session and repeated on each output row; a
partial window must not be promoted as a whole-session sample. `available_at` must be no earlier
than the window end and no earlier than any event receive timestamp.

## Bounds and storage contract

- Raw event frame: at most 16 KiB. JSONL input: at most 64 MiB and 100,000 records.
- The separate broker raw-MessagePack frame contract allows at most 1 MiB per frame. An MDP raw
  capture object is capped at 16 MiB of frame bytes and 1,024 frame rows. The raw Parquet manifest
  records one row per application frame, has no source-time range, and counts every frame as missing
  a source timestamp; receive time is not substituted for provider event time. `symbols_json=[]`
  is valid for control/unknown rows only when the whole capture contains a nonempty symbol union.
  Captures with an empty union, sequence gaps, malformed frames, or provider errors do not receive
  a paired capture receipt. Malformed/error frames may remain in bounded staging for operator
  quarantine and recovery. This raw MessagePack schema accepts Alpaca OPRA frames or explicitly
  synthetic frames; it rejects SIP JSON and indicative-feed relabeling, which require their own
  wire-format contract.
- `ArchivePublisher::publish_local_diagnostic_capture_pair` verifies raw frame hashes, canonical
  generation/sequence continuity, event-to-frame hashes, provider/feed/entitlement/receive-time
  identity, symbol membership, and complete per-frame event ordinals before publishing the pair to
  the injected `local-test` transport. It writes a private pair receipt containing both object IDs,
  manifest hashes, content hashes, schemas, and counts. The receipt scope is only
  `local_parquet_pair_verified`; provider completeness is `not_asserted`. Raw-only and event-v2-only
  publication are rejected, and this pair API cannot write to Google Drive. Its caller must supply
  a fresh cryptographically random UUIDv4 for each provider subscription/capture instance, so a
  process restart that reuses a numeric generation does not reuse an immutable dataset identity.
  Durable pair state records `in_flight`/`unknown` and component progress; a half-published pair
  has no pair receipt. Restart reconciles each immutable component before publishing the pair
  receipt, and the same capture UUID cannot be reused with different frame/event bytes. Pair
  correlation decodes inline and accepts only MDP-owned producer files in its configured staging
  directory; arbitrary Parquet must use the CLI worker boundary. A future capture CLI must add
  cross-object correlation to the worker before exposing such an entrypoint.
- MDP implements a Linux-only owner-private `LocalRawFrameSpoolFactory` for the broker's two-stage
  persist-before-decode/finalization ACK contract. It syncs exact source bytes before each ACK,
  poisons ambiguous or cancelled subscriptions, tracks blocking writes through explicit shutdown,
  and preserves prior-process spool directories as unknown without resuming them. The spool is not
  yet wired to a provider capture command or a Parquet writer; the existing pair API still consumes
  producer staging files after decode. No real OPRA capture, Google Drive write, or provider
  watermark was exercised.
- Collection/archive queues: 256 events and 64 pending submissions in the replay path; at most 32
  combined dedicated collection and archive writer threads per process; tracked provider/feed
  cursors: 64; durable gap ledger: 64 MiB.
- Aggregation: at most 500 expected symbols, 390 minutes per request, 100,000 input records, and
  100,000 output rows. The checked `symbols × minutes` bound is applied before bar allocation.
- Archive queue defaults to 4 and cannot exceed 64. Default object, manifest, and total staging
  limits are 8 GiB, 64 KiB, and 32 GiB; the CLI exposes these values for each JSONL replay. Before
  replay, staging reserves peak room for two output objects, one object readback, three manifest
  buffers, and receipt overhead. Parquet writers stop at the configured object-byte cap and remove
  failed temporary output.
- Local predecode spool defaults to 8 GiB per logical subscription, 32 GiB total, 65,536 frames per
  capture identity, and 1,024 identities. It requires owner-only directories/files and never resumes
  a prior-process capture; old spool bytes remain unknown and consume the configured capacity. The
  parent directory must exist before `LocalRawFrameSpoolFactory::open`; the factory creates only its
  final private root directory.
- Parquet writer uses Zstandard level 1, row groups of at most 10,000 rows, and 1 MiB data/dictionary
  page targets. Reader preflight rejects more than 1,024 row groups, a row group over 32 MiB of
  footer-advertised uncompressed column bytes, or more than 512 MiB total; column and cumulative
  sizes use checked conversion/addition. Arrow reads use 256-row batches.
- Apache Parquet 60.0.0 does not expose a maximum uncompressed page-size option on the Arrow reader.
  The footer checks therefore cannot trust a page header by themselves. Verify, query, remote-cache
  verification, and remote-cache cleanup decode in a Linux worker capped at 1 GiB address space,
  60 CPU seconds, 120 seconds wall time, 256 MiB stdout, and two active workers per process. Worker
  failure is rejected before cache receipt creation; subprocess groups are terminated and reaped.
  The CLI uses Tokio's current-thread runtime so short-lived workers do not reserve a per-core thread
  pool inside the address-space cap. The file-level null fixture starts from a production-written,
  valid 390-row synthetic session: the positive file passes CLI `verify` and `query-bars`, then the
  negative copy changes only the first `symbol` to null and rewrites the footer and Arrow schema hint
  to declare that column required. Other decoded columns are compared unchanged, and the Parquet
  decoder plus both CLI paths reject the negative copy before publishing output or a receipt. This
  covers that malformed-null case, not every invalid definition-level encoding.
  Non-Linux platforms return `Unsupported` for isolated decode and do not fall back to inline decode.
  The OS boundary protects the parent from page-header allocation bombs; it is not a per-page size
  validator or a claim that the Parquet reader itself bounds every allocation.
- Low-level `parquet_store` in-process read functions are for MDP-owned producer output and trusted
  local fixtures only. Use the CLI or `remote-query-bars` worker boundary for externally supplied
  Parquet. The `parquet_worker` launcher resolves `current_exe` and is CLI-owned, not a reusable
  library SDK; embedding applications must launch a pinned MDP worker executable or provide an
  equivalent isolated process boundary.
- The optional 100,000-row codec benchmark compares Zstandard 1, Snappy, and uncompressed output
  with full Parquet readback. Run it with the command below and report the measured file sizes,
  elapsed time, rows per second, and peak resident memory. This synthetic benchmark does not measure
  SIP/OPRA production load, rclone transfer, Drive quota, or broker queue capacity.

```sh
CARGO_BUILD_JOBS=2 cargo +1.98.1 test --release --offline --locked \
  --features benchmark-snappy \
  parquet_store::tests::benchmark_100k_synthetic_trade_events -- --ignored --nocapture
```
- One rclone operation has a configurable total wall-clock deadline (`MDP_DRIVE_OPERATION_TIMEOUT_SECS`,
  default 1800 seconds, maximum 7200). This supervisor deadline is independent of rclone's 60-second
  idle timeout; timeout or parent exit kills and reaps the owned process group, including descendants
  holding inherited pipes. Platforms without process-group termination fail closed as unsupported.
  The subprocess supervisor also consumes the process-wide background-worker permit.
- The operator-only `remote-query-bars` CLI is read-only. It fetches exact manifest bytes, observes
  current remote IDs and sizes, streams downloads into private temporary cache files under the
  observed-size cap, then verifies SHA-256, trusted schema fingerprint, footer rows, and decoded
  dataset facts before query/export. Concurrent cache misses share a cross-process budget lock held
  through download and verification. A private mode-0700 local cache receipt stores observed IDs,
  hashes, and timestamps without changing the shared manifest. Defaults are 32 GiB total cache,
  64 entries, 15-minute freshness TTL, 256 MiB decoded query result, and 1 GiB JSONL export.
  Expired cache entries are not evicted during queries. The local `cleanup-remote-cache` command
  reports candidates by default; `--apply` removes only expired entries with the exact MDP cache
  layout, a valid private receipt, and reverified manifest, SHA-256, Parquet schema, footer, and row
  facts. It skips active dataset locks and preserves unknown, malformed, or incomplete directories.
  It needs no rclone configuration and does not contact Drive. Namespace is mandatory: `curated` requires authorized Alpaca
  SIP/OPRA with exact numeric tokens; `diagnostic` preserves its explicit unqualified purpose.
  Drive object folders are separated as `curated-<dataset-id>` and `diagnostic-<dataset-id>`.
  Browsers cannot read rclone configuration or access Drive directly; UI query must use an
  authenticated BFF.

## Read-only HTTP service

`mdp serve` exposes a bounded HTTP facade over the same hash-verified archive cache and isolated
Parquet worker used by `remote-query-bars`. It emits the existing `lqepoch.us_equity_trade_bar_1m.v1`
rows and an HTTP projection of `RemoteQuerySummary` in the JSON envelope
`{ "summary": ..., "rows": [...] }`; `row_count` and `returned_rows` are canonical decimal strings
on HTTP, while CLI JSON keeps its existing numeric encoding. It does not reinterpret V1 rows as
DatasetManifestV2 completion evidence. See [`docs/openapi-v1.yaml`](docs/openapi-v1.yaml).
The only data route is `GET /v1/datasets/{dataset_id}/bars?namespace=diagnostic|curated&symbol=QQQ`.
Responses are capped at 4 MiB and 390 rows. An outer router middleware applies
`Cache-Control: no-store` to every response, including authentication failures, extractor failures,
unmatched routes, and query overloads; existing cache directives are preserved. Each process allows
two active query tasks with a bounded queue and 120-second deadline. Dropping a request,
timing out, or shutting down cancels owned work and waits for child workers to be reaped before
capacity is released. On Unix, startup registers SIGINT and SIGTERM before binding; both signals
use the same graceful shutdown path, which joins the query supervisor before returning. Signal
registration errors abort startup, and signal-handler errors still stop the listener and join owned
work. Other platforms retain Tokio's portable Ctrl-C handling.

The listener defaults to `127.0.0.1:8088`. `GET /healthz` is liveness only. `/readyz` reports
identity/transport configuration but always returns `market_ready: false` and
`source_entitlement: unverified`; it is not evidence that data is available or entitled. With no
MDP key pair, loopback liveness remains available while readiness and data requests return 503.
Non-loopback bind is rejected unless both independent MDP keys are configured.

```sh
MDP_TERMINAL_JWT_SECRET='<terminal-only MDP signing key, at least 32 bytes>' \
MDP_RESEARCH_JWT_SECRET='<research-only MDP signing key, at least 32 bytes>' \
mdp serve --local-test-root /secure/local-test-objects --cache-dir /secure/mdp-cache
```

Tokens use HS256, audience `lqepoch-market-data`, exactly one scope `market:read`, and at most 60
seconds lifetime. `kid=mdp-terminal` is bound to issuer `eqoboard-openterminal` and the terminal MDP
key; `kid=mdp-research` is bound to issuer `openterminal-research` and the research MDP key. The
two MDP keys must be different and must not reuse Gateway keys. Each trusted BFF signer receives
only its matching MDP key; the service verifier receives both. This service only verifies tokens;
the separate EqoBoard BFF signing helper and browser integration are outside this slice. The
browser must call through its trusted BFF and must never read rclone configuration or private cache
files directly.

The LocalTest transport serves only `diagnostic`; the response must retain
`synthetic/synthetic/unknown` provenance or the request fails closed. It cannot silently fall back to
another transport and does not expose
capture, upload, account, or order routes. The checked-in Dockerfile is a runtime-only LocalTest
smoke wrapper: it requires an explicit immutable `MDP_RUNTIME_BASE`, contains no Rust builder, and
does not include rclone. A Drive-enabled deployment needs its own reviewed immutable runtime base
that includes a pinned rclone executable. `scripts/container_smoke.sh` uses a cached image ID,
`--pull=false`, an offline Docker network, an exact copied-binary SHA-256 check, synthetic replay,
authorization denial, an authenticated V1 query, and real SIGINT/SIGTERM shutdown. The SIGTERM case
starts concurrent authenticated synthetic queries, observes an active Parquet worker, and confirms
the service joins its query supervisor before exiting. The smoke makes no provider, OAuth, Drive, or
broker calls; it reports host/runtime architecture and glibc versions. It verifies only that a
host-built executable runs in the selected cached runtime image, not a container source build or
production deployment. Production images must use a reviewed immutable registry digest.
- Each immutable manifest describes one Parquet object. SHA-256 is calculated locally and confirmed
  by downloading and hashing the complete remote object before manifest publication. Drive MD5 is
  advisory only. UNKNOWN outcomes are reconciled from a durable receipt and remote readback; a
  restart never blindly retries a create.
- The shared core descriptor is the only Parquet schema fingerprint authority. Event and bar golden
  fingerprints are checked against the Arrow fields before write and read. Arrow/Parquet 60.0.0 is
  pinned as this public crate's Rust storage stack; it is an independent choice, not a reuse of the
  private QQQ archive's Arrow/Parquet 53.2.0 implementation.
- New Parquet writers include the core canonical schema descriptor and fingerprint in both Arrow
  schema metadata and footer key/value metadata. Readers accept legacy files with no such metadata,
  allow unrelated metadata when the pair is absent, and reject a partial or mismatching pair.

The optional 100,000-event synthetic codec benchmark was run on this host with full write and
readback. Zstandard level 1 produced 1,557,077 bytes at about 152,000–162,000 rows/second; Snappy
produced 3,261,541 bytes at about 169,000–172,000 rows/second; uncompressed output was 4,577,055
bytes at about 180,000–186,000 rows/second. The Zstandard test process peaked at about 113 MiB RSS.
This small deterministic fixture favors a roughly 2.1x reduction versus Snappy for a modest observed
throughput tradeoff; it does not estimate real OPRA/SIP file sizes, production memory, network speed,
Drive quotas, or daily shard sizing.

Google Drive quotas are project- and age-dependent; the configured Cloud project is currently
**UNKNOWN**. Do not turn an example API quota into an operational budget. Confirm the real project,
Workspace storage headroom, rclone remote, authorized root, and operator-approved capacity snapshot
before enabling external writes. References: [Drive API limits](https://developers.google.com/workspace/drive/api/guides/limits),
[rclone Google Drive backend](https://rclone.org/drive/), [rclone `lsjson`](https://rclone.org/commands/rclone_lsjson/).

## Validation

```sh
cargo +1.98.1 fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --offline --all-targets -- -D warnings
CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline
```

Dependency policy, the generated CycloneDX SBOM, license inventory, exact git source pins, and
source hashes are documented in [`docs/SUPPLY_CHAIN.md`](docs/SUPPLY_CHAIN.md). After changing
dependencies, sources, or implementation files, refresh and check those artifacts with the
commands in that document.

Tests use synthetic contracts, local files, and injected fake storage failures. They do not prove
Alpaca entitlement, live feed behavior, rclone authentication, Google Drive connectivity, quota, or
production publication authorization.

For an explicitly operator-invoked remote query, configure `MDP_DRIVE_REMOTE`,
`MDP_DRIVE_ROOT_FOLDER_ID`, and `MDP_DRIVE_DATASET_PREFIX`, then run:

```sh
mdp remote-query-bars --namespace diagnostic --dataset-id DATASET_ID \
  --rclone-config /secure/operator/rclone.conf --cache-dir /secure/mdp-cache \
  --export-jsonl ./bars.jsonl
```

The command does not print the remote name, folder ID, or config path. Do not run it in public CI or
expose its rclone configuration to a browser process.

For local cache maintenance, first inspect the report and then explicitly apply it if the listed
entries are expected:

```sh
mdp cleanup-remote-cache --cache-dir /secure/mdp-cache
mdp cleanup-remote-cache --cache-dir /secure/mdp-cache --apply
```

The cleaner scans at most the configured cache-entry bound, shares the per-dataset and global cache
budget locks with readers, and leaves staging, active, unknown, and unverified files untouched.

## License

This repository's original source is MIT licensed in `LICENSE`. Dependencies retain their respective
licenses; review the generated lockfile and dependency notices before redistribution. The private
`qqq-sip-drive-archive` implementation is not copied into this repository and is not relicensed.
