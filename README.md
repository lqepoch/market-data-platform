# LQEpoch Market Data Platform

Offline-first Rust pipeline for validating shared market event envelopes, preserving immutable
event Parquet, producing strict one-minute equity trade bars, and publishing readback-verified
objects through a single-writer archive boundary.

`broker-connectors` owns Alpaca SDK/REST/WebSocket transport, MessagePack decoding, typed
subscription ACKs, feed selection, and connection generations. This repository consumes the
versioned `market-contracts` DTOs; it does not create a second Alpaca client. Today the runnable
CLI accepts shared-contract JSONL and synthetic input. No live SIP/OPRA feed was connected in this
change.

## Status and evidence

- Synthetic replay passes JSONL validation, bounded collection, trade-bar aggregation, Arrow/Parquet
  writes, shared schema fingerprint checks, footer/readback verification, local immutable archive,
  query, and JSONL export.
- Synthetic records are `provider=synthetic`, `feed=synthetic`, `entitlement=unknown`. They are
  diagnostic data and cannot qualify as SIP/OPRA.
- The rclone Google Drive adapter is implemented behind the archive boundary and has fake-transport
  reconciliation tests. The public CLI only exposes `local-test`; real rclone execution, OAuth,
  remote reads/writes, project quota discovery, and Drive upload are **NOT RUN**.
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
`4230418f7fe25f70e3011fed2ba7eb59c7e4d875` in `Cargo.toml` and `Cargo.lock`.

```sh
cargo +1.98.1 run --offline -- synthetic --output /tmp/mdp-demo
cargo +1.98.1 run --offline -- verify \
  --parquet /tmp/mdp-demo/staging/synthetic-2026-10-08-events-v1.parquet \
  --schema market-events-v1
cargo +1.98.1 run --offline -- query-bars \
  --parquet /tmp/mdp-demo/staging/synthetic-2026-10-08-bars-1m-v1.parquet \
  --symbol QQQ --export-jsonl /tmp/mdp-bars.jsonl
```

`synthetic` runs the full local workflow and prints a JSON report. It creates immutable output
files, so use a fresh `--output` directory for another run. `verify` accepts `market-events-v1` or
`us-equity-trade-bar1m-v1`. `query-bars` prints rows as JSONL or writes them to a new export file.

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
  --max-staging-bytes 17179869184 \
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
symbol/minute must contain at least one timestamped trade. The requested
window is contained in the caller-supplied session and repeated on each output row; a partial
window must not be promoted as a whole-session sample. `available_at` must be no earlier than the
window end and no earlier than any event receive timestamp.

## Bounds and storage contract

- Raw event frame: at most 16 KiB. JSONL input: at most 64 MiB and 100,000 records.
- Collection/archive queues: 256 events and 64 pending submissions in the replay path; at most 32
  combined dedicated collection and archive writer threads per process; tracked provider/feed
  cursors: 64; durable gap ledger: 64 MiB.
- Aggregation: at most 500 expected symbols, 390 minutes per request, 100,000 input records, and
  100,000 output rows. The checked `symbols × minutes` bound is applied before bar allocation.
- Archive queue defaults to 4 and cannot exceed 64. Default object, manifest, and total staging
  limits are 8 GiB, 64 KiB, and 16 GiB; the CLI exposes these values for each JSONL replay.
- Each immutable manifest describes one Parquet object. SHA-256 is calculated locally and confirmed
  by downloading and hashing the complete remote object before manifest publication. Drive MD5 is
  advisory only. UNKNOWN outcomes are reconciled from a durable receipt and remote readback; a
  restart never blindly retries a create.
- The shared core descriptor is the only Parquet schema fingerprint authority. Event and bar golden
  fingerprints are checked against the Arrow fields before write and read. Arrow/Parquet 60.0.0 is
  pinned as this public crate's Rust storage stack; it is an independent choice, not a reuse of the
  private QQQ archive's Arrow/Parquet 53.2.0 implementation.

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

Tests use synthetic contracts, local files, and injected fake storage failures. They do not prove
Alpaca entitlement, live feed behavior, rclone authentication, Google Drive connectivity, quota, or
production publication authorization.

## License

This repository's original source is MIT licensed in `LICENSE`. Dependencies retain their respective
licenses; review the generated lockfile and dependency notices before redistribution. The private
`qqq-sip-drive-archive` implementation is not copied into this repository and is not relicensed.
