# Architecture and evidence boundaries

## Ownership

- `broker-connectors`: sole provider SDK, Alpaca REST/WebSocket transport, MessagePack decoder,
  timestamp extensions, subscription ACK state, reconnect generation, and feed-specific limits.
- `market-data-platform`: bounded shared-envelope ingestion, sequence/gap accounting, event
  persistence, caller-configured session windows, complete trade-only one-minute aggregation,
  Parquet query/export, and immutable dataset publication.
- `trading-core/market-contracts`: event/control DTOs, exact decimal and timestamp types, manifest,
  uint64 JSON encoding, and canonical Parquet schema descriptors/fingerprints.
- Quant research consumes the canonical minute-bar dataset through its own admission/materializer
  adapter. Event Parquet is not a Qlib DatasetH input.

The MDP accepts `MarketEventEnvelopeV1` / control envelopes. It does not infer provider entitlement or
confirm a WebSocket subscription. `send()` completion is not ACK evidence; only the broker adapter's
typed ACK is eligible for downstream confirmed state.

## Ingestion and transform

JSONL input is read one line at a time. Each raw line is checked against the shared 16 KiB frame cap
before typed Serde deserialization, and the total stream is capped at 64 MiB/100,000 records. Direct
typed decoding rejects duplicate fields; the envelope selector rejects unknown keys, unknown kinds,
and a payload that mixes event and control forms.

Collection uses a bounded Tokio channel feeding one dedicated writer thread. The thread serializes
per-provider/feed generation and sequence decisions with queue insertion. Gaps, stale generations,
duplicates, and queue drops are synced to a bounded journal before the event is reported incomplete.
Journal exhaustion and I/O failures fail the run; the pipeline does not call the gap evidence durable
when a write fails. The process admits at most 32 such writer threads; journal append holds a file
lock while enforcing its byte cap and syncing the record.

The current bar transform requires caller-supplied trade date, session ID, timezone, session policy
ID/hash, exact session bounds, exact requested half-open window, and expected symbols. A complete
window contains one or more source-timestamped trades for every expected symbol/minute. Trades are
ordered by source time then sequence, exact decimal values drive OHLCV, source range is per-bar
`[min_source_time, max_source_time + 1ns)`, and actual `available_at` remains collection completion
time. Quotes are excluded from OHLCV and counted separately; no NBBO or quote-state completeness is
claimed. Missing trade minutes, missing source timestamps, invalid provenance, gaps, unsupported
options, or incomplete EOF prevent bar output.

This EOF transform does not imply realtime watermark completion or historical point-in-time
availability. Those claims require explicit source-page exhaustion, clock/watermark evidence,
allowed-lateness policy, and corresponding recovery tests.

## Parquet and manifest

Event and minute-bar Parquet schemas and fingerprints are imported from core's trusted schema
registry. The MDP maps logical field types to Arrow and compares exact field order, names, types, and
nullability. It does not hash Arrow's display or serialization format. Parquet footer row count must
match decoded rows. Readers and exporters stay within file/row caps.

One immutable `DatasetManifestV1` describes one Parquet object. It records source, sorted unique
symbols, half-open source-time range, missing-source-time count, row count, canonical Parquet schema
fingerprint, object name/identity/size/SHA-256, footer rows, and completion evidence. A manifest is
published only after full object readback and local SHA-256 equality. The manifest itself is then
uploaded immutably and read back before the local receipt enters `Committed`.

The receipt is a durable state machine (`ObjectInFlight`/`ObjectUnknown`/`ObjectVerified`/
`ManifestInFlight`/`ManifestUnknown`/`Committed`). A process restart after an ambiguous create must
lookup and verify the exact immutable identity; absence or failed lookup remains UNKNOWN and does not
repeat the create. A conflicting name/content pair fails closed. Cross-process publication is
serialized by a per-dataset filesystem lock.

The Drive adapter calls rclone with an argv array, explicit config and root ID, bounded subprocess
output, one rclone retry, and `--immutable`. It does not use the Google Drive SDK or implement OAuth.
Subprocess stderr and private config identifiers are never logged. Current public CLI paths only
select local-test storage; the rclone adapter has not been run against a real account.

## Source admission and publication

Local-test manifests carry a `local-test:` identity and are diagnostic only. Curated Drive
publication has an explicit provider=`alpaca`, feed=`sip|opra`, authorized-entitlement, and exact-token
numeric-encoding gate. These data fields are assertions consumed by the archive boundary, not
cryptographic proof that an operator/provider actually granted entitlement. Production use remains
blocked until the trusted source-admission path supplies separately verified entitlement, feed,
capture-completeness, and licensing evidence. Readback never upgrades an UNKNOWN or unauthorized
source.

The repository contains no Alpaca secret and no live feed/upload test. Drive project/root identity and
available quota remain UNKNOWN; use an operator-provided authorized root and quota snapshot before
any external write is enabled.
