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

Collection uses a bounded Tokio channel feeding one dedicated writer thread. Archive publication
uses a bounded Tokio queue and one dedicated writer thread per queue. Both draw from the same
process-wide 32-thread permit budget. The collection thread serializes
per-provider/feed generation and sequence decisions with queue insertion. Gaps, stale generations,
duplicates, and queue drops are synced to a bounded journal before the event is reported incomplete.
Journal exhaustion and I/O failures fail the run; the pipeline does not call the gap evidence durable
when a write fails. The process-wide permit budget caps both writer classes together at 32; journal
append holds a file lock while enforcing its byte cap and syncing the record.

The current bar transform requires caller-supplied trade date, session ID, timezone, session policy
ID/hash, exact session bounds, exact requested half-open window, and expected symbols. A complete
window contains one or more source-timestamped trades for every expected symbol/minute. Trades are
ordered by source time then sequence, exact decimal values drive OHLCV, source range is per-bar
`[min_source_time, max_source_time + 1ns)`, and actual `available_at` remains collection completion
time. Quotes are excluded from OHLCV and counted separately; no NBBO or quote-state completeness is
claimed. Missing trade minutes, missing source timestamps, invalid provenance, gaps, unsupported
options, or incomplete EOF prevent bar output.

Historical replay configuration must explicitly say whether the provider endpoint is paged. Paged
history requires `source_pages_exhausted=true` and writes `completion_mode=historical_eof_paged`;
non-paged sources require null exhaustion and write `historical_eof_nonpaged`. Ambiguous legacy
`historical_eof` rows are rejected.

This EOF transform does not imply realtime watermark completion or historical point-in-time
availability. Those claims require explicit source-page exhaustion, clock/watermark evidence,
allowed-lateness policy, and corresponding recovery tests.

## Parquet and manifest

Event and minute-bar Parquet schemas and fingerprints are imported from core's trusted schema
registry. The MDP maps logical field types to Arrow and compares exact field order, names, types, and
nullability. It does not hash Arrow's display or serialization format. Parquet footer row count must
match decoded rows. Readers and exporters stay within file/row caps.
Parquet output writers enforce the object-byte limit while writing and use a cleanup guard for
failed temporary files. Replay preflights staging against peak object/readback/manifest/receipt
reserve, rather than only checking the directory after output has already been written.

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
output, one rclone retry, and `--immutable`. A separate total wall-clock deadline kills and reaps the
child process group; descendants that keep inherited output pipes open are terminated when the
parent exits. rclone's `--timeout` remains an idle timeout. The supervisor uses the same
process-wide worker permit. Platforms without process-group termination fail closed as unsupported.
It does not use the Google Drive SDK or implement OAuth.
Subprocess stderr and private config identifiers are never logged. Current public CLI paths only
select local-test storage for writes; the rclone adapter has not been run against a real account.
The explicit operator-only `remote-query-bars` command uses the same argv-only rclone adapter for
read-only access. It reads the existing shared manifest bytes without adding self-hash fields,
observes the current manifest/object IDs, then verifies downloaded object SHA-256, size, registered
schema, footer, and decoded facts before query/export. Its private mode-0700 cache receipt stores
observed IDs/hashes/times; a cache hit requires an unexpired TTL and unchanged remote identity
metadata, and rechecks local hashes and row semantics. Curated and diagnostic datasets use
separate remote folders. Misses hold a cross-process cache-budget lock through bounded streaming
downloads and verification; each download is capped at the observed remote size and inherits the
rclone total operation deadline. Queries never evict expired entries. The local
`cleanup-remote-cache` command defaults to report-only; `--apply` removes an expired entry only when
the private receipt, exact cache layout, manifest bytes, SHA-256, trusted Parquet schema/footer, and
decoded row facts all verify. It holds the per-dataset lock and global cache-budget lock, skips active
datasets, and preserves unknown, malformed, incomplete, or unverified directories. This command
does not load rclone configuration or contact Drive. Browser access remains behind an authenticated
BFF.

`cleanup-staging` defaults to report-only. Applying it considers only recognized MDP temp filename
patterns whose owner PID is no longer live, whose per-dataset publication lock can be acquired, and
whose receipt is absent or whose committed receipt matches a validated local manifest. Mismatched,
malformed, or unresolved receipts are preserved. Published Parquet, manifests, receipts, lock files,
and unrecognized files are not yet cleanup targets. The staging and state directories must be trusted
operator-owned paths; metadata-check/open races are outside the threat model. Non-Linux systems
without equivalent process evidence preserve all PID-owned temps.

## Source admission and publication

Local-test manifests carry a `local-test:` identity and are diagnostic only. Curated Drive
publication has an explicit provider=`alpaca`, feed=`sip|opra`, authorized-entitlement, and exact-token
numeric-encoding gate. These data fields are assertions consumed by the archive boundary, not
cryptographic proof that an operator/provider actually granted entitlement. Production use remains
blocked until the trusted source-admission path supplies separately verified entitlement, feed,
capture-completeness, and licensing evidence. Readback never upgrades an UNKNOWN or unauthorized
source. OPRA binary-float projections are eligible only for the Drive `diagnostic` namespace and
only after explicit authorized-entitlement evidence; they are not source-exact and cannot be read as
curated research data. Byte-exact raw OPRA frames require the separate bounded sidecar contract and
remain unavailable until that broker adapter lands.

The repository contains no Alpaca secret and no live feed/upload test. Drive project/root identity and
available quota remain UNKNOWN; use an operator-provided authorized root and quota snapshot before
any external write is enabled.
