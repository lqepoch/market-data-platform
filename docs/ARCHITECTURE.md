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

Event v1, event v2, raw MessagePack frame v1, and minute-bar Parquet schemas and fingerprints are
imported from core's trusted schema registry. The MDP maps logical field types to Arrow and compares
exact field order, names, types, and nullability. It does not hash Arrow's display or serialization
format. Parquet footer row count must match decoded rows. Readers and exporters stay within
file/row caps.
New writers place the descriptor and fingerprint under the core-owned metadata key names in both
Arrow schema metadata and flat footer key/value metadata. Readers allow a legacy V1 file with no
registry keys, allow unrelated metadata when both registry keys are absent, and require both exact
registered values when either key is present. Partial or mismatching pairs fail closed.
Parquet output writers enforce the object-byte limit while writing and use a cleanup guard for
failed temporary files. Replay preflights staging against peak object/readback/manifest/receipt
reserve, rather than only checking the directory after output has already been written.

The writer uses Zstandard level 1, at most 10,000 rows per row group, and 1 MiB data/dictionary
page targets. Readers preflight footer metadata before building an Arrow reader: at most 1,024 row
groups, 32 MiB advertised uncompressed bytes per row group, 512 MiB total, checked per-column sums,
and 256-row Arrow batches. The pinned Apache Parquet 60.0.0 Arrow API has no configurable maximum
uncompressed page-size limit. Footer totals therefore do not by themselves constrain hostile page
headers. Public verify/query paths, remote-cache verification, and cache cleanup use a separate
Linux worker with 1 GiB address-space, 60 CPU-second, and 120-second wall-clock limits; no more than
two workers run per process and IPC stdout is capped at 256 MiB. The existing process-group
supervisor kills and reaps failed/timed-out workers. A decode error cannot create a verified cache
receipt or return rows. Other operating systems fail closed with `Unsupported` instead of decoding
inline. This is process containment, not a per-page validator or an absolute guarantee against
every allocation pattern within the capped worker. The CLI uses Tokio's current-thread runtime so
short-lived workers do not reserve a per-core thread pool inside the address-space cap. The file-level
fixture starts from a production-written, valid 390-row synthetic session whose CLI `verify` and
`query-bars` both pass. Its negative copy changes only the first `symbol` to null, then rewrites the
footer and Arrow schema hint to declare that column required. The test compares all other decoded
columns with the positive file, asserts that the Parquet decoder rejects the required-field null,
and confirms that CLI `verify` and `query-bars` publish no report, export, manifest, or receipt. The
page-data prefix is unchanged during the footer rewrite and the malformed file still matches the
trusted logical schema and fingerprint. This covers null definition levels for a required field,
not every malformed Parquet encoding.

The low-level `parquet_store` in-process reader functions are limited to MDP-owned output and
trusted local fixtures. The `parquet_worker` launcher resolves `current_exe` and is CLI-owned, not a
general SDK for embedding applications. External consumers must launch a pinned MDP worker
executable or provide an equivalent isolated process boundary; the low-level reader is not an
untrusted-file boundary.

The core raw-frame v1 schema stores one exact MessagePack application frame per row, with provider,
feed, entitlement, projection encoding when known, generation/frame sequence, receive timestamp,
SHA-256, expected normalized-event count, disposition, and sorted symbol JSON. The payload itself
is binary and never emitted in `Debug` output. Each frame is capped at 1 MiB; an object is capped at
16 MiB and 1,024 frame rows. Raw-frame manifests have no source-time range and count every frame as
missing a source timestamp. Local receive time is not substituted for provider event time. An empty
symbol list is valid for control/unknown rows only when the complete object has a nonempty symbol
union. The MDP adapter accepts `alpaca/opra` or explicit `synthetic/synthetic` identity for this
MessagePack schema; it rejects SIP JSON and indicative-feed relabeling rather than treating distinct
wire formats as MessagePack.

Event v2 appends an all-or-none raw reference tuple: generation, frame sequence, one-based event
ordinal, and expected event count. Before local pair publication, MDP verifies frame SHA-256 values,
event/frame provider-feed-entitlement-receive-time identity, projection encoding where present,
symbol membership, duplicate/missing ordinals, per-frame expected counts, and contiguous frame
sequence within one generation. The pair publisher accepts only the injected local-test transport;
raw-only and event-v2-only publication are rejected. Empty symbol unions, frame sequence gaps,
malformed/provider-error dispositions, or failed event correlation do not produce a pair receipt.
Control and unknown-message frames can be retained alongside market frames without inventing event
rows. Each pair uses a caller-created fresh UUIDv4 capture identity, distinct from the adapter's
process-local numeric generation. The private receipt binds that capture identity and both local
object IDs, content hashes, Parquet schema fingerprints, manifest hashes, generation/sequence range,
and row counts. Its scope is `local_parquet_pair_verified`, and provider
completeness is `not_asserted`; the receipt verifies the finite Parquet pair, not provider stream
drain or EOF. It is not an entitlement, source-authorization, historical-completeness, or
research-admission certificate.

A separate durable pair-state file records `in_flight`, `raw_committed`, `event_committed`,
`unknown`, or `committed` around component publication. If an object create has an ambiguous
outcome, no pair receipt is written; restart reconciles each component through its existing
immutable publication receipt and exact remote readback, then writes the pair receipt only after
both components verify. Capture IDs are bound to the complete input intent, so reuse with changed
frame bytes, sequence, or event rows fails with a conflict before another object is published.

Pair correlation currently decodes inline and is restricted to MDP-owned files in the configured
producer staging directory. The stable frame key is the pair's UUIDv4 capture identity plus source
generation and frame sequence; reuse of that capture identity with different input bytes conflicts
before publication. Do not route operator-supplied or remote Parquet through this API. A future CLI
capture-pair command must add pair correlation to the isolated worker protocol first; the ordinary
CLI `verify` worker currently verifies each object separately and does not claim cross-object
correlation.

This storage step does not guarantee raw persistence before normalization. The current pinned broker
adapter decodes/analyzes an inbound frame, then places the raw-frame record and normalized events on
an ordered bounded in-memory channel. MDP can publish the raw object before the event object after it
consumes that channel, but the raw bytes have already passed through the decoder. A durable
broker-side raw sink and acknowledgement before decoding are still required for a persist-before-
normalize guarantee. No provider socket or real Drive is connected by this local API.

The optional release benchmark writes and fully reads back 100,000 synthetic trade events using
Zstandard level 1, Snappy, and no compression. It is used to compare this schema and configuration;
it does not establish live SIP/OPRA throughput, broker capacity, Drive transfer speed, or quotas.
On the recorded host, the files were 1,557,077 bytes (Zstandard), 3,261,541 bytes (Snappy), and
4,577,055 bytes (uncompressed); observed write-plus-readback throughput was approximately 152k–162k,
169k–172k, and 180k–186k rows/second respectively. The Zstandard run peaked near 113 MiB RSS.
These figures describe only the deterministic 100,000-row synthetic fixture and do not determine
production shard sizes or capacity.

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

The HTTP service is a read-only facade over this same query path. Its only data route is
`GET /v1/datasets/{dataset_id}/bars`, with required `namespace` and `symbol` query parameters. The
JSON response is `{summary: ..., rows: TradeMinuteBarV1[]}` and uses the existing V1 bar
contract. The HTTP summary preserves `RemoteQuerySummary` fields but projects `row_count` and
`returned_rows` as canonical decimal strings; CLI JSON keeps numeric values. It does not attach or
imply core DatasetManifestV2 completion evidence. The
hand-maintained OpenAPI description is `docs/openapi-v1.yaml`, and schema goldens remain the type
authority. LocalTest service instances accept only `diagnostic` and preserve
`synthetic/synthetic/unknown` provenance. There is no route for capture, upload, accounts, or orders.

The HTTP verifier requires an independent JWT key pair and audience. It accepts HS256 only, requires
exactly `scope=["market:read"]`, an `lqepoch-market-data` audience, a lifetime no longer than 60
seconds, and a fixed issuer/key mapping: `mdp-terminal` with `eqoboard-openterminal`, or
`mdp-research` with `openterminal-research`. Terminal and research MDP secrets differ from one
another and from Gateway keys. The corresponding trusted BFF signer receives only its one MDP key;
MDP receives both. This repository slice verifies tokens but does not issue them or modify the Eqo
BFF. Authorization runs before request-level storage reads, index lookup, or Parquet query. The
default listener is loopback; any non-loopback bind requires both independent keys. `/healthz` is
liveness only. `/readyz` confirms configured identity/transport and always reports
`market_ready=false` / `source_entitlement=unverified`.

The offline container smoke copies a host-built binary into an already cached runtime image by
exact SHA-256, disables image pulls and networking, and reports host/runtime architecture and glibc
versions. It exercises SIGINT and SIGTERM against the running process; the SIGTERM case observes an
active isolated Parquet worker during authenticated synthetic query load and confirms the service
joins its query supervisor before exiting. Successful startup and HTTP probes establish
compatibility only with that local runtime image; this is not a container source build or production
deployment validation. Production images must use a separately reviewed immutable registry digest.

HTTP queries run through a service-owned supervisor, not Tokio's async executor. At most two
blocking query jobs are active and two more can wait. The 120-second request deadline and dropped
request cancel the shared token; the supervisor keeps job capacity until all owned children are
killed and reaped. Shutdown cancels and joins active jobs before returning. On Unix the service
registers SIGINT and SIGTERM before binding and routes both to the same shutdown path; registration
failure aborts startup. Other platforms retain Tokio's portable Ctrl-C handler. Any handler error
stops the listener and is reported after supervisor cleanup. Cache publication has a cleanup guard
so a canceled query cannot leave a successful receipt. Linux process-group supervision remains
mandatory; unsupported platforms fail closed instead of decoding inline.

The root Dockerfile is runtime-only and requires a caller-supplied immutable base image reference.
It does not compile Rust, fetch packages, or include rclone; the checked-in offline container smoke
uses a cached base image ID and a host-built binary whose SHA-256 is checked after copying. This is a
LocalTest startup/auth/query smoke, not a Drive-enabled deployment image or a production build
attestation. A production runtime must supply a reviewed base with pinned rclone.

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
curated research data. Byte-exact raw MessagePack and event-v2 pair storage now use the additive core
schemas, but the MDP pair publisher is local-test only and no provider capture command is connected.
The broker adapter currently decodes before the MDP receives raw bytes, so persist-before-normalize
remains unavailable. No raw data is uploaded to Google Drive, and a successful hash/readback never
promotes unknown or unauthorized provenance.

The repository contains no Alpaca secret and no live feed/upload test. Drive project/root identity and
available quota remain UNKNOWN; use an operator-provided authorized root and quota snapshot before
any external write is enabled.

## Supply-chain records

`deny.toml` is the dependency policy for the locked all-target graph. The committed CycloneDX SBOM
covers all-target, all-feature normal and build dependencies; dev-only packages remain in the lock
inventory and are still checked by cargo-deny. Exact git origins and revisions are checked against
`supply-chain/git-source-pins.json`. See `docs/SUPPLY_CHAIN.md` for generation, verification, license,
and source-hash boundaries.
