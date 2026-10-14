# MDP Cache Reconciliation V1

This policy describes the crate-private local cache reconciliation module in
`src/archive/reconciliation`. The HTTP route and provider fetch logic are owned elsewhere. The
module does not write to the existing QQQ Drive or Stock S3OSS archives, contact providers, or
create an entitlement/completeness certificate.

## Identity and coverage

`CacheKeyV1` is derived before a provider fetch from the request and a stable
`CacheQuerySourceScopeV1`. It fingerprints the selected actual provider and feed (`unknown` remains
`unknown`), stable non-secret route identity, SHA-256 of the stable entitlement realm, entitlement
state, expected numeric encoding, asset class, requested and effective resolution, provider mode,
domain-separated rights-profile SHA-256, adjustment, symbol, session scope and calendar-derived
session instances, half-open UTC request window, optional calendar version, request-cursor digest,
and Core-owned schema ID/version/fingerprint. It contains no credential or raw page cursor. `auto`
requests for distinct actual providers remain distinct keys.
Response hashes, row hashes, observation times and random provenance IDs are version evidence, never
lookup dimensions. One cache snapshot represents one actual provider/feed; mixed-source responses
are rejected.

Each coverage receipt retains the adapter's response-observed/receive timestamp. Each segment binds
the SDK/provider observation SHA-256, canonical-row SHA-256, lineage-receipt hash, optional
source-sequence range, and row counts. `source_observation_sha256`
identifies SDK-normalized observations or Gateway observation bytes; it is not an HTTP wire-body
hash and must not include credentials. If the adapter has no observation-byte hash, the cache
computes a deterministic digest of the raw Core rows. Historical bars do not require tick/stream
sequence numbers. Unknown sequences do not block a complete history result when the requested time
range, page count, page exhaustion, and Core gap facts are known. Missing page count or exhaustion,
a known gap, or truncation prevents a complete cache claim. Such rows can still be stored
provisionally. If page facts do not support completeness, the cache downgrades a Core `Complete`
response and its complete segments to `Partial` before publication. The pre-fetch key retains only
the request cursor digest. A truncated response may additionally persist the A02-validated typed
provider-local continuation in its private immutable receipt, bound to the exact key/provider/feed,
entitlement realm, payload hash, and original expiry. The MDP outer cursor, arbitrary caller input,
delegations, and credentials are never persisted or logged.

The caller derives the pre-fetch key, then supplies the SDK-observation `BarsResponseV1`, the
adapter's canonical `BarsResponseV1`, and `ProviderPageEvidenceV1` to
`CacheVersionStoreV1::publish_bars_responses`. The raw observation response is Core-validated and
persisted unchanged, including provider-local session IDs and rows later filtered by the trusted
calendar mapper. The canonical response is validated against the request and key window; its
trusted sessions, segments, gaps, and completeness facts alone define cache coverage. Each
canonical row must link to a raw observation by exact instrument, half-open timestamps, provenance,
and Core `ExactDecimal`-normalized OHLCV; the provider-local session ID is ignored for that link,
and filtered raw rows remain in the raw Parquet object without entering canonical coverage. The
cache validates both row/evidence envelopes and repeats the lineage/count checks after bounded
Parquet readback. The compact exact-decimal `CanonicalRowV1` view is generated internally for
reconciliation. The module does not reconstruct row-level provider identity from a response summary
or accept caller-supplied artifact paths.

A cache hit is shaped with
`read.bars_evidence.to_cached_response(read.bars_rows, BarsCacheStatusV1::Hit, continuation_cursor)`.
The read verifies the full version receipt, row Parquet, coverage, and lineage before returning. A
truncated page can be replayed only when the provider adapter can rewrap the next-group selection or
the private receipt contains its validated provider-local SDK cursor. Only that typed provider-page
locator may be retained; the MDP outer cursor, caller headers, Gateway delegation, and credentials
are never stored. The receipt is `0600` inside the cache's `0700` tree, and binds the cursor hash,
provider kind, actual provider/feed, entitlement realm, and exact cache-key fingerprint. Its expiry
is bounded to the provider cursor's original lifetime (at most 30 minutes); the adapter checks
expiry and issues a fresh outer cursor for the current `as_of` without extending the locator's
lifetime. Cursor receipts are omitted from `Debug` output. A truncated page without a usable
provider receipt or next-group continuation is not returned as a cache hit. Map `NotFound` to a
cache miss. A truncated page with a missing or expired cursor receipt returns `Expired` and must be
treated as a miss so the provider page is fetched again; never advance to the next query group from
that page. Treat `Unsupported`, `UnknownOutcome`, `Readback`, and `Conflict` as unavailable/
fail-closed cache state, not as authorization or coverage proof. A caller may fetch fresh data after
a cache error only if its independent provider route and entitlement checks pass.

## Authority and reconciliation

Production publication currently has no trusted provider-admission proof and therefore remains
`Provisional`, including Alpaca SIP/BOATS. `verify_provider_admission_v1` accepts only a signed,
query-bound receipt verified by an injected `TrustedProviderAdmissionVerifierV1`, together with
actual authorization-context and SDK/Gateway runtime observations. The default
`UnavailableProviderAdmissionVerifierV1` fails closed. Missing actual feed, entitlement, runtime
facts, or an independently reviewed issuer/key set keeps admission `BlockedExternal`; request feed,
environment variables, dependency lockfiles, and top-level summaries cannot substitute for source
provenance. Capability receipts are cursor-agnostic; dataset receipts bind the exact page cursor and
the raw/canonical observations, rows, coverage, and reconciliation-policy hashes. Test-only
synthetic evidence exercises authoritative rollback and conflict paths but is not provider or
production evidence. OPRA is never a historical stock-bars authority candidate. Cache status does
not establish adapter or user entitlement.

When an eligible same-feed candidate is compared with an already verified authoritative current
version, rows are normalized with Core `ExactDecimal`, deduplicated by stable identity, and compared
field by field. Conflicting duplicate identities fail. Decimal changes are accepted only within
explicit per-field tolerances; non-decimal changes conflict. The default tolerance is exact equality,
permits no new rows, and permits no removed rows. A same-feed conflict writes a compact
credential-free rejection audit and leaves the verified current pointer untouched. Operators or the
owning adapter can surface the `CacheInvalidationV1` returned after a successful replacement,
rollback, or recovery.

Cross-source reconciliation uses a separate bounded logical index partitioned by the verified
rights-profile hash, instrument, requested/effective resolution, adjustment, calendar version, and
trusted schema. It deliberately omits provider/feed, entitlement realm, request window, page cursor,
and session ID. The partition maps actual `session_instance_id` values to immutable version
references. A month used for physical storage is derived from the earliest validated calendar phase
slice for that session; the opaque session ID is never parsed to infer a date. Missing or invalid
phase mapping leaves the raw/canonical artifacts available as provisional versions but does not
advance the logical index. For every overlapping session, the cache compares only the exact shared
half-open interval covered by contiguous `Complete` canonical segments and rows. It does not clip
bars, infer session identity from provider-local IDs, compare after-hours rows as regular-session
data, or treat a partial/truncated page as complete. Disjoint or incomplete overlap does not advance
that session's logical pointer.

The cache finishes every session comparison before publishing any pointer. One material conflict
rejects the entire candidate update, including a first authoritative Alpaca candidate that conflicts
with a complete Futu provisional baseline. The raw/canonical candidate and receipt remain as an
immutable `conflict_rejected` version for review, a credential-free rejection audit is written, and
neither the provider current pointer nor any session in the prior logical manifest advances. A
conflict-rejected version is visible in `list_verified_versions` for audit but cannot be selected by
automatic load, rollback, or recovery. Minor decimal differences are accepted only under the
explicit persisted same-feed or cross-provider tolerance policy. A successful multi-session update
replaces one checksummed bounded logical root atomically. The root is capped at 1500 shard references
and 4 MiB; each immutable shard is capped at 64 sessions and 256 KiB; referenced shard bytes are
capped at 32 MiB and total indexed sessions at 96,000. A candidate is capped at 2048 sessions for
one reconciliation pass. Shards are content-addressed, create-only, and read back by hash before the
single root pointer changes. Capacity exhaustion keeps the new immutable raw/canonical version while
leaving the previous logical root unchanged. The version receipt and publish result expose a precise
index status and overflow flag; callers must not present an overflowed or phase-unmapped logical
index as complete coverage. A legacy logical root without phase-to-month evidence is not inferred or
migrated; a write that cannot validate the existing root fails closed before changing its pointer.
No cleanup or migration of legacy logical roots is implicit.

Expired observations are not served as a current cache hit, but their immutable rows remain available
to the reconciliation audit. An expired provisional Futu baseline is labelled
`expired_provisional_historical`; an expired authority-bearing baseline is labelled
`expired_authority_historical`. An unexpired old authority receipt that was not checked by the current
issuer/revocation verifier is labelled `unrevalidated_authority_historical`. None of these historical
labels grants current rights. Material overlap conflicts still reject the full candidate update,
including when the conflicting Futu baseline has expired; expiry cannot silently erase major-conflict
protection. The new candidate still requires its own current, exact-input-bound admission before it
can be authoritative.

Provisional snapshots are retained as immutable versions. They can become current only when there
is no verified authoritative current version for that exact key. They cannot replace an
authoritative current version. A complete provisional session may seed the logical comparison
baseline, but it never gains authority from being indexed; a later provider candidate must pass the
cross-source tolerance gate before the logical pointer can advance.

## Durable publication and recovery

The store root is local and private (`0700` directories, `0600` files). A new root or an existing
empty directory is claimed with the fixed `.mdp-cache-owner` marker using create-only semantics.
An unmarked non-empty directory is rejected before changing permissions or contents; the store
does not migrate another cache or archive root. Reopening requires the exact marker and private
owner-only modes. For each key it holds a bounded `versions/<version-id>/` directory with raw and
canonical Parquet, normalized comparison rows, and a version receipt, plus compact
rejected-candidate audit entries. The mutable `logical/<identity>/transaction.json` is the single
effective commitpoint for one source-key current pointer and its provider-neutral logical root.
Per-key and logical `current.json` files are checksummed mirrors, not independent commitpoints. Once
the identity transaction marker has been installed, a missing or malformed non-BASE head
returns `UnknownOutcome` instead of falling back to a possibly divergent mirror. A valid checksummed
first BASE head is the only unmarked transaction state readers accept; removing the marker from a
later NEW head remains fail-closed.
Version receipts bind the exact key, coverage, artifact sizes/hashes, Core schema fingerprints,
source observations, row checksum/count, authority basis, generation, predecessor, and TTL.
The current pointer contains a domain-separated checksum and the exact version-receipt hash.

Publish takes process-shared per-key, total-budget, and logical-identity file locks. It writes both
Core-schema Parquet objects into private staging, verifies them through the existing bounded Linux
worker, checks source facts/schema/row counts, hashes and reads back the objects, syncs files and
directories, and renames the complete stage to an immutable version. It also writes and verifies
any new immutable logical shards. The transaction head then atomically commits the source pointer
and the complete logical root together. Only after that commitpoint are the two `current.json`
mirrors materialized, synced, read back, and sealed with transaction-hash sidecars. Before the head
rename, a failure leaves the previous effective pointer in place; after it, callers may receive
`UnknownOutcome`, but readers resolve the whole new source/logical pair from the head. Every next
mutation verifies and materializes the prior committed mirrors before replacing the head. The
first transaction installs a checksummed BASE head containing only the already verified old source
current and logical root (or `None`) before writing the identity-level transaction marker. A crash
before that BASE rename leaves the prior unmarked state available; a crash after it but before the
NEW head resolves to the same old state and can be retried. The NEW head remains the only candidate
promotion point. Later transactions keep the identity marker, and a missing or malformed head with
that marker returns `UnknownOutcome`; recovery never guesses a replacement logical state. A missing
marker is accepted only with a valid BASE head, and BASE installation never materializes pointers
for another source key. The per-source marker is deferred until after a committed head, so a failed
attempt cannot hide a legacy current for another source key in this provider-neutral identity. An
explicit `recover_to`
may reassert a selected version or repair a damaged source mirror through the same transaction
path; it does not replace a missing or corrupt transaction head because the associated logical
state cannot then be reconstructed safely. Process death after immutable rename but before the BASE
or NEW transaction-head rename leaves an orphan version, never an auto-promoted cache entry.

Before persisting logical shards or installing a transaction head, the store reserves the new shard
bytes plus bounded head/current/root/mirror temporary files and both transaction markers against
the current tree total. On first use the reservation includes both the BASE head and the NEW-head
temporary-file peak. A limit failure stays before the commitpoint; the store does not evict old
versions to make room.

Linux fault-injection tests exercise the pre-commit boundary, post-commit head, and each mirror
replacement, then reopen the store and assert that source and logical reads select the same complete
version. These are synthetic reopen simulations, not process-kill or storage-device durability
qualification.

`list_verified_versions` verifies immutable data and receipt integrity, but its authority field is a
historical publication result, not current permission to serve that data. `rollback_to` and
`recover_to` accept provisional targets without an issuer verifier; production-authoritative targets
require `rollback_to_with_verifier` or `recover_to_with_verifier`, which recheck the retained signed
admission, expiry, issuer allowlist, and revocation policy. Authoritative reads similarly require
`load_current_with_verifier` or `load_logical_current_with_verifier`; the default production read
path fails closed for authoritative versions. Neither pointer operation scans and promotes an orphan
automatically. Conflict-rejected versions are not eligible rollback/recovery targets. Expired versions
are retained for audit and are not eligible for load, rollback, or recovery. Cache invalidation
returned by a pointer change includes every session changed by one logical-manifest commit and is the
signal the caller must propagate to downstream query caches.

The synchronous cancellable facade checks cancellation before starting. Once publication enters its
bounded atomic section, it completes and joins all worker processes; if cancellation arrives during
that operation the facade reports `UnknownOutcome`, requiring a current-manifest reload instead of
a blind retry. A cancelled read discards its verified result. The store enforces per-object,
metadata, total-tree, row, and per-key version caps. Expired data is not automatically evicted.
Capacity exhaustion fails closed; cleanup policy must be separately reviewed so verified rollback
targets and forensic evidence are not removed implicitly.

## Platform and verification boundary

Parquet readback uses the existing bounded `parquet-worker` process. On non-Linux systems the cache
store returns `Unsupported`; it does not fall back to inline decoding. Therefore the Linux-only
publish/read/rollback/recovery integration test is `NOT_RUN` on macOS. The owning change has not run
local Cargo build or test commands; the Linux GitHub Actions run is required for executable
validation. The module is crate-private and becomes usable by the HTTP query path only when its
owner wires the adapter and pins the frozen Core contract containing the trusted canonical BarsV1
schema.
