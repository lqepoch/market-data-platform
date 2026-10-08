# Alpaca offline fixture replay v1

The optional Cargo feature `offline-capture-synthetic` enables exactly one
`capture-synthetic --output <fresh-directory>` command. It invokes the broker-owned
`capture_reviewed_fixture(AlpacaOpraTradeV1, ...)` runner and the normal MDP durable spool and
LocalTest Pair V2 publisher. MDP does not own a connector, Alpaca decoder, arbitrary fixture loader,
credential input, provider URL, or live capture entry point. The feature is off by default and the
command is Linux-only.

The fixed fixture ID is `alpaca-opra-trade-v1`, sealed by SHA-256
`852850f472e4434267bb9d853234fbbced201e229b57d9d1f3c5a0c914818c8c`. It contains four inbound
provider binary MessagePack frames in order: connected, authenticated, subscription ACK, and one
trade. The script and runner digest include all four frames. The first two frames are pre-auth and
are not sent to the raw sink. The post-auth subscription ACK control frame and trade frame each
require a matching predecode ACK and matching finalization ACK; the trade frame alone contributes a
market event. Outbound auth/subscription messages and the local terminal marker are not provider
frames. Limits are eight inbound/captured frames, 64 KiB fixture bytes, 1 MiB per frame, 64 output
items, and a 15-second run deadline.

The connector keeps provider/feed identity `alpaca/opra` and entitlement `unknown`. Pair manifests
use Core's `LocalArchive` finite-batch kind because they describe the exact local finite input and
verified readback; they do not establish provider authority. Dataset IDs and `input_identity` include
`synthetic-offline-fixture` and `alpaca-opra-trade-v1`. The separate MDP-private receipt records
`SYNTHETIC_REPLAY_FIXTURE`, the exact `FINITE_BATCH_SOURCE_KIND_LOCAL_ARCHIVE` projection, and
`source_completeness=NOT_ASSERTED`; the CLI labels the result
`SYNTHETIC_NOT_REAL_OPRA_NOT_LIVE`. The fixed freshness clock (`1791460800`) is only a fixture
cutoff recorded in the receipt. Frame `received_at_utc` remains the actual runner receive timestamp,
not the fixed clock and not evidence of fresh live data. Core manifest `input_sha256` binds the
ordered exact payload bytes of the two post-auth captured frames (subscription ACK and trade), with
`input_size_bytes` equal to their combined byte length. The four-frame script seal stays separately
in the SDK/MDP fixture receipt, and the MDP pair receipt's composite hash separately binds frame
metadata, receive time, and finalization summaries. The single `FixtureEnd` marker exists only in the
offline test transport control plane. EOF, timeout, cancellation, early/missing marker, digest
mismatch, failed or mismatched ACK, and spool/readback failure return an error and create no fixture
receipt. The run does not prove a provider connection, OPRA entitlement, source completeness, a
watermark, Drive durability, or research admission. Partial immutable Pair objects or spool records
are preserved as unresolved local state and are not automatically resumed.

## Digest encodings

The broker's script and runner digest is
`SHA256("eqoboard.alpaca.offline-fixture-input.v1\0" || u32_be(frame_count) || frames...)`. Each
frame contributes `u32_be(exact_byte_length) || exact_MessagePack_bytes` in arrival order. The runner
receipt is accepted only when script count, received count, and both digests match the fixed seal.

The broker's ordered capture digest is
`SHA256("eqoboard.alpaca.offline-fixture-raw-frames.v1\0" || u32_be(count) || frames...)`. Each
captured frame contributes, in predecode order, `capture_uuid[16] || u64_be(source_generation) ||
u64_be(source_frame_sequence) || u32_be(payload_length) || exact_payload_bytes`.

The broker's finalization digest is
`SHA256("eqoboard.alpaca.offline-fixture-finalization.v1\0" || u32_be(count) || frames...)`. Each
frame contributes `capture_uuid[16] || u64_be(source_generation) || u64_be(source_frame_sequence) ||
raw32(frame_sha256) || raw32(matched_finalization_summary_sha256)` in capture order. All integers are
fixed-width big-endian; `raw32` is the 32 bytes decoded from canonical lowercase SHA-256 hex.

The MDP-local receipt is create-only JSON under the private archive state directory. Its hash uses
SHA-256 over `lqepoch.mdp.alpaca-offline-fixture-replay-receipt.v1\0`, then fields in this exact
order: schema version u32; capture UUID bytes; fixture ID as UTF-8 with u16 big-endian length;
fixture SHA-256 raw32; fixture freshness clock u64; manifest completion source-kind label as
length-prefixed UTF-8; provider and feed as length-prefixed UTF-8; entitlement tag u8; capture mode,
terminal scope, and source-completeness strings as length-prefixed UTF-8; script frame count u32;
runner-received frame count u32; runner-received digest raw32; captured frame count u32; raw market
frame count u32; predecode ACK count u32; finalization ACK count u32; captured bytes u64; ordered raw
frame digest raw32; finalization digest raw32; output item count u32; Pair rollup SHA-256 raw32.
String lengths count UTF-8 bytes. The `captured_bytes` JSON projection is a canonical decimal string.
The receipt hash detects field changes; it is not a signature or external authority.
