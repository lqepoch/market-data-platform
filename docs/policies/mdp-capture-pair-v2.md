MDP local raw/event capture-pair policy version 2

The input is an ordered, finite chunk from one broker capture UUID and one source-local generation.
It contains one canonical generation and one raw wire encoding, contiguous one-based source frame
sequences, no more than 1,024 frames, and no more than 16 MiB of exact payload bytes. The MDP
`input_chunk_sha256` composite digest binds the UUID, provider, exact feed, entitlement observation,
source and canonical generations, every complete source key, exact payload bytes, receive timestamp,
wire encoding, and the broker's post-decode finalization summary hash.
Only decoded market-data and control-message frames are eligible for Pair publication. Unknown,
malformed, or provider-error frames remain in bounded spool/quarantine and reject publication until
they are separately classified.

The raw-frame and normalized-event Parquet artifacts must both reference the same finite input
identity and Core exact-input facts. Core manifest `finite_batch.input_sha256` is SHA-256 of the
exact raw frame payload bytes concatenated in source frame order; `finite_batch.input_size_bytes` is
the sum of those payload lengths. This physical input includes the exact frames in the chunk (for
the broker fixture, the post-auth subscription ACK and trade), not the separate four-frame offline
script seal. The MDP pair receipt separately records the metadata-rich `input_chunk_sha256`
composite. Never project that composite digest into Core's `finite_batch.input_sha256`. Independent
object and manifest readbacks must match exact IDs, names, byte sizes, SHA-256 values, registered
schema descriptors, footer and decoded row counts, all decoded row values, and source facts. Every
event reference must join to the full raw source key and canonical generation, and all expected
frame/event ordinals must be present before a pair receipt is created.

This policy records only a finite local observation. It does not assert provider completeness,
entitlement, a watermark, a point-in-time guarantee, Drive durability, or research admission.
Interrupted or ambiguous publication remains unknown and quarantined; it is never silently resumed.

## Digest byte encoding

All digests use SHA-256 over concatenated fields in the order below. Unsigned integers are fixed
width and big-endian. Metadata strings are UTF-8 preceded by their byte length as a big-endian
u16; a string longer than 1,024 bytes is rejected. SHA values in the hash preimage are decoded
from canonical lowercase hex to their raw 32-byte value. The JSON receipts serialize u64 values as
canonical decimal strings.

The MDP `input_chunk_sha256` composite digest starts with the raw bytes of
`lqepoch.mdp.raw-event-pair-input.v2\0`, then the 16 capture UUID bytes, source generation u64,
canonical generation u64, provider and feed strings, entitlement tag u8, and frame count u32. Each
frame in order contributes capture UUID bytes, source generation u64, source frame sequence u64,
payload SHA-256 bytes, payload length u32, exact payload bytes, receive timestamp string, wire
encoding tag u8, and the broker finalization-summary SHA-256 bytes. Tags are: entitlement
unknown/authorized/unauthorized = 0/1/2; JSON/MessagePack = 1/2. The fixed MDP composite-digest
golden is `fixtures/raw-event-pair-input-v2.json`; its one-frame digest is also checked by the Rust
test. Core `finite_batch.input_sha256` instead hashes only the ordered exact payload-byte
concatenation described above.

The chunk receipt hash starts with
`lqepoch.mdp.raw-event-pair-receipt.v2\0`, then schema version u32, capture UUID bytes, provider
and feed strings, entitlement tag u8, source and canonical generations u64, first and last source
frame sequences u64, first and last frame SHA-256 bytes, raw-frame and normalized-event counts u32,
input payload bytes u64, and input-chunk SHA-256 bytes. It then appends the raw-artifact and
normalized-event artifact fields in that order. Each artifact contributes manifest schema version
u32; dataset ID, manifest object name, and manifest object ID strings; manifest size u64 and manifest
SHA-256 bytes; object name and object ID strings; content SHA-256 bytes; Parquet schema ID string
and schema SHA-256 bytes; transport tag u8; object size u64; row count u64. The verification name
string and verification version u32 finish the preimage. Transport tags are LocalTest/RcloneDrive =
1/2. The fixed receipt and recalculated digest are in
`fixtures/raw-event-pair-receipt-v2.json`; tests mutate artifact identity, schema, and input fields
and require the original receipt digest to stop validating.

The bounded rollup uses an initial domain-separated digest of capture UUID bytes, then a chained
digest per receipt: chunk-domain bytes, previous chain digest, next chunk count u32, and pair
receipt SHA-256 bytes. Its final hash uses the rollup domain, schema version u32, capture UUID bytes,
provider/feed strings, entitlement tag u8, source-completeness and verification strings,
verification version u32, chunk count u32, frame/event counts and payload bytes u64, first/last
frame boundaries, and the ordered chunk-chain digest. A boundary is source generation u64, source
frame sequence u64, frame SHA-256 bytes, and canonical generation u64. This bounded rollup binds
observed chunk receipts; it has no EOF or provider-completion input.
