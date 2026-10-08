use market_contracts::EntitlementState;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    LocalCapturePairChunkReceiptV2, MAX_CAPTURE_FRAMES, MAX_CAPTURE_PAYLOAD_BYTES,
    PairReceiptError, decode_capture_instance_id, decode_sha256, entitlement_tag,
    valid_local_pair_source, valid_sha256, valid_source_field,
};

const ROLLUP_INIT_HASH_DOMAIN: &[u8] = b"lqepoch.mdp.raw-event-pair-rollup-init.v2\0";
const ROLLUP_CHUNK_HASH_DOMAIN: &[u8] = b"lqepoch.mdp.raw-event-pair-rollup-chunk.v2\0";
const ROLLUP_HASH_DOMAIN: &[u8] = b"lqepoch.mdp.raw-event-pair-rollup.v2\0";
const PAIR_VERIFICATION: &str = "EXACT_RAW_EVENT_ARTIFACT_READBACK_MATCH_LOCAL_ONLY";
const PAIR_VERIFICATION_VERSION: u32 = 1;
const CAPTURE_ROLLUP_SCHEMA_VERSION: u32 = 2;
const MAX_CAPTURE_EVENTS: u64 = MAX_CAPTURE_FRAMES * 512;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct PairFrameBoundaryV2 {
    #[serde(with = "market_contracts::wire_u64")]
    pub source_generation: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub source_frame_sequence: u64,
    pub frame_sha256: String,
    #[serde(with = "market_contracts::wire_u64")]
    pub canonical_generation: u64,
}

/// Bounded digest-only summary of the pair receipts accumulated for one capture UUID.
///
/// This reports only the exact finite chunks already added to the rollup. It intentionally has
/// no EOF or provider-completion field.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct LocalCapturePairRollupV2 {
    pub schema_version: u32,
    pub capture_instance_id: String,
    pub provider: String,
    pub feed: String,
    pub entitlement: EntitlementState,
    pub source_completeness: String,
    pub pair_verification: String,
    pub pair_verification_version: u32,
    pub chunk_count: u32,
    #[serde(with = "market_contracts::wire_u64")]
    pub raw_frame_count: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub normalized_event_count: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub input_payload_bytes: u64,
    pub first_frame: PairFrameBoundaryV2,
    pub last_frame: PairFrameBoundaryV2,
    pub ordered_chunk_receipts_sha256: String,
    pub rollup_sha256: String,
}

/// Bounded in-memory accumulator. It stores no per-frame or per-chunk array.
pub(super) struct CapturePairRollupBuilderV2 {
    capture_instance_id: String,
    provider: String,
    feed: String,
    entitlement: EntitlementState,
    chunk_count: u32,
    raw_frame_count: u64,
    normalized_event_count: u64,
    input_payload_bytes: u64,
    first_frame: Option<PairFrameBoundaryV2>,
    last_frame: Option<PairFrameBoundaryV2>,
    ordered_chunk_receipts_sha256: [u8; 32],
    poisoned: bool,
}

impl CapturePairRollupBuilderV2 {
    pub(super) fn new(
        capture_instance_id: &str,
        provider: &str,
        feed: &str,
        entitlement: EntitlementState,
    ) -> Result<Self, PairReceiptError> {
        let capture_bytes = decode_capture_instance_id(capture_instance_id)?;
        if !valid_local_pair_source(provider, feed)
            || entitlement != EntitlementState::Unknown
            || !valid_source_field(provider)
            || !valid_source_field(feed)
        {
            return Err(PairReceiptError::InvalidInput);
        }
        let mut hasher = Sha256::new();
        hasher.update(ROLLUP_INIT_HASH_DOMAIN);
        hasher.update(capture_bytes);
        Ok(Self {
            capture_instance_id: capture_instance_id.to_owned(),
            provider: provider.to_owned(),
            feed: feed.to_owned(),
            entitlement,
            chunk_count: 0,
            raw_frame_count: 0,
            normalized_event_count: 0,
            input_payload_bytes: 0,
            first_frame: None,
            last_frame: None,
            ordered_chunk_receipts_sha256: hasher.finalize().into(),
            poisoned: false,
        })
    }

    /// Adds one already artifact-read-back-verified immutable chunk receipt.
    pub(super) fn add_chunk(
        &mut self,
        receipt: &LocalCapturePairChunkReceiptV2,
    ) -> Result<(), PairReceiptError> {
        if self.poisoned {
            return Err(PairReceiptError::InvalidReceipt);
        }
        if let Err(error) = self.add_chunk_inner(receipt) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    fn add_chunk_inner(
        &mut self,
        receipt: &LocalCapturePairChunkReceiptV2,
    ) -> Result<(), PairReceiptError> {
        receipt.validate()?;
        if receipt.capture_instance_id != self.capture_instance_id
            || receipt.provider != self.provider
            || receipt.feed != self.feed
            || receipt.entitlement != self.entitlement
        {
            return Err(PairReceiptError::IdentityChanged);
        }
        if self.chunk_count == 0 {
            if receipt.first_source_frame_sequence != 1 {
                return Err(PairReceiptError::SequenceGap);
            }
        } else {
            let previous = self
                .last_frame
                .as_ref()
                .ok_or(PairReceiptError::InvalidReceipt)?;
            if receipt.source_generation == previous.source_generation {
                let expected_first = previous
                    .source_frame_sequence
                    .checked_add(1)
                    .ok_or(PairReceiptError::SequenceGap)?;
                if receipt.first_source_frame_sequence != expected_first {
                    return Err(PairReceiptError::SequenceGap);
                }
            } else if receipt.source_generation > previous.source_generation {
                if receipt.first_source_frame_sequence != 1 {
                    return Err(PairReceiptError::SequenceGap);
                }
            } else {
                return Err(PairReceiptError::SequenceGap);
            }
        }

        let next_chunk_count = self
            .chunk_count
            .checked_add(1)
            .ok_or(PairReceiptError::CapacityExceeded)?;
        let next_frame_count = self
            .raw_frame_count
            .checked_add(u64::from(receipt.raw_frame_count))
            .ok_or(PairReceiptError::CapacityExceeded)?;
        let next_event_count = self
            .normalized_event_count
            .checked_add(u64::from(receipt.normalized_event_count))
            .ok_or(PairReceiptError::CapacityExceeded)?;
        let next_payload_bytes = self
            .input_payload_bytes
            .checked_add(receipt.input_payload_bytes)
            .ok_or(PairReceiptError::CapacityExceeded)?;
        if next_frame_count > MAX_CAPTURE_FRAMES || next_payload_bytes > MAX_CAPTURE_PAYLOAD_BYTES {
            return Err(PairReceiptError::CapacityExceeded);
        }

        let pair_receipt_hash = decode_sha256(&receipt.pair_receipt_sha256)?;
        let mut hasher = Sha256::new();
        hasher.update(ROLLUP_CHUNK_HASH_DOMAIN);
        hasher.update(self.ordered_chunk_receipts_sha256);
        hasher.update(next_chunk_count.to_be_bytes());
        hasher.update(pair_receipt_hash);
        self.ordered_chunk_receipts_sha256 = hasher.finalize().into();

        if self.first_frame.is_none() {
            self.first_frame = Some(boundary(receipt, true));
        }
        self.last_frame = Some(boundary(receipt, false));
        self.chunk_count = next_chunk_count;
        self.raw_frame_count = next_frame_count;
        self.normalized_event_count = next_event_count;
        self.input_payload_bytes = next_payload_bytes;
        Ok(())
    }

    /// Returns a local finite-chunk summary; this method accepts no caller EOF or completeness bit.
    pub(super) fn snapshot(&self) -> Result<LocalCapturePairRollupV2, PairReceiptError> {
        if self.poisoned || self.chunk_count == 0 {
            return Err(PairReceiptError::InvalidReceipt);
        }
        let mut rollup = LocalCapturePairRollupV2 {
            schema_version: CAPTURE_ROLLUP_SCHEMA_VERSION,
            capture_instance_id: self.capture_instance_id.clone(),
            provider: self.provider.clone(),
            feed: self.feed.clone(),
            entitlement: self.entitlement,
            source_completeness: "NOT_ASSERTED".to_owned(),
            pair_verification: PAIR_VERIFICATION.to_owned(),
            pair_verification_version: PAIR_VERIFICATION_VERSION,
            chunk_count: self.chunk_count,
            raw_frame_count: self.raw_frame_count,
            normalized_event_count: self.normalized_event_count,
            input_payload_bytes: self.input_payload_bytes,
            first_frame: self
                .first_frame
                .clone()
                .ok_or(PairReceiptError::InvalidReceipt)?,
            last_frame: self
                .last_frame
                .clone()
                .ok_or(PairReceiptError::InvalidReceipt)?,
            ordered_chunk_receipts_sha256: hex::encode(self.ordered_chunk_receipts_sha256),
            rollup_sha256: String::new(),
        };
        rollup.rollup_sha256 = rollup.compute_rollup_sha256()?;
        rollup.validate()?;
        Ok(rollup)
    }
}

impl LocalCapturePairRollupV2 {
    pub(super) fn validate(&self) -> Result<(), PairReceiptError> {
        let capture_id = decode_capture_instance_id(&self.capture_instance_id)?;
        if self.schema_version != CAPTURE_ROLLUP_SCHEMA_VERSION
            || !valid_local_pair_source(&self.provider, &self.feed)
            || self.entitlement != EntitlementState::Unknown
            || self.source_completeness != "NOT_ASSERTED"
            || self.pair_verification != PAIR_VERIFICATION
            || self.pair_verification_version != PAIR_VERIFICATION_VERSION
            || self.chunk_count == 0
            || self.raw_frame_count == 0
            || self.raw_frame_count > MAX_CAPTURE_FRAMES
            || self.normalized_event_count > MAX_CAPTURE_EVENTS
            || self.input_payload_bytes > MAX_CAPTURE_PAYLOAD_BYTES
            || u64::from(self.chunk_count) > self.raw_frame_count
            || !valid_sha256(&self.ordered_chunk_receipts_sha256)
            || !valid_boundary(&self.first_frame)
            || !valid_boundary(&self.last_frame)
            || self.first_frame.source_frame_sequence != 1
            || self.last_frame.source_generation < self.first_frame.source_generation
            || (self.last_frame.source_generation == self.first_frame.source_generation
                && self.last_frame.source_frame_sequence < self.first_frame.source_frame_sequence)
            || !valid_sha256(&self.rollup_sha256)
            || self.compute_rollup_sha256()? != self.rollup_sha256
        {
            return Err(PairReceiptError::InvalidReceipt);
        }
        let _ = capture_id;
        Ok(())
    }

    pub(super) fn to_json_bytes(&self) -> Result<Vec<u8>, PairReceiptError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| PairReceiptError::InvalidReceipt)
    }

    fn compute_rollup_sha256(&self) -> Result<String, PairReceiptError> {
        let capture_bytes = decode_capture_instance_id(&self.capture_instance_id)?;
        let digest = decode_sha256(&self.ordered_chunk_receipts_sha256)?;
        let mut hasher = Sha256::new();
        hasher.update(ROLLUP_HASH_DOMAIN);
        hasher.update(self.schema_version.to_be_bytes());
        hasher.update(capture_bytes);
        append_string(&mut hasher, &self.provider)?;
        append_string(&mut hasher, &self.feed)?;
        hasher.update([entitlement_tag(self.entitlement)]);
        append_string(&mut hasher, &self.source_completeness)?;
        append_string(&mut hasher, &self.pair_verification)?;
        hasher.update(self.pair_verification_version.to_be_bytes());
        hasher.update(self.chunk_count.to_be_bytes());
        hasher.update(self.raw_frame_count.to_be_bytes());
        hasher.update(self.normalized_event_count.to_be_bytes());
        hasher.update(self.input_payload_bytes.to_be_bytes());
        append_boundary(&mut hasher, &self.first_frame)?;
        append_boundary(&mut hasher, &self.last_frame)?;
        hasher.update(digest);
        Ok(hex::encode(hasher.finalize()))
    }
}

fn boundary(receipt: &LocalCapturePairChunkReceiptV2, first: bool) -> PairFrameBoundaryV2 {
    if first {
        PairFrameBoundaryV2 {
            source_generation: receipt.source_generation,
            source_frame_sequence: receipt.first_source_frame_sequence,
            frame_sha256: receipt.first_frame_sha256.clone(),
            canonical_generation: receipt.canonical_generation,
        }
    } else {
        PairFrameBoundaryV2 {
            source_generation: receipt.source_generation,
            source_frame_sequence: receipt.last_source_frame_sequence,
            frame_sha256: receipt.last_frame_sha256.clone(),
            canonical_generation: receipt.canonical_generation,
        }
    }
}

fn valid_boundary(value: &PairFrameBoundaryV2) -> bool {
    value.source_generation > 0
        && value.source_frame_sequence > 0
        && value.canonical_generation > 0
        && valid_sha256(&value.frame_sha256)
}

fn append_boundary(
    hasher: &mut Sha256,
    value: &PairFrameBoundaryV2,
) -> Result<(), PairReceiptError> {
    hasher.update(value.source_generation.to_be_bytes());
    hasher.update(value.source_frame_sequence.to_be_bytes());
    hasher.update(decode_sha256(&value.frame_sha256)?);
    hasher.update(value.canonical_generation.to_be_bytes());
    Ok(())
}

fn append_string(hasher: &mut Sha256, value: &str) -> Result<(), PairReceiptError> {
    if value.len() > super::MAX_METADATA_BYTES {
        return Err(PairReceiptError::CapacityExceeded);
    }
    let length = u16::try_from(value.len()).map_err(|_| PairReceiptError::CapacityExceeded)?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}
