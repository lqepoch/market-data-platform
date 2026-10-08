//! Bounded, non-authoritative receipts for one raw/event Parquet chunk.
//!
//! A chunk receipt binds the exact input frame bytes and finalization summaries to two
//! independently read-back-verified artifacts. It does not claim provider completeness or
//! entitlement. The rolling capture receipt retains only bounded counts, boundary keys, and an
//! ordered digest of chunk receipts.

use broker_ports::{RawCaptureInstanceId, RawFrameCaptureKey, RawFrameWireEncoding};
use market_contracts::{
    DatasetManifestV2, DatasetTransportV1, EntitlementState, FiniteBatchSourceKindV2,
    NumericEncodingProtoJsonV2, NumericEncodingV1, UtcTimestamp, parse_dataset_manifest_v2_json,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::parquet_store::ParquetVerification;

const CHUNK_INPUT_HASH_DOMAIN: &[u8] = b"lqepoch.mdp.raw-event-pair-input.v2\0";
const CHUNK_RECEIPT_HASH_DOMAIN: &[u8] = b"lqepoch.mdp.raw-event-pair-receipt.v2\0";
const PAIR_VERIFICATION: &str = "EXACT_RAW_EVENT_ARTIFACT_READBACK_MATCH_LOCAL_ONLY";
const PAIR_VERIFICATION_VERSION: u32 = 1;
const PAIR_RECEIPT_SCHEMA_VERSION: u32 = 2;
const MAX_PAIR_CHUNK_FRAMES: usize = 1_024;
const MAX_PAIR_CHUNK_PAYLOAD_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CAPTURE_FRAMES: u64 = 65_536;
const MAX_CAPTURE_PAYLOAD_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 1_024;
const CAPTURE_PAIR_POLICY_BYTES: &[u8] =
    include_bytes!("../../docs/policies/mdp-capture-pair-v2.md");

mod rollup;
use rollup::CapturePairRollupBuilderV2;

mod publish;

#[cfg(feature = "offline-capture-synthetic")]
pub(crate) mod offline_fixture;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub(super) enum PairReceiptError {
    #[error("pair receipt input is invalid")]
    InvalidInput,
    #[error("pair receipt is invalid")]
    InvalidReceipt,
    #[error("pair receipt sequence continuity failed")]
    SequenceGap,
    #[error("pair receipt capture identity changed")]
    IdentityChanged,
    #[error("pair receipt resource limit exceeded")]
    CapacityExceeded,
}

pub(super) fn valid_local_pair_source(provider: &str, feed: &str) -> bool {
    matches!(
        (provider, feed),
        ("synthetic", "synthetic") | ("alpaca", "opra")
    )
}

/// One frame and its durable, post-decode finalization summary for input hashing.
#[derive(Clone, Copy)]
pub(super) struct PairChunkFrameInput<'a> {
    pub capture_key: &'a RawFrameCaptureKey,
    pub canonical_generation: u64,
    pub received_timestamp_utc: &'a UtcTimestamp,
    pub wire_encoding: RawFrameWireEncoding,
    pub payload: &'a [u8],
    pub finalization_summary_sha256: &'a str,
}

/// One bounded chunk whose rows all share a source and canonical generation.
pub(super) struct PairChunkInputV2<'a> {
    pub capture_instance_id: RawCaptureInstanceId,
    pub provider: &'a str,
    pub feed: &'a str,
    pub entitlement: EntitlementState,
    pub source_generation: u64,
    pub canonical_generation: u64,
    pub frames: &'a [PairChunkFrameInput<'a>],
}

impl PairChunkInputV2<'_> {
    pub(super) fn input_identity(&self) -> Result<String, PairReceiptError> {
        let digest = digest_pair_chunk_input_v2(self)?;
        chunk_input_identity(
            self.provider,
            self.feed,
            self.capture_instance_id,
            self.source_generation,
            digest.first_source_frame_sequence,
            digest.last_source_frame_sequence,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PairChunkInputDigestV2 {
    pub first_source_frame_sequence: u64,
    pub last_source_frame_sequence: u64,
    pub first_frame_sha256: [u8; 32],
    pub last_frame_sha256: [u8; 32],
    pub raw_frame_count: u32,
    pub input_payload_bytes: u64,
    /// SHA-256 of the exact ordered payload byte concatenation consumed by the manifests.
    pub manifest_input_sha256: [u8; 32],
    /// Metadata-rich pair identity digest retained only in the MDP pair receipt.
    pub input_chunk_sha256: [u8; 32],
}

/// Artifact identity derived only after verifying the exact manifest and object readback.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct CaptureArtifactReceiptV2 {
    pub dataset_manifest_schema_version: u32,
    pub dataset_id: String,
    pub manifest_object_name: String,
    pub manifest_object_id: String,
    #[serde(with = "market_contracts::wire_u64")]
    pub manifest_size_bytes: u64,
    pub manifest_sha256: String,
    pub object_name: String,
    pub object_id: String,
    pub content_sha256: String,
    pub parquet_schema_id: String,
    pub parquet_schema_sha256: String,
    pub transport: DatasetTransportV1,
    #[serde(with = "market_contracts::wire_u64")]
    pub size_bytes: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub row_count: u64,
}

/// Exact object and manifest readback facts collected by the archive boundary.
pub(super) struct ArtifactReadbackV2<'a> {
    pub manifest_bytes: &'a [u8],
    pub manifest_object_name: &'a str,
    pub manifest_object_id: &'a str,
    pub manifest_object_size_bytes: u64,
    pub parquet_object_id: &'a str,
    pub parquet_content_sha256: &'a str,
    pub transport: DatasetTransportV1,
    pub parquet: &'a ParquetVerification,
}

/// Facts derived from the one bounded capture chunk being published.
pub(super) struct CaptureChunkArtifactExpectationV2<'a> {
    pub role: ArtifactRole,
    pub dataset_id: &'a str,
    pub object_name: &'a str,
    pub schema_id: &'a str,
    pub provider: &'a str,
    pub feed: &'a str,
    pub entitlement: EntitlementState,
    pub numeric_encoding: NumericEncodingProtoJsonV2,
    pub input_identity: &'a str,
    pub manifest_input_sha256: &'a str,
    pub input_chunk_sha256: &'a str,
    pub input_payload_bytes: u64,
    pub input_record_count: u64,
    pub output_row_count: u64,
}

/// Constructible only after the manifest bytes and Parquet object readback match their contract.
pub(super) struct VerifiedCaptureArtifactReceiptV2 {
    receipt: CaptureArtifactReceiptV2,
    input_identity: String,
}

/// Exact pair verification performed over two finite artifacts and their complete raw-frame keys.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct LocalCapturePairChunkReceiptV2 {
    pub schema_version: u32,
    /// UUIDv4/RFC-variant bytes encoded as exactly 32 lowercase hexadecimal digits.
    pub capture_instance_id: String,
    pub provider: String,
    pub feed: String,
    pub entitlement: EntitlementState,
    #[serde(with = "market_contracts::wire_u64")]
    pub source_generation: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub canonical_generation: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub first_source_frame_sequence: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub last_source_frame_sequence: u64,
    pub first_frame_sha256: String,
    pub last_frame_sha256: String,
    pub raw_frame_count: u32,
    pub normalized_event_count: u32,
    #[serde(with = "market_contracts::wire_u64")]
    pub input_payload_bytes: u64,
    pub input_chunk_sha256: String,
    pub raw_frames: CaptureArtifactReceiptV2,
    pub normalized_events: CaptureArtifactReceiptV2,
    pub pair_verification: String,
    pub pair_verification_version: u32,
    pub pair_receipt_sha256: String,
}

impl LocalCapturePairChunkReceiptV2 {
    pub(super) fn new(
        input: &PairChunkInputV2<'_>,
        normalized_event_count: u32,
        raw_frames: VerifiedCaptureArtifactReceiptV2,
        normalized_events: VerifiedCaptureArtifactReceiptV2,
    ) -> Result<Self, PairReceiptError> {
        let digest = digest_pair_chunk_input_v2(input)?;
        if !valid_local_pair_source(input.provider, input.feed)
            || input.entitlement != EntitlementState::Unknown
            || raw_frames.receipt.transport != DatasetTransportV1::LocalTest
            || normalized_events.receipt.transport != DatasetTransportV1::LocalTest
        {
            return Err(PairReceiptError::InvalidInput);
        }
        let expected_input_identity = chunk_input_identity(
            input.provider,
            input.feed,
            input.capture_instance_id,
            input.source_generation,
            digest.first_source_frame_sequence,
            digest.last_source_frame_sequence,
        )?;
        if raw_frames.input_identity != expected_input_identity
            || normalized_events.input_identity != expected_input_identity
        {
            return Err(PairReceiptError::InvalidReceipt);
        }
        let raw_frames = raw_frames.receipt;
        let normalized_events = normalized_events.receipt;
        let expected_raw_schema_id = match input.frames[0].wire_encoding {
            RawFrameWireEncoding::Json => "lqepoch.market_raw_json_frame.v2",
            RawFrameWireEncoding::MessagePack => "lqepoch.market_raw_frame.v2",
            RawFrameWireEncoding::Unknown => return Err(PairReceiptError::InvalidInput),
            _ => return Err(PairReceiptError::InvalidInput),
        };
        if raw_frames.parquet_schema_id != expected_raw_schema_id
            || normalized_events.parquet_schema_id != "lqepoch.market_event.v3"
        {
            return Err(PairReceiptError::InvalidReceipt);
        }
        let mut receipt = Self {
            schema_version: PAIR_RECEIPT_SCHEMA_VERSION,
            capture_instance_id: hex::encode(input.capture_instance_id.as_bytes()),
            provider: input.provider.to_owned(),
            feed: input.feed.to_owned(),
            entitlement: input.entitlement,
            source_generation: input.source_generation,
            canonical_generation: input.canonical_generation,
            first_source_frame_sequence: digest.first_source_frame_sequence,
            last_source_frame_sequence: digest.last_source_frame_sequence,
            first_frame_sha256: hex::encode(digest.first_frame_sha256),
            last_frame_sha256: hex::encode(digest.last_frame_sha256),
            raw_frame_count: digest.raw_frame_count,
            normalized_event_count,
            input_payload_bytes: digest.input_payload_bytes,
            input_chunk_sha256: hex::encode(digest.input_chunk_sha256),
            raw_frames,
            normalized_events,
            pair_verification: PAIR_VERIFICATION.to_owned(),
            pair_verification_version: PAIR_VERIFICATION_VERSION,
            pair_receipt_sha256: String::new(),
        };
        receipt.pair_receipt_sha256 = receipt.compute_receipt_sha256()?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub(super) fn validate(&self) -> Result<(), PairReceiptError> {
        if self.schema_version != PAIR_RECEIPT_SCHEMA_VERSION
            || !valid_capture_instance_id(&self.capture_instance_id)
            || !valid_local_pair_source(&self.provider, &self.feed)
            || self.entitlement != EntitlementState::Unknown
            || self.source_generation == 0
            || self.canonical_generation == 0
            || self.first_source_frame_sequence == 0
            || self.last_source_frame_sequence < self.first_source_frame_sequence
            || self.raw_frame_count == 0
            || self.raw_frame_count as usize > MAX_PAIR_CHUNK_FRAMES
            || self.last_source_frame_sequence - self.first_source_frame_sequence + 1
                != u64::from(self.raw_frame_count)
            || !valid_sha256(&self.first_frame_sha256)
            || !valid_sha256(&self.last_frame_sha256)
            || self.input_payload_bytes > MAX_PAIR_CHUNK_PAYLOAD_BYTES
            || u64::from(self.normalized_event_count)
                > u64::from(self.raw_frame_count)
                    * u64::from(market_contracts::MAX_RAW_FRAME_EVENT_COUNT)
            || !valid_sha256(&self.input_chunk_sha256)
            || self.pair_verification != PAIR_VERIFICATION
            || self.pair_verification_version != PAIR_VERIFICATION_VERSION
            || self.raw_frames.row_count != u64::from(self.raw_frame_count)
            || self.normalized_events.row_count != u64::from(self.normalized_event_count)
            || self.raw_frames.transport != DatasetTransportV1::LocalTest
            || self.normalized_events.transport != DatasetTransportV1::LocalTest
        {
            return Err(PairReceiptError::InvalidReceipt);
        }
        self.raw_frames.validate(ArtifactRole::RawFrames)?;
        self.normalized_events
            .validate(ArtifactRole::NormalizedEvents)?;
        if self.raw_frames.dataset_id == self.normalized_events.dataset_id
            || self.raw_frames.object_name == self.normalized_events.object_name
            || self.raw_frames.object_id == self.normalized_events.object_id
            || self.raw_frames.manifest_object_name == self.normalized_events.manifest_object_name
            || self.raw_frames.manifest_object_id == self.normalized_events.manifest_object_id
            || self.raw_frames.manifest_sha256 == self.normalized_events.manifest_sha256
            || self.raw_frames.content_sha256 == self.normalized_events.content_sha256
            || self.raw_frames.transport != self.normalized_events.transport
        {
            return Err(PairReceiptError::InvalidReceipt);
        }
        if !valid_sha256(&self.pair_receipt_sha256)
            || self.compute_receipt_sha256()? != self.pair_receipt_sha256
        {
            return Err(PairReceiptError::InvalidReceipt);
        }
        Ok(())
    }

    pub(super) fn to_json_bytes(&self) -> Result<Vec<u8>, PairReceiptError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| PairReceiptError::InvalidReceipt)
    }

    fn compute_receipt_sha256(&self) -> Result<String, PairReceiptError> {
        let capture_id = decode_capture_instance_id(&self.capture_instance_id)?;
        let input_hash = decode_sha256(&self.input_chunk_sha256)?;
        let mut hasher = Sha256::new();
        hasher.update(CHUNK_RECEIPT_HASH_DOMAIN);
        hasher.update(self.schema_version.to_be_bytes());
        hasher.update(capture_id);
        append_string(&mut hasher, &self.provider)?;
        append_string(&mut hasher, &self.feed)?;
        hasher.update(entitlement_tag(self.entitlement).to_be_bytes());
        hasher.update(self.source_generation.to_be_bytes());
        hasher.update(self.canonical_generation.to_be_bytes());
        hasher.update(self.first_source_frame_sequence.to_be_bytes());
        hasher.update(self.last_source_frame_sequence.to_be_bytes());
        hasher.update(decode_sha256(&self.first_frame_sha256)?);
        hasher.update(decode_sha256(&self.last_frame_sha256)?);
        hasher.update(self.raw_frame_count.to_be_bytes());
        hasher.update(self.normalized_event_count.to_be_bytes());
        hasher.update(self.input_payload_bytes.to_be_bytes());
        hasher.update(input_hash);
        self.raw_frames.append_hash_fields(&mut hasher)?;
        self.normalized_events.append_hash_fields(&mut hasher)?;
        append_string(&mut hasher, &self.pair_verification)?;
        hasher.update(self.pair_verification_version.to_be_bytes());
        Ok(hex::encode(hasher.finalize()))
    }
}

impl CaptureArtifactReceiptV2 {
    pub(super) fn verify_manifest_and_readback(
        readback: &ArtifactReadbackV2<'_>,
        expected: &CaptureChunkArtifactExpectationV2<'_>,
    ) -> Result<VerifiedCaptureArtifactReceiptV2, PairReceiptError> {
        let manifest_size_bytes = u64::try_from(readback.manifest_bytes.len())
            .map_err(|_| PairReceiptError::CapacityExceeded)?;
        if readback.manifest_object_size_bytes != manifest_size_bytes
            || readback.manifest_object_name != format!("{}.manifest.json", expected.dataset_id)
            || !valid_local_pair_source(expected.provider, expected.feed)
            || expected.entitlement != EntitlementState::Unknown
            || readback.transport != DatasetTransportV1::LocalTest
            || !valid_transport_object_id(readback.transport, readback.manifest_object_id)
            || !valid_transport_object_id(readback.transport, readback.parquet_object_id)
            || !valid_sha256(readback.parquet_content_sha256)
            || !valid_sha256(expected.manifest_input_sha256)
            || !valid_sha256(expected.input_chunk_sha256)
            || !schema_id_matches_role(expected.schema_id, expected.role)
        {
            return Err(PairReceiptError::InvalidReceipt);
        }

        let manifest = parse_dataset_manifest_v2_json(readback.manifest_bytes)
            .map_err(|_| PairReceiptError::InvalidReceipt)?;
        let manifest_sha256 = hex::encode(Sha256::digest(readback.manifest_bytes));
        let parquet = readback.parquet;
        let expected_numeric_encoding: NumericEncodingV1 = expected.numeric_encoding.into();
        let manifest_numeric_encoding: NumericEncodingV1 = manifest.source.numeric_encoding.into();
        let Some(finite_batch) = manifest.completion_evidence.finite_batch.as_ref() else {
            return Err(PairReceiptError::InvalidReceipt);
        };
        let Some(actual_parquet_object_id) = manifest.object.object_id.clone() else {
            return Err(PairReceiptError::InvalidReceipt);
        };
        let Some(expected_schema_sha256) =
            market_contracts::trusted_schema_fingerprint(expected.schema_id).ok()
        else {
            return Err(PairReceiptError::InvalidReceipt);
        };

        if manifest.dataset_id != expected.dataset_id
            || manifest.row_count != expected.output_row_count
            || manifest.symbols != parquet.symbols
            || !same_time_range(&manifest, parquet)
            || manifest.source_timestamp_missing_rows != parquet.source_timestamp_missing_rows
            || manifest.source.provider != expected.provider
            || manifest.source.feed != expected.feed
            || manifest.source.entitlement != expected.entitlement
            || manifest_numeric_encoding != parquet.source.numeric_encoding
            || manifest.source.source_record_id != parquet.source.source_record_id
            || manifest_numeric_encoding != expected_numeric_encoding
            || manifest.object.object_name != expected.object_name
            || actual_parquet_object_id != readback.parquet_object_id
            || manifest.object.size_bytes != parquet.size_bytes
            || manifest.object.content_sha256 != readback.parquet_content_sha256
            || manifest.object.parquet_schema_sha256 != parquet.schema_sha256
            || manifest.object.parquet_footer_rows != parquet.footer_rows
            || manifest.object.transport != readback.transport
            || manifest.storage_verification.readback_sha256 != readback.parquet_content_sha256
            || !manifest.storage_verification.verified_before_publish
            || parquet.schema_id != expected.schema_id
            || parquet.schema_sha256 != expected_schema_sha256
            || parquet.footer_rows != expected.output_row_count
            || parquet.decoded_rows != expected.output_row_count
            || parquet.size_bytes == 0
            || Some(finite_batch.source_kind)
                != finite_batch_source_kind(expected.provider, expected.feed)
            || !finite_batch_matches_input(
                finite_batch,
                expected.input_identity,
                expected.manifest_input_sha256,
                expected.input_payload_bytes,
                expected.input_record_count,
            )
            || finite_batch.reviewed_policy_sha256 != capture_pair_policy_sha256()
        {
            return Err(PairReceiptError::InvalidReceipt);
        }

        let receipt = Self {
            dataset_manifest_schema_version: manifest.schema_version,
            dataset_id: manifest.dataset_id,
            manifest_object_name: readback.manifest_object_name.to_owned(),
            manifest_object_id: readback.manifest_object_id.to_owned(),
            manifest_size_bytes,
            manifest_sha256,
            object_name: manifest.object.object_name,
            object_id: actual_parquet_object_id,
            content_sha256: readback.parquet_content_sha256.to_owned(),
            parquet_schema_id: expected.schema_id.to_owned(),
            parquet_schema_sha256: parquet.schema_sha256.clone(),
            transport: readback.transport,
            size_bytes: parquet.size_bytes,
            row_count: parquet.decoded_rows,
        };
        receipt.validate(expected.role)?;
        Ok(VerifiedCaptureArtifactReceiptV2 {
            receipt,
            input_identity: expected.input_identity.to_owned(),
        })
    }

    fn validate(&self, role: ArtifactRole) -> Result<(), PairReceiptError> {
        if self.dataset_manifest_schema_version != 2
            || !valid_identifier(&self.dataset_id, 256)
            || !valid_identifier(&self.manifest_object_name, 512)
            || self.manifest_object_name != format!("{}.manifest.json", self.dataset_id)
            || !valid_identifier(&self.manifest_object_id, 2_048)
            || self.manifest_size_bytes == 0
            || !valid_sha256(&self.manifest_sha256)
            || !valid_identifier(&self.object_name, 256)
            || !valid_identifier(&self.object_id, 2_048)
            || !valid_sha256(&self.content_sha256)
            || !valid_sha256(&self.parquet_schema_sha256)
            || self.size_bytes == 0
            || !schema_id_matches_role(&self.parquet_schema_id, role)
            || market_contracts::trusted_schema_fingerprint(&self.parquet_schema_id)
                .ok()
                .as_deref()
                != Some(self.parquet_schema_sha256.as_str())
            || !valid_transport_object_id(self.transport, &self.manifest_object_id)
            || !valid_transport_object_id(self.transport, &self.object_id)
            || self.manifest_object_id == self.object_id
        {
            return Err(PairReceiptError::InvalidReceipt);
        }
        Ok(())
    }

    fn append_hash_fields(&self, hasher: &mut Sha256) -> Result<(), PairReceiptError> {
        hasher.update(self.dataset_manifest_schema_version.to_be_bytes());
        append_string(hasher, &self.dataset_id)?;
        append_string(hasher, &self.manifest_object_name)?;
        append_string(hasher, &self.manifest_object_id)?;
        hasher.update(self.manifest_size_bytes.to_be_bytes());
        hasher.update(decode_sha256(&self.manifest_sha256)?);
        append_string(hasher, &self.object_name)?;
        append_string(hasher, &self.object_id)?;
        hasher.update(decode_sha256(&self.content_sha256)?);
        append_string(hasher, &self.parquet_schema_id)?;
        hasher.update(decode_sha256(&self.parquet_schema_sha256)?);
        hasher.update(transport_tag(self.transport));
        hasher.update(self.size_bytes.to_be_bytes());
        hasher.update(self.row_count.to_be_bytes());
        Ok(())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ArtifactRole {
    RawFrames,
    NormalizedEvents,
}

fn schema_id_matches_role(value: &str, role: ArtifactRole) -> bool {
    match role {
        ArtifactRole::RawFrames => matches!(
            value,
            "lqepoch.market_raw_frame.v2" | "lqepoch.market_raw_json_frame.v2"
        ),
        ArtifactRole::NormalizedEvents => value == "lqepoch.market_event.v3",
    }
}

pub(super) fn digest_pair_chunk_input_v2(
    input: &PairChunkInputV2<'_>,
) -> Result<PairChunkInputDigestV2, PairReceiptError> {
    if !valid_source_field(input.provider)
        || !valid_source_field(input.feed)
        || input.source_generation == 0
        || input.canonical_generation == 0
        || input.frames.is_empty()
        || input.frames.len() > MAX_PAIR_CHUNK_FRAMES
    {
        return Err(PairReceiptError::InvalidInput);
    }

    let mut hasher = Sha256::new();
    let mut manifest_input_hasher = Sha256::new();
    hasher.update(CHUNK_INPUT_HASH_DOMAIN);
    hasher.update(input.capture_instance_id.as_bytes());
    hasher.update(input.source_generation.to_be_bytes());
    hasher.update(input.canonical_generation.to_be_bytes());
    append_string(&mut hasher, input.provider)?;
    append_string(&mut hasher, input.feed)?;
    hasher.update([entitlement_tag(input.entitlement)]);
    let frame_count =
        u32::try_from(input.frames.len()).map_err(|_| PairReceiptError::CapacityExceeded)?;
    hasher.update(frame_count.to_be_bytes());

    let mut payload_bytes = 0_u64;
    let first = input.frames[0].capture_key.frame_sequence();
    let first_wire_encoding = input.frames[0].wire_encoding;
    wire_encoding_tag(first_wire_encoding)?;
    let mut last = first;
    let mut first_frame_sha256 = [0; 32];
    let mut last_frame_sha256 = [0; 32];
    for (index, frame) in input.frames.iter().enumerate() {
        let key = frame.capture_key;
        let expected_sequence = if index == 0 {
            first
        } else {
            last.checked_add(1).ok_or(PairReceiptError::SequenceGap)?
        };
        if key.frame_sequence() != expected_sequence {
            return Err(PairReceiptError::SequenceGap);
        }
        if key.capture_instance_id() != input.capture_instance_id
            || key.source_generation() != input.source_generation
            || frame.canonical_generation != input.canonical_generation
            || frame.wire_encoding != first_wire_encoding
            || !valid_sha256(frame.finalization_summary_sha256)
            || frame.payload.len() > market_contracts::MAX_RAW_FRAME_BYTES
        {
            return Err(PairReceiptError::InvalidInput);
        }
        let payload_sha256: [u8; 32] = Sha256::digest(frame.payload).into();
        if hex::encode(payload_sha256) != key.frame_sha256() {
            return Err(PairReceiptError::InvalidInput);
        }
        payload_bytes = payload_bytes
            .checked_add(
                u64::try_from(frame.payload.len())
                    .map_err(|_| PairReceiptError::CapacityExceeded)?,
            )
            .ok_or(PairReceiptError::CapacityExceeded)?;
        if payload_bytes > MAX_PAIR_CHUNK_PAYLOAD_BYTES {
            return Err(PairReceiptError::CapacityExceeded);
        }

        // Each frame contributes the complete source-local key, exact bytes, receive time,
        // wire encoding, and finalization digest in a fixed byte order.
        hasher.update(key.capture_instance_id().as_bytes());
        hasher.update(key.source_generation().to_be_bytes());
        hasher.update(key.frame_sequence().to_be_bytes());
        hasher.update(payload_sha256);
        let payload_len =
            u32::try_from(frame.payload.len()).map_err(|_| PairReceiptError::CapacityExceeded)?;
        hasher.update(payload_len.to_be_bytes());
        hasher.update(frame.payload);
        manifest_input_hasher.update(frame.payload);
        append_string(&mut hasher, frame.received_timestamp_utc.as_str())?;
        hasher.update([wire_encoding_tag(frame.wire_encoding)?]);
        hasher.update(decode_sha256(frame.finalization_summary_sha256)?);
        if index == 0 {
            first_frame_sha256 = payload_sha256;
        }
        last_frame_sha256 = payload_sha256;
        last = key.frame_sequence();
    }

    let input_chunk_sha256 = hasher.finalize().into();
    let manifest_input_sha256 = manifest_input_hasher.finalize().into();
    Ok(PairChunkInputDigestV2 {
        first_source_frame_sequence: first,
        last_source_frame_sequence: last,
        first_frame_sha256,
        last_frame_sha256,
        raw_frame_count: frame_count,
        input_payload_bytes: payload_bytes,
        manifest_input_sha256,
        input_chunk_sha256,
    })
}

fn valid_capture_instance_id(value: &str) -> bool {
    decode_capture_instance_id(value).is_ok()
}

fn decode_capture_instance_id(value: &str) -> Result<[u8; 16], PairReceiptError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PairReceiptError::InvalidReceipt);
    }
    let bytes = hex::decode(value).map_err(|_| PairReceiptError::InvalidReceipt)?;
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| PairReceiptError::InvalidReceipt)?;
    RawCaptureInstanceId::new(bytes).map_err(|_| PairReceiptError::InvalidReceipt)?;
    Ok(bytes)
}

fn valid_source_field(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_METADATA_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_identifier(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
        && !value.contains('/')
        && !value.contains('\\')
        && value != "."
        && value != ".."
}

fn valid_transport_object_id(transport: DatasetTransportV1, object_id: &str) -> bool {
    match transport {
        DatasetTransportV1::LocalTest => object_id
            .strip_prefix("local-test:")
            .is_some_and(|value| !value.is_empty() && !value.contains("..")),
        DatasetTransportV1::RcloneGoogleDrive => !object_id.starts_with("local-test:"),
    }
}

fn same_time_range(manifest: &DatasetManifestV2, parquet: &ParquetVerification) -> bool {
    match (&manifest.time_range, &parquet.time_range) {
        (None, None) => true,
        (Some(manifest_range), Some(parquet_range)) => {
            manifest_range.start_inclusive.as_str() == parquet_range.start_inclusive.as_str()
                && manifest_range.end_exclusive.as_str() == parquet_range.end_exclusive.as_str()
        }
        _ => false,
    }
}

fn capture_pair_policy_sha256() -> String {
    hex::encode(Sha256::digest(CAPTURE_PAIR_POLICY_BYTES))
}

fn chunk_input_identity(
    provider: &str,
    feed: &str,
    capture_instance_id: RawCaptureInstanceId,
    source_generation: u64,
    first_sequence: u64,
    last_sequence: u64,
) -> Result<String, PairReceiptError> {
    let capture_id = hex::encode(capture_instance_id.as_bytes());
    let identity = match (provider, feed) {
        ("synthetic", "synthetic") => format!(
            "mdp-capture-pair-v2:{capture_id}:source:{source_generation}:frames:{first_sequence}-{last_sequence}"
        ),
        ("alpaca", "opra") => format!(
            "mdp-synthetic-offline-fixture:alpaca-opra-trade-v1:capture:{capture_id}:source:{source_generation}:frames:{first_sequence}-{last_sequence}"
        ),
        _ => return Err(PairReceiptError::InvalidInput),
    };
    Ok(identity)
}

fn finite_batch_source_kind(provider: &str, feed: &str) -> Option<FiniteBatchSourceKindV2> {
    match (provider, feed) {
        ("synthetic", "synthetic") => Some(FiniteBatchSourceKindV2::SyntheticReplay),
        ("alpaca", "opra") => Some(FiniteBatchSourceKindV2::LocalArchive),
        _ => None,
    }
}

fn finite_batch_matches_input(
    finite_batch: &market_contracts::FiniteBatchCompletionV2,
    expected_identity: &str,
    expected_sha256: &str,
    expected_size_bytes: u64,
    expected_record_count: u64,
) -> bool {
    finite_batch.input_identity == expected_identity
        && finite_batch.input_sha256 == expected_sha256
        && finite_batch.input_size_bytes == expected_size_bytes
        && finite_batch.input_record_count == expected_record_count
        && finite_batch.consumed_record_count == expected_record_count
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn decode_sha256(value: &str) -> Result<[u8; 32], PairReceiptError> {
    if !valid_sha256(value) {
        return Err(PairReceiptError::InvalidReceipt);
    }
    hex::decode(value)
        .map_err(|_| PairReceiptError::InvalidReceipt)?
        .try_into()
        .map_err(|_| PairReceiptError::InvalidReceipt)
}

fn append_string(hasher: &mut Sha256, value: &str) -> Result<(), PairReceiptError> {
    if value.len() > MAX_METADATA_BYTES {
        return Err(PairReceiptError::CapacityExceeded);
    }
    let length = u16::try_from(value.len()).map_err(|_| PairReceiptError::CapacityExceeded)?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

const fn entitlement_tag(value: EntitlementState) -> u8 {
    match value {
        EntitlementState::Unknown => 0,
        EntitlementState::Authorized => 1,
        EntitlementState::Unauthorized => 2,
    }
}

fn wire_encoding_tag(value: RawFrameWireEncoding) -> Result<u8, PairReceiptError> {
    match value {
        RawFrameWireEncoding::Json => Ok(1),
        RawFrameWireEncoding::MessagePack => Ok(2),
        RawFrameWireEncoding::Unknown => Err(PairReceiptError::InvalidInput),
        _ => Err(PairReceiptError::InvalidInput),
    }
}

const fn transport_tag(value: DatasetTransportV1) -> [u8; 1] {
    match value {
        DatasetTransportV1::LocalTest => [1],
        DatasetTransportV1::RcloneGoogleDrive => [2],
    }
}
