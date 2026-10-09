//! Isolated verification implementation for one staged LocalTest capture-pair chunk.

use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

use broker_ports::{
    RawCaptureInstanceId, RawFrameCapture, RawFrameCaptureAck, RawFrameDisposition,
    RawFrameFinalization, RawFrameFinalizationAck, RawFramePayload, RawFrameWireEncoding,
};
use market_contracts::{
    DatasetTransportV1, EntitlementState, NumericEncodingV1, RawFrameDispositionV1,
    RawFrameStorageRecordV2, RawJsonFrameStorageRecordV2, parse_dataset_manifest_v2_json,
    validate_json_capture_chunk_v2, validate_messagepack_capture_chunk_v2,
};
use sha2::{Digest, Sha256};

use super::super::{
    ArtifactReadbackV2, ArtifactRole, CaptureArtifactReceiptV2, CaptureChunkArtifactExpectationV2,
    LocalCapturePairChunkReceiptV2, PairChunkFrameInput, PairChunkInputDigestV2, PairChunkInputV2,
    decode_capture_instance_id, digest_pair_chunk_input_v2, valid_local_pair_source,
};
use super::{
    MAX_PAIR_RECEIPT_BYTES, PAIR_SCOPE, PAIR_STATUS, VerifiedCapturePairChunkV2, map_pair_error,
    valid_chunk_receipt_name,
};
use crate::{
    MarketDataError, Result,
    archive::{
        capture_pair_v2::publish::{dataset_id_for_identity, numeric_encoding_proto_json},
        hash_file,
    },
    parquet_store::{self, ParquetVerification},
    parquet_worker::CapturePairV2WorkerRequest,
    schema::{EVENT_SCHEMA_V3_ID, RAW_FRAME_SCHEMA_V2_ID, RAW_JSON_FRAME_SCHEMA_V2_ID},
};

/// Verifies staged receipt/manifests/objects and returns compact facts only.
pub(crate) fn verify_pair_chunk_files(
    request: &CapturePairV2WorkerRequest<'_>,
) -> Result<VerifiedCapturePairChunkV2> {
    let (receipt, _) = read_staged_pair_receipt(request.receipt_path)?;
    receipt.validate().map_err(map_pair_error)?;
    verify_receipt_source(&receipt)?;
    if request.raw_schema_id != receipt.raw_frames.parquet_schema_id
        || receipt.normalized_events.parquet_schema_id != EVENT_SCHEMA_V3_ID
    {
        return Err(MarketDataError::ParquetSchema);
    }

    let raw_manifest_bytes =
        read_bounded_file(request.raw_manifest_path, request.max_manifest_bytes)?;
    let event_manifest_bytes =
        read_bounded_file(request.event_manifest_path, request.max_manifest_bytes)?;
    parse_dataset_manifest_v2_json(&raw_manifest_bytes).map_err(|_| MarketDataError::Contract)?;
    parse_dataset_manifest_v2_json(&event_manifest_bytes).map_err(|_| MarketDataError::Contract)?;
    verify_bytes(&raw_manifest_bytes, &receipt.raw_frames.manifest_sha256)?;
    verify_bytes(
        &event_manifest_bytes,
        &receipt.normalized_events.manifest_sha256,
    )?;
    verify_file(
        request.raw_parquet_path,
        request.max_object_bytes,
        &receipt.raw_frames,
    )?;
    verify_file(
        request.event_parquet_path,
        request.max_object_bytes,
        &receipt.normalized_events,
    )?;

    let raw_verification = parquet_store::verify_with_limit(
        request.raw_parquet_path,
        request.raw_schema_id,
        request.max_object_bytes,
    )?;
    let event_verification = parquet_store::verify_with_limit(
        request.event_parquet_path,
        EVENT_SCHEMA_V3_ID,
        request.max_object_bytes,
    )?;
    let (digest, raw_count, event_count) = verify_pair_parquet_rows(
        request.raw_parquet_path,
        request.raw_schema_id,
        request.event_parquet_path,
        request.max_object_bytes,
        &receipt,
    )?;
    verify_receipt_identity(
        &receipt,
        request.receipt_name,
        &digest,
        raw_count,
        event_count,
    )?;

    let input_identity = input_identity(&receipt, &digest)?;
    let raw_dataset_id =
        dataset_id_for_identity(&input_identity, "raw-v2", &receipt.provider, &receipt.feed)?;
    let event_dataset_id = dataset_id_for_identity(
        &input_identity,
        "events-v3",
        &receipt.provider,
        &receipt.feed,
    )?;
    if receipt.raw_frames.dataset_id != raw_dataset_id
        || receipt.normalized_events.dataset_id != event_dataset_id
    {
        return Err(MarketDataError::IncompleteWindow);
    }

    let raw_verified = verify_artifact_manifest(
        &receipt.raw_frames,
        &raw_manifest_bytes,
        &raw_verification,
        &input_identity,
        &digest,
        &receipt,
        ArtifactRole::RawFrames,
    )?;
    let event_verified = verify_artifact_manifest(
        &receipt.normalized_events,
        &event_manifest_bytes,
        &event_verification,
        &input_identity,
        &digest,
        &receipt,
        ArtifactRole::NormalizedEvents,
    )?;
    if raw_verified != receipt.raw_frames || event_verified != receipt.normalized_events {
        return Err(MarketDataError::Conflict);
    }

    Ok(VerifiedCapturePairChunkV2 {
        status: PAIR_STATUS.to_owned(),
        verification_scope: PAIR_SCOPE.to_owned(),
        source_completeness: "NOT_ASSERTED".to_owned(),
        pair_verification: receipt.pair_verification,
        pair_verification_version: receipt.pair_verification_version,
        transport: "local_test".to_owned(),
        provider: receipt.provider,
        feed: receipt.feed,
        entitlement: receipt.entitlement,
        capture_instance_id: receipt.capture_instance_id,
        source_generation: receipt.source_generation,
        canonical_generation: receipt.canonical_generation,
        first_source_frame_sequence: receipt.first_source_frame_sequence,
        last_source_frame_sequence: receipt.last_source_frame_sequence,
        raw_frame_count: receipt.raw_frame_count,
        normalized_event_count: receipt.normalized_event_count,
        input_payload_bytes: receipt.input_payload_bytes,
        input_chunk_sha256: receipt.input_chunk_sha256,
        manifest_input_sha256: hex::encode(digest.manifest_input_sha256),
        raw_dataset_id,
        raw_manifest_sha256: receipt.raw_frames.manifest_sha256,
        raw_object_sha256: receipt.raw_frames.content_sha256,
        raw_schema_id: receipt.raw_frames.parquet_schema_id,
        raw_row_count: receipt.raw_frames.row_count,
        event_dataset_id,
        event_manifest_sha256: receipt.normalized_events.manifest_sha256,
        event_object_sha256: receipt.normalized_events.content_sha256,
        event_schema_id: receipt.normalized_events.parquet_schema_id,
        event_row_count: receipt.normalized_events.row_count,
    })
}

fn verify_receipt_source(receipt: &LocalCapturePairChunkReceiptV2) -> Result<()> {
    if !valid_local_pair_source(&receipt.provider, &receipt.feed)
        || receipt.entitlement != EntitlementState::Unknown
        || receipt.raw_frames.transport != DatasetTransportV1::LocalTest
        || receipt.normalized_events.transport != DatasetTransportV1::LocalTest
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    Ok(())
}

fn verify_receipt_identity(
    receipt: &LocalCapturePairChunkReceiptV2,
    receipt_name: &str,
    digest: &PairChunkInputDigestV2,
    raw_count: usize,
    event_count: usize,
) -> Result<()> {
    let input_identity = input_identity(receipt, digest)?;
    let expected_name = format!(
        "chunk-{}.receipt.json",
        hex::encode(Sha256::digest(input_identity.as_bytes()))
    );
    if !valid_chunk_receipt_name(receipt_name)
        || receipt_name != expected_name
        || hex::encode(digest.input_chunk_sha256) != receipt.input_chunk_sha256
        || digest.input_payload_bytes != receipt.input_payload_bytes
        || digest.raw_frame_count != receipt.raw_frame_count
        || digest.first_source_frame_sequence != receipt.first_source_frame_sequence
        || digest.last_source_frame_sequence != receipt.last_source_frame_sequence
        || hex::encode(digest.first_frame_sha256) != receipt.first_frame_sha256
        || hex::encode(digest.last_frame_sha256) != receipt.last_frame_sha256
        || raw_count != receipt.raw_frame_count as usize
        || event_count != receipt.normalized_event_count as usize
    {
        return Err(MarketDataError::Conflict);
    }
    Ok(())
}

fn input_identity(
    receipt: &LocalCapturePairChunkReceiptV2,
    digest: &PairChunkInputDigestV2,
) -> Result<String> {
    let capture_id = RawCaptureInstanceId::new(
        decode_capture_instance_id(&receipt.capture_instance_id).map_err(map_pair_error)?,
    )
    .map_err(|_| MarketDataError::IncompleteWindow)?;
    super::super::chunk_input_identity(
        &receipt.provider,
        &receipt.feed,
        capture_id,
        receipt.source_generation,
        digest.first_source_frame_sequence,
        digest.last_source_frame_sequence,
    )
    .map_err(map_pair_error)
}

fn verify_artifact_manifest(
    artifact: &CaptureArtifactReceiptV2,
    manifest_bytes: &[u8],
    parquet: &ParquetVerification,
    input_identity: &str,
    digest: &PairChunkInputDigestV2,
    pair: &LocalCapturePairChunkReceiptV2,
    role: ArtifactRole,
) -> Result<CaptureArtifactReceiptV2> {
    let manifest_input_sha256 = hex::encode(digest.manifest_input_sha256);
    let expected = CaptureChunkArtifactExpectationV2 {
        role,
        dataset_id: &artifact.dataset_id,
        object_name: &artifact.object_name,
        schema_id: &artifact.parquet_schema_id,
        provider: &pair.provider,
        feed: &pair.feed,
        entitlement: pair.entitlement,
        numeric_encoding: numeric_encoding_proto_json(parquet.source.numeric_encoding)?,
        input_identity,
        manifest_input_sha256: &manifest_input_sha256,
        input_chunk_sha256: &pair.input_chunk_sha256,
        input_payload_bytes: digest.input_payload_bytes,
        input_record_count: u64::from(digest.raw_frame_count),
        output_row_count: artifact.row_count,
    };
    let readback = ArtifactReadbackV2 {
        manifest_bytes,
        manifest_object_name: &artifact.manifest_object_name,
        manifest_object_id: &artifact.manifest_object_id,
        manifest_object_size_bytes: artifact.manifest_size_bytes,
        parquet_object_id: &artifact.object_id,
        parquet_content_sha256: &artifact.content_sha256,
        transport: DatasetTransportV1::LocalTest,
        parquet,
    };
    CaptureArtifactReceiptV2::verify_manifest_and_readback(&readback, &expected)
        .map(|verified| verified.receipt)
        .map_err(map_pair_error)
}

fn verify_pair_parquet_rows(
    raw_path: &Path,
    raw_schema_id: &str,
    event_path: &Path,
    max_object_bytes: u64,
    receipt: &LocalCapturePairChunkReceiptV2,
) -> Result<(PairChunkInputDigestV2, usize, usize)> {
    let event_rows = parquet_store::read_capture_event_v3(event_path, max_object_bytes)?;
    let frames = match raw_schema_id {
        RAW_FRAME_SCHEMA_V2_ID => {
            let rows = parquet_store::read_capture_raw_frames_v2(raw_path, max_object_bytes)?;
            validate_messagepack_capture_chunk_v2(&rows, &event_rows)
                .map_err(|_| MarketDataError::IncompleteWindow)?;
            rows.into_iter()
                .map(raw_messagepack_fact)
                .collect::<Result<Vec<_>>>()?
        }
        RAW_JSON_FRAME_SCHEMA_V2_ID => {
            let rows = parquet_store::read_capture_raw_json_frames_v2(raw_path, max_object_bytes)?;
            validate_json_capture_chunk_v2(&rows, &event_rows)
                .map_err(|_| MarketDataError::IncompleteWindow)?;
            rows.into_iter()
                .map(raw_json_fact)
                .collect::<Result<Vec<_>>>()?
        }
        _ => return Err(MarketDataError::ParquetSchema),
    };
    if frames.len() != receipt.raw_frame_count as usize
        || event_rows.len() != receipt.normalized_event_count as usize
    {
        return Err(MarketDataError::IncompleteWindow);
    }

    let capture_id = RawCaptureInstanceId::new(
        decode_capture_instance_id(&receipt.capture_instance_id).map_err(map_pair_error)?,
    )
    .map_err(|_| MarketDataError::IncompleteWindow)?;
    let mut reconstructed = Vec::with_capacity(frames.len());
    for frame in &frames {
        let id = RawCaptureInstanceId::new(
            decode_capture_instance_id(&frame.capture_instance_id).map_err(map_pair_error)?,
        )
        .map_err(|_| MarketDataError::IncompleteWindow)?;
        let capture = RawFrameCapture::new(
            id,
            frame.provider.clone(),
            frame.feed.clone(),
            frame.entitlement,
            frame.source_generation,
            frame.frame_sequence,
            frame.received_timestamp_utc.clone(),
            frame.wire_encoding,
            RawFramePayload::capture(frame.frame_bytes.clone())
                .map_err(|_| MarketDataError::InputLimit)?,
        )
        .map_err(|_| MarketDataError::IncompleteWindow)?;
        if capture.capture_key().frame_sha256() != frame.frame_sha256 {
            return Err(MarketDataError::Conflict);
        }
        let finalization = RawFrameFinalization::new(
            frame.event_count,
            frame.symbols.clone(),
            frame.source_numeric_encoding,
            disposition_to_broker(frame.disposition)?,
        )
        .map_err(|_| MarketDataError::IncompleteWindow)?;
        let predecode_ack = RawFrameCaptureAck::for_capture(&capture);
        let finalization_ack =
            RawFrameFinalizationAck::for_finalization(&predecode_ack, &finalization);
        reconstructed.push((capture, finalization_ack.summary_sha256().to_owned()));
    }
    let frame_inputs = frames
        .iter()
        .zip(&reconstructed)
        .map(|(frame, (capture, summary_sha256))| PairChunkFrameInput {
            capture_key: capture.capture_key(),
            canonical_generation: frame.canonical_generation,
            received_timestamp_utc: &frame.received_timestamp_utc,
            wire_encoding: frame.wire_encoding,
            payload: &frame.frame_bytes,
            finalization_summary_sha256: summary_sha256,
        })
        .collect::<Vec<_>>();
    let first = frames.first().ok_or(MarketDataError::IncompleteWindow)?;
    let pair_input = PairChunkInputV2 {
        capture_instance_id: capture_id,
        provider: &first.provider,
        feed: &first.feed,
        entitlement: first.entitlement,
        source_generation: first.source_generation,
        canonical_generation: first.canonical_generation,
        frames: &frame_inputs,
    };
    if pair_input.provider != receipt.provider
        || pair_input.feed != receipt.feed
        || pair_input.entitlement != EntitlementState::Unknown
        || pair_input.source_generation != receipt.source_generation
        || pair_input.canonical_generation != receipt.canonical_generation
    {
        return Err(MarketDataError::IncompleteWindow);
    }
    let digest = digest_pair_chunk_input_v2(&pair_input).map_err(map_pair_error)?;
    Ok((digest, frames.len(), event_rows.len()))
}

struct RawFrameFact {
    provider: String,
    feed: String,
    entitlement: EntitlementState,
    capture_instance_id: String,
    source_generation: u64,
    frame_sequence: u64,
    canonical_generation: u64,
    received_timestamp_utc: market_contracts::UtcTimestamp,
    frame_sha256: String,
    frame_bytes: Vec<u8>,
    event_count: u32,
    disposition: RawFrameDispositionV1,
    source_numeric_encoding: Option<NumericEncodingV1>,
    wire_encoding: RawFrameWireEncoding,
    symbols: Vec<String>,
}

fn raw_messagepack_fact(row: RawFrameStorageRecordV2) -> Result<RawFrameFact> {
    let symbols = row.symbols().map_err(|_| MarketDataError::Contract)?;
    Ok(RawFrameFact {
        provider: row.provider,
        feed: row.feed,
        entitlement: row.entitlement,
        capture_instance_id: row.capture_instance_id.as_str().to_owned(),
        source_generation: row.source_generation,
        frame_sequence: row.source_frame_sequence,
        canonical_generation: row.canonical_generation,
        received_timestamp_utc: row.received_timestamp_utc,
        frame_sha256: row.frame_sha256,
        frame_bytes: row.frame_bytes,
        event_count: row.event_count,
        disposition: row.disposition,
        source_numeric_encoding: row.source_numeric_encoding,
        wire_encoding: RawFrameWireEncoding::MessagePack,
        symbols,
    })
}

fn raw_json_fact(row: RawJsonFrameStorageRecordV2) -> Result<RawFrameFact> {
    let symbols = row.symbols().map_err(|_| MarketDataError::Contract)?;
    Ok(RawFrameFact {
        provider: row.provider,
        feed: row.feed,
        entitlement: row.entitlement,
        capture_instance_id: row.capture_instance_id.as_str().to_owned(),
        source_generation: row.source_generation,
        frame_sequence: row.source_frame_sequence,
        canonical_generation: row.canonical_generation,
        received_timestamp_utc: row.received_timestamp_utc,
        frame_sha256: row.frame_sha256,
        frame_bytes: row.frame_bytes,
        event_count: row.event_count,
        disposition: row.disposition,
        source_numeric_encoding: row.source_numeric_encoding,
        wire_encoding: RawFrameWireEncoding::Json,
        symbols,
    })
}

fn disposition_to_broker(value: RawFrameDispositionV1) -> Result<RawFrameDisposition> {
    match value {
        RawFrameDispositionV1::MarketData => Ok(RawFrameDisposition::DecodedMarketData),
        RawFrameDispositionV1::Control => Ok(RawFrameDisposition::ControlMessage),
        RawFrameDispositionV1::UnknownMessage => Ok(RawFrameDisposition::UnknownMessage),
        RawFrameDispositionV1::MalformedMessage => Ok(RawFrameDisposition::DecodeFailure),
        RawFrameDispositionV1::ProviderError => Ok(RawFrameDisposition::ProviderError),
    }
}

fn verify_file(path: &Path, maximum: u64, expected: &CaptureArtifactReceiptV2) -> Result<()> {
    let actual = hash_file(path, maximum)?;
    if actual.content_sha256 != expected.content_sha256 || actual.size_bytes != expected.size_bytes
    {
        return Err(MarketDataError::Conflict);
    }
    Ok(())
}

fn verify_bytes(bytes: &[u8], expected_sha256: &str) -> Result<()> {
    if hex::encode(Sha256::digest(bytes)) != expected_sha256 {
        return Err(MarketDataError::Conflict);
    }
    Ok(())
}

fn read_staged_pair_receipt(path: &Path) -> Result<(LocalCapturePairChunkReceiptV2, Vec<u8>)> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
        || metadata.len() == 0
        || metadata.len() > MAX_PAIR_RECEIPT_BYTES
    {
        return Err(MarketDataError::InputLimit);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(MAX_PAIR_RECEIPT_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > MAX_PAIR_RECEIPT_BYTES {
        return Err(MarketDataError::InputLimit);
    }
    let receipt: LocalCapturePairChunkReceiptV2 =
        serde_json::from_slice(&bytes).map_err(|_| MarketDataError::IncompleteWindow)?;
    if receipt.to_json_bytes().map_err(map_pair_error)? != bytes {
        return Err(MarketDataError::IncompleteWindow);
    }
    Ok((receipt, bytes))
}

fn read_bounded_file(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if maximum == 0
        || !metadata.file_type().is_file()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(MarketDataError::InputLimit);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > maximum {
        return Err(MarketDataError::InputLimit);
    }
    Ok(bytes)
}
