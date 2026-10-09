//! Local-only publisher for durable synthetic captures under Core-owned V2/V3 contracts.

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use broker_ports::{
    RawCaptureInstanceId, RawFrameCaptureAck, RawFrameDisposition, RawFrameFinalizationAck,
    RawFrameWireEncoding, RawMarketFrame,
};
use market_contracts::{
    DatasetCompletionEvidenceV2, DatasetManifestV2, DatasetObjectV2, DatasetSourceV2,
    DatasetStorageVerificationV2, DatasetTimeRangeV2, DatasetTransportV1, EntitlementState,
    FiniteBatchCompletionV2, FiniteBatchSourceKindV2, MarketEventEnvelopeV1,
    MarketEventParquetRowV3, NumericEncodingProtoJsonV2, NumericEncodingV1, ProtoTimestampV2,
    RawFrameCaptureInstanceIdV2, RawFrameDispositionV1, RawFrameReferenceV3,
    RawFrameStorageRecordV2, RawJsonFrameStorageRecordV2, dataset_manifest_v2_protojson_bytes,
    finite_batch_seal_receipt_sha256, parse_dataset_manifest_v2_json,
    validate_json_capture_chunk_v2, validate_messagepack_capture_chunk_v2,
};
use sha2::{Digest, Sha256};

use super::rollup::LocalCapturePairRollupV2;
use super::{
    ArtifactReadbackV2, ArtifactRole, CaptureArtifactReceiptV2, CaptureChunkArtifactExpectationV2,
    CapturePairRollupBuilderV2, LocalCapturePairChunkReceiptV2, MAX_CAPTURE_FRAMES,
    MAX_CAPTURE_PAYLOAD_BYTES, PairChunkFrameInput, PairChunkInputV2, PairReceiptError,
    VerifiedCaptureArtifactReceiptV2, digest_pair_chunk_input_v2,
};
use crate::{
    MarketDataError, Result,
    archive::{
        ArchivePublisher, TransportKind,
        raw_spool::{LocalRawFrameSpoolFactory, SpooledRawFrame},
        safe_component,
    },
    parquet_store::{self, ParquetVerification},
    schema::{EVENT_SCHEMA_V3_ID, RAW_FRAME_SCHEMA_V2_ID, RAW_JSON_FRAME_SCHEMA_V2_ID},
};

const PRIVATE_PAIR_DIRECTORY: &str = "capture-pair-v2";
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case")]
struct PairIntentV2<'a> {
    schema_version: u32,
    input_identity: &'a str,
    input_chunk_sha256: &'a str,
    state: &'static str,
}

#[derive(Clone, Copy)]
enum CapturePairSourceV2 {
    SyntheticJsonl,
    #[cfg(feature = "offline-capture-synthetic")]
    AlpacaOfflineFixture,
}

impl CapturePairSourceV2 {
    const fn provider(self) -> &'static str {
        match self {
            Self::SyntheticJsonl => "synthetic",
            #[cfg(feature = "offline-capture-synthetic")]
            Self::AlpacaOfflineFixture => "alpaca",
        }
    }

    const fn feed(self) -> &'static str {
        match self {
            Self::SyntheticJsonl => "synthetic",
            #[cfg(feature = "offline-capture-synthetic")]
            Self::AlpacaOfflineFixture => "opra",
        }
    }

    const fn finite_batch_source_kind(self) -> FiniteBatchSourceKindV2 {
        match self {
            Self::SyntheticJsonl => FiniteBatchSourceKindV2::SyntheticReplay,
            #[cfg(feature = "offline-capture-synthetic")]
            Self::AlpacaOfflineFixture => FiniteBatchSourceKindV2::LocalArchive,
        }
    }
}

/// Publishes the fully finalized records owned by the current spool factory as immutable local
/// Parquet pairs. Any intent without a completed receipt is unknown and is never resumed.
impl ArchivePublisher {
    /// Publish one live-process synthetic spool as Core-owned raw-frame V2 and event V3 pairs.
    ///
    /// This method is restricted to the `LocalTest` transport and `synthetic`/`unknown`
    /// provenance. Each immutable artifact is read back and cross-validated before the local
    /// pair receipt is created. The receipt does not attest to provider entitlement or source
    /// completeness. A failed or partial publication leaves an unknown intent and cannot resume.
    pub fn publish_local_synthetic_capture_pair_v2(
        &self,
        spool: &LocalRawFrameSpoolFactory,
        capture_instance_id: RawCaptureInstanceId,
        raw_frames: &[RawMarketFrame],
        events: &[MarketEventEnvelopeV1],
    ) -> Result<()> {
        self.publish_capture_pair_v2(
            spool,
            capture_instance_id,
            raw_frames,
            events,
            CapturePairSourceV2::SyntheticJsonl,
        )
        .map(|_| ())
    }

    #[cfg(feature = "offline-capture-synthetic")]
    pub(crate) fn publish_local_offline_alpaca_fixture_capture_pair_v2(
        &self,
        spool: &LocalRawFrameSpoolFactory,
        capture_instance_id: RawCaptureInstanceId,
        items: &[broker_ports::MarketDataItem],
        raw_frames: &[RawMarketFrame],
        events: &[MarketEventEnvelopeV1],
        fixture_receipt: &alpaca_stream::offline_test_support::OfflineFixtureReceipt,
    ) -> Result<super::offline_fixture::OfflineFixturePairSummaryV1> {
        super::offline_fixture::validate_broker_fixture_receipt(
            fixture_receipt,
            items,
            raw_frames,
            events,
        )?;
        super::offline_fixture::validate_spooled_fixture_frames(
            spool,
            capture_instance_id,
            fixture_receipt,
            raw_frames,
        )?;
        let rollup = self.publish_capture_pair_v2(
            spool,
            capture_instance_id,
            raw_frames,
            events,
            CapturePairSourceV2::AlpacaOfflineFixture,
        )?;
        super::offline_fixture::persist_offline_fixture_receipt(
            &self.state_dir,
            capture_instance_id,
            fixture_receipt,
            &rollup,
        )
    }

    fn publish_capture_pair_v2(
        &self,
        spool: &LocalRawFrameSpoolFactory,
        capture_instance_id: RawCaptureInstanceId,
        raw_frames: &[RawMarketFrame],
        events: &[MarketEventEnvelopeV1],
        source: CapturePairSourceV2,
    ) -> Result<LocalCapturePairRollupV2> {
        if self.transport_kind != TransportKind::LocalTest
            || raw_frames.is_empty()
            || raw_frames.len() > MAX_CAPTURE_FRAMES as usize
            || events.is_empty()
            || events.len() > crate::protocol::DEFAULT_MAX_JSONL_RECORDS
            || events.len()
                > raw_frames
                    .len()
                    .checked_mul(market_contracts::MAX_RAW_FRAME_EVENT_COUNT as usize)
                    .ok_or(MarketDataError::InputLimit)?
        {
            return Err(MarketDataError::PublicationNotAuthorized);
        }

        let capture_instance_hex = hex::encode(capture_instance_id.as_bytes());
        let mut rollup = CapturePairRollupBuilderV2::new(
            &capture_instance_hex,
            source.provider(),
            source.feed(),
            EntitlementState::Unknown,
        )
        .map_err(map_pair_error)?;
        let mut reader = spool
            .open_current_capture_reader(capture_instance_id)
            .map_err(|_| MarketDataError::IncompleteWindow)?;
        let mut temp_files = TemporaryFiles::default();
        let mut raw_offset = 0_usize;
        let mut event_offset = 0_usize;
        let mut total_payload_bytes = 0_u64;

        while let Some(spooled_chunk) = reader
            .next_chunk()
            .map_err(|_| MarketDataError::IncompleteWindow)?
        {
            for frame in &spooled_chunk {
                total_payload_bytes = total_payload_bytes
                    .checked_add(frame.capture.payload().as_bytes().len() as u64)
                    .ok_or(MarketDataError::InputLimit)?;
                if total_payload_bytes > MAX_CAPTURE_PAYLOAD_BYTES {
                    return Err(MarketDataError::InputLimit);
                }
            }
            let mut frame_offset = 0_usize;
            let chunk_raw_start = raw_offset;
            while frame_offset < spooled_chunk.len() {
                let canonical_generation = raw_frames
                    .get(chunk_raw_start + frame_offset)
                    .ok_or(MarketDataError::IncompleteWindow)?
                    .generation;
                let mut chunk_end = frame_offset + 1;
                let mut chunk_event_count =
                    spooled_chunk[frame_offset].finalization.event_count() as usize;
                while chunk_end < spooled_chunk.len() {
                    let same_generation = raw_frames
                        .get(chunk_raw_start + chunk_end)
                        .is_some_and(|frame| frame.generation == canonical_generation);
                    let next_event_count =
                        spooled_chunk[chunk_end].finalization.event_count() as usize;
                    if !same_generation
                        || chunk_event_count
                            .checked_add(next_event_count)
                            .ok_or(MarketDataError::InputLimit)?
                            > crate::protocol::DEFAULT_MAX_JSONL_RECORDS
                    {
                        break;
                    }
                    chunk_event_count += next_event_count;
                    chunk_end += 1;
                }
                let count = chunk_end - frame_offset;
                let spooled = &spooled_chunk[frame_offset..chunk_end];
                let projected = raw_frames
                    .get(chunk_raw_start + frame_offset..chunk_raw_start + chunk_end)
                    .ok_or(MarketDataError::IncompleteWindow)?;
                let event_count = spooled.iter().try_fold(0_usize, |total, frame| {
                    total
                        .checked_add(frame.finalization.event_count() as usize)
                        .ok_or(MarketDataError::InputLimit)
                })?;
                let event_end = event_offset
                    .checked_add(event_count)
                    .ok_or(MarketDataError::InputLimit)?;
                let projected_events = events
                    .get(event_offset..event_end)
                    .ok_or(MarketDataError::IncompleteWindow)?;
                let receipt = self.publish_one_chunk(
                    &capture_instance_hex,
                    spooled,
                    projected,
                    projected_events,
                    source,
                    &mut temp_files,
                )?;
                rollup.add_chunk(&receipt).map_err(map_pair_error)?;
                raw_offset = raw_offset
                    .checked_add(count)
                    .ok_or(MarketDataError::InputLimit)?;
                event_offset = event_end;
                frame_offset = chunk_end;
            }
        }

        if raw_offset != raw_frames.len() || event_offset != events.len() {
            return Err(MarketDataError::IncompleteWindow);
        }
        let summary = rollup.snapshot().map_err(map_pair_error)?;
        let rollup_bytes = summary.to_json_bytes().map_err(map_pair_error)?;
        write_private_capture_rollup(&self.state_dir, &capture_instance_hex, &rollup_bytes)?;
        Ok(summary)
    }

    fn publish_one_chunk(
        &self,
        capture_instance_hex: &str,
        spooled: &[SpooledRawFrame],
        projected: &[RawMarketFrame],
        events: &[MarketEventEnvelopeV1],
        source: CapturePairSourceV2,
        temp_files: &mut TemporaryFiles,
    ) -> Result<LocalCapturePairChunkReceiptV2> {
        let BuiltCaptureChunk {
            capture_instance_id,
            source_generation,
            canonical_generation,
            frame_inputs,
            raw_messagepack,
            raw_json,
            event_rows,
        } = build_capture_chunk(
            capture_instance_hex,
            spooled,
            projected,
            events,
            source.provider(),
            source.feed(),
        )?;
        let pair_input = PairChunkInputV2 {
            capture_instance_id,
            provider: source.provider(),
            feed: source.feed(),
            entitlement: EntitlementState::Unknown,
            source_generation,
            canonical_generation,
            frames: &frame_inputs,
        };
        let digest = digest_pair_chunk_input_v2(&pair_input).map_err(map_pair_error)?;
        if event_rows.is_empty() {
            return Err(MarketDataError::IncompleteWindow);
        }
        if let Some(frames) = raw_messagepack.as_ref() {
            validate_messagepack_capture_chunk_v2(frames, &event_rows)
                .map_err(|_| MarketDataError::Contract)?;
        } else if let Some(frames) = raw_json.as_ref() {
            validate_json_capture_chunk_v2(frames, &event_rows)
                .map_err(|_| MarketDataError::Contract)?;
        } else {
            return Err(MarketDataError::Contract);
        }

        let input_identity = pair_input.input_identity().map_err(map_pair_error)?;
        let input_chunk_sha256 = hex::encode(digest.input_chunk_sha256);
        let manifest_input_sha256 = hex::encode(digest.manifest_input_sha256);
        let raw_schema_id = if raw_json.is_some() {
            RAW_JSON_FRAME_SCHEMA_V2_ID
        } else {
            RAW_FRAME_SCHEMA_V2_ID
        };
        let raw_dataset_id = dataset_id(&input_identity, "raw-v2", source)?;
        let event_dataset_id = dataset_id(&input_identity, "events-v3", source)?;
        let raw_object_name = format!("{raw_dataset_id}.parquet");
        let event_object_name = format!("{event_dataset_id}.parquet");
        let raw_path = self.temp_path(&raw_dataset_id, "capture-pair-raw")?;
        let event_path = self.temp_path(&event_dataset_id, "capture-pair-events")?;
        temp_files.push(raw_path.clone());
        temp_files.push(event_path.clone());
        let raw_local = if let Some(frames) = raw_messagepack.as_ref() {
            parquet_store::write_raw_capture_frames_v2_with_limit(
                &raw_path,
                frames,
                self.limits.max_object_bytes,
            )?
        } else {
            parquet_store::write_raw_json_capture_frames_v2_with_limit(
                &raw_path,
                raw_json.as_deref().ok_or(MarketDataError::Contract)?,
                self.limits.max_object_bytes,
            )?
        };
        let event_local = parquet_store::write_event_v3_with_limit(
            &event_path,
            &event_rows,
            self.limits.max_object_bytes,
        )?;
        if raw_local.schema_id != raw_schema_id || event_local.schema_id != EVENT_SCHEMA_V3_ID {
            return Err(MarketDataError::ParquetSchema);
        }

        let state_directory = open_private_pair_directory(&self.state_dir, true)?;
        preflight_absent(self, &raw_dataset_id, &raw_object_name)?;
        preflight_absent(
            self,
            &raw_dataset_id,
            &format!("{raw_dataset_id}.manifest.json"),
        )?;
        preflight_absent(self, &event_dataset_id, &event_object_name)?;
        preflight_absent(
            self,
            &event_dataset_id,
            &format!("{event_dataset_id}.manifest.json"),
        )?;
        let intent_name = format!(
            "chunk-{}.intent.json",
            hex::encode(Sha256::digest(input_identity.as_bytes()))
        );
        let intent = PairIntentV2 {
            schema_version: 2,
            input_identity: &input_identity,
            input_chunk_sha256: &input_chunk_sha256,
            state: "UNKNOWN_UNLESS_PAIR_RECEIPT_EXISTS",
        };
        write_private_create_only(
            &state_directory,
            &intent_name,
            &serde_json::to_vec(&intent)?,
        )?;

        let raw_expected = CaptureChunkArtifactExpectationV2 {
            role: ArtifactRole::RawFrames,
            dataset_id: &raw_dataset_id,
            object_name: &raw_object_name,
            schema_id: raw_schema_id,
            provider: source.provider(),
            feed: source.feed(),
            entitlement: EntitlementState::Unknown,
            numeric_encoding: numeric_encoding_proto_json(raw_local.source.numeric_encoding)?,
            input_identity: &input_identity,
            manifest_input_sha256: &manifest_input_sha256,
            input_chunk_sha256: &input_chunk_sha256,
            input_payload_bytes: digest.input_payload_bytes,
            input_record_count: u64::from(digest.raw_frame_count),
            output_row_count: u64::from(digest.raw_frame_count),
        };
        let event_expected = CaptureChunkArtifactExpectationV2 {
            role: ArtifactRole::NormalizedEvents,
            dataset_id: &event_dataset_id,
            object_name: &event_object_name,
            schema_id: EVENT_SCHEMA_V3_ID,
            provider: source.provider(),
            feed: source.feed(),
            entitlement: EntitlementState::Unknown,
            numeric_encoding: numeric_encoding_proto_json(event_local.source.numeric_encoding)?,
            input_identity: &input_identity,
            manifest_input_sha256: &manifest_input_sha256,
            input_chunk_sha256: &input_chunk_sha256,
            input_payload_bytes: digest.input_payload_bytes,
            input_record_count: u64::from(digest.raw_frame_count),
            output_row_count: event_rows.len() as u64,
        };
        let raw_contents = if let Some(frames) = raw_messagepack.as_deref() {
            ArtifactContentsV2::MessagePackFrames(frames)
        } else {
            ArtifactContentsV2::JsonFrames(raw_json.as_deref().ok_or(MarketDataError::Contract)?)
        };
        let raw_published = self.publish_artifact(
            &raw_path,
            &raw_local,
            &raw_expected,
            raw_contents,
            source,
            temp_files,
        )?;
        let event_published = self.publish_artifact(
            &event_path,
            &event_local,
            &event_expected,
            ArtifactContentsV2::NormalizedEvents(&event_rows),
            source,
            temp_files,
        )?;

        let correlation = parquet_store::verify_capture_pair_v2(
            &raw_published.readback_path,
            raw_schema_id,
            &event_published.readback_path,
            self.limits.max_object_bytes,
        )?;
        if correlation.raw_frame_rows != u64::from(digest.raw_frame_count)
            || correlation.event_rows != event_rows.len() as u64
        {
            return Err(MarketDataError::Conflict);
        }

        let receipt = LocalCapturePairChunkReceiptV2::new(
            &pair_input,
            u32::try_from(event_rows.len()).map_err(|_| MarketDataError::InputLimit)?,
            raw_published.receipt,
            event_published.receipt,
        )
        .map_err(map_pair_error)?;
        let receipt_bytes = receipt.to_json_bytes().map_err(map_pair_error)?;
        let receipt_name = format!(
            "chunk-{}.receipt.json",
            hex::encode(Sha256::digest(input_identity.as_bytes()))
        );
        write_private_create_only(&state_directory, &receipt_name, &receipt_bytes)?;
        Ok(receipt)
    }

    fn publish_artifact(
        &self,
        local_path: &Path,
        local_verification: &ParquetVerification,
        expected: &CaptureChunkArtifactExpectationV2<'_>,
        contents: ArtifactContentsV2<'_>,
        source: CapturePairSourceV2,
        temp_files: &mut TemporaryFiles,
    ) -> Result<PublishedArtifactV2> {
        let local_hash = super::super::hash_file(local_path, self.limits.max_object_bytes)?;
        let local_numeric_encoding =
            numeric_encoding_proto_json(local_verification.source.numeric_encoding)?;
        if local_verification.footer_rows != expected.output_row_count
            || local_verification.decoded_rows != expected.output_row_count
            || local_verification.schema_id != expected.schema_id
            || local_verification.source.provider != expected.provider
            || local_verification.source.feed != expected.feed
            || local_verification.source.entitlement != expected.entitlement
            || local_numeric_encoding != expected.numeric_encoding
        {
            return Err(MarketDataError::Contract);
        }
        self.transport
            .upload_immutable(local_path, expected.dataset_id, expected.object_name)
            .map_err(|_| MarketDataError::UnknownOutcome)?;
        let object = self
            .transport
            .lookup(expected.dataset_id, expected.object_name)
            .map_err(|_| MarketDataError::UnknownOutcome)?
            .ok_or(MarketDataError::UnknownOutcome)?;
        if object.size_bytes != local_hash.size_bytes {
            return Err(MarketDataError::Conflict);
        }
        let readback_path = self.temp_path(expected.dataset_id, "pair-object-readback")?;
        temp_files.push(readback_path.clone());
        self.transport
            .download_with_limit(
                expected.dataset_id,
                expected.object_name,
                &readback_path,
                local_hash.size_bytes,
            )
            .map_err(|_| MarketDataError::UnknownOutcome)?;
        let readback_hash = super::super::hash_file(&readback_path, self.limits.max_object_bytes)?;
        if readback_hash.content_sha256 != local_hash.content_sha256
            || readback_hash.size_bytes != local_hash.size_bytes
        {
            return Err(MarketDataError::Conflict);
        }
        let parquet = parquet_store::verify_with_limit(
            &readback_path,
            expected.schema_id,
            self.limits.max_object_bytes,
        )?;
        if &parquet != local_verification {
            return Err(MarketDataError::Conflict);
        }

        // Read the raw artifact through the exact Core decoder before binding its storage receipt.
        match contents {
            ArtifactContentsV2::MessagePackFrames(frames)
                if expected.role == ArtifactRole::RawFrames =>
            {
                let actual = parquet_store::read_capture_raw_frames_v2(
                    &readback_path,
                    self.limits.max_object_bytes,
                )?;
                if actual.as_slice() != frames {
                    return Err(MarketDataError::Conflict);
                }
            }
            ArtifactContentsV2::JsonFrames(frames) if expected.role == ArtifactRole::RawFrames => {
                let actual = parquet_store::read_capture_raw_json_frames_v2(
                    &readback_path,
                    self.limits.max_object_bytes,
                )?;
                if actual.as_slice() != frames {
                    return Err(MarketDataError::Conflict);
                }
            }
            ArtifactContentsV2::NormalizedEvents(rows)
                if expected.role == ArtifactRole::NormalizedEvents =>
            {
                let actual = parquet_store::read_capture_event_v3(
                    &readback_path,
                    self.limits.max_object_bytes,
                )?;
                verify_event_rows_readback(&actual, rows, expected.output_row_count)?;
            }
            _ => return Err(MarketDataError::Contract),
        }

        let manifest = make_manifest_v2(
            expected,
            source.finite_batch_source_kind(),
            &object.id,
            local_hash.size_bytes,
            &local_hash.content_sha256,
            &parquet,
        )?;
        let manifest_bytes = dataset_manifest_v2_protojson_bytes(&manifest)
            .map_err(|_| MarketDataError::Contract)?;
        if manifest_bytes.len() as u64 > self.limits.max_manifest_bytes
            || parse_dataset_manifest_v2_json(&manifest_bytes)
                .map_err(|_| MarketDataError::Contract)?
                != manifest
        {
            return Err(MarketDataError::Contract);
        }
        let manifest_name = format!("{}.manifest.json", expected.dataset_id);
        let manifest_path =
            self.write_temp_bytes(expected.dataset_id, "pair-manifest", &manifest_bytes)?;
        temp_files.push(manifest_path.clone());
        let manifest_sha256 = super::super::sha256_bytes(&manifest_bytes);
        self.transport
            .upload_immutable(&manifest_path, expected.dataset_id, &manifest_name)
            .map_err(|_| MarketDataError::UnknownOutcome)?;
        let manifest_object = self
            .transport
            .lookup(expected.dataset_id, &manifest_name)
            .map_err(|_| MarketDataError::UnknownOutcome)?
            .ok_or(MarketDataError::UnknownOutcome)?;
        if manifest_object.size_bytes != manifest_bytes.len() as u64 {
            return Err(MarketDataError::Conflict);
        }
        let manifest_readback_path =
            self.temp_path(expected.dataset_id, "pair-manifest-readback")?;
        temp_files.push(manifest_readback_path.clone());
        self.transport
            .download_with_limit(
                expected.dataset_id,
                &manifest_name,
                &manifest_readback_path,
                self.limits.max_manifest_bytes,
            )
            .map_err(|_| MarketDataError::UnknownOutcome)?;
        let manifest_readback = fs::read(&manifest_readback_path)?;
        if manifest_readback != manifest_bytes
            || super::super::sha256_bytes(&manifest_readback) != manifest_sha256
            || parse_dataset_manifest_v2_json(&manifest_readback)
                .map_err(|_| MarketDataError::Contract)?
                != manifest
        {
            return Err(MarketDataError::Conflict);
        }

        let artifact_receipt = CaptureArtifactReceiptV2::verify_manifest_and_readback(
            &ArtifactReadbackV2 {
                manifest_bytes: &manifest_readback,
                manifest_object_name: &manifest_name,
                manifest_object_id: &manifest_object.id,
                manifest_object_size_bytes: manifest_object.size_bytes,
                parquet_object_id: &object.id,
                parquet_content_sha256: &local_hash.content_sha256,
                transport: DatasetTransportV1::LocalTest,
                parquet: &parquet,
            },
            expected,
        )
        .map_err(map_pair_error)?;
        Ok(PublishedArtifactV2 {
            receipt: artifact_receipt,
            readback_path,
        })
    }
}

fn verify_event_rows_readback(
    actual: &[MarketEventParquetRowV3],
    expected: &[MarketEventParquetRowV3],
    expected_row_count: u64,
) -> Result<()> {
    if actual.len() as u64 != expected_row_count || actual != expected {
        return Err(MarketDataError::Conflict);
    }
    Ok(())
}

struct PublishedArtifactV2 {
    receipt: VerifiedCaptureArtifactReceiptV2,
    readback_path: PathBuf,
}

enum ArtifactContentsV2<'a> {
    MessagePackFrames(&'a [RawFrameStorageRecordV2]),
    JsonFrames(&'a [RawJsonFrameStorageRecordV2]),
    NormalizedEvents(&'a [MarketEventParquetRowV3]),
}

struct BuiltCaptureChunk<'a> {
    capture_instance_id: RawCaptureInstanceId,
    source_generation: u64,
    canonical_generation: u64,
    frame_inputs: Vec<PairChunkFrameInput<'a>>,
    raw_messagepack: Option<Vec<RawFrameStorageRecordV2>>,
    raw_json: Option<Vec<RawJsonFrameStorageRecordV2>>,
    event_rows: Vec<MarketEventParquetRowV3>,
}

#[derive(Default)]
struct TemporaryFiles(Vec<PathBuf>);

impl TemporaryFiles {
    fn push(&mut self, path: PathBuf) {
        self.0.push(path);
    }
}

impl Drop for TemporaryFiles {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = fs::remove_file(path);
        }
    }
}

fn build_capture_chunk<'a>(
    capture_instance_hex: &str,
    spooled: &'a [SpooledRawFrame],
    projected: &'a [RawMarketFrame],
    events: &'a [MarketEventEnvelopeV1],
    expected_provider: &str,
    expected_feed: &str,
) -> Result<BuiltCaptureChunk<'a>> {
    if spooled.is_empty()
        || spooled.len() != projected.len()
        || projected.is_empty()
        || events.is_empty()
        || spooled.len() > market_contracts::MAX_RAW_CAPTURE_CHUNK_FRAMES_V2
    {
        return Err(MarketDataError::IncompleteWindow);
    }
    let first = &spooled[0];
    let capture_instance_id = first.capture.capture_instance_id();
    let source_generation = first.capture.source_generation();
    let canonical_generation = projected[0].generation;
    let raw_capture_id = RawFrameCaptureInstanceIdV2::parse(capture_instance_hex.to_owned())
        .map_err(|_| MarketDataError::Contract)?;
    let wire_encoding = first.capture.wire_encoding();
    let mut raw_messagepack = Vec::new();
    let mut raw_json = Vec::new();
    let mut event_rows = Vec::with_capacity(events.len());
    let mut frame_inputs = Vec::with_capacity(spooled.len());
    let mut event_offset = 0_usize;

    for (index, (spooled_frame, frame)) in spooled.iter().zip(projected).enumerate() {
        let capture = &spooled_frame.capture;
        let finalization = &spooled_frame.finalization;
        let key = capture.capture_key();
        let ack = RawFrameCaptureAck::for_capture(capture);
        let final_ack = RawFrameFinalizationAck::for_finalization(&ack, finalization);
        if final_ack.summary_sha256() != spooled_frame.finalization_summary_sha256
            || capture.capture_instance_id() != capture_instance_id
            || hex::encode(capture.capture_instance_id().as_bytes()) != capture_instance_hex
            || capture.provider() != expected_provider
            || capture.feed() != expected_feed
            || capture.entitlement() != EntitlementState::Unknown
            || capture.wire_encoding() != wire_encoding
            || frame.provider != capture.provider()
            || frame.feed != capture.feed()
            || frame.entitlement != capture.entitlement()
            || frame.capture_key.as_ref() != Some(key)
            || frame.wire_encoding != wire_encoding
            || frame.generation != canonical_generation
            || frame.frame_sequence != capture.frame_sequence()
            || frame.received_timestamp_utc != *capture.received_timestamp_utc()
            || frame.payload.as_bytes() != capture.payload().as_bytes()
            || frame.event_count != finalization.event_count()
            || frame.symbols != finalization.symbols()
            || frame.numeric_encoding != finalization.numeric_encoding()
            || frame.disposition != finalization.disposition()
            || key.source_generation() != source_generation
            || index > 0
                && key.frame_sequence()
                    != spooled[index - 1]
                        .capture
                        .frame_sequence()
                        .checked_add(1)
                        .ok_or(MarketDataError::IncompleteWindow)?
        {
            return Err(MarketDataError::Contract);
        }
        if !matches!(
            frame.disposition,
            RawFrameDisposition::DecodedMarketData | RawFrameDisposition::ControlMessage
        ) {
            return Err(MarketDataError::IncompleteWindow);
        }

        let core_disposition = disposition_v1(frame.disposition)?;
        let source_numeric_encoding = frame.numeric_encoding;
        let symbols_json = serde_json::to_string(&frame.symbols)?;
        let frame_bytes = capture.payload().as_bytes().to_vec();
        match wire_encoding {
            RawFrameWireEncoding::MessagePack => raw_messagepack.push(RawFrameStorageRecordV2 {
                schema_version: 2,
                provider: frame.provider.clone(),
                feed: frame.feed.clone(),
                entitlement: frame.entitlement,
                source_numeric_encoding,
                capture_instance_id: raw_capture_id.clone(),
                source_generation,
                source_frame_sequence: key.frame_sequence(),
                canonical_generation,
                received_timestamp_utc: frame.received_timestamp_utc.clone(),
                frame_sha256: key.frame_sha256().to_owned(),
                frame_bytes,
                event_count: frame.event_count,
                disposition: core_disposition,
                symbols_json,
            }),
            RawFrameWireEncoding::Json => raw_json.push(RawJsonFrameStorageRecordV2 {
                schema_version: 2,
                provider: frame.provider.clone(),
                feed: frame.feed.clone(),
                entitlement: frame.entitlement,
                source_numeric_encoding,
                capture_instance_id: raw_capture_id.clone(),
                source_generation,
                source_frame_sequence: key.frame_sequence(),
                canonical_generation,
                received_timestamp_utc: frame.received_timestamp_utc.clone(),
                frame_sha256: key.frame_sha256().to_owned(),
                frame_bytes,
                event_count: frame.event_count,
                disposition: core_disposition,
                symbols_json,
            }),
            RawFrameWireEncoding::Unknown => return Err(MarketDataError::Contract),
            _ => return Err(MarketDataError::Contract),
        }

        frame_inputs.push(PairChunkFrameInput {
            capture_key: key,
            canonical_generation,
            received_timestamp_utc: capture.received_timestamp_utc(),
            wire_encoding,
            payload: capture.payload().as_bytes(),
            finalization_summary_sha256: &spooled_frame.finalization_summary_sha256,
        });

        let frame_event_end = event_offset
            .checked_add(frame.event_count as usize)
            .ok_or(MarketDataError::InputLimit)?;
        let frame_events = events
            .get(event_offset..frame_event_end)
            .ok_or(MarketDataError::IncompleteWindow)?;
        for (ordinal, event) in frame_events.iter().enumerate() {
            if event.metadata.generation != frame.generation
                || event.metadata.source.provider != frame.provider
                || event.metadata.source.feed != frame.feed
                || event.metadata.source.entitlement != frame.entitlement
                || source_numeric_encoding
                    .is_some_and(|encoding| event.metadata.source.numeric_encoding != encoding)
                || event.metadata.raw_frame_sha256.as_deref() != Some(key.frame_sha256())
                || event.metadata.received_timestamp != frame.received_timestamp_utc
                || !frame
                    .symbols
                    .iter()
                    .any(|symbol| symbol == event_symbol(&event.event))
            {
                return Err(MarketDataError::Contract);
            }
            event_rows.push(MarketEventParquetRowV3 {
                event: event.clone(),
                raw_frame_reference: Some(RawFrameReferenceV3 {
                    raw_frame_capture_instance_id: raw_capture_id.clone(),
                    raw_frame_source_generation: source_generation,
                    raw_frame_generation: canonical_generation,
                    raw_frame_sequence: key.frame_sequence(),
                    raw_frame_event_ordinal: u32::try_from(ordinal + 1)
                        .map_err(|_| MarketDataError::InputLimit)?,
                    raw_frame_event_count: frame.event_count,
                }),
            });
        }
        event_offset = frame_event_end;
    }
    if event_offset != events.len() {
        return Err(MarketDataError::IncompleteWindow);
    }
    let total_payload_bytes = spooled.iter().try_fold(0_usize, |total, frame| {
        total
            .checked_add(frame.capture.payload().as_bytes().len())
            .ok_or(MarketDataError::InputLimit)
    })?;
    let exactly_one_wire_format = raw_messagepack.is_empty() != raw_json.is_empty();
    let mixed_source_generation = raw_messagepack
        .iter()
        .any(|row| row.source_generation != source_generation)
        || raw_json
            .iter()
            .any(|row| row.source_generation != source_generation);
    if !exactly_one_wire_format
        || total_payload_bytes > market_contracts::MAX_RAW_CAPTURE_CHUNK_BYTES_V2
        || mixed_source_generation
    {
        return Err(MarketDataError::InputLimit);
    }
    Ok(BuiltCaptureChunk {
        capture_instance_id,
        source_generation,
        canonical_generation,
        frame_inputs,
        raw_messagepack: (!raw_messagepack.is_empty()).then_some(raw_messagepack),
        raw_json: (!raw_json.is_empty()).then_some(raw_json),
        event_rows,
    })
}

fn make_manifest_v2(
    expected: &CaptureChunkArtifactExpectationV2<'_>,
    finite_batch_source_kind: FiniteBatchSourceKindV2,
    object_id: &str,
    object_size_bytes: u64,
    object_sha256: &str,
    parquet: &ParquetVerification,
) -> Result<DatasetManifestV2> {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let completed = ProtoTimestampV2::parse(&now).map_err(|_| MarketDataError::Contract)?;
    let time_range = parquet
        .time_range
        .as_ref()
        .map(|range| -> Result<DatasetTimeRangeV2> {
            Ok(DatasetTimeRangeV2 {
                start_inclusive: ProtoTimestampV2::parse(range.start_inclusive.as_str())
                    .map_err(|_| MarketDataError::Contract)?,
                end_exclusive: ProtoTimestampV2::parse(range.end_exclusive.as_str())
                    .map_err(|_| MarketDataError::Contract)?,
            })
        })
        .transpose()?;
    let cutoff = time_range
        .as_ref()
        .map_or_else(|| completed.clone(), |range| range.end_exclusive.clone());
    if cutoff > completed {
        return Err(MarketDataError::IncompleteWindow);
    }
    let mut finite = FiniteBatchCompletionV2 {
        source_kind: finite_batch_source_kind,
        input_identity: expected.input_identity.to_owned(),
        input_sha256: expected.manifest_input_sha256.to_owned(),
        input_size_bytes: expected.input_payload_bytes,
        input_record_count: expected.input_record_count,
        consumed_record_count: expected.input_record_count,
        reviewed_policy_sha256: super::capture_pair_policy_sha256(),
        seal_receipt_sha256: String::new(),
        data_cutoff_exclusive: cutoff,
        sealed_at: completed.clone(),
        completed_at: completed,
        page_count: None,
        pages_exhausted: None,
        page_set_sha256: None,
    };
    finite.seal_receipt_sha256 =
        finite_batch_seal_receipt_sha256(&finite).map_err(|_| MarketDataError::Contract)?;
    let manifest = DatasetManifestV2 {
        schema_version: 2,
        dataset_id: expected.dataset_id.to_owned(),
        source: DatasetSourceV2 {
            provider: expected.provider.to_owned(),
            feed: expected.feed.to_owned(),
            entitlement: expected.entitlement,
            numeric_encoding: expected.numeric_encoding,
            source_record_id: parquet.source.source_record_id.clone(),
        },
        symbols: parquet.symbols.clone(),
        time_range,
        source_timestamp_missing_rows: parquet.source_timestamp_missing_rows,
        row_count: parquet.decoded_rows,
        object: DatasetObjectV2 {
            object_name: expected.object_name.to_owned(),
            object_id: Some(object_id.to_owned()),
            size_bytes: object_size_bytes,
            content_sha256: object_sha256.to_owned(),
            parquet_schema_sha256: parquet.schema_sha256.clone(),
            parquet_footer_rows: parquet.footer_rows,
            transport: DatasetTransportV1::LocalTest,
        },
        storage_verification: DatasetStorageVerificationV2 {
            readback_sha256: object_sha256.to_owned(),
            verified_before_publish: true,
        },
        completion_evidence: DatasetCompletionEvidenceV2 {
            finite_batch: Some(finite),
            provider_watermark: None,
            diagnostic_stream: None,
        },
    };
    manifest.validate().map_err(|_| MarketDataError::Contract)?;
    Ok(manifest)
}

fn disposition_v1(value: RawFrameDisposition) -> Result<RawFrameDispositionV1> {
    match value {
        RawFrameDisposition::DecodedMarketData => Ok(RawFrameDispositionV1::MarketData),
        RawFrameDisposition::ControlMessage => Ok(RawFrameDispositionV1::Control),
        RawFrameDisposition::UnknownMessage => Ok(RawFrameDispositionV1::UnknownMessage),
        RawFrameDisposition::ProviderError => Ok(RawFrameDispositionV1::ProviderError),
        RawFrameDisposition::DecodeFailure => Ok(RawFrameDispositionV1::MalformedMessage),
    }
}

fn event_symbol(event: &market_contracts::MarketEventV1) -> &str {
    match event {
        market_contracts::MarketEventV1::StockQuote { symbol, .. }
        | market_contracts::MarketEventV1::StockTrade { symbol, .. }
        | market_contracts::MarketEventV1::OptionQuote { symbol, .. }
        | market_contracts::MarketEventV1::OptionTrade { symbol, .. } => symbol,
    }
}

fn dataset_id(input_identity: &str, role: &str, source: CapturePairSourceV2) -> Result<String> {
    dataset_id_for_identity(input_identity, role, source.provider(), source.feed())
}

pub(super) fn dataset_id_for_identity(
    input_identity: &str,
    role: &str,
    provider: &str,
    feed: &str,
) -> Result<String> {
    let digest = hex::encode(Sha256::digest(input_identity.as_bytes()));
    let (prefix, role) = match (provider, feed) {
        ("synthetic", "synthetic") => ("synthetic-capture-pair-v2", role),
        ("alpaca", "opra") => (
            "synthetic-offline-fixture-alpaca-opra-trade-v1",
            match role {
                "raw-v2" => "r2",
                "events-v3" => "e3",
                _ => return Err(MarketDataError::InvalidInput),
            },
        ),
        _ => return Err(MarketDataError::InvalidInput),
    };
    let value = format!("{prefix}-{digest}-{role}");
    if !safe_component(&value) || value.len() > 128 {
        return Err(MarketDataError::InvalidInput);
    }
    Ok(value)
}

fn preflight_absent(
    publisher: &ArchivePublisher,
    dataset_id: &str,
    object_name: &str,
) -> Result<()> {
    match publisher.transport.lookup(dataset_id, object_name) {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(MarketDataError::Conflict),
        Err(_) => Err(MarketDataError::UnknownOutcome),
    }
}

fn write_private_capture_rollup(
    state_root: &Path,
    capture_id_hex: &str,
    bytes: &[u8],
) -> Result<()> {
    write_private_capture_record(state_root, capture_id_hex, "rollup.json", bytes)
}

pub(super) fn write_private_capture_record(
    state_root: &Path,
    capture_id_hex: &str,
    suffix: &str,
    bytes: &[u8],
) -> Result<()> {
    if capture_id_hex.len() != 32
        || !capture_id_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || suffix.is_empty()
        || suffix.len() > 120
        || !suffix.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'.' || byte == b'-'
        })
    {
        return Err(MarketDataError::InvalidInput);
    }
    let directory = open_private_pair_directory(state_root, true)?;
    write_private_create_only(
        &directory,
        &format!("capture-{capture_id_hex}.{suffix}"),
        bytes,
    )
}

#[cfg(unix)]
pub(super) fn open_private_pair_directory(state_root: &Path, create: bool) -> Result<File> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let parent_metadata = fs::symlink_metadata(state_root)?;
    if !parent_metadata.file_type().is_dir()
        || parent_metadata.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    let parent_path = fs::canonicalize(state_root)?;
    let parent = File::from(
        rustix::fs::open(
            &parent_path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| MarketDataError::PublicationNotAuthorized)?,
    );
    if create {
        match rustix::fs::mkdirat(
            &parent,
            PRIVATE_PAIR_DIRECTORY,
            rustix::fs::Mode::from_raw_mode(PRIVATE_DIRECTORY_MODE),
        ) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(_) => return Err(MarketDataError::PublicationNotAuthorized),
        }
    }
    let directory = File::from(
        rustix::fs::openat(
            &parent,
            PRIVATE_PAIR_DIRECTORY,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| MarketDataError::PublicationNotAuthorized)?,
    );
    let metadata = directory.metadata()?;
    let opened_parent = parent.metadata()?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != PRIVATE_DIRECTORY_MODE
        || opened_parent.dev() != parent_metadata.dev()
        || opened_parent.ino() != parent_metadata.ino()
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    Ok(directory)
}

#[cfg(not(unix))]
pub(super) fn open_private_pair_directory(_state_root: &Path, _create: bool) -> Result<File> {
    Err(MarketDataError::PublicationNotAuthorized)
}

#[cfg(unix)]
fn write_private_create_only(directory: &File, name: &str, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if name.is_empty()
        || name.len() > 160
        || !name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
        })
    {
        return Err(MarketDataError::InvalidInput);
    }
    let fd = rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(PRIVATE_FILE_MODE),
    )
    .map_err(|error| {
        if error == rustix::io::Errno::EXIST {
            MarketDataError::Conflict
        } else {
            MarketDataError::PublicationNotAuthorized
        }
    })?;
    let mut file = File::from(fd);
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != PRIVATE_FILE_MODE
        || metadata.nlink() != 1
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    file.write_all(bytes)?;
    file.sync_all()?;
    directory.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn write_private_create_only(_directory: &File, _name: &str, _bytes: &[u8]) -> Result<()> {
    Err(MarketDataError::PublicationNotAuthorized)
}

pub(super) fn map_pair_error(error: PairReceiptError) -> MarketDataError {
    match error {
        PairReceiptError::CapacityExceeded => MarketDataError::InputLimit,
        PairReceiptError::InvalidInput | PairReceiptError::IdentityChanged => {
            MarketDataError::InvalidInput
        }
        PairReceiptError::InvalidReceipt | PairReceiptError::SequenceGap => {
            MarketDataError::IncompleteWindow
        }
    }
}

pub(super) fn numeric_encoding_proto_json(
    value: NumericEncodingV1,
) -> Result<NumericEncodingProtoJsonV2> {
    match value {
        NumericEncodingV1::DecimalToken => Ok(NumericEncodingProtoJsonV2::DecimalToken),
        NumericEncodingV1::IntegerToken => Ok(NumericEncodingProtoJsonV2::IntegerToken),
        NumericEncodingV1::BinaryFloat64ShortestDecimal => {
            Ok(NumericEncodingProtoJsonV2::BinaryFloat64ShortestDecimal)
        }
        NumericEncodingV1::BinaryFloat32ShortestDecimal => {
            Ok(NumericEncodingProtoJsonV2::BinaryFloat32ShortestDecimal)
        }
        NumericEncodingV1::RawMessagePackBytes => {
            Ok(NumericEncodingProtoJsonV2::RawMessagePackBytes)
        }
        NumericEncodingV1::RawJsonBytes => Ok(NumericEncodingProtoJsonV2::RawJsonBytes),
        NumericEncodingV1::Unspecified => Err(MarketDataError::Contract),
    }
}

#[cfg(test)]
mod tests {
    use super::verify_event_rows_readback;
    use market_contracts::{
        DecimalString, EntitlementState, EventMetadataV1, MarketDataSourceV1,
        MarketEventEnvelopeV1, MarketEventParquetRowV3, MarketEventV1, NumericEncodingV1,
        RawFrameCaptureInstanceIdV2, RawFrameReferenceV3, UtcTimestamp,
    };

    #[test]
    fn event_readback_rejects_changed_price_or_size_with_same_row_count() {
        let expected = vec![event_row("600.25", "1")];
        let changed_price = vec![event_row("600.26", "1")];
        let changed_size = vec![event_row("600.25", "2")];

        assert!(verify_event_rows_readback(&expected, &expected, 1).is_ok());
        assert_eq!(changed_price.len(), expected.len());
        assert_eq!(changed_size.len(), expected.len());
        assert!(verify_event_rows_readback(&changed_price, &expected, 1).is_err());
        assert!(verify_event_rows_readback(&changed_size, &expected, 1).is_err());
    }

    fn event_row(price: &str, size: &str) -> MarketEventParquetRowV3 {
        let source = MarketDataSourceV1::new(
            "synthetic",
            "synthetic",
            EntitlementState::Unknown,
            NumericEncodingV1::DecimalToken,
            None,
        )
        .unwrap();
        let event = MarketEventEnvelopeV1 {
            metadata: EventMetadataV1 {
                schema_version: 1,
                source,
                generation: 41,
                sequence: 1,
                raw_frame_sha256: Some("ab".repeat(32)),
                source_timestamp: None,
                received_timestamp: UtcTimestamp::parse("2026-10-08T12:00:00Z").unwrap(),
            },
            event: MarketEventV1::StockTrade {
                symbol: "QQQ".to_owned(),
                price: DecimalString::new(price).unwrap(),
                size: DecimalString::new(size).unwrap(),
            },
        };
        MarketEventParquetRowV3 {
            event,
            raw_frame_reference: Some(RawFrameReferenceV3 {
                raw_frame_capture_instance_id: RawFrameCaptureInstanceIdV2::parse(
                    "00112233445546778899aabbccddeeff",
                )
                .unwrap(),
                raw_frame_source_generation: 7,
                raw_frame_generation: 41,
                raw_frame_sequence: 1,
                raw_frame_event_ordinal: 1,
                raw_frame_event_count: 1,
            }),
        }
    }
}
