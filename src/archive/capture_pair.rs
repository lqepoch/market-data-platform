//! Local-only immutable publication for an exact raw-frame and normalized-event pair.

use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    path::Path,
};

use fs2::FileExt;
use market_contracts::{DatasetManifestV1, NumericEncodingV1, RawFrameDispositionV1};
use serde::{Deserialize, Serialize};

use super::{
    ArchivePublisher, ArchiveRequest, PublicationPurpose, TransportKind, safe_component,
    sha256_bytes, validate_request,
};
use crate::{
    MarketDataError, Result,
    error::StorageFailure,
    parquet_store::{self, ParquetVerification},
    schema::{EVENT_SCHEMA_V2_ID, RAW_FRAME_SCHEMA_ID},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalDiagnosticCapturePairReceiptV1 {
    pub schema_version: u32,
    pub capture_id: String,
    pub evidence_scope: String,
    pub provider_completeness: String,
    pub raw_frames: CaptureArtifactReceiptV1,
    pub normalized_events: CaptureArtifactReceiptV1,
    #[serde(with = "market_contracts::wire_u64")]
    pub source_generation: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub first_frame_sequence: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub last_frame_sequence: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub raw_frame_rows: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub normalized_event_rows: u64,
    pub raw_event_references_verified: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CaptureArtifactReceiptV1 {
    pub dataset_id: String,
    pub object_name: String,
    pub object_id: String,
    pub manifest_sha256: String,
    pub content_sha256: String,
    pub parquet_schema_sha256: String,
    #[serde(with = "market_contracts::wire_u64")]
    pub size_bytes: u64,
    #[serde(with = "market_contracts::wire_u64")]
    pub row_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PairPublicationPhase {
    InFlight,
    RawCommitted,
    EventCommitted,
    Unknown,
    Committed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PairPublicationStateV1 {
    schema_version: u32,
    capture_id: String,
    intent_sha256: String,
    phase: PairPublicationPhase,
    raw_manifest_sha256: Option<String>,
    event_manifest_sha256: Option<String>,
}

impl ArchivePublisher {
    /// Publish a sealed, local-only diagnostic pair after exact frame/event correlation passes.
    ///
    /// The receipt means only that the two finite local objects were linked and read-back verified.
    /// It does not attest that a provider stream reached EOF, nor does it attest entitlement.
    ///
    /// This path decodes Parquet inline and therefore accepts only MDP-owned producer files in the
    /// configured staging directory. Do not pass operator-supplied or remotely downloaded files;
    /// external inputs must use the CLI worker boundary. Any future CLI capture-pair command must
    /// add a worker operation for pair correlation before calling the publication logic.
    pub fn publish_local_diagnostic_capture_pair(
        &self,
        capture_id: &str,
        raw_request: &ArchiveRequest,
        event_request: &ArchiveRequest,
    ) -> Result<LocalDiagnosticCapturePairReceiptV1> {
        if self.transport_kind != TransportKind::LocalTest
            || !valid_capture_instance_id(capture_id)
            || raw_request.dataset_id != format!("{capture_id}-raw")
            || event_request.dataset_id != format!("{capture_id}-events")
            || raw_request.purpose != PublicationPurpose::Raw
            || event_request.purpose != PublicationPurpose::Diagnostic
            || raw_request.schema_id != RAW_FRAME_SCHEMA_ID
            || event_request.schema_id != EVENT_SCHEMA_V2_ID
            || !raw_request.input_eof
            || !event_request.input_eof
            || raw_request.source_pages_exhausted.is_some()
            || event_request.source_pages_exhausted.is_some()
        {
            return Err(MarketDataError::PublicationNotAuthorized);
        }

        validate_request(raw_request, TransportKind::LocalTest, true)?;
        validate_request(event_request, TransportKind::LocalTest, true)?;

        let pair_lock_path = self.state_path(capture_id, "capture-pair.lock")?;
        let pair_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(pair_lock_path)?;
        pair_lock
            .try_lock_exclusive()
            .map_err(|_| MarketDataError::LockHeld)?;

        validate_existing_pair_receipt(self, capture_id)?;
        let raw_size = fs::metadata(&raw_request.parquet_path)?.len();
        let event_size = fs::metadata(&event_request.parquet_path)?.len();
        self.validate_staging_budget(&raw_request.parquet_path, raw_size)?;
        self.validate_staging_budget(&event_request.parquet_path, event_size)?;

        let raw_frames = parquet_store::read_capture_raw_frames(
            &raw_request.parquet_path,
            self.limits.max_object_bytes,
        )?;
        validate_raw_capture_sequence(&raw_frames)?;
        let raw_verification = parquet_store::verify_with_limit(
            &raw_request.parquet_path,
            RAW_FRAME_SCHEMA_ID,
            self.limits.max_object_bytes,
        )?;
        let event_verification = parquet_store::verify_with_limit(
            &event_request.parquet_path,
            EVENT_SCHEMA_V2_ID,
            self.limits.max_object_bytes,
        )?;
        validate_request_facts(raw_request, &raw_verification)?;
        validate_request_facts(event_request, &event_verification)?;
        if raw_verification.symbols.is_empty()
            || !same_provider_identity(&raw_verification, &event_verification)
            || raw_frames.iter().any(|frame| {
                !matches!(
                    frame.disposition,
                    RawFrameDispositionV1::MarketData
                        | RawFrameDispositionV1::Control
                        | RawFrameDispositionV1::UnknownMessage
                )
            })
        {
            return Err(MarketDataError::IncompleteWindow);
        }

        let correlation = parquet_store::verify_event_v2_against_raw(
            &raw_request.parquet_path,
            &event_request.parquet_path,
            self.limits.max_object_bytes,
        )?;
        if correlation.raw_frame_rows != raw_verification.footer_rows
            || correlation.event_rows != event_verification.footer_rows
        {
            return Err(MarketDataError::IncompleteWindow);
        }

        let raw_file = super::hash_file(&raw_request.parquet_path, self.limits.max_object_bytes)?;
        let event_file =
            super::hash_file(&event_request.parquet_path, self.limits.max_object_bytes)?;
        let intent_sha256 = pair_intent_fingerprint(
            capture_id,
            raw_request,
            &raw_file,
            event_request,
            &event_file,
        )?;
        let state_path = self.state_path(capture_id, "capture-pair.state.json")?;
        let mut state = match load_pair_state(self, capture_id)? {
            Some(existing) if existing.intent_sha256 == intent_sha256 => existing,
            Some(_) => return Err(MarketDataError::Conflict),
            None => PairPublicationStateV1 {
                schema_version: 1,
                capture_id: capture_id.to_owned(),
                intent_sha256,
                phase: PairPublicationPhase::InFlight,
                raw_manifest_sha256: None,
                event_manifest_sha256: None,
            },
        };
        state.phase = PairPublicationPhase::InFlight;
        persist_pair_state(self, &state_path, &state)?;

        // Raw-frame manifests are accepted only through this paired path. A raw-only object has
        // no correlation receipt and cannot be mistaken for a completed normalized capture.
        let raw_manifest = match self.publish_capture_component(raw_request) {
            Ok(manifest) => manifest,
            Err(error) => {
                state.phase = PairPublicationPhase::Unknown;
                let _ = persist_pair_state(self, &state_path, &state);
                return Err(error);
            }
        };
        let raw_artifact = artifact_receipt(self, &raw_manifest)?;
        state.phase = PairPublicationPhase::RawCommitted;
        state.raw_manifest_sha256 = Some(raw_artifact.manifest_sha256.clone());
        persist_pair_state(self, &state_path, &state)?;

        let event_manifest = match self.publish_capture_component(event_request) {
            Ok(manifest) => manifest,
            Err(error) => {
                state.phase = PairPublicationPhase::Unknown;
                let _ = persist_pair_state(self, &state_path, &state);
                return Err(error);
            }
        };
        let event_artifact = artifact_receipt(self, &event_manifest)?;
        state.phase = PairPublicationPhase::EventCommitted;
        state.event_manifest_sha256 = Some(event_artifact.manifest_sha256.clone());
        persist_pair_state(self, &state_path, &state)?;
        let receipt = LocalDiagnosticCapturePairReceiptV1 {
            schema_version: 1,
            capture_id: capture_id.to_owned(),
            evidence_scope: "local_parquet_pair_verified".to_owned(),
            provider_completeness: "not_asserted".to_owned(),
            raw_frames: raw_artifact,
            normalized_events: event_artifact,
            source_generation: raw_frames[0].generation,
            first_frame_sequence: raw_frames[0].frame_sequence,
            last_frame_sequence: raw_frames
                .last()
                .ok_or(MarketDataError::IncompleteWindow)?
                .frame_sequence,
            raw_frame_rows: correlation.raw_frame_rows,
            normalized_event_rows: correlation.event_rows,
            raw_event_references_verified: true,
        };
        persist_pair_receipt(self, capture_id, &receipt)?;
        state.phase = PairPublicationPhase::Committed;
        persist_pair_state(self, &state_path, &state)?;
        Ok(receipt)
    }
}

fn pair_intent_fingerprint(
    capture_id: &str,
    raw_request: &ArchiveRequest,
    raw_file: &super::FileHash,
    event_request: &ArchiveRequest,
    event_file: &super::FileHash,
) -> Result<String> {
    let identity = (
        capture_id,
        super::intent_fingerprint(raw_request)?,
        &raw_file.content_sha256,
        raw_file.size_bytes,
        super::intent_fingerprint(event_request)?,
        &event_file.content_sha256,
        event_file.size_bytes,
    );
    Ok(sha256_bytes(&serde_json::to_vec(&identity)?))
}

fn load_pair_state(
    publisher: &ArchivePublisher,
    capture_id: &str,
) -> Result<Option<PairPublicationStateV1>> {
    let path = publisher.state_path(capture_id, "capture-pair.state.json")?;
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(MarketDataError::Io(error)),
    };
    if !metadata.file_type().is_file() {
        return Err(MarketDataError::InvalidInput);
    }
    if metadata.len() > publisher.limits.max_manifest_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let bytes = read_bounded(&path, publisher.limits.max_manifest_bytes)?;
    let state: PairPublicationStateV1 = serde_json::from_slice(&bytes)
        .map_err(|_| MarketDataError::Storage(StorageFailure::ReceiptFailed))?;
    if state.schema_version != 1
        || state.capture_id != capture_id
        || !super::valid_sha256(&state.intent_sha256)
        || state
            .raw_manifest_sha256
            .as_deref()
            .is_some_and(|value| !super::valid_sha256(value))
        || state
            .event_manifest_sha256
            .as_deref()
            .is_some_and(|value| !super::valid_sha256(value))
    {
        return Err(MarketDataError::Storage(StorageFailure::ReceiptFailed));
    }
    Ok(Some(state))
}

fn persist_pair_state(
    publisher: &ArchivePublisher,
    path: &Path,
    state: &PairPublicationStateV1,
) -> Result<()> {
    let bytes = serde_json::to_vec(state)?;
    if bytes.len() as u64 > publisher.limits.max_manifest_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let temporary = publisher.write_temp_bytes(&state.capture_id, "capture-pair-state", &bytes)?;
    fs::rename(temporary, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn valid_capture_instance_id(value: &str) -> bool {
    if !safe_component(value) || value.len() != 36 {
        return false;
    }
    let bytes = value.as_bytes();
    for position in [8, 13, 18, 23] {
        if bytes[position] != b'-' {
            return false;
        }
    }
    for (index, byte) in bytes.iter().enumerate() {
        if [8, 13, 18, 23].contains(&index) {
            continue;
        }
        if !byte.is_ascii_digit() && !(b'a'..=b'f').contains(byte) {
            return false;
        }
    }
    bytes[14] == b'4' && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
}

fn validate_raw_capture_sequence(
    frames: &[market_contracts::RawFrameStorageRecordV1],
) -> Result<()> {
    if frames.is_empty() {
        return Err(MarketDataError::IncompleteWindow);
    }
    let generation = frames[0].generation;
    for pair in frames.windows(2) {
        let expected = pair[0]
            .frame_sequence
            .checked_add(1)
            .ok_or(MarketDataError::InputLimit)?;
        if pair[1].generation != generation || pair[1].frame_sequence != expected {
            return Err(MarketDataError::IncompleteWindow);
        }
    }
    Ok(())
}

fn validate_request_facts(
    request: &ArchiveRequest,
    verification: &ParquetVerification,
) -> Result<()> {
    let mut source = request.source.clone();
    source.source_record_id = None;
    if verification.schema_id != request.schema_id
        || verification.footer_rows != request.row_count
        || verification.source != source
        || verification.symbols != request.symbols
        || verification.time_range != request.time_range
        || verification.source_timestamp_missing_rows != request.source_timestamp_missing_rows
    {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(())
}

fn same_provider_identity(raw: &ParquetVerification, events: &ParquetVerification) -> bool {
    raw.source.provider == events.source.provider
        && raw.source.feed == events.source.feed
        && raw.source.entitlement == events.source.entitlement
        && raw.source.numeric_encoding == NumericEncodingV1::RawMessagePackBytes
        && events.source.numeric_encoding != NumericEncodingV1::RawMessagePackBytes
}

fn artifact_receipt(
    publisher: &ArchivePublisher,
    manifest: &DatasetManifestV1,
) -> Result<CaptureArtifactReceiptV1> {
    let path = publisher.state_path(&manifest.dataset_id, "manifest.json")?;
    let bytes = read_bounded(&path, publisher.limits.max_manifest_bytes)?;
    let persisted: DatasetManifestV1 = serde_json::from_slice(&bytes)?;
    if &persisted != manifest {
        return Err(MarketDataError::Conflict);
    }
    Ok(CaptureArtifactReceiptV1 {
        dataset_id: manifest.dataset_id.clone(),
        object_name: manifest.object.object_name.clone(),
        object_id: manifest
            .object
            .object_id
            .clone()
            .ok_or(MarketDataError::Contract)?,
        manifest_sha256: sha256_bytes(&bytes),
        content_sha256: manifest.object.content_sha256.clone(),
        parquet_schema_sha256: manifest.object.parquet_schema_sha256.clone(),
        size_bytes: manifest.object.size_bytes,
        row_count: manifest.object.parquet_footer_rows,
    })
}

fn persist_pair_receipt(
    publisher: &ArchivePublisher,
    capture_id: &str,
    receipt: &LocalDiagnosticCapturePairReceiptV1,
) -> Result<()> {
    let path = publisher.state_path(capture_id, "capture-pair.json")?;
    let bytes = serde_json::to_vec(receipt)?;
    if bytes.len() as u64 > publisher.limits.max_manifest_bytes {
        return Err(MarketDataError::InputLimit);
    }
    match fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(metadata) if metadata.len() > publisher.limits.max_manifest_bytes => {
            return Err(MarketDataError::InputLimit);
        }
        Ok(_) => {
            let existing = read_bounded(&path, publisher.limits.max_manifest_bytes)?;
            if existing == bytes {
                return Ok(());
            }
            return Err(MarketDataError::Conflict);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(MarketDataError::Io(error)),
    }
    let temporary = publisher.write_temp_bytes(capture_id, "capture-pair", &bytes)?;
    match fs::hard_link(&temporary, &path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_bounded(&path, publisher.limits.max_manifest_bytes)?;
            if existing != bytes {
                return Err(MarketDataError::Conflict);
            }
        }
        Err(_) => return Err(MarketDataError::InvalidInput),
    }
    fs::remove_file(temporary)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn validate_existing_pair_receipt(publisher: &ArchivePublisher, capture_id: &str) -> Result<()> {
    let path = publisher.state_path(capture_id, "capture-pair.json")?;
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(MarketDataError::Io(error)),
    };
    if !metadata.file_type().is_file() {
        return Err(MarketDataError::InvalidInput);
    }
    if metadata.len() > publisher.limits.max_manifest_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let bytes = read_bounded(&path, publisher.limits.max_manifest_bytes)?;
    let receipt: LocalDiagnosticCapturePairReceiptV1 = serde_json::from_slice(&bytes)
        .map_err(|_| MarketDataError::Storage(StorageFailure::ReceiptFailed))?;
    if receipt.schema_version != 1
        || receipt.capture_id != capture_id
        || receipt.evidence_scope != "local_parquet_pair_verified"
        || receipt.provider_completeness != "not_asserted"
        || !receipt.raw_event_references_verified
        || receipt.raw_frames.dataset_id != format!("{capture_id}-raw")
        || receipt.normalized_events.dataset_id != format!("{capture_id}-events")
    {
        return Err(MarketDataError::Storage(StorageFailure::ReceiptFailed));
    }
    Ok(())
}

fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(MarketDataError::InputLimit);
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| MarketDataError::InputLimit)?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)?
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > maximum {
        return Err(MarketDataError::InputLimit);
    }
    Ok(bytes)
}
