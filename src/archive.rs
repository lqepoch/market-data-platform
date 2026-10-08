//! Single-writer immutable archive publication with durable reconciliation receipts.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

use fs2::FileExt;
use market_contracts::{
    DatasetCompletionEvidenceV1, DatasetManifestV1, DatasetObjectV1, DatasetTimeRangeV1,
    DatasetTransportV1, MarketDataSourceV1,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};

use crate::{
    MarketDataError, Result,
    config::DriveConfig,
    error::StorageFailure,
    parquet_store::{self, ParquetVerification},
    queue::BackgroundWorkerPermit,
    schema::{EVENT_SCHEMA_ID, MINUTE_BAR_SCHEMA_ID},
    storage::{LocalTestTransport, ObjectTransport, RcloneDriveTransport, RemoteObject},
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
pub const DEFAULT_UPLOAD_QUEUE_CAPACITY: usize = 4;
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_MANIFEST_BYTES: u64 = 64 * 1024;
pub const DEFAULT_MAX_STAGING_BYTES: u64 = 16 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    LocalTest,
    RcloneGoogleDrive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationPurpose {
    Curated,
    Diagnostic,
}

#[derive(Clone, Debug)]
pub struct ArchiveLimits {
    pub max_object_bytes: u64,
    pub max_manifest_bytes: u64,
    pub max_staging_bytes: u64,
    pub upload_queue_capacity: usize,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            max_manifest_bytes: DEFAULT_MAX_MANIFEST_BYTES,
            max_staging_bytes: DEFAULT_MAX_STAGING_BYTES,
            upload_queue_capacity: DEFAULT_UPLOAD_QUEUE_CAPACITY,
        }
    }
}

impl ArchiveLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_object_bytes == 0
            || self.max_manifest_bytes == 0
            || self.max_staging_bytes < self.max_object_bytes
            || self.upload_queue_capacity == 0
            || self.upload_queue_capacity > 64
        {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ArchivePublisher {
    transport: Arc<dyn ObjectTransport>,
    transport_kind: TransportKind,
    state_dir: PathBuf,
    staging_dir: PathBuf,
    limits: ArchiveLimits,
}

impl fmt::Debug for ArchivePublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArchivePublisher")
            .field("transport", &self.transport_kind)
            .field("state_dir", &"redacted")
            .field("staging_dir", &"redacted")
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct ArchiveRequest {
    pub dataset_id: String,
    pub object_name: String,
    pub schema_id: String,
    pub purpose: PublicationPurpose,
    pub source: MarketDataSourceV1,
    pub symbols: Vec<String>,
    pub time_range: Option<DatasetTimeRangeV1>,
    pub source_timestamp_missing_rows: u64,
    pub row_count: u64,
    pub source_pages_exhausted: Option<bool>,
    pub input_eof: bool,
    pub parquet_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Receipt {
    version: u32,
    dataset_id: String,
    object_name: String,
    content_sha256: String,
    schema_sha256: String,
    size_bytes: u64,
    row_count: u64,
    purpose: PublicationPurpose,
    intent_sha256: String,
    phase: ReceiptPhase,
    manifest_sha256: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReceiptPhase {
    ObjectInFlight,
    ObjectUnknown,
    ObjectVerified,
    ManifestInFlight,
    ManifestUnknown,
    Committed,
}

impl ArchivePublisher {
    pub fn local_test(
        transport: LocalTestTransport,
        state_dir: impl Into<PathBuf>,
        staging_dir: impl Into<PathBuf>,
        limits: ArchiveLimits,
    ) -> Result<Self> {
        Self::new(
            Arc::new(transport),
            TransportKind::LocalTest,
            state_dir.into(),
            staging_dir.into(),
            limits,
        )
    }

    pub(crate) fn rclone_drive(
        config: DriveConfig,
        state_dir: impl Into<PathBuf>,
        staging_dir: impl Into<PathBuf>,
        limits: ArchiveLimits,
    ) -> Result<Self> {
        let transport = RcloneDriveTransport::new(config)?;
        Self::new(
            Arc::new(transport),
            TransportKind::RcloneGoogleDrive,
            state_dir.into(),
            staging_dir.into(),
            limits,
        )
    }

    fn new(
        transport: Arc<dyn ObjectTransport>,
        transport_kind: TransportKind,
        state_dir: PathBuf,
        staging_dir: PathBuf,
        limits: ArchiveLimits,
    ) -> Result<Self> {
        limits.validate()?;
        fs::create_dir_all(&state_dir)?;
        fs::create_dir_all(&staging_dir)?;
        Ok(Self {
            transport,
            transport_kind,
            state_dir,
            staging_dir,
            limits,
        })
    }

    pub(crate) fn publish(&self, request: &ArchiveRequest) -> Result<DatasetManifestV1> {
        validate_request(request, self.transport_kind)?;
        let lock_path = self.state_path(&request.dataset_id, "lock")?;
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| MarketDataError::LockHeld)?;

        self.validate_staging_budget(&request.parquet_path)?;
        let local = hash_file(&request.parquet_path, self.limits.max_object_bytes)?;
        let parquet = parquet_store::verify(&request.parquet_path, &request.schema_id)?;
        let mut request_source = request.source.clone();
        request_source.source_record_id = None;
        if parquet.footer_rows != request.row_count
            || parquet.source != request_source
            || parquet.symbols != request.symbols
            || parquet.time_range != request.time_range
            || parquet.source_timestamp_missing_rows != request.source_timestamp_missing_rows
        {
            return Err(MarketDataError::ParquetSchema);
        }
        let placeholder_id = match self.transport_kind {
            TransportKind::LocalTest => "local-test:preflight".to_owned(),
            TransportKind::RcloneGoogleDrive => "drive-preflight".to_owned(),
        };
        let _ = make_manifest(
            request,
            &local,
            &parquet,
            &RemoteObject {
                id: placeholder_id,
                size_bytes: local.size_bytes,
                md5: None,
            },
            self.transport_kind,
        )?;
        let manifest_name = format!("{}.manifest.json", request.dataset_id);
        let intent_sha256 = intent_fingerprint(request)?;
        let receipt_path = self.state_path(&request.dataset_id, "receipt.json")?;
        let (mut receipt, fresh_receipt) = match self.load_receipt(&receipt_path)? {
            Some(existing) => {
                if !same_intent(&existing, request, &local, &parquet, &intent_sha256) {
                    return Err(MarketDataError::Conflict);
                }
                (existing, false)
            }
            None => (
                Receipt {
                    version: 1,
                    dataset_id: request.dataset_id.clone(),
                    object_name: request.object_name.clone(),
                    content_sha256: local.content_sha256.clone(),
                    schema_sha256: parquet.schema_sha256.clone(),
                    size_bytes: local.size_bytes,
                    row_count: request.row_count,
                    purpose: request.purpose,
                    intent_sha256,
                    phase: ReceiptPhase::ObjectInFlight,
                    manifest_sha256: None,
                },
                true,
            ),
        };

        if receipt.phase == ReceiptPhase::Committed {
            return self.load_committed_manifest(request, &receipt);
        }

        let remote_object = match self
            .transport
            .lookup(&request.dataset_id, &request.object_name)
        {
            Ok(Some(object)) => object,
            Ok(None) if fresh_receipt => {
                // Fresh publication: a successful preflight absence is followed by a durable
                // receipt before the one create attempt. Restarts never repeat this create blindly.
                self.persist_receipt(&receipt_path, &receipt)?;
                match self.transport.upload_immutable(
                    &request.parquet_path,
                    &request.dataset_id,
                    &request.object_name,
                ) {
                    Ok(()) => {}
                    Err(_) => {
                        receipt.phase = ReceiptPhase::ObjectUnknown;
                        self.persist_receipt(&receipt_path, &receipt)?;
                        return Err(MarketDataError::UnknownOutcome);
                    }
                }
                match self
                    .transport
                    .lookup(&request.dataset_id, &request.object_name)
                {
                    Ok(Some(object)) => object,
                    Ok(None) | Err(_) => {
                        receipt.phase = ReceiptPhase::ObjectUnknown;
                        self.persist_receipt(&receipt_path, &receipt)?;
                        return Err(MarketDataError::UnknownOutcome);
                    }
                }
            }
            Ok(None) => return Err(MarketDataError::UnknownOutcome),
            Err(_) => return Err(MarketDataError::UnknownOutcome),
        };
        self.verify_remote_object(request, &local, &remote_object)?;
        if !matches!(
            receipt.phase,
            ReceiptPhase::ManifestInFlight | ReceiptPhase::ManifestUnknown
        ) {
            receipt.phase = ReceiptPhase::ObjectVerified;
            self.persist_receipt(&receipt_path, &receipt)?;
        }

        let manifest = make_manifest(
            request,
            &local,
            &parquet,
            &remote_object,
            self.transport_kind,
        )?;
        let manifest_bytes = serde_json::to_vec(&manifest)?;
        if manifest_bytes.len() as u64 > self.limits.max_manifest_bytes {
            return Err(MarketDataError::InputLimit);
        }
        let manifest_sha = sha256_bytes(&manifest_bytes);
        let manifest_local_path =
            self.write_temp_bytes(&request.dataset_id, "manifest", &manifest_bytes)?;
        let remote_manifest = match self.transport.lookup(&request.dataset_id, &manifest_name) {
            Ok(Some(object)) => Some(object),
            Ok(None) if matches!(receipt.phase, ReceiptPhase::ObjectVerified) => None,
            Ok(None) => return Err(MarketDataError::UnknownOutcome),
            Err(_) => return Err(MarketDataError::UnknownOutcome),
        };
        if let Some(remote_manifest) = remote_manifest {
            self.verify_remote_bytes(
                &request.dataset_id,
                &manifest_name,
                &remote_manifest,
                &manifest_sha,
                manifest_bytes.len() as u64,
            )?;
        } else {
            receipt.phase = ReceiptPhase::ManifestInFlight;
            receipt.manifest_sha256 = Some(manifest_sha.clone());
            self.persist_receipt(&receipt_path, &receipt)?;
            match self.transport.upload_immutable(
                &manifest_local_path,
                &request.dataset_id,
                &manifest_name,
            ) {
                Ok(()) => {}
                Err(_) => {
                    receipt.phase = ReceiptPhase::ManifestUnknown;
                    self.persist_receipt(&receipt_path, &receipt)?;
                    return Err(MarketDataError::UnknownOutcome);
                }
            }
            let found = match self.transport.lookup(&request.dataset_id, &manifest_name) {
                Ok(Some(object)) => object,
                Ok(None) | Err(_) => {
                    receipt.phase = ReceiptPhase::ManifestUnknown;
                    self.persist_receipt(&receipt_path, &receipt)?;
                    return Err(MarketDataError::UnknownOutcome);
                }
            };
            self.verify_remote_bytes(
                &request.dataset_id,
                &manifest_name,
                &found,
                &manifest_sha,
                manifest_bytes.len() as u64,
            )?;
        }
        self.publish_local_manifest(&request.dataset_id, &manifest_bytes)?;
        receipt.phase = ReceiptPhase::Committed;
        receipt.manifest_sha256 = Some(manifest_sha);
        self.persist_receipt(&receipt_path, &receipt)?;
        let _ = fs::remove_file(&manifest_local_path);
        Ok(manifest)
    }

    fn verify_remote_object(
        &self,
        request: &ArchiveRequest,
        local: &FileHash,
        remote: &RemoteObject,
    ) -> Result<()> {
        if remote.size_bytes != local.size_bytes {
            return Err(MarketDataError::Conflict);
        }
        let downloaded = self.temp_path(&request.dataset_id, "readback")?;
        match self
            .transport
            .download(&request.dataset_id, &request.object_name, &downloaded)
        {
            Ok(()) => {}
            Err(_) => return Err(MarketDataError::UnknownOutcome),
        }
        let readback = hash_file(&downloaded, self.limits.max_object_bytes)?;
        if readback.size_bytes != local.size_bytes
            || readback.content_sha256 != local.content_sha256
        {
            return Err(MarketDataError::Conflict);
        }
        fs::remove_file(downloaded)?;
        Ok(())
    }

    fn verify_remote_bytes(
        &self,
        dataset_id: &str,
        object_name: &str,
        remote: &RemoteObject,
        expected_sha256: &str,
        expected_size: u64,
    ) -> Result<()> {
        if remote.size_bytes != expected_size {
            return Err(MarketDataError::Conflict);
        }
        let downloaded = self.temp_path(dataset_id, "manifest-readback")?;
        match self
            .transport
            .download(dataset_id, object_name, &downloaded)
        {
            Ok(()) => {}
            Err(_) => return Err(MarketDataError::UnknownOutcome),
        }
        let hash = hash_file(&downloaded, self.limits.max_manifest_bytes)?;
        if hash.size_bytes != expected_size || hash.content_sha256 != expected_sha256 {
            return Err(MarketDataError::Conflict);
        }
        fs::remove_file(downloaded)?;
        Ok(())
    }

    fn publish_local_manifest(&self, dataset_id: &str, bytes: &[u8]) -> Result<()> {
        let path = self.state_path(dataset_id, "manifest.json")?;
        if path.exists() {
            let existing = fs::read(&path)?;
            if existing == bytes {
                return Ok(());
            }
            return Err(MarketDataError::Conflict);
        }
        let temp = self.write_temp_bytes(dataset_id, "manifest-local", bytes)?;
        fs::hard_link(&temp, &path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                MarketDataError::Conflict
            } else {
                MarketDataError::Io(std::io::Error::other("manifest commit failed"))
            }
        })?;
        fs::remove_file(temp)?;
        File::open(&self.state_dir)?.sync_all()?;
        Ok(())
    }

    fn load_committed_manifest(
        &self,
        request: &ArchiveRequest,
        receipt: &Receipt,
    ) -> Result<DatasetManifestV1> {
        let path = self.state_path(&request.dataset_id, "manifest.json")?;
        let bytes = fs::read(path)?;
        if sha256_bytes(&bytes) != receipt.manifest_sha256.as_deref().unwrap_or_default() {
            return Err(MarketDataError::Conflict);
        }
        let manifest: DatasetManifestV1 = serde_json::from_slice(&bytes)?;
        manifest
            .validate()
            .map_err(|_| MarketDataError::Storage(StorageFailure::InvalidManifest))?;
        Ok(manifest)
    }

    fn load_receipt(&self, path: &Path) -> Result<Option<Receipt>> {
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        let receipt: Receipt = serde_json::from_slice(&bytes)?;
        if receipt.version != 1
            || !safe_component(&receipt.dataset_id)
            || !safe_object_name(&receipt.object_name)
            || !valid_sha256(&receipt.content_sha256)
            || !valid_sha256(&receipt.schema_sha256)
            || !valid_sha256(&receipt.intent_sha256)
            || receipt
                .manifest_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
        {
            return Err(MarketDataError::Storage(StorageFailure::ReceiptFailed));
        }
        Ok(Some(receipt))
    }

    fn persist_receipt(&self, path: &Path, receipt: &Receipt) -> Result<()> {
        let bytes = serde_json::to_vec(receipt)?;
        let temp = self.write_temp_bytes(&receipt.dataset_id, "receipt", &bytes)?;
        fs::rename(&temp, path)?;
        File::open(&self.state_dir)?.sync_all()?;
        Ok(())
    }

    fn write_temp_bytes(&self, dataset_id: &str, suffix: &str, bytes: &[u8]) -> Result<PathBuf> {
        let path = self.temp_path(dataset_id, suffix)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(path)
    }

    fn temp_path(&self, dataset_id: &str, suffix: &str) -> Result<PathBuf> {
        if !safe_component(dataset_id) || !safe_component(suffix) {
            return Err(MarketDataError::InvalidInput);
        }
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Ok(self.staging_dir.join(format!(
            ".{dataset_id}-{suffix}-{}-{sequence}.tmp",
            std::process::id()
        )))
    }

    fn state_path(&self, dataset_id: &str, suffix: &str) -> Result<PathBuf> {
        if !safe_component(dataset_id) || !safe_component(suffix) {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(self.state_dir.join(format!("{dataset_id}.{suffix}")))
    }

    fn validate_staging_budget(&self, target: &Path) -> Result<()> {
        let metadata = fs::symlink_metadata(target)?;
        if !metadata.file_type().is_file() {
            return Err(MarketDataError::InvalidInput);
        }
        let canonical_target = fs::canonicalize(target)?;
        let canonical_staging = fs::canonicalize(&self.staging_dir)?;
        if !canonical_target.starts_with(canonical_staging) {
            return Err(MarketDataError::InvalidInput);
        }
        let mut total = 0_u64;
        let mut pending = vec![self.staging_dir.clone()];
        let mut visited = 0usize;
        while let Some(path) = pending.pop() {
            visited += 1;
            if visited > 100_000 {
                return Err(MarketDataError::InputLimit);
            }
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_symlink() {
                    return Err(MarketDataError::InvalidInput);
                }
                if file_type.is_dir() {
                    pending.push(entry.path());
                } else if file_type.is_file() {
                    total = total
                        .checked_add(entry.metadata()?.len())
                        .ok_or(MarketDataError::InputLimit)?;
                    if total > self.limits.max_staging_bytes {
                        return Err(MarketDataError::InputLimit);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FileHash {
    content_sha256: String,
    size_bytes: u64,
}

fn hash_file(path: &Path, maximum: u64) -> Result<FileHash> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(MarketDataError::InputLimit);
    }
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or(MarketDataError::InputLimit)?;
        if total > maximum {
            return Err(MarketDataError::InputLimit);
        }
        hasher.update(&buffer[..count]);
    }
    Ok(FileHash {
        content_sha256: hex::encode(hasher.finalize()),
        size_bytes: total,
    })
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn same_intent(
    receipt: &Receipt,
    request: &ArchiveRequest,
    local: &FileHash,
    parquet: &ParquetVerification,
    intent_sha256: &str,
) -> bool {
    receipt.dataset_id == request.dataset_id
        && receipt.object_name == request.object_name
        && receipt.content_sha256 == local.content_sha256
        && receipt.schema_sha256 == parquet.schema_sha256
        && receipt.size_bytes == local.size_bytes
        && receipt.row_count == request.row_count
        && receipt.purpose == request.purpose
        && receipt.intent_sha256 == intent_sha256
}

#[derive(Serialize)]
struct RequestIdentity<'a> {
    dataset_id: &'a str,
    object_name: &'a str,
    schema_id: &'a str,
    purpose: PublicationPurpose,
    source: &'a MarketDataSourceV1,
    symbols: &'a [String],
    time_range: &'a Option<DatasetTimeRangeV1>,
    source_timestamp_missing_rows: u64,
    row_count: u64,
    source_pages_exhausted: Option<bool>,
    input_eof: bool,
}

fn intent_fingerprint(request: &ArchiveRequest) -> Result<String> {
    let identity = RequestIdentity {
        dataset_id: &request.dataset_id,
        object_name: &request.object_name,
        schema_id: &request.schema_id,
        purpose: request.purpose,
        source: &request.source,
        symbols: &request.symbols,
        time_range: &request.time_range,
        source_timestamp_missing_rows: request.source_timestamp_missing_rows,
        row_count: request.row_count,
        source_pages_exhausted: request.source_pages_exhausted,
        input_eof: request.input_eof,
    };
    Ok(sha256_bytes(&serde_json::to_vec(&identity)?))
}

fn validate_request(request: &ArchiveRequest, transport: TransportKind) -> Result<()> {
    if !safe_component(&request.dataset_id)
        || !safe_object_name(&request.object_name)
        || request.object_name != format!("{}.parquet", request.dataset_id)
        || request.symbols.is_empty()
        || request.symbols.windows(2).any(|pair| pair[0] >= pair[1])
        || !matches!(
            request.schema_id.as_str(),
            EVENT_SCHEMA_ID | MINUTE_BAR_SCHEMA_ID
        )
        || !request.input_eof
        || request.source_pages_exhausted == Some(false)
    {
        return Err(MarketDataError::InvalidInput);
    }
    request
        .source
        .validate()
        .map_err(|_| MarketDataError::Contract)?;
    match (transport, request.purpose) {
        (TransportKind::RcloneGoogleDrive, PublicationPurpose::Curated) => {
            let exact_numeric = matches!(
                request.source.numeric_encoding,
                market_contracts::NumericEncodingV1::DecimalToken
                    | market_contracts::NumericEncodingV1::IntegerToken
            );
            if request.source.provider != "alpaca"
                || !matches!(request.source.feed.as_str(), "sip" | "opra")
                || request.source.entitlement != market_contracts::EntitlementState::Authorized
                || !exact_numeric
            {
                return Err(MarketDataError::PublicationNotAuthorized);
            }
        }
        (TransportKind::LocalTest, PublicationPurpose::Diagnostic) => {}
        _ => return Err(MarketDataError::PublicationNotAuthorized),
    }
    Ok(())
}

fn make_manifest(
    request: &ArchiveRequest,
    local: &FileHash,
    parquet: &ParquetVerification,
    remote: &RemoteObject,
    transport: TransportKind,
) -> Result<DatasetManifestV1> {
    let manifest = DatasetManifestV1 {
        schema_version: 1,
        dataset_id: request.dataset_id.clone(),
        source: request.source.clone(),
        symbols: request.symbols.clone(),
        time_range: request.time_range.clone(),
        source_timestamp_missing_rows: request.source_timestamp_missing_rows,
        row_count: request.row_count,
        object: DatasetObjectV1 {
            object_name: request.object_name.clone(),
            object_id: Some(remote.id.clone()),
            size_bytes: local.size_bytes,
            content_sha256: local.content_sha256.clone(),
            parquet_schema_sha256: parquet.schema_sha256.clone(),
            parquet_footer_rows: parquet.footer_rows,
            transport: match transport {
                TransportKind::LocalTest => DatasetTransportV1::LocalTest,
                TransportKind::RcloneGoogleDrive => DatasetTransportV1::RcloneGoogleDrive,
            },
        },
        completion: DatasetCompletionEvidenceV1 {
            input_eof: request.input_eof,
            source_pages_exhausted: request.source_pages_exhausted,
            readback_sha256: local.content_sha256.clone(),
            verified_before_publish: true,
        },
    };
    manifest
        .validate()
        .map_err(|_| MarketDataError::Storage(StorageFailure::InvalidManifest))?;
    Ok(manifest)
}

fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.contains("..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn safe_object_name(value: &str) -> bool {
    safe_component(value) && !value.starts_with('.') && !value.ends_with('.')
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub struct ArchiveWriterQueue {
    sender: mpsc::Sender<QueuedPublish>,
}

struct QueuedPublish {
    request: ArchiveRequest,
    response: oneshot::Sender<Result<DatasetManifestV1>>,
}

impl ArchiveWriterQueue {
    pub fn spawn(publisher: ArchivePublisher, capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > 64 {
            return Err(MarketDataError::InvalidInput);
        }
        let (sender, mut receiver) = mpsc::channel::<QueuedPublish>(capacity);
        let worker_permit = BackgroundWorkerPermit::acquire()?;
        thread::Builder::new()
            .name("mdp-archive-writer".to_owned())
            .spawn(move || {
                let _worker_permit = worker_permit;
                while let Some(queued) = receiver.blocking_recv() {
                    let result = publisher.publish(&queued.request);
                    let _ = queued.response.send(result);
                }
            })
            .map_err(|_| MarketDataError::InvalidInput)?;
        Ok(Self { sender })
    }

    /// Bounded queue backpressures the producer; one dedicated OS thread performs locks and fsync.
    pub async fn submit(&self, request: ArchiveRequest) -> Result<DatasetManifestV1> {
        let (response, result) = oneshot::channel();
        self.sender
            .send(QueuedPublish { request, response })
            .await
            .map_err(|_| MarketDataError::QueueClosed)?;
        result.await.map_err(|_| MarketDataError::QueueClosed)?
    }
}

#[cfg(test)]
mod tests;
