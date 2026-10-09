//! Read-only verification for one explicitly named LocalTest capture-pair chunk.
//!
//! This reader never discovers receipts by scanning a directory. It verifies one bounded
//! receipt, its two manifests and objects, and the raw/event join in the existing isolated
//! Parquet worker. A verified chunk is not a capture rollup or provider-completeness claim.

#[cfg(target_os = "linux")]
use std::path::PathBuf;

#[cfg(target_os = "linux")]
use std::{
    fs::{self, File},
    io::Read,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(target_os = "linux")]
use fs2::FileExt;

#[cfg(target_os = "linux")]
use crate::{
    MarketDataError, Result, archive::ArchiveLimits, cancellation::CancellationToken,
    error::StorageFailure, storage::LocalTestTransport,
};
use market_contracts::EntitlementState;
use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use crate::{
    archive::{
        ArchivePublisher, DEFAULT_MAX_MANIFEST_BYTES,
        capture_pair_v2::publish::open_private_pair_directory,
    },
    parquet_worker,
    storage::{ObjectTransport, RemoteObject},
};

#[cfg(target_os = "linux")]
use super::{LocalCapturePairChunkReceiptV2, PairReceiptError};

#[cfg(target_os = "linux")]
mod verify;
#[cfg(target_os = "linux")]
pub(crate) use verify::verify_pair_chunk_files;

#[cfg(target_os = "linux")]
const MAX_PAIR_RECEIPT_BYTES: u64 = DEFAULT_MAX_MANIFEST_BYTES;
#[cfg(target_os = "linux")]
const PAIR_SCOPE: &str = "SINGLE_CHUNK_LOCAL_READBACK";
#[cfg(target_os = "linux")]
const PAIR_STATUS: &str = "VERIFIED_LOCAL_TEST_CHUNK_ONLY";
#[cfg(target_os = "linux")]
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[cfg(target_os = "linux")]
const PRIVATE_STAGING_DIRECTORY_MODE: u32 = 0o700;
#[cfg(target_os = "linux")]
const PRIVATE_STAGING_FILE_MODE: u32 = 0o600;

/// Compact facts from exact readback of one LocalTest raw/event V2/V3 pair.
///
/// Counts and byte totals use canonical decimal strings on the JSON wire. No raw payload or
/// normalized event rows are returned. `source_completeness` is always `NOT_ASSERTED`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct VerifiedCapturePairChunkV2 {
    status: String,
    verification_scope: String,
    source_completeness: String,
    pair_verification: String,
    pair_verification_version: u32,
    transport: String,
    provider: String,
    feed: String,
    entitlement: EntitlementState,
    capture_instance_id: String,
    #[serde(with = "market_contracts::wire_u64")]
    source_generation: u64,
    #[serde(with = "market_contracts::wire_u64")]
    canonical_generation: u64,
    #[serde(with = "market_contracts::wire_u64")]
    first_source_frame_sequence: u64,
    #[serde(with = "market_contracts::wire_u64")]
    last_source_frame_sequence: u64,
    raw_frame_count: u32,
    normalized_event_count: u32,
    #[serde(with = "market_contracts::wire_u64")]
    input_payload_bytes: u64,
    input_chunk_sha256: String,
    manifest_input_sha256: String,
    raw_dataset_id: String,
    raw_manifest_sha256: String,
    raw_object_sha256: String,
    raw_schema_id: String,
    #[serde(with = "market_contracts::wire_u64")]
    raw_row_count: u64,
    event_dataset_id: String,
    event_manifest_sha256: String,
    event_object_sha256: String,
    event_schema_id: String,
    #[serde(with = "market_contracts::wire_u64")]
    event_row_count: u64,
}

/// Explicit LocalTest-only reader for one private V2 capture-pair receipt.
#[cfg(target_os = "linux")]
pub(crate) struct LocalCapturePairV2Reader {
    transport: LocalTestTransport,
    state_root: PathBuf,
    staging_root: PathBuf,
    staging_device: u64,
    staging_inode: u64,
    limits: ArchiveLimits,
}

#[cfg(target_os = "linux")]
impl std::fmt::Debug for LocalCapturePairV2Reader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalCapturePairV2Reader")
            .field("transport", &"local-test")
            .field("paths", &"redacted")
            .field("limits", &self.limits)
            .finish()
    }
}

#[cfg(target_os = "linux")]
impl LocalCapturePairV2Reader {
    /// Opens existing LocalTest archive and state roots without modifying them.
    /// Requires an existing staging root; each readback uses a private temporary child directory.
    #[cfg(target_os = "linux")]
    pub(crate) fn local_test(
        local_test_root: impl Into<PathBuf>,
        state_root: impl Into<PathBuf>,
        staging_root: impl Into<PathBuf>,
        limits: ArchiveLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let staging_root = staging_root.into();
        let supplied_metadata = fs::symlink_metadata(&staging_root)?;
        if !supplied_metadata.file_type().is_dir() {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let canonical_staging = fs::canonicalize(&staging_root)?;
        let metadata = fs::symlink_metadata(&canonical_staging)?;
        if !metadata.file_type().is_dir() {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if metadata.uid() != rustix::process::geteuid().as_raw()
                || metadata.permissions().mode() & 0o022 != 0
                || metadata.dev() != supplied_metadata.dev()
                || metadata.ino() != supplied_metadata.ino()
            {
                return Err(MarketDataError::PublicationNotAuthorized);
            }
        }
        Ok(Self {
            transport: LocalTestTransport::open_existing(local_test_root)?,
            state_root: state_root.into(),
            staging_root: canonical_staging,
            staging_device: metadata.dev(),
            staging_inode: metadata.ino(),
            limits,
        })
    }

    /// Verifies exactly one explicitly named private chunk receipt and its two LocalTest artifacts.
    ///
    /// `receipt_name` must be a canonical `chunk-<sha256>.receipt.json` basename. This method
    /// does not search the receipt directory or interpret a capture rollup.
    #[cfg(target_os = "linux")]
    pub(crate) fn verify_chunk(&self, receipt_name: &str) -> Result<VerifiedCapturePairChunkV2> {
        self.verify_chunk_cancellable(receipt_name, &CancellationToken::new())
    }

    /// Verifies one explicit chunk while propagating request cancellation through exact local
    /// reads, private staging copies, and the isolated Parquet worker.
    #[cfg(target_os = "linux")]
    pub(crate) fn verify_chunk_cancellable(
        &self,
        receipt_name: &str,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedCapturePairChunkV2> {
        ensure_not_cancelled(cancellation)?;
        if !valid_chunk_receipt_name(receipt_name) {
            return Err(MarketDataError::InvalidInput);
        }
        let _budget_lock = StagingBudgetLock::acquire(
            &self.staging_root,
            self.staging_device,
            self.staging_inode,
            cancellation,
        )?;
        ensure_not_cancelled(cancellation)?;
        ArchivePublisher::preflight_existing_replay_staging(
            &_budget_lock.root_directory,
            &self.limits,
            cancellation,
        )?;
        ensure_not_cancelled(cancellation)?;
        let mut staging = PrivateStagingRun::create(
            &self.staging_root,
            &_budget_lock.root_directory,
            self.staging_device,
            self.staging_inode,
            cancellation,
        )?;
        ensure_not_cancelled(cancellation)?;
        let receipt_directory = open_private_pair_directory(&self.state_root, false)?;
        let (receipt, receipt_bytes) =
            read_private_pair_receipt(&receipt_directory, receipt_name, cancellation)?;
        receipt.validate().map_err(map_pair_error)?;

        let raw = stage_artifact(
            &self.transport,
            &receipt.raw_frames,
            self.limits.max_manifest_bytes,
            self.limits.max_object_bytes,
            &mut staging,
            "raw",
            cancellation,
        )?;
        let events = stage_artifact(
            &self.transport,
            &receipt.normalized_events,
            self.limits.max_manifest_bytes,
            self.limits.max_object_bytes,
            &mut staging,
            "events",
            cancellation,
        )?;
        ensure_not_cancelled(cancellation)?;
        let staged_receipt = staging.create_bytes(&receipt_bytes, "receipt", "json")?;

        let request = parquet_worker::CapturePairV2WorkerRequest {
            receipt_path: &staged_receipt,
            raw_manifest_path: &raw.manifest_path,
            raw_parquet_path: &raw.object_path,
            event_manifest_path: &events.manifest_path,
            event_parquet_path: &events.object_path,
            raw_schema_id: &receipt.raw_frames.parquet_schema_id,
            receipt_name,
            max_manifest_bytes: self.limits.max_manifest_bytes,
            max_object_bytes: self.limits.max_object_bytes,
        };
        parquet_worker::verify_capture_pair_v2_cancellable(&request, cancellation.clone())
    }
}

#[cfg(target_os = "linux")]
struct StagingBudgetLock {
    root_directory: File,
    lock_file: File,
}

#[cfg(target_os = "linux")]
impl StagingBudgetLock {
    fn acquire(
        root: &Path,
        expected_device: u64,
        expected_inode: u64,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        ensure_not_cancelled(cancellation)?;
        const LOCK_NAME: &str = ".pair-readback-budget.lock";
        let supplied_metadata = fs::symlink_metadata(root)?;
        if !supplied_metadata.file_type().is_dir()
            || supplied_metadata.uid() != rustix::process::geteuid().as_raw()
            || supplied_metadata.permissions().mode() & 0o022 != 0
            || supplied_metadata.dev() != expected_device
            || supplied_metadata.ino() != expected_inode
        {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let canonical_root = fs::canonicalize(root)?;
        if canonical_root != root {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let root_directory = File::from(
            rustix::fs::open(
                &canonical_root,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| MarketDataError::PublicationNotAuthorized)?,
        );
        let opened_root = root_directory.metadata()?;
        if opened_root.dev() != supplied_metadata.dev()
            || opened_root.ino() != supplied_metadata.ino()
            || opened_root.uid() != rustix::process::geteuid().as_raw()
        {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let lock_file = File::from(
            rustix::fs::openat(
                &root_directory,
                LOCK_NAME,
                rustix::fs::OFlags::RDWR
                    | rustix::fs::OFlags::CREATE
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::from_raw_mode(0o600),
            )
            .map_err(|_| MarketDataError::PublicationNotAuthorized)?,
        );
        let metadata = lock_file.metadata()?;
        if !metadata.file_type().is_file()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.permissions().mode() & 0o7777 != 0o600
            || metadata.nlink() != 1
        {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        ensure_not_cancelled(cancellation)?;
        lock_file.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                MarketDataError::LockHeld
            } else {
                MarketDataError::Io(error)
            }
        })?;
        Ok(Self {
            root_directory,
            lock_file,
        })
    }
}

#[cfg(target_os = "linux")]
impl Drop for StagingBudgetLock {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

#[cfg(target_os = "linux")]
struct StagedArtifact {
    manifest_path: PathBuf,
    object_path: PathBuf,
}

#[cfg(target_os = "linux")]
fn stage_artifact(
    transport: &LocalTestTransport,
    receipt: &super::CaptureArtifactReceiptV2,
    max_manifest_bytes: u64,
    max_object_bytes: u64,
    staging: &mut PrivateStagingRun,
    label: &str,
    cancellation: &CancellationToken,
) -> Result<StagedArtifact> {
    ensure_not_cancelled(cancellation)?;
    if receipt.transport != market_contracts::DatasetTransportV1::LocalTest
        || receipt.manifest_size_bytes > max_manifest_bytes
        || receipt.size_bytes > max_object_bytes
    {
        return Err(MarketDataError::InputLimit);
    }
    let manifest_object = transport
        .lookup_cancellable(
            &receipt.dataset_id,
            &receipt.manifest_object_name,
            cancellation,
        )?
        .ok_or(MarketDataError::IncompleteWindow)?;
    ensure_not_cancelled(cancellation)?;
    let parquet_object = transport
        .lookup_cancellable(&receipt.dataset_id, &receipt.object_name, cancellation)?
        .ok_or(MarketDataError::IncompleteWindow)?;
    ensure_not_cancelled(cancellation)?;
    if !same_remote_object(
        &manifest_object,
        &receipt.manifest_object_id,
        receipt.manifest_size_bytes,
        &receipt.dataset_id,
        &receipt.manifest_object_name,
    ) || !same_remote_object(
        &parquet_object,
        &receipt.object_id,
        receipt.size_bytes,
        &receipt.dataset_id,
        &receipt.object_name,
    ) {
        return Err(MarketDataError::Conflict);
    }

    let manifest_path = staging.new_path(label, "manifest")?;
    transport.download_private_with_limit_cancellable(
        &receipt.dataset_id,
        &receipt.manifest_object_name,
        &manifest_path,
        max_manifest_bytes,
        cancellation,
    )?;
    let object_path = staging.new_path(label, "parquet")?;
    transport.download_private_with_limit_cancellable(
        &receipt.dataset_id,
        &receipt.object_name,
        &object_path,
        max_object_bytes,
        cancellation,
    )?;
    Ok(StagedArtifact {
        manifest_path,
        object_path,
    })
}

#[cfg(target_os = "linux")]
fn same_remote_object(
    actual: &RemoteObject,
    expected_id: &str,
    expected_size: u64,
    dataset_id: &str,
    object_name: &str,
) -> bool {
    actual.id == format!("local-test:{dataset_id}-{object_name}")
        && actual.id == expected_id
        && actual.size_bytes == expected_size
}

#[cfg(target_os = "linux")]
fn valid_chunk_receipt_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() == b"chunk-".len() + 64 + b".receipt.json".len()
        && &bytes[..6] == b"chunk-"
        && &bytes[70..] == b".receipt.json"
        && bytes[6..70]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

#[cfg(target_os = "linux")]
fn read_private_pair_receipt(
    directory: &File,
    name: &str,
    cancellation: &CancellationToken,
) -> Result<(LocalCapturePairChunkReceiptV2, Vec<u8>)> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    ensure_not_cancelled(cancellation)?;
    let fd = rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| MarketDataError::IncompleteWindow)?;
    let file = File::from(fd);
    let metadata = file.metadata()?;
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
    let mut reader = file.take(MAX_PAIR_RECEIPT_BYTES + 1);
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        ensure_not_cancelled(cancellation)?;
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
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

#[cfg(target_os = "linux")]
fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(MarketDataError::Storage(StorageFailure::Cancelled))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
struct PrivateStagingRun {
    parent: File,
    directory: File,
    directory_name: String,
    path: PathBuf,
    file_names: Vec<String>,
}

#[cfg(target_os = "linux")]
impl PrivateStagingRun {
    fn create(
        root: &Path,
        root_directory: &File,
        expected_device: u64,
        expected_inode: u64,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        Self::create_with_hook(
            root,
            root_directory,
            expected_device,
            expected_inode,
            cancellation,
            || {},
        )
    }

    fn create_with_hook(
        root: &Path,
        root_directory: &File,
        expected_device: u64,
        expected_inode: u64,
        cancellation: &CancellationToken,
        mut after_directory_created: impl FnMut(),
    ) -> Result<Self> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        ensure_not_cancelled(cancellation)?;
        let root_metadata = fs::symlink_metadata(root)?;
        if !root_metadata.file_type().is_dir()
            || root_metadata.uid() != rustix::process::geteuid().as_raw()
            || root_metadata.permissions().mode() & 0o022 != 0
            || root_metadata.dev() != expected_device
            || root_metadata.ino() != expected_inode
        {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let canonical_root = fs::canonicalize(root)?;
        if canonical_root != root {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let parent = root_directory.try_clone()?;
        let opened_root = parent.metadata()?;
        if opened_root.dev() != expected_device || opened_root.ino() != expected_inode {
            return Err(MarketDataError::PublicationNotAuthorized);
        }

        for _ in 0..8 {
            ensure_not_cancelled(cancellation)?;
            let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let directory_name = format!("pair-readback-{}-{sequence}", std::process::id());
            match rustix::fs::mkdirat(
                &parent,
                directory_name.as_str(),
                rustix::fs::Mode::from_raw_mode(PRIVATE_STAGING_DIRECTORY_MODE),
            ) {
                Ok(()) => after_directory_created(),
                Err(error) if error == rustix::io::Errno::EXIST => continue,
                Err(_) => return Err(MarketDataError::PublicationNotAuthorized),
            }
            let directory = match rustix::fs::openat(
                &parent,
                directory_name.as_str(),
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            ) {
                Ok(fd) => File::from(fd),
                Err(_) => {
                    let _ = rustix::fs::unlinkat(
                        &parent,
                        directory_name.as_str(),
                        rustix::fs::AtFlags::REMOVEDIR,
                    );
                    return Err(MarketDataError::PublicationNotAuthorized);
                }
            };
            let metadata = match directory.metadata() {
                Ok(metadata) => metadata,
                Err(error) => {
                    drop(directory);
                    let _ = rustix::fs::unlinkat(
                        &parent,
                        directory_name.as_str(),
                        rustix::fs::AtFlags::REMOVEDIR,
                    );
                    return Err(MarketDataError::Io(error));
                }
            };
            let path_metadata = match fs::symlink_metadata(canonical_root.join(&directory_name)) {
                Ok(metadata) => metadata,
                Err(error) => {
                    drop(directory);
                    let _ = rustix::fs::unlinkat(
                        &parent,
                        directory_name.as_str(),
                        rustix::fs::AtFlags::REMOVEDIR,
                    );
                    return Err(MarketDataError::Io(error));
                }
            };
            if !metadata.file_type().is_dir()
                || metadata.uid() != rustix::process::geteuid().as_raw()
                || metadata.permissions().mode() & 0o7777 != PRIVATE_STAGING_DIRECTORY_MODE
                || !path_metadata.file_type().is_dir()
                || metadata.dev() != path_metadata.dev()
                || metadata.ino() != path_metadata.ino()
            {
                drop(directory);
                let _ = rustix::fs::unlinkat(
                    &parent,
                    directory_name.as_str(),
                    rustix::fs::AtFlags::REMOVEDIR,
                );
                return Err(MarketDataError::PublicationNotAuthorized);
            }
            return Ok(Self {
                parent,
                directory,
                path: canonical_root.join(&directory_name),
                directory_name,
                file_names: Vec::new(),
            });
        }
        Err(MarketDataError::PublicationNotAuthorized)
    }

    fn new_path(&mut self, label: &str, extension: &str) -> Result<PathBuf> {
        if !label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'-')
            || !extension
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'-')
        {
            return Err(MarketDataError::InvalidInput);
        }
        let sequence =
            u32::try_from(self.file_names.len()).map_err(|_| MarketDataError::InputLimit)?;
        let name = format!("{label}-{sequence}.{extension}");
        self.file_names.push(name.clone());
        Ok(self.path.join(name))
    }

    fn create_bytes(&mut self, bytes: &[u8], label: &str, extension: &str) -> Result<PathBuf> {
        use std::io::Write;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        if bytes.is_empty() || bytes.len() as u64 > MAX_PAIR_RECEIPT_BYTES {
            return Err(MarketDataError::InputLimit);
        }
        let path = self.new_path(label, extension)?;
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or(MarketDataError::InvalidInput)?;
        let fd = rustix::fs::openat(
            &self.directory,
            name,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(PRIVATE_STAGING_FILE_MODE),
        )
        .map_err(|_| MarketDataError::PublicationNotAuthorized)?;
        let mut file = File::from(fd);
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.permissions().mode() & 0o7777 != PRIVATE_STAGING_FILE_MODE
            || metadata.nlink() != 1
        {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(path)
    }
}

#[cfg(target_os = "linux")]
impl Drop for PrivateStagingRun {
    fn drop(&mut self) {
        for name in &self.file_names {
            let _ =
                rustix::fs::unlinkat(&self.directory, name.as_str(), rustix::fs::AtFlags::empty());
        }
        let _ = self.directory.sync_all();
        let _ = rustix::fs::unlinkat(
            &self.parent,
            self.directory_name.as_str(),
            rustix::fs::AtFlags::REMOVEDIR,
        );
    }
}

#[cfg(all(test, target_os = "linux"))]
mod staging_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn cancellation_after_private_run_directory_creation_still_drops_cleanup_guard() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("staging");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let root = fs::canonicalize(root).unwrap();
        let root_directory = File::from(
            rustix::fs::open(
                &root,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .unwrap(),
        );
        let metadata = root_directory.metadata().unwrap();
        let cancellation = CancellationToken::new();
        let cancel_after_mkdir = cancellation.clone();

        let result = (|| {
            let staging = PrivateStagingRun::create_with_hook(
                &root,
                &root_directory,
                metadata.dev(),
                metadata.ino(),
                &cancellation,
                move || cancel_after_mkdir.cancel(),
            )?;
            ensure_not_cancelled(&cancellation)?;
            Ok::<_, MarketDataError>(staging)
        })();

        assert!(matches!(
            result,
            Err(MarketDataError::Storage(StorageFailure::Cancelled))
        ));
        assert_eq!(fs::read_dir(root).unwrap().count(), 0);
    }
}

#[cfg(target_os = "linux")]
fn map_pair_error(error: PairReceiptError) -> MarketDataError {
    super::publish::map_pair_error(error)
}
