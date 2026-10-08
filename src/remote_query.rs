//! Read-only archive query with bounded, hash-verified local caching.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use fs2::FileExt;
use market_contracts::{DatasetManifestV1, DatasetTransportV1};
use serde::{Deserialize, Serialize};

use crate::{
    MarketDataError, Result,
    aggregate::TradeMinuteBarV1,
    archive::TransportKind,
    cancellation::CancellationToken,
    config::DriveConfig,
    parquet_store::{self, ParquetVerification},
    parquet_worker,
    schema::{EVENT_SCHEMA_ID, MINUTE_BAR_SCHEMA_ID},
    storage::{
        LocalTestTransport, ObjectTransport, RcloneDriveTransport, RemoteObject,
        namespaced_dataset_id, safe_component, safe_object_name,
    },
};

pub const DEFAULT_REMOTE_CACHE_BYTES: u64 = 32 * 1024 * 1024 * 1024;
pub const DEFAULT_REMOTE_CACHE_ENTRIES: usize = 64;
pub const DEFAULT_REMOTE_CACHE_TTL: Duration = Duration::from_secs(15 * 60);
pub const DEFAULT_MAX_QUERY_RESULT_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_MAX_EXPORT_BYTES: u64 = 1024 * 1024 * 1024;
const CACHE_RECEIPT_NAME: &str = ".cache-receipt.json";
const CACHE_MANIFEST_NAME: &str = "manifest.json";
const MAX_CACHE_RECEIPT_BYTES: u64 = 64 * 1024;
const CACHE_DIR_ENTRY_LIMIT: usize = 100_000;
static CACHE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

mod cache_fs;
mod cleanup;
mod export;
use cache_fs::*;
pub use cleanup::{RemoteCacheCleaner, RemoteCacheCleanupReport};
use cleanup::{read_cache_receipt, verify_cache_contents_cancellable};
pub use export::write_bars_jsonl_bounded;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetNamespace {
    Curated,
    Diagnostic,
}

impl DatasetNamespace {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Curated => "curated",
            Self::Diagnostic => "diagnostic",
        }
    }
}

#[derive(Clone, Debug)]
pub struct RemoteCacheLimits {
    pub max_object_bytes: u64,
    pub max_manifest_bytes: u64,
    pub max_total_cache_bytes: u64,
    pub max_entries: usize,
    pub ttl: Duration,
    pub max_query_rows: usize,
    pub max_query_result_bytes: u64,
    pub max_export_bytes: u64,
}

impl Default for RemoteCacheLimits {
    fn default() -> Self {
        Self {
            max_object_bytes: crate::archive::DEFAULT_MAX_OBJECT_BYTES,
            max_manifest_bytes: crate::archive::DEFAULT_MAX_MANIFEST_BYTES,
            max_total_cache_bytes: DEFAULT_REMOTE_CACHE_BYTES,
            max_entries: DEFAULT_REMOTE_CACHE_ENTRIES,
            ttl: DEFAULT_REMOTE_CACHE_TTL,
            max_query_rows: crate::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize,
            max_query_result_bytes: DEFAULT_MAX_QUERY_RESULT_BYTES,
            max_export_bytes: DEFAULT_MAX_EXPORT_BYTES,
        }
    }
}

impl RemoteCacheLimits {
    fn validate(&self) -> Result<()> {
        if self.max_object_bytes == 0
            || self.max_manifest_bytes == 0
            || self.max_total_cache_bytes < self.max_object_bytes
            || self.max_entries == 0
            || self.max_entries > 4096
            || self.ttl.is_zero()
            || self.ttl > Duration::from_secs(24 * 60 * 60)
            || self.max_query_rows == 0
            || self.max_query_rows > crate::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize
            || self.max_query_result_bytes == 0
            || self.max_export_bytes == 0
        {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RemoteQuerySummary {
    pub namespace: DatasetNamespace,
    pub dataset_id: String,
    pub schema_id: String,
    pub source: market_contracts::MarketDataSourceV1,
    pub row_count: u64,
    pub returned_rows: u64,
    pub content_sha256: String,
    pub parquet_schema_sha256: String,
    pub cache_hit: bool,
}

pub struct RemoteArchiveReader {
    transport: Arc<dyn ObjectTransport>,
    transport_kind: TransportKind,
    cache_root: PathBuf,
    limits: RemoteCacheLimits,
    now_unix_seconds: Arc<dyn Fn() -> u64 + Send + Sync>,
    isolate_parquet_decode: bool,
}

impl fmt::Debug for RemoteArchiveReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteArchiveReader")
            .field("transport", &self.transport_kind)
            .field("cache_root", &"redacted")
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteCacheReceiptV1 {
    version: u32,
    namespace: DatasetNamespace,
    dataset_id: String,
    manifest_remote_id: String,
    manifest_remote_size_bytes: u64,
    manifest_remote_md5: Option<String>,
    manifest_sha256: String,
    object_remote_id: String,
    object_remote_size_bytes: u64,
    object_remote_md5: Option<String>,
    object_sha256: String,
    parquet_schema_sha256: String,
    verified_at_unix_seconds: u64,
}

struct VerifiedCache {
    path: PathBuf,
    manifest: DatasetManifestV1,
    parquet: ParquetVerification,
    content_sha256: String,
    cache_hit: bool,
    _dataset_lock: Option<File>,
}

struct TempDirectoryCleanup(Option<PathBuf>);

impl Drop for TempDirectoryCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

struct PublishedCacheCleanup(Option<PathBuf>);

impl PublishedCacheCleanup {
    fn new(path: PathBuf) -> Self {
        Self(Some(path))
    }

    fn keep(&mut self) {
        self.0 = None;
    }
}

impl Drop for PublishedCacheCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let receipt = path.join(CACHE_RECEIPT_NAME);
            let _ = fs::remove_file(receipt);
            let _ = fs::remove_dir_all(path);
        }
    }
}

pub(super) fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(MarketDataError::Storage(
            crate::error::StorageFailure::Cancelled,
        ))
    } else {
        Ok(())
    }
}

impl RemoteArchiveReader {
    pub fn transport_kind(&self) -> TransportKind {
        self.transport_kind
    }

    pub fn local_test(
        transport: LocalTestTransport,
        cache_root: impl Into<PathBuf>,
        limits: RemoteCacheLimits,
    ) -> Result<Self> {
        Self::new(
            Arc::new(transport),
            TransportKind::LocalTest,
            cache_root.into(),
            limits,
        )
    }

    /// Construct a local-test reader whose Parquet decoding always runs in the bounded worker.
    pub fn local_test_isolated(
        transport: LocalTestTransport,
        cache_root: impl Into<PathBuf>,
        limits: RemoteCacheLimits,
    ) -> Result<Self> {
        let mut reader = Self::new(
            Arc::new(transport),
            TransportKind::LocalTest,
            cache_root.into(),
            limits,
        )?;
        reader.isolate_parquet_decode = true;
        Ok(reader)
    }

    pub fn rclone_drive(
        config: DriveConfig,
        cache_root: impl Into<PathBuf>,
        limits: RemoteCacheLimits,
    ) -> Result<Self> {
        let transport = RcloneDriveTransport::new(config)?;
        let mut reader = Self::new(
            Arc::new(transport),
            TransportKind::RcloneGoogleDrive,
            cache_root.into(),
            limits,
        )?;
        reader.isolate_parquet_decode = true;
        Ok(reader)
    }

    fn new(
        transport: Arc<dyn ObjectTransport>,
        transport_kind: TransportKind,
        cache_root: PathBuf,
        limits: RemoteCacheLimits,
    ) -> Result<Self> {
        Self::with_clock(
            transport,
            transport_kind,
            cache_root,
            limits,
            Arc::new(system_time_unix_seconds),
        )
    }

    fn with_clock(
        transport: Arc<dyn ObjectTransport>,
        transport_kind: TransportKind,
        cache_root: PathBuf,
        limits: RemoteCacheLimits,
        now_unix_seconds: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Result<Self> {
        limits.validate()?;
        fs::create_dir_all(&cache_root)?;
        require_plain_dir(&cache_root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&cache_root, fs::Permissions::from_mode(0o700))?;
        }
        let cache_root = fs::canonicalize(cache_root)?;
        Ok(Self {
            transport,
            transport_kind,
            cache_root,
            limits,
            now_unix_seconds,
            isolate_parquet_decode: false,
        })
    }

    pub fn query_bars(
        &self,
        namespace: DatasetNamespace,
        dataset_id: &str,
        symbol_filter: Option<&str>,
    ) -> Result<(Vec<TradeMinuteBarV1>, RemoteQuerySummary)> {
        self.query_bars_with_cancellation(namespace, dataset_id, symbol_filter, None)
    }

    pub fn query_bars_cancellable(
        &self,
        namespace: DatasetNamespace,
        dataset_id: &str,
        symbol_filter: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<TradeMinuteBarV1>, RemoteQuerySummary)> {
        self.query_bars_with_cancellation(namespace, dataset_id, symbol_filter, Some(cancellation))
    }

    fn query_bars_with_cancellation(
        &self,
        namespace: DatasetNamespace,
        dataset_id: &str,
        symbol_filter: Option<&str>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(Vec<TradeMinuteBarV1>, RemoteQuerySummary)> {
        if let Some(token) = cancellation {
            ensure_not_cancelled(token)?;
        }
        let verified = self.ensure_cached_with_cancellation(namespace, dataset_id, cancellation)?;
        let mut cleanup = (!verified.cache_hit)
            .then(|| PublishedCacheCleanup::new(verified.path.parent().unwrap().to_path_buf()));
        if verified.parquet.schema_id != MINUTE_BAR_SCHEMA_ID {
            return Err(MarketDataError::ParquetSchema);
        }
        let rows = self.query_cached_bars(&verified.path, symbol_filter, cancellation)?;
        if let Some(token) = cancellation {
            ensure_not_cancelled(token)?;
        }
        let returned_rows = u64::try_from(rows.len()).map_err(|_| MarketDataError::InputLimit)?;
        let summary = RemoteQuerySummary {
            namespace,
            dataset_id: dataset_id.to_owned(),
            schema_id: verified.parquet.schema_id,
            source: verified.manifest.source,
            row_count: verified.parquet.footer_rows,
            returned_rows,
            content_sha256: verified.content_sha256,
            parquet_schema_sha256: verified.parquet.schema_sha256,
            cache_hit: verified.cache_hit,
        };
        if let Some(cleanup) = cleanup.as_mut() {
            cleanup.keep();
        }
        Ok((rows, summary))
    }

    pub fn export_bars_jsonl(
        &self,
        namespace: DatasetNamespace,
        dataset_id: &str,
        symbol_filter: Option<&str>,
        destination: &Path,
    ) -> Result<RemoteQuerySummary> {
        let verified = self.ensure_cached(namespace, dataset_id)?;
        if verified.parquet.schema_id != MINUTE_BAR_SCHEMA_ID {
            return Err(MarketDataError::ParquetSchema);
        }
        let rows = self.query_cached_bars(&verified.path, symbol_filter, None)?;
        write_bars_jsonl_bounded(destination, &rows, self.limits.max_export_bytes)?;
        let returned_rows = u64::try_from(rows.len()).map_err(|_| MarketDataError::InputLimit)?;
        Ok(RemoteQuerySummary {
            namespace,
            dataset_id: dataset_id.to_owned(),
            schema_id: verified.parquet.schema_id,
            source: verified.manifest.source,
            row_count: verified.parquet.footer_rows,
            returned_rows,
            content_sha256: verified.content_sha256,
            parquet_schema_sha256: verified.parquet.schema_sha256,
            cache_hit: verified.cache_hit,
        })
    }

    fn query_cached_bars(
        &self,
        path: &Path,
        symbol_filter: Option<&str>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Vec<TradeMinuteBarV1>> {
        if self.isolate_parquet_decode {
            match cancellation {
                Some(token) => parquet_worker::query_bars_cancellable(
                    path,
                    symbol_filter,
                    self.limits.max_query_rows,
                    self.limits.max_query_result_bytes,
                    token.clone(),
                ),
                None => parquet_worker::query_bars(
                    path,
                    symbol_filter,
                    self.limits.max_query_rows,
                    self.limits.max_query_result_bytes,
                ),
            }
        } else {
            if cancellation.is_some() {
                return Err(MarketDataError::Storage(
                    crate::error::StorageFailure::Unsupported,
                ));
            }
            parquet_store::query_bars_with_limits(
                path,
                symbol_filter,
                self.limits.max_query_rows,
                self.limits.max_query_result_bytes,
            )
        }
    }

    fn ensure_cached(
        &self,
        namespace: DatasetNamespace,
        dataset_id: &str,
    ) -> Result<VerifiedCache> {
        self.ensure_cached_with_cancellation(namespace, dataset_id, None)
    }

    fn ensure_cached_with_cancellation(
        &self,
        namespace: DatasetNamespace,
        dataset_id: &str,
        cancellation: Option<&CancellationToken>,
    ) -> Result<VerifiedCache> {
        if let Some(token) = cancellation {
            ensure_not_cancelled(token)?;
        }
        if !safe_component(dataset_id)
            || dataset_id.starts_with('.')
            || dataset_id.ends_with('.')
            || dataset_id.contains("..")
        {
            return Err(MarketDataError::InvalidInput);
        }
        let lock_dir = self.cache_root.join(".locks");
        fs::create_dir_all(&lock_dir)?;
        require_plain_dir(&lock_dir)?;
        let lock_path = lock_dir.join(format!("{}-{dataset_id}.lock", namespace.as_str()));
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| MarketDataError::LockHeld)?;

        let manifest_name = format!("{dataset_id}.manifest.json");
        let object_name = format!("{dataset_id}.parquet");
        let transport_dataset_id = match self.transport_kind {
            TransportKind::LocalTest => dataset_id.to_owned(),
            TransportKind::RcloneGoogleDrive => {
                namespaced_dataset_id(namespace.as_str(), dataset_id)?
            }
        };
        let observed_manifest = match cancellation {
            Some(token) => {
                self.transport
                    .lookup_cancellable(&transport_dataset_id, &manifest_name, token)?
            }
            None => self
                .transport
                .lookup(&transport_dataset_id, &manifest_name)?,
        }
        .ok_or(MarketDataError::UnknownOutcome)?;
        validate_remote_object(&observed_manifest, self.limits.max_manifest_bytes)?;
        let observed_object = match cancellation {
            Some(token) => {
                self.transport
                    .lookup_cancellable(&transport_dataset_id, &object_name, token)?
            }
            None => self.transport.lookup(&transport_dataset_id, &object_name)?,
        }
        .ok_or(MarketDataError::UnknownOutcome)?;
        validate_remote_object(&observed_object, self.limits.max_object_bytes)?;

        let namespace_dir = self.cache_root.join(namespace.as_str());
        fs::create_dir_all(&namespace_dir)?;
        require_plain_dir(&namespace_dir)?;
        let cache_dir = namespace_dir.join(dataset_id);
        if cache_dir.exists() {
            require_plain_dir(&cache_dir)?;
            let existing = self.read_verified_cache(
                namespace,
                dataset_id,
                &cache_dir,
                &observed_manifest,
                &observed_object,
            )?;
            let age = (self.now_unix_seconds)()
                .checked_sub(existing.verified_at_unix_seconds)
                .ok_or(MarketDataError::UnknownOutcome)?;
            if age < self.limits.ttl.as_secs() {
                let mut verified = verify_cache_contents_cancellable(
                    dataset_id,
                    &cache_dir,
                    &existing,
                    namespace,
                    &self.limits,
                    self.isolate_parquet_decode,
                    cancellation,
                )?;
                verified.cache_hit = true;
                verified._dataset_lock = Some(lock_file);
                return Ok(verified);
            }
        }

        let _budget_lock = self.lock_cache_budget()?;
        self.check_cache_budget(
            &cache_dir,
            observed_manifest.size_bytes,
            observed_object.size_bytes,
        )?;
        let partial_root = self.cache_root.join(".partial");
        fs::create_dir_all(&partial_root)?;
        require_plain_dir(&partial_root)?;
        let sequence = CACHE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let partial = partial_root.join(format!(
            "{}-{}-{}-{sequence}",
            namespace.as_str(),
            dataset_id,
            std::process::id()
        ));
        fs::create_dir(&partial)?;
        let mut cleanup = TempDirectoryCleanup(Some(partial.clone()));
        let manifest_path = partial.join(CACHE_MANIFEST_NAME);
        let object_path = partial.join(&object_name);
        match cancellation {
            Some(token) => self.transport.download_with_limit_cancellable(
                &transport_dataset_id,
                &manifest_name,
                &manifest_path,
                observed_manifest.size_bytes,
                token,
            )?,
            None => self.transport.download_with_limit(
                &transport_dataset_id,
                &manifest_name,
                &manifest_path,
                observed_manifest.size_bytes,
            )?,
        }
        let manifest_bytes =
            read_bounded_cancellable(&manifest_path, self.limits.max_manifest_bytes, cancellation)?;
        if manifest_bytes.len() as u64 != observed_manifest.size_bytes {
            return Err(MarketDataError::UnknownOutcome);
        }
        let manifest_sha256 = sha256(&manifest_bytes);
        let manifest: DatasetManifestV1 =
            serde_json::from_slice(&manifest_bytes).map_err(|_| MarketDataError::Contract)?;
        validate_manifest(
            namespace,
            dataset_id,
            self.transport_kind,
            &manifest,
            &observed_object,
        )?;
        match cancellation {
            Some(token) => self.transport.download_with_limit_cancellable(
                &transport_dataset_id,
                &object_name,
                &object_path,
                observed_object.size_bytes,
                token,
            )?,
            None => self.transport.download_with_limit(
                &transport_dataset_id,
                &object_name,
                &object_path,
                observed_object.size_bytes,
            )?,
        }
        let object_hash =
            hash_file_cancellable(&object_path, self.limits.max_object_bytes, cancellation)?;
        if object_hash.size_bytes != observed_object.size_bytes
            || object_hash.content_sha256 != manifest.object.content_sha256
        {
            return Err(MarketDataError::Conflict);
        }
        let schema_id = schema_id_for_hash(&manifest.object.parquet_schema_sha256)?;
        let parquet = if self.isolate_parquet_decode {
            match cancellation {
                Some(token) => parquet_worker::verify_cancellable(
                    &object_path,
                    schema_id,
                    self.limits.max_object_bytes,
                    token.clone(),
                )?,
                None => {
                    parquet_worker::verify(&object_path, schema_id, self.limits.max_object_bytes)?
                }
            }
        } else {
            if cancellation.is_some() {
                return Err(MarketDataError::Storage(
                    crate::error::StorageFailure::Unsupported,
                ));
            }
            parquet_store::verify_with_limit(&object_path, schema_id, self.limits.max_object_bytes)?
        };
        if let Some(token) = cancellation {
            ensure_not_cancelled(token)?;
        }
        validate_manifest_facts(&manifest, &parquet)?;

        let receipt = RemoteCacheReceiptV1 {
            version: 1,
            namespace,
            dataset_id: dataset_id.to_owned(),
            manifest_remote_id: observed_manifest.id.clone(),
            manifest_remote_size_bytes: observed_manifest.size_bytes,
            manifest_remote_md5: observed_manifest.md5.clone(),
            manifest_sha256,
            object_remote_id: observed_object.id.clone(),
            object_remote_size_bytes: observed_object.size_bytes,
            object_remote_md5: observed_object.md5.clone(),
            object_sha256: object_hash.content_sha256.clone(),
            parquet_schema_sha256: parquet.schema_sha256.clone(),
            verified_at_unix_seconds: (self.now_unix_seconds)(),
        };
        if let Some(token) = cancellation {
            ensure_not_cancelled(token)?;
        }
        write_new_synced(
            &partial.join(CACHE_RECEIPT_NAME),
            &serde_json::to_vec(&receipt)?,
        )?;
        File::open(&partial)?.sync_all()?;
        if let Some(token) = cancellation {
            ensure_not_cancelled(token)?;
        }

        if cache_dir.exists() {
            let previous = self.read_receipt(&cache_dir, namespace, dataset_id)?;
            if previous.manifest_remote_id != receipt.manifest_remote_id
                || previous.manifest_sha256 != receipt.manifest_sha256
                || previous.object_remote_id != receipt.object_remote_id
                || previous.object_sha256 != receipt.object_sha256
                || previous.parquet_schema_sha256 != receipt.parquet_schema_sha256
            {
                return Err(MarketDataError::Conflict);
            }
            let mut published_cleanup = PublishedCacheCleanup::new(cache_dir.clone());
            replace_verified_file(
                &partial.join(CACHE_MANIFEST_NAME),
                &cache_dir.join(CACHE_MANIFEST_NAME),
            )?;
            replace_verified_file(&object_path, &cache_dir.join(&object_name))?;
            replace_verified_file(
                &partial.join(CACHE_RECEIPT_NAME),
                &cache_dir.join(CACHE_RECEIPT_NAME),
            )?;
            File::open(&cache_dir)?.sync_all()?;
            fs::remove_dir(&partial)?;
            cleanup.0 = None;
            if let Some(token) = cancellation {
                ensure_not_cancelled(token)?;
            }
            let mut verified = verify_cache_contents_cancellable(
                dataset_id,
                &cache_dir,
                &receipt,
                namespace,
                &self.limits,
                self.isolate_parquet_decode,
                cancellation,
            )?;
            verified.cache_hit = false;
            verified._dataset_lock = Some(lock_file);
            published_cleanup.keep();
            return Ok(verified);
        } else {
            fs::rename(&partial, &cache_dir)?;
            let mut published_cleanup = PublishedCacheCleanup::new(cache_dir.clone());
            File::open(&namespace_dir)?.sync_all()?;
            if let Some(token) = cancellation {
                ensure_not_cancelled(token)?;
            }
            let mut verified = verify_cache_contents_cancellable(
                dataset_id,
                &cache_dir,
                &receipt,
                namespace,
                &self.limits,
                self.isolate_parquet_decode,
                cancellation,
            )?;
            verified.cache_hit = false;
            verified._dataset_lock = Some(lock_file);
            published_cleanup.keep();
            cleanup.0 = None;
            return Ok(verified);
        }
        #[allow(unreachable_code)]
        Err(MarketDataError::Storage(
            crate::error::StorageFailure::CommandFailed,
        ))
    }

    fn check_cache_budget(
        &self,
        cache_dir: &Path,
        manifest_size: u64,
        object_size: u64,
    ) -> Result<()> {
        let bytes = directory_bytes_bounded(&self.cache_root)?;
        let reserve = manifest_size
            .checked_add(object_size)
            .and_then(|total| total.checked_add(MAX_CACHE_RECEIPT_BYTES))
            .ok_or(MarketDataError::InputLimit)?;
        if bytes
            .checked_add(reserve)
            .is_none_or(|total| total > self.limits.max_total_cache_bytes)
        {
            return Err(MarketDataError::InputLimit);
        }
        let entries = cache_entry_count(&self.cache_root)?;
        if !cache_dir.exists() && entries >= self.limits.max_entries {
            return Err(MarketDataError::InputLimit);
        }
        Ok(())
    }

    fn lock_cache_budget(&self) -> Result<File> {
        let lock_dir = self.cache_root.join(".locks");
        fs::create_dir_all(&lock_dir)?;
        require_plain_dir(&lock_dir)?;
        let lock_path = lock_dir.join("cache-budget.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        file.try_lock_exclusive()
            .map_err(|_| MarketDataError::LockHeld)?;
        Ok(file)
    }

    fn read_receipt(
        &self,
        cache_dir: &Path,
        namespace: DatasetNamespace,
        dataset_id: &str,
    ) -> Result<RemoteCacheReceiptV1> {
        read_cache_receipt(cache_dir, namespace, dataset_id)
    }

    fn read_verified_cache(
        &self,
        namespace: DatasetNamespace,
        dataset_id: &str,
        cache_dir: &Path,
        observed_manifest: &RemoteObject,
        observed_object: &RemoteObject,
    ) -> Result<RemoteCacheReceiptV1> {
        let receipt = self.read_receipt(cache_dir, namespace, dataset_id)?;
        if receipt.manifest_remote_id != observed_manifest.id
            || receipt.manifest_remote_size_bytes != observed_manifest.size_bytes
            || receipt.manifest_remote_md5 != observed_manifest.md5
            || receipt.object_remote_id != observed_object.id
            || receipt.object_remote_size_bytes != observed_object.size_bytes
            || receipt.object_remote_md5 != observed_object.md5
        {
            return Err(MarketDataError::Conflict);
        }
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests;
