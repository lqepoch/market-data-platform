use super::*;

pub struct RemoteCacheCleaner {
    cache_root: PathBuf,
    limits: RemoteCacheLimits,
    now_unix_seconds: Arc<dyn Fn() -> u64 + Send + Sync>,
    isolate_parquet_decode: bool,
}

impl fmt::Debug for RemoteCacheCleaner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteCacheCleaner")
            .field("cache_root", &"redacted")
            .field("limits", &self.limits)
            .finish()
    }
}

impl RemoteCacheCleaner {
    pub fn new(cache_root: impl Into<PathBuf>, limits: RemoteCacheLimits) -> Result<Self> {
        Self::with_clock(cache_root, limits, Arc::new(system_time_unix_seconds))
    }

    /// Construct a cleaner that verifies cached Parquet only in a resource-limited worker.
    pub fn new_isolated(cache_root: impl Into<PathBuf>, limits: RemoteCacheLimits) -> Result<Self> {
        let mut cleaner = Self::new(cache_root, limits)?;
        cleaner.isolate_parquet_decode = true;
        Ok(cleaner)
    }

    pub(super) fn with_clock(
        cache_root: impl Into<PathBuf>,
        limits: RemoteCacheLimits,
        now_unix_seconds: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Result<Self> {
        limits.validate()?;
        let cache_root = cache_root.into();
        fs::create_dir_all(&cache_root)?;
        require_plain_dir(&cache_root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&cache_root, fs::Permissions::from_mode(0o700))?;
        }
        Ok(Self {
            cache_root: fs::canonicalize(cache_root)?,
            limits,
            now_unix_seconds,
            isolate_parquet_decode: false,
        })
    }

    /// Scans at most `limits.max_entries` cache directories. `apply=false` is report-only.
    pub fn cleanup_expired(&self, apply: bool) -> Result<RemoteCacheCleanupReport> {
        let (candidates, scan_limited) = self.cache_candidates()?;
        let mut report = RemoteCacheCleanupReport {
            dry_run: !apply,
            scan_limited,
            ..RemoteCacheCleanupReport::default()
        };

        for (namespace, dataset_id, cache_dir) in candidates {
            report.scanned += 1;
            let lock_file = match try_lock_dataset(&self.cache_root, namespace, &dataset_id) {
                Ok(Some(file)) => file,
                Ok(None) => {
                    report.active_skipped += 1;
                    continue;
                }
                Err(_) => {
                    report.unknown_preserved += 1;
                    continue;
                }
            };

            if validate_cache_layout(&cache_dir, &dataset_id).is_err() {
                report.unknown_preserved += 1;
                drop(lock_file);
                continue;
            }
            let receipt = match read_cache_receipt(&cache_dir, namespace, &dataset_id) {
                Ok(receipt) => receipt,
                Err(_) => {
                    report.unknown_preserved += 1;
                    drop(lock_file);
                    continue;
                }
            };
            let verified = match verify_cache_contents(
                &dataset_id,
                &cache_dir,
                &receipt,
                namespace,
                &self.limits,
                self.isolate_parquet_decode,
            ) {
                Ok(cache) => cache,
                Err(_) => {
                    report.unknown_preserved += 1;
                    drop(lock_file);
                    continue;
                }
            };
            let age = match (self.now_unix_seconds)().checked_sub(receipt.verified_at_unix_seconds)
            {
                Some(age) => age,
                None => {
                    report.unknown_preserved += 1;
                    drop(verified);
                    drop(lock_file);
                    continue;
                }
            };
            if age < self.limits.ttl.as_secs() {
                report.fresh += 1;
                drop(verified);
                drop(lock_file);
                continue;
            }
            report.expired_verified += 1;
            if !apply {
                drop(verified);
                drop(lock_file);
                continue;
            }

            // Cache misses lock dataset first and then the global budget; keep the same order.
            let budget_lock = match self.lock_cache_budget() {
                Ok(file) => file,
                Err(error) => {
                    drop(verified);
                    drop(lock_file);
                    return Err(error);
                }
            };
            validate_cache_layout(&cache_dir, &dataset_id)?;
            let current_receipt = read_cache_receipt(&cache_dir, namespace, &dataset_id)?;
            let current = verify_cache_contents(
                &dataset_id,
                &cache_dir,
                &current_receipt,
                namespace,
                &self.limits,
                self.isolate_parquet_decode,
            )?;
            let current_age = match (self.now_unix_seconds)()
                .checked_sub(current_receipt.verified_at_unix_seconds)
            {
                Some(age) => age,
                None => {
                    report.unknown_preserved += 1;
                    drop(current);
                    drop(verified);
                    drop(budget_lock);
                    drop(lock_file);
                    continue;
                }
            };
            if current_age < self.limits.ttl.as_secs()
                || current.content_sha256 != verified.content_sha256
                || current_receipt.verified_at_unix_seconds != receipt.verified_at_unix_seconds
            {
                report.unknown_preserved += 1;
                drop(current);
                drop(verified);
                drop(budget_lock);
                drop(lock_file);
                continue;
            }
            let bytes = match directory_bytes_bounded(&cache_dir) {
                Ok(bytes) => bytes,
                Err(_) => {
                    report.unknown_preserved += 1;
                    drop(current);
                    drop(verified);
                    drop(budget_lock);
                    drop(lock_file);
                    continue;
                }
            };
            drop(current);
            drop(verified);
            // Remove the receipt first: after interruption, a partial directory is unknown
            // and will be preserved rather than mistaken for a verified cache on restart.
            fs::remove_file(cache_dir.join(CACHE_RECEIPT_NAME))?;
            fs::remove_file(cache_dir.join(CACHE_MANIFEST_NAME))?;
            fs::remove_file(cache_dir.join(format!("{dataset_id}.parquet")))?;
            fs::remove_dir(&cache_dir)?;
            File::open(cache_dir.parent().ok_or(MarketDataError::InvalidInput)?)?.sync_all()?;
            report.evicted += 1;
            report.bytes_reclaimed = report
                .bytes_reclaimed
                .checked_add(bytes)
                .ok_or(MarketDataError::InputLimit)?;
            drop(budget_lock);
            drop(lock_file);
        }
        Ok(report)
    }

    fn cache_candidates(&self) -> Result<CacheCandidateList> {
        let mut candidates = Vec::new();
        let mut scan_limited = false;
        for (namespace, name) in [
            (DatasetNamespace::Curated, "curated"),
            (DatasetNamespace::Diagnostic, "diagnostic"),
        ] {
            let root = self.cache_root.join(name);
            if !root.exists() {
                continue;
            }
            require_plain_dir(&root)?;
            let entries = fs::read_dir(root)?;
            let mut entries_seen = 0usize;
            for entry in entries {
                entries_seen += 1;
                if entries_seen > self.limits.max_entries {
                    scan_limited = true;
                    break;
                }
                let entry = entry?;
                let kind = entry.file_type()?;
                if kind.is_dir() || kind.is_symlink() {
                    if candidates.len() == self.limits.max_entries {
                        scan_limited = true;
                        break;
                    }
                    let dataset_id = match entry.file_name().into_string() {
                        Ok(value) => value,
                        Err(_) => {
                            scan_limited = true;
                            continue;
                        }
                    };
                    candidates.push((namespace, dataset_id, entry.path()));
                }
            }
            if scan_limited {
                break;
            }
        }
        candidates.sort_by(|left, right| {
            left.0
                .as_str()
                .cmp(right.0.as_str())
                .then_with(|| left.1.cmp(&right.1))
        });
        Ok((candidates, scan_limited))
    }

    fn lock_cache_budget(&self) -> Result<File> {
        let lock_dir = self.cache_root.join(".locks");
        fs::create_dir_all(&lock_dir)?;
        require_plain_dir(&lock_dir)?;
        let path = lock_dir.join("cache-budget.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock_exclusive()
            .map_err(|_| MarketDataError::LockHeld)?;
        Ok(file)
    }
}

type CacheCandidate = (DatasetNamespace, String, PathBuf);
type CacheCandidateList = (Vec<CacheCandidate>, bool);

#[derive(Clone, Debug, Default, Serialize)]
pub struct RemoteCacheCleanupReport {
    pub dry_run: bool,
    pub scan_limited: bool,
    pub scanned: usize,
    pub fresh: usize,
    pub expired_verified: usize,
    pub evicted: usize,
    pub active_skipped: usize,
    pub unknown_preserved: usize,
    #[serde(with = "market_contracts::wire_u64")]
    pub bytes_reclaimed: u64,
}

fn try_lock_dataset(
    cache_root: &Path,
    namespace: DatasetNamespace,
    dataset_id: &str,
) -> Result<Option<File>> {
    if !safe_component(dataset_id)
        || dataset_id.starts_with('.')
        || dataset_id.ends_with('.')
        || dataset_id.contains("..")
    {
        return Err(MarketDataError::InvalidInput);
    }
    let lock_dir = cache_root.join(".locks");
    fs::create_dir_all(&lock_dir)?;
    require_plain_dir(&lock_dir)?;
    let path = lock_dir.join(format!("{}-{dataset_id}.lock", namespace.as_str()));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(MarketDataError::Io(error)),
    }
}

fn validate_cache_layout(cache_dir: &Path, dataset_id: &str) -> Result<()> {
    require_plain_dir(cache_dir)?;
    let mut actual = Vec::new();
    for entry in fs::read_dir(cache_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(MarketDataError::UnknownOutcome);
        }
        actual.push(
            entry
                .file_name()
                .into_string()
                .map_err(|_| MarketDataError::UnknownOutcome)?,
        );
    }
    actual.sort();
    let mut expected = vec![
        CACHE_MANIFEST_NAME.to_owned(),
        CACHE_RECEIPT_NAME.to_owned(),
        format!("{dataset_id}.parquet"),
    ];
    expected.sort();
    if actual != expected {
        return Err(MarketDataError::UnknownOutcome);
    }
    Ok(())
}

pub(super) fn read_cache_receipt(
    cache_dir: &Path,
    namespace: DatasetNamespace,
    dataset_id: &str,
) -> Result<RemoteCacheReceiptV1> {
    let bytes = read_bounded(&cache_dir.join(CACHE_RECEIPT_NAME), MAX_CACHE_RECEIPT_BYTES)?;
    let receipt: RemoteCacheReceiptV1 =
        serde_json::from_slice(&bytes).map_err(|_| MarketDataError::UnknownOutcome)?;
    if receipt.version != 1
        || receipt.namespace != namespace
        || receipt.dataset_id != dataset_id
        || !valid_external_id(&receipt.manifest_remote_id)
        || !valid_external_id(&receipt.object_remote_id)
        || !valid_sha256(&receipt.manifest_sha256)
        || !valid_sha256(&receipt.object_sha256)
        || !valid_sha256(&receipt.parquet_schema_sha256)
        || receipt.manifest_remote_size_bytes == 0
        || receipt.object_remote_size_bytes == 0
    {
        return Err(MarketDataError::UnknownOutcome);
    }
    Ok(receipt)
}

pub(super) fn verify_cache_contents(
    dataset_id: &str,
    cache_dir: &Path,
    receipt: &RemoteCacheReceiptV1,
    namespace: DatasetNamespace,
    limits: &RemoteCacheLimits,
    isolate_parquet_decode: bool,
) -> Result<VerifiedCache> {
    verify_cache_contents_cancellable(
        dataset_id,
        cache_dir,
        receipt,
        namespace,
        limits,
        isolate_parquet_decode,
        None,
    )
}

pub(super) fn verify_cache_contents_cancellable(
    dataset_id: &str,
    cache_dir: &Path,
    receipt: &RemoteCacheReceiptV1,
    namespace: DatasetNamespace,
    limits: &RemoteCacheLimits,
    isolate_parquet_decode: bool,
    cancellation: Option<&CancellationToken>,
) -> Result<VerifiedCache> {
    if let Some(token) = cancellation {
        ensure_not_cancelled(token)?;
    }
    validate_cache_layout(cache_dir, dataset_id)?;
    let manifest_path = cache_dir.join(CACHE_MANIFEST_NAME);
    let manifest_bytes =
        read_bounded_cancellable(&manifest_path, limits.max_manifest_bytes, cancellation)?;
    if sha256(&manifest_bytes) != receipt.manifest_sha256
        || manifest_bytes.len() as u64 != receipt.manifest_remote_size_bytes
    {
        return Err(MarketDataError::Conflict);
    }
    let manifest: DatasetManifestV1 =
        serde_json::from_slice(&manifest_bytes).map_err(|_| MarketDataError::Contract)?;
    let object_name = format!("{dataset_id}.parquet");
    if manifest.dataset_id != dataset_id
        || manifest.object.object_name != object_name
        || !safe_object_name(&manifest.object.object_name)
        || manifest.object.object_id.as_deref() != Some(&receipt.object_remote_id)
    {
        return Err(MarketDataError::Conflict);
    }
    manifest.validate().map_err(|_| MarketDataError::Contract)?;
    validate_namespace(namespace, &manifest)?;
    let object_path = cache_dir.join(object_name);
    let object_hash = hash_file_cancellable(&object_path, limits.max_object_bytes, cancellation)?;
    if object_hash.size_bytes != receipt.object_remote_size_bytes
        || object_hash.content_sha256 != receipt.object_sha256
        || object_hash.content_sha256 != manifest.object.content_sha256
    {
        return Err(MarketDataError::Conflict);
    }
    let schema_id = schema_id_for_hash(&manifest.object.parquet_schema_sha256)?;
    let parquet = if isolate_parquet_decode {
        match cancellation {
            Some(token) => parquet_worker::verify_cancellable(
                &object_path,
                schema_id,
                limits.max_object_bytes,
                token.clone(),
            )?,
            None => parquet_worker::verify(&object_path, schema_id, limits.max_object_bytes)?,
        }
    } else {
        parquet_store::verify_with_limit(&object_path, schema_id, limits.max_object_bytes)?
    };
    validate_manifest_facts(&manifest, &parquet)?;
    if receipt.parquet_schema_sha256 != parquet.schema_sha256 {
        return Err(MarketDataError::ParquetSchema);
    }
    if let Some(token) = cancellation {
        ensure_not_cancelled(token)?;
    }
    Ok(VerifiedCache {
        path: object_path,
        manifest,
        parquet,
        content_sha256: object_hash.content_sha256,
        cache_hit: false,
        _dataset_lock: None,
    })
}
