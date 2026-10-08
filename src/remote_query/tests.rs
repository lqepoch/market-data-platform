use super::*;
use std::sync::atomic::AtomicUsize;

use crate::storage::LocalTestTransport;

#[derive(Clone)]
struct CountingTransport {
    inner: LocalTestTransport,
    downloads: Arc<AtomicUsize>,
    lookups: Arc<AtomicUsize>,
}

impl ObjectTransport for CountingTransport {
    fn lookup(&self, dataset_id: &str, object_name: &str) -> Result<Option<RemoteObject>> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        self.inner.lookup(dataset_id, object_name)
    }

    fn upload_immutable(
        &self,
        local_file: &Path,
        dataset_id: &str,
        object_name: &str,
    ) -> Result<()> {
        self.inner
            .upload_immutable(local_file, dataset_id, object_name)
    }

    fn download_with_limit(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<()> {
        self.downloads.fetch_add(1, Ordering::Relaxed);
        self.inner
            .download_with_limit(dataset_id, object_name, destination, max_bytes)
    }
}

async fn seed_synthetic_archive(output: &Path) -> PathBuf {
    crate::pipeline::synthetic_replay(output).await.unwrap();
    output.join("local-test-store")
}

fn reader_with_clock(
    transport: CountingTransport,
    cache_root: PathBuf,
    now: Arc<AtomicU64>,
    limits: RemoteCacheLimits,
) -> RemoteArchiveReader {
    RemoteArchiveReader::with_clock(
        Arc::new(transport),
        TransportKind::LocalTest,
        cache_root,
        limits,
        Arc::new(move || now.load(Ordering::Relaxed)),
    )
    .unwrap()
}

#[tokio::test]
async fn remote_query_hash_verifies_then_reuses_only_fresh_cache() {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = seed_synthetic_archive(&temp.path().join("remote")).await;
    let downloads = Arc::new(AtomicUsize::new(0));
    let lookups = Arc::new(AtomicUsize::new(0));
    let transport = CountingTransport {
        inner: LocalTestTransport::new(remote_root).unwrap(),
        downloads: Arc::clone(&downloads),
        lookups: Arc::clone(&lookups),
    };
    let now = Arc::new(AtomicU64::new(1_000));
    let reader = reader_with_clock(
        transport,
        temp.path().join("cache"),
        Arc::clone(&now),
        RemoteCacheLimits::default(),
    );
    let dataset_id = "synthetic-2026-10-08-four-bars-v1-bars-1m-v1";

    let (bars, first) = reader
        .query_bars(DatasetNamespace::Diagnostic, dataset_id, Some("QQQ"))
        .unwrap();
    assert_eq!(bars.len(), 4);
    assert_eq!(first.row_count, 4);
    assert_eq!(first.returned_rows, 4);
    assert!(!first.cache_hit);
    assert_eq!(downloads.load(Ordering::Relaxed), 2);

    let (_, cached) = reader
        .query_bars(DatasetNamespace::Diagnostic, dataset_id, None)
        .unwrap();
    assert!(cached.cache_hit);
    assert_eq!(downloads.load(Ordering::Relaxed), 2);
    assert_eq!(lookups.load(Ordering::Relaxed), 4);

    now.fetch_add(DEFAULT_REMOTE_CACHE_TTL.as_secs() + 1, Ordering::Relaxed);
    let (_, refreshed) = reader
        .query_bars(DatasetNamespace::Diagnostic, dataset_id, None)
        .unwrap();
    assert!(!refreshed.cache_hit);
    assert_eq!(downloads.load(Ordering::Relaxed), 4);
}

#[test]
fn cache_budget_lock_serializes_misses_across_reader_instances() {
    let temp = tempfile::tempdir().unwrap();
    let remote = LocalTestTransport::new(temp.path().join("remote")).unwrap();
    let cache = temp.path().join("cache");
    let first = RemoteArchiveReader::local_test(
        remote.clone(),
        cache.clone(),
        RemoteCacheLimits::default(),
    )
    .unwrap();
    let second =
        RemoteArchiveReader::local_test(remote, cache, RemoteCacheLimits::default()).unwrap();

    let first_lock = first.lock_cache_budget().unwrap();
    assert!(matches!(
        second.lock_cache_budget(),
        Err(MarketDataError::LockHeld)
    ));
    drop(first_lock);
    assert!(second.lock_cache_budget().is_ok());
}

#[tokio::test]
async fn remote_query_rejects_corrupt_cache_without_using_unverified_rows() {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = seed_synthetic_archive(&temp.path().join("remote")).await;
    let transport = CountingTransport {
        inner: LocalTestTransport::new(remote_root).unwrap(),
        downloads: Arc::new(AtomicUsize::new(0)),
        lookups: Arc::new(AtomicUsize::new(0)),
    };
    let reader = reader_with_clock(
        transport,
        temp.path().join("cache"),
        Arc::new(AtomicU64::new(1_000)),
        RemoteCacheLimits::default(),
    );
    let dataset_id = "synthetic-2026-10-08-four-bars-v1-bars-1m-v1";
    reader
        .query_bars(DatasetNamespace::Diagnostic, dataset_id, None)
        .unwrap();

    let cached_object = temp
        .path()
        .join("cache/diagnostic")
        .join(dataset_id)
        .join(format!("{dataset_id}.parquet"));
    fs::write(&cached_object, b"changed after verified download").unwrap();
    assert!(matches!(
        reader.query_bars(DatasetNamespace::Diagnostic, dataset_id, None),
        Err(MarketDataError::Conflict)
    ));
}

#[tokio::test]
async fn curated_namespace_rejects_synthetic_before_downloading_parquet() {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = seed_synthetic_archive(&temp.path().join("remote")).await;
    let downloads = Arc::new(AtomicUsize::new(0));
    let transport = CountingTransport {
        inner: LocalTestTransport::new(remote_root).unwrap(),
        downloads: Arc::clone(&downloads),
        lookups: Arc::new(AtomicUsize::new(0)),
    };
    let reader = reader_with_clock(
        transport,
        temp.path().join("cache"),
        Arc::new(AtomicU64::new(1_000)),
        RemoteCacheLimits::default(),
    );
    assert!(matches!(
        reader.query_bars(
            DatasetNamespace::Curated,
            "synthetic-2026-10-08-four-bars-v1-bars-1m-v1",
            None
        ),
        Err(MarketDataError::PublicationNotAuthorized)
    ));
    assert_eq!(downloads.load(Ordering::Relaxed), 1);
    assert!(
        !temp
            .path()
            .join("cache/curated/synthetic-2026-10-08-four-bars-v1-bars-1m-v1")
            .exists()
    );
}

#[tokio::test]
async fn remote_query_and_jsonl_export_obey_result_and_output_byte_caps() {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = seed_synthetic_archive(&temp.path().join("remote")).await;
    let transport = CountingTransport {
        inner: LocalTestTransport::new(remote_root).unwrap(),
        downloads: Arc::new(AtomicUsize::new(0)),
        lookups: Arc::new(AtomicUsize::new(0)),
    };
    let limits = RemoteCacheLimits {
        max_query_rows: 3,
        ..RemoteCacheLimits::default()
    };
    let reader = reader_with_clock(
        transport,
        temp.path().join("cache"),
        Arc::new(AtomicU64::new(1_000)),
        limits,
    );
    let dataset_id = "synthetic-2026-10-08-four-bars-v1-bars-1m-v1";
    assert!(matches!(
        reader.query_bars(DatasetNamespace::Diagnostic, dataset_id, None),
        Err(MarketDataError::InputLimit)
    ));

    let limits = RemoteCacheLimits {
        max_export_bytes: 1,
        ..RemoteCacheLimits::default()
    };
    let reader = reader_with_clock(
        CountingTransport {
            inner: LocalTestTransport::new(temp.path().join("remote/local-test-store")).unwrap(),
            downloads: Arc::new(AtomicUsize::new(0)),
            lookups: Arc::new(AtomicUsize::new(0)),
        },
        temp.path().join("cache-2"),
        Arc::new(AtomicU64::new(1_000)),
        limits,
    );
    let export = temp.path().join("too-large.jsonl");
    assert!(matches!(
        reader.export_bars_jsonl(DatasetNamespace::Diagnostic, dataset_id, None, &export),
        Err(MarketDataError::InputLimit)
    ));
    assert!(!export.exists());
}

#[tokio::test]
async fn cleanup_remote_cache_is_dry_run_by_default_and_evicts_only_expired_verified_entry() {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = seed_synthetic_archive(&temp.path().join("remote")).await;
    let downloads = Arc::new(AtomicUsize::new(0));
    let transport = CountingTransport {
        inner: LocalTestTransport::new(remote_root).unwrap(),
        downloads: Arc::clone(&downloads),
        lookups: Arc::new(AtomicUsize::new(0)),
    };
    let now = Arc::new(AtomicU64::new(1_000));
    let cache_root = temp.path().join("cache");
    let reader = reader_with_clock(
        transport.clone(),
        cache_root.clone(),
        Arc::clone(&now),
        RemoteCacheLimits::default(),
    );
    let dataset_id = "synthetic-2026-10-08-four-bars-v1-bars-1m-v1";
    reader
        .query_bars(DatasetNamespace::Diagnostic, dataset_id, None)
        .unwrap();
    let cached_dir = cache_root.join("diagnostic").join(dataset_id);
    assert!(cached_dir.is_dir());
    now.fetch_add(DEFAULT_REMOTE_CACHE_TTL.as_secs() + 1, Ordering::Relaxed);

    let cleaner = RemoteCacheCleaner::with_clock(
        cache_root.clone(),
        RemoteCacheLimits::default(),
        Arc::new({
            let now = Arc::clone(&now);
            move || now.load(Ordering::Relaxed)
        }),
    )
    .unwrap();
    let dry_run = cleaner.cleanup_expired(false).unwrap();
    assert!(dry_run.dry_run);
    assert_eq!(dry_run.expired_verified, 1);
    assert_eq!(dry_run.evicted, 0);
    assert_eq!(dry_run.bytes_reclaimed, 0);
    assert!(cached_dir.is_dir());

    let applied = cleaner.cleanup_expired(true).unwrap();
    assert!(!applied.dry_run);
    assert_eq!(applied.evicted, 1);
    assert!(applied.bytes_reclaimed > 0);
    assert!(!cached_dir.exists());
    assert!(cache_root.join(".locks").is_dir());

    let (_, refreshed) = reader
        .query_bars(DatasetNamespace::Diagnostic, dataset_id, None)
        .unwrap();
    assert!(!refreshed.cache_hit);
    assert_eq!(downloads.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn cleanup_remote_cache_skips_active_entries_and_preserves_unknown_receipts() {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = seed_synthetic_archive(&temp.path().join("remote")).await;
    let transport = CountingTransport {
        inner: LocalTestTransport::new(remote_root).unwrap(),
        downloads: Arc::new(AtomicUsize::new(0)),
        lookups: Arc::new(AtomicUsize::new(0)),
    };
    let now = Arc::new(AtomicU64::new(2_000));
    let cache_root = temp.path().join("cache");
    let reader = reader_with_clock(
        transport,
        cache_root.clone(),
        Arc::clone(&now),
        RemoteCacheLimits::default(),
    );
    let dataset_id = "synthetic-2026-10-08-four-bars-v1-bars-1m-v1";
    reader
        .query_bars(DatasetNamespace::Diagnostic, dataset_id, None)
        .unwrap();

    // Keep the reader's dataset lock alive while cleanup attempts eviction.
    let active = reader
        .ensure_cached(DatasetNamespace::Diagnostic, dataset_id)
        .unwrap();
    now.fetch_add(DEFAULT_REMOTE_CACHE_TTL.as_secs() + 1, Ordering::Relaxed);
    let cleaner = RemoteCacheCleaner::with_clock(
        cache_root.clone(),
        RemoteCacheLimits::default(),
        Arc::new({
            let now = Arc::clone(&now);
            move || now.load(Ordering::Relaxed)
        }),
    )
    .unwrap();
    let active_report = cleaner.cleanup_expired(true).unwrap();
    assert_eq!(active_report.active_skipped, 1);
    let cached_dir = cache_root.join("diagnostic").join(dataset_id);
    assert!(cached_dir.is_dir());
    drop(active);

    fs::write(
        cached_dir.join("operator-note.txt"),
        b"unknown cache content",
    )
    .unwrap();
    let layout_report = cleaner.cleanup_expired(true).unwrap();
    assert_eq!(layout_report.unknown_preserved, 1);
    assert!(cached_dir.is_dir());
    fs::remove_file(cached_dir.join("operator-note.txt")).unwrap();

    fs::write(cached_dir.join(CACHE_RECEIPT_NAME), b"not a receipt").unwrap();
    let unknown_report = cleaner.cleanup_expired(true).unwrap();
    assert_eq!(unknown_report.unknown_preserved, 1);
    assert_eq!(unknown_report.evicted, 0);
    assert!(cached_dir.is_dir());
}
