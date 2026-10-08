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
