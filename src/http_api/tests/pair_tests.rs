use std::{
    fs,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use axum::{Router, body::to_bytes, http::StatusCode};
use fs2::FileExt;
use serde_json::Value;
use tower::ServiceExt;

use crate::{
    archive::{ArchiveLimits, capture_pair_v2::reader::LocalCapturePairV2Reader},
    remote_query::{RemoteArchiveReader, RemoteCacheLimits},
    storage::LocalTestTransport,
};

use super::{assert_no_store, request};
use crate::http_api::{
    AuthConfig, ServiceState, app_router, auth::test_support, supervisor::QuerySupervisor,
};

async fn make_reviewed_pair(output_root: &Path) -> String {
    crate::capture_synthetic::run(output_root.to_path_buf(), std::future::pending())
        .await
        .unwrap();
    let pair_state = output_root.join("archive-state/capture-pair-v2");
    let receipt = fs::read_dir(pair_state)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .find(|name| name.starts_with("chunk-") && name.ends_with(".receipt.json"))
        .unwrap();
    receipt
        .strip_prefix("chunk-")
        .unwrap()
        .strip_suffix(".receipt.json")
        .unwrap()
        .to_owned()
}

async fn make_synthetic_provider_pair(output_root: &Path) -> String {
    use broker_ports::{
        RawFrameCapture, RawFrameDisposition, RawFrameFinalization, RawFramePayload,
        RawFrameSinkFactory, RawFrameWireEncoding, RawMarketFrame,
    };
    use market_contracts::{
        DecimalString, EventMetadataV1, MarketDataSourceV1, MarketEventEnvelopeV1, MarketEventV1,
        NumericEncodingV1,
    };

    const RECEIVED: &str = "2020-01-02T13:30:00Z";
    const WIRE: &[u8] = br#"{"fixture":true,"T":"t","S":"QQQ","p":600.25,"s":1}"#;

    let spool = crate::archive::LocalRawFrameSpoolFactory::open(
        output_root.join("raw-spool"),
        crate::archive::RawFrameSpoolLimits::default(),
    )
    .unwrap();
    let sink = spool.create_sink("synthetic", "synthetic").unwrap();
    let capture_id = sink.capture_instance_id();
    let received_at = market_contracts::UtcTimestamp::parse(RECEIVED).unwrap();
    let capture = RawFrameCapture::new(
        capture_id,
        "synthetic",
        "synthetic",
        market_contracts::EntitlementState::Unknown,
        1,
        1,
        received_at.clone(),
        RawFrameWireEncoding::Json,
        RawFramePayload::capture(WIRE.to_vec()).unwrap(),
    )
    .unwrap();
    let predecode_ack = sink.persist_before_decode(&capture).await.unwrap();
    assert!(predecode_ack.matches(&capture));
    let finalization = RawFrameFinalization::new(
        1,
        vec!["QQQ".to_owned()],
        Some(NumericEncodingV1::DecimalToken),
        RawFrameDisposition::DecodedMarketData,
    )
    .unwrap();
    let finalization_ack = sink
        .finalize_after_decode(&predecode_ack, &finalization)
        .await
        .unwrap();
    assert!(finalization_ack.matches(&predecode_ack, &finalization));

    let frame = RawMarketFrame {
        provider: "synthetic".to_owned(),
        feed: "synthetic".to_owned(),
        entitlement: market_contracts::EntitlementState::Unknown,
        capture_key: Some(capture.capture_key().clone()),
        wire_encoding: RawFrameWireEncoding::Json,
        numeric_encoding: Some(NumericEncodingV1::DecimalToken),
        generation: 1,
        frame_sequence: 1,
        received_timestamp_utc: received_at.clone(),
        event_count: 1,
        symbols: vec!["QQQ".to_owned()],
        disposition: RawFrameDisposition::DecodedMarketData,
        payload: capture.payload().clone(),
    };
    let source = MarketDataSourceV1::new(
        "synthetic",
        "synthetic",
        market_contracts::EntitlementState::Unknown,
        NumericEncodingV1::DecimalToken,
        None,
    )
    .unwrap();
    let event = MarketEventEnvelopeV1 {
        metadata: EventMetadataV1 {
            schema_version: 1,
            source,
            generation: 1,
            sequence: 1,
            raw_frame_sha256: Some(capture.capture_key().frame_sha256().to_owned()),
            source_timestamp: None,
            received_timestamp: received_at,
        },
        event: MarketEventV1::StockTrade {
            symbol: "QQQ".to_owned(),
            price: DecimalString::new("600.25").unwrap(),
            size: DecimalString::new("1").unwrap(),
        },
    };
    let publisher = crate::archive::ArchivePublisher::local_test(
        LocalTestTransport::new(output_root.join("local-test-archive")).unwrap(),
        output_root.join("archive-state"),
        output_root.join("staging"),
        ArchiveLimits::default(),
    )
    .unwrap();
    publisher
        .publish_local_synthetic_capture_pair_v2(&spool, capture_id, &[frame], &[event])
        .unwrap();
    spool.shutdown().await;

    fs::read_dir(output_root.join("archive-state/capture-pair-v2"))
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .find(|name| name.starts_with("chunk-") && name.ends_with(".receipt.json"))
        .unwrap()
        .strip_prefix("chunk-")
        .unwrap()
        .strip_suffix(".receipt.json")
        .unwrap()
        .to_owned()
}

fn pair_service(
    output_root: &Path,
    cache_root: PathBuf,
    staging_root: PathBuf,
    auth: Option<AuthConfig>,
) -> (Router, QuerySupervisor) {
    pair_service_with_deadline(output_root, cache_root, staging_root, auth, None)
}

fn pair_service_with_deadline(
    output_root: &Path,
    cache_root: PathBuf,
    staging_root: PathBuf,
    auth: Option<AuthConfig>,
    test_deadline: Option<Duration>,
) -> (Router, QuerySupervisor) {
    let archive_root = output_root.join("local-test-archive");
    let query_reader = Arc::new(
        RemoteArchiveReader::local_test_isolated(
            LocalTestTransport::new(archive_root.clone()).unwrap(),
            cache_root,
            RemoteCacheLimits::default(),
        )
        .unwrap(),
    );
    let pair_reader = Arc::new(
        LocalCapturePairV2Reader::local_test(
            archive_root,
            output_root.join("archive-state"),
            staging_root,
            ArchiveLimits::default(),
        )
        .unwrap(),
    );
    let (supervisor, query_client) = match test_deadline {
        Some(deadline) => QuerySupervisor::start_with_deadline_for_test(
            Arc::clone(&query_reader),
            Some(pair_reader),
            deadline,
        ),
        None => QuerySupervisor::start(Arc::clone(&query_reader), Some(pair_reader)),
    };
    let state = Arc::new(ServiceState {
        auth_configured: auth.is_some(),
        query_client,
        reader: query_reader,
        pair_query_available: true,
    });
    (app_router(state, auth), supervisor)
}

fn pair_uri(receipt_sha256: &str) -> String {
    format!("/v2/local-test/capture-pairs/{receipt_sha256}/verify")
}

fn staging_contains_only_budget_lock(staging_root: &Path) {
    let mut names = fs::read_dir(staging_root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, [".pair-readback-budget.lock"]);
    let metadata = fs::symlink_metadata(staging_root.join(&names[0])).unwrap();
    assert!(metadata.file_type().is_file());
    assert_eq!(metadata.uid(), rustix::process::geteuid().as_raw());
    assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
    assert_eq!(metadata.nlink(), 1);
}

async fn wait_for_budget_unlock(staging_root: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut names = fs::read_dir(staging_root)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            names.sort();
            if names == [".pair-readback-budget.lock"] {
                let lock = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(staging_root.join(&names[0]))
                    .unwrap();
                if lock.try_lock_exclusive().is_ok() {
                    lock.unlock().unwrap();
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("canceled Pair work must remove its run directory and release the shared lock");
}

async fn wait_for_worker_pid(pid_file: &Path) -> u32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(contents) = fs::read_to_string(pid_file)
                && let Ok(pid) = contents.parse::<u32>()
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Pair worker should publish its PID marker within the bounded test wait")
}

async fn wait_for_reap(pid: u32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while Path::new("/proc").join(pid.to_string()).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled Pair worker should be killed and reaped");
}

#[tokio::test]
async fn market_read_verifies_one_local_pair_and_never_reads_before_auth() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test_async().await;
    let temporary = tempfile::tempdir().unwrap();
    let output_root = temporary.path().join("offline-pair");
    let receipt_sha = make_reviewed_pair(&output_root).await;
    let staging_root = temporary.path().join("readback-staging");
    fs::create_dir(&staging_root).unwrap();
    let (app, mut supervisor) = pair_service(
        &output_root,
        temporary.path().join("query-cache"),
        staging_root.clone(),
        Some(test_support::auth_config()),
    );

    let unauthorized = app
        .clone()
        .oneshot(request(&pair_uri(&receipt_sha), None))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_no_store(&unauthorized);
    assert!(!staging_root.join(".pair-readback-budget.lock").exists());

    let token = test_support::terminal_token("market:read");
    let invalid = app
        .clone()
        .oneshot(request(
            &pair_uri(&receipt_sha.to_uppercase()),
            Some(&token),
        ))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_no_store(&invalid);

    let response = app
        .clone()
        .oneshot(request(&pair_uri(&receipt_sha), Some(&token)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_no_store(&response);
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let summary: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(summary["status"], "VERIFIED_LOCAL_TEST_CHUNK_ONLY");
    assert_eq!(summary["verification_scope"], "SINGLE_CHUNK_LOCAL_READBACK");
    assert_eq!(summary["source_completeness"], "NOT_ASSERTED");
    assert_eq!(summary["transport"], "local_test");
    assert_eq!(summary["provider"], "alpaca");
    assert_eq!(summary["feed"], "opra");
    assert_eq!(summary["entitlement"], "unknown");
    assert_eq!(summary["raw_frame_count"], 2);
    assert_eq!(summary["normalized_event_count"], 1);
    assert!(summary["input_payload_bytes"].is_string());
    assert!(summary["raw_row_count"].is_string());
    assert!(summary["event_row_count"].is_string());
    assert!(summary.get("rows").is_none());
    assert!(summary.get("raw_payload").is_none());
    assert!(summary.get("local_test_root").is_none());
    assert!(!String::from_utf8_lossy(&bytes).contains("600.25"));
    staging_contains_only_budget_lock(&staging_root);
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn pair_route_preserves_the_other_validated_synthetic_source_pair() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test_async().await;
    let temporary = tempfile::tempdir().unwrap();
    let output_root = temporary.path().join("synthetic-provider-pair");
    let receipt_sha = make_synthetic_provider_pair(&output_root).await;
    let staging_root = temporary.path().join("readback-staging");
    fs::create_dir(&staging_root).unwrap();
    let (app, mut supervisor) = pair_service(
        &output_root,
        temporary.path().join("query-cache"),
        staging_root,
        Some(test_support::auth_config()),
    );

    let token = test_support::terminal_token("market:read");
    let response = app
        .oneshot(request(&pair_uri(&receipt_sha), Some(&token)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_no_store(&response);
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let summary: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(summary["provider"], "synthetic");
    assert_eq!(summary["feed"], "synthetic");
    assert_eq!(summary["entitlement"], "unknown");
    assert_eq!(summary["source_completeness"], "NOT_ASSERTED");
    assert!(summary.get("rows").is_none());
    assert!(summary.get("raw_payload").is_none());
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn pair_route_reuses_shared_staging_lock_and_rejects_tampered_artifacts() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test_async().await;
    let temporary = tempfile::tempdir().unwrap();
    let output_root = temporary.path().join("offline-pair");
    let receipt_sha = make_reviewed_pair(&output_root).await;
    let staging_root = temporary.path().join("readback-staging");
    fs::create_dir(&staging_root).unwrap();
    let (app, mut supervisor) = pair_service(
        &output_root,
        temporary.path().join("query-cache"),
        staging_root.clone(),
        Some(test_support::auth_config()),
    );
    let token = test_support::terminal_token("market:read");
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(staging_root.join(".pair-readback-budget.lock"))
        .unwrap();
    lock.try_lock_exclusive().unwrap();
    let locked = app
        .clone()
        .oneshot(request(&pair_uri(&receipt_sha), Some(&token)))
        .await
        .unwrap();
    assert_eq!(locked.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_no_store(&locked);
    assert!(fs::read_dir(&staging_root).unwrap().all(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with("budget.lock")
    }));
    lock.unlock().unwrap();
    drop(lock);

    let recovered = app
        .clone()
        .oneshot(request(&pair_uri(&receipt_sha), Some(&token)))
        .await
        .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
    assert_no_store(&recovered);
    staging_contains_only_budget_lock(&staging_root);

    let state_root = output_root.join("archive-state/capture-pair-v2");
    let receipt_path = fs::read_dir(&state_root)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("chunk-")
        })
        .unwrap();
    let receipt: Value = serde_json::from_slice(&fs::read(receipt_path).unwrap()).unwrap();
    let event_artifact = &receipt["normalized_events"];
    let event_manifest = output_root
        .join("local-test-archive")
        .join(event_artifact["dataset_id"].as_str().unwrap())
        .join(event_artifact["manifest_object_name"].as_str().unwrap());
    let original_manifest = fs::read(&event_manifest).unwrap();
    let mut tampered_manifest = original_manifest.clone();
    let last = tampered_manifest.last_mut().unwrap();
    *last ^= 1;
    fs::write(&event_manifest, tampered_manifest).unwrap();
    let tampered = app
        .oneshot(request(&pair_uri(&receipt_sha), Some(&token)))
        .await
        .unwrap();
    assert_eq!(tampered.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_no_store(&tampered);
    fs::write(&event_manifest, original_manifest).unwrap();
    staging_contains_only_budget_lock(&staging_root);
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn pair_reader_rejects_replaced_startup_staging_root_without_recreating_or_forking_it() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test_async().await;
    let temporary = tempfile::tempdir().unwrap();
    let output_root = temporary.path().join("offline-pair");
    let receipt_sha = make_reviewed_pair(&output_root).await;
    let archive_root = output_root.join("local-test-archive");
    let state_root = output_root.join("archive-state");
    let staging_root = temporary.path().join("readback-staging");
    fs::create_dir(&staging_root).unwrap();
    let reader = LocalCapturePairV2Reader::local_test(
        archive_root,
        state_root,
        staging_root.clone(),
        ArchiveLimits::default(),
    )
    .unwrap();

    let original_staging = temporary.path().join("readback-staging-original");
    fs::rename(&staging_root, &original_staging).unwrap();

    let missing = reader.verify_chunk_cancellable(
        &format!("chunk-{receipt_sha}.receipt.json"),
        &crate::cancellation::CancellationToken::new(),
    );
    assert!(matches!(
        missing,
        Err(crate::MarketDataError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound
    ));
    assert!(!staging_root.exists());

    fs::create_dir(&staging_root).unwrap();

    let result = reader.verify_chunk_cancellable(
        &format!("chunk-{receipt_sha}.receipt.json"),
        &crate::cancellation::CancellationToken::new(),
    );
    assert!(matches!(
        result,
        Err(crate::MarketDataError::PublicationNotAuthorized)
    ));
    assert_eq!(fs::read_dir(&staging_root).unwrap().count(), 0);
    assert_eq!(fs::read_dir(&original_staging).unwrap().count(), 0);
}

#[tokio::test]
async fn dropping_pair_request_kills_and_reaps_worker_then_restores_capacity() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test_async().await;
    let temporary = tempfile::tempdir().unwrap();
    let output_root = temporary.path().join("offline-pair");
    let receipt_sha = make_reviewed_pair(&output_root).await;
    let receipt_name = format!("chunk-{receipt_sha}.receipt.json");
    let staging_root = temporary.path().join("readback-staging");
    fs::create_dir(&staging_root).unwrap();
    let (app, mut supervisor) = pair_service(
        &output_root,
        temporary.path().join("query-cache"),
        staging_root.clone(),
        Some(test_support::auth_config()),
    );
    let token = test_support::terminal_token("market:read");
    let pid_file = temporary.path().join("pair-worker.pid");
    let _pid_guard = crate::parquet_worker::track_worker_pid(&pid_file);
    let delay =
        crate::parquet_worker::test_support::hold_pair_worker_for_test(&receipt_name, 30_000);
    let request_task = tokio::spawn(
        app.clone()
            .oneshot(request(&pair_uri(&receipt_sha), Some(&token))),
    );
    let pid = wait_for_worker_pid(&pid_file).await;
    request_task.abort();
    let _ = request_task.await;
    drop(delay);
    wait_for_reap(pid).await;
    wait_for_budget_unlock(&staging_root).await;
    staging_contains_only_budget_lock(&staging_root);

    let recovered = tokio::time::timeout(
        Duration::from_secs(5),
        app.clone()
            .oneshot(request(&pair_uri(&receipt_sha), Some(&token))),
    )
    .await
    .expect("the supervisor must accept work after canceled child cleanup")
    .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
    assert_no_store(&recovered);
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn pair_http_deadline_cancels_worker_and_cleans_private_staging() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test_async().await;
    let temporary = tempfile::tempdir().unwrap();
    let output_root = temporary.path().join("offline-pair");
    let receipt_sha = make_reviewed_pair(&output_root).await;
    let receipt_name = format!("chunk-{receipt_sha}.receipt.json");
    let staging_root = temporary.path().join("readback-staging");
    fs::create_dir(&staging_root).unwrap();
    let (app, mut supervisor) = pair_service_with_deadline(
        &output_root,
        temporary.path().join("query-cache"),
        staging_root.clone(),
        Some(test_support::auth_config()),
        Some(Duration::from_secs(2)),
    );
    let token = test_support::terminal_token("market:read");
    let pid_file = temporary.path().join("pair-worker.pid");
    let _pid_guard = crate::parquet_worker::track_worker_pid(&pid_file);
    let _delay =
        crate::parquet_worker::test_support::hold_pair_worker_for_test(&receipt_name, 30_000);

    let request_task = tokio::spawn(app.oneshot(request(&pair_uri(&receipt_sha), Some(&token))));
    let pid = wait_for_worker_pid(&pid_file).await;
    let response = request_task.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_no_store(&response);
    wait_for_reap(pid).await;
    wait_for_budget_unlock(&staging_root).await;
    staging_contains_only_budget_lock(&staging_root);
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn pair_supervisor_shutdown_cancels_and_joins_active_worker() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test_async().await;
    let temporary = tempfile::tempdir().unwrap();
    let output_root = temporary.path().join("offline-pair");
    let receipt_sha = make_reviewed_pair(&output_root).await;
    let receipt_name = format!("chunk-{receipt_sha}.receipt.json");
    let staging_root = temporary.path().join("readback-staging");
    fs::create_dir(&staging_root).unwrap();
    let (app, mut supervisor) = pair_service(
        &output_root,
        temporary.path().join("query-cache"),
        staging_root.clone(),
        Some(test_support::auth_config()),
    );
    let token = test_support::terminal_token("market:read");
    let pid_file = temporary.path().join("pair-worker.pid");
    let _pid_guard = crate::parquet_worker::track_worker_pid(&pid_file);
    let _delay =
        crate::parquet_worker::test_support::hold_pair_worker_for_test(&receipt_name, 30_000);
    let request_task = tokio::spawn(app.oneshot(request(&pair_uri(&receipt_sha), Some(&token))));
    let pid = wait_for_worker_pid(&pid_file).await;
    tokio::time::timeout(Duration::from_secs(5), supervisor.shutdown())
        .await
        .expect("service shutdown must join the canceled Pair request")
        .unwrap();
    wait_for_reap(pid).await;
    let response = tokio::time::timeout(Duration::from_secs(2), request_task)
        .await
        .expect("the canceled HTTP handler should finish after worker reap")
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_no_store(&response);
    staging_contains_only_budget_lock(&staging_root);
}
