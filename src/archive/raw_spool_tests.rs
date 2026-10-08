#![cfg(target_os = "linux")]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use broker_ports::{
    RawCaptureInstanceId, RawFrameCapture, RawFrameDisposition, RawFrameFinalization,
    RawFramePayload, RawFrameSinkError, RawFrameSinkFactory, RawFrameWireEncoding,
};
use market_contracts::{EntitlementState, NumericEncodingV1, UtcTimestamp};
use tempfile::TempDir;

use super::{LocalRawFrameSpoolFactory, RawFrameSpoolLimits, raw_spool::RawSpoolTestPoint};

const FIXTURE_FRAME: &[u8] = br#"{"T":"t","S":"SYNTH","p":1.25}"#;

#[tokio::test]
async fn local_spool_syncs_exact_bytes_and_quarantines_prior_process_identity() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let factory = LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()).unwrap();
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    let first_id = sink.capture_instance_id();
    let first_capture = capture(first_id, 7, 1, FIXTURE_FRAME);
    let first_ack = sink.persist_before_decode(&first_capture).await.unwrap();
    assert!(first_ack.matches(&first_capture));

    let first_summary = control_summary();
    let first_final_ack = sink
        .finalize_after_decode(&first_ack, &first_summary)
        .await
        .unwrap();
    assert!(first_final_ack.matches(&first_ack, &first_summary));

    // The same logical sink retains its UUID while a reconnect advances the
    // adapter-local generation and resets the one-based frame sequence.
    assert_eq!(sink.capture_instance_id(), first_id);
    let reconnect_capture = capture(first_id, 9, 1, b"synthetic reconnect frame");
    let reconnect_ack = sink
        .persist_before_decode(&reconnect_capture)
        .await
        .unwrap();
    let reconnect_summary = market_summary();
    let reconnect_final_ack = sink
        .finalize_after_decode(&reconnect_ack, &reconnect_summary)
        .await
        .unwrap();
    assert!(reconnect_final_ack.matches(&reconnect_ack, &reconnect_summary));

    let old_dir = capture_dir(&root, first_id);
    let old_log = old_dir.join("frames.log");
    let committed_bytes = fs::read(&old_log).unwrap();
    assert!(committed_bytes.starts_with(b"MDPRAW01"));
    assert!(
        committed_bytes
            .windows(FIXTURE_FRAME.len())
            .any(|window| window == FIXTURE_FRAME)
    );
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&old_dir).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&old_log).unwrap().permissions().mode() & 0o777,
        0o600
    );

    drop(sink);
    drop(factory);

    let restarted = LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()).unwrap();
    let recovery = restarted.recovery_summary();
    assert_eq!(recovery.preserved_capture_directories, 1);
    assert_eq!(recovery.preserved_bytes, committed_bytes.len() as u64);
    assert_eq!(fs::read(&old_log).unwrap(), committed_bytes);

    let new_sink = restarted.create_sink("synthetic", "synthetic").unwrap();
    assert_ne!(new_sink.capture_instance_id(), first_id);
    assert_eq!(fs::read(&old_log).unwrap(), committed_bytes);
}

#[tokio::test]
async fn local_spool_rejects_sequence_gaps_and_poisons_the_generation() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let factory = LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()).unwrap();
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    let id = sink.capture_instance_id();
    let first_frame = capture(id, 13, 1, b"synthetic first frame");
    let ack = sink.persist_before_decode(&first_frame).await.unwrap();
    let summary = control_summary();
    sink.finalize_after_decode(&ack, &summary).await.unwrap();
    let log = capture_dir(&root, id).join("frames.log");
    let before_gap = fs::metadata(&log).unwrap().len();

    let skipped = capture(id, 13, 3, b"synthetic skipped sequence");
    assert_eq!(
        sink.persist_before_decode(&skipped).await,
        Err(RawFrameSinkError::Poisoned)
    );
    let later = capture(id, 13, 4, b"synthetic later sequence");
    assert_eq!(
        sink.persist_before_decode(&later).await,
        Err(RawFrameSinkError::Poisoned)
    );
    assert_eq!(fs::metadata(&log).unwrap().len(), before_gap);
}

#[tokio::test]
async fn local_spool_capacity_failure_returns_no_predecode_ack() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let limits = RawFrameSpoolLimits {
        max_capture_bytes: 128,
        max_total_bytes: 128,
        max_frames_per_capture: 2,
        max_capture_identities: 2,
    };
    let factory = LocalRawFrameSpoolFactory::open(&root, limits).unwrap();
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    let capture = capture(sink.capture_instance_id(), 1, 1, FIXTURE_FRAME);
    assert_eq!(
        sink.persist_before_decode(&capture).await,
        Err(RawFrameSinkError::CapacityExceeded)
    );
    assert_eq!(
        sink.finalize_after_decode(
            &&broker_ports::RawFrameCaptureAck::for_capture(&capture),
            &control_summary()
        )
        .await,
        Err(RawFrameSinkError::Poisoned)
    );
}

#[tokio::test]
async fn local_spool_enforces_frame_count_after_each_finalization() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let limits = RawFrameSpoolLimits {
        max_frames_per_capture: 1,
        ..RawFrameSpoolLimits::default()
    };
    let factory = LocalRawFrameSpoolFactory::open(&root, limits).unwrap();
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    let id = sink.capture_instance_id();
    let first = capture(id, 1, 1, b"synthetic first frame");
    let first_ack = sink.persist_before_decode(&first).await.unwrap();
    sink.finalize_after_decode(&first_ack, &control_summary())
        .await
        .unwrap();

    let log = capture_dir(&root, id).join("frames.log");
    let committed_len = fs::metadata(&log).unwrap().len();
    let second = capture(id, 1, 2, b"synthetic second frame");
    assert_eq!(
        sink.persist_before_decode(&second).await,
        Err(RawFrameSinkError::CapacityExceeded)
    );
    assert_eq!(fs::metadata(&log).unwrap().len(), committed_len);
}

#[tokio::test]
async fn local_spool_does_not_accept_next_frame_before_finalization() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let factory = LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()).unwrap();
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    let id = sink.capture_instance_id();
    let first = capture(id, 1, 1, b"synthetic first frame");
    sink.persist_before_decode(&first).await.unwrap();
    let second = capture(id, 1, 2, b"synthetic second frame");

    assert_eq!(
        sink.persist_before_decode(&second).await,
        Err(RawFrameSinkError::Poisoned)
    );
}

#[test]
fn local_spool_process_lock_is_held_until_the_factory_and_sinks_are_dropped() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let factory = LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()).unwrap();
    assert!(matches!(
        LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()),
        Err(RawFrameSinkError::Unavailable)
    ));
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    drop(factory);
    assert!(matches!(
        LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()),
        Err(RawFrameSinkError::Unavailable)
    ));
    drop(sink);
    LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()).unwrap();
}

#[test]
fn local_spool_rejects_nonempty_lock_metadata() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let lock_path = root.join(".raw-spool.lock");
    fs::write(&lock_path, b"unexpected metadata").unwrap();
    fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600)).unwrap();

    assert!(matches!(
        LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()),
        Err(RawFrameSinkError::Unavailable)
    ));
}

#[test]
fn local_spool_preserves_incomplete_prior_directories_as_unknown() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let old_dir = root.join("capture-00000000000040008000000000000000");
    fs::create_dir(&old_dir).unwrap();
    fs::set_permissions(&old_dir, fs::Permissions::from_mode(0o700)).unwrap();

    let factory = LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()).unwrap();
    assert_eq!(factory.recovery_summary().preserved_capture_directories, 1);
    assert_eq!(factory.recovery_summary().preserved_bytes, 0);
    assert!(old_dir.is_dir());
    let new_sink = factory.create_sink("synthetic", "synthetic").unwrap();
    assert_ne!(capture_dir(&root, new_sink.capture_instance_id()), old_dir);
}

#[tokio::test]
async fn cancelled_inflight_write_gets_no_ack_and_shutdown_waits_for_worker_exit() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let hook_release = Arc::clone(&release);
    let hook = Arc::new(move |point| {
        if point != RawSpoolTestPoint::AfterWriteBeforeSync {
            return false;
        }
        let _ = entered_tx.send(());
        let (released, changed) = &*hook_release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        false
    });
    let factory =
        LocalRawFrameSpoolFactory::open_with_test_hook(&root, RawFrameSpoolLimits::default(), hook)
            .unwrap();
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    let capture = capture(sink.capture_instance_id(), 1, 1, FIXTURE_FRAME);
    let writer_sink = Arc::clone(&sink);
    let writer = tokio::spawn(async move { writer_sink.persist_before_decode(&capture).await });

    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(2)))
        .await
        .unwrap()
        .expect("writer reached the injected pre-sync pause");
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());

    let mut shutdown = tokio::spawn(async move { factory.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );

    let (released, changed) = &*release;
    *released.lock().unwrap() = true;
    changed.notify_all();
    shutdown.await.unwrap();
}

#[tokio::test]
async fn ambiguous_append_failure_returns_no_ack_and_poison_closes_sink() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let hook = Arc::new(|point| point == RawSpoolTestPoint::AfterWriteBeforeSync);
    let factory =
        LocalRawFrameSpoolFactory::open_with_test_hook(&root, RawFrameSpoolLimits::default(), hook)
            .unwrap();
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    let capture = capture(sink.capture_instance_id(), 1, 1, FIXTURE_FRAME);

    assert_eq!(
        sink.persist_before_decode(&capture).await,
        Err(RawFrameSinkError::Ambiguous)
    );
    assert_eq!(
        sink.persist_before_decode(&capture).await,
        Err(RawFrameSinkError::Poisoned)
    );
    factory.shutdown().await;
    assert!(matches!(
        factory.create_sink("synthetic", "synthetic"),
        Err(RawFrameSinkError::Cancelled)
    ));
}

#[tokio::test]
async fn cancelled_finalization_returns_no_final_ack_and_shutdown_joins_write() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let hook_release = Arc::clone(&release);
    let append_count = Arc::new(AtomicUsize::new(0));
    let hook_count = Arc::clone(&append_count);
    let hook = Arc::new(move |point| {
        let call = hook_count.fetch_add(1, Ordering::AcqRel);
        if point != RawSpoolTestPoint::AfterWriteBeforeSync || call != 1 {
            return false;
        }
        let _ = entered_tx.send(());
        let (released, changed) = &*hook_release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        false
    });
    let factory =
        LocalRawFrameSpoolFactory::open_with_test_hook(&root, RawFrameSpoolLimits::default(), hook)
            .unwrap();
    let sink = factory.create_sink("synthetic", "synthetic").unwrap();
    let capture = capture(sink.capture_instance_id(), 1, 1, FIXTURE_FRAME);
    let predecode_ack = sink.persist_before_decode(&capture).await.unwrap();
    let summary = control_summary();
    let finalize_sink = Arc::clone(&sink);
    let finalizer = tokio::spawn(async move {
        finalize_sink
            .finalize_after_decode(&predecode_ack, &summary)
            .await
    });

    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(2)))
        .await
        .unwrap()
        .expect("finalizer reached the injected pre-sync pause");
    finalizer.abort();
    assert!(finalizer.await.unwrap_err().is_cancelled());

    let mut shutdown = tokio::spawn(async move { factory.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );

    let (released, changed) = &*release;
    *released.lock().unwrap() = true;
    changed.notify_all();
    shutdown.await.unwrap();
}

#[test]
fn local_spool_rejects_old_capture_directory_without_uuid_v4_variant() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("raw-spool");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let invalid_dir = root.join(format!("capture-{}", "a".repeat(32)));
    fs::create_dir(&invalid_dir).unwrap();
    fs::set_permissions(&invalid_dir, fs::Permissions::from_mode(0o700)).unwrap();

    assert!(matches!(
        LocalRawFrameSpoolFactory::open(&root, RawFrameSpoolLimits::default()),
        Err(RawFrameSinkError::Unavailable)
    ));
}

fn capture(
    id: RawCaptureInstanceId,
    source_generation: u64,
    frame_sequence: u64,
    bytes: &[u8],
) -> RawFrameCapture {
    RawFrameCapture::new(
        id,
        "synthetic",
        "synthetic",
        EntitlementState::Unknown,
        source_generation,
        frame_sequence,
        UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
        RawFrameWireEncoding::Json,
        RawFramePayload::capture(bytes.to_vec()).unwrap(),
    )
    .unwrap()
}

fn control_summary() -> RawFrameFinalization {
    RawFrameFinalization::new(0, Vec::new(), None, RawFrameDisposition::ControlMessage).unwrap()
}

fn market_summary() -> RawFrameFinalization {
    RawFrameFinalization::new(
        1,
        vec!["SYNTH".to_owned()],
        Some(NumericEncodingV1::DecimalToken),
        RawFrameDisposition::DecodedMarketData,
    )
    .unwrap()
}

fn capture_dir(root: &Path, id: RawCaptureInstanceId) -> PathBuf {
    root.join(format!("capture-{}", hex::encode(id.as_bytes())))
}
