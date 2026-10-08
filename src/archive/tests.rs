use super::*;

use std::{collections::HashMap, sync::Mutex};

use crate::queue::CollectionMessage;
use market_contracts::{
    DecimalString, EntitlementState, EventMetadataV1, MarketDataSourceV1, MarketEventEnvelopeV1,
    MarketEventV1, NumericEncodingV1, UtcTimestamp,
};
use tempfile::tempdir;

#[derive(Default)]
struct FakeState {
    objects: HashMap<(String, String), Vec<u8>>,
    fail_after_create: Option<String>,
    corrupt_download: Option<String>,
    upload_count: usize,
}

#[derive(Clone, Default)]
struct FakeTransport {
    state: Arc<Mutex<FakeState>>,
}

impl FakeTransport {
    fn fail_after_create(&self, object_name: &str) {
        self.state.lock().unwrap().fail_after_create = Some(object_name.to_owned());
    }

    fn corrupt_download(&self, object_name: &str) {
        self.state.lock().unwrap().corrupt_download = Some(object_name.to_owned());
    }

    fn upload_count(&self) -> usize {
        self.state.lock().unwrap().upload_count
    }

    fn seed(&self, dataset_id: &str, object_name: &str, bytes: Vec<u8>) {
        self.state
            .lock()
            .unwrap()
            .objects
            .insert((dataset_id.to_owned(), object_name.to_owned()), bytes);
    }
}

impl ObjectTransport for FakeTransport {
    fn lookup(&self, dataset_id: &str, object_name: &str) -> Result<Option<RemoteObject>> {
        let state = self.state.lock().unwrap();
        Ok(state
            .objects
            .get(&(dataset_id.to_owned(), object_name.to_owned()))
            .map(|bytes| RemoteObject {
                id: format!("local-test:{dataset_id}-{object_name}"),
                size_bytes: bytes.len() as u64,
                md5: None,
            }))
    }

    fn upload_immutable(
        &self,
        local_file: &Path,
        dataset_id: &str,
        object_name: &str,
    ) -> Result<()> {
        let bytes = fs::read(local_file)?;
        let mut state = self.state.lock().unwrap();
        let key = (dataset_id.to_owned(), object_name.to_owned());
        if let Some(existing) = state.objects.get(&key) {
            return if *existing == bytes {
                Ok(())
            } else {
                Err(MarketDataError::Conflict)
            };
        }
        state.objects.insert(key, bytes);
        state.upload_count += 1;
        if state.fail_after_create.as_deref() == Some(object_name) {
            state.fail_after_create = None;
            return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
        }
        Ok(())
    }

    fn download_with_limit(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<()> {
        let mut bytes = self
            .state
            .lock()
            .unwrap()
            .objects
            .get(&(dataset_id.to_owned(), object_name.to_owned()))
            .cloned()
            .ok_or(MarketDataError::UnknownOutcome)?;
        if bytes.len() as u64 > max_bytes {
            return Err(MarketDataError::InputLimit);
        }
        if self.state.lock().unwrap().corrupt_download.as_deref() == Some(object_name)
            && let Some(last) = bytes.last_mut()
        {
            *last ^= 1;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }
}

fn fixture(root: &Path) -> (ArchivePublisher, FakeTransport, ArchiveRequest) {
    let staging = root.join("staging");
    let state = root.join("state");
    fs::create_dir_all(&staging).unwrap();
    let timestamp = UtcTimestamp::parse("2026-10-08T13:30:15Z").unwrap();
    let source = MarketDataSourceV1::new(
        "synthetic",
        "synthetic",
        EntitlementState::Unknown,
        NumericEncodingV1::DecimalToken,
        None,
    )
    .unwrap();
    let event = CollectionMessage::Market(MarketEventEnvelopeV1 {
        metadata: EventMetadataV1 {
            schema_version: 1,
            source: source.clone(),
            generation: 1,
            sequence: 1,
            raw_frame_sha256: None,
            source_timestamp: Some(timestamp.clone()),
            received_timestamp: timestamp.clone(),
        },
        event: MarketEventV1::StockTrade {
            symbol: "QQQ".into(),
            price: DecimalString::new("600.25").unwrap(),
            size: DecimalString::new("2").unwrap(),
        },
    });
    let window = crate::aggregate::SessionWindow {
        trade_date: "2026-10-08".into(),
        session_id: "synthetic-regular-2026-10-08".into(),
        timezone: "America/New_York".into(),
        policy_id: "synthetic-session-policy-v1".into(),
        policy_sha256: "a".repeat(64),
        session_start: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
        session_end_exclusive: UtcTimestamp::parse("2026-10-08T20:00:00Z").unwrap(),
        window_start: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
        window_end_exclusive: UtcTimestamp::parse("2026-10-08T13:31:00Z").unwrap(),
        expected_symbols: vec!["QQQ".into()],
    };
    let bars = crate::aggregate::aggregate_trade_bars(
        &[event],
        &window,
        &crate::aggregate::CompletionEvidence {
            mode: crate::aggregate::CompletionMode::SyntheticEof,
            input_eof: true,
            source_is_paged: false,
            source_pages_exhausted: None,
            available_at: UtcTimestamp::parse("2026-10-08T13:31:00Z").unwrap(),
        },
    )
    .unwrap();
    let object = staging.join("synthetic-1.parquet");
    let verification = parquet_store::write_bars(&object, &bars).unwrap();
    let request = ArchiveRequest {
        dataset_id: "synthetic-1".into(),
        object_name: "synthetic-1.parquet".into(),
        schema_id: MINUTE_BAR_SCHEMA_ID.into(),
        purpose: PublicationPurpose::Diagnostic,
        source,
        symbols: vec!["QQQ".into()],
        time_range: Some(DatasetTimeRangeV1 {
            start_inclusive: timestamp.clone(),
            end_exclusive: UtcTimestamp::parse("2026-10-08T13:30:15.000000001Z").unwrap(),
        }),
        source_timestamp_missing_rows: 0,
        row_count: verification.footer_rows,
        source_pages_exhausted: None,
        input_eof: true,
        parquet_path: object,
    };
    let transport = FakeTransport::default();
    let publisher = ArchivePublisher::new(
        Arc::new(transport.clone()),
        TransportKind::LocalTest,
        state,
        staging,
        ArchiveLimits::default(),
    )
    .unwrap();
    (publisher, transport, request)
}

#[test]
fn immutable_publish_is_idempotent_after_verified_commit() {
    let temp = tempdir().unwrap();
    let (publisher, transport, request) = fixture(temp.path());
    let first = publisher.publish(&request).unwrap();
    assert_eq!(first.object.transport, DatasetTransportV1::LocalTest);
    assert!(
        first
            .object
            .object_id
            .as_deref()
            .unwrap()
            .starts_with("local-test:")
    );
    assert_eq!(transport.upload_count(), 2); // Parquet object + manifest sidecar.
    let second = publisher.publish(&request).unwrap();
    assert_eq!(first, second);
    assert_eq!(transport.upload_count(), 2);
}

#[test]
fn restart_reconciles_abort_after_remote_create_without_duplicate_upload() {
    let temp = tempdir().unwrap();
    let (publisher, transport, request) = fixture(temp.path());
    transport.fail_after_create("synthetic-1.parquet");
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::UnknownOutcome)
    ));
    assert_eq!(transport.upload_count(), 1);
    let restarted = publisher.clone();
    let manifest = restarted.publish(&request).unwrap();
    assert_eq!(manifest.object.content_sha256.len(), 64);
    assert_eq!(transport.upload_count(), 2);
}

#[test]
fn absent_object_after_unknown_create_is_never_blindly_retried() {
    let temp = tempdir().unwrap();
    let (publisher, transport, request) = fixture(temp.path());
    transport.fail_after_create("synthetic-1.parquet");
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::UnknownOutcome)
    ));
    // Simulate an operator-observed remote absence after a failed create. A restart remains
    // UNKNOWN; it does not repeat copyto because absence is not proof the original create cannot commit.
    transport.state.lock().unwrap().objects.clear();
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::UnknownOutcome)
    ));
    assert_eq!(transport.upload_count(), 1);
}

#[test]
fn hash_corruption_and_same_name_conflict_fail_closed() {
    let temp = tempdir().unwrap();
    let (publisher, transport, request) = fixture(temp.path());
    transport.corrupt_download("synthetic-1.parquet");
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::Conflict)
    ));

    let another_temp = tempdir().unwrap();
    let (publisher, transport, request) = fixture(another_temp.path());
    transport.seed(
        "synthetic-1",
        "synthetic-1.parquet",
        vec![0; fs::metadata(&request.parquet_path).unwrap().len() as usize],
    );
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::Conflict)
    ));
    assert_eq!(transport.upload_count(), 0);
}

#[test]
fn manifest_abort_after_create_is_reconciled_and_unknown_manifest_is_not_reuploaded() {
    let temp = tempdir().unwrap();
    let (publisher, transport, request) = fixture(temp.path());
    transport.fail_after_create("synthetic-1.manifest.json");
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::UnknownOutcome)
    ));
    assert_eq!(transport.upload_count(), 2);
    let manifest = publisher.publish(&request).unwrap();
    assert_eq!(manifest.dataset_id, "synthetic-1");
    assert_eq!(transport.upload_count(), 2);

    let another = tempdir().unwrap();
    let (publisher, transport, request) = fixture(another.path());
    transport.fail_after_create("synthetic-1.manifest.json");
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::UnknownOutcome)
    ));
    transport.state.lock().unwrap().objects.remove(&(
        "synthetic-1".to_owned(),
        "synthetic-1.manifest.json".to_owned(),
    ));
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::UnknownOutcome)
    ));
    assert_eq!(transport.upload_count(), 2);
}

#[test]
fn drive_publication_requires_verified_entitled_exact_feed() {
    let temp = tempdir().unwrap();
    let (_, _, mut request) = fixture(temp.path());
    request.purpose = PublicationPurpose::Curated;
    assert!(validate_request(&request, TransportKind::RcloneGoogleDrive).is_err());
}

#[test]
fn drive_opra_binary_projection_is_diagnostic_only_and_paths_are_namespaced() {
    let temp = tempdir().unwrap();
    let (_, _, mut request) = fixture(temp.path());
    request.purpose = PublicationPurpose::Diagnostic;
    request.schema_id = EVENT_SCHEMA_ID.into();
    request.source = MarketDataSourceV1::new(
        "alpaca",
        "opra",
        EntitlementState::Authorized,
        NumericEncodingV1::BinaryFloat64ShortestDecimal,
        None,
    )
    .unwrap();
    assert!(validate_request(&request, TransportKind::RcloneGoogleDrive).is_ok());
    assert_eq!(
        namespaced_dataset_id("curated", "qqq-week-1").unwrap(),
        "curated-qqq-week-1"
    );
    assert_eq!(
        namespaced_dataset_id("diagnostic", "qqq-week-1").unwrap(),
        "diagnostic-qqq-week-1"
    );

    request.source = MarketDataSourceV1::new(
        "alpaca",
        "opra",
        EntitlementState::Unknown,
        NumericEncodingV1::BinaryFloat64ShortestDecimal,
        None,
    )
    .unwrap();
    assert!(validate_request(&request, TransportKind::RcloneGoogleDrive).is_err());
}

fn assert_request_mismatch_rejected(change: impl FnOnce(&mut ArchiveRequest)) {
    let temp = tempdir().unwrap();
    let (publisher, transport, mut request) = fixture(temp.path());
    change(&mut request);
    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::ParquetSchema)
    ));
    assert_eq!(transport.upload_count(), 0);
}

#[test]
fn publication_compares_actual_symbols_time_range_and_missing_count() {
    assert_request_mismatch_rejected(|request| request.symbols = vec!["SPY".into()]);
    assert_request_mismatch_rejected(|request| {
        request.time_range = Some(DatasetTimeRangeV1 {
            start_inclusive: UtcTimestamp::parse("2026-10-08T13:30:16Z").unwrap(),
            end_exclusive: UtcTimestamp::parse("2026-10-08T13:30:16.000000001Z").unwrap(),
        });
    });
    assert_request_mismatch_rejected(|request| request.source_timestamp_missing_rows = 1);
}

#[test]
fn synthetic_parquet_cannot_be_relabelled_as_authorized_alpaca_drive_data() {
    let temp = tempdir().unwrap();
    let (_, transport, mut request) = fixture(temp.path());
    request.purpose = PublicationPurpose::Curated;
    request.source = MarketDataSourceV1::new(
        "alpaca",
        "sip",
        EntitlementState::Authorized,
        NumericEncodingV1::DecimalToken,
        None,
    )
    .unwrap();
    request.source_pages_exhausted = Some(true);
    let publisher = ArchivePublisher::new(
        Arc::new(transport.clone()),
        TransportKind::RcloneGoogleDrive,
        temp.path().join("drive-state"),
        temp.path().join("staging"),
        ArchiveLimits::default(),
    )
    .unwrap();

    assert!(matches!(
        publisher.publish(&request),
        Err(MarketDataError::ParquetSchema)
    ));
    assert_eq!(transport.upload_count(), 0);
}

#[test]
fn replay_preflight_accounts_for_existing_files_and_peak_readback_reserve() {
    let temp = tempdir().unwrap();
    let staging = temp.path().join("staging");
    fs::create_dir_all(&staging).unwrap();
    let limits = ArchiveLimits {
        max_object_bytes: 1024,
        max_manifest_bytes: 128,
        max_staging_bytes: 70_000,
        upload_queue_capacity: 1,
    };
    assert!(limits.validate().is_ok());
    assert!(ArchivePublisher::preflight_replay_staging(&staging, &limits).is_ok());
    fs::write(staging.join("old-output.bin"), vec![0; 2048]).unwrap();
    assert!(matches!(
        ArchivePublisher::preflight_replay_staging(&staging, &limits),
        Err(MarketDataError::InputLimit)
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn staging_cleanup_dry_run_and_apply_preserve_active_locked_and_unknown_state() {
    let temp = tempdir().unwrap();
    let state = temp.path().join("state");
    let staging = temp.path().join("staging");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir_all(&staging).unwrap();

    let stale = staging.join(".stale-dataset-readback-4294967295-1.tmp");
    fs::write(&stale, b"orphan").unwrap();
    let active = staging.join(format!(
        ".active-dataset-receipt-{}-2.tmp",
        std::process::id()
    ));
    fs::write(&active, b"active").unwrap();
    let locked = staging.join(".locked-dataset-manifest-4294967295-3.tmp");
    fs::write(&locked, b"locked").unwrap();
    let locked_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(state.join("locked-dataset.lock"))
        .unwrap();
    locked_file.lock_exclusive().unwrap();
    let unknown = staging.join(".unknown-dataset-manifest-readback-4294967295-4.tmp");
    fs::write(&unknown, b"unknown").unwrap();
    fs::write(state.join("unknown-dataset.receipt.json"), b"not-json").unwrap();

    let preview = ArchivePublisher::cleanup_staging(&state, &staging, false).unwrap();
    assert!(preview.dry_run);
    assert_eq!(preview.eligible_files, 1);
    assert_eq!(preview.eligible_bytes, 6);
    assert_eq!(preview.skipped_active_process, 1);
    assert_eq!(preview.skipped_locked, 1);
    assert_eq!(preview.skipped_unresolved_receipt, 1);
    assert!(stale.exists());

    let applied = ArchivePublisher::cleanup_staging(&state, &staging, true).unwrap();
    assert_eq!(applied.removed_files, 1);
    assert!(!stale.exists());
    assert!(active.exists());
    assert!(locked.exists());
    assert!(unknown.exists());

    drop(locked_file);
    let unlocked = ArchivePublisher::cleanup_staging(&state, &staging, true).unwrap();
    assert_eq!(unlocked.removed_files, 1);
    assert!(!locked.exists());
    assert!(unknown.exists());
}

#[test]
fn cleanup_releases_only_temp_files_with_a_matching_committed_manifest() {
    let temp = tempdir().unwrap();
    let (publisher, _transport, request) = fixture(temp.path());
    publisher.publish(&request).unwrap();
    let staging = temp.path().join("staging");
    let state = temp.path().join("state");
    let candidate = staging.join(".synthetic-1-readback-4294967295-7.tmp");
    fs::write(&candidate, b"orphaned verified readback temp").unwrap();

    let preview = ArchivePublisher::cleanup_staging(&state, &staging, false).unwrap();
    assert_eq!(preview.eligible_files, 1);
    assert!(candidate.exists());

    let applied = ArchivePublisher::cleanup_staging(&state, &staging, true).unwrap();
    assert_eq!(applied.removed_files, 1);
    assert!(!candidate.exists());

    let mismatched = staging.join(".synthetic-1-manifest-4294967295-8.tmp");
    fs::write(&mismatched, b"must be retained").unwrap();
    let receipt_path = state.join("synthetic-1.receipt.json");
    let mut receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    receipt["dataset_id"] = serde_json::Value::String("other-dataset".into());
    fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();

    let result = ArchivePublisher::cleanup_staging(&state, &staging, true).unwrap();
    assert_eq!(result.removed_files, 0);
    assert_eq!(result.skipped_unresolved_receipt, 1);
    assert!(mismatched.exists());
}
