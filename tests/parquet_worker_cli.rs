#![cfg(target_os = "linux")]

#[path = "common/parquet_page_bomb.rs"]
mod page_bomb_fixture;
#[path = "common/parquet_required_null.rs"]
mod required_null_fixture;

use std::{
    io::Read,
    process::{Child, Command, ExitStatus, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

use market_contracts::{
    DecimalString, EntitlementState, EventMetadataV1, MarketDataSourceV1, MarketEventEnvelopeV1,
    MarketEventParquetRowV2, MarketEventV1, NumericEncodingV1, RawFrameDispositionV1,
    RawFrameReferenceV2, RawFrameStorageRecordV1, UtcTimestamp,
};
use market_data_platform::pipeline::synthetic_390_minute_session_replay;
use market_data_platform::{parquet_store, schema};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::sync::Mutex;

static CLI_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

struct CliOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_cli(args: &[&str]) -> CliOutput {
    let mut child = Command::new(env!("CARGO_BIN_EXE_market-data-platform"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            reap_after_timeout(&mut child);
            panic!("market-data-platform CLI exceeded the 30-second test deadline");
        }
        thread::sleep(Duration::from_millis(20));
    };
    CliOutput {
        status,
        stdout: read_pipe(child.stdout.take().unwrap()),
        stderr: read_pipe(child.stderr.take().unwrap()),
    }
}

fn read_pipe(mut pipe: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    pipe.read_to_end(&mut bytes).unwrap();
    bytes
}

fn reap_after_timeout(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn production_cli_verifies_and_queries_through_the_isolated_worker() {
    let _serial = CLI_SERIAL.get_or_init(Default::default).lock().await;
    let temp = tempdir().unwrap();
    synthetic_390_minute_session_replay(temp.path())
        .await
        .unwrap();
    let event_parquet = temp
        .path()
        .join("staging/synthetic-2026-10-08-full-390-minute-session-parquet-v2-events-v1.parquet");
    let event_parquet = event_parquet.to_str().unwrap();
    let event_verify = run_cli(&[
        "verify",
        "--parquet",
        event_parquet,
        "--schema",
        "market-events-v1",
    ]);
    assert!(
        event_verify.status.success(),
        "{}",
        String::from_utf8_lossy(&event_verify.stderr)
    );
    let event_json: serde_json::Value = serde_json::from_slice(&event_verify.stdout).unwrap();
    assert_eq!(event_json["decoded_rows"], 390);

    let parquet = temp
        .path()
        .join("staging/synthetic-2026-10-08-full-390-minute-session-parquet-v2-bars-1m-v1.parquet");
    let parquet = parquet.to_str().unwrap();

    let verify = run_cli(&[
        "verify",
        "--parquet",
        parquet,
        "--schema",
        "us-equity-trade-bar1m-v1",
    ]);
    assert!(
        verify.status.success(),
        "{}",
        String::from_utf8_lossy(&verify.stderr)
    );
    let verify_json: serde_json::Value = serde_json::from_slice(&verify.stdout).unwrap();
    assert_eq!(verify_json["decoded_rows"], 390);

    let export = temp.path().join("verified-query.jsonl");
    let export_arg = export.to_str().unwrap();
    let query = run_cli(&[
        "query-bars",
        "--parquet",
        parquet,
        "--symbol",
        "QQQ",
        "--export-jsonl",
        export_arg,
    ]);
    assert!(
        query.status.success(),
        "{}",
        String::from_utf8_lossy(&query.stderr)
    );
    let query_json: serde_json::Value = serde_json::from_slice(&query.stdout).unwrap();
    assert_eq!(query_json["exported_rows"], 390);
    assert_eq!(
        std::fs::read_to_string(export).unwrap().lines().count(),
        390
    );
}

#[tokio::test]
async fn production_cli_verifies_core_raw_frame_v1_and_correlated_event_v2() {
    let _serial = CLI_SERIAL.get_or_init(Default::default).lock().await;
    let temp = tempdir().unwrap();
    let raw_path = temp.path().join("synthetic-raw.parquet");
    let event_path = temp.path().join("synthetic-events-v2.parquet");
    let frame_bytes = vec![
        0x92, 0xa5, b't', b'r', b'a', b'd', b'e', 0xa4, b'1', b'.', b'2', b'5',
    ];
    let source_time = UtcTimestamp::parse("2026-10-08T13:30:15Z").unwrap();
    let frame = RawFrameStorageRecordV1 {
        schema_version: 1,
        provider: "synthetic".to_owned(),
        feed: "synthetic".to_owned(),
        entitlement: EntitlementState::Unknown,
        source_numeric_encoding: Some(NumericEncodingV1::DecimalToken),
        generation: 1,
        frame_sequence: 1,
        received_timestamp_utc: source_time.clone(),
        frame_sha256: hex::encode(Sha256::digest(&frame_bytes)),
        frame_bytes,
        event_count: 1,
        disposition: RawFrameDispositionV1::MarketData,
        symbols_json: r#"["QQQ"]"#.to_owned(),
    };
    parquet_store::write_raw_frames_with_limit(
        &raw_path,
        std::slice::from_ref(&frame),
        1024 * 1024,
    )
    .unwrap();
    let event_source = MarketDataSourceV1::new(
        "synthetic",
        "synthetic",
        EntitlementState::Unknown,
        NumericEncodingV1::DecimalToken,
        None,
    )
    .unwrap();
    let event = MarketEventParquetRowV2 {
        event: MarketEventEnvelopeV1 {
            metadata: EventMetadataV1 {
                schema_version: 1,
                source: event_source,
                generation: 1,
                sequence: 1,
                raw_frame_sha256: Some(frame.frame_sha256.clone()),
                source_timestamp: Some(source_time.clone()),
                received_timestamp: source_time,
            },
            event: MarketEventV1::StockTrade {
                symbol: "QQQ".to_owned(),
                price: DecimalString::new("600.25").unwrap(),
                size: DecimalString::new("1").unwrap(),
            },
        },
        raw_frame_reference: Some(RawFrameReferenceV2 {
            raw_frame_generation: 1,
            raw_frame_sequence: 1,
            raw_frame_event_ordinal: 1,
            raw_frame_event_count: 1,
        }),
    };
    parquet_store::write_event_v2_with_limit(&event_path, &[event], 1024 * 1024).unwrap();

    for (path, schema_id, schema_arg) in [
        (
            &raw_path,
            schema::RAW_FRAME_SCHEMA_ID,
            "market-raw-frame-v1",
        ),
        (&event_path, schema::EVENT_SCHEMA_V2_ID, "market-events-v2"),
    ] {
        let path = path.to_str().unwrap();
        let verified = run_cli(&["verify", "--parquet", path, "--schema", schema_arg]);
        assert!(
            verified.status.success(),
            "{}",
            String::from_utf8_lossy(&verified.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&verified.stdout).unwrap();
        assert_eq!(report["schema_id"], schema_id);
        assert_eq!(report["decoded_rows"], 1);
        assert_eq!(report["source"]["provider"], "synthetic");
        assert_eq!(report["source"]["entitlement"], "unknown");
    }
}

#[tokio::test]
async fn production_cli_generates_and_verifies_an_explicit_past_date_fixture() {
    let _serial = CLI_SERIAL.get_or_init(Default::default).lock().await;
    let temp = tempdir().unwrap();
    let output = temp.path().join("oct-7-session");
    let output_arg = output.to_str().unwrap();
    let synthetic = run_cli(&[
        "synthetic",
        "--output",
        output_arg,
        "--regular-session",
        "--session-date",
        "2026-10-07",
    ]);
    assert!(
        synthetic.status.success(),
        "{}",
        String::from_utf8_lossy(&synthetic.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&synthetic.stdout).unwrap();
    assert_eq!(report["event_rows"], 390);
    assert_eq!(report["minute_bar_rows"], 390);
    assert_eq!(report["provider"], "synthetic");
    assert_eq!(report["entitlement"], "unknown");
    assert_eq!(report["research_readiness"], "UNVERIFIED");
    assert_eq!(report["google_drive_upload"], "NOTRUN");

    let dataset_id = "synthetic-2026-10-07-full-390-minute-session-parquet-v2-bars-1m-v1";
    let manifest_path = output
        .join("local-test-store")
        .join(dataset_id)
        .join(format!("{dataset_id}.manifest.json"));
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["dataset_id"], dataset_id);
    assert_eq!(manifest["row_count"], "390");
    assert_eq!(manifest["source"]["provider"], "synthetic");
    assert_eq!(manifest["source"]["feed"], "synthetic");
    assert_eq!(manifest["source"]["entitlement"], "unknown");

    let parquet = output.join("staging").join(format!("{dataset_id}.parquet"));
    let parquet_arg = parquet.to_str().unwrap();
    let verify = run_cli(&[
        "verify",
        "--parquet",
        parquet_arg,
        "--schema",
        "us-equity-trade-bar1m-v1",
    ]);
    assert!(
        verify.status.success(),
        "{}",
        String::from_utf8_lossy(&verify.stderr)
    );
    let verified: serde_json::Value = serde_json::from_slice(&verify.stdout).unwrap();
    assert_eq!(verified["decoded_rows"], 390);

    let invalid_output = temp.path().join("invalid-date");
    let invalid_output_arg = invalid_output.to_str().unwrap();
    let invalid_date = run_cli(&[
        "synthetic",
        "--output",
        invalid_output_arg,
        "--regular-session",
        "--session-date",
        "2026-10-7",
    ]);
    assert!(!invalid_date.status.success());
    assert!(!invalid_output.exists());

    let missing_profile_output = temp.path().join("missing-profile");
    let missing_profile_output_arg = missing_profile_output.to_str().unwrap();
    let missing_profile = run_cli(&[
        "synthetic",
        "--output",
        missing_profile_output_arg,
        "--session-date",
        "2026-10-07",
    ]);
    assert!(!missing_profile.status.success());
    assert!(!missing_profile_output.exists());
}

#[tokio::test]
async fn production_cli_rejects_a_real_page_header_bomb_without_publishing_query_output() {
    let _serial = CLI_SERIAL.get_or_init(Default::default).lock().await;
    let temp = tempdir().unwrap();
    synthetic_390_minute_session_replay(temp.path())
        .await
        .unwrap();
    let event_source = temp
        .path()
        .join("staging/synthetic-2026-10-08-full-390-minute-session-parquet-v2-events-v1.parquet");
    let hostile_events = temp.path().join("hostile-verify.parquet");
    std::fs::copy(&event_source, &hostile_events).unwrap();
    page_bomb_fixture::mutate_dictionary_page_to_memory_bomb(&hostile_events);
    let hostile_events = hostile_events.to_str().unwrap();

    let verify = run_cli(&[
        "verify",
        "--parquet",
        hostile_events,
        "--schema",
        "market-events-v1",
    ]);
    assert!(!verify.status.success());
    assert!(!String::from_utf8_lossy(&verify.stdout).contains("\"decoded_rows\""));

    let bar_source = temp
        .path()
        .join("staging/synthetic-2026-10-08-full-390-minute-session-parquet-v2-bars-1m-v1.parquet");
    let hostile_bars = temp.path().join("hostile-bars.parquet");
    std::fs::copy(&bar_source, &hostile_bars).unwrap();
    page_bomb_fixture::mutate_dictionary_page_to_memory_bomb(&hostile_bars);
    let hostile_bars = hostile_bars.to_str().unwrap();
    let export = temp.path().join("must-not-be-published.jsonl");
    let export_arg = export.to_str().unwrap();
    let query = run_cli(&[
        "query-bars",
        "--parquet",
        hostile_bars,
        "--symbol",
        "QQQ",
        "--export-jsonl",
        export_arg,
    ]);
    assert!(!query.status.success());
    assert!(!export.exists());
}

#[tokio::test]
async fn production_cli_rejects_null_definition_levels_after_a_valid_positive_control() {
    let _serial = CLI_SERIAL.get_or_init(Default::default).lock().await;
    let temp = tempdir().unwrap();
    let source_dir = temp.path().join("source");
    std::fs::create_dir(&source_dir).unwrap();
    synthetic_390_minute_session_replay(&source_dir)
        .await
        .unwrap();
    let positive = source_dir
        .join("staging/synthetic-2026-10-08-full-390-minute-session-parquet-v2-bars-1m-v1.parquet");

    let positive_verify = run_cli(&[
        "verify",
        "--parquet",
        positive.to_str().unwrap(),
        "--schema",
        "us-equity-trade-bar1m-v1",
    ]);
    assert!(
        positive_verify.status.success(),
        "valid positive control failed: {}",
        String::from_utf8_lossy(&positive_verify.stderr)
    );
    let positive_json: serde_json::Value = serde_json::from_slice(&positive_verify.stdout).unwrap();
    assert_eq!(positive_json["decoded_rows"], 390);
    let positive_export = temp.path().join("positive-query.jsonl");
    let positive_query = run_cli(&[
        "query-bars",
        "--parquet",
        positive.to_str().unwrap(),
        "--symbol",
        "QQQ",
        "--export-jsonl",
        positive_export.to_str().unwrap(),
    ]);
    assert!(
        positive_query.status.success(),
        "valid positive-control query failed: {}",
        String::from_utf8_lossy(&positive_query.stderr)
    );
    let positive_query_json: serde_json::Value =
        serde_json::from_slice(&positive_query.stdout).unwrap();
    assert_eq!(positive_query_json["exported_rows"], 390);
    assert_eq!(
        std::fs::read_to_string(positive_export)
            .unwrap()
            .lines()
            .count(),
        390
    );

    let negative_dir = temp.path().join("negative");
    std::fs::create_dir(&negative_dir).unwrap();
    let malformed = negative_dir.join("required-symbol-with-null-level.parquet");
    required_null_fixture::write_required_schema_with_null_definition_level(&positive, &malformed);

    let verify = run_cli(&[
        "verify",
        "--parquet",
        malformed.to_str().unwrap(),
        "--schema",
        "us-equity-trade-bar1m-v1",
    ]);
    assert!(
        !verify.status.success(),
        "verify accepted malformed Parquet; stdout={} stderr={}",
        String::from_utf8_lossy(&verify.stdout),
        String::from_utf8_lossy(&verify.stderr)
    );
    assert!(
        String::from_utf8_lossy(&verify.stderr).contains("Error: Parquet"),
        "verify did not report the required-field Parquet decode failure: {}",
        String::from_utf8_lossy(&verify.stderr)
    );
    assert!(!String::from_utf8_lossy(&verify.stdout).contains("\"decoded_rows\""));

    let export = temp.path().join("must-not-be-published.jsonl");
    let query = run_cli(&[
        "query-bars",
        "--parquet",
        malformed.to_str().unwrap(),
        "--symbol",
        "QQQ",
        "--export-jsonl",
        export.to_str().unwrap(),
    ]);
    assert!(
        !query.status.success(),
        "query accepted malformed Parquet; stdout={} stderr={}",
        String::from_utf8_lossy(&query.stdout),
        String::from_utf8_lossy(&query.stderr)
    );
    assert!(
        String::from_utf8_lossy(&query.stderr).contains("Error: Parquet"),
        "query did not report the required-field Parquet decode failure: {}",
        String::from_utf8_lossy(&query.stderr)
    );
    assert!(!String::from_utf8_lossy(&query.stdout).contains("\"exported_rows\""));
    assert!(!export.exists());

    for entry in std::fs::read_dir(&negative_dir).unwrap() {
        let name = entry.unwrap().file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.ends_with(".manifest.json"),
            "unexpected manifest: {name}"
        );
        assert!(
            !name.ends_with(".receipt.json"),
            "unexpected receipt: {name}"
        );
    }
}
