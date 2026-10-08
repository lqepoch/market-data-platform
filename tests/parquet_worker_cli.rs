#![cfg(target_os = "linux")]

#[path = "common/parquet_page_bomb.rs"]
mod page_bomb_fixture;

use std::{
    io::Read,
    process::{Child, Command, ExitStatus, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

use market_data_platform::pipeline::synthetic_390_minute_session_replay;
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
