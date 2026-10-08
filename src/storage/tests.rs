use super::*;
use std::time::{Duration, Instant};

use crate::error::StorageFailure;

#[cfg(unix)]
#[test]
fn total_operation_deadline_kills_and_reaps_child() {
    let mut command = Command::new("sleep");
    command.arg("5");
    let started = Instant::now();
    let result = run_bounded_command(command, 1024, Duration::from_millis(50));
    assert!(matches!(
        result,
        Err(MarketDataError::Storage(StorageFailure::Timeout))
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[cfg(unix)]
#[test]
fn output_cap_cancels_unbounded_child_output() {
    let command = Command::new("yes");
    let started = Instant::now();
    let result = run_bounded_command(command, 128, Duration::from_secs(2));
    assert!(matches!(
        result,
        Err(MarketDataError::Storage(StorageFailure::MalformedListing))
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[cfg(unix)]
#[test]
fn download_cap_kills_child_and_removes_partial_file() {
    let temp = tempfile::tempdir().unwrap();
    let destination = temp.path().join("partial.bin");
    let started = Instant::now();
    let result = run_bounded_download(
        Command::new("yes"),
        &destination,
        128,
        Duration::from_secs(2),
    );
    assert!(matches!(result, Err(MarketDataError::InputLimit)));
    assert!(!destination.exists());
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[cfg(unix)]
#[test]
fn command_deadline_closes_pipe_inherited_by_descendant_after_parent_exit() {
    let mut command = Command::new("sh");
    command.args(["-c", "sleep 30 & exit 0"]);
    let started = Instant::now();
    let result = run_bounded_command(command, 1024, Duration::from_secs(2)).unwrap();
    assert_eq!(result.status_code, 0);
    assert!(result.stdout.is_empty());
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[cfg(unix)]
#[test]
fn download_supervisor_closes_inherited_pipe_and_removes_empty_output() {
    let temp = tempfile::tempdir().unwrap();
    let destination = temp.path().join("download.bin");
    let mut command = Command::new("sh");
    command.args(["-c", "sleep 30 & exit 0"]);
    let started = Instant::now();
    let result = run_bounded_download(command, &destination, 1024, Duration::from_secs(2));
    assert!(matches!(
        result,
        Err(MarketDataError::Storage(StorageFailure::ReadbackFailed))
    ));
    assert!(!destination.exists());
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn local_transport_rejects_oversized_download_without_partial_file() {
    let temp = tempfile::tempdir().unwrap();
    let transport = LocalTestTransport::new(temp.path().join("remote")).unwrap();
    let source = temp.path().join("source.bin");
    fs::write(&source, b"too large").unwrap();
    transport
        .upload_immutable(&source, "dataset-v1", "object.bin")
        .unwrap();
    let destination = temp.path().join("download.bin");
    assert!(matches!(
        transport.download_with_limit("dataset-v1", "object.bin", &destination, 4),
        Err(MarketDataError::InputLimit)
    ));
    assert!(!destination.exists());
}
