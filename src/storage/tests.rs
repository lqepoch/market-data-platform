use super::*;
use std::time::{Duration, Instant};

use crate::error::StorageFailure;

#[cfg(unix)]
#[test]
fn total_operation_deadline_kills_and_reaps_child() {
    let temp = tempfile::tempdir().unwrap();
    let pid_path = temp.path().join("timeout-descendant.pid");
    let mut command = Command::new("sh");
    command
        .args([
            "-c",
            "sleep 30 & echo $! > \"$MDP_TEST_CHILD_PID_FILE\"; wait",
        ])
        .env("MDP_TEST_CHILD_PID_FILE", &pid_path);
    let started = Instant::now();
    let result = run_bounded_command(command, 1024, Duration::from_millis(50));
    assert!(matches!(
        result,
        Err(MarketDataError::Storage(StorageFailure::Timeout))
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_pid_is_gone(read_pid(&pid_path));
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
    let temp = tempfile::tempdir().unwrap();
    let pid_path = temp.path().join("inherited-pipe-descendant.pid");
    let mut command = Command::new("sh");
    command
        .args([
            "-c",
            "sleep 30 & echo $! > \"$MDP_TEST_CHILD_PID_FILE\"; exit 0",
        ])
        .env("MDP_TEST_CHILD_PID_FILE", &pid_path);
    let started = Instant::now();
    let result = run_bounded_command(command, 1024, Duration::from_secs(2)).unwrap();
    assert_eq!(result.status_code, 0);
    assert!(result.stdout.is_empty());
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_pid_is_gone(read_pid(&pid_path));
}

#[cfg(unix)]
#[test]
fn request_cancellation_kills_child_group_and_waits_for_reap() {
    use std::thread;

    let temp = tempfile::tempdir().unwrap();
    let pid_path = temp.path().join("cancel-descendant.pid");
    let cancellation = crate::cancellation::CancellationToken::new();
    let worker_cancellation = cancellation.clone();
    let worker_pid_path = pid_path.clone();
    let worker = thread::spawn(move || {
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "sleep 30 & echo $! > \"$MDP_TEST_CHILD_PID_FILE\"; wait",
            ])
            .env("MDP_TEST_CHILD_PID_FILE", worker_pid_path);
        run_bounded_command_cancellable(command, 1024, Duration::from_secs(10), worker_cancellation)
    });
    wait_for_file(&pid_path);
    let descendant = read_pid(&pid_path);
    cancellation.cancel();
    assert!(matches!(
        worker.join().unwrap(),
        Err(MarketDataError::Storage(StorageFailure::Cancelled))
    ));
    assert_pid_is_gone(descendant);
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

#[cfg(target_os = "linux")]
#[test]
fn local_pair_copy_cancellation_removes_a_partially_copied_private_file() {
    use std::os::unix::fs::PermissionsExt;

    let temporary = tempfile::tempdir().unwrap();
    let private_run = temporary.path().join("pair-readback-1");
    fs::create_dir(&private_run).unwrap();
    fs::set_permissions(&private_run, fs::Permissions::from_mode(0o700)).unwrap();
    let source_path = temporary.path().join("source.bin");
    fs::write(&source_path, vec![0x5a; 256 * 1024]).unwrap();
    let destination = private_run.join("object-0.parquet");
    let cancellation = crate::cancellation::CancellationToken::new();
    let worker_cancellation = cancellation.clone();
    let result = copy_open_file_limited_private_with_progress(
        File::open(source_path).unwrap(),
        &destination,
        512 * 1024,
        &cancellation,
        move || worker_cancellation.cancel(),
    );

    assert!(matches!(
        result,
        Err(MarketDataError::Storage(StorageFailure::Cancelled))
    ));
    assert!(!destination.exists());
    assert_eq!(fs::read_dir(private_run).unwrap().count(), 0);
}

#[cfg(target_os = "linux")]
#[test]
fn decode_worker_memory_bomb_is_contained_and_reaped() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let temp = tempfile::tempdir().unwrap();
    let pid_path = temp.path().join("worker.pid");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg("storage::tests::decode_worker_memory_bomb_child")
        .arg("--nocapture")
        .env("MDP_TEST_DECODE_MEMORY_BOMB", "1")
        .env("MDP_TEST_DECODE_WORKER_PID_FILE", &pid_path);
    let output = run_bounded_decode_worker(command, 16 * 1024, Duration::from_secs(30)).unwrap();
    assert!(String::from_utf8_lossy(&output).contains("RESOURCE_LIMIT_REACHED"));
    let process_id = std::fs::read_to_string(pid_path)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    let pid = rustix::process::Pid::from_raw(process_id).unwrap();
    assert!(matches!(
        rustix::process::kill_process(pid, rustix::process::Signal::KILL),
        Err(rustix::io::Errno::SRCH)
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn decode_worker_memory_bomb_child() {
    if std::env::var_os("MDP_TEST_DECODE_MEMORY_BOMB").is_none() {
        return;
    }
    let pid_path = std::env::var_os("MDP_TEST_DECODE_WORKER_PID_FILE").unwrap();
    std::fs::write(pid_path, std::process::id().to_string()).unwrap();
    let mut retained_pages = Vec::<Vec<u8>>::new();
    loop {
        let mut page = Vec::new();
        if page.try_reserve_exact(64 * 1024 * 1024).is_err() {
            println!("RESOURCE_LIMIT_REACHED");
            return;
        }
        page.resize(64 * 1024 * 1024, 0x5a);
        retained_pages.push(page);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn decode_worker_third_concurrent_request_fails_closed() {
    use std::thread;

    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let temp = tempfile::tempdir().unwrap();
    let markers = [
        temp.path().join("worker-1.started"),
        temp.path().join("worker-2.started"),
    ];
    let workers = markers
        .iter()
        .map(|marker| {
            let marker = marker.clone();
            thread::spawn(move || {
                let mut command = Command::new(std::env::current_exe().unwrap());
                command
                    .args([
                        "--exact",
                        "storage::tests::decode_worker_wait_child",
                        "--nocapture",
                    ])
                    .env("MDP_TEST_DECODE_WAIT_MARKER", marker);
                run_bounded_decode_worker(command, 16 * 1024, Duration::from_secs(5))
            })
        })
        .collect::<Vec<_>>();

    let deadline = Instant::now() + Duration::from_secs(2);
    while !(markers.iter().all(|marker| marker.exists())) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(markers.iter().all(|marker| marker.exists()));
    assert!(matches!(
        run_bounded_decode_worker(Command::new("true"), 1024, Duration::from_secs(1)),
        Err(MarketDataError::WriterLimit)
    ));
    for worker in workers {
        let output = worker.join().unwrap().unwrap();
        assert!(String::from_utf8_lossy(&output).contains("DECODE_WORKER_DONE"));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn rapid_worker_cancellation_keeps_capacity_until_child_group_is_reaped() {
    use std::thread;

    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let temp = tempfile::tempdir().unwrap();
    let markers = [
        temp.path().join("cancel-1.pid"),
        temp.path().join("cancel-2.pid"),
    ];
    let cancellations = [
        crate::cancellation::CancellationToken::new(),
        crate::cancellation::CancellationToken::new(),
    ];
    let workers = markers
        .iter()
        .zip(cancellations.iter())
        .map(|(marker, cancellation)| {
            let marker = marker.clone();
            let cancellation = cancellation.clone();
            thread::spawn(move || {
                let mut command = Command::new("sh");
                command
                    .args([
                        "-c",
                        "sleep 30 & echo $! > \"$MDP_TEST_CHILD_PID_FILE\"; wait",
                    ])
                    .env("MDP_TEST_CHILD_PID_FILE", marker);
                run_bounded_decode_worker_cancellable(
                    command,
                    1024,
                    Duration::from_secs(10),
                    cancellation,
                )
            })
        })
        .collect::<Vec<_>>();
    for marker in &markers {
        wait_for_file(marker);
    }
    let descendants = markers
        .iter()
        .map(|path| read_pid(path))
        .collect::<Vec<_>>();
    assert_eq!(process::active_decode_worker_count(), 2);
    assert!(matches!(
        run_bounded_decode_worker(Command::new("true"), 1024, Duration::from_secs(2)),
        Err(MarketDataError::WriterLimit)
    ));

    cancellations
        .iter()
        .for_each(crate::cancellation::CancellationToken::cancel);
    let attempt_while_cleaning =
        run_bounded_decode_worker(Command::new("true"), 1024, Duration::from_secs(2));
    for worker in workers {
        assert!(matches!(
            worker.join().unwrap(),
            Err(MarketDataError::Storage(StorageFailure::Cancelled))
        ));
    }
    for descendant in descendants {
        assert_pid_is_gone(descendant);
    }
    if matches!(attempt_while_cleaning, Err(MarketDataError::WriterLimit)) {
        run_bounded_decode_worker(Command::new("true"), 1024, Duration::from_secs(2)).unwrap();
    } else {
        attempt_while_cleaning.unwrap();
    }
    assert_eq!(process::active_decode_worker_count(), 0);
}

#[cfg(target_os = "linux")]
#[test]
fn decode_worker_enforces_output_and_deadline_and_rejects_child_error() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    assert!(matches!(
        run_bounded_decode_worker(Command::new("yes"), 128, Duration::from_secs(2)),
        Err(MarketDataError::Storage(StorageFailure::MalformedListing))
    ));
    let mut timeout = Command::new("sh");
    timeout.args(["-c", "sleep 30"]);
    assert!(matches!(
        run_bounded_decode_worker(timeout, 1024, Duration::from_millis(50)),
        Err(MarketDataError::Storage(StorageFailure::Timeout))
    ));
    assert!(matches!(
        run_bounded_decode_worker(Command::new("false"), 1024, Duration::from_secs(2)),
        Err(MarketDataError::Parquet)
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn decode_worker_wait_child() {
    if let Some(marker) = std::env::var_os("MDP_TEST_DECODE_WAIT_MARKER") {
        std::fs::write(marker, std::process::id().to_string()).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        println!("DECODE_WORKER_DONE");
    }
}

#[cfg(unix)]
fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        path.is_file(),
        "expected child PID marker at {}",
        path.display()
    );
}

#[cfg(unix)]
fn read_pid(path: &Path) -> i32 {
    std::fs::read_to_string(path)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[cfg(unix)]
fn assert_pid_is_gone(process_id: i32) {
    use rustix::process::{Pid, Signal, kill_process};

    let pid = Pid::from_raw(process_id).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match kill_process(pid, Signal::KILL) {
            Err(rustix::io::Errno::SRCH) => return,
            Ok(()) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            result => {
                panic!("descendant PID {process_id} remained after supervised cleanup: {result:?}")
            }
        }
    }
}
