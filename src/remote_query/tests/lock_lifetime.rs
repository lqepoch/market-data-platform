use super::seed_synthetic_archive;
use crate::{
    MarketDataError, Result,
    archive::TransportKind,
    storage::{LocalTestTransport, ObjectTransport, RemoteObject},
};
use fs2::FileExt;
use rustix::io::{Errno, read, write};
use std::{
    fs::{self, OpenOptions},
    io,
    os::unix::{net::UnixStream, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use super::super::{DatasetNamespace, RemoteArchiveReader, RemoteCacheLimits};

const DATASET_ID: &str = "synthetic-2026-10-08-four-bars-parquet-v3-bars-1m-v1";
const PRE_EXEC_TIMEOUT: Duration = Duration::from_secs(8);

struct ForkExecLockGate {
    parent: UnixStream,
    child: Option<UnixStream>,
    spawn_thread: Option<JoinHandle<io::Result<ExitStatus>>>,
}

impl ForkExecLockGate {
    fn new() -> io::Result<Self> {
        let (parent, child) = UnixStream::pair()?;
        for stream in [&parent, &child] {
            stream.set_read_timeout(Some(PRE_EXEC_TIMEOUT))?;
            stream.set_write_timeout(Some(PRE_EXEC_TIMEOUT))?;
        }
        Ok(Self {
            parent,
            child: Some(child),
            spawn_thread: None,
        })
    }

    fn start_and_wait_for_pre_exec(&mut self) -> io::Result<()> {
        let child = self
            .child
            .take()
            .ok_or_else(|| io::Error::from(io::ErrorKind::AlreadyExists))?;
        let mut command = Command::new("true");
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        // SAFETY: The hook only uses pre-created Unix streams and rustix read/write syscalls.
        // It takes no locks, allocates no memory, and performs no formatting or logging.
        unsafe {
            command.pre_exec(move || {
                write_one(&child, b'R').map_err(io_error_from_errno)?;
                let release = read_one(&child).map_err(io_error_from_errno)?;
                if release != b'G' {
                    return Err(io_error_from_errno(Errno::INVAL));
                }
                Ok(())
            });
        }

        self.spawn_thread = Some(
            thread::Builder::new()
                .name("mdp-flock-pre-exec-gate".to_owned())
                .spawn(move || {
                    let mut child = command.spawn()?;
                    child.wait()
                })?,
        );

        let entered = read_one(&self.parent).map_err(io_error_from_errno)?;
        if entered != b'R' {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        Ok(())
    }

    fn release_and_join(&mut self) -> io::Result<()> {
        let Some(spawn_thread) = self.spawn_thread.take() else {
            return Ok(());
        };

        let release_result = write_one(&self.parent, b'G').map_err(io_error_from_errno);
        let child_status = spawn_thread
            .join()
            .map_err(|_| io::Error::other("pre-exec spawn thread panicked"))??;
        release_result?;
        if !child_status.success() {
            return Err(io::Error::other("gated child command failed"));
        }
        Ok(())
    }
}

impl Drop for ForkExecLockGate {
    fn drop(&mut self) {
        let _ = self.release_and_join();
    }
}

#[derive(Clone, Copy, Debug)]
struct LockAttempt {
    blocked: bool,
    kind: Option<io::ErrorKind>,
    raw_os_error: Option<i32>,
}

impl LockAttempt {
    fn from_result(result: io::Result<()>) -> Self {
        match result {
            Ok(()) => Self {
                blocked: false,
                kind: None,
                raw_os_error: None,
            },
            Err(error) => Self {
                blocked: error.kind() == io::ErrorKind::WouldBlock,
                kind: Some(error.kind()),
                raw_os_error: error.raw_os_error(),
            },
        }
    }
}

struct ForkExecGateTransport {
    inner: LocalTestTransport,
    gate: Arc<Mutex<ForkExecLockGate>>,
    lock_path: PathBuf,
    active_lock_attempt: Arc<Mutex<Option<LockAttempt>>>,
    first_lookup: AtomicBool,
    fail_first_lookup: bool,
}

impl ObjectTransport for ForkExecGateTransport {
    fn lookup(&self, dataset_id: &str, object_name: &str) -> Result<Option<RemoteObject>> {
        if !self.first_lookup.swap(true, Ordering::AcqRel) {
            self.gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .start_and_wait_for_pre_exec()?;
            let active_attempt = LockAttempt::from_result(lock_probe(&self.lock_path));
            *self
                .active_lock_attempt
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(active_attempt);
            if self.fail_first_lookup {
                return Err(MarketDataError::UnknownOutcome);
            }
        }
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
        self.inner
            .download_with_limit(dataset_id, object_name, destination, max_bytes)
    }
}

fn reader_with_fork_exec_gate(
    remote_root: PathBuf,
    cache_root: PathBuf,
    gate: Arc<Mutex<ForkExecLockGate>>,
    active_lock_attempt: Arc<Mutex<Option<LockAttempt>>>,
    fail_first_lookup: bool,
) -> Result<RemoteArchiveReader> {
    fs::create_dir_all(&cache_root)?;
    let cache_root = fs::canonicalize(cache_root)?;
    let lock_path = cache_root
        .join(".locks")
        .join(format!("diagnostic-{DATASET_ID}.lock"));
    let transport = ForkExecGateTransport {
        inner: LocalTestTransport::new(remote_root)?,
        gate,
        lock_path,
        active_lock_attempt,
        first_lookup: AtomicBool::new(false),
        fail_first_lookup,
    };
    RemoteArchiveReader::with_clock(
        Arc::new(transport),
        TransportKind::LocalTest,
        cache_root,
        RemoteCacheLimits::default(),
        Arc::new(|| 1_000),
    )
}

fn lock_probe(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    file.try_lock_exclusive()?;
    file.unlock()
}

fn read_one(fd: &UnixStream) -> rustix::io::Result<u8> {
    loop {
        let mut byte = [0_u8; 1];
        match read(fd, &mut byte) {
            Err(Errno::INTR) => continue,
            Err(error) => return Err(error),
            Ok(0) => return Err(Errno::PIPE),
            Ok(1) => return Ok(byte[0]),
            Ok(_) => return Err(Errno::IO),
        }
    }
}

fn write_one(fd: &UnixStream, byte: u8) -> rustix::io::Result<()> {
    loop {
        match write(fd, &[byte]) {
            Err(Errno::INTR) => continue,
            Err(error) => return Err(error),
            Ok(0) => return Err(Errno::PIPE),
            Ok(1) => return Ok(()),
            Ok(_) => return Err(Errno::IO),
        }
    }
}

fn io_error_from_errno(error: Errno) -> io::Error {
    io::Error::from_raw_os_error(error.raw_os_error())
}

fn assert_active_query_excluded_second_lock(attempt: Option<LockAttempt>, path: &Path) {
    let attempt = attempt.expect("transport must probe the active dataset lock");
    assert!(
        attempt.blocked,
        "active query must keep dataset lock exclusive: path={}, error_kind={:?}, raw_os_error={:?}",
        path.display(),
        attempt.kind,
        attempt.raw_os_error
    );
}

#[tokio::test]
async fn completed_query_releases_dataset_lock_before_inherited_child_execs() {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = seed_synthetic_archive(&temp.path().join("remote")).await;
    let cache_root = temp.path().join("cache");
    let gate = Arc::new(Mutex::new(ForkExecLockGate::new().unwrap()));
    let active_lock_attempt = Arc::new(Mutex::new(None));
    let reader = reader_with_fork_exec_gate(
        remote_root,
        cache_root.clone(),
        Arc::clone(&gate),
        Arc::clone(&active_lock_attempt),
        false,
    )
    .unwrap();
    let lock_path = fs::canonicalize(&cache_root)
        .unwrap()
        .join(".locks")
        .join(format!("diagnostic-{DATASET_ID}.lock"));

    let (_, first) = reader
        .query_bars(DatasetNamespace::Diagnostic, DATASET_ID, Some("QQQ"))
        .unwrap();
    assert!(!first.cache_hit);
    let active_attempt = *active_lock_attempt
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_active_query_excluded_second_lock(active_attempt, &lock_path);

    let second = reader.query_bars(DatasetNamespace::Diagnostic, DATASET_ID, None);
    let inherited_lock_attempt = LockAttempt::from_result(lock_probe(&lock_path));
    let release = gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .release_and_join();
    let lock_after_child_exit = lock_probe(&lock_path);

    assert!(release.is_ok(), "gated child cleanup failed: {release:?}");
    assert!(
        lock_after_child_exit.is_ok(),
        "dataset lock must be available after child exit: path={}, error_kind={:?}, raw_os_error={:?}",
        lock_path.display(),
        lock_after_child_exit.as_ref().err().map(io::Error::kind),
        lock_after_child_exit
            .as_ref()
            .err()
            .and_then(io::Error::raw_os_error)
    );
    assert!(
        matches!(second, Ok((_, ref summary)) if summary.cache_hit),
        "completed cache-hit query must reacquire dataset lock while inherited child waits before exec: path={}, active_probe={active_attempt:?}, child_wait_probe={inherited_lock_attempt:?}, second_query={second:?}",
        lock_path.display()
    );
}

#[tokio::test]
async fn failed_query_releases_dataset_lock_before_inherited_child_execs() {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = seed_synthetic_archive(&temp.path().join("remote")).await;
    let cache_root = temp.path().join("cache");
    let gate = Arc::new(Mutex::new(ForkExecLockGate::new().unwrap()));
    let active_lock_attempt = Arc::new(Mutex::new(None));
    let reader = reader_with_fork_exec_gate(
        remote_root,
        cache_root.clone(),
        Arc::clone(&gate),
        Arc::clone(&active_lock_attempt),
        true,
    )
    .unwrap();
    let lock_path = fs::canonicalize(&cache_root)
        .unwrap()
        .join(".locks")
        .join(format!("diagnostic-{DATASET_ID}.lock"));

    assert!(matches!(
        reader.query_bars(DatasetNamespace::Diagnostic, DATASET_ID, None),
        Err(MarketDataError::UnknownOutcome)
    ));
    let active_attempt = *active_lock_attempt
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_active_query_excluded_second_lock(active_attempt, &lock_path);

    let retry = reader.query_bars(DatasetNamespace::Diagnostic, DATASET_ID, None);
    let inherited_lock_attempt = LockAttempt::from_result(lock_probe(&lock_path));
    let release = gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .release_and_join();
    let lock_after_child_exit = lock_probe(&lock_path);

    assert!(release.is_ok(), "gated child cleanup failed: {release:?}");
    assert!(lock_after_child_exit.is_ok());
    assert!(
        matches!(retry, Ok((_, ref summary)) if !summary.cache_hit),
        "retry after failed query must reacquire dataset lock while inherited child waits before exec: path={}, active_probe={active_attempt:?}, child_wait_probe={inherited_lock_attempt:?}, retry={retry:?}",
        lock_path.display()
    );
}
