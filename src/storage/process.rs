//! Bounded subprocess lifecycle shared by rclone metadata and streaming operations.

use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
    process::{Child, ChildStdout, Command, ExitStatus, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    MarketDataError, Result, cancellation::CancellationToken, error::StorageFailure,
    queue::BackgroundWorkerPermit,
};

use super::RemoveFileOnDrop;

const MAX_ACTIVE_DECODE_WORKERS: usize = 2;
const DECODE_WORKER_MAX_ADDRESS_SPACE: u64 = 1024 * 1024 * 1024;
const DECODE_WORKER_CPU_SECONDS: u64 = 60;
pub(crate) const MAX_DECODE_WORKER_OUTPUT_BYTES: usize = 256 * 1024 * 1024;
static ACTIVE_DECODE_WORKERS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(super) fn active_decode_worker_count() -> usize {
    ACTIVE_DECODE_WORKERS.load(Ordering::Acquire)
}

struct DecodeWorkerPermit;

impl DecodeWorkerPermit {
    fn acquire() -> Result<Self> {
        ACTIVE_DECODE_WORKERS
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < MAX_ACTIVE_DECODE_WORKERS).then_some(active + 1)
            })
            .map_err(|_| MarketDataError::WriterLimit)?;
        Ok(Self)
    }
}

impl Drop for DecodeWorkerPermit {
    fn drop(&mut self) {
        ACTIVE_DECODE_WORKERS.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct CommandOutput {
    pub(super) status_code: i32,
    pub(super) stdout: Vec<u8>,
}

enum SupervisedExit {
    Exited(ExitStatus),
    TimedOut,
    Cancelled,
    Failed,
}

struct SupervisedProcess {
    stdout: ChildStdout,
    cancel_sender: SyncSender<()>,
    exit_receiver: Receiver<SupervisedExit>,
    supervisor: Option<JoinHandle<()>>,
    deadline: Instant,
}

impl SupervisedProcess {
    fn start(
        mut command: Command,
        operation_timeout: Duration,
        worker_name: &'static str,
        cancellation: Option<CancellationToken>,
    ) -> Result<Self> {
        if cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(MarketDataError::Storage(StorageFailure::Cancelled));
        }
        configure_process_group(&mut command)?;
        let supervisor_permit = BackgroundWorkerPermit::acquire()?;
        let deadline = Instant::now()
            .checked_add(operation_timeout)
            .ok_or(MarketDataError::InvalidInput)?;
        let (child_sender, child_receiver) = mpsc::sync_channel::<Child>(1);
        let (cancel_sender, cancel_receiver) = mpsc::sync_channel::<()>(1);
        let (exit_sender, exit_receiver) = mpsc::sync_channel::<SupervisedExit>(1);
        let supervisor = thread::Builder::new()
            .name(worker_name.to_owned())
            .spawn(move || {
                let _supervisor_permit = supervisor_permit;
                supervise_child(
                    child_receiver,
                    cancel_receiver,
                    exit_sender,
                    deadline,
                    cancellation,
                );
            })
            .map_err(|_| MarketDataError::Storage(StorageFailure::Spawn))?;

        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(_) => {
                drop(child_sender);
                let _ = supervisor.join();
                return Err(MarketDataError::Storage(StorageFailure::Spawn));
            }
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = terminate_process_group(&mut child);
            drop(child_sender);
            let _ = supervisor.join();
            return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
        };
        if let Err(error) = child_sender.send(child) {
            let mut child = error.0;
            let _ = terminate_process_group(&mut child);
            let _ = supervisor.join();
            return Err(MarketDataError::Storage(StorageFailure::Spawn));
        }

        Ok(Self {
            stdout,
            cancel_sender,
            exit_receiver,
            supervisor: Some(supervisor),
            deadline,
        })
    }

    fn cancel(&self) {
        let _ = self.cancel_sender.try_send(());
    }

    fn finish(&mut self) -> Result<SupervisedExit> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        let outcome = match self.exit_receiver.recv_timeout(remaining) {
            Ok(outcome) => outcome,
            Err(RecvTimeoutError::Timeout) => {
                self.cancel();
                self.join_supervisor()?;
                return Err(MarketDataError::Storage(StorageFailure::Timeout));
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.cancel();
                self.join_supervisor()?;
                return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
            }
        };
        self.join_supervisor()?;
        Ok(outcome)
    }

    fn join_supervisor(&mut self) -> Result<()> {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor
                .join()
                .map_err(|_| MarketDataError::Storage(StorageFailure::CommandFailed))?;
        }
        Ok(())
    }
}

impl Drop for SupervisedProcess {
    fn drop(&mut self) {
        if self.supervisor.is_some() {
            self.cancel();
            let _ = self.join_supervisor();
        }
    }
}

fn supervise_child(
    child_receiver: Receiver<Child>,
    cancel_receiver: Receiver<()>,
    exit_sender: SyncSender<SupervisedExit>,
    deadline: Instant,
    cancellation: Option<CancellationToken>,
) {
    let Ok(mut child) = child_receiver.recv() else {
        return;
    };
    loop {
        if cancel_receiver.try_recv().is_ok()
            || cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        {
            let status = terminate_process_group(&mut child);
            let outcome = if status.is_ok() {
                SupervisedExit::Cancelled
            } else {
                SupervisedExit::Failed
            };
            let _ = exit_sender.send(outcome);
            return;
        }
        if Instant::now() >= deadline {
            let status = terminate_process_group(&mut child);
            let outcome = if status.is_ok() {
                SupervisedExit::TimedOut
            } else {
                SupervisedExit::Failed
            };
            let _ = exit_sender.send(outcome);
            return;
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                let outcome = if terminate_process_group_members(child.id()).is_ok() {
                    SupervisedExit::Exited(status)
                } else {
                    SupervisedExit::Failed
                };
                let _ = exit_sender.send(outcome);
                return;
            }
            Ok(None) => thread::sleep(Duration::from_millis(5)),
            Err(_) => {
                let _ = terminate_process_group(&mut child);
                let _ = exit_sender.send(SupervisedExit::Failed);
                return;
            }
        }
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) -> Result<()> {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
    Ok(())
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) -> Result<()> {
    Err(MarketDataError::Storage(StorageFailure::Unsupported))
}

fn terminate_process_group(child: &mut Child) -> std::io::Result<ExitStatus> {
    let group_result = terminate_process_group_members(child.id());
    let _ = child.kill();
    let status = child.wait()?;
    group_result?;
    Ok(status)
}

#[cfg(unix)]
fn terminate_process_group_members(process_id: u32) -> std::io::Result<()> {
    use rustix::{
        io::Errno,
        process::{Pid, Signal, kill_process_group},
    };

    let process_id = i32::try_from(process_id)
        .map_err(|_| std::io::Error::other("child process id is outside platform range"))?;
    let pid = Pid::from_raw(process_id)
        .ok_or_else(|| std::io::Error::other("child process id is invalid"))?;
    match kill_process_group(pid, Signal::KILL) {
        Ok(()) | Err(Errno::SRCH) => Ok(()),
        Err(_) => Err(std::io::Error::other("process group termination failed")),
    }
}

#[cfg(not(unix))]
fn terminate_process_group_members(_process_id: u32) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "process group termination is unsupported",
    ))
}

pub(super) fn run_bounded_command(
    command: Command,
    output_cap: usize,
    operation_timeout: Duration,
) -> Result<CommandOutput> {
    run_bounded_command_with_cancel(command, output_cap, operation_timeout, None)
}

pub(super) fn run_bounded_command_cancellable(
    command: Command,
    output_cap: usize,
    operation_timeout: Duration,
    cancellation: CancellationToken,
) -> Result<CommandOutput> {
    run_bounded_command_with_cancel(command, output_cap, operation_timeout, Some(cancellation))
}

fn run_bounded_command_with_cancel(
    command: Command,
    output_cap: usize,
    operation_timeout: Duration,
    cancellation: Option<CancellationToken>,
) -> Result<CommandOutput> {
    let mut process = SupervisedProcess::start(
        command,
        operation_timeout,
        "mdp-command-supervisor",
        cancellation.clone(),
    )?;
    let mut output = Vec::with_capacity(output_cap.min(64 * 1024));
    let read_result = process
        .stdout
        .by_ref()
        .take(
            u64::try_from(output_cap)
                .unwrap_or(u64::MAX)
                .saturating_add(1),
        )
        .read_to_end(&mut output);
    let malformed_output = read_result.is_err() || output.len() > output_cap;
    if malformed_output {
        process.cancel();
    }
    if cancellation
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        process.cancel();
    }
    let outcome = process.finish()?;
    if cancellation
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        return Err(MarketDataError::Storage(StorageFailure::Cancelled));
    }
    if matches!(outcome, SupervisedExit::TimedOut) {
        return Err(MarketDataError::Storage(StorageFailure::Timeout));
    }
    if malformed_output || matches!(outcome, SupervisedExit::Cancelled) {
        return Err(MarketDataError::Storage(StorageFailure::MalformedListing));
    }
    let SupervisedExit::Exited(status) = outcome else {
        return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
    };
    Ok(CommandOutput {
        status_code: status.code().unwrap_or(255),
        stdout: output,
    })
}

pub(super) fn run_bounded_decode_worker(
    command: Command,
    output_cap: usize,
    operation_timeout: Duration,
) -> Result<Vec<u8>> {
    run_bounded_decode_worker_with_cancel(command, output_cap, operation_timeout, None)
}

pub(super) fn run_bounded_decode_worker_cancellable(
    command: Command,
    output_cap: usize,
    operation_timeout: Duration,
    cancellation: CancellationToken,
) -> Result<Vec<u8>> {
    run_bounded_decode_worker_with_cancel(
        command,
        output_cap,
        operation_timeout,
        Some(cancellation),
    )
}

fn run_bounded_decode_worker_with_cancel(
    mut command: Command,
    output_cap: usize,
    operation_timeout: Duration,
    cancellation: Option<CancellationToken>,
) -> Result<Vec<u8>> {
    if output_cap == 0 || output_cap > MAX_DECODE_WORKER_OUTPUT_BYTES {
        return Err(MarketDataError::InputLimit);
    }
    let _decode_permit = DecodeWorkerPermit::acquire()?;
    configure_decode_worker_limits(&mut command)?;
    let output =
        run_bounded_command_with_cancel(command, output_cap, operation_timeout, cancellation)?;
    if output.status_code != 0 {
        return Err(MarketDataError::Parquet);
    }
    Ok(output.stdout)
}

#[cfg(target_os = "linux")]
fn configure_decode_worker_limits(command: &mut Command) -> Result<()> {
    use rustix::process::{Resource, Rlimit, setrlimit};
    use std::os::unix::process::CommandExt;

    // Keep this hook to async-signal-safe syscalls and errno-only error conversion. The hard and
    // soft limits match, so the child cannot raise them after exec.
    unsafe {
        command.pre_exec(|| {
            setrlimit(
                Resource::As,
                Rlimit {
                    current: Some(DECODE_WORKER_MAX_ADDRESS_SPACE),
                    maximum: Some(DECODE_WORKER_MAX_ADDRESS_SPACE),
                },
            )
            .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))?;
            setrlimit(
                Resource::Cpu,
                Rlimit {
                    current: Some(DECODE_WORKER_CPU_SECONDS),
                    maximum: Some(DECODE_WORKER_CPU_SECONDS),
                },
            )
            .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))?;
            Ok(())
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn configure_decode_worker_limits(_command: &mut Command) -> Result<()> {
    Err(MarketDataError::Storage(StorageFailure::Unsupported))
}

pub(super) fn run_bounded_download(
    command: Command,
    destination: &Path,
    output_cap: u64,
    operation_timeout: Duration,
) -> Result<()> {
    run_bounded_download_with_cancel(command, destination, output_cap, operation_timeout, None)
}

pub(super) fn run_bounded_download_cancellable(
    command: Command,
    destination: &Path,
    output_cap: u64,
    operation_timeout: Duration,
    cancellation: CancellationToken,
) -> Result<()> {
    run_bounded_download_with_cancel(
        command,
        destination,
        output_cap,
        operation_timeout,
        Some(cancellation),
    )
}

fn run_bounded_download_with_cancel(
    command: Command,
    destination: &Path,
    output_cap: u64,
    operation_timeout: Duration,
    cancellation: Option<CancellationToken>,
) -> Result<()> {
    if output_cap == 0 {
        return Err(MarketDataError::InvalidInput);
    }
    match std::fs::symlink_metadata(destination) {
        Ok(_) => return Err(MarketDataError::Conflict),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(MarketDataError::Io(error)),
    }
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut cleanup = RemoveFileOnDrop(Some(destination.to_path_buf()));
    let mut process = SupervisedProcess::start(
        command,
        operation_timeout,
        "mdp-download-supervisor",
        cancellation.clone(),
    )?;
    let mut received = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    let mut over_limit = false;
    let mut output_error = None;
    loop {
        if cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            process.cancel();
            break;
        }
        match process.stdout.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                let Some(next) = received.checked_add(count as u64) else {
                    over_limit = true;
                    process.cancel();
                    break;
                };
                if next > output_cap {
                    over_limit = true;
                    process.cancel();
                    break;
                }
                if let Err(error) = target.write_all(&buffer[..count]) {
                    output_error = Some(error);
                    process.cancel();
                    break;
                }
                received = next;
            }
            Err(error) => {
                output_error = Some(error);
                process.cancel();
                break;
            }
        }
    }
    let outcome = process.finish()?;
    if cancellation
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        return Err(MarketDataError::Storage(StorageFailure::Cancelled));
    }
    if matches!(outcome, SupervisedExit::TimedOut) {
        return Err(MarketDataError::Storage(StorageFailure::Timeout));
    }
    if over_limit {
        return Err(MarketDataError::InputLimit);
    }
    if let Some(error) = output_error {
        return Err(MarketDataError::Io(error));
    }
    let SupervisedExit::Exited(status) = outcome else {
        return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
    };
    if !status.success() {
        return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
    }
    if received == 0 {
        return Err(MarketDataError::Storage(StorageFailure::ReadbackFailed));
    }
    target.sync_all()?;
    cleanup.0 = None;
    Ok(())
}
