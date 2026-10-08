//! Private, append-only local storage for exact pre-decode frames.
//!
//! A successful acknowledgement means that the exact frame and its source-local
//! recovery identity were synchronized to this local spool. It does not attest
//! provider entitlement, capture completeness, or archive publication. A spool
//! found after process restart is preserved as unknown and is never resumed.

use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use broker_ports::{
    RawCaptureInstanceId, RawFrameCapture, RawFrameFinalization, RawFrameSinkError,
    RawFrameSinkFactory,
};
use tokio::sync::watch;

#[cfg(target_os = "linux")]
mod linux;

pub const DEFAULT_RAW_SPOOL_MAX_CAPTURE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const DEFAULT_RAW_SPOOL_MAX_TOTAL_BYTES: u64 = 32 * 1024 * 1024 * 1024;
pub const DEFAULT_RAW_SPOOL_MAX_FRAMES_PER_CAPTURE: u32 = 65_536;
pub const DEFAULT_RAW_SPOOL_MAX_CAPTURE_IDENTITIES: u32 = 1_024;

const HARD_MAX_CAPTURE_BYTES: u64 = DEFAULT_RAW_SPOOL_MAX_CAPTURE_BYTES;
const HARD_MAX_TOTAL_BYTES: u64 = DEFAULT_RAW_SPOOL_MAX_TOTAL_BYTES;
const HARD_MAX_FRAMES_PER_CAPTURE: u32 = DEFAULT_RAW_SPOOL_MAX_FRAMES_PER_CAPTURE;
const HARD_MAX_CAPTURE_IDENTITIES: u32 = DEFAULT_RAW_SPOOL_MAX_CAPTURE_IDENTITIES;

/// Fixed limits for a private local raw-frame spool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawFrameSpoolLimits {
    pub max_capture_bytes: u64,
    pub max_total_bytes: u64,
    pub max_frames_per_capture: u32,
    pub max_capture_identities: u32,
}

impl Default for RawFrameSpoolLimits {
    fn default() -> Self {
        Self {
            max_capture_bytes: DEFAULT_RAW_SPOOL_MAX_CAPTURE_BYTES,
            max_total_bytes: DEFAULT_RAW_SPOOL_MAX_TOTAL_BYTES,
            max_frames_per_capture: DEFAULT_RAW_SPOOL_MAX_FRAMES_PER_CAPTURE,
            max_capture_identities: DEFAULT_RAW_SPOOL_MAX_CAPTURE_IDENTITIES,
        }
    }
}

impl RawFrameSpoolLimits {
    fn valid(self) -> bool {
        self.max_capture_bytes > 0
            && self.max_capture_bytes <= HARD_MAX_CAPTURE_BYTES
            && self.max_total_bytes >= self.max_capture_bytes
            && self.max_total_bytes <= HARD_MAX_TOTAL_BYTES
            && self.max_frames_per_capture > 0
            && self.max_frames_per_capture <= HARD_MAX_FRAMES_PER_CAPTURE
            && self.max_capture_identities > 0
            && self.max_capture_identities <= HARD_MAX_CAPTURE_IDENTITIES
    }
}

/// Counts pre-existing spool directories that were preserved as unknown.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RawFrameSpoolRecoverySummary {
    pub preserved_capture_directories: u32,
    pub preserved_bytes: u64,
}

/// Linux-only owner-private raw-frame spool factory.
///
/// The factory holds an exclusive process lock for its lifetime. Existing
/// capture directories are counted against capacity, left untouched, and never
/// resumed. Each returned sink gets a fresh UUIDv4 that remains stable across
/// reconnect generations handled by that sink.
pub struct LocalRawFrameSpoolFactory {
    #[cfg(target_os = "linux")]
    inner: Arc<linux::FactoryInner>,
    recovery: RawFrameSpoolRecoverySummary,
}

impl fmt::Debug for LocalRawFrameSpoolFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalRawFrameSpoolFactory")
            .field("root", &"redacted")
            .field("recovery", &self.recovery)
            .finish()
    }
}

impl LocalRawFrameSpoolFactory {
    /// Opens a private spool root whose parent directory already exists.
    ///
    /// The root and all files created below it are restricted to the effective Unix user.
    /// Non-Linux platforms fail closed.
    pub fn open(
        root: impl Into<PathBuf>,
        limits: RawFrameSpoolLimits,
    ) -> Result<Self, RawFrameSinkError> {
        #[cfg(target_os = "linux")]
        {
            let (inner, recovery) = linux::open(root.into(), limits)?;
            Ok(Self { inner, recovery })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (root.into(), limits);
            Err(RawFrameSinkError::Unavailable)
        }
    }

    /// Stops accepting frame writes and waits until every owned blocking write has exited.
    ///
    /// Call this after cancelling and joining the subscription tasks that use the factory.
    /// Cancelled writes fail closed and cannot return an acknowledgement. The worker permit and
    /// spool lock remain held until each blocking filesystem operation has actually exited.
    pub async fn shutdown(&self) {
        #[cfg(target_os = "linux")]
        self.inner.supervisor.close_and_wait().await;
    }

    /// Opens a read-only cursor over one capture created by this live factory instance.
    ///
    /// The cursor snapshots the synchronized log length after confirming that no frame is
    /// awaiting finalization. It never opens directories from an earlier process. Returned chunks
    /// are bounded by the shared Core raw-capture limits and preserve source-local generations.
    pub(crate) fn open_current_capture_reader(
        &self,
        capture_instance_id: RawCaptureInstanceId,
    ) -> Result<RawFrameSpoolCaptureReader, RawFrameSinkError> {
        #[cfg(target_os = "linux")]
        {
            Ok(RawFrameSpoolCaptureReader {
                inner: linux::open_capture_reader(&self.inner, capture_instance_id)?,
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = capture_instance_id;
            Err(RawFrameSinkError::Unavailable)
        }
    }

    /// Returns the bounded summary of old directories preserved as unknown.
    #[must_use]
    pub const fn recovery_summary(&self) -> RawFrameSpoolRecoverySummary {
        self.recovery
    }
}

/// Exact pre-decode bytes and the matching durable post-decode summary reconstructed from WAL.
pub(crate) struct SpooledRawFrame {
    pub(crate) capture: RawFrameCapture,
    pub(crate) finalization: RawFrameFinalization,
    pub(crate) finalization_summary_sha256: String,
}

/// Streaming reader for one current-process capture's fully finalized WAL records.
pub(crate) struct RawFrameSpoolCaptureReader {
    #[cfg(target_os = "linux")]
    inner: linux::CaptureReader,
}

impl RawFrameSpoolCaptureReader {
    /// Returns the next bounded chunk, or `None` after the complete snapshotted WAL is verified.
    pub(crate) fn next_chunk(&mut self) -> Result<Option<Vec<SpooledRawFrame>>, RawFrameSinkError> {
        #[cfg(target_os = "linux")]
        {
            self.inner.next_chunk()
        }

        #[cfg(not(target_os = "linux"))]
        {
            Err(RawFrameSinkError::Unavailable)
        }
    }
}

/// A test-only pause/failure point around spool record writes.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RawSpoolTestPoint {
    AfterWriteBeforeSync,
}

#[cfg(test)]
pub(crate) type RawSpoolTestHook = Arc<dyn Fn(RawSpoolTestPoint) -> bool + Send + Sync>;

#[cfg(test)]
impl LocalRawFrameSpoolFactory {
    pub(crate) fn open_with_test_hook(
        root: impl Into<PathBuf>,
        limits: RawFrameSpoolLimits,
        hook: RawSpoolTestHook,
    ) -> Result<Self, RawFrameSinkError> {
        #[cfg(target_os = "linux")]
        {
            let (inner, recovery) = linux::open_with_test_hook(root.into(), limits, hook)?;
            Ok(Self { inner, recovery })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (root.into(), limits, hook);
            Err(RawFrameSinkError::Unavailable)
        }
    }
}

/// Tracks blocking file operations so shutdown can close admission and wait for real completion.
pub(super) struct BlockingWorkerSupervisor {
    state: Mutex<BlockingWorkerState>,
    changed: watch::Sender<usize>,
}

struct BlockingWorkerState {
    closing: bool,
    active: usize,
}

impl BlockingWorkerSupervisor {
    pub(super) fn new() -> Arc<Self> {
        let (changed, _) = watch::channel(0);
        Arc::new(Self {
            state: Mutex::new(BlockingWorkerState {
                closing: false,
                active: 0,
            }),
            changed,
        })
    }

    pub(super) fn is_closing(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closing
    }

    pub(super) fn try_track(self: &Arc<Self>) -> Result<BlockingWorkerGuard, RawFrameSinkError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closing {
            return Err(RawFrameSinkError::Cancelled);
        }
        if state.active >= crate::queue::MAX_BACKGROUND_WORKERS {
            return Err(RawFrameSinkError::CapacityExceeded);
        }
        state.active += 1;
        self.changed.send_replace(state.active);
        drop(state);
        Ok(BlockingWorkerGuard {
            supervisor: Arc::clone(self),
            completed: false,
        })
    }

    pub(super) async fn close_and_wait(&self) {
        let mut changed = self.changed.subscribe();
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closing = true;
            self.changed.send_replace(state.active);
        }
        loop {
            let active = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active;
            if active == 0 {
                break;
            }
            if changed.changed().await.is_err() {
                break;
            }
        }
    }
}

pub(super) struct BlockingWorkerGuard {
    supervisor: Arc<BlockingWorkerSupervisor>,
    completed: bool,
}

impl BlockingWorkerGuard {
    pub(super) fn complete(&mut self) -> bool {
        self.finish()
    }

    fn finish(&mut self) -> bool {
        if self.completed {
            return true;
        }
        let mut state = self
            .supervisor
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active -= 1;
        self.supervisor.changed.send_replace(state.active);
        self.completed = true;
        !state.closing
    }
}

impl Drop for BlockingWorkerGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

impl RawFrameSinkFactory for LocalRawFrameSpoolFactory {
    fn create_sink(
        &self,
        provider: &str,
        feed: &str,
    ) -> Result<Arc<dyn broker_ports::RawFrameSink>, RawFrameSinkError> {
        #[cfg(target_os = "linux")]
        {
            linux::create_sink(&self.inner, provider, feed)
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (provider, feed);
            Err(RawFrameSinkError::Unavailable)
        }
    }
}

/// Makes a cancelled request poison its sink even while its blocking write is
/// still owned by Tokio's blocking pool.
pub(super) struct PoisonOnDrop {
    poisoned: Arc<AtomicBool>,
    completed: bool,
}

impl PoisonOnDrop {
    pub(super) fn new(poisoned: Arc<AtomicBool>) -> Self {
        Self {
            poisoned,
            completed: false,
        }
    }

    pub(super) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for PoisonOnDrop {
    fn drop(&mut self) {
        if !self.completed {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}
