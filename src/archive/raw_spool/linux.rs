use std::{
    fs::{self, DirBuilder, File},
    io::{Read, Write},
    mem::MaybeUninit,
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use broker_ports::{
    RawCaptureInstanceId, RawFrameCapture, RawFrameCaptureAck, RawFrameCaptureKey,
    RawFrameFinalization, RawFrameFinalizationAck, RawFrameSink, RawFrameSinkError,
};
use fs2::FileExt;

use crate::{
    archive::raw_spool::{
        BlockingWorkerGuard, BlockingWorkerSupervisor, HARD_MAX_CAPTURE_IDENTITIES,
        HARD_MAX_TOTAL_BYTES, PoisonOnDrop, RawFrameSpoolLimits, RawFrameSpoolRecoverySummary,
    },
    queue::BackgroundWorkerPermit,
};

const ROOT_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
const LOCK_FILE_NAME: &str = ".raw-spool.lock";
const CAPTURE_PREFIX: &str = "capture-";
const FRAME_LOG_NAME: &str = "frames.log";
const FRAME_LOG_MAGIC: &[u8; 8] = b"MDPRAW01";
const MAX_CAPTURE_METADATA_BYTES: usize = 1024;

mod record;
use record::{encode_finalization_record, encode_predecode_record};

pub(super) struct FactoryInner {
    root_directory: File,
    limits: RawFrameSpoolLimits,
    budget: Mutex<FactoryBudget>,
    _lock_file: File,
    pub(super) supervisor: Arc<BlockingWorkerSupervisor>,
    #[cfg(test)]
    test_hook: Option<super::RawSpoolTestHook>,
}

struct FactoryBudget {
    total_bytes: u64,
    capture_identities: u32,
    poisoned: bool,
}

struct SinkInner {
    capture_id: RawCaptureInstanceId,
    provider: String,
    feed: String,
    factory: Arc<FactoryInner>,
    poisoned: Arc<AtomicBool>,
    state: Mutex<SinkState>,
}

struct SinkState {
    file: File,
    capture_bytes: u64,
    frames: u32,
    last_generation: Option<u64>,
    last_sequence: Option<u64>,
    pending: Option<RawFrameCaptureKey>,
    poisoned: bool,
}

pub(super) fn open(
    root: PathBuf,
    limits: RawFrameSpoolLimits,
) -> Result<(Arc<FactoryInner>, RawFrameSpoolRecoverySummary), RawFrameSinkError> {
    open_impl(
        root,
        limits,
        #[cfg(test)]
        None,
    )
}

#[cfg(test)]
pub(super) fn open_with_test_hook(
    root: PathBuf,
    limits: RawFrameSpoolLimits,
    hook: super::RawSpoolTestHook,
) -> Result<(Arc<FactoryInner>, RawFrameSpoolRecoverySummary), RawFrameSinkError> {
    open_impl(root, limits, Some(hook))
}

fn open_impl(
    root: PathBuf,
    limits: RawFrameSpoolLimits,
    #[cfg(test)] test_hook: Option<super::RawSpoolTestHook>,
) -> Result<(Arc<FactoryInner>, RawFrameSpoolRecoverySummary), RawFrameSinkError> {
    if !limits.valid() {
        return Err(RawFrameSinkError::CapacityExceeded);
    }

    let root = normalize_root(root)?;
    create_or_validate_root(&root)?;
    let root_directory = open_root_directory(&root)?;
    let lock_file = open_lock_file(&root_directory)?;
    lock_file
        .try_lock_exclusive()
        .map_err(|_| RawFrameSinkError::Unavailable)?;
    let (capture_identities, total_bytes) = scan_existing_captures(&root_directory, limits)?;
    let recovery = RawFrameSpoolRecoverySummary {
        preserved_capture_directories: capture_identities,
        preserved_bytes: total_bytes,
    };
    let inner = Arc::new(FactoryInner {
        root_directory,
        limits,
        budget: Mutex::new(FactoryBudget {
            total_bytes,
            capture_identities,
            poisoned: false,
        }),
        _lock_file: lock_file,
        supervisor: BlockingWorkerSupervisor::new(),
        #[cfg(test)]
        test_hook,
    });
    Ok((inner, recovery))
}

pub(super) fn create_sink(
    factory: &Arc<FactoryInner>,
    provider: &str,
    feed: &str,
) -> Result<Arc<dyn RawFrameSink>, RawFrameSinkError> {
    let mut startup_guard = factory.supervisor.try_track()?;
    if !valid_source_field(provider) || !valid_source_field(feed) {
        return Err(RawFrameSinkError::Unavailable);
    }
    let _worker_permit =
        BackgroundWorkerPermit::acquire().map_err(|_| RawFrameSinkError::CapacityExceeded)?;
    let mut budget = factory
        .budget
        .lock()
        .map_err(|_| RawFrameSinkError::Poisoned)?;
    if budget.poisoned {
        return Err(RawFrameSinkError::Poisoned);
    }
    if budget.capture_identities >= factory.limits.max_capture_identities {
        return Err(RawFrameSinkError::CapacityExceeded);
    }
    let next_total = budget
        .total_bytes
        .checked_add(FRAME_LOG_MAGIC.len() as u64)
        .ok_or(RawFrameSinkError::CapacityExceeded)?;
    if next_total > factory.limits.max_total_bytes {
        return Err(RawFrameSinkError::CapacityExceeded);
    }

    // Reserve before creating anything. If creation becomes ambiguous, this
    // factory stays poisoned and cannot undercount a possible orphan directory.
    budget.total_bytes = next_total;
    budget.capture_identities += 1;
    let (capture_id, capture_dir) = match create_capture_directory(factory) {
        Ok(capture) => capture,
        Err(error) => {
            budget.poisoned = true;
            return Err(error);
        }
    };
    let mut file = match rustix::fs::openat(
        &capture_dir,
        FRAME_LOG_NAME,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::APPEND
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(FILE_MODE),
    ) {
        Ok(file) => File::from(file),
        Err(_) => {
            budget.poisoned = true;
            return Err(RawFrameSinkError::Ambiguous);
        }
    };
    if file.write_all(FRAME_LOG_MAGIC).is_err()
        || file.sync_all().is_err()
        || capture_dir.sync_all().is_err()
        || factory.root_directory.sync_all().is_err()
    {
        budget.poisoned = true;
        return Err(RawFrameSinkError::Ambiguous);
    }
    validate_private_file(&file, FILE_MODE).map_err(|_| {
        budget.poisoned = true;
        RawFrameSinkError::Unavailable
    })?;

    if !startup_guard.complete() {
        budget.poisoned = true;
        return Err(RawFrameSinkError::Cancelled);
    }

    drop(budget);
    let inner = Arc::new(SinkInner {
        capture_id,
        provider: provider.to_owned(),
        feed: feed.to_owned(),
        factory: Arc::clone(factory),
        poisoned: Arc::new(AtomicBool::new(false)),
        state: Mutex::new(SinkState {
            file,
            capture_bytes: FRAME_LOG_MAGIC.len() as u64,
            frames: 0,
            last_generation: None,
            last_sequence: None,
            pending: None,
            poisoned: false,
        }),
    });
    Ok(Arc::new(LocalRawFrameSink { inner }))
}

struct LocalRawFrameSink {
    inner: Arc<SinkInner>,
}

impl RawFrameSink for LocalRawFrameSink {
    fn capture_instance_id(&self) -> RawCaptureInstanceId {
        self.inner.capture_id
    }

    fn persist_before_decode<'a>(
        &'a self,
        capture: &'a RawFrameCapture,
    ) -> broker_ports::PortFuture<'a, Result<RawFrameCaptureAck, RawFrameSinkError>> {
        Box::pin(async move {
            if capture.capture_instance_id() != self.inner.capture_id
                || capture.provider() != self.inner.provider
                || capture.feed() != self.inner.feed
                || self.is_poisoned()
            {
                self.poison();
                return Err(RawFrameSinkError::Poisoned);
            }
            let record = match encode_predecode_record(capture) {
                Ok(record) => record,
                Err(error) => {
                    self.poison();
                    return Err(error);
                }
            };
            let ack = RawFrameCaptureAck::for_capture(capture);
            let key = capture.capture_key().clone();
            let inner = Arc::clone(&self.inner);
            let poison = Arc::clone(&self.inner.poisoned);
            let worker_guard = match self.inner.factory.supervisor.try_track() {
                Ok(guard) => guard,
                Err(error) => {
                    self.poison();
                    return Err(error);
                }
            };
            let mut cancel_guard = PoisonOnDrop::new(Arc::clone(&poison));
            let joined = tokio::task::spawn_blocking(move || {
                let mut worker_guard: BlockingWorkerGuard = worker_guard;
                let mut worker_failure_guard = PoisonOnDrop::new(Arc::clone(&poison));
                let _worker_permit = BackgroundWorkerPermit::acquire()
                    .map_err(|_| RawFrameSinkError::CapacityExceeded)?;
                let result = persist_predecode(&inner, key, record);
                if worker_guard.complete() && result.is_ok() {
                    worker_failure_guard.complete();
                    result
                } else if result.is_err() {
                    result
                } else {
                    Err(RawFrameSinkError::Cancelled)
                }
            })
            .await;
            cancel_guard.complete();
            match joined {
                Ok(Ok(())) => Ok(ack),
                Ok(Err(error)) => {
                    self.poison();
                    Err(error)
                }
                Err(_) => {
                    self.poison();
                    Err(RawFrameSinkError::Ambiguous)
                }
            }
        })
    }

    fn finalize_after_decode<'a>(
        &'a self,
        predecode_ack: &'a RawFrameCaptureAck,
        summary: &'a RawFrameFinalization,
    ) -> broker_ports::PortFuture<'a, Result<RawFrameFinalizationAck, RawFrameSinkError>> {
        Box::pin(async move {
            if self.is_poisoned()
                || predecode_ack.capture_key().capture_instance_id() != self.inner.capture_id
            {
                self.poison();
                return Err(RawFrameSinkError::Poisoned);
            }
            let final_ack = RawFrameFinalizationAck::for_finalization(predecode_ack, summary);
            let record = match encode_finalization_record(
                predecode_ack.capture_key(),
                summary,
                final_ack.summary_sha256(),
            ) {
                Ok(record) => record,
                Err(error) => {
                    self.poison();
                    return Err(error);
                }
            };
            let key = predecode_ack.capture_key().clone();
            let summary_hash = final_ack.summary_sha256().to_owned();
            let inner = Arc::clone(&self.inner);
            let poison = Arc::clone(&self.inner.poisoned);
            let worker_guard = match self.inner.factory.supervisor.try_track() {
                Ok(guard) => guard,
                Err(error) => {
                    self.poison();
                    return Err(error);
                }
            };
            let mut cancel_guard = PoisonOnDrop::new(Arc::clone(&poison));
            let joined = tokio::task::spawn_blocking(move || {
                let mut worker_guard: BlockingWorkerGuard = worker_guard;
                let mut worker_failure_guard = PoisonOnDrop::new(Arc::clone(&poison));
                let _worker_permit = BackgroundWorkerPermit::acquire()
                    .map_err(|_| RawFrameSinkError::CapacityExceeded)?;
                let result = persist_finalization(&inner, &key, &summary_hash, record);
                if worker_guard.complete() && result.is_ok() {
                    worker_failure_guard.complete();
                    result
                } else if result.is_err() {
                    result
                } else {
                    Err(RawFrameSinkError::Cancelled)
                }
            })
            .await;
            cancel_guard.complete();
            match joined {
                Ok(Ok(())) => Ok(final_ack),
                Ok(Err(error)) => {
                    self.poison();
                    Err(error)
                }
                Err(_) => {
                    self.poison();
                    Err(RawFrameSinkError::Ambiguous)
                }
            }
        })
    }
}

impl LocalRawFrameSink {
    fn is_poisoned(&self) -> bool {
        self.inner.poisoned.load(Ordering::Acquire)
    }

    fn poison(&self) {
        self.inner.poisoned.store(true, Ordering::Release);
        if let Ok(mut state) = self.inner.state.lock() {
            state.poisoned = true;
        }
    }
}

fn persist_predecode(
    inner: &SinkInner,
    key: RawFrameCaptureKey,
    record: Vec<u8>,
) -> Result<(), RawFrameSinkError> {
    let mut budget = inner
        .factory
        .budget
        .lock()
        .map_err(|_| RawFrameSinkError::Poisoned)?;
    let mut state = inner
        .state
        .lock()
        .map_err(|_| RawFrameSinkError::Poisoned)?;
    if budget.poisoned
        || state.poisoned
        || inner.poisoned.load(Ordering::Acquire)
        || inner.factory.supervisor.is_closing()
    {
        state.poisoned = true;
        return Err(RawFrameSinkError::Poisoned);
    }
    if state.pending.is_some() || !is_next_frame(&state, &key) {
        state.poisoned = true;
        inner.poisoned.store(true, Ordering::Release);
        return Err(RawFrameSinkError::Poisoned);
    }
    if state.frames >= inner.factory.limits.max_frames_per_capture {
        state.poisoned = true;
        inner.poisoned.store(true, Ordering::Release);
        return Err(RawFrameSinkError::CapacityExceeded);
    }
    if let Err(error) =
        reserve_record_bytes(&mut budget, &mut state, record.len(), inner.factory.limits)
    {
        state.poisoned = true;
        inner.poisoned.store(true, Ordering::Release);
        return Err(error);
    }
    drop(budget);
    if inner.poisoned.load(Ordering::Acquire) {
        state.poisoned = true;
        return Err(RawFrameSinkError::Cancelled);
    }
    if let Err(error) = append_synchronized(inner, &mut state.file, &record) {
        state.poisoned = true;
        inner.poisoned.store(true, Ordering::Release);
        return Err(error);
    }
    if inner.poisoned.load(Ordering::Acquire) || inner.factory.supervisor.is_closing() {
        state.poisoned = true;
        return Err(RawFrameSinkError::Cancelled);
    }
    state.frames += 1;
    state.last_generation = Some(key.source_generation());
    state.last_sequence = Some(key.frame_sequence());
    state.pending = Some(key);
    Ok(())
}

fn persist_finalization(
    inner: &SinkInner,
    key: &RawFrameCaptureKey,
    summary_hash: &str,
    record: Vec<u8>,
) -> Result<(), RawFrameSinkError> {
    let mut budget = inner
        .factory
        .budget
        .lock()
        .map_err(|_| RawFrameSinkError::Poisoned)?;
    let mut state = inner
        .state
        .lock()
        .map_err(|_| RawFrameSinkError::Poisoned)?;
    if budget.poisoned
        || state.poisoned
        || inner.poisoned.load(Ordering::Acquire)
        || inner.factory.supervisor.is_closing()
    {
        state.poisoned = true;
        return Err(RawFrameSinkError::Poisoned);
    }
    if state.pending.as_ref() != Some(key) {
        state.poisoned = true;
        inner.poisoned.store(true, Ordering::Release);
        return Err(RawFrameSinkError::Poisoned);
    }
    if !valid_sha256(summary_hash) {
        state.poisoned = true;
        inner.poisoned.store(true, Ordering::Release);
        return Err(RawFrameSinkError::Poisoned);
    }
    if let Err(error) =
        reserve_record_bytes(&mut budget, &mut state, record.len(), inner.factory.limits)
    {
        state.poisoned = true;
        inner.poisoned.store(true, Ordering::Release);
        return Err(error);
    }
    drop(budget);
    if inner.poisoned.load(Ordering::Acquire) {
        state.poisoned = true;
        return Err(RawFrameSinkError::Cancelled);
    }
    if let Err(error) = append_synchronized(inner, &mut state.file, &record) {
        state.poisoned = true;
        inner.poisoned.store(true, Ordering::Release);
        return Err(error);
    }
    if inner.poisoned.load(Ordering::Acquire) || inner.factory.supervisor.is_closing() {
        state.poisoned = true;
        return Err(RawFrameSinkError::Cancelled);
    }
    state.pending = None;
    Ok(())
}

fn reserve_record_bytes(
    budget: &mut FactoryBudget,
    state: &mut SinkState,
    record_len: usize,
    limits: RawFrameSpoolLimits,
) -> Result<(), RawFrameSinkError> {
    let record_len = u64::try_from(record_len).map_err(|_| RawFrameSinkError::CapacityExceeded)?;
    let capture_total = state
        .capture_bytes
        .checked_add(record_len)
        .ok_or(RawFrameSinkError::CapacityExceeded)?;
    let total = budget
        .total_bytes
        .checked_add(record_len)
        .ok_or(RawFrameSinkError::CapacityExceeded)?;
    if capture_total > limits.max_capture_bytes || total > limits.max_total_bytes {
        state.poisoned = true;
        return Err(RawFrameSinkError::CapacityExceeded);
    }
    // Keep reservations after attempted I/O, including ambiguous failures.
    state.capture_bytes = capture_total;
    budget.total_bytes = total;
    Ok(())
}

fn append_synchronized(
    inner: &SinkInner,
    file: &mut File,
    record: &[u8],
) -> Result<(), RawFrameSinkError> {
    if file.write_all(record).is_err() {
        return Err(RawFrameSinkError::Ambiguous);
    }
    #[cfg(test)]
    if inner
        .factory
        .test_hook
        .as_ref()
        .is_some_and(|hook| hook(super::RawSpoolTestPoint::AfterWriteBeforeSync))
    {
        return Err(RawFrameSinkError::Ambiguous);
    }
    if inner.poisoned.load(Ordering::Acquire) || inner.factory.supervisor.is_closing() {
        return Err(RawFrameSinkError::Cancelled);
    }
    if file.sync_all().is_err() {
        return Err(RawFrameSinkError::Ambiguous);
    }
    Ok(())
}

fn is_next_frame(state: &SinkState, key: &RawFrameCaptureKey) -> bool {
    match (state.last_generation, state.last_sequence) {
        (None, None) => key.frame_sequence() == 1,
        (Some(generation), Some(sequence)) if key.source_generation() == generation => {
            sequence.checked_add(1) == Some(key.frame_sequence())
        }
        (Some(generation), Some(_)) if key.source_generation() > generation => {
            key.frame_sequence() == 1
        }
        _ => false,
    }
}

fn normalize_root(path: PathBuf) -> Result<PathBuf, RawFrameSinkError> {
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|_| RawFrameSinkError::Unavailable)?
            .join(path)
    };
    let parent = path.parent().ok_or(RawFrameSinkError::Unavailable)?;
    let name = path.file_name().ok_or(RawFrameSinkError::Unavailable)?;
    let parent = fs::canonicalize(parent).map_err(|_| RawFrameSinkError::Unavailable)?;
    Ok(parent.join(name))
}

fn create_or_validate_root(root: &Path) -> Result<(), RawFrameSinkError> {
    match fs::symlink_metadata(root) {
        Ok(metadata) => validate_private_directory_metadata(&metadata, ROOT_MODE),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = DirBuilder::new();
            builder.mode(ROOT_MODE);
            builder
                .create(root)
                .map_err(|_| RawFrameSinkError::Unavailable)?;
            let metadata =
                fs::symlink_metadata(root).map_err(|_| RawFrameSinkError::Unavailable)?;
            validate_private_directory_metadata(&metadata, ROOT_MODE)?;
            if let Some(parent) = root.parent() {
                sync_directory(parent).map_err(|_| RawFrameSinkError::Unavailable)?;
            }
            Ok(())
        }
        Err(_) => Err(RawFrameSinkError::Unavailable),
    }
}

fn open_root_directory(root: &Path) -> Result<File, RawFrameSinkError> {
    let directory = File::from(
        rustix::fs::open(
            root,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| RawFrameSinkError::Unavailable)?,
    );
    let metadata = directory
        .metadata()
        .map_err(|_| RawFrameSinkError::Unavailable)?;
    validate_private_directory_metadata(&metadata, ROOT_MODE)?;
    Ok(directory)
}

fn open_lock_file(root_directory: &File) -> Result<File, RawFrameSinkError> {
    let file = File::from(
        rustix::fs::openat(
            root_directory,
            LOCK_FILE_NAME,
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(FILE_MODE),
        )
        .map_err(|_| RawFrameSinkError::Unavailable)?,
    );
    validate_private_file(&file, FILE_MODE)?;
    if file
        .metadata()
        .map_err(|_| RawFrameSinkError::Unavailable)?
        .len()
        != 0
    {
        return Err(RawFrameSinkError::Unavailable);
    }
    root_directory
        .sync_all()
        .map_err(|_| RawFrameSinkError::Unavailable)?;
    Ok(file)
}

fn scan_existing_captures(
    root_directory: &File,
    limits: RawFrameSpoolLimits,
) -> Result<(u32, u64), RawFrameSinkError> {
    let mut identities = 0u32;
    let mut total_bytes = 0u64;
    let mut root_buffer = [MaybeUninit::uninit(); 8192];
    let mut entries = rustix::fs::RawDir::new(root_directory, &mut root_buffer);
    while let Some(name_bytes) = next_directory_entry_name(&mut entries)? {
        if name_bytes == LOCK_FILE_NAME.as_bytes() {
            continue;
        }
        let name = std::str::from_utf8(&name_bytes).map_err(|_| RawFrameSinkError::Unavailable)?;
        if !valid_capture_dir_name(name) {
            return Err(RawFrameSinkError::Unavailable);
        }
        let capture_directory = File::from(
            rustix::fs::openat(
                root_directory,
                name,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| RawFrameSinkError::Unavailable)?,
        );
        let dir_metadata = capture_directory
            .metadata()
            .map_err(|_| RawFrameSinkError::Unavailable)?;
        validate_private_directory_metadata(&dir_metadata, ROOT_MODE)?;
        identities = identities
            .checked_add(1)
            .ok_or(RawFrameSinkError::CapacityExceeded)?;
        if identities > limits.max_capture_identities || identities > HARD_MAX_CAPTURE_IDENTITIES {
            return Err(RawFrameSinkError::CapacityExceeded);
        }
        let mut child_buffer = [MaybeUninit::uninit(); 2048];
        let mut children = rustix::fs::RawDir::new(&capture_directory, &mut child_buffer);
        let Some(log_name_bytes) = next_directory_entry_name(&mut children)? else {
            // A process may have stopped after creating the capture directory.
            // Keep the empty identity as unknown; never reuse it.
            continue;
        };
        if log_name_bytes != FRAME_LOG_NAME.as_bytes() {
            return Err(RawFrameSinkError::Unavailable);
        }
        if next_directory_entry_name(&mut children)?.is_some() {
            return Err(RawFrameSinkError::Unavailable);
        }
        let log_file = File::from(
            rustix::fs::openat(
                &capture_directory,
                FRAME_LOG_NAME,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| RawFrameSinkError::Unavailable)?,
        );
        let log_metadata = log_file
            .metadata()
            .map_err(|_| RawFrameSinkError::Unavailable)?;
        validate_private_file_metadata(&log_metadata, FILE_MODE)?;
        if log_metadata.len() > limits.max_capture_bytes {
            return Err(RawFrameSinkError::CapacityExceeded);
        }
        total_bytes = total_bytes
            .checked_add(log_metadata.len())
            .ok_or(RawFrameSinkError::CapacityExceeded)?;
        if total_bytes > limits.max_total_bytes || total_bytes > HARD_MAX_TOTAL_BYTES {
            return Err(RawFrameSinkError::CapacityExceeded);
        }
    }
    Ok((identities, total_bytes))
}

fn create_capture_directory(
    factory: &FactoryInner,
) -> Result<(RawCaptureInstanceId, File), RawFrameSinkError> {
    for _ in 0..8 {
        let mut bytes = [0u8; 16];
        File::open("/dev/urandom")
            .and_then(|mut random| random.read_exact(&mut bytes))
            .map_err(|_| RawFrameSinkError::Unavailable)?;
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let capture_id =
            RawCaptureInstanceId::new(bytes).map_err(|_| RawFrameSinkError::Unavailable)?;
        let name = capture_dir_name(capture_id);
        match rustix::fs::mkdirat(
            &factory.root_directory,
            name.as_str(),
            rustix::fs::Mode::from_raw_mode(ROOT_MODE),
        ) {
            Ok(()) => {}
            Err(error) if error == rustix::io::Errno::EXIST => continue,
            Err(_) => return Err(RawFrameSinkError::Ambiguous),
        }
        let capture_directory = File::from(
            rustix::fs::openat(
                &factory.root_directory,
                name.as_str(),
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| RawFrameSinkError::Ambiguous)?,
        );
        let metadata = capture_directory
            .metadata()
            .map_err(|_| RawFrameSinkError::Ambiguous)?;
        validate_private_directory_metadata(&metadata, ROOT_MODE)
            .map_err(|_| RawFrameSinkError::Ambiguous)?;
        return Ok((capture_id, capture_directory));
    }
    Err(RawFrameSinkError::Unavailable)
}

fn capture_dir_name(id: RawCaptureInstanceId) -> String {
    format!("{CAPTURE_PREFIX}{}", hex::encode(id.as_bytes()))
}

fn valid_capture_dir_name(value: &str) -> bool {
    value.strip_prefix(CAPTURE_PREFIX).is_some_and(|id| {
        id.len() == 32
            && id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            && id.as_bytes()[12] == b'4'
            && matches!(id.as_bytes()[16], b'8' | b'9' | b'a' | b'b')
    })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_source_field(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CAPTURE_METADATA_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn next_directory_entry_name(
    directory: &mut rustix::fs::RawDir<'_, &File>,
) -> Result<Option<Vec<u8>>, RawFrameSinkError> {
    loop {
        let Some(entry) = directory.next() else {
            return Ok(None);
        };
        let entry = entry.map_err(|_| RawFrameSinkError::Unavailable)?;
        let name = entry.file_name().to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        return Ok(Some(name.to_vec()));
    }
}

fn validate_private_directory_metadata(
    metadata: &fs::Metadata,
    expected_mode: u32,
) -> Result<(), RawFrameSinkError> {
    if !metadata.file_type().is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != expected_mode
    {
        return Err(RawFrameSinkError::Unavailable);
    }
    Ok(())
}

fn validate_private_file(file: &File, expected_mode: u32) -> Result<(), RawFrameSinkError> {
    let metadata = file
        .metadata()
        .map_err(|_| RawFrameSinkError::Unavailable)?;
    validate_private_file_metadata(&metadata, expected_mode)
}

fn validate_private_file_metadata(
    metadata: &fs::Metadata,
    expected_mode: u32,
) -> Result<(), RawFrameSinkError> {
    if !metadata.file_type().is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != expected_mode
        || metadata.nlink() != 1
    {
        return Err(RawFrameSinkError::Unavailable);
    }
    Ok(())
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    let directory = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    File::from(directory).sync_all()
}
