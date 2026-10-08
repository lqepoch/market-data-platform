use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::{MarketDataError, Result};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) struct CappedWriter {
    pub(super) file: File,
    pub(super) max_bytes: u64,
    pub(super) written_bytes: u64,
    pub(super) over_limit: Arc<AtomicBool>,
}

impl Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let requested = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if self.written_bytes.saturating_add(requested) > self.max_bytes {
            self.over_limit.store(true, Ordering::Release);
            return Err(std::io::Error::other("parquet byte limit exceeded"));
        }
        let written = self.file.write(bytes)?;
        self.written_bytes = self
            .written_bytes
            .checked_add(u64::try_from(written).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("parquet byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

pub(super) fn parquet_write_error(over_limit: &AtomicBool) -> MarketDataError {
    if over_limit.load(Ordering::Acquire) {
        MarketDataError::InputLimit
    } else {
        MarketDataError::Parquet
    }
}

pub(super) struct TempFileCleanup(pub(super) PathBuf);

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

pub(super) fn create_new_file(path: &Path) -> Result<File> {
    fs::create_dir_all(parent_dir(path))?;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                MarketDataError::Conflict
            } else {
                MarketDataError::Io(error)
            }
        })
}

pub(super) fn temporary_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("dataset.parquet");
    parent_dir(path).join(format!(".{name}.tmp-{}-{sequence}", std::process::id()))
}

pub(super) fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
