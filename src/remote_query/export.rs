use super::*;
use std::io::Write;

use crate::storage::safe_object_name;

pub(super) fn write_bars_jsonl_bounded(
    destination: &Path,
    rows: &[TradeMinuteBarV1],
    max_bytes: u64,
) -> Result<()> {
    if max_bytes == 0 || destination.exists() {
        return Err(if destination.exists() {
            MarketDataError::Conflict
        } else {
            MarketDataError::InvalidInput
        });
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| safe_object_name(value))
        .ok_or(MarketDataError::InvalidInput)?;
    let sequence = CACHE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        ".{name}.remote-export-{}-{sequence}.tmp",
        std::process::id()
    ));
    let mut cleanup = TempFileCleanup(Some(temporary.clone()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let mut writer = CappedExportWriter {
        file,
        max_bytes,
        written_bytes: 0,
        over_limit: false,
    };
    for row in rows {
        if let Err(error) = serde_json::to_writer(&mut writer, row) {
            if writer.over_limit {
                return Err(MarketDataError::InputLimit);
            }
            return Err(MarketDataError::Json(error));
        }
        if let Err(error) = writer.write_all(b"\n") {
            if writer.over_limit {
                return Err(MarketDataError::InputLimit);
            }
            return Err(MarketDataError::Io(error));
        }
    }
    writer.flush()?;
    writer.file.sync_all()?;
    drop(writer);
    match fs::hard_link(&temporary, destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(MarketDataError::Conflict);
        }
        Err(_) => {
            return Err(MarketDataError::Io(std::io::Error::other(
                "export commit failed",
            )));
        }
    }
    fs::remove_file(&temporary)?;
    cleanup.0 = None;
    File::open(parent)?.sync_all()?;
    Ok(())
}

struct CappedExportWriter {
    file: File,
    max_bytes: u64,
    written_bytes: u64,
    over_limit: bool,
}

impl Write for CappedExportWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let requested = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if self.written_bytes.saturating_add(requested) > self.max_bytes {
            self.over_limit = true;
            return Err(std::io::Error::other("export byte limit exceeded"));
        }
        let written = self.file.write(bytes)?;
        self.written_bytes = self
            .written_bytes
            .checked_add(u64::try_from(written).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("export byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

struct TempFileCleanup(Option<PathBuf>);

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}
