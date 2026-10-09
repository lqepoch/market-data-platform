//! Immutable object transports: offline local-test and an argv-only rclone Drive adapter.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
};

use serde::Deserialize;

use crate::{
    MarketDataError, Result, cancellation::CancellationToken, config::DriveConfig,
    error::StorageFailure,
};

mod process;
pub(crate) use process::MAX_DECODE_WORKER_OUTPUT_BYTES;
use process::{
    CommandOutput, run_bounded_command, run_bounded_command_cancellable, run_bounded_decode_worker,
    run_bounded_decode_worker_cancellable, run_bounded_download, run_bounded_download_cancellable,
};

pub(crate) fn run_decode_worker(
    command: Command,
    output_cap: usize,
    operation_timeout: std::time::Duration,
) -> Result<Vec<u8>> {
    run_bounded_decode_worker(command, output_cap, operation_timeout)
}

pub(crate) fn run_decode_worker_cancellable(
    command: Command,
    output_cap: usize,
    operation_timeout: std::time::Duration,
    cancellation: CancellationToken,
) -> Result<Vec<u8>> {
    run_bounded_decode_worker_cancellable(command, output_cap, operation_timeout, cancellation)
}

pub const MAX_RCLONE_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const RCLONE_TIMEOUT: &str = "60s";
const RCLONE_CONNECT_TIMEOUT: &str = "10s";

#[derive(Clone, Eq, PartialEq)]
pub struct RemoteObject {
    pub id: String,
    pub size_bytes: u64,
    /// Drive MD5 is advisory only; SHA-256 is established by local readback hashing.
    pub md5: Option<String>,
}

impl fmt::Debug for RemoteObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteObject")
            .field("id", &"redacted")
            .field("size_bytes", &self.size_bytes)
            .field("md5", &self.md5.as_ref().map(|_| "available"))
            .finish()
    }
}

pub trait ObjectTransport: Send + Sync {
    fn lookup(&self, dataset_id: &str, object_name: &str) -> Result<Option<RemoteObject>>;
    fn lookup_cancellable(
        &self,
        dataset_id: &str,
        object_name: &str,
        cancellation: &CancellationToken,
    ) -> Result<Option<RemoteObject>> {
        ensure_not_cancelled(cancellation)?;
        self.lookup(dataset_id, object_name)
    }
    fn upload_immutable(
        &self,
        local_file: &Path,
        dataset_id: &str,
        object_name: &str,
    ) -> Result<()>;
    fn download_with_limit(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<()>;
    fn download_with_limit_cancellable(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        ensure_not_cancelled(cancellation)?;
        self.download_with_limit(dataset_id, object_name, destination, max_bytes)
    }
}

#[derive(Clone)]
pub struct LocalTestTransport {
    root: PathBuf,
    no_follow: bool,
}

impl LocalTestTransport {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            no_follow: false,
        })
    }

    /// Opens an existing archive for the Unix-only, read-only capture-pair verifier.
    ///
    /// The legacy `new` constructor remains available on every platform with its existing
    /// behavior. This stricter constructor requires descriptor-relative no-follow operations.
    #[cfg(target_os = "linux")]
    pub(crate) fn open_existing(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let metadata = fs::symlink_metadata(&root)?;
        if !metadata.file_type().is_dir() {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let canonical_root = fs::canonicalize(&root)?;
        use std::os::unix::fs::MetadataExt;

        if metadata.uid() != rustix::process::geteuid().as_raw() {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let directory = open_directory_nofollow(&canonical_root)?;
        let opened = directory.metadata()?;
        if !opened.file_type().is_dir()
            || opened.uid() != metadata.uid()
            || opened.dev() != metadata.dev()
            || opened.ino() != metadata.ino()
        {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        Ok(Self {
            root: canonical_root,
            no_follow: true,
        })
    }

    fn object_path(&self, dataset_id: &str, object_name: &str) -> Result<PathBuf> {
        if !safe_component(dataset_id) || !safe_object_name(object_name) {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(self.root.join(dataset_id).join(object_name))
    }
}

impl ObjectTransport for LocalTestTransport {
    fn lookup(&self, dataset_id: &str, object_name: &str) -> Result<Option<RemoteObject>> {
        let metadata = if self.no_follow {
            let Some(file) = self.open_object(dataset_id, object_name)? else {
                return Ok(None);
            };
            let metadata = file.metadata()?;
            if !metadata.is_file() || local_file_has_multiple_links(&metadata) {
                return Err(MarketDataError::Storage(StorageFailure::MalformedListing));
            }
            metadata
        } else {
            let path = self.object_path(dataset_id, object_name)?;
            if !path.exists() {
                return Ok(None);
            }
            let metadata = fs::metadata(path)?;
            if !metadata.is_file() {
                return Err(MarketDataError::Storage(StorageFailure::MalformedListing));
            }
            metadata
        };
        Ok(Some(RemoteObject {
            id: format!("local-test:{dataset_id}-{object_name}"),
            size_bytes: metadata.len(),
            md5: None,
        }))
    }

    fn upload_immutable(
        &self,
        local_file: &Path,
        dataset_id: &str,
        object_name: &str,
    ) -> Result<()> {
        let destination = self.object_path(dataset_id, object_name)?;
        let parent = destination.parent().ok_or(MarketDataError::InvalidInput)?;
        fs::create_dir_all(parent)?;
        let source = File::open(local_file)?;
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    MarketDataError::Conflict
                } else {
                    MarketDataError::Io(error)
                }
            })?;
        let mut source = source;
        std::io::copy(&mut source, &mut target)?;
        target.sync_all()?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }

    fn download_with_limit(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<()> {
        if self.no_follow {
            let source = self
                .open_object(dataset_id, object_name)?
                .ok_or(MarketDataError::Storage(StorageFailure::ReadbackFailed))?;
            copy_open_file_limited(source, destination, max_bytes, None)
        } else {
            let path = self.object_path(dataset_id, object_name)?;
            copy_new_file_limited(&path, destination, max_bytes)
        }
    }

    fn download_with_limit_cancellable(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if self.no_follow {
            let source = self
                .open_object(dataset_id, object_name)?
                .ok_or(MarketDataError::Storage(StorageFailure::ReadbackFailed))?;
            copy_open_file_limited(source, destination, max_bytes, Some(cancellation))
        } else {
            let path = self.object_path(dataset_id, object_name)?;
            copy_new_file_limited_cancellable(&path, destination, max_bytes, Some(cancellation))
        }
    }
}

impl LocalTestTransport {
    /// Copy one LocalTest object into an already-created private readback directory.
    ///
    /// This path is intentionally Linux-only and separate from the legacy ObjectTransport
    /// download behavior so existing V1 callers retain their established permissions.
    #[cfg(target_os = "linux")]
    pub(crate) fn download_private_with_limit_cancellable(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        ensure_not_cancelled(cancellation)?;
        if !self.no_follow {
            return Err(MarketDataError::PublicationNotAuthorized);
        }
        let source = self
            .open_object(dataset_id, object_name)?
            .ok_or(MarketDataError::Storage(StorageFailure::ReadbackFailed))?;
        ensure_not_cancelled(cancellation)?;
        copy_open_file_limited_private(source, destination, max_bytes, cancellation)
    }

    #[cfg(unix)]
    fn open_object(&self, dataset_id: &str, object_name: &str) -> Result<Option<File>> {
        use std::os::unix::fs::MetadataExt;

        if !safe_component(dataset_id) || !safe_object_name(object_name) {
            return Err(MarketDataError::InvalidInput);
        }
        let root = open_directory_nofollow(&self.root)?;
        let dataset = match open_directory_at_nofollow(&root, dataset_id)? {
            Some(directory) => directory,
            None => return Ok(None),
        };
        let Some(file) = open_file_at_nofollow(&dataset, object_name)? else {
            return Ok(None);
        };
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.nlink() != 1
        {
            return Err(MarketDataError::Storage(StorageFailure::MalformedListing));
        }
        Ok(Some(file))
    }

    #[cfg(not(unix))]
    fn open_object(&self, _dataset_id: &str, _object_name: &str) -> Result<Option<File>> {
        Err(MarketDataError::PublicationNotAuthorized)
    }
}

#[cfg(unix)]
fn local_file_has_multiple_links(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() != 1
}

#[cfg(not(unix))]
fn local_file_has_multiple_links(_metadata: &fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn open_directory_nofollow(path: &Path) -> Result<File> {
    Ok(File::from(
        rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))?,
    ))
}

#[cfg(unix)]
fn open_directory_at_nofollow(parent: &File, name: &str) -> Result<Option<File>> {
    match rustix::fs::openat(
        parent,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    ) {
        Ok(fd) => Ok(Some(File::from(fd))),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(MarketDataError::Io(std::io::Error::from_raw_os_error(
            error.raw_os_error(),
        ))),
    }
}

#[cfg(unix)]
fn open_file_at_nofollow(parent: &File, name: &str) -> Result<Option<File>> {
    match rustix::fs::openat(
        parent,
        name,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    ) {
        Ok(fd) => Ok(Some(File::from(fd))),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(MarketDataError::Io(std::io::Error::from_raw_os_error(
            error.raw_os_error(),
        ))),
    }
}

#[derive(Clone)]
pub(crate) struct RcloneDriveTransport {
    config: DriveConfig,
}

impl fmt::Debug for RcloneDriveTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RcloneDriveTransport { configuration: redacted }")
    }
}

impl RcloneDriveTransport {
    pub(crate) fn new(config: DriveConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    fn remote_dataset_path(&self, dataset_id: &str) -> Result<String> {
        if !safe_component(dataset_id) {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(format!(
            "{}:{}/{dataset_id}",
            self.config.remote, self.config.dataset_prefix
        ))
    }

    fn run(&self, operation: &str, args: &[String], output_cap: usize) -> Result<Vec<u8>> {
        self.run_with_cancel(operation, args, output_cap, None)
    }

    fn run_with_cancel(
        &self,
        operation: &str,
        args: &[String],
        output_cap: usize,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Vec<u8>> {
        let output = self.run_raw_with_cancel(operation, args, output_cap, cancellation)?;
        if output.status_code != 0 {
            return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
        }
        Ok(output.stdout)
    }

    fn run_raw_with_cancel(
        &self,
        operation: &str,
        args: &[String],
        output_cap: usize,
        cancellation: Option<&CancellationToken>,
    ) -> Result<CommandOutput> {
        let command = self.command(operation, args);
        match cancellation {
            Some(token) => run_bounded_command_cancellable(
                command,
                output_cap,
                self.config.operation_timeout,
                token.clone(),
            ),
            None => run_bounded_command(command, output_cap, self.config.operation_timeout),
        }
    }

    fn object_arg(&self, dataset_id: &str, object_name: &str) -> Result<String> {
        if !safe_object_name(object_name) {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(format!(
            "{}/{}",
            self.remote_dataset_path(dataset_id)?,
            object_name
        ))
    }
}

#[derive(Deserialize)]
struct RcloneItem {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "ID")]
    id: Option<String>,
    #[serde(rename = "Size")]
    size: u64,
    #[serde(rename = "Hashes")]
    hashes: Option<std::collections::HashMap<String, String>>,
    #[serde(rename = "IsDir")]
    is_dir: Option<bool>,
}

impl ObjectTransport for RcloneDriveTransport {
    fn lookup(&self, dataset_id: &str, object_name: &str) -> Result<Option<RemoteObject>> {
        self.lookup_with_cancel(dataset_id, object_name, None)
    }

    fn lookup_cancellable(
        &self,
        dataset_id: &str,
        object_name: &str,
        cancellation: &CancellationToken,
    ) -> Result<Option<RemoteObject>> {
        self.lookup_with_cancel(dataset_id, object_name, Some(cancellation))
    }

    fn upload_immutable(
        &self,
        local_file: &Path,
        dataset_id: &str,
        object_name: &str,
    ) -> Result<()> {
        let destination = self.object_arg(dataset_id, object_name)?;
        let args = vec![
            "--immutable".to_owned(),
            local_file.to_string_lossy().into_owned(),
            destination,
        ];
        self.run("copyto", &args, 64 * 1024)?;
        Ok(())
    }

    fn download_with_limit(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<()> {
        self.download_with_cancel(dataset_id, object_name, destination, max_bytes, None)
    }

    fn download_with_limit_cancellable(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.download_with_cancel(
            dataset_id,
            object_name,
            destination,
            max_bytes,
            Some(cancellation),
        )
    }
}

impl RcloneDriveTransport {
    fn lookup_with_cancel(
        &self,
        dataset_id: &str,
        object_name: &str,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<RemoteObject>> {
        if !safe_object_name(object_name) {
            return Err(MarketDataError::InvalidInput);
        }
        if let Some(token) = cancellation {
            ensure_not_cancelled(token)?;
        }
        let folder = self.remote_dataset_path(dataset_id)?;
        let args = vec![
            "--files-only".to_owned(),
            "--hash".to_owned(),
            "--no-mimetype".to_owned(),
            folder,
        ];
        let output =
            self.run_raw_with_cancel("lsjson", &args, MAX_RCLONE_OUTPUT_BYTES, cancellation)?;
        if output.status_code == 3 {
            // Official rclone exit code 3 is directory-not-found. All other failures remain
            // ambiguous and cannot be treated as absence (especially auth/quota failures).
            return Ok(None);
        }
        if output.status_code != 0 {
            return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
        }
        let items: Vec<RcloneItem> = serde_json::from_slice(&output.stdout)
            .map_err(|_| MarketDataError::Storage(StorageFailure::MalformedListing))?;
        let matches = items
            .into_iter()
            .filter(|item| item.name == object_name && item.is_dir != Some(true))
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            return Err(MarketDataError::Conflict);
        }
        let Some(item) = matches.into_iter().next() else {
            return Ok(None);
        };
        let id = item
            .id
            .filter(|id| !id.is_empty() && id.len() <= 512)
            .ok_or(MarketDataError::Storage(StorageFailure::MalformedListing))?;
        let md5 = item.hashes.and_then(|hashes| {
            hashes
                .into_iter()
                .find(|(kind, _)| kind.eq_ignore_ascii_case("md5"))
                .map(|(_, value)| value)
        });
        Ok(Some(RemoteObject {
            id,
            size_bytes: item.size,
            md5,
        }))
    }

    fn download_with_cancel(
        &self,
        dataset_id: &str,
        object_name: &str,
        destination: &Path,
        max_bytes: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<()> {
        if max_bytes == 0 {
            return Err(MarketDataError::InvalidInput);
        }
        let source = self.object_arg(dataset_id, object_name)?;
        let args = vec![source];
        let command = self.command("cat", &args);
        match cancellation {
            Some(token) => run_bounded_download_cancellable(
                command,
                destination,
                max_bytes,
                self.config.operation_timeout,
                token.clone(),
            ),
            None => run_bounded_download(
                command,
                destination,
                max_bytes,
                self.config.operation_timeout,
            ),
        }
    }
}

impl RcloneDriveTransport {
    fn command(&self, operation: &str, args: &[String]) -> Command {
        let mut command = Command::new("rclone");
        command
            .arg("--config")
            .arg(&self.config.rclone_config)
            .arg("--drive-root-folder-id")
            .arg(&self.config.root_folder_id)
            .arg("--retries")
            .arg("1")
            .arg("--low-level-retries")
            .arg("1")
            .arg("--contimeout")
            .arg(RCLONE_CONNECT_TIMEOUT)
            .arg("--timeout")
            .arg(RCLONE_TIMEOUT)
            .arg("--log-level")
            .arg("ERROR")
            .arg(operation)
            .args(args);
        command
    }
}

fn copy_new_file_limited(source: &Path, destination: &Path, max_bytes: u64) -> Result<()> {
    copy_new_file_limited_cancellable(source, destination, max_bytes, None)
}

fn copy_new_file_limited_cancellable(
    source: &Path,
    destination: &Path,
    max_bytes: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    if let Some(token) = cancellation {
        ensure_not_cancelled(token)?;
    }
    if max_bytes == 0 {
        return Err(MarketDataError::InvalidInput);
    }
    match fs::symlink_metadata(destination) {
        Ok(_) => return Err(MarketDataError::Conflict),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(MarketDataError::Io(error)),
    }
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.file_type().is_file() || metadata.len() > max_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let source = File::open(source)?;
    copy_open_file_limited(source, destination, max_bytes, cancellation)
}

fn copy_open_file_limited(
    mut source: File,
    destination: &Path,
    max_bytes: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    if let Some(token) = cancellation {
        ensure_not_cancelled(token)?;
    }
    if max_bytes == 0 {
        return Err(MarketDataError::InvalidInput);
    }
    let metadata = source.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(MarketDataError::InputLimit);
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut cleanup = RemoveFileOnDrop(Some(destination.to_path_buf()));
    let mut copied = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if let Some(token) = cancellation {
            ensure_not_cancelled(token)?;
        }
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        copied = copied
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or(MarketDataError::InputLimit)?;
        if copied > max_bytes {
            return Err(MarketDataError::InputLimit);
        }
        target.write_all(&buffer[..count])?;
    }
    target.sync_all()?;
    cleanup.0 = None;
    Ok(())
}

#[cfg(target_os = "linux")]
fn copy_open_file_limited_private(
    source: File,
    destination: &Path,
    max_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    copy_open_file_limited_private_with_progress(
        source,
        destination,
        max_bytes,
        cancellation,
        || {},
    )
}

#[cfg(target_os = "linux")]
fn copy_open_file_limited_private_with_progress(
    mut source: File,
    destination: &Path,
    max_bytes: u64,
    cancellation: &CancellationToken,
    mut after_chunk: impl FnMut(),
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if max_bytes == 0 {
        return Err(MarketDataError::InvalidInput);
    }
    let source_metadata = source.metadata()?;
    if !source_metadata.is_file() || source_metadata.len() > max_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let parent_path = destination.parent().ok_or(MarketDataError::InvalidInput)?;
    let parent_metadata = fs::symlink_metadata(parent_path)?;
    if !parent_metadata.file_type().is_dir()
        || parent_metadata.uid() != rustix::process::geteuid().as_raw()
        || parent_metadata.permissions().mode() & 0o7777 != 0o700
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    let parent = File::from(
        rustix::fs::open(
            parent_path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| MarketDataError::PublicationNotAuthorized)?,
    );
    let opened_parent = parent.metadata()?;
    if opened_parent.dev() != parent_metadata.dev() || opened_parent.ino() != parent_metadata.ino()
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    let name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
        })
        .ok_or(MarketDataError::InvalidInput)?;
    let fd = rustix::fs::openat(
        &parent,
        name,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(0o600),
    )
    .map_err(|error| {
        if error == rustix::io::Errno::EXIST {
            MarketDataError::Conflict
        } else {
            MarketDataError::PublicationNotAuthorized
        }
    })?;
    let mut target = File::from(fd);
    let mut cleanup = RemoveAtOnDrop {
        directory: parent,
        name: name.to_owned(),
        active: true,
    };
    let target_metadata = target.metadata()?;
    if !target_metadata.is_file()
        || target_metadata.uid() != rustix::process::geteuid().as_raw()
        || target_metadata.permissions().mode() & 0o7777 != 0o600
        || target_metadata.nlink() != 1
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    let mut copied = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        ensure_not_cancelled(cancellation)?;
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        copied = copied
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or(MarketDataError::InputLimit)?;
        if copied > max_bytes {
            return Err(MarketDataError::InputLimit);
        }
        target.write_all(&buffer[..count])?;
        after_chunk();
    }
    ensure_not_cancelled(cancellation)?;
    target.sync_all()?;
    cleanup.active = false;
    Ok(())
}

#[cfg(target_os = "linux")]
struct RemoveAtOnDrop {
    directory: File,
    name: String,
    active: bool,
}

#[cfg(target_os = "linux")]
impl Drop for RemoveAtOnDrop {
    fn drop(&mut self) {
        if self.active {
            let _ = rustix::fs::unlinkat(
                &self.directory,
                self.name.as_str(),
                rustix::fs::AtFlags::empty(),
            );
        }
    }
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(MarketDataError::Storage(StorageFailure::Cancelled))
    } else {
        Ok(())
    }
}

struct RemoveFileOnDrop(Option<PathBuf>);

impl Drop for RemoveFileOnDrop {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

pub(crate) fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.contains("..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

pub(crate) fn safe_object_name(value: &str) -> bool {
    safe_component(value) && !value.starts_with('.') && !value.ends_with('.')
}

pub(crate) fn namespaced_dataset_id(namespace: &str, dataset_id: &str) -> Result<String> {
    if !matches!(namespace, "curated" | "diagnostic")
        || !safe_component(dataset_id)
        || dataset_id.starts_with('.')
        || dataset_id.ends_with('.')
    {
        return Err(MarketDataError::InvalidInput);
    }
    let key = format!("{namespace}-{dataset_id}");
    if !safe_component(&key) {
        return Err(MarketDataError::InvalidInput);
    }
    Ok(key)
}

#[cfg(test)]
mod tests;
