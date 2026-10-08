//! Immutable object transports: offline local-test and an argv-only rclone Drive adapter.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use serde::Deserialize;

use crate::{MarketDataError, Result, config::DriveConfig, error::StorageFailure};

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
    fn upload_immutable(
        &self,
        local_file: &Path,
        dataset_id: &str,
        object_name: &str,
    ) -> Result<()>;
    fn download(&self, dataset_id: &str, object_name: &str, destination: &Path) -> Result<()>;
}

#[derive(Clone)]
pub struct LocalTestTransport {
    root: PathBuf,
}

impl LocalTestTransport {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
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
        let path = self.object_path(dataset_id, object_name)?;
        if !path.exists() {
            return Ok(None);
        }
        let metadata = fs::metadata(&path)?;
        if !metadata.is_file() {
            return Err(MarketDataError::Storage(StorageFailure::MalformedListing));
        }
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

    fn download(&self, dataset_id: &str, object_name: &str, destination: &Path) -> Result<()> {
        let source = self.object_path(dataset_id, object_name)?;
        copy_new_file(&source, destination)
    }
}

#[derive(Clone)]
pub struct RcloneDriveTransport {
    config: DriveConfig,
}

impl fmt::Debug for RcloneDriveTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RcloneDriveTransport { configuration: redacted }")
    }
}

impl RcloneDriveTransport {
    pub fn new(config: DriveConfig) -> Result<Self> {
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
        let output = self.run_raw(operation, args, output_cap)?;
        if output.status_code != 0 {
            return Err(MarketDataError::Storage(StorageFailure::CommandFailed));
        }
        Ok(output.stdout)
    }

    fn run_raw(
        &self,
        operation: &str,
        args: &[String],
        output_cap: usize,
    ) -> Result<CommandOutput> {
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
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|_| MarketDataError::Storage(StorageFailure::Spawn))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(MarketDataError::Storage(StorageFailure::CommandFailed))?;
        let mut output = Vec::with_capacity(output_cap.min(64 * 1024));
        let read_result = stdout
            .take(
                u64::try_from(output_cap)
                    .unwrap_or(u64::MAX)
                    .saturating_add(1),
            )
            .read_to_end(&mut output);
        if read_result.is_err() || output.len() > output_cap {
            let _ = child.kill();
            let _ = child.wait();
            return Err(MarketDataError::Storage(StorageFailure::MalformedListing));
        }
        let status = child
            .wait()
            .map_err(|_| MarketDataError::Storage(StorageFailure::CommandFailed))?;
        Ok(CommandOutput {
            status_code: status.code().unwrap_or(255),
            stdout: output,
        })
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

struct CommandOutput {
    status_code: i32,
    stdout: Vec<u8>,
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
        if !safe_object_name(object_name) {
            return Err(MarketDataError::InvalidInput);
        }
        let folder = self.remote_dataset_path(dataset_id)?;
        let args = vec![
            "--files-only".to_owned(),
            "--hash".to_owned(),
            "--no-mimetype".to_owned(),
            folder,
        ];
        let output = self.run_raw("lsjson", &args, MAX_RCLONE_OUTPUT_BYTES)?;
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

    fn download(&self, dataset_id: &str, object_name: &str, destination: &Path) -> Result<()> {
        let source = self.object_arg(dataset_id, object_name)?;
        if destination.exists() {
            return Err(MarketDataError::Conflict);
        }
        let args = vec![source, destination.to_string_lossy().into_owned()];
        self.run("copyto", &args, 64 * 1024)?;
        Ok(())
    }
}

fn copy_new_file(source: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        return Err(MarketDataError::Conflict);
    }
    let mut source = File::open(source)?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut destination_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    std::io::copy(&mut source, &mut destination_file)?;
    destination_file.sync_all()?;
    Ok(())
}

fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.contains("..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn safe_object_name(value: &str) -> bool {
    safe_component(value) && !value.starts_with('.') && !value.ends_with('.')
}
