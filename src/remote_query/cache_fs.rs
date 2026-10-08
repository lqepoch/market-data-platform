use super::*;
use std::{
    io::{Read, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use market_contracts::{EntitlementState, NumericEncodingV1};
use sha2::{Digest, Sha256};

pub(super) fn validate_remote_object(object: &RemoteObject, max_size: u64) -> Result<()> {
    if !valid_external_id(&object.id) || object.size_bytes == 0 || object.size_bytes > max_size {
        return Err(MarketDataError::Storage(
            crate::error::StorageFailure::MalformedListing,
        ));
    }
    if object
        .md5
        .as_deref()
        .is_some_and(|md5| md5.is_empty() || md5.len() > 128 || md5.chars().any(char::is_control))
    {
        return Err(MarketDataError::Storage(
            crate::error::StorageFailure::MalformedListing,
        ));
    }
    Ok(())
}

pub(super) fn validate_manifest(
    namespace: DatasetNamespace,
    dataset_id: &str,
    transport_kind: TransportKind,
    manifest: &DatasetManifestV1,
    observed_object: &RemoteObject,
) -> Result<()> {
    manifest.validate().map_err(|_| MarketDataError::Contract)?;
    if manifest.dataset_id != dataset_id
        || manifest.object.object_name != format!("{dataset_id}.parquet")
        || manifest.object.object_id.as_deref() != Some(&observed_object.id)
        || manifest.object.size_bytes != observed_object.size_bytes
    {
        return Err(MarketDataError::Conflict);
    }
    let expected_transport = match transport_kind {
        TransportKind::LocalTest => DatasetTransportV1::LocalTest,
        TransportKind::RcloneGoogleDrive => DatasetTransportV1::RcloneGoogleDrive,
    };
    if manifest.object.transport != expected_transport {
        return Err(MarketDataError::Conflict);
    }
    validate_namespace(namespace, manifest)
}

pub(super) fn validate_namespace(
    namespace: DatasetNamespace,
    manifest: &DatasetManifestV1,
) -> Result<()> {
    if namespace == DatasetNamespace::Curated
        && (manifest.source.provider != "alpaca"
            || !matches!(manifest.source.feed.as_str(), "sip" | "opra")
            || manifest.source.entitlement != EntitlementState::Authorized
            || !matches!(
                manifest.source.numeric_encoding,
                NumericEncodingV1::DecimalToken | NumericEncodingV1::IntegerToken
            ))
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    Ok(())
}

pub(super) fn validate_manifest_facts(
    manifest: &DatasetManifestV1,
    parquet: &ParquetVerification,
) -> Result<()> {
    let mut manifest_source = manifest.source.clone();
    manifest_source.source_record_id = None;
    if parquet.footer_rows != manifest.row_count
        || parquet.footer_rows != manifest.object.parquet_footer_rows
        || parquet.schema_sha256 != manifest.object.parquet_schema_sha256
        || parquet.size_bytes != manifest.object.size_bytes
        || parquet.source != manifest_source
        || parquet.symbols != manifest.symbols
        || parquet.time_range != manifest.time_range
        || parquet.source_timestamp_missing_rows != manifest.source_timestamp_missing_rows
        || !manifest.completion.input_eof
        || manifest.completion.readback_sha256 != manifest.object.content_sha256
    {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(())
}

pub(super) fn schema_id_for_hash(hash: &str) -> Result<&'static str> {
    if hash
        == market_contracts::parquet_schema::trusted_schema_fingerprint(EVENT_SCHEMA_ID)
            .map_err(|_| MarketDataError::ParquetSchema)?
    {
        Ok(EVENT_SCHEMA_ID)
    } else if hash
        == market_contracts::parquet_schema::trusted_schema_fingerprint(MINUTE_BAR_SCHEMA_ID)
            .map_err(|_| MarketDataError::ParquetSchema)?
    {
        Ok(MINUTE_BAR_SCHEMA_ID)
    } else {
        Err(MarketDataError::ParquetSchema)
    }
}

pub(super) fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(MarketDataError::InputLimit);
    }
    let file = File::open(path)?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(MarketDataError::InputLimit);
    }
    Ok(bytes)
}

pub(super) struct FileHash {
    pub(super) content_sha256: String,
    pub(super) size_bytes: u64,
}

pub(super) fn hash_file(path: &Path, maximum: u64) -> Result<FileHash> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(MarketDataError::InputLimit);
    }
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or(MarketDataError::InputLimit)?;
        if total > maximum {
            return Err(MarketDataError::InputLimit);
        }
        hasher.update(&buffer[..count]);
    }
    Ok(FileHash {
        content_sha256: hex::encode(hasher.finalize()),
        size_bytes: total,
    })
}

pub(super) fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

pub(super) fn replace_verified_file(source: &Path, destination: &Path) -> Result<()> {
    match fs::symlink_metadata(destination) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(MarketDataError::Io(error)),
    }
    fs::rename(source, destination)?;
    Ok(())
}

pub(super) fn require_plain_dir(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(MarketDataError::InvalidInput);
    }
    Ok(())
}

pub(super) fn directory_bytes_bounded(root: &Path) -> Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    let mut entries = 0usize;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            entries += 1;
            if entries > CACHE_DIR_ENTRY_LIMIT {
                return Err(MarketDataError::InputLimit);
            }
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(MarketDataError::InvalidInput);
            }
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                total = total
                    .checked_add(entry.metadata()?.len())
                    .ok_or(MarketDataError::InputLimit)?;
            }
        }
    }
    Ok(total)
}

pub(super) fn cache_entry_count(root: &Path) -> Result<usize> {
    let mut count = 0usize;
    for namespace in ["curated", "diagnostic"] {
        let path = root.join(namespace);
        if !path.exists() {
            continue;
        }
        require_plain_dir(&path)?;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_symlink() {
                return Err(MarketDataError::InvalidInput);
            }
            if entry.file_type()?.is_dir() && entry.path().join(CACHE_RECEIPT_NAME).exists() {
                count = count.checked_add(1).ok_or(MarketDataError::InputLimit)?;
            }
        }
    }
    Ok(count)
}

pub(super) fn valid_external_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

pub(super) fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(super) fn system_time_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
