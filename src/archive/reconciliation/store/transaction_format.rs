use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use super::super::logical_index::CacheLogicalIdentityV1;
use super::logical_shards::{CacheLogicalIndexRootEnvelopeV2, read_logical_current_from_envelope};
use super::{
    CURRENT_DOMAIN, CacheCurrentEnvelopeV1, CacheCurrentV1, CacheKeyV1, CacheReconciliationError,
    CacheVersionStoreV1, Result, fresh_temp_component, hash_domain, read_optional_bounded,
    read_required_bounded, replace_plain_file, sync_directory, valid_sha256, write_new_synced,
};

pub(super) const TRANSACTION_DOMAIN: &[u8] = b"lqepoch.mdp.cache-pointer-transaction.v1\0";
pub(super) const TRANSACTION_ID_DOMAIN: &[u8] = b"lqepoch.mdp.cache-pointer-transaction-id.v1\0";
const TRANSACTION_MIRROR_DOMAIN: &[u8] = b"lqepoch.mdp.cache-pointer-transaction-mirror.v1\0";
pub(super) const TRANSACTION_FILE: &str = "transaction.json";
const TRANSACTION_MODE_FILE: &str = ".transaction-mode";
pub(super) const TRANSACTION_MODE_MAGIC: &[u8] = b"lqepoch.mdp.cache-transactional.v1\n";
pub(super) const MIRROR_FILE: &str = "transaction-mirror.json";
pub(super) const MAX_TRANSACTION_BYTES: u64 = 8 * 1024 * 1024 + 256 * 1024;
pub(super) const MAX_MIRROR_BYTES: u64 = 16 * 1024;
pub(super) const MAX_CURRENT_MANIFEST_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachePointerTransactionV1 {
    pub(super) format_version: u32,
    pub(super) transaction_id: String,
    pub(super) logical_identity_fingerprint_sha256: String,
    pub(super) source_key: CacheKeyV1,
    pub(super) source_key_fingerprint_sha256: String,
    pub(super) previous_source_current: Option<CacheCurrentV1>,
    pub(super) source_current: Option<CacheCurrentV1>,
    pub(super) previous_logical_root: Option<CacheLogicalIndexRootEnvelopeV2>,
    pub(super) logical_root: Option<CacheLogicalIndexRootEnvelopeV2>,
    pub(super) committed_at_unix_seconds: u64,
    pub(super) transaction_nonce: String,
    pub(super) previous_transaction_sha256: Option<String>,
    pub(super) recovery_replaced_damaged_source_manifest: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachePointerTransactionEnvelopeV1 {
    pub(super) payload: CachePointerTransactionV1,
    pub(super) payload_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachePointerTransactionMirrorV1 {
    pub(super) format_version: u32,
    pub(super) transaction_id: String,
    pub(super) transaction_sha256: String,
    pub(super) logical_identity_fingerprint_sha256: String,
    pub(super) source_key_fingerprint_sha256: String,
    pub(super) source_current_sha256: Option<String>,
    pub(super) logical_root_sha256: Option<String>,
    pub(super) sealed: bool,
    pub(super) payload_sha256: String,
}

pub(super) fn validate_transaction(
    payload: &CachePointerTransactionV1,
    identity: &CacheLogicalIdentityV1,
    identity_fingerprint: &str,
) -> Result<()> {
    if payload.format_version != 1
        || payload.logical_identity_fingerprint_sha256 != identity_fingerprint
        || payload.source_key_fingerprint_sha256 != payload.source_key.fingerprint()?
        || CacheLogicalIdentityV1::from_key(&payload.source_key)? != *identity
        || payload.transaction_id != transaction_id_from_payload(payload)?
        || payload
            .previous_transaction_sha256
            .as_deref()
            .is_some_and(|value| !valid_sha256(value))
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    if let Some(current) = payload.source_current.as_ref()
        && (current.key_fingerprint_sha256 != payload.source_key_fingerprint_sha256
            || !valid_sha256(&current.version_id)
            || !valid_sha256(&current.receipt_sha256)
            || current.generation == 0)
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(())
}

pub(super) fn transaction_id(
    identity_fingerprint: &str,
    key_fingerprint: &str,
    current: Option<&CacheCurrentV1>,
    root: Option<&CacheLogicalIndexRootEnvelopeV2>,
    now_unix_seconds: u64,
    nonce: &str,
) -> Result<String> {
    Ok(hash_domain(
        TRANSACTION_ID_DOMAIN,
        &serde_json::to_vec(&(
            identity_fingerprint,
            key_fingerprint,
            current,
            root,
            now_unix_seconds,
            nonce,
        ))?,
    ))
}

fn transaction_id_from_payload(payload: &CachePointerTransactionV1) -> Result<String> {
    transaction_id(
        &payload.logical_identity_fingerprint_sha256,
        &payload.source_key_fingerprint_sha256,
        payload.source_current.as_ref(),
        payload.logical_root.as_ref(),
        payload.committed_at_unix_seconds,
        &payload.transaction_nonce,
    )
}

pub(super) fn transaction_hash(payload: &CachePointerTransactionV1) -> Result<String> {
    Ok(hash_domain(
        TRANSACTION_DOMAIN,
        &serde_json::to_vec(payload)?,
    ))
}

pub(super) fn ensure_transaction_mode(directory: &Path) -> Result<bool> {
    let path = directory.join(TRANSACTION_MODE_FILE);
    match write_new_synced(&path, TRANSACTION_MODE_MAGIC) {
        Ok(()) => {
            sync_directory(directory)?;
            Ok(true)
        }
        Err(CacheReconciliationError::Io(error))
            if error.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            let bytes = read_required_bounded(&path, 128)?;
            if bytes == TRANSACTION_MODE_MAGIC {
                Ok(false)
            } else {
                Err(CacheReconciliationError::UnknownOutcome)
            }
        }
        Err(error) => Err(error),
    }
}

pub(super) fn remove_created_transaction_mode(directory: &Path, created: bool) -> Result<()> {
    if !created {
        return Ok(());
    }
    let path = directory.join(TRANSACTION_MODE_FILE);
    if read_required_bounded(&path, 128)? != TRANSACTION_MODE_MAGIC {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    fs::remove_file(path)?;
    sync_directory(directory).map_err(|_| CacheReconciliationError::UnknownOutcome)
}

pub(super) fn transaction_mode_exists(directory: &Path) -> Result<bool> {
    let path = directory.join(TRANSACTION_MODE_FILE);
    let Some(bytes) = read_optional_bounded(&path, 128)? else {
        return Ok(false);
    };
    if bytes != TRANSACTION_MODE_MAGIC {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(true)
}

pub(super) fn make_mirror(
    envelope: &CachePointerTransactionEnvelopeV1,
) -> Result<CachePointerTransactionMirrorV1> {
    let mut mirror = CachePointerTransactionMirrorV1 {
        format_version: 1,
        transaction_id: envelope.payload.transaction_id.clone(),
        transaction_sha256: envelope.payload_sha256.clone(),
        logical_identity_fingerprint_sha256: envelope
            .payload
            .logical_identity_fingerprint_sha256
            .clone(),
        source_key_fingerprint_sha256: envelope.payload.source_key_fingerprint_sha256.clone(),
        source_current_sha256: envelope
            .payload
            .source_current
            .as_ref()
            .map(current_hash)
            .transpose()?,
        logical_root_sha256: envelope
            .payload
            .logical_root
            .as_ref()
            .map(root_envelope_hash)
            .transpose()?,
        sealed: true,
        payload_sha256: String::new(),
    };
    mirror.payload_sha256 = mirror_hash(&mirror)?;
    Ok(mirror)
}

pub(super) fn write_mirror(
    directory: &Path,
    mirror: &CachePointerTransactionMirrorV1,
) -> Result<()> {
    let bytes = serde_json::to_vec(mirror)?;
    if bytes.len() as u64 > MAX_MIRROR_BYTES {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    let temporary = directory.join(format!(".mirror-{}.tmp", fresh_temp_component()?));
    write_new_synced(&temporary, &bytes)?;
    replace_plain_file(&temporary, &directory.join(MIRROR_FILE))
        .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    sync_directory(directory).map_err(|_| CacheReconciliationError::UnknownOutcome)
}

pub(super) fn read_mirror(directory: &Path) -> Result<CachePointerTransactionMirrorV1> {
    let bytes = read_required_bounded(&directory.join(MIRROR_FILE), MAX_MIRROR_BYTES)?;
    let mirror: CachePointerTransactionMirrorV1 =
        serde_json::from_slice(&bytes).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    if mirror.format_version != 1
        || !mirror.sealed
        || !valid_sha256(&mirror.transaction_id)
        || !valid_sha256(&mirror.transaction_sha256)
        || !valid_sha256(&mirror.logical_identity_fingerprint_sha256)
        || !valid_sha256(&mirror.source_key_fingerprint_sha256)
        || mirror
            .source_current_sha256
            .as_deref()
            .is_some_and(|value| !valid_sha256(value))
        || mirror
            .logical_root_sha256
            .as_deref()
            .is_some_and(|value| !valid_sha256(value))
        || mirror_hash(&mirror)? != mirror.payload_sha256
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(mirror)
}

pub(super) fn current_hash(current: &CacheCurrentV1) -> Result<String> {
    Ok(hash_domain(
        TRANSACTION_DOMAIN,
        &serde_json::to_vec(current)?,
    ))
}

pub(super) fn current_manifest_bytes(payload: &CacheCurrentV1) -> Result<Vec<u8>> {
    let payload_bytes = serde_json::to_vec(payload)?;
    let envelope = CacheCurrentEnvelopeV1 {
        payload: payload.clone(),
        payload_sha256: hash_domain(CURRENT_DOMAIN, &payload_bytes),
    };
    let bytes = serde_json::to_vec(&envelope)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_CURRENT_MANIFEST_BYTES {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    Ok(bytes)
}

pub(super) fn root_envelope_hash(root: &CacheLogicalIndexRootEnvelopeV2) -> Result<String> {
    Ok(hash_domain(TRANSACTION_DOMAIN, &serde_json::to_vec(root)?))
}

pub(super) fn read_current_payload_hash(directory: &Path) -> Result<Option<String>> {
    read_current_payload(directory)?
        .as_ref()
        .map(current_hash)
        .transpose()
}

pub(super) fn read_current_payload(directory: &Path) -> Result<Option<CacheCurrentV1>> {
    let path = directory.join(super::CURRENT_FILE);
    let Some(bytes) = read_optional_bounded(&path, MAX_CURRENT_MANIFEST_BYTES)? else {
        return Ok(None);
    };
    let envelope: CacheCurrentEnvelopeV1 =
        serde_json::from_slice(&bytes).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    let expected = hash_domain(CURRENT_DOMAIN, &serde_json::to_vec(&envelope.payload)?);
    if expected != envelope.payload_sha256 {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(Some(envelope.payload))
}

pub(super) fn read_physical_logical_root(
    store: &CacheVersionStoreV1,
    identity: &CacheLogicalIdentityV1,
    identity_fingerprint: &str,
) -> Result<Option<CacheLogicalIndexRootEnvelopeV2>> {
    let path = store
        .logical_dir(identity_fingerprint)?
        .join("current.json");
    let Some(bytes) = read_optional_bounded(&path, 4 * 1024 * 1024)? else {
        return Ok(None);
    };
    let root: CacheLogicalIndexRootEnvelopeV2 =
        serde_json::from_slice(&bytes).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    let current = read_logical_current_from_envelope(store, identity, identity_fingerprint, &root)?;
    if current.is_none() {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(Some(root))
}

pub(super) fn path_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn mirror_hash(mirror: &CachePointerTransactionMirrorV1) -> Result<String> {
    let mut unsigned = mirror.clone();
    unsigned.payload_sha256.clear();
    Ok(hash_domain(
        TRANSACTION_MIRROR_DOMAIN,
        &serde_json::to_vec(&unsigned)?,
    ))
}
