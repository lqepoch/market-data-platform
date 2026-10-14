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
const TRANSACTION_DOMAIN_V2: &[u8] = b"lqepoch.mdp.cache-pointer-transaction.v2\0";
const TRANSACTION_ID_DOMAIN_V1: &[u8] = b"lqepoch.mdp.cache-pointer-transaction-id.v1\0";
const TRANSACTION_ID_DOMAIN_V2: &[u8] = b"lqepoch.mdp.cache-pointer-transition-id.v2\0";
const TRANSACTION_MIRROR_DOMAIN: &[u8] = b"lqepoch.mdp.cache-pointer-transaction-mirror.v1\0";
const TRANSACTION_MIRROR_DOMAIN_V2: &[u8] = b"lqepoch.mdp.cache-pointer-transaction-mirror.v2\0";
pub(super) const TRANSACTION_FILE: &str = "transaction.json";
const TRANSACTION_MODE_FILE: &str = ".transaction-mode";
pub(super) const TRANSACTION_MODE_MAGIC: &[u8] = b"lqepoch.mdp.cache-transactional.v1\n";
pub(super) const MIRROR_FILE: &str = "transaction-mirror.json";
pub(super) const MAX_TRANSACTION_BYTES: u64 = 8 * 1024 * 1024 + 256 * 1024;
pub(super) const MAX_MIRROR_BYTES: u64 = 16 * 1024;
pub(super) const MAX_CURRENT_MANIFEST_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachePointerTransactionPayload {
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
    // Keep the legacy JSON field name for V1 compatibility, but model it as an opaque historic
    // operation suffix. V2 writes omit it and never use it in their content-addressed ID.
    #[serde(
        rename = "transaction_nonce",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub(super) legacy_operation_suffix: Option<String>,
    pub(super) previous_transaction_sha256: Option<String>,
    pub(super) recovery_replaced_damaged_source_manifest: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) initial_base: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachePointerTransactionEnvelope {
    pub(super) payload: CachePointerTransactionPayload,
    pub(super) payload_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachePointerTransactionMirror {
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

#[derive(Serialize)]
struct CachePointerTransitionIdMaterialV2<'a> {
    logical_identity_fingerprint_sha256: &'a str,
    source_key_fingerprint_sha256: &'a str,
    previous_source_current: Option<&'a CacheCurrentV1>,
    source_current: Option<&'a CacheCurrentV1>,
    previous_logical_root: Option<&'a CacheLogicalIndexRootEnvelopeV2>,
    logical_root: Option<&'a CacheLogicalIndexRootEnvelopeV2>,
    committed_at_unix_seconds: u64,
    previous_transaction_sha256: Option<&'a str>,
    recovery_replaced_damaged_source_manifest: bool,
    initial_base: bool,
}

pub(super) fn validate_transaction(
    payload: &CachePointerTransactionPayload,
    identity: &CacheLogicalIdentityV1,
    identity_fingerprint: &str,
) -> Result<()> {
    if !matches!(payload.format_version, 1 | 2)
        || payload.logical_identity_fingerprint_sha256 != identity_fingerprint
        || payload.source_key_fingerprint_sha256 != payload.source_key.fingerprint()?
        || CacheLogicalIdentityV1::from_key(&payload.source_key)? != *identity
        || (payload.format_version == 1 && payload.legacy_operation_suffix.is_none())
        || (payload.format_version == 2 && payload.legacy_operation_suffix.is_some())
        || payload.transaction_id != transaction_id_from_payload(payload)?
        || payload
            .previous_transaction_sha256
            .as_deref()
            .is_some_and(|value| !valid_sha256(value))
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    if payload.initial_base {
        let previous_source_hash = payload
            .previous_source_current
            .as_ref()
            .map(current_hash)
            .transpose()?;
        let source_hash = payload
            .source_current
            .as_ref()
            .map(current_hash)
            .transpose()?;
        let previous_root_hash = payload
            .previous_logical_root
            .as_ref()
            .map(root_envelope_hash)
            .transpose()?;
        let root_hash = payload
            .logical_root
            .as_ref()
            .map(root_envelope_hash)
            .transpose()?;
        if payload.previous_transaction_sha256.is_some()
            || previous_source_hash != source_hash
            || previous_root_hash != root_hash
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
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

fn is_false(value: &bool) -> bool {
    !value
}

pub(super) fn transaction_id_from_payload(
    payload: &CachePointerTransactionPayload,
) -> Result<String> {
    match payload.format_version {
        1 => {
            let legacy_operation_suffix = payload
                .legacy_operation_suffix
                .as_deref()
                .ok_or(CacheReconciliationError::UnknownOutcome)?;
            Ok(hash_domain(
                TRANSACTION_ID_DOMAIN_V1,
                &serde_json::to_vec(&(
                    &payload.logical_identity_fingerprint_sha256,
                    &payload.source_key_fingerprint_sha256,
                    payload.source_current.as_ref(),
                    payload.logical_root.as_ref(),
                    payload.committed_at_unix_seconds,
                    legacy_operation_suffix,
                ))?,
            ))
        }
        2 => {
            // V2 IDs identify the complete immutable transition. They are content addresses,
            // not random nonces, authorization tokens, or cryptographic capabilities.
            let material = CachePointerTransitionIdMaterialV2 {
                logical_identity_fingerprint_sha256: &payload.logical_identity_fingerprint_sha256,
                source_key_fingerprint_sha256: &payload.source_key_fingerprint_sha256,
                previous_source_current: payload.previous_source_current.as_ref(),
                source_current: payload.source_current.as_ref(),
                previous_logical_root: payload.previous_logical_root.as_ref(),
                logical_root: payload.logical_root.as_ref(),
                committed_at_unix_seconds: payload.committed_at_unix_seconds,
                previous_transaction_sha256: payload.previous_transaction_sha256.as_deref(),
                recovery_replaced_damaged_source_manifest: payload
                    .recovery_replaced_damaged_source_manifest,
                initial_base: payload.initial_base,
            };
            Ok(hash_domain(
                TRANSACTION_ID_DOMAIN_V2,
                &serde_json::to_vec(&material)?,
            ))
        }
        _ => Err(CacheReconciliationError::UnknownOutcome),
    }
}

pub(super) fn transaction_hash(payload: &CachePointerTransactionPayload) -> Result<String> {
    let domain = match payload.format_version {
        1 => TRANSACTION_DOMAIN,
        2 => TRANSACTION_DOMAIN_V2,
        _ => return Err(CacheReconciliationError::UnknownOutcome),
    };
    Ok(hash_domain(domain, &serde_json::to_vec(payload)?))
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
    envelope: &CachePointerTransactionEnvelope,
) -> Result<CachePointerTransactionMirror> {
    let mut mirror = CachePointerTransactionMirror {
        format_version: envelope.payload.format_version,
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

pub(super) fn write_mirror(directory: &Path, mirror: &CachePointerTransactionMirror) -> Result<()> {
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

pub(super) fn read_mirror(directory: &Path) -> Result<CachePointerTransactionMirror> {
    let bytes = read_required_bounded(&directory.join(MIRROR_FILE), MAX_MIRROR_BYTES)?;
    let mirror: CachePointerTransactionMirror =
        serde_json::from_slice(&bytes).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    if !matches!(mirror.format_version, 1 | 2)
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

fn mirror_hash(mirror: &CachePointerTransactionMirror) -> Result<String> {
    let mut unsigned = mirror.clone();
    unsigned.payload_sha256.clear();
    let domain = match mirror.format_version {
        1 => TRANSACTION_MIRROR_DOMAIN,
        2 => TRANSACTION_MIRROR_DOMAIN_V2,
        _ => return Err(CacheReconciliationError::UnknownOutcome),
    };
    Ok(hash_domain(domain, &serde_json::to_vec(&unsigned)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::reconciliation::{
        CacheAuthorityV1, CacheCurrentOperationV1, CacheCurrentV1, CacheKeyV1,
    };
    use market_contracts::EntitlementState;

    fn test_key() -> CacheKeyV1 {
        let schema_id = crate::schema::MARKET_BARS_SCHEMA_V1_ID.to_owned();
        let descriptor = market_contracts::trusted_parquet_schema(&schema_id).unwrap();
        let schema_fingerprint = market_contracts::trusted_schema_fingerprint(&schema_id).unwrap();
        CacheKeyV1 {
            provider: "alpaca".to_owned(),
            feed: "sip".to_owned(),
            entitlement: EntitlementState::Unknown,
            numeric_encoding: "decimal_string".to_owned(),
            adjustment: "raw".to_owned(),
            requested_resolution: "one_minute".to_owned(),
            effective_resolution: "one_minute".to_owned(),
            provider_mode: "alpaca_only".to_owned(),
            source_scope_id: "test-scope".to_owned(),
            entitlement_scope_sha256: "1".repeat(64),
            rights_profile_sha256: "2".repeat(64),
            asset_class: "stock".to_owned(),
            symbol: "AAPL".to_owned(),
            session_scope: "regular".to_owned(),
            session_instance_id: "session-1".to_owned(),
            window_start_utc: "2026-10-14T13:30:00Z".to_owned(),
            window_end_exclusive_utc: "2026-10-14T14:30:00Z".to_owned(),
            market_calendar_version: Some("calendar-v1".to_owned()),
            request_cursor_sha256: None,
            schema_version: descriptor.schema_version,
            schema_id,
            schema_fingerprint_sha256: schema_fingerprint,
        }
    }

    fn test_current(key: &CacheKeyV1) -> CacheCurrentV1 {
        CacheCurrentV1 {
            key_fingerprint_sha256: key.fingerprint().unwrap(),
            version_id: "a".repeat(64),
            receipt_sha256: "b".repeat(64),
            generation: 1,
            authority: CacheAuthorityV1::Provisional,
            operation: CacheCurrentOperationV1::Publish,
            previous_current_version_id: None,
            updated_at_unix_seconds: 1_800_000_000,
        }
    }

    fn payload_v2(key: CacheKeyV1) -> CachePointerTransactionPayload {
        let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();
        CachePointerTransactionPayload {
            format_version: 2,
            transaction_id: String::new(),
            logical_identity_fingerprint_sha256: identity.fingerprint().unwrap(),
            source_key_fingerprint_sha256: key.fingerprint().unwrap(),
            source_key: key,
            previous_source_current: None,
            source_current: None,
            previous_logical_root: None,
            logical_root: None,
            committed_at_unix_seconds: 1_800_000_000,
            legacy_operation_suffix: None,
            previous_transaction_sha256: None,
            recovery_replaced_damaged_source_manifest: false,
            initial_base: true,
        }
    }

    #[test]
    fn legacy_v1_transaction_id_and_payload_hash_remain_readable() {
        let key = test_key();
        let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();
        let identity_fingerprint = identity.fingerprint().unwrap();
        let key_fingerprint = key.fingerprint().unwrap();
        let mut payload = CachePointerTransactionPayload {
            format_version: 1,
            transaction_id: String::new(),
            logical_identity_fingerprint_sha256: identity_fingerprint.clone(),
            source_key_fingerprint_sha256: key_fingerprint,
            source_key: key,
            previous_source_current: None,
            source_current: None,
            previous_logical_root: None,
            logical_root: None,
            committed_at_unix_seconds: 1_800_000_000,
            legacy_operation_suffix: Some("legacy-readable-value".to_owned()),
            previous_transaction_sha256: None,
            recovery_replaced_damaged_source_manifest: false,
            initial_base: false,
        };
        payload.transaction_id = transaction_id_from_payload(&payload).unwrap();

        validate_transaction(&payload, &identity, &identity_fingerprint).unwrap();
        let stored = serde_json::to_vec(&payload).unwrap();
        let decoded: CachePointerTransactionPayload = serde_json::from_slice(&stored).unwrap();
        assert_eq!(decoded.format_version, payload.format_version);
        assert_eq!(decoded.transaction_id, payload.transaction_id);
        assert_eq!(
            decoded.legacy_operation_suffix,
            payload.legacy_operation_suffix
        );
        assert_eq!(
            transaction_hash(&decoded).unwrap(),
            transaction_hash(&payload).unwrap()
        );
    }

    #[test]
    fn v2_transaction_id_binds_both_sides_of_transition_without_random_suffix() {
        let key = test_key();
        let mut payload = payload_v2(key.clone());
        let current = test_current(&key);
        payload.source_current = Some(current.clone());
        payload.transaction_id = transaction_id_from_payload(&payload).unwrap();
        let id_with_no_previous = payload.transaction_id.clone();

        payload.previous_source_current = Some(current);
        assert_ne!(
            transaction_id_from_payload(&payload).unwrap(),
            id_with_no_previous
        );
        payload.transaction_id = transaction_id_from_payload(&payload).unwrap();
        let encoded = serde_json::to_vec(&payload).unwrap();
        let decoded: CachePointerTransactionPayload = serde_json::from_slice(&encoded).unwrap();
        assert!(decoded.legacy_operation_suffix.is_none());
        assert_eq!(
            transaction_id_from_payload(&decoded).unwrap(),
            decoded.transaction_id
        );
        let identity = CacheLogicalIdentityV1::from_key(&decoded.source_key).unwrap();
        validate_transaction(
            &decoded,
            &identity,
            &decoded.logical_identity_fingerprint_sha256,
        )
        .unwrap();
    }
}
