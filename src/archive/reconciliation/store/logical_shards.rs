use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::super::logical_index::{
    CacheLogicalCurrentV1, CacheLogicalIdentityV1, CacheLogicalIndexStatusV1,
    CacheLogicalSessionCurrentV1, CacheLogicalShardRefV1, MAX_LOGICAL_INDEX_BYTES,
    MAX_LOGICAL_SESSIONS_PER_SHARD, MAX_LOGICAL_SHARD_BYTES, MAX_LOGICAL_SHARDS,
    MAX_LOGICAL_TOTAL_BYTES, MAX_LOGICAL_TOTAL_SESSIONS, valid_shard_month,
};
use super::{
    CacheReconciliationError, CacheVersionStoreV1, Result, create_private_dir,
    fresh_temp_component, hash_domain, read_optional_bounded, read_required_bounded,
    replace_plain_file, sync_directory, valid_dimension, valid_sha256, write_new_synced,
};

const ROOT_DOMAIN: &[u8] = b"lqepoch.mdp.cache-logical-index-root.v2\0";
const SHARD_DOMAIN: &[u8] = b"lqepoch.mdp.cache-logical-index-shard.v2\0";
const ROOT_FILE: &str = "current.json";
const SHARDS_DIR: &str = "shards";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheLogicalIndexRootV2 {
    format_version: u32,
    identity: CacheLogicalIdentityV1,
    generation: u64,
    updated_at_unix_seconds: u64,
    session_count: u64,
    shard_bytes: u64,
    shards: BTreeMap<String, CacheLogicalShardRefV1>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheLogicalIndexRootEnvelopeV2 {
    payload: CacheLogicalIndexRootV2,
    payload_sha256: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheLogicalShardV2 {
    format_version: u32,
    identity_fingerprint_sha256: String,
    shard_id: String,
    sessions: BTreeMap<String, CacheLogicalSessionCurrentV1>,
    session_months: BTreeMap<String, String>,
}

struct PlannedShardV2 {
    reference: CacheLogicalShardRefV1,
    bytes: Vec<u8>,
}

pub(super) fn read_logical_current(
    store: &CacheVersionStoreV1,
    identity: &CacheLogicalIdentityV1,
    identity_fingerprint: &str,
) -> Result<Option<CacheLogicalCurrentV1>> {
    let logical_dir = store.logical_dir(identity_fingerprint)?;
    let Some(bytes) =
        read_optional_bounded(&logical_dir.join(ROOT_FILE), MAX_LOGICAL_INDEX_BYTES as u64)?
    else {
        return Ok(None);
    };
    let envelope: CacheLogicalIndexRootEnvelopeV2 = serde_json::from_slice(&bytes)?;
    let payload_sha = root_hash(&envelope.payload)?;
    if envelope.payload.format_version != 2
        || payload_sha != envelope.payload_sha256
        || envelope.payload.identity != *identity
        || envelope.payload.identity.fingerprint()? != identity_fingerprint
        || envelope.payload.generation == 0
        || envelope.payload.shards.is_empty()
        || envelope.payload.shards.len() > MAX_LOGICAL_SHARDS
        || envelope.payload.session_count == 0
        || usize::try_from(envelope.payload.session_count)
            .map_or(true, |count| count > MAX_LOGICAL_TOTAL_SESSIONS)
        || usize::try_from(envelope.payload.shard_bytes)
            .map_or(true, |bytes| bytes > MAX_LOGICAL_TOTAL_BYTES)
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }

    let shards_dir = logical_dir.join(SHARDS_DIR);
    let mut sessions = BTreeMap::new();
    let mut session_months = BTreeMap::new();
    let mut total_bytes = 0_u64;
    let mut referenced_sessions = 0_u64;
    for (shard_id, reference) in &envelope.payload.shards {
        if shard_id != &reference.shard_id || !reference.validate() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let shard_path = shards_dir.join(format!("{}.json", reference.content_sha256));
        let shard_bytes = read_required_bounded(&shard_path, MAX_LOGICAL_SHARD_BYTES as u64)?;
        if shard_bytes.len() as u64 != reference.byte_length
            || shard_hash(&shard_bytes) != reference.content_sha256
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        total_bytes = total_bytes
            .checked_add(shard_bytes.len() as u64)
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
        let shard: CacheLogicalShardV2 = serde_json::from_slice(&shard_bytes)?;
        let (month, _) = shard_id
            .rsplit_once('-')
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
        if shard.format_version != 2
            || shard.identity_fingerprint_sha256 != identity_fingerprint
            || shard.shard_id != *shard_id
            || !valid_shard_month(month)
            || shard.sessions.is_empty()
            || shard.sessions.len() > MAX_LOGICAL_SESSIONS_PER_SHARD
            || usize::try_from(reference.session_count)
                .map_or(true, |count| shard.sessions.len() != count)
            || shard.session_months.len() != shard.sessions.len()
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        for (session_id, entry) in shard.sessions {
            let Some(session_month) = shard.session_months.get(&session_id) else {
                return Err(CacheReconciliationError::UnknownOutcome);
            };
            if session_month != month
                || !valid_dimension(&session_id)
                || sessions.contains_key(&session_id)
            {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            entry.validate(identity, &session_id)?;
            if entry.current.shard_month != month {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            session_months.insert(session_id.clone(), session_month.clone());
            sessions.insert(session_id, entry);
        }
        referenced_sessions = referenced_sessions
            .checked_add(reference.session_count as u64)
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
    }
    if total_bytes > MAX_LOGICAL_TOTAL_BYTES as u64
        || total_bytes != envelope.payload.shard_bytes
        || referenced_sessions != envelope.payload.session_count
        || usize::try_from(referenced_sessions).map_or(true, |count| sessions.len() != count)
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    let current = CacheLogicalCurrentV1 {
        identity: envelope.payload.identity,
        generation: envelope.payload.generation,
        sessions,
        session_months,
        shards: envelope.payload.shards,
        updated_at_unix_seconds: envelope.payload.updated_at_unix_seconds,
    };
    current.validate()?;
    let (expected_shards, expected_shard_bytes) = plan_shards(&current, identity_fingerprint)?;
    let expected_references = expected_shards
        .into_iter()
        .map(|planned| (planned.reference.shard_id.clone(), planned.reference))
        .collect::<BTreeMap<_, _>>();
    if expected_references != current.shards || expected_shard_bytes != total_bytes {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(Some(current))
}

pub(super) fn write_logical_current(
    store: &CacheVersionStoreV1,
    identity_fingerprint: &str,
    payload: &CacheLogicalCurrentV1,
) -> Result<()> {
    payload.validate()?;
    if payload.identity.fingerprint()? != identity_fingerprint {
        return Err(CacheReconciliationError::InvalidInput);
    }
    let (planned_shards, shard_bytes) = plan_shards(payload, identity_fingerprint)?;
    let references = planned_shards
        .iter()
        .map(|planned| {
            (
                planned.reference.shard_id.clone(),
                planned.reference.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if references.len() > MAX_LOGICAL_SHARDS {
        return Err(CacheReconciliationError::LimitExceeded);
    }

    let root = CacheLogicalIndexRootV2 {
        format_version: 2,
        identity: payload.identity.clone(),
        generation: payload.generation,
        updated_at_unix_seconds: payload.updated_at_unix_seconds,
        session_count: u64::try_from(payload.sessions.len())
            .map_err(|_| CacheReconciliationError::LimitExceeded)?,
        shard_bytes,
        shards: references.clone(),
    };
    let bytes = encode_root(&root)?;
    if bytes.len() > MAX_LOGICAL_INDEX_BYTES {
        return Err(CacheReconciliationError::LimitExceeded);
    }

    let logical_dir = store.logical_dir(identity_fingerprint)?;
    let shards_dir = logical_dir.join(SHARDS_DIR);
    create_private_dir(&shards_dir)?;
    for planned in planned_shards {
        let path = shards_dir.join(format!("{}.json", planned.reference.content_sha256));
        match write_new_synced(&path, &planned.bytes) {
            Ok(()) => {}
            Err(CacheReconciliationError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                let existing = read_required_bounded(&path, MAX_LOGICAL_SHARD_BYTES as u64)?;
                if existing != planned.bytes
                    || shard_hash(&existing) != planned.reference.content_sha256
                {
                    return Err(CacheReconciliationError::UnknownOutcome);
                }
            }
            Err(error) => return Err(error),
        }
    }
    sync_directory(&shards_dir)?;

    let mut committed = payload.clone();
    committed.shards = references.clone();
    committed.validate()?;
    let temporary = logical_dir.join(format!(
        ".current-{}-{}.tmp",
        payload.generation,
        fresh_temp_component()?
    ));
    write_new_synced(&temporary, &bytes)?;
    replace_plain_file(&temporary, &logical_dir.join(ROOT_FILE))
        .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    sync_directory(&logical_dir).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    let readback = read_logical_current(store, &payload.identity, identity_fingerprint)?
        .ok_or(CacheReconciliationError::UnknownOutcome)?;
    if readback != committed {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(())
}

pub(super) fn logical_capacity_status(
    payload: &CacheLogicalCurrentV1,
    identity_fingerprint: &str,
) -> Result<CacheLogicalIndexStatusV1> {
    if payload.sessions.len() > MAX_LOGICAL_TOTAL_SESSIONS {
        return Ok(CacheLogicalIndexStatusV1::SessionShardOverflow);
    }
    let required_shards = required_shard_count(payload)?;
    if required_shards > MAX_LOGICAL_SHARDS {
        return Ok(CacheLogicalIndexStatusV1::ShardCountOverflow);
    }
    match plan_shards(payload, identity_fingerprint) {
        Ok((shards, shard_bytes)) => {
            let references = shards
                .iter()
                .map(|planned| {
                    (
                        planned.reference.shard_id.clone(),
                        planned.reference.clone(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let root = CacheLogicalIndexRootV2 {
                format_version: 2,
                identity: payload.identity.clone(),
                generation: payload.generation,
                updated_at_unix_seconds: payload.updated_at_unix_seconds,
                session_count: u64::try_from(payload.sessions.len())
                    .map_err(|_| CacheReconciliationError::LimitExceeded)?,
                shard_bytes,
                shards: references,
            };
            if encode_root(&root)?.len() > MAX_LOGICAL_INDEX_BYTES {
                Ok(CacheLogicalIndexStatusV1::IndexByteOverflow)
            } else {
                Ok(CacheLogicalIndexStatusV1::Updated)
            }
        }
        Err(CacheReconciliationError::LimitExceeded) => {
            Ok(CacheLogicalIndexStatusV1::IndexByteOverflow)
        }
        Err(error) => Err(error),
    }
}

fn required_shard_count(payload: &CacheLogicalCurrentV1) -> Result<usize> {
    let mut sessions_per_month = BTreeMap::<&str, usize>::new();
    for (session_id, entry) in &payload.sessions {
        let month = payload
            .session_months
            .get(session_id)
            .filter(|month| *month == &entry.current.shard_month)
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
        if !valid_shard_month(month) {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let count = sessions_per_month.entry(month.as_str()).or_default();
        *count = count
            .checked_add(1)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
    }
    sessions_per_month
        .values()
        .try_fold(0_usize, |total, count| {
            total
                .checked_add(count.div_ceil(MAX_LOGICAL_SESSIONS_PER_SHARD))
                .ok_or(CacheReconciliationError::LimitExceeded)
        })
}

fn plan_shards(
    payload: &CacheLogicalCurrentV1,
    identity_fingerprint: &str,
) -> Result<(Vec<PlannedShardV2>, u64)> {
    let mut sessions_by_month: BTreeMap<String, Vec<(String, CacheLogicalSessionCurrentV1)>> =
        BTreeMap::new();
    for (session_id, entry) in &payload.sessions {
        let month = payload
            .session_months
            .get(session_id)
            .filter(|month| *month == &entry.current.shard_month)
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
        sessions_by_month
            .entry(month.clone())
            .or_default()
            .push((session_id.clone(), entry.clone()));
    }
    let mut planned = Vec::new();
    let mut total_bytes = 0_u64;
    for (month, sessions) in sessions_by_month {
        if !valid_shard_month(&month) {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        for (subshard_index, batch) in sessions.chunks(MAX_LOGICAL_SESSIONS_PER_SHARD).enumerate() {
            let shard_id = format!("{month}-{subshard_index:04}");
            let shard_sessions = batch
                .iter()
                .map(|(session, entry)| (session.clone(), entry.clone()))
                .collect::<BTreeMap<_, _>>();
            let shard_months = batch
                .iter()
                .map(|(session, _)| (session.clone(), month.clone()))
                .collect::<BTreeMap<_, _>>();
            let shard = CacheLogicalShardV2 {
                format_version: 2,
                identity_fingerprint_sha256: identity_fingerprint.to_owned(),
                shard_id: shard_id.clone(),
                sessions: shard_sessions,
                session_months: shard_months,
            };
            let bytes = serde_json::to_vec(&shard)?;
            if bytes.is_empty() || bytes.len() > MAX_LOGICAL_SHARD_BYTES {
                return Err(CacheReconciliationError::LimitExceeded);
            }
            total_bytes = total_bytes
                .checked_add(bytes.len() as u64)
                .ok_or(CacheReconciliationError::LimitExceeded)?;
            if total_bytes > MAX_LOGICAL_TOTAL_BYTES as u64 {
                return Err(CacheReconciliationError::LimitExceeded);
            }
            let reference = CacheLogicalShardRefV1 {
                shard_id,
                content_sha256: shard_hash(&bytes),
                byte_length: bytes.len() as u64,
                session_count: u32::try_from(batch.len())
                    .map_err(|_| CacheReconciliationError::LimitExceeded)?,
            };
            if !reference.validate() {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            planned.push(PlannedShardV2 { reference, bytes });
            if planned.len() > MAX_LOGICAL_SHARDS {
                return Err(CacheReconciliationError::LimitExceeded);
            }
        }
    }
    Ok((planned, total_bytes))
}

fn root_hash(payload: &CacheLogicalIndexRootV2) -> Result<String> {
    Ok(hash_domain(ROOT_DOMAIN, &serde_json::to_vec(payload)?))
}

fn encode_root(payload: &CacheLogicalIndexRootV2) -> Result<Vec<u8>> {
    let envelope = CacheLogicalIndexRootEnvelopeV2 {
        payload_sha256: root_hash(payload)?,
        payload: payload.clone(),
    };
    Ok(serde_json::to_vec(&envelope)?)
}

fn shard_hash(bytes: &[u8]) -> String {
    hash_domain(SHARD_DOMAIN, bytes)
}
