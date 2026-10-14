use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
};

use serde::{Deserialize, Serialize};

use super::super::logical_index::{
    MAX_LOGICAL_INDEX_BYTES, MAX_LOGICAL_SESSIONS, session_coverage_bounds, version_ref_for_session,
};
use super::{
    CacheAuthorityV1, CacheLogicalCurrentV1, CacheLogicalIdentityV1, CacheLogicalInvalidationV1,
    CacheLogicalReadResultV1, CacheLogicalSessionChangeV1, CacheLogicalSessionCurrentV1,
    CacheLogicalVersionRefV1, CacheLogicalVersionSourceV1, CacheReadResultV1,
    CacheReconciliationError, CacheVersionStoreV1, Result, VERSION_NONCE, ensure_worker_supported,
    hash_domain, is_fresh_at, read_optional_bounded, read_required_bounded, replace_plain_file,
    sync_directory, valid_dimension, valid_sha256, write_new_synced,
};

const LOGICAL_CURRENT_DOMAIN: &[u8] = b"lqepoch.mdp.cache-logical-current-manifest.v1\0";
const LOGICAL_CURRENT_FILE: &str = "current.json";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheLogicalCurrentEnvelopeV1 {
    payload: CacheLogicalCurrentV1,
    payload_sha256: String,
}

impl CacheVersionStoreV1 {
    /// Load provisional provider-neutral data. Production authoritative reads require the
    /// verifier-taking method, which rechecks the retained signature and current expiry.
    pub(crate) fn load_logical_current(
        &self,
        identity: &CacheLogicalIdentityV1,
        session_instance_id: &str,
        now_unix_seconds: u64,
    ) -> Result<CacheLogicalReadResultV1> {
        let result =
            self.load_logical_current_unverified(identity, session_instance_id, now_unix_seconds)?;
        self.require_current_admission(&result.snapshot, now_unix_seconds, None)?;
        Ok(result)
    }

    /// Load a provider-neutral authoritative pointer after rechecking its retained signed
    /// admission against the current trusted issuer/revocation policy.
    pub(crate) fn load_logical_current_with_verifier(
        &self,
        identity: &CacheLogicalIdentityV1,
        session_instance_id: &str,
        now_unix_seconds: u64,
        verifier: &dyn super::authority::TrustedProviderAdmissionVerifierV1,
    ) -> Result<CacheLogicalReadResultV1> {
        let result =
            self.load_logical_current_unverified(identity, session_instance_id, now_unix_seconds)?;
        self.require_current_admission(&result.snapshot, now_unix_seconds, Some(verifier))?;
        Ok(result)
    }

    fn load_logical_current_unverified(
        &self,
        identity: &CacheLogicalIdentityV1,
        session_instance_id: &str,
        now_unix_seconds: u64,
    ) -> Result<CacheLogicalReadResultV1> {
        ensure_worker_supported()?;
        identity.validate()?;
        if !valid_dimension(session_instance_id) {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let fingerprint = identity.fingerprint()?;
        let _lock = self.lock(&format!("logical-{fingerprint}.lock"))?;
        let logical_current = self
            .read_logical_current_unlocked(identity, &fingerprint)?
            .ok_or(CacheReconciliationError::NotFound)?;
        let session = logical_current
            .sessions
            .get(session_instance_id)
            .cloned()
            .ok_or(CacheReconciliationError::NotFound)?;
        let snapshot = self.read_version(
            &session.current.key,
            &session.current.key_fingerprint_sha256,
            &session.current.version_id,
        )?;
        if snapshot.receipt_sha256 != session.current.receipt_sha256
            || snapshot.conflict_rejected
            || snapshot.current.authority != session.current.authority
            || version_ref_for_session(
                CacheLogicalVersionSourceV1 {
                    key: &snapshot.key,
                    key_fingerprint_sha256: &snapshot.current.key_fingerprint_sha256,
                    version_id: &snapshot.current.version_id,
                    receipt_sha256: &snapshot.receipt_sha256,
                    authority: snapshot.current.authority,
                    created_at_unix_seconds: snapshot.created_at_unix_seconds,
                    expires_at_unix_seconds: snapshot.expires_at_unix_seconds,
                    coverage: &snapshot.coverage,
                },
                session_instance_id,
            )?
            .as_ref()
                != Some(&session.current)
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        if !is_fresh_at(
            snapshot.created_at_unix_seconds,
            snapshot.expires_at_unix_seconds,
            now_unix_seconds,
        ) {
            return Err(CacheReconciliationError::Expired);
        }
        Ok(CacheLogicalReadResultV1 {
            logical_current,
            session,
            snapshot,
        })
    }

    pub(super) fn read_logical_current_unlocked(
        &self,
        identity: &CacheLogicalIdentityV1,
        identity_fingerprint: &str,
    ) -> Result<Option<CacheLogicalCurrentV1>> {
        let logical_dir = self.logical_dir(identity_fingerprint)?;
        let Some(bytes) = read_optional_bounded(
            &logical_dir.join(LOGICAL_CURRENT_FILE),
            MAX_LOGICAL_INDEX_BYTES as u64,
        )?
        else {
            return Ok(None);
        };
        let envelope: CacheLogicalCurrentEnvelopeV1 = serde_json::from_slice(&bytes)?;
        let payload_sha = hash_domain(
            LOGICAL_CURRENT_DOMAIN,
            &serde_json::to_vec(&envelope.payload)?,
        );
        if payload_sha != envelope.payload_sha256
            || envelope.payload.identity != *identity
            || envelope.payload.identity.fingerprint()? != identity_fingerprint
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        envelope.payload.validate()?;
        Ok(Some(envelope.payload))
    }

    fn write_logical_current_atomic(
        &self,
        identity_fingerprint: &str,
        payload: &CacheLogicalCurrentV1,
    ) -> Result<()> {
        payload.validate()?;
        if payload.identity.fingerprint()? != identity_fingerprint {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let logical_dir = self.logical_dir(identity_fingerprint)?;
        let envelope = CacheLogicalCurrentEnvelopeV1 {
            payload: payload.clone(),
            payload_sha256: hash_domain(LOGICAL_CURRENT_DOMAIN, &serde_json::to_vec(payload)?),
        };
        let bytes = serde_json::to_vec(&envelope)?;
        if bytes.len() > MAX_LOGICAL_INDEX_BYTES {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        let nonce = VERSION_NONCE.fetch_add(1, Ordering::Relaxed);
        let temporary = logical_dir.join(format!(".current-{}-{nonce}.tmp", payload.generation));
        write_new_synced(&temporary, &bytes)?;
        replace_plain_file(&temporary, &logical_dir.join(LOGICAL_CURRENT_FILE))
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        sync_directory(&logical_dir).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        let readback = read_required_bounded(
            &logical_dir.join(LOGICAL_CURRENT_FILE),
            MAX_LOGICAL_INDEX_BYTES as u64,
        )?;
        let readback: CacheLogicalCurrentEnvelopeV1 = serde_json::from_slice(&readback)?;
        if readback.payload != *payload
            || readback.payload_sha256
                != hash_domain(
                    LOGICAL_CURRENT_DOMAIN,
                    &serde_json::to_vec(&readback.payload)?,
                )
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn write_logical_current_for_test(
        &self,
        payload: &CacheLogicalCurrentV1,
    ) -> Result<()> {
        self.write_logical_current_atomic(&payload.identity.fingerprint()?, payload)
    }

    pub(super) fn commit_logical_session_updates_unlocked(
        &self,
        identity: &CacheLogicalIdentityV1,
        identity_fingerprint: &str,
        previous: Option<&CacheLogicalCurrentV1>,
        updates: Vec<(String, CacheLogicalSessionCurrentV1)>,
        now_unix_seconds: u64,
    ) -> Result<Option<CacheLogicalInvalidationV1>> {
        if updates.is_empty() {
            return Ok(None);
        }
        identity.validate()?;
        let mut next = previous.cloned().unwrap_or_else(|| CacheLogicalCurrentV1 {
            identity: identity.clone(),
            generation: 0,
            sessions: BTreeMap::new(),
            updated_at_unix_seconds: now_unix_seconds,
        });
        let mut changes = Vec::with_capacity(updates.len());
        for (session, entry) in updates {
            entry.validate(identity, &session)?;
            let invalidated_version = next
                .sessions
                .get(&session)
                .map(|previous| previous.current.clone());
            if invalidated_version.as_ref() == Some(&entry.current) {
                continue;
            }
            changes.push(CacheLogicalSessionChangeV1 {
                session_instance_id: session.clone(),
                invalidated_version,
                current_version: entry.current.clone(),
            });
            next.sessions.insert(session, entry);
        }
        if changes.is_empty() {
            return Ok(None);
        }
        if next.sessions.len() > MAX_LOGICAL_SESSIONS {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        let generation = previous
            .map(|current| current.generation.checked_add(1))
            .unwrap_or(Some(1))
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        next.generation = generation;
        next.updated_at_unix_seconds = now_unix_seconds;
        self.write_logical_current_atomic(identity_fingerprint, &next)?;
        Ok(Some(CacheLogicalInvalidationV1 {
            identity_fingerprint_sha256: identity_fingerprint.to_owned(),
            changes,
            generation,
            updated_at_unix_seconds: now_unix_seconds,
        }))
    }

    pub(super) fn move_logical_pointer_to_version(
        &self,
        target: &CacheReadResultV1,
        now_unix_seconds: u64,
        repair_damaged_pointer: bool,
    ) -> Result<Option<CacheLogicalInvalidationV1>> {
        if target.current.authority != CacheAuthorityV1::Authoritative {
            return Ok(None);
        }
        let identity = CacheLogicalIdentityV1::from_key(&target.key)?;
        let identity_fingerprint = identity.fingerprint()?;
        let _logical_lock = self.lock(&format!("logical-{identity_fingerprint}.lock"))?;
        let previous = if repair_damaged_pointer {
            self.read_logical_current_unlocked(&identity, &identity_fingerprint)
                .ok()
                .flatten()
        } else {
            self.read_logical_current_unlocked(&identity, &identity_fingerprint)?
        };
        let (version_receipt, _) = self.read_version_receipt(
            &target.current.key_fingerprint_sha256,
            &target.current.version_id,
        )?;
        if version_receipt.conflict_rejected {
            return Err(CacheReconciliationError::Conflict);
        }
        let policy_sha = version_receipt
            .reconciliation_policy_sha256
            .as_deref()
            .filter(|value| valid_sha256(value))
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
        let mut updates = Vec::new();
        let sessions = target
            .coverage
            .segments
            .iter()
            .map(|segment| segment.session_instance_id.clone())
            .collect::<BTreeSet<_>>();
        if sessions.len() > MAX_LOGICAL_SESSIONS {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        for session in sessions {
            let Some(reference) = version_ref_for_session(
                CacheLogicalVersionSourceV1 {
                    key: &target.key,
                    key_fingerprint_sha256: &target.current.key_fingerprint_sha256,
                    version_id: &target.current.version_id,
                    receipt_sha256: &target.receipt_sha256,
                    authority: target.current.authority,
                    created_at_unix_seconds: target.created_at_unix_seconds,
                    expires_at_unix_seconds: target.expires_at_unix_seconds,
                    coverage: &target.coverage,
                },
                &session,
            )?
            else {
                continue;
            };
            let comparison = version_receipt
                .logical_comparisons
                .iter()
                .find(|comparison| comparison.session_instance_id == session)
                .cloned();
            updates.push((
                session,
                CacheLogicalSessionCurrentV1 {
                    current: reference,
                    reconciliation_policy_sha256: policy_sha.to_owned(),
                    comparison,
                },
            ));
        }
        self.commit_logical_session_updates_unlocked(
            &identity,
            &identity_fingerprint,
            previous.as_ref(),
            updates,
            now_unix_seconds,
        )
    }
}
