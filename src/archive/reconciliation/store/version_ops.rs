use super::super::authority::TrustedProviderAdmissionVerifierV1;
use super::*;

impl CacheVersionStoreV1 {
    /// Atomically point current at an older still-fresh verified immutable version.
    pub(crate) fn rollback_to(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
    ) -> Result<CachePublishResultV1> {
        self.rollback_to_checked(key, target_version_id, now_unix_seconds, None)
    }

    /// Roll back to a signed-admission version only after the caller's current trusted issuer and
    /// revocation verifier accepts its retained receipt. A local Alpaca target requires its live
    /// same-process query lease through `rollback_to`.
    pub(crate) fn rollback_to_with_verifier(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
        verifier: &dyn TrustedProviderAdmissionVerifierV1,
    ) -> Result<CachePublishResultV1> {
        self.rollback_to_checked(key, target_version_id, now_unix_seconds, Some(verifier))
    }

    fn rollback_to_checked(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
        verifier: Option<&dyn TrustedProviderAdmissionVerifierV1>,
    ) -> Result<CachePublishResultV1> {
        ensure_worker_supported()?;
        key.validate()?;
        if !valid_sha256(target_version_id) {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let key_fingerprint = key.fingerprint()?;
        let _key_lock = self.lock(&format!("key-{key_fingerprint}.lock"))?;
        let _budget_lock = self.lock("budget.lock")?;
        let logical_identity = CacheLogicalIdentityV1::from_key(key)?;
        let logical_fingerprint = logical_identity.fingerprint()?;
        let _logical_lock = self.lock(&format!("logical-{logical_fingerprint}.lock"))?;
        let previous = self
            .read_current_unlocked(key, &key_fingerprint, now_unix_seconds, false)?
            .ok_or(CacheReconciliationError::NotFound)?;
        let previous_logical =
            self.read_logical_current_unlocked(&logical_identity, &logical_fingerprint)?;
        let target = self.read_version(key, &key_fingerprint, target_version_id)?;
        let (target_receipt, _) = self.read_version_receipt(&key_fingerprint, target_version_id)?;
        self.require_current_admission(&target, now_unix_seconds, verifier)?;
        if target_receipt.conflict_rejected {
            return Err(CacheReconciliationError::Conflict);
        }
        if !is_fresh_at(
            target.created_at_unix_seconds,
            target.expires_at_unix_seconds,
            now_unix_seconds,
        ) {
            return Err(CacheReconciliationError::Expired);
        }
        if previous.current.version_id == target_version_id {
            let (logical_next, logical_invalidation) = self
                .plan_logical_pointer_to_version_unlocked(
                    &target,
                    previous_logical.as_ref(),
                    now_unix_seconds,
                )?;
            if logical_next.is_some() {
                self.commit_pointer_transaction(
                    key,
                    Some(&previous.current),
                    logical_next.as_ref().or(previous_logical.as_ref()),
                    now_unix_seconds,
                    false,
                )?;
            }
            return Ok(CachePublishResultV1 {
                current: Some(previous.current),
                published_version_id: target_version_id.to_owned(),
                authority: target.current.authority,
                current_changed: false,
                invalidation: None,
                logical_invalidation,
                comparison: None,
                cross_provider_comparison: None,
                cross_provider_conflict: false,
                logical_index_overflow: target_receipt.logical_index_overflow,
                logical_index_status: target_receipt.logical_index_status,
            });
        }
        let generation = previous
            .current
            .generation
            .checked_add(1)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        let next = CacheCurrentV1 {
            key_fingerprint_sha256: key_fingerprint.clone(),
            version_id: target_version_id.to_owned(),
            receipt_sha256: target.receipt_sha256.clone(),
            generation,
            authority: target.current.authority,
            operation: CacheCurrentOperationV1::Rollback,
            previous_current_version_id: Some(previous.current.version_id.clone()),
            updated_at_unix_seconds: now_unix_seconds,
        };
        let (logical_next, logical_invalidation) = self.plan_logical_pointer_to_version_unlocked(
            &target,
            previous_logical.as_ref(),
            now_unix_seconds,
        )?;
        self.commit_pointer_transaction(
            key,
            Some(&next),
            logical_next.as_ref().or(previous_logical.as_ref()),
            now_unix_seconds,
            false,
        )?;
        Ok(CachePublishResultV1 {
            current: Some(next.clone()),
            published_version_id: target_version_id.to_owned(),
            authority: target.current.authority,
            current_changed: true,
            invalidation: Some(CacheInvalidationV1 {
                key_fingerprint_sha256: key_fingerprint,
                invalidated_version_id: previous.current.version_id,
                current_version_id: target_version_id.to_owned(),
                generation,
                reason: CacheCurrentOperationV1::Rollback,
            }),
            logical_invalidation,
            comparison: None,
            cross_provider_comparison: None,
            cross_provider_conflict: false,
            logical_index_overflow: target_receipt.logical_index_overflow,
            logical_index_status: target_receipt.logical_index_status,
        })
    }

    /// Repair a missing or damaged current manifest by explicitly selecting a still-fresh,
    /// immutable version. This never scans for or promotes an orphan automatically.
    pub(crate) fn recover_to(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
    ) -> Result<CachePublishResultV1> {
        self.recover_to_checked(key, target_version_id, now_unix_seconds, None)
    }

    /// Recover a signed-admission pointer only after the caller's current trusted issuer and
    /// revocation verifier accepts its retained receipt. A local Alpaca target requires its live
    /// same-process query lease through `recover_to`.
    pub(crate) fn recover_to_with_verifier(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
        verifier: &dyn TrustedProviderAdmissionVerifierV1,
    ) -> Result<CachePublishResultV1> {
        self.recover_to_checked(key, target_version_id, now_unix_seconds, Some(verifier))
    }

    fn recover_to_checked(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
        verifier: Option<&dyn TrustedProviderAdmissionVerifierV1>,
    ) -> Result<CachePublishResultV1> {
        ensure_worker_supported()?;
        key.validate()?;
        if !valid_sha256(target_version_id) {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let key_fingerprint = key.fingerprint()?;
        let _key_lock = self.lock(&format!("key-{key_fingerprint}.lock"))?;
        let _budget_lock = self.lock("budget.lock")?;
        let logical_identity = CacheLogicalIdentityV1::from_key(key)?;
        let logical_fingerprint = logical_identity.fingerprint()?;
        let _logical_lock = self.lock(&format!("logical-{logical_fingerprint}.lock"))?;
        let target = self.read_version(key, &key_fingerprint, target_version_id)?;
        let (target_receipt, _) = self.read_version_receipt(&key_fingerprint, target_version_id)?;
        self.require_current_admission(&target, now_unix_seconds, verifier)?;
        if target_receipt.conflict_rejected {
            return Err(CacheReconciliationError::Conflict);
        }
        if !is_fresh_at(
            target.created_at_unix_seconds,
            target.expires_at_unix_seconds,
            now_unix_seconds,
        ) {
            return Err(CacheReconciliationError::Expired);
        }
        let previous = self
            .read_current_unlocked(key, &key_fingerprint, now_unix_seconds, false)
            .ok()
            .flatten();
        let previous_logical = self
            .read_logical_current_unlocked(&logical_identity, &logical_fingerprint)
            .ok()
            .flatten();
        if let Some(current) = previous.as_ref()
            && current.current.version_id == target_version_id
        {
            let (logical_next, logical_invalidation) = self
                .plan_logical_pointer_to_version_unlocked(
                    &target,
                    previous_logical.as_ref(),
                    now_unix_seconds,
                )?;
            let generation = current
                .current
                .generation
                .checked_add(1)
                .ok_or(CacheReconciliationError::LimitExceeded)?;
            let next = CacheCurrentV1 {
                key_fingerprint_sha256: key_fingerprint,
                version_id: target_version_id.to_owned(),
                receipt_sha256: target.receipt_sha256.clone(),
                generation,
                authority: target.current.authority,
                operation: CacheCurrentOperationV1::Recovery,
                previous_current_version_id: Some(current.current.version_id.clone()),
                updated_at_unix_seconds: now_unix_seconds,
            };
            self.commit_pointer_transaction(
                key,
                Some(&next),
                logical_next.as_ref().or(previous_logical.as_ref()),
                now_unix_seconds,
                true,
            )?;
            return Ok(CachePublishResultV1 {
                current: Some(next),
                published_version_id: target_version_id.to_owned(),
                authority: target.current.authority,
                current_changed: false,
                invalidation: None,
                logical_invalidation,
                comparison: None,
                cross_provider_comparison: None,
                cross_provider_conflict: false,
                logical_index_overflow: target_receipt.logical_index_overflow,
                logical_index_status: target_receipt.logical_index_status,
            });
        }
        let generation = match previous.as_ref() {
            Some(current) => current
                .current
                .generation
                .checked_add(1)
                .ok_or(CacheReconciliationError::LimitExceeded)?,
            None => target
                .current
                .generation
                .checked_add(1)
                .ok_or(CacheReconciliationError::LimitExceeded)?,
        };
        let next = CacheCurrentV1 {
            key_fingerprint_sha256: key_fingerprint.clone(),
            version_id: target_version_id.to_owned(),
            receipt_sha256: target.receipt_sha256.clone(),
            generation,
            authority: target.current.authority,
            operation: CacheCurrentOperationV1::Recovery,
            previous_current_version_id: previous
                .as_ref()
                .map(|current| current.current.version_id.clone()),
            updated_at_unix_seconds: now_unix_seconds,
        };
        let (logical_next, logical_invalidation) = self.plan_logical_pointer_to_version_unlocked(
            &target,
            previous_logical.as_ref(),
            now_unix_seconds,
        )?;
        self.commit_pointer_transaction(
            key,
            Some(&next),
            logical_next.as_ref().or(previous_logical.as_ref()),
            now_unix_seconds,
            true,
        )?;
        let invalidation = previous.as_ref().map(|current| CacheInvalidationV1 {
            key_fingerprint_sha256: key_fingerprint,
            invalidated_version_id: current.current.version_id.clone(),
            current_version_id: target_version_id.to_owned(),
            generation,
            reason: CacheCurrentOperationV1::Recovery,
        });
        Ok(CachePublishResultV1 {
            current: Some(next),
            published_version_id: target_version_id.to_owned(),
            authority: target.current.authority,
            current_changed: true,
            invalidation,
            logical_invalidation,
            comparison: None,
            cross_provider_comparison: None,
            cross_provider_conflict: false,
            logical_index_overflow: target_receipt.logical_index_overflow,
            logical_index_status: target_receipt.logical_index_status,
        })
    }

    /// Enumerate fully verified immutable versions for an operator-directed rollback/recovery.
    /// Temporary staging directories left by process death are deliberately omitted; they are
    /// never promoted automatically.
    pub(crate) fn list_verified_versions(
        &self,
        key: &CacheKeyV1,
        now_unix_seconds: u64,
    ) -> Result<Vec<CacheVersionSummaryV1>> {
        ensure_worker_supported()?;
        key.validate()?;
        let key_fingerprint = key.fingerprint()?;
        let _key_lock = self.lock(&format!("key-{key_fingerprint}.lock"))?;
        let versions_dir = self.key_dir(&key_fingerprint)?.join("versions");
        if !versions_dir.exists() {
            return Ok(Vec::new());
        }
        require_plain_dir(&versions_dir)?;
        let mut summaries = Vec::new();
        for entry in fs::read_dir(&versions_dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() || !kind.is_dir() {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
            if name.starts_with(".cache-stage-") {
                continue;
            }
            if !valid_sha256(&name) {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            let version = self.read_version(key, &key_fingerprint, &name)?;
            let (receipt, _) = self.read_version_receipt(&key_fingerprint, &name)?;
            summaries.push(CacheVersionSummaryV1 {
                version_id: name,
                generation: receipt.generation,
                authority: receipt.authority,
                created_at_unix_seconds: receipt.created_at_unix_seconds,
                expires_at_unix_seconds: receipt.expires_at_unix_seconds,
                is_fresh: is_fresh_at(
                    receipt.created_at_unix_seconds,
                    receipt.expires_at_unix_seconds,
                    now_unix_seconds,
                ),
                raw_sha256: version.raw_sha256,
                canonical_sha256: version.canonical_sha256,
                canonical_rows_sha256: version.rows_sha256,
                bars_rows_sha256: version.bars_rows_sha256,
                canonical_rows_count: version.rows.len() as u64,
                coverage_complete: version.coverage.is_complete_for_key(key),
                conflict_rejected: receipt.conflict_rejected,
                logical_index_overflow: receipt.logical_index_overflow,
                logical_index_status: receipt.logical_index_status,
            });
        }
        summaries.sort_by(|left, right| {
            left.generation
                .cmp(&right.generation)
                .then_with(|| left.version_id.cmp(&right.version_id))
        });
        Ok(summaries)
    }
}
