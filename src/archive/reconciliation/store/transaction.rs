use std::{fs, path::Path};

use super::super::logical_index::{CacheLogicalCurrentV1, CacheLogicalIdentityV1};
use super::logical_shards::{
    PreparedLogicalRootV2, persist_logical_shards, planned_shard_bytes_to_add,
    prepare_logical_root, read_logical_current_from_envelope, write_logical_root,
};
use super::transaction_format::{
    CachePointerTransactionEnvelope, CachePointerTransactionMirror, CachePointerTransactionPayload,
    MAX_MIRROR_BYTES, MAX_TRANSACTION_BYTES, MIRROR_FILE, TRANSACTION_FILE, TRANSACTION_MODE_MAGIC,
    current_hash, current_manifest_bytes, ensure_transaction_mode, make_mirror, path_exists,
    read_current_payload, read_current_payload_hash, read_mirror, read_physical_logical_root,
    root_envelope_hash, transaction_hash, transaction_id_from_payload, transaction_mode_exists,
    validate_transaction, write_mirror,
};
use super::{
    CacheCurrentV1, CacheKeyV1, CacheReadResultV1, CacheReconciliationError, CacheVersionStoreV1,
    Result, fresh_temp_component, read_optional_bounded, replace_plain_file, sync_directory,
    write_new_synced,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CacheTransactionFaultV1 {
    BeforeCommitpoint = 1,
    AfterCommitpoint = 2,
    AfterSourceMirror = 3,
    AfterLogicalMirror = 4,
    AfterIdentityMarker = 5,
    AfterBaseCommitpoint = 6,
}

impl CacheVersionStoreV1 {
    /// Seal a previously committed transaction head in tests without publishing another data
    /// version. This keeps fault-injection sequences focused on pointer recovery rather than
    /// creating a same-price source refresh that may be logically equivalent to an older version.
    #[cfg(test)]
    pub(crate) fn settle_pointer_transaction_for_test(&self, key: &CacheKeyV1) -> Result<()> {
        key.validate()?;
        let source_key_fingerprint = key.fingerprint()?;
        let identity = CacheLogicalIdentityV1::from_key(key)?;
        let identity_fingerprint = identity.fingerprint()?;
        let _key_lock = self.lock(&format!("key-{source_key_fingerprint}.lock"))?;
        let _budget_lock = self.lock("budget.lock")?;
        let _logical_lock = self.lock(&format!("logical-{identity_fingerprint}.lock"))?;

        let envelope = self
            .read_pointer_transaction(&identity, &identity_fingerprint)?
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
        if envelope.payload.source_key_fingerprint_sha256 != source_key_fingerprint
            || &envelope.payload.source_key != key
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        self.ensure_transaction_materialized(&envelope)?;
        if !self.transaction_mirrors_are_sealed(&envelope)? {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(())
    }

    /// Atomically publish the source pointer and logical root through one identity-scoped head.
    /// The caller holds key, budget, and logical locks in that order.
    pub(super) fn commit_pointer_transaction(
        &self,
        key: &CacheKeyV1,
        source_current: Option<&CacheCurrentV1>,
        logical_current: Option<&CacheLogicalCurrentV1>,
        now_unix_seconds: u64,
        allow_damaged_source_recovery: bool,
    ) -> Result<()> {
        key.validate()?;
        let source_key_fingerprint = key.fingerprint()?;
        let identity = CacheLogicalIdentityV1::from_key(key)?;
        let logical_identity_fingerprint = identity.fingerprint()?;
        if let Some(current) = source_current {
            if current.key_fingerprint_sha256 != source_key_fingerprint {
                return Err(CacheReconciliationError::InvalidInput);
            }
            let verified = self.read_version(key, &source_key_fingerprint, &current.version_id)?;
            if current.receipt_sha256 != verified.receipt_sha256
                || current.authority != verified.current.authority
                || current.generation == 0
            {
                return Err(CacheReconciliationError::Readback);
            }
        }
        let prepared_logical = logical_current
            .map(|current| prepare_logical_root(&logical_identity_fingerprint, current))
            .transpose()?;
        let logical_root = prepared_logical
            .as_ref()
            .map(|prepared| prepared.envelope.clone());

        let previous_head =
            self.read_pointer_transaction(&identity, &logical_identity_fingerprint)?;
        if let Some(previous) = previous_head.as_ref() {
            self.ensure_transaction_materialized(previous)?;
        }

        let key_dir = self.key_dir(&source_key_fingerprint)?;
        let logical_dir = self.logical_dir(&logical_identity_fingerprint)?;
        sync_directory(
            key_dir
                .parent()
                .ok_or(CacheReconciliationError::UnknownOutcome)?,
        )?;
        sync_directory(
            logical_dir
                .parent()
                .ok_or(CacheReconciliationError::UnknownOutcome)?,
        )?;
        let mut recovery_replaced_damaged_source_manifest = false;
        let previous_source_current = if let Some(previous) = previous_head.as_ref()
            && previous.payload.source_key_fingerprint_sha256 == source_key_fingerprint
        {
            previous.payload.source_current.clone()
        } else {
            let current = if transaction_mode_exists(&key_dir)? {
                self.validate_source_mirror_identity(
                    &source_key_fingerprint,
                    &logical_identity_fingerprint,
                )?;
                let mirror = read_mirror(&key_dir)?;
                match read_current_payload(&key_dir) {
                    Ok(current)
                        if current.as_ref().map(current_hash).transpose()?
                            == mirror.source_current_sha256 =>
                    {
                        current
                    }
                    Err(CacheReconciliationError::UnknownOutcome) | Ok(_)
                        if allow_damaged_source_recovery =>
                    {
                        recovery_replaced_damaged_source_manifest = true;
                        None
                    }
                    Err(error) => return Err(error),
                    Ok(_) => return Err(CacheReconciliationError::UnknownOutcome),
                }
            } else {
                match read_current_payload(&key_dir) {
                    Ok(current) => current,
                    Err(CacheReconciliationError::UnknownOutcome)
                        if allow_damaged_source_recovery =>
                    {
                        recovery_replaced_damaged_source_manifest = true;
                        None
                    }
                    Err(error) => return Err(error),
                }
            };
            if let Some(current) = current.as_ref() {
                if current.key_fingerprint_sha256 != source_key_fingerprint {
                    return Err(CacheReconciliationError::UnknownOutcome);
                }
                let verified =
                    self.read_version(key, &source_key_fingerprint, &current.version_id)?;
                if current.receipt_sha256 != verified.receipt_sha256 {
                    return Err(CacheReconciliationError::UnknownOutcome);
                }
            }
            current
        };
        let previous_logical_root = if let Some(previous) = previous_head.as_ref() {
            previous.payload.logical_root.clone()
        } else {
            read_physical_logical_root(self, &identity, &logical_identity_fingerprint)?
        };

        let mut previous_transaction_sha256 = previous_head
            .as_ref()
            .map(|head| head.payload_sha256.clone());
        let initial_base_bytes = if previous_head.is_none() {
            let mut payload = CachePointerTransactionPayload {
                format_version: 2,
                transaction_id: String::new(),
                logical_identity_fingerprint_sha256: logical_identity_fingerprint.clone(),
                source_key: key.clone(),
                source_key_fingerprint_sha256: source_key_fingerprint.clone(),
                previous_source_current: previous_source_current.clone(),
                source_current: previous_source_current.clone(),
                previous_logical_root: previous_logical_root.clone(),
                logical_root: previous_logical_root.clone(),
                committed_at_unix_seconds: now_unix_seconds,
                legacy_operation_suffix: None,
                previous_transaction_sha256: None,
                recovery_replaced_damaged_source_manifest,
                initial_base: true,
            };
            payload.transaction_id = transaction_id_from_payload(&payload)?;
            let (base, bytes) =
                encode_transaction(payload, &identity, &logical_identity_fingerprint)?;
            previous_transaction_sha256 = Some(base.payload_sha256);
            Some(bytes)
        } else {
            None
        };

        let mut payload = CachePointerTransactionPayload {
            format_version: 2,
            transaction_id: String::new(),
            logical_identity_fingerprint_sha256: logical_identity_fingerprint.clone(),
            source_key: key.clone(),
            source_key_fingerprint_sha256: source_key_fingerprint.clone(),
            previous_source_current,
            source_current: source_current.cloned(),
            previous_logical_root,
            logical_root,
            committed_at_unix_seconds: now_unix_seconds,
            legacy_operation_suffix: None,
            previous_transaction_sha256,
            recovery_replaced_damaged_source_manifest,
            initial_base: false,
        };
        payload.transaction_id = transaction_id_from_payload(&payload)?;
        let (envelope, transaction_bytes) =
            encode_transaction(payload, &identity, &logical_identity_fingerprint)?;
        self.check_transaction_budget(
            &transaction_bytes,
            &envelope,
            prepared_logical.as_ref(),
            initial_base_bytes.as_deref(),
        )?;
        if let Some(prepared) = prepared_logical.as_ref() {
            persist_logical_shards(self, &logical_identity_fingerprint, prepared)?;
        }
        // On first use, install a checksummed BASE head that reproduces only the already verified
        // source/logical state. If the process stops before the identity marker or NEW head, reads
        // can still select this unchanged state and a retry can continue from it.
        if let Some(base_bytes) = initial_base_bytes.as_deref() {
            self.install_transaction_head_resolving(&logical_dir, base_bytes)?;
            self.inject_fault(CacheTransactionFaultV1::AfterBaseCommitpoint)?;
        }
        // Once a valid BASE or prior committed head exists, the marker can no longer hide an
        // uncommitted change. The NEW head below remains the only candidate-promotion point.
        ensure_transaction_mode(&logical_dir)?;
        self.inject_fault(CacheTransactionFaultV1::AfterIdentityMarker)?;
        self.install_transaction_head_resolving(&logical_dir, &transaction_bytes)?;
        self.inject_fault(CacheTransactionFaultV1::AfterCommitpoint)?;
        self.materialize_pointer_transaction(&envelope)
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        Ok(())
    }

    pub(super) fn read_transactional_current(
        &self,
        key: &CacheKeyV1,
        key_fingerprint: &str,
        now_unix_seconds: u64,
        require_fresh: bool,
    ) -> Result<Option<CacheReadResultV1>> {
        let identity = CacheLogicalIdentityV1::from_key(key)?;
        let identity_fingerprint = identity.fingerprint()?;
        let head = self.read_pointer_transaction(&identity, &identity_fingerprint)?;
        if let Some(head) = head {
            if head.payload.source_key_fingerprint_sha256 == key_fingerprint {
                if head.payload.source_key != *key {
                    return Err(CacheReconciliationError::UnknownOutcome);
                }
                return self.read_current_from_payload(
                    key,
                    key_fingerprint,
                    head.payload.source_current.as_ref(),
                    now_unix_seconds,
                    require_fresh,
                );
            }
            let key_dir = self.key_dir(key_fingerprint)?;
            if transaction_mode_exists(&key_dir)? || path_exists(&key_dir.join(MIRROR_FILE))? {
                self.validate_source_mirror(key, key_fingerprint, &identity_fingerprint)?;
            }
            return self.read_current_physical_unlocked(
                key,
                key_fingerprint,
                now_unix_seconds,
                require_fresh,
            );
        }
        let key_dir = self.key_dir(key_fingerprint)?;
        if transaction_mode_exists(&key_dir)? || path_exists(&key_dir.join(MIRROR_FILE))? {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        self.read_current_physical_unlocked(key, key_fingerprint, now_unix_seconds, require_fresh)
    }

    pub(super) fn read_transactional_logical_current(
        &self,
        identity: &CacheLogicalIdentityV1,
        identity_fingerprint: &str,
    ) -> Result<Option<CacheLogicalCurrentV1>> {
        let head = self.read_pointer_transaction(identity, identity_fingerprint)?;
        if let Some(head) = head {
            return match head.payload.logical_root {
                Some(root) => {
                    read_logical_current_from_envelope(self, identity, identity_fingerprint, &root)
                }
                None => Ok(None),
            };
        }
        super::logical_shards::read_logical_current(self, identity, identity_fingerprint)
    }

    fn read_current_from_payload(
        &self,
        key: &CacheKeyV1,
        key_fingerprint: &str,
        current: Option<&CacheCurrentV1>,
        now_unix_seconds: u64,
        require_fresh: bool,
    ) -> Result<Option<CacheReadResultV1>> {
        let Some(current) = current else {
            return Ok(None);
        };
        let mut result = self.read_version(key, key_fingerprint, &current.version_id)?;
        if current.key_fingerprint_sha256 != key_fingerprint
            || current.receipt_sha256 != result.receipt_sha256
            || current.authority != result.current.authority
            || current.generation < result.current.generation
            || result.conflict_rejected
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        result.current = current.clone();
        if require_fresh
            && !super::is_fresh_at(
                result.created_at_unix_seconds,
                result.expires_at_unix_seconds,
                now_unix_seconds,
            )
        {
            return Err(CacheReconciliationError::Expired);
        }
        if result.coverage.truncated
            && result
                .provider_cursor_receipt
                .as_ref()
                .is_none_or(|cursor| cursor.expires_at_unix_seconds <= now_unix_seconds)
        {
            return Err(CacheReconciliationError::Expired);
        }
        Ok(Some(result))
    }

    fn read_pointer_transaction(
        &self,
        identity: &CacheLogicalIdentityV1,
        identity_fingerprint: &str,
    ) -> Result<Option<CachePointerTransactionEnvelope>> {
        identity.validate()?;
        if identity.fingerprint()? != identity_fingerprint {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let logical_dir = self.logical_dir(identity_fingerprint)?;
        let logical_mode = transaction_mode_exists(&logical_dir)?;
        let path = logical_dir.join(TRANSACTION_FILE);
        let Some(bytes) = read_optional_bounded(&path, MAX_TRANSACTION_BYTES)? else {
            if logical_mode || path_exists(&logical_dir.join(MIRROR_FILE))? {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            return Ok(None);
        };
        let envelope: CachePointerTransactionEnvelope =
            serde_json::from_slice(&bytes).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        let expected_hash = transaction_hash(&envelope.payload)
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        let stored_source_key_fingerprint = envelope
            .payload
            .source_key
            .fingerprint()
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        if expected_hash != envelope.payload_sha256
            || envelope.payload.logical_identity_fingerprint_sha256 != identity_fingerprint
            || envelope.payload.source_key_fingerprint_sha256 != stored_source_key_fingerprint
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        validate_transaction(&envelope.payload, identity, identity_fingerprint)
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        if !logical_mode && !envelope.payload.initial_base {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        if let Some(root) = envelope.payload.logical_root.as_ref()
            && read_logical_current_from_envelope(self, identity, identity_fingerprint, root)
                .map_err(|_| CacheReconciliationError::UnknownOutcome)?
                .is_none()
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(Some(envelope))
    }

    fn materialize_pointer_transaction(
        &self,
        envelope: &CachePointerTransactionEnvelope,
    ) -> Result<()> {
        let payload = &envelope.payload;
        let identity = CacheLogicalIdentityV1::from_key(&payload.source_key)?;
        validate_transaction(
            payload,
            &identity,
            &payload.logical_identity_fingerprint_sha256,
        )?;
        let key_dir = self.key_dir(&payload.source_key_fingerprint_sha256)?;
        let logical_dir = self.logical_dir(&payload.logical_identity_fingerprint_sha256)?;
        // The head is already durable here, so introducing this marker cannot hide an
        // uncommitted legacy source current.
        ensure_transaction_mode(&key_dir)?;
        ensure_transaction_mode(&logical_dir)?;

        match payload.source_current.as_ref() {
            Some(current) => self.write_current_atomic(&key_dir, current)?,
            None => remove_if_present(&key_dir.join(super::CURRENT_FILE), &key_dir)?,
        }
        self.inject_fault(CacheTransactionFaultV1::AfterSourceMirror)?;

        match payload.logical_root.as_ref() {
            Some(root) => {
                write_logical_root(self, &payload.logical_identity_fingerprint_sha256, root)?
            }
            None => remove_if_present(&logical_dir.join("current.json"), &logical_dir)?,
        }
        self.inject_fault(CacheTransactionFaultV1::AfterLogicalMirror)?;

        let physical_current = read_current_payload(&key_dir)?;
        if physical_current.as_ref() != payload.source_current.as_ref() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        if let Some(current) = physical_current.as_ref() {
            let version = self.read_version(
                &payload.source_key,
                &payload.source_key_fingerprint_sha256,
                &current.version_id,
            )?;
            if current.receipt_sha256 != version.receipt_sha256
                || current.authority != version.current.authority
            {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
        }
        let physical_logical = super::logical_shards::read_logical_current(
            self,
            &identity,
            &payload.logical_identity_fingerprint_sha256,
        )?;
        let expected_logical = match payload.logical_root.as_ref() {
            Some(root) => read_logical_current_from_envelope(
                self,
                &identity,
                &payload.logical_identity_fingerprint_sha256,
                root,
            )?,
            None => None,
        };
        if physical_logical != expected_logical {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        sync_directory(&key_dir).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        sync_directory(&logical_dir).map_err(|_| CacheReconciliationError::UnknownOutcome)?;

        let mirror = make_mirror(envelope)?;
        write_mirror(&key_dir, &mirror)?;
        write_mirror(&logical_dir, &mirror)?;
        Ok(())
    }

    fn ensure_transaction_materialized(
        &self,
        envelope: &CachePointerTransactionEnvelope,
    ) -> Result<()> {
        if self.transaction_mirrors_are_sealed(envelope)? {
            return Ok(());
        }
        self.check_materialization_budget(envelope)?;
        self.materialize_pointer_transaction(envelope)
            .map_err(|_| CacheReconciliationError::UnknownOutcome)
    }

    fn transaction_mirrors_are_sealed(
        &self,
        envelope: &CachePointerTransactionEnvelope,
    ) -> Result<bool> {
        let payload = &envelope.payload;
        let identity = CacheLogicalIdentityV1::from_key(&payload.source_key)?;
        let key_dir = self.key_dir(&payload.source_key_fingerprint_sha256)?;
        let logical_dir = self.logical_dir(&payload.logical_identity_fingerprint_sha256)?;
        ensure_transaction_mode(&key_dir)?;
        ensure_transaction_mode(&logical_dir)?;
        let expected_mirror = make_mirror(envelope)?;
        let (Ok(key_mirror), Ok(logical_mirror)) =
            (read_mirror(&key_dir), read_mirror(&logical_dir))
        else {
            return Ok(false);
        };
        if key_mirror != expected_mirror || logical_mirror != expected_mirror {
            return Ok(false);
        }
        let current = match read_current_payload(&key_dir) {
            Ok(current) => current,
            Err(_) => return Ok(false),
        };
        if current.as_ref() != payload.source_current.as_ref() {
            return Ok(false);
        }
        if let Some(current) = current.as_ref() {
            let version = match self.read_version(
                &payload.source_key,
                &payload.source_key_fingerprint_sha256,
                &current.version_id,
            ) {
                Ok(version) => version,
                Err(_) => return Ok(false),
            };
            if current.receipt_sha256 != version.receipt_sha256
                || current.authority != version.current.authority
            {
                return Ok(false);
            }
        }
        let logical_root = match read_physical_logical_root(
            self,
            &identity,
            &payload.logical_identity_fingerprint_sha256,
        ) {
            Ok(root) => root,
            Err(_) => return Ok(false),
        };
        if logical_root.as_ref().map(root_envelope_hash).transpose()?
            != payload
                .logical_root
                .as_ref()
                .map(root_envelope_hash)
                .transpose()?
        {
            return Ok(false);
        }
        sync_directory(&key_dir).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        sync_directory(&logical_dir).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        Ok(true)
    }

    fn validate_source_mirror_identity(
        &self,
        key_fingerprint: &str,
        identity_fingerprint: &str,
    ) -> Result<CachePointerTransactionMirror> {
        let key_dir = self.key_dir(key_fingerprint)?;
        let mirror = read_mirror(&key_dir)?;
        if !mirror.sealed
            || mirror.logical_identity_fingerprint_sha256 != identity_fingerprint
            || mirror.source_key_fingerprint_sha256 != key_fingerprint
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(mirror)
    }

    fn validate_source_mirror(
        &self,
        key: &CacheKeyV1,
        key_fingerprint: &str,
        identity_fingerprint: &str,
    ) -> Result<()> {
        let key_dir = self.key_dir(key_fingerprint)?;
        let mirror = self.validate_source_mirror_identity(key_fingerprint, identity_fingerprint)?;
        let current = read_current_payload_hash(&key_dir)?;
        if current != mirror.source_current_sha256 {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        if let Some(current) = read_current_payload(&key_dir)? {
            if current.key_fingerprint_sha256 != key_fingerprint {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            let version = self.read_version(key, key_fingerprint, &current.version_id)?;
            if version.receipt_sha256 != current.receipt_sha256 {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
        }
        Ok(())
    }

    fn install_transaction_head(&self, logical_dir: &Path, bytes: &[u8]) -> Result<()> {
        self.inject_fault(CacheTransactionFaultV1::BeforeCommitpoint)?;
        let temporary = logical_dir.join(format!(".transaction-{}.tmp", fresh_temp_component()?));
        write_new_synced(&temporary, bytes)?;
        if replace_plain_file(&temporary, &logical_dir.join(TRANSACTION_FILE)).is_err() {
            match fs::symlink_metadata(&temporary) {
                Ok(metadata) if metadata.file_type().is_file() => {
                    fs::remove_file(&temporary)?;
                    sync_directory(logical_dir)?;
                }
                Ok(_) => return Err(CacheReconciliationError::UnknownOutcome),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        sync_directory(logical_dir).map_err(|_| CacheReconciliationError::UnknownOutcome)
    }

    fn install_transaction_head_resolving(&self, logical_dir: &Path, bytes: &[u8]) -> Result<()> {
        if let Err(error) = self.install_transaction_head(logical_dir, bytes) {
            let committed = match read_optional_bounded(
                &logical_dir.join(TRANSACTION_FILE),
                MAX_TRANSACTION_BYTES,
            ) {
                Ok(Some(current)) => current == bytes,
                Ok(None) => false,
                Err(_) => return Err(CacheReconciliationError::UnknownOutcome),
            };
            if committed {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            return Err(error);
        }
        Ok(())
    }

    fn check_transaction_budget(
        &self,
        next: &[u8],
        envelope: &CachePointerTransactionEnvelope,
        prepared_logical: Option<&PreparedLogicalRootV2>,
        initial_base: Option<&[u8]>,
    ) -> Result<()> {
        let mut additional =
            u64::try_from(next.len()).map_err(|_| CacheReconciliationError::LimitExceeded)?;
        if let Some(base) = initial_base {
            additional = additional
                .checked_add(
                    u64::try_from(base.len())
                        .map_err(|_| CacheReconciliationError::LimitExceeded)?,
                )
                .ok_or(CacheReconciliationError::LimitExceeded)?;
        }
        additional = additional
            .checked_add(self.materialization_reserve(envelope)?)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        if let Some(prepared) = prepared_logical {
            additional = additional
                .checked_add(planned_shard_bytes_to_add(
                    self,
                    &envelope.payload.logical_identity_fingerprint_sha256,
                    prepared,
                )?)
                .ok_or(CacheReconciliationError::LimitExceeded)?;
        }
        self.check_additional_budget(additional)
    }

    fn check_materialization_budget(
        &self,
        envelope: &CachePointerTransactionEnvelope,
    ) -> Result<()> {
        self.check_additional_budget(self.materialization_reserve(envelope)?)
    }

    fn materialization_reserve(&self, envelope: &CachePointerTransactionEnvelope) -> Result<u64> {
        let source_current_bytes = envelope
            .payload
            .source_current
            .as_ref()
            .map(current_manifest_bytes)
            .transpose()?
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0);
        let logical_root_bytes = envelope
            .payload
            .logical_root
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()?
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0);
        let mirror_bytes = u64::try_from(serde_json::to_vec(&make_mirror(envelope)?)?.len())
            .map_err(|_| CacheReconciliationError::LimitExceeded)?;
        if mirror_bytes > MAX_MIRROR_BYTES {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        let markers = u64::try_from(TRANSACTION_MODE_MAGIC.len())
            .map_err(|_| CacheReconciliationError::LimitExceeded)?
            .checked_mul(2)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        source_current_bytes
            .checked_add(logical_root_bytes)
            .and_then(|value| value.checked_add(mirror_bytes.checked_mul(2)?))
            .and_then(|value| value.checked_add(markers))
            .ok_or(CacheReconciliationError::LimitExceeded)
    }

    fn check_additional_budget(&self, additional: u64) -> Result<()> {
        let total = super::tree_bytes_bounded(&self.root, self.limits.max_total_bytes)?;
        let projected = total
            .checked_add(additional)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        if projected > self.limits.max_total_bytes {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_transaction_fault_for_test(&self, fault: CacheTransactionFaultV1) {
        self.transaction_fault
            .store(fault as u8, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn inject_fault(&self, fault: CacheTransactionFaultV1) -> Result<()> {
        if self
            .transaction_fault
            .compare_exchange(
                fault as u8,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(())
    }

    #[cfg(not(test))]
    fn inject_fault(&self, _fault: CacheTransactionFaultV1) -> Result<()> {
        Ok(())
    }
}

fn encode_transaction(
    payload: CachePointerTransactionPayload,
    identity: &CacheLogicalIdentityV1,
    identity_fingerprint: &str,
) -> Result<(CachePointerTransactionEnvelope, Vec<u8>)> {
    if payload.format_version != 2 || payload.legacy_operation_suffix.is_some() {
        return Err(CacheReconciliationError::InvalidInput);
    }
    validate_transaction(&payload, identity, identity_fingerprint)?;
    let envelope = CachePointerTransactionEnvelope {
        payload_sha256: transaction_hash(&payload)?,
        payload,
    };
    let bytes = serde_json::to_vec(&envelope)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_TRANSACTION_BYTES {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    Ok((envelope, bytes))
}

fn remove_if_present(path: &Path, directory: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(path)?;
            sync_directory(directory).map_err(|_| CacheReconciliationError::UnknownOutcome)
        }
        Ok(_) => Err(CacheReconciliationError::UnknownOutcome),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
