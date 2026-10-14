//! Independently verifiable source-admission receipts.
//!
//! There is intentionally no production key, environment switch, or self-attested verifier in
//! this module. An embedding owner must inject a verifier backed by a separately reviewed
//! issuer/key allowlist. Without that verifier and the actual runtime evidence, admission remains
//! `BlockedExternal`; SDK dependency metadata and requested feed labels are never evidence.

use std::{collections::BTreeSet, fmt};

use market_contracts::{
    AdjustmentV1, AssetClassV1, BarResolutionV1, BarsRequestV1, EntitlementState, SessionScopeV1,
    UtcTimestamp,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub(crate) use super::authority_types::{
    ProviderAdmissionClaimsV1, ProviderAdmissionDatasetBindingV1, ProviderAdmissionEvidenceV1,
    ProviderAdmissionReceiptV1, ProviderPhaseSliceV1, ReconciliationPolicyV1,
    TrustedProviderAdmissionVerifierV1,
};
use super::{
    CacheKeyV1, CacheQuerySourceScopeV1, CacheReconciliationError, Result,
    SameFeedTolerancePolicyV1, enum_label, hex_digest, valid_dimension, valid_sha256,
};

const RECEIPT_DOMAIN: &[u8] = b"lqepoch.mdp.provider-admission-receipt.v1\0";
const POLICY_DOMAIN: &[u8] = b"lqepoch.mdp.reconciliation-policy.v1\0";
const MAX_ADMISSION_RECEIPT_BYTES: usize = 64 * 1024;
const MAX_PHASE_SLICES: usize = 4096;
const MAX_ADMISSION_TTL_SECONDS: u64 = 24 * 60 * 60;

impl ProviderPhaseSliceV1 {
    fn validate(&self) -> Result<()> {
        if !valid_phase(&self.phase) || !valid_dimension(&self.session_instance_id) {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        let start = parse_utc(&self.start_utc)?;
        let end = parse_utc(&self.end_exclusive_utc)?;
        if start >= end {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        Ok(())
    }
}

impl ProviderAdmissionDatasetBindingV1 {
    pub(crate) fn validate(&self) -> Result<()> {
        if [
            self.source_observation_sha256.as_str(),
            self.raw_response_facts_sha256.as_str(),
            self.canonical_response_facts_sha256.as_str(),
            self.canonical_rows_sha256.as_str(),
            self.coverage_evidence_sha256.as_str(),
            self.reconciliation_policy_sha256.as_str(),
        ]
        .into_iter()
        .any(|value| !valid_sha256(value))
            || !self.coverage_complete
            || self.has_gaps
            || self.truncated
        {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        Ok(())
    }
}

impl ReconciliationPolicyV1 {
    pub(crate) fn new(
        policy_id: impl Into<String>,
        policy_version: u32,
        same_feed: SameFeedTolerancePolicyV1,
        cross_provider: SameFeedTolerancePolicyV1,
    ) -> Self {
        Self {
            policy_id: policy_id.into(),
            policy_version,
            same_feed,
            cross_provider,
        }
    }

    pub(crate) fn legacy_same_feed(tolerance: SameFeedTolerancePolicyV1) -> Self {
        Self::new("caller-specified-v1", 1, tolerance.clone(), tolerance)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if !valid_dimension(&self.policy_id) || self.policy_version == 0 {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        self.same_feed.validate()?;
        self.cross_provider.validate()?;
        Ok(())
    }

    pub(crate) fn fingerprint(&self) -> Result<String> {
        self.validate()?;
        let encoded = serde_json::to_vec(self)?;
        Ok(hash_domain(POLICY_DOMAIN, &encoded))
    }
}

/// Opaque admission after receipt signature, runtime observations, route, rights, phase, and
/// request bindings have all been checked. Its fields and constructor remain private.
#[derive(Clone)]
pub(crate) struct VerifiedProviderAdmissionV1 {
    query_key: CacheKeyV1,
    source_scope: CacheQuerySourceScopeV1,
    receipt: ProviderAdmissionReceiptV1,
    receipt_sha256: String,
}

impl VerifiedProviderAdmissionV1 {
    /// Validate a capability for any page in the same frozen query plan. Capability receipts must
    /// be cursor-agnostic; this deliberately does not validate a cache key for stored data.
    pub(crate) fn validate_for_query(
        &self,
        request: &BarsRequestV1,
        source_scope: &CacheQuerySourceScopeV1,
        phase_slices: &[ProviderPhaseSliceV1],
        now_unix_seconds: u64,
    ) -> Result<()> {
        self.validate_live(now_unix_seconds)?;
        if self.receipt.claims.dataset_binding.is_some()
            || self.receipt.claims.request_cursor_sha256.is_some()
        {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        let mut cursorless = request.clone();
        cursorless.cursor = None;
        let expected_key = CacheKeyV1::from_request(&cursorless, source_scope)?;
        if self.query_key != expected_key
            || &self.source_scope != source_scope
            || self.receipt.claims.ordered_phase_slices != phase_slices
        {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        Ok(())
    }

    /// Validate a dataset-bound admission against the exact current provider page, including its
    /// cursor digest. Scope-only capability receipts cannot pass this method.
    pub(crate) fn validate_for_dataset_query(
        &self,
        request: &BarsRequestV1,
        source_scope: &CacheQuerySourceScopeV1,
        phase_slices: &[ProviderPhaseSliceV1],
        now_unix_seconds: u64,
    ) -> Result<()> {
        self.validate_live(now_unix_seconds)?;
        let expected_key = CacheKeyV1::from_request(request, source_scope)?;
        if self.receipt.claims.dataset_binding.is_none()
            || self.query_key != expected_key
            || &self.source_scope != source_scope
            || self.receipt.claims.ordered_phase_slices != phase_slices
            || self.receipt.claims.request_cursor_sha256 != expected_key.request_cursor_sha256
        {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        Ok(())
    }

    fn validate_live(&self, now_unix_seconds: u64) -> Result<()> {
        if self.receipt.issued_at_unix_seconds > now_unix_seconds
            || now_unix_seconds >= self.receipt.expires_at_unix_seconds
        {
            return Err(CacheReconciliationError::AdmissionExpired);
        }
        Ok(())
    }

    pub(crate) fn is_valid_at(&self, now_unix_seconds: u64) -> bool {
        self.receipt.issued_at_unix_seconds <= now_unix_seconds
            && now_unix_seconds < self.receipt.expires_at_unix_seconds
    }

    pub(crate) fn source_scope(&self) -> &CacheQuerySourceScopeV1 {
        &self.source_scope
    }

    pub(crate) fn provider(&self) -> &str {
        &self.receipt.claims.provider
    }

    pub(crate) fn feed(&self) -> &str {
        &self.receipt.claims.feed
    }

    pub(crate) fn entitlement(&self) -> EntitlementState {
        self.receipt.claims.entitlement
    }

    pub(crate) fn entitlement_scope_sha256(&self) -> &str {
        &self.receipt.claims.entitlement_scope_sha256
    }

    pub(crate) fn rights_profile_sha256(&self) -> &str {
        &self.receipt.claims.rights_profile_sha256
    }

    pub(crate) fn symbol(&self) -> &str {
        &self.receipt.claims.symbol
    }

    pub(crate) fn asset_class(&self) -> AssetClassV1 {
        self.receipt.claims.asset_class
    }

    pub(crate) fn resolution(&self) -> BarResolutionV1 {
        self.receipt.claims.effective_resolution
    }

    pub(crate) fn adjustment(&self) -> AdjustmentV1 {
        self.receipt.claims.adjustment
    }

    pub(crate) fn window_start_utc(&self) -> &str {
        &self.receipt.claims.window_start_utc
    }

    pub(crate) fn window_end_exclusive_utc(&self) -> &str {
        &self.receipt.claims.window_end_exclusive_utc
    }

    pub(crate) fn market_calendar_version(&self) -> Option<&str> {
        self.receipt.claims.market_calendar_version.as_deref()
    }

    pub(crate) fn numeric_encoding(&self) -> &str {
        &self.receipt.claims.numeric_encoding
    }

    pub(crate) fn requested_resolution(&self) -> BarResolutionV1 {
        self.receipt.claims.requested_resolution
    }

    pub(crate) fn session_scope(&self) -> SessionScopeV1 {
        self.receipt.claims.session_scope
    }

    pub(crate) fn session_instance_ids(&self) -> &[String] {
        &self.receipt.claims.session_instance_ids
    }

    pub(crate) fn schema_id(&self) -> &str {
        &self.receipt.claims.schema_id
    }

    pub(crate) fn schema_fingerprint_sha256(&self) -> &str {
        &self.receipt.claims.schema_fingerprint_sha256
    }

    pub(crate) fn license_scope_id(&self) -> &str {
        &self.receipt.claims.license_scope_id
    }

    pub(crate) fn license_terms_sha256(&self) -> &str {
        &self.receipt.claims.license_terms_sha256
    }

    pub(crate) fn ordered_phase_slices(&self) -> &[ProviderPhaseSliceV1] {
        &self.receipt.claims.ordered_phase_slices
    }

    pub(crate) fn authorization_context_sha256(&self) -> &str {
        &self.receipt.claims.authorization_context_sha256
    }

    pub(crate) fn sdk_name(&self) -> &str {
        &self.receipt.claims.sdk_name
    }

    pub(crate) fn sdk_version(&self) -> &str {
        &self.receipt.claims.sdk_version
    }

    pub(crate) fn gateway_version(&self) -> &str {
        &self.receipt.claims.gateway_version
    }

    pub(crate) fn issuer(&self) -> &str {
        &self.receipt.issuer
    }

    pub(crate) fn key_id(&self) -> &str {
        &self.receipt.key_id
    }

    pub(crate) fn receipt_sha256(&self) -> &str {
        &self.receipt_sha256
    }

    pub(crate) fn expires_at_unix_seconds(&self) -> u64 {
        self.receipt.expires_at_unix_seconds
    }

    pub(crate) fn issued_at_unix_seconds(&self) -> u64 {
        self.receipt.issued_at_unix_seconds
    }

    pub(crate) fn dataset_binding(&self) -> Option<&ProviderAdmissionDatasetBindingV1> {
        self.receipt.claims.dataset_binding.as_ref()
    }

    pub(crate) fn receipt(&self) -> &ProviderAdmissionReceiptV1 {
        &self.receipt
    }
}

impl fmt::Debug for VerifiedProviderAdmissionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedProviderAdmissionV1")
            .field("provider", &self.provider())
            .field("feed", &self.feed())
            .field("symbol", &self.symbol())
            .field("issuer", &self.issuer())
            .field("receipt_sha256", &self.receipt_sha256)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds())
            .field("dataset_bound", &self.dataset_binding().is_some())
            .finish()
    }
}

/// Verify an externally signed receipt. Scope-only receipts bind a cursorless query for provider
/// capability preflight; dataset receipts bind the exact current page and are separately checked
/// against cache hashes before publication.
pub(crate) fn verify_provider_admission_v1(
    request: &BarsRequestV1,
    source_scope: &CacheQuerySourceScopeV1,
    evidence: &ProviderAdmissionEvidenceV1,
    verifier: &dyn TrustedProviderAdmissionVerifierV1,
    phase_slices: &[ProviderPhaseSliceV1],
    now_unix_seconds: u64,
) -> Result<VerifiedProviderAdmissionV1> {
    let exact_key = CacheKeyV1::from_request(request, source_scope)?;
    if phase_slices.is_empty() || phase_slices.len() > MAX_PHASE_SLICES {
        return Err(CacheReconciliationError::BlockedExternal);
    }
    validate_phase_slices(
        phase_slices,
        &source_scope.session_instance_ids,
        request.start.as_str(),
        request.end.as_str(),
    )?;
    let receipt = evidence
        .signed_receipt
        .as_ref()
        .ok_or(CacheReconciliationError::BlockedExternal)?;
    if receipt.issuer.is_empty()
        || receipt.key_id.is_empty()
        || receipt.signature_base64.is_empty()
        || receipt.signature_base64.len() > MAX_ADMISSION_RECEIPT_BYTES
        || receipt.issued_at_unix_seconds > now_unix_seconds
        || now_unix_seconds >= receipt.expires_at_unix_seconds
        || receipt.expires_at_unix_seconds <= receipt.issued_at_unix_seconds
        || receipt.expires_at_unix_seconds - receipt.issued_at_unix_seconds
            > MAX_ADMISSION_TTL_SECONDS
    {
        return Err(CacheReconciliationError::AdmissionExpired);
    }
    validate_runtime_evidence(evidence, &receipt.claims)?;
    let dataset_bound = receipt.claims.dataset_binding.is_some();
    let (query_key, expected_cursor_sha256) = if dataset_bound {
        (exact_key.clone(), exact_key.request_cursor_sha256.clone())
    } else {
        let mut cursorless = request.clone();
        cursorless.cursor = None;
        (CacheKeyV1::from_request(&cursorless, source_scope)?, None)
    };
    validate_claims_for_query(
        &receipt.claims,
        request,
        source_scope,
        &query_key,
        expected_cursor_sha256,
        phase_slices,
    )?;
    if let Some(binding) = receipt.claims.dataset_binding.as_ref() {
        binding.validate()?;
    }
    let encoded_receipt = serde_json::to_vec(receipt)?;
    if encoded_receipt.len() > MAX_ADMISSION_RECEIPT_BYTES {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    let payload = signed_payload(receipt)?;
    verifier
        .verify_signature(
            &receipt.issuer,
            &receipt.key_id,
            &payload,
            &receipt.signature_base64,
        )
        .map_err(|_| CacheReconciliationError::BlockedExternal)?;
    Ok(VerifiedProviderAdmissionV1 {
        query_key,
        source_scope: source_scope.clone(),
        receipt: receipt.clone(),
        receipt_sha256: provider_admission_receipt_sha256(receipt)?,
    })
}

pub(super) fn provider_admission_receipt_sha256(
    receipt: &ProviderAdmissionReceiptV1,
) -> Result<String> {
    let encoded_receipt = serde_json::to_vec(receipt)?;
    if encoded_receipt.len() > MAX_ADMISSION_RECEIPT_BYTES {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    Ok(hash_domain(RECEIPT_DOMAIN, &encoded_receipt))
}

pub(super) fn validate_stored_receipt_binding(
    receipt: &ProviderAdmissionReceiptV1,
    key: &CacheKeyV1,
    dataset: &ProviderAdmissionDatasetBindingV1,
) -> Result<()> {
    let claims = &receipt.claims;
    if claims.provider != key.provider
        || claims.feed != key.feed
        || claims.entitlement != key.entitlement
        || claims.numeric_encoding != key.numeric_encoding
        || enum_label(&claims.adjustment)? != key.adjustment
        || enum_label(&claims.requested_resolution)? != key.requested_resolution
        || enum_label(&claims.effective_resolution)? != key.effective_resolution
        || claims.source_scope_id != key.source_scope_id
        || claims.entitlement_scope_sha256 != key.entitlement_scope_sha256
        || claims.rights_profile_sha256 != key.rights_profile_sha256
        || enum_label(&claims.asset_class)? != key.asset_class
        || claims.symbol != key.symbol
        || enum_label(&claims.session_scope)? != key.session_scope
        || claims.window_start_utc != key.window_start_utc
        || claims.window_end_exclusive_utc != key.window_end_exclusive_utc
        || claims.market_calendar_version != key.market_calendar_version
        || claims.request_cursor_sha256 != key.request_cursor_sha256
        || claims.schema_id != key.schema_id
        || claims.schema_fingerprint_sha256 != key.schema_fingerprint_sha256
        || claims.dataset_binding.as_ref() != Some(dataset)
        || claims.entitlement != EntitlementState::Authorized
        || !is_supported_historical_bars_source(&claims.provider, &claims.feed)
        || !valid_sha256(&claims.authorization_context_sha256)
        || !valid_dimension(&claims.sdk_name)
        || !valid_dimension(&claims.sdk_version)
        || !valid_dimension(&claims.gateway_version)
    {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    validate_claims_dimension_set(claims, key)?;
    dataset.validate()
}

/// Re-verify an immutable version's original signed admission at read time. The injected
/// verifier owns the current issuer/key allowlist and revocation policy; a valid cache receipt
/// checksum alone never grants current permission to serve authoritative data.
pub(super) fn verify_stored_provider_admission_v1(
    receipt: &ProviderAdmissionReceiptV1,
    key: &CacheKeyV1,
    now_unix_seconds: u64,
    verifier: &dyn TrustedProviderAdmissionVerifierV1,
) -> Result<()> {
    if receipt.issuer.is_empty()
        || receipt.key_id.is_empty()
        || receipt.signature_base64.is_empty()
        || receipt.signature_base64.len() > MAX_ADMISSION_RECEIPT_BYTES
        || receipt.issued_at_unix_seconds > now_unix_seconds
        || now_unix_seconds >= receipt.expires_at_unix_seconds
        || receipt.expires_at_unix_seconds <= receipt.issued_at_unix_seconds
        || receipt.expires_at_unix_seconds - receipt.issued_at_unix_seconds
            > MAX_ADMISSION_TTL_SECONDS
    {
        return Err(CacheReconciliationError::AdmissionExpired);
    }
    let dataset = receipt
        .claims
        .dataset_binding
        .as_ref()
        .ok_or(CacheReconciliationError::AdmissionMismatch)?;
    validate_stored_receipt_binding(receipt, key, dataset)?;
    let encoded = serde_json::to_vec(receipt)?;
    if encoded.len() > MAX_ADMISSION_RECEIPT_BYTES {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    let payload = signed_payload(receipt)?;
    verifier
        .verify_signature(
            &receipt.issuer,
            &receipt.key_id,
            &payload,
            &receipt.signature_base64,
        )
        .map_err(|_| CacheReconciliationError::BlockedExternal)
}

#[cfg(test)]
pub(super) fn signed_payload_for_test(receipt: &ProviderAdmissionReceiptV1) -> Result<Vec<u8>> {
    signed_payload(receipt)
}

fn validate_runtime_evidence(
    evidence: &ProviderAdmissionEvidenceV1,
    claims: &ProviderAdmissionClaimsV1,
) -> Result<()> {
    let observed_auth = evidence
        .authorization_context_sha256
        .as_deref()
        .filter(|value| valid_sha256(value))
        .ok_or(CacheReconciliationError::BlockedExternal)?;
    let sdk_name = evidence
        .sdk_name
        .as_deref()
        .filter(|value| valid_dimension(value))
        .ok_or(CacheReconciliationError::BlockedExternal)?;
    let sdk_version = evidence
        .sdk_version
        .as_deref()
        .filter(|value| valid_dimension(value))
        .ok_or(CacheReconciliationError::BlockedExternal)?;
    let gateway_version = evidence
        .gateway_version
        .as_deref()
        .filter(|value| valid_dimension(value))
        .ok_or(CacheReconciliationError::BlockedExternal)?;
    if observed_auth != claims.authorization_context_sha256
        || sdk_name != claims.sdk_name
        || sdk_version != claims.sdk_version
        || gateway_version != claims.gateway_version
        || evidence.ordered_phase_slices != claims.ordered_phase_slices
    {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    Ok(())
}

fn validate_claims_for_query(
    claims: &ProviderAdmissionClaimsV1,
    request: &BarsRequestV1,
    scope: &CacheQuerySourceScopeV1,
    key: &CacheKeyV1,
    expected_cursor_sha256: Option<String>,
    phases: &[ProviderPhaseSliceV1],
) -> Result<()> {
    if !is_supported_historical_bars_source(&scope.provider, &scope.feed)
        || scope.entitlement != EntitlementState::Authorized
        || claims.provider != scope.provider
        || claims.feed != scope.feed
        || claims.source_scope_id != scope.source_scope_id
        || claims.entitlement != EntitlementState::Authorized
        || claims.entitlement_scope_sha256 != scope.entitlement_scope_sha256
        || claims.rights_profile_sha256 != scope.rights_profile_sha256
        || claims.numeric_encoding != scope.numeric_encoding.as_str()
        || claims.symbol != request.symbol
        || claims.asset_class != request.asset_class
        || claims.requested_resolution != request.resolution
        || claims.effective_resolution != scope.effective_resolution
        || claims.adjustment != request.adjustment
        || claims.session_scope != request.session_scope
        || claims.session_instance_ids != scope.session_instance_ids
        || claims.window_start_utc != request.start.as_str()
        || claims.window_end_exclusive_utc != request.end.as_str()
        || claims.market_calendar_version != scope.market_calendar_version
        || claims.request_cursor_sha256 != expected_cursor_sha256
        || claims.schema_id != key.schema_id
        || claims.schema_fingerprint_sha256 != key.schema_fingerprint_sha256
        || claims.ordered_phase_slices != phases
        || !valid_dimension(&claims.license_scope_id)
        || !valid_sha256(&claims.license_terms_sha256)
    {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    validate_claims_dimension_set(claims, key)
}

fn validate_claims_dimension_set(
    claims: &ProviderAdmissionClaimsV1,
    key: &CacheKeyV1,
) -> Result<()> {
    if claims.provider != key.provider
        || claims.feed != key.feed
        || claims.entitlement_scope_sha256 != key.entitlement_scope_sha256
        || claims.rights_profile_sha256 != key.rights_profile_sha256
        || claims.symbol != key.symbol
        || enum_label(&claims.asset_class)? != key.asset_class
        || enum_label(&claims.requested_resolution)? != key.requested_resolution
        || enum_label(&claims.effective_resolution)? != key.effective_resolution
        || enum_label(&claims.adjustment)? != key.adjustment
        || enum_label(&claims.session_scope)? != key.session_scope
    {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    let sessions = claims
        .session_instance_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if sessions.len() != claims.session_instance_ids.len()
        || super::session_identity(&sessions)? != key.session_instance_id
    {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    Ok(())
}

fn validate_phase_slices(
    slices: &[ProviderPhaseSliceV1],
    session_ids: &[String],
    query_start_utc: &str,
    query_end_exclusive_utc: &str,
) -> Result<()> {
    let sessions = session_ids.iter().collect::<BTreeSet<_>>();
    let query_start = parse_utc(query_start_utc)?;
    let query_end = parse_utc(query_end_exclusive_utc)?;
    let mut prior_end: Option<UtcTimestamp> = None;
    for slice in slices {
        slice.validate()?;
        if !sessions.contains(&slice.session_instance_id) {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        let start = parse_utc(&slice.start_utc)?;
        let end = parse_utc(&slice.end_exclusive_utc)?;
        if start < query_start
            || end > query_end
            || prior_end.as_ref().is_some_and(|previous| &start < previous)
        {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        prior_end = Some(end);
    }
    Ok(())
}

fn signed_payload(receipt: &ProviderAdmissionReceiptV1) -> Result<Vec<u8>> {
    #[derive(Serialize)]
    struct Payload<'a> {
        version: u32,
        issuer: &'a str,
        key_id: &'a str,
        issued_at_unix_seconds: u64,
        expires_at_unix_seconds: u64,
        claims: &'a ProviderAdmissionClaimsV1,
    }
    serde_json::to_vec(&Payload {
        version: 1,
        issuer: &receipt.issuer,
        key_id: &receipt.key_id,
        issued_at_unix_seconds: receipt.issued_at_unix_seconds,
        expires_at_unix_seconds: receipt.expires_at_unix_seconds,
        claims: &receipt.claims,
    })
    .map_err(Into::into)
}

fn parse_utc(value: &str) -> Result<UtcTimestamp> {
    UtcTimestamp::parse_for_arrow_nanoseconds(value)
        .map_err(|_| CacheReconciliationError::AdmissionMismatch)
}

fn valid_phase(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_supported_historical_bars_source(provider: &str, feed: &str) -> bool {
    matches!(
        (provider, feed),
        ("alpaca", "sip" | "boats") | ("futu", "futu")
    )
}

fn hash_domain(domain: &[u8], payload: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(payload);
    hex_digest(digest.finalize().as_slice())
}
