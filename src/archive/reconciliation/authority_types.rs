//! Serializable, credential-free provider-admission claim and evidence types.

use std::fmt;

use market_contracts::{
    AdjustmentV1, AssetClassV1, BarResolutionV1, EntitlementState, SessionScopeV1,
};
use serde::{Deserialize, Serialize};

/// One ordered provider phase intersecting a query. The interval is half-open and the times are
/// canonical UTC RFC3339 strings with nanosecond precision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderPhaseSliceV1 {
    /// Stable snake_case phase label, such as `regular` or `overnight`.
    pub phase: String,
    pub session_instance_id: String,
    pub start_utc: String,
    pub end_exclusive_utc: String,
}

/// Dataset hashes attached to a signed admission receipt. This is a content binding, not a hash
/// of an HTTP frame. A dataset receipt is admissible only for complete, gap-free, non-truncated
/// coverage; scope-only receipts omit this structure and can grant no cache authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderAdmissionDatasetBindingV1 {
    pub source_observation_sha256: String,
    pub raw_response_facts_sha256: String,
    pub canonical_response_facts_sha256: String,
    pub canonical_rows_sha256: String,
    pub coverage_evidence_sha256: String,
    pub reconciliation_policy_sha256: String,
    pub coverage_complete: bool,
    pub has_gaps: bool,
    pub truncated: bool,
}

/// Claims signed by an independently trusted source-rights issuer. All runtime-sensitive values
/// are explicit. The dataset binding, when present, must match cache validation and readback.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderAdmissionClaimsV1 {
    pub provider: String,
    pub feed: String,
    pub source_scope_id: String,
    pub entitlement: EntitlementState,
    pub entitlement_scope_sha256: String,
    pub rights_profile_sha256: String,
    pub numeric_encoding: String,
    pub symbol: String,
    pub asset_class: AssetClassV1,
    pub requested_resolution: BarResolutionV1,
    pub effective_resolution: BarResolutionV1,
    pub adjustment: AdjustmentV1,
    pub session_scope: SessionScopeV1,
    pub session_instance_ids: Vec<String>,
    pub window_start_utc: String,
    pub window_end_exclusive_utc: String,
    pub market_calendar_version: Option<String>,
    /// A capability receipt has no request cursor digest; a dataset receipt binds the exact page.
    pub request_cursor_sha256: Option<String>,
    pub schema_id: String,
    pub schema_fingerprint_sha256: String,
    pub authorization_context_sha256: String,
    pub sdk_name: String,
    pub sdk_version: String,
    pub gateway_version: String,
    pub ordered_phase_slices: Vec<ProviderPhaseSliceV1>,
    pub license_scope_id: String,
    pub license_terms_sha256: String,
    pub dataset_binding: Option<ProviderAdmissionDatasetBindingV1>,
}

impl fmt::Debug for ProviderAdmissionClaimsV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderAdmissionClaimsV1")
            .field("provider", &self.provider)
            .field("feed", &self.feed)
            .field("entitlement", &self.entitlement)
            .field("symbol", &self.symbol)
            .field("asset_class", &self.asset_class)
            .field("requested_resolution", &self.requested_resolution)
            .field("window_start_utc", &self.window_start_utc)
            .field("window_end_exclusive_utc", &self.window_end_exclusive_utc)
            .field("rights_profile_sha256", &"[REDACTED]")
            .field("authorization_context_sha256", &"[REDACTED]")
            .field("ordered_phase_slices", &self.ordered_phase_slices.len())
            .field("has_dataset_binding", &self.dataset_binding.is_some())
            .finish()
    }
}

/// Signed admission receipt. Signature and claims are excluded from Debug output.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderAdmissionReceiptV1 {
    pub issuer: String,
    pub key_id: String,
    pub issued_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
    pub claims: ProviderAdmissionClaimsV1,
    pub signature_base64: String,
}

impl fmt::Debug for ProviderAdmissionReceiptV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderAdmissionReceiptV1")
            .field("issuer", &self.issuer)
            .field("key_id", &self.key_id)
            .field("issued_at_unix_seconds", &self.issued_at_unix_seconds)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field("claims", &self.claims)
            .field("signature_base64", &"[REDACTED]")
            .finish()
    }
}

/// Facts observed on the actual provider/Gateway path. `None` means unavailable and blocks
/// admission. The values are never filled from requested feed, dependency lockfiles, or env vars.
#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) struct ProviderAdmissionEvidenceV1 {
    pub authorization_context_sha256: Option<String>,
    pub sdk_name: Option<String>,
    pub sdk_version: Option<String>,
    pub gateway_version: Option<String>,
    pub ordered_phase_slices: Vec<ProviderPhaseSliceV1>,
    pub signed_receipt: Option<ProviderAdmissionReceiptV1>,
}

impl fmt::Debug for ProviderAdmissionEvidenceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderAdmissionEvidenceV1")
            .field(
                "authorization_context_sha256",
                &self
                    .authorization_context_sha256
                    .as_ref()
                    .map(|_| "[REDACTED]"),
            )
            .field("sdk_name", &self.sdk_name)
            .field("sdk_version", &self.sdk_version)
            .field("gateway_version", &self.gateway_version)
            .field("ordered_phase_slices", &self.ordered_phase_slices.len())
            .field(
                "signed_receipt",
                &self.signed_receipt.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Injected trust boundary. Implementations must verify against an independently reviewed
/// issuer/key set and enforce issuer revocation policy; accepting arbitrary receipt keys is unsafe.
pub(crate) trait TrustedProviderAdmissionVerifierV1: Send + Sync {
    fn verify_signature(
        &self,
        issuer: &str,
        key_id: &str,
        canonical_payload: &[u8],
        signature_base64: &str,
    ) -> std::result::Result<(), AdmissionVerifyErrorV1>;
}

/// Fail-closed verifier used until an independently reviewed issuer/key allowlist is injected.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct UnavailableProviderAdmissionVerifierV1;

impl TrustedProviderAdmissionVerifierV1 for UnavailableProviderAdmissionVerifierV1 {
    fn verify_signature(
        &self,
        _issuer: &str,
        _key_id: &str,
        _canonical_payload: &[u8],
        _signature_base64: &str,
    ) -> std::result::Result<(), AdmissionVerifyErrorV1> {
        Err(AdmissionVerifyErrorV1::Unavailable)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionVerifyErrorV1 {
    Unavailable,
    Rejected,
}

/// Versioned same-feed and cross-provider tolerances. The complete policy is persisted and its
/// fingerprint is included in data-bound signatures so a changed threshold cannot silently reuse
/// an older admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReconciliationPolicyV1 {
    pub policy_id: String,
    pub policy_version: u32,
    pub same_feed: super::SameFeedTolerancePolicyV1,
    pub cross_provider: super::SameFeedTolerancePolicyV1,
}
