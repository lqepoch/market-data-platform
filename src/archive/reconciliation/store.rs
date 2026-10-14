use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use fs2::FileExt;
use market_contracts::{
    BarRowV1, BarsRequestV1, BarsResponseV1, EntitlementState, NumericEncodingV1,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cancellation::CancellationToken;
use crate::parquet_store::ParquetVerification;

use super::authority::{
    ProviderAdmissionDatasetBindingV1, ProviderAdmissionReceiptV1, ProviderPhaseSliceV1,
    ReconciliationPolicyV1, VerifiedProviderAdmissionV1,
};
mod logical;

use super::logical_index::{
    CacheLogicalComparisonKindV1, CacheLogicalComparisonV1, CacheLogicalIdentityV1,
    CacheLogicalInvalidationV1, CacheLogicalSessionCurrentV1, CacheLogicalVersionSourceV1,
    LogicalSessionSnapshotRefV1, MAX_LOGICAL_SESSIONS, compare_session_batch,
    session_coverage_bounds, version_ref_for_session,
};
use super::{
    BarsCacheEvidenceV1, CacheKeyV1, CacheQuerySourceScopeV1, CacheReconciliationError,
    CanonicalRowV1, CoverageEvidenceV1, ProviderCursorCacheInputV1, ProviderPageEvidenceV1, Result,
    SameFeedComparisonV1, SameFeedTolerancePolicyV1, ValidatedProviderCursorReceiptV1,
    canonical_rows_from_bars, canonical_rows_sha256, compare_same_feed_authoritative, hex_digest,
    normalize_and_deduplicate_rows, normalize_bars_response, source_evidence_sha256,
    valid_dimension, valid_sha256, validate_canonical_rows_linked_to_raw, validate_coverage_rows,
};

const ROOT_DIR_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
const ROOT_OWNERSHIP_MARKER_FILE: &str = ".mdp-cache-owner";
const ROOT_OWNERSHIP_MARKER: &[u8] = b"lqepoch.mdp.cache-root.v1\n";
const MAX_TREE_ENTRIES: usize = 100_000;
const RECEIPT_DOMAIN: &[u8] = b"lqepoch.mdp.cache-version-receipt.v6\0";
const LEGACY_RECEIPT_DOMAIN_V5: &[u8] = b"lqepoch.mdp.cache-version-receipt.v5\0";
const LEGACY_RECEIPT_DOMAIN_V4: &[u8] = b"lqepoch.mdp.cache-version-receipt.v4\0";
const CURRENT_DOMAIN: &[u8] = b"lqepoch.mdp.cache-current-manifest.v1\0";
const REJECTION_DOMAIN: &[u8] = b"lqepoch.mdp.cache-rejected-candidate.v1\0";
const BARS_ROWS_DOMAIN: &[u8] = b"lqepoch.mdp.cache-bars-v1-rows.v1\0";
static VERSION_NONCE: AtomicU64 = AtomicU64::new(1);

const CURRENT_FILE: &str = "current.json";
const RAW_FILE: &str = "raw.parquet";
const CANONICAL_FILE: &str = "canonical.parquet";
const ROWS_FILE: &str = "canonical-rows.json";
const RECEIPT_FILE: &str = "receipt.json";

/// Bounds for local immutable cache versions. Expired versions are retained; this store never
/// performs automatic eviction.
#[derive(Clone, Debug)]
pub(crate) struct CacheLimitsV1 {
    pub max_object_bytes: u64,
    pub max_metadata_bytes: u64,
    pub max_total_bytes: u64,
    pub max_versions_per_key: usize,
    pub max_rows: usize,
    pub ttl: Duration,
}

impl Default for CacheLimitsV1 {
    fn default() -> Self {
        Self {
            max_object_bytes: 512 * 1024 * 1024,
            max_metadata_bytes: 128 * 1024 * 1024,
            max_total_bytes: 8 * 1024 * 1024 * 1024,
            max_versions_per_key: 256,
            max_rows: 1000,
            ttl: Duration::from_secs(15 * 60),
        }
    }
}

impl CacheLimitsV1 {
    fn validate(&self) -> Result<()> {
        if self.max_object_bytes == 0
            || self.max_metadata_bytes == 0
            || self.max_total_bytes == 0
            || self.max_versions_per_key == 0
            || self.max_rows == 0
            || self.max_rows > 1000
            || self.ttl.is_zero()
            || self.ttl.subsec_nanos() != 0
            || self.max_object_bytes > self.max_total_bytes
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        Ok(())
    }
}

/// One private staging Parquet object and the schema identifier owned by Core's trusted registry.
#[derive(Clone, Debug)]
struct CacheArtifactInputV1 {
    pub path: PathBuf,
    pub schema_id: String,
}

impl CacheArtifactInputV1 {
    fn new(path: impl Into<PathBuf>, schema_id: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            schema_id: schema_id.into(),
        }
    }
}

/// Candidate data mapped from frozen Core rows and adapter coverage evidence.
#[derive(Clone, Debug)]
pub(crate) struct CacheSnapshotV1 {
    pub key: CacheKeyV1,
    pub coverage: CoverageEvidenceV1,
    pub raw_response: BarsResponseV1,
    pub response: BarsResponseV1,
    provider_cursor_cache_input: Option<ProviderCursorCacheInputV1>,
    raw_bars_evidence: BarsCacheEvidenceV1,
    bars_evidence: BarsCacheEvidenceV1,
}

/// Inputs shared by cache publication paths. Grouping the immutable query identity and the two
/// separately validated row sets keeps callers from accidentally swapping raw/canonical data.
pub(crate) struct CacheBarsResponsesInputV1<'a> {
    request: &'a BarsRequestV1,
    source_scope: &'a CacheQuerySourceScopeV1,
    raw_observations: BarsResponseV1,
    canonical_response: BarsResponseV1,
    page_evidence: ProviderPageEvidenceV1,
}

impl<'a> CacheBarsResponsesInputV1<'a> {
    pub(crate) fn new(
        request: &'a BarsRequestV1,
        source_scope: &'a CacheQuerySourceScopeV1,
        raw_observations: BarsResponseV1,
        canonical_response: BarsResponseV1,
        page_evidence: ProviderPageEvidenceV1,
    ) -> Self {
        Self {
            request,
            source_scope,
            raw_observations,
            canonical_response,
            page_evidence,
        }
    }
}

#[derive(Clone, Copy)]
struct CachePublishContextV1<'a> {
    now_unix_seconds: u64,
    policy: &'a ReconciliationPolicyV1,
    authority_basis: CacheAuthorityBasisV1,
    admission: Option<&'a VerifiedProviderAdmissionV1>,
    phase_slices: Option<&'a [ProviderPhaseSliceV1]>,
}

struct CacheSnapshotPublicationV1<'a> {
    request: &'a BarsRequestV1,
    source_scope: &'a CacheQuerySourceScopeV1,
    snapshot: CacheSnapshotV1,
    raw_input: &'a CacheArtifactInputV1,
    canonical_input: &'a CacheArtifactInputV1,
    context: CachePublishContextV1<'a>,
}

struct CacheVersionIdentityV1<'a> {
    key_fingerprint: &'a str,
    generation: u64,
    now_unix_seconds: u64,
    raw_sha: &'a str,
    canonical_sha: &'a str,
    rows_sha: &'a str,
    raw_bars_rows_sha: &'a str,
    bars_rows_sha: &'a str,
    raw_source_evidence_sha: &'a str,
    source_evidence_sha: &'a str,
    coverage_evidence_sha: &'a str,
    reconciliation_policy_sha: &'a str,
    admission_receipt_sha: &'a str,
}

impl CacheSnapshotV1 {
    pub(crate) fn from_bars_response(
        request: &BarsRequestV1,
        source_scope: &CacheQuerySourceScopeV1,
        response: BarsResponseV1,
        page_evidence: ProviderPageEvidenceV1,
    ) -> Result<Self> {
        Self::from_bars_responses(
            request,
            source_scope,
            response.clone(),
            response,
            page_evidence,
        )
    }

    pub(crate) fn from_bars_responses(
        request: &BarsRequestV1,
        source_scope: &CacheQuerySourceScopeV1,
        raw_response: BarsResponseV1,
        canonical_response: BarsResponseV1,
        page_evidence: ProviderPageEvidenceV1,
    ) -> Result<Self> {
        raw_response
            .validate()
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        request
            .validate_response(&canonical_response)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        if raw_response.requested_resolution != request.resolution
            || raw_response.adjustment != request.adjustment
            || raw_response.effective_resolution != source_scope.effective_resolution
            || raw_response.rows.len() > usize::from(request.limit)
            || raw_response.rows.iter().any(|row| {
                row.symbol != request.symbol
                    || row.asset_class != request.asset_class
                    || row.adjustment != request.adjustment
                    || row.resolution != source_scope.effective_resolution
            })
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let provider_cursor_cache_input = page_evidence.provider_cursor_cache_input.clone();
        let mut response = normalize_bars_response(&canonical_response)?;
        if raw_response.rows.is_empty() || response.rows.is_empty() {
            return Err(CacheReconciliationError::InvalidInput);
        }
        validate_canonical_rows_linked_to_raw(&raw_response.rows, &response.rows)?;
        let key = CacheKeyV1::from_request(request, source_scope)?;
        let mut coverage = CoverageEvidenceV1::from_bars_responses(
            source_scope,
            &raw_response,
            &response,
            &page_evidence,
        )?;
        if !coverage.is_complete_for_key(&key) {
            downgrade_complete_coverage(&mut response);
            for segment in &mut coverage.segments {
                if segment.coverage_status == market_contracts::BarsCoverageStatusV1::Complete {
                    segment.coverage_status = market_contracts::BarsCoverageStatusV1::Partial;
                }
            }
        }
        coverage.validate_shape(&key)?;
        validate_coverage_rows(&coverage, &raw_response.rows, &response.rows)?;
        let raw_evidence = BarsCacheEvidenceV1::from_response(request, &raw_response)?;
        raw_evidence.validate_raw_against(&key, &raw_response.rows)?;
        let evidence = BarsCacheEvidenceV1::from_response(request, &response)?;
        evidence.validate_against(&key, &coverage, &response.rows)?;
        Ok(Self {
            key,
            coverage,
            raw_response,
            response,
            provider_cursor_cache_input,
            raw_bars_evidence: raw_evidence,
            bars_evidence: evidence,
        })
    }

    fn rows_for_comparison(&self) -> Result<Vec<CanonicalRowV1>> {
        normalize_and_deduplicate_rows(canonical_rows_from_bars(&self.response.rows)?)
    }

    fn admission_dataset_binding(
        &self,
        policy: &ReconciliationPolicyV1,
    ) -> Result<ProviderAdmissionDatasetBindingV1> {
        policy.validate()?;
        let source_observation_sha256 = self
            .coverage
            .segments
            .first()
            .map(|segment| segment.source_observation_sha256.clone())
            .ok_or(CacheReconciliationError::Incomplete)?;
        if self
            .coverage
            .segments
            .iter()
            .any(|segment| segment.source_observation_sha256 != source_observation_sha256)
        {
            return Err(CacheReconciliationError::Conflict);
        }
        let raw_rows_sha256 = bars_rows_sha256(&self.raw_response.rows)?;
        let canonical_bar_rows_sha256 = bars_rows_sha256(&self.response.rows)?;
        let raw_response_facts =
            serde_json::to_vec(&(&self.raw_bars_evidence, raw_rows_sha256.as_str()))?;
        let canonical_response_facts =
            serde_json::to_vec(&(&self.bars_evidence, canonical_bar_rows_sha256.as_str()))?;
        let canonical_rows_sha256 = canonical_rows_sha256(&self.rows_for_comparison()?)?;
        let coverage_evidence_sha256 = coverage_evidence_sha256(&self.coverage)?;
        let coverage_complete = self.coverage.is_complete_for_key(&self.key);
        let has_gaps = self.coverage.coverage_gap_count != Some(0);
        let binding = ProviderAdmissionDatasetBindingV1 {
            source_observation_sha256,
            raw_response_facts_sha256: hash_domain(
                b"lqepoch.mdp.provider-admission.raw-response-facts.v1\0",
                &raw_response_facts,
            ),
            canonical_response_facts_sha256: hash_domain(
                b"lqepoch.mdp.provider-admission.canonical-response-facts.v1\0",
                &canonical_response_facts,
            ),
            canonical_rows_sha256,
            coverage_evidence_sha256,
            reconciliation_policy_sha256: policy.fingerprint()?,
            coverage_complete,
            has_gaps,
            truncated: self.coverage.truncated,
        };
        Ok(binding)
    }
}

/// Produce the exact dataset binding that an independent issuer must sign. This hashes the
/// credential-free SDK observation response, mapped response, Core rows, coverage, and complete
/// reconciliation policy; it is not an HTTP-body hash and does not attest to rights itself.
pub(crate) fn provider_admission_dataset_binding_for_bars_responses(
    request: &BarsRequestV1,
    source_scope: &CacheQuerySourceScopeV1,
    raw_observations: &BarsResponseV1,
    canonical_response: &BarsResponseV1,
    page_evidence: &ProviderPageEvidenceV1,
    policy: &ReconciliationPolicyV1,
) -> Result<ProviderAdmissionDatasetBindingV1> {
    let snapshot = CacheSnapshotV1::from_bars_responses(
        request,
        source_scope,
        raw_observations.clone(),
        canonical_response.clone(),
        page_evidence.clone(),
    )?;
    snapshot.admission_dataset_binding(policy)
}

fn downgrade_complete_coverage(response: &mut BarsResponseV1) {
    if response.coverage_status == market_contracts::BarsCoverageStatusV1::Complete {
        response.coverage_status = market_contracts::BarsCoverageStatusV1::Partial;
    }
    for segment in &mut response.segments {
        if segment.coverage_status == market_contracts::BarsCoverageStatusV1::Complete {
            segment.coverage_status = market_contracts::BarsCoverageStatusV1::Partial;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CacheAuthorityV1 {
    Authoritative,
    Provisional,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheCurrentV1 {
    pub key_fingerprint_sha256: String,
    pub version_id: String,
    pub receipt_sha256: String,
    pub generation: u64,
    pub authority: CacheAuthorityV1,
    pub operation: CacheCurrentOperationV1,
    pub previous_current_version_id: Option<String>,
    pub updated_at_unix_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CacheCurrentOperationV1 {
    Publish,
    Rollback,
    Recovery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CacheInvalidationV1 {
    pub key_fingerprint_sha256: String,
    pub invalidated_version_id: String,
    pub current_version_id: String,
    pub generation: u64,
    pub reason: CacheCurrentOperationV1,
}

#[derive(Clone, Debug)]
pub(crate) struct CachePublishResultV1 {
    pub current: Option<CacheCurrentV1>,
    pub published_version_id: String,
    pub authority: CacheAuthorityV1,
    pub current_changed: bool,
    pub invalidation: Option<CacheInvalidationV1>,
    pub logical_invalidation: Option<CacheLogicalInvalidationV1>,
    pub comparison: Option<SameFeedComparisonV1>,
    pub cross_provider_comparison: Option<SameFeedComparisonV1>,
    pub cross_provider_conflict: bool,
    pub logical_index_overflow: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct CacheReadResultV1 {
    pub current: CacheCurrentV1,
    pub conflict_rejected: bool,
    pub key: CacheKeyV1,
    pub coverage: CoverageEvidenceV1,
    pub rows: Vec<CanonicalRowV1>,
    pub raw_bars_rows: Vec<BarRowV1>,
    pub bars_rows: Vec<BarRowV1>,
    pub raw_bars_rows_sha256: String,
    pub bars_rows_sha256: String,
    pub raw_source_evidence_sha256: String,
    pub source_evidence_sha256: String,
    pub coverage_evidence_sha256: String,
    pub raw_bars_evidence: BarsCacheEvidenceV1,
    pub bars_evidence: BarsCacheEvidenceV1,
    pub provider_cursor_receipt: Option<ValidatedProviderCursorReceiptV1>,
    pub raw_parquet_path: PathBuf,
    pub canonical_parquet_path: PathBuf,
    pub raw_sha256: String,
    pub canonical_sha256: String,
    pub rows_sha256: String,
    pub receipt_sha256: String,
    pub created_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CacheVersionSummaryV1 {
    pub version_id: String,
    pub generation: u64,
    pub authority: CacheAuthorityV1,
    pub created_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
    pub is_fresh: bool,
    pub raw_sha256: String,
    pub canonical_sha256: String,
    pub canonical_rows_sha256: String,
    pub bars_rows_sha256: String,
    pub canonical_rows_count: u64,
    pub coverage_complete: bool,
    pub conflict_rejected: bool,
    pub logical_index_overflow: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheCurrentEnvelopeV1 {
    payload: CacheCurrentV1,
    payload_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheVersionReceiptV1 {
    receipt_version: u32,
    key: CacheKeyV1,
    key_fingerprint_sha256: String,
    version_id: String,
    generation: u64,
    previous_version_id: Option<String>,
    authority: CacheAuthorityV1,
    authority_basis: CacheAuthorityBasisV1,
    created_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    raw: ArtifactReceiptV1,
    canonical: ArtifactReceiptV1,
    canonical_rows_sha256: String,
    canonical_rows_count: u64,
    raw_bars_rows_count: u64,
    raw_bars_rows_sha256: String,
    bars_rows_sha256: String,
    raw_source_evidence_sha256: String,
    source_evidence_sha256: String,
    coverage_evidence_sha256: String,
    coverage: CoverageEvidenceV1,
    provider_cursor_receipt: Option<ValidatedProviderCursorReceiptV1>,
    raw_bars_evidence: BarsCacheEvidenceV1,
    bars_evidence: BarsCacheEvidenceV1,
    same_feed_comparison: Option<SameFeedComparisonV1>,
    #[serde(default)]
    reconciliation_policy: Option<ReconciliationPolicyV1>,
    #[serde(default)]
    reconciliation_policy_sha256: Option<String>,
    #[serde(default)]
    provider_admission_receipt: Option<ProviderAdmissionReceiptV1>,
    #[serde(default)]
    provider_admission_receipt_sha256: Option<String>,
    #[serde(default)]
    cross_provider_comparison: Option<SameFeedComparisonV1>,
    #[serde(default)]
    cross_provider_conflict: bool,
    #[serde(default)]
    conflict_rejected: bool,
    #[serde(default)]
    logical_conflict_sessions: Vec<String>,
    #[serde(default)]
    logical_comparisons: Vec<CacheLogicalComparisonV1>,
    #[serde(default)]
    logical_index_overflow: bool,
}

/// Production publication lacks a trusted provider admission proof and therefore remains
/// provisional. The synthetic basis exists only in tests to exercise authoritative rollback and
/// conflict protection without presenting fixture evidence as provider authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CacheAuthorityBasisV1 {
    ProviderAdmissionUnavailable,
    ProviderAdmissionVerified,
    #[cfg(test)]
    SyntheticVerified,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactReceiptV1 {
    sha256: String,
    size_bytes: u64,
    schema_id: String,
    schema_fingerprint_sha256: String,
    parquet_rows: u64,
    source_provider: String,
    source_feed: String,
    source_entitlement: EntitlementState,
    numeric_encoding: NumericEncodingV1,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct RejectedCandidateV1<'a> {
    version: u32,
    key_fingerprint_sha256: &'a str,
    candidate_canonical_sha256: &'a str,
    previous_current_version_id: &'a str,
    rejected_at_unix_seconds: u64,
    reason: &'a str,
    payload_sha256: String,
}

/// Local mutable index over immutable, content-verified Parquet versions.
///
/// The store has no provider or remote-archive writer. Candidate Parquet is copied into a private
/// version directory, verified using the CLI's bounded Linux worker, hashed, read back and only
/// then made current with one atomic manifest rename. On non-Linux platforms the store fails
/// closed because the existing resource-limited worker is unsupported there.
pub(crate) struct CacheVersionStoreV1 {
    root: PathBuf,
    limits: CacheLimitsV1,
}

impl CacheVersionStoreV1 {
    pub(crate) fn new(root: impl Into<PathBuf>, limits: CacheLimitsV1) -> Result<Self> {
        limits.validate()?;
        let root = root.into();
        initialize_owned_cache_root(&root)?;
        let root = fs::canonicalize(root)?;
        create_private_dir(&root.join("keys"))?;
        create_private_dir(&root.join("locks"))?;
        create_private_dir(&root.join("logical"))?;
        Ok(Self { root, limits })
    }

    /// Persist SDK-normalized provider observations and the adapter's canonical response as a
    /// private raw/canonical Parquet pair, then validate both through the bounded worker before
    /// immutable version publication. This input is not an HTTP raw-frame capture.
    pub(crate) fn publish_bars_responses(
        &self,
        input: CacheBarsResponsesInputV1<'_>,
        now_unix_seconds: u64,
        tolerance: &SameFeedTolerancePolicyV1,
    ) -> Result<CachePublishResultV1> {
        let policy = ReconciliationPolicyV1::legacy_same_feed(tolerance.clone());
        self.publish_bars_responses_with_basis(
            input,
            CachePublishContextV1 {
                now_unix_seconds,
                policy: &policy,
                authority_basis: CacheAuthorityBasisV1::ProviderAdmissionUnavailable,
                admission: None,
                phase_slices: None,
            },
        )
    }

    /// Publish provisional raw/canonical versions with an explicit, persisted same-feed and
    /// cross-provider tolerance policy. This does not grant authority or alter the verifier gate.
    pub(crate) fn publish_bars_responses_with_policy(
        &self,
        input: CacheBarsResponsesInputV1<'_>,
        now_unix_seconds: u64,
        policy: &ReconciliationPolicyV1,
    ) -> Result<CachePublishResultV1> {
        policy.validate()?;
        self.publish_bars_responses_with_basis(
            input,
            CachePublishContextV1 {
                now_unix_seconds,
                policy,
                authority_basis: CacheAuthorityBasisV1::ProviderAdmissionUnavailable,
                admission: None,
                phase_slices: None,
            },
        )
    }

    /// Publish an authority-eligible Alpaca historical-bars response only after an independent
    /// verifier has checked the signed receipt and its exact raw/canonical/data-policy bindings.
    /// Scope-only receipts and provisional Futu/unknown-feed evidence cannot use this path.
    pub(crate) fn publish_bars_responses_with_admission(
        &self,
        input: CacheBarsResponsesInputV1<'_>,
        now_unix_seconds: u64,
        policy: &ReconciliationPolicyV1,
        phase_slices: &[ProviderPhaseSliceV1],
        admission: &VerifiedProviderAdmissionV1,
    ) -> Result<CachePublishResultV1> {
        if input.source_scope.provider != "alpaca"
            || !matches!(input.source_scope.feed.as_str(), "sip" | "boats")
            || input.source_scope.entitlement != EntitlementState::Authorized
            || matches!(input.source_scope.feed.as_str(), "opra" | "unknown")
        {
            return Err(CacheReconciliationError::BlockedExternal);
        }
        admission.validate_for_dataset_query(
            input.request,
            input.source_scope,
            phase_slices,
            now_unix_seconds,
        )?;
        policy.validate()?;
        self.publish_bars_responses_with_basis(
            input,
            CachePublishContextV1 {
                now_unix_seconds,
                policy,
                authority_basis: CacheAuthorityBasisV1::ProviderAdmissionVerified,
                admission: Some(admission),
                phase_slices: Some(phase_slices),
            },
        )
    }

    /// Synchronous supervisor-friendly wrapper. Cancellation before the operation prevents work;
    /// once the bounded atomic publication begins it is allowed to finish and all workers are
    /// joined. If cancellation arrives meanwhile, the caller gets `UnknownOutcome` and must
    /// reload the current manifest rather than retrying blindly.
    pub(crate) fn publish_bars_responses_cancellable(
        &self,
        input: CacheBarsResponsesInputV1<'_>,
        now_unix_seconds: u64,
        tolerance: &SameFeedTolerancePolicyV1,
        cancellation: &CancellationToken,
    ) -> Result<CachePublishResultV1> {
        if cancellation.is_cancelled() {
            return Err(CacheReconciliationError::Cancelled);
        }
        let result = self.publish_bars_responses(input, now_unix_seconds, tolerance)?;
        if cancellation.is_cancelled() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn publish_synthetic_verified_bars_responses(
        &self,
        input: CacheBarsResponsesInputV1<'_>,
        now_unix_seconds: u64,
        tolerance: &SameFeedTolerancePolicyV1,
    ) -> Result<CachePublishResultV1> {
        let policy = ReconciliationPolicyV1::legacy_same_feed(tolerance.clone());
        self.publish_bars_responses_with_basis(
            input,
            CachePublishContextV1 {
                now_unix_seconds,
                policy: &policy,
                authority_basis: CacheAuthorityBasisV1::SyntheticVerified,
                admission: None,
                phase_slices: None,
            },
        )
    }

    fn publish_bars_responses_with_basis(
        &self,
        input: CacheBarsResponsesInputV1<'_>,
        context: CachePublishContextV1<'_>,
    ) -> Result<CachePublishResultV1> {
        let CacheBarsResponsesInputV1 {
            request,
            source_scope,
            raw_observations,
            canonical_response,
            page_evidence,
        } = input;
        let authority_basis = context.authority_basis;
        let admission = context.admission;
        if (authority_basis == CacheAuthorityBasisV1::ProviderAdmissionVerified)
            != admission.is_some()
        {
            return Err(CacheReconciliationError::BlockedExternal);
        }
        ensure_worker_supported()?;
        let snapshot = CacheSnapshotV1::from_bars_responses(
            request,
            source_scope,
            raw_observations,
            canonical_response,
            page_evidence,
        )?;
        let staging = CacheStageDir::create(&self.root.join("locks"))?;
        let raw_path = staging.path().join("sdk-observations.parquet");
        let canonical_path = staging.path().join("canonical.parquet");
        crate::parquet_store::write_market_bars_v1_object_with_limit(
            &raw_path,
            &snapshot.raw_response,
            self.limits.max_object_bytes,
        )
        .map_err(|_| CacheReconciliationError::Readback)?;
        crate::parquet_store::write_market_bars_v1_object_with_limit(
            &canonical_path,
            &snapshot.response,
            self.limits.max_object_bytes,
        )
        .map_err(|_| CacheReconciliationError::Readback)?;
        let raw_input = CacheArtifactInputV1::new(&raw_path, snapshot.key.schema_id.clone());
        let canonical_input =
            CacheArtifactInputV1::new(&canonical_path, snapshot.key.schema_id.clone());
        self.publish_snapshot(CacheSnapshotPublicationV1 {
            request,
            source_scope,
            snapshot,
            raw_input: &raw_input,
            canonical_input: &canonical_input,
            context,
        })
    }

    /// Store a raw/canonical pair and exact normalized comparison rows. Incomplete coverage and
    /// sources without a trusted provider-admission proof remain provisional immutable versions.
    /// They never replace an existing authoritative current version.
    fn publish_snapshot(
        &self,
        publication: CacheSnapshotPublicationV1<'_>,
    ) -> Result<CachePublishResultV1> {
        let CacheSnapshotPublicationV1 {
            request,
            source_scope,
            snapshot,
            raw_input,
            canonical_input,
            context,
        } = publication;
        let CachePublishContextV1 {
            now_unix_seconds,
            policy,
            authority_basis,
            admission,
            phase_slices,
        } = context;
        ensure_worker_supported()?;
        snapshot.key.validate()?;
        snapshot.coverage.validate_shape(&snapshot.key)?;
        policy.validate()?;
        if raw_input.schema_id != snapshot.key.schema_id
            || canonical_input.schema_id != snapshot.key.schema_id
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let raw_bars_rows = snapshot.raw_response.rows.clone();
        let raw_bars_evidence = snapshot.raw_bars_evidence.clone();
        let bars_rows = snapshot.response.rows.clone();
        let bars_evidence = snapshot.bars_evidence.clone();
        raw_bars_evidence.validate_raw_against(&snapshot.key, &raw_bars_rows)?;
        bars_evidence.validate_against(&snapshot.key, &snapshot.coverage, &bars_rows)?;
        validate_canonical_rows_linked_to_raw(&raw_bars_rows, &bars_rows)?;
        let provider_cursor_receipt = match snapshot.provider_cursor_cache_input.clone() {
            Some(input) if snapshot.coverage.truncated => {
                Some(ValidatedProviderCursorReceiptV1::bind_to_key(
                    input,
                    &snapshot.key,
                    now_unix_seconds,
                )?)
            }
            Some(_) => return Err(CacheReconciliationError::InvalidInput),
            None => None,
        };
        let rows = snapshot.rows_for_comparison()?;
        if rows.len() > self.limits.max_rows {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        let raw_bars_rows_sha = bars_rows_sha256(&raw_bars_rows)?;
        let bars_rows_sha = bars_rows_sha256(&bars_rows)?;
        let rows_bytes = serde_json::to_vec(&rows)?;
        if rows_bytes.len() as u64 > self.limits.max_metadata_bytes {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        let rows_sha = canonical_rows_sha256(&rows)?;
        let raw_source_evidence_sha = source_evidence_sha256(&raw_bars_evidence)?;
        let source_evidence_sha = source_evidence_sha256(&bars_evidence)?;
        let coverage_evidence_sha = coverage_evidence_sha256(&snapshot.coverage)?;
        let policy_sha = policy.fingerprint()?;
        let key_fingerprint = snapshot.key.fingerprint()?;
        let dataset_binding = snapshot.admission_dataset_binding(policy)?;
        let complete = snapshot.coverage.is_complete_for_key(&snapshot.key);
        if authority_basis == CacheAuthorityBasisV1::ProviderAdmissionVerified {
            let admission = admission.ok_or(CacheReconciliationError::BlockedExternal)?;
            let phases = phase_slices.ok_or(CacheReconciliationError::BlockedExternal)?;
            admission.validate_for_dataset_query(
                request,
                source_scope,
                phases,
                now_unix_seconds,
            )?;
            if !complete
                || snapshot.coverage.coverage_gap_count != Some(0)
                || snapshot.coverage.truncated
            {
                return Err(CacheReconciliationError::AdmissionMismatch);
            }
        }
        let coverage = snapshot.coverage.clone();
        let (_, expected_canonical_rows) = coverage_row_counts(&coverage)?;

        let key_dir = self.key_dir(&key_fingerprint)?;
        create_private_dir(&key_dir.join("versions"))?;
        create_private_dir(&key_dir.join("audit"))?;
        let _key_lock = self.lock(&format!("key-{key_fingerprint}.lock"))?;
        let _budget_lock = self.lock("budget.lock")?;
        let logical_identity = CacheLogicalIdentityV1::from_key(&snapshot.key)?;
        let logical_fingerprint = logical_identity.fingerprint()?;
        self.logical_dir(&logical_fingerprint)?;
        let _logical_lock = self.lock(&format!("logical-{logical_fingerprint}.lock"))?;

        let existing =
            self.read_current_unlocked(&snapshot.key, &key_fingerprint, now_unix_seconds, false)?;
        let existing_logical =
            self.read_logical_current_unlocked(&logical_identity, &logical_fingerprint)?;
        let candidate_authority =
            derive_response_authority(authority_basis, &snapshot.key, complete, &bars_rows);
        let previous_current = existing.as_ref().map(|read| read.current.clone());
        let previous_receipt = existing
            .as_ref()
            .map(|read| read.current.version_id.clone());

        let baseline_bytes = tree_bytes_bounded(&self.root, self.limits.max_total_bytes)?;
        let version_count = directory_entry_count(&key_dir.join("versions"))?;
        if version_count >= self.limits.max_versions_per_key {
            return Err(CacheReconciliationError::LimitExceeded);
        }

        let stage = CacheStageDir::create(&key_dir.join("versions"))?;
        let raw_path = stage.path().join(RAW_FILE);
        let canonical_path = stage.path().join(CANONICAL_FILE);
        let raw_size =
            copy_bounded_nofollow(&raw_input.path, &raw_path, self.limits.max_object_bytes)?;
        let canonical_size = copy_bounded_nofollow(
            &canonical_input.path,
            &canonical_path,
            self.limits.max_object_bytes,
        )?;
        let (raw_verified, raw_sha) = verify_staged_artifact(
            &raw_path,
            &raw_input.schema_id,
            self.limits.max_object_bytes,
            raw_size,
        )?;
        let (canonical_verified, canonical_sha) = verify_staged_artifact(
            &canonical_path,
            &canonical_input.schema_id,
            self.limits.max_object_bytes,
            canonical_size,
        )?;
        validate_source_facts(&snapshot.key, &raw_verified)?;
        validate_source_facts(&snapshot.key, &canonical_verified)?;
        if raw_verified.schema_sha256 != snapshot.key.schema_fingerprint_sha256
            || canonical_verified.schema_sha256 != snapshot.key.schema_fingerprint_sha256
            || canonical_verified.footer_rows != rows.len() as u64
            || canonical_verified.decoded_rows != rows.len() as u64
            || canonical_verified.footer_rows != expected_canonical_rows
            || raw_verified.footer_rows != raw_bars_rows.len() as u64
            || raw_verified.decoded_rows != raw_verified.footer_rows
        {
            return Err(CacheReconciliationError::Readback);
        }
        let raw_readback = read_market_bars_v1(&raw_path, &self.limits)?;
        let canonical_readback = read_market_bars_v1(&canonical_path, &self.limits)?;
        if raw_readback != raw_bars_rows
            || canonical_readback != bars_rows
            || normalize_and_deduplicate_rows(canonical_rows_from_bars(&canonical_readback)?)?
                != rows
            || validate_canonical_rows_linked_to_raw(&raw_readback, &canonical_readback).is_err()
        {
            return Err(CacheReconciliationError::Readback);
        }
        if raw_size == 0 || canonical_size == 0 {
            return Err(CacheReconciliationError::InvalidInput);
        }

        if authority_basis == CacheAuthorityBasisV1::ProviderAdmissionVerified {
            let admission = admission.ok_or(CacheReconciliationError::BlockedExternal)?;
            let expected_receipt = admission.receipt();
            let expected_binding = admission
                .dataset_binding()
                .ok_or(CacheReconciliationError::BlockedExternal)?;
            if expected_binding != &dataset_binding
                || super::authority::provider_admission_receipt_sha256(expected_receipt)?
                    != admission.receipt_sha256()
            {
                return Err(CacheReconciliationError::AdmissionMismatch);
            }
            super::authority::validate_stored_receipt_binding(
                expected_receipt,
                &snapshot.key,
                expected_binding,
            )?;
        }

        let next_generation = match previous_current.as_ref() {
            Some(current) => current
                .generation
                .checked_add(1)
                .ok_or(CacheReconciliationError::LimitExceeded)?,
            None => 1,
        };
        let version_id = version_id(CacheVersionIdentityV1 {
            key_fingerprint: &key_fingerprint,
            generation: next_generation,
            now_unix_seconds,
            raw_sha: &raw_sha,
            canonical_sha: &canonical_sha,
            rows_sha: &rows_sha,
            raw_bars_rows_sha: &raw_bars_rows_sha,
            bars_rows_sha: &bars_rows_sha,
            raw_source_evidence_sha: &raw_source_evidence_sha,
            source_evidence_sha: &source_evidence_sha,
            coverage_evidence_sha: &coverage_evidence_sha,
            reconciliation_policy_sha: &policy_sha,
            admission_receipt_sha: admission
                .map(VerifiedProviderAdmissionV1::receipt_sha256)
                .unwrap_or(""),
        });
        let expires_at = now_unix_seconds
            .checked_add(self.limits.ttl.as_secs())
            .ok_or(CacheReconciliationError::LimitExceeded)?;

        let comparison = if candidate_authority == CacheAuthorityV1::Authoritative {
            match existing
                .as_ref()
                .filter(|read| read.current.authority == CacheAuthorityV1::Authoritative)
            {
                Some(previous) => {
                    match compare_same_feed_authoritative(&previous.rows, &rows, &policy.same_feed)
                    {
                        Ok(summary) => Some(summary),
                        Err(CacheReconciliationError::Conflict) => None,
                        Err(error) => return Err(error),
                    }
                }
                None => None,
            }
        } else {
            None
        };
        let same_feed_conflict = candidate_authority == CacheAuthorityV1::Authoritative
            && existing.as_ref().is_some_and(|previous| {
                previous.current.authority == CacheAuthorityV1::Authoritative
                    && comparison.is_none()
            });

        let candidate_sessions = snapshot
            .coverage
            .segments
            .iter()
            .map(|segment| segment.session_instance_id.clone())
            .collect::<BTreeSet<_>>();
        let mut logical_index_overflow = candidate_sessions.len() > MAX_LOGICAL_SESSIONS;
        let mut logical_comparisons: Vec<CacheLogicalComparisonV1> = Vec::new();
        let mut logical_conflict_sessions;
        let mut logical_session_updates = BTreeMap::new();
        let mut rejected_baseline_version = previous_current
            .as_ref()
            .map(|value| value.version_id.clone());
        let mut old_snapshots = Vec::new();
        for session in &candidate_sessions {
            if session_coverage_bounds(&snapshot.coverage, session)?.is_none() {
                continue;
            }
            if let Some(previous_session) = existing_logical
                .as_ref()
                .and_then(|manifest| manifest.sessions.get(session))
            {
                let old_reference = &previous_session.current;
                let old = self.read_version(
                    &old_reference.key,
                    &old_reference.key_fingerprint_sha256,
                    &old_reference.version_id,
                )?;
                rejected_baseline_version.get_or_insert_with(|| old_reference.version_id.clone());
                old_snapshots.push((session.clone(), old));
            }
        }
        let baseline_refs = old_snapshots
            .iter()
            .map(|(session, old)| {
                (
                    session.clone(),
                    LogicalSessionSnapshotRefV1 {
                        key: &old.key,
                        authority: old.current.authority,
                        version_id: &old.current.version_id,
                        coverage: &old.coverage,
                        rows: &old.bars_rows,
                    },
                )
            })
            .collect::<Vec<_>>();
        let batch_comparison = compare_session_batch(
            &logical_identity,
            LogicalSessionSnapshotRefV1 {
                key: &snapshot.key,
                authority: candidate_authority,
                version_id: &version_id,
                coverage: &snapshot.coverage,
                rows: &bars_rows,
            },
            &baseline_refs,
            &policy.same_feed,
            &policy.cross_provider,
        )?;
        logical_conflict_sessions = batch_comparison.conflict_sessions.clone();
        let cross_provider_conflict = !batch_comparison.cross_provider_conflict_sessions.is_empty();
        logical_comparisons.extend(batch_comparison.comparisons.values().cloned());
        let cross_provider_comparison = batch_comparison
            .comparisons
            .values()
            .find(|comparison| comparison.kind == CacheLogicalComparisonKindV1::CrossProvider)
            .map(|comparison| comparison.summary);

        for session in candidate_sessions {
            if session_coverage_bounds(&snapshot.coverage, &session)?.is_none()
                || batch_comparison.conflict_sessions.contains(&session)
                || batch_comparison.incomplete_sessions.contains(&session)
            {
                continue;
            }
            let previous_session = existing_logical
                .as_ref()
                .and_then(|manifest| manifest.sessions.get(&session));
            let session_comparison = batch_comparison.comparisons.get(&session).cloned();
            if previous_session.is_some_and(|previous| {
                previous.current.authority == CacheAuthorityV1::Authoritative
                    && candidate_authority == CacheAuthorityV1::Provisional
            }) {
                continue;
            }
            logical_session_updates.insert(session, session_comparison);
        }

        if same_feed_conflict {
            logical_conflict_sessions.extend(
                snapshot
                    .coverage
                    .segments
                    .iter()
                    .map(|segment| segment.session_instance_id.clone()),
            );
        }
        let conflict_rejected = same_feed_conflict || !logical_conflict_sessions.is_empty();
        if conflict_rejected {
            logical_session_updates.clear();
        } else {
            let existing_count = existing_logical
                .as_ref()
                .map_or(0, |manifest| manifest.sessions.len());
            let new_sessions = logical_session_updates
                .keys()
                .filter(|session| {
                    existing_logical
                        .as_ref()
                        .is_none_or(|manifest| !manifest.sessions.contains_key(*session))
                })
                .count();
            if existing_count + new_sessions > MAX_LOGICAL_SESSIONS {
                logical_index_overflow = true;
                logical_session_updates.clear();
            }
        }

        let can_replace_current = !conflict_rejected
            && !matches!(
                previous_current.as_ref(),
                Some(previous) if previous.authority == CacheAuthorityV1::Authoritative
                    && candidate_authority == CacheAuthorityV1::Provisional
            );
        let receipt = CacheVersionReceiptV1 {
            receipt_version: 6,
            key: snapshot.key.clone(),
            key_fingerprint_sha256: key_fingerprint.clone(),
            version_id: version_id.clone(),
            generation: next_generation,
            previous_version_id: previous_receipt.clone(),
            authority: candidate_authority,
            authority_basis,
            created_at_unix_seconds: now_unix_seconds,
            expires_at_unix_seconds: expires_at,
            raw: artifact_receipt(&raw_verified, raw_sha.clone()),
            canonical: artifact_receipt(&canonical_verified, canonical_sha.clone()),
            canonical_rows_sha256: rows_sha.clone(),
            canonical_rows_count: rows.len() as u64,
            raw_bars_rows_count: raw_bars_rows.len() as u64,
            raw_bars_rows_sha256: raw_bars_rows_sha.clone(),
            bars_rows_sha256: bars_rows_sha.clone(),
            raw_source_evidence_sha256: raw_source_evidence_sha.clone(),
            source_evidence_sha256: source_evidence_sha.clone(),
            coverage_evidence_sha256: coverage_evidence_sha.clone(),
            coverage,
            provider_cursor_receipt,
            raw_bars_evidence,
            bars_evidence,
            same_feed_comparison: comparison,
            reconciliation_policy: Some(policy.clone()),
            reconciliation_policy_sha256: Some(policy_sha.clone()),
            provider_admission_receipt: admission.map(|value| value.receipt().clone()),
            provider_admission_receipt_sha256: admission
                .map(|value| value.receipt_sha256().to_owned()),
            cross_provider_comparison,
            cross_provider_conflict,
            conflict_rejected,
            logical_conflict_sessions: logical_conflict_sessions.into_iter().collect(),
            logical_comparisons,
            logical_index_overflow,
        };
        let receipt_bytes = encode_receipt(&receipt)?;
        let receipt_sha256 = hash_domain(RECEIPT_DOMAIN, &receipt_bytes);
        if receipt_bytes.len() as u64 > self.limits.max_metadata_bytes {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        write_new_synced(&stage.path().join(ROWS_FILE), &rows_bytes)?;
        write_new_synced(&stage.path().join(RECEIPT_FILE), &receipt_bytes)?;
        sync_directory(stage.path())?;

        let staged_bytes = tree_bytes_bounded(stage.path(), self.limits.max_total_bytes)?;
        if baseline_bytes
            .checked_add(staged_bytes)
            .is_none_or(|total| total > self.limits.max_total_bytes)
        {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        if version_count >= self.limits.max_versions_per_key {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        let version_path = key_dir.join("versions").join(&version_id);
        if version_path.exists() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let staged_path = stage.path().to_path_buf();
        if fs::rename(&staged_path, &version_path).is_err() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        sync_directory(&key_dir.join("versions"))?;

        if conflict_rejected {
            let reason = if cross_provider_conflict {
                "logical_cross_provider_session_conflict"
            } else {
                "logical_same_feed_session_conflict"
            };
            self.write_rejected_candidate(
                &key_dir,
                &key_fingerprint,
                &canonical_sha,
                rejected_baseline_version.as_deref().unwrap_or(&version_id),
                now_unix_seconds,
                reason,
            )?;
            return Err(CacheReconciliationError::Conflict);
        }

        let current = if can_replace_current {
            let next = CacheCurrentV1 {
                key_fingerprint_sha256: key_fingerprint.clone(),
                version_id: version_id.clone(),
                receipt_sha256: receipt_sha256.clone(),
                generation: next_generation,
                authority: candidate_authority,
                operation: CacheCurrentOperationV1::Publish,
                previous_current_version_id: previous_current
                    .as_ref()
                    .map(|value| value.version_id.clone()),
                updated_at_unix_seconds: now_unix_seconds,
            };
            self.write_current_atomic(&key_dir, &next)?;
            Some(next)
        } else {
            previous_current.clone()
        };
        let mut logical_updates = Vec::with_capacity(logical_session_updates.len());
        for (session, session_comparison) in logical_session_updates {
            let Some(logical_version) = version_ref_for_session(
                CacheLogicalVersionSourceV1 {
                    key: &snapshot.key,
                    key_fingerprint_sha256: &key_fingerprint,
                    version_id: &version_id,
                    receipt_sha256: &receipt_sha256,
                    authority: candidate_authority,
                    created_at_unix_seconds: now_unix_seconds,
                    expires_at_unix_seconds: expires_at,
                    coverage: &snapshot.coverage,
                },
                &session,
            )?
            else {
                continue;
            };
            logical_updates.push((
                session,
                CacheLogicalSessionCurrentV1 {
                    current: logical_version,
                    reconciliation_policy_sha256: policy_sha.clone(),
                    comparison: session_comparison,
                },
            ));
        }
        let logical_invalidation = self.commit_logical_session_updates_unlocked(
            &logical_identity,
            &logical_fingerprint,
            existing_logical.as_ref(),
            logical_updates,
            now_unix_seconds,
        )?;
        let invalidation = previous_current.as_ref().and_then(|previous| {
            current
                .as_ref()
                .filter(|current| current.version_id != previous.version_id)
                .map(|current| CacheInvalidationV1 {
                    key_fingerprint_sha256: key_fingerprint.clone(),
                    invalidated_version_id: previous.version_id.clone(),
                    current_version_id: current.version_id.clone(),
                    generation: current.generation,
                    reason: current.operation,
                })
        });
        Ok(CachePublishResultV1 {
            current,
            published_version_id: version_id,
            authority: candidate_authority,
            current_changed: can_replace_current,
            invalidation,
            logical_invalidation,
            comparison,
            cross_provider_comparison,
            cross_provider_conflict,
            logical_index_overflow,
        })
    }

    /// Load provisional data. Production callers must use the verifier-taking method before an
    /// authoritative version can be read; the unit-test synthetic-authority path is test-only.
    pub(crate) fn load_current(
        &self,
        key: &CacheKeyV1,
        now_unix_seconds: u64,
    ) -> Result<CacheReadResultV1> {
        let result = self.load_current_unverified(key, now_unix_seconds)?;
        self.require_current_admission(&result, now_unix_seconds, None)?;
        Ok(result)
    }

    /// Load a source-authoritative version only after rechecking its retained signed receipt
    /// against the caller's current trusted issuer/revocation verifier and exact cache key.
    pub(crate) fn load_current_with_verifier(
        &self,
        key: &CacheKeyV1,
        now_unix_seconds: u64,
        verifier: &dyn super::authority::TrustedProviderAdmissionVerifierV1,
    ) -> Result<CacheReadResultV1> {
        let result = self.load_current_unverified(key, now_unix_seconds)?;
        self.require_current_admission(&result, now_unix_seconds, Some(verifier))?;
        Ok(result)
    }

    fn load_current_unverified(
        &self,
        key: &CacheKeyV1,
        now_unix_seconds: u64,
    ) -> Result<CacheReadResultV1> {
        ensure_worker_supported()?;
        key.validate()?;
        let key_fingerprint = key.fingerprint()?;
        let _key_lock = self.lock(&format!("key-{key_fingerprint}.lock"))?;
        self.read_current_unlocked(key, &key_fingerprint, now_unix_seconds, true)?
            .ok_or(CacheReconciliationError::NotFound)
    }

    fn require_current_admission(
        &self,
        result: &CacheReadResultV1,
        now_unix_seconds: u64,
        verifier: Option<&dyn super::authority::TrustedProviderAdmissionVerifierV1>,
    ) -> Result<()> {
        if result.current.authority != CacheAuthorityV1::Authoritative {
            return Ok(());
        }
        let (receipt, _) = self.read_version_receipt(
            &result.current.key_fingerprint_sha256,
            &result.current.version_id,
        )?;
        if let Some(verifier) = verifier {
            return super::authority::verify_stored_provider_admission_v1(
                receipt
                    .provider_admission_receipt
                    .as_ref()
                    .ok_or(CacheReconciliationError::BlockedExternal)?,
                &result.key,
                now_unix_seconds,
                verifier,
            );
        }
        #[cfg(test)]
        if receipt.authority_basis == CacheAuthorityBasisV1::SyntheticVerified
            && receipt.provider_admission_receipt.is_none()
        {
            return Ok(());
        }
        Err(CacheReconciliationError::BlockedExternal)
    }

    /// Verify a current version synchronously and discard the read result if the supervising
    /// request was cancelled while its bounded worker was running.
    pub(crate) fn load_current_cancellable(
        &self,
        key: &CacheKeyV1,
        now_unix_seconds: u64,
        cancellation: &CancellationToken,
    ) -> Result<CacheReadResultV1> {
        if cancellation.is_cancelled() {
            return Err(CacheReconciliationError::Cancelled);
        }
        let result = self.load_current(key, now_unix_seconds)?;
        if cancellation.is_cancelled() {
            return Err(CacheReconciliationError::Cancelled);
        }
        Ok(result)
    }

    /// Atomically point current at an older still-fresh verified immutable version.
    pub(crate) fn rollback_to(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
    ) -> Result<CachePublishResultV1> {
        self.rollback_to_checked(key, target_version_id, now_unix_seconds, None)
    }

    /// Roll back to an authoritative version only after the caller's current trusted issuer and
    /// revocation verifier accepts its retained signed admission receipt.
    pub(crate) fn rollback_to_with_verifier(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
        verifier: &dyn super::authority::TrustedProviderAdmissionVerifierV1,
    ) -> Result<CachePublishResultV1> {
        self.rollback_to_checked(key, target_version_id, now_unix_seconds, Some(verifier))
    }

    fn rollback_to_checked(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
        verifier: Option<&dyn super::authority::TrustedProviderAdmissionVerifierV1>,
    ) -> Result<CachePublishResultV1> {
        ensure_worker_supported()?;
        key.validate()?;
        if !valid_sha256(target_version_id) {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let key_fingerprint = key.fingerprint()?;
        let key_dir = self.key_dir(&key_fingerprint)?;
        let _key_lock = self.lock(&format!("key-{key_fingerprint}.lock"))?;
        let _budget_lock = self.lock("budget.lock")?;
        let previous = self
            .read_current_unlocked(key, &key_fingerprint, now_unix_seconds, false)?
            .ok_or(CacheReconciliationError::NotFound)?;
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
            let logical_invalidation =
                self.move_logical_pointer_to_version(&target, now_unix_seconds, false)?;
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
                logical_index_overflow: false,
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
        self.write_current_atomic(&key_dir, &next)?;
        let logical_invalidation =
            self.move_logical_pointer_to_version(&target, now_unix_seconds, false)?;
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
            logical_index_overflow: false,
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

    /// Recover an authoritative pointer only after the caller's current trusted issuer and
    /// revocation verifier accepts its retained signed admission receipt.
    pub(crate) fn recover_to_with_verifier(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
        verifier: &dyn super::authority::TrustedProviderAdmissionVerifierV1,
    ) -> Result<CachePublishResultV1> {
        self.recover_to_checked(key, target_version_id, now_unix_seconds, Some(verifier))
    }

    fn recover_to_checked(
        &self,
        key: &CacheKeyV1,
        target_version_id: &str,
        now_unix_seconds: u64,
        verifier: Option<&dyn super::authority::TrustedProviderAdmissionVerifierV1>,
    ) -> Result<CachePublishResultV1> {
        ensure_worker_supported()?;
        key.validate()?;
        if !valid_sha256(target_version_id) {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let key_fingerprint = key.fingerprint()?;
        let key_dir = self.key_dir(&key_fingerprint)?;
        let _key_lock = self.lock(&format!("key-{key_fingerprint}.lock"))?;
        let _budget_lock = self.lock("budget.lock")?;
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
        if let Some(current) = previous.as_ref()
            && current.current.version_id == target_version_id
        {
            let logical_invalidation =
                self.move_logical_pointer_to_version(&target, now_unix_seconds, true)?;
            return Ok(CachePublishResultV1 {
                current: Some(current.current.clone()),
                published_version_id: target_version_id.to_owned(),
                authority: target.current.authority,
                current_changed: false,
                invalidation: None,
                logical_invalidation,
                comparison: None,
                cross_provider_comparison: None,
                cross_provider_conflict: false,
                logical_index_overflow: false,
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
        self.write_current_atomic(&key_dir, &next)?;
        let logical_invalidation =
            self.move_logical_pointer_to_version(&target, now_unix_seconds, true)?;
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
            logical_index_overflow: false,
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
            });
        }
        summaries.sort_by(|left, right| {
            left.generation
                .cmp(&right.generation)
                .then_with(|| left.version_id.cmp(&right.version_id))
        });
        Ok(summaries)
    }

    fn read_current_unlocked(
        &self,
        key: &CacheKeyV1,
        key_fingerprint: &str,
        now_unix_seconds: u64,
        require_fresh: bool,
    ) -> Result<Option<CacheReadResultV1>> {
        let key_dir = self.key_dir(key_fingerprint)?;
        let current_path = key_dir.join(CURRENT_FILE);
        let Some(bytes) = read_optional_bounded(&current_path, 64 * 1024)? else {
            return Ok(None);
        };
        let envelope: CacheCurrentEnvelopeV1 = serde_json::from_slice(&bytes)?;
        let expected = hash_domain(CURRENT_DOMAIN, &serde_json::to_vec(&envelope.payload)?);
        if expected != envelope.payload_sha256
            || envelope.payload.key_fingerprint_sha256 != key_fingerprint
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let mut result = self.read_version(key, key_fingerprint, &envelope.payload.version_id)?;
        if envelope.payload.receipt_sha256 != result.receipt_sha256
            || envelope.payload.authority != result.current.authority
            || envelope.payload.generation < result.current.generation
            || result.conflict_rejected
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        result.current = envelope.payload;
        if require_fresh
            && !is_fresh_at(
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

    fn read_version(
        &self,
        key: &CacheKeyV1,
        key_fingerprint: &str,
        version_id: &str,
    ) -> Result<CacheReadResultV1> {
        if !valid_sha256(version_id) {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let version_dir = self
            .key_dir(key_fingerprint)?
            .join("versions")
            .join(version_id);
        require_plain_dir(&version_dir)?;
        let (receipt, receipt_sha) = self.read_version_receipt(key_fingerprint, version_id)?;
        let cursor_receipt_is_valid =
            receipt
                .provider_cursor_receipt
                .as_ref()
                .is_none_or(|cursor| {
                    cursor.validate_for_cache_key(key).is_ok()
                        && cursor.expires_at_unix_seconds > receipt.created_at_unix_seconds
                        && receipt
                            .created_at_unix_seconds
                            .checked_add(super::MAX_PROVIDER_CURSOR_TTL_SECONDS)
                            .is_some_and(|maximum| cursor.expires_at_unix_seconds <= maximum)
                        && receipt.coverage.truncated
                });
        if !matches!(receipt.receipt_version, 4..=6)
            || receipt.key != *key
            || receipt.key_fingerprint_sha256 != key_fingerprint
            || receipt.version_id != version_id
            || receipt.generation == 0
            || !valid_sha256(&receipt.canonical_rows_sha256)
            || !valid_sha256(&receipt.raw_bars_rows_sha256)
            || !valid_sha256(&receipt.bars_rows_sha256)
            || !valid_sha256(&receipt.raw_source_evidence_sha256)
            || !valid_sha256(&receipt.source_evidence_sha256)
            || !valid_sha256(&receipt.coverage_evidence_sha256)
            || receipt.canonical.schema_id != key.schema_id
            || receipt.canonical.schema_fingerprint_sha256 != key.schema_fingerprint_sha256
            || receipt.raw.schema_id != key.schema_id
            || receipt.raw.schema_fingerprint_sha256 != key.schema_fingerprint_sha256
            || receipt.expires_at_unix_seconds <= receipt.created_at_unix_seconds
            || receipt.coverage.validate_shape(key).is_err()
            || !cursor_receipt_is_valid
            || receipt.logical_conflict_sessions.len() > super::MAX_SOURCE_SEGMENTS
            || receipt
                .logical_conflict_sessions
                .iter()
                .any(|session| !valid_dimension(session))
            || receipt.logical_comparisons.len() > MAX_LOGICAL_SESSIONS
            || receipt
                .logical_comparisons
                .iter()
                .any(|comparison| comparison.validate().is_err())
            || (receipt.cross_provider_conflict && !receipt.conflict_rejected)
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let current = CacheCurrentV1 {
            key_fingerprint_sha256: key_fingerprint.to_owned(),
            version_id: version_id.to_owned(),
            receipt_sha256: receipt_sha.clone(),
            generation: receipt.generation,
            authority: receipt.authority,
            operation: CacheCurrentOperationV1::Publish,
            previous_current_version_id: receipt.previous_version_id.clone(),
            updated_at_unix_seconds: receipt.created_at_unix_seconds,
        };
        let raw_path = version_dir.join(RAW_FILE);
        let canonical_path = version_dir.join(CANONICAL_FILE);
        let raw_verified = verify_receipt_artifact(&raw_path, &receipt.raw, &self.limits)?;
        let canonical_verified =
            verify_receipt_artifact(&canonical_path, &receipt.canonical, &self.limits)?;
        validate_source_facts(key, &raw_verified)?;
        validate_source_facts(key, &canonical_verified)?;
        let (raw_segment_count, canonical_segment_count) = coverage_row_counts(&receipt.coverage)?;
        if raw_verified.footer_rows != receipt.raw_bars_rows_count
            || raw_segment_count > raw_verified.footer_rows
            || canonical_verified.footer_rows != receipt.canonical_rows_count
            || canonical_verified.footer_rows != canonical_segment_count
            || raw_verified.decoded_rows != raw_verified.footer_rows
            || canonical_verified.decoded_rows != canonical_verified.footer_rows
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let rows_bytes =
            read_required_bounded(&version_dir.join(ROWS_FILE), self.limits.max_metadata_bytes)?;
        let rows: Vec<CanonicalRowV1> = serde_json::from_slice(&rows_bytes)?;
        let normalized = normalize_and_deduplicate_rows(rows)?;
        if normalized.len() as u64 != receipt.canonical_rows_count
            || canonical_rows_sha256(&normalized)? != receipt.canonical_rows_sha256
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let raw_bars_rows = read_market_bars_v1(&raw_path, &self.limits)?;
        let bars_rows = read_market_bars_v1(&canonical_path, &self.limits)?;
        if bars_rows_sha256(&raw_bars_rows)? != receipt.raw_bars_rows_sha256
            || bars_rows_sha256(&bars_rows)? != receipt.bars_rows_sha256
            || source_evidence_sha256(&receipt.raw_bars_evidence)?
                != receipt.raw_source_evidence_sha256
            || source_evidence_sha256(&receipt.bars_evidence)? != receipt.source_evidence_sha256
            || coverage_evidence_sha256(&receipt.coverage)? != receipt.coverage_evidence_sha256
            || normalize_and_deduplicate_rows(canonical_rows_from_bars(&bars_rows)?)? != normalized
            || receipt
                .bars_evidence
                .validate_against(key, &receipt.coverage, &bars_rows)
                .is_err()
            || receipt
                .raw_bars_evidence
                .validate_raw_against(key, &raw_bars_rows)
                .is_err()
            || validate_coverage_rows(&receipt.coverage, &raw_bars_rows, &bars_rows).is_err()
            || validate_canonical_rows_linked_to_raw(&raw_bars_rows, &bars_rows).is_err()
            || derive_response_authority(
                receipt.authority_basis,
                key,
                receipt.coverage.is_complete_for_key(key),
                &bars_rows,
            ) != receipt.authority
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        validate_stored_version_admission(&receipt, key, &raw_bars_rows, &bars_rows, &normalized)?;
        Ok(CacheReadResultV1 {
            current,
            conflict_rejected: receipt.conflict_rejected,
            key: receipt.key,
            coverage: receipt.coverage,
            rows: normalized,
            raw_bars_rows,
            bars_rows,
            raw_bars_rows_sha256: receipt.raw_bars_rows_sha256,
            bars_rows_sha256: receipt.bars_rows_sha256,
            raw_source_evidence_sha256: receipt.raw_source_evidence_sha256,
            source_evidence_sha256: receipt.source_evidence_sha256,
            coverage_evidence_sha256: receipt.coverage_evidence_sha256,
            raw_bars_evidence: receipt.raw_bars_evidence,
            bars_evidence: receipt.bars_evidence,
            provider_cursor_receipt: receipt.provider_cursor_receipt,
            raw_parquet_path: raw_path,
            canonical_parquet_path: canonical_path,
            raw_sha256: receipt.raw.sha256,
            canonical_sha256: receipt.canonical.sha256,
            rows_sha256: receipt.canonical_rows_sha256,
            receipt_sha256: receipt_sha,
            created_at_unix_seconds: receipt.created_at_unix_seconds,
            expires_at_unix_seconds: receipt.expires_at_unix_seconds,
        })
    }

    fn read_version_receipt(
        &self,
        key_fingerprint: &str,
        version_id: &str,
    ) -> Result<(CacheVersionReceiptV1, String)> {
        if !valid_sha256(version_id) {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let version_dir = self
            .key_dir(key_fingerprint)?
            .join("versions")
            .join(version_id);
        require_plain_dir(&version_dir)?;
        let receipt_bytes = read_required_bounded(
            &version_dir.join(RECEIPT_FILE),
            self.limits.max_metadata_bytes,
        )?;
        let receipt: CacheVersionReceiptV1 = serde_json::from_slice(&receipt_bytes)?;
        let domain = match receipt.receipt_version {
            4 => LEGACY_RECEIPT_DOMAIN_V4,
            5 => LEGACY_RECEIPT_DOMAIN_V5,
            6 => RECEIPT_DOMAIN,
            _ => return Err(CacheReconciliationError::UnknownOutcome),
        };
        Ok((receipt, hash_domain(domain, &receipt_bytes)))
    }

    fn write_current_atomic(&self, key_dir: &Path, payload: &CacheCurrentV1) -> Result<()> {
        let payload_bytes = serde_json::to_vec(payload)?;
        let envelope = CacheCurrentEnvelopeV1 {
            payload: payload.clone(),
            payload_sha256: hash_domain(CURRENT_DOMAIN, &payload_bytes),
        };
        let bytes = serde_json::to_vec(&envelope)?;
        let nonce = VERSION_NONCE.fetch_add(1, Ordering::Relaxed);
        let temporary = key_dir.join(format!(".current-{}-{nonce}.tmp", payload.generation));
        write_new_synced(&temporary, &bytes)?;
        replace_plain_file(&temporary, &key_dir.join(CURRENT_FILE))
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        sync_directory(key_dir).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        let readback = read_required_bounded(&key_dir.join(CURRENT_FILE), 64 * 1024)?;
        let envelope: CacheCurrentEnvelopeV1 = serde_json::from_slice(&readback)?;
        if envelope.payload != *payload
            || envelope.payload_sha256
                != hash_domain(CURRENT_DOMAIN, &serde_json::to_vec(&envelope.payload)?)
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(())
    }

    fn write_rejected_candidate(
        &self,
        key_dir: &Path,
        key_fingerprint: &str,
        candidate_canonical_sha256: &str,
        previous_current_version_id: &str,
        now_unix_seconds: u64,
        reason: &str,
    ) -> Result<()> {
        let nonce = VERSION_NONCE.fetch_add(1, Ordering::Relaxed);
        let file_name = format!("rejected-{now_unix_seconds}-{nonce}.json");
        let audit_dir = key_dir.join("audit");
        let mut receipt = RejectedCandidateV1 {
            version: 1,
            key_fingerprint_sha256: key_fingerprint,
            candidate_canonical_sha256,
            previous_current_version_id,
            rejected_at_unix_seconds: now_unix_seconds,
            reason,
            payload_sha256: String::new(),
        };
        let payload = serde_json::to_vec(&(
            receipt.version,
            receipt.key_fingerprint_sha256,
            receipt.candidate_canonical_sha256,
            receipt.previous_current_version_id,
            receipt.rejected_at_unix_seconds,
            receipt.reason,
        ))?;
        receipt.payload_sha256 = hash_domain(REJECTION_DOMAIN, &payload);
        write_new_synced(&audit_dir.join(file_name), &serde_json::to_vec(&receipt)?)?;
        sync_directory(&audit_dir)?;
        Ok(())
    }

    fn key_dir(&self, fingerprint: &str) -> Result<PathBuf> {
        if !valid_sha256(fingerprint) {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let path = self.root.join("keys").join(fingerprint);
        create_private_dir(&path)?;
        Ok(path)
    }

    fn logical_dir(&self, fingerprint: &str) -> Result<PathBuf> {
        if !valid_sha256(fingerprint) {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let path = self.root.join("logical").join(fingerprint);
        create_private_dir(&path)?;
        Ok(path)
    }

    fn lock(&self, name: &str) -> Result<CacheLockGuard> {
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-_".contains(&byte))
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let path = self.root.join("locks").join(name);
        let file = open_lock_file(&path)?;
        file.lock_exclusive()?;
        Ok(CacheLockGuard(file))
    }
}

struct CacheLockGuard(File);

impl Drop for CacheLockGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

struct CacheStageDir {
    path: PathBuf,
}

impl CacheStageDir {
    fn create(parent: &Path) -> Result<Self> {
        for _ in 0..16 {
            let nonce = VERSION_NONCE.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(".cache-stage-{}-{nonce}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => {
                    set_private_dir(&path)?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(CacheReconciliationError::UnknownOutcome)
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for CacheStageDir {
    fn drop(&mut self) {
        // After a successful rename, the staging path no longer exists. Before that point,
        // removing it keeps failed, unpublished candidates from consuming the cache budget.
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub(super) fn derive_response_authority(
    basis: CacheAuthorityBasisV1,
    key: &CacheKeyV1,
    complete: bool,
    rows: &[BarRowV1],
) -> CacheAuthorityV1 {
    let data_is_verified = key.provider == "alpaca"
        && matches!(key.feed.as_str(), "sip" | "boats")
        && key.entitlement == EntitlementState::Authorized
        && matches!(
            key.numeric_encoding.as_str(),
            "decimal_token" | "integer_token"
        )
        && complete
        && !rows.is_empty()
        && rows
            .iter()
            .all(|row| row.validation_state == market_contracts::BarsValidationStateV1::Validated);
    if basis == CacheAuthorityBasisV1::ProviderAdmissionVerified && data_is_verified {
        return CacheAuthorityV1::Authoritative;
    }
    #[cfg(test)]
    if basis == CacheAuthorityBasisV1::SyntheticVerified && data_is_verified {
        return CacheAuthorityV1::Authoritative;
    }
    let _ = (basis, data_is_verified);
    CacheAuthorityV1::Provisional
}

fn has_authoritative_comparison_basis(key: &CacheKeyV1, complete: bool, rows: &[BarRowV1]) -> bool {
    key.provider == "alpaca"
        && matches!(key.feed.as_str(), "sip" | "boats")
        && key.entitlement == EntitlementState::Authorized
        && matches!(
            key.numeric_encoding.as_str(),
            "decimal_token" | "integer_token"
        )
        && complete
        && !rows.is_empty()
        && rows
            .iter()
            .all(|row| row.validation_state == market_contracts::BarsValidationStateV1::Validated)
}

fn bars_rows_sha256(rows: &[BarRowV1]) -> Result<String> {
    if rows.is_empty() || rows.len() > crate::protocol::DEFAULT_MAX_JSONL_RECORDS {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    let bytes = serde_json::to_vec(rows)?;
    Ok(hash_domain(BARS_ROWS_DOMAIN, &bytes))
}

fn coverage_evidence_sha256(coverage: &CoverageEvidenceV1) -> Result<String> {
    let bytes = serde_json::to_vec(coverage)?;
    Ok(hash_domain(
        b"lqepoch.mdp.cache-coverage-evidence.v1\0",
        &bytes,
    ))
}

fn validate_stored_version_admission(
    receipt: &CacheVersionReceiptV1,
    key: &CacheKeyV1,
    raw_rows: &[BarRowV1],
    canonical_rows: &[BarRowV1],
    normalized_rows: &[CanonicalRowV1],
) -> Result<()> {
    if receipt.receipt_version == 4 {
        if receipt.authority == CacheAuthorityV1::Authoritative {
            #[cfg(not(test))]
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        return Ok(());
    }
    let policy = receipt
        .reconciliation_policy
        .as_ref()
        .ok_or(CacheReconciliationError::UnknownOutcome)?;
    let policy_sha = receipt
        .reconciliation_policy_sha256
        .as_deref()
        .ok_or(CacheReconciliationError::UnknownOutcome)?;
    if policy.fingerprint()? != policy_sha {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    match receipt.authority_basis {
        CacheAuthorityBasisV1::ProviderAdmissionUnavailable => {
            if receipt.authority != CacheAuthorityV1::Provisional
                || receipt.provider_admission_receipt.is_some()
                || receipt.provider_admission_receipt_sha256.is_some()
            {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
        }
        CacheAuthorityBasisV1::ProviderAdmissionVerified => {
            if receipt.authority != CacheAuthorityV1::Authoritative {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            let admission = receipt
                .provider_admission_receipt
                .as_ref()
                .ok_or(CacheReconciliationError::UnknownOutcome)?;
            let admission_sha = receipt
                .provider_admission_receipt_sha256
                .as_deref()
                .ok_or(CacheReconciliationError::UnknownOutcome)?;
            if !valid_sha256(admission_sha)
                || super::authority::provider_admission_receipt_sha256(admission)? != admission_sha
                || receipt.created_at_unix_seconds < admission.issued_at_unix_seconds
                || receipt.created_at_unix_seconds >= admission.expires_at_unix_seconds
            {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            let binding = admission
                .claims
                .dataset_binding
                .as_ref()
                .ok_or(CacheReconciliationError::UnknownOutcome)?;
            super::authority::validate_stored_receipt_binding(admission, key, binding)
                .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
            let expected = stored_dataset_binding(
                receipt,
                raw_rows,
                canonical_rows,
                normalized_rows,
                policy_sha,
            )?;
            if binding != &expected {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
        }
        #[cfg(test)]
        CacheAuthorityBasisV1::SyntheticVerified => {
            if receipt.authority != CacheAuthorityV1::Authoritative
                || receipt.provider_admission_receipt.is_some()
                || receipt.provider_admission_receipt_sha256.is_some()
            {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
        }
    }
    Ok(())
}

fn stored_dataset_binding(
    receipt: &CacheVersionReceiptV1,
    raw_rows: &[BarRowV1],
    canonical_rows: &[BarRowV1],
    normalized_rows: &[CanonicalRowV1],
    policy_sha: &str,
) -> Result<ProviderAdmissionDatasetBindingV1> {
    let source_observation_sha256 = receipt
        .coverage
        .segments
        .first()
        .map(|segment| segment.source_observation_sha256.clone())
        .ok_or(CacheReconciliationError::UnknownOutcome)?;
    if receipt
        .coverage
        .segments
        .iter()
        .any(|segment| segment.source_observation_sha256 != source_observation_sha256)
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    let raw_rows_sha = bars_rows_sha256(raw_rows)?;
    let canonical_bar_rows_sha = bars_rows_sha256(canonical_rows)?;
    let raw_facts = serde_json::to_vec(&(&receipt.raw_bars_evidence, raw_rows_sha.as_str()))?;
    let canonical_facts =
        serde_json::to_vec(&(&receipt.bars_evidence, canonical_bar_rows_sha.as_str()))?;
    let binding = ProviderAdmissionDatasetBindingV1 {
        source_observation_sha256,
        raw_response_facts_sha256: hash_domain(
            b"lqepoch.mdp.provider-admission.raw-response-facts.v1\0",
            &raw_facts,
        ),
        canonical_response_facts_sha256: hash_domain(
            b"lqepoch.mdp.provider-admission.canonical-response-facts.v1\0",
            &canonical_facts,
        ),
        canonical_rows_sha256: canonical_rows_sha256(normalized_rows)?,
        coverage_evidence_sha256: coverage_evidence_sha256(&receipt.coverage)?,
        reconciliation_policy_sha256: policy_sha.to_owned(),
        coverage_complete: receipt.coverage.is_complete_for_key(&receipt.key),
        has_gaps: receipt.coverage.coverage_gap_count != Some(0),
        truncated: receipt.coverage.truncated,
    };
    binding.validate()?;
    Ok(binding)
}

fn read_market_bars_v1(path: &Path, limits: &CacheLimitsV1) -> Result<Vec<BarRowV1>> {
    let max_rows = limits
        .max_rows
        .min(crate::protocol::DEFAULT_MAX_JSONL_RECORDS)
        .min(1000);
    let max_result_bytes = limits
        .max_metadata_bytes
        .min((crate::storage::MAX_DECODE_WORKER_OUTPUT_BYTES - 1) as u64);
    crate::parquet_worker::read_market_bars_v1(
        path,
        limits.max_object_bytes,
        max_rows,
        max_result_bytes,
    )
    .map_err(|_| CacheReconciliationError::Readback)
}

fn validate_source_facts(key: &CacheKeyV1, verification: &ParquetVerification) -> Result<()> {
    let source = &verification.source;
    if source.provider != key.provider
        || source.feed != key.feed
        || source.entitlement != key.entitlement
        || source.numeric_encoding.as_str() != key.numeric_encoding
    {
        return Err(CacheReconciliationError::Conflict);
    }
    Ok(())
}

fn artifact_receipt(verification: &ParquetVerification, sha256: String) -> ArtifactReceiptV1 {
    ArtifactReceiptV1 {
        sha256,
        size_bytes: verification.size_bytes,
        schema_id: verification.schema_id.clone(),
        schema_fingerprint_sha256: verification.schema_sha256.clone(),
        parquet_rows: verification.footer_rows,
        source_provider: verification.source.provider.clone(),
        source_feed: verification.source.feed.clone(),
        source_entitlement: verification.source.entitlement,
        numeric_encoding: verification.source.numeric_encoding,
    }
}

fn verify_receipt_artifact(
    path: &Path,
    expected: &ArtifactReceiptV1,
    limits: &CacheLimitsV1,
) -> Result<ParquetVerification> {
    let (sha, size) = hash_file_bounded(path, limits.max_object_bytes)?;
    if sha != expected.sha256 || size != expected.size_bytes {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    let verified = verify_artifact(path, &expected.schema_id, limits.max_object_bytes)?;
    let (readback_sha, readback_size) = hash_file_bounded(path, limits.max_object_bytes)?;
    if verified.schema_sha256 != expected.schema_fingerprint_sha256
        || verified.footer_rows != expected.parquet_rows
        || verified.size_bytes != expected.size_bytes
        || readback_sha != sha
        || readback_size != size
        || verified.source.provider != expected.source_provider
        || verified.source.feed != expected.source_feed
        || verified.source.entitlement != expected.source_entitlement
        || verified.source.numeric_encoding != expected.numeric_encoding
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(verified)
}

fn verify_staged_artifact(
    path: &Path,
    schema_id: &str,
    maximum: u64,
    copied_size: u64,
) -> Result<(ParquetVerification, String)> {
    let (before_sha, before_size) = hash_file_bounded(path, maximum)?;
    if before_size != copied_size {
        return Err(CacheReconciliationError::Readback);
    }
    let verified = verify_artifact(path, schema_id, maximum)?;
    let (after_sha, after_size) = hash_file_bounded(path, maximum)?;
    if before_sha != after_sha || before_size != after_size || verified.size_bytes != before_size {
        return Err(CacheReconciliationError::Readback);
    }
    Ok((verified, after_sha))
}

fn verify_artifact(path: &Path, schema_id: &str, maximum: u64) -> Result<ParquetVerification> {
    #[cfg(target_os = "linux")]
    {
        crate::parquet_worker::verify(path, schema_id, maximum)
            .map_err(|_| CacheReconciliationError::Readback)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (path, schema_id, maximum);
        Err(CacheReconciliationError::Unsupported)
    }
}

fn ensure_worker_supported() -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(CacheReconciliationError::Unsupported)
    }
}

fn version_id(identity: CacheVersionIdentityV1<'_>) -> String {
    let CacheVersionIdentityV1 {
        key_fingerprint,
        generation,
        now_unix_seconds,
        raw_sha,
        canonical_sha,
        rows_sha,
        raw_bars_rows_sha,
        bars_rows_sha,
        raw_source_evidence_sha,
        source_evidence_sha,
        coverage_evidence_sha,
        reconciliation_policy_sha,
        admission_receipt_sha,
    } = identity;
    let nonce = VERSION_NONCE.fetch_add(1, Ordering::Relaxed);
    let mut digest = Sha256::new();
    digest.update(b"lqepoch.mdp.cache-version-id.v1\0");
    digest.update(key_fingerprint.as_bytes());
    digest.update(generation.to_be_bytes());
    digest.update(now_unix_seconds.to_be_bytes());
    digest.update(raw_sha.as_bytes());
    digest.update(canonical_sha.as_bytes());
    digest.update(rows_sha.as_bytes());
    digest.update(raw_bars_rows_sha.as_bytes());
    digest.update(bars_rows_sha.as_bytes());
    digest.update(raw_source_evidence_sha.as_bytes());
    digest.update(source_evidence_sha.as_bytes());
    digest.update(coverage_evidence_sha.as_bytes());
    digest.update(reconciliation_policy_sha.as_bytes());
    digest.update(admission_receipt_sha.as_bytes());
    digest.update(std::process::id().to_be_bytes());
    digest.update(nonce.to_be_bytes());
    hex_digest(digest.finalize().as_slice())
}

fn encode_receipt(receipt: &CacheVersionReceiptV1) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(receipt)?)
}

fn hash_domain(domain: &[u8], payload: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(payload);
    hex_digest(digest.finalize().as_slice())
}

fn read_optional_bounded(path: &Path, maximum: u64) -> Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() || metadata.len() == 0 || metadata.len() > maximum {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            read_file_bounded(path, maximum).map(Some)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_required_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    read_optional_bounded(path, maximum)?.ok_or(CacheReconciliationError::UnknownOutcome)
}

fn read_file_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let file = open_read_nofollow(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    Ok(bytes)
}

fn copy_bounded_nofollow(source: &Path, destination: &Path, maximum: u64) -> Result<u64> {
    let mut input = open_read_nofollow(source)?;
    let input_meta = input.metadata()?;
    if !input_meta.is_file() || input_meta.len() == 0 || input_meta.len() > maximum {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    let mut output = create_new_file(destination)?;
    let mut copied = 0_u64;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        copied = copied
            .checked_add(count as u64)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        if copied > maximum {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        digest.update(&buffer[..count]);
        output.write_all(&buffer[..count])?;
    }
    if copied == 0 {
        return Err(CacheReconciliationError::InvalidInput);
    }
    output.sync_all()?;
    set_private_file(destination)?;
    let _source_digest = digest.finalize();
    Ok(copied)
}

fn hash_file_bounded(path: &Path, maximum: u64) -> Result<(String, u64)> {
    let mut file = open_read_nofollow(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        if total > maximum {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        digest.update(&buffer[..count]);
    }
    Ok((hex_digest(digest.finalize().as_slice()), total))
}

fn create_new_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(FILE_MODE);
    }
    let file = options.open(path)?;
    set_private_file(path)?;
    Ok(file)
}

fn open_read_nofollow(path: &Path) -> Result<File> {
    #[cfg(unix)]
    {
        rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map(File::from)
        .map_err(|error| {
            CacheReconciliationError::Io(std::io::Error::from_raw_os_error(error.raw_os_error()))
        })
    }
    #[cfg(not(unix))]
    {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_file() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let file = OpenOptions::new().read(true).open(path)?;
        if !file.metadata()?.is_file() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(file)
    }
}

fn open_lock_file(path: &Path) -> Result<File> {
    #[cfg(unix)]
    {
        rustix::fs::open(
            path,
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(FILE_MODE as _),
        )
        .map(File::from)
        .map_err(|error| {
            CacheReconciliationError::Io(std::io::Error::from_raw_os_error(error.raw_os_error()))
        })
    }
    #[cfg(not(unix))]
    {
        if let Ok(metadata) = fs::symlink_metadata(path)
            && !metadata.file_type().is_file()
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        set_private_file(path)?;
        Ok(file)
    }
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = create_new_file(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn replace_plain_file(source: &Path, destination: &Path) -> Result<()> {
    match fs::symlink_metadata(destination) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::rename(source, destination)?;
    Ok(())
}

fn create_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    require_plain_dir(path)?;
    set_private_dir(path)?;
    Ok(())
}

/// Claim a new or empty cache root exactly once. Existing non-empty directories are never
/// chmodded or populated unless their private ownership marker already validates.
fn initialize_owned_cache_root(path: &Path) -> Result<()> {
    let existed = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir() {
                return Err(CacheReconciliationError::ForeignNonEmptyRoot);
            }
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path)?;
            false
        }
        Err(error) => return Err(error.into()),
    };

    let marker_path = path.join(ROOT_OWNERSHIP_MARKER_FILE);
    if existed {
        match fs::symlink_metadata(&marker_path) {
            Ok(_) => return validate_owned_cache_root_after_concurrent_init(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut entries = fs::read_dir(path)?;
        if entries.next().transpose()?.is_some() {
            return Err(CacheReconciliationError::ForeignNonEmptyRoot);
        }
    }

    require_current_user_cache_root(path)?;
    set_private_dir(path)?;
    match write_new_synced(&marker_path, ROOT_OWNERSHIP_MARKER) {
        Ok(()) => {
            sync_directory(path)?;
            validate_owned_cache_root(path)
        }
        Err(CacheReconciliationError::Io(error))
            if error.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            // Another initializer won the O_EXCL race. Reuse only its exact valid marker.
            validate_owned_cache_root_after_concurrent_init(path)
        }
        Err(error) => Err(error),
    }
}

fn require_current_user_cache_root(path: &Path) -> Result<()> {
    require_plain_dir(path).map_err(|_| CacheReconciliationError::ForeignNonEmptyRoot)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if fs::symlink_metadata(path)?.uid() != rustix::process::geteuid().as_raw() {
            return Err(CacheReconciliationError::ForeignNonEmptyRoot);
        }
    }
    Ok(())
}

fn validate_owned_cache_root_after_concurrent_init(path: &Path) -> Result<()> {
    for attempt in 0..50 {
        match validate_owned_cache_root(path) {
            Ok(()) => return Ok(()),
            Err(CacheReconciliationError::ForeignNonEmptyRoot) => {
                let marker_path = path.join(ROOT_OWNERSHIP_MARKER_FILE);
                let marker_is_still_being_written =
                    fs::symlink_metadata(&marker_path).is_ok_and(|metadata| {
                        metadata.file_type().is_file()
                            && metadata.len() < ROOT_OWNERSHIP_MARKER.len() as u64
                    });
                if !marker_is_still_being_written || attempt == 49 {
                    return Err(CacheReconciliationError::ForeignNonEmptyRoot);
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error),
        }
    }
    Err(CacheReconciliationError::ForeignNonEmptyRoot)
}

fn validate_owned_cache_root(path: &Path) -> Result<()> {
    require_plain_dir(path).map_err(|_| CacheReconciliationError::ForeignNonEmptyRoot)?;
    let marker_path = path.join(ROOT_OWNERSHIP_MARKER_FILE);
    let path_metadata = fs::symlink_metadata(&marker_path)
        .map_err(|_| CacheReconciliationError::ForeignNonEmptyRoot)?;
    if !path_metadata.file_type().is_file()
        || path_metadata.len() != ROOT_OWNERSHIP_MARKER.len() as u64
    {
        return Err(CacheReconciliationError::ForeignNonEmptyRoot);
    }
    let marker = open_read_nofollow(&marker_path)
        .map_err(|_| CacheReconciliationError::ForeignNonEmptyRoot)?;
    let opened_metadata = marker
        .metadata()
        .map_err(|_| CacheReconciliationError::ForeignNonEmptyRoot)?;
    if !opened_metadata.file_type().is_file()
        || opened_metadata.len() != ROOT_OWNERSHIP_MARKER.len() as u64
    {
        return Err(CacheReconciliationError::ForeignNonEmptyRoot);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let root_metadata = fs::symlink_metadata(path)?;
        if root_metadata.uid() != rustix::process::geteuid().as_raw()
            || root_metadata.permissions().mode() & 0o777 != ROOT_DIR_MODE
            || opened_metadata.dev() != path_metadata.dev()
            || opened_metadata.ino() != path_metadata.ino()
            || opened_metadata.uid() != rustix::process::geteuid().as_raw()
            || opened_metadata.permissions().mode() & 0o777 != FILE_MODE
            || opened_metadata.nlink() != 1
        {
            return Err(CacheReconciliationError::ForeignNonEmptyRoot);
        }
    }
    let mut bytes = Vec::with_capacity(ROOT_OWNERSHIP_MARKER.len());
    marker
        .take(ROOT_OWNERSHIP_MARKER.len() as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CacheReconciliationError::ForeignNonEmptyRoot)?;
    if bytes != ROOT_OWNERSHIP_MARKER {
        return Err(CacheReconciliationError::ForeignNonEmptyRoot);
    }
    Ok(())
}

fn require_plain_dir(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    Ok(())
}

fn set_private_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(ROOT_DIR_MODE))?;
    }
    Ok(())
}

fn set_private_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(FILE_MODE))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn tree_bytes_bounded(root: &Path, maximum: u64) -> Result<u64> {
    require_plain_dir(root)?;
    let mut pending = vec![root.to_path_buf()];
    let mut total = 0_u64;
    let mut entries = 0usize;
    while let Some(directory) = pending.pop() {
        for item in fs::read_dir(directory)? {
            entries = entries
                .checked_add(1)
                .ok_or(CacheReconciliationError::LimitExceeded)?;
            if entries > MAX_TREE_ENTRIES {
                return Err(CacheReconciliationError::LimitExceeded);
            }
            let entry = item?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                total = total
                    .checked_add(entry.metadata()?.len())
                    .ok_or(CacheReconciliationError::LimitExceeded)?;
                if total > maximum {
                    return Err(CacheReconciliationError::LimitExceeded);
                }
            } else {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
        }
    }
    Ok(total)
}

fn directory_entry_count(path: &Path) -> Result<usize> {
    require_plain_dir(path)?;
    let mut count = 0usize;
    for entry in fs::read_dir(path)? {
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
        count = count
            .checked_add(1)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
    }
    Ok(count)
}

fn is_fresh_at(created_at: u64, expires_at: u64, now: u64) -> bool {
    created_at < expires_at && created_at <= now && now < expires_at
}

fn coverage_row_counts(coverage: &CoverageEvidenceV1) -> Result<(u64, u64)> {
    coverage
        .segments
        .iter()
        .try_fold((0_u64, 0_u64), |(raw_total, canonical_total), segment| {
            Ok((
                raw_total
                    .checked_add(segment.raw_row_count)
                    .ok_or(CacheReconciliationError::LimitExceeded)?,
                canonical_total
                    .checked_add(segment.canonical_row_count)
                    .ok_or(CacheReconciliationError::LimitExceeded)?,
            ))
        })
}
