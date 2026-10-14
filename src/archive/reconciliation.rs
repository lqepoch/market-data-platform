//! Versioned, credential-free cache reconciliation for verified market snapshots.
//!
//! The adapter that calls this module owns mapping from `market-contracts` rows and provenance
//! receipts. This module owns cache identity, exact duplicate handling, same-source comparison,
//! and immutable local version publication. It does not connect to providers or publish remotely.
#![allow(dead_code)] // The HTTP adapter is intentionally owned by the integration branch.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use exact_decimal::ExactDecimal;
use market_contracts::{
    AdjustmentV1, BarRowV1, BarsCacheStatusV1, BarsCoverageStatusV1, BarsEntitlementStatusV1,
    BarsProviderModeV1, BarsRequestV1, BarsResponseV1, EntitlementState, NumericEncodingV1,
    UtcTimestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod store;
#[allow(unused_imports)] // The frozen HTTP adapter consumes these crate-private store APIs.
pub(crate) use store::{
    CacheAuthorityV1, CacheCurrentOperationV1, CacheCurrentV1, CacheInvalidationV1, CacheLimitsV1,
    CachePublishResultV1, CacheReadResultV1, CacheSnapshotV1, CacheVersionStoreV1,
    CacheVersionSummaryV1,
};

#[cfg(test)]
mod tests;

const CACHE_KEY_DOMAIN: &[u8] = b"lqepoch.mdp.cache-key.v1\0";
const ROWS_DOMAIN: &[u8] = b"lqepoch.mdp.cache-canonical-rows.v1\0";
const COVERAGE_DOMAIN: &[u8] = b"lqepoch.mdp.cache-coverage.v1\0";
const MAX_DIMENSION_BYTES: usize = 256;
const MAX_ROW_ID_BYTES: usize = 512;
const MAX_ROW_FIELDS: usize = 64;
const MAX_FIELD_NAME_BYTES: usize = 128;
const MAX_FIELD_VALUE_BYTES: usize = 4096;
const MAX_RECONCILIATION_ROWS: usize = 1_000_000;
const MAX_SOURCE_SEGMENTS: usize = 4096;
const MAX_PROVIDER_CURSOR_JSON_BYTES: usize = 2048;
const MAX_PROVIDER_CURSOR_TTL_SECONDS: u64 = 30 * 60;

#[derive(Debug, Error)]
pub(crate) enum CacheReconciliationError {
    #[error("cache reconciliation input is invalid")]
    InvalidInput,
    #[error("cache source coverage is incomplete")]
    Incomplete,
    #[error("cache candidate conflicts with the verified authoritative version")]
    Conflict,
    #[error("cache state is unknown and must be preserved")]
    UnknownOutcome,
    #[error("cache key is currently locked")]
    LockHeld,
    #[error("cache reconciliation resource limit exceeded")]
    LimitExceeded,
    #[error("cache reconciliation I/O failed")]
    Io(#[from] std::io::Error),
    #[error("cache reconciliation metadata is malformed")]
    Json(#[from] serde_json::Error),
    #[error("cache reconciliation Parquet readback failed")]
    Readback,
    #[error("cache operation was cancelled")]
    Cancelled,
    #[error("cache reconciliation worker is unsupported on this platform")]
    Unsupported,
    #[error("cache version is not present")]
    NotFound,
    #[error("cache version has expired")]
    Expired,
    #[error("cache root is non-empty and is not owned by this cache store")]
    ForeignNonEmptyRoot,
}

pub(crate) type Result<T> = std::result::Result<T, CacheReconciliationError>;

/// All non-secret dimensions that must distinguish one cached result from another.
///
/// Actual route/feed, stable entitlement scope, adjustment, requested/effective resolution,
/// symbol, session/window, calendar, and Core schema are included in the digest. The key is
/// derivable before a fetch and deliberately contains no response hashes or raw cursor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheKeyV1 {
    pub provider: String,
    pub feed: String,
    pub entitlement: EntitlementState,
    pub numeric_encoding: String,
    pub adjustment: String,
    pub requested_resolution: String,
    pub effective_resolution: String,
    pub provider_mode: String,
    pub source_scope_id: String,
    pub entitlement_scope_sha256: String,
    pub asset_class: String,
    pub symbol: String,
    pub session_scope: String,
    pub session_instance_id: String,
    pub window_start_utc: String,
    pub window_end_exclusive_utc: String,
    pub market_calendar_version: Option<String>,
    pub request_cursor_sha256: Option<String>,
    pub schema_version: u32,
    pub schema_id: String,
    pub schema_fingerprint_sha256: String,
}

impl CacheKeyV1 {
    /// Derive the lookup key before a provider fetch from stable query routing and calendar scope.
    pub(crate) fn from_request(
        request: &BarsRequestV1,
        source_scope: &CacheQuerySourceScopeV1,
    ) -> Result<Self> {
        request
            .validate()
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        source_scope.validate()?;
        if source_scope.effective_resolution != request.resolution
            && !(request.allow_daily_downgrade
                && source_scope.effective_resolution == market_contracts::BarResolutionV1::OneDay)
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        match request.provider_mode {
            BarsProviderModeV1::AlpacaOnly if source_scope.provider != "alpaca" => {
                return Err(CacheReconciliationError::InvalidInput);
            }
            BarsProviderModeV1::FutuOnly if source_scope.provider != "futu" => {
                return Err(CacheReconciliationError::InvalidInput);
            }
            _ => {}
        }
        let schema_id = crate::schema::MARKET_BARS_SCHEMA_V1_ID;
        let descriptor = market_contracts::trusted_parquet_schema(schema_id)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let schema_fingerprint = market_contracts::trusted_schema_fingerprint(schema_id)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let session_ids = source_scope
            .session_instance_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let key = Self {
            provider: source_scope.provider.clone(),
            feed: source_scope.feed.clone(),
            entitlement: source_scope.entitlement,
            numeric_encoding: source_scope.numeric_encoding.as_str().to_owned(),
            adjustment: enum_label(&request.adjustment)?,
            requested_resolution: request.resolution.as_str().to_owned(),
            effective_resolution: source_scope.effective_resolution.as_str().to_owned(),
            provider_mode: enum_label(&request.provider_mode)?,
            source_scope_id: source_scope.source_scope_id.clone(),
            entitlement_scope_sha256: source_scope.entitlement_scope_sha256.clone(),
            asset_class: enum_label(&request.asset_class)?,
            symbol: request.symbol.clone(),
            session_scope: enum_label(&request.session_scope)?,
            session_instance_id: session_identity(&session_ids)?,
            window_start_utc: request.start.as_str().to_owned(),
            window_end_exclusive_utc: request.end.as_str().to_owned(),
            market_calendar_version: source_scope.market_calendar_version.clone(),
            request_cursor_sha256: request.cursor.as_deref().map(sha256_text),
            schema_version: descriptor.schema_version,
            schema_id: schema_id.to_owned(),
            schema_fingerprint_sha256: schema_fingerprint,
        };
        key.validate()?;
        Ok(key)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for value in [
            self.provider.as_str(),
            self.feed.as_str(),
            self.numeric_encoding.as_str(),
            self.adjustment.as_str(),
            self.requested_resolution.as_str(),
            self.effective_resolution.as_str(),
            self.provider_mode.as_str(),
            self.source_scope_id.as_str(),
            self.asset_class.as_str(),
            self.symbol.as_str(),
            self.session_scope.as_str(),
            self.session_instance_id.as_str(),
            self.schema_id.as_str(),
        ] {
            if !valid_dimension(value) {
                return Err(CacheReconciliationError::InvalidInput);
            }
        }
        if self.schema_version == 0
            || !matches!(self.asset_class.as_str(), "stock" | "etf")
            || !valid_sha256(&self.schema_fingerprint_sha256)
            || !valid_sha256(&self.entitlement_scope_sha256)
            || self
                .request_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
            || self
                .market_calendar_version
                .as_deref()
                .is_some_and(|value| !valid_dimension(value))
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let start = UtcTimestamp::parse_for_arrow_nanoseconds(&self.window_start_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let end = UtcTimestamp::parse_for_arrow_nanoseconds(&self.window_end_exclusive_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        if start >= end {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let trusted_schema_id = crate::schema::MARKET_BARS_SCHEMA_V1_ID;
        let descriptor = market_contracts::trusted_parquet_schema(trusted_schema_id)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let fingerprint = market_contracts::trusted_schema_fingerprint(trusted_schema_id)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        if self.schema_id != trusted_schema_id
            || self.schema_version != descriptor.schema_version
            || self.schema_fingerprint_sha256 != fingerprint
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        Ok(())
    }

    pub(crate) fn fingerprint(&self) -> Result<String> {
        self.validate()?;
        let body = serde_json::to_vec(self)?;
        let mut digest = Sha256::new();
        digest.update(CACHE_KEY_DOMAIN);
        digest.update(body);
        Ok(hex_digest(digest.finalize().as_slice()))
    }
}

/// Stable, credential-free source routing dimensions required before cache lookup.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheQuerySourceScopeV1 {
    /// Actual route provider selected by the adapter, not the request's `auto` mode.
    pub provider: String,
    /// Actual route feed; use `unknown` when the adapter cannot establish it.
    pub feed: String,
    /// Stable, non-secret policy/route identity such as `alpaca-sip-v1`.
    pub source_scope_id: String,
    /// SHA-256 of a stable entitlement realm; never a key, token, header, or account password.
    pub entitlement_scope_sha256: String,
    /// Entitlement state observed for this route.
    pub entitlement: EntitlementState,
    /// Numeric representation expected from the actual source route.
    pub numeric_encoding: NumericEncodingV1,
    /// Resolution expected from the source adapter after any explicitly allowed downgrade.
    pub effective_resolution: market_contracts::BarResolutionV1,
    /// Calendar-derived session identities intersecting the requested window.
    pub session_instance_ids: Vec<String>,
    /// Immutable calendar version used to produce those session identities, when known.
    pub market_calendar_version: Option<String>,
}

impl CacheQuerySourceScopeV1 {
    fn validate(&self) -> Result<()> {
        if !valid_dimension(&self.provider)
            || !valid_dimension(&self.feed)
            || !valid_dimension(&self.source_scope_id)
            || !valid_sha256(&self.entitlement_scope_sha256)
            || self.numeric_encoding == NumericEncodingV1::Unspecified
            || self.numeric_encoding.is_raw_bytes()
            || self.session_instance_ids.is_empty()
            || self.session_instance_ids.len() > MAX_SOURCE_SEGMENTS
            || self
                .session_instance_ids
                .iter()
                .any(|session| !valid_dimension(session))
            || self
                .market_calendar_version
                .as_deref()
                .is_some_and(|value| !valid_dimension(value))
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let unique = self.session_instance_ids.iter().collect::<BTreeSet<_>>();
        if unique.len() != self.session_instance_ids.len() {
            return Err(CacheReconciliationError::InvalidInput);
        }
        Ok(())
    }
}

/// Exact source segment identity. Hashes bind raw input, normalized output, and their pair proof.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceSegmentV1 {
    /// Stable identity derived from Core's source/provenance/session/range segment, not a claimed
    /// provider page ID.
    pub segment_id: String,
    pub provider: String,
    pub feed: String,
    pub entitlement: EntitlementState,
    pub session_instance_id: String,
    pub covered_start_utc: String,
    pub covered_end_exclusive_utc: String,
    pub provenance_id: Option<String>,
    pub coverage_status: BarsCoverageStatusV1,
    /// Hash of SDK/provider observations; this is not a raw HTTP wire-body hash.
    pub source_observation_sha256: String,
    /// Hash of the exact normalized Core rows associated with this coverage segment.
    pub canonical_observation_sha256: String,
    pub lineage_receipt_sha256: String,
    /// Historical bars adapters generally have no stream sequence; absence is explicit.
    pub first_source_sequence: Option<u64>,
    pub last_source_sequence: Option<u64>,
    pub raw_row_count: u64,
    pub canonical_row_count: u64,
    /// Only a digest is retained; page cursor contents can contain provider state or identifiers.
    pub continuation_cursor_sha256: Option<String>,
}

impl SourceSegmentV1 {
    fn validate(&self, key: &CacheKeyV1) -> Result<()> {
        if !valid_dimension(&self.segment_id)
            || self.provider != key.provider
            || self.feed != key.feed
            || self.entitlement != key.entitlement
            || !valid_dimension(&self.session_instance_id)
            || self
                .provenance_id
                .as_deref()
                .is_some_and(|value| !valid_dimension(value))
            || self.coverage_status == BarsCoverageStatusV1::Empty
            || self.coverage_status == BarsCoverageStatusV1::Unsupported
            || !valid_sha256(&self.source_observation_sha256)
            || !valid_sha256(&self.canonical_observation_sha256)
            || !valid_sha256(&self.lineage_receipt_sha256)
            || matches!(
                (self.first_source_sequence, self.last_source_sequence),
                (Some(first), Some(last)) if first == 0 || last < first
            )
            || self.first_source_sequence.is_some() != self.last_source_sequence.is_some()
            || self.raw_row_count == 0
            || self.canonical_row_count > self.raw_row_count
            || self
                .continuation_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let start = UtcTimestamp::parse_for_arrow_nanoseconds(&self.covered_start_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let end = UtcTimestamp::parse_for_arrow_nanoseconds(&self.covered_end_exclusive_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        if start >= end {
            return Err(CacheReconciliationError::InvalidInput);
        }
        Ok(())
    }
}

/// Provider pagination and source-sequence evidence for one requested half-open window.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageEvidenceV1 {
    pub covered_start_utc: String,
    pub covered_end_exclusive_utc: String,
    /// Adapter observation/receive clock retained for audit, not a source sequence.
    pub response_observed_at_utc: String,
    /// Unknown page counts/exhaustion remain unknown and make coverage incomplete.
    pub pages_seen: Option<u32>,
    pub pages_exhausted: Option<bool>,
    /// Optional stream sequence facts; historical bars normally leave these absent.
    pub sequence_gap_count: Option<u64>,
    /// Derived from Core's actual time-gap list, independently of stream sequences.
    pub coverage_gap_count: Option<u32>,
    pub truncated: bool,
    pub segments: Vec<SourceSegmentV1>,
}

/// Provider-local page-cursor kind. The payload itself is retained only in a private cache receipt
/// and is never included in a Debug representation or an HTTP response.
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderCursorKindV1 {
    Alpaca,
    Futu,
}

impl fmt::Debug for ProviderCursorKindV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Alpaca => "Alpaca",
            Self::Futu => "Futu",
        })
    }
}

/// Validated, provider-local SDK cursor input. This is deliberately not a Core or HTTP cursor.
/// A02 constructs it only from its typed provider cursor after validating provider and query scope.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ProviderCursorCacheInputV1 {
    kind: ProviderCursorKindV1,
    payload_json: String,
    expires_at_unix_seconds: u64,
}

impl ProviderCursorCacheInputV1 {
    pub(crate) fn new(
        kind: ProviderCursorKindV1,
        payload_json: String,
        expires_at_unix_seconds: u64,
    ) -> Result<Self> {
        if payload_json.is_empty() || payload_json.len() > MAX_PROVIDER_CURSOR_JSON_BYTES {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        let value: serde_json::Value = serde_json::from_str(&payload_json)?;
        if value.is_null() {
            return Err(CacheReconciliationError::InvalidInput);
        }
        if kind == ProviderCursorKindV1::Futu
            && !value.as_str().is_some_and(valid_futu_provider_cursor)
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let canonical_payload = serde_json::to_string(&value)?;
        if canonical_payload.len() > MAX_PROVIDER_CURSOR_JSON_BYTES {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        Ok(Self {
            kind,
            payload_json: canonical_payload,
            expires_at_unix_seconds,
        })
    }
}

impl fmt::Debug for ProviderCursorCacheInputV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderCursorCacheInputV1")
            .field("kind", &self.kind)
            .field("payload_json", &"[REDACTED]")
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// Provider cursor recovered only after the immutable receipt, payload hash, and query binding
/// have been verified. The provider adapter is the only consumer of the private payload getter.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ValidatedProviderCursorReceiptV1 {
    kind: ProviderCursorKindV1,
    payload_json: String,
    payload_sha256: String,
    key_fingerprint_sha256: String,
    provider: String,
    feed: String,
    entitlement_scope_sha256: String,
    expires_at_unix_seconds: u64,
}

impl ValidatedProviderCursorReceiptV1 {
    pub(crate) fn kind(&self) -> ProviderCursorKindV1 {
        self.kind
    }

    pub(crate) fn payload_json(&self) -> &str {
        &self.payload_json
    }

    pub(crate) fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }

    pub(crate) fn validate_for_cache_key(&self, key: &CacheKeyV1) -> Result<()> {
        let expected_provider = match self.kind {
            ProviderCursorKindV1::Alpaca => "alpaca",
            ProviderCursorKindV1::Futu => "futu",
        };
        let value: serde_json::Value = serde_json::from_str(&self.payload_json)?;
        if self.provider != key.provider
            || self.provider != expected_provider
            || self.feed != key.feed
            || self.entitlement_scope_sha256 != key.entitlement_scope_sha256
            || self.key_fingerprint_sha256 != key.fingerprint()?
            || self.payload_json.is_empty()
            || self.payload_json.len() > MAX_PROVIDER_CURSOR_JSON_BYTES
            || serde_json::to_string(&value)? != self.payload_json
            || self.payload_sha256 != provider_cursor_payload_sha256(&self.payload_json)
            || !valid_sha256(&self.payload_sha256)
            || !valid_sha256(&self.key_fingerprint_sha256)
            || !valid_sha256(&self.entitlement_scope_sha256)
            || (self.kind == ProviderCursorKindV1::Futu
                && !value.as_str().is_some_and(valid_futu_provider_cursor))
            || self.expires_at_unix_seconds == 0
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(())
    }

    fn bind_to_key(
        input: ProviderCursorCacheInputV1,
        key: &CacheKeyV1,
        now_unix_seconds: u64,
    ) -> Result<Self> {
        let maximum_expiry = now_unix_seconds
            .checked_add(MAX_PROVIDER_CURSOR_TTL_SECONDS)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        if input.expires_at_unix_seconds <= now_unix_seconds {
            return Err(CacheReconciliationError::Expired);
        }
        if input.expires_at_unix_seconds > maximum_expiry {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let provider = match input.kind {
            ProviderCursorKindV1::Alpaca => "alpaca",
            ProviderCursorKindV1::Futu => "futu",
        };
        if key.provider != provider {
            return Err(CacheReconciliationError::Conflict);
        }
        let receipt = Self {
            kind: input.kind,
            payload_sha256: provider_cursor_payload_sha256(&input.payload_json),
            payload_json: input.payload_json,
            key_fingerprint_sha256: key.fingerprint()?,
            provider: key.provider.clone(),
            feed: key.feed.clone(),
            entitlement_scope_sha256: key.entitlement_scope_sha256.clone(),
            expires_at_unix_seconds: input.expires_at_unix_seconds,
        };
        receipt.validate_for_cache_key(key)?;
        Ok(receipt)
    }
}

impl fmt::Debug for ValidatedProviderCursorReceiptV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedProviderCursorReceiptV1")
            .field("kind", &self.kind)
            .field("payload_json", &"[REDACTED]")
            .field("payload_sha256", &"[REDACTED]")
            .field("key_fingerprint_sha256", &self.key_fingerprint_sha256)
            .field("provider", &self.provider)
            .field("feed", &self.feed)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

fn valid_futu_provider_cursor(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn provider_cursor_payload_sha256(payload_json: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lqepoch.mdp.provider-page-cursor.v1\0");
    digest.update(payload_json.as_bytes());
    hex_digest(digest.finalize().as_slice())
}

/// Observed provider-page facts supplied by the query adapter. Observation hashes cover
/// SDK/provider observation data, not HTTP wire bodies or credentials. The provider cursor is
/// intentionally omitted from serialization and redacted from Debug; it is persisted separately
/// in a query-bound private version receipt.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ProviderPageEvidenceV1 {
    pub source_observation_sha256: Option<String>,
    pub response_observed_at_utc: Option<market_contracts::ProtoTimestampV2>,
    pub pages_seen: Option<u32>,
    pub pages_exhausted: Option<bool>,
    pub first_source_sequence: Option<u64>,
    pub last_source_sequence: Option<u64>,
    pub sequence_gap_count: Option<u64>,
    pub truncated: bool,
    pub provider_cursor_cache_input: Option<ProviderCursorCacheInputV1>,
}

impl fmt::Debug for ProviderPageEvidenceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderPageEvidenceV1")
            .field("source_observation_sha256", &self.source_observation_sha256)
            .field("response_observed_at_utc", &self.response_observed_at_utc)
            .field("pages_seen", &self.pages_seen)
            .field("pages_exhausted", &self.pages_exhausted)
            .field("first_source_sequence", &self.first_source_sequence)
            .field("last_source_sequence", &self.last_source_sequence)
            .field("sequence_gap_count", &self.sequence_gap_count)
            .field("truncated", &self.truncated)
            .field(
                "provider_cursor_cache_input",
                &self
                    .provider_cursor_cache_input
                    .as_ref()
                    .map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl Default for ProviderPageEvidenceV1 {
    fn default() -> Self {
        Self {
            source_observation_sha256: None,
            response_observed_at_utc: None,
            pages_seen: None,
            pages_exhausted: None,
            first_source_sequence: None,
            last_source_sequence: None,
            sequence_gap_count: None,
            truncated: false,
            provider_cursor_cache_input: None,
        }
    }
}

impl CoverageEvidenceV1 {
    pub(crate) fn from_bars_responses(
        source_scope: &CacheQuerySourceScopeV1,
        raw_response: &BarsResponseV1,
        canonical_response: &BarsResponseV1,
        page_evidence: &ProviderPageEvidenceV1,
    ) -> Result<Self> {
        if page_evidence.truncated != canonical_response.truncated
            || raw_response.provider != source_scope.provider
            || raw_response.feed != source_scope.feed
            || entitlement_state(raw_response.entitlement_status)? != source_scope.entitlement
            || canonical_response.provider != source_scope.provider
            || canonical_response.feed != source_scope.feed
            || entitlement_state(canonical_response.entitlement_status)? != source_scope.entitlement
            || page_evidence
                .source_observation_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
            || page_evidence.pages_seen.is_some_and(|pages| pages == 0)
            || page_evidence.first_source_sequence.is_some()
                != page_evidence.last_source_sequence.is_some()
            || matches!(
                (
                    page_evidence.first_source_sequence,
                    page_evidence.last_source_sequence
                ),
                (Some(first), Some(last)) if first == 0 || last < first
            )
        {
            return Err(CacheReconciliationError::Conflict);
        }
        let source_observation_sha256 = match page_evidence.source_observation_sha256.as_ref() {
            Some(value) => value.clone(),
            None => {
                let bytes = serde_json::to_vec(&raw_response.rows)?;
                sha256_bytes(&bytes)
            }
        };
        let observed_at = page_evidence
            .response_observed_at_utc
            .as_ref()
            .unwrap_or(&raw_response.fetched_at)
            .as_str()
            .to_owned();
        UtcTimestamp::parse_for_arrow_nanoseconds(&observed_at)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;

        let mut segments = Vec::with_capacity(canonical_response.segments.len());
        let mut covered_start: Option<UtcTimestamp> = None;
        let mut covered_end: Option<UtcTimestamp> = None;
        let mut canonical_matched_rows = 0usize;
        for core_segment in &canonical_response.segments {
            let start = UtcTimestamp::parse_for_arrow_nanoseconds(core_segment.start_utc.as_str())
                .map_err(|_| CacheReconciliationError::InvalidInput)?;
            let end =
                UtcTimestamp::parse_for_arrow_nanoseconds(core_segment.end_exclusive_utc.as_str())
                    .map_err(|_| CacheReconciliationError::InvalidInput)?;
            covered_start = Some(covered_start.map_or(start, |current| current.min(start)));
            covered_end = Some(covered_end.map_or(end, |current| current.max(end)));
            let raw_rows = rows_for_raw_core_segment(&raw_response.rows, core_segment)?;
            let canonical_rows = rows_for_core_segment(&canonical_response.rows, core_segment)?;
            if raw_rows.is_empty() || canonical_rows.is_empty() {
                return Err(CacheReconciliationError::InvalidInput);
            }
            let raw_identities = raw_rows
                .iter()
                .map(bar_market_observation_identity)
                .collect::<Result<BTreeSet<_>>>()?;
            if canonical_rows
                .iter()
                .map(bar_market_observation_identity)
                .collect::<Result<Vec<_>>>()?
                .iter()
                .any(|identity| !raw_identities.contains(identity))
            {
                return Err(CacheReconciliationError::Conflict);
            }
            canonical_matched_rows = canonical_matched_rows
                .checked_add(canonical_rows.len())
                .ok_or(CacheReconciliationError::LimitExceeded)?;
            let canonical_bytes = serde_json::to_vec(&canonical_rows)?;
            let canonical_observation_sha256 = sha256_bytes(&canonical_bytes);
            let provenance_id = core_segment.provenance_id.clone();
            let segment_identity = serde_json::to_vec(&(
                source_scope.provider.as_str(),
                source_scope.feed.as_str(),
                source_scope.entitlement,
                core_segment.session_instance_id.as_str(),
                core_segment.start_utc.as_str(),
                core_segment.end_exclusive_utc.as_str(),
                provenance_id.as_deref(),
            ))?;
            let segment_id = format!("segment-{}", sha256_bytes(&segment_identity));
            let lineage_identity = serde_json::to_vec(&(
                segment_id.as_str(),
                source_observation_sha256.as_str(),
                canonical_observation_sha256.as_str(),
                observed_at.as_str(),
                page_evidence.pages_seen,
                page_evidence.pages_exhausted,
                page_evidence.first_source_sequence,
                page_evidence.last_source_sequence,
                page_evidence.sequence_gap_count,
                page_evidence.truncated,
                raw_rows.len() as u64,
                canonical_rows.len() as u64,
            ))?;
            segments.push(SourceSegmentV1 {
                segment_id,
                provider: source_scope.provider.clone(),
                feed: source_scope.feed.clone(),
                entitlement: source_scope.entitlement,
                session_instance_id: core_segment.session_instance_id.clone(),
                covered_start_utc: core_segment.start_utc.as_str().to_owned(),
                covered_end_exclusive_utc: core_segment.end_exclusive_utc.as_str().to_owned(),
                provenance_id,
                coverage_status: core_segment.coverage_status,
                source_observation_sha256: source_observation_sha256.clone(),
                canonical_observation_sha256,
                lineage_receipt_sha256: sha256_bytes(&lineage_identity),
                first_source_sequence: page_evidence.first_source_sequence,
                last_source_sequence: page_evidence.last_source_sequence,
                raw_row_count: raw_rows.len() as u64,
                canonical_row_count: canonical_rows.len() as u64,
                continuation_cursor_sha256: canonical_response
                    .continuation_cursor
                    .as_deref()
                    .map(sha256_text),
            });
        }
        if canonical_matched_rows != canonical_response.rows.len()
            || segments.is_empty()
            || canonical_response.gaps.len() > u32::MAX as usize
        {
            return Err(CacheReconciliationError::Conflict);
        }
        let covered_start = covered_start.ok_or(CacheReconciliationError::InvalidInput)?;
        let covered_end = covered_end.ok_or(CacheReconciliationError::InvalidInput)?;
        Ok(Self {
            covered_start_utc: covered_start.as_str().to_owned(),
            covered_end_exclusive_utc: covered_end.as_str().to_owned(),
            response_observed_at_utc: observed_at,
            pages_seen: page_evidence.pages_seen,
            pages_exhausted: page_evidence.pages_exhausted,
            sequence_gap_count: page_evidence.sequence_gap_count,
            coverage_gap_count: Some(canonical_response.gaps.len() as u32),
            truncated: canonical_response.truncated,
            segments,
        })
    }

    pub(crate) fn validate_shape(&self, key: &CacheKeyV1) -> Result<()> {
        let start = UtcTimestamp::parse_for_arrow_nanoseconds(&self.covered_start_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let end = UtcTimestamp::parse_for_arrow_nanoseconds(&self.covered_end_exclusive_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        UtcTimestamp::parse_for_arrow_nanoseconds(&self.response_observed_at_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        if start >= end
            || self.pages_seen.is_some_and(|pages| pages == 0)
            || self.segments.is_empty()
            || self.segments.len() > MAX_SOURCE_SEGMENTS
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        let requested_start = UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_start_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let requested_end =
            UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_end_exclusive_utc)
                .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let mut ids = BTreeSet::new();
        let mut session_ids = BTreeSet::new();
        for segment in &self.segments {
            segment.validate(key)?;
            let segment_start =
                UtcTimestamp::parse_for_arrow_nanoseconds(&segment.covered_start_utc)
                    .map_err(|_| CacheReconciliationError::InvalidInput)?;
            let segment_end =
                UtcTimestamp::parse_for_arrow_nanoseconds(&segment.covered_end_exclusive_utc)
                    .map_err(|_| CacheReconciliationError::InvalidInput)?;
            if segment_start < requested_start
                || segment_end > requested_end
                || segment_start < start
                || segment_end > end
                || !ids.insert(&segment.segment_id)
            {
                return Err(CacheReconciliationError::Conflict);
            }
            session_ids.insert(segment.session_instance_id.clone());
        }
        if session_identity(&session_ids)? != key.session_instance_id {
            return Err(CacheReconciliationError::Conflict);
        }
        Ok(())
    }

    pub(crate) fn validate_complete(&self, key: &CacheKeyV1) -> Result<()> {
        self.validate_shape(key)?;
        if !self.is_complete_for_key(key) {
            return Err(CacheReconciliationError::Incomplete);
        }
        Ok(())
    }

    pub(crate) fn is_complete_for(
        &self,
        requested_start: UtcTimestamp,
        requested_end: UtcTimestamp,
        covered_start: UtcTimestamp,
        covered_end: UtcTimestamp,
    ) -> bool {
        self.pages_seen.is_some_and(|pages| pages > 0)
            && self.pages_exhausted == Some(true)
            && self.coverage_gap_count == Some(0)
            && self.sequence_gap_count.is_none_or(|count| count == 0)
            && (!self
                .segments
                .iter()
                .any(|segment| segment.first_source_sequence.is_some())
                || self.sequence_gap_count == Some(0))
            && !self.truncated
            && self
                .segments
                .iter()
                .all(|segment| segment.coverage_status == BarsCoverageStatusV1::Complete)
            && covered_start <= requested_start
            && covered_end >= requested_end
    }

    pub(crate) fn is_complete_for_key(&self, key: &CacheKeyV1) -> bool {
        let (Ok(requested_start), Ok(requested_end), Ok(covered_start), Ok(covered_end)) = (
            UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_start_utc),
            UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_end_exclusive_utc),
            UtcTimestamp::parse_for_arrow_nanoseconds(&self.covered_start_utc),
            UtcTimestamp::parse_for_arrow_nanoseconds(&self.covered_end_exclusive_utc),
        ) else {
            return false;
        };
        self.is_complete_for(requested_start, requested_end, covered_start, covered_end)
    }
}

/// Non-row Core evidence stored alongside a cache version. Opaque cursors are deliberately
/// excluded; their digests stay in the credential-free coverage receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BarsCacheEvidenceV1 {
    pub schema_version: u32,
    pub provider: String,
    pub feed: String,
    pub entitlement_status: BarsEntitlementStatusV1,
    pub requested_provider_mode: BarsProviderModeV1,
    pub requested_resolution: market_contracts::BarResolutionV1,
    pub effective_resolution: market_contracts::BarResolutionV1,
    pub adjustment: AdjustmentV1,
    pub coverage_status: BarsCoverageStatusV1,
    pub segments: Vec<market_contracts::BarCoverageSegmentV1>,
    pub gaps: Vec<market_contracts::BarCoverageGapV1>,
    pub source_provenance: Vec<market_contracts::BarSourceProvenanceV1>,
    pub request_cursor_sha256: Option<String>,
    pub response_cursor_sha256: Option<String>,
    pub as_of: Option<market_contracts::ProtoTimestampV2>,
    pub fetched_at: market_contracts::ProtoTimestampV2,
    pub verified_at: Option<market_contracts::ProtoTimestampV2>,
    pub market_calendar_version: Option<String>,
    pub truncated: bool,
}

impl BarsCacheEvidenceV1 {
    pub(crate) fn from_response(
        request: &BarsRequestV1,
        response: &BarsResponseV1,
    ) -> Result<Self> {
        Ok(Self {
            schema_version: response.schema_version,
            provider: response.provider.clone(),
            feed: response.feed.clone(),
            entitlement_status: response.entitlement_status,
            requested_provider_mode: request.provider_mode,
            requested_resolution: response.requested_resolution,
            effective_resolution: response.effective_resolution,
            adjustment: response.adjustment,
            coverage_status: response.coverage_status,
            segments: response.segments.clone(),
            gaps: response.gaps.clone(),
            source_provenance: response.source_provenance.clone(),
            request_cursor_sha256: request.cursor.as_deref().map(sha256_text),
            response_cursor_sha256: response.continuation_cursor.as_deref().map(sha256_text),
            as_of: response.as_of.clone(),
            fetched_at: response.fetched_at.clone(),
            verified_at: response.verified_at.clone(),
            market_calendar_version: response.market_calendar_version.clone(),
            truncated: response.truncated,
        })
    }

    pub(crate) fn validate_against(
        &self,
        key: &CacheKeyV1,
        coverage: &CoverageEvidenceV1,
        rows: &[BarRowV1],
    ) -> Result<()> {
        if self.schema_version != key.schema_version
            || self.provider != key.provider
            || self.feed != key.feed
            || self.source_provenance.iter().any(|source| {
                source.provider != key.provider
                    || source.feed != key.feed
                    || entitlement_state(source.entitlement_status).ok() != Some(key.entitlement)
                    || source.numeric_encoding.as_str() != key.numeric_encoding
            })
            || self.adjustment != parse_adjustment(&key.adjustment)?
            || self.requested_resolution.as_str() != key.requested_resolution
            || self.effective_resolution.as_str() != key.effective_resolution
            || enum_label(&self.requested_provider_mode)? != key.provider_mode
            || self.market_calendar_version != key.market_calendar_version
            || self.request_cursor_sha256 != key.request_cursor_sha256
            || self.truncated != coverage.truncated
            || self.truncated && self.coverage_status == BarsCoverageStatusV1::Complete
            || self
                .request_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
            || self
                .response_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
        {
            return Err(CacheReconciliationError::Conflict);
        }
        if self
            .response_cursor_sha256
            .as_deref()
            .is_some_and(|cursor| {
                !coverage
                    .segments
                    .iter()
                    .any(|segment| segment.continuation_cursor_sha256.as_deref() == Some(cursor))
            })
        {
            return Err(CacheReconciliationError::Conflict);
        }
        let response = BarsResponseV1 {
            schema_version: self.schema_version,
            provider: self.provider.clone(),
            feed: self.feed.clone(),
            entitlement_status: self.entitlement_status,
            requested_resolution: self.requested_resolution,
            effective_resolution: self.effective_resolution,
            adjustment: self.adjustment,
            rows: rows.to_vec(),
            coverage_status: self.coverage_status,
            segments: self.segments.clone(),
            gaps: self.gaps.clone(),
            source_provenance: self.source_provenance.clone(),
            as_of: self.as_of.clone(),
            fetched_at: self.fetched_at.clone(),
            verified_at: self.verified_at.clone(),
            cache_status: BarsCacheStatusV1::Unknown,
            market_calendar_version: self.market_calendar_version.clone(),
            continuation_cursor: self
                .truncated
                .then(|| "cursor-not-retained-by-cache".to_owned()),
            truncated: self.truncated,
        };
        response
            .validate()
            .map_err(|_| CacheReconciliationError::Conflict)?;
        let request_start = UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_start_utc)
            .map_err(|_| CacheReconciliationError::Conflict)?;
        let request_end = UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_end_exclusive_utc)
            .map_err(|_| CacheReconciliationError::Conflict)?;
        for row in rows {
            let row_start = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_start_utc.as_str())
                .map_err(|_| CacheReconciliationError::Conflict)?;
            let row_end =
                UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_end_exclusive_utc.as_str())
                    .map_err(|_| CacheReconciliationError::Conflict)?;
            if row.symbol != key.symbol
                || enum_label(&row.asset_class)? != key.asset_class
                || row.adjustment != self.adjustment
                || row.resolution != self.effective_resolution
                || row_start < request_start
                || row_end > request_end
            {
                return Err(CacheReconciliationError::Conflict);
            }
        }
        let session_ids = self
            .segments
            .iter()
            .map(|segment| segment.session_instance_id.clone())
            .collect::<BTreeSet<_>>();
        let cache_sessions = coverage
            .segments
            .iter()
            .map(|segment| segment.session_instance_id.clone())
            .collect::<BTreeSet<_>>();
        let core_segments = self
            .segments
            .iter()
            .map(core_segment_identity)
            .collect::<Result<Vec<_>>>()?;
        let cache_segments = coverage
            .segments
            .iter()
            .map(cache_segment_identity)
            .collect::<Result<Vec<_>>>()?;
        let mut core_segments = core_segments;
        let mut cache_segments = cache_segments;
        core_segments.sort();
        cache_segments.sort();
        if session_identity(&session_ids)? != key.session_instance_id
            || session_identity(&cache_sessions)? != key.session_instance_id
            || core_segments != cache_segments
            || entitlement_state(self.entitlement_status)? != key.entitlement
            || (coverage.is_complete_for_key(key)
                != (self.coverage_status == BarsCoverageStatusV1::Complete))
        {
            return Err(CacheReconciliationError::Conflict);
        }
        Ok(())
    }

    /// Validate the SDK-observation response independently of canonical calendar coverage. Raw
    /// observations can use provider-local session identities and can contain rows that a trusted
    /// mapper intentionally filters from the canonical response.
    pub(crate) fn validate_raw_against(&self, key: &CacheKeyV1, rows: &[BarRowV1]) -> Result<()> {
        if self.schema_version != key.schema_version
            || self.provider != key.provider
            || self.feed != key.feed
            || self.source_provenance.iter().any(|source| {
                source.provider != key.provider
                    || source.feed != key.feed
                    || entitlement_state(source.entitlement_status).ok() != Some(key.entitlement)
                    || source.numeric_encoding.as_str() != key.numeric_encoding
            })
            || self.adjustment != parse_adjustment(&key.adjustment)?
            || self.requested_resolution.as_str() != key.requested_resolution
            || self.effective_resolution.as_str() != key.effective_resolution
            || enum_label(&self.requested_provider_mode)? != key.provider_mode
            || self.request_cursor_sha256 != key.request_cursor_sha256
            || self.truncated != self.response_cursor_sha256.is_some()
            || self
                .request_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
            || self
                .response_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
            || (self.truncated && self.coverage_status == BarsCoverageStatusV1::Complete)
        {
            return Err(CacheReconciliationError::Conflict);
        }
        let response = BarsResponseV1 {
            schema_version: self.schema_version,
            provider: self.provider.clone(),
            feed: self.feed.clone(),
            entitlement_status: self.entitlement_status,
            requested_resolution: self.requested_resolution,
            effective_resolution: self.effective_resolution,
            adjustment: self.adjustment,
            rows: rows.to_vec(),
            coverage_status: self.coverage_status,
            segments: self.segments.clone(),
            gaps: self.gaps.clone(),
            source_provenance: self.source_provenance.clone(),
            as_of: self.as_of.clone(),
            fetched_at: self.fetched_at.clone(),
            verified_at: self.verified_at.clone(),
            cache_status: BarsCacheStatusV1::Unknown,
            market_calendar_version: self.market_calendar_version.clone(),
            continuation_cursor: self
                .truncated
                .then(|| "cursor-not-retained-by-cache".to_owned()),
            truncated: self.truncated,
        };
        response
            .validate()
            .map_err(|_| CacheReconciliationError::Conflict)?;
        validate_rows_for_key(key, self.adjustment, self.effective_resolution, rows, false)
    }

    pub(crate) fn to_cached_response(
        &self,
        rows: Vec<BarRowV1>,
        cache_status: BarsCacheStatusV1,
        continuation_cursor: Option<String>,
    ) -> Result<BarsResponseV1> {
        if self.truncated != continuation_cursor.is_some()
            || self.truncated != self.response_cursor_sha256.is_some()
        {
            return Err(CacheReconciliationError::Incomplete);
        }
        let response = BarsResponseV1 {
            schema_version: self.schema_version,
            provider: self.provider.clone(),
            feed: self.feed.clone(),
            entitlement_status: self.entitlement_status,
            requested_resolution: self.requested_resolution,
            effective_resolution: self.effective_resolution,
            adjustment: self.adjustment,
            rows,
            coverage_status: self.coverage_status,
            segments: self.segments.clone(),
            gaps: self.gaps.clone(),
            source_provenance: self.source_provenance.clone(),
            as_of: self.as_of.clone(),
            fetched_at: self.fetched_at.clone(),
            verified_at: self.verified_at.clone(),
            cache_status,
            market_calendar_version: self.market_calendar_version.clone(),
            continuation_cursor,
            truncated: self.truncated,
        };
        response
            .validate()
            .map_err(|_| CacheReconciliationError::Conflict)?;
        Ok(response)
    }
}

fn validate_rows_for_key(
    key: &CacheKeyV1,
    adjustment: AdjustmentV1,
    resolution: market_contracts::BarResolutionV1,
    rows: &[BarRowV1],
    enforce_window: bool,
) -> Result<()> {
    let request_start = UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_start_utc)
        .map_err(|_| CacheReconciliationError::Conflict)?;
    let request_end = UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_end_exclusive_utc)
        .map_err(|_| CacheReconciliationError::Conflict)?;
    for row in rows {
        let row_start = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_start_utc.as_str())
            .map_err(|_| CacheReconciliationError::Conflict)?;
        let row_end = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_end_exclusive_utc.as_str())
            .map_err(|_| CacheReconciliationError::Conflict)?;
        if row.symbol != key.symbol
            || enum_label(&row.asset_class)? != key.asset_class
            || row.adjustment != adjustment
            || row.resolution != resolution
            || (enforce_window && (row_start < request_start || row_end > request_end))
        {
            return Err(CacheReconciliationError::Conflict);
        }
    }
    Ok(())
}

pub(crate) fn session_identity(session_ids: &BTreeSet<String>) -> Result<String> {
    if session_ids.is_empty() {
        return Err(CacheReconciliationError::InvalidInput);
    }
    if session_ids.len() == 1 {
        return Ok(session_ids.first().expect("one session id").clone());
    }
    let bytes = serde_json::to_vec(session_ids)?;
    let mut digest = Sha256::new();
    digest.update(b"lqepoch.mdp.cache-session-identity.v1\0");
    digest.update(bytes);
    Ok(format!(
        "multi-{}",
        hex_digest(digest.finalize().as_slice())
    ))
}

pub(super) fn source_evidence_sha256(evidence: &BarsCacheEvidenceV1) -> Result<String> {
    let identity = serde_json::to_vec(evidence)?;
    Ok(hash_domain(
        b"lqepoch.mdp.cache-source-evidence.v1\0",
        &identity,
    ))
}

fn core_segment_identity(
    segment: &market_contracts::BarCoverageSegmentV1,
) -> Result<(String, String, String, Option<String>, String)> {
    Ok((
        segment.start_utc.as_str().to_owned(),
        segment.end_exclusive_utc.as_str().to_owned(),
        segment.session_instance_id.clone(),
        segment.provenance_id.clone(),
        enum_label(&segment.coverage_status)?,
    ))
}

fn cache_segment_identity(
    segment: &SourceSegmentV1,
) -> Result<(String, String, String, Option<String>, String)> {
    Ok((
        segment.covered_start_utc.clone(),
        segment.covered_end_exclusive_utc.clone(),
        segment.session_instance_id.clone(),
        segment.provenance_id.clone(),
        enum_label(&segment.coverage_status)?,
    ))
}

fn sha256_text(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    hex_digest(digest.as_slice())
}

pub(crate) fn entitlement_state(status: BarsEntitlementStatusV1) -> Result<EntitlementState> {
    match status {
        BarsEntitlementStatusV1::Authorized => Ok(EntitlementState::Authorized),
        BarsEntitlementStatusV1::Unauthorized => Ok(EntitlementState::Unauthorized),
        BarsEntitlementStatusV1::Unknown => Ok(EntitlementState::Unknown),
        BarsEntitlementStatusV1::Mixed => Err(CacheReconciliationError::InvalidInput),
    }
}

fn parse_adjustment(value: &str) -> Result<AdjustmentV1> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| CacheReconciliationError::InvalidInput)
}

/// Map Core rows into stable comparison identities. Receive/availability times and source IDs
/// remain in Parquet/evidence but do not create false price conflicts across re-fetches.
pub(crate) fn canonical_rows_from_bars(rows: &[BarRowV1]) -> Result<Vec<CanonicalRowV1>> {
    rows.iter().map(canonical_row_from_bar).collect()
}

fn canonical_row_from_bar(row: &BarRowV1) -> Result<CanonicalRowV1> {
    let identity = serde_json::to_string(&(
        row.symbol.as_str(),
        row.session_instance_id.as_str(),
        row.bar_start_utc.as_str(),
        row.bar_end_exclusive_utc.as_str(),
    ))?;
    CanonicalRowV1::new(
        identity,
        BTreeMap::from([
            (
                "open".to_owned(),
                CanonicalValueV1::Decimal(row.open.as_str().to_owned()),
            ),
            (
                "high".to_owned(),
                CanonicalValueV1::Decimal(row.high.as_str().to_owned()),
            ),
            (
                "low".to_owned(),
                CanonicalValueV1::Decimal(row.low.as_str().to_owned()),
            ),
            (
                "close".to_owned(),
                CanonicalValueV1::Decimal(row.close.as_str().to_owned()),
            ),
            (
                "volume".to_owned(),
                CanonicalValueV1::Decimal(row.volume.as_str().to_owned()),
            ),
            (
                "validation_state".to_owned(),
                CanonicalValueV1::Text(enum_label(&row.validation_state)?),
            ),
        ]),
    )
}

pub(crate) fn normalize_bars_response(response: &BarsResponseV1) -> Result<BarsResponseV1> {
    response
        .validate()
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    let mut unique = BTreeMap::<String, (CanonicalRowV1, BarRowV1, Vec<u8>)>::new();
    for original in &response.rows {
        validate_market_bar_row(original)?;
        let mut row = original.clone();
        row.open = normalized_decimal(row.open.as_str())?;
        row.high = normalized_decimal(row.high.as_str())?;
        row.low = normalized_decimal(row.low.as_str())?;
        row.close = normalized_decimal(row.close.as_str())?;
        row.volume = normalized_decimal(row.volume.as_str())?;
        let canonical = canonical_row_from_bar(&row)?;
        let encoded = serde_json::to_vec(&row)?;
        match unique.get_mut(&canonical.identity) {
            Some((existing_canonical, existing_row, existing_encoded)) => {
                if existing_canonical != &canonical {
                    return Err(CacheReconciliationError::Conflict);
                }
                if encoded < *existing_encoded {
                    *existing_row = row;
                    *existing_encoded = encoded;
                }
            }
            None => {
                unique.insert(canonical.identity.clone(), (canonical, row, encoded));
            }
        }
        if unique.len() > MAX_RECONCILIATION_ROWS {
            return Err(CacheReconciliationError::LimitExceeded);
        }
    }
    let mut normalized = response.clone();
    normalized.rows = unique.into_values().map(|(_, row, _)| row).collect();
    normalized
        .validate()
        .map_err(|_| CacheReconciliationError::Conflict)?;
    Ok(normalized)
}

fn validate_market_bar_row(row: &BarRowV1) -> Result<()> {
    let parse = |value: &str| {
        ExactDecimal::parse_json_number(value).map_err(|_| CacheReconciliationError::InvalidInput)
    };
    let open = parse(row.open.as_str())?;
    let high = parse(row.high.as_str())?;
    let low = parse(row.low.as_str())?;
    let close = parse(row.close.as_str())?;
    let volume = parse(row.volume.as_str())?;
    let start = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_start_utc.as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    let end = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_end_exclusive_utc.as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    if open <= ExactDecimal::ZERO
        || high <= ExactDecimal::ZERO
        || low <= ExactDecimal::ZERO
        || close <= ExactDecimal::ZERO
        || volume < ExactDecimal::ZERO
        || high < open
        || high < low
        || high < close
        || low > open
        || low > close
        || start >= end
        || row.provenance_id.trim().is_empty()
        || row.session_instance_id.trim().is_empty()
    {
        return Err(CacheReconciliationError::InvalidInput);
    }
    Ok(())
}

fn normalized_decimal(value: &str) -> Result<market_contracts::DecimalString> {
    let exact = ExactDecimal::parse_json_number(value)
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    market_contracts::DecimalString::new(exact.to_string())
        .map_err(|_| CacheReconciliationError::InvalidInput)
}

fn rows_for_core_segment(
    rows: &[BarRowV1],
    segment: &market_contracts::BarCoverageSegmentV1,
) -> Result<Vec<BarRowV1>> {
    let segment_start = UtcTimestamp::parse_for_arrow_nanoseconds(segment.start_utc.as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    let segment_end = UtcTimestamp::parse_for_arrow_nanoseconds(segment.end_exclusive_utc.as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    let mut matched = Vec::new();
    for row in rows {
        if row.session_instance_id != segment.session_instance_id
            || segment
                .provenance_id
                .as_deref()
                .is_some_and(|provenance| provenance != row.provenance_id.as_str())
        {
            continue;
        }
        let start = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_start_utc.as_str())
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let end = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_end_exclusive_utc.as_str())
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let overlaps = start < segment_end && end > segment_start;
        if overlaps {
            if start < segment_start || end > segment_end {
                return Err(CacheReconciliationError::Conflict);
            }
            matched.push(row.clone());
        }
    }
    Ok(matched)
}

fn rows_for_raw_core_segment(
    rows: &[BarRowV1],
    segment: &market_contracts::BarCoverageSegmentV1,
) -> Result<Vec<BarRowV1>> {
    let segment_start = UtcTimestamp::parse_for_arrow_nanoseconds(segment.start_utc.as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    let segment_end = UtcTimestamp::parse_for_arrow_nanoseconds(segment.end_exclusive_utc.as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    let mut matched = Vec::new();
    for row in rows {
        if segment
            .provenance_id
            .as_deref()
            .is_some_and(|provenance| provenance != row.provenance_id.as_str())
        {
            continue;
        }
        let start = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_start_utc.as_str())
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let end = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_end_exclusive_utc.as_str())
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        if start < segment_end && end > segment_start {
            if start < segment_start || end > segment_end {
                continue;
            }
            matched.push(row.clone());
        }
    }
    Ok(matched)
}

pub(crate) fn validate_coverage_rows(
    coverage: &CoverageEvidenceV1,
    raw_rows: &[BarRowV1],
    canonical_rows: &[BarRowV1],
) -> Result<()> {
    let mut canonical_matched = 0usize;
    for segment in &coverage.segments {
        let start = UtcTimestamp::parse_for_arrow_nanoseconds(&segment.covered_start_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let end = UtcTimestamp::parse_for_arrow_nanoseconds(&segment.covered_end_exclusive_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let raw = rows_for_lineage_segment(raw_rows, segment, start, end, false)?;
        let canonical = rows_for_lineage_segment(canonical_rows, segment, start, end, true)?;
        let raw_identities = raw
            .iter()
            .map(bar_market_observation_identity)
            .collect::<Result<BTreeSet<_>>>()?;
        if raw.len() as u64 != segment.raw_row_count
            || canonical.len() as u64 != segment.canonical_row_count
            || canonical
                .iter()
                .map(bar_market_observation_identity)
                .collect::<Result<Vec<_>>>()?
                .iter()
                .any(|identity| !raw_identities.contains(identity))
        {
            return Err(CacheReconciliationError::Conflict);
        }
        canonical_matched = canonical_matched
            .checked_add(canonical.len())
            .ok_or(CacheReconciliationError::LimitExceeded)?;
    }
    if canonical_matched != canonical_rows.len() {
        return Err(CacheReconciliationError::Conflict);
    }
    validate_canonical_rows_linked_to_raw(raw_rows, canonical_rows)
}

fn rows_for_lineage_segment(
    rows: &[BarRowV1],
    segment: &SourceSegmentV1,
    segment_start: UtcTimestamp,
    segment_end: UtcTimestamp,
    require_session: bool,
) -> Result<Vec<BarRowV1>> {
    let mut matched = Vec::new();
    for row in rows {
        if (require_session && row.session_instance_id != segment.session_instance_id)
            || segment
                .provenance_id
                .as_deref()
                .is_some_and(|provenance| provenance != row.provenance_id.as_str())
        {
            continue;
        }
        let start = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_start_utc.as_str())
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let end = UtcTimestamp::parse_for_arrow_nanoseconds(row.bar_end_exclusive_utc.as_str())
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        if start < segment_end && end > segment_start {
            if start < segment_start || end > segment_end {
                if require_session {
                    return Err(CacheReconciliationError::Conflict);
                }
                continue;
            }
            matched.push(row.clone());
        }
    }
    Ok(matched)
}

/// Ensure every canonical row is a value-preserving projection of at least one SDK observation.
/// Session IDs are omitted because the trusted calendar mapper replaces provider-local IDs;
/// OHLCV uses ExactDecimal normalization so equivalent lexical scales remain exactly equal.
pub(crate) fn validate_canonical_rows_linked_to_raw(
    raw_rows: &[BarRowV1],
    canonical_rows: &[BarRowV1],
) -> Result<()> {
    if raw_rows.is_empty() || canonical_rows.is_empty() {
        return Err(CacheReconciliationError::InvalidInput);
    }
    let raw_identities = raw_rows
        .iter()
        .map(bar_market_observation_identity)
        .collect::<Result<BTreeSet<_>>>()?;
    for canonical in canonical_rows {
        if !raw_identities.contains(&bar_market_observation_identity(canonical)?) {
            return Err(CacheReconciliationError::Conflict);
        }
    }
    Ok(())
}

fn bar_market_observation_identity(row: &BarRowV1) -> Result<Vec<u8>> {
    let decimals = [
        normalized_decimal(row.open.as_str())?,
        normalized_decimal(row.high.as_str())?,
        normalized_decimal(row.low.as_str())?,
        normalized_decimal(row.close.as_str())?,
        normalized_decimal(row.volume.as_str())?,
    ];
    Ok(serde_json::to_vec(&(
        row.symbol.as_str(),
        enum_label(&row.asset_class)?,
        row.resolution.as_str(),
        enum_label(&row.adjustment)?,
        row.bar_start_utc.as_str(),
        row.bar_end_exclusive_utc.as_str(),
        row.provenance_id.as_str(),
        decimals.map(|value| value.as_str().to_owned()),
    ))?)
}

fn sha256_bytes(value: &[u8]) -> String {
    hex_digest(Sha256::digest(value).as_slice())
}

pub(crate) fn enum_label<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_value(value)?
        .as_str()
        .map(str::to_owned)
        .ok_or(CacheReconciliationError::InvalidInput)
}

pub(crate) fn coverage_identity_sha256(segments: &[SourceSegmentV1]) -> Result<String> {
    if segments.is_empty() || segments.len() > MAX_SOURCE_SEGMENTS {
        return Err(CacheReconciliationError::InvalidInput);
    }
    #[derive(Serialize)]
    struct StableCoverageIdentity<'a> {
        segment_id: &'a str,
        provider: &'a str,
        feed: &'a str,
        entitlement: EntitlementState,
        session_instance_id: &'a str,
        covered_start_utc: &'a str,
        covered_end_exclusive_utc: &'a str,
        provenance_id: &'a Option<String>,
        coverage_status: BarsCoverageStatusV1,
    }
    let mut ordered = segments
        .iter()
        .map(|segment| StableCoverageIdentity {
            segment_id: &segment.segment_id,
            provider: &segment.provider,
            feed: &segment.feed,
            entitlement: segment.entitlement,
            session_instance_id: &segment.session_instance_id,
            covered_start_utc: &segment.covered_start_utc,
            covered_end_exclusive_utc: &segment.covered_end_exclusive_utc,
            provenance_id: &segment.provenance_id,
            coverage_status: segment.coverage_status,
        })
        .collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.segment_id.cmp(right.segment_id));
    let bytes = serde_json::to_vec(&ordered)?;
    let mut digest = Sha256::new();
    digest.update(COVERAGE_DOMAIN);
    digest.update(bytes);
    Ok(hex_digest(digest.finalize().as_slice()))
}

/// Normalized values used only by the same-feed cache comparison adapter.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum CanonicalValueV1 {
    Decimal(String),
    Text(String),
    Integer(u64),
    Boolean(bool),
    Null,
}

/// A stable cache-level row identity and its exact canonical fields.
///
/// This is not a wire DTO. The Core adapter maps its frozen row type into this compact comparison
/// view. Decimal tokens are normalized with Core's `ExactDecimal` arithmetic implementation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CanonicalRowV1 {
    pub identity: String,
    pub fields: BTreeMap<String, CanonicalValueV1>,
}

impl CanonicalRowV1 {
    pub(crate) fn new(
        identity: impl Into<String>,
        mut fields: BTreeMap<String, CanonicalValueV1>,
    ) -> Result<Self> {
        let identity = identity.into();
        if identity.is_empty() || identity.len() > MAX_ROW_ID_BYTES || fields.is_empty() {
            return Err(CacheReconciliationError::InvalidInput);
        }
        if fields.len() > MAX_ROW_FIELDS {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        for (name, value) in &mut fields {
            if !valid_field_name(name) {
                return Err(CacheReconciliationError::InvalidInput);
            }
            match value {
                CanonicalValueV1::Decimal(token) => {
                    let exact = ExactDecimal::parse_json_number(token)
                        .map_err(|_| CacheReconciliationError::InvalidInput)?;
                    *token = exact.to_string();
                }
                CanonicalValueV1::Text(text)
                    if text.len() > MAX_FIELD_VALUE_BYTES || text.chars().any(char::is_control) =>
                {
                    return Err(CacheReconciliationError::InvalidInput);
                }
                CanonicalValueV1::Text(_) => {}
                CanonicalValueV1::Integer(_)
                | CanonicalValueV1::Boolean(_)
                | CanonicalValueV1::Null => {}
            }
        }
        Ok(Self { identity, fields })
    }
}

pub(crate) fn normalize_and_deduplicate_rows(
    rows: impl IntoIterator<Item = CanonicalRowV1>,
) -> Result<Vec<CanonicalRowV1>> {
    let mut unique = BTreeMap::<String, CanonicalRowV1>::new();
    let mut count = 0usize;
    for row in rows {
        count = count
            .checked_add(1)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        if count > MAX_RECONCILIATION_ROWS {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        let normalized = CanonicalRowV1::new(row.identity, row.fields)?;
        if let Some(existing) = unique.get(&normalized.identity) {
            if existing != &normalized {
                return Err(CacheReconciliationError::Conflict);
            }
        } else {
            unique.insert(normalized.identity.clone(), normalized);
        }
    }
    Ok(unique.into_values().collect())
}

pub(crate) fn canonical_rows_sha256(rows: &[CanonicalRowV1]) -> Result<String> {
    if rows.len() > MAX_RECONCILIATION_ROWS {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    let normalized = normalize_and_deduplicate_rows(rows.iter().cloned())?;
    let bytes = serde_json::to_vec(&normalized)?;
    let mut digest = Sha256::new();
    digest.update(ROWS_DOMAIN);
    digest.update(bytes);
    Ok(hex_digest(digest.finalize().as_slice()))
}

/// Explicit decimal tolerances for comparing a new Alpaca snapshot with its verified predecessor.
/// Missing fields are exact; no tolerance is inferred from the provider or numeric encoding.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SameFeedTolerancePolicyV1 {
    pub max_abs_delta_by_field: BTreeMap<String, String>,
    pub max_changed_rows: usize,
    pub max_added_rows: usize,
    pub allow_removed_rows: bool,
}

impl SameFeedTolerancePolicyV1 {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_changed_rows > MAX_RECONCILIATION_ROWS
            || self.max_added_rows > MAX_RECONCILIATION_ROWS
            || self.max_abs_delta_by_field.len() > MAX_ROW_FIELDS
        {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        for (field, token) in &self.max_abs_delta_by_field {
            if !valid_field_name(field) {
                return Err(CacheReconciliationError::InvalidInput);
            }
            let value = ExactDecimal::parse_json_number(token)
                .map_err(|_| CacheReconciliationError::InvalidInput)?;
            if value.is_negative() {
                return Err(CacheReconciliationError::InvalidInput);
            }
        }
        Ok(())
    }

    fn tolerance_for(&self, field: &str) -> Result<ExactDecimal> {
        let token = self
            .max_abs_delta_by_field
            .get(field)
            .map(String::as_str)
            .unwrap_or("0");
        ExactDecimal::parse_json_number(token).map_err(|_| CacheReconciliationError::InvalidInput)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SameFeedComparisonV1 {
    pub compared_rows: usize,
    pub changed_rows: usize,
    pub tolerated_decimal_fields: usize,
    pub added_rows: usize,
    pub removed_rows: usize,
}

pub(crate) fn compare_same_feed_authoritative(
    previous: &[CanonicalRowV1],
    candidate: &[CanonicalRowV1],
    policy: &SameFeedTolerancePolicyV1,
) -> Result<SameFeedComparisonV1> {
    policy.validate()?;
    let previous = normalize_and_deduplicate_rows(previous.iter().cloned())?;
    let candidate = normalize_and_deduplicate_rows(candidate.iter().cloned())?;
    let previous = previous
        .into_iter()
        .map(|row| (row.identity.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let candidate = candidate
        .into_iter()
        .map(|row| (row.identity.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let mut summary = SameFeedComparisonV1 {
        added_rows: candidate
            .keys()
            .filter(|identity| !previous.contains_key(*identity))
            .count(),
        removed_rows: previous
            .keys()
            .filter(|identity| !candidate.contains_key(*identity))
            .count(),
        ..SameFeedComparisonV1::default()
    };
    if summary.removed_rows > 0 && !policy.allow_removed_rows {
        return Err(CacheReconciliationError::Conflict);
    }
    if summary.added_rows > policy.max_added_rows {
        return Err(CacheReconciliationError::Conflict);
    }
    for (identity, old_row) in &previous {
        let Some(new_row) = candidate.get(identity) else {
            continue;
        };
        summary.compared_rows = summary
            .compared_rows
            .checked_add(1)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        if old_row.fields.keys().ne(new_row.fields.keys()) {
            return Err(CacheReconciliationError::Conflict);
        }
        let mut row_changed = false;
        for (field, old_value) in &old_row.fields {
            let new_value = new_row
                .fields
                .get(field)
                .ok_or(CacheReconciliationError::Conflict)?;
            if old_value == new_value {
                continue;
            }
            match (old_value, new_value) {
                (CanonicalValueV1::Decimal(old), CanonicalValueV1::Decimal(new)) => {
                    let old = ExactDecimal::parse_json_number(old)
                        .map_err(|_| CacheReconciliationError::InvalidInput)?;
                    let new = ExactDecimal::parse_json_number(new)
                        .map_err(|_| CacheReconciliationError::InvalidInput)?;
                    let delta = new
                        .checked_sub(old)
                        .map_err(|_| CacheReconciliationError::LimitExceeded)?;
                    let absolute = if delta.is_negative() {
                        ExactDecimal::ZERO
                            .checked_sub(delta)
                            .map_err(|_| CacheReconciliationError::LimitExceeded)?
                    } else {
                        delta
                    };
                    if absolute > policy.tolerance_for(field)? {
                        return Err(CacheReconciliationError::Conflict);
                    }
                    summary.tolerated_decimal_fields = summary
                        .tolerated_decimal_fields
                        .checked_add(1)
                        .ok_or(CacheReconciliationError::LimitExceeded)?;
                    row_changed = true;
                }
                _ => return Err(CacheReconciliationError::Conflict),
            }
        }
        if row_changed {
            summary.changed_rows = summary
                .changed_rows
                .checked_add(1)
                .ok_or(CacheReconciliationError::LimitExceeded)?;
            if summary.changed_rows > policy.max_changed_rows {
                return Err(CacheReconciliationError::Conflict);
            }
        }
    }
    Ok(summary)
}

fn valid_dimension(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_DIMENSION_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
        && !value.contains('/')
        && !value.contains('\\')
}

fn valid_field_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_FIELD_NAME_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

pub(crate) fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}
