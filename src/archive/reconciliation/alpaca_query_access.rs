//! Local, per-query Alpaca protocol evidence for cache reconciliation.
//!
//! This proof is distinct from provider-admission receipts used for external archive publication.
//! It records one successful, validated Broker SDK page and does not establish entitlement,
//! licensing, completeness, or a provider-wide capability.

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use market_contracts::{
    AdjustmentV1, AssetClassV1, BarResolutionV1, BarRowV1, BarsCoverageStatusV1,
    BarsEntitlementStatusV1, BarsRequestV1, BarsResponseV1, BarsValidationStateV1,
    EntitlementState, NumericEncodingV1, UtcTimestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use super::{
    CacheQuerySourceScopeV1, CacheReconciliationError, ProviderCursorCacheInputV1,
    ProviderCursorKindV1, ProviderPageEvidenceV1, Result, hex_digest, valid_sha256,
};

const ALPACA_BARS_CONTRACT_ID: &str = "alpaca-stock-bars-single-symbol-v2/typed-observation-v1";
const ALPACA_BARS_ORIGIN: &str = "https://data.alpaca.markets";
const ALPACA_BARS_PATH_TEMPLATE: &str = "/v2/stocks/{symbol}/bars";
const QUERY_ACCESS_RECEIPT_DOMAIN: &[u8] = b"lqepoch.mdp.alpaca-query-access-receipt.v1\0";
const QUERY_ACCESS_CHAIN_DOMAIN: &[u8] = b"lqepoch.mdp.alpaca-query-access-page-chain.v1\0";
const QUERY_ACCESS_CHAIN_HASH_DOMAIN: &[u8] =
    b"lqepoch.mdp.alpaca-query-access-page-chain-hash.v1\0";
const QUERY_ACCESS_PROJECTED_RAW_ROWS_DOMAIN: &[u8] =
    b"lqepoch.mdp.alpaca-query-projected-raw-rows.v1\0";
const MAX_ALPACA_PAGE_SIZE: u16 = 1_000;
const MAX_ALPACA_PAGE_PROJECTED_BYTES: usize = 32 * 1024 * 1024;
const MAX_ALPACA_QUERY_AGE_SECONDS: i64 = 60;
pub(crate) const MAX_ALPACA_QUERY_ACCESS_PAGES: usize = 64;

pub(crate) mod stored;

/// A Broker-owned opaque witness copied for synchronous cache-finalization checks only.
///
/// The object has no serde implementation and cannot be created from caller-supplied fields in
/// production. It intentionally omits the provider cursor value; only its independently supplied
/// digest is kept, and the cache receipt below stores digests rather than cursor text.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct AlpacaStockBarsQueryWitnessV1 {
    witness: alpaca_rest_read::AlpacaStockBarsQueryAccessWitness,
    expected_request_cursor_sha256: Option<String>,
    projected_raw_rows_sha256: String,
}

impl AlpacaStockBarsQueryWitnessV1 {
    /// Copy an opaque Broker witness after checking the request's separately derived cursor hash.
    ///
    /// `expected_request_cursor_sha256` comes from the selected typed provider cursor, not the
    /// witness itself; the first SDK page uses `None`. `projected_raw_rows` is the exact Core row
    /// projection for this page, before calendar remapping.
    pub(crate) fn from_broker_witness(
        witness: &alpaca_rest_read::AlpacaStockBarsQueryAccessWitness,
        expected_request_cursor_sha256: Option<&str>,
        projected_raw_rows: &[BarRowV1],
    ) -> Result<Self> {
        validate_static_witness(witness)?;
        if expected_request_cursor_sha256 != witness.request_cursor_sha256()
            || expected_request_cursor_sha256.is_some_and(|value| !valid_sha256(value))
        {
            return Err(CacheReconciliationError::Conflict);
        }
        let witnessed_row_count = usize::try_from(witness.row_count())
            .map_err(|_| CacheReconciliationError::LimitExceeded)?;
        if projected_raw_rows.len() != witnessed_row_count {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        let projected_raw_rows_sha256 = alpaca_query_projected_raw_rows_sha256(projected_raw_rows)?;
        Ok(Self {
            witness: witness.clone(),
            expected_request_cursor_sha256: expected_request_cursor_sha256.map(str::to_owned),
            projected_raw_rows_sha256,
        })
    }
}

/// Validated local query evidence for one or more ordered Broker pages. The boolean is derived
/// from the first/last page cursors and never from a caller's `pages_exhausted` assertion alone.
pub(crate) struct AlpacaQueryAccessValidationV1 {
    pub receipts: Vec<AlpacaQueryAccessReceiptV1>,
    pub full_provider_chain: bool,
}

/// Recheck the bounded page chain against the exact Core query and both row envelopes.
pub(crate) fn validate_query_access_pages(
    request: &BarsRequestV1,
    source_scope: &CacheQuerySourceScopeV1,
    raw_response: &BarsResponseV1,
    canonical_response: &BarsResponseV1,
    page_evidence: &ProviderPageEvidenceV1,
    now_unix_seconds: u64,
) -> Result<AlpacaQueryAccessValidationV1> {
    let pages = &page_evidence.alpaca_query_access_pages;
    if pages.is_empty() || pages.len() > MAX_ALPACA_QUERY_ACCESS_PAGES {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }

    let (expected_start, expected_end) = request_bounds(request)?;
    let expected_entitlement = BarsEntitlementStatusV1::Unknown;
    let cache_key_fingerprint =
        super::CacheKeyV1::from_request(request, source_scope)?.fingerprint()?;
    let first = pages
        .first()
        .ok_or(CacheReconciliationError::AdmissionMismatch)?;
    let feed = first.witness.requested_feed().as_str();
    if source_scope.provider != "alpaca"
        || source_scope.feed != feed
        || source_scope.entitlement != EntitlementState::Unknown
        || source_scope.numeric_encoding != NumericEncodingV1::DecimalToken
        || !matches!(request.asset_class, AssetClassV1::Stock | AssetClassV1::Etf)
        || request.adjustment != AdjustmentV1::Unadjusted
        || raw_response.provider != "alpaca"
        || raw_response.feed != feed
        || raw_response.entitlement_status != expected_entitlement
        || canonical_response.provider != "alpaca"
        || canonical_response.feed != feed
        || canonical_response.entitlement_status != expected_entitlement
        || raw_response.rows.is_empty()
        || canonical_response.rows.is_empty()
        || raw_response.rows.len() > usize::from(request.limit)
        || canonical_response.rows.len() > raw_response.rows.len()
        || !valid_alpaca_provenance(&raw_response.source_provenance, feed)
        || !valid_alpaca_provenance(&canonical_response.source_provenance, feed)
        || page_evidence.first_source_sequence.is_some()
        || page_evidence.last_source_sequence.is_some()
        || page_evidence.sequence_gap_count.is_some()
    {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }

    let mut receipts = Vec::with_capacity(pages.len());
    let mut page_observation_times = BTreeSet::new();
    let mut total_rows = 0u64;
    let mut previous_next: Option<&str> = None;
    let now =
        i64::try_from(now_unix_seconds).map_err(|_| CacheReconciliationError::InvalidInput)?;
    let sort = first.witness.sort();
    for (index, page) in pages.iter().enumerate() {
        let witness = &page.witness;
        validate_static_witness(witness)?;
        if page.expected_request_cursor_sha256.as_deref() != witness.request_cursor_sha256()
            || witness.symbol() != request.symbol
            || witness.requested_feed().as_str() != feed
            || witness.adjustment() != "raw"
            || witness.currency() != "USD"
            || witness.asof() != "-"
            || witness.timeframe() != timeframe_for_resolution(source_scope.effective_resolution)
            || witness.start_inclusive() != &expected_start
            || witness.end_inclusive() != &expected_end
            || witness.page_size() != request.limit
            || witness.sort() != sort
            || index > 0 && previous_next != witness.request_cursor_sha256()
            || index + 1 < pages.len() && witness.next_page_cursor_sha256().is_none()
        {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        let observed_at = witness.response_observed_at().as_str().to_owned();
        if !page_observation_times.insert(observed_at.clone()) {
            // Cold read partitions rows by this timestamp. Shared timestamps would make
            // multi-page row boundaries ambiguous, so do not claim query evidence for them.
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
        let observed_seconds = parse_utc_datetime(&observed_at)?.timestamp();
        let age = now
            .checked_sub(observed_seconds)
            .ok_or(CacheReconciliationError::AdmissionExpired)?;
        if !(0..=MAX_ALPACA_QUERY_AGE_SECONDS).contains(&age) {
            return Err(CacheReconciliationError::AdmissionExpired);
        }
        let row_count = u64::from(witness.row_count());
        total_rows = total_rows
            .checked_add(row_count)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        previous_next = witness.next_page_cursor_sha256();
        let mut receipt = AlpacaQueryAccessReceiptV1::from_witness(
            witness,
            page.expected_request_cursor_sha256.clone(),
            page.projected_raw_rows_sha256.clone(),
        );
        receipt.cache_key_fingerprint_sha256 = cache_key_fingerprint.clone();
        receipt.cache_request_cursor_sha256 = request.cursor.as_deref().map(super::sha256_text);
        receipts.push(receipt.bind_source_observation(witness.typed_response_sha256())?);
    }

    let last = pages
        .last()
        .ok_or(CacheReconciliationError::AdmissionMismatch)?;
    let has_next = last.witness.has_next_page();
    let expected_pages_seen =
        u32::try_from(pages.len()).map_err(|_| CacheReconciliationError::LimitExceeded)?;
    if total_rows != raw_response.rows.len() as u64
        || page_evidence.pages_seen != Some(expected_pages_seen)
        || page_evidence.pages_exhausted != Some(!has_next)
        || page_evidence.truncated != has_next
        || raw_response.truncated != has_next
        || canonical_response.truncated != has_next
        || raw_response.continuation_cursor.is_some() != has_next
        || canonical_response.continuation_cursor.is_some() != has_next
        || page_evidence
            .response_observed_at_utc
            .as_ref()
            .is_none_or(|value| value.as_str() != last.witness.response_observed_at().as_str())
        || page_evidence.source_observation_sha256.as_deref()
            != Some(alpaca_query_access_chain_observation_sha256(&receipts)?.as_str())
    {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    let mut raw_rows_by_observation = BTreeMap::<String, Vec<&BarRowV1>>::new();
    for row in &raw_response.rows {
        raw_rows_by_observation
            .entry(row.received_at_utc.as_str().to_owned())
            .or_default()
            .push(row);
    }
    if raw_rows_by_observation.len() != receipts.len() {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    for receipt in &receipts {
        let rows = raw_rows_by_observation
            .get(&receipt.response_observed_at_utc)
            .ok_or(CacheReconciliationError::AdmissionMismatch)?;
        let expected_rows_sha256 = receipt
            .projected_raw_rows_sha256
            .as_deref()
            .ok_or(CacheReconciliationError::AdmissionMismatch)?;
        if rows.len()
            != usize::try_from(receipt.row_count)
                .map_err(|_| CacheReconciliationError::LimitExceeded)?
            || projected_raw_row_refs_sha256(rows.iter().copied())? != expected_rows_sha256
        {
            return Err(CacheReconciliationError::AdmissionMismatch);
        }
    }
    match (
        last.witness.next_page_cursor_sha256(),
        page_evidence.provider_cursor_cache_input.as_ref(),
    ) {
        (None, None) => {}
        (Some(expected), Some(input))
            if input.kind == ProviderCursorKindV1::Alpaca
                && valid_sha256(expected)
                && alpaca_cursor_payload_matches(input, &last.witness, expected) => {}
        _ => return Err(CacheReconciliationError::AdmissionMismatch),
    }

    Ok(AlpacaQueryAccessValidationV1 {
        full_provider_chain: pages[0].witness.request_cursor_sha256().is_none()
            && last.witness.next_page_cursor_sha256().is_none(),
        receipts,
    })
}

/// Hash an exact ordered Core-row projection of one SDK page. This is separate from the Broker's
/// typed SDK response digest and binds the rows MDP actually persists.
pub(crate) fn alpaca_query_projected_raw_rows_sha256(rows: &[BarRowV1]) -> Result<String> {
    projected_raw_row_refs_sha256(rows.iter())
}

fn projected_raw_row_refs_sha256<'a>(rows: impl Iterator<Item = &'a BarRowV1>) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(QUERY_ACCESS_PROJECTED_RAW_ROWS_DOMAIN);
    let mut count = 0u64;
    let mut total_bytes = 0usize;
    let mut encoded_rows = Vec::new();
    for row in rows {
        let encoded = serde_json::to_vec(row)?;
        total_bytes = total_bytes
            .checked_add(encoded.len())
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        if total_bytes > MAX_ALPACA_PAGE_PROJECTED_BYTES {
            return Err(CacheReconciliationError::LimitExceeded);
        }
        count = count
            .checked_add(1)
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        let length =
            u64::try_from(encoded.len()).map_err(|_| CacheReconciliationError::LimitExceeded)?;
        encoded_rows.push((length, encoded));
    }
    if count == 0 || count > u64::from(MAX_ALPACA_PAGE_SIZE) {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    digest.update(count.to_be_bytes());
    for (length, encoded) in encoded_rows {
        digest.update(length.to_be_bytes());
        digest.update(encoded);
    }
    Ok(hex_digest(digest.finalize().as_slice()))
}

/// Promote only a response whose full, query-bound provider chain and actual Core rows tile every
/// trusted calendar phase. A missing bar is never interpreted as a no-trade interval.
pub(crate) fn derive_complete_coverage_from_timeline(
    request: &BarsRequestV1,
    source_scope: &CacheQuerySourceScopeV1,
    raw_response: &BarsResponseV1,
    canonical_response: &mut BarsResponseV1,
    phase_slices: Option<&[super::authority::ProviderPhaseSliceV1]>,
    full_provider_chain: bool,
) -> Result<bool> {
    let complete = derive_complete_coverage_from_timeline_inner(
        request,
        source_scope,
        raw_response,
        canonical_response,
        phase_slices,
        full_provider_chain,
    )?;
    if !complete {
        super::store::downgrade_complete_coverage(canonical_response);
    }
    Ok(complete)
}

fn derive_complete_coverage_from_timeline_inner(
    request: &BarsRequestV1,
    source_scope: &CacheQuerySourceScopeV1,
    raw_response: &BarsResponseV1,
    canonical_response: &mut BarsResponseV1,
    phase_slices: Option<&[super::authority::ProviderPhaseSliceV1]>,
    full_provider_chain: bool,
) -> Result<bool> {
    if !full_provider_chain
        || raw_response.truncated
        || canonical_response.truncated
        || !raw_response.gaps.is_empty()
        || !canonical_response.gaps.is_empty()
        || raw_response.rows.is_empty()
        || canonical_response.rows.is_empty()
        || source_scope.market_calendar_version.is_none()
        || canonical_response
            .market_calendar_version
            .as_ref()
            .is_some_and(|version| Some(version) != source_scope.market_calendar_version.as_ref())
    {
        return Ok(false);
    }
    let Some(phases) = phase_slices else {
        return Ok(false);
    };
    if phases.is_empty() || phases.len() > super::authority::MAX_PHASE_SLICES {
        return Ok(false);
    }
    super::authority::validate_phase_slices(
        phases,
        &source_scope.session_instance_ids,
        request.start.as_str(),
        request.end.as_str(),
    )?;

    let phase_sessions = phases
        .iter()
        .map(|slice| slice.session_instance_id.as_str())
        .collect::<BTreeSet<_>>();
    let expected_sessions = source_scope
        .session_instance_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if phase_sessions != expected_sessions {
        return Ok(false);
    }

    // Promotion requires a one-to-one exact market-observation match. Session/provenance
    // normalization may differ, but no raw bar can disappear or be synthesized.
    let raw_observations = raw_response
        .rows
        .iter()
        .map(super::bar_market_observation_identity)
        .collect::<Result<BTreeSet<_>>>()?;
    let canonical_observations = canonical_response
        .rows
        .iter()
        .map(super::bar_market_observation_identity)
        .collect::<Result<BTreeSet<_>>>()?;
    if raw_observations.len() != raw_response.rows.len()
        || canonical_observations.len() != canonical_response.rows.len()
        || raw_response.rows.len() != canonical_response.rows.len()
        || raw_observations != canonical_observations
    {
        return Ok(false);
    }

    let mut matched_rows = 0usize;
    for phase in phases {
        let start = parse_utc_datetime(&phase.start_utc)?;
        let end = parse_utc_datetime(&phase.end_exclusive_utc)?;
        let mut rows = Vec::new();
        for row in &canonical_response.rows {
            if row.session_instance_id == phase.session_instance_id
                && parse_utc_datetime(row.bar_start_utc.as_str())? >= start
                && parse_utc_datetime(row.bar_end_exclusive_utc.as_str())? <= end
            {
                rows.push(row);
            }
        }
        rows.sort_by(|left, right| left.bar_start_utc.cmp(&right.bar_start_utc));
        if rows.is_empty() {
            return Ok(false);
        }
        matched_rows = matched_rows
            .checked_add(rows.len())
            .ok_or(CacheReconciliationError::LimitExceeded)?;

        if source_scope.effective_resolution == BarResolutionV1::OneDay {
            if rows.len() != 1
                || parse_utc_datetime(rows[0].bar_start_utc.as_str())? != start
                || parse_utc_datetime(rows[0].bar_end_exclusive_utc.as_str())? != end
            {
                return Ok(false);
            }
            continue;
        }

        let step = resolution_step(source_scope.effective_resolution)
            .ok_or(CacheReconciliationError::AdmissionMismatch)?;
        let mut cursor = start;
        for row in rows {
            let row_start = parse_utc_datetime(row.bar_start_utc.as_str())?;
            let row_end = parse_utc_datetime(row.bar_end_exclusive_utc.as_str())?;
            let step_end = cursor
                .checked_add_signed(step)
                .ok_or(CacheReconciliationError::AdmissionMismatch)?;
            let expected_end = std::cmp::min(step_end, end);
            if row_start != cursor || row_end != expected_end || row_end > end {
                return Ok(false);
            }
            cursor = row_end;
        }
        if cursor != end {
            return Ok(false);
        }
    }
    if matched_rows != canonical_response.rows.len() {
        return Ok(false);
    }

    canonical_response.coverage_status = BarsCoverageStatusV1::Complete;
    canonical_response.market_calendar_version = source_scope.market_calendar_version.clone();
    for segment in &mut canonical_response.segments {
        segment.coverage_status = BarsCoverageStatusV1::Complete;
    }
    for row in &mut canonical_response.rows {
        row.validation_state = BarsValidationStateV1::Validated;
    }
    canonical_response
        .validate()
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    request
        .validate_response(canonical_response)
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    Ok(true)
}

pub(super) fn key_request_bounds(key: &super::CacheKeyV1) -> Result<(String, String)> {
    let start = UtcTimestamp::parse_for_arrow_nanoseconds(&key.window_start_utc)
        .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    let end_exclusive = DateTime::parse_from_rfc3339(&key.window_end_exclusive_utc)
        .map_err(|_| CacheReconciliationError::UnknownOutcome)?
        .with_timezone(&Utc);
    let end = end_exclusive
        .checked_sub_signed(Duration::nanoseconds(1))
        .ok_or(CacheReconciliationError::UnknownOutcome)?
        .to_rfc3339_opts(SecondsFormat::Nanos, true);
    let end = UtcTimestamp::parse(&end).map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    Ok((start.as_str().to_owned(), end.as_str().to_owned()))
}

pub(super) fn timeframe_label_for_key(key: &super::CacheKeyV1) -> Result<String> {
    let resolution = parse_resolution(&key.effective_resolution)?;
    Ok(timeframe_for_resolution(resolution).as_str().to_owned())
}

pub(super) fn parse_resolution(value: &str) -> Result<BarResolutionV1> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| CacheReconciliationError::UnknownOutcome)
}

pub(super) fn resolution_step(resolution: BarResolutionV1) -> Option<Duration> {
    match resolution {
        BarResolutionV1::OneMinute => Some(Duration::minutes(1)),
        BarResolutionV1::FiveMinute => Some(Duration::minutes(5)),
        BarResolutionV1::FifteenMinute => Some(Duration::minutes(15)),
        BarResolutionV1::OneHour => Some(Duration::hours(1)),
        BarResolutionV1::OneDay => None,
    }
}

pub(super) fn parse_utc_datetime(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| CacheReconciliationError::InvalidInput)
}

fn valid_alpaca_provenance(
    provenance: &[market_contracts::BarSourceProvenanceV1],
    feed: &str,
) -> bool {
    !provenance.is_empty()
        && provenance.len() <= super::MAX_SOURCE_SEGMENTS
        && provenance.iter().all(|source| {
            source.provider == "alpaca"
                && source.feed == feed
                && source.entitlement_status == BarsEntitlementStatusV1::Unknown
                && source.numeric_encoding == NumericEncodingV1::DecimalToken
        })
}

/// Derive the coverage-level hash used for a page chain. The single-page form retains the
/// Broker observation hash unchanged for compatibility with current A02 producers.
pub(crate) fn alpaca_query_access_chain_observation_sha256(
    receipts: &[AlpacaQueryAccessReceiptV1],
) -> Result<String> {
    if receipts.is_empty() || receipts.len() > MAX_ALPACA_QUERY_ACCESS_PAGES {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    for receipt in receipts {
        receipt.validate()?;
    }
    if receipts.len() == 1 {
        return Ok(receipts[0].typed_response_sha256.clone());
    }
    let bytes = serde_json::to_vec(receipts)?;
    Ok(super::hash_domain(QUERY_ACCESS_CHAIN_DOMAIN, &bytes))
}

pub(crate) fn alpaca_query_access_receipts_sha256(
    receipts: &[AlpacaQueryAccessReceiptV1],
) -> Result<String> {
    if receipts.is_empty() || receipts.len() > MAX_ALPACA_QUERY_ACCESS_PAGES {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    for receipt in receipts {
        receipt.validate()?;
    }
    let bytes = serde_json::to_vec(receipts)?;
    Ok(super::hash_domain(QUERY_ACCESS_CHAIN_HASH_DOMAIN, &bytes))
}

impl std::fmt::Debug for AlpacaStockBarsQueryWitnessV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaStockBarsQueryWitnessV1")
            .field("success_class", &self.witness.success_class())
            .field("row_count", &self.witness.row_count())
            .field(
                "has_request_cursor",
                &self.expected_request_cursor_sha256.is_some(),
            )
            .field("has_next_page", &self.witness.has_next_page())
            .finish()
    }
}

/// Credential-free, locally persisted facts copied from a validated opaque Broker witness.
///
/// This receipt records a successful query attempt and binds its source-observation digest in the
/// enclosing cache version. It is not an issuer signature, entitlement proof, or completeness
/// certificate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AlpacaQueryAccessReceiptV1 {
    pub cache_key_fingerprint_sha256: String,
    pub cache_request_cursor_sha256: Option<String>,
    pub contract_id: String,
    pub method: String,
    pub origin: String,
    pub path_template: String,
    pub success_class: String,
    pub symbol: String,
    pub requested_feed: String,
    pub adjustment: String,
    pub currency: String,
    pub asof: String,
    pub timeframe: String,
    pub start_inclusive_utc: String,
    pub end_inclusive_utc: String,
    pub sort: String,
    pub page_size: u16,
    pub request_cursor_sha256: Option<String>,
    pub typed_response_sha256: String,
    pub response_observed_at_utc: String,
    pub row_count: u32,
    pub next_page_cursor_sha256: Option<String>,
    #[serde(default)]
    pub projected_raw_rows_sha256: Option<String>,
    pub source_observation_sha256: String,
}

impl AlpacaQueryAccessReceiptV1 {
    fn from_witness(
        witness: &alpaca_rest_read::AlpacaStockBarsQueryAccessWitness,
        request_cursor_sha256: Option<String>,
        projected_raw_rows_sha256: String,
    ) -> Self {
        Self {
            cache_key_fingerprint_sha256: String::new(),
            cache_request_cursor_sha256: None,
            contract_id: witness.api_contract_id().to_owned(),
            method: witness.api_method().to_owned(),
            origin: witness.api_origin().to_owned(),
            path_template: witness.api_path_template().to_owned(),
            success_class: "sdk_returned_decoded_response".to_owned(),
            symbol: witness.symbol().to_owned(),
            requested_feed: witness.requested_feed().as_str().to_owned(),
            adjustment: witness.adjustment().to_owned(),
            currency: witness.currency().to_owned(),
            asof: witness.asof().to_owned(),
            timeframe: witness.timeframe().as_str().to_owned(),
            start_inclusive_utc: witness.start_inclusive().as_str().to_owned(),
            end_inclusive_utc: witness.end_inclusive().as_str().to_owned(),
            sort: witness.sort().as_str().to_owned(),
            page_size: witness.page_size(),
            request_cursor_sha256,
            typed_response_sha256: witness.typed_response_sha256().to_owned(),
            response_observed_at_utc: witness.response_observed_at().as_str().to_owned(),
            row_count: witness.row_count(),
            next_page_cursor_sha256: witness.next_page_cursor_sha256().map(str::to_owned),
            projected_raw_rows_sha256: Some(projected_raw_rows_sha256),
            source_observation_sha256: String::new(),
        }
    }

    pub(crate) fn bind_source_observation(mut self, sha256: &str) -> Result<Self> {
        if !valid_sha256(sha256) {
            return Err(CacheReconciliationError::InvalidInput);
        }
        self.source_observation_sha256 = sha256.to_owned();
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn fingerprint(&self) -> Result<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)?;
        let mut digest = Sha256::new();
        digest.update(QUERY_ACCESS_RECEIPT_DOMAIN);
        digest.update(bytes);
        Ok(hex_digest(digest.finalize().as_slice()))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.contract_id != ALPACA_BARS_CONTRACT_ID
            || self.method != "GET"
            || self.origin != ALPACA_BARS_ORIGIN
            || self.path_template != ALPACA_BARS_PATH_TEMPLATE
            || self.success_class != "sdk_returned_decoded_response"
            || self.symbol.is_empty()
            || !matches!(self.requested_feed.as_str(), "sip" | "boats")
            || self.adjustment != "raw"
            || self.currency != "USD"
            || self.asof != "-"
            || !matches!(
                self.timeframe.as_str(),
                "1Min" | "5Min" | "15Min" | "1Hour" | "1Day"
            )
            || !matches!(self.sort.as_str(), "asc" | "desc")
            || self.page_size == 0
            || self.page_size > MAX_ALPACA_PAGE_SIZE
            || self.row_count == 0
            || self.row_count > u32::from(self.page_size)
            || !valid_sha256(&self.cache_key_fingerprint_sha256)
            || self
                .cache_request_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
            || self
                .request_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
            || !valid_sha256(&self.typed_response_sha256)
            || self
                .projected_raw_rows_sha256
                .as_deref()
                .is_none_or(|value| !valid_sha256(value))
            || !valid_sha256(&self.source_observation_sha256)
            || self
                .next_page_cursor_sha256
                .as_deref()
                .is_some_and(|value| !valid_sha256(value))
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        UtcTimestamp::parse_for_arrow_nanoseconds(&self.start_inclusive_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        UtcTimestamp::parse_for_arrow_nanoseconds(&self.end_inclusive_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        UtcTimestamp::parse_for_arrow_nanoseconds(&self.response_observed_at_utc)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        if self.start_inclusive_utc > self.end_inclusive_utc {
            return Err(CacheReconciliationError::InvalidInput);
        }
        Ok(())
    }
}

fn validate_static_witness(
    witness: &alpaca_rest_read::AlpacaStockBarsQueryAccessWitness,
) -> Result<()> {
    if witness.api_contract_id() != ALPACA_BARS_CONTRACT_ID
        || witness.api_method() != "GET"
        || witness.api_origin() != ALPACA_BARS_ORIGIN
        || witness.api_path_template() != ALPACA_BARS_PATH_TEMPLATE
        || witness.success_class()
            != alpaca_rest_read::AlpacaStockBarsQuerySuccessClass::SdkReturnedDecodedResponse
        || witness.adjustment() != "raw"
        || witness.currency() != "USD"
        || witness.asof() != "-"
        || witness.page_size() == 0
        || witness.page_size() > MAX_ALPACA_PAGE_SIZE
        || witness.row_count() == 0
        || witness.row_count() > u32::from(witness.page_size())
        || !valid_sha256(witness.typed_response_sha256())
        || witness
            .request_cursor_sha256()
            .is_some_and(|value| !valid_sha256(value))
        || witness
            .next_page_cursor_sha256()
            .is_some_and(|value| !valid_sha256(value))
    {
        return Err(CacheReconciliationError::InvalidInput);
    }
    UtcTimestamp::parse_for_arrow_nanoseconds(witness.start_inclusive().as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    UtcTimestamp::parse_for_arrow_nanoseconds(witness.end_inclusive().as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    UtcTimestamp::parse_for_arrow_nanoseconds(witness.response_observed_at().as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    Ok(())
}

fn request_bounds(request: &BarsRequestV1) -> Result<(UtcTimestamp, UtcTimestamp)> {
    let start = UtcTimestamp::parse_for_arrow_nanoseconds(request.start.as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?;
    let end_exclusive = DateTime::parse_from_rfc3339(request.end.as_str())
        .map_err(|_| CacheReconciliationError::InvalidInput)?
        .with_timezone(&Utc);
    let end_inclusive = end_exclusive
        .checked_sub_signed(Duration::nanoseconds(1))
        .ok_or(CacheReconciliationError::InvalidInput)?
        .to_rfc3339_opts(SecondsFormat::Nanos, true);
    let end =
        UtcTimestamp::parse(&end_inclusive).map_err(|_| CacheReconciliationError::InvalidInput)?;
    Ok((start, end))
}

fn timeframe_for_resolution(resolution: BarResolutionV1) -> alpaca_rest_read::StockBarsTimeframe {
    match resolution {
        BarResolutionV1::OneMinute => alpaca_rest_read::StockBarsTimeframe::Minute1,
        BarResolutionV1::FiveMinute => alpaca_rest_read::StockBarsTimeframe::Minute5,
        BarResolutionV1::FifteenMinute => alpaca_rest_read::StockBarsTimeframe::Minute15,
        BarResolutionV1::OneHour => alpaca_rest_read::StockBarsTimeframe::Hour1,
        BarResolutionV1::OneDay => alpaca_rest_read::StockBarsTimeframe::Day1,
    }
}

fn alpaca_cursor_payload_matches(
    input: &ProviderCursorCacheInputV1,
    witness: &alpaca_rest_read::AlpacaStockBarsQueryAccessWitness,
    expected_token_sha256: &str,
) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&input.payload_json) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    if object.len() != 10
        || object.get("version").and_then(serde_json::Value::as_u64) != Some(1)
        || object.get("symbol").and_then(serde_json::Value::as_str) != Some(witness.symbol())
        || object.get("timeframe").and_then(serde_json::Value::as_str)
            != Some(timeframe_for_resolution_from_witness(witness).as_str())
        || object.get("start").and_then(serde_json::Value::as_str)
            != Some(witness.start_inclusive().as_str())
        || object.get("end").and_then(serde_json::Value::as_str)
            != Some(witness.end_inclusive().as_str())
        || object.get("page_size").and_then(serde_json::Value::as_u64)
            != Some(u64::from(witness.page_size()))
        || object.get("feed").and_then(serde_json::Value::as_str)
            != Some(witness.requested_feed().as_str())
        || object.get("sort").and_then(serde_json::Value::as_str) != Some(witness.sort().as_str())
        || object.get("last_bar_timestamp").is_none()
    {
        return false;
    }
    let Some(page_token) = object.get("page_token").and_then(serde_json::Value::as_str) else {
        return false;
    };
    let mut digest = Sha256::new();
    digest.update(page_token.as_bytes());
    hex_digest(digest.finalize().as_slice()) == expected_token_sha256
}

fn timeframe_for_resolution_from_witness(
    witness: &alpaca_rest_read::AlpacaStockBarsQueryAccessWitness,
) -> alpaca_rest_read::StockBarsTimeframe {
    witness.timeframe()
}
