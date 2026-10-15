//! Cold-read structural validation for the locally persisted Alpaca query lineage.

use std::collections::{BTreeMap, BTreeSet};

use market_contracts::{AdjustmentV1, BarResolutionV1, EntitlementState, NumericEncodingV1};

use super::{
    AlpacaQueryAccessReceiptV1, MAX_ALPACA_QUERY_ACCESS_PAGES,
    alpaca_query_access_chain_observation_sha256, key_request_bounds, parse_resolution,
    parse_utc_datetime, resolution_step, timeframe_label_for_key,
};
use crate::archive::reconciliation::{
    CacheKeyV1, CacheReconciliationError, CoverageEvidenceV1, Result,
    authority::{ProviderPhaseSliceV1, validate_phase_slices},
    bar_market_observation_identity,
};

/// Revalidate persisted query receipts against their exact cache key and read-back rows. This
/// proves local lineage integrity only; a live same-process lease is checked separately before a
/// cache hit is returned.
pub(crate) fn validate_stored_query_access_receipts(
    key: &CacheKeyV1,
    coverage: &CoverageEvidenceV1,
    raw_rows: &[market_contracts::BarRowV1],
    canonical_rows: &[market_contracts::BarRowV1],
    receipts: &[AlpacaQueryAccessReceiptV1],
    full_provider_chain: bool,
    phase_slices: &[ProviderPhaseSliceV1],
) -> Result<()> {
    if receipts.is_empty() || receipts.len() > MAX_ALPACA_QUERY_ACCESS_PAGES {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    let (expected_start, expected_end) = key_request_bounds(key)?;
    let first = receipts
        .first()
        .ok_or(CacheReconciliationError::UnknownOutcome)?;
    let last = receipts
        .last()
        .ok_or(CacheReconciliationError::UnknownOutcome)?;
    let timeframe = timeframe_label_for_key(key)?;
    let expected_key_fingerprint = key.fingerprint()?;
    let expected_observation_sha256 = alpaca_query_access_chain_observation_sha256(receipts)
        .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    let mut row_counts = BTreeMap::<String, u64>::new();
    let mut receipt_observation_times = BTreeSet::new();
    let mut total_rows = 0u64;
    let mut previous_next: Option<&str> = None;
    for (index, receipt) in receipts.iter().enumerate() {
        receipt
            .validate()
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        if !receipt_observation_times.insert(receipt.response_observed_at_utc.clone()) {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        if receipt.symbol != key.symbol
            || receipt.requested_feed != key.feed
            || receipt.cache_key_fingerprint_sha256 != expected_key_fingerprint
            || receipt.cache_request_cursor_sha256 != key.request_cursor_sha256
            || receipt.adjustment != "raw"
            || receipt.timeframe != timeframe
            || receipt.start_inclusive_utc != expected_start
            || receipt.end_inclusive_utc != expected_end
            || receipt.source_observation_sha256 != receipt.typed_response_sha256
            || receipt
                .request_cursor_sha256
                .as_deref()
                .is_some_and(|value| !super::super::valid_sha256(value))
            || index > 0 && previous_next != receipt.request_cursor_sha256.as_deref()
            || index + 1 < receipts.len() && receipt.next_page_cursor_sha256.is_none()
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        total_rows = total_rows
            .checked_add(u64::from(receipt.row_count))
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        let page_rows = row_counts
            .entry(receipt.response_observed_at_utc.clone())
            .or_default();
        *page_rows = page_rows
            .checked_add(u64::from(receipt.row_count))
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        previous_next = receipt.next_page_cursor_sha256.as_deref();
    }
    let provider_chain_is_full =
        first.request_cursor_sha256.is_none() && last.next_page_cursor_sha256.is_none();
    let expected_adjustment = super::super::enum_label(&AdjustmentV1::Unadjusted)?;
    if !matches!(key.asset_class.as_str(), "stock" | "etf")
        || key.provider != "alpaca"
        || !matches!(key.feed.as_str(), "sip" | "boats")
        || key.entitlement != EntitlementState::Unknown
        || key.numeric_encoding != NumericEncodingV1::DecimalToken.as_str()
        || key.adjustment != expected_adjustment
        || full_provider_chain != provider_chain_is_full
        || total_rows != raw_rows.len() as u64
        || coverage.pages_seen != Some(receipts.len() as u32)
        || coverage.pages_exhausted != Some(last.next_page_cursor_sha256.is_none())
        || coverage.truncated != last.next_page_cursor_sha256.is_some()
        || coverage.response_observed_at_utc != last.response_observed_at_utc
        || coverage
            .segments
            .iter()
            .any(|segment| segment.source_observation_sha256 != expected_observation_sha256)
    {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    for row in raw_rows {
        let count = row_counts
            .get_mut(row.received_at_utc.as_str())
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
        *count = count
            .checked_sub(1)
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
    }
    if row_counts.values().any(|count| *count != 0) {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    let mut rows_by_observation = BTreeMap::<String, Vec<&market_contracts::BarRowV1>>::new();
    for row in raw_rows {
        rows_by_observation
            .entry(row.received_at_utc.as_str().to_owned())
            .or_default()
            .push(row);
    }
    if rows_by_observation.len() != receipts.len() {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    for receipt in receipts {
        let rows = rows_by_observation
            .get(&receipt.response_observed_at_utc)
            .ok_or(CacheReconciliationError::UnknownOutcome)?;
        let projection_sha256 = super::projected_raw_row_refs_sha256(rows.iter().copied())
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        if rows.len()
            != usize::try_from(receipt.row_count)
                .map_err(|_| CacheReconciliationError::UnknownOutcome)?
            || receipt.projected_raw_rows_sha256.as_deref() != Some(projection_sha256.as_str())
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
    }
    if coverage.is_complete_for_key(key) {
        if !full_provider_chain
            || coverage.coverage_gap_count != Some(0)
            || coverage.truncated
            || phase_slices.is_empty()
            || raw_rows.len() != canonical_rows.len()
            || raw_rows
                .iter()
                .map(bar_market_observation_identity)
                .collect::<Result<BTreeSet<_>>>()?
                != canonical_rows
                    .iter()
                    .map(bar_market_observation_identity)
                    .collect::<Result<BTreeSet<_>>>()?
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        validate_stored_timeline(key, coverage, canonical_rows, phase_slices)?;
    }
    Ok(())
}

fn validate_stored_timeline(
    key: &CacheKeyV1,
    coverage: &CoverageEvidenceV1,
    rows: &[market_contracts::BarRowV1],
    phases: &[ProviderPhaseSliceV1],
) -> Result<()> {
    let coverage_sessions = coverage
        .segments
        .iter()
        .map(|segment| segment.session_instance_id.as_str())
        .collect::<BTreeSet<_>>();
    let phase_sessions = phases
        .iter()
        .map(|phase| phase.session_instance_id.as_str())
        .collect::<BTreeSet<_>>();
    if phase_sessions != coverage_sessions || key.market_calendar_version.is_none() {
        return Err(CacheReconciliationError::UnknownOutcome);
    }
    validate_phase_slices(
        phases,
        &phases
            .iter()
            .map(|phase| phase.session_instance_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
        &key.window_start_utc,
        &key.window_end_exclusive_utc,
    )
    .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
    for phase in phases {
        let start = parse_utc_datetime(&phase.start_utc)?;
        let end = parse_utc_datetime(&phase.end_exclusive_utc)?;
        let mut phase_rows = Vec::new();
        for row in rows {
            if row.session_instance_id == phase.session_instance_id
                && parse_utc_datetime(row.bar_start_utc.as_str())? >= start
                && parse_utc_datetime(row.bar_end_exclusive_utc.as_str())? <= end
            {
                phase_rows.push(row);
            }
        }
        phase_rows.sort_by(|left, right| left.bar_start_utc.cmp(&right.bar_start_utc));
        if phase_rows.is_empty() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        if key.effective_resolution == BarResolutionV1::OneDay.as_str() {
            if phase_rows.len() != 1
                || parse_utc_datetime(phase_rows[0].bar_start_utc.as_str())? != start
                || parse_utc_datetime(phase_rows[0].bar_end_exclusive_utc.as_str())? != end
            {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            continue;
        }
        let resolution = parse_resolution(&key.effective_resolution)?;
        let step = resolution_step(resolution).ok_or(CacheReconciliationError::UnknownOutcome)?;
        let mut cursor = start;
        for row in phase_rows {
            let row_start = parse_utc_datetime(row.bar_start_utc.as_str())?;
            let row_end = parse_utc_datetime(row.bar_end_exclusive_utc.as_str())?;
            let step_end = cursor
                .checked_add_signed(step)
                .ok_or(CacheReconciliationError::UnknownOutcome)?;
            let expected_end = std::cmp::min(step_end, end);
            if row_start != cursor || row_end != expected_end || row_end > end {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            cursor = row_end;
        }
        if cursor != end {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
    }
    Ok(())
}
