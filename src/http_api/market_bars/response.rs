//! Contract response assembly, coverage normalization, and provider error mapping.

use std::collections::BTreeSet;

use chrono::{DateTime, SecondsFormat, Utc};
use exact_decimal::ExactDecimal;
use market_contracts::{
    BarCoverageGapV1, BarCoverageSegmentV1, BarRowV1, BarsCacheStatusV1, BarsCoverageStatusV1,
    BarsEntitlementStatusV1, BarsRequestV1, BarsResponseV1, BarsValidationStateV1,
    CoverageGapReasonV1, DecimalString, ProtoTimestampV2,
};

use super::current_timestamp;

const MAX_COMBINED_GAPS: usize = 400_000;
const MAX_COMBINED_SEGMENTS: usize = 400_000;

pub(super) fn response_from_page(
    request: &BarsRequestV1,
    plan: &crate::market_query::planning::MarketBarsPlan,
    selection: Option<&crate::market_query::providers::ProviderPageSelection>,
    page: BarsResponseV1,
    cache_status: BarsCacheStatusV1,
) -> crate::Result<BarsResponseV1> {
    let Some(selection) = selection else {
        return response_without_provider(request, plan, cache_status);
    };
    let mapped =
        crate::market_query::planning::map_provider_run_page(plan, selection.provider_run(), &page)
            .map_err(planning_error_to_market_data)?;
    response_from_mapped_page(
        request,
        plan,
        Some(selection),
        mapped,
        true,
        true,
        cache_status,
    )
}

/// Preserve local cache verification metadata only when the mapped response rows are an exact
/// subset of the canonical rows from a successful complete authoritative cache read. Entitlement
/// and coverage summaries stay as mapped; unknown entitlement may therefore keep coverage partial.
pub(super) fn attach_verified_at_from_cache(
    response: &mut BarsResponseV1,
    verified_at: &ProtoTimestampV2,
    expected_provider: &str,
    expected_feed: &str,
    verified_canonical_rows: &[BarRowV1],
) -> bool {
    if response.validate().is_err()
        || response.verified_at.is_some()
        || response.rows.is_empty()
        || response.truncated
        || response.continuation_cursor.is_some()
        || !response.gaps.is_empty()
        || response.segments.is_empty()
        || response.market_calendar_version.is_none()
        || !matches!(
            response.coverage_status,
            BarsCoverageStatusV1::Complete | BarsCoverageStatusV1::Partial
        )
        || matches!(
            response.entitlement_status,
            BarsEntitlementStatusV1::Unauthorized | BarsEntitlementStatusV1::Mixed
        )
        || response.provider != expected_provider
        || response.feed != expected_feed
        || response.source_provenance.is_empty()
        || response.source_provenance.iter().any(|source| {
            source.provider != expected_provider
                || source.feed != expected_feed
                || matches!(
                    source.entitlement_status,
                    BarsEntitlementStatusV1::Unauthorized | BarsEntitlementStatusV1::Mixed
                )
        })
        || response.segments.iter().any(|segment| {
            !matches!(
                segment.coverage_status,
                BarsCoverageStatusV1::Complete | BarsCoverageStatusV1::Unknown
            ) || segment.provenance_id.is_none()
        })
        || response.rows.iter().any(|row| {
            !matches!(
                row.validation_state,
                BarsValidationStateV1::Validated | BarsValidationStateV1::Provisional
            )
        })
        || verified_canonical_rows.is_empty()
        || verified_canonical_rows
            .iter()
            .any(|row| row.validation_state != BarsValidationStateV1::Validated)
    {
        return false;
    }

    let mut matched_rows = vec![false; verified_canonical_rows.len()];
    for row in &response.rows {
        let Some((index, _)) = verified_canonical_rows
            .iter()
            .enumerate()
            .find(|(index, cached)| !matched_rows[*index] && same_bar_observation(row, cached))
        else {
            return false;
        };
        matched_rows[index] = true;
    }

    response.verified_at = Some(verified_at.clone());
    if response.validate().is_err() {
        response.verified_at = None;
        return false;
    }
    true
}

fn same_bar_observation(left: &BarRowV1, right: &BarRowV1) -> bool {
    left.symbol == right.symbol
        && left.asset_class == right.asset_class
        && left.resolution == right.resolution
        && left.adjustment == right.adjustment
        && left.bar_start_utc == right.bar_start_utc
        && left.bar_end_exclusive_utc == right.bar_end_exclusive_utc
        && left.received_at_utc == right.received_at_utc
        && left.available_at_utc == right.available_at_utc
        && same_decimal_value(&left.open, &right.open)
        && same_decimal_value(&left.high, &right.high)
        && same_decimal_value(&left.low, &right.low)
        && same_decimal_value(&left.close, &right.close)
        && same_decimal_value(&left.volume, &right.volume)
        && left.provenance_id == right.provenance_id
        && left.session_instance_id == right.session_instance_id
}

fn same_decimal_value(left: &DecimalString, right: &DecimalString) -> bool {
    match (
        ExactDecimal::parse_json_number(left.as_str()),
        ExactDecimal::parse_json_number(right.as_str()),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

/// Build the cache's single-provider-run response without unrelated planner gaps or an
/// outer cross-run cursor. The source page's own gaps and continuation remain intact.
pub(super) fn response_for_cache_page(
    request: &BarsRequestV1,
    plan: &crate::market_query::planning::MarketBarsPlan,
    selection: &crate::market_query::providers::ProviderPageSelection,
    page: BarsResponseV1,
    cache_status: BarsCacheStatusV1,
) -> crate::Result<BarsResponseV1> {
    let mapped =
        crate::market_query::planning::map_provider_run_page(plan, selection.provider_run(), &page)
            .map_err(planning_error_to_market_data)?;
    response_from_mapped_page(
        request,
        plan,
        Some(selection),
        mapped,
        false,
        false,
        cache_status,
    )
}

fn response_from_mapped_page(
    request: &BarsRequestV1,
    plan: &crate::market_query::planning::MarketBarsPlan,
    selection: Option<&crate::market_query::providers::ProviderPageSelection>,
    mapped: crate::market_query::planning::MappedProviderPage,
    include_planner_gaps: bool,
    add_next_group_cursor: bool,
    cache_status: BarsCacheStatusV1,
) -> crate::Result<BarsResponseV1> {
    let mut gaps = if include_planner_gaps {
        plan.gaps.clone()
    } else {
        Vec::new()
    };
    gaps.extend(mapped.gaps.iter().cloned());
    gaps = normalize_gaps(gaps)?;
    let gap_intervals = parse_gap_intervals(&gaps)?;

    // A page bar that intersects a calendar/source gap is ambiguous even if the bar starts
    // outside it. Expand the conflict to the complete bar interval before dropping the row.
    let mut conflicting_rows = Vec::new();
    for row in &mapped.rows {
        let row_start = timestamp_datetime(&row.bar_start_utc)?;
        let row_end = timestamp_datetime(&row.bar_end_exclusive_utc)?;
        if gap_intersects(&gap_intervals, &row_start, &row_end) {
            conflicting_rows.push(BarCoverageGapV1 {
                start_utc: row.bar_start_utc.clone(),
                end_exclusive_utc: row.bar_end_exclusive_utc.clone(),
                reason_code: CoverageGapReasonV1::ValidationConflict,
            });
        }
    }
    gaps.extend(conflicting_rows);
    gaps = normalize_gaps(gaps)?;
    let gap_intervals = parse_gap_intervals(&gaps)?;
    let mut rows = Vec::with_capacity(mapped.rows.len());
    for row in mapped.rows {
        let start = timestamp_datetime(&row.bar_start_utc)?;
        let end = timestamp_datetime(&row.bar_end_exclusive_utc)?;
        if !gap_intersects(&gap_intervals, &start, &end) {
            rows.push(row);
        }
    }

    let mut cursor = mapped.continuation_cursor.clone();
    let mut truncated = mapped.truncated;
    if add_next_group_cursor
        && cursor.is_none()
        && let Some(selection) = selection
        && let Some(next_group) = selection
            .cursor_for_next_group()
            .map_err(provider_query_error_to_market_data)?
    {
        cursor = Some(next_group);
        truncated = true;
    }
    if truncated && cursor.is_none() {
        return Err(crate::MarketDataError::Provider(
            crate::error::ProviderFailure::InvalidResponse,
        ));
    }

    let mut segments = subtract_complete_segment_gaps(mapped.segments, &gaps)?;
    segments.sort_by(|left, right| left.start_utc.cmp(&right.start_utc));
    let provenance_ids = rows
        .iter()
        .map(|row| row.provenance_id.as_str())
        .chain(
            segments
                .iter()
                .filter_map(|segment| segment.provenance_id.as_deref()),
        )
        .collect::<BTreeSet<_>>();
    let source_provenance = mapped
        .source_provenance
        .into_iter()
        .filter(|source| provenance_ids.contains(source.provenance_id.as_str()))
        .collect::<Vec<_>>();
    let provisional = if include_planner_gaps {
        mapped.provisional || plan.provisional
    } else {
        mapped.calendar_evidence.iter().any(|evidence| {
            evidence.provisional
                || evidence.verification_status
                    != crate::session_router::CalendarVerificationStatus::Verified
        }) || source_provenance.iter().any(|source| {
            source.feed == "unknown"
                || source.entitlement_status != BarsEntitlementStatusV1::Authorized
        }) || rows
            .iter()
            .any(|row| row.validation_state != BarsValidationStateV1::Validated)
    };
    let mut response = BarsResponseV1 {
        schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
        provider: summarize_source(&source_provenance, |item| item.provider.as_str()),
        feed: summarize_source(&source_provenance, |item| item.feed.as_str()),
        entitlement_status: summarize_entitlement(&source_provenance),
        requested_resolution: request.resolution,
        effective_resolution: mapped.effective_resolution,
        adjustment: request.adjustment,
        rows,
        coverage_status: response_coverage_status(
            mapped.coverage_status,
            &gaps,
            truncated,
            provisional,
        ),
        segments,
        gaps,
        source_provenance,
        as_of: mapped.as_of,
        fetched_at: mapped.fetched_at,
        verified_at: None,
        cache_status,
        market_calendar_version: Some(plan.calendar_version.clone()),
        continuation_cursor: cursor,
        truncated,
    };
    if response.truncated {
        response.coverage_status = match response.coverage_status {
            BarsCoverageStatusV1::Unsupported => BarsCoverageStatusV1::Unsupported,
            _ => BarsCoverageStatusV1::Partial,
        };
    }
    response.validate().map_err(|_| {
        crate::MarketDataError::Provider(crate::error::ProviderFailure::InvalidResponse)
    })?;
    Ok(response)
}

pub(super) fn response_without_provider(
    request: &BarsRequestV1,
    plan: &crate::market_query::planning::MarketBarsPlan,
    cache_status: BarsCacheStatusV1,
) -> crate::Result<BarsResponseV1> {
    let mut gaps = plan.gaps.clone();
    if gaps.is_empty() {
        gaps.push(BarCoverageGapV1 {
            start_utc: request.start.clone(),
            end_exclusive_utc: request.end.clone(),
            reason_code: CoverageGapReasonV1::Unknown,
        });
    }
    let gaps = normalize_gaps(gaps)?;
    let all_unsupported = !gaps.is_empty()
        && gaps
            .iter()
            .all(|gap| gap.reason_code == CoverageGapReasonV1::UnsupportedResolution);
    let response = BarsResponseV1 {
        schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
        provider: "unknown".to_owned(),
        feed: "unknown".to_owned(),
        entitlement_status: BarsEntitlementStatusV1::Unknown,
        requested_resolution: request.resolution,
        effective_resolution: plan.effective_resolution,
        adjustment: request.adjustment,
        rows: Vec::new(),
        coverage_status: if all_unsupported {
            BarsCoverageStatusV1::Unsupported
        } else if gaps.is_empty() {
            BarsCoverageStatusV1::Unknown
        } else {
            BarsCoverageStatusV1::Partial
        },
        segments: Vec::new(),
        gaps,
        source_provenance: Vec::new(),
        as_of: None,
        fetched_at: current_timestamp().map_err(|_| crate::MarketDataError::InvalidInput)?,
        verified_at: None,
        cache_status,
        market_calendar_version: Some(plan.calendar_version.clone()),
        continuation_cursor: None,
        truncated: false,
    };
    response
        .validate()
        .map_err(|_| crate::MarketDataError::Contract)?;
    Ok(response)
}

pub(super) fn response_for_provider_failure(
    request: &BarsRequestV1,
    plan: &crate::market_query::planning::MarketBarsPlan,
    selection: Option<&crate::market_query::providers::ProviderPageSelection>,
    error: crate::market_query::providers::ProviderQueryError,
    cache_status: BarsCacheStatusV1,
) -> crate::Result<BarsResponseV1> {
    let Some(selection) = selection else {
        return response_without_provider(request, plan, cache_status);
    };
    let group = selection.provider_group();
    let reason = provider_failure_gap_reason(&error);
    let mut gaps = plan.gaps.clone();
    for slice in &group.slices_recent_to_old {
        gaps.push(BarCoverageGapV1 {
            start_utc: timestamp_from_datetime(slice.window.start)?,
            end_exclusive_utc: timestamp_from_datetime(slice.window.end_exclusive)?,
            reason_code: reason,
        });
    }
    let gaps = normalize_gaps(gaps)?;
    let only_unsupported = !gaps.is_empty()
        && gaps
            .iter()
            .all(|gap| gap.reason_code == CoverageGapReasonV1::UnsupportedResolution);
    let mut response =
        response_without_provider_with_gaps(request, plan, gaps, cache_status, only_unsupported)?;
    if response.coverage_status != BarsCoverageStatusV1::Unsupported {
        response.coverage_status = BarsCoverageStatusV1::Partial;
    }
    Ok(response)
}

pub(super) fn provider_failure_gap_reason(
    error: &crate::market_query::providers::ProviderQueryError,
) -> CoverageGapReasonV1 {
    use crate::market_query::providers::ProviderQueryError as ProviderError;
    match error {
        // Core V1 has no adjustment-specific gap enum. Unknown is the honest coverage reason;
        // these failures must not be described as unsupported resolution.
        ProviderError::UnsupportedAdjustment | ProviderError::AdjustmentSemanticsUnverified => {
            CoverageGapReasonV1::Unknown
        }
        ProviderError::Unauthorized | ProviderError::Forbidden => {
            CoverageGapReasonV1::EntitlementLimited
        }
        ProviderError::MalformedResponse | ProviderError::InvalidCorePage => {
            CoverageGapReasonV1::ValidationConflict
        }
        // Local page/byte bounds stop collection before the source window is exhausted. Core V1
        // has no resource-limit gap reason, and these bounds do not prove a source discontinuity
        // or a conflict in returned OHLCV data.
        ProviderError::ResponseTooLarge | ProviderError::PartialPageLimit => {
            CoverageGapReasonV1::Unknown
        }
        _ => CoverageGapReasonV1::ProviderUnavailable,
    }
}

fn response_without_provider_with_gaps(
    request: &BarsRequestV1,
    plan: &crate::market_query::planning::MarketBarsPlan,
    gaps: Vec<BarCoverageGapV1>,
    cache_status: BarsCacheStatusV1,
    unsupported: bool,
) -> crate::Result<BarsResponseV1> {
    let response = BarsResponseV1 {
        schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
        provider: "unknown".to_owned(),
        feed: "unknown".to_owned(),
        entitlement_status: BarsEntitlementStatusV1::Unknown,
        requested_resolution: request.resolution,
        effective_resolution: plan.effective_resolution,
        adjustment: request.adjustment,
        rows: Vec::new(),
        coverage_status: if unsupported {
            BarsCoverageStatusV1::Unsupported
        } else {
            BarsCoverageStatusV1::Partial
        },
        segments: Vec::new(),
        gaps,
        source_provenance: Vec::new(),
        as_of: None,
        fetched_at: current_timestamp().map_err(|_| crate::MarketDataError::InvalidInput)?,
        verified_at: None,
        cache_status,
        market_calendar_version: Some(plan.calendar_version.clone()),
        continuation_cursor: None,
        truncated: false,
    };
    response
        .validate()
        .map_err(|_| crate::MarketDataError::Contract)?;
    Ok(response)
}

fn response_coverage_status(
    source_status: BarsCoverageStatusV1,
    gaps: &[BarCoverageGapV1],
    truncated: bool,
    provisional: bool,
) -> BarsCoverageStatusV1 {
    if source_status == BarsCoverageStatusV1::Unsupported {
        return BarsCoverageStatusV1::Unsupported;
    }
    if truncated || !gaps.is_empty() || provisional {
        return BarsCoverageStatusV1::Partial;
    }
    source_status
}

fn summarize_source<'a>(
    provenance: &'a [market_contracts::BarSourceProvenanceV1],
    select: impl Fn(&'a market_contracts::BarSourceProvenanceV1) -> &'a str,
) -> String {
    let values = provenance.iter().map(select).collect::<BTreeSet<_>>();
    match values.len() {
        0 => "unknown".to_owned(),
        1 => values.into_iter().next().unwrap_or("unknown").to_owned(),
        _ => "mixed".to_owned(),
    }
}

fn summarize_entitlement(
    provenance: &[market_contracts::BarSourceProvenanceV1],
) -> BarsEntitlementStatusV1 {
    let values = provenance
        .iter()
        .map(|item| item.entitlement_status)
        .collect::<BTreeSet<_>>();
    match values.len() {
        0 => BarsEntitlementStatusV1::Unknown,
        1 => values
            .into_iter()
            .next()
            .unwrap_or(BarsEntitlementStatusV1::Unknown),
        _ => BarsEntitlementStatusV1::Mixed,
    }
}

pub(super) fn normalize_gaps(gaps: Vec<BarCoverageGapV1>) -> crate::Result<Vec<BarCoverageGapV1>> {
    if gaps.len() > MAX_COMBINED_GAPS {
        return Err(crate::MarketDataError::InputLimit);
    }
    if gaps.is_empty() {
        return Ok(Vec::new());
    }
    let mut events = Vec::with_capacity(gaps.len().saturating_mul(2));
    for gap in gaps {
        let start = timestamp_datetime(&gap.start_utc)?;
        let end = timestamp_datetime(&gap.end_exclusive_utc)?;
        if start >= end {
            return Err(crate::MarketDataError::Contract);
        }
        events.push((start, true, gap.reason_code));
        events.push((end, false, gap.reason_code));
    }
    events.sort_by_key(|event| event.0);
    let mut active = [0u32; 7];
    let mut normalized: Vec<BarCoverageGapV1> = Vec::new();
    let mut index = 0;
    while index < events.len() {
        let boundary = events[index].0;
        let mut next = index;
        while next < events.len() && events[next].0 == boundary {
            let (_, entering, reason) = events[next];
            let slot = gap_reason_slot(reason);
            if entering {
                active[slot] = active[slot].saturating_add(1);
            } else {
                active[slot] = active[slot].saturating_sub(1);
            }
            next += 1;
        }
        if let Some((next_boundary, _, _)) = events.get(next) {
            let next_boundary = *next_boundary;
            if boundary < next_boundary
                && let Some(reason) = dominant_gap_reason(&active)
            {
                let start = timestamp_from_datetime(boundary)?;
                let end = timestamp_from_datetime(next_boundary)?;
                if let Some(previous) = normalized.last_mut()
                    && previous.reason_code == reason
                    && previous.end_exclusive_utc == start
                {
                    previous.end_exclusive_utc = end;
                    index = next;
                    continue;
                }
                normalized.push(BarCoverageGapV1 {
                    start_utc: start,
                    end_exclusive_utc: end,
                    reason_code: reason,
                });
            }
        }
        index = next;
    }
    if normalized.len() > MAX_COMBINED_GAPS {
        return Err(crate::MarketDataError::InputLimit);
    }
    Ok(normalized)
}

fn dominant_gap_reason(active: &[u32; 7]) -> Option<CoverageGapReasonV1> {
    [
        CoverageGapReasonV1::ValidationConflict,
        CoverageGapReasonV1::EntitlementLimited,
        CoverageGapReasonV1::UnsupportedResolution,
        CoverageGapReasonV1::ProviderUnavailable,
        CoverageGapReasonV1::SourceGap,
        CoverageGapReasonV1::SessionExcluded,
        CoverageGapReasonV1::Unknown,
    ]
    .into_iter()
    .find(|reason| active[gap_reason_slot(*reason)] > 0)
}

const fn gap_reason_slot(reason: CoverageGapReasonV1) -> usize {
    match reason {
        CoverageGapReasonV1::SourceGap => 0,
        CoverageGapReasonV1::ProviderUnavailable => 1,
        CoverageGapReasonV1::EntitlementLimited => 2,
        CoverageGapReasonV1::UnsupportedResolution => 3,
        CoverageGapReasonV1::SessionExcluded => 4,
        CoverageGapReasonV1::ValidationConflict => 5,
        CoverageGapReasonV1::Unknown => 6,
    }
}

pub(super) fn subtract_complete_segment_gaps(
    segments: Vec<BarCoverageSegmentV1>,
    gaps: &[BarCoverageGapV1],
) -> crate::Result<Vec<BarCoverageSegmentV1>> {
    if segments.len() > MAX_COMBINED_SEGMENTS {
        return Err(crate::MarketDataError::InputLimit);
    }
    let gap_intervals = parse_gap_intervals(gaps)?;
    let mut segments = segments;
    segments.sort_by(|left, right| left.start_utc.cmp(&right.start_utc));
    let mut result = Vec::with_capacity(segments.len());
    for segment in segments {
        if segment.coverage_status != BarsCoverageStatusV1::Complete {
            result.push(segment);
            continue;
        }
        let start = timestamp_datetime(&segment.start_utc)?;
        let end = timestamp_datetime(&segment.end_exclusive_utc)?;
        let mut cursor = start;
        let mut gap_index = gap_intervals.partition_point(|(_, gap_end)| gap_end <= &start);
        while let Some((gap_start, gap_end)) = gap_intervals.get(gap_index) {
            if gap_start >= &end {
                break;
            }
            if &cursor < gap_start {
                let mut remaining = segment.clone();
                remaining.start_utc = timestamp_from_datetime(cursor)?;
                remaining.end_exclusive_utc =
                    timestamp_from_datetime(std::cmp::min(*gap_start, end))?;
                result.push(remaining);
            }
            cursor = std::cmp::max(cursor, *gap_end);
            if cursor >= end {
                break;
            }
            gap_index += 1;
        }
        if cursor < end {
            let mut remaining = segment;
            remaining.start_utc = timestamp_from_datetime(cursor)?;
            result.push(remaining);
        }
    }
    if result.len() > MAX_COMBINED_SEGMENTS {
        return Err(crate::MarketDataError::InputLimit);
    }
    Ok(result)
}

fn parse_gap_intervals(
    gaps: &[BarCoverageGapV1],
) -> crate::Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    gaps.iter()
        .map(|gap| {
            let start = timestamp_datetime(&gap.start_utc)?;
            let end = timestamp_datetime(&gap.end_exclusive_utc)?;
            if start >= end {
                return Err(crate::MarketDataError::Contract);
            }
            Ok((start, end))
        })
        .collect()
}

fn gap_intersects(
    gaps: &[(DateTime<Utc>, DateTime<Utc>)],
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> bool {
    let index = gaps.partition_point(|(_, gap_end)| gap_end <= start);
    gaps.get(index)
        .is_some_and(|(gap_start, _)| gap_start < end)
}

fn timestamp_datetime(value: &ProtoTimestampV2) -> crate::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.as_str())
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| crate::MarketDataError::Contract)
}

fn timestamp_from_datetime(value: DateTime<Utc>) -> crate::Result<ProtoTimestampV2> {
    let value = value.to_rfc3339_opts(SecondsFormat::Nanos, true);
    ProtoTimestampV2::parse(&value).map_err(|_| crate::MarketDataError::Contract)
}

pub(super) fn planning_error_to_market_data(
    error: crate::market_query::planning::PlanningError,
) -> crate::MarketDataError {
    use crate::market_query::planning::PlanningError;
    match error {
        PlanningError::InvalidRequest | PlanningError::InvalidTimestamp => {
            crate::MarketDataError::InvalidInput
        }
        PlanningError::ProviderPageTooLarge => crate::MarketDataError::InputLimit,
        PlanningError::SessionPlan(_) => crate::MarketDataError::Contract,
        PlanningError::UnknownProviderGroup
        | PlanningError::InvalidTimelinePosition
        | PlanningError::InvalidProviderRun
        | PlanningError::ProviderRunMismatch
        | PlanningError::ProviderPageMismatch
        | PlanningError::InvalidProviderPage(_) => {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::InvalidResponse)
        }
    }
}

pub(super) fn provider_query_error_to_market_data(
    error: crate::market_query::providers::ProviderQueryError,
) -> crate::MarketDataError {
    use crate::market_query::providers::ProviderQueryError as ProviderError;
    match error {
        ProviderError::InvalidCursor
        | ProviderError::CursorQueryMismatch
        | ProviderError::CursorPlanChanged => crate::MarketDataError::InvalidInput,
        ProviderError::CursorExpired => {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::HttpStatus(410))
        }
        ProviderError::Unauthorized => {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::Unauthorized)
        }
        ProviderError::Forbidden => {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::Forbidden)
        }
        ProviderError::RateLimited => {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::RateLimited)
        }
        ProviderError::Timeout => {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::HttpStatus(504))
        }
        ProviderError::UnsupportedAdjustment | ProviderError::AdjustmentSemanticsUnverified => {
            crate::MarketDataError::Contract
        }
        ProviderError::ProviderDisabled
        | ProviderError::MissingCredentials
        | ProviderError::GatewayDelegationMissing
        | ProviderError::ProviderUnavailable
        | ProviderError::ProviderGroupUnavailable
        | ProviderError::ProviderCapabilityUnavailable => {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::Transport)
        }
        ProviderError::InvalidConfiguration => crate::MarketDataError::InvalidInput,
        ProviderError::MalformedResponse
        | ProviderError::EmptyResearchObservation
        | ProviderError::ResponseTooLarge
        | ProviderError::PartialPageLimit
        | ProviderError::InvalidCorePage => {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::InvalidResponse)
        }
    }
}

pub(super) fn provider_error_is_http_fatal(
    error: crate::market_query::providers::ProviderQueryError,
) -> bool {
    use crate::market_query::providers::ProviderQueryError as ProviderError;
    matches!(
        error,
        ProviderError::InvalidCursor
            | ProviderError::CursorExpired
            | ProviderError::CursorQueryMismatch
            | ProviderError::CursorPlanChanged
            | ProviderError::Unauthorized
            | ProviderError::Forbidden
            | ProviderError::RateLimited
            | ProviderError::Timeout
    )
}

#[cfg(test)]
mod tests {
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarCoverageGapV1, BarCoverageSegmentV1, BarResolutionV1,
        BarRowV1, BarSourceProvenanceV1, BarsCacheStatusV1, BarsCoverageStatusV1,
        BarsEntitlementStatusV1, BarsResponseV1, BarsValidationStateV1, CoverageGapReasonV1,
        DecimalString, NumericEncodingV1, ProtoTimestampV2,
    };

    use crate::market_query::providers::ProviderQueryError;

    use super::{attach_verified_at_from_cache, provider_failure_gap_reason};

    fn timestamp(value: &str) -> ProtoTimestampV2 {
        ProtoTimestampV2::parse(value).expect("synthetic timestamp fixture")
    }

    fn synthetic_unknown_entitlement_response() -> (BarsResponseV1, BarRowV1) {
        // This fixture exercises response metadata gates only. It is not provider evidence.
        let row = BarRowV1 {
            symbol: "SPY".to_owned(),
            asset_class: AssetClassV1::Etf,
            resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            bar_start_utc: timestamp("2026-01-05T14:30:00Z"),
            bar_end_exclusive_utc: timestamp("2026-01-05T14:31:00Z"),
            received_at_utc: timestamp("2026-01-05T14:31:00Z"),
            available_at_utc: timestamp("2026-01-05T14:31:00Z"),
            open: DecimalString::new("10.00").expect("valid decimal fixture"),
            high: DecimalString::new("11.00").expect("valid decimal fixture"),
            low: DecimalString::new("9.00").expect("valid decimal fixture"),
            close: DecimalString::new("10.50").expect("valid decimal fixture"),
            volume: DecimalString::new("100").expect("valid decimal fixture"),
            provenance_id: "synthetic-alpaca-page".to_owned(),
            session_instance_id: "XNYS:2026-01-05:regular".to_owned(),
            validation_state: BarsValidationStateV1::Provisional,
        };
        let response = BarsResponseV1 {
            schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
            provider: "alpaca".to_owned(),
            feed: "sip".to_owned(),
            entitlement_status: BarsEntitlementStatusV1::Unknown,
            requested_resolution: BarResolutionV1::OneMinute,
            effective_resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            rows: vec![row.clone()],
            coverage_status: BarsCoverageStatusV1::Partial,
            segments: vec![BarCoverageSegmentV1 {
                start_utc: row.bar_start_utc.clone(),
                end_exclusive_utc: row.bar_end_exclusive_utc.clone(),
                coverage_status: BarsCoverageStatusV1::Unknown,
                provenance_id: Some(row.provenance_id.clone()),
                session_instance_id: row.session_instance_id.clone(),
            }],
            gaps: Vec::new(),
            source_provenance: vec![BarSourceProvenanceV1 {
                provenance_id: row.provenance_id.clone(),
                provider: "alpaca".to_owned(),
                feed: "sip".to_owned(),
                source: "Alpaca SIP".to_owned(),
                entitlement_status: BarsEntitlementStatusV1::Unknown,
                numeric_encoding: NumericEncodingV1::DecimalToken,
            }],
            as_of: None,
            fetched_at: timestamp("2026-01-05T14:31:00Z"),
            verified_at: None,
            cache_status: BarsCacheStatusV1::Hit,
            market_calendar_version: Some("synthetic-calendar-v1".to_owned()),
            continuation_cursor: None,
            truncated: false,
        };
        response
            .validate()
            .expect("valid synthetic response fixture");
        let mut verified_row = row.clone();
        verified_row.validation_state = BarsValidationStateV1::Validated;
        (response, verified_row)
    }

    #[test]
    fn unknown_adjustment_semantics_are_not_reported_as_resolution_failures() {
        for error in [
            ProviderQueryError::UnsupportedAdjustment,
            ProviderQueryError::AdjustmentSemanticsUnverified,
        ] {
            assert_eq!(
                provider_failure_gap_reason(&error),
                CoverageGapReasonV1::Unknown
            );
        }
    }

    #[test]
    fn authoritative_cache_timestamp_preserves_unknown_entitlement_and_partial_status() {
        let (mut response, verified_row) = synthetic_unknown_entitlement_response();
        let verified_at = timestamp("2026-01-05T14:31:01Z");

        assert!(attach_verified_at_from_cache(
            &mut response,
            &verified_at,
            "alpaca",
            "sip",
            &[verified_row],
        ));

        assert_eq!(response.verified_at, Some(verified_at));
        assert_eq!(
            response.entitlement_status,
            BarsEntitlementStatusV1::Unknown
        );
        assert_eq!(response.coverage_status, BarsCoverageStatusV1::Partial);
        assert_eq!(
            response.segments[0].coverage_status,
            BarsCoverageStatusV1::Unknown
        );
    }

    #[test]
    fn exact_decimal_normalization_keeps_cache_verification_but_minimal_value_change_rejects_it() {
        let (mut response, mut canonical_row) = synthetic_unknown_entitlement_response();
        let verified_at = timestamp("2026-01-05T14:31:01Z");
        canonical_row.open = DecimalString::new("10").expect("normalized open fixture");
        canonical_row.high = DecimalString::new("11").expect("normalized high fixture");
        canonical_row.low = DecimalString::new("9").expect("normalized low fixture");
        canonical_row.close = DecimalString::new("10.5").expect("normalized close fixture");
        response.rows[0].volume =
            DecimalString::new("9007199254740993.00").expect("large exact volume fixture");
        canonical_row.volume =
            DecimalString::new("9007199254740993").expect("normalized large volume fixture");
        canonical_row.validation_state = BarsValidationStateV1::Validated;

        assert!(attach_verified_at_from_cache(
            &mut response,
            &verified_at,
            "alpaca",
            "sip",
            std::slice::from_ref(&canonical_row),
        ));
        assert_eq!(response.verified_at, Some(verified_at.clone()));
        assert_eq!(
            response.entitlement_status,
            BarsEntitlementStatusV1::Unknown
        );
        assert_eq!(response.coverage_status, BarsCoverageStatusV1::Partial);

        let (mut changed_response, mut changed_canonical) =
            synthetic_unknown_entitlement_response();
        changed_canonical.validation_state = BarsValidationStateV1::Validated;
        changed_response.rows[0].close =
            DecimalString::new("10.51").expect("minimal decimal difference fixture");
        assert!(!attach_verified_at_from_cache(
            &mut changed_response,
            &verified_at,
            "alpaca",
            "sip",
            std::slice::from_ref(&changed_canonical),
        ));
        assert_eq!(changed_response.verified_at, None);
    }

    #[test]
    fn incomplete_or_mismatched_mapped_rows_do_not_receive_cache_verification_time() {
        let verified_at = timestamp("2026-01-05T14:31:01Z");

        let (mut response, verified_row) = synthetic_unknown_entitlement_response();
        response.gaps.push(BarCoverageGapV1 {
            start_utc: timestamp("2026-01-05T14:31:00Z"),
            end_exclusive_utc: timestamp("2026-01-05T14:32:00Z"),
            reason_code: CoverageGapReasonV1::SourceGap,
        });
        response.validate().expect("valid synthetic gap fixture");
        assert!(!attach_verified_at_from_cache(
            &mut response,
            &verified_at,
            "alpaca",
            "sip",
            std::slice::from_ref(&verified_row),
        ));
        assert_eq!(response.verified_at, None);

        let (mut response, verified_row) = synthetic_unknown_entitlement_response();
        response.truncated = true;
        response.continuation_cursor = Some("synthetic-next-page".to_owned());
        response.validate().expect("valid synthetic cursor fixture");
        assert!(!attach_verified_at_from_cache(
            &mut response,
            &verified_at,
            "alpaca",
            "sip",
            std::slice::from_ref(&verified_row),
        ));
        assert_eq!(response.verified_at, None);

        let (mut response, verified_row) = synthetic_unknown_entitlement_response();
        response.rows[0].close = DecimalString::new("10.75").expect("valid decimal fixture");
        response.validate().expect("valid synthetic row mutation");
        assert!(!attach_verified_at_from_cache(
            &mut response,
            &verified_at,
            "alpaca",
            "sip",
            &[verified_row],
        ));
        assert_eq!(response.verified_at, None);

        let (mut response, verified_row) = synthetic_unknown_entitlement_response();
        assert!(!attach_verified_at_from_cache(
            &mut response,
            &verified_at,
            "alpaca",
            "boats",
            &[verified_row],
        ));
        assert_eq!(response.verified_at, None);
    }
}
