//! Contract response assembly, coverage normalization, and provider error mapping.

use std::collections::BTreeSet;

use chrono::{DateTime, SecondsFormat, Utc};
use market_contracts::{
    BarCoverageGapV1, BarCoverageSegmentV1, BarsCacheStatusV1, BarsCoverageStatusV1,
    BarsEntitlementStatusV1, BarsRequestV1, BarsResponseV1, BarsValidationStateV1,
    CoverageGapReasonV1, ProtoTimestampV2,
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
        ProviderError::MalformedResponse
        | ProviderError::ResponseTooLarge
        | ProviderError::PartialPageLimit
        | ProviderError::InvalidCorePage => CoverageGapReasonV1::ValidationConflict,
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
