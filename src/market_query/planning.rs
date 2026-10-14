//! Pure query planning and trusted-calendar mapping for Core Market Bars V1.
//!
//! This module owns no HTTP, provider client, cursor, or cache behavior. It adapts the frozen Core
//! request/response contracts to the lower-level session planner, coalesces eligible intervals
//! into a small number of source-specific requests, and maps returned source timestamps back to
//! calendar identities without modifying exact OHLCV values.

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use market_contracts::{
    AdjustmentV1, AssetClassV1, BarCoverageGapV1, BarCoverageSegmentV1, BarResolutionV1, BarRowV1,
    BarSourceProvenanceV1, BarsContractErrorV1, BarsCoverageStatusV1, BarsEntitlementStatusV1,
    BarsRequestV1, BarsResponseV1, BarsValidationStateV1, CoverageGapReasonV1, ProtoTimestampV2,
    SessionScopeV1,
};
use std::cmp::{max, min};
use thiserror::Error;

use crate::session_router::{
    CalendarDayState, CalendarVerificationStatus, DataInterval, GapReason, InstrumentKind,
    PlanError, PlanRequest, PlannedSlice, PlannedSource, SessionPhase, SourceCapability,
    SourceMode, TrustedCalendarSnapshot, UtcWindow, intraday_cutoff_utc, plan_sessions,
    validate_calendar_snapshot,
};
use std::collections::BTreeMap;

const MAX_PROVIDER_GROUPS: usize = 16;
const MAX_PAGE_RANGES: usize = 200_000;

/// One coalesced source call. The interval list is authoritative; `fetch_window` is a bounded
/// envelope for provider APIs that accept only a contiguous start/end range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderQueryGroup {
    pub source: PlannedSource,
    pub resolution: BarResolutionV1,
    pub adjustment: AdjustmentV1,
    pub fetch_window: UtcWindow,
    /// Preserve newest-first provider pagination while retaining the exact intervals to filter.
    pub slices_recent_to_old: Vec<PlannedSlice>,
}

/// A time-ordered portion of a source group suitable for one cursor run.
///
/// `timeline_position` is the input offset in `MarketBarsPlan::slices_recent_to_old`; the clipped
/// group contains only the next contiguous run of actual provider-backed slices. Gaps are skipped,
/// but a different actual source ends the run so pagination cannot jump ahead of an older source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRun {
    pub group_index: usize,
    pub group: ProviderQueryGroup,
    pub timeline_position: usize,
    pub next_timeline_position: usize,
}

/// A deterministic plan for the exact Core request window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketBarsPlan {
    pub request: BarsRequestV1,
    /// The plan's time snapshot, pinned across provider pagination.
    pub as_of: ProtoTimestampV2,
    pub requested_window: UtcWindow,
    pub effective_resolution: BarResolutionV1,
    pub calendar_source_id: String,
    pub calendar_version: String,
    pub calendar_verification_status: Vec<CalendarSliceEvidence>,
    /// All in-scope slices, newest first. Slices without a source remain explicit gaps.
    pub slices_recent_to_old: Vec<PlannedSlice>,
    /// A bounded set of coalesced provider requests, newest source interval first.
    pub provider_groups: Vec<ProviderQueryGroup>,
    /// Gaps known before contacting a provider, constrained to the request window.
    pub gaps: Vec<BarCoverageGapV1>,
    pub provisional: bool,
}

/// Calendar evidence retained separately from Core's provider coverage status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CalendarSliceEvidence {
    pub trade_date: NaiveDate,
    pub session_id: String,
    pub session_instance_id: Option<String>,
    pub phase: Option<SessionPhase>,
    pub window: UtcWindow,
    pub verification_status: CalendarVerificationStatus,
    pub provisional: bool,
}

/// A provider page after strict range and trusted-calendar mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappedProviderPage {
    pub effective_resolution: BarResolutionV1,
    pub rows: Vec<BarRowV1>,
    pub segments: Vec<BarCoverageSegmentV1>,
    pub gaps: Vec<BarCoverageGapV1>,
    pub source_provenance: Vec<BarSourceProvenanceV1>,
    pub coverage_status: BarsCoverageStatusV1,
    pub as_of: Option<ProtoTimestampV2>,
    pub fetched_at: ProtoTimestampV2,
    /// Provider-supplied verification is deliberately not propagated by the mapper.
    pub calendar_version: String,
    pub continuation_cursor: Option<String>,
    pub truncated: bool,
    pub calendar_evidence: Vec<CalendarSliceEvidence>,
    pub provisional: bool,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PlanningError {
    #[error("invalid Core market bars request")]
    InvalidRequest,
    #[error("Core UTC timestamp cannot be parsed")]
    InvalidTimestamp,
    #[error("session planner rejected the request: {0}")]
    SessionPlan(#[from] PlanError),
    #[error("provider group index is outside the plan")]
    UnknownProviderGroup,
    #[error("provider timeline position is outside the plan")]
    InvalidTimelinePosition,
    #[error("provider run does not match the deterministic plan")]
    InvalidProviderRun,
    #[error("provider run contains mixed bar resolutions")]
    ProviderRunMismatch,
    #[error("provider page exceeds the bounded mapping limits")]
    ProviderPageTooLarge,
    #[error("provider page does not match its planned resolution or adjustment")]
    ProviderPageMismatch,
    #[error("provider page failed Core structural validation: {0}")]
    InvalidProviderPage(#[from] BarsContractErrorV1),
}

/// Plan a frozen Core V1 request using only the supplied, validated calendar and capabilities.
///
/// Daily windows are coalesced by actual source/feed rather than emitted as one call per NY date.
/// The provider layer may paginate each group, but must filter every page against its listed slices
/// and the original half-open UTC request window.
pub fn plan_market_bars(
    request: &BarsRequestV1,
    as_of: &ProtoTimestampV2,
    capabilities: &[SourceCapability],
    calendar: &TrustedCalendarSnapshot,
) -> Result<MarketBarsPlan, PlanningError> {
    request
        .validate()
        .map_err(|_| PlanningError::InvalidRequest)?;
    let start_utc = parse_timestamp(&request.start)?;
    let end_utc_exclusive = parse_timestamp(&request.end)?;
    let as_of_utc = parse_timestamp(as_of)?;
    let requested_interval = resolution_to_interval(request.resolution);
    let requested_window = UtcWindow {
        start: start_utc,
        end_exclusive: end_utc_exclusive,
    };
    let first_day = calendar.days.first().ok_or(PlanningError::SessionPlan(
        PlanError::InvalidCalendarCoverage,
    ))?;
    let last_day = calendar.days.last().ok_or(PlanningError::SessionPlan(
        PlanError::InvalidCalendarCoverage,
    ))?;
    let calendar_window = UtcWindow {
        start: first_day.day_window.start,
        end_exclusive: last_day.day_window.end_exclusive,
    };
    let covered_window = intersect_windows(requested_window, calendar_window);
    let source_mode = match request.provider_mode {
        market_contracts::BarsProviderModeV1::Auto => SourceMode::Auto,
        market_contracts::BarsProviderModeV1::AlpacaOnly => SourceMode::AlpacaOnly,
        market_contracts::BarsProviderModeV1::FutuOnly => SourceMode::FutuOnly,
        market_contracts::BarsProviderModeV1::Research => SourceMode::Research,
    };
    let intraday = request.resolution != BarResolutionV1::OneDay;
    let cutoff = intraday_cutoff_utc();
    let pre_cutoff_request = intraday && start_utc < cutoff;
    let research_intraday = intraday && source_mode == SourceMode::Research;
    let daily_downgrade =
        intraday && request.allow_daily_downgrade && (pre_cutoff_request || research_intraday);

    let mut plan_gaps = Vec::new();
    if start_utc < calendar_window.start {
        let end = min(end_utc_exclusive, calendar_window.start);
        if start_utc < end {
            push_bounded(
                &mut plan_gaps,
                gap_from_window(
                    UtcWindow {
                        start: start_utc,
                        end_exclusive: end,
                    },
                    CoverageGapReasonV1::Unknown,
                )?,
            )?;
        }
    }
    if end_utc_exclusive > calendar_window.end_exclusive {
        let start = max(start_utc, calendar_window.end_exclusive);
        if start < end_utc_exclusive {
            push_bounded(
                &mut plan_gaps,
                gap_from_window(
                    UtcWindow {
                        start,
                        end_exclusive: end_utc_exclusive,
                    },
                    CoverageGapReasonV1::Unknown,
                )?,
            )?;
        }
    }

    let mut planning_window = covered_window;
    if let Some(covered) = covered_window {
        if research_intraday && !request.allow_daily_downgrade {
            push_bounded(
                &mut plan_gaps,
                gap_from_window(covered, CoverageGapReasonV1::UnsupportedResolution)?,
            )?;
            planning_window = None;
        } else if pre_cutoff_request && !daily_downgrade {
            let unsupported_end = min(covered.end_exclusive, cutoff);
            if covered.start < unsupported_end {
                push_bounded(
                    &mut plan_gaps,
                    gap_from_window(
                        UtcWindow {
                            start: covered.start,
                            end_exclusive: unsupported_end,
                        },
                        CoverageGapReasonV1::UnsupportedResolution,
                    )?,
                )?;
            }
            let supported_start = max(covered.start, cutoff);
            planning_window = (supported_start < covered.end_exclusive).then_some(UtcWindow {
                start: supported_start,
                end_exclusive: covered.end_exclusive,
            });
        }
    }

    let router_plan = if let Some(window) = planning_window {
        Some(plan_sessions(PlanRequest {
            symbol: &request.symbol,
            instrument: match request.asset_class {
                AssetClassV1::Stock => InstrumentKind::UsStock,
                AssetClassV1::Etf => InstrumentKind::UsEtf,
            },
            start_utc: window.start,
            end_utc_exclusive: window.end_exclusive,
            as_of_utc,
            interval: if daily_downgrade {
                DataInterval::Daily
            } else {
                requested_interval
            },
            source_mode,
            allow_daily_downgrade: request.allow_daily_downgrade,
            capabilities,
            calendar,
        })?)
    } else {
        validate_calendar_snapshot(calendar)?;
        None
    };

    let effective_resolution = router_plan
        .as_ref()
        .map(|plan| interval_to_resolution(plan.effective_interval))
        .transpose()?
        .unwrap_or(if daily_downgrade {
            BarResolutionV1::OneDay
        } else {
            request.resolution
        });
    let mut slices_recent_to_old = Vec::new();
    let daily_scope_unsupported = effective_resolution == BarResolutionV1::OneDay
        && matches!(
            request.session_scope,
            SessionScopeV1::Extended | SessionScopeV1::Overnight
        );
    let overnight_gap_windows = missing_overnight_edge_windows(calendar);

    for mut slice in router_plan
        .as_ref()
        .into_iter()
        .flat_map(|plan| plan.slices.iter().rev().cloned())
    {
        let in_scope = effective_resolution == BarResolutionV1::OneDay
            || slice_is_in_scope(&slice, request.session_scope, &overnight_gap_windows);
        if !in_scope {
            continue;
        }
        if daily_scope_unsupported {
            slice.source = None;
            slice.source_label = None;
            slice.gap = Some(GapReason::MissingProviderCapability);
            push_bounded(
                &mut plan_gaps,
                gap_from_window(slice.window, CoverageGapReasonV1::UnsupportedResolution)?,
            )?;
        } else if let Some(reason) = slice.gap {
            push_bounded(
                &mut plan_gaps,
                gap_from_window(slice.window, gap_reason_to_core(reason))?,
            )?;
        }
        slices_recent_to_old.push(slice);
    }

    let mut provider_groups: Vec<ProviderQueryGroup> = Vec::new();
    for slice in &slices_recent_to_old {
        if slice.gap.is_some() {
            continue;
        }
        let Some(source) = slice.source else {
            continue;
        };
        if let Some(group) = provider_groups.iter_mut().find(|group| {
            group.source == source
                && group.resolution == effective_resolution
                && group.adjustment == request.adjustment
        }) {
            group.fetch_window = UtcWindow {
                start: min(group.fetch_window.start, slice.window.start),
                end_exclusive: max(group.fetch_window.end_exclusive, slice.window.end_exclusive),
            };
            group.slices_recent_to_old.push(slice.clone());
        } else {
            provider_groups.push(ProviderQueryGroup {
                source,
                resolution: effective_resolution,
                adjustment: request.adjustment,
                fetch_window: slice.window,
                slices_recent_to_old: vec![slice.clone()],
            });
        }
    }
    if provider_groups.len() > MAX_PROVIDER_GROUPS
        || slices_recent_to_old.len() > MAX_PAGE_RANGES
        || plan_gaps.len() > MAX_PAGE_RANGES
    {
        return Err(PlanningError::ProviderPageTooLarge);
    }
    provider_groups.sort_by(|left, right| {
        right
            .fetch_window
            .end_exclusive
            .cmp(&left.fetch_window.end_exclusive)
            .then_with(|| left.source.label().cmp(right.source.label()))
    });
    for group in &mut provider_groups {
        group.slices_recent_to_old.sort_by(|left, right| {
            right
                .window
                .end_exclusive
                .cmp(&left.window.end_exclusive)
                .then_with(|| right.window.start.cmp(&left.window.start))
        });
    }

    let calendar_verification_status = slices_recent_to_old
        .iter()
        .map(calendar_evidence_for_slice)
        .collect::<Vec<_>>();
    let provisional = !plan_gaps.is_empty()
        || calendar_verification_status.iter().any(|item| {
            item.provisional || item.verification_status != CalendarVerificationStatus::Verified
        });
    let gaps = normalize_gaps(plan_gaps)?;

    Ok(MarketBarsPlan {
        request: request.clone(),
        as_of: timestamp_from_datetime(as_of_utc)?,
        requested_window,
        effective_resolution,
        calendar_source_id: calendar.source_id.clone(),
        calendar_version: calendar.version.clone(),
        calendar_verification_status,
        slices_recent_to_old,
        provider_groups,
        gaps,
        provisional,
    })
}

/// Select the next source-homogeneous run from the newest-to-oldest plan timeline.
///
/// Slices with no actual provider route or an explicit gap are skipped. Such slices do not split a
/// run, allowing ordinary single-source history to coalesce across non-fetchable intervals. The
/// first different actual source ends the run and remains the next timeline position.
pub fn next_provider_run(
    plan: &MarketBarsPlan,
    timeline_position: usize,
) -> Result<Option<ProviderRun>, PlanningError> {
    let slices = &plan.slices_recent_to_old;
    if slices.len() > MAX_PAGE_RANGES || plan.provider_groups.len() > MAX_PROVIDER_GROUPS {
        return Err(PlanningError::ProviderPageTooLarge);
    }
    if timeline_position > slices.len() {
        return Err(PlanningError::InvalidTimelinePosition);
    }

    let Some(first_index) = (timeline_position..slices.len()).find(|index| {
        let slice = &slices[*index];
        slice.gap.is_none() && slice.source.is_some()
    }) else {
        return Ok(None);
    };
    let first_slice = &slices[first_index];
    let source = first_slice
        .source
        .ok_or(PlanningError::InvalidProviderRun)?;
    let resolution = interval_to_resolution(first_slice.interval)?;
    let adjustment = plan.request.adjustment;

    let mut run_slices = Vec::new();
    let mut next_position = first_index;
    while next_position < slices.len() {
        let slice = &slices[next_position];
        if slice.gap.is_none() {
            if let Some(slice_source) = slice.source {
                if slice_source != source {
                    break;
                }
                if interval_to_resolution(slice.interval)? != resolution {
                    return Err(PlanningError::ProviderRunMismatch);
                }
                run_slices.push(slice.clone());
            }
        }
        next_position += 1;
    }
    if run_slices.is_empty() {
        return Err(PlanningError::InvalidProviderRun);
    }

    let mut matching_groups = plan
        .provider_groups
        .iter()
        .enumerate()
        .filter(|(_, group)| {
            group.source == source
                && group.resolution == resolution
                && group.adjustment == adjustment
        })
        .map(|(index, _)| index);
    let group_index = matching_groups
        .next()
        .ok_or(PlanningError::UnknownProviderGroup)?;
    if matching_groups.next().is_some() {
        return Err(PlanningError::InvalidProviderRun);
    }
    let fetch_window = run_slices.iter().fold(
        UtcWindow {
            start: run_slices[0].window.start,
            end_exclusive: run_slices[0].window.end_exclusive,
        },
        |window, slice| UtcWindow {
            start: min(window.start, slice.window.start),
            end_exclusive: max(window.end_exclusive, slice.window.end_exclusive),
        },
    );
    let group = ProviderQueryGroup {
        source,
        resolution,
        adjustment,
        fetch_window,
        slices_recent_to_old: run_slices,
    };

    Ok(Some(ProviderRun {
        group_index,
        group,
        timeline_position,
        next_timeline_position: next_position,
    }))
}

/// Rebind a validated provider page to the selected source group and trusted calendar.
///
/// Provider-returned session IDs and calendar versions are treated as hints only. A row is emitted
/// only when its complete source timestamp interval fits within exactly one planned slice and the
/// original request window. No OHLCV values or timestamps are clipped or synthesized.
pub fn map_provider_page(
    plan: &MarketBarsPlan,
    group_index: usize,
    page: &BarsResponseV1,
) -> Result<MappedProviderPage, PlanningError> {
    let group = plan
        .provider_groups
        .get(group_index)
        .ok_or(PlanningError::UnknownProviderGroup)?;
    map_provider_group_page(plan, group, page)
}

/// Map one page using the clipped source run selected by [`next_provider_run`].
///
/// The run is re-derived from its input timeline position before mapping so callers cannot widen
/// the mapper to another same-source run or reorder the stable cursor timeline.
pub fn map_provider_run_page(
    plan: &MarketBarsPlan,
    run: &ProviderRun,
    page: &BarsResponseV1,
) -> Result<MappedProviderPage, PlanningError> {
    let expected =
        next_provider_run(plan, run.timeline_position)?.ok_or(PlanningError::InvalidProviderRun)?;
    if &expected != run {
        return Err(PlanningError::InvalidProviderRun);
    }
    map_provider_group_page(plan, &run.group, page)
}

fn map_provider_group_page(
    plan: &MarketBarsPlan,
    group: &ProviderQueryGroup,
    page: &BarsResponseV1,
) -> Result<MappedProviderPage, PlanningError> {
    if group.slices_recent_to_old.len() > MAX_PAGE_RANGES
        || !slices_are_recent_first(&group.slices_recent_to_old)
    {
        return Err(PlanningError::ProviderPageMismatch);
    }
    if page.rows.len() > 1000
        || page.segments.len() > MAX_PAGE_RANGES
        || page.gaps.len() > MAX_PAGE_RANGES
        || page.source_provenance.len() > MAX_PAGE_RANGES
    {
        return Err(PlanningError::ProviderPageTooLarge);
    }
    page.validate()
        .map_err(PlanningError::InvalidProviderPage)?;
    if page.effective_resolution != group.resolution || page.adjustment != group.adjustment {
        return Err(PlanningError::ProviderPageMismatch);
    }

    let provenance_by_id = page
        .source_provenance
        .iter()
        .map(|item| (item.provenance_id.as_str(), item))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut gaps = Vec::new();
    let mut segments = Vec::new();
    let mut used_provenance = std::collections::BTreeSet::new();
    let mut provisional = plan.provisional
        || page
            .market_calendar_version
            .as_deref()
            .is_some_and(|version| version != plan.calendar_version);

    for source_gap in &page.gaps {
        let source_window =
            window_from_timestamps(&source_gap.start_utc, &source_gap.end_exclusive_utc)?;
        for slice in overlapping_slices(source_window, &group.slices_recent_to_old) {
            if let Some(window) = intersect_windows(source_window, slice.window) {
                push_bounded(&mut gaps, gap_from_window(window, source_gap.reason_code)?)?;
            }
        }
    }

    for source_segment in &page.segments {
        let source_window =
            window_from_timestamps(&source_segment.start_utc, &source_segment.end_exclusive_utc)?;
        let provenance = source_segment
            .provenance_id
            .as_deref()
            .and_then(|id| provenance_by_id.get(id).copied());
        let mut source_is_trusted = false;
        if let Some(provenance) = provenance {
            match source_identity_state(group.source, provenance) {
                SourceIdentityState::Mismatch => {
                    extend_bounded(
                        &mut gaps,
                        overlap_gaps_for_row(
                            source_window,
                            &group.slices_recent_to_old,
                            CoverageGapReasonV1::ValidationConflict,
                        )?,
                    )?;
                    continue;
                }
                SourceIdentityState::Unknown => provisional = true,
                SourceIdentityState::Match => source_is_trusted = true,
            }
            if provenance.entitlement_status == BarsEntitlementStatusV1::Unauthorized {
                extend_bounded(
                    &mut gaps,
                    overlap_gaps_for_row(
                        source_window,
                        &group.slices_recent_to_old,
                        CoverageGapReasonV1::EntitlementLimited,
                    )?,
                )?;
                continue;
            }
            if provenance.entitlement_status != BarsEntitlementStatusV1::Authorized {
                provisional = true;
                source_is_trusted = false;
            }
            used_provenance.insert(provenance.provenance_id.as_str());
        } else {
            provisional = true;
        }
        for slice in overlapping_slices(source_window, &group.slices_recent_to_old) {
            if let Some(window) = intersect_windows(source_window, slice.window) {
                let Some(session_instance_id) = slice.session_instance_id.as_ref() else {
                    push_bounded(
                        &mut gaps,
                        gap_from_window(window, CoverageGapReasonV1::Unknown)?,
                    )?;
                    continue;
                };
                let mut status = source_segment.coverage_status;
                if provenance.is_none() || !source_is_trusted {
                    status = BarsCoverageStatusV1::Unknown;
                    provisional = true;
                }
                push_bounded(
                    &mut segments,
                    BarCoverageSegmentV1 {
                        start_utc: timestamp_from_datetime(window.start)?,
                        end_exclusive_utc: timestamp_from_datetime(window.end_exclusive)?,
                        coverage_status: status,
                        provenance_id: provenance.map(|item| item.provenance_id.clone()),
                        session_instance_id: session_instance_id.clone(),
                    },
                )?;
            }
        }
    }

    let mut candidates = Vec::new();
    for source_row in &page.rows {
        let row_window =
            window_from_timestamps(&source_row.bar_start_utc, &source_row.bar_end_exclusive_utc)?;
        let Some(provenance) = provenance_by_id
            .get(source_row.provenance_id.as_str())
            .copied()
        else {
            extend_bounded(
                &mut gaps,
                overlap_gaps_for_row(
                    row_window,
                    &group.slices_recent_to_old,
                    CoverageGapReasonV1::Unknown,
                )?,
            )?;
            continue;
        };
        match source_identity_state(group.source, provenance) {
            SourceIdentityState::Mismatch => {
                extend_bounded(
                    &mut gaps,
                    overlap_gaps_for_row(
                        row_window,
                        &group.slices_recent_to_old,
                        CoverageGapReasonV1::ValidationConflict,
                    )?,
                )?;
                continue;
            }
            SourceIdentityState::Unknown => provisional = true,
            SourceIdentityState::Match => {}
        }
        if provenance.entitlement_status == BarsEntitlementStatusV1::Unauthorized {
            extend_bounded(
                &mut gaps,
                overlap_gaps_for_row(
                    row_window,
                    &group.slices_recent_to_old,
                    CoverageGapReasonV1::EntitlementLimited,
                )?,
            )?;
            continue;
        }
        if provenance.entitlement_status != BarsEntitlementStatusV1::Authorized {
            provisional = true;
        }
        if source_row.validation_state == BarsValidationStateV1::Conflict {
            extend_bounded(
                &mut gaps,
                overlap_gaps_for_row(
                    row_window,
                    &group.slices_recent_to_old,
                    CoverageGapReasonV1::ValidationConflict,
                )?,
            )?;
            continue;
        }
        if source_row.symbol != plan.request.symbol
            || source_row.asset_class != plan.request.asset_class
            || source_row.resolution != group.resolution
            || source_row.adjustment != group.adjustment
        {
            extend_bounded(
                &mut gaps,
                overlap_gaps_for_row(
                    row_window,
                    &group.slices_recent_to_old,
                    CoverageGapReasonV1::ValidationConflict,
                )?,
            )?;
            continue;
        }
        if !window_contains(plan.requested_window, row_window) {
            extend_bounded(
                &mut gaps,
                overlap_gaps_for_row(
                    row_window,
                    &group.slices_recent_to_old,
                    CoverageGapReasonV1::Unknown,
                )?,
            )?;
            continue;
        }
        let matching_slices = overlapping_slices(row_window, &group.slices_recent_to_old)
            .iter()
            .filter(|slice| window_contains(slice.window, row_window))
            .collect::<Vec<_>>();
        if matching_slices.len() != 1 {
            extend_bounded(
                &mut gaps,
                overlap_gaps_for_row(
                    row_window,
                    &group.slices_recent_to_old,
                    CoverageGapReasonV1::Unknown,
                )?,
            )?;
            continue;
        }
        let slice = matching_slices[0];
        let Some(session_instance_id) = slice.session_instance_id.as_ref() else {
            extend_bounded(
                &mut gaps,
                overlap_gaps_for_row(
                    row_window,
                    &group.slices_recent_to_old,
                    CoverageGapReasonV1::Unknown,
                )?,
            )?;
            continue;
        };
        let mut row = source_row.clone();
        row.session_instance_id = session_instance_id.clone();
        if slice.provisional
            || slice.calendar_verification_status != CalendarVerificationStatus::Verified
            || provenance.entitlement_status != BarsEntitlementStatusV1::Authorized
            || !provenance_feed_is_known(group.source, provenance)
            || page
                .market_calendar_version
                .as_deref()
                .is_some_and(|version| version != plan.calendar_version)
            || source_row.validation_state == BarsValidationStateV1::Provisional
        {
            row.validation_state = BarsValidationStateV1::Provisional;
            provisional = true;
        }
        used_provenance.insert(provenance.provenance_id.as_str());
        candidates.push((row, row_window, slice));
    }

    // Conflicting or duplicate provider bars cannot be resolved by choosing an arbitrary row.
    let mut conflicted = vec![false; candidates.len()];
    for left in 0..candidates.len() {
        for right in left + 1..candidates.len() {
            if candidates[left].1.start < candidates[right].1.end_exclusive
                && candidates[right].1.start < candidates[left].1.end_exclusive
            {
                conflicted[left] = true;
                conflicted[right] = true;
                extend_bounded(
                    &mut gaps,
                    overlap_gaps_for_row(
                        candidates[left].1,
                        &group.slices_recent_to_old,
                        CoverageGapReasonV1::ValidationConflict,
                    )?,
                )?;
                extend_bounded(
                    &mut gaps,
                    overlap_gaps_for_row(
                        candidates[right].1,
                        &group.slices_recent_to_old,
                        CoverageGapReasonV1::ValidationConflict,
                    )?,
                )?;
            }
        }
    }
    let gaps = normalize_gaps(gaps)?;
    let rows = candidates
        .into_iter()
        .enumerate()
        .filter_map(|(index, (row, _, _))| (!conflicted[index]).then_some(row))
        .collect::<Vec<_>>();

    // A locally detected conflict may overlap a provider's complete segment. Remove only that
    // disputed interval; retaining unrelated source-backed coverage is safe and useful.
    segments = subtract_complete_segment_gaps(segments, &gaps)?;
    let mut sortable_segments = segments
        .into_iter()
        .map(|segment| {
            let window = window_from_timestamps(&segment.start_utc, &segment.end_exclusive_utc)?;
            Ok((window, segment))
        })
        .collect::<Result<Vec<_>, PlanningError>>()?;
    sortable_segments.sort_by_key(|(window, _)| (window.start, window.end_exclusive));
    let segments: Vec<BarCoverageSegmentV1> = sortable_segments
        .into_iter()
        .map(|(_, segment)| segment)
        .collect();

    let source_provenance = page
        .source_provenance
        .iter()
        .filter(|item| used_provenance.contains(item.provenance_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let calendar_evidence = group
        .slices_recent_to_old
        .iter()
        .map(calendar_evidence_for_slice)
        .collect::<Vec<_>>();
    provisional |= calendar_evidence.iter().any(|item| {
        item.provisional || item.verification_status != CalendarVerificationStatus::Verified
    });
    let coverage_status =
        mapped_coverage_status(page, group, &segments, &gaps, &source_provenance)?;

    Ok(MappedProviderPage {
        effective_resolution: group.resolution,
        rows,
        segments,
        gaps,
        source_provenance,
        coverage_status,
        as_of: page.as_of.clone(),
        fetched_at: page.fetched_at.clone(),
        calendar_version: plan.calendar_version.clone(),
        continuation_cursor: page.continuation_cursor.clone(),
        truncated: page.truncated,
        calendar_evidence,
        provisional,
    })
}

fn resolution_to_interval(value: BarResolutionV1) -> DataInterval {
    match value {
        BarResolutionV1::OneMinute => DataInterval::Minute(1),
        BarResolutionV1::FiveMinute => DataInterval::Minute(5),
        BarResolutionV1::FifteenMinute => DataInterval::Minute(15),
        BarResolutionV1::OneHour => DataInterval::Minute(60),
        BarResolutionV1::OneDay => DataInterval::Daily,
    }
}

fn interval_to_resolution(value: DataInterval) -> Result<BarResolutionV1, PlanningError> {
    match value {
        DataInterval::Daily => Ok(BarResolutionV1::OneDay),
        DataInterval::Minute(1) => Ok(BarResolutionV1::OneMinute),
        DataInterval::Minute(5) => Ok(BarResolutionV1::FiveMinute),
        DataInterval::Minute(15) => Ok(BarResolutionV1::FifteenMinute),
        DataInterval::Minute(60) => Ok(BarResolutionV1::OneHour),
        _ => Err(PlanningError::InvalidRequest),
    }
}

fn slice_is_in_scope(
    slice: &PlannedSlice,
    scope: SessionScopeV1,
    overnight_gap_windows: &BTreeMap<NaiveDate, Vec<UtcWindow>>,
) -> bool {
    if slice.phase.is_some() {
        return phase_is_in_scope(slice.phase, scope);
    }
    match scope {
        SessionScopeV1::All => true,
        SessionScopeV1::Regular | SessionScopeV1::Extended => false,
        SessionScopeV1::Overnight => is_day_edge_gap(slice, overnight_gap_windows),
    }
}

fn phase_is_in_scope(phase: Option<SessionPhase>, scope: SessionScopeV1) -> bool {
    match (scope, phase) {
        (SessionScopeV1::All, Some(_)) => true,
        (SessionScopeV1::Regular, Some(SessionPhase::Regular)) => true,
        (SessionScopeV1::Extended, Some(SessionPhase::Pre | SessionPhase::After)) => true,
        (SessionScopeV1::Overnight, Some(SessionPhase::Over)) => true,
        _ => false,
    }
}

/// The XNYS source has no OVER instance. Only the uncovered edge of an explicit open NY day is
/// returned as a missing-phase interval for an Overnight request; no BOATS session is inferred.
fn missing_overnight_edge_windows(
    calendar: &TrustedCalendarSnapshot,
) -> BTreeMap<NaiveDate, Vec<UtcWindow>> {
    let mut first_last: BTreeMap<NaiveDate, (DateTime<Utc>, DateTime<Utc>)> = BTreeMap::new();
    for instance in &calendar.instances {
        first_last
            .entry(instance.trade_date)
            .and_modify(|bounds| {
                bounds.0 = min(bounds.0, instance.window.start);
                bounds.1 = max(bounds.1, instance.window.end_exclusive);
            })
            .or_insert((instance.window.start, instance.window.end_exclusive));
    }
    let mut result = BTreeMap::new();
    for day in &calendar.days {
        if !matches!(day.state, CalendarDayState::Open { .. }) {
            continue;
        }
        let Some((first_start, last_end)) = first_last.get(&day.trade_date).copied() else {
            continue;
        };
        let mut windows = Vec::with_capacity(2);
        if day.day_window.start < first_start {
            windows.push(UtcWindow {
                start: day.day_window.start,
                end_exclusive: first_start,
            });
        }
        if last_end < day.day_window.end_exclusive {
            windows.push(UtcWindow {
                start: last_end,
                end_exclusive: day.day_window.end_exclusive,
            });
        }
        if !windows.is_empty() {
            result.insert(day.trade_date, windows);
        }
    }
    result
}

fn is_day_edge_gap(
    slice: &PlannedSlice,
    edge_windows: &BTreeMap<NaiveDate, Vec<UtcWindow>>,
) -> bool {
    edge_windows.get(&slice.trade_date).is_some_and(|windows| {
        windows
            .iter()
            .any(|window| window_contains(*window, slice.window))
    })
}

fn gap_reason_to_core(reason: GapReason) -> CoverageGapReasonV1 {
    match reason {
        GapReason::MissingProviderCapability => CoverageGapReasonV1::ProviderUnavailable,
        GapReason::SourceModeExcludesRequiredProvider => CoverageGapReasonV1::Unknown,
        GapReason::FutureSession => CoverageGapReasonV1::Unknown,
        GapReason::MissingCalendarPhase => CoverageGapReasonV1::Unknown,
    }
}

fn calendar_evidence_for_slice(slice: &PlannedSlice) -> CalendarSliceEvidence {
    CalendarSliceEvidence {
        trade_date: slice.trade_date,
        session_id: slice.session_id.clone(),
        session_instance_id: slice.session_instance_id.clone(),
        phase: slice.phase,
        window: slice.window,
        verification_status: slice.calendar_verification_status,
        provisional: slice.provisional,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceIdentityState {
    Match,
    Unknown,
    Mismatch,
}

fn source_identity_state(
    source: PlannedSource,
    provenance: &BarSourceProvenanceV1,
) -> SourceIdentityState {
    let expected_provider = match source {
        PlannedSource::Futu => "futu",
        PlannedSource::Alpaca { .. } => "alpaca",
        PlannedSource::YFinanceDailyResearch => "yfinance",
    };
    if is_unknown_label(&provenance.provider) {
        return SourceIdentityState::Unknown;
    }
    if !provenance.provider.eq_ignore_ascii_case(expected_provider) {
        return SourceIdentityState::Mismatch;
    }
    if let PlannedSource::Alpaca { feed } = source {
        if is_unknown_label(&provenance.feed) {
            return SourceIdentityState::Unknown;
        }
        let expected_feed = match feed {
            crate::session_router::AlpacaFeed::Sip => "sip",
            crate::session_router::AlpacaFeed::Boats => "boats",
        };
        if !provenance.feed.eq_ignore_ascii_case(expected_feed) {
            return SourceIdentityState::Mismatch;
        }
    } else if is_unknown_label(&provenance.feed) {
        return SourceIdentityState::Unknown;
    }
    SourceIdentityState::Match
}

fn provenance_feed_is_known(source: PlannedSource, provenance: &BarSourceProvenanceV1) -> bool {
    !matches!(
        source_identity_state(source, provenance),
        SourceIdentityState::Unknown
    )
}

fn is_unknown_label(value: &str) -> bool {
    value.eq_ignore_ascii_case("unknown") || value.eq_ignore_ascii_case("mixed")
}

fn overlap_gaps_for_row(
    row_window: UtcWindow,
    slices: &[PlannedSlice],
    reason: CoverageGapReasonV1,
) -> Result<Vec<BarCoverageGapV1>, PlanningError> {
    overlapping_slices(row_window, slices)
        .iter()
        .filter_map(|slice| intersect_windows(row_window, slice.window))
        .map(|window| gap_from_window(window, reason))
        .collect()
}

fn mapped_coverage_status(
    page: &BarsResponseV1,
    group: &ProviderQueryGroup,
    segments: &[BarCoverageSegmentV1],
    gaps: &[BarCoverageGapV1],
    provenance: &[BarSourceProvenanceV1],
) -> Result<BarsCoverageStatusV1, PlanningError> {
    if page.coverage_status == BarsCoverageStatusV1::Unsupported {
        return Ok(BarsCoverageStatusV1::Unsupported);
    }
    if !gaps.is_empty() {
        return Ok(BarsCoverageStatusV1::Partial);
    }
    if page.truncated {
        return Ok(if segments.is_empty() {
            BarsCoverageStatusV1::Unknown
        } else {
            BarsCoverageStatusV1::Partial
        });
    }
    let ids = provenance
        .iter()
        .map(|item| item.provenance_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let has_partial_segment = segments.iter().any(|segment| {
        segment.coverage_status == BarsCoverageStatusV1::Partial
            && segment
                .provenance_id
                .as_deref()
                .is_some_and(|id| ids.contains(id))
    });
    let mut complete_ranges = segments
        .iter()
        .filter(|segment| {
            segment.coverage_status == BarsCoverageStatusV1::Complete
                && segment
                    .provenance_id
                    .as_deref()
                    .is_some_and(|id| ids.contains(id))
        })
        .map(|segment| window_from_timestamps(&segment.start_utc, &segment.end_exclusive_utc))
        .collect::<Result<Vec<_>, _>>()?;
    complete_ranges.sort_by_key(|window| (window.start, window.end_exclusive));
    let mut fully_covered = true;
    let mut any_covered = false;
    let mut range_index = 0;
    for slice in group.slices_recent_to_old.iter().rev() {
        let slice_window = slice.window;
        let mut cursor = slice_window.start;
        while range_index < complete_ranges.len()
            && complete_ranges[range_index].end_exclusive <= slice_window.start
        {
            range_index += 1;
        }
        while range_index < complete_ranges.len()
            && complete_ranges[range_index].start < slice_window.end_exclusive
        {
            let range = complete_ranges[range_index];
            if range.start > cursor {
                fully_covered = false;
            }
            if range.start <= cursor && range.end_exclusive > cursor {
                cursor = range.end_exclusive;
                any_covered = true;
            }
            range_index += 1;
        }
        if cursor < slice_window.end_exclusive {
            fully_covered = false;
        }
    }
    if fully_covered && any_covered {
        return Ok(
            if page.coverage_status == BarsCoverageStatusV1::Empty && page.rows.is_empty() {
                BarsCoverageStatusV1::Empty
            } else {
                BarsCoverageStatusV1::Complete
            },
        );
    }
    if any_covered {
        Ok(BarsCoverageStatusV1::Partial)
    } else if has_partial_segment {
        Ok(BarsCoverageStatusV1::Partial)
    } else {
        Ok(BarsCoverageStatusV1::Unknown)
    }
}

fn subtract_complete_segment_gaps(
    segments: Vec<BarCoverageSegmentV1>,
    gaps: &[BarCoverageGapV1],
) -> Result<Vec<BarCoverageSegmentV1>, PlanningError> {
    let gap_windows = gaps
        .iter()
        .map(|gap| window_from_timestamps(&gap.start_utc, &gap.end_exclusive_utc))
        .collect::<Result<Vec<_>, _>>()?;
    let mut result = Vec::new();
    for segment in segments {
        if segment.coverage_status != BarsCoverageStatusV1::Complete {
            push_bounded(&mut result, segment)?;
            continue;
        }
        let segment_window =
            window_from_timestamps(&segment.start_utc, &segment.end_exclusive_utc)?;
        let mut gap_index =
            gap_windows.partition_point(|gap| gap.end_exclusive <= segment_window.start);
        let mut cursor = segment_window.start;
        while gap_index < gap_windows.len()
            && gap_windows[gap_index].start < segment_window.end_exclusive
        {
            let Some(cut) = intersect_windows(segment_window, gap_windows[gap_index]) else {
                gap_index += 1;
                continue;
            };
            if cut.start > cursor {
                push_bounded(
                    &mut result,
                    segment_piece(
                        &segment,
                        UtcWindow {
                            start: cursor,
                            end_exclusive: cut.start,
                        },
                    )?,
                )?;
            }
            cursor = max(cursor, cut.end_exclusive);
            gap_index += 1;
        }
        if cursor < segment_window.end_exclusive {
            push_bounded(
                &mut result,
                segment_piece(
                    &segment,
                    UtcWindow {
                        start: cursor,
                        end_exclusive: segment_window.end_exclusive,
                    },
                )?,
            )?;
        }
    }
    Ok(result)
}

fn segment_piece(
    template: &BarCoverageSegmentV1,
    window: UtcWindow,
) -> Result<BarCoverageSegmentV1, PlanningError> {
    Ok(BarCoverageSegmentV1 {
        start_utc: timestamp_from_datetime(window.start)?,
        end_exclusive_utc: timestamp_from_datetime(window.end_exclusive)?,
        coverage_status: template.coverage_status,
        provenance_id: template.provenance_id.clone(),
        session_instance_id: template.session_instance_id.clone(),
    })
}

fn normalize_gaps(gaps: Vec<BarCoverageGapV1>) -> Result<Vec<BarCoverageGapV1>, PlanningError> {
    if gaps.len() > MAX_PAGE_RANGES {
        return Err(PlanningError::ProviderPageTooLarge);
    }
    let mut events = Vec::with_capacity(gaps.len().saturating_mul(2));
    for gap in gaps {
        let window = window_from_timestamps(&gap.start_utc, &gap.end_exclusive_utc)?;
        let priority = gap_reason_priority(gap.reason_code);
        events.push((window.start, priority, true));
        events.push((window.end_exclusive, priority, false));
    }
    events.sort_unstable_by_key(|(at, priority, starts)| (*at, *priority, *starts));
    let Some((mut cursor, _, _)) = events.first().copied() else {
        return Ok(Vec::new());
    };
    let mut active = [0usize; 7];
    let mut normalized: Vec<(UtcWindow, CoverageGapReasonV1)> = Vec::new();
    let mut event_index = 0;
    while event_index < events.len() {
        let at = events[event_index].0;
        if cursor < at
            && let Some(priority) = active.iter().rposition(|count| *count > 0)
        {
            let window = UtcWindow {
                start: cursor,
                end_exclusive: at,
            };
            let reason = gap_reason_from_priority(priority as u8);
            if let Some((previous_window, previous_reason)) = normalized.last_mut()
                && *previous_reason == reason
                && previous_window.end_exclusive == window.start
            {
                previous_window.end_exclusive = window.end_exclusive;
            } else {
                normalized.push((window, reason));
                if normalized.len() > MAX_PAGE_RANGES {
                    return Err(PlanningError::ProviderPageTooLarge);
                }
            }
        }
        while event_index < events.len() && events[event_index].0 == at {
            let (_, priority, starts) = events[event_index];
            let count = &mut active[usize::from(priority)];
            if starts {
                *count = count.saturating_add(1);
            } else {
                *count = count
                    .checked_sub(1)
                    .ok_or(PlanningError::InvalidTimestamp)?;
            }
            event_index += 1;
        }
        cursor = at;
    }
    normalized
        .into_iter()
        .map(|(window, reason)| gap_from_window(window, reason))
        .collect()
}

fn gap_reason_priority(reason: CoverageGapReasonV1) -> u8 {
    match reason {
        CoverageGapReasonV1::EntitlementLimited => 6,
        CoverageGapReasonV1::ValidationConflict => 5,
        CoverageGapReasonV1::UnsupportedResolution => 4,
        CoverageGapReasonV1::ProviderUnavailable => 3,
        CoverageGapReasonV1::SourceGap => 2,
        CoverageGapReasonV1::SessionExcluded => 1,
        CoverageGapReasonV1::Unknown => 0,
    }
}

fn gap_reason_from_priority(priority: u8) -> CoverageGapReasonV1 {
    match priority {
        6 => CoverageGapReasonV1::EntitlementLimited,
        5 => CoverageGapReasonV1::ValidationConflict,
        4 => CoverageGapReasonV1::UnsupportedResolution,
        3 => CoverageGapReasonV1::ProviderUnavailable,
        2 => CoverageGapReasonV1::SourceGap,
        1 => CoverageGapReasonV1::SessionExcluded,
        _ => CoverageGapReasonV1::Unknown,
    }
}

fn gap_from_window(
    window: UtcWindow,
    reason_code: CoverageGapReasonV1,
) -> Result<BarCoverageGapV1, PlanningError> {
    Ok(BarCoverageGapV1 {
        start_utc: timestamp_from_datetime(window.start)?,
        end_exclusive_utc: timestamp_from_datetime(window.end_exclusive)?,
        reason_code,
    })
}

fn window_from_timestamps(
    start: &ProtoTimestampV2,
    end_exclusive: &ProtoTimestampV2,
) -> Result<UtcWindow, PlanningError> {
    let start = parse_timestamp(start)?;
    let end_exclusive = parse_timestamp(end_exclusive)?;
    if start >= end_exclusive {
        return Err(PlanningError::InvalidTimestamp);
    }
    Ok(UtcWindow {
        start,
        end_exclusive,
    })
}

fn parse_timestamp(value: &ProtoTimestampV2) -> Result<DateTime<Utc>, PlanningError> {
    DateTime::parse_from_rfc3339(value.as_str())
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| PlanningError::InvalidTimestamp)
}

fn timestamp_from_datetime(value: DateTime<Utc>) -> Result<ProtoTimestampV2, PlanningError> {
    ProtoTimestampV2::parse(&value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
        .map_err(|_| PlanningError::InvalidTimestamp)
}

fn window_contains(container: UtcWindow, candidate: UtcWindow) -> bool {
    container.start <= candidate.start && candidate.end_exclusive <= container.end_exclusive
}

fn intersect_windows(left: UtcWindow, right: UtcWindow) -> Option<UtcWindow> {
    let start = max(left.start, right.start);
    let end_exclusive = min(left.end_exclusive, right.end_exclusive);
    (start < end_exclusive).then_some(UtcWindow {
        start,
        end_exclusive,
    })
}

fn push_bounded<T>(target: &mut Vec<T>, value: T) -> Result<(), PlanningError> {
    if target.len() >= MAX_PAGE_RANGES {
        return Err(PlanningError::ProviderPageTooLarge);
    }
    target.push(value);
    Ok(())
}

fn extend_bounded<T>(target: &mut Vec<T>, values: Vec<T>) -> Result<(), PlanningError> {
    if target.len().saturating_add(values.len()) > MAX_PAGE_RANGES {
        return Err(PlanningError::ProviderPageTooLarge);
    }
    target.extend(values);
    Ok(())
}

fn slices_are_recent_first(slices: &[PlannedSlice]) -> bool {
    slices.windows(2).all(|pair| {
        pair[0].window.end_exclusive >= pair[1].window.end_exclusive
            && pair[1].window.end_exclusive <= pair[0].window.start
    })
}

/// Return the contiguous subset of a validated newest-first, non-overlapping slice list that may
/// intersect `query`. This keeps page-to-calendar mapping proportional to actual overlaps instead
/// of scanning every planned day for every provider row.
fn overlapping_slices<'a>(query: UtcWindow, slices: &'a [PlannedSlice]) -> &'a [PlannedSlice] {
    let end = slices.partition_point(|slice| slice.window.end_exclusive > query.start);
    let start = slices[..end].partition_point(|slice| slice.window.start >= query.end_exclusive);
    &slices[start..end]
}

#[cfg(test)]
#[path = "planning_tests.rs"]
mod tests;
