use chrono::{DateTime, Duration, SecondsFormat, Utc};
use market_contracts::{
    AdjustmentV1, BarCoverageGapV1, BarCoverageSegmentV1, BarResolutionV1, BarRowV1,
    BarSourceProvenanceV1, BarsCacheStatusV1, BarsCoverageStatusV1, BarsEntitlementStatusV1,
    BarsRequestV1, BarsResponseV1, BarsValidationStateV1, CoverageGapReasonV1, NumericEncodingV1,
    ProtoTimestampV2,
};

use crate::market_query::planning::{MarketBarsPlan, ProviderQueryGroup};

use super::error::ProviderQueryError;

pub(super) const PROVIDER_LOCAL_SESSION_ID: &str = "provider-local";

pub(super) fn now_proto() -> Result<ProtoTimestampV2, ProviderQueryError> {
    proto_from_datetime(Utc::now())
}

pub(super) fn proto_from_datetime(
    value: DateTime<Utc>,
) -> Result<ProtoTimestampV2, ProviderQueryError> {
    ProtoTimestampV2::parse(&value.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .map_err(|_| ProviderQueryError::MalformedResponse)
}

pub(super) fn parse_proto(value: &ProtoTimestampV2) -> Result<DateTime<Utc>, ProviderQueryError> {
    DateTime::parse_from_rfc3339(value.as_str())
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ProviderQueryError::InvalidCorePage)
}

pub(super) fn format_utc(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

pub(super) fn next_bar_end(
    start: DateTime<Utc>,
    resolution: BarResolutionV1,
    group: &ProviderQueryGroup,
) -> Result<DateTime<Utc>, ProviderQueryError> {
    let end = match resolution {
        BarResolutionV1::OneMinute => start.checked_add_signed(Duration::minutes(1)),
        BarResolutionV1::FiveMinute => start.checked_add_signed(Duration::minutes(5)),
        BarResolutionV1::FifteenMinute => start.checked_add_signed(Duration::minutes(15)),
        BarResolutionV1::OneHour => start.checked_add_signed(Duration::hours(1)),
        BarResolutionV1::OneDay => group
            .slices_recent_to_old
            .iter()
            .find(|slice| slice.window.start == start)
            .map(|slice| slice.window.end_exclusive),
    };
    end.ok_or(ProviderQueryError::MalformedResponse)
}

pub(super) fn build_page(
    plan: &MarketBarsPlan,
    group: &ProviderQueryGroup,
    provenance: BarSourceProvenanceV1,
    rows: Vec<BarRowV1>,
    mut gaps: Vec<BarCoverageGapV1>,
    fetched_at: ProtoTimestampV2,
    continuation_cursor: Option<String>,
) -> Result<BarsResponseV1, ProviderQueryError> {
    gaps.sort_by(|left, right| {
        left.start_utc
            .cmp(&right.start_utc)
            .then_with(|| left.end_exclusive_utc.cmp(&right.end_exclusive_utc))
    });
    gaps = merge_overlapping_gaps(gaps)?;

    let mut segments = rows
        .iter()
        .map(|row| BarCoverageSegmentV1 {
            start_utc: row.bar_start_utc.clone(),
            end_exclusive_utc: row.bar_end_exclusive_utc.clone(),
            coverage_status: BarsCoverageStatusV1::Unknown,
            provenance_id: Some(provenance.provenance_id.clone()),
            session_instance_id: row.session_instance_id.clone(),
        })
        .collect::<Vec<_>>();
    segments.sort_by(|left, right| {
        left.start_utc
            .cmp(&right.start_utc)
            .then_with(|| left.end_exclusive_utc.cmp(&right.end_exclusive_utc))
    });
    if segments
        .windows(2)
        .any(|pair| pair[0].end_exclusive_utc > pair[1].start_utc)
    {
        return Err(ProviderQueryError::MalformedResponse);
    }

    let truncated = continuation_cursor.is_some();
    let coverage_status = if truncated || !gaps.is_empty() {
        BarsCoverageStatusV1::Partial
    } else {
        // A historical endpoint's lack of a continuation token is not a completeness receipt.
        BarsCoverageStatusV1::Unknown
    };
    let response = BarsResponseV1 {
        schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
        provider: provenance.provider.clone(),
        feed: provenance.feed.clone(),
        entitlement_status: provenance.entitlement_status,
        requested_resolution: plan.request.resolution,
        effective_resolution: group.resolution,
        adjustment: group.adjustment,
        rows,
        coverage_status,
        segments,
        gaps,
        source_provenance: vec![provenance],
        // The planner's as_of is a fixed routing clock, not a provider data watermark.
        as_of: None,
        fetched_at,
        verified_at: None,
        cache_status: BarsCacheStatusV1::Unknown,
        market_calendar_version: None,
        continuation_cursor,
        truncated,
    };
    if response.validate().is_err() {
        return Err(ProviderQueryError::InvalidCorePage);
    }
    Ok(response)
}

pub(super) fn make_row(
    request: &BarsRequestV1,
    resolution: BarResolutionV1,
    adjustment: AdjustmentV1,
    bar_start: DateTime<Utc>,
    bar_end: DateTime<Utc>,
    received_at: ProtoTimestampV2,
    available_at: ProtoTimestampV2,
    open: &str,
    high: &str,
    low: &str,
    close: &str,
    volume: &str,
    provenance_id: &str,
) -> Result<BarRowV1, ProviderQueryError> {
    let decimal = |value: &str| {
        market_contracts::DecimalString::new(value.to_owned())
            .map_err(|_| ProviderQueryError::MalformedResponse)
    };
    let row = BarRowV1 {
        symbol: request.symbol.clone(),
        asset_class: request.asset_class,
        resolution,
        adjustment,
        bar_start_utc: proto_from_datetime(bar_start)?,
        bar_end_exclusive_utc: proto_from_datetime(bar_end)?,
        received_at_utc: received_at,
        available_at_utc: available_at,
        open: decimal(open)?,
        high: decimal(high)?,
        low: decimal(low)?,
        close: decimal(close)?,
        volume: decimal(volume)?,
        provenance_id: provenance_id.to_owned(),
        session_instance_id: PROVIDER_LOCAL_SESSION_ID.to_owned(),
        validation_state: BarsValidationStateV1::Provisional,
    };
    // Core validates an individual row only as part of a response. Keep the exact values intact.
    if row.bar_start_utc >= row.bar_end_exclusive_utc || row.received_at_utc > row.available_at_utc
    {
        return Err(ProviderQueryError::MalformedResponse);
    }
    Ok(row)
}

pub(super) fn provenance(
    provider: &str,
    feed: &str,
    source: &str,
    numeric_encoding: NumericEncodingV1,
    provenance_id: &str,
) -> BarSourceProvenanceV1 {
    BarSourceProvenanceV1 {
        provenance_id: provenance_id.to_owned(),
        provider: provider.to_owned(),
        feed: feed.to_owned(),
        source: source.to_owned(),
        entitlement_status: BarsEntitlementStatusV1::Unknown,
        numeric_encoding,
    }
}

pub(super) fn gap_for_window(
    start: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
    reason_code: CoverageGapReasonV1,
) -> Result<BarCoverageGapV1, ProviderQueryError> {
    if start >= end_exclusive {
        return Err(ProviderQueryError::MalformedResponse);
    }
    Ok(BarCoverageGapV1 {
        start_utc: proto_from_datetime(start)?,
        end_exclusive_utc: proto_from_datetime(end_exclusive)?,
        reason_code,
    })
}

fn merge_overlapping_gaps(
    gaps: Vec<BarCoverageGapV1>,
) -> Result<Vec<BarCoverageGapV1>, ProviderQueryError> {
    let mut merged: Vec<BarCoverageGapV1> = Vec::with_capacity(gaps.len());
    for gap in gaps {
        if gap.start_utc >= gap.end_exclusive_utc {
            return Err(ProviderQueryError::MalformedResponse);
        }
        if let Some(previous) = merged.last_mut() {
            if gap.start_utc < previous.end_exclusive_utc {
                if gap.end_exclusive_utc > previous.end_exclusive_utc {
                    previous.end_exclusive_utc = gap.end_exclusive_utc;
                }
                previous.reason_code = CoverageGapReasonV1::Unknown;
                continue;
            }
        }
        merged.push(gap);
    }
    Ok(merged)
}
