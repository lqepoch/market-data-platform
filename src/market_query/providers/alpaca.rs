use chrono::{DateTime, Duration, Utc};
use market_contracts::{
    AdjustmentV1, BarResolutionV1, CoverageGapReasonV1, ProtoTimestampV2, UtcTimestamp,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    market_query::planning::{MarketBarsPlan, ProviderQueryGroup},
    session_router::{AlpacaFeed, PlannedSource},
};
use alpaca_rest_read::{
    AlpacaRestError, AlpacaRestReadClient, AlpacaStockBarsRequest, RequestedStockBarsFeed,
    StockBarsSort, StockBarsTimeframe,
};

use super::{
    cursor::{
        alpaca_cursor, alpaca_cursor_cache_input, make_alpaca_cursor, validate_provider_cursor,
    },
    error::ProviderQueryError,
    projection::{
        build_page, format_utc, make_row, next_bar_end, now_proto, parse_proto, provenance,
    },
    types::{ProviderPageFetch, ProviderRequestContext},
};

const PROVENANCE_ID: &str = "alpaca-history-page";

#[derive(Serialize)]
struct ObservationHashInput<'a> {
    requested_feed: &'a str,
    response_observed_at: &'a str,
    rows: Vec<AlpacaObservationFact<'a>>,
}

#[derive(Serialize)]
struct AlpacaObservationFact<'a> {
    symbol: &'a str,
    timestamp: &'a str,
    open: &'a str,
    high: &'a str,
    low: &'a str,
    close: &'a str,
    volume: u64,
    trade_count: Option<u64>,
    volume_weighted_price: Option<&'a str>,
    actual_provider: &'a str,
    actual_feed: &'a str,
    entitlement: &'static str,
    numeric_encoding: &'static str,
}

pub(super) async fn fetch_page(
    client: &AlpacaRestReadClient,
    plan: &MarketBarsPlan,
    group: &ProviderQueryGroup,
    selection: &super::cursor::ProviderPageSelection,
    context: &ProviderRequestContext,
) -> Result<ProviderPageFetch, ProviderQueryError> {
    let feed = match group.source {
        PlannedSource::Alpaca { feed } => feed,
        _ => return Err(ProviderQueryError::CursorQueryMismatch),
    };
    if group.adjustment != AdjustmentV1::Unadjusted {
        return Err(ProviderQueryError::UnsupportedAdjustment);
    }
    validate_provider_cursor(selection, group.source)?;

    let start = UtcTimestamp::parse(&format_utc(group.fetch_window.start))
        .map_err(|_| ProviderQueryError::InvalidCorePage)?;
    // The Core request uses an exclusive end; Alpaca's request facade accepts an inclusive end.
    let inclusive_end = group
        .fetch_window
        .end_exclusive
        .checked_sub_signed(Duration::nanoseconds(1))
        .ok_or(ProviderQueryError::InvalidCorePage)?;
    let end = UtcTimestamp::parse(&format_utc(inclusive_end))
        .map_err(|_| ProviderQueryError::InvalidCorePage)?;
    let requested_feed = match feed {
        AlpacaFeed::Sip => RequestedStockBarsFeed::Sip,
        AlpacaFeed::Boats => RequestedStockBarsFeed::Boats,
    };
    let timeframe = match group.resolution {
        BarResolutionV1::OneMinute => StockBarsTimeframe::Minute1,
        BarResolutionV1::FiveMinute => StockBarsTimeframe::Minute5,
        BarResolutionV1::FifteenMinute => StockBarsTimeframe::Minute15,
        BarResolutionV1::OneHour => StockBarsTimeframe::Hour1,
        BarResolutionV1::OneDay => StockBarsTimeframe::Day1,
    };
    let mut request = AlpacaStockBarsRequest::new_with_feed_and_sort(
        requested_feed,
        StockBarsSort::Desc,
        plan.request.symbol.clone(),
        timeframe,
        start,
        end,
        plan.request.limit,
    )
    .map_err(map_alpaca_error)?;
    if let Some(cursor) = alpaca_cursor(selection)? {
        request = request.with_cursor(cursor).map_err(map_alpaca_error)?;
    }

    let mut result = tokio::time::timeout_at(
        context.deadline(),
        client.historical_stock_bars_page(&request),
    )
    .await
    .map_err(|_| ProviderQueryError::Timeout)?
    .map_err(map_alpaca_error)?;
    let response_observed_at = ProtoTimestampV2::parse(result.response_observed_at().as_str())
        .map_err(|_| ProviderQueryError::MalformedResponse)?;
    let response_observed_instant = parse_proto(&response_observed_at)?;
    let fetched_at = now_proto()?;
    if response_observed_instant > parse_proto(&fetched_at)? {
        return Err(ProviderQueryError::MalformedResponse);
    }

    let has_provider_cursor = result.has_next_page();
    let next_provider_cursor = result.take_next_cursor();
    let (continuation_cursor, provider_cursor_cache_input) = match next_provider_cursor {
        Some(cursor) => {
            let cache_input = alpaca_cursor_cache_input(selection, &cursor)?;
            (
                Some(make_alpaca_cursor(selection, cursor)?),
                Some(cache_input),
            )
        }
        // Provider RAW pages carry only provider-local continuation. The HTTP assembler adds a
        // separate outer MDP cursor after mapping when another planned run remains; persisting
        // that outer cursor here would make an exhausted provider page look resumable in cache.
        None if !has_provider_cursor => (None, None),
        None => return Err(ProviderQueryError::MalformedResponse),
    };

    let mut rows = Vec::with_capacity(result.bars().len());
    let mut gaps = Vec::new();
    let request_start = parse_proto(&plan.request.start)?;
    let request_end = parse_proto(&plan.request.end)?;
    for bar in result.bars() {
        let bar_start = DateTime::parse_from_rfc3339(bar.timestamp().as_str())
            .map(|value| value.with_timezone(&Utc))
            .map_err(|_| ProviderQueryError::MalformedResponse)?;
        let bar_end = next_bar_end(bar_start, group.resolution, group)?;
        if bar_start < request_start || bar_end > request_end {
            let gap_start = bar_start.max(request_start);
            let gap_end = bar_end.min(request_end);
            if gap_start < gap_end {
                gaps.push(super::projection::gap_for_window(
                    gap_start,
                    gap_end,
                    CoverageGapReasonV1::Unknown,
                )?);
            }
            continue;
        }
        let received_at = ProtoTimestampV2::parse(result.response_observed_at().as_str())
            .map_err(|_| ProviderQueryError::MalformedResponse)?;
        rows.push(make_row(
            &plan.request,
            group.resolution,
            group.adjustment,
            bar_start,
            bar_end,
            received_at,
            fetched_at.clone(),
            bar.open().as_str(),
            bar.high().as_str(),
            bar.low().as_str(),
            bar.close().as_str(),
            &bar.volume().to_string(),
            PROVENANCE_ID,
        )?);
    }

    if rows.is_empty() && !has_provider_cursor {
        gaps.push(super::projection::gap_for_window(
            group.fetch_window.start,
            group.fetch_window.end_exclusive,
            CoverageGapReasonV1::Unknown,
        )?);
    }
    let observation_hash = observation_hash(
        requested_feed.as_str(),
        result.response_observed_at().as_str(),
        result.bars(),
    )?;
    let response = build_page(
        plan,
        group,
        provenance(
            "alpaca",
            "unknown",
            "Alpaca historical stock-bars API",
            market_contracts::NumericEncodingV1::DecimalToken,
            PROVENANCE_ID,
        ),
        rows,
        gaps,
        fetched_at,
        continuation_cursor,
    )?;
    ProviderPageFetch::new(
        response,
        Some(observation_hash),
        Some(response_observed_at),
        Some(1),
        Some(!has_provider_cursor),
        provider_cursor_cache_input,
    )
}

fn observation_hash(
    requested_feed: &str,
    response_observed_at: &str,
    bars: &[alpaca_rest_read::AlpacaStockBarObservation],
) -> Result<String, ProviderQueryError> {
    let rows = bars
        .iter()
        .map(|bar| {
            let source = bar.source();
            AlpacaObservationFact {
                symbol: bar.symbol(),
                timestamp: bar.timestamp().as_str(),
                open: bar.open().as_str(),
                high: bar.high().as_str(),
                low: bar.low().as_str(),
                close: bar.close().as_str(),
                volume: bar.volume(),
                trade_count: bar.trade_count(),
                volume_weighted_price: bar.volume_weighted_price().map(|value| value.as_str()),
                actual_provider: &source.provider,
                actual_feed: &source.feed,
                entitlement: "unknown",
                numeric_encoding: source.numeric_encoding.as_str(),
            }
        })
        .collect();
    let bytes = serde_json::to_vec(&ObservationHashInput {
        requested_feed,
        response_observed_at,
        rows,
    })
    .map_err(|_| ProviderQueryError::MalformedResponse)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn map_alpaca_error(error: AlpacaRestError) -> ProviderQueryError {
    match error {
        AlpacaRestError::AuthenticationRejected => ProviderQueryError::Unauthorized,
        AlpacaRestError::AccessDenied => ProviderQueryError::Forbidden,
        AlpacaRestError::RateLimited => ProviderQueryError::RateLimited,
        AlpacaRestError::Timeout => ProviderQueryError::Timeout,
        AlpacaRestError::ResponseTooLarge => ProviderQueryError::ResponseTooLarge,
        AlpacaRestError::InvalidConfiguration => ProviderQueryError::InvalidConfiguration,
        AlpacaRestError::InvalidRequest | AlpacaRestError::ProtocolViolation => {
            ProviderQueryError::MalformedResponse
        }
        AlpacaRestError::ProviderRejected | AlpacaRestError::Transport => {
            ProviderQueryError::ProviderUnavailable
        }
    }
}
