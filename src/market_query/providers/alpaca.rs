use chrono::{DateTime, Duration, Utc};
use market_contracts::{
    AdjustmentV1, BarResolutionV1, CoverageGapReasonV1, ProtoTimestampV2, UtcTimestamp,
};
use sha2::{Digest, Sha256};

use crate::{
    market_query::planning::{MarketBarsPlan, ProviderQueryGroup},
    session_router::{AlpacaFeed, PlannedSource},
};
use alpaca_rest_read::{
    AlpacaRestError, AlpacaRestReadClient, AlpacaStockBarsQueryAccessWitness,
    AlpacaStockBarsQuerySuccessClass, AlpacaStockBarsRequest, RequestedStockBarsFeed,
    StockBarsCursor, StockBarsSort, StockBarsTimeframe,
};

use super::{
    cursor::{
        alpaca_cursor, alpaca_cursor_cache_input, make_alpaca_cursor, validate_provider_cursor,
    },
    error::ProviderQueryError,
    projection::{
        ProviderBarRowInput, build_page, format_utc, make_row, next_bar_end, now_proto,
        parse_proto, provenance,
    },
    types::{ProviderPageFetch, ProviderRequestContext},
};

const PROVENANCE_ID: &str = "alpaca-history-page";
const ALPACA_QUERY_CONTRACT_ID: &str = "alpaca-stock-bars-single-symbol-v2/typed-observation-v1";
const ALPACA_DATA_ORIGIN: &str = "https://data.alpaca.markets";
const ALPACA_STOCK_BARS_PATH: &str = "/v2/stocks/{symbol}/bars";

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
    let incoming_cursor = alpaca_cursor(selection)?;
    let incoming_cursor_sha256 = incoming_cursor
        .as_ref()
        .map(stock_bars_cursor_token_sha256)
        .transpose()?;
    if let Some(cursor) = incoming_cursor {
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
    let witness = result
        .query_access_witness()
        .cloned()
        .ok_or(ProviderQueryError::MalformedResponse)?;
    let next_provider_cursor = result.take_next_cursor();
    let next_provider_cursor_sha256 = next_provider_cursor
        .as_ref()
        .map(stock_bars_cursor_token_sha256)
        .transpose()?;
    validate_query_access_witness(
        &witness,
        &request,
        incoming_cursor_sha256.as_deref(),
        result.response_observed_at().as_str(),
        result.bars().len(),
        has_provider_cursor,
        next_provider_cursor_sha256.as_deref(),
    )?;
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
        let volume = bar.volume().to_string();
        rows.push(make_row(ProviderBarRowInput {
            request: &plan.request,
            resolution: group.resolution,
            adjustment: group.adjustment,
            bar_start,
            bar_end,
            received_at,
            available_at: fetched_at.clone(),
            open: bar.open().as_str(),
            high: bar.high().as_str(),
            low: bar.low().as_str(),
            close: bar.close().as_str(),
            volume: &volume,
            provenance_id: PROVENANCE_ID,
        })?);
    }

    if rows.is_empty() && !has_provider_cursor {
        gaps.push(super::projection::gap_for_window(
            group.fetch_window.start,
            group.fetch_window.end_exclusive,
            CoverageGapReasonV1::Unknown,
        )?);
    }
    let witness_projection_is_complete =
        projection_contains_every_sdk_bar(result.bars().len(), rows.len());
    let feed_label = requested_feed.as_str();
    let response_feed = if result.bars().is_empty() {
        "unknown"
    } else {
        feed_label
    };
    let response = build_page(
        plan,
        group,
        provenance(
            "alpaca",
            response_feed,
            "Alpaca stock-bars SDK success; feed is the documented request selector, not a response echo",
            market_contracts::NumericEncodingV1::DecimalToken,
            PROVENANCE_ID,
        ),
        rows,
        gaps,
        fetched_at,
        continuation_cursor,
    )?;
    let page = ProviderPageFetch::new(
        response,
        Some(witness.typed_response_sha256().to_owned()),
        Some(response_observed_at),
        Some(1),
        Some(!has_provider_cursor),
        provider_cursor_cache_input,
    )?;
    if result.bars().is_empty() {
        // A successful empty observation has no row-level feed fact and Broker witnesses require
        // a non-empty decoded page. Preserve the observation for the caller, which bypasses cache
        // publication for empty Alpaca pages.
        Ok(page)
    } else if witness_projection_is_complete {
        page.with_alpaca_query_access(&witness, incoming_cursor_sha256.as_deref())
    } else {
        // A bar clipped at the Core request boundary is represented by the existing Unknown gap,
        // not by a fabricated partial bar. Keep the successful SDK observation and selected-feed
        // protocol fact, but do not bind a witness whose row count cannot match the Core rows.
        Ok(page)
    }
}

fn projection_contains_every_sdk_bar(sdk_bar_count: usize, projected_row_count: usize) -> bool {
    sdk_bar_count > 0 && sdk_bar_count == projected_row_count
}

fn validate_query_access_witness(
    witness: &AlpacaStockBarsQueryAccessWitness,
    request: &AlpacaStockBarsRequest,
    expected_request_cursor_sha256: Option<&str>,
    response_observed_at: &str,
    row_count: usize,
    has_next_page: bool,
    next_page_cursor_sha256: Option<&str>,
) -> Result<(), ProviderQueryError> {
    let row_count_u32 =
        u32::try_from(row_count).map_err(|_| ProviderQueryError::MalformedResponse)?;
    if witness.api_contract_id() != ALPACA_QUERY_CONTRACT_ID
        || witness.api_method() != "GET"
        || witness.api_origin() != ALPACA_DATA_ORIGIN
        || witness.api_path_template() != ALPACA_STOCK_BARS_PATH
        || witness.success_class() != AlpacaStockBarsQuerySuccessClass::SdkReturnedDecodedResponse
        || witness.symbol() != request.symbol()
        || witness.requested_feed() != request.requested_feed()
        || witness.adjustment() != "raw"
        || witness.currency() != "USD"
        || witness.asof() != "-"
        || witness.timeframe() != request.timeframe()
        || witness.start_inclusive().as_str() != request.start().as_str()
        || witness.end_inclusive().as_str() != request.end().as_str()
        || witness.sort() != StockBarsSort::Desc
        || witness.page_size() != request.page_size()
        || witness.request_cursor_sha256() != expected_request_cursor_sha256
        || witness.response_observed_at().as_str() != response_observed_at
        || witness.row_count() != row_count_u32
        || witness.contains_rows() != (row_count_u32 > 0)
        || witness.has_next_page() != has_next_page
        || witness.next_page_cursor_sha256() != next_page_cursor_sha256
        || !valid_sha256(witness.typed_response_sha256())
    {
        return Err(ProviderQueryError::MalformedResponse);
    }
    Ok(())
}

/// Hash only the opaque provider token from Broker's pinned typed cursor wire format.
/// The temporary JSON value is dropped immediately and is never logged or persisted here.
fn stock_bars_cursor_token_sha256(cursor: &StockBarsCursor) -> Result<String, ProviderQueryError> {
    let payload = serde_json::to_value(cursor).map_err(|_| ProviderQueryError::InvalidCursor)?;
    let token = payload
        .get("page_token")
        .and_then(serde_json::Value::as_str)
        .filter(|token| !token.is_empty() && !token.bytes().any(|byte| byte.is_ascii_control()))
        .ok_or(ProviderQueryError::InvalidCursor)?;
    Ok(hex::encode(Sha256::digest(token.as_bytes())))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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

#[cfg(test)]
mod tests {
    use super::{projection_contains_every_sdk_bar, stock_bars_cursor_token_sha256};
    use alpaca_rest_read::StockBarsCursor;

    #[test]
    fn clipped_boundary_page_cannot_attach_a_query_access_witness() {
        // Example: five one-minute SDK bars intersect a request ending at 13:34:30 UTC.
        // The 13:34 bar is represented only by an Unknown gap, leaving four full Core rows.
        assert!(!projection_contains_every_sdk_bar(5, 4));
        assert!(!projection_contains_every_sdk_bar(5, 6));
        assert!(projection_contains_every_sdk_bar(5, 5));
        assert!(!projection_contains_every_sdk_bar(0, 0));
    }

    #[test]
    fn typed_cursor_hash_uses_only_the_page_token_and_keeps_debug_redacted() {
        // Synthetic typed cursor for projection testing only; it is not provider evidence.
        let cursor: StockBarsCursor = serde_json::from_str(
            r#"{"version":1,"symbol":"QQQ","timeframe":"minute1","start":"2026-10-08T13:30:00Z","end":"2026-10-08T13:32:00Z","page_size":100,"feed":"sip","sort":"desc","page_token":"abc","last_bar_timestamp":null}"#,
        )
        .expect("valid synthetic typed cursor");

        assert_eq!(
            stock_bars_cursor_token_sha256(&cursor).expect("page-token digest"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(!format!("{cursor:?}").contains("abc"));
    }
}
