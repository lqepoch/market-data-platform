use std::fmt;

use chrono::{DateTime, Utc};
use market_contracts::{AdjustmentV1, BarResolutionV1, CoverageGapReasonV1, ProtoTimestampV2};
use reqwest::{Client, redirect::Policy};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::market_query::planning::{MarketBarsPlan, ProviderQueryGroup};

use super::{
    cursor::{futu_cursor, make_futu_cursor, validate_provider_cursor},
    error::{ProviderConfigError, ProviderQueryError},
    projection::{
        build_page, format_utc, make_row, next_bar_end, now_proto, parse_proto, provenance,
    },
    types::{ProviderPageFetch, ProviderRequestContext},
};

const MAX_GATEWAY_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_GATEWAY_BARS: usize = 1_000;
const MAX_GATEWAY_PAGES: u32 = 20;
const FUTU_PATH: &str = "/api/v1/stocks/bars";
const PROVENANCE_ID: &str = "futu-gateway-history-page";

/// Fixed-loopback Futu Gateway client. It owns no OpenD connection or SDK.
pub(super) struct FutuGateway {
    client: Client,
    port: u16,
}

impl fmt::Debug for FutuGateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FutuGateway")
            .field("host", &"127.0.0.1")
            .field("port", &self.port)
            .finish()
    }
}

impl FutuGateway {
    pub(super) fn new(port: u16) -> Result<Self, ProviderConfigError> {
        if port == 0 {
            return Err(ProviderConfigError::InvalidValue);
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .build()
            .map_err(|_| ProviderConfigError::ClientInitialization)?;
        Ok(Self { client, port })
    }

    pub(super) async fn fetch_page(
        &self,
        plan: &MarketBarsPlan,
        group: &ProviderQueryGroup,
        selection: &super::cursor::ProviderPageSelection,
        context: &ProviderRequestContext,
    ) -> Result<ProviderPageFetch, ProviderQueryError> {
        if group.source != crate::session_router::PlannedSource::Futu {
            return Err(ProviderQueryError::CursorQueryMismatch);
        }
        if group.adjustment != AdjustmentV1::Unadjusted {
            return Err(ProviderQueryError::UnsupportedAdjustment);
        }
        validate_provider_cursor(selection, group.source)?;
        let delegation = context
            .gateway_delegation()
            .filter(|token| valid_delegation(token))
            .ok_or(ProviderQueryError::GatewayDelegationMissing)?;
        let timeframe = match group.resolution {
            BarResolutionV1::OneMinute => "1Min",
            BarResolutionV1::FiveMinute => "5Min",
            BarResolutionV1::FifteenMinute => "15Min",
            BarResolutionV1::OneHour => "1Hour",
            BarResolutionV1::OneDay => "1Day",
        };
        let start = format_utc(group.fetch_window.start);
        let end = format_utc(group.fetch_window.end_exclusive);
        if group.fetch_window.start.timestamp_subsec_nanos() % 1_000_000 != 0
            || group.fetch_window.end_exclusive.timestamp_subsec_nanos() % 1_000_000 != 0
        {
            // The Gateway's Node boundary stores request instants at millisecond precision.
            return Err(ProviderQueryError::ProviderCapabilityUnavailable);
        }
        let cursor = futu_cursor(selection)?;
        let remaining = context.remaining()?;
        let url = format!("http://127.0.0.1:{}{FUTU_PATH}", self.port);
        let limit = plan.request.limit.to_string();
        let mut request = self
            .client
            .get(url)
            .query(&[
                ("symbol", plan.request.symbol.as_str()),
                ("timeframe", timeframe),
                ("limit", limit.as_str()),
                ("start", start.as_str()),
                ("end", end.as_str()),
            ])
            .bearer_auth(delegation)
            .timeout(remaining);
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let response = tokio::time::timeout_at(context.deadline(), request.send())
            .await
            .map_err(|_| ProviderQueryError::Timeout)?
            .map_err(map_transport_error)?;
        if !response.status().is_success() {
            return Err(map_status(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_GATEWAY_RESPONSE_BYTES as u64)
        {
            return Err(ProviderQueryError::ResponseTooLarge);
        }
        let body = read_bounded_body(response, context).await?;
        let page: FutuHistoryPage =
            serde_json::from_slice(&body).map_err(|_| ProviderQueryError::MalformedResponse)?;
        validate_page(
            &page,
            &plan.request.symbol,
            timeframe,
            group.fetch_window.start,
            group.fetch_window.end_exclusive,
        )?;
        require_exhaustive_page(&page)?;

        let fetched_at = now_proto()?;
        let fetched_instant = parse_proto(&fetched_at)?;
        let mut observed_at: Option<ProtoTimestampV2> = None;
        let mut rows = Vec::with_capacity(page.bars.len());
        let mut gaps = Vec::new();
        for bar in &page.bars {
            let bar_start = parse_rfc3339(&bar.time)?;
            let bar_end = next_bar_end(bar_start, group.resolution, group)?;
            let received_at = ProtoTimestampV2::parse(&bar.received_at)
                .map_err(|_| ProviderQueryError::MalformedResponse)?;
            let received_instant = parse_proto(&received_at)?;
            if received_instant > fetched_instant {
                return Err(ProviderQueryError::MalformedResponse);
            }
            if observed_at
                .as_ref()
                .is_none_or(|current| received_at > *current)
            {
                observed_at = Some(received_at.clone());
            }
            let Some(volume) = bar.volume.as_deref() else {
                let request_start = parse_proto(&plan.request.start)?;
                let request_end = parse_proto(&plan.request.end)?;
                let overlap_start = bar_start.max(request_start);
                let overlap_end = bar_end.min(request_end);
                if overlap_start < overlap_end {
                    gaps.push(super::projection::gap_for_window(
                        overlap_start,
                        overlap_end,
                        CoverageGapReasonV1::Unknown,
                    )?);
                }
                continue;
            };
            let request_start = parse_proto(&plan.request.start)?;
            let request_end = parse_proto(&plan.request.end)?;
            if bar_start < request_start || bar_end > request_end {
                let overlap_start = bar_start.max(request_start);
                let overlap_end = bar_end.min(request_end);
                if overlap_start < overlap_end {
                    gaps.push(super::projection::gap_for_window(
                        overlap_start,
                        overlap_end,
                        CoverageGapReasonV1::Unknown,
                    )?);
                }
                continue;
            }
            rows.push(make_row(
                &plan.request,
                group.resolution,
                group.adjustment,
                bar_start,
                bar_end,
                received_at,
                fetched_at.clone(),
                &bar.open,
                &bar.high,
                &bar.low,
                &bar.close,
                volume,
                PROVENANCE_ID,
            )?);
        }
        if rows.is_empty() && !page.has_more && gaps.is_empty() {
            gaps.push(super::projection::gap_for_window(
                group.fetch_window.start,
                group.fetch_window.end_exclusive,
                CoverageGapReasonV1::Unknown,
            )?);
        }
        let continuation_cursor = match page.cursor {
            Some(cursor) => Some(make_futu_cursor(selection, cursor)?),
            None => selection.cursor_for_next_group()?,
        };
        let response = build_page(
            plan,
            group,
            provenance(
                "futu",
                "futu",
                "Futu OpenD via local Gateway; binary-float projection",
                market_contracts::NumericEncodingV1::BinaryFloat64ShortestDecimal,
                PROVENANCE_ID,
            ),
            rows,
            gaps,
            fetched_at,
            continuation_cursor,
        )?;
        let source_observation_sha256 = hex::encode(Sha256::digest(&body));
        ProviderPageFetch::new(
            response,
            Some(source_observation_sha256),
            observed_at,
            Some(page.pages_fetched),
            Some(!page.has_more),
        )
    }
}

#[derive(Deserialize)]
struct FutuHistoryPage {
    symbol: String,
    timeframe: String,
    feed: String,
    source_mode: String,
    source_label: String,
    source_entitlement: String,
    adjustment: String,
    session: String,
    numeric_encoding: String,
    start: String,
    end: String,
    bars: Vec<FutuHistoryBar>,
    pages_fetched: u32,
    has_more: bool,
    cursor: Option<String>,
    truncated: bool,
}

#[derive(Deserialize)]
struct FutuHistoryBar {
    time: String,
    event_time_et: String,
    received_at: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: Option<String>,
}

fn validate_page(
    page: &FutuHistoryPage,
    symbol: &str,
    timeframe: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(), ProviderQueryError> {
    let expected_session = if timeframe == "1Day" {
        "REGULAR"
    } else {
        "ALL"
    };
    if page.symbol != symbol
        || page.timeframe != timeframe
        || page.feed != "futu"
        || page.source_mode != "futu"
        || page.source_label != "Futu OpenD"
        || page.source_entitlement != "unknown"
        || page.adjustment != "none"
        || page.session != expected_session
        || page.numeric_encoding != "binary_float_projection"
        || parse_rfc3339(&page.start)? != start
        || parse_rfc3339(&page.end)? != end
        || page.pages_fetched == 0
        || page.pages_fetched > MAX_GATEWAY_PAGES
        || page.bars.len() > MAX_GATEWAY_BARS
        || page.has_more != page.cursor.is_some() && !page.truncated
        || page.truncated && (!page.has_more || page.cursor.is_some())
        || page
            .cursor
            .as_deref()
            .is_some_and(|cursor| !valid_futu_cursor(cursor))
    {
        return Err(ProviderQueryError::MalformedResponse);
    }
    for bar in &page.bars {
        if bar.event_time_et.len() > 32 || bar.event_time_et.chars().any(char::is_control) {
            return Err(ProviderQueryError::MalformedResponse);
        }
        parse_rfc3339(&bar.time)?;
        ProtoTimestampV2::parse(&bar.received_at)
            .map_err(|_| ProviderQueryError::MalformedResponse)?;
    }
    Ok(())
}

async fn read_bounded_body(
    mut response: reqwest::Response,
    context: &ProviderRequestContext,
) -> Result<Vec<u8>, ProviderQueryError> {
    let mut body = Vec::new();
    loop {
        let chunk = tokio::time::timeout_at(context.deadline(), response.chunk())
            .await
            .map_err(|_| ProviderQueryError::Timeout)?
            .map_err(map_transport_error)?;
        let Some(chunk) = chunk else { break };
        if body.len().saturating_add(chunk.len()) > MAX_GATEWAY_RESPONSE_BYTES {
            return Err(ProviderQueryError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn map_status(status: u16) -> ProviderQueryError {
    match status {
        401 => ProviderQueryError::Unauthorized,
        403 => ProviderQueryError::Forbidden,
        413 => ProviderQueryError::ResponseTooLarge,
        429 => ProviderQueryError::RateLimited,
        501 => ProviderQueryError::ProviderCapabilityUnavailable,
        504 => ProviderQueryError::Timeout,
        500..=599 => ProviderQueryError::ProviderUnavailable,
        _ => ProviderQueryError::MalformedResponse,
    }
}

fn map_transport_error(error: reqwest::Error) -> ProviderQueryError {
    if error.is_timeout() {
        ProviderQueryError::Timeout
    } else if error.is_connect() {
        ProviderQueryError::ProviderUnavailable
    } else {
        ProviderQueryError::ProviderUnavailable
    }
}

fn parse_rfc3339(value: &str) -> Result<DateTime<Utc>, ProviderQueryError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ProviderQueryError::MalformedResponse)
}

fn valid_delegation(token: &str) -> bool {
    (1..=8_192).contains(&token.len()) && token.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

fn valid_futu_cursor(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn require_exhaustive_page(page: &FutuHistoryPage) -> Result<(), ProviderQueryError> {
    if page.truncated {
        Err(ProviderQueryError::PartialPageLimit)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, NaiveDate, Utc};
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarResolutionV1, BarsProviderModeV1, BarsRequestV1,
        ProtoTimestampV2, SessionScopeV1,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::{
        market_query::{
            planning::{MarketBarsPlan, ProviderQueryGroup},
            providers::{ProviderRequestContext, cursor::select_page},
        },
        session_router::{
            CalendarVerificationStatus, DataInterval, PlannedSlice, PlannedSource, SessionRank,
            UtcWindow,
        },
    };

    use super::{FutuGateway, ProviderQueryError};

    fn fixture() -> (BarsRequestV1, MarketBarsPlan) {
        let start = DateTime::parse_from_rfc3339("2026-10-13T13:30:00Z")
            .expect("valid fixture timestamp")
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-10-13T20:00:00Z")
            .expect("valid fixture timestamp")
            .with_timezone(&Utc);
        let timestamp = |value: DateTime<Utc>| {
            ProtoTimestampV2::parse(&value.to_rfc3339()).expect("valid Core timestamp")
        };
        let window = UtcWindow {
            start,
            end_exclusive: end,
        };
        let request = BarsRequestV1 {
            symbol: "AAPL".to_owned(),
            asset_class: AssetClassV1::Stock,
            start: timestamp(start),
            end: timestamp(end),
            resolution: BarResolutionV1::OneDay,
            session_scope: SessionScopeV1::Regular,
            adjustment: AdjustmentV1::Unadjusted,
            provider_mode: BarsProviderModeV1::FutuOnly,
            allow_daily_downgrade: false,
            limit: 10,
            cursor: None,
        };
        let slice = PlannedSlice {
            symbol: "AAPL".to_owned(),
            trade_date: NaiveDate::from_ymd_opt(2026, 10, 13).expect("valid fixture date"),
            session_id: "XNYS-2026-10-13".to_owned(),
            session_instance_id: Some("XNYS-2026-10-13-regular".to_owned()),
            phase: None,
            rank: Some(SessionRank::Old),
            interval: DataInterval::Daily,
            window,
            calendar_verification_status: CalendarVerificationStatus::Verified,
            source: Some(PlannedSource::Futu),
            source_label: Some("Futu".to_owned()),
            historical: true,
            realtime: false,
            provisional: false,
            gap: None,
        };
        let plan = MarketBarsPlan {
            request: request.clone(),
            as_of: timestamp(end),
            requested_window: window,
            effective_resolution: BarResolutionV1::OneDay,
            calendar_source_id: "fixture-calendar".to_owned(),
            calendar_version: "fixture-v1".to_owned(),
            calendar_verification_status: Vec::new(),
            slices_recent_to_old: vec![slice.clone()],
            provider_groups: vec![ProviderQueryGroup {
                source: PlannedSource::Futu,
                resolution: BarResolutionV1::OneDay,
                adjustment: AdjustmentV1::Unadjusted,
                fetch_window: window,
                slices_recent_to_old: vec![slice],
            }],
            gaps: Vec::new(),
            provisional: false,
        };
        (request, plan)
    }

    #[tokio::test]
    async fn sends_only_fixed_loopback_read_and_projects_gateway_observation() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local test server");
        let port = listener.local_addr().expect("local address").port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept local request");
            let mut bytes = [0_u8; 8_192];
            let read = stream.read(&mut bytes).await.expect("read request");
            let request_text = String::from_utf8_lossy(&bytes[..read]).to_ascii_lowercase();
            assert!(request_text.contains("get /api/v1/stocks/bars?"));
            assert!(request_text.contains("symbol=aapl"));
            assert!(request_text.contains("timeframe=1day"));
            assert!(request_text.contains("authorization: bearer test-gateway-delegation"));

            let body = r#"{"symbol":"AAPL","timeframe":"1Day","feed":"futu","source_mode":"futu","source_label":"Futu OpenD","source_entitlement":"unknown","adjustment":"none","session":"REGULAR","numeric_encoding":"binary_float_projection","start":"2026-10-13T13:30:00.000000000Z","end":"2026-10-13T20:00:00.000000000Z","bars":[],"pages_fetched":1,"has_more":false,"cursor":null,"truncated":false}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });

        let (request, plan) = fixture();
        let selection = select_page(&request, &plan).expect("select query group");
        let gateway = FutuGateway::new(port).expect("build fixed-loopback gateway");
        let context = ProviderRequestContext::new(
            std::time::Instant::now() + Duration::from_secs(5),
            Some("test-gateway-delegation".to_owned()),
        );
        let fetched = gateway
            .fetch_page(&plan, &plan.provider_groups[0], &selection, &context)
            .await
            .expect("fetch one bounded Futu page");
        server.await.expect("local HTTP server completed");

        assert!(fetched.response.rows.is_empty());
        assert_eq!(fetched.response.gaps.len(), 1);
        assert_eq!(fetched.evidence.pages_seen, Some(1));
        assert_eq!(fetched.evidence.pages_exhausted, Some(true));
        assert_eq!(fetched.evidence.first_source_sequence, None);
        assert_eq!(fetched.evidence.last_source_sequence, None);
        assert!(fetched.evidence.source_observation_sha256.is_some());
        assert!(fetched.response.validate().is_ok());
    }

    #[tokio::test]
    async fn missing_gateway_delegation_fails_before_network_access() {
        let (request, plan) = fixture();
        let selection = select_page(&request, &plan).expect("select query group");
        let gateway = FutuGateway::new(1).expect("build client without connecting");
        let context =
            ProviderRequestContext::new(std::time::Instant::now() + Duration::from_secs(1), None);
        let error = gateway
            .fetch_page(&plan, &plan.provider_groups[0], &selection, &context)
            .await
            .expect_err("missing delegation must fail closed");
        assert_eq!(error, ProviderQueryError::GatewayDelegationMissing);
    }

    #[test]
    fn a_twentieth_page_with_more_rows_is_a_partial_page_error() {
        let start = DateTime::parse_from_rfc3339("2026-10-13T13:30:00Z")
            .expect("valid fixture timestamp")
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-10-13T20:00:00Z")
            .expect("valid fixture timestamp")
            .with_timezone(&Utc);
        let page = super::FutuHistoryPage {
            symbol: "AAPL".to_owned(),
            timeframe: "1Day".to_owned(),
            feed: "futu".to_owned(),
            source_mode: "futu".to_owned(),
            source_label: "Futu OpenD".to_owned(),
            source_entitlement: "unknown".to_owned(),
            adjustment: "none".to_owned(),
            session: "REGULAR".to_owned(),
            numeric_encoding: "binary_float_projection".to_owned(),
            start: start.to_rfc3339(),
            end: end.to_rfc3339(),
            bars: Vec::new(),
            pages_fetched: 20,
            has_more: true,
            cursor: None,
            truncated: true,
        };
        assert!(super::validate_page(&page, "AAPL", "1Day", start, end).is_ok());
        assert_eq!(
            super::require_exhaustive_page(&page),
            Err(ProviderQueryError::PartialPageLimit)
        );
    }
}
