use std::fmt;

use chrono::{DateTime, Duration, Utc};
use market_contracts::{
    AdjustmentV1, AssetClassV1, BarResolutionV1, BarsProviderModeV1, BarsRequestV1,
    ProtoTimestampV2,
};
use reqwest::{Client, redirect::Policy};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use super::{
    error::{ProviderConfigError, ProviderQueryError},
    projection::now_proto,
    types::ProviderRequestContext,
};

const BRIDGE_PATH: &str = "/internal/research/yfinance/v1/bars";
const MAX_BRIDGE_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_BRIDGE_REQUEST_BYTES: usize = 16 * 1024;
const MAX_BRIDGE_HISTORY_DAYS: i64 = 3_650;
const MAX_BRIDGE_ROWS: usize = 24_000;

/// Yahoo response retained as provider-native research evidence, never projected to Core Bars V1.
pub struct YFinanceResearchObservationPage {
    payload: Value,
    source_observation_sha256: String,
    fetched_at_utc: ProtoTimestampV2,
}

impl YFinanceResearchObservationPage {
    /// Borrow the validated, provider-native JSON observation.
    #[must_use]
    pub const fn payload(&self) -> &Value {
        &self.payload
    }

    /// Return the digest of the internal Gateway observation JSON body (not HTTP wire bytes).
    #[must_use]
    pub fn source_observation_sha256(&self) -> &str {
        &self.source_observation_sha256
    }

    /// Return the local observation time after the bounded response was decoded.
    #[must_use]
    pub const fn fetched_at_utc(&self) -> &ProtoTimestampV2 {
        &self.fetched_at_utc
    }
}

impl fmt::Debug for YFinanceResearchObservationPage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self
            .payload
            .get("bars")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        formatter
            .debug_struct("YFinanceResearchObservationPage")
            .field("bar_count", &count)
            .field("fetched_at_utc", &self.fetched_at_utc)
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

/// Optional fixed-loopback caller for the isolated read-only Yahoo research bridge.
pub(super) struct YFinanceBridge {
    enabled: bool,
    port: u16,
    bearer: Option<Zeroizing<String>>,
    client: Client,
}

impl fmt::Debug for YFinanceBridge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("YFinanceBridge")
            .field("enabled", &self.enabled)
            .field("host", &"127.0.0.1")
            .field("port", &self.port)
            .field("has_bearer", &self.bearer.is_some())
            .finish()
    }
}

impl YFinanceBridge {
    pub(super) fn from_environment() -> Result<Self, ProviderConfigError> {
        let enabled = match std::env::var("MDP_YFINANCE_BRIDGE_ENABLED") {
            Ok(value) if value == "true" => true,
            Ok(value) if value == "false" => false,
            Ok(_) => return Err(ProviderConfigError::InvalidValue),
            Err(std::env::VarError::NotPresent) => false,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(ProviderConfigError::InvalidValue);
            }
        };
        let port = super::parse_port("MDP_YFINANCE_BRIDGE_PORT", 8_765)?;
        let bearer = match std::env::var("PUBLIC_MDP_YFINANCE_BRIDGE_BEARER_TOKEN") {
            Ok(value) if !value.is_empty() => Some(Zeroizing::new(value)),
            Ok(_) => return Err(ProviderConfigError::InvalidValue),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(ProviderConfigError::InvalidValue);
            }
        };
        if bearer.as_deref().is_some_and(|token| !valid_bearer(token)) {
            return Err(ProviderConfigError::InvalidValue);
        }
        Self::new(enabled, port, bearer)
    }

    fn new(
        enabled: bool,
        port: u16,
        bearer: Option<Zeroizing<String>>,
    ) -> Result<Self, ProviderConfigError> {
        if port == 0 || bearer.as_deref().is_some_and(|token| !valid_bearer(token)) {
            return Err(ProviderConfigError::InvalidValue);
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .build()
            .map_err(|_| ProviderConfigError::ClientInitialization)?;
        Ok(Self {
            enabled,
            port,
            bearer,
            client,
        })
    }

    pub(super) async fn fetch_research_observations(
        &self,
        request: &BarsRequestV1,
        context: &ProviderRequestContext,
    ) -> Result<YFinanceResearchObservationPage, ProviderQueryError> {
        if !self.enabled {
            return Err(ProviderQueryError::ProviderDisabled);
        }
        let bearer = self
            .bearer
            .as_deref()
            .map(String::as_str)
            .filter(|value| valid_bearer(value))
            .ok_or(ProviderQueryError::MissingCredentials)?;
        request
            .validate()
            .map_err(|_| ProviderQueryError::CursorQueryMismatch)?;
        if request.provider_mode != BarsProviderModeV1::Research
            || request.cursor.is_some()
            || request.resolution != BarResolutionV1::OneDay
        {
            return Err(ProviderQueryError::ProviderCapabilityUnavailable);
        }
        if !matches!(
            request.adjustment,
            AdjustmentV1::Unadjusted | AdjustmentV1::All
        ) {
            return Err(ProviderQueryError::AdjustmentSemanticsUnverified);
        }
        let start = DateTime::parse_from_rfc3339(request.start.as_str())
            .map_err(|_| ProviderQueryError::CursorQueryMismatch)?
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339(request.end.as_str())
            .map_err(|_| ProviderQueryError::CursorQueryMismatch)?
            .with_timezone(&Utc);
        if end.signed_duration_since(start) > Duration::days(MAX_BRIDGE_HISTORY_DAYS) {
            return Err(ProviderQueryError::ProviderCapabilityUnavailable);
        }
        let adjustment = match request.adjustment {
            AdjustmentV1::Unadjusted => "unadjusted",
            AdjustmentV1::All => "adjusted",
            AdjustmentV1::Split | AdjustmentV1::Dividend => {
                return Err(ProviderQueryError::AdjustmentSemanticsUnverified);
            }
        };
        let asset_class = match request.asset_class {
            AssetClassV1::Stock => "stock",
            AssetClassV1::Etf => "etf",
        };
        let body = json!({
            "symbols": [request.symbol.as_str()],
            "start": request.start.as_str(),
            "end": request.end.as_str(),
            "resolution": "1d",
            "asset_class": asset_class,
            "adjustment": adjustment,
        });
        let body = serde_json::to_vec(&body).map_err(|_| ProviderQueryError::MalformedResponse)?;
        if body.len() > MAX_BRIDGE_REQUEST_BYTES {
            return Err(ProviderQueryError::ResponseTooLarge);
        }
        let remaining = context.remaining()?;
        let url = format!("http://127.0.0.1:{}{BRIDGE_PATH}", self.port);
        let response = tokio::time::timeout_at(
            context.deadline(),
            self.client
                .post(url)
                .bearer_auth(bearer)
                .timeout(remaining)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body)
                .send(),
        )
        .await
        .map_err(|_| ProviderQueryError::Timeout)?
        .map_err(map_transport_error)?;
        if !response.status().is_success() {
            return Err(map_status(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_BRIDGE_RESPONSE_BYTES as u64)
        {
            return Err(ProviderQueryError::ResponseTooLarge);
        }
        let bytes = read_bounded_body(response, context).await?;
        let payload: Value =
            serde_json::from_slice(&bytes).map_err(|_| ProviderQueryError::MalformedResponse)?;
        validate_research_page(&payload, request, start, end, adjustment)?;
        let source_observation_sha256 = hex::encode(Sha256::digest(&bytes));
        Ok(YFinanceResearchObservationPage {
            payload,
            source_observation_sha256,
            fetched_at_utc: now_proto()?,
        })
    }
}

fn validate_research_page(
    payload: &Value,
    request: &BarsRequestV1,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    requested_adjustment: &str,
) -> Result<(), ProviderQueryError> {
    let asset_class = match request.asset_class {
        AssetClassV1::Stock => "stock",
        AssetClassV1::Etf => "etf",
    };
    let requested_range = payload
        .get("requested_range")
        .ok_or(ProviderQueryError::MalformedResponse)?;
    let response_start = parse_json_timestamp(requested_range.get("start"))?;
    let response_end = parse_json_timestamp(requested_range.get("end"))?;
    let bars = payload
        .get("bars")
        .and_then(Value::as_array)
        .ok_or(ProviderQueryError::MalformedResponse)?;
    let symbols = payload
        .get("requested_symbols")
        .and_then(Value::as_array)
        .ok_or(ProviderQueryError::MalformedResponse)?;
    if payload.get("schema_version").and_then(Value::as_str) != Some("yfinance-history-bridge-v1")
        || payload.get("provider").and_then(Value::as_str) != Some("yfinance")
        || payload.get("source").and_then(Value::as_str) != Some("Yahoo Finance")
        || payload.get("asset_class").and_then(Value::as_str) != Some(asset_class)
        || payload.get("resolution").and_then(Value::as_str) != Some("1d")
        || payload.get("effective_resolution").and_then(Value::as_str) != Some("1d")
        || payload.get("adjustment").and_then(Value::as_str) != Some(requested_adjustment)
        || payload
            .get("provider_adjustment_semantics")
            .and_then(Value::as_str)
            != Some("unknown_or_provider_native")
        || payload
            .get("adjustment_confirmed_raw")
            .and_then(Value::as_bool)
            != Some(false)
        || payload.get("numeric_encoding").and_then(Value::as_str)
            != Some("binary_float_projection")
        || payload.get("canonical").and_then(Value::as_bool) != Some(false)
        || payload.get("point_in_time").and_then(Value::as_bool) != Some(false)
        || payload.get("executable").and_then(Value::as_bool) != Some(false)
        || payload.get("display_only").and_then(Value::as_bool) != Some(true)
        || payload
            .get("provider_repair_enabled")
            .and_then(Value::as_bool)
            != Some(false)
        || requested_range.get("semantics").and_then(Value::as_str)
            != Some("start_inclusive_end_exclusive")
        || response_start != start
        || response_end != end
        || symbols.len() != 1
        || symbols[0].as_str() != Some(request.symbol.as_str())
        || bars.len() > MAX_BRIDGE_ROWS
    {
        return Err(ProviderQueryError::MalformedResponse);
    }
    for bar in bars {
        if bar.get("symbol").and_then(Value::as_str) != Some(request.symbol.as_str())
            || bar.get("asset_class").and_then(Value::as_str) != Some(asset_class)
            || bar.get("resolution").and_then(Value::as_str) != Some("1d")
            || bar.get("adjustment").and_then(Value::as_str) != Some(requested_adjustment)
            || parse_json_timestamp(bar.get("event_ts_utc")).is_err()
        {
            return Err(ProviderQueryError::MalformedResponse);
        }
    }
    for name in [
        "available_at",
        "fetched_at",
        "source_observed_at",
        "request_started_at",
    ] {
        parse_json_timestamp(payload.get(name))?;
    }
    Ok(())
}

fn parse_json_timestamp(value: Option<&Value>) -> Result<DateTime<Utc>, ProviderQueryError> {
    value
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .ok_or(ProviderQueryError::MalformedResponse)
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
        if body.len().saturating_add(chunk.len()) > MAX_BRIDGE_RESPONSE_BYTES {
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
        504 => ProviderQueryError::Timeout,
        500..=599 => ProviderQueryError::ProviderUnavailable,
        _ => ProviderQueryError::MalformedResponse,
    }
}

fn map_transport_error(error: reqwest::Error) -> ProviderQueryError {
    if error.is_timeout() {
        ProviderQueryError::Timeout
    } else {
        ProviderQueryError::ProviderUnavailable
    }
}

fn valid_bearer(value: &str) -> bool {
    (32..=4_096).contains(&value.len()) && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarResolutionV1, BarsProviderModeV1, BarsRequestV1,
        ProtoTimestampV2, SessionScopeV1,
    };
    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::market_query::providers::ProviderRequestContext;

    use super::{YFinanceBridge, Zeroizing};

    #[tokio::test]
    async fn posts_only_to_fixed_loopback_and_keeps_unknown_adjustment_native() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local test server");
        let port = listener.local_addr().expect("local address").port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept local request");
            let mut header_bytes = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                stream
                    .read_exact(&mut byte)
                    .await
                    .expect("read request header");
                header_bytes.push(byte[0]);
                if header_bytes.ends_with(b"\r\n\r\n") {
                    break;
                }
                assert!(header_bytes.len() <= 8_192, "bounded request headers");
            }
            let headers = String::from_utf8(header_bytes).expect("ASCII request headers");
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                })
                .expect("content length header");
            let mut body = vec![0_u8; content_length];
            stream.read_exact(&mut body).await.expect("read JSON body");
            let lower_headers = headers.to_ascii_lowercase();
            assert!(lower_headers.contains("post /internal/research/yfinance/v1/bars "));
            assert!(
                lower_headers.contains("authorization: bearer test-research-bridge-token-12345")
            );
            let sent: Value = serde_json::from_slice(&body).expect("valid request JSON");
            assert_eq!(sent["symbols"], json!(["AAPL"]));
            assert_eq!(sent["resolution"], "1d");
            assert_eq!(sent["adjustment"], "unadjusted");

            let response_body = json!({
                "schema_version": "yfinance-history-bridge-v1",
                "provider": "yfinance",
                "source": "Yahoo Finance",
                "asset_class": "stock",
                "resolution": "1d",
                "effective_resolution": "1d",
                "adjustment": "unadjusted",
                "provider_adjustment_semantics": "unknown_or_provider_native",
                "adjustment_confirmed_raw": false,
                "numeric_encoding": "binary_float_projection",
                "canonical": false,
                "point_in_time": false,
                "executable": false,
                "display_only": true,
                "provider_repair_enabled": false,
                "requested_range": {
                    "start": "2026-10-13T00:00:00Z",
                    "end": "2026-10-14T00:00:00Z",
                    "semantics": "start_inclusive_end_exclusive"
                },
                "requested_symbols": ["AAPL"],
                "bars": [],
                "available_at": "2026-10-14T00:00:00Z",
                "fetched_at": "2026-10-14T00:00:00Z",
                "source_observed_at": "2026-10-14T00:00:00Z",
                "request_started_at": "2026-10-14T00:00:00Z"
            });
            let response_body = serde_json::to_vec(&response_body).expect("serialize response");
            let response_headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                response_body.len()
            );
            stream
                .write_all(response_headers.as_bytes())
                .await
                .expect("write response headers");
            stream
                .write_all(&response_body)
                .await
                .expect("write response body");
        });

        let request = BarsRequestV1 {
            symbol: "AAPL".to_owned(),
            asset_class: AssetClassV1::Stock,
            start: ProtoTimestampV2::parse("2026-10-13T00:00:00Z").expect("valid start timestamp"),
            end: ProtoTimestampV2::parse("2026-10-14T00:00:00Z").expect("valid end timestamp"),
            resolution: BarResolutionV1::OneDay,
            session_scope: SessionScopeV1::Regular,
            adjustment: AdjustmentV1::Unadjusted,
            provider_mode: BarsProviderModeV1::Research,
            allow_daily_downgrade: false,
            limit: 10,
            cursor: None,
        };
        let bridge = YFinanceBridge::new(
            true,
            port,
            Some(Zeroizing::new(
                "test-research-bridge-token-12345".to_owned(),
            )),
        )
        .expect("build local bridge client");
        let context =
            ProviderRequestContext::new(tokio::time::Instant::now() + Duration::from_secs(5), None);
        let page = bridge
            .fetch_research_observations(&request, &context)
            .await
            .expect("fetch provider-native research page");
        server.await.expect("local HTTP server completed");

        assert_eq!(
            page.payload()["provider_adjustment_semantics"],
            "unknown_or_provider_native"
        );
        assert_eq!(page.payload()["adjustment_confirmed_raw"], false);
        assert!(!page.source_observation_sha256().is_empty());
    }
}
