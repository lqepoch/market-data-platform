use std::fmt;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
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
const MAX_BRIDGE_SYMBOLS: usize = 8;

/// Yahoo response retained as provider-native research evidence, never projected to Core Bars V1.
pub struct YFinanceResearchObservationPage {
    payload: Value,
    source_observation_sha256: String,
    fetched_at_utc: ProtoTimestampV2,
}

/// Adjustment requested from Yahoo for one bounded provider-native observation window.
///
/// This records the request preference only. Yahoo's response does not prove as-traded raw
/// prices, even when `Unadjusted` is requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum YFinanceResearchAdjustment {
    Unadjusted,
    Adjusted,
}

impl YFinanceResearchAdjustment {
    const fn bridge_value(self) -> &'static str {
        match self {
            Self::Unadjusted => "unadjusted",
            Self::Adjusted => "adjusted",
        }
    }
}

/// One bounded multi-symbol request to the fixed Yahoo research bridge.
#[derive(Clone, Debug)]
pub struct YFinanceResearchWindowRequest {
    pub symbols: Vec<String>,
    pub asset_class: AssetClassV1,
    pub start_utc: DateTime<Utc>,
    pub end_utc: DateTime<Utc>,
    pub adjustment: YFinanceResearchAdjustment,
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
            .finish_non_exhaustive()
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
            .finish_non_exhaustive()
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

    #[cfg(test)]
    pub(super) fn disabled_for_test() -> Result<Self, ProviderConfigError> {
        Self::new(false, 8_765, None)
    }

    pub(super) async fn fetch_research_observations(
        &self,
        request: &BarsRequestV1,
        context: &ProviderRequestContext,
    ) -> Result<YFinanceResearchObservationPage, ProviderQueryError> {
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
        let start_utc = DateTime::parse_from_rfc3339(request.start.as_str())
            .map_err(|_| ProviderQueryError::CursorQueryMismatch)?
            .with_timezone(&Utc);
        let end_utc = DateTime::parse_from_rfc3339(request.end.as_str())
            .map_err(|_| ProviderQueryError::CursorQueryMismatch)?
            .with_timezone(&Utc);
        let adjustment = match request.adjustment {
            AdjustmentV1::Unadjusted => YFinanceResearchAdjustment::Unadjusted,
            AdjustmentV1::All => YFinanceResearchAdjustment::Adjusted,
            AdjustmentV1::Split | AdjustmentV1::Dividend => {
                return Err(ProviderQueryError::AdjustmentSemanticsUnverified);
            }
        };
        self.fetch_research_window(
            &YFinanceResearchWindowRequest {
                symbols: vec![request.symbol.clone()],
                asset_class: request.asset_class,
                start_utc,
                end_utc,
                adjustment,
            },
            context,
        )
        .await
    }

    pub(super) async fn fetch_research_window(
        &self,
        request: &YFinanceResearchWindowRequest,
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
        validate_window_request(request)?;
        let start = request.start_utc;
        let end = request.end_utc;
        let adjustment = request.adjustment.bridge_value();
        let start_text = start.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        let end_text = end.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        let asset_class = match request.asset_class {
            AssetClassV1::Stock => "stock",
            AssetClassV1::Etf => "etf",
        };
        let body = json!({
            "symbols": &request.symbols,
            "start": start_text,
            "end": end_text,
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
        if response.status().as_u16() != 200 {
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
        validate_research_window_page(&payload, request)?;
        let source_observation_sha256 = hex::encode(Sha256::digest(&bytes));
        Ok(YFinanceResearchObservationPage {
            payload,
            source_observation_sha256,
            fetched_at_utc: now_proto()?,
        })
    }
}

fn validate_window_request(
    request: &YFinanceResearchWindowRequest,
) -> Result<(), ProviderQueryError> {
    if request.symbols.is_empty()
        || request.symbols.len() > MAX_BRIDGE_SYMBOLS
        || request.symbols.iter().any(|symbol| !valid_symbol(symbol))
        || request
            .symbols
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != request.symbols.len()
        || request.start_utc >= request.end_utc
        || request.end_utc.signed_duration_since(request.start_utc)
            > Duration::days(MAX_BRIDGE_HISTORY_DAYS)
    {
        return Err(ProviderQueryError::ProviderCapabilityUnavailable);
    }
    Ok(())
}

fn validate_research_window_page(
    payload: &Value,
    request: &YFinanceResearchWindowRequest,
) -> Result<(), ProviderQueryError> {
    let asset_class = match request.asset_class {
        AssetClassV1::Stock => "stock",
        AssetClassV1::Etf => "etf",
    };
    let requested_adjustment = request.adjustment.bridge_value();
    let requested_range = payload
        .get("requested_range")
        .ok_or(ProviderQueryError::MalformedResponse)?;
    let response_start = parse_json_timestamp(requested_range.get("start"))?;
    let response_end = parse_json_timestamp(requested_range.get("end"))?;
    let start = request.start_utc;
    let end = request.end_utc;
    let bars = payload
        .get("bars")
        .and_then(Value::as_array)
        .ok_or(ProviderQueryError::MalformedResponse)?;
    let symbols = payload
        .get("requested_symbols")
        .and_then(Value::as_array)
        .ok_or(ProviderQueryError::MalformedResponse)?;
    let returned_symbols = string_array(payload.get("returned_symbols"))?;
    let missing_symbols = string_array(payload.get("missing_symbols"))?;
    let coverage = payload
        .get("coverage")
        .and_then(Value::as_object)
        .ok_or(ProviderQueryError::MalformedResponse)?;
    if !has_exact_keys(
        payload.as_object(),
        &[
            "schema_version",
            "status",
            "provider",
            "source",
            "asset_class",
            "classification_source",
            "resolution",
            "effective_resolution",
            "adjustment",
            "provider_adjustment_semantics",
            "adjustment_confirmed_raw",
            "provider_repair_enabled",
            "numeric_encoding",
            "volume_encoding",
            "canonical",
            "point_in_time",
            "executable",
            "display_only",
            "requested_range",
            "requested_symbols",
            "returned_symbols",
            "missing_symbols",
            "available_at",
            "fetched_at",
            "source_observed_at",
            "request_started_at",
            "cache_status",
            "freshness",
            "coverage",
            "action_coverage",
            "actions",
            "bars",
            "provenance",
        ],
    ) || payload.get("schema_version").and_then(Value::as_str)
        != Some("yfinance-history-bridge-v1")
        || !matches!(
            payload.get("status").and_then(Value::as_str),
            Some("ready" | "partial")
        )
        || payload.get("provider").and_then(Value::as_str) != Some("yfinance")
        || payload.get("source").and_then(Value::as_str) != Some("Yahoo Finance")
        || payload.get("asset_class").and_then(Value::as_str) != Some(asset_class)
        || payload.get("classification_source").and_then(Value::as_str) != Some("caller_declared")
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
        || payload.get("volume_encoding").and_then(Value::as_str)
            != Some("integer_exact_or_safe_integer_float")
        || payload.get("canonical").and_then(Value::as_bool) != Some(false)
        || payload.get("point_in_time").and_then(Value::as_bool) != Some(false)
        || payload.get("executable").and_then(Value::as_bool) != Some(false)
        || payload.get("display_only").and_then(Value::as_bool) != Some(true)
        || payload
            .get("provider_repair_enabled")
            .and_then(Value::as_bool)
            != Some(false)
        || !has_exact_keys(requested_range.as_object(), &["start", "end", "semantics"])
        || requested_range.get("semantics").and_then(Value::as_str)
            != Some("start_inclusive_end_exclusive")
        || response_start != start
        || response_end != end
        || !symbols_partition_request(symbols, &returned_symbols, &missing_symbols, request)
        || (payload.get("status").and_then(Value::as_str) == Some("partial"))
            != !missing_symbols.is_empty()
        || !has_exact_keys(
            Some(coverage),
            &[
                "requested_start",
                "requested_end",
                "bar_count",
                "per_symbol",
                "gaps",
                "calendar_gaps_assessed",
            ],
        )
        || parse_json_timestamp(coverage.get("requested_start")) != Ok(start.clone())
        || parse_json_timestamp(coverage.get("requested_end")) != Ok(end.clone())
        || coverage
            .get("calendar_gaps_assessed")
            .and_then(Value::as_bool)
            != Some(false)
        || coverage.get("bar_count").and_then(Value::as_u64) != Some(bars.len() as u64)
        || symbols.len() != request.symbols.len()
        || !symbols
            .iter()
            .zip(&request.symbols)
            .all(|(actual, expected)| actual.as_str() == Some(expected.as_str()))
        || bars.len() > MAX_BRIDGE_ROWS
    {
        return Err(ProviderQueryError::MalformedResponse);
    }
    if bars.is_empty() {
        return Err(ProviderQueryError::EmptyResearchObservation);
    }
    let mut seen_bars = std::collections::BTreeSet::new();
    let mut bar_counts = std::collections::BTreeMap::<&str, usize>::new();
    let mut first_last = std::collections::BTreeMap::<&str, (DateTime<Utc>, DateTime<Utc>)>::new();
    let mut previous_bar_key: Option<(String, DateTime<Utc>)> = None;
    for bar in bars {
        if !has_exact_keys(
            bar.as_object(),
            &[
                "symbol",
                "asset_class",
                "event_ts_utc",
                "resolution",
                "adjustment",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
                "dividends",
                "stock_splits",
            ],
        ) {
            return Err(ProviderQueryError::MalformedResponse);
        }
        let symbol = bar
            .get("symbol")
            .and_then(Value::as_str)
            .ok_or(ProviderQueryError::MalformedResponse)?;
        let event_ts = parse_json_timestamp(bar.get("event_ts_utc"))?;
        if request.symbols.iter().all(|requested| requested != symbol)
            || bar.get("asset_class").and_then(Value::as_str) != Some(asset_class)
            || bar.get("resolution").and_then(Value::as_str) != Some("1d")
            || bar.get("adjustment").and_then(Value::as_str) != Some(requested_adjustment)
            || event_ts < start
            || event_ts >= end
            || !seen_bars.insert((symbol.to_owned(), event_ts.clone()))
            || previous_bar_key
                .as_ref()
                .is_some_and(|previous| previous > &(symbol.to_owned(), event_ts.clone()))
            || !bar.as_object().is_some_and(valid_bar_numbers)
        {
            return Err(ProviderQueryError::MalformedResponse);
        }
        previous_bar_key = Some((symbol.to_owned(), event_ts.clone()));
        *bar_counts.entry(symbol).or_default() += 1;
        first_last
            .entry(symbol)
            .and_modify(|bounds| {
                if event_ts < bounds.0 {
                    bounds.0 = event_ts.clone();
                }
                if event_ts > bounds.1 {
                    bounds.1 = event_ts.clone();
                }
            })
            .or_insert((event_ts.clone(), event_ts));
    }
    let bar_symbols = bar_counts
        .keys()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let returned_symbols = returned_symbols
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if !validate_per_symbol_coverage(coverage, request, &bar_counts, &first_last)
        || returned_symbols != bar_symbols
        || !validate_coverage_gaps(coverage.get("gaps"))
        || !validate_actions(payload.get("actions"), &request.symbols, start, end)
        || !validate_action_coverage(payload.get("action_coverage"))
    {
        return Err(ProviderQueryError::MalformedResponse);
    }
    let provenance = payload
        .get("provenance")
        .and_then(Value::as_object)
        .ok_or(ProviderQueryError::MalformedResponse)?;
    if !validate_provenance(payload, provenance, request, bars.len()) {
        return Err(ProviderQueryError::MalformedResponse);
    }
    Ok(())
}

fn has_exact_keys(object: Option<&serde_json::Map<String, Value>>, expected: &[&str]) -> bool {
    object.is_some_and(|object| {
        object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
    })
}

fn validate_provenance(
    payload: &Value,
    provenance: &serde_json::Map<String, Value>,
    request: &YFinanceResearchWindowRequest,
    bar_count: usize,
) -> bool {
    if !has_exact_keys(
        Some(provenance),
        &[
            "acquired_at",
            "adapter_version",
            "adjustment",
            "cache_status",
            "config_fingerprint",
            "end",
            "include_actions",
            "interval",
            "missing_symbols",
            "prepost",
            "provider",
            "query_fingerprint",
            "requested_at",
            "requested_symbols",
            "returned_symbols",
            "row_count",
            "schema_version",
            "served_at",
            "source",
            "start",
        ],
    ) {
        return false;
    }
    let adjustment = request.adjustment.bridge_value();
    let requested_symbols = string_array(provenance.get("requested_symbols"));
    let returned_symbols = string_array(provenance.get("returned_symbols"));
    let missing_symbols = string_array(provenance.get("missing_symbols"));
    let payload_returned_symbols = string_array(payload.get("returned_symbols"));
    let payload_missing_symbols = string_array(payload.get("missing_symbols"));
    let (Ok(requested_symbols), Ok(returned_symbols), Ok(missing_symbols)) =
        (requested_symbols, returned_symbols, missing_symbols)
    else {
        return false;
    };
    let (Ok(payload_returned_symbols), Ok(payload_missing_symbols)) =
        (payload_returned_symbols, payload_missing_symbols)
    else {
        return false;
    };
    let requested_set = request
        .symbols
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let returned_set = returned_symbols
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let missing_set = missing_symbols
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let payload_returned_set = payload_returned_symbols
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let payload_missing_set = payload_missing_symbols
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if requested_symbols
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        != requested_set
        || returned_set != payload_returned_set
        || missing_set != payload_missing_set
        || returned_set.len() != returned_symbols.len()
        || missing_set.len() != missing_symbols.len()
        || !returned_set.is_disjoint(&missing_set)
        || returned_set
            .union(&missing_set)
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            != requested_set
    {
        return false;
    }
    let cache_status = provenance.get("cache_status").and_then(Value::as_str);
    let response_cache_status = payload.get("cache_status").and_then(Value::as_str);
    let freshness = payload.get("freshness").and_then(Value::as_str);
    let timestamps = (
        parse_json_timestamp(provenance.get("served_at")),
        parse_json_timestamp(provenance.get("acquired_at")),
        parse_json_timestamp(provenance.get("requested_at")),
        parse_json_timestamp(provenance.get("start")),
        parse_json_timestamp(provenance.get("end")),
        parse_json_timestamp(payload.get("available_at")),
        parse_json_timestamp(payload.get("fetched_at")),
        parse_json_timestamp(payload.get("source_observed_at")),
        parse_json_timestamp(payload.get("request_started_at")),
    );
    let (
        Ok(served_at),
        Ok(acquired_at),
        Ok(requested_at),
        Ok(start),
        Ok(end),
        Ok(available_at),
        Ok(fetched_at),
        Ok(source_observed_at),
        Ok(request_started_at),
    ) = timestamps
    else {
        return false;
    };
    let Some(row_count) = provenance.get("row_count").and_then(Value::as_u64) else {
        return false;
    };
    provenance.get("schema_version").and_then(Value::as_str) == Some("research-bars-v1")
        && provenance.get("provider").and_then(Value::as_str) == Some("yahoo_finance")
        && provenance.get("source").and_then(Value::as_str) == Some("yfinance")
        && provenance
            .get("adapter_version")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty() && value.len() <= 128)
        && valid_sha256(provenance.get("query_fingerprint").and_then(Value::as_str))
        && valid_sha256(provenance.get("config_fingerprint").and_then(Value::as_str))
        && provenance.get("interval").and_then(Value::as_str) == Some("1d")
        && provenance.get("adjustment").and_then(Value::as_str) == Some(adjustment)
        && provenance.get("prepost").and_then(Value::as_bool) == Some(false)
        && provenance.get("include_actions").and_then(Value::as_bool) == Some(true)
        && row_count == bar_count as u64
        && cache_status
            .is_some_and(|value| matches!(value, "network" | "fresh_cache" | "stale_cache"))
        && response_cache_status == cache_status
        && freshness.is_some_and(|value| matches!(value, "retrieved" | "stale"))
        && start == request.start_utc
        && end == request.end_utc
        && requested_at <= acquired_at
        && acquired_at <= served_at
        && served_at == available_at
        && served_at == fetched_at
        && acquired_at == source_observed_at
        && requested_at == request_started_at
        && match (cache_status, freshness) {
            (Some("stale_cache"), Some("stale")) => true,
            (Some("network" | "fresh_cache"), Some("retrieved")) => true,
            _ => false,
        }
}

fn valid_sha256(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn string_array(value: Option<&Value>) -> Result<Vec<&str>, ProviderQueryError> {
    value
        .and_then(Value::as_array)
        .ok_or(ProviderQueryError::MalformedResponse)?
        .iter()
        .map(|item| item.as_str().ok_or(ProviderQueryError::MalformedResponse))
        .collect()
}

fn symbols_partition_request(
    requested: &[Value],
    returned: &[&str],
    missing: &[&str],
    request: &YFinanceResearchWindowRequest,
) -> bool {
    let requested_names = requested
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let requested_set = request
        .symbols
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let returned_set = returned
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let missing_set = missing
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    requested_names
        == request
            .symbols
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
        && returned_set.len() == returned.len()
        && missing_set.len() == missing.len()
        && returned_set.is_disjoint(&missing_set)
        && returned_set
            .union(&missing_set)
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            == requested_set
}

fn validate_per_symbol_coverage(
    coverage: &serde_json::Map<String, Value>,
    request: &YFinanceResearchWindowRequest,
    bar_counts: &std::collections::BTreeMap<&str, usize>,
    first_last: &std::collections::BTreeMap<&str, (DateTime<Utc>, DateTime<Utc>)>,
) -> bool {
    let Some(per_symbol) = coverage.get("per_symbol").and_then(Value::as_array) else {
        return false;
    };
    if per_symbol.len() != request.symbols.len() {
        return false;
    }
    let mut seen = std::collections::BTreeSet::new();
    for item in per_symbol {
        let Some(item) = item.as_object() else {
            return false;
        };
        if !has_exact_keys(
            Some(item),
            &["symbol", "bar_count", "first_bar_at", "last_bar_at", "gaps"],
        ) {
            return false;
        }
        let Some(symbol) = item.get("symbol").and_then(Value::as_str) else {
            return false;
        };
        if request.symbols.iter().all(|requested| requested != symbol) || !seen.insert(symbol) {
            return false;
        }
        let count = item.get("bar_count").and_then(Value::as_u64);
        let expected_count = bar_counts.get(symbol).copied().unwrap_or(0) as u64;
        if count != Some(expected_count) {
            return false;
        }
        let expected_bounds = first_last.get(symbol).cloned();
        if !matches_optional_timestamp(
            item.get("first_bar_at"),
            expected_bounds.as_ref().map(|bounds| bounds.0.clone()),
        ) || !matches_optional_timestamp(
            item.get("last_bar_at"),
            expected_bounds.map(|bounds| bounds.1),
        ) || !item
            .get("gaps")
            .and_then(Value::as_array)
            .is_some_and(|gaps| {
                gaps.len() <= 8
                    && gaps.iter().all(|gap| {
                        gap.as_str()
                            .is_some_and(|code| !code.is_empty() && code.len() <= 64)
                    })
            })
        {
            return false;
        }
    }
    seen.len() == request.symbols.len()
}

fn matches_optional_timestamp(value: Option<&Value>, expected: Option<DateTime<Utc>>) -> bool {
    match (value, expected) {
        (Some(Value::Null), None) => true,
        (Some(value), Some(expected)) => parse_json_timestamp(Some(value)) == Ok(expected),
        _ => false,
    }
}

fn validate_coverage_gaps(value: Option<&Value>) -> bool {
    value.and_then(Value::as_array).is_some_and(|gaps| {
        gaps.len() <= MAX_BRIDGE_SYMBOLS * 8 + 8
            && gaps.iter().all(|gap| {
                gap.as_object().is_some_and(|gap| {
                    ((gap.len() == 1 && gap.contains_key("code"))
                        || (gap.len() == 2
                            && gap.contains_key("code")
                            && gap.contains_key("symbol")))
                        && gap
                            .get("code")
                            .and_then(Value::as_str)
                            .is_some_and(|code| !code.is_empty() && code.len() <= 64)
                        && gap
                            .get("symbol")
                            .is_none_or(|symbol| symbol.as_str().is_some_and(valid_symbol))
                })
            })
    })
}

fn validate_action_coverage(value: Option<&Value>) -> bool {
    let Some(value) = value.and_then(Value::as_object) else {
        return false;
    };
    has_exact_keys(
        Some(value),
        &["dividends", "stock_splits", "symbol_renames"],
    ) && ["dividends", "stock_splits"].iter().all(|field| {
        matches!(
            value.get(*field).and_then(Value::as_str),
            Some("provider_field_present" | "provider_field_unavailable")
        )
    }) && value.get("symbol_renames").and_then(Value::as_str) == Some("unsupported")
}

fn validate_actions(
    value: Option<&Value>,
    symbols: &[String],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> bool {
    value.and_then(Value::as_array).is_some_and(|actions| {
        actions.len() <= MAX_BRIDGE_ROWS * 2
            && actions.iter().all(|action| {
                let Some(action) = action.as_object() else {
                    return false;
                };
                let Some(symbol) = action.get("symbol").and_then(Value::as_str) else {
                    return false;
                };
                let Ok(event_ts) = parse_json_timestamp(action.get("event_ts_utc")) else {
                    return false;
                };
                let action_keys = match action.get("kind").and_then(Value::as_str) {
                    Some("DIVIDEND") => &["symbol", "event_ts_utc", "kind", "amount", "source"][..],
                    Some("SPLIT") => &["symbol", "event_ts_utc", "kind", "ratio", "source"][..],
                    _ => return false,
                };
                if !has_exact_keys(Some(action), action_keys) {
                    return false;
                }
                let source_ok =
                    action.get("source").and_then(Value::as_str) == Some("Yahoo Finance");
                let common_ok = symbols.iter().any(|requested| requested == symbol)
                    && event_ts >= start
                    && event_ts < end
                    && source_ok;
                match action.get("kind").and_then(Value::as_str) {
                    Some("DIVIDEND") => {
                        common_ok && valid_non_null_decimal_value(action.get("amount"))
                    }
                    Some("SPLIT") => common_ok && valid_non_null_decimal_value(action.get("ratio")),
                    _ => false,
                }
            })
    })
}

fn valid_bar_numbers(bar: &serde_json::Map<String, Value>) -> bool {
    [
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "dividends",
        "stock_splits",
    ]
    .iter()
    .all(|field| bar.get(*field).is_some_and(valid_decimal_value))
        && bar.get("volume").is_some_and(valid_integer_value)
}

fn valid_integer_value(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        }
        _ => false,
    }
}

fn valid_decimal_value(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => valid_decimal_string(value),
        _ => false,
    }
}

fn valid_non_null_decimal_value(value: Option<&Value>) -> bool {
    value.is_some_and(|value| !value.is_null() && valid_decimal_value(value))
}

fn valid_decimal_string(value: &str) -> bool {
    if value.is_empty() || value.len() > 64 {
        return false;
    }
    let bytes = value.as_bytes();
    let digits = bytes.iter().filter(|byte| byte.is_ascii_digit()).count();
    let dots = bytes.iter().filter(|byte| **byte == b'.').count();
    let sign_offset = usize::from(matches!(bytes.first(), Some(b'-' | b'+')));
    digits > 0
        && dots <= 1
        && bytes
            .iter()
            .skip(sign_offset)
            .all(|byte| byte.is_ascii_digit() || *byte == b'.')
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
        422 => ProviderQueryError::ProviderCapabilityUnavailable,
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

fn valid_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
        && !is_occ_option_symbol(value)
}

fn is_occ_option_symbol(value: &str) -> bool {
    let bytes = value.as_bytes();
    let Some(root_length) = bytes.len().checked_sub(15) else {
        return false;
    };
    (1..=6).contains(&root_length)
        && bytes[..root_length]
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'.')
        && bytes[root_length..root_length + 6]
            .iter()
            .all(u8::is_ascii_digit)
        && matches!(bytes[root_length + 6], b'C' | b'P')
        && bytes[root_length + 7..].iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, Utc};
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

    use super::{
        YFinanceBridge, YFinanceResearchAdjustment, YFinanceResearchWindowRequest, Zeroizing,
        validate_research_window_page, validate_window_request,
    };

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

            let response_body = bridge_payload(
                &["AAPL"],
                json!([{
                    "symbol": "AAPL",
                    "asset_class": "stock",
                    "resolution": "1d",
                    "adjustment": "unadjusted",
                    "event_ts_utc": "2026-10-13T00:00:00Z",
                    "close": "250.0"
                }]),
                "2026-10-13T00:00:00Z",
                "2026-10-14T00:00:00Z",
                "unadjusted",
            );
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

    #[tokio::test]
    async fn posts_all_symbols_in_one_bounded_window_and_preserves_native_rows() {
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
            let sent: Value = serde_json::from_slice(&body).expect("valid request JSON");
            assert_eq!(sent["symbols"], json!(["AAPL", "SPY"]));
            assert_eq!(sent["start"], "2026-10-13T00:00:00Z");
            assert_eq!(sent["end"], "2026-10-14T00:00:00Z");
            assert_eq!(sent["asset_class"], "stock");
            assert_eq!(sent["adjustment"], "adjusted");

            let response_body = bridge_payload(
                &["AAPL", "SPY"],
                json!([
                    {
                        "symbol": "AAPL",
                        "asset_class": "stock",
                        "resolution": "1d",
                        "adjustment": "adjusted",
                        "event_ts_utc": "2026-10-13T00:00:00Z",
                        "close": "250.0"
                    },
                    {
                        "symbol": "SPY",
                        "asset_class": "stock",
                        "resolution": "1d",
                        "adjustment": "adjusted",
                        "event_ts_utc": "2026-10-13T00:00:00Z",
                        "close": "700.0"
                    }
                ]),
                "2026-10-13T00:00:00Z",
                "2026-10-14T00:00:00Z",
                "adjusted",
            );
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

        let bridge = YFinanceBridge::new(
            true,
            port,
            Some(Zeroizing::new(
                "test-research-bridge-token-12345".to_owned(),
            )),
        )
        .expect("build local bridge client");
        let request = YFinanceResearchWindowRequest {
            symbols: vec!["AAPL".to_owned(), "SPY".to_owned()],
            asset_class: AssetClassV1::Stock,
            start_utc: DateTime::parse_from_rfc3339("2026-10-13T00:00:00Z")
                .expect("valid start")
                .with_timezone(&Utc),
            end_utc: DateTime::parse_from_rfc3339("2026-10-14T00:00:00Z")
                .expect("valid end")
                .with_timezone(&Utc),
            adjustment: YFinanceResearchAdjustment::Adjusted,
        };
        let context =
            ProviderRequestContext::new(tokio::time::Instant::now() + Duration::from_secs(5), None);
        let page = bridge
            .fetch_research_window(&request, &context)
            .await
            .expect("fetch provider-native multi-symbol research page");
        server.await.expect("local HTTP server completed");

        assert_eq!(page.payload()["requested_symbols"], json!(["AAPL", "SPY"]));
        assert_eq!(page.payload()["bars"].as_array().unwrap().len(), 2);
        assert_eq!(page.payload()["bars"][0]["close"], "250.0");
        assert_eq!(page.payload()["bars"][1]["close"], "700.0");
    }

    #[test]
    fn research_window_validation_rejects_duplicates_oversize_and_out_of_window_rows() {
        let request = YFinanceResearchWindowRequest {
            symbols: vec!["AAPL".to_owned(), "SPY".to_owned()],
            asset_class: AssetClassV1::Stock,
            start_utc: DateTime::parse_from_rfc3339("2026-10-13T00:00:00Z")
                .expect("valid start")
                .with_timezone(&Utc),
            end_utc: DateTime::parse_from_rfc3339("2026-10-14T00:00:00Z")
                .expect("valid end")
                .with_timezone(&Utc),
            adjustment: YFinanceResearchAdjustment::Unadjusted,
        };
        assert!(validate_window_request(&request).is_ok());

        let mut long_symbol = request.clone();
        long_symbol.symbols[0] = "A".repeat(32);
        assert!(validate_window_request(&long_symbol).is_ok());

        let mut option_symbol = request.clone();
        option_symbol.symbols[0] = "AAPL261016C00100000".to_owned();
        assert!(validate_window_request(&option_symbol).is_err());

        let mut duplicate = request.clone();
        duplicate.symbols[1] = "AAPL".to_owned();
        assert!(validate_window_request(&duplicate).is_err());

        let mut oversized = request.clone();
        oversized.end_utc = oversized.start_utc + chrono::Duration::days(3_651);
        assert!(validate_window_request(&oversized).is_err());

        let payload = bridge_payload(
            &["AAPL", "SPY"],
            json!([{
                "symbol": "AAPL",
                "asset_class": "stock",
                "resolution": "1d",
                "adjustment": "unadjusted",
                "event_ts_utc": "2026-10-14T00:00:00Z",
                "close": "250.0"
            }]),
            "2026-10-13T00:00:00Z",
            "2026-10-14T00:00:00Z",
            "unadjusted",
        );
        assert!(validate_research_window_page(&payload, &request).is_err());
    }

    #[test]
    fn all_empty_bridge_windows_fail_closed_as_typed_provider_error() {
        let request = YFinanceResearchWindowRequest {
            symbols: vec!["AAPL".to_owned(), "SPY".to_owned()],
            asset_class: AssetClassV1::Stock,
            start_utc: DateTime::parse_from_rfc3339("2026-10-13T00:00:00Z")
                .expect("valid start")
                .with_timezone(&Utc),
            end_utc: DateTime::parse_from_rfc3339("2026-10-14T00:00:00Z")
                .expect("valid end")
                .with_timezone(&Utc),
            adjustment: YFinanceResearchAdjustment::Unadjusted,
        };
        let payload = bridge_payload(
            &["AAPL", "SPY"],
            json!([]),
            "2026-10-13T00:00:00Z",
            "2026-10-14T00:00:00Z",
            "unadjusted",
        );
        assert_eq!(
            validate_research_window_page(&payload, &request),
            Err(crate::market_query::providers::ProviderQueryError::EmptyResearchObservation)
        );
    }

    fn bridge_payload(
        symbols: &[&str],
        mut bars: Value,
        start: &str,
        end: &str,
        adjustment: &str,
    ) -> Value {
        let rows = bars.as_array_mut().expect("fixture bars are an array");
        let mut per_symbol = std::collections::BTreeMap::<String, Vec<String>>::new();
        for row in rows.iter_mut() {
            let object = row.as_object_mut().expect("fixture bar is an object");
            object.insert("asset_class".to_owned(), Value::String("stock".to_owned()));
            object.insert("resolution".to_owned(), Value::String("1d".to_owned()));
            object.insert(
                "adjustment".to_owned(),
                Value::String(adjustment.to_owned()),
            );
            for field in [
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
                "dividends",
                "stock_splits",
            ] {
                object
                    .entry(field.to_owned())
                    .or_insert_with(|| Value::String("0".to_owned()));
            }
            let symbol = object
                .get("symbol")
                .and_then(Value::as_str)
                .expect("bar symbol")
                .to_owned();
            let event_ts = object
                .get("event_ts_utc")
                .and_then(Value::as_str)
                .expect("bar timestamp")
                .to_owned();
            per_symbol.entry(symbol).or_default().push(event_ts);
        }
        let returned_symbols = per_symbol.keys().cloned().collect::<Vec<_>>();
        let missing_symbols = symbols
            .iter()
            .filter(|symbol| !per_symbol.contains_key(**symbol))
            .copied()
            .collect::<Vec<_>>();
        let per_symbol_coverage = symbols
            .iter()
            .map(|symbol| {
                let events = per_symbol.get(*symbol);
                json!({
                    "symbol": symbol,
                    "bar_count": events.map_or(0, Vec::len),
                    "first_bar_at": events.and_then(|events| events.first()),
                    "last_bar_at": events.and_then(|events| events.last()),
                    "gaps": ["DATE_GAPS_UNASSESSED", "SYMBOL_RENAME_HISTORY_UNAVAILABLE"]
                })
            })
            .collect::<Vec<_>>();
        let bars_len = rows.len();
        let status = if missing_symbols.is_empty() {
            "ready"
        } else {
            "partial"
        };
        let requested_range = json!({
            "start": start,
            "end": end,
            "semantics": "start_inclusive_end_exclusive"
        });
        let coverage_gaps = json!([
            {"code": "DATE_GAPS_UNASSESSED"},
            {"code": "SYMBOL_RENAME_HISTORY_UNAVAILABLE"},
            {"code": "NON_POINT_IN_TIME_PROVIDER_HISTORY"}
        ]);
        let coverage = json!({
            "requested_start": start,
            "requested_end": end,
            "bar_count": bars_len,
            "per_symbol": per_symbol_coverage,
            "gaps": coverage_gaps,
            "calendar_gaps_assessed": false
        });
        let action_coverage = json!({
            "dividends": "provider_field_present",
            "stock_splits": "provider_field_present",
            "symbol_renames": "unsupported"
        });
        let provenance = json!({
            "schema_version": "research-bars-v1",
            "provider": "yahoo_finance",
            "source": "yfinance",
            "adapter_version": "synthetic-fixture",
            "query_fingerprint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "config_fingerprint": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "interval": "1d",
            "adjustment": adjustment,
            "prepost": false,
            "include_actions": true,
            "requested_symbols": symbols,
            "returned_symbols": returned_symbols,
            "missing_symbols": missing_symbols,
            "start": start,
            "end": end,
            "requested_at": "2026-10-13T00:00:00Z",
            "acquired_at": "2026-10-14T00:00:00Z",
            "served_at": "2026-10-14T00:00:00Z",
            "cache_status": "network",
            "row_count": bars_len
        });
        json!({
            "schema_version": "yfinance-history-bridge-v1",
            "status": status,
            "provider": "yfinance",
            "source": "Yahoo Finance",
            "asset_class": "stock",
            "classification_source": "caller_declared",
            "resolution": "1d",
            "effective_resolution": "1d",
            "adjustment": adjustment,
            "provider_adjustment_semantics": "unknown_or_provider_native",
            "adjustment_confirmed_raw": false,
            "provider_repair_enabled": false,
            "numeric_encoding": "binary_float_projection",
            "volume_encoding": "integer_exact_or_safe_integer_float",
            "canonical": false,
            "point_in_time": false,
            "executable": false,
            "display_only": true,
            "requested_range": requested_range,
            "requested_symbols": symbols,
            "returned_symbols": returned_symbols,
            "missing_symbols": missing_symbols,
            "cache_status": "network",
            "freshness": "retrieved",
            "coverage": coverage,
            "action_coverage": action_coverage,
            "actions": [],
            "bars": bars,
            "available_at": "2026-10-14T00:00:00Z",
            "fetched_at": "2026-10-14T00:00:00Z",
            "source_observed_at": "2026-10-14T00:00:00Z",
            "request_started_at": "2026-10-13T00:00:00Z",
            "provenance": provenance
        })
    }
}
