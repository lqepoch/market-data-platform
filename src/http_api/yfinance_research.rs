//! Authenticated, provider-native Yahoo daily observations for research display.
//!
//! This route is separate from Core Bars V1 and from the market-bars feature. Its response keeps
//! the bridge payload intact and never asserts raw-price semantics, complete coverage, PIT, or
//! execution authority.

use std::{io, io::Write, sync::Arc, time::Duration};

use axum::{
    Json,
    body::Body,
    extract::{Extension, Query, State},
    http::{StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use market_contracts::AssetClassV1;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::market_query::providers::{
    ProviderQueryError, ProviderRequestContext, YFinanceResearchAdjustment,
    YFinanceResearchObservationPage, YFinanceResearchWindowRequest,
};

use super::{ServiceState, auth::VerifiedMarketPrincipal, supervisor::YFinanceResearchFailure};

const MAX_QUERY_BYTES: usize = 16 * 1024;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CURSOR_BYTES: usize = 8 * 1024;
const MAX_SYMBOLS: usize = 8;
const MAX_RANGE_DAYS: i64 = 36_500;
const MAX_FUTURE_DAYS: i64 = 1;
const MAX_PAGE_DAYS: i64 = 3_650;
const PROVIDER_DEADLINE: Duration = Duration::from_secs(110);
const SCHEMA_VERSION: &str = "mdp-yfinance-research-observations-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DailyQuery {
    symbols: String,
    asset_class: String,
    start_utc: String,
    end_utc: String,
    adjustment: Option<String>,
    cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryError {
    Invalid,
    Unsupported,
    TooLarge,
}

#[derive(Clone)]
struct ParsedQuery {
    request: YFinanceResearchWindowRequest,
    cursor: Option<String>,
}

#[derive(Serialize)]
struct UtcRange {
    start_utc: String,
    end_utc: String,
    semantics: &'static str,
}

#[derive(Serialize)]
struct Observation<'a> {
    source_observation_sha256: &'a str,
    fetched_at_utc: &'a str,
    payload: &'a Value,
}

#[derive(Serialize)]
struct DailyEnvelope<'a> {
    schema_version: &'static str,
    provider: &'static str,
    source: &'static str,
    asset_class: &'static str,
    resolution: &'static str,
    requested_adjustment: &'static str,
    provider_adjustment_semantics: &'static str,
    adjustment_confirmed_raw: bool,
    numeric_encoding: &'static str,
    canonical: bool,
    point_in_time: bool,
    executable: bool,
    display_only: bool,
    mdp_coverage_asserted: bool,
    requested_symbols: &'a [String],
    requested_range: UtcRange,
    page_range: UtcRange,
    observation: Observation<'a>,
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
struct ApiErrorBody {
    schema_version: &'static str,
    status: &'static str,
    error: ApiErrorDetail,
}

#[derive(Serialize)]
struct ApiErrorDetail {
    code: &'static str,
    message: &'static str,
    retryable: bool,
}

pub(super) async fn get_daily_observations(
    State(state): State<Arc<ServiceState>>,
    Extension(principal): Extension<VerifiedMarketPrincipal>,
    uri: Uri,
) -> Response {
    if !principal.is_research() {
        return error_response(
            StatusCode::FORBIDDEN,
            "research_principal_required",
            "This route requires the research market-read principal.",
            false,
        );
    }
    let query_string = uri.query().unwrap_or_default();
    if query_string.len() > MAX_QUERY_BYTES {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "The query string exceeds the configured byte limit.",
            false,
        );
    }
    let Query(query) = match Query::<DailyQuery>::try_from_uri(&uri) {
        Ok(query) => query,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_query",
                "The query parameters are malformed or contain unsupported fields.",
                false,
            );
        }
    };
    let parsed = match parse_query(query, Utc::now()) {
        Ok(parsed) => parsed,
        Err(error) => return query_error_response(error),
    };
    let Some(runtime) = state.yfinance_research.as_ref().cloned() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "research_api_disabled",
            "The Yahoo research API is disabled by server configuration.",
            true,
        );
    };

    let now_unix_seconds = match current_unix_seconds() {
        Some(now) => now,
        None => {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "clock_unavailable",
                "The server clock is unavailable.",
                true,
            );
        }
    };
    let selection = match runtime.select_page(
        &parsed.request,
        parsed.cursor.as_deref(),
        principal.principal_scope_sha256(),
        now_unix_seconds,
    ) {
        Ok(selection) => selection,
        Err(error) => return provider_error_response(error),
    };
    if selection.window.end_utc - selection.window.start_utc > ChronoDuration::days(MAX_PAGE_DAYS) {
        return provider_error_response(ProviderQueryError::InvalidCursor);
    }

    let page_request = YFinanceResearchWindowRequest {
        symbols: parsed.request.symbols.clone(),
        asset_class: parsed.request.asset_class,
        start_utc: selection.window.start_utc,
        end_utc: selection.window.end_utc,
        adjustment: parsed.request.adjustment,
    };
    let fetch_runtime = Arc::clone(&runtime);
    let page = match state
        .query_client
        .yfinance_research(move |_cancellation| async move {
            let context =
                ProviderRequestContext::new(tokio::time::Instant::now() + PROVIDER_DEADLINE, None);
            fetch_runtime.fetch_window(&page_request, &context).await
        })
        .await
    {
        Ok(page) => page,
        Err(YFinanceResearchFailure::Timeout) => {
            return error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "provider_timeout",
                "The Yahoo research provider exceeded its request deadline.",
                true,
            );
        }
        Err(YFinanceResearchFailure::Unavailable) => {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "research_worker_unavailable",
                "The bounded Yahoo research worker is unavailable or at capacity.",
                true,
            );
        }
        Err(YFinanceResearchFailure::Provider(error)) => {
            return provider_error_response(error);
        }
    };

    let now_unix_seconds = match current_unix_seconds() {
        Some(now) => now,
        None => {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "clock_unavailable",
                "The server clock is unavailable.",
                true,
            );
        }
    };
    let next_cursor = match runtime.cursor_for_previous_page(
        &selection,
        parsed.request.start_utc,
        principal.principal_scope_sha256(),
        now_unix_seconds,
    ) {
        Ok(cursor) => cursor,
        Err(error) => return provider_error_response(error),
    };
    success_response(
        &parsed.request,
        selection.window,
        &page,
        next_cursor.as_deref(),
    )
}

fn parse_query(query: DailyQuery, now: DateTime<Utc>) -> Result<ParsedQuery, QueryError> {
    let symbols = query
        .symbols
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if symbols.is_empty()
        || symbols.len() > MAX_SYMBOLS
        || symbols.iter().any(|symbol| !valid_symbol(symbol))
        || symbols
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != symbols.len()
    {
        return Err(QueryError::Unsupported);
    }
    let asset_class = match query.asset_class.as_str() {
        "stock" => AssetClassV1::Stock,
        "etf" => AssetClassV1::Etf,
        _ => return Err(QueryError::Unsupported),
    };
    let adjustment = match query.adjustment.as_deref().unwrap_or("unadjusted") {
        "unadjusted" => YFinanceResearchAdjustment::Unadjusted,
        "adjusted" => YFinanceResearchAdjustment::Adjusted,
        _ => return Err(QueryError::Unsupported),
    };
    let start_utc = parse_utc_timestamp(&query.start_utc)?;
    let end_utc = parse_utc_timestamp(&query.end_utc)?;
    let supported_start = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
        .expect("constant timestamp")
        .with_timezone(&Utc);
    if start_utc < supported_start
        || start_utc >= end_utc
        || end_utc > now + ChronoDuration::days(MAX_FUTURE_DAYS)
    {
        return Err(QueryError::Unsupported);
    }
    if end_utc.signed_duration_since(start_utc) > ChronoDuration::days(MAX_RANGE_DAYS) {
        return Err(QueryError::TooLarge);
    }
    if query
        .cursor
        .as_ref()
        .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES)
    {
        return Err(
            if query
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.len() > MAX_CURSOR_BYTES)
            {
                QueryError::TooLarge
            } else {
                QueryError::Invalid
            },
        );
    }
    Ok(ParsedQuery {
        request: YFinanceResearchWindowRequest {
            symbols,
            asset_class,
            start_utc,
            end_utc,
            adjustment,
        },
        cursor: query.cursor,
    })
}

fn parse_utc_timestamp(value: &str) -> Result<DateTime<Utc>, QueryError> {
    if !value.ends_with('Z') {
        return Err(QueryError::Invalid);
    }
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| QueryError::Invalid)?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err(QueryError::Invalid);
    }
    Ok(parsed.with_timezone(&Utc))
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

fn success_response(
    request: &YFinanceResearchWindowRequest,
    page_window: crate::market_query::providers::research_cursor::ResearchWindow,
    page: &YFinanceResearchObservationPage,
    next_cursor: Option<&str>,
) -> Response {
    let requested_adjustment = match request.adjustment {
        YFinanceResearchAdjustment::Unadjusted => "unadjusted",
        YFinanceResearchAdjustment::Adjusted => "adjusted",
    };
    let asset_class = match request.asset_class {
        AssetClassV1::Stock => "stock",
        AssetClassV1::Etf => "etf",
    };
    let envelope = DailyEnvelope {
        schema_version: SCHEMA_VERSION,
        provider: "yfinance",
        source: "Yahoo Finance",
        asset_class,
        resolution: "1d",
        requested_adjustment,
        provider_adjustment_semantics: "unknown_or_provider_native",
        adjustment_confirmed_raw: false,
        numeric_encoding: "binary_float_projection",
        canonical: false,
        point_in_time: false,
        executable: false,
        display_only: true,
        mdp_coverage_asserted: false,
        requested_symbols: &request.symbols,
        requested_range: utc_range(request.start_utc, request.end_utc),
        page_range: utc_range(page_window.start_utc, page_window.end_utc),
        observation: Observation {
            source_observation_sha256: page.source_observation_sha256(),
            fetched_at_utc: page.fetched_at_utc().as_str(),
            payload: page.payload(),
        },
        next_cursor,
    };
    let mut bytes = Vec::with_capacity(64 * 1024);
    let mut writer = ResearchBoundedWriter::new(&mut bytes);
    if serde_json::to_writer(&mut writer, &envelope).is_err() {
        return if writer.exceeded {
            error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "response_too_large",
                "The provider-native observation exceeds the response byte limit.",
                false,
            )
        } else {
            error_response(
                StatusCode::BAD_GATEWAY,
                "response_serialization_failed",
                "The provider-native observation could not be serialized.",
                false,
            )
        };
    }
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response
}

fn utc_range(start_utc: DateTime<Utc>, end_utc: DateTime<Utc>) -> UtcRange {
    UtcRange {
        start_utc: start_utc.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        end_utc: end_utc.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        semantics: "start_inclusive_end_exclusive",
    }
}

fn query_error_response(error: QueryError) -> Response {
    match error {
        QueryError::Invalid => error_response(
            StatusCode::BAD_REQUEST,
            "invalid_query",
            "The query is malformed or uses a non-UTC timestamp.",
            false,
        ),
        QueryError::Unsupported => error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_query",
            "The query is outside the supported Yahoo daily stock/ETF contract.",
            false,
        ),
        QueryError::TooLarge => error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "query_limit_exceeded",
            "The query exceeds the configured range or cursor bound.",
            false,
        ),
    }
}

fn provider_error_response(error: ProviderQueryError) -> Response {
    let (status, code, message, retryable) = match error {
        ProviderQueryError::ProviderDisabled => (
            StatusCode::SERVICE_UNAVAILABLE,
            "bridge_disabled",
            "The Yahoo research bridge is disabled by server configuration.",
            true,
        ),
        ProviderQueryError::MissingCredentials => (
            StatusCode::SERVICE_UNAVAILABLE,
            "bridge_unavailable",
            "The Yahoo research bridge is not configured.",
            true,
        ),
        ProviderQueryError::Unauthorized => (
            StatusCode::UNAUTHORIZED,
            "bridge_unauthorized",
            "The Yahoo research bridge rejected its service credential.",
            false,
        ),
        ProviderQueryError::Forbidden => (
            StatusCode::FORBIDDEN,
            "bridge_forbidden",
            "The Yahoo research bridge denied the request.",
            false,
        ),
        ProviderQueryError::RateLimited => (
            StatusCode::TOO_MANY_REQUESTS,
            "bridge_rate_limited",
            "The Yahoo research bridge rate limited the request.",
            true,
        ),
        ProviderQueryError::Timeout => (
            StatusCode::GATEWAY_TIMEOUT,
            "provider_timeout",
            "The Yahoo research provider exceeded its request deadline.",
            true,
        ),
        ProviderQueryError::ProviderUnavailable
        | ProviderQueryError::InvalidConfiguration
        | ProviderQueryError::GatewayDelegationMissing
        | ProviderQueryError::ProviderGroupUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "bridge_unavailable",
            "The Yahoo research bridge is unavailable.",
            true,
        ),
        ProviderQueryError::ResponseTooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "response_too_large",
            "The provider-native observation exceeds the configured byte limit.",
            false,
        ),
        ProviderQueryError::CursorExpired => (
            StatusCode::GONE,
            "cursor_expired",
            "The research cursor has expired; restart from the newest page.",
            false,
        ),
        ProviderQueryError::InvalidCursor
        | ProviderQueryError::CursorQueryMismatch
        | ProviderQueryError::CursorPlanChanged => (
            StatusCode::BAD_REQUEST,
            "invalid_cursor",
            "The research cursor is invalid or bound to a different request.",
            false,
        ),
        ProviderQueryError::UnsupportedAdjustment
        | ProviderQueryError::AdjustmentSemanticsUnverified
        | ProviderQueryError::ProviderCapabilityUnavailable => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_query",
            "The provider cannot satisfy the requested research query.",
            false,
        ),
        ProviderQueryError::MalformedResponse
        | ProviderQueryError::EmptyResearchObservation
        | ProviderQueryError::PartialPageLimit
        | ProviderQueryError::InvalidCorePage => (
            StatusCode::BAD_GATEWAY,
            "provider_observation_invalid",
            "The Yahoo research bridge returned an unavailable or invalid observation.",
            false,
        ),
    };
    error_response(status, code, message, retryable)
}

fn current_unix_seconds() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn error_response(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    retryable: bool,
) -> Response {
    (
        status,
        Json(ApiErrorBody {
            schema_version: SCHEMA_VERSION,
            status: "error",
            error: ApiErrorDetail {
                code,
                message,
                retryable,
            },
        }),
    )
        .into_response()
}

struct ResearchBoundedWriter<'a> {
    bytes: &'a mut Vec<u8>,
    exceeded: bool,
}

impl<'a> ResearchBoundedWriter<'a> {
    fn new(bytes: &'a mut Vec<u8>) -> Self {
        Self {
            bytes,
            exceeded: false,
        }
    }
}

impl Write for ResearchBoundedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_RESPONSE_BYTES)
            .ok_or_else(|| {
                self.exceeded = true;
                io::Error::other("Yahoo research response limit exceeded")
            })?;
        self.bytes.reserve(length - self.bytes.len());
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "yfinance_research_tests.rs"]
mod tests;
