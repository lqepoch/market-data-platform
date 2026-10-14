//! Read-only market bars and trusted-session HTTP adapters.
//!
//! This module is deliberately mounted behind the existing `market:read` middleware. It accepts
//! only Core Bars V1 fields, uses the startup-pinned calendar snapshot, and delegates provider
//! work to the shared bounded query supervisor. Gateway delegation is passed only to provider
//! code; it never authenticates an MDP request.

use std::{
    collections::VecDeque,
    env,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    body::Body,
    extract::{Extension, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};
use market_contracts::{BarsCoverageStatusV1, BarsRequestV1, BarsResponseV1, ProtoTimestampV2};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{QueryClient, QueryFailure, ServiceState, auth::VerifiedMarketPrincipal};

const MAX_QUERY_BYTES: usize = 8 * 1024;
const MAX_SESSION_WINDOW_DAYS: i64 = 366;
const MAX_SESSION_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const PROVIDER_DEADLINE: Duration = Duration::from_secs(110);
const MARKET_BARS_ENABLED_ENV: &str = "MDP_MARKET_BARS_ENABLED";
const MARKET_CACHE_ROOT_ENV: &str = "MDP_MARKET_CACHE_ROOT";
const MARKET_CACHE_SCOPE_ENV: &str = "MDP_MARKET_CACHE_SCOPE_ID";
const GATEWAY_DELEGATION_HEADER: HeaderName = HeaderName::from_static("x-eqo-gateway-delegation");
const MAX_CACHE_INVALIDATION_EVENTS: usize = 128;

#[cfg(all(test, target_os = "linux"))]
#[path = "market_bars_authority_tests.rs"]
mod authority_tests;
mod query;
mod response;

/// Service-owned read-only provider and optional local cache runtime.
///
/// Root/state initialization is owned by the HTTP service. Credentials are loaded by the provider
/// runtime and are never copied into query-derived cache keys or response objects.
pub(super) struct MarketBarsRuntime {
    provider: crate::market_query::providers::MarketBarsProviderRuntime,
    cache: Option<crate::archive::reconciliation::CacheVersionStoreV1>,
    cache_scope_id: Option<String>,
    authority: Option<Arc<MarketBarsAuthorityRuntime>>,
    page_admission_evidence_source: Option<Arc<dyn MarketBarsPageAdmissionEvidenceSource>>,
    cache_invalidation_events: Arc<Mutex<VecDeque<MarketBarsCacheInvalidationEvent>>>,
    #[cfg(test)]
    synthetic_page_source: Option<Arc<dyn SyntheticMarketBarsPageSource>>,
}

/// Test-only provider seam for exercising the real authenticated HTTP, supervisor, mapping, and
/// cache path. The explicit type name and compile guard prevent fixture facts from entering a
/// production build or being presented as provider verification.
#[cfg(test)]
pub(crate) trait SyntheticMarketBarsPageSource: Send + Sync {
    fn source_capabilities(&self) -> Vec<crate::session_router::SourceCapability>;

    fn cache_identity(
        &self,
        selection: &crate::market_query::providers::ProviderPageSelection,
    ) -> crate::market_query::providers::ProviderCacheIdentity;

    fn fetch_page(
        &self,
        plan: &crate::market_query::planning::MarketBarsPlan,
        selection: &crate::market_query::providers::ProviderPageSelection,
    ) -> std::result::Result<
        crate::market_query::providers::ProviderPageFetch,
        crate::market_query::providers::ProviderQueryError,
    >;
}

/// Independently trusted admission material supplied by an embedding owner. There is deliberately
/// no environment or HTTP path that can construct this authority context.
pub(super) struct MarketBarsAuthorityRuntime {
    pub(super) verifier:
        Arc<dyn crate::archive::reconciliation::authority::TrustedProviderAdmissionVerifierV1>,
    pub(super) reconciliation_policy:
        crate::archive::reconciliation::authority::ReconciliationPolicyV1,
}

/// Optional integration hook for an independently trusted issuer that can attest facts from the
/// actual provider page. It receives immutable raw and canonical observations plus their exact
/// query scope. Implementations must return `None` when any source, feed, entitlement, runtime, or
/// page fact is unavailable; they must never infer those facts from request labels or configuration.
///
/// Production startup leaves this unset. The trait does not create keys, sign receipts, or widen
/// provider permissions; returned evidence still passes the A04 verifier and dataset-binding path.
pub(crate) struct MarketBarsPageAdmissionContext<'a> {
    pub(crate) request: &'a BarsRequestV1,
    pub(crate) source_scope: &'a crate::archive::reconciliation::CacheQuerySourceScopeV1,
    pub(crate) raw_observations: &'a BarsResponseV1,
    pub(crate) canonical_response: &'a BarsResponseV1,
    pub(crate) page_evidence: &'a crate::archive::reconciliation::ProviderPageEvidenceV1,
    pub(crate) phase_slices:
        &'a [crate::archive::reconciliation::authority::ProviderPhaseSliceV1],
    pub(crate) policy: &'a crate::archive::reconciliation::authority::ReconciliationPolicyV1,
    pub(crate) now_unix_seconds: u64,
}

pub(crate) trait MarketBarsPageAdmissionEvidenceSource: Send + Sync {
    fn evidence_for_page(
        &self,
        context: MarketBarsPageAdmissionContext<'_>,
    ) -> Option<crate::archive::reconciliation::authority::ProviderAdmissionEvidenceV1>;
}

/// A bounded, credential-free record that a trusted logical-session pointer changed and was
/// reread. This is an integration event for status consumers, not a claim about full-window data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct MarketBarsCacheInvalidationEvent {
    #[serde(skip_serializing)]
    rights_profile_sha256: String,
    pub(super) identity_fingerprint_sha256: Option<String>,
    pub(super) session_instance_id: Option<String>,
    pub(super) logical_generation: Option<u64>,
    pub(super) logical_updated_at_unix_seconds: Option<u64>,
    pub(super) reread_status: &'static str,
    pub(super) current_provider: Option<String>,
    pub(super) current_feed: Option<String>,
    pub(super) current_authority: Option<String>,
}

/// Static runtime configuration summary for health/status projections.
///
/// These flags describe configured code paths and local cache prerequisites only. They do not
/// imply provider reachability, account entitlement, data availability, or source verification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct MarketBarsRuntimeSummary {
    pub(super) runtime_enabled: bool,
    pub(super) alpaca_historical_attemptable: bool,
    pub(super) futu_historical_attemptable: bool,
    pub(super) yfinance_core_bars_attemptable: bool,
    pub(super) cache_configured: bool,
    pub(super) operator_scope_configured: bool,
    pub(super) cache_worker_platform_supported: bool,
    pub(super) provisional_cache_prerequisites_configured: bool,
}

impl MarketBarsRuntime {
    /// The feature is explicitly disabled unless `MDP_MARKET_BARS_ENABLED=true`.
    /// Provider credentials may be absent; those routes remain unadvertised and return accurate
    /// gaps/unsupported responses. A cache is created only for an operator-supplied local root.
    pub(super) fn from_environment() -> crate::Result<Option<Arc<Self>>> {
        if !market_bars_enabled()? {
            return Ok(None);
        }

        let provider =
            crate::market_query::providers::MarketBarsProviderRuntime::from_environment()
                .map_err(|_| crate::MarketDataError::InvalidInput)?;
        let cache_scope_id = read_cache_scope_id()?;
        let cache = read_cache_root()?
            .map(|root| {
                crate::archive::reconciliation::CacheVersionStoreV1::new(
                    root,
                    crate::archive::reconciliation::CacheLimitsV1::default(),
                )
                .map_err(|_| crate::MarketDataError::InvalidInput)
            })
            .transpose()?;

        Ok(Some(Arc::new(Self {
            provider,
            cache,
            cache_scope_id,
            authority: None,
            page_admission_evidence_source: None,
            cache_invalidation_events: Arc::new(Mutex::new(VecDeque::new())),
            #[cfg(test)]
            synthetic_page_source: None,
        })))
    }

    #[cfg(test)]
    pub(crate) fn synthetic_for_test(
        provider: crate::market_query::providers::MarketBarsProviderRuntime,
        cache: crate::archive::reconciliation::CacheVersionStoreV1,
        cache_scope_id: String,
        source: Arc<dyn SyntheticMarketBarsPageSource>,
    ) -> Self {
        Self {
            provider,
            cache: Some(cache),
            cache_scope_id: Some(cache_scope_id),
            authority: None,
            page_admission_evidence_source: None,
            cache_invalidation_events: Arc::new(Mutex::new(VecDeque::new())),
            synthetic_page_source: Some(source),
        }
    }

    /// Add authority only from a trusted in-process issuer/loader. Ordinary startup always leaves
    /// this unset; no boolean setting, OIDC claim, provider label, or cache profile grants it.
    pub(crate) fn with_verified_admission_context(
        mut self,
        verifier: Arc<
            dyn crate::archive::reconciliation::authority::TrustedProviderAdmissionVerifierV1,
        >,
        admissions: Vec<crate::archive::reconciliation::authority::VerifiedProviderAdmissionV1>,
        reconciliation_policy: crate::archive::reconciliation::authority::ReconciliationPolicyV1,
    ) -> Self {
        self.provider = self
            .provider
            .with_verified_provider_admissions(admissions.clone());
        self.authority = Some(Arc::new(MarketBarsAuthorityRuntime {
            verifier,
            reconciliation_policy,
        }));
        self
    }

    /// Inject a page evidence source owned by a separately reviewed in-process trust boundary.
    /// Without this hook, fresh provider pages can only be published as provisional observations.
    pub(crate) fn with_page_admission_evidence_source(
        mut self,
        source: Arc<dyn MarketBarsPageAdmissionEvidenceSource>,
    ) -> Self {
        self.page_admission_evidence_source = Some(source);
        self
    }

    /// Return only the authenticated rights partition's bounded invalidation history. The ring is
    /// volatile and process-local; callers must serve it with `Cache-Control: no-store`.
    /// No source rows, provider credentials, tokens, raw hashes, or signed receipts are retained.
    pub(super) fn cache_invalidation_events_for_rights_profile(
        &self,
        rights_profile_sha256: &str,
    ) -> Vec<MarketBarsCacheInvalidationEvent> {
        self.cache_invalidation_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|event| event.rights_profile_sha256 == rights_profile_sha256)
            .cloned()
            .collect()
    }

    pub(super) fn record_cache_invalidation_event(&self, event: MarketBarsCacheInvalidationEvent) {
        let mut events = self
            .cache_invalidation_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if events.len() == MAX_CACHE_INVALIDATION_EVENTS {
            events.pop_front();
        }
        events.push_back(event);
    }

    /// Describe static provider attemptability and cache prerequisites without exposing secrets,
    /// local paths, or any claim about entitlement, reachability, or returned data.
    pub(super) fn summary(&self) -> MarketBarsRuntimeSummary {
        runtime_summary(
            &self.provider.source_capabilities(),
            self.cache.is_some(),
            self.cache_scope_id.is_some(),
        )
    }
}

fn runtime_summary(
    capabilities: &[crate::session_router::SourceCapability],
    cache_configured: bool,
    operator_scope_configured: bool,
) -> MarketBarsRuntimeSummary {
    let historical_attemptable = |provider| {
        capabilities.iter().any(|capability| {
            capability.provider == provider && capability.historical && !capability.realtime
        })
    };
    let cache_worker_platform_supported = cfg!(target_os = "linux");
    MarketBarsRuntimeSummary {
        runtime_enabled: true,
        alpaca_historical_attemptable: historical_attemptable(
            crate::session_router::Provider::Alpaca,
        ),
        futu_historical_attemptable: historical_attemptable(crate::session_router::Provider::Futu),
        yfinance_core_bars_attemptable: historical_attemptable(
            crate::session_router::Provider::YFinance,
        ),
        cache_configured,
        operator_scope_configured,
        cache_worker_platform_supported,
        provisional_cache_prerequisites_configured: cache_configured
            && operator_scope_configured
            && cache_worker_platform_supported,
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MarketBarsQuery {
    symbol: String,
    asset_class: String,
    start: String,
    end: String,
    resolution: String,
    session_scope: String,
    adjustment: String,
    provider_mode: String,
    allow_daily_downgrade: bool,
    limit: u16,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct MarketSessionsQuery {
    start: String,
    end: String,
}

#[derive(Serialize)]
struct MarketSessionsResponse<'a> {
    calendar_source_id: &'a str,
    market_calendar_version: &'a str,
    calendar_input_sha256: &'a str,
    coverage_start: chrono::NaiveDate,
    coverage_end_exclusive: chrono::NaiveDate,
    request_start: &'a str,
    request_end: &'a str,
    days: Vec<&'a crate::session_router::CalendarDay>,
    instances: Vec<&'a crate::session_router::SessionInstance>,
}

#[derive(Serialize)]
struct MarketBarsErrorBody {
    error: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarketBarsHttpError {
    InvalidRequest,
    Forbidden,
    ResultTooLarge,
    CursorExpired,
    Unsupported,
    ProviderUnauthorized,
    ProviderForbidden,
    RateLimited,
    Timeout,
    Unavailable,
    UpstreamFailure,
}

impl IntoResponse for MarketBarsHttpError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "not_authorized"),
            Self::ResultTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "result_limit_exceeded"),
            Self::CursorExpired => (StatusCode::GONE, "cursor_expired"),
            Self::Unsupported => (StatusCode::UNPROCESSABLE_ENTITY, "unsupported_request"),
            Self::ProviderUnauthorized => (StatusCode::UNAUTHORIZED, "provider_unauthorized"),
            Self::ProviderForbidden => (StatusCode::FORBIDDEN, "provider_forbidden"),
            Self::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "provider_rate_limited"),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "provider_timeout"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "market_data_unavailable"),
            Self::UpstreamFailure => (StatusCode::BAD_GATEWAY, "provider_failure"),
        };
        (status, Json(MarketBarsErrorBody { error })).into_response()
    }
}

/// Read one bounded Core Bars V1 page. The same normalized request is reused through the cursor;
/// provider errors never trigger a source-label rewrite or silent fallback.
pub(super) async fn get_market_bars(
    State(state): State<Arc<ServiceState>>,
    Extension(principal): Extension<VerifiedMarketPrincipal>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let request = match parse_bars_request(&uri) {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };
    if !principal_may_request_research(&principal, request.provider_mode) {
        return MarketBarsHttpError::Forbidden.into_response();
    }
    let delegation = match read_gateway_delegation(&headers) {
        Ok(delegation) => delegation,
        Err(error) => return error.into_response(),
    };
    let Some(calendar) = state.market_calendar.clone() else {
        return MarketBarsHttpError::Unavailable.into_response();
    };
    let Some(runtime) = state.market_bars.clone() else {
        return MarketBarsHttpError::Unavailable.into_response();
    };

    let query_client = state.query_client.clone();
    let response = query::run_market_bars_query(
        query_client,
        runtime,
        request,
        calendar,
        principal,
        delegation,
    )
    .await;
    match response {
        Ok(response) => serialize_bars_response(response),
        Err(error) => map_query_failure(error).into_response(),
    }
}

pub(super) async fn get_market_sessions(
    State(state): State<Arc<ServiceState>>,
    uri: Uri,
) -> Response {
    let query = match parse_sessions_query(&uri) {
        Ok(query) => query,
        Err(error) => return error.into_response(),
    };
    let Some(calendar) = state.market_calendar.as_ref() else {
        return MarketBarsHttpError::Unavailable.into_response();
    };
    let start = match parse_utc_window_endpoint(&query.start) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let end = match parse_utc_window_endpoint(&query.end) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if !session_window_is_bounded(&start, &end) {
        return MarketBarsHttpError::InvalidRequest.into_response();
    }
    let Some(first_day) = calendar.snapshot.days.first() else {
        return MarketBarsHttpError::Unavailable.into_response();
    };
    let Some(last_day) = calendar.snapshot.days.last() else {
        return MarketBarsHttpError::Unavailable.into_response();
    };
    if start < first_day.day_window.start || end > last_day.day_window.end_exclusive {
        return MarketBarsHttpError::Unavailable.into_response();
    }

    let days = calendar
        .snapshot
        .days
        .iter()
        .filter(|day| {
            intersects(
                &start,
                &end,
                &day.day_window.start,
                &day.day_window.end_exclusive,
            )
        })
        .collect::<Vec<_>>();
    let instances = calendar
        .snapshot
        .instances
        .iter()
        .filter(|instance| {
            intersects(
                &start,
                &end,
                &instance.window.start,
                &instance.window.end_exclusive,
            )
        })
        .collect::<Vec<_>>();
    let response = MarketSessionsResponse {
        calendar_source_id: &calendar.snapshot.source_id,
        market_calendar_version: &calendar.snapshot.version,
        calendar_input_sha256: &calendar.input_sha256,
        coverage_start: calendar.snapshot.coverage_start,
        coverage_end_exclusive: calendar.snapshot.coverage_end_exclusive,
        request_start: &query.start,
        request_end: &query.end,
        days,
        instances,
    };
    serialize_json_response(StatusCode::OK, &response, MAX_SESSION_RESPONSE_BYTES)
}

fn parse_bars_request(uri: &Uri) -> Result<BarsRequestV1, MarketBarsHttpError> {
    let query = parse_query::<MarketBarsQuery>(uri)?;
    let json = serde_json::to_vec(&query).map_err(|_| MarketBarsHttpError::InvalidRequest)?;
    BarsRequestV1::parse_json(&json).map_err(|_| MarketBarsHttpError::InvalidRequest)
}

fn parse_sessions_query(uri: &Uri) -> Result<MarketSessionsQuery, MarketBarsHttpError> {
    parse_query(uri)
}

fn principal_may_request_research(
    principal: &VerifiedMarketPrincipal,
    provider_mode: market_contracts::BarsProviderModeV1,
) -> bool {
    provider_mode != market_contracts::BarsProviderModeV1::Research || principal.is_research()
}

fn parse_query<T>(uri: &Uri) -> Result<T, MarketBarsHttpError>
where
    T: for<'de> Deserialize<'de>,
{
    if uri
        .query()
        .is_none_or(|query| query.len() > MAX_QUERY_BYTES)
    {
        return Err(MarketBarsHttpError::InvalidRequest);
    }
    Query::<T>::try_from_uri(uri)
        .map(|Query(value)| value)
        .map_err(|_| MarketBarsHttpError::InvalidRequest)
}

fn parse_utc_window_endpoint(value: &str) -> Result<DateTime<Utc>, MarketBarsHttpError> {
    if !value.ends_with('Z') || value.len() > 35 {
        return Err(MarketBarsHttpError::InvalidRequest);
    }
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| MarketBarsHttpError::InvalidRequest)
}

fn read_gateway_delegation(headers: &HeaderMap) -> Result<Option<String>, MarketBarsHttpError> {
    let values = headers.get_all(&GATEWAY_DELEGATION_HEADER);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(MarketBarsHttpError::InvalidRequest);
    }
    let token = value
        .to_str()
        .map_err(|_| MarketBarsHttpError::InvalidRequest)?;
    if token.is_empty()
        || token.len() > 8 * 1024
        || token.bytes().any(|byte| !byte.is_ascii_graphic())
    {
        return Err(MarketBarsHttpError::InvalidRequest);
    }
    Ok(Some(token.to_owned()))
}

fn market_bars_enabled() -> crate::Result<bool> {
    match env::var(MARKET_BARS_ENABLED_ENV) {
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Ok(_) => Err(crate::MarketDataError::InvalidInput),
        Err(env::VarError::NotPresent) => Ok(false),
        Err(env::VarError::NotUnicode(_)) => Err(crate::MarketDataError::InvalidInput),
    }
}

fn read_cache_root() -> crate::Result<Option<std::path::PathBuf>> {
    match env::var_os(MARKET_CACHE_ROOT_ENV) {
        None => Ok(None),
        Some(root) if root.is_empty() => Err(crate::MarketDataError::InvalidInput),
        Some(root) => Ok(Some(root.into())),
    }
}

fn read_cache_scope_id() -> crate::Result<Option<String>> {
    match env::var(MARKET_CACHE_SCOPE_ENV) {
        Ok(value) if valid_cache_scope_id(&value) => Ok(Some(value)),
        Ok(_) => Err(crate::MarketDataError::InvalidInput),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(crate::MarketDataError::InvalidInput),
    }
}

fn valid_cache_scope_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn hash_cache_entitlement_scope(
    profile: &str,
    principal_scope_sha256: &str,
    provider: &str,
    feed: &str,
    source_scope_id: &str,
    calendar_version: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"mdp-market-bars-cache-entitlement-scope-v1\0");
    for value in [
        profile,
        principal_scope_sha256,
        provider,
        feed,
        source_scope_id,
        calendar_version,
    ] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    hex::encode(digest.finalize())
}

/// Bind provisional cache reuse to both the operator-selected profile and the authenticated
/// principal. This identity is provider-neutral and intentionally excludes credentials, tokens,
/// request windows, calendar versions, and provider/feed labels.
fn hash_cache_rights_profile(profile: &str, principal_scope_sha256: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"mdp-market-bars-cache-rights-profile-v1\0");
    for value in [profile, principal_scope_sha256] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn current_timestamp() -> Result<ProtoTimestampV2, MarketBarsHttpError> {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
    ProtoTimestampV2::parse(&now).map_err(|_| MarketBarsHttpError::Unavailable)
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn intersects<T: Ord>(start: &T, end: &T, other_start: &T, other_end: &T) -> bool {
    start < other_end && other_start < end
}

fn session_window_is_bounded(start: &DateTime<Utc>, end: &DateTime<Utc>) -> bool {
    start
        .to_owned()
        .checked_add_signed(chrono::Duration::days(MAX_SESSION_WINDOW_DAYS))
        .is_some_and(|maximum_end| start < end && end <= &maximum_end)
}

fn serialize_bars_response(response: BarsResponseV1) -> Response {
    let status = http_status_for_coverage(response.coverage_status);
    serialize_json_response(status, &response, super::MAX_HTTP_RESPONSE_BYTES)
}

fn http_status_for_coverage(coverage: BarsCoverageStatusV1) -> StatusCode {
    match coverage {
        BarsCoverageStatusV1::Complete | BarsCoverageStatusV1::Empty => StatusCode::OK,
        BarsCoverageStatusV1::Partial | BarsCoverageStatusV1::Unknown => {
            StatusCode::PARTIAL_CONTENT
        }
        BarsCoverageStatusV1::Unsupported => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

fn serialize_json_response<T: Serialize>(
    status: StatusCode,
    value: &T,
    max_bytes: usize,
) -> Response {
    let bytes = match serde_json::to_vec(value) {
        Ok(bytes) if bytes.len() <= max_bytes => bytes,
        Ok(_) => {
            return MarketBarsHttpError::ResultTooLarge.into_response();
        }
        Err(_) => {
            return MarketBarsHttpError::UpstreamFailure.into_response();
        }
    };
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn map_query_failure(failure: QueryFailure) -> MarketBarsHttpError {
    match failure {
        QueryFailure::Timeout => MarketBarsHttpError::Timeout,
        QueryFailure::Unavailable => MarketBarsHttpError::Unavailable,
        QueryFailure::Data(crate::MarketDataError::InvalidInput) => {
            MarketBarsHttpError::InvalidRequest
        }
        QueryFailure::Data(
            crate::MarketDataError::InputLimit | crate::MarketDataError::SourceLimit,
        ) => MarketBarsHttpError::ResultTooLarge,
        QueryFailure::Data(crate::MarketDataError::Contract) => MarketBarsHttpError::Unsupported,
        QueryFailure::Data(crate::MarketDataError::Provider(error)) => match error {
            crate::error::ProviderFailure::MissingCredentials => MarketBarsHttpError::Unavailable,
            crate::error::ProviderFailure::Unauthorized => {
                MarketBarsHttpError::ProviderUnauthorized
            }
            crate::error::ProviderFailure::Forbidden => MarketBarsHttpError::ProviderForbidden,
            crate::error::ProviderFailure::RateLimited => MarketBarsHttpError::RateLimited,
            crate::error::ProviderFailure::HttpStatus(410) => MarketBarsHttpError::CursorExpired,
            crate::error::ProviderFailure::HttpStatus(400) => MarketBarsHttpError::InvalidRequest,
            crate::error::ProviderFailure::HttpStatus(422) => MarketBarsHttpError::Unsupported,
            crate::error::ProviderFailure::HttpStatus(429) => MarketBarsHttpError::RateLimited,
            crate::error::ProviderFailure::HttpStatus(504) => MarketBarsHttpError::Timeout,
            crate::error::ProviderFailure::HttpStatus(401) => {
                MarketBarsHttpError::ProviderUnauthorized
            }
            crate::error::ProviderFailure::HttpStatus(403) => {
                MarketBarsHttpError::ProviderForbidden
            }
            crate::error::ProviderFailure::HttpStatus(_) => MarketBarsHttpError::UpstreamFailure,
            crate::error::ProviderFailure::Transport => MarketBarsHttpError::Unavailable,
            crate::error::ProviderFailure::InvalidResponse => MarketBarsHttpError::UpstreamFailure,
        },
        QueryFailure::Data(crate::MarketDataError::Storage(
            crate::error::StorageFailure::Timeout,
        )) => MarketBarsHttpError::Timeout,
        QueryFailure::Data(crate::MarketDataError::Storage(
            crate::error::StorageFailure::Unsupported,
        )) => MarketBarsHttpError::Unavailable,
        QueryFailure::Data(_) => MarketBarsHttpError::UpstreamFailure,
    }
}

#[cfg(test)]
#[path = "market_bars_tests.rs"]
mod tests;
