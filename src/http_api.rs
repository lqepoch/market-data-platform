//! Bounded, read-only HTTP facade for the existing verified archive query path.

use std::{future::Future, io, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use market_contracts::EntitlementState;
use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use crate::archive::capture_pair_v2::reader::{
    LocalCapturePairV2Reader, VerifiedCapturePairChunkV2,
};
use crate::{
    MarketDataError,
    aggregate::TradeMinuteBarV1,
    archive::TransportKind,
    remote_query::{DatasetNamespace, RemoteArchiveReader, RemoteQuerySummary},
};

mod auth;
mod supervisor;

pub use auth::AuthConfig;
use auth::require_market_read;
use supervisor::{QueryClient, QueryFailure, QuerySupervisor};

pub const DEFAULT_BIND: &str = "127.0.0.1:8088";
pub const MAX_HTTP_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const HTTP_MAX_QUERY_ROWS: usize = 390;
pub const HTTP_MAX_QUERY_RESULT_BYTES: u64 = 1024 * 1024;
const QUERY_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Clone)]
struct ServiceState {
    auth_configured: bool,
    query_client: QueryClient,
    reader: Arc<RemoteArchiveReader>,
    pair_query_available: bool,
}

/// Operator-supplied LocalTest roots used to enable the single-chunk Pair verification route.
/// These paths are startup configuration and are never accepted or returned over HTTP.
#[derive(Clone)]
pub struct LocalTestPairReadConfig {
    pub local_test_root: PathBuf,
    pub state_dir: PathBuf,
    pub staging_dir: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BarsQuery {
    namespace: DatasetNamespace,
    symbol: String,
}

#[derive(Serialize)]
struct BarsResponse<'a> {
    summary: HttpRemoteQuerySummary<'a>,
    rows: &'a [TradeMinuteBarV1],
}

// The internal/CLI summary retains numeric u64 values. HTTP projects those values as
// canonical decimal strings so JavaScript clients do not lose precision above 2^53.
#[derive(Serialize)]
struct HttpRemoteQuerySummary<'a> {
    namespace: DatasetNamespace,
    dataset_id: &'a str,
    schema_id: &'a str,
    source: &'a market_contracts::MarketDataSourceV1,
    row_count: String,
    returned_rows: String,
    content_sha256: &'a str,
    parquet_schema_sha256: &'a str,
    cache_hit: bool,
}

impl<'a> From<&'a RemoteQuerySummary> for HttpRemoteQuerySummary<'a> {
    fn from(summary: &'a RemoteQuerySummary) -> Self {
        Self {
            namespace: summary.namespace,
            dataset_id: &summary.dataset_id,
            schema_id: &summary.schema_id,
            source: &summary.source,
            row_count: summary.row_count.to_string(),
            returned_rows: summary.returned_rows.to_string(),
            content_sha256: &summary.content_sha256,
            parquet_schema_sha256: &summary.parquet_schema_sha256,
            cache_hit: summary.cache_hit,
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
}

#[derive(Clone, Copy)]
enum ApiError {
    Unauthorized,
    InvalidRequest,
    Forbidden,
    TooLarge,
    Timeout,
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "not_authorized"),
            Self::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "result_limit_exceeded"),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "query_timeout"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "query_unavailable"),
        };
        (status, Json(ErrorResponse { error })).into_response()
    }
}

fn api_error(error: ApiError) -> Response {
    error.into_response()
}

pub async fn serve(
    bind: SocketAddr,
    reader: Arc<RemoteArchiveReader>,
    auth: Option<AuthConfig>,
    graceful_shutdown: impl Future<Output = io::Result<()>> + Send + 'static,
) -> crate::Result<()> {
    serve_with_pair_read_config(bind, reader, auth, None, graceful_shutdown).await
}

/// Starts the read-only HTTP service, optionally enabling a LocalTest-only one-chunk Pair route.
/// Pair filesystem roots are explicit operator startup configuration, never request parameters.
///
/// This function launches the bounded Parquet worker through `current_exe`; call it only from the
/// matching `market-data-platform` CLI process, not from an embedding application's executable.
pub async fn serve_with_pair_read_config(
    bind: SocketAddr,
    reader: Arc<RemoteArchiveReader>,
    auth: Option<AuthConfig>,
    pair_config: Option<LocalTestPairReadConfig>,
    graceful_shutdown: impl Future<Output = io::Result<()>> + Send + 'static,
) -> crate::Result<()> {
    if !bind.ip().is_loopback() && auth.is_none() {
        return Err(MarketDataError::InvalidInput);
    }
    if pair_config.is_some() && reader.transport_kind() != TransportKind::LocalTest {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    #[cfg(target_os = "linux")]
    let pair_reader = pair_config
        .map(|config| {
            LocalCapturePairV2Reader::local_test(
                config.local_test_root,
                config.state_dir,
                config.staging_dir,
                crate::archive::ArchiveLimits::default(),
            )
            .map(Arc::new)
        })
        .transpose()?;
    #[cfg(not(target_os = "linux"))]
    let pair_reader = {
        if pair_config.is_some() {
            return Err(MarketDataError::Storage(
                crate::error::StorageFailure::Unsupported,
            ));
        }
        None::<Arc<()>>
    };
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let pair_query_available = pair_reader.is_some();
    let (mut query_supervisor, query_client) = QuerySupervisor::start(
        Arc::clone(&reader),
        #[cfg(target_os = "linux")]
        pair_reader,
    );
    let state = Arc::new(ServiceState {
        auth_configured: auth.is_some(),
        query_client,
        reader,
        pair_query_available,
    });
    let app = app_router(state, auth);
    let shutdown_signal = query_supervisor.shutdown_signal();
    let (signal_result_tx, signal_result_rx) = tokio::sync::oneshot::channel();
    let graceful_shutdown = async move {
        let signal_result = graceful_shutdown.await;
        shutdown_signal.send_replace(true);
        let _ = signal_result_tx.send(signal_result);
    };
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(graceful_shutdown)
        .await;
    let shutdown_result = query_supervisor.shutdown().await;
    server_result?;
    shutdown_result?;
    signal_result_rx.await.map_err(|_| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "HTTP graceful shutdown signal task ended without a result",
        )
    })??;
    tracing::info!("MDP HTTP query supervisor joined all active workers");
    Ok(())
}

fn app_router(state: Arc<ServiceState>, auth: Option<AuthConfig>) -> Router {
    let mut protected_routes = Router::new().route("/v1/datasets/{dataset_id}/bars", get(get_bars));
    #[cfg(target_os = "linux")]
    if state.pair_query_available {
        protected_routes = protected_routes.route(
            "/v2/local-test/capture-pairs/{receipt_sha256}/verify",
            get(verify_local_test_pair),
        );
    }
    let protected_routes =
        protected_routes.route_layer(middleware::from_fn_with_state(auth, require_market_read));
    Router::new()
        .route("/healthz", get(liveness))
        .route("/readyz", get(readiness))
        .merge(protected_routes)
        .with_state(state)
        .layer(middleware::from_fn(no_store))
}

async fn no_store(request: axum::extract::Request, next: middleware::Next) -> Response {
    let mut response = next.run(request).await;
    let has_no_store = response
        .headers()
        .get_all(header::CACHE_CONTROL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|directive| {
            directive
                .trim()
                .split_once('=')
                .map_or(directive.trim(), |(name, _)| name.trim())
        })
        .any(|directive| directive.eq_ignore_ascii_case("no-store"));
    if !has_no_store {
        response
            .headers_mut()
            .append(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    response
}

async fn liveness() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(serde_json::json!({ "status": "alive" })),
    )
}

async fn readiness(State(state): State<Arc<ServiceState>>) -> Response {
    let response = serde_json::json!({
        "status": if state.auth_configured { "ready" } else { "not_ready" },
        "market_ready": false,
        "source_entitlement": "unverified",
    });
    let status = if state.auth_configured {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(response),
    )
        .into_response()
}

async fn get_bars(
    State(state): State<Arc<ServiceState>>,
    Path(dataset_id): Path<String>,
    Query(query): Query<BarsQuery>,
) -> Response {
    if !valid_symbol(&query.symbol) {
        return api_error(ApiError::InvalidRequest);
    }
    if state.reader.transport_kind() == TransportKind::LocalTest
        && query.namespace != DatasetNamespace::Diagnostic
    {
        return api_error(ApiError::Forbidden);
    }
    let result = state
        .query_client
        .query(query.namespace, dataset_id, query.symbol)
        .await;
    let (rows, summary) = match result {
        Ok(result) => result,
        Err(QueryFailure::Timeout) => return api_error(ApiError::Timeout),
        Err(QueryFailure::Unavailable) => return api_error(ApiError::Unavailable),
        Err(QueryFailure::Data(error)) => return api_error(map_data_error(error)),
    };
    if state.reader.transport_kind() == TransportKind::LocalTest
        && (summary.source.provider != "synthetic"
            || summary.source.feed != "synthetic"
            || summary.source.entitlement != EntitlementState::Unknown)
    {
        return api_error(ApiError::Unavailable);
    }
    let mut bytes = Vec::with_capacity(64 * 1024);
    let response = BarsResponse {
        summary: HttpRemoteQuerySummary::from(&summary),
        rows: &rows,
    };
    let mut writer = BoundedWriter::new(&mut bytes);
    let serialized = serde_json::to_writer(&mut writer, &response);
    if writer.exceeded {
        return api_error(ApiError::TooLarge);
    }
    if serialized.is_err() {
        return api_error(ApiError::Unavailable);
    }
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(target_os = "linux")]
async fn verify_local_test_pair(
    State(state): State<Arc<ServiceState>>,
    Path(receipt_sha256): Path<String>,
) -> Response {
    if !valid_lower_sha256(&receipt_sha256) {
        return api_error(ApiError::InvalidRequest);
    }
    let receipt_name = format!("chunk-{receipt_sha256}.receipt.json");
    let summary = match state.query_client.verify_pair(receipt_name).await {
        Ok(summary) => summary,
        Err(QueryFailure::Timeout) => return api_error(ApiError::Timeout),
        Err(QueryFailure::Unavailable) => return api_error(ApiError::Unavailable),
        Err(QueryFailure::Data(error)) => return api_error(map_data_error(error)),
    };
    pair_summary_response(&summary)
}

#[cfg(target_os = "linux")]
fn pair_summary_response(summary: &VerifiedCapturePairChunkV2) -> Response {
    let mut bytes = Vec::with_capacity(4096);
    let mut writer = BoundedWriter::new(&mut bytes);
    if serde_json::to_writer(&mut writer, summary).is_err() {
        return api_error(if writer.exceeded {
            ApiError::TooLarge
        } else {
            ApiError::Unavailable
        });
    }
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

#[cfg(target_os = "linux")]
fn valid_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn map_data_error(error: MarketDataError) -> ApiError {
    match error {
        MarketDataError::InvalidInput => ApiError::InvalidRequest,
        MarketDataError::UnknownOutcome => ApiError::Unavailable,
        MarketDataError::PublicationNotAuthorized => ApiError::Forbidden,
        MarketDataError::InputLimit | MarketDataError::SourceLimit => ApiError::TooLarge,
        MarketDataError::Storage(crate::error::StorageFailure::Timeout) => ApiError::Timeout,
        MarketDataError::Storage(crate::error::StorageFailure::Cancelled) => ApiError::Unavailable,
        _ => ApiError::Unavailable,
    }
}

fn valid_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol.len() <= 16
        && symbol.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
}

struct BoundedWriter<'a> {
    bytes: &'a mut Vec<u8>,
    exceeded: bool,
}

impl<'a> BoundedWriter<'a> {
    fn new(bytes: &'a mut Vec<u8>) -> Self {
        Self {
            bytes,
            exceeded: false,
        }
    }
}

impl io::Write for BoundedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_HTTP_RESPONSE_BYTES)
            .ok_or_else(|| {
                self.exceeded = true;
                io::Error::other("HTTP response limit exceeded")
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
mod tests;
