use std::sync::Arc;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use market_contracts::{EntitlementState, MarketDataSourceV1, NumericEncodingV1};
use serde_json::Value;
use tower::ServiceExt;

use crate::{
    remote_query::{DatasetNamespace, RemoteArchiveReader, RemoteCacheLimits},
    storage::LocalTestTransport,
};

use super::{
    AuthConfig, HTTP_MAX_QUERY_ROWS, ServiceState, app_router,
    auth::test_support,
    supervisor::{QueryClient, QuerySupervisor},
};

const DATASET_ID: &str = "synthetic-2026-10-08-four-bars-parquet-v3-bars-1m-v1";

async fn synthetic_remote_root(root: &std::path::Path) -> std::path::PathBuf {
    crate::pipeline::synthetic_replay(root).await.unwrap();
    root.join("local-test-store")
}

fn service(
    remote_root: std::path::PathBuf,
    cache_root: std::path::PathBuf,
    auth: Option<AuthConfig>,
) -> (Router, QuerySupervisor) {
    let reader = Arc::new(
        RemoteArchiveReader::local_test_isolated(
            LocalTestTransport::new(remote_root).unwrap(),
            cache_root,
            RemoteCacheLimits {
                max_query_rows: HTTP_MAX_QUERY_ROWS,
                max_query_result_bytes: 1024 * 1024,
                ..RemoteCacheLimits::default()
            },
        )
        .unwrap(),
    );
    let (supervisor, query_client) = QuerySupervisor::start(Arc::clone(&reader));
    let state = Arc::new(ServiceState {
        auth_configured: auth.is_some(),
        query_client,
        reader,
    });
    (app_router(state, auth), supervisor)
}

fn request(uri: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(uri).method("GET");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

fn assert_no_store(response: &axum::response::Response) {
    let directives = response
        .headers()
        .get_all(header::CACHE_CONTROL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|directive| directive.trim())
        .collect::<Vec<_>>();
    assert!(
        directives
            .iter()
            .any(|directive| directive.eq_ignore_ascii_case("no-store")),
        "missing Cache-Control no-store: {directives:?}"
    );
}

#[tokio::test]
async fn health_and_readiness_are_probeable_without_market_auth() {
    let temp = tempfile::tempdir().unwrap();
    let remote = synthetic_remote_root(&temp.path().join("remote")).await;
    let (app, mut supervisor) = service(
        remote,
        temp.path().join("cache"),
        Some(test_support::auth_config()),
    );

    let health = app
        .clone()
        .oneshot(request("/healthz", None))
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    assert_no_store(&health);
    let readiness = app.oneshot(request("/readyz", None)).await.unwrap();
    assert_eq!(readiness.status(), StatusCode::OK);
    assert_no_store(&readiness);
    let body = to_bytes(readiness.into_body(), 1024).await.unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["market_ready"], false);
    assert_eq!(value["source_entitlement"], "unverified");
    supervisor.shutdown().await.unwrap();
}

#[test]
fn http_summary_projects_uint64_as_exact_decimal_strings_without_changing_internal_json() {
    let summary = crate::remote_query::RemoteQuerySummary {
        namespace: DatasetNamespace::Diagnostic,
        dataset_id: DATASET_ID.to_owned(),
        schema_id: "lqepoch.us_equity_trade_bar_1m.v1".to_owned(),
        source: MarketDataSourceV1 {
            provider: "synthetic".to_owned(),
            feed: "synthetic".to_owned(),
            entitlement: EntitlementState::Unknown,
            numeric_encoding: NumericEncodingV1::DecimalToken,
            source_record_id: None,
        },
        row_count: 9_007_199_254_740_993,
        returned_rows: u64::MAX,
        content_sha256: "0".repeat(64),
        parquet_schema_sha256: "1".repeat(64),
        cache_hit: false,
    };

    let http = serde_json::to_value(super::HttpRemoteQuerySummary::from(&summary)).unwrap();
    assert_eq!(http["row_count"], "9007199254740993");
    assert_eq!(http["returned_rows"], "18446744073709551615");

    let internal = serde_json::to_value(&summary).unwrap();
    assert_eq!(internal["row_count"].as_u64(), Some(9_007_199_254_740_993));
    assert_eq!(internal["returned_rows"].as_u64(), Some(u64::MAX));
}

#[tokio::test]
async fn no_key_loopback_profile_exposes_liveness_but_never_readiness_or_data() {
    let temp = tempfile::tempdir().unwrap();
    let remote = synthetic_remote_root(&temp.path().join("remote")).await;
    let (app, mut supervisor) = service(remote, temp.path().join("cache"), None);

    let health = app
        .clone()
        .oneshot(request("/healthz", None))
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let readiness = app.clone().oneshot(request("/readyz", None)).await.unwrap();
    assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_no_store(&readiness);
    let protected = app
        .oneshot(request(
            &format!("/v1/datasets/{DATASET_ID}/bars?namespace=diagnostic&symbol=QQQ"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(protected.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_no_store(&protected);
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn non_loopback_bind_is_rejected_without_independent_market_auth() {
    let temp = tempfile::tempdir().unwrap();
    let remote = synthetic_remote_root(&temp.path().join("remote")).await;
    let reader = Arc::new(
        RemoteArchiveReader::local_test_isolated(
            LocalTestTransport::new(remote).unwrap(),
            temp.path().join("cache"),
            RemoteCacheLimits::default(),
        )
        .unwrap(),
    );
    let result = super::serve(
        "0.0.0.0:0".parse().unwrap(),
        reader,
        None,
        std::future::pending(),
    )
    .await;
    assert!(matches!(result, Err(crate::MarketDataError::InvalidInput)));
}

#[tokio::test]
async fn missing_auth_is_rejected_before_any_cache_or_dataset_read() {
    let temp = tempfile::tempdir().unwrap();
    let remote = synthetic_remote_root(&temp.path().join("remote")).await;
    let cache = temp.path().join("cache");
    let (app, mut supervisor) = service(remote, cache.clone(), Some(test_support::auth_config()));

    let response = app
        .oneshot(request(
            &format!("/v1/datasets/{DATASET_ID}/bars?namespace=diagnostic&symbol=QQQ"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_no_store(&response);
    assert!(!cache.join("diagnostic").join(DATASET_ID).exists());
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_policy_covers_auth_extractor_and_router_fallback_responses() {
    let temp = tempfile::tempdir().unwrap();
    let remote = synthetic_remote_root(&temp.path().join("remote")).await;
    let (app, mut supervisor) = service(
        remote,
        temp.path().join("cache"),
        Some(test_support::auth_config()),
    );
    let token = test_support::terminal_token("market:read");
    let cases = [
        (
            format!("/v1/datasets/{DATASET_ID}/bars?namespace=diagnostic&symbol=QQQ"),
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            format!("/v1/datasets/{DATASET_ID}/bars?namespace=diagnostic&symbol=QQQ&unknown=1"),
            Some(token.as_str()),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/datasets/%FF/bars?namespace=diagnostic&symbol=QQQ".to_owned(),
            Some(token.as_str()),
            StatusCode::BAD_REQUEST,
        ),
        ("/unknown-path".to_owned(), None, StatusCode::NOT_FOUND),
    ];
    for (uri, bearer, expected_status) in cases {
        let response = app.clone().oneshot(request(&uri, bearer)).await.unwrap();
        assert_eq!(
            response.status(),
            expected_status,
            "unexpected response for {uri}"
        );
        assert_no_store(&response);
    }
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_store_middleware_preserves_existing_cache_control_headers() {
    use axum::routing::get;

    let app = Router::new()
        .route(
            "/",
            get(|| async {
                (
                    [(header::CACHE_CONTROL, "private, max-age=60")],
                    "bounded test response",
                )
            }),
        )
        .layer(axum::middleware::from_fn(super::no_store));
    let response = app.oneshot(request("/", None)).await.unwrap();
    let values = response
        .headers()
        .get_all(header::CACHE_CONTROL)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect::<Vec<_>>();
    assert!(values.contains(&"private, max-age=60"));
    assert!(values.contains(&"no-store"));
}

#[tokio::test]
async fn cache_policy_covers_query_supervisor_overload() {
    let temp = tempfile::tempdir().unwrap();
    let remote = synthetic_remote_root(&temp.path().join("remote")).await;
    let reader = Arc::new(
        RemoteArchiveReader::local_test_isolated(
            LocalTestTransport::new(remote).unwrap(),
            temp.path().join("cache"),
            RemoteCacheLimits {
                max_query_rows: HTTP_MAX_QUERY_ROWS,
                max_query_result_bytes: 1024 * 1024,
                ..RemoteCacheLimits::default()
            },
        )
        .unwrap(),
    );
    let (query_client, _held_full_queue) = QueryClient::with_full_queue_for_test();
    let state = Arc::new(ServiceState {
        auth_configured: true,
        query_client,
        reader,
    });
    let app = app_router(state, Some(test_support::auth_config()));
    let token = test_support::terminal_token("market:read");
    let response = app
        .oneshot(request(
            &format!("/v1/datasets/{DATASET_ID}/bars?namespace=diagnostic&symbol=QQQ"),
            Some(&token),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_no_store(&response);
}

#[tokio::test]
async fn terminal_and_research_delegations_read_only_synthetic_diagnostic_v1_bars() {
    let temp = tempfile::tempdir().unwrap();
    let remote = synthetic_remote_root(&temp.path().join("remote")).await;
    let diagnostic_reader = RemoteArchiveReader::local_test_isolated(
        LocalTestTransport::new(remote.clone()).unwrap(),
        temp.path().join("direct-cache"),
        RemoteCacheLimits {
            max_query_rows: HTTP_MAX_QUERY_ROWS,
            max_query_result_bytes: 1024 * 1024,
            ..RemoteCacheLimits::default()
        },
    )
    .unwrap();
    let direct_result =
        diagnostic_reader.query_bars(DatasetNamespace::Diagnostic, DATASET_ID, Some("QQQ"));
    assert!(
        direct_result.is_ok(),
        "direct isolated query failed: {direct_result:?}"
    );
    let cache = temp.path().join("cache");
    let (app, mut supervisor) = service(remote, cache.clone(), Some(test_support::auth_config()));
    for token in [
        test_support::terminal_token("market:read"),
        test_support::research_token("market:read"),
    ] {
        let response = app
            .clone()
            .oneshot(request(
                &format!("/v1/datasets/{DATASET_ID}/bars?namespace=diagnostic&symbol=QQQ"),
                Some(&token),
            ))
            .await
            .unwrap();
        let response_status = response.status();
        let response_headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), super::MAX_HTTP_RESPONSE_BYTES)
            .await
            .unwrap();
        assert_eq!(
            response_status,
            StatusCode::OK,
            "unexpected HTTP body: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(response_headers["cache-control"], "no-store");
        assert_eq!(response_headers["content-type"], "application/json");
        assert!(bytes.len() <= super::MAX_HTTP_RESPONSE_BYTES);
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["summary"]["namespace"], "diagnostic");
        assert_eq!(
            value["summary"]["schema_id"],
            "lqepoch.us_equity_trade_bar_1m.v1"
        );
        assert_eq!(value["summary"]["source"]["provider"], "synthetic");
        assert_eq!(value["summary"]["source"]["feed"], "synthetic");
        assert_eq!(value["summary"]["source"]["entitlement"], "unknown");
        assert_eq!(value["summary"]["row_count"], "4");
        assert_eq!(value["rows"].as_array().unwrap().len(), 4);
        assert_eq!(value["summary"]["returned_rows"], "4");
        assert_eq!(value["rows"][0]["schema_version"], 1);
        assert!(value["rows"][0]["trade_count"].is_string());
        assert!(value["summary"].get("completion_evidence").is_none());
        assert!(value["rows"][0].get("completion_evidence").is_none());
    }
    assert!(
        cache
            .join("diagnostic")
            .join(DATASET_ID)
            .join(".cache-receipt.json")
            .is_file()
    );
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn scope_and_local_test_namespace_fail_closed_without_reading_archive() {
    let temp = tempfile::tempdir().unwrap();
    let remote = synthetic_remote_root(&temp.path().join("remote")).await;
    let cache = temp.path().join("cache");
    let (app, mut supervisor) = service(remote, cache.clone(), Some(test_support::auth_config()));
    let bad_scope = test_support::terminal_token("market:read orders:read");
    let unauthorized = app
        .clone()
        .oneshot(request(
            &format!("/v1/datasets/{DATASET_ID}/bars?namespace=diagnostic&symbol=QQQ"),
            Some(&bad_scope),
        ))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let curated = test_support::terminal_token("market:read");
    let forbidden = app
        .oneshot(request(
            &format!("/v1/datasets/{DATASET_ID}/bars?namespace=curated&symbol=QQQ"),
            Some(&curated),
        ))
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    assert!(!cache.join("diagnostic").join(DATASET_ID).exists());
    assert!(!cache.join("curated").join(DATASET_ID).exists());
    supervisor.shutdown().await.unwrap();
}
