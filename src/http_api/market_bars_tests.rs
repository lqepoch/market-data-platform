use std::sync::Arc;

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Request, StatusCode, Uri},
    response::IntoResponse,
};
use market_contracts::BarsCoverageStatusV1;
use market_contracts::{
    BarCoverageGapV1, BarCoverageSegmentV1, BarsProviderModeV1, CoverageGapReasonV1,
    ProtoTimestampV2,
};
use tower::ServiceExt;

use super::{
    MAX_QUERY_BYTES, MarketBarsHttpError, parse_bars_request, parse_query,
    parse_utc_window_endpoint, principal_may_request_research, read_gateway_delegation,
    valid_cache_scope_id,
};
use crate::{
    http_api::{ServiceState, app_router, auth::test_support, supervisor::QuerySupervisor},
    remote_query::{RemoteArchiveReader, RemoteCacheLimits},
    storage::LocalTestTransport,
};

fn bars_uri(extra: &str) -> Uri {
    let query = format!(
        "symbol=SPY&asset_class=etf&start=2024-01-02T14:30:00Z&end=2024-01-02T21:00:00Z&resolution=1m&session_scope=all&adjustment=unadjusted&provider_mode=auto&allow_daily_downgrade=true&limit=390{extra}"
    );
    format!("/v1/market/bars?{query}").parse().unwrap()
}

#[test]
fn parses_core_bars_v1_query_without_losing_the_opaque_cursor() {
    let request = parse_bars_request(&bars_uri("&cursor=opaque-page-token"))
        .expect("valid Core Bars V1 request");

    assert_eq!(request.symbol, "SPY");
    assert_eq!(request.asset_class, market_contracts::AssetClassV1::Etf);
    assert_eq!(request.start.as_str(), "2024-01-02T14:30:00Z");
    assert_eq!(request.end.as_str(), "2024-01-02T21:00:00Z");
    assert_eq!(
        request.resolution,
        market_contracts::BarResolutionV1::OneMinute
    );
    assert_eq!(request.cursor.as_deref(), Some("opaque-page-token"));
}

#[test]
fn rejects_unknown_duplicate_oversized_and_missing_query_fields() {
    assert!(matches!(
        parse_bars_request(&bars_uri("&debug=true")),
        Err(MarketBarsHttpError::InvalidRequest)
    ));
    assert!(matches!(
        parse_bars_request(&bars_uri("&symbol=QQQ")),
        Err(MarketBarsHttpError::InvalidRequest)
    ));
    assert_eq!(
        parse_query::<super::MarketSessionsQuery>(&Uri::from_static("/v1/market/sessions")),
        Err(MarketBarsHttpError::InvalidRequest)
    );

    let oversized = format!("&cursor={}", "x".repeat(MAX_QUERY_BYTES));
    assert!(matches!(
        parse_bars_request(&bars_uri(&oversized)),
        Err(MarketBarsHttpError::InvalidRequest)
    ));
}

#[test]
fn rejects_non_z_timestamps_and_empty_or_reversed_windows() {
    let offset = bars_uri("")
        .to_string()
        .replace("2024-01-02T14:30:00Z", "2024-01-02T14:30:00%2B00:00")
        .parse()
        .unwrap();
    assert!(matches!(
        parse_bars_request(&offset),
        Err(MarketBarsHttpError::InvalidRequest)
    ));

    let reversed = bars_uri("")
        .to_string()
        .replace("2024-01-02T21:00:00Z", "2024-01-02T13:00:00Z")
        .parse()
        .unwrap();
    assert!(matches!(
        parse_bars_request(&reversed),
        Err(MarketBarsHttpError::InvalidRequest)
    ));
}

#[test]
fn research_provider_mode_requires_the_verified_research_channel() {
    let terminal = test_support::terminal_principal();
    let research = test_support::research_principal();

    assert!(principal_may_request_research(
        &terminal,
        BarsProviderModeV1::Auto
    ));
    assert!(principal_may_request_research(
        &terminal,
        BarsProviderModeV1::AlpacaOnly
    ));
    assert!(!principal_may_request_research(
        &terminal,
        BarsProviderModeV1::Research
    ));
    assert!(principal_may_request_research(
        &research,
        BarsProviderModeV1::Research
    ));
}

#[test]
fn gateway_delegation_is_optional_single_opaque_value_and_never_trimmed() {
    assert_eq!(read_gateway_delegation(&HeaderMap::new()), Ok(None));

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-eqo-gateway-delegation",
        HeaderValue::from_static("short.jwt.token"),
    );
    assert_eq!(
        read_gateway_delegation(&headers),
        Ok(Some("short.jwt.token".to_owned()))
    );

    headers.append(
        "x-eqo-gateway-delegation",
        HeaderValue::from_static("another.jwt.token"),
    );
    assert_eq!(
        read_gateway_delegation(&headers),
        Err(MarketBarsHttpError::InvalidRequest)
    );
}

#[test]
fn sessions_endpoints_accept_only_rfc3339_utc_z_values() {
    assert_eq!(
        parse_utc_window_endpoint("2024-11-03T05:30:00Z")
            .unwrap()
            .to_rfc3339(),
        "2024-11-03T05:30:00+00:00"
    );
    assert_eq!(
        parse_utc_window_endpoint("2024-11-03T01:30:00-04:00"),
        Err(MarketBarsHttpError::InvalidRequest)
    );
}

#[test]
fn sessions_window_limit_rejects_any_fractional_excess() {
    let start = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let exact_end = start + chrono::Duration::days(366);
    assert!(super::session_window_is_bounded(&start, &exact_end));
    assert!(!super::session_window_is_bounded(
        &start,
        &(exact_end + chrono::Duration::nanoseconds(1))
    ));
    assert!(!super::session_window_is_bounded(&start, &start));
}

#[test]
fn cache_scope_profile_is_a_bounded_non_secret_identifier() {
    assert!(valid_cache_scope_id("alpaca-primary-v1"));
    assert!(valid_cache_scope_id("research.account_2"));
    assert!(!valid_cache_scope_id(""));
    assert!(!valid_cache_scope_id("profile with spaces"));
    assert!(!valid_cache_scope_id(&"x".repeat(129)));
}

#[test]
fn response_status_never_calls_partial_or_unknown_coverage_complete() {
    assert_eq!(
        super::http_status_for_coverage(BarsCoverageStatusV1::Complete),
        axum::http::StatusCode::OK
    );
    assert_eq!(
        super::http_status_for_coverage(BarsCoverageStatusV1::Partial),
        axum::http::StatusCode::PARTIAL_CONTENT
    );
    assert_eq!(
        super::http_status_for_coverage(BarsCoverageStatusV1::Unknown),
        axum::http::StatusCode::PARTIAL_CONTENT
    );
    assert_eq!(
        super::http_status_for_coverage(BarsCoverageStatusV1::Unsupported),
        axum::http::StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        super::MarketBarsHttpError::ProviderUnauthorized
            .into_response()
            .status(),
        axum::http::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        super::MarketBarsHttpError::ProviderForbidden
            .into_response()
            .status(),
        axum::http::StatusCode::FORBIDDEN
    );
    assert_eq!(
        super::map_query_failure(super::QueryFailure::Data(
            crate::MarketDataError::InputLimit
        ))
        .into_response()
        .status(),
        axum::http::StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[test]
fn combined_plan_and_provider_gaps_are_sorted_and_non_overlapping() {
    let gap = |start: &str, end: &str, reason_code| BarCoverageGapV1 {
        start_utc: ProtoTimestampV2::parse(start).unwrap(),
        end_exclusive_utc: ProtoTimestampV2::parse(end).unwrap(),
        reason_code,
    };
    let gaps = super::response::normalize_gaps(vec![
        gap(
            "2024-01-02T14:30:00Z",
            "2024-01-02T14:35:00Z",
            CoverageGapReasonV1::Unknown,
        ),
        gap(
            "2024-01-02T14:33:00Z",
            "2024-01-02T14:38:00Z",
            CoverageGapReasonV1::ProviderUnavailable,
        ),
    ])
    .unwrap();

    assert_eq!(gaps.len(), 2);
    assert_eq!(gaps[0].reason_code, CoverageGapReasonV1::Unknown);
    assert_eq!(
        gaps[1].reason_code,
        CoverageGapReasonV1::ProviderUnavailable
    );
    assert_eq!(gaps[0].end_exclusive_utc, gaps[1].start_utc);
    assert!(
        gaps.windows(2)
            .all(|pair| { pair[0].end_exclusive_utc <= pair[1].start_utc })
    );
}

#[test]
fn complete_segments_are_split_around_gaps_but_partial_segments_may_overlap() {
    let timestamp = |value: &str| ProtoTimestampV2::parse(value).unwrap();
    let gap = BarCoverageGapV1 {
        start_utc: timestamp("2024-01-02T14:33:00Z"),
        end_exclusive_utc: timestamp("2024-01-02T14:35:00Z"),
        reason_code: CoverageGapReasonV1::Unknown,
    };
    let segment = |coverage_status| BarCoverageSegmentV1 {
        start_utc: timestamp("2024-01-02T14:30:00Z"),
        end_exclusive_utc: timestamp("2024-01-02T14:40:00Z"),
        coverage_status,
        provenance_id: Some("source-1".to_owned()),
        session_instance_id: "session-1".to_owned(),
    };

    let complete = super::response::subtract_complete_segment_gaps(
        vec![segment(BarsCoverageStatusV1::Complete)],
        std::slice::from_ref(&gap),
    )
    .unwrap();
    assert_eq!(complete.len(), 2);
    assert_eq!(complete[0].end_exclusive_utc, gap.start_utc);
    assert_eq!(complete[1].start_utc, gap.end_exclusive_utc);

    let partial = super::response::subtract_complete_segment_gaps(
        vec![segment(BarsCoverageStatusV1::Partial)],
        std::slice::from_ref(&gap),
    )
    .unwrap();
    assert_eq!(partial.len(), 1);
    assert_eq!(
        partial[0].start_utc,
        segment(BarsCoverageStatusV1::Partial).start_utc
    );
    assert_eq!(
        partial[0].end_exclusive_utc,
        segment(BarsCoverageStatusV1::Partial).end_exclusive_utc
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn bars_route_authenticates_before_research_gate_and_ignores_identity_headers() {
    let temp = tempfile::tempdir().unwrap();
    let archive_root = temp.path().join("archive");
    std::fs::create_dir_all(&archive_root).unwrap();
    let reader = Arc::new(
        RemoteArchiveReader::local_test_isolated(
            LocalTestTransport::new(archive_root).unwrap(),
            temp.path().join("query-cache"),
            RemoteCacheLimits::default(),
        )
        .unwrap(),
    );
    let (mut supervisor, query_client) = QuerySupervisor::start(Arc::clone(&reader), None);
    let state = Arc::new(ServiceState {
        auth_configured: true,
        market_calendar: None,
        market_bars: None,
        query_client,
        reader,
        pair_query_available: false,
    });
    let app = app_router(state, Some(test_support::auth_config()));
    let research_uri = bars_uri("")
        .to_string()
        .replace("provider_mode=auto", "provider_mode=research");

    let forged_identity = Request::builder()
        .method("GET")
        .uri(&research_uri)
        .header("x-eqo-principal", "research")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(forged_identity).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let terminal_request = Request::builder()
        .method("GET")
        .uri(&research_uri)
        .header(
            "authorization",
            format!("Bearer {}", test_support::terminal_token("market:read")),
        )
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(terminal_request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let research_request = Request::builder()
        .method("GET")
        .uri(&research_uri)
        .header(
            "authorization",
            format!("Bearer {}", test_support::research_token("market:read")),
        )
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(research_request).await.unwrap();
    // This authenticated research request reaches the service and reports its missing trusted
    // calendar accurately; it is not rejected as an unauthorized research principal.
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    supervisor.shutdown().await.unwrap();
}
