use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use market_contracts::AssetClassV1;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

use super::{
    DailyQuery, MAX_RANGE_DAYS, MAX_SYMBOLS, QueryError, is_occ_option_symbol, parse_query,
    parse_utc_timestamp, valid_symbol,
};
use crate::market_query::providers::YFinanceResearchAdjustment;
use crate::{
    remote_query::{RemoteArchiveReader, RemoteCacheLimits},
    storage::LocalTestTransport,
};

use super::super::auth::test_support;
use super::super::supervisor::QuerySupervisor;
use super::super::{ServiceState, app_router};

fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-10-15T12:00:00Z")
        .expect("valid fixture clock")
        .with_timezone(&Utc)
}

fn query(start_utc: &str, end_utc: &str) -> DailyQuery {
    DailyQuery {
        symbols: "AAPL,SPY".to_owned(),
        asset_class: "stock".to_owned(),
        start_utc: start_utc.to_owned(),
        end_utc: end_utc.to_owned(),
        adjustment: None,
        cursor: None,
    }
}

#[test]
fn accepts_half_open_utc_daily_requests_and_defaults_to_unadjusted_preference() {
    let parsed = parse_query(query("2010-01-01T00:00:00Z", "2026-01-01T00:00:00Z"), now())
        .expect("valid research query");
    assert_eq!(
        parsed.request.symbols,
        vec!["AAPL".to_owned(), "SPY".to_owned()]
    );
    assert_eq!(parsed.request.asset_class, AssetClassV1::Stock);
    assert_eq!(
        parsed.request.adjustment,
        YFinanceResearchAdjustment::Unadjusted
    );
    assert_eq!(
        parsed
            .request
            .end_utc
            .signed_duration_since(parsed.request.start_utc),
        Duration::days(5_844)
    );
}

#[test]
fn rejects_non_utc_timestamps_options_duplicate_symbols_and_more_than_eight_symbols() {
    assert_eq!(
        parse_utc_timestamp("2026-10-15T12:00:00+01:00"),
        Err(QueryError::Invalid)
    );
    assert_eq!(
        parse_utc_timestamp("2026-10-15T12:00:00+00:00"),
        Err(QueryError::Invalid)
    );
    assert!(is_occ_option_symbol("AAPL261016C00100000"));
    assert!(!valid_symbol("AAPL261016C00100000"));
    assert!(valid_symbol(&"A".repeat(32)));

    let mut duplicate = query("2026-10-13T00:00:00Z", "2026-10-14T00:00:00Z");
    duplicate.symbols = "AAPL,AAPL".to_owned();
    assert!(matches!(
        parse_query(duplicate, now()),
        Err(QueryError::Unsupported)
    ));

    let mut too_many = query("2026-10-13T00:00:00Z", "2026-10-14T00:00:00Z");
    too_many.symbols = (0..=MAX_SYMBOLS)
        .map(|index| format!("S{index}"))
        .collect::<Vec<_>>()
        .join(",");
    assert!(matches!(
        parse_query(too_many, now()),
        Err(QueryError::Unsupported)
    ));
}

#[test]
fn enforces_1970_range_future_and_maximum_duration_bounds() {
    assert!(matches!(
        parse_query(query("1969-12-31T23:59:59Z", "1970-01-02T00:00:00Z"), now()),
        Err(QueryError::Unsupported)
    ));
    assert!(matches!(
        parse_query(query("2026-10-13T00:00:00Z", "2026-10-16T12:00:01Z"), now()),
        Err(QueryError::Unsupported)
    ));
    let start = "1970-01-01T00:00:00Z";
    let maximum_end = DateTime::parse_from_rfc3339(start)
        .expect("start")
        .with_timezone(&Utc)
        + Duration::days(MAX_RANGE_DAYS);
    // Exercise duration independently of the earlier future-date rejection. This is a
    // synthetic clock, not an assertion that the provider offers this historical interval.
    let synthetic_now = maximum_end + Duration::days(2);
    assert!(
        parse_query(
            query(
                start,
                &maximum_end.to_rfc3339_opts(SecondsFormat::AutoSi, true)
            ),
            synthetic_now,
        )
        .is_ok()
    );
    let end = (maximum_end + Duration::days(1)).to_rfc3339_opts(SecondsFormat::AutoSi, true);
    assert!(matches!(
        parse_query(query(start, &end), synthetic_now),
        Err(QueryError::TooLarge)
    ));
}

#[test]
fn synthetic_success_envelope_keeps_native_payload_and_non_authoritative_flags() {
    let payload = json!({
        "schema_version": "yfinance-history-bridge-v1",
        "status": "ready",
        "bars": [{"symbol":"AAPL","close":"123.45"}],
        "coverage": {"calendar_gaps_assessed": false}
    });
    let symbols = vec!["AAPL".to_owned()];
    let hash = "a".repeat(64);
    let start = DateTime::parse_from_rfc3339("2010-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339("2010-01-02T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let envelope = super::DailyEnvelope {
        schema_version: super::SCHEMA_VERSION,
        provider: "yfinance",
        source: "Yahoo Finance",
        asset_class: "stock",
        resolution: "1d",
        requested_adjustment: "unadjusted",
        provider_adjustment_semantics: "unknown_or_provider_native",
        adjustment_confirmed_raw: false,
        numeric_encoding: "binary_float_projection",
        canonical: false,
        point_in_time: false,
        executable: false,
        display_only: true,
        mdp_coverage_asserted: false,
        requested_symbols: &symbols,
        requested_range: super::utc_range(start, end),
        page_range: super::utc_range(start, end),
        observation: super::Observation {
            source_observation_sha256: &hash,
            fetched_at_utc: "2026-10-14T00:00:00Z",
            payload: &payload,
        },
        next_cursor: None,
    };
    let serialized = serde_json::to_value(envelope).expect("serialize synthetic envelope");
    assert_eq!(serialized["observation"]["payload"], payload);
    assert_eq!(serialized["requested_adjustment"], "unadjusted");
    assert_eq!(serialized["adjustment_confirmed_raw"], false);
    assert_eq!(serialized["point_in_time"], false);
    assert_eq!(serialized["executable"], false);
    assert_eq!(serialized["display_only"], true);
    assert_eq!(serialized["mdp_coverage_asserted"], false);
    assert_eq!(serialized["next_cursor"], Value::Null);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn route_requires_research_principal_and_returns_closed_when_disabled() {
    let temp = tempfile::tempdir().expect("create temporary route fixture");
    let reader = Arc::new(
        RemoteArchiveReader::local_test_isolated(
            LocalTestTransport::new(temp.path().join("remote")).expect("local transport"),
            temp.path().join("cache"),
            RemoteCacheLimits::default(),
        )
        .expect("isolated local-test reader"),
    );
    let (mut supervisor, query_client) = QuerySupervisor::start(
        Arc::clone(&reader),
        #[cfg(target_os = "linux")]
        None,
    );
    let state = Arc::new(ServiceState {
        auth_configured: true,
        market_calendar: None,
        market_bars: None,
        yfinance_research: None,
        query_client,
        reader,
        #[cfg(target_os = "linux")]
        pair_query_available: false,
    });
    let app = app_router(state, Some(test_support::auth_config()));
    let uri = "/v1/research/yfinance/daily?symbols=AAPL&asset_class=stock&start_utc=2010-01-01T00%3A00%3A00Z&end_utc=2010-01-02T00%3A00%3A00Z";

    let terminal = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", test_support::terminal_token("market:read")),
                )
                .body(Body::empty())
                .expect("build terminal request"),
        )
        .await
        .expect("terminal route response");
    assert_eq!(terminal.status(), StatusCode::FORBIDDEN);
    assert!(terminal.headers().get(header::CACHE_CONTROL).is_some());

    let research = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", test_support::research_token("market:read")),
                )
                .body(Body::empty())
                .expect("build research request"),
        )
        .await
        .expect("research route response");
    assert_eq!(research.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(research.headers().get(header::CACHE_CONTROL).is_some());
    let body = to_bytes(research.into_body(), 16 * 1024)
        .await
        .expect("bounded research error body");
    let error: Value = serde_json::from_slice(&body).expect("typed research error");
    assert_eq!(
        error["schema_version"],
        "mdp-yfinance-research-observations-v1"
    );
    assert_eq!(error["error"]["code"], "research_api_disabled");

    supervisor.shutdown().await.expect("join query supervisor");
}
