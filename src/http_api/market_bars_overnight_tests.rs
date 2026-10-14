//! Linux-only authenticated HTTP coverage for signed overnight calendar selection.
//!
//! All pages and issuer signatures are synthetic fixtures. They prove route wiring and fail-closed
//! behavior only; they do not prove that any real provider serves OVER data or grants entitlement.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use market_contracts::{
    AssetClassV1, BarCoverageSegmentV1, BarRowV1, BarSourceProvenanceV1, BarsCacheStatusV1,
    BarsCoverageStatusV1, BarsEntitlementStatusV1, BarsResponseV1, BarsValidationStateV1,
    DecimalString, NumericEncodingV1, ProtoTimestampV2,
};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use super::{MarketBarsRuntime, SyntheticMarketBarsPageSource};
use crate::{
    http_api::{ServiceState, app_router, auth::test_support, supervisor::QuerySupervisor},
    market_calendar::{
        LoadedMarketCalendar, OvernightRouteFeedV1, OvernightRouteV1,
        overnight_overlay::test_support::{
            SyntheticOverlayVerifier, SyntheticOvernightWindow, signed_overlay_for_tests,
        },
    },
    market_query::providers::{
        MarketBarsProviderRuntime, ProviderCacheIdentity, ProviderPageFetch, ProviderQueryError,
    },
    remote_query::{RemoteArchiveReader, RemoteCacheLimits},
    session_router::{
        AlpacaFeed, CalendarDay, CalendarDayState, CalendarVerificationStatus, DataInterval,
        PlannedSource, Provider, SessionPhase, SourceCapability, TrustedCalendarSnapshot,
        UtcWindow,
    },
    storage::LocalTestTransport,
};

const CASH_INPUT_SHA256: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const SYNTHETIC_PAGE_SOURCE: &str = "Synthetic OVER page fixture; not ProviderVerified";

struct SyntheticOvernightPageSource {
    capabilities: Vec<SourceCapability>,
    fetch_count: AtomicUsize,
}

impl SyntheticOvernightPageSource {
    fn new(capabilities: Vec<SourceCapability>) -> Self {
        Self {
            capabilities,
            fetch_count: AtomicUsize::new(0),
        }
    }
}

impl SyntheticMarketBarsPageSource for SyntheticOvernightPageSource {
    fn source_capabilities(&self) -> Vec<SourceCapability> {
        self.capabilities.clone()
    }

    fn cache_identity(
        &self,
        selection: &crate::market_query::providers::ProviderPageSelection,
    ) -> ProviderCacheIdentity {
        let mut identity = selection.cache_identity().clone();
        identity.provider = match selection.provider_group().source.provider() {
            Provider::Alpaca => "alpaca",
            Provider::Futu => "futu",
            Provider::YFinance => "yfinance",
        }
        .to_owned();
        identity.feed = "unknown".to_owned();
        identity.source_scope_id = "synthetic-overnight-page-v1".to_owned();
        identity.entitlement_status = BarsEntitlementStatusV1::Unknown;
        identity.numeric_encoding = match selection.provider_group().source {
            PlannedSource::Futu => NumericEncodingV1::BinaryFloat64ShortestDecimal,
            PlannedSource::Alpaca { .. } | PlannedSource::YFinanceDailyResearch => {
                NumericEncodingV1::DecimalToken
            }
        };
        identity
    }

    fn fetch_page(
        &self,
        plan: &crate::market_query::planning::MarketBarsPlan,
        selection: &crate::market_query::providers::ProviderPageSelection,
    ) -> Result<ProviderPageFetch, ProviderQueryError> {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        let group = selection.provider_group();
        let (provider, encoding) = match group.source {
            PlannedSource::Futu => ("futu", NumericEncodingV1::BinaryFloat64ShortestDecimal),
            PlannedSource::Alpaca { .. } => ("alpaca", NumericEncodingV1::DecimalToken),
            PlannedSource::YFinanceDailyResearch => {
                return Err(ProviderQueryError::InvalidCorePage);
            }
        };
        let start = group.fetch_window.start;
        let end = group.fetch_window.end_exclusive;
        let fetched_at = timestamp_now();
        let mut rows = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let bar_end = (cursor + Duration::minutes(1)).min(end);
            rows.push(BarRowV1 {
                symbol: plan.request.symbol.clone(),
                asset_class: plan.request.asset_class,
                resolution: group.resolution,
                adjustment: group.adjustment,
                bar_start_utc: timestamp(&cursor.to_rfc3339_opts(SecondsFormat::Nanos, true))?,
                bar_end_exclusive_utc: timestamp(
                    &bar_end.to_rfc3339_opts(SecondsFormat::Nanos, true),
                )?,
                received_at_utc: fetched_at.clone(),
                available_at_utc: fetched_at.clone(),
                open: decimal("10.00")?,
                high: decimal("11.00")?,
                low: decimal("9.00")?,
                close: decimal("10.50")?,
                volume: decimal("100")?,
                provenance_id: "synthetic-over-page-v1".to_owned(),
                session_instance_id: "provider-local".to_owned(),
                validation_state: BarsValidationStateV1::Provisional,
            });
            cursor = bar_end;
        }
        let start_utc = timestamp(&start.to_rfc3339_opts(SecondsFormat::Nanos, true))?;
        let end_exclusive_utc = timestamp(&end.to_rfc3339_opts(SecondsFormat::Nanos, true))?;
        let response = BarsResponseV1 {
            schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
            provider: provider.to_owned(),
            feed: "unknown".to_owned(),
            entitlement_status: BarsEntitlementStatusV1::Unknown,
            requested_resolution: plan.request.resolution,
            effective_resolution: group.resolution,
            adjustment: group.adjustment,
            rows,
            coverage_status: BarsCoverageStatusV1::Partial,
            segments: vec![BarCoverageSegmentV1 {
                start_utc,
                end_exclusive_utc,
                coverage_status: BarsCoverageStatusV1::Partial,
                provenance_id: Some("synthetic-over-page-v1".to_owned()),
                session_instance_id: "provider-local".to_owned(),
            }],
            gaps: Vec::new(),
            source_provenance: vec![BarSourceProvenanceV1 {
                provenance_id: "synthetic-over-page-v1".to_owned(),
                provider: provider.to_owned(),
                feed: "unknown".to_owned(),
                source: SYNTHETIC_PAGE_SOURCE.to_owned(),
                entitlement_status: BarsEntitlementStatusV1::Unknown,
                numeric_encoding: encoding,
            }],
            as_of: None,
            fetched_at: fetched_at.clone(),
            verified_at: None,
            cache_status: BarsCacheStatusV1::Miss,
            market_calendar_version: Some(plan.calendar_version.clone()),
            continuation_cursor: None,
            truncated: false,
        };
        ProviderPageFetch::new(
            response,
            Some("e".repeat(64)),
            Some(fetched_at),
            Some(1),
            Some(true),
            None,
        )
    }
}

struct Harness {
    _temp: tempfile::TempDir,
    app: axum::Router,
    supervisor: QuerySupervisor,
    source: Arc<SyntheticOvernightPageSource>,
}

#[derive(Clone, Copy)]
struct BarsQuery<'a> {
    symbol: &'a str,
    provider_mode: &'a str,
    session_scope: &'a str,
    start: &'a str,
    end: &'a str,
    cursor: Option<&'a str>,
}

fn bars_query<'a>(
    symbol: &'a str,
    provider_mode: &'a str,
    session_scope: &'a str,
    start: &'a str,
    end: &'a str,
    cursor: Option<&'a str>,
) -> BarsQuery<'a> {
    BarsQuery {
        symbol,
        provider_mode,
        session_scope,
        start,
        end,
        cursor,
    }
}

async fn harness(calendar: LoadedMarketCalendar, capabilities: Vec<SourceCapability>) -> Harness {
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
    let (supervisor, query_client) = QuerySupervisor::start(Arc::clone(&reader), None);
    let source = Arc::new(SyntheticOvernightPageSource::new(capabilities));
    let provider = MarketBarsProviderRuntime::from_environment().unwrap();
    let runtime = Arc::new(MarketBarsRuntime::synthetic_without_cache_for_test(
        provider,
        source.clone(),
    ));
    let state = Arc::new(ServiceState {
        auth_configured: true,
        market_calendar: Some(calendar),
        market_bars: Some(runtime),
        query_client,
        reader,
        pair_query_available: false,
    });
    let app = app_router(state, Some(test_support::auth_config()));
    Harness {
        _temp: temp,
        app,
        supervisor,
        source,
    }
}

#[tokio::test]
async fn signed_over_selection_flows_through_http_without_claiming_market_rights() {
    let (calendar, _verifier, _artifact) = signed_calendar(
        vec![futu_route(), alpaca_boats_route()],
        vec!["SPY"],
        current_unix_seconds().saturating_sub(60),
        current_unix_seconds().saturating_add(3_600),
        None,
    );
    let mut harness = harness(calendar, vec![futu_over_capability()]).await;

    let bars = send_bars(
        &harness.app,
        &test_support::terminal_token("market:read"),
        bars_query(
            "SPY",
            "futu_only",
            "overnight",
            "2026-01-05T01:00:00Z",
            "2026-01-05T01:02:00Z",
            None,
        ),
    )
    .await;
    assert_eq!(bars.status(), StatusCode::PARTIAL_CONTENT);
    let body = response_json(bars).await;
    assert_eq!(body["provider"], "futu");
    assert_eq!(body["feed"], "unknown");
    assert_eq!(body["entitlement_status"], "unknown");
    assert_eq!(body["coverage_status"], "partial");
    assert_eq!(body["verified_at"], serde_json::Value::Null);
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["rows"][0]["session_instance_id"],
        "XNYS:2026-01-05:overnight"
    );
    assert!(
        body["market_calendar_version"]
            .as_str()
            .unwrap()
            .starts_with("composite.v1.")
    );
    assert!(
        body["source_provenance"][0]["source"]
            .as_str()
            .unwrap()
            .contains("not ProviderVerified")
    );
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 1);

    let sessions = send_sessions(
        &harness.app,
        &test_support::terminal_token("market:read"),
        Some("SPY"),
        Some("etf"),
        "2026-01-05T01:00:00Z",
        "2026-01-05T01:02:00Z",
    )
    .await;
    assert_eq!(sessions.status(), StatusCode::OK);
    let sessions_body = response_json(sessions).await;
    assert_eq!(sessions_body["calendar_overlay_status"], "applied");
    assert_eq!(sessions_body["symbol"], "SPY");
    assert_eq!(sessions_body["asset_class"], "etf");
    assert_eq!(
        sessions_body["market_calendar_version"],
        body["market_calendar_version"]
    );
    assert_eq!(
        sessions_body["calendar_components"]["overnight"]["source_id"],
        "synthetic-overnight-calendar"
    );
    assert_eq!(
        sessions_body["allowed_overnight_routes"][0]["provider"],
        "alpaca"
    );
    assert_eq!(
        sessions_body["allowed_overnight_routes"][1]["provider"],
        "futu"
    );
    assert!(
        sessions_body["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|instance| instance["phase"] == "over")
    );
    let public_body = sessions_body.to_string();
    for private_value in ["signature_base64", "synthetic-overlay-key", "issuer"] {
        assert!(!public_body.contains(private_value));
    }

    // Omitting the paired instrument explicitly keeps the endpoint on the stable cash view.
    let cash_only = send_sessions(
        &harness.app,
        &test_support::terminal_token("market:read"),
        None,
        None,
        "2026-01-05T01:00:00Z",
        "2026-01-05T01:02:00Z",
    )
    .await;
    assert_eq!(cash_only.status(), StatusCode::OK);
    let cash_body = response_json(cash_only).await;
    assert_eq!(
        cash_body["calendar_overlay_status"],
        serde_json::Value::Null
    );
    assert!(
        cash_body["market_calendar_version"]
            .as_str()
            .unwrap()
            .starts_with("cash.v1.")
    );
    assert_eq!(
        cash_body["calendar_components"]["overnight"],
        serde_json::Value::Null
    );
    assert!(
        cash_body["allowed_overnight_routes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        !cash_body["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|instance| instance["phase"] == "over")
    );

    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn signed_calendar_keeps_overnight_pre_and_regular_boundaries_distinct() {
    let (calendar, _, _) = signed_calendar(
        vec![futu_route()],
        vec!["SPY"],
        current_unix_seconds().saturating_sub(60),
        current_unix_seconds().saturating_add(3_600),
        None,
    );
    let mut harness = harness(calendar, Vec::new()).await;
    let token = test_support::terminal_token("market:read");

    // 22:00 ET Sunday is 03:00 UTC Monday and belongs to the signed OVER interval.
    let overnight = send_sessions(
        &harness.app,
        &token,
        Some("SPY"),
        Some("etf"),
        "2026-01-05T03:00:00Z",
        "2026-01-05T03:01:00Z",
    )
    .await;
    let overnight = response_json(overnight).await;
    assert_eq!(overnight["calendar_overlay_status"], "applied");
    assert_eq!(overnight["instances"][0]["phase"], "over");

    // 07:00 ET Monday (12:00 UTC) remains pre-market and does not acquire an OVER identity.
    let pre = send_sessions(
        &harness.app,
        &token,
        Some("SPY"),
        Some("etf"),
        "2026-01-05T12:00:00Z",
        "2026-01-05T12:01:00Z",
    )
    .await;
    let pre = response_json(pre).await;
    assert!(
        pre["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|instance| instance["phase"] == "pre")
    );
    assert!(
        !pre["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|instance| instance["phase"] == "over")
    );

    // 09:40 ET Monday (14:40 UTC) is regular trading, independent of the OVER timetable.
    let regular = send_sessions(
        &harness.app,
        &token,
        Some("SPY"),
        Some("etf"),
        "2026-01-05T14:40:00Z",
        "2026-01-05T14:41:00Z",
    )
    .await;
    let regular = response_json(regular).await;
    assert!(
        regular["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|instance| instance["phase"] == "regular")
    );
    assert!(
        !regular["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|instance| instance["phase"] == "over")
    );

    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn default_cash_calendar_leaves_unattested_over_as_a_gap() {
    let calendar = cash_calendar();
    let mut harness = harness(calendar, vec![futu_over_capability()]).await;
    let response = send_bars(
        &harness.app,
        &test_support::terminal_token("market:read"),
        bars_query(
            "SPY",
            "futu_only",
            "overnight",
            "2026-01-05T01:00:00Z",
            "2026-01-05T01:02:00Z",
            None,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = response_json(response).await;
    assert_eq!(body["coverage_status"], "partial");
    assert_eq!(body["rows"].as_array().unwrap().len(), 0);
    assert_eq!(body["provider"], "unknown");
    assert_eq!(body["feed"], "unknown");
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 0);
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn signed_routes_and_symbol_scope_filter_over_capabilities_exactly() {
    // A Futu capability is not accepted by a signed Alpaca/BOATS-only route.
    let (calendar, _, _) = signed_calendar(
        vec![alpaca_boats_route()],
        vec!["SPY"],
        current_unix_seconds().saturating_sub(60),
        current_unix_seconds().saturating_add(3_600),
        None,
    );
    let mut futu_harness = harness(calendar, vec![futu_over_capability()]).await;
    let response = send_bars(
        &futu_harness.app,
        &test_support::terminal_token("market:read"),
        bars_query(
            "SPY",
            "futu_only",
            "overnight",
            "2026-01-05T01:00:00Z",
            "2026-01-05T01:02:00Z",
            None,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response_json(response).await["rows"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(futu_harness.source.fetch_count.load(Ordering::SeqCst), 0);
    futu_harness.supervisor.shutdown().await.unwrap();

    // A valid route for SPY cannot extend to an unlisted symbol.
    let (calendar, _, _) = signed_calendar(
        vec![futu_route()],
        vec!["SPY"],
        current_unix_seconds().saturating_sub(60),
        current_unix_seconds().saturating_add(3_600),
        None,
    );
    let mut symbol_harness = harness(calendar, vec![futu_over_capability()]).await;
    let status = send_sessions(
        &symbol_harness.app,
        &test_support::terminal_token("market:read"),
        Some("QQQ"),
        Some("etf"),
        "2026-01-05T01:00:00Z",
        "2026-01-05T01:02:00Z",
    )
    .await;
    assert_eq!(
        response_json(status).await["calendar_overlay_status"],
        "not_eligible"
    );
    let response = send_bars(
        &symbol_harness.app,
        &test_support::terminal_token("market:read"),
        bars_query(
            "QQQ",
            "futu_only",
            "overnight",
            "2026-01-05T01:00:00Z",
            "2026-01-05T01:02:00Z",
            None,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response_json(response).await["rows"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(symbol_harness.source.fetch_count.load(Ordering::SeqCst), 0);
    symbol_harness.supervisor.shutdown().await.unwrap();

    // Alpaca SIP is never an overnight route, even when BOATS is signed for the same symbol.
    let (calendar, _, _) = signed_calendar(
        vec![alpaca_boats_route()],
        vec!["SPY"],
        current_unix_seconds().saturating_sub(60),
        current_unix_seconds().saturating_add(3_600),
        None,
    );
    let mut sip_harness = harness(calendar, vec![alpaca_over_capability(AlpacaFeed::Sip)]).await;
    let response = send_bars(
        &sip_harness.app,
        &test_support::terminal_token("market:read"),
        bars_query(
            "SPY",
            "alpaca_only",
            "overnight",
            "2026-01-05T01:00:00Z",
            "2026-01-05T01:02:00Z",
            None,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response_json(response).await["rows"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(sip_harness.source.fetch_count.load(Ordering::SeqCst), 0);
    sip_harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_expired_revoked_or_bad_signature_overlays_keep_cash_only() {
    let now = current_unix_seconds();
    let cases = [
        (
            "expired",
            now.saturating_sub(3_600),
            now.saturating_sub(1),
            None,
            "expired",
        ),
        (
            "revoked",
            now.saturating_sub(3_600),
            now.saturating_add(3_600),
            Some(now.saturating_sub(1)),
            "revoked",
        ),
    ];
    for (_label, issue, expiry, revoked, expected_status) in cases {
        let (calendar, _, _) =
            signed_calendar(vec![futu_route()], vec!["SPY"], issue, expiry, revoked);
        let mut harness = harness(calendar, vec![futu_over_capability()]).await;
        assert_overlay_rejected(&mut harness, expected_status).await;
    }

    let (calendar, _) = bad_signature_calendar();
    let mut harness = harness(calendar, vec![futu_over_capability()]).await;
    assert_overlay_rejected(&mut harness, "trust_rejected").await;
}

async fn assert_overlay_rejected(harness: &mut Harness, expected_status: &str) {
    let session_response = send_sessions(
        &harness.app,
        &test_support::terminal_token("market:read"),
        Some("SPY"),
        Some("etf"),
        "2026-01-05T01:00:00Z",
        "2026-01-05T01:02:00Z",
    )
    .await;
    assert_eq!(session_response.status(), StatusCode::OK);
    let sessions = response_json(session_response).await;
    assert_eq!(sessions["calendar_overlay_status"], expected_status);
    assert!(
        !sessions["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|instance| instance["phase"] == "over")
    );

    let response = send_bars(
        &harness.app,
        &test_support::terminal_token("market:read"),
        bars_query(
            "SPY",
            "futu_only",
            "overnight",
            "2026-01-05T01:00:00Z",
            "2026-01-05T01:02:00Z",
            None,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = response_json(response).await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 0);
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 0);
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn revoking_overlay_invalidates_an_existing_signed_plan_cursor() {
    let (calendar, verifier, _) = signed_calendar(
        vec![alpaca_boats_route()],
        vec!["SPY"],
        current_unix_seconds().saturating_sub(60),
        current_unix_seconds().saturating_add(3_600),
        None,
    );
    let capabilities = vec![
        alpaca_over_capability(AlpacaFeed::Boats),
        alpaca_capability(AlpacaFeed::Sip, SessionPhase::Pre),
    ];
    let mut harness = harness(calendar, capabilities).await;
    let token = test_support::terminal_token("market:read");
    let first = send_bars(
        &harness.app,
        &token,
        bars_query(
            "SPY",
            "auto",
            "all",
            "2026-01-05T01:00:00Z",
            "2026-01-05T10:00:00Z",
            None,
        ),
    )
    .await;
    assert_eq!(first.status(), StatusCode::PARTIAL_CONTENT);
    let first_body = response_json(first).await;
    assert_eq!(first_body["truncated"], true);
    let cursor = first_body["continuation_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 1);

    // The same rights and request cannot continue against a different current calendar version.
    verifier.set_revoked(true);
    let continued = send_bars(
        &harness.app,
        &token,
        bars_query(
            "SPY",
            "auto",
            "all",
            "2026-01-05T01:00:00Z",
            "2026-01-05T10:00:00Z",
            Some(&cursor),
        ),
    )
    .await;
    assert_eq!(continued.status(), StatusCode::BAD_REQUEST);
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 1);
    harness.supervisor.shutdown().await.unwrap();
}

fn cash_calendar() -> LoadedMarketCalendar {
    LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(
        two_day_cash_snapshot(),
        CASH_INPUT_SHA256,
    )
}

fn two_day_cash_snapshot() -> TrustedCalendarSnapshot {
    let mut cash = super::authority_tests::one_day_calendar();
    let trade_date = cash.coverage_start;
    let prior_date = trade_date.pred_opt().unwrap();
    let trade_day_start = cash.days[0].day_window.start;
    let prior_start = trade_day_start - Duration::days(1);
    cash.coverage_start = prior_date;
    cash.days.insert(
        0,
        CalendarDay {
            trade_date: prior_date,
            day_window: UtcWindow {
                start: prior_start,
                end_exclusive: trade_day_start,
            },
            state: CalendarDayState::Closed {
                reason: Some("weekend".to_owned()),
            },
            verification_status: CalendarVerificationStatus::ModeledWeekend,
        },
    );
    cash
}

fn signed_calendar(
    routes: Vec<OvernightRouteV1>,
    symbols: Vec<&str>,
    issued_at: u64,
    expires_at: u64,
    revoked_at: Option<u64>,
) -> (LoadedMarketCalendar, Arc<SyntheticOverlayVerifier>, Vec<u8>) {
    let cash = two_day_cash_snapshot();
    let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let fixture = signed_overlay_for_tests(
        &cash,
        CASH_INPUT_SHA256,
        Provider::Futu,
        OvernightRouteFeedV1::Futu,
        canonical_routes(routes),
        vec![AssetClassV1::Stock, AssetClassV1::Etf],
        symbols.into_iter().map(str::to_owned).collect(),
        vec![SyntheticOvernightWindow {
            trade_date,
            session_id: "XNYS:2026-01-05".to_owned(),
            session_instance_id: "XNYS:2026-01-05:overnight".to_owned(),
            window: UtcWindow {
                start: parse_time("2026-01-05T01:00:00Z"),
                end_exclusive: parse_time("2026-01-05T09:00:00Z"),
            },
        }],
        issued_at,
        expires_at,
        revoked_at,
    );
    let bytes = fixture.bytes.clone();
    let verifier = Arc::clone(&fixture.verifier);
    let input_sha256 = sha256_hex(&bytes);
    let calendar = LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash, CASH_INPUT_SHA256)
        .with_signed_overnight_overlay(&bytes, &input_sha256, verifier.clone());
    (calendar, verifier, bytes)
}

fn bad_signature_calendar() -> (LoadedMarketCalendar, Vec<u8>) {
    let now = current_unix_seconds();
    let cash = two_day_cash_snapshot();
    let fixture = signed_overlay_for_tests(
        &cash,
        CASH_INPUT_SHA256,
        Provider::Futu,
        OvernightRouteFeedV1::Futu,
        vec![futu_route()],
        vec![AssetClassV1::Stock, AssetClassV1::Etf],
        vec!["SPY".to_owned()],
        vec![SyntheticOvernightWindow {
            trade_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            session_id: "XNYS:2026-01-05".to_owned(),
            session_instance_id: "XNYS:2026-01-05:overnight".to_owned(),
            window: UtcWindow {
                start: parse_time("2026-01-05T01:00:00Z"),
                end_exclusive: parse_time("2026-01-05T09:00:00Z"),
            },
        }],
        now.saturating_sub(60),
        now.saturating_add(3_600),
        None,
    );
    let mut artifact: serde_json::Value = serde_json::from_slice(&fixture.bytes).unwrap();
    artifact["signature_base64"] =
        serde_json::Value::String("synthetic-overlay:invalid".to_owned());
    let bytes = serde_json::to_vec(&artifact).unwrap();
    let calendar = LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash, CASH_INPUT_SHA256)
        .with_signed_overnight_overlay(&bytes, &sha256_hex(&bytes), fixture.verifier.clone());
    (calendar, bytes)
}

fn futu_route() -> OvernightRouteV1 {
    OvernightRouteV1 {
        provider: Provider::Futu,
        feed: OvernightRouteFeedV1::Futu,
    }
}

fn canonical_routes(mut routes: Vec<OvernightRouteV1>) -> Vec<OvernightRouteV1> {
    routes.sort_by_key(|route| match (route.provider, route.feed) {
        (Provider::Alpaca, OvernightRouteFeedV1::Boats) => "alpaca/boats",
        (Provider::Futu, OvernightRouteFeedV1::Futu) => "futu/futu",
        _ => "unsupported/unsupported",
    });
    routes
}

fn alpaca_boats_route() -> OvernightRouteV1 {
    OvernightRouteV1 {
        provider: Provider::Alpaca,
        feed: OvernightRouteFeedV1::Boats,
    }
}

fn futu_over_capability() -> SourceCapability {
    SourceCapability {
        provider: Provider::Futu,
        feed: None,
        phase: Some(SessionPhase::Over),
        interval: DataInterval::Minute(1),
        historical: true,
        realtime: false,
    }
}

fn alpaca_over_capability(feed: AlpacaFeed) -> SourceCapability {
    SourceCapability {
        provider: Provider::Alpaca,
        feed: Some(feed),
        phase: Some(SessionPhase::Over),
        interval: DataInterval::Minute(1),
        historical: true,
        realtime: false,
    }
}

fn alpaca_capability(feed: AlpacaFeed, phase: SessionPhase) -> SourceCapability {
    SourceCapability {
        provider: Provider::Alpaca,
        feed: Some(feed),
        phase: Some(phase),
        interval: DataInterval::Minute(1),
        historical: true,
        realtime: false,
    }
}

async fn send_bars(
    app: &axum::Router,
    token: &str,
    query: BarsQuery<'_>,
) -> axum::response::Response {
    let mut uri = format!(
        "/v1/market/bars?symbol={}&asset_class=etf&start={}&end={}&resolution=1m&session_scope={}&adjustment=unadjusted&provider_mode={}&allow_daily_downgrade=false&limit=1000",
        query.symbol, query.start, query.end, query.session_scope, query.provider_mode,
    );
    if let Some(cursor) = query.cursor {
        uri.push_str("&cursor=");
        uri.push_str(cursor);
    }
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap()
}

async fn send_sessions(
    app: &axum::Router,
    token: &str,
    symbol: Option<&str>,
    asset_class: Option<&str>,
    start: &str,
    end: &str,
) -> axum::response::Response {
    let mut uri = format!("/v1/market/sessions?start={start}&end={end}");
    if let (Some(symbol), Some(asset_class)) = (symbol, asset_class) {
        uri.push_str("&symbol=");
        uri.push_str(symbol);
        uri.push_str("&asset_class=");
        uri.push_str(asset_class);
    }
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap()
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn decimal(value: &str) -> Result<DecimalString, ProviderQueryError> {
    DecimalString::new(value).map_err(|_| ProviderQueryError::InvalidCorePage)
}

fn timestamp(value: &str) -> Result<ProtoTimestampV2, ProviderQueryError> {
    ProtoTimestampV2::parse(value).map_err(|_| ProviderQueryError::InvalidCorePage)
}

fn timestamp_now() -> ProtoTimestampV2 {
    ProtoTimestampV2::parse(&Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true))
        .expect("UTC test clock is a valid Core timestamp")
}

fn parse_time(value: &str) -> chrono::DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
