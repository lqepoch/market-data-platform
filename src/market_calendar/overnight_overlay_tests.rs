//! Unit tests for trusted provider-specific overnight calendar overlays.

use chrono::{Datelike, NaiveDate};
use market_contracts::AssetClassV1;

use super::{
    OvernightOverlayStatus, OvernightRouteFeedV1, OvernightRouteV1,
    test_support::{
        SyntheticOvernightWindow, resign_with_identity_for_tests, signed_overlay_for_tests,
    },
};
use crate::{
    market_calendar::LoadedMarketCalendar,
    session_router::{
        AlpacaFeed, CalendarDay, CalendarDayState, CalendarSourceMetadata,
        CalendarVerificationStatus, DataInterval, Provider, SessionInstance, SessionPhase,
        SourceCapability, TrustedCalendarSnapshot, UtcWindow,
    },
};

const CASH_INPUT_SHA256: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn utc(value: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture")
        .with_timezone(&chrono::Utc)
}

fn date(value: &str) -> NaiveDate {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("date fixture")
}

fn window(start: &str, end: &str) -> UtcWindow {
    UtcWindow {
        start: utc(start),
        end_exclusive: utc(end),
    }
}

fn cash_day(trade_date: &str, start: &str, end: &str, session_id: Option<&str>) -> CalendarDay {
    let trade_date = date(trade_date);
    CalendarDay {
        trade_date,
        day_window: window(start, end),
        state: session_id.map_or_else(
            || CalendarDayState::Closed {
                reason: Some("weekend".to_owned()),
            },
            |session_id| CalendarDayState::Open {
                session_id: session_id.to_owned(),
            },
        ),
        verification_status: if matches!(
            trade_date.weekday(),
            chrono::Weekday::Sat | chrono::Weekday::Sun
        ) {
            CalendarVerificationStatus::ModeledWeekend
        } else {
            CalendarVerificationStatus::Provisional
        },
    }
}

fn cash_instance(
    session_id: &str,
    suffix: &str,
    trade_date: &str,
    phase: SessionPhase,
    start: &str,
    end: &str,
) -> SessionInstance {
    SessionInstance {
        session_id: session_id.to_owned(),
        session_instance_id: format!("{session_id}-{suffix}"),
        trade_date: date(trade_date),
        phase,
        window: window(start, end),
    }
}

fn source_metadata() -> CalendarSourceMetadata {
    CalendarSourceMetadata {
        source_name: "NYSE Holidays & Trading Hours".to_owned(),
        source_reference: "https://www.nyse.com/trade/hours-calendars".to_owned(),
        generator_package: "pandas_market_calendars".to_owned(),
        generator_version: "5.4.0".to_owned(),
        valid_from: date("1990-01-01"),
        valid_to: date("2031-12-31"),
        official_verified_from: date("2026-01-01"),
        official_verified_through: date("2028-12-31"),
        verification_status: "verified_through_official_fixture".to_owned(),
        artifact_checksum: "a".repeat(64),
        pinned_input_sha256: "b".repeat(64),
        generated_at: "2026-09-04T00:00:00Z".to_owned(),
        source_git_commit: "d".repeat(40),
        compiler_config_sha256: "c".repeat(64),
        next_review_at: date("2026-12-15"),
    }
}

fn spring_cash_snapshot() -> TrustedCalendarSnapshot {
    let monday = "XNYS:2025-03-10";
    TrustedCalendarSnapshot {
        source_id: "XNYS".to_owned(),
        version: "nyse.v1".to_owned(),
        source_metadata: source_metadata(),
        coverage_start: date("2025-03-08"),
        coverage_end_exclusive: date("2025-03-11"),
        days: vec![
            cash_day(
                "2025-03-08",
                "2025-03-08T05:00:00Z",
                "2025-03-09T05:00:00Z",
                None,
            ),
            cash_day(
                "2025-03-09",
                "2025-03-09T05:00:00Z",
                "2025-03-10T04:00:00Z",
                None,
            ),
            cash_day(
                "2025-03-10",
                "2025-03-10T04:00:00Z",
                "2025-03-11T04:00:00Z",
                Some(monday),
            ),
        ],
        instances: vec![
            cash_instance(
                monday,
                "pre",
                "2025-03-10",
                SessionPhase::Pre,
                "2025-03-10T08:00:00Z",
                "2025-03-10T13:30:00Z",
            ),
            cash_instance(
                monday,
                "regular",
                "2025-03-10",
                SessionPhase::Regular,
                "2025-03-10T13:30:00Z",
                "2025-03-10T20:00:00Z",
            ),
            cash_instance(
                monday,
                "after",
                "2025-03-10",
                SessionPhase::After,
                "2025-03-10T20:00:00Z",
                "2025-03-11T00:00:00Z",
            ),
        ],
    }
}

fn fall_cash_snapshot() -> TrustedCalendarSnapshot {
    let monday = "XNYS:2025-11-03";
    TrustedCalendarSnapshot {
        source_id: "XNYS".to_owned(),
        version: "nyse.v1".to_owned(),
        source_metadata: source_metadata(),
        coverage_start: date("2025-11-01"),
        coverage_end_exclusive: date("2025-11-04"),
        days: vec![
            cash_day(
                "2025-11-01",
                "2025-11-01T04:00:00Z",
                "2025-11-02T04:00:00Z",
                None,
            ),
            cash_day(
                "2025-11-02",
                "2025-11-02T04:00:00Z",
                "2025-11-03T05:00:00Z",
                None,
            ),
            cash_day(
                "2025-11-03",
                "2025-11-03T05:00:00Z",
                "2025-11-04T05:00:00Z",
                Some(monday),
            ),
        ],
        instances: vec![
            cash_instance(
                monday,
                "pre",
                "2025-11-03",
                SessionPhase::Pre,
                "2025-11-03T09:00:00Z",
                "2025-11-03T14:30:00Z",
            ),
            cash_instance(
                monday,
                "regular",
                "2025-11-03",
                SessionPhase::Regular,
                "2025-11-03T14:30:00Z",
                "2025-11-03T21:00:00Z",
            ),
            cash_instance(
                monday,
                "after",
                "2025-11-03",
                SessionPhase::After,
                "2025-11-03T21:00:00Z",
                "2025-11-04T01:00:00Z",
            ),
        ],
    }
}

fn signed_fixture(
    cash: &TrustedCalendarSnapshot,
    over_window: UtcWindow,
    issue: u64,
    expiry: u64,
    revoked_at: Option<u64>,
    routes: Vec<OvernightRouteV1>,
) -> super::test_support::SyntheticOverlayArtifact {
    let index = cash.days.len() - 1;
    let day = &cash.days[index];
    let CalendarDayState::Open { session_id } = &day.state else {
        panic!("last cash day is open");
    };
    signed_overlay_for_tests(
        cash,
        CASH_INPUT_SHA256,
        Provider::Futu,
        OvernightRouteFeedV1::Futu,
        routes,
        vec![AssetClassV1::Stock, AssetClassV1::Etf],
        vec!["QQQ".to_owned(), "SPY".to_owned()],
        vec![SyntheticOvernightWindow {
            trade_date: day.trade_date,
            session_id: session_id.clone(),
            session_instance_id: format!("{session_id}-over"),
            window: over_window,
        }],
        issue,
        expiry,
        revoked_at,
    )
}

fn futu_only_route() -> OvernightRouteV1 {
    OvernightRouteV1 {
        provider: Provider::Futu,
        feed: OvernightRouteFeedV1::Futu,
    }
}

fn all_routes() -> Vec<OvernightRouteV1> {
    vec![
        OvernightRouteV1 {
            provider: Provider::Alpaca,
            feed: OvernightRouteFeedV1::Boats,
        },
        futu_only_route(),
    ]
}

fn install(
    cash: TrustedCalendarSnapshot,
    fixture: &super::test_support::SyntheticOverlayArtifact,
) -> LoadedMarketCalendar {
    let input_sha256 = super::sha256(&fixture.bytes);
    LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash, CASH_INPUT_SHA256)
        .with_signed_overnight_overlay(&fixture.bytes, &input_sha256, fixture.verifier.clone())
}

fn select(
    calendar: &LoadedMarketCalendar,
    request_window: UtcWindow,
    now: u64,
) -> super::CalendarSnapshotSelection {
    calendar.snapshot_for_instrument_at(AssetClassV1::Etf, "SPY", request_window, now)
}

fn assert_cash_projection(snapshot: &TrustedCalendarSnapshot, raw: &TrustedCalendarSnapshot) {
    assert_eq!(snapshot.source_id, raw.source_id);
    assert_eq!(snapshot.source_metadata, raw.source_metadata);
    assert_eq!(snapshot.coverage_start, raw.coverage_start);
    assert_eq!(snapshot.coverage_end_exclusive, raw.coverage_end_exclusive);
    assert_eq!(snapshot.days, raw.days);
    assert_eq!(snapshot.instances, raw.instances);
    assert!(snapshot.version.starts_with("cash.v1."));
    assert_ne!(snapshot.version, raw.version);
}

#[test]
fn signed_overlay_adds_only_explicit_spring_dst_window_and_stable_routes() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let fixture = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_742_500_000,
        None,
        all_routes(),
    );
    let loaded = install(cash, &fixture);
    let selection = select(&loaded, over_window, 1_741_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::Applied);
    assert!(selection.snapshot.version.starts_with("composite.v1."));
    assert_eq!(selection.components.cash.version, "nyse.v1");
    assert_eq!(selection.components.cash.input_sha256, CASH_INPUT_SHA256);
    assert_eq!(
        selection
            .snapshot
            .instances
            .iter()
            .filter(|instance| instance.phase == SessionPhase::Over)
            .map(|instance| instance.window)
            .collect::<Vec<_>>(),
        vec![over_window]
    );
    assert_eq!(selection.allowed_overnight_routes, all_routes());
    assert_eq!(
        selection.components.overnight_input_sha256,
        Some(super::sha256(&fixture.bytes))
    );
    assert_eq!(
        selection
            .components
            .overnight
            .as_ref()
            .unwrap()
            .source_provider,
        Provider::Futu
    );
    let futu = SourceCapability {
        provider: Provider::Futu,
        feed: None,
        phase: Some(SessionPhase::Over),
        interval: DataInterval::Minute(1),
        historical: true,
        realtime: false,
    };
    let boats = SourceCapability {
        provider: Provider::Alpaca,
        feed: Some(AlpacaFeed::Boats),
        ..futu
    };
    assert!(selection.allowed_overnight_routes[0].matches_source_capability(&boats));
    assert!(selection.allowed_overnight_routes[1].matches_source_capability(&futu));
}

#[test]
fn signed_overlay_uses_fall_dst_25_hour_cash_boundary_without_handwritten_timezone() {
    let cash = fall_cash_snapshot();
    let over_window = window("2025-11-03T01:00:00Z", "2025-11-03T09:00:00Z");
    let fixture = signed_fixture(
        &cash,
        over_window,
        1_761_500_000,
        1_762_500_000,
        None,
        vec![futu_only_route()],
    );
    let loaded = install(cash, &fixture);
    let selection = select(&loaded, over_window, 1_761_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::Applied);
    assert_eq!(selection.allowed_overnight_routes, vec![futu_only_route()]);
    assert_eq!(
        selection
            .snapshot
            .instances
            .iter()
            .find(|instance| instance.phase == SessionPhase::Over)
            .unwrap()
            .window,
        over_window
    );
}

#[test]
fn renewed_signature_lease_does_not_change_calendar_content_version() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let first = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_742_500_000,
        None,
        all_routes(),
    );
    let renewed = signed_fixture(
        &cash,
        over_window,
        1_741_500_100,
        1_742_500_100,
        None,
        all_routes(),
    );
    assert_ne!(super::sha256(&first.bytes), super::sha256(&renewed.bytes));
    let first = select(&install(cash.clone(), &first), over_window, 1_741_600_000);
    let renewed = select(&install(cash, &renewed), over_window, 1_741_600_000);
    assert_eq!(first.status, OvernightOverlayStatus::Applied);
    assert_eq!(renewed.status, OvernightOverlayStatus::Applied);
    assert_eq!(first.snapshot.version, renewed.snapshot.version);
}

#[test]
fn cash_only_default_never_invents_over_or_routes() {
    let cash = spring_cash_snapshot();
    let loaded =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256);
    let request = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let selection = select(&loaded, request, 1_741_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::NoOverlay);
    assert_cash_projection(selection.snapshot.as_ref(), &cash);
    assert_eq!(
        selection.snapshot.as_ref(),
        loaded.cash_snapshot_for_selection().as_ref()
    );
    assert_eq!(selection.components.cash.version, cash.version);
    assert!(selection.allowed_overnight_routes.is_empty());
    assert!(
        selection
            .snapshot
            .instances
            .iter()
            .all(|instance| instance.phase != SessionPhase::Over)
    );
}

#[test]
fn stable_cash_version_binds_source_identity_and_exact_input_sha_for_all_selections() {
    let cash = spring_cash_snapshot();
    let loaded =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256);
    let first = loaded.cash_snapshot_for_selection();
    let repeated = loaded.cash_snapshot_for_selection();
    assert_eq!(first.version, repeated.version);
    assert_cash_projection(first.as_ref(), &cash);

    let request = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let no_overlay = select(&loaded, request, 1_741_600_000);
    assert_eq!(no_overlay.snapshot.version, first.version);
    assert_eq!(no_overlay.components.cash.version, cash.version);

    let other_input =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), "c".repeat(64))
            .cash_snapshot_for_selection();
    assert_ne!(first.version, other_input.version);

    let mut other_source_version = cash.clone();
    other_source_version.version = "nyse.v2".to_owned();
    let other_version = LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(
        other_source_version,
        CASH_INPUT_SHA256,
    )
    .cash_snapshot_for_selection();
    assert_ne!(first.version, other_version.version);

    let mut other_source_id = cash;
    other_source_id.source_id = "XNYS-ALT".to_owned();
    let other_source =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(other_source_id, CASH_INPUT_SHA256)
            .cash_snapshot_for_selection();
    assert_ne!(first.version, other_source.version);
}

#[test]
fn one_signed_route_does_not_admit_the_other_provider() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let fixture = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_742_500_000,
        None,
        vec![futu_only_route()],
    );
    let selection = select(&install(cash, &fixture), over_window, 1_741_600_000);
    let futu = SourceCapability {
        provider: Provider::Futu,
        feed: None,
        phase: Some(SessionPhase::Over),
        interval: DataInterval::Minute(1),
        historical: true,
        realtime: false,
    };
    let boats = SourceCapability {
        provider: Provider::Alpaca,
        feed: Some(AlpacaFeed::Boats),
        ..futu
    };
    assert_eq!(selection.status, OvernightOverlayStatus::Applied);
    assert_eq!(selection.allowed_overnight_routes, vec![futu_only_route()]);
    assert!(selection.allowed_overnight_routes[0].matches_source_capability(&futu));
    assert!(!selection.allowed_overnight_routes[0].matches_source_capability(&boats));
}

#[test]
fn overlay_fails_closed_for_expiry_revocation_ineligible_symbol_or_asset() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let expired = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_741_600_000,
        None,
        vec![futu_only_route()],
    );
    let selection = select(&install(cash.clone(), &expired), over_window, 1_741_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::Expired);
    assert!(!selection.snapshot.version.starts_with("composite.v1."));

    let revoked = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_742_500_000,
        Some(1_741_600_000),
        vec![futu_only_route()],
    );
    assert_eq!(
        select(&install(cash.clone(), &revoked), over_window, 1_741_600_000).status,
        OvernightOverlayStatus::Revoked
    );

    let ineligible = select(&install(cash.clone(), &expired), over_window, 1_741_550_000);
    assert_eq!(ineligible.status, OvernightOverlayStatus::Applied);
    let not_eligible =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256)
            .with_signed_overnight_overlay(
                &expired.bytes,
                &super::sha256(&expired.bytes),
                expired.verifier.clone(),
            )
            .snapshot_for_instrument_at(AssetClassV1::Etf, "DIA", over_window, 1_741_550_000);
    assert_eq!(not_eligible.status, OvernightOverlayStatus::NotEligible);
    assert_cash_projection(not_eligible.snapshot.as_ref(), &cash);

    let future = signed_fixture(
        &cash,
        over_window,
        1_742_000_000,
        1_742_500_000,
        None,
        vec![futu_only_route()],
    );
    assert_eq!(
        select(&install(cash, &future), over_window, 1_741_600_000).status,
        OvernightOverlayStatus::NotYetValid
    );
}

#[test]
fn revocation_timestamp_after_signed_expiry_rejects_the_overlay_artifact() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let fixture = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_741_600_000,
        Some(1_741_600_001),
        vec![futu_only_route()],
    );
    let selection = select(&install(cash.clone(), &fixture), over_window, 1_741_550_000);
    assert_eq!(selection.status, OvernightOverlayStatus::InvalidArtifact);
    assert_cash_projection(selection.snapshot.as_ref(), &cash);
    assert!(selection.allowed_overnight_routes.is_empty());
}

#[test]
fn dynamic_trust_rejection_and_exact_cash_binding_mismatch_return_cash_only() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let fixture = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_742_500_000,
        None,
        vec![futu_only_route()],
    );
    let loaded = install(cash.clone(), &fixture);
    fixture.verifier.set_revoked(true);
    let revoked_by_verifier = select(&loaded, over_window, 1_741_600_000);
    assert_eq!(
        revoked_by_verifier.status,
        OvernightOverlayStatus::TrustRejected
    );
    assert_cash_projection(revoked_by_verifier.snapshot.as_ref(), &cash);

    let mismatched = signed_overlay_for_tests(
        &cash,
        &"c".repeat(64),
        Provider::Futu,
        OvernightRouteFeedV1::Futu,
        vec![futu_only_route()],
        vec![AssetClassV1::Stock, AssetClassV1::Etf],
        vec!["SPY".to_owned()],
        vec![SyntheticOvernightWindow {
            trade_date: date("2025-03-10"),
            session_id: "XNYS:2025-03-10".to_owned(),
            session_instance_id: "XNYS:2025-03-10-over".to_owned(),
            window: over_window,
        }],
        1_741_500_000,
        1_742_500_000,
        None,
    );
    let mismatch = select(
        &install(cash.clone(), &mismatched),
        over_window,
        1_741_600_000,
    );
    assert_eq!(mismatch.status, OvernightOverlayStatus::CashBindingMismatch);
    assert_cash_projection(mismatch.snapshot.as_ref(), &cash);
}

#[test]
fn signature_tamper_raw_pin_mismatch_and_out_of_coverage_never_add_windows() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let fixture = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_742_500_000,
        None,
        vec![futu_only_route()],
    );
    // A request inside cash-calendar coverage retains the complete trusted
    // overlay for historical ranking, even when it does not intersect an OVER
    // window. NoWindowCoverage is reserved for requests outside the full cash
    // snapshot coverage.
    let ranking_context = select(
        &install(cash.clone(), &fixture),
        window("2025-03-11T01:00:00Z", "2025-03-11T02:00:00Z"),
        1_741_600_000,
    );
    assert_eq!(ranking_context.status, OvernightOverlayStatus::Applied);
    assert_eq!(
        ranking_context
            .snapshot
            .instances
            .iter()
            .find(|instance| instance.phase == SessionPhase::Over)
            .map(|instance| instance.window),
        Some(over_window)
    );
    assert_eq!(
        ranking_context.allowed_overnight_routes,
        vec![futu_only_route()]
    );

    let outside = select(
        &install(cash.clone(), &fixture),
        window("2025-03-11T04:00:00Z", "2025-03-11T04:00:01Z"),
        1_741_600_000,
    );
    assert_eq!(outside.status, OvernightOverlayStatus::NoWindowCoverage);
    assert_cash_projection(outside.snapshot.as_ref(), &cash);
    assert!(outside.allowed_overnight_routes.is_empty());

    let wrong_pin =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256)
            .with_signed_overnight_overlay(
                &fixture.bytes,
                &"a".repeat(64),
                fixture.verifier.clone(),
            );
    assert_eq!(
        select(&wrong_pin, over_window, 1_741_600_000).status,
        OvernightOverlayStatus::RawShaMismatch
    );

    let mut tampered: serde_json::Value =
        serde_json::from_slice(&fixture.bytes).expect("synthetic artifact JSON");
    tampered["signature_base64"] =
        serde_json::Value::String(format!("synthetic-hmac-sha256-v1:{}", "0".repeat(64)));
    let tampered = serde_json::to_vec(&tampered).expect("tampered JSON serializes");
    let tampered_calendar =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256)
            .with_signed_overnight_overlay(
                &tampered,
                &super::sha256(&tampered),
                fixture.verifier.clone(),
            );
    let selection = select(&tampered_calendar, over_window, 1_741_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::TrustRejected);
    assert_cash_projection(selection.snapshot.as_ref(), &cash);
    assert_eq!(
        selection.components.overnight_input_sha256,
        Some(super::sha256(&tampered))
    );
}

#[test]
fn synthetic_hmac_authentication_rejects_tampered_claims_and_untrusted_identity() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let fixture = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_742_500_000,
        None,
        vec![futu_only_route()],
    );

    let mut changed_claims: serde_json::Value =
        serde_json::from_slice(&fixture.bytes).expect("synthetic artifact JSON");
    changed_claims["claims"]["content"]["calendar_version"] =
        serde_json::Value::String("synthetic.v2".to_owned());
    let changed_claims = serde_json::to_vec(&changed_claims).expect("tampered claims serialize");
    let calendar =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256)
            .with_signed_overnight_overlay(
                &changed_claims,
                &super::sha256(&changed_claims),
                fixture.verifier.clone(),
            );
    let selection = select(&calendar, over_window, 1_741_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::TrustRejected);
    assert!(selection.allowed_overnight_routes.is_empty());

    for (issuer, key_id) in [
        ("untrusted-test-issuer", "synthetic-test-key"),
        ("synthetic-test-issuer", "untrusted-test-key"),
    ] {
        let wrong_identity = resign_with_identity_for_tests(&fixture.bytes, issuer, key_id);
        let calendar = LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(
            cash.clone(),
            CASH_INPUT_SHA256,
        )
        .with_signed_overnight_overlay(
            &wrong_identity,
            &super::sha256(&wrong_identity),
            fixture.verifier.clone(),
        );
        let selection = select(&calendar, over_window, 1_741_600_000);
        assert_eq!(selection.status, OvernightOverlayStatus::TrustRejected);
        assert!(selection.allowed_overnight_routes.is_empty());
    }
}

#[test]
fn overlay_resource_limits_reject_oversized_bytes_windows_and_symbols() {
    let cash = spring_cash_snapshot();
    let over_window = window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z");
    let day = cash.days.last().expect("open cash day");
    let CalendarDayState::Open { session_id } = &day.state else {
        panic!("last cash day is open");
    };
    let make_window = |index: usize| SyntheticOvernightWindow {
        trade_date: day.trade_date,
        session_id: session_id.clone(),
        session_instance_id: format!("{session_id}-over-{index:05}"),
        window: over_window,
    };

    let too_many_windows = signed_overlay_for_tests(
        &cash,
        CASH_INPUT_SHA256,
        Provider::Futu,
        OvernightRouteFeedV1::Futu,
        vec![futu_only_route()],
        vec![AssetClassV1::Etf],
        vec!["SPY".to_owned()],
        (0..=super::MAX_OVERLAY_WINDOWS).map(make_window).collect(),
        1_741_500_000,
        1_742_500_000,
        None,
    );
    assert!(too_many_windows.bytes.len() <= super::MAX_OVERLAY_BYTES);
    let window_calendar =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256)
            .with_signed_overnight_overlay(
                &too_many_windows.bytes,
                &super::sha256(&too_many_windows.bytes),
                too_many_windows.verifier.clone(),
            );
    let selection = select(&window_calendar, over_window, 1_741_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::InvalidArtifact);
    assert!(selection.allowed_overnight_routes.is_empty());

    let mut symbols = (0..super::MAX_ELIGIBLE_SYMBOLS)
        .map(|index| format!("S{index:05}"))
        .collect::<Vec<_>>();
    symbols.push("SPY".to_owned());
    let too_many_symbols = signed_overlay_for_tests(
        &cash,
        CASH_INPUT_SHA256,
        Provider::Futu,
        OvernightRouteFeedV1::Futu,
        vec![futu_only_route()],
        vec![AssetClassV1::Etf],
        symbols,
        vec![make_window(0)],
        1_741_500_000,
        1_742_500_000,
        None,
    );
    let symbol_calendar =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256)
            .with_signed_overnight_overlay(
                &too_many_symbols.bytes,
                &super::sha256(&too_many_symbols.bytes),
                too_many_symbols.verifier.clone(),
            );
    let selection = select(&symbol_calendar, over_window, 1_741_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::InvalidArtifact);
    assert!(selection.allowed_overnight_routes.is_empty());

    let valid_fixture = signed_fixture(
        &cash,
        over_window,
        1_741_500_000,
        1_742_500_000,
        None,
        vec![futu_only_route()],
    );
    let oversized_bytes = vec![b'x'; super::MAX_OVERLAY_BYTES + 1];
    let oversized_calendar =
        LoadedMarketCalendar::synthetic_cash_snapshot_for_tests(cash.clone(), CASH_INPUT_SHA256)
            .with_signed_overnight_overlay(
                &oversized_bytes,
                &super::sha256(&oversized_bytes),
                valid_fixture.verifier.clone(),
            );
    let selection = select(&oversized_calendar, over_window, 1_741_600_000);
    assert_eq!(selection.status, OvernightOverlayStatus::InvalidArtifact);
    assert!(selection.allowed_overnight_routes.is_empty());
}

#[test]
fn signed_overlay_requires_a_cash_open_trade_date_and_contiguous_ny_bounds() {
    let cash = spring_cash_snapshot();
    let wrong_day = signed_overlay_for_tests(
        &cash,
        CASH_INPUT_SHA256,
        Provider::Futu,
        OvernightRouteFeedV1::Futu,
        vec![futu_only_route()],
        vec![AssetClassV1::Etf],
        vec!["SPY".to_owned()],
        vec![SyntheticOvernightWindow {
            trade_date: date("2025-03-09"),
            session_id: "XNYS:2025-03-10".to_owned(),
            session_instance_id: "bad-weekend-over".to_owned(),
            window: window("2025-03-09T20:00:00Z", "2025-03-10T04:00:00Z"),
        }],
        1_741_500_000,
        1_742_500_000,
        None,
    );
    let selected = select(
        &install(cash, &wrong_day),
        window("2025-03-10T00:00:00Z", "2025-03-10T08:00:00Z"),
        1_741_600_000,
    );
    assert_eq!(selected.status, OvernightOverlayStatus::InvalidArtifact);
    assert!(
        selected
            .snapshot
            .instances
            .iter()
            .all(|instance| instance.phase != SessionPhase::Over)
    );
}
