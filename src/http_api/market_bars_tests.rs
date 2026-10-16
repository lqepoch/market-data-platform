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
    MAX_QUERY_BYTES, MarketBarsHttpError, hash_cache_rights_profile, parse_bars_request,
    parse_query, parse_sessions_query, parse_utc_window_endpoint, principal_may_request_research,
    read_gateway_delegation, valid_cache_scope_id,
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

fn market_bars_test_runtime() -> crate::market_query::providers::MarketBarsProviderRuntime {
    crate::market_query::providers::MarketBarsProviderRuntime::from_environment()
        .expect("test provider runtime has a valid cursor signer configuration")
}

fn market_bars_test_rights_profile_sha256() -> String {
    let principal = test_support::terminal_principal();
    hash_cache_rights_profile("", principal.principal_scope_sha256())
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
fn sessions_instrument_selector_requires_symbol_and_asset_class_together() {
    let base = "/v1/market/sessions?start=2026-01-05T01:00:00Z&end=2026-01-05T02:00:00Z";
    let omitted = parse_sessions_query(&base.parse().unwrap()).expect("cash-only request");
    assert!(omitted.symbol.is_none());
    assert!(omitted.asset_class.is_none());

    let symbol_only = format!("{base}&symbol=SPY").parse().unwrap();
    assert!(matches!(
        parse_sessions_query(&symbol_only),
        Err(MarketBarsHttpError::InvalidRequest)
    ));

    let class_only = format!("{base}&asset_class=etf").parse().unwrap();
    assert!(matches!(
        parse_sessions_query(&class_only),
        Err(MarketBarsHttpError::InvalidRequest)
    ));

    let paired = format!("{base}&symbol=SPY&asset_class=etf")
        .parse()
        .unwrap();
    let paired = parse_sessions_query(&paired).expect("instrument-scoped request");
    assert_eq!(paired.symbol.as_deref(), Some("SPY"));
    assert_eq!(
        paired.asset_class,
        Some(market_contracts::AssetClassV1::Etf)
    );
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
fn cache_rights_profile_hash_partitions_operator_profiles_and_verified_principals() {
    let terminal = test_support::terminal_principal();
    let research = test_support::research_principal();

    let terminal_hash =
        hash_cache_rights_profile("alpaca-primary-v1", terminal.principal_scope_sha256());
    let same_scope_hash =
        hash_cache_rights_profile("alpaca-primary-v1", terminal.principal_scope_sha256());
    let other_profile_hash =
        hash_cache_rights_profile("alpaca-secondary-v1", terminal.principal_scope_sha256());
    let other_principal_hash =
        hash_cache_rights_profile("alpaca-primary-v1", research.principal_scope_sha256());

    assert_eq!(terminal_hash, same_scope_hash);
    assert_ne!(terminal_hash, other_profile_hash);
    assert_ne!(terminal_hash, other_principal_hash);
    assert_eq!(terminal_hash.len(), 64);
    assert!(
        terminal_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );

    let unprofiled_terminal_hash = hash_cache_rights_profile("", terminal.principal_scope_sha256());
    let unprofiled_research_hash = hash_cache_rights_profile("", research.principal_scope_sha256());
    assert_ne!(unprofiled_terminal_hash, unprofiled_research_hash);
    assert_ne!(unprofiled_terminal_hash, terminal.principal_scope_sha256());
}

#[test]
fn unknown_adjustment_semantics_are_not_mislabeled_as_resolution_gaps() {
    use crate::market_query::providers::ProviderQueryError;

    for error in [
        ProviderQueryError::UnsupportedAdjustment,
        ProviderQueryError::AdjustmentSemanticsUnverified,
    ] {
        assert_eq!(
            super::response::provider_failure_gap_reason(&error),
            CoverageGapReasonV1::Unknown
        );
        assert_ne!(
            super::response::provider_failure_gap_reason(&error),
            CoverageGapReasonV1::UnsupportedResolution
        );
    }
}

#[test]
fn runtime_summary_reports_attemptability_without_claiming_provider_authority() {
    let capabilities = [crate::session_router::SourceCapability {
        provider: crate::session_router::Provider::Alpaca,
        feed: Some(crate::session_router::AlpacaFeed::Sip),
        phase: Some(crate::session_router::SessionPhase::Regular),
        interval: crate::session_router::DataInterval::Minute(1),
        historical: true,
        realtime: false,
    }];
    assert!(
        capabilities
            .iter()
            .all(|capability| capability.provider != crate::session_router::Provider::Futu)
    );
    let summary = super::runtime_summary(&capabilities, false, true, true);

    assert!(summary.runtime_enabled);
    assert!(summary.alpaca_historical_attemptable);
    assert!(!summary.futu_historical_attemptable);
    assert!(!summary.yfinance_core_bars_attemptable);
    assert!(summary.cache_configured);
    assert!(summary.operator_scope_configured);
    assert_eq!(
        summary.cache_worker_platform_supported,
        cfg!(target_os = "linux")
    );
    assert_eq!(
        summary.provisional_cache_prerequisites_configured,
        cfg!(target_os = "linux")
    );

    // Futu's configured history attempt is separate from generic planner capabilities.
    let configured_futu = super::runtime_summary(&capabilities, true, true, true);
    assert!(configured_futu.futu_historical_attemptable);
    assert!(configured_futu.alpaca_historical_attemptable);
    assert!(!configured_futu.yfinance_core_bars_attemptable);

    let unavailable = super::runtime_summary(&[], false, false, false);
    assert!(!unavailable.alpaca_historical_attemptable);
    assert!(!unavailable.futu_historical_attemptable);
    assert!(!unavailable.yfinance_core_bars_attemptable);
    assert!(!unavailable.cache_configured);
    assert!(!unavailable.operator_scope_configured);
    assert!(!unavailable.provisional_cache_prerequisites_configured);
}

#[test]
fn canonical_cache_hit_page_is_revalidated_without_losing_rows_or_provisional_status() {
    use chrono::{Duration, NaiveDate};
    use market_contracts::{
        AdjustmentV1, BarResolutionV1, BarRowV1, BarSourceProvenanceV1, DecimalString,
        NumericEncodingV1, SessionScopeV1,
    };

    let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let day_start = trade_date.and_hms_opt(5, 0, 0).unwrap().and_utc();
    let day_end = day_start + Duration::days(1);
    let session_open = day_start + Duration::hours(9) + Duration::minutes(30);
    let session_close = day_start + Duration::hours(16);
    let day_label = trade_date.format("%Y-%m-%d").to_string();
    let session_id = format!("XNYS:{day_label}");
    let calendar = crate::session_router::TrustedCalendarSnapshot {
        source_id: "XNYS".to_owned(),
        version: "nyse.v1".to_owned(),
        source_metadata: crate::session_router::CalendarSourceMetadata {
            source_name: "NYSE Holidays & Trading Hours".to_owned(),
            source_reference: "https://www.nyse.com/trade/hours-calendars".to_owned(),
            generator_package: "pandas_market_calendars".to_owned(),
            generator_version: "5.4.0".to_owned(),
            valid_from: NaiveDate::from_ymd_opt(1990, 1, 1).unwrap(),
            valid_to: NaiveDate::from_ymd_opt(2031, 12, 31).unwrap(),
            official_verified_from: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            official_verified_through: NaiveDate::from_ymd_opt(2028, 12, 31).unwrap(),
            verification_status: "verified_through_official_fixture".to_owned(),
            artifact_checksum: "a".repeat(64),
            pinned_input_sha256: "b".repeat(64),
            generated_at: "2026-09-04T00:00:00Z".to_owned(),
            source_git_commit: "d".repeat(40),
            compiler_config_sha256: "c".repeat(64),
            next_review_at: NaiveDate::from_ymd_opt(2026, 12, 15).unwrap(),
        },
        coverage_start: trade_date,
        coverage_end_exclusive: trade_date.succ_opt().unwrap(),
        days: vec![crate::session_router::CalendarDay {
            trade_date,
            day_window: crate::session_router::UtcWindow {
                start: day_start,
                end_exclusive: day_end,
            },
            state: crate::session_router::CalendarDayState::Open {
                session_id: session_id.clone(),
            },
            verification_status: crate::session_router::CalendarVerificationStatus::Verified,
        }],
        instances: vec![
            crate::session_router::SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: format!("{session_id}:pre"),
                trade_date,
                phase: crate::session_router::SessionPhase::Pre,
                window: crate::session_router::UtcWindow {
                    start: day_start + Duration::hours(4),
                    end_exclusive: session_open,
                },
            },
            crate::session_router::SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: format!("{session_id}:regular"),
                trade_date,
                phase: crate::session_router::SessionPhase::Regular,
                window: crate::session_router::UtcWindow {
                    start: session_open,
                    end_exclusive: session_close,
                },
            },
            crate::session_router::SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: format!("{session_id}:after"),
                trade_date,
                phase: crate::session_router::SessionPhase::After,
                window: crate::session_router::UtcWindow {
                    start: session_close,
                    end_exclusive: day_start + Duration::hours(20),
                },
            },
        ],
    };
    let request = market_contracts::BarsRequestV1 {
        symbol: "SPY".to_owned(),
        asset_class: market_contracts::AssetClassV1::Etf,
        start: ProtoTimestampV2::parse("2026-01-05T14:30:00Z").unwrap(),
        end: ProtoTimestampV2::parse("2026-01-05T14:32:00Z").unwrap(),
        resolution: BarResolutionV1::OneMinute,
        session_scope: SessionScopeV1::Regular,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::AlpacaOnly,
        allow_daily_downgrade: false,
        limit: 10,
        cursor: None,
    };
    let capabilities = [crate::session_router::SourceCapability {
        provider: crate::session_router::Provider::Alpaca,
        feed: Some(crate::session_router::AlpacaFeed::Sip),
        phase: Some(crate::session_router::SessionPhase::Regular),
        interval: crate::session_router::DataInterval::Minute(1),
        historical: true,
        realtime: false,
    }];
    let plan = crate::market_query::planning::plan_market_bars(
        &request,
        &ProtoTimestampV2::parse("2026-01-05T21:00:00Z").unwrap(),
        &capabilities,
        &calendar,
    )
    .unwrap();
    let provider_runtime = market_bars_test_runtime();
    let rights_profile_sha256 = market_bars_test_rights_profile_sha256();
    let selection = provider_runtime
        .select_page_at(
            &request,
            &plan,
            &rights_profile_sha256,
            super::unix_seconds_now(),
        )
        .unwrap();
    let row_start = request.start.clone();
    let row_end = ProtoTimestampV2::parse("2026-01-05T14:31:00Z").unwrap();
    let received_at = ProtoTimestampV2::parse("2026-01-05T14:32:01Z").unwrap();
    let raw_page = market_contracts::BarsResponseV1 {
        schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
        provider: "alpaca".to_owned(),
        feed: "unknown".to_owned(),
        entitlement_status: market_contracts::BarsEntitlementStatusV1::Unknown,
        requested_resolution: BarResolutionV1::OneMinute,
        effective_resolution: BarResolutionV1::OneMinute,
        adjustment: AdjustmentV1::Unadjusted,
        rows: vec![BarRowV1 {
            symbol: request.symbol.clone(),
            asset_class: request.asset_class,
            resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            bar_start_utc: row_start.clone(),
            bar_end_exclusive_utc: row_end.clone(),
            received_at_utc: received_at.clone(),
            available_at_utc: received_at.clone(),
            open: DecimalString::new("10.00").unwrap(),
            high: DecimalString::new("11.00").unwrap(),
            low: DecimalString::new("9.00").unwrap(),
            close: DecimalString::new("10.50").unwrap(),
            volume: DecimalString::new("100").unwrap(),
            provenance_id: "alpaca-page".to_owned(),
            session_instance_id: "provider-local".to_owned(),
            validation_state: market_contracts::BarsValidationStateV1::Provisional,
        }],
        coverage_status: BarsCoverageStatusV1::Complete,
        segments: vec![BarCoverageSegmentV1 {
            start_utc: row_start,
            end_exclusive_utc: row_end,
            coverage_status: BarsCoverageStatusV1::Complete,
            provenance_id: Some("alpaca-page".to_owned()),
            session_instance_id: "provider-local".to_owned(),
        }],
        gaps: Vec::new(),
        source_provenance: vec![BarSourceProvenanceV1 {
            provenance_id: "alpaca-page".to_owned(),
            provider: "alpaca".to_owned(),
            feed: "unknown".to_owned(),
            source: "Alpaca historical stock-bars API".to_owned(),
            entitlement_status: market_contracts::BarsEntitlementStatusV1::Unknown,
            numeric_encoding: NumericEncodingV1::DecimalToken,
        }],
        as_of: None,
        fetched_at: received_at,
        verified_at: None,
        cache_status: market_contracts::BarsCacheStatusV1::Miss,
        market_calendar_version: None,
        continuation_cursor: None,
        truncated: false,
    };
    let canonical = super::response::response_for_cache_page(
        &request,
        &plan,
        &selection,
        raw_page,
        market_contracts::BarsCacheStatusV1::Miss,
    )
    .unwrap();
    let mut cache_hit_page = canonical;
    cache_hit_page.cache_status = market_contracts::BarsCacheStatusV1::Hit;

    // The real hit path replays A04's canonical Core response through this same run mapper.
    let replayed = super::response::response_from_page(
        &request,
        &plan,
        Some(&selection),
        cache_hit_page,
        market_contracts::BarsCacheStatusV1::Hit,
    )
    .unwrap();

    request.validate_response(&replayed).unwrap();
    assert_eq!(replayed.rows.len(), 1);
    assert_eq!(
        replayed.rows[0].session_instance_id,
        format!("{session_id}:regular")
    );
    assert_eq!(replayed.feed, "unknown");
    assert_eq!(
        replayed.entitlement_status,
        market_contracts::BarsEntitlementStatusV1::Unknown
    );
    assert_eq!(replayed.coverage_status, BarsCoverageStatusV1::Partial);
    assert_eq!(
        replayed.cache_status,
        market_contracts::BarsCacheStatusV1::Hit
    );
    assert!(replayed.verified_at.is_none());
}

#[test]
fn failed_and_exhausted_futu_run_advances_to_the_next_planned_provider_run() {
    use chrono::{Duration, NaiveDate};
    use market_contracts::{
        AdjustmentV1, BarResolutionV1, BarRowV1, BarSourceProvenanceV1, DecimalString,
        NumericEncodingV1, SessionScopeV1,
    };

    let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let day_start = trade_date.and_hms_opt(5, 0, 0).unwrap().and_utc();
    let day_end = day_start + Duration::days(1);
    let session_open = day_start + Duration::hours(9) + Duration::minutes(30);
    let session_close = day_start + Duration::hours(16);
    let day_label = trade_date.format("%Y-%m-%d").to_string();
    let session_id = format!("XNYS:{day_label}");
    let calendar = crate::session_router::TrustedCalendarSnapshot {
        source_id: "XNYS".to_owned(),
        version: "nyse.v1".to_owned(),
        source_metadata: crate::session_router::CalendarSourceMetadata {
            source_name: "NYSE Holidays & Trading Hours".to_owned(),
            source_reference: "https://www.nyse.com/trade/hours-calendars".to_owned(),
            generator_package: "pandas_market_calendars".to_owned(),
            generator_version: "5.4.0".to_owned(),
            valid_from: NaiveDate::from_ymd_opt(1990, 1, 1).unwrap(),
            valid_to: NaiveDate::from_ymd_opt(2031, 12, 31).unwrap(),
            official_verified_from: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            official_verified_through: NaiveDate::from_ymd_opt(2028, 12, 31).unwrap(),
            verification_status: "verified_through_official_fixture".to_owned(),
            artifact_checksum: "a".repeat(64),
            pinned_input_sha256: "b".repeat(64),
            generated_at: "2026-09-04T00:00:00Z".to_owned(),
            source_git_commit: "d".repeat(40),
            compiler_config_sha256: "c".repeat(64),
            next_review_at: NaiveDate::from_ymd_opt(2026, 12, 15).unwrap(),
        },
        coverage_start: trade_date,
        coverage_end_exclusive: trade_date.succ_opt().unwrap(),
        days: vec![crate::session_router::CalendarDay {
            trade_date,
            day_window: crate::session_router::UtcWindow {
                start: day_start,
                end_exclusive: day_end,
            },
            state: crate::session_router::CalendarDayState::Open {
                session_id: session_id.clone(),
            },
            verification_status: crate::session_router::CalendarVerificationStatus::Verified,
        }],
        instances: vec![
            crate::session_router::SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: format!("{session_id}:pre"),
                trade_date,
                phase: crate::session_router::SessionPhase::Pre,
                window: crate::session_router::UtcWindow {
                    start: day_start + Duration::hours(4),
                    end_exclusive: session_open,
                },
            },
            crate::session_router::SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: format!("{session_id}:regular"),
                trade_date,
                phase: crate::session_router::SessionPhase::Regular,
                window: crate::session_router::UtcWindow {
                    start: session_open,
                    end_exclusive: session_close,
                },
            },
            crate::session_router::SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: format!("{session_id}:after"),
                trade_date,
                phase: crate::session_router::SessionPhase::After,
                window: crate::session_router::UtcWindow {
                    start: session_close,
                    end_exclusive: day_start + Duration::hours(20),
                },
            },
        ],
    };
    let request = market_contracts::BarsRequestV1 {
        symbol: "SPY".to_owned(),
        asset_class: market_contracts::AssetClassV1::Etf,
        start: ProtoTimestampV2::parse("2026-01-05T14:29:00Z").unwrap(),
        end: ProtoTimestampV2::parse("2026-01-05T21:01:00Z").unwrap(),
        resolution: BarResolutionV1::OneMinute,
        session_scope: SessionScopeV1::All,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::Auto,
        allow_daily_downgrade: false,
        limit: 10,
        cursor: None,
    };
    // Synthetic planner capabilities exercise pagination only; they do not assert live Futu
    // admission, entitlement, reachability, or source verification.
    let capabilities = [
        crate::session_router::SourceCapability {
            provider: crate::session_router::Provider::Alpaca,
            feed: Some(crate::session_router::AlpacaFeed::Sip),
            phase: Some(crate::session_router::SessionPhase::Pre),
            interval: crate::session_router::DataInterval::Minute(1),
            historical: true,
            realtime: false,
        },
        crate::session_router::SourceCapability {
            provider: crate::session_router::Provider::Futu,
            feed: None,
            phase: Some(crate::session_router::SessionPhase::Regular),
            interval: crate::session_router::DataInterval::Minute(1),
            historical: true,
            realtime: false,
        },
        crate::session_router::SourceCapability {
            provider: crate::session_router::Provider::Futu,
            feed: None,
            phase: Some(crate::session_router::SessionPhase::After),
            interval: crate::session_router::DataInterval::Minute(1),
            historical: true,
            realtime: false,
        },
    ];
    // Keep source ranking fixed at the post-close window; the selector's injected clock controls
    // only cursor issue/expiry time, so the historical test plan does not inherit today's ranks.
    let as_of = ProtoTimestampV2::parse("2026-01-05T21:00:00Z").unwrap();
    let now_unix_seconds = super::unix_seconds_now();
    let plan =
        crate::market_query::planning::plan_market_bars(&request, &as_of, &capabilities, &calendar)
            .unwrap();
    let provider_runtime = market_bars_test_runtime();
    let rights_profile_sha256 = market_bars_test_rights_profile_sha256();
    assert!(matches!(
        provider_runtime.select_page_at(&request, &plan, &rights_profile_sha256, now_unix_seconds,),
        Err(crate::market_query::providers::ProviderQueryError::ProviderCapabilityUnavailable)
    ));
    // This synthetic fixture only exercises cache replay mechanics. It does not grant Futu
    // admission, create a provider claim, or make a network request.
    let selection = provider_runtime
        .select_page_at_synthetic_for_test(
            &request,
            &plan,
            &rights_profile_sha256,
            now_unix_seconds,
        )
        .unwrap();
    assert_eq!(plan.provider_groups.len(), 2);
    assert_eq!(
        selection.provider_group().source,
        crate::session_router::PlannedSource::Futu
    );

    // A failed NEW/BUFFER attempt keeps its own coverage gap while issuing the signed cursor for
    // the remaining OLD run. This is continuation through the existing plan, not source fallback.
    let failed_futu = super::response::response_for_provider_failure(
        &request,
        &plan,
        Some(&selection),
        crate::market_query::providers::ProviderQueryError::ProviderUnavailable,
        market_contracts::BarsCacheStatusV1::Unavailable,
    )
    .unwrap();
    request.validate_response(&failed_futu).unwrap();
    assert!(failed_futu.rows.is_empty());
    assert_eq!(failed_futu.coverage_status, BarsCoverageStatusV1::Partial);
    assert!(failed_futu.truncated);
    assert!(
        failed_futu
            .gaps
            .iter()
            .any(|gap| { gap.reason_code == CoverageGapReasonV1::ProviderUnavailable })
    );
    let failed_futu_cursor = failed_futu
        .continuation_cursor
        .as_ref()
        .expect("failed provider run continues to the next planned source");

    let mut continued_request = request.clone();
    continued_request.cursor = Some(failed_futu_cursor.clone());
    let next_selection = provider_runtime
        .select_page_at_synthetic_for_test(
            &continued_request,
            &plan,
            &rights_profile_sha256,
            now_unix_seconds,
        )
        .expect("signed failure cursor selects the next planned run");
    assert_eq!(
        next_selection.provider_group().source,
        crate::session_router::PlannedSource::Alpaca {
            feed: crate::session_router::AlpacaFeed::Sip
        }
    );
    assert!(
        next_selection
            .provider_group()
            .slices_recent_to_old
            .iter()
            .all(|slice| slice.phase == Some(crate::session_router::SessionPhase::Pre))
    );

    let mut tampered_cursor_request = continued_request.clone();
    let mut tampered_cursor = failed_futu_cursor.clone();
    let payload_start = "mdp2.".len();
    let replacement = if tampered_cursor.as_bytes()[payload_start] == b'A' {
        'B'
    } else {
        'A'
    };
    tampered_cursor.replace_range(payload_start..payload_start + 1, &replacement.to_string());
    tampered_cursor_request.cursor = Some(tampered_cursor);
    assert!(
        provider_runtime
            .select_page_at_synthetic_for_test(
                &tampered_cursor_request,
                &plan,
                &rights_profile_sha256,
                now_unix_seconds,
            )
            .is_err()
    );

    let research_rights_profile_sha256 = hash_cache_rights_profile(
        "",
        test_support::research_principal().principal_scope_sha256(),
    );
    assert!(matches!(
        provider_runtime.select_page_at_synthetic_for_test(
            &continued_request,
            &plan,
            &research_rights_profile_sha256,
            now_unix_seconds,
        ),
        Err(crate::market_query::providers::ProviderQueryError::CursorQueryMismatch)
    ));

    let mut changed_plan = plan.clone();
    changed_plan.calendar_version.push_str("-changed");
    assert!(matches!(
        provider_runtime.select_page_at_synthetic_for_test(
            &continued_request,
            &changed_plan,
            &rights_profile_sha256,
            now_unix_seconds,
        ),
        Err(crate::market_query::providers::ProviderQueryError::CursorPlanChanged)
    ));

    let last_run_failure = super::response::response_for_provider_failure(
        &continued_request,
        &plan,
        Some(&next_selection),
        crate::market_query::providers::ProviderQueryError::ProviderUnavailable,
        market_contracts::BarsCacheStatusV1::Unavailable,
    )
    .unwrap();
    continued_request
        .validate_response(&last_run_failure)
        .unwrap();
    assert!(!last_run_failure.truncated);
    assert!(last_run_failure.continuation_cursor.is_none());

    let row_start = ProtoTimestampV2::parse("2026-01-05T21:00:00Z").unwrap();
    let row_end = ProtoTimestampV2::parse("2026-01-05T21:01:00Z").unwrap();
    let received_at = ProtoTimestampV2::parse("2026-01-05T21:01:01Z").unwrap();
    let raw_page = market_contracts::BarsResponseV1 {
        schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
        provider: "futu".to_owned(),
        feed: "unknown".to_owned(),
        entitlement_status: market_contracts::BarsEntitlementStatusV1::Unknown,
        requested_resolution: BarResolutionV1::OneMinute,
        effective_resolution: BarResolutionV1::OneMinute,
        adjustment: AdjustmentV1::Unadjusted,
        rows: vec![BarRowV1 {
            symbol: request.symbol.clone(),
            asset_class: request.asset_class,
            resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            bar_start_utc: row_start.clone(),
            bar_end_exclusive_utc: row_end.clone(),
            received_at_utc: received_at.clone(),
            available_at_utc: received_at.clone(),
            open: DecimalString::new("10.00").unwrap(),
            high: DecimalString::new("11.00").unwrap(),
            low: DecimalString::new("9.00").unwrap(),
            close: DecimalString::new("10.50").unwrap(),
            volume: DecimalString::new("100").unwrap(),
            provenance_id: "futu-page".to_owned(),
            session_instance_id: "provider-local".to_owned(),
            validation_state: market_contracts::BarsValidationStateV1::Provisional,
        }],
        coverage_status: BarsCoverageStatusV1::Complete,
        segments: vec![BarCoverageSegmentV1 {
            start_utc: row_start,
            end_exclusive_utc: row_end,
            coverage_status: BarsCoverageStatusV1::Complete,
            provenance_id: Some("futu-page".to_owned()),
            session_instance_id: "provider-local".to_owned(),
        }],
        gaps: Vec::new(),
        source_provenance: vec![BarSourceProvenanceV1 {
            provenance_id: "futu-page".to_owned(),
            provider: "futu".to_owned(),
            feed: "unknown".to_owned(),
            source: "Futu OpenD".to_owned(),
            entitlement_status: market_contracts::BarsEntitlementStatusV1::Unknown,
            numeric_encoding: NumericEncodingV1::BinaryFloat64ShortestDecimal,
        }],
        as_of: None,
        fetched_at: received_at,
        verified_at: None,
        cache_status: market_contracts::BarsCacheStatusV1::Miss,
        market_calendar_version: None,
        continuation_cursor: None,
        truncated: false,
    };
    let canonical = super::response::response_for_cache_page(
        &request,
        &plan,
        &selection,
        raw_page,
        market_contracts::BarsCacheStatusV1::Miss,
    )
    .unwrap();
    let mut cache_hit_page = canonical;
    cache_hit_page.cache_status = market_contracts::BarsCacheStatusV1::Hit;
    assert!(!cache_hit_page.truncated);
    assert!(cache_hit_page.continuation_cursor.is_none());

    // A04 returns this shape for an exhausted page without a provider cursor receipt. The HTTP
    // assembler must still emit the outer cursor for the next planned source run.
    let replayed = super::response::response_from_page(
        &request,
        &plan,
        Some(&selection),
        cache_hit_page,
        market_contracts::BarsCacheStatusV1::Hit,
    )
    .unwrap();

    request.validate_response(&replayed).unwrap();
    assert_eq!(replayed.rows.len(), 1);
    assert_eq!(
        replayed.rows[0].session_instance_id,
        format!("{session_id}:after")
    );
    assert_eq!(replayed.feed, "unknown");
    assert_eq!(
        replayed.entitlement_status,
        market_contracts::BarsEntitlementStatusV1::Unknown
    );
    assert_eq!(replayed.coverage_status, BarsCoverageStatusV1::Partial);
    assert_eq!(
        replayed.cache_status,
        market_contracts::BarsCacheStatusV1::Hit
    );
    assert!(replayed.verified_at.is_none());
    assert!(replayed.truncated);
    let continuation = replayed
        .continuation_cursor
        .as_ref()
        .expect("next planned source run has an outer continuation");

    let mut next_request = request.clone();
    next_request.cursor = Some(continuation.clone());
    let observed_now = chrono::DateTime::<chrono::Utc>::from_timestamp(
        i64::try_from(now_unix_seconds).unwrap(),
        0,
    )
    .unwrap()
    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let observed_now = ProtoTimestampV2::parse(&observed_now).unwrap();
    let next_as_of = provider_runtime
        .planning_as_of(&next_request, &observed_now, &rights_profile_sha256)
        .unwrap();
    assert_eq!(next_as_of, as_of);
    let next_plan = crate::market_query::planning::plan_market_bars(
        &next_request,
        &next_as_of,
        &capabilities,
        &calendar,
    )
    .unwrap();
    assert!(matches!(
        provider_runtime.select_page_at(
            &next_request,
            &next_plan,
            &rights_profile_sha256,
            now_unix_seconds,
        ),
        Err(crate::market_query::providers::ProviderQueryError::ProviderCapabilityUnavailable)
    ));
    // This selector bypass is confined to a no-network mechanical cursor/cache fixture. The
    // production selector above continues to fail closed without provider admission/configuration.
    let next_selection = provider_runtime
        .select_page_at_synthetic_for_test(
            &next_request,
            &next_plan,
            &rights_profile_sha256,
            now_unix_seconds,
        )
        .unwrap();
    assert_eq!(
        next_selection.provider_group().source,
        crate::session_router::PlannedSource::Alpaca {
            feed: crate::session_router::AlpacaFeed::Sip
        }
    );
    assert!(next_selection.timeline_position() > selection.timeline_position());
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
        yfinance_research: None,
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
