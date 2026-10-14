use super::*;
use crate::session_router::{CalendarDay, CalendarSourceMetadata, SessionInstance};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use market_contracts::{
    BarSourceProvenanceV1, BarsCacheStatusV1, BarsEntitlementStatusV1, BarsProviderModeV1,
    DecimalString, NumericEncodingV1,
};

fn timestamp(value: &str) -> ProtoTimestampV2 {
    ProtoTimestampV2::parse(value).expect("Core timestamp fixture")
}

fn test_calendar(start_date: NaiveDate, day_count: usize) -> TrustedCalendarSnapshot {
    let mut days = Vec::with_capacity(day_count);
    let mut instances = Vec::new();
    for offset in 0..day_count {
        let trade_date = start_date + Duration::days(offset as i64);
        let day_start = trade_date
            .and_hms_opt(5, 0, 0)
            .expect("day start")
            .and_utc();
        let next_day_start = day_start + Duration::days(1);
        let date_label = trade_date.format("%Y-%m-%d").to_string();
        let session_id = format!("XNYS:{date_label}");
        days.push(CalendarDay {
            trade_date,
            day_window: UtcWindow {
                start: day_start,
                end_exclusive: next_day_start,
            },
            state: CalendarDayState::Open {
                session_id: session_id.clone(),
            },
            verification_status: if (NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..=NaiveDate::from_ymd_opt(2028, 12, 31).unwrap())
                .contains(&trade_date)
            {
                CalendarVerificationStatus::Verified
            } else {
                CalendarVerificationStatus::Provisional
            },
        });
        let open = day_start + Duration::hours(9) + Duration::minutes(30);
        let close = day_start + Duration::hours(16);
        instances.extend([
            SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: format!("{session_id}:pre"),
                trade_date,
                phase: SessionPhase::Pre,
                window: UtcWindow {
                    start: day_start + Duration::hours(4),
                    end_exclusive: open,
                },
            },
            SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: format!("{session_id}:regular"),
                trade_date,
                phase: SessionPhase::Regular,
                window: UtcWindow {
                    start: open,
                    end_exclusive: close,
                },
            },
            SessionInstance {
                session_id,
                session_instance_id: format!("XNYS:{date_label}:after"),
                trade_date,
                phase: SessionPhase::After,
                window: UtcWindow {
                    start: close,
                    end_exclusive: day_start + Duration::hours(20),
                },
            },
        ]);
    }
    let coverage_start = days.first().expect("at least one day").trade_date;
    let coverage_end_exclusive = days
        .last()
        .expect("at least one day")
        .trade_date
        .succ_opt()
        .expect("next date");
    TrustedCalendarSnapshot {
        source_id: "XNYS".to_owned(),
        version: "nyse.v1".to_owned(),
        source_metadata: CalendarSourceMetadata {
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
        coverage_start,
        coverage_end_exclusive,
        days,
        instances,
    }
}

fn minute_capabilities() -> Vec<SourceCapability> {
    [
        SessionPhase::Pre,
        SessionPhase::Regular,
        SessionPhase::After,
    ]
    .into_iter()
    .map(|phase| SourceCapability {
        provider: crate::session_router::Provider::Futu,
        feed: None,
        phase: Some(phase),
        interval: DataInterval::Minute(1),
        historical: true,
        realtime: true,
    })
    .collect()
}

fn intraday_request(start: &str, end: &str) -> BarsRequestV1 {
    BarsRequestV1 {
        symbol: "SPY".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: timestamp(start),
        end: timestamp(end),
        resolution: BarResolutionV1::OneMinute,
        session_scope: SessionScopeV1::All,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::FutuOnly,
        allow_daily_downgrade: false,
        limit: 100,
        cursor: None,
    }
}

fn provider_page(start: &str, end: &str) -> BarsResponseV1 {
    let row_start = timestamp(start);
    let row_end = timestamp(end);
    let received = timestamp("2026-01-05T14:32:00Z");
    BarsResponseV1 {
        schema_version: 1,
        provider: "futu".to_owned(),
        feed: "unknown".to_owned(),
        entitlement_status: BarsEntitlementStatusV1::Unknown,
        requested_resolution: BarResolutionV1::OneMinute,
        effective_resolution: BarResolutionV1::OneMinute,
        adjustment: AdjustmentV1::Unadjusted,
        rows: vec![BarRowV1 {
            symbol: "SPY".to_owned(),
            asset_class: AssetClassV1::Etf,
            resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            bar_start_utc: row_start.clone(),
            bar_end_exclusive_utc: row_end.clone(),
            received_at_utc: received.clone(),
            available_at_utc: received.clone(),
            open: DecimalString::new("10.00").unwrap(),
            high: DecimalString::new("11.00").unwrap(),
            low: DecimalString::new("9.00").unwrap(),
            close: DecimalString::new("10.50").unwrap(),
            volume: DecimalString::new("100").unwrap(),
            provenance_id: "futu-page".to_owned(),
            session_instance_id: "provider-local-session-id".to_owned(),
            validation_state: BarsValidationStateV1::Validated,
        }],
        coverage_status: BarsCoverageStatusV1::Complete,
        segments: vec![BarCoverageSegmentV1 {
            start_utc: row_start,
            end_exclusive_utc: row_end,
            coverage_status: BarsCoverageStatusV1::Complete,
            provenance_id: Some("futu-page".to_owned()),
            session_instance_id: "provider-local-session-id".to_owned(),
        }],
        gaps: Vec::new(),
        source_provenance: vec![BarSourceProvenanceV1 {
            provenance_id: "futu-page".to_owned(),
            provider: "futu".to_owned(),
            feed: "unknown".to_owned(),
            source: "Futu OpenD".to_owned(),
            entitlement_status: BarsEntitlementStatusV1::Unknown,
            numeric_encoding: NumericEncodingV1::DecimalToken,
        }],
        as_of: None,
        fetched_at: received,
        verified_at: Some(timestamp("2026-01-05T14:33:00Z")),
        cache_status: BarsCacheStatusV1::Miss,
        market_calendar_version: None,
        continuation_cursor: None,
        truncated: false,
    }
}

#[test]
fn mapper_replaces_provider_session_id_and_keeps_unknown_source_evidence_provisional() {
    let date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let calendar = test_calendar(date, 1);
    let request = intraday_request("2026-01-05T14:31:00Z", "2026-01-05T14:32:00Z");
    let plan = plan_market_bars(
        &request,
        &timestamp("2026-01-05T15:00:00Z"),
        &minute_capabilities(),
        &calendar,
    )
    .unwrap();
    assert_eq!(plan.as_of.as_str(), "2026-01-05T15:00:00Z");
    let mapped = map_provider_page(
        &plan,
        0,
        &provider_page("2026-01-05T14:31:00Z", "2026-01-05T14:32:00Z"),
    )
    .unwrap();

    assert_eq!(mapped.rows.len(), 1);
    assert_eq!(
        mapped.rows[0].session_instance_id,
        "XNYS:2026-01-05:regular"
    );
    assert_eq!(
        mapped.rows[0].validation_state,
        BarsValidationStateV1::Provisional
    );
    assert!(mapped.provisional);
    assert!(
        mapped
            .segments
            .iter()
            .all(|segment| { segment.session_instance_id == "XNYS:2026-01-05:regular" })
    );
    assert!(
        mapped
            .segments
            .iter()
            .all(|segment| segment.coverage_status == BarsCoverageStatusV1::Unknown)
    );
    assert_eq!(mapped.coverage_status, BarsCoverageStatusV1::Unknown);
    assert!(mapped.gaps.is_empty());
}

#[test]
fn gap_normalization_uses_reason_priority_and_merges_adjacent_equal_reasons() {
    let gaps = normalize_gaps(vec![
        gap_from_window(
            UtcWindow {
                start: utc_dt("2026-01-05T14:30:00Z"),
                end_exclusive: utc_dt("2026-01-05T14:33:00Z"),
            },
            CoverageGapReasonV1::Unknown,
        )
        .unwrap(),
        gap_from_window(
            UtcWindow {
                start: utc_dt("2026-01-05T14:31:00Z"),
                end_exclusive: utc_dt("2026-01-05T14:32:00Z"),
            },
            CoverageGapReasonV1::EntitlementLimited,
        )
        .unwrap(),
    ])
    .unwrap();

    assert_eq!(gaps.len(), 3);
    assert_eq!(gaps[0].reason_code, CoverageGapReasonV1::Unknown);
    assert_eq!(gaps[0].start_utc.as_str(), "2026-01-05T14:30:00Z");
    assert_eq!(gaps[0].end_exclusive_utc.as_str(), "2026-01-05T14:31:00Z");
    assert_eq!(gaps[1].reason_code, CoverageGapReasonV1::EntitlementLimited);
    assert_eq!(gaps[1].start_utc.as_str(), "2026-01-05T14:31:00Z");
    assert_eq!(gaps[1].end_exclusive_utc.as_str(), "2026-01-05T14:32:00Z");
    assert_eq!(gaps[2].reason_code, CoverageGapReasonV1::Unknown);
    assert_eq!(gaps[2].start_utc.as_str(), "2026-01-05T14:32:00Z");
    assert_eq!(gaps[2].end_exclusive_utc.as_str(), "2026-01-05T14:33:00Z");
}

#[test]
fn mapper_rejects_a_bar_that_crosses_the_strict_utc_query_boundary() {
    let date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let calendar = test_calendar(date, 1);
    let request = intraday_request("2026-01-05T14:31:30Z", "2026-01-05T14:32:30Z");
    let plan = plan_market_bars(
        &request,
        &timestamp("2026-01-05T15:00:00Z"),
        &minute_capabilities(),
        &calendar,
    )
    .unwrap();
    let mapped = map_provider_page(
        &plan,
        0,
        &provider_page("2026-01-05T14:31:00Z", "2026-01-05T14:32:00Z"),
    )
    .unwrap();

    assert!(mapped.rows.is_empty());
    assert_eq!(mapped.coverage_status, BarsCoverageStatusV1::Partial);
    assert_eq!(mapped.gaps.len(), 1);
    assert_eq!(mapped.gaps[0].reason_code, CoverageGapReasonV1::Unknown);
    assert_eq!(mapped.gaps[0].start_utc, request.start);
    assert_eq!(
        mapped.gaps[0].end_exclusive_utc.as_str(),
        "2026-01-05T14:32:00Z"
    );
}

#[test]
fn mapper_drops_a_current_minute_bar_that_extends_past_query_end_without_clipping() {
    let date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let calendar = test_calendar(date, 1);
    let request = intraday_request("2026-01-05T14:31:00Z", "2026-01-05T14:31:30Z");
    let plan = plan_market_bars(
        &request,
        &timestamp("2026-01-05T15:00:00Z"),
        &minute_capabilities(),
        &calendar,
    )
    .unwrap();
    let mapped = map_provider_page(
        &plan,
        0,
        &provider_page("2026-01-05T14:31:00Z", "2026-01-05T14:32:00Z"),
    )
    .unwrap();

    assert!(mapped.rows.is_empty());
    assert_eq!(mapped.coverage_status, BarsCoverageStatusV1::Partial);
    assert_eq!(mapped.gaps.len(), 1);
    assert_eq!(mapped.gaps[0].reason_code, CoverageGapReasonV1::Unknown);
    assert_eq!(mapped.gaps[0].start_utc, request.start);
    assert_eq!(mapped.gaps[0].end_exclusive_utc, request.end);
}

#[test]
fn daily_queries_coalesce_by_source_instead_of_emitting_one_group_per_date() {
    let start_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let calendar = test_calendar(start_date, 3);
    let request = BarsRequestV1 {
        symbol: "SPY".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: timestamp("2026-01-05T05:00:00Z"),
        end: timestamp("2026-01-08T05:00:00Z"),
        resolution: BarResolutionV1::OneDay,
        session_scope: SessionScopeV1::Regular,
        adjustment: AdjustmentV1::Split,
        provider_mode: BarsProviderModeV1::Auto,
        allow_daily_downgrade: false,
        limit: 100,
        cursor: None,
    };
    let capabilities = [
        SourceCapability {
            provider: crate::session_router::Provider::Futu,
            feed: None,
            phase: None,
            interval: DataInterval::Daily,
            historical: true,
            realtime: true,
        },
        SourceCapability {
            provider: crate::session_router::Provider::Alpaca,
            feed: Some(crate::session_router::AlpacaFeed::Sip),
            phase: None,
            interval: DataInterval::Daily,
            historical: true,
            realtime: false,
        },
    ];
    let plan = plan_market_bars(
        &request,
        &timestamp("2026-01-07T22:00:00Z"),
        &capabilities,
        &calendar,
    )
    .unwrap();

    assert_eq!(plan.slices_recent_to_old.len(), 3);
    assert_eq!(plan.provider_groups.len(), 2);
    assert!(
        plan.provider_groups
            .iter()
            .all(|group| group.slices_recent_to_old.len() >= 1)
    );
    assert!(plan.provider_groups.iter().any(|group| {
        group.source == PlannedSource::Futu && group.slices_recent_to_old.len() == 1
    }));
    assert!(plan.provider_groups.iter().any(|group| {
        matches!(group.source, PlannedSource::Alpaca { .. })
            && group.slices_recent_to_old.len() == 2
    }));
    assert!(
        plan.slices_recent_to_old
            .iter()
            .find(|slice| slice.trade_date == start_date.succ_opt().unwrap().succ_opt().unwrap())
            .is_some_and(|slice| slice.provisional)
    );
}

#[test]
fn request_outside_calendar_coverage_keeps_known_slices_and_explicit_calendar_gaps() {
    let start_date = NaiveDate::from_ymd_opt(1990, 1, 2).unwrap();
    let calendar = test_calendar(start_date, 1);
    let request = BarsRequestV1 {
        symbol: "SPY".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: timestamp("1970-01-01T00:00:00Z"),
        end: timestamp("1990-01-04T05:00:00Z"),
        resolution: BarResolutionV1::OneDay,
        session_scope: SessionScopeV1::Regular,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::Auto,
        allow_daily_downgrade: false,
        limit: 100,
        cursor: None,
    };
    let daily_yfinance = [SourceCapability {
        provider: crate::session_router::Provider::YFinance,
        feed: None,
        phase: None,
        interval: DataInterval::Daily,
        historical: true,
        realtime: true,
    }];
    let plan = plan_market_bars(
        &request,
        &timestamp("1990-01-02T15:00:00Z"),
        &daily_yfinance,
        &calendar,
    )
    .unwrap();

    assert_eq!(plan.requested_window.start, utc_dt("1970-01-01T00:00:00Z"));
    assert_eq!(plan.slices_recent_to_old.len(), 1);
    assert_eq!(plan.provider_groups.len(), 1);
    assert_eq!(
        plan.provider_groups[0].source,
        PlannedSource::YFinanceDailyResearch
    );
    assert_eq!(plan.gaps.len(), 2);
    assert!(
        plan.gaps
            .iter()
            .all(|gap| gap.reason_code == CoverageGapReasonV1::Unknown)
    );
    assert_eq!(plan.gaps[0].start_utc.as_str(), "1970-01-01T00:00:00Z");
    assert_eq!(
        plan.gaps[0].end_exclusive_utc.as_str(),
        "1990-01-02T05:00:00Z"
    );
    assert_eq!(plan.gaps[1].start_utc.as_str(), "1990-01-03T05:00:00Z");
    assert_eq!(
        plan.gaps[1].end_exclusive_utc.as_str(),
        "1990-01-04T05:00:00Z"
    );
    assert!(plan.provisional);
}

#[test]
fn overnight_scope_reports_only_calendar_edge_gaps_without_fabricating_over_routes() {
    let date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let calendar = test_calendar(date, 1);
    let mut request = intraday_request("2026-01-06T01:30:00Z", "2026-01-06T02:00:00Z");
    request.session_scope = SessionScopeV1::Overnight;
    let plan = plan_market_bars(
        &request,
        &timestamp("2026-01-06T02:30:00Z"),
        &minute_capabilities(),
        &calendar,
    )
    .unwrap();

    assert!(plan.provider_groups.is_empty());
    assert_eq!(plan.gaps.len(), 1);
    assert_eq!(plan.gaps[0].reason_code, CoverageGapReasonV1::Unknown);
    assert_eq!(plan.gaps[0].start_utc.as_str(), "2026-01-06T01:30:00Z");
    assert_eq!(
        plan.gaps[0].end_exclusive_utc.as_str(),
        "2026-01-06T02:00:00Z"
    );
}

#[test]
fn daily_extended_scope_is_explicitly_unsupported() {
    let start_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let calendar = test_calendar(start_date, 1);
    let request = BarsRequestV1 {
        symbol: "SPY".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: timestamp("2026-01-05T05:00:00Z"),
        end: timestamp("2026-01-06T05:00:00Z"),
        resolution: BarResolutionV1::OneDay,
        session_scope: SessionScopeV1::Extended,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::Research,
        allow_daily_downgrade: false,
        limit: 100,
        cursor: None,
    };
    let plan =
        plan_market_bars(&request, &timestamp("2026-01-05T15:00:00Z"), &[], &calendar).unwrap();

    assert!(plan.provider_groups.is_empty());
    assert_eq!(plan.gaps.len(), 1);
    assert_eq!(
        plan.gaps[0].reason_code,
        CoverageGapReasonV1::UnsupportedResolution
    );
}

#[test]
fn intraday_before_ny_cutoff_requires_whole_request_daily_downgrade() {
    let date = NaiveDate::from_ymd_opt(2015, 12, 31).unwrap();
    let calendar = test_calendar(date, 1);
    let mut request = intraday_request("2015-12-31T20:00:00Z", "2016-01-01T06:00:00Z");
    request.provider_mode = BarsProviderModeV1::Auto;
    let yfinance_daily = [SourceCapability {
        provider: crate::session_router::Provider::YFinance,
        feed: None,
        phase: None,
        interval: DataInterval::Daily,
        historical: true,
        realtime: false,
    }];

    let denied = plan_market_bars(
        &request,
        &timestamp("2015-12-31T21:00:00Z"),
        &yfinance_daily,
        &calendar,
    )
    .unwrap();
    assert!(denied.provider_groups.is_empty());
    assert_eq!(denied.gaps.len(), 2);
    assert_eq!(
        denied.gaps[0].reason_code,
        CoverageGapReasonV1::UnsupportedResolution
    );
    assert_eq!(denied.gaps[0].start_utc.as_str(), "2015-12-31T20:00:00Z");
    assert_eq!(
        denied.gaps[0].end_exclusive_utc.as_str(),
        "2016-01-01T05:00:00Z"
    );
    assert_eq!(denied.gaps[1].reason_code, CoverageGapReasonV1::Unknown);
    assert_eq!(denied.gaps[1].start_utc.as_str(), "2016-01-01T05:00:00Z");
    assert_eq!(
        denied.gaps[1].end_exclusive_utc.as_str(),
        "2016-01-01T06:00:00Z"
    );

    request.allow_daily_downgrade = true;
    let plan = plan_market_bars(
        &request,
        &timestamp("2015-12-31T21:00:00Z"),
        &yfinance_daily,
        &calendar,
    )
    .unwrap();
    assert_eq!(plan.effective_resolution, BarResolutionV1::OneDay);
    assert_eq!(plan.provider_groups.len(), 1);
    assert_eq!(
        plan.provider_groups[0].source,
        PlannedSource::YFinanceDailyResearch
    );
    assert_eq!(plan.slices_recent_to_old.len(), 1);
    assert_eq!(
        plan.provider_groups[0].fetch_window.end_exclusive,
        utc_dt("2016-01-01T05:00:00Z")
    );
    assert_eq!(plan.gaps.len(), 1);
    assert_eq!(plan.gaps[0].reason_code, CoverageGapReasonV1::Unknown);
    assert_eq!(plan.gaps[0].start_utc.as_str(), "2016-01-01T05:00:00Z");
    assert_eq!(
        plan.slices_recent_to_old[0].window.start,
        utc_dt("2015-12-31T20:00:00Z")
    );
    assert_eq!(
        plan.slices_recent_to_old[0].window.end_exclusive,
        utc_dt("2016-01-01T05:00:00Z")
    );
    assert_eq!(
        plan.calendar_verification_status[0].verification_status,
        CalendarVerificationStatus::Provisional
    );
}

#[test]
fn explicit_pre_cutoff_daily_downgrade_keeps_one_resolution_across_2016_cutoff() {
    let start_date = NaiveDate::from_ymd_opt(2015, 12, 31).unwrap();
    let mut calendar = test_calendar(start_date, 5);
    for day in &mut calendar.days {
        if day.trade_date == NaiveDate::from_ymd_opt(2016, 1, 1).unwrap() {
            day.state = CalendarDayState::Closed {
                reason: Some("New Year holiday fixture".to_owned()),
            };
        } else if matches!(
            day.trade_date.weekday(),
            chrono::Weekday::Sat | chrono::Weekday::Sun
        ) {
            day.state = CalendarDayState::Closed {
                reason: Some("Weekend fixture".to_owned()),
            };
            day.verification_status = CalendarVerificationStatus::ModeledWeekend;
        }
    }
    calendar.instances.retain(|instance| {
        instance.trade_date == start_date
            || instance.trade_date == NaiveDate::from_ymd_opt(2016, 1, 4).unwrap()
    });

    let mut request = intraday_request("2015-12-31T20:00:00Z", "2016-01-05T05:00:00Z");
    request.allow_daily_downgrade = true;
    let capabilities = [
        SourceCapability {
            provider: crate::session_router::Provider::YFinance,
            feed: None,
            phase: None,
            interval: DataInterval::Daily,
            historical: true,
            realtime: false,
        },
        SourceCapability {
            provider: crate::session_router::Provider::Futu,
            feed: None,
            phase: None,
            interval: DataInterval::Daily,
            historical: true,
            realtime: true,
        },
    ];
    let plan = plan_market_bars(
        &request,
        &timestamp("2016-01-04T15:00:00Z"),
        &capabilities,
        &calendar,
    )
    .unwrap();

    assert_eq!(plan.effective_resolution, BarResolutionV1::OneDay);
    assert_eq!(plan.slices_recent_to_old.len(), 2);
    assert!(
        plan.provider_groups
            .iter()
            .all(|group| group.resolution == BarResolutionV1::OneDay)
    );
    assert!(
        plan.provider_groups
            .iter()
            .any(|group| group.source == PlannedSource::YFinanceDailyResearch)
    );
    assert!(
        plan.provider_groups
            .iter()
            .any(|group| group.source == PlannedSource::Futu)
    );
    assert!(plan.gaps.is_empty());
}

fn utc_dt(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture")
        .with_timezone(&Utc)
}
