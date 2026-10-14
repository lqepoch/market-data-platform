use super::*;
use chrono::{Duration, Weekday};
use sha2::{Digest, Sha256};

fn utc(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp fixture")
        .with_timezone(&Utc)
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

fn fixture_status(trade_date: NaiveDate) -> CalendarVerificationStatus {
    match trade_date.weekday() {
        Weekday::Sat | Weekday::Sun => CalendarVerificationStatus::ModeledWeekend,
        _ if (date("2026-01-01")..=date("2028-12-31")).contains(&trade_date) => {
            CalendarVerificationStatus::Verified
        }
        _ => CalendarVerificationStatus::Provisional,
    }
}

fn day(trade_date: &str, start: &str, end: &str, state: CalendarDayState) -> CalendarDay {
    let trade_date = date(trade_date);
    CalendarDay {
        trade_date,
        day_window: window(start, end),
        state,
        verification_status: fixture_status(trade_date),
    }
}

fn open_day(trade_date: &str, start: &str, end: &str, id: &str) -> CalendarDay {
    day(
        trade_date,
        start,
        end,
        CalendarDayState::Open {
            session_id: id.to_owned(),
        },
    )
}

fn closed_day(trade_date: &str, start: &str, end: &str, reason: &str) -> CalendarDay {
    day(
        trade_date,
        start,
        end,
        CalendarDayState::Closed {
            reason: Some(reason.to_owned()),
        },
    )
}

fn instance(
    session: &str,
    id: &str,
    trade_date: &str,
    phase: SessionPhase,
    start: &str,
    end: &str,
) -> SessionInstance {
    SessionInstance {
        session_id: session.to_owned(),
        session_instance_id: id.to_owned(),
        trade_date: date(trade_date),
        phase,
        window: window(start, end),
    }
}

fn phases(
    session: &str,
    trade_date: &str,
    pre: &str,
    open: &str,
    close: &str,
    post: &str,
) -> [SessionInstance; 3] {
    [
        instance(
            session,
            &format!("{session}-pre"),
            trade_date,
            SessionPhase::Pre,
            pre,
            open,
        ),
        instance(
            session,
            &format!("{session}-regular"),
            trade_date,
            SessionPhase::Regular,
            open,
            close,
        ),
        instance(
            session,
            &format!("{session}-after"),
            trade_date,
            SessionPhase::After,
            close,
            post,
        ),
    ]
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

fn snapshot(days: Vec<CalendarDay>, instances: Vec<SessionInstance>) -> TrustedCalendarSnapshot {
    TrustedCalendarSnapshot {
        source_id: "XNYS".to_owned(),
        version: "nyse.v1".to_owned(),
        source_metadata: source_metadata(),
        coverage_start: days.first().expect("date rows").trade_date,
        coverage_end_exclusive: days
            .last()
            .expect("date rows")
            .trade_date
            .succ_opt()
            .unwrap(),
        days,
        instances,
    }
}

fn xnys_week_snapshot() -> TrustedCalendarSnapshot {
    // XNYS artifact 2026-01-02 and 2026-01-05..07: PRE/REG/AFTER only. It supplies no OVER.
    let days = vec![
        open_day(
            "2026-01-02",
            "2026-01-02T05:00:00Z",
            "2026-01-03T05:00:00Z",
            "XNYS:2026-01-02",
        ),
        closed_day(
            "2026-01-03",
            "2026-01-03T05:00:00Z",
            "2026-01-04T05:00:00Z",
            "weekend",
        ),
        closed_day(
            "2026-01-04",
            "2026-01-04T05:00:00Z",
            "2026-01-05T05:00:00Z",
            "weekend",
        ),
        open_day(
            "2026-01-05",
            "2026-01-05T05:00:00Z",
            "2026-01-06T05:00:00Z",
            "XNYS:2026-01-05",
        ),
        open_day(
            "2026-01-06",
            "2026-01-06T05:00:00Z",
            "2026-01-07T05:00:00Z",
            "XNYS:2026-01-06",
        ),
        open_day(
            "2026-01-07",
            "2026-01-07T05:00:00Z",
            "2026-01-08T05:00:00Z",
            "XNYS:2026-01-07",
        ),
    ];
    let mut instances = Vec::new();
    instances.extend(phases(
        "XNYS:2026-01-02",
        "2026-01-02",
        "2026-01-02T09:00:00Z",
        "2026-01-02T14:30:00Z",
        "2026-01-02T21:00:00Z",
        "2026-01-03T01:00:00Z",
    ));
    instances.extend(phases(
        "XNYS:2026-01-05",
        "2026-01-05",
        "2026-01-05T09:00:00Z",
        "2026-01-05T14:30:00Z",
        "2026-01-05T21:00:00Z",
        "2026-01-06T01:00:00Z",
    ));
    instances.extend(phases(
        "XNYS:2026-01-06",
        "2026-01-06",
        "2026-01-06T09:00:00Z",
        "2026-01-06T14:30:00Z",
        "2026-01-06T21:00:00Z",
        "2026-01-07T01:00:00Z",
    ));
    instances.extend(phases(
        "XNYS:2026-01-07",
        "2026-01-07",
        "2026-01-07T09:00:00Z",
        "2026-01-07T14:30:00Z",
        "2026-01-07T21:00:00Z",
        "2026-01-08T01:00:00Z",
    ));
    snapshot(days, instances)
}

fn synthetic_four_phase_snapshot() -> TrustedCalendarSnapshot {
    // This intentionally synthetic planner fixture adds provider-specific OVER instances to the
    // XNYS-shaped rows. Runtime parsing must continue to reject it as an XNYS calendar artifact.
    let mut calendar = xnys_week_snapshot();
    for (trade_date, start, end) in [
        (
            "2026-01-05",
            "2026-01-05T01:00:00Z", // Sunday 20:00 ET, crossing into Monday's trade date.
            "2026-01-05T09:00:00Z", // Monday 04:00 ET.
        ),
        (
            "2026-01-06",
            "2026-01-06T01:00:00Z", // Monday 20:00 ET, crossing into Tuesday's trade date.
            "2026-01-06T09:00:00Z", // Tuesday 04:00 ET.
        ),
    ] {
        let session_id = format!("XNYS:{trade_date}");
        calendar.instances.push(instance(
            &session_id,
            &format!("{session_id}-over"),
            trade_date,
            SessionPhase::Over,
            start,
            end,
        ));
    }
    calendar
        .instances
        .sort_by_key(|instance| instance.window.start);
    calendar
}

fn capabilities() -> Vec<SourceCapability> {
    let mut values = Vec::new();
    for phase in [
        SessionPhase::Pre,
        SessionPhase::Regular,
        SessionPhase::After,
        SessionPhase::Over,
    ] {
        values.push(SourceCapability {
            provider: Provider::Futu,
            feed: None,
            phase: Some(phase),
            interval: DataInterval::Minute(1),
            historical: true,
            realtime: true,
        });
        values.push(SourceCapability {
            provider: Provider::Alpaca,
            feed: Some(if phase == SessionPhase::Over {
                AlpacaFeed::Boats
            } else {
                AlpacaFeed::Sip
            }),
            phase: Some(phase),
            interval: DataInterval::Minute(1),
            historical: true,
            realtime: true,
        });
    }
    values.extend([
        SourceCapability {
            provider: Provider::Futu,
            feed: None,
            phase: None,
            interval: DataInterval::Daily,
            historical: true,
            realtime: true,
        },
        SourceCapability {
            provider: Provider::Alpaca,
            feed: Some(AlpacaFeed::Sip),
            phase: None,
            interval: DataInterval::Daily,
            historical: true,
            realtime: true,
        },
        SourceCapability {
            provider: Provider::YFinance,
            feed: None,
            phase: None,
            interval: DataInterval::Daily,
            historical: true,
            realtime: false,
        },
    ]);
    values
}

#[allow(clippy::too_many_arguments)]
fn request<'a>(
    calendar: &'a TrustedCalendarSnapshot,
    capabilities: &'a [SourceCapability],
    start: &str,
    end: &str,
    as_of: &str,
    interval: DataInterval,
    mode: SourceMode,
    allow_daily_downgrade: bool,
) -> PlanRequest<'a> {
    PlanRequest {
        symbol: "SPY",
        instrument: InstrumentKind::UsEtf,
        start_utc: utc(start),
        end_utc_exclusive: utc(end),
        as_of_utc: utc(as_of),
        interval,
        source_mode: mode,
        allow_daily_downgrade,
        capabilities,
        calendar,
    }
}

fn slice_for_phase(plan: &SessionPlan, phase: SessionPhase) -> &PlannedSlice {
    plan.slices
        .iter()
        .find(|slice| slice.phase == Some(phase))
        .expect("phase slice exists")
}

#[test]
fn xnys_snapshot_validates_without_fabricating_an_overnight_phase() {
    let calendar = xnys_week_snapshot();
    validate_calendar_snapshot(&calendar).expect("verified snapshot shape");
    assert!(
        calendar
            .instances
            .iter()
            .all(|instance| instance.phase != SessionPhase::Over)
    );
}

#[test]
fn ranks_follow_the_ordered_trusted_instances_and_unknown_overnight_stays_a_gap() {
    let calendar = xnys_week_snapshot();
    let caps = capabilities();
    for (as_of, current, previous) in [
        ("2026-01-06T03:00:00Z", None, Some("XNYS:2026-01-05-after")), // Mon 22:00 ET: no trusted OVER
        (
            "2026-01-05T14:40:00Z",
            Some("XNYS:2026-01-05-regular"),
            Some("XNYS:2026-01-05-pre"),
        ),
        (
            "2026-01-05T22:30:00Z",
            Some("XNYS:2026-01-05-after"),
            Some("XNYS:2026-01-05-regular"),
        ),
        (
            "2026-01-05T12:00:00Z",
            Some("XNYS:2026-01-05-pre"),
            Some("XNYS:2026-01-02-after"),
        ),
    ] {
        let plan = plan_sessions(request(
            &calendar,
            &caps,
            "2026-01-02T09:00:00Z",
            "2026-01-07T21:00:00Z",
            as_of,
            DataInterval::Minute(1),
            SourceMode::Auto,
            false,
        ))
        .expect("valid plan");
        assert_eq!(plan.active_session_instance_id.as_deref(), current);
        if let Some(current_id) = current {
            let current_slice = plan
                .slices
                .iter()
                .find(|slice| slice.session_instance_id.as_deref() == Some(current_id))
                .unwrap();
            assert_eq!(current_slice.rank, Some(SessionRank::New));
            assert_eq!(current_slice.source, Some(PlannedSource::Futu));
            assert!(current_slice.realtime);
        }
        if let Some(previous_id) = previous {
            let previous_slice = plan
                .slices
                .iter()
                .find(|slice| slice.session_instance_id.as_deref() == Some(previous_id))
                .unwrap();
            assert_eq!(previous_slice.rank, Some(SessionRank::Buffer));
            assert_eq!(previous_slice.source, Some(PlannedSource::Futu));
            assert!(!previous_slice.realtime);
            if current.is_none() {
                assert!(previous_slice.provisional);
            }
        }
        if current.is_none() {
            let gap = plan
                .slices
                .iter()
                .find(|slice| slice.window.contains(utc(as_of)))
                .unwrap();
            assert_eq!(gap.gap, Some(GapReason::MissingCalendarPhase));
            assert!(gap.source.is_none());
            assert!(!gap.realtime);
        }
    }
}

#[test]
fn synthetic_four_phase_plans_route_current_buffer_and_old_instances_across_midnight() {
    let calendar = synthetic_four_phase_snapshot();
    let caps = capabilities();
    let alpaca_sip = PlannedSource::Alpaca {
        feed: AlpacaFeed::Sip,
    };
    let alpaca_boats = PlannedSource::Alpaca {
        feed: AlpacaFeed::Boats,
    };

    for (as_of, start, end, expected_active, expected_slices) in [
        (
            "2026-01-06T03:00:00Z", // Monday 22:00 ET: Tuesday OVER is active.
            "2026-01-05T09:00:00Z",
            "2026-01-06T09:00:00Z",
            Some("XNYS:2026-01-06-over"),
            vec![
                (
                    "XNYS:2026-01-05-after",
                    SessionRank::Buffer,
                    PlannedSource::Futu,
                ),
                ("XNYS:2026-01-05-regular", SessionRank::Old, alpaca_sip),
                ("XNYS:2026-01-05-pre", SessionRank::Old, alpaca_sip),
            ],
        ),
        (
            "2026-01-05T14:40:00Z", // Monday 09:40 ET: REG follows PRE and OVER.
            "2026-01-05T01:00:00Z",
            "2026-01-05T21:00:00Z",
            Some("XNYS:2026-01-05-regular"),
            vec![
                (
                    "XNYS:2026-01-05-regular",
                    SessionRank::New,
                    PlannedSource::Futu,
                ),
                (
                    "XNYS:2026-01-05-pre",
                    SessionRank::Buffer,
                    PlannedSource::Futu,
                ),
                ("XNYS:2026-01-05-over", SessionRank::Old, alpaca_boats),
            ],
        ),
        (
            "2026-01-05T22:30:00Z", // Monday 17:30 ET: AFTER follows REG.
            "2026-01-05T09:00:00Z",
            "2026-01-06T01:00:00Z",
            Some("XNYS:2026-01-05-after"),
            vec![
                (
                    "XNYS:2026-01-05-after",
                    SessionRank::New,
                    PlannedSource::Futu,
                ),
                (
                    "XNYS:2026-01-05-regular",
                    SessionRank::Buffer,
                    PlannedSource::Futu,
                ),
                ("XNYS:2026-01-05-pre", SessionRank::Old, alpaca_sip),
            ],
        ),
        (
            "2026-01-05T12:00:00Z", // Monday 07:00 ET: PRE follows OVER, then Friday AFTER.
            "2026-01-02T21:00:00Z",
            "2026-01-05T14:30:00Z",
            Some("XNYS:2026-01-05-pre"),
            vec![
                ("XNYS:2026-01-05-pre", SessionRank::New, PlannedSource::Futu),
                (
                    "XNYS:2026-01-05-over",
                    SessionRank::Buffer,
                    PlannedSource::Futu,
                ),
                ("XNYS:2026-01-02-after", SessionRank::Old, alpaca_sip),
            ],
        ),
    ] {
        let plan = plan_sessions(request(
            &calendar,
            &caps,
            start,
            end,
            as_of,
            DataInterval::Minute(1),
            SourceMode::Auto,
            false,
        ))
        .expect("four-phase synthetic plan");

        assert_eq!(plan.active_session_instance_id.as_deref(), expected_active);
        let current_slice = plan
            .slices
            .iter()
            .find(|slice| slice.session_instance_id.as_deref() == expected_active)
            .expect("active phase slice is present in the request window");
        assert_eq!(current_slice.rank, Some(SessionRank::New));
        assert_eq!(current_slice.source, Some(PlannedSource::Futu));
        assert!(current_slice.realtime);
        for (session_instance_id, rank, source) in expected_slices {
            let slice = plan
                .slices
                .iter()
                .find(|slice| slice.session_instance_id.as_deref() == Some(session_instance_id))
                .expect("expected phase slice is in the bounded request window");
            assert_eq!(slice.rank, Some(rank), "{session_instance_id}");
            assert_eq!(slice.source, Some(source), "{session_instance_id}");
            assert_eq!(
                slice.realtime,
                rank == SessionRank::New,
                "{session_instance_id}"
            );
        }
    }
}

#[test]
fn synthetic_overnight_boundaries_are_left_closed_and_closed_weekend_does_not_gain_over() {
    let calendar = synthetic_four_phase_snapshot();
    let caps = capabilities();

    for (as_of, expected_active) in [
        ("2026-01-05T00:59:59Z", None),
        ("2026-01-05T01:00:00Z", Some("XNYS:2026-01-05-over")),
        ("2026-01-05T01:00:01Z", Some("XNYS:2026-01-05-over")),
        ("2026-01-05T08:59:59Z", Some("XNYS:2026-01-05-over")),
        ("2026-01-05T09:00:00Z", Some("XNYS:2026-01-05-pre")),
        ("2026-01-05T09:00:01Z", Some("XNYS:2026-01-05-pre")),
    ] {
        let plan = plan_sessions(request(
            &calendar,
            &caps,
            "2026-01-05T00:59:59Z",
            "2026-01-05T09:00:01Z",
            as_of,
            DataInterval::Minute(1),
            SourceMode::Auto,
            false,
        ))
        .expect("overnight boundary plan");
        assert_eq!(
            plan.active_session_instance_id.as_deref(),
            expected_active,
            "as_of={as_of}"
        );
    }

    assert!(calendar.instances.iter().all(|instance| {
        !matches!(instance.trade_date.weekday(), Weekday::Sat | Weekday::Sun)
            || instance.phase != SessionPhase::Over
    }));
    let closed_saturday = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-04T03:00:00Z", // Saturday 22:00 ET; before Sunday's explicit Monday OVER.
        "2026-01-04T03:01:00Z",
        "2026-01-04T03:00:30Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .expect("closed-weekend plan");
    assert!(closed_saturday.active_session_instance_id.is_none());
    assert!(closed_saturday.slices.is_empty());
}

#[test]
fn native_xnys_snapshot_parser_still_rejects_the_synthetic_fourth_phase() {
    let calendar = synthetic_four_phase_snapshot();
    validate_calendar_snapshot(&calendar).expect("generic planner accepts explicit fixture");

    let bytes = serde_json::to_vec(&calendar).expect("snapshot JSON");
    let expected_sha256 = hex::encode(Sha256::digest(&bytes));
    assert!(matches!(
        parse_calendar_snapshot(&bytes, &expected_sha256),
        Err(CalendarArtifactError::UnsupportedPhaseSet)
    ));
}

#[test]
fn phase_boundaries_are_left_closed_right_open_at_and_around_each_boundary() {
    let calendar = xnys_week_snapshot();
    let caps = capabilities();
    for (boundary, before_id, at_id) in [
        ("2026-01-05T09:00:00Z", None, Some("XNYS:2026-01-05-pre")),
        (
            "2026-01-05T14:30:00Z",
            Some("XNYS:2026-01-05-pre"),
            Some("XNYS:2026-01-05-regular"),
        ),
        (
            "2026-01-05T21:00:00Z",
            Some("XNYS:2026-01-05-regular"),
            Some("XNYS:2026-01-05-after"),
        ),
        ("2026-01-06T01:00:00Z", Some("XNYS:2026-01-05-after"), None),
    ] {
        let at = utc(boundary);
        for (time, expected) in [
            (at - Duration::seconds(1), before_id),
            (at, at_id),
            (at + Duration::seconds(1), at_id),
        ] {
            let plan = plan_sessions(request(
                &calendar,
                &caps,
                "2026-01-05T05:00:00Z",
                "2026-01-06T05:00:00Z",
                &time.to_rfc3339(),
                DataInterval::Minute(1),
                SourceMode::Auto,
                false,
            ))
            .expect("boundary plan");
            assert_eq!(plan.active_session_instance_id.as_deref(), expected);
        }
    }
}

#[test]
fn old_alpaca_routes_use_sip_and_boats_is_selected_only_for_an_explicit_over_phase() {
    let calendar = xnys_week_snapshot();
    let caps = capabilities();
    let plan = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T09:00:00Z",
        "2026-01-06T01:00:00Z",
        "2026-01-05T22:30:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .expect("plan");
    let old_pre = plan
        .slices
        .iter()
        .find(|slice| slice.session_instance_id.as_deref() == Some("XNYS:2026-01-05-pre"))
        .unwrap();
    assert_eq!(old_pre.rank, Some(SessionRank::Old));
    assert_eq!(
        old_pre.source,
        Some(PlannedSource::Alpaca {
            feed: AlpacaFeed::Sip
        })
    );
    let over_choice = choose_source(
        SourceMode::Auto,
        date("2026-01-05"),
        DataInterval::Minute(1),
        Some(SessionPhase::Over),
        Some(SessionRank::Old),
    )
    .expect("caller-supplied OVER would select BOATS");
    assert_eq!(
        over_choice.source,
        PlannedSource::Alpaca {
            feed: AlpacaFeed::Boats
        }
    );
}

#[test]
fn closed_holiday_weekend_and_early_close_use_explicit_xnys_day_rows() {
    let days = vec![
        open_day(
            "2026-11-25",
            "2026-11-25T05:00:00Z",
            "2026-11-26T05:00:00Z",
            "XNYS:2026-11-25",
        ),
        closed_day(
            "2026-11-26",
            "2026-11-26T05:00:00Z",
            "2026-11-27T05:00:00Z",
            "official_closure",
        ),
        open_day(
            "2026-11-27",
            "2026-11-27T05:00:00Z",
            "2026-11-28T05:00:00Z",
            "XNYS:2026-11-27",
        ),
        closed_day(
            "2026-11-28",
            "2026-11-28T05:00:00Z",
            "2026-11-29T05:00:00Z",
            "weekend",
        ),
        closed_day(
            "2026-11-29",
            "2026-11-29T05:00:00Z",
            "2026-11-30T05:00:00Z",
            "weekend",
        ),
        open_day(
            "2026-11-30",
            "2026-11-30T05:00:00Z",
            "2026-12-01T05:00:00Z",
            "XNYS:2026-11-30",
        ),
    ];
    let mut instances = Vec::new();
    instances.extend(phases(
        "XNYS:2026-11-25",
        "2026-11-25",
        "2026-11-25T09:00:00Z",
        "2026-11-25T14:30:00Z",
        "2026-11-25T21:00:00Z",
        "2026-11-26T01:00:00Z",
    ));
    instances.extend(phases(
        "XNYS:2026-11-27",
        "2026-11-27",
        "2026-11-27T09:00:00Z",
        "2026-11-27T14:30:00Z",
        "2026-11-27T18:00:00Z",
        "2026-11-28T01:00:00Z",
    ));
    instances.extend(phases(
        "XNYS:2026-11-30",
        "2026-11-30",
        "2026-11-30T09:00:00Z",
        "2026-11-30T14:30:00Z",
        "2026-11-30T21:00:00Z",
        "2026-12-01T01:00:00Z",
    ));
    let calendar = snapshot(days, instances);
    let caps = capabilities();

    let holiday = plan_sessions(request(
        &calendar,
        &caps,
        "2026-11-26T05:00:00Z",
        "2026-11-27T05:00:00Z",
        "2026-11-26T15:00:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert!(holiday.slices.is_empty());
    assert!(holiday.active_session_instance_id.is_none());

    let closed_market = plan_sessions(request(
        &calendar,
        &caps,
        "2026-11-25T05:00:00Z",
        "2026-11-27T05:00:00Z",
        "2026-11-26T15:00:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert!(closed_market.active_session_instance_id.is_none());
    let last_futu = closed_market
        .slices
        .iter()
        .find(|slice| slice.session_instance_id.as_deref() == Some("XNYS:2026-11-25-after"))
        .unwrap();
    assert_eq!(last_futu.rank, Some(SessionRank::Buffer));
    assert_eq!(last_futu.source, Some(PlannedSource::Futu));
    assert!(last_futu.provisional);
    assert!(!last_futu.realtime);

    let weekend = plan_sessions(request(
        &calendar,
        &caps,
        "2026-11-28T05:00:00Z",
        "2026-11-30T05:00:00Z",
        "2026-11-29T15:00:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert!(weekend.slices.is_empty());
    assert!(weekend.active_session_instance_id.is_none());

    let early_close = plan_sessions(request(
        &calendar,
        &caps,
        "2026-11-27T18:00:00Z",
        "2026-11-27T19:00:00Z",
        "2026-11-27T18:00:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert_eq!(
        early_close.active_session_instance_id.as_deref(),
        Some("XNYS:2026-11-27-after")
    );
    assert_eq!(early_close.slices.len(), 1);
    assert_eq!(early_close.slices[0].phase, Some(SessionPhase::After));
    assert!(early_close.slices[0].realtime);
}

#[test]
fn dst_day_bounds_and_phases_are_supplied_as_utc_and_include_the_23_hour_day() {
    let calendar = snapshot(
        vec![
            closed_day(
                "2026-03-08",
                "2026-03-08T05:00:00Z",
                "2026-03-09T04:00:00Z",
                "weekend",
            ),
            open_day(
                "2026-03-09",
                "2026-03-09T04:00:00Z",
                "2026-03-10T04:00:00Z",
                "XNYS:2026-03-09",
            ),
        ],
        phases(
            "XNYS:2026-03-09",
            "2026-03-09",
            "2026-03-09T08:00:00Z",
            "2026-03-09T13:30:00Z",
            "2026-03-09T20:00:00Z",
            "2026-03-10T00:00:00Z",
        )
        .into(),
    );
    assert_eq!(
        calendar.days[0].day_window.end_exclusive - calendar.days[0].day_window.start,
        Duration::hours(23)
    );
    let caps = capabilities();
    let plan = plan_sessions(request(
        &calendar,
        &caps,
        "2026-03-09T04:00:00Z",
        "2026-03-10T04:00:00Z",
        "2026-03-09T13:40:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert_eq!(
        plan.active_session_instance_id.as_deref(),
        Some("XNYS:2026-03-09-regular")
    );
    assert_eq!(
        slice_for_phase(&plan, SessionPhase::Pre).window.start,
        utc("2026-03-09T08:00:00Z")
    );
}

#[test]
fn pre_2016_requests_use_utc_ny_midnight_even_when_the_window_has_no_phase_instance() {
    let days = vec![
        open_day(
            "2015-12-31",
            "2015-12-31T05:00:00Z",
            "2016-01-01T05:00:00Z",
            "XNYS:2015-12-31",
        ),
        closed_day(
            "2016-01-01",
            "2016-01-01T05:00:00Z",
            "2016-01-02T05:00:00Z",
            "official_closure",
        ),
        closed_day(
            "2016-01-02",
            "2016-01-02T05:00:00Z",
            "2016-01-03T05:00:00Z",
            "weekend",
        ),
        closed_day(
            "2016-01-03",
            "2016-01-03T05:00:00Z",
            "2016-01-04T05:00:00Z",
            "weekend",
        ),
        open_day(
            "2016-01-04",
            "2016-01-04T05:00:00Z",
            "2016-01-05T05:00:00Z",
            "XNYS:2016-01-04",
        ),
        open_day(
            "2016-01-05",
            "2016-01-05T05:00:00Z",
            "2016-01-06T05:00:00Z",
            "XNYS:2016-01-05",
        ),
    ];
    let mut instances = Vec::new();
    instances.extend(phases(
        "XNYS:2015-12-31",
        "2015-12-31",
        "2015-12-31T09:00:00Z",
        "2015-12-31T14:30:00Z",
        "2015-12-31T21:00:00Z",
        "2016-01-01T01:00:00Z",
    ));
    instances.extend(phases(
        "XNYS:2016-01-04",
        "2016-01-04",
        "2016-01-04T09:00:00Z",
        "2016-01-04T14:30:00Z",
        "2016-01-04T21:00:00Z",
        "2016-01-05T01:00:00Z",
    ));
    instances.extend(phases(
        "XNYS:2016-01-05",
        "2016-01-05",
        "2016-01-05T09:00:00Z",
        "2016-01-05T14:30:00Z",
        "2016-01-05T21:00:00Z",
        "2016-01-06T01:00:00Z",
    ));
    let calendar = snapshot(days, instances);
    let caps = capabilities();
    let request_start = "2016-01-01T01:00:00Z"; // 2015-12-31 20:00 ET; no trusted phase remains.
    let request_end = "2016-01-01T05:00:00Z"; // Exact NY civil-day cutoff.
    assert!(matches!(
        plan_sessions(request(&calendar, &caps, request_start, request_end, "2016-01-05T14:40:00Z", DataInterval::Minute(1), SourceMode::Auto, false)),
        Err(PlanError::UnsupportedPre2016Intraday { first_trade_date }) if first_trade_date == date("2015-12-31")
    ));
    let downgraded = plan_sessions(request(
        &calendar,
        &caps,
        request_start,
        request_end,
        "2016-01-05T14:40:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        true,
    ))
    .unwrap();
    assert_eq!(downgraded.effective_interval, DataInterval::Daily);
    assert_eq!(
        downgraded.downgrade_reason,
        Some(DowngradeReason::Pre2016History)
    );
    assert_eq!(downgraded.slices.len(), 1);
    assert_eq!(downgraded.slices[0].trade_date, date("2015-12-31"));
    assert_eq!(
        downgraded.slices[0].source,
        Some(PlannedSource::YFinanceDailyResearch)
    );
    assert_eq!(
        downgraded.slices[0].source_label.as_deref(),
        Some(YFINANCE_DAILY_RESEARCH_LABEL)
    );

    let crosses_cutoff = plan_sessions(request(
        &calendar,
        &caps,
        "2016-01-01T04:59:00Z",
        "2016-01-01T05:01:00Z",
        "2016-01-05T14:40:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        true,
    ))
    .unwrap();
    assert_eq!(crosses_cutoff.effective_interval, DataInterval::Daily);
    assert!(
        crosses_cutoff
            .slices
            .iter()
            .all(|slice| slice.interval == DataInterval::Daily)
    );
}

#[test]
fn cutoff_uses_requested_utc_time_when_an_overnight_instance_has_a_2016_trade_date() {
    // This isolated planner fixture models the identity edge case raised in review: an OVER
    // instance can carry the next NY trade date while beginning at 20:00 on the prior date.
    // It is not part of the XNYS runtime snapshot, which supplies no OVER phase.
    let days = vec![
        open_day(
            "2015-12-31",
            "2015-12-31T05:00:00Z",
            "2016-01-01T05:00:00Z",
            "fixture:2015-12-31",
        ),
        open_day(
            "2016-01-01",
            "2016-01-01T05:00:00Z",
            "2016-01-02T05:00:00Z",
            "fixture:2016-01-01",
        ),
        closed_day(
            "2016-01-02",
            "2016-01-02T05:00:00Z",
            "2016-01-03T05:00:00Z",
            "weekend",
        ),
    ];
    let mut instances = phases(
        "fixture:2015-12-31",
        "2015-12-31",
        "2015-12-31T09:00:00Z",
        "2015-12-31T14:30:00Z",
        "2015-12-31T21:00:00Z",
        "2016-01-01T01:00:00Z",
    )
    .to_vec();
    instances.push(instance(
        "fixture:2016-01-01",
        "fixture:2016-01-01-over",
        "2016-01-01",
        SessionPhase::Over,
        "2016-01-01T01:00:00Z",
        "2016-01-01T09:00:00Z",
    ));
    let mut calendar = snapshot(days, instances);
    calendar.source_id = "fixture-cross-midnight".to_owned();
    let caps = capabilities();

    let strict = request(
        &calendar,
        &caps,
        "2016-01-01T01:00:00Z",
        "2016-01-01T05:00:00Z",
        "2016-01-01T03:00:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    );
    assert!(matches!(
        plan_sessions(strict),
        Err(PlanError::UnsupportedPre2016Intraday { first_trade_date })
            if first_trade_date == date("2015-12-31")
    ));

    let downgraded = plan_sessions(request(
        &calendar,
        &caps,
        "2016-01-01T01:00:00Z",
        "2016-01-01T05:00:00Z",
        "2016-01-01T03:00:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        true,
    ))
    .unwrap();
    assert_eq!(downgraded.effective_interval, DataInterval::Daily);
    assert_eq!(downgraded.slices.len(), 1);
    assert_eq!(downgraded.slices[0].trade_date, date("2015-12-31"));
    assert_eq!(
        downgraded.slices[0].source,
        Some(PlannedSource::YFinanceDailyResearch)
    );
}

#[test]
fn research_mode_is_daily_only_and_labeled_and_source_modes_stay_strict() {
    let calendar = xnys_week_snapshot();
    let caps = capabilities();
    assert_eq!(
        plan_sessions(request(
            &calendar,
            &caps,
            "2026-01-05T05:00:00Z",
            "2026-01-06T05:00:00Z",
            "2026-01-05T14:40:00Z",
            DataInterval::Minute(1),
            SourceMode::Research,
            false
        )),
        Err(PlanError::UnsupportedResearchIntraday)
    );
    let research = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T05:00:00Z",
        "2026-01-06T05:00:00Z",
        "2026-01-05T14:40:00Z",
        DataInterval::Minute(1),
        SourceMode::Research,
        true,
    ))
    .unwrap();
    assert!(research.slices.iter().all(|slice| slice.source
        == Some(PlannedSource::YFinanceDailyResearch)
        && slice.source_label.as_deref() == Some(YFINANCE_DAILY_RESEARCH_LABEL)
        && !slice.realtime));

    let alpaca = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T09:00:00Z",
        "2026-01-06T01:00:00Z",
        "2026-01-05T22:30:00Z",
        DataInterval::Minute(1),
        SourceMode::AlpacaOnly,
        false,
    ))
    .unwrap();
    assert!(alpaca.slices.iter().all(|slice| {
        slice
            .source
            .is_none_or(|source| source.provider() == Provider::Alpaca)
    }));
    let futu = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T09:00:00Z",
        "2026-01-06T01:00:00Z",
        "2026-01-05T22:30:00Z",
        DataInterval::Minute(1),
        SourceMode::FutuOnly,
        false,
    ))
    .unwrap();
    assert!(futu.slices.iter().all(|slice| {
        slice
            .source
            .is_none_or(|source| source.provider() == Provider::Futu)
    }));
}

#[test]
fn missing_source_capability_is_an_explicit_gap() {
    let calendar = xnys_week_snapshot();
    let caps = capabilities()
        .into_iter()
        .filter(|capability| {
            !(capability.provider == Provider::Alpaca && capability.feed == Some(AlpacaFeed::Sip))
        })
        .collect::<Vec<_>>();
    let plan = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T09:00:00Z",
        "2026-01-05T14:30:00Z",
        "2026-01-05T22:30:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    let old_pre = plan
        .slices
        .iter()
        .find(|slice| slice.session_instance_id.as_deref() == Some("XNYS:2026-01-05-pre"))
        .unwrap();
    assert_eq!(
        old_pre.source,
        Some(PlannedSource::Alpaca {
            feed: AlpacaFeed::Sip
        })
    );
    assert_eq!(old_pre.gap, Some(GapReason::MissingProviderCapability));
    assert!(!old_pre.realtime);
}

#[test]
fn realtime_only_capability_cannot_cover_a_slice_that_ends_before_as_of() {
    let calendar = xnys_week_snapshot();
    let caps = [SourceCapability {
        provider: Provider::Futu,
        feed: None,
        phase: Some(SessionPhase::Regular),
        interval: DataInterval::Minute(1),
        historical: false,
        realtime: true,
    }];
    let prior_window = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:35:00Z",
        "2026-01-05T14:40:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert_eq!(
        prior_window.slices[0].gap,
        Some(GapReason::MissingProviderCapability)
    );
    assert!(!prior_window.slices[0].realtime);

    let current_window = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:45:00Z",
        "2026-01-05T14:40:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert_eq!(current_window.slices[0].gap, None);
    assert!(current_window.slices[0].realtime);
}

#[test]
fn daily_request_emits_one_ny_date_segment_and_references_the_active_phase() {
    let calendar = xnys_week_snapshot();
    let caps = capabilities();
    let plan = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T05:00:00Z",
        "2026-01-06T05:00:00Z",
        "2026-01-05T14:40:00Z",
        DataInterval::Daily,
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert_eq!(plan.slices.len(), 1);
    assert_eq!(plan.slices[0].phase, None);
    assert_eq!(
        plan.slices[0].session_instance_id.as_deref(),
        Some("XNYS:2026-01-05-regular")
    );
    assert_eq!(plan.slices[0].rank, Some(SessionRank::New));
    assert_eq!(plan.slices[0].source, Some(PlannedSource::Futu));
    assert!(plan.slices[0].provisional);
}

#[test]
fn calendar_snapshot_json_rejects_unknown_fields_and_invalid_bounds() {
    let calendar = xnys_week_snapshot();
    let mut serialized = serde_json::to_value(calendar).expect("serialize snapshot");
    serialized["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<TrustedCalendarSnapshot>(serialized).is_err());

    let mut invalid = xnys_week_snapshot();
    invalid.days.pop();
    assert_eq!(
        validate_calendar_snapshot(&invalid),
        Err(PlanError::InvalidCalendarCoverage)
    );
}

#[test]
fn parse_calendar_snapshot_checks_operator_pin_and_source_metadata() {
    let calendar = xnys_week_snapshot();
    let bytes = serde_json::to_vec(&calendar).unwrap();
    let pin = hex::encode(Sha256::digest(&bytes));
    assert_eq!(parse_calendar_snapshot(&bytes, &pin).unwrap(), calendar);
    assert_eq!(
        parse_calendar_snapshot(&bytes, &"0".repeat(64)),
        Err(CalendarArtifactError::SnapshotSha256Mismatch)
    );
}

#[test]
fn an_open_day_without_a_trusted_phase_is_reported_as_a_gap_not_a_live_route() {
    let calendar = snapshot(
        vec![open_day(
            "2026-01-05",
            "2026-01-05T05:00:00Z",
            "2026-01-06T05:00:00Z",
            "XNYS:2026-01-05",
        )],
        Vec::new(),
    );
    let caps = capabilities();
    let plan = plan_sessions(request(
        &calendar,
        &caps,
        "2026-01-05T05:00:00Z",
        "2026-01-06T05:00:00Z",
        "2026-01-05T14:40:00Z",
        DataInterval::Minute(1),
        SourceMode::Auto,
        false,
    ))
    .unwrap();
    assert_eq!(plan.active_session_instance_id, None);
    assert_eq!(plan.slices.len(), 1);
    assert_eq!(plan.slices[0].gap, Some(GapReason::MissingCalendarPhase));
    assert!(plan.slices[0].source.is_none());
    assert!(!plan.slices[0].realtime);
}
