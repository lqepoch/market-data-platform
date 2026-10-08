use super::*;
use tempfile::tempdir;

fn valid_bar() -> TradeMinuteBarV1 {
    TradeMinuteBarV1 {
        schema_version: 1,
        source_provider: "synthetic".into(),
        source_feed: "synthetic".into(),
        source_entitlement: "unknown".into(),
        source_numeric_encoding: "decimal_token".into(),
        symbol: "QQQ".into(),
        bar_start_utc: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
        bar_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T13:31:00Z").unwrap(),
        available_at_utc: UtcTimestamp::parse("2026-10-08T13:31:00Z").unwrap(),
        trade_date: "2026-10-08".into(),
        session_id: "test-regular-session".into(),
        session_timezone: "America/New_York".into(),
        session_policy_id: "test-session-policy-v1".into(),
        session_policy_sha256: "a".repeat(64),
        session_start_utc: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
        session_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T20:00:00Z").unwrap(),
        window_start_utc: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
        window_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T13:31:00Z").unwrap(),
        open: "600.25".into(),
        high: "600.25".into(),
        low: "600.25".into(),
        close: "600.25".into(),
        volume: "2".into(),
        trade_count: 1,
        quote_events_excluded: 0,
        source_timestamp_missing_rows: 0,
        sequence_gap_count: 0,
        late_event_count: 0,
        window_expected_minutes: 1,
        window_empty_trade_minutes: 0,
        source_start_utc: UtcTimestamp::parse("2026-10-08T13:30:15Z").unwrap(),
        source_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T13:30:15.000000001Z").unwrap(),
        window_input_eof: true,
        source_pages_exhausted: None,
        completion_mode: "synthetic_eof".into(),
        nbbo_input_status: "excluded".into(),
    }
}

#[test]
fn write_verify_and_query_share_semantic_bar_validation() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("bars.parquet");
    let verification = write_bars(&path, &[valid_bar()]).unwrap();
    assert_eq!(verification.footer_rows, 1);
    assert_eq!(verification.source.provider, "synthetic");
    assert_eq!(verification.symbols, ["QQQ"]);
    assert_eq!(query_bars(&path, Some("QQQ")).unwrap(), [valid_bar()]);
    assert!(query_bars(&path, Some("SPY")).unwrap().is_empty());

    let mut bad_decimal = valid_bar();
    bad_decimal.open = "600.2oops".into();
    let mut bad_date = valid_bar();
    bad_date.trade_date = "2026-02-30".into();
    let mut bad_version = valid_bar();
    bad_version.schema_version = 2;
    let mut bad_range = valid_bar();
    bad_range.source_end_exclusive_utc = UtcTimestamp::parse("2026-10-08T13:30:14Z").unwrap();
    let mut bad_missing_count = valid_bar();
    bad_missing_count.source_timestamp_missing_rows = 1;
    let mut bad_symbol = valid_bar();
    bad_symbol.symbol = "QQQ\nspoof".into();
    let mut bad_session_id = valid_bar();
    bad_session_id.session_id = "s".repeat(257);
    let mut bad_policy_id = valid_bar();
    bad_policy_id.session_policy_id = "policy\nspoof".into();

    for (index, row) in [
        bad_decimal,
        bad_date,
        bad_version,
        bad_range,
        bad_missing_count,
        bad_symbol,
        bad_session_id,
        bad_policy_id,
    ]
    .into_iter()
    .enumerate()
    {
        let rejected_path = temp.path().join(format!("invalid-{index}.parquet"));
        assert!(matches!(
            write_bars(&rejected_path, &[row]),
            Err(MarketDataError::Contract)
        ));
        assert!(!rejected_path.exists());
    }
}

#[test]
fn nonpaged_historical_bar_roundtrips_without_fabricated_page_evidence() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("historical-nonpaged.parquet");
    let mut bar = valid_bar();
    bar.source_provider = "fixture-provider".into();
    bar.source_feed = "sip".into();
    bar.completion_mode = "historical_eof_nonpaged".into();

    let verification = write_bars(&path, &[bar.clone()]).unwrap();
    assert_eq!(verification.footer_rows, 1);
    assert_eq!(query_bars(&path, None).unwrap(), [bar]);

    let paged_path = temp.path().join("historical-paged.parquet");
    let mut paged_bar = valid_bar();
    paged_bar.source_provider = "fixture-provider".into();
    paged_bar.source_feed = "sip".into();
    paged_bar.source_pages_exhausted = Some(true);
    paged_bar.completion_mode = "historical_eof_paged".into();
    write_bars(&paged_path, &[paged_bar.clone()]).unwrap();
    assert_eq!(query_bars(&paged_path, None).unwrap(), [paged_bar]);

    let mut legacy_ambiguous = valid_bar();
    legacy_ambiguous.source_provider = "fixture-provider".into();
    legacy_ambiguous.source_feed = "sip".into();
    legacy_ambiguous.completion_mode = "historical_eof".into();
    assert!(validate_bar_row(&legacy_ambiguous).is_err());
}

#[test]
fn parquet_byte_limit_rejects_and_cleans_temporary_output() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("capped.parquet");
    assert!(matches!(
        write_bars_with_limit(&path, &[valid_bar()], 32),
        Err(MarketDataError::InputLimit)
    ));
    assert!(!path.exists());
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
}
