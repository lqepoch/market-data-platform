use super::*;
use crate::queue::CollectionMessage;
#[cfg(feature = "benchmark-snappy")]
use chrono::{DateTime, SecondsFormat, Utc};
use market_contracts::{
    DecimalString, EntitlementState, EventMetadataV1, MarketDataSourceV1, MarketEventEnvelopeV1,
    MarketEventParquetRowV2, MarketEventV1, NumericEncodingV1, RawFrameDispositionV1,
    RawFrameReferenceV2, RawFrameStorageRecordV1,
};
#[cfg(feature = "benchmark-snappy")]
use parquet::basic::Compression;
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, basic::CompressionCodec};
use sha2::{Digest, Sha256};
#[cfg(feature = "benchmark-snappy")]
use std::time::Instant;
use tempfile::tempdir;

#[path = "../../tests/common/parquet_page_bomb.rs"]
mod page_bomb_fixture;
pub(crate) use page_bomb_fixture::mutate_dictionary_page_to_memory_bomb;

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

pub(crate) fn event_with_record_id(sequence: u64, source_record_id: String) -> CollectionMessage {
    let timestamp = UtcTimestamp::parse("2026-10-08T13:30:15Z").unwrap();
    let source = MarketDataSourceV1::new(
        "synthetic",
        "synthetic",
        EntitlementState::Unknown,
        NumericEncodingV1::DecimalToken,
        Some(source_record_id),
    )
    .unwrap();
    CollectionMessage::Market(MarketEventEnvelopeV1 {
        metadata: EventMetadataV1 {
            schema_version: 1,
            source,
            generation: 1,
            sequence,
            raw_frame_sha256: None,
            source_timestamp: Some(timestamp.clone()),
            received_timestamp: timestamp,
        },
        event: MarketEventV1::StockTrade {
            symbol: "QQQ".into(),
            price: DecimalString::new("600.25").unwrap(),
            size: DecimalString::new("1").unwrap(),
        },
    })
}

fn raw_frame_record(
    frame_sequence: u64,
    frame_bytes: Vec<u8>,
    event_count: u32,
    disposition: RawFrameDispositionV1,
    symbols_json: &str,
) -> RawFrameStorageRecordV1 {
    let digest = Sha256::digest(&frame_bytes);
    RawFrameStorageRecordV1 {
        schema_version: 1,
        provider: "synthetic".to_owned(),
        feed: "synthetic".to_owned(),
        entitlement: EntitlementState::Unknown,
        source_numeric_encoding: (event_count > 0).then_some(NumericEncodingV1::DecimalToken),
        generation: 1,
        frame_sequence,
        received_timestamp_utc: UtcTimestamp::parse("2026-10-08T14:30:00Z").unwrap(),
        frame_sha256: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
        frame_bytes,
        event_count,
        disposition,
        symbols_json: symbols_json.to_owned(),
    }
}

fn event_v2_row(frame: &RawFrameStorageRecordV1, ordinal: u32) -> MarketEventParquetRowV2 {
    MarketEventParquetRowV2 {
        event: MarketEventEnvelopeV1 {
            metadata: EventMetadataV1 {
                schema_version: 1,
                source: MarketDataSourceV1::new(
                    "synthetic",
                    "synthetic",
                    EntitlementState::Unknown,
                    NumericEncodingV1::DecimalToken,
                    None,
                )
                .unwrap(),
                generation: frame.generation,
                sequence: u64::from(ordinal),
                raw_frame_sha256: Some(frame.frame_sha256.clone()),
                source_timestamp: Some(frame.received_timestamp_utc.clone()),
                received_timestamp: frame.received_timestamp_utc.clone(),
            },
            event: MarketEventV1::OptionTrade {
                symbol: "QQQ   261016C00600000".to_owned(),
                price: DecimalString::new("1.25").unwrap(),
                size: DecimalString::new("1").unwrap(),
            },
        },
        raw_frame_reference: Some(RawFrameReferenceV2 {
            raw_frame_generation: frame.generation,
            raw_frame_sequence: frame.frame_sequence,
            raw_frame_event_ordinal: ordinal,
            raw_frame_event_count: frame.event_count,
        }),
    }
}

#[test]
fn core_raw_frame_and_event_v2_schemas_roundtrip_exact_bytes_and_complete_references() {
    let temp = tempdir().unwrap();
    let raw_path = temp.path().join("capture-raw.parquet");
    let event_path = temp.path().join("capture-events.parquet");
    let frame = raw_frame_record(
        1,
        vec![
            0x92, 0xa5, b't', b'r', b'a', b'd', b'e', 0xa4, b'1', b'.', b'2', b'5',
        ],
        1,
        RawFrameDispositionV1::MarketData,
        r#"["QQQ   261016C00600000"]"#,
    );
    let raw =
        write_raw_frames_with_limit(&raw_path, std::slice::from_ref(&frame), 1024 * 1024).unwrap();
    assert_eq!(raw.schema_id, RAW_FRAME_SCHEMA_ID);
    assert_eq!(
        raw.source.numeric_encoding,
        NumericEncodingV1::RawMessagePackBytes
    );
    assert_eq!(raw.source_timestamp_missing_rows, 1);
    assert_eq!(raw.time_range, None);
    assert_eq!(raw.symbols, ["QQQ   261016C00600000"]);
    let decoded_frames = read_capture_raw_frames(&raw_path, 1024 * 1024).unwrap();
    assert_eq!(decoded_frames.as_slice(), std::slice::from_ref(&frame));

    let event = event_v2_row(&frame, 1);
    let events =
        write_event_v2_with_limit(&event_path, std::slice::from_ref(&event), 1024 * 1024).unwrap();
    assert_eq!(events.schema_id, EVENT_SCHEMA_V2_ID);
    assert_eq!(events.footer_rows, 1);
    assert_eq!(
        verify_event_v2_against_raw(&raw_path, &event_path, 1024 * 1024).unwrap(),
        RawEventCorrelationVerification {
            raw_frame_rows: 1,
            event_rows: 1,
        }
    );
}

#[test]
fn raw_diagnostics_allow_empty_symbol_frames_when_capture_union_is_nonempty() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("capture-raw.parquet");
    let diagnostic = raw_frame_record(
        1,
        b"unknown message".to_vec(),
        0,
        RawFrameDispositionV1::UnknownMessage,
        "[]",
    );
    let market = raw_frame_record(
        2,
        b"synthetic market message".to_vec(),
        1,
        RawFrameDispositionV1::MarketData,
        r#"["QQQ   261016C00600000"]"#,
    );
    let verification =
        write_raw_frames_with_limit(&path, &[diagnostic, market], 1024 * 1024).unwrap();
    assert_eq!(verification.footer_rows, 2);
    assert_eq!(verification.symbols, ["QQQ   261016C00600000"]);
}

#[test]
fn messagepack_raw_schema_rejects_sip_and_indicative_source_relabeling() {
    let temp = tempdir().unwrap();
    for feed in ["sip", "indicative"] {
        let path = temp.path().join(format!("{feed}-raw.parquet"));
        let mut frame = raw_frame_record(
            1,
            b"synthetic-only bytes".to_vec(),
            1,
            RawFrameDispositionV1::MarketData,
            r#"["QQQ"]"#,
        );
        frame.provider = "alpaca".to_owned();
        frame.feed = feed.to_owned();
        assert!(matches!(
            write_raw_frames_with_limit(&path, &[frame], 1024 * 1024),
            Err(MarketDataError::Contract)
        ));
        assert!(!path.exists());
    }
}

#[test]
fn event_v2_pair_rejects_missing_expected_event_ordinals() {
    let temp = tempdir().unwrap();
    let raw_path = temp.path().join("capture-raw.parquet");
    let event_path = temp.path().join("capture-events.parquet");
    let frame = raw_frame_record(
        1,
        b"two synthetic trades".to_vec(),
        2,
        RawFrameDispositionV1::MarketData,
        r#"["QQQ   261016C00600000"]"#,
    );
    write_raw_frames_with_limit(&raw_path, std::slice::from_ref(&frame), 1024 * 1024).unwrap();
    let event = event_v2_row(&frame, 1);
    write_event_v2_with_limit(&event_path, &[event], 1024 * 1024).unwrap();
    assert!(matches!(
        verify_event_v2_against_raw(&raw_path, &event_path, 1024 * 1024),
        Err(MarketDataError::IncompleteWindow)
    ));
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
    let parquet = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    let schema_metadata = parquet.schema().metadata();
    let footer_metadata = parquet
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    for key in [
        SCHEMA_DESCRIPTOR_METADATA_KEY,
        SCHEMA_FINGERPRINT_METADATA_KEY,
    ] {
        let arrow_value = schema_metadata.get(key).unwrap();
        let footer_value = footer_metadata
            .iter()
            .find(|entry| entry.key == key)
            .and_then(|entry| entry.value.as_ref())
            .unwrap();
        assert_eq!(arrow_value, footer_value);
    }

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
fn identical_rows_write_identical_parquet_bytes_and_sorted_footer_metadata() {
    let temp = tempdir().unwrap();
    let first = temp.path().join("first.parquet");
    let rows = [valid_bar()];
    write_bars(&first, &rows).unwrap();
    let expected_bytes = fs::read(&first).unwrap();
    for index in 0..8 {
        let repeated = temp.path().join(format!("repeat-{index}.parquet"));
        write_bars(&repeated, &rows).unwrap();
        assert_eq!(fs::read(&repeated).unwrap(), expected_bytes);
    }

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(first).unwrap()).unwrap();
    let keys = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .iter()
        .map(|entry| entry.key.as_str())
        .filter(|key| {
            *key == SCHEMA_DESCRIPTOR_METADATA_KEY || *key == SCHEMA_FINGERPRINT_METADATA_KEY
        })
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            SCHEMA_DESCRIPTOR_METADATA_KEY,
            SCHEMA_FINGERPRINT_METADATA_KEY
        ]
    );
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

#[test]
fn parquet_writes_use_zstd_and_bounded_row_groups() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("zstd.parquet");
    write_bars(&path, &[valid_bar()]).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    assert_eq!(builder.metadata().num_row_groups(), 1);
    assert!(builder.metadata().row_groups()[0].total_byte_size() > 0);
    assert!(
        builder.metadata().row_groups()[0]
            .columns()
            .iter()
            .all(|column| column.compression_codec() == CompressionCodec::ZSTD)
    );
}

#[test]
fn decode_budget_rejects_expanded_footer_before_building_arrow_reader() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("bounded.parquet");
    write_bars(&path, &[valid_bar()]).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let mut metadata = builder.metadata().as_ref().clone();
    let mut file_builder = metadata.into_builder();
    let mut row_groups = file_builder.take_row_groups();
    let mut row_group_builder = row_groups.remove(0).into_builder();
    let mut columns = row_group_builder.take_columns();
    let expanded_column_size = i64::try_from(MAX_PARQUET_ROW_GROUP_UNCOMPRESSED_BYTES + 1).unwrap();
    columns[0] = columns[0]
        .clone()
        .into_builder()
        .set_total_uncompressed_size(expanded_column_size)
        .build()
        .unwrap();
    let expanded_group_size = columns
        .iter()
        .map(|column| column.uncompressed_size())
        .sum::<i64>();
    let expanded_group = row_group_builder
        .set_column_metadata(columns)
        .set_total_byte_size(expanded_group_size)
        .build()
        .unwrap();
    row_groups.push(expanded_group);
    metadata = file_builder.set_row_groups(row_groups).build();
    assert!(matches!(
        validate_decode_budget(&metadata),
        Err(MarketDataError::InputLimit)
    ));
}

#[test]
fn decode_budget_rejects_negative_and_cumulative_footer_sizes() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("footer-budget.parquet");
    write_bars(&path, &[valid_bar()]).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let original = builder.metadata().as_ref().clone();

    let mut negative_file = original.clone().into_builder();
    let mut negative_groups = negative_file.take_row_groups();
    let negative_group = negative_groups.remove(0).into_builder();
    let negative_group = negative_group.set_total_byte_size(-1).build().unwrap();
    negative_groups.push(negative_group);
    let negative_metadata = negative_file.set_row_groups(negative_groups).build();
    assert!(matches!(
        validate_decode_budget(&negative_metadata),
        Err(MarketDataError::Parquet)
    ));

    let mut negative_column_file = original.clone().into_builder();
    let mut negative_column_groups = negative_column_file.take_row_groups();
    let mut negative_column_group = negative_column_groups.remove(0).into_builder();
    let mut negative_columns = negative_column_group.take_columns();
    negative_columns[0] = negative_columns[0]
        .clone()
        .into_builder()
        .set_total_uncompressed_size(-1)
        .build()
        .unwrap();
    let negative_column_group = negative_column_group
        .set_column_metadata(negative_columns)
        .build()
        .unwrap();
    negative_column_groups.push(negative_column_group);
    let negative_column_metadata = negative_column_file
        .set_row_groups(negative_column_groups)
        .build();
    assert!(matches!(
        validate_decode_budget(&negative_column_metadata),
        Err(MarketDataError::Parquet)
    ));

    let base_group = original.row_groups()[0].clone();
    let cumulative_file = original.into_builder();
    let mut cumulative_groups = Vec::new();
    for _ in 0..17 {
        let mut group_builder = base_group.clone().into_builder();
        let mut columns = group_builder.take_columns();
        columns[0] = columns[0]
            .clone()
            .into_builder()
            .set_total_uncompressed_size(
                i64::try_from(MAX_PARQUET_ROW_GROUP_UNCOMPRESSED_BYTES).unwrap(),
            )
            .build()
            .unwrap();
        for column in columns.iter_mut().skip(1) {
            *column = column
                .clone()
                .into_builder()
                .set_total_uncompressed_size(0)
                .build()
                .unwrap();
        }
        let group = group_builder
            .set_column_metadata(columns)
            .set_total_byte_size(i64::try_from(MAX_PARQUET_ROW_GROUP_UNCOMPRESSED_BYTES).unwrap())
            .build()
            .unwrap();
        cumulative_groups.push(group);
    }
    let cumulative_metadata = cumulative_file.set_row_groups(cumulative_groups).build();
    assert!(matches!(
        validate_decode_budget(&cumulative_metadata),
        Err(MarketDataError::InputLimit)
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn hostile_parquet_page_header_isolated_and_worker_is_reaped() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let temp = tempdir().unwrap();
    let path = temp.path().join("page-header-bomb.parquet");
    let messages = (1_u64..=10_000)
        .map(|sequence| event_with_record_id(sequence, format!("r{:0>127}", sequence)))
        .collect::<Vec<_>>();
    write_events(&path, &messages).unwrap();
    mutate_dictionary_page_to_memory_bomb(&path);

    let pid_path = temp.path().join("worker.pid");
    let _pid_guard = crate::parquet_worker::track_worker_pid(&pid_path);
    let result = crate::parquet_worker::verify(&path, EVENT_SCHEMA_ID, u64::MAX);
    assert!(matches!(result, Err(MarketDataError::Parquet)));
    assert!(path.exists(), "caller remains alive after worker rejection");

    let process_id = fs::read_to_string(pid_path)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    let pid = rustix::process::Pid::from_raw(process_id).unwrap();
    assert!(matches!(
        rustix::process::kill_process(pid, rustix::process::Signal::KILL),
        Err(rustix::io::Errno::SRCH)
    ));
}

#[cfg(feature = "benchmark-snappy")]
#[test]
#[ignore = "explicit 100k-row codec and readback benchmark; run with --release --nocapture"]
fn benchmark_100k_synthetic_trade_events() {
    let temp = tempdir().unwrap();
    let base = DateTime::parse_from_rfc3339("2026-10-08T13:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let messages = (1_u64..=100_000)
        .map(|sequence| {
            let source_time =
                base + chrono::Duration::milliseconds(i64::try_from(sequence - 1).unwrap());
            let timestamp =
                UtcTimestamp::parse(&source_time.to_rfc3339_opts(SecondsFormat::AutoSi, true))
                    .unwrap();
            let price = format!(
                "{}.{:04}",
                600 + (sequence % 700),
                (sequence * 7919) % 10_000
            );
            CollectionMessage::Market(MarketEventEnvelopeV1 {
                metadata: EventMetadataV1 {
                    schema_version: 1,
                    source: MarketDataSourceV1::new(
                        "synthetic",
                        "synthetic",
                        EntitlementState::Unknown,
                        NumericEncodingV1::DecimalToken,
                        None,
                    )
                    .unwrap(),
                    generation: 1,
                    sequence,
                    raw_frame_sha256: None,
                    source_timestamp: Some(timestamp.clone()),
                    received_timestamp: timestamp,
                },
                event: MarketEventV1::StockTrade {
                    symbol: "QQQ".to_owned(),
                    price: DecimalString::new(&price).unwrap(),
                    size: DecimalString::new((1 + sequence % 1_000).to_string()).unwrap(),
                },
            })
        })
        .collect::<Vec<_>>();

    let cases = [
        ("zstd-1", default_compression()),
        ("snappy", Compression::SNAPPY),
        ("uncompressed", Compression::UNCOMPRESSED),
    ];
    for (label, codec) in cases {
        let path = temp.path().join(format!("{label}.parquet"));
        let started = Instant::now();
        let verification = write_events_with_compression(
            &path,
            &messages,
            crate::archive::DEFAULT_MAX_OBJECT_BYTES,
            codec,
        )
        .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(verification.footer_rows, 100_000);
        assert_eq!(verification.decoded_rows, 100_000);
        println!(
            "codec={label} rows={} bytes={} write_plus_readback_ms={} rows_per_second={:.0}",
            verification.decoded_rows,
            verification.size_bytes,
            elapsed.as_millis(),
            100_000_f64 / elapsed.as_secs_f64(),
        );
    }
}
