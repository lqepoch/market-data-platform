//! Bounded offline replay from shared JSON envelopes through Parquet, query, and archive.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use market_contracts::{
    DatasetTimeRangeV1, EntitlementState, MarketDataSourceV1, NumericEncodingV1, UtcTimestamp,
};
use serde::{Deserialize, Serialize};

use crate::{
    MarketDataError, Result,
    aggregate::{CompletionEvidence, CompletionMode, SessionWindow, aggregate_trade_bars},
    archive::{ArchiveLimits, ArchivePublisher, ArchiveRequest, PublicationPurpose, TransportKind},
    config::{AlpacaFeed, DriveConfig},
    parquet_store,
    protocol::{self, DEFAULT_MAX_JSONL_BYTES, DEFAULT_MAX_JSONL_RECORDS},
    queue::{CollectionMessage, CollectionSubmitter},
    storage::LocalTestTransport,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaySessionConfig {
    pub window: SessionWindow,
    pub mode: CompletionMode,
    pub source_pages_exhausted: Option<bool>,
    pub available_at: UtcTimestamp,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReplayReport {
    pub status: &'static str,
    pub provider: String,
    pub feed: String,
    pub entitlement: String,
    pub event_rows: u64,
    pub minute_bar_rows: u64,
    pub event_schema_sha256: String,
    pub minute_bar_schema_sha256: String,
    pub local_manifest_count: usize,
    pub bars_exported: usize,
    pub alpaca_connection: &'static str,
    pub google_drive_upload: &'static str,
    pub research_readiness: &'static str,
}

#[derive(Clone)]
pub enum OutputTransport {
    LocalTest { root: PathBuf },
    RcloneGoogleDrive { config: DriveConfig },
}

#[derive(Clone, Debug)]
pub struct ReplayOptions {
    pub dataset_prefix: String,
    pub purpose: PublicationPurpose,
    pub limits: ArchiveLimits,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            dataset_prefix: "replay".into(),
            purpose: PublicationPurpose::Diagnostic,
            limits: ArchiveLimits::default(),
        }
    }
}

pub async fn synthetic_replay(output: &Path) -> Result<ReplayReport> {
    fs::create_dir_all(output)?;
    let input = output.join("synthetic-input.jsonl");
    write_synthetic_input(&input)?;
    let config = ReplaySessionConfig {
        window: SessionWindow {
            trade_date: "2026-10-08".into(),
            session_id: "synthetic-regular-2026-10-08".into(),
            timezone: "America/New_York".into(),
            policy_id: "synthetic-session-policy-v1".into(),
            policy_sha256: "a".repeat(64),
            session_start: parse_time("2026-10-08T13:30:00Z")?,
            session_end_exclusive: parse_time("2026-10-08T20:00:00Z")?,
            window_start: parse_time("2026-10-08T13:30:00Z")?,
            window_end_exclusive: parse_time("2026-10-08T13:34:00Z")?,
            expected_symbols: vec!["QQQ".into()],
        },
        mode: CompletionMode::SyntheticEof,
        source_pages_exhausted: None,
        available_at: parse_time("2026-10-08T13:34:00Z")?,
    };
    replay_file(
        &input,
        output,
        "synthetic-2026-10-08-four-bars-v1",
        &config,
        OutputTransport::LocalTest {
            root: output.join("local-test-store"),
        },
        &ReplayOptions::default(),
    )
    .await
}

pub async fn replay_file(
    input: &Path,
    output: &Path,
    dataset_id: &str,
    config: &ReplaySessionConfig,
    transport: OutputTransport,
    options: &ReplayOptions,
) -> Result<ReplayReport> {
    if !safe_component(dataset_id)
        || !safe_component(&options.dataset_prefix)
        || !config
            .window
            .expected_symbols
            .iter()
            .all(|symbol| safe_symbol(symbol))
    {
        return Err(MarketDataError::InvalidInput);
    }
    options.limits.validate()?;
    fs::create_dir_all(output)?;
    let input_messages = protocol::read_envelopes_from_path(input)?;
    if input_messages.is_empty()
        || input_messages.len() > DEFAULT_MAX_JSONL_RECORDS
        || fs::metadata(input)?.len() > DEFAULT_MAX_JSONL_BYTES
    {
        return Err(MarketDataError::InputLimit);
    }

    let (submitter, mut receiver) = CollectionSubmitter::bounded_with_limits(
        format!("{dataset_id}-gaps"),
        256,
        64,
        64 * 1024 * 1024,
        output
            .join("state")
            .join(format!("{dataset_id}.gaps.jsonl")),
    )?;
    let consumer = tokio::spawn(async move {
        let mut messages = Vec::new();
        while let Some(message) = receiver.recv().await {
            messages.push(message);
        }
        messages
    });
    for message in input_messages {
        submitter.submit(message).await?;
    }
    let has_gaps = submitter.has_gaps()?;
    drop(submitter);
    let collected = consumer.await.map_err(|_| MarketDataError::QueueClosed)?;
    if has_gaps {
        return Err(MarketDataError::IncompleteWindow);
    }
    if collected.len() > DEFAULT_MAX_JSONL_RECORDS {
        return Err(MarketDataError::InputLimit);
    }

    let market_messages = collected
        .iter()
        .filter(|message| matches!(message, CollectionMessage::Market(_)))
        .cloned()
        .collect::<Vec<_>>();
    let source = single_source(&market_messages)?;
    let bars = aggregate_trade_bars(
        &collected,
        &config.window,
        &CompletionEvidence {
            mode: config.mode,
            input_eof: true,
            source_pages_exhausted: config.source_pages_exhausted,
            available_at: config.available_at.clone(),
        },
    )?;

    let staging = output.join("staging");
    let state = output.join("state");
    let export = output.join("exports");
    fs::create_dir_all(&staging)?;
    fs::create_dir_all(&export)?;
    let event_dataset_id = format!("{dataset_id}-events-v1");
    let bar_dataset_id = format!("{dataset_id}-bars-1m-v1");
    let event_path = staging.join(format!("{event_dataset_id}.parquet"));
    let bar_path = staging.join(format!("{bar_dataset_id}.parquet"));
    let event_verification = parquet_store::write_events(&event_path, &collected)?;
    let bar_verification = parquet_store::write_bars(&bar_path, &bars)?;

    let source_range = event_source_range(&market_messages)?;
    let event_missing = market_messages
        .iter()
        .filter(|message| message.metadata().source_timestamp.is_none())
        .count() as u64;
    let purpose = options.purpose;
    let (publisher, transport_kind) = match transport {
        OutputTransport::LocalTest { root } => (
            ArchivePublisher::local_test(
                LocalTestTransport::new(root)?,
                &state,
                &staging,
                options.limits.clone(),
            )?,
            TransportKind::LocalTest,
        ),
        OutputTransport::RcloneGoogleDrive { config } => (
            ArchivePublisher::rclone_drive(config, &state, &staging, options.limits.clone())?,
            TransportKind::RcloneGoogleDrive,
        ),
    };
    if matches!(transport_kind, TransportKind::RcloneGoogleDrive)
        && purpose != PublicationPurpose::Curated
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    let event_request = ArchiveRequest {
        dataset_id: event_dataset_id.clone(),
        object_name: format!("{event_dataset_id}.parquet"),
        schema_id: crate::schema::EVENT_SCHEMA_ID.to_owned(),
        purpose,
        source: source.clone(),
        symbols: config.window.expected_symbols.clone(),
        time_range: source_range.clone(),
        source_timestamp_missing_rows: event_missing,
        row_count: event_verification.footer_rows,
        source_pages_exhausted: config.source_pages_exhausted,
        input_eof: true,
        parquet_path: event_path,
    };
    let bars_source_range = bar_source_range(&bars)?;
    let bar_request = ArchiveRequest {
        dataset_id: bar_dataset_id.clone(),
        object_name: format!("{bar_dataset_id}.parquet"),
        schema_id: crate::schema::MINUTE_BAR_SCHEMA_ID.to_owned(),
        purpose,
        source: source.clone(),
        symbols: config.window.expected_symbols.clone(),
        time_range: bars_source_range,
        source_timestamp_missing_rows: 0,
        row_count: bar_verification.footer_rows,
        source_pages_exhausted: config.source_pages_exhausted,
        input_eof: true,
        parquet_path: bar_path.clone(),
    };
    let archive_queue =
        crate::archive::ArchiveWriterQueue::spawn(publisher, options.limits.upload_queue_capacity)?;
    let event_manifest = archive_queue.submit(event_request).await?;
    let bars_manifest = archive_queue.submit(bar_request).await?;
    event_manifest
        .validate()
        .map_err(|_| MarketDataError::Contract)?;
    bars_manifest
        .validate()
        .map_err(|_| MarketDataError::Contract)?;

    let queried = parquet_store::query_bars(&bar_path, None)?;
    let export_path = export.join("bars.jsonl");
    let bars_exported = parquet_store::export_bars_jsonl(&bar_path, &export_path, None)?;
    if queried.len() != bars.len() || bars_exported != bars.len() {
        return Err(MarketDataError::Parquet);
    }
    let report = ReplayReport {
        status: "synthetic_or_offline_replay_only",
        provider: source.provider.clone(),
        feed: source.feed.clone(),
        entitlement: enum_text(source.entitlement),
        event_rows: event_verification.footer_rows,
        minute_bar_rows: bar_verification.footer_rows,
        event_schema_sha256: event_verification.schema_sha256,
        minute_bar_schema_sha256: bar_verification.schema_sha256,
        local_manifest_count: 2,
        bars_exported,
        alpaca_connection: "NOTRUN",
        google_drive_upload: if transport_kind == TransportKind::LocalTest {
            "NOTRUN"
        } else {
            "verified"
        },
        research_readiness: "UNVERIFIED",
    };
    write_json_new(&output.join("replay-report.json"), &report)?;
    Ok(report)
}

fn write_synthetic_input(path: &Path) -> Result<()> {
    let source = MarketDataSourceV1::new(
        "synthetic",
        "synthetic",
        EntitlementState::Unknown,
        NumericEncodingV1::DecimalToken,
        None,
    )
    .map_err(|_| MarketDataError::Contract)?;
    let rows = [
        market_event(
            &source,
            1,
            "2026-10-08T13:30:10Z",
            "2026-10-08T13:30:10.1Z",
            MarketDataEvent::Trade("10.000", "2"),
        )?,
        market_event(
            &source,
            2,
            "2026-10-08T13:30:20Z",
            "2026-10-08T13:30:20.1Z",
            MarketDataEvent::Quote("9.9", "10.1"),
        )?,
        market_event(
            &source,
            3,
            "2026-10-08T13:30:50Z",
            "2026-10-08T13:30:50.1Z",
            MarketDataEvent::Trade("10.500", "3"),
        )?,
        market_event(
            &source,
            4,
            "2026-10-08T13:31:03Z",
            "2026-10-08T13:31:03.1Z",
            MarketDataEvent::Trade("10.250", "1"),
        )?,
        market_event(
            &source,
            5,
            "2026-10-08T13:31:30Z",
            "2026-10-08T13:31:30.1Z",
            MarketDataEvent::Trade("10.750", "4"),
        )?,
        market_event(
            &source,
            6,
            "2026-10-08T13:32:12Z",
            "2026-10-08T13:32:12.1Z",
            MarketDataEvent::Trade("10.625", "2"),
        )?,
        market_event(
            &source,
            7,
            "2026-10-08T13:33:44Z",
            "2026-10-08T13:33:44.1Z",
            MarketDataEvent::Trade("10.875", "3"),
        )?,
    ];
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                MarketDataError::Conflict
            } else {
                MarketDataError::Io(error)
            }
        })?;
    for row in rows {
        serde_json::to_writer(&mut file, &row)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    Ok(())
}

enum MarketDataEvent<'a> {
    Trade(&'a str, &'a str),
    Quote(&'a str, &'a str),
}

fn market_event(
    source: &MarketDataSourceV1,
    sequence: u64,
    source_timestamp: &str,
    received_timestamp: &str,
    value: MarketDataEvent<'_>,
) -> Result<market_contracts::MarketEventEnvelopeV1> {
    let event = match value {
        MarketDataEvent::Trade(price, size) => market_contracts::MarketEventV1::StockTrade {
            symbol: "QQQ".into(),
            price: market_contracts::DecimalString::new(price)
                .map_err(|_| MarketDataError::Contract)?,
            size: market_contracts::DecimalString::new(size)
                .map_err(|_| MarketDataError::Contract)?,
        },
        MarketDataEvent::Quote(bid, ask) => market_contracts::MarketEventV1::StockQuote {
            symbol: "QQQ".into(),
            bid: Some(
                market_contracts::DecimalString::new(bid).map_err(|_| MarketDataError::Contract)?,
            ),
            ask: Some(
                market_contracts::DecimalString::new(ask).map_err(|_| MarketDataError::Contract)?,
            ),
            bid_size: None,
            ask_size: None,
        },
    };
    Ok(market_contracts::MarketEventEnvelopeV1 {
        metadata: market_contracts::EventMetadataV1 {
            schema_version: 1,
            source: source.clone(),
            generation: 1,
            sequence,
            raw_frame_sha256: None,
            source_timestamp: Some(
                UtcTimestamp::parse(source_timestamp).map_err(|_| MarketDataError::Contract)?,
            ),
            received_timestamp: UtcTimestamp::parse(received_timestamp)
                .map_err(|_| MarketDataError::Contract)?,
        },
        event,
    })
}

fn single_source(messages: &[CollectionMessage]) -> Result<MarketDataSourceV1> {
    let mut source: Option<MarketDataSourceV1> = None;
    for message in messages {
        if let CollectionMessage::Market(event) = message {
            let mut candidate = event.metadata.source.clone();
            candidate.source_record_id = None;
            if let Some(existing) = &source {
                if existing != &candidate {
                    return Err(MarketDataError::MixedProvenance);
                }
            } else {
                source = Some(candidate);
            }
        }
    }
    source.ok_or(MarketDataError::IncompleteWindow)
}

fn event_source_range(messages: &[CollectionMessage]) -> Result<Option<DatasetTimeRangeV1>> {
    let mut timestamps = Vec::new();
    for message in messages {
        if let CollectionMessage::Market(event) = message
            && let Some(timestamp) = &event.metadata.source_timestamp
        {
            timestamps.push(
                DateTime::parse_from_rfc3339(timestamp.as_str())
                    .map_err(|_| MarketDataError::Contract)?
                    .with_timezone(&Utc),
            );
        }
    }
    let Some(minimum) = timestamps.iter().min().copied() else {
        return Ok(None);
    };
    let maximum = timestamps
        .iter()
        .max()
        .copied()
        .ok_or(MarketDataError::IncompleteWindow)?;
    let max_ns = maximum
        .timestamp_nanos_opt()
        .ok_or(MarketDataError::InvalidInput)?;
    let end_ns = max_ns.checked_add(1).ok_or(MarketDataError::InvalidInput)?;
    let end = timestamp_from_ns(end_ns)?;
    Ok(Some(DatasetTimeRangeV1 {
        start_inclusive: UtcTimestamp::parse(&minimum.to_rfc3339())
            .map_err(|_| MarketDataError::Contract)?,
        end_exclusive: end,
    }))
}

fn bar_source_range(
    bars: &[crate::aggregate::TradeMinuteBarV1],
) -> Result<Option<DatasetTimeRangeV1>> {
    let start = bars
        .iter()
        .map(|bar| DateTime::parse_from_rfc3339(bar.source_start_utc.as_str()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| MarketDataError::Contract)?
        .into_iter()
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .min();
    let end = bars
        .iter()
        .map(|bar| DateTime::parse_from_rfc3339(bar.source_end_exclusive_utc.as_str()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| MarketDataError::Contract)?
        .into_iter()
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .max();
    match (start, end) {
        (Some(start), Some(end)) => Ok(Some(DatasetTimeRangeV1 {
            start_inclusive: UtcTimestamp::parse(&start.to_rfc3339())
                .map_err(|_| MarketDataError::Contract)?,
            end_exclusive: UtcTimestamp::parse(&end.to_rfc3339())
                .map_err(|_| MarketDataError::Contract)?,
        })),
        (None, None) => Ok(None),
        _ => Err(MarketDataError::Contract),
    }
}

fn timestamp_from_ns(nanos: i64) -> Result<UtcTimestamp> {
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsec = nanos.rem_euclid(1_000_000_000) as u32;
    let value =
        DateTime::<Utc>::from_timestamp(seconds, subsec).ok_or(MarketDataError::InvalidInput)?;
    UtcTimestamp::parse(&value.to_rfc3339()).map_err(|_| MarketDataError::Contract)
}

fn parse_time(value: &str) -> Result<UtcTimestamp> {
    UtcTimestamp::parse(value).map_err(|_| MarketDataError::Contract)
}

fn enum_text<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn write_json_new(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                MarketDataError::Conflict
            } else {
                MarketDataError::Io(error)
            }
        })?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.contains("..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn safe_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

pub fn drive_config_from_env(config_path: PathBuf) -> Result<DriveConfig> {
    let remote = std::env::var("MDP_DRIVE_REMOTE").map_err(|_| MarketDataError::InvalidInput)?;
    let root_folder_id =
        std::env::var("MDP_DRIVE_ROOT_FOLDER_ID").map_err(|_| MarketDataError::InvalidInput)?;
    let dataset_prefix =
        std::env::var("MDP_DRIVE_DATASET_PREFIX").map_err(|_| MarketDataError::InvalidInput)?;
    let config = DriveConfig {
        remote,
        root_folder_id,
        rclone_config: config_path,
        dataset_prefix,
    };
    config.validate()?;
    Ok(config)
}

pub fn primary_feed(feed: AlpacaFeed) -> &'static str {
    feed.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn synthetic_cli_path_runs_full_json_parquet_archive_query_export() {
        let temp = tempdir().unwrap();
        let report = synthetic_replay(temp.path()).await.unwrap();
        assert_eq!(report.status, "synthetic_or_offline_replay_only");
        assert_eq!(report.provider, "synthetic");
        assert_eq!(report.feed, "synthetic");
        assert_eq!(report.entitlement, "unknown");
        assert_eq!(report.event_rows, 7);
        assert_eq!(report.minute_bar_rows, 4);
        assert_eq!(report.bars_exported, 4);
        assert_eq!(report.google_drive_upload, "NOTRUN");
        let bars = parquet_store::query_bars(
            &temp
                .path()
                .join("staging/synthetic-2026-10-08-four-bars-v1-bars-1m-v1.parquet"),
            Some("QQQ"),
        )
        .unwrap();
        assert_eq!(bars[0].quote_events_excluded, 1);
        assert_eq!(bars[0].completion_mode, "synthetic_eof");
    }
}
