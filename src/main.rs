use std::{
    io::Read,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use clap::{Parser, ValueEnum};
use market_data_platform::{
    MarketDataError, Result,
    archive::{
        ArchiveLimits, ArchivePublisher, DEFAULT_MAX_MANIFEST_BYTES, DEFAULT_MAX_OBJECT_BYTES,
        DEFAULT_MAX_STAGING_BYTES, DEFAULT_UPLOAD_QUEUE_CAPACITY,
    },
    parquet_store, parquet_worker,
    pipeline::{self, OutputTransport, ReplayOptions, ReplaySessionConfig},
    remote_query::{
        DEFAULT_MAX_EXPORT_BYTES, DEFAULT_MAX_QUERY_RESULT_BYTES, DEFAULT_REMOTE_CACHE_BYTES,
        DEFAULT_REMOTE_CACHE_ENTRIES, DEFAULT_REMOTE_CACHE_TTL, DatasetNamespace,
        RemoteArchiveReader, RemoteCacheCleaner, RemoteCacheLimits,
    },
    schema::{EVENT_SCHEMA_ID, MINUTE_BAR_SCHEMA_ID},
};

const MAX_SESSION_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "mdp", about = "Offline-first market data pipeline")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Run a fully synthetic JSONL -> Parquet -> archive -> query/export replay.
    Synthetic {
        #[arg(long)]
        output: PathBuf,
        /// Use a distinct four-minute synthetic session whose session window is complete.
        #[arg(long, conflicts_with = "regular_session")]
        full_session: bool,
        /// Use a distinct 390-minute synthetic session; this is not exchange-calendar evidence.
        #[arg(long)]
        regular_session: bool,
        /// Set the date for the synthetic 390-minute session; does not consult an exchange calendar.
        #[arg(long, requires = "regular_session", value_parser = parse_synthetic_date)]
        session_date: Option<NaiveDate>,
    },
    /// Load a bounded shared-contract JSONL file and replay it to the local-test archive.
    ReplayJsonl {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        session_config: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        dataset_id: String,
        #[arg(long)]
        local_test_store: PathBuf,
        #[arg(long, default_value_t = DEFAULT_MAX_OBJECT_BYTES)]
        max_object_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_MAX_MANIFEST_BYTES)]
        max_manifest_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_MAX_STAGING_BYTES)]
        max_staging_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_UPLOAD_QUEUE_CAPACITY)]
        upload_queue_capacity: usize,
    },
    /// Verify a Parquet footer, physical schema, schema fingerprint, and decoded rows.
    Verify {
        #[arg(long)]
        parquet: PathBuf,
        #[arg(long, value_enum)]
        schema: SchemaArg,
    },
    /// Query the bounded one-minute trade-bar Parquet object and optionally export JSONL.
    QueryBars {
        #[arg(long)]
        parquet: PathBuf,
        #[arg(long)]
        symbol: Option<String>,
        #[arg(long)]
        export_jsonl: Option<PathBuf>,
    },
    /// Internal subprocess entrypoint; applies only to bounded Parquet decode workers.
    #[command(hide = true)]
    ParquetWorker {
        #[arg(value_enum)]
        action: ParquetWorkerAction,
        #[arg(long)]
        parquet: PathBuf,
        #[arg(long)]
        schema_id: Option<String>,
        #[arg(long, default_value_t = DEFAULT_MAX_OBJECT_BYTES)]
        max_object_bytes: u64,
        #[arg(long)]
        symbol: Option<String>,
        #[arg(
            long,
            default_value_t = market_data_platform::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize
        )]
        max_rows: usize,
        #[arg(long, default_value_t = DEFAULT_MAX_QUERY_RESULT_BYTES)]
        max_result_bytes: u64,
    },
    /// Read a manifest and Parquet object from rclone, verify them, then query/export locally.
    RemoteQueryBars {
        #[arg(long)]
        dataset_id: String,
        #[arg(long, value_enum)]
        namespace: NamespaceArg,
        #[arg(long)]
        rclone_config: PathBuf,
        #[arg(long)]
        cache_dir: PathBuf,
        #[arg(long)]
        symbol: Option<String>,
        #[arg(long)]
        export_jsonl: Option<PathBuf>,
        #[arg(long, default_value_t = DEFAULT_MAX_OBJECT_BYTES)]
        max_object_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_MAX_MANIFEST_BYTES)]
        max_manifest_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_REMOTE_CACHE_BYTES)]
        max_cache_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_REMOTE_CACHE_ENTRIES)]
        max_cache_entries: usize,
        #[arg(long, default_value_t = DEFAULT_REMOTE_CACHE_TTL.as_secs())]
        cache_ttl_secs: u64,
        #[arg(long, default_value_t = DEFAULT_MAX_QUERY_RESULT_BYTES)]
        max_query_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_MAX_EXPORT_BYTES)]
        max_export_bytes: u64,
    },
    /// Report expired verified cache entries; deletion requires the explicit --apply flag.
    CleanupRemoteCache {
        #[arg(long)]
        cache_dir: PathBuf,
        #[arg(long)]
        apply: bool,
        #[arg(long, default_value_t = DEFAULT_REMOTE_CACHE_TTL.as_secs())]
        cache_ttl_secs: u64,
        #[arg(long, default_value_t = DEFAULT_MAX_OBJECT_BYTES)]
        max_object_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_MAX_MANIFEST_BYTES)]
        max_manifest_bytes: u64,
        #[arg(long, default_value_t = DEFAULT_REMOTE_CACHE_ENTRIES)]
        max_cache_entries: usize,
    },
    /// Report orphaned MDP temporary files; deletion requires the explicit --apply flag.
    CleanupStaging {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        staging_dir: PathBuf,
        #[arg(long)]
        apply: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SchemaArg {
    MarketEventsV1,
    UsEquityTradeBar1mV1,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum NamespaceArg {
    Curated,
    Diagnostic,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ParquetWorkerAction {
    Verify,
    QueryBars,
}

impl NamespaceArg {
    const fn namespace(self) -> DatasetNamespace {
        match self {
            Self::Curated => DatasetNamespace::Curated,
            Self::Diagnostic => DatasetNamespace::Diagnostic,
        }
    }
}

impl SchemaArg {
    const fn schema_id(self) -> &'static str {
        match self {
            Self::MarketEventsV1 => EVENT_SCHEMA_ID,
            Self::UsEquityTradeBar1mV1 => MINUTE_BAR_SCHEMA_ID,
        }
    }
}

fn parse_synthetic_date(value: &str) -> std::result::Result<NaiveDate, String> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| "expected a valid YYYY-MM-DD date".to_owned())?;
    if date.format("%Y-%m-%d").to_string() != value {
        return Err("date must use canonical YYYY-MM-DD format".to_owned());
    }
    Ok(date)
}

// Parquet decode workers run under a 1 GiB address-space limit; a per-core Tokio thread pool
// consumes unnecessary virtual address space in these short-lived CLI processes.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    match Args::parse().command {
        Command::Synthetic {
            output,
            full_session,
            regular_session,
            session_date,
        } => {
            let report = if regular_session {
                match session_date {
                    Some(date) => {
                        pipeline::synthetic_390_minute_session_replay_on(&output, date).await?
                    }
                    None => pipeline::synthetic_390_minute_session_replay(&output).await?,
                }
            } else if full_session {
                pipeline::synthetic_full_session_replay(&output).await?
            } else {
                pipeline::synthetic_replay(&output).await?
            };
            print_json(&report)
        }
        Command::ReplayJsonl {
            input,
            session_config,
            output,
            dataset_id,
            local_test_store,
            max_object_bytes,
            max_manifest_bytes,
            max_staging_bytes,
            upload_queue_capacity,
        } => {
            let config = read_session_config(&session_config)?;
            let options = ReplayOptions {
                limits: ArchiveLimits {
                    max_object_bytes,
                    max_manifest_bytes,
                    max_staging_bytes,
                    upload_queue_capacity,
                },
                ..ReplayOptions::default()
            };
            let report = pipeline::replay_file(
                &input,
                &output,
                &dataset_id,
                &config,
                OutputTransport::LocalTest {
                    root: local_test_store,
                },
                &options,
            )
            .await?;
            print_json(&report)
        }
        Command::Verify { parquet, schema } => {
            let verification =
                parquet_worker::verify(&parquet, schema.schema_id(), DEFAULT_MAX_OBJECT_BYTES)?;
            print_json(&verification)
        }
        Command::QueryBars {
            parquet,
            symbol,
            export_jsonl,
        } => {
            if let Some(destination) = export_jsonl {
                let rows = parquet_worker::query_bars(
                    &parquet,
                    symbol.as_deref(),
                    market_data_platform::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize,
                    DEFAULT_MAX_QUERY_RESULT_BYTES,
                )?;
                let exported_rows = rows.len();
                market_data_platform::remote_query::write_bars_jsonl_bounded(
                    &destination,
                    &rows,
                    DEFAULT_MAX_EXPORT_BYTES,
                )?;
                print_json(&serde_json::json!({ "exported_rows": exported_rows }))
            } else {
                let rows = parquet_worker::query_bars(
                    &parquet,
                    symbol.as_deref(),
                    market_data_platform::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize,
                    DEFAULT_MAX_QUERY_RESULT_BYTES,
                )?;
                for row in rows {
                    println!("{}", serde_json::to_string(&row)?);
                }
                Ok(())
            }
        }
        Command::ParquetWorker {
            action,
            parquet,
            schema_id,
            max_object_bytes,
            symbol,
            max_rows,
            max_result_bytes,
        } => match action {
            ParquetWorkerAction::Verify => {
                let schema_id = schema_id.ok_or(MarketDataError::InvalidInput)?;
                let verification =
                    parquet_store::verify_with_limit(&parquet, &schema_id, max_object_bytes)?;
                print_json(&verification)
            }
            ParquetWorkerAction::QueryBars => {
                let rows = parquet_store::query_bars_with_limits(
                    &parquet,
                    symbol.as_deref(),
                    max_rows,
                    max_result_bytes,
                )?;
                let stdout = std::io::stdout();
                let mut output = std::io::BufWriter::new(stdout.lock());
                for row in rows {
                    serde_json::to_writer(&mut output, &row)?;
                    use std::io::Write;
                    output.write_all(b"\n")?;
                }
                use std::io::Write;
                output.flush()?;
                Ok(())
            }
        },
        Command::RemoteQueryBars {
            dataset_id,
            namespace,
            rclone_config,
            cache_dir,
            symbol,
            export_jsonl,
            max_object_bytes,
            max_manifest_bytes,
            max_cache_bytes,
            max_cache_entries,
            cache_ttl_secs,
            max_query_bytes,
            max_export_bytes,
        } => {
            let config = pipeline::drive_config_from_env(rclone_config)?;
            let reader = RemoteArchiveReader::rclone_drive(
                config,
                cache_dir,
                RemoteCacheLimits {
                    max_object_bytes,
                    max_manifest_bytes,
                    max_total_cache_bytes: max_cache_bytes,
                    max_entries: max_cache_entries,
                    ttl: std::time::Duration::from_secs(cache_ttl_secs),
                    max_query_result_bytes: max_query_bytes,
                    max_export_bytes,
                    ..RemoteCacheLimits::default()
                },
            )?;
            let namespace = namespace.namespace();
            if let Some(destination) = export_jsonl {
                let summary = reader.export_bars_jsonl(
                    namespace,
                    &dataset_id,
                    symbol.as_deref(),
                    &destination,
                )?;
                print_json(&summary)
            } else {
                let (rows, summary) =
                    reader.query_bars(namespace, &dataset_id, symbol.as_deref())?;
                print_json(&RemoteQueryOutput {
                    summary: &summary,
                    rows: &rows,
                })
            }
        }
        Command::CleanupRemoteCache {
            cache_dir,
            apply,
            cache_ttl_secs,
            max_object_bytes,
            max_manifest_bytes,
            max_cache_entries,
        } => {
            let cleaner = RemoteCacheCleaner::new_isolated(
                cache_dir,
                RemoteCacheLimits {
                    max_object_bytes,
                    max_manifest_bytes,
                    max_entries: max_cache_entries,
                    ttl: std::time::Duration::from_secs(cache_ttl_secs),
                    ..RemoteCacheLimits::default()
                },
            )?;
            print_json(&cleaner.cleanup_expired(apply)?)
        }
        Command::CleanupStaging {
            state_dir,
            staging_dir,
            apply,
        } => {
            let report = ArchivePublisher::cleanup_staging(&state_dir, &staging_dir, apply)?;
            print_json(&report)
        }
    }
}

#[derive(serde::Serialize)]
struct RemoteQueryOutput<'a> {
    summary: &'a market_data_platform::remote_query::RemoteQuerySummary,
    rows: &'a [market_data_platform::aggregate::TradeMinuteBarV1],
}

fn read_session_config(path: &Path) -> Result<ReplaySessionConfig> {
    let file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.take(u64::try_from(MAX_SESSION_CONFIG_BYTES + 1).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SESSION_CONFIG_BYTES {
        return Err(MarketDataError::InputLimit);
    }
    serde_json::from_slice(&bytes).map_err(MarketDataError::from)
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
