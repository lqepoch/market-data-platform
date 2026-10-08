use std::{
    io::Read,
    path::{Path, PathBuf},
};

use clap::{Parser, ValueEnum};
use market_data_platform::{
    MarketDataError, Result,
    archive::{
        ArchiveLimits, ArchivePublisher, DEFAULT_MAX_MANIFEST_BYTES, DEFAULT_MAX_OBJECT_BYTES,
        DEFAULT_MAX_STAGING_BYTES, DEFAULT_UPLOAD_QUEUE_CAPACITY,
    },
    parquet_store,
    pipeline::{self, OutputTransport, ReplayOptions, ReplaySessionConfig},
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

impl SchemaArg {
    const fn schema_id(self) -> &'static str {
        match self {
            Self::MarketEventsV1 => EVENT_SCHEMA_ID,
            Self::UsEquityTradeBar1mV1 => MINUTE_BAR_SCHEMA_ID,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    match Args::parse().command {
        Command::Synthetic { output } => {
            let report = pipeline::synthetic_replay(&output).await?;
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
            let verification = parquet_store::verify(&parquet, schema.schema_id())?;
            print_json(&verification)
        }
        Command::QueryBars {
            parquet,
            symbol,
            export_jsonl,
        } => {
            if let Some(destination) = export_jsonl {
                let rows =
                    parquet_store::export_bars_jsonl(&parquet, &destination, symbol.as_deref())?;
                print_json(&serde_json::json!({ "exported_rows": rows }))
            } else {
                let rows = parquet_store::query_bars(&parquet, symbol.as_deref())?;
                for row in rows {
                    println!("{}", serde_json::to_string(&row)?);
                }
                Ok(())
            }
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
