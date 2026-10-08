//! CLI-owned resource-limited process boundary for decoding Parquet files.
//!
//! The launcher resolves [`std::env::current_exe`], so these functions are only valid inside the
//! `market-data-platform` CLI process. They are not a reusable SDK worker launcher for an embedding
//! application's executable.

use std::{path::Path, process::Command, time::Duration};

use crate::{
    MarketDataError, Result,
    aggregate::TradeMinuteBarV1,
    cancellation::CancellationToken,
    parquet_store::ParquetVerification,
    storage::{MAX_DECODE_WORKER_OUTPUT_BYTES, run_decode_worker, run_decode_worker_cancellable},
};

const WORKER_WALL_TIMEOUT: Duration = Duration::from_secs(120);

/// Verify a Parquet object from the MDP CLI in a worker with fixed OS memory/CPU limits.
pub fn verify(
    parquet: &Path,
    schema_id: &str,
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    verify_with_cancellation(parquet, schema_id, max_object_bytes, None)
}

pub fn verify_cancellable(
    parquet: &Path,
    schema_id: &str,
    max_object_bytes: u64,
    cancellation: CancellationToken,
) -> Result<ParquetVerification> {
    verify_with_cancellation(parquet, schema_id, max_object_bytes, Some(cancellation))
}

fn verify_with_cancellation(
    parquet: &Path,
    schema_id: &str,
    max_object_bytes: u64,
    cancellation: Option<CancellationToken>,
) -> Result<ParquetVerification> {
    if schema_id != crate::schema::EVENT_SCHEMA_ID
        && schema_id != crate::schema::EVENT_SCHEMA_V2_ID
        && schema_id != crate::schema::EVENT_SCHEMA_V3_ID
        && schema_id != crate::schema::RAW_FRAME_SCHEMA_ID
        && schema_id != crate::schema::RAW_FRAME_SCHEMA_V2_ID
        && schema_id != crate::schema::RAW_JSON_FRAME_SCHEMA_V2_ID
        && schema_id != crate::schema::MINUTE_BAR_SCHEMA_ID
    {
        return Err(MarketDataError::ParquetSchema);
    }
    let mut command = worker_command()?;
    #[cfg(test)]
    command
        .env("MDP_TEST_WORKER_ACTION", "verify")
        .env("MDP_TEST_WORKER_PARQUET", parquet)
        .env("MDP_TEST_WORKER_SCHEMA", schema_id)
        .env("MDP_TEST_WORKER_MAX_BYTES", max_object_bytes.to_string());
    #[cfg(not(test))]
    command
        .arg("verify")
        .arg("--parquet")
        .arg(parquet)
        .arg("--schema-id")
        .arg(schema_id)
        .arg("--max-object-bytes")
        .arg(max_object_bytes.to_string());
    let output = match cancellation {
        Some(token) => {
            run_decode_worker_cancellable(command, 64 * 1024, WORKER_WALL_TIMEOUT, token)?
        }
        None => run_decode_worker(command, 64 * 1024, WORKER_WALL_TIMEOUT)?,
    };
    #[cfg(test)]
    let output = output
        .split(|byte| *byte == b'\n')
        .find(|line| line.first() == Some(&b'{'))
        .ok_or(MarketDataError::Parquet)?;
    #[cfg(not(test))]
    let output = &output;
    serde_json::from_slice(output).map_err(MarketDataError::from)
}

/// Query bars from the MDP CLI in a worker; IPC bytes and row results remain bounded.
pub fn query_bars(
    parquet: &Path,
    symbol: Option<&str>,
    max_rows: usize,
    max_result_bytes: u64,
) -> Result<Vec<TradeMinuteBarV1>> {
    query_bars_with_cancellation(parquet, symbol, max_rows, max_result_bytes, None)
}

pub fn query_bars_cancellable(
    parquet: &Path,
    symbol: Option<&str>,
    max_rows: usize,
    max_result_bytes: u64,
    cancellation: CancellationToken,
) -> Result<Vec<TradeMinuteBarV1>> {
    query_bars_with_cancellation(
        parquet,
        symbol,
        max_rows,
        max_result_bytes,
        Some(cancellation),
    )
}

fn query_bars_with_cancellation(
    parquet: &Path,
    symbol: Option<&str>,
    max_rows: usize,
    max_result_bytes: u64,
    cancellation: Option<CancellationToken>,
) -> Result<Vec<TradeMinuteBarV1>> {
    let output_limit =
        usize::try_from(max_result_bytes).map_err(|_| MarketDataError::InputLimit)?;
    if output_limit == 0 || output_limit > MAX_DECODE_WORKER_OUTPUT_BYTES {
        return Err(MarketDataError::InputLimit);
    }
    let mut command = worker_command()?;
    #[cfg(test)]
    command
        .env("MDP_TEST_WORKER_ACTION", "query-bars")
        .env("MDP_TEST_WORKER_PARQUET", parquet)
        .env("MDP_TEST_WORKER_SYMBOL", symbol.unwrap_or_default())
        .env("MDP_TEST_WORKER_MAX_ROWS", max_rows.to_string())
        .env("MDP_TEST_WORKER_MAX_BYTES", max_result_bytes.to_string());
    #[cfg(not(test))]
    command
        .arg("query-bars")
        .arg("--parquet")
        .arg(parquet)
        .arg("--max-rows")
        .arg(max_rows.to_string())
        .arg("--max-result-bytes")
        .arg(max_result_bytes.to_string());
    #[cfg(not(test))]
    if let Some(symbol) = symbol {
        command.arg("--symbol").arg(symbol);
    }
    let output = match cancellation {
        Some(token) => {
            run_decode_worker_cancellable(command, output_limit, WORKER_WALL_TIMEOUT, token)?
        }
        None => run_decode_worker(command, output_limit, WORKER_WALL_TIMEOUT)?,
    };
    let lines = output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty());
    #[cfg(test)]
    let lines = lines.filter(|line| line.first() == Some(&b'{'));
    lines
        .map(|line| serde_json::from_slice(line).map_err(MarketDataError::from))
        .collect()
}

fn worker_command() -> Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    #[cfg(test)]
    command
        .arg("--quiet")
        .arg("--exact")
        .arg("parquet_worker::tests::worker_entrypoint")
        .arg("--nocapture");
    #[cfg(test)]
    if let Some(pid_file) = test_support::PID_FILE
        .get_or_init(Default::default)
        .lock()
        .expect("worker pid marker mutex")
        .clone()
    {
        command.env("MDP_TEST_WORKER_PID_FILE", pid_file);
    }
    #[cfg(not(test))]
    command.arg("parquet-worker");
    Ok(command)
}

#[cfg(test)]
pub(crate) fn track_worker_pid(path: &Path) -> WorkerPidFileGuard {
    let pid_file = test_support::PID_FILE.get_or_init(Default::default);
    *pid_file.lock().expect("worker pid marker mutex") = Some(path.to_owned());
    WorkerPidFileGuard(path.to_owned())
}

#[cfg(test)]
pub(crate) fn serialize_worker_test() -> std::sync::MutexGuard<'static, ()> {
    test_support::SERIAL
        .get_or_init(Default::default)
        .lock()
        .unwrap()
}

#[cfg(test)]
pub(crate) struct WorkerPidFileGuard(std::path::PathBuf);

#[cfg(test)]
impl Drop for WorkerPidFileGuard {
    fn drop(&mut self) {
        let pid_file = test_support::PID_FILE.get_or_init(Default::default);
        let mut current = pid_file.lock().expect("worker pid marker mutex");
        if current.as_deref() == Some(self.0.as_path()) {
            *current = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_entrypoint() {
        let Ok(action) = std::env::var("MDP_TEST_WORKER_ACTION") else {
            return;
        };
        if let Some(pid_file) = std::env::var_os("MDP_TEST_WORKER_PID_FILE") {
            std::fs::write(pid_file, std::process::id().to_string()).unwrap();
        } else if let Some(pid_file) = test_support::PID_FILE
            .get_or_init(Default::default)
            .lock()
            .expect("worker pid marker mutex")
            .as_ref()
        {
            std::fs::write(pid_file, std::process::id().to_string()).unwrap();
        }
        match action.as_str() {
            "verify" => {
                let path = std::env::var_os("MDP_TEST_WORKER_PARQUET").unwrap();
                let schema = std::env::var("MDP_TEST_WORKER_SCHEMA").unwrap();
                let limit = std::env::var("MDP_TEST_WORKER_MAX_BYTES")
                    .unwrap()
                    .parse()
                    .unwrap();
                let verification =
                    crate::parquet_store::verify_with_limit(Path::new(&path), &schema, limit)
                        .unwrap();
                println!("{}", serde_json::to_string(&verification).unwrap());
            }
            "query-bars" => {
                let path = std::env::var_os("MDP_TEST_WORKER_PARQUET").unwrap();
                let symbol = std::env::var("MDP_TEST_WORKER_SYMBOL").ok();
                let max_rows = std::env::var("MDP_TEST_WORKER_MAX_ROWS")
                    .unwrap()
                    .parse()
                    .unwrap();
                let max_bytes = std::env::var("MDP_TEST_WORKER_MAX_BYTES")
                    .unwrap()
                    .parse()
                    .unwrap();
                let rows = crate::parquet_store::query_bars_with_limits(
                    Path::new(&path),
                    symbol.as_deref(),
                    max_rows,
                    max_bytes,
                )
                .unwrap();
                for row in rows {
                    println!("{}", serde_json::to_string(&row).unwrap());
                }
            }
            _ => panic!("unknown test worker action"),
        }
        // The harness writes a trailing `test ... ok` line to stdout after this function returns,
        // which would corrupt the worker's bounded JSON/JSONL IPC payload.
        std::process::exit(0);
    }
}

#[cfg(test)]
mod test_support {
    use std::sync::{Mutex, OnceLock};

    pub(super) static PID_FILE: OnceLock<Mutex<Option<std::path::PathBuf>>> = OnceLock::new();
    pub(super) static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
}
