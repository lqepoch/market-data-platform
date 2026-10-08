#![cfg(all(feature = "offline-capture-synthetic", target_os = "linux"))]

use std::{
    fs::{self, File},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use arrow_array::{Array, BinaryArray};
use market_contracts::{DatasetTransportV1, EntitlementState, FiniteBatchSourceKindV2};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use sha2::{Digest, Sha256};

const FIXTURE_SHA256: &str = "852850f472e4434267bb9d853234fbbced201e229b57d9d1f3c5a0c914818c8c";

#[test]
fn fixed_fake_wire_runs_through_cli_spool_pair_and_independent_readback() {
    let temporary = tempfile::tempdir().unwrap();
    let output_root = temporary.path().join("capture-output");
    let output = Command::new(env!("CARGO_BIN_EXE_market-data-platform"))
        .env_clear()
        .args([
            "capture-synthetic",
            "--output",
            output_root.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "LOCAL_TEST_SYNTHETIC_FIXTURE_REPLAY_ONLY");
    assert_eq!(report["source_kind"], "synthetic_replay");
    assert_eq!(
        report["market_data_authority"],
        "SYNTHETIC_NOT_REAL_OPRA_NOT_LIVE"
    );
    assert_eq!(
        report["manifest_completion_source_kind"],
        "FINITE_BATCH_SOURCE_KIND_LOCAL_ARCHIVE"
    );
    assert_eq!(report["protocol_provider"], "alpaca");
    assert_eq!(report["protocol_feed"], "opra");
    assert_eq!(report["entitlement"], "unknown");
    assert_eq!(report["source_completeness"], "NOT_ASSERTED");
    assert_eq!(report["local_pair"]["fixture_sha256"], FIXTURE_SHA256);
    assert_eq!(
        report["local_pair"]["runner_received_digest_sha256"],
        FIXTURE_SHA256
    );
    assert_eq!(report["local_pair"]["script_frame_count"], 4);
    assert_eq!(report["local_pair"]["runner_received_frame_count"], 4);
    assert_eq!(report["local_pair"]["captured_frame_count"], 2);
    assert_eq!(report["local_pair"]["raw_market_frame_count"], 1);
    assert_eq!(report["local_pair"]["predecode_ack_count"], 2);
    assert_eq!(report["local_pair"]["finalization_ack_count"], 2);

    let output_metadata = fs::symlink_metadata(&output_root).unwrap();
    assert!(output_metadata.file_type().is_dir());
    assert_eq!(output_metadata.permissions().mode() & 0o7777, 0o700);

    let capture_id = report["local_pair"]["capture_instance_id"]
        .as_str()
        .unwrap();
    let receipt_path = output_root
        .join("archive-state/capture-pair-v2")
        .join(format!(
            "capture-{capture_id}.offline-fixture-replay.receipt.json"
        ));
    let receipt_metadata = fs::symlink_metadata(&receipt_path).unwrap();
    assert!(receipt_metadata.file_type().is_file());
    assert_eq!(receipt_metadata.permissions().mode() & 0o7777, 0o600);
    let receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["runner_received_digest_sha256"], FIXTURE_SHA256);
    assert_eq!(receipt["capture_mode"], "SYNTHETIC_REPLAY_FIXTURE");
    assert_eq!(
        receipt["fixture_freshness_clock_unix_seconds"],
        "1791460800"
    );
    assert_eq!(
        receipt["manifest_completion_source_kind"],
        "FINITE_BATCH_SOURCE_KIND_LOCAL_ARCHIVE"
    );
    assert_eq!(receipt["terminal_scope"], "FIXTURE_END_TEST_CONTROL_ONLY");
    assert_eq!(receipt["source_completeness"], "NOT_ASSERTED");
    assert_eq!(
        receipt["pair_rollup_sha256"],
        report["local_pair"]["pair_rollup_sha256"]
    );
    let receipt_bytes = fs::read(&receipt_path).unwrap();

    let archive = output_root.join("local-test-archive");
    let mut manifests = Vec::new();
    let mut parquet_files = Vec::new();
    collect_files(&archive, &mut manifests, &mut parquet_files);
    manifests.sort();
    parquet_files.sort();
    assert_eq!(manifests.len(), 2);
    assert_eq!(parquet_files.len(), 2);

    let mut verified_rows = Vec::new();
    let mut input_digests = Vec::new();
    let mut input_identities = Vec::new();
    let mut input_sizes = Vec::new();
    let mut payload_sha_from_raw_artifact = None;
    for manifest_path in manifests {
        let manifest_bytes = fs::read(&manifest_path).unwrap();
        let manifest = market_contracts::parse_dataset_manifest_v2_json(&manifest_bytes).unwrap();
        assert_eq!(manifest.source.provider, "alpaca");
        assert_eq!(manifest.source.feed, "opra");
        assert_eq!(manifest.source.entitlement, EntitlementState::Unknown);
        assert_eq!(manifest.object.transport, DatasetTransportV1::LocalTest);
        assert!(manifest.dataset_id.len() <= 128);
        assert!(
            manifest
                .dataset_id
                .starts_with("synthetic-offline-fixture-alpaca-opra-trade-v1-")
        );
        assert!(manifest.object.object_name.len() <= 128);
        assert!(manifest.dataset_id.len() + ".manifest.json".len() <= 128);
        let finite = manifest.completion_evidence.finite_batch.unwrap();
        assert_eq!(finite.source_kind, FiniteBatchSourceKindV2::LocalArchive);
        assert!(
            finite
                .input_identity
                .starts_with("mdp-synthetic-offline-fixture:alpaca-opra-trade-v1:")
        );
        input_identities.push(finite.input_identity);
        input_digests.push(finite.input_sha256);
        input_sizes.push(finite.input_size_bytes);
        verified_rows.push(manifest.row_count);
        if manifest.dataset_id.ends_with("-r2") {
            let raw_path = archive
                .join(&manifest.dataset_id)
                .join(format!("{}.parquet", manifest.dataset_id));
            payload_sha_from_raw_artifact = Some(hash_ordered_raw_payloads(&raw_path));
        }
    }
    verified_rows.sort_unstable();
    assert_eq!(verified_rows, [1, 2]);
    assert_eq!(input_digests.len(), 2);
    assert_eq!(input_digests[0], input_digests[1]);
    assert_eq!(input_identities.len(), 2);
    assert_eq!(input_identities[0], input_identities[1]);
    let (raw_payload_sha256, raw_payload_bytes) = payload_sha_from_raw_artifact.unwrap();
    assert_eq!(
        input_digests,
        [raw_payload_sha256.clone(), raw_payload_sha256]
    );
    assert_eq!(input_sizes, [raw_payload_bytes, raw_payload_bytes]);
    assert_ne!(input_digests[0], FIXTURE_SHA256);

    for path in &parquet_files {
        let filename = path.file_name().unwrap().to_string_lossy();
        let schema = if filename.contains("-e3.parquet") {
            "market-events-v3"
        } else if filename.contains("-r2.parquet") {
            "market-raw-frame-v2"
        } else {
            panic!("unexpected Parquet artifact name")
        };
        let verification = Command::new(env!("CARGO_BIN_EXE_market-data-platform"))
            .env_clear()
            .args([
                "verify",
                "--parquet",
                path.to_str().unwrap(),
                "--schema",
                schema,
            ])
            .output()
            .unwrap();
        assert!(verification.status.success());
    }

    let repeated = Command::new(env!("CARGO_BIN_EXE_market-data-platform"))
        .env_clear()
        .args([
            "capture-synthetic",
            "--output",
            output_root.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!repeated.status.success());
    assert_eq!(fs::read(receipt_path).unwrap(), receipt_bytes);
}

fn hash_ordered_raw_payloads(path: &Path) -> (String, u64) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let frame_bytes_index = builder.schema().index_of("frame_bytes").unwrap();
    let mut reader = builder.build().unwrap();
    let mut hasher = Sha256::new();
    let mut total_bytes = 0_u64;
    for batch in &mut reader {
        let batch = batch.unwrap();
        let frames = batch
            .column(frame_bytes_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for index in 0..frames.len() {
            let bytes = frames.value(index);
            hasher.update(bytes);
            total_bytes = total_bytes
                .checked_add(u64::try_from(bytes.len()).unwrap())
                .unwrap();
        }
    }
    (hex::encode(hasher.finalize()), total_bytes)
}

fn collect_files(root: &Path, manifests: &mut Vec<PathBuf>, parquet_files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            collect_files(&path, manifests, parquet_files);
        } else if file_type.is_file() {
            match path.extension().and_then(|extension| extension.to_str()) {
                Some("json")
                    if path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .ends_with(".manifest.json") =>
                {
                    manifests.push(path);
                }
                Some("parquet") => parquet_files.push(path),
                _ => {}
            }
        }
    }
}
