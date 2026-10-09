#![cfg(all(feature = "offline-capture-synthetic", target_os = "linux"))]

use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
};

use arrow_array::{Array, BinaryArray};
use fs2::FileExt;
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
    let readback_staging = temporary.path().join("readback-staging");
    fs::create_dir(&readback_staging).unwrap();
    fs::set_permissions(&readback_staging, fs::Permissions::from_mode(0o755)).unwrap();
    let chunk_receipt_entry = fs::read_dir(output_root.join("archive-state/capture-pair-v2"))
        .unwrap()
        .filter_map(std::result::Result::ok)
        .find(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("chunk-") && name.ends_with(".receipt.json")
        })
        .unwrap();
    let chunk_receipt_path = chunk_receipt_entry.path();
    let chunk_receipt_name = chunk_receipt_entry.file_name().into_string().unwrap();
    let chunk_receipt_bytes = fs::read(&chunk_receipt_path).unwrap();
    let chunk_receipt: Value = serde_json::from_slice(&chunk_receipt_bytes).unwrap();
    let budget_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(readback_staging.join(".pair-readback-budget.lock"))
        .unwrap();
    budget_lock.try_lock_exclusive().unwrap();
    let blocked_pair = run_pair_verifier_with_permissive_umask(
        &archive,
        &output_root,
        &readback_staging,
        &chunk_receipt_name,
    );
    assert!(
        !blocked_pair.status.success(),
        "staging budget lock was ignored"
    );
    assert!(
        blocked_pair.stderr == b"Error: LockHeld\n",
        "staging budget lock did not fail closed"
    );
    budget_lock.unlock().unwrap();
    drop(budget_lock);
    let verified_pair = run_pair_verifier_with_permissive_umask(
        &archive,
        &output_root,
        &readback_staging,
        &chunk_receipt_name,
    );
    assert!(verified_pair.status.success());
    let summary: Value = serde_json::from_slice(&verified_pair.stdout).unwrap();
    assert_json_field(
        &summary,
        "status",
        serde_json::json!("VERIFIED_LOCAL_TEST_CHUNK_ONLY"),
        "unexpected pair verifier status",
    );
    assert_json_field(
        &summary,
        "verification_scope",
        serde_json::json!("SINGLE_CHUNK_LOCAL_READBACK"),
        "unexpected pair verifier scope",
    );
    assert_json_field(
        &summary,
        "source_completeness",
        serde_json::json!("NOT_ASSERTED"),
        "unexpected pair completeness label",
    );
    assert_json_field(
        &summary,
        "pair_verification",
        serde_json::json!("EXACT_RAW_EVENT_ARTIFACT_READBACK_MATCH_LOCAL_ONLY"),
        "unexpected pair verification label",
    );
    assert_json_field(
        &summary,
        "transport",
        serde_json::json!("local_test"),
        "unexpected pair transport",
    );
    assert_json_field(
        &summary,
        "provider",
        serde_json::json!("alpaca"),
        "unexpected pair provider",
    );
    assert_json_field(
        &summary,
        "feed",
        serde_json::json!("opra"),
        "unexpected pair feed",
    );
    assert_json_field(
        &summary,
        "entitlement",
        serde_json::json!("unknown"),
        "unexpected pair entitlement",
    );
    assert_json_field(
        &summary,
        "raw_schema_id",
        serde_json::json!("lqepoch.market_raw_frame.v2"),
        "unexpected raw schema",
    );
    assert_json_field(
        &summary,
        "event_schema_id",
        serde_json::json!("lqepoch.market_event.v3"),
        "unexpected event schema",
    );
    assert!(
        summary["raw_frame_count"].as_u64() == Some(2),
        "unexpected raw frame count"
    );
    assert!(
        summary["normalized_event_count"].as_u64() == Some(1),
        "unexpected normalized event count"
    );
    assert!(summary.get("rows").is_none());
    assert!(!String::from_utf8_lossy(&verified_pair.stdout).contains("600.25"));
    assert!(
        fs::read(&chunk_receipt_path).unwrap() == chunk_receipt_bytes,
        "pair receipt changed during readback"
    );
    assert_staging_has_only_budget_lock(&readback_staging);
    assert_eq!(
        fs::symlink_metadata(&readback_staging)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o755
    );

    let rejected_path = run_pair_verifier(
        &archive,
        &output_root,
        &readback_staging,
        "../capture.receipt.json",
    );
    assert!(!rejected_path.status.success());
    let missing_receipt = run_pair_verifier(
        &archive,
        &output_root,
        &readback_staging,
        &format!("chunk-{}.receipt.json", "0".repeat(64)),
    );
    assert!(!missing_receipt.status.success());
    let mut tampered_receipt_bytes = chunk_receipt_bytes.clone();
    let receipt_last = tampered_receipt_bytes.last_mut().unwrap();
    *receipt_last ^= 1;
    fs::write(&chunk_receipt_path, tampered_receipt_bytes).unwrap();
    let tampered_receipt = run_pair_verifier(
        &archive,
        &output_root,
        &readback_staging,
        &chunk_receipt_name,
    );
    assert!(!tampered_receipt.status.success());
    fs::write(&chunk_receipt_path, &chunk_receipt_bytes).unwrap();

    let raw_artifact = &chunk_receipt["raw_frames"];
    let raw_manifest = archive
        .join(raw_artifact["dataset_id"].as_str().unwrap())
        .join(raw_artifact["manifest_object_name"].as_str().unwrap());
    let raw_manifest_bytes = fs::read(&raw_manifest).unwrap();
    fs::remove_file(&raw_manifest).unwrap();
    let missing_manifest = run_pair_verifier(
        &archive,
        &output_root,
        &readback_staging,
        &chunk_receipt_name,
    );
    assert!(!missing_manifest.status.success());
    fs::write(&raw_manifest, raw_manifest_bytes).unwrap();

    let event_artifact = &chunk_receipt["normalized_events"];
    let event_object = archive
        .join(event_artifact["dataset_id"].as_str().unwrap())
        .join(event_artifact["object_name"].as_str().unwrap());
    let event_object_bytes = fs::read(&event_object).unwrap();
    fs::remove_file(&event_object).unwrap();
    let missing_object = run_pair_verifier(
        &archive,
        &output_root,
        &readback_staging,
        &chunk_receipt_name,
    );
    assert!(!missing_object.status.success());
    fs::write(&event_object, &event_object_bytes).unwrap();
    let mut tampered_event_bytes = event_object_bytes.clone();
    let last = tampered_event_bytes.last_mut().unwrap();
    *last ^= 1;
    fs::write(&event_object, tampered_event_bytes).unwrap();
    let tampered_object = run_pair_verifier(
        &archive,
        &output_root,
        &readback_staging,
        &chunk_receipt_name,
    );
    assert!(!tampered_object.status.success());
    fs::write(&event_object, event_object_bytes).unwrap();
    assert_staging_has_only_budget_lock(&readback_staging);
    assert!(
        fs::read(&chunk_receipt_path).unwrap() == chunk_receipt_bytes,
        "pair receipt changed during rejected readback"
    );

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

#[test]
fn local_test_reader_rejects_linked_artifacts_and_budget_lock_paths() {
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

    let archive = output_root.join("local-test-archive");
    let state_root = output_root.join("archive-state");
    let receipt_directory = state_root.join("capture-pair-v2");
    let chunk_receipt_entry = fs::read_dir(&receipt_directory)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .find(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("chunk-") && name.ends_with(".receipt.json")
        })
        .unwrap();
    let chunk_receipt_name = chunk_receipt_entry.file_name().into_string().unwrap();
    let chunk_receipt: Value =
        serde_json::from_slice(&fs::read(chunk_receipt_entry.path()).unwrap()).unwrap();
    let event_artifact = &chunk_receipt["normalized_events"];
    let event_object = archive
        .join(event_artifact["dataset_id"].as_str().unwrap())
        .join(event_artifact["object_name"].as_str().unwrap());
    let event_object_bytes = fs::read(&event_object).unwrap();
    let staging_root = temporary.path().join("readback-staging");
    fs::create_dir(&staging_root).unwrap();
    fs::set_permissions(&staging_root, fs::Permissions::from_mode(0o700)).unwrap();

    let external_object = temporary.path().join("external-object-target");
    let external_object_bytes = event_object_bytes.clone();
    fs::write(&external_object, &external_object_bytes).unwrap();
    for link_kind in ["symlink", "hardlink"] {
        fs::remove_file(&event_object).unwrap();
        match link_kind {
            "symlink" => symlink(&external_object, &event_object).unwrap(),
            "hardlink" => fs::hard_link(&external_object, &event_object).unwrap(),
            _ => unreachable!(),
        }

        let rejected =
            run_pair_verifier(&archive, &output_root, &staging_root, &chunk_receipt_name);
        assert!(
            !rejected.status.success(),
            "LocalTest opener accepted an artifact {link_kind}"
        );
        assert_eq!(fs::read(&external_object).unwrap(), external_object_bytes);
        assert_staging_has_only_budget_lock(&staging_root);

        fs::remove_file(&event_object).unwrap();
        fs::write(&event_object, &event_object_bytes).unwrap();
    }

    let budget_lock_path = staging_root.join(".pair-readback-budget.lock");
    let external_lock_target = temporary.path().join("external-budget-lock-target");
    let external_lock_bytes = b"external lock target must remain unchanged";
    fs::write(&external_lock_target, external_lock_bytes).unwrap();
    fs::set_permissions(&external_lock_target, fs::Permissions::from_mode(0o600)).unwrap();

    for link_kind in ["symlink", "hardlink"] {
        fs::remove_file(&budget_lock_path).unwrap();
        match link_kind {
            "symlink" => symlink(&external_lock_target, &budget_lock_path).unwrap(),
            "hardlink" => fs::hard_link(&external_lock_target, &budget_lock_path).unwrap(),
            _ => unreachable!(),
        }

        let rejected =
            run_pair_verifier(&archive, &output_root, &staging_root, &chunk_receipt_name);
        assert!(
            !rejected.status.success(),
            "staging budget lock accepted a {link_kind}"
        );
        assert_eq!(
            fs::read(&external_lock_target).unwrap(),
            external_lock_bytes
        );
        assert_staging_has_only_lock_entry(
            &staging_root,
            link_kind == "symlink",
            if link_kind == "hardlink" { 2 } else { 0 },
        );
    }

    fs::remove_file(&budget_lock_path).unwrap();
    let restored_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&budget_lock_path)
        .unwrap();
    drop(restored_lock);
    let recovered = run_pair_verifier(&archive, &output_root, &staging_root, &chunk_receipt_name);
    assert!(recovered.status.success());
    assert_eq!(fs::read(&external_object).unwrap(), external_object_bytes);
    assert_eq!(
        fs::read(&external_lock_target).unwrap(),
        external_lock_bytes
    );
    assert_staging_has_only_budget_lock(&staging_root);
}

fn run_pair_verifier(
    archive: &Path,
    output_root: &Path,
    staging_root: &Path,
    receipt_name: &str,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_market-data-platform"))
        .env_clear()
        .args([
            "verify-capture-pair-v2",
            "--local-test-root",
            archive.to_str().unwrap(),
            "--state-dir",
            output_root.join("archive-state").to_str().unwrap(),
            "--staging-dir",
            staging_root.to_str().unwrap(),
            "--receipt",
            receipt_name,
        ])
        .output()
        .unwrap()
}

fn run_pair_verifier_with_permissive_umask(
    archive: &Path,
    output_root: &Path,
    staging_root: &Path,
    receipt_name: &str,
) -> std::process::Output {
    Command::new("/bin/sh")
        .env_clear()
        .args(["-c", "umask 000; exec \"$@\"", "pair-reader"])
        .arg(env!("CARGO_BIN_EXE_market-data-platform"))
        .args([
            "verify-capture-pair-v2",
            "--local-test-root",
            archive.to_str().unwrap(),
            "--state-dir",
            output_root.join("archive-state").to_str().unwrap(),
            "--staging-dir",
            staging_root.to_str().unwrap(),
            "--receipt",
            receipt_name,
        ])
        .output()
        .unwrap()
}

fn assert_json_field(value: &Value, key: &str, expected: Value, message: &'static str) {
    assert!(value.get(key) == Some(&expected), "{message}");
}

fn assert_staging_has_only_budget_lock(path: &Path) {
    assert_staging_has_only_lock_entry(path, false, 1);
}

fn assert_staging_has_only_lock_entry(path: &Path, expect_symlink: bool, expected_nlink: u64) {
    let entries: Vec<_> = fs::read_dir(path).unwrap().collect();
    assert_eq!(
        entries.len(),
        1,
        "readback staging retained private run files"
    );
    let entry = entries[0].as_ref().unwrap();
    assert_eq!(
        entry.file_name(),
        ".pair-readback-budget.lock",
        "unexpected readback staging artifact"
    );
    let metadata = fs::symlink_metadata(entry.path()).unwrap();
    assert_eq!(metadata.file_type().is_symlink(), expect_symlink);
    if expect_symlink {
        return;
    }
    assert!(metadata.file_type().is_file());
    assert_eq!(metadata.nlink(), expected_nlink);
    if expected_nlink == 1 {
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
    }
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
