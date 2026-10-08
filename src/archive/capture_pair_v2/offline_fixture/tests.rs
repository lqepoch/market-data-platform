use super::*;

fn valid_receipt() -> OfflineFixturePairReceiptV1 {
    let mut receipt = OfflineFixturePairReceiptV1 {
        schema_version: 1,
        fixture_id: ReviewedFixtureId::AlpacaOpraTradeV1.as_str().to_owned(),
        fixture_sha256: ReviewedFixtureId::AlpacaOpraTradeV1
            .expected_sha256()
            .to_owned(),
        fixture_freshness_clock_unix_seconds: FIXTURE_FRESHNESS_CLOCK_UNIX_SECONDS,
        manifest_completion_source_kind: MANIFEST_COMPLETION_SOURCE_KIND.to_owned(),
        capture_instance_id: "00112233445546778899aabbccddeeff".to_owned(),
        protocol_provider: "alpaca".to_owned(),
        protocol_feed: "opra".to_owned(),
        entitlement: EntitlementState::Unknown,
        capture_mode: CAPTURE_MODE.to_owned(),
        terminal_scope: FIXTURE_TERMINAL_SCOPE.to_owned(),
        source_completeness: SOURCE_COMPLETENESS.to_owned(),
        script_frame_count: FIXTURE_SCRIPT_FRAME_COUNT,
        runner_received_frame_count: FIXTURE_SCRIPT_FRAME_COUNT,
        runner_received_digest_sha256: ReviewedFixtureId::AlpacaOpraTradeV1
            .expected_sha256()
            .to_owned(),
        captured_frame_count: 2,
        raw_market_frame_count: 1,
        predecode_ack_count: 2,
        finalization_ack_count: 2,
        captured_bytes: 135,
        ordered_raw_frames_sha256: "11".repeat(32),
        finalization_rollup_sha256: "22".repeat(32),
        output_item_count: 5,
        pair_rollup_sha256: "33".repeat(32),
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.compute_sha256().unwrap();
    receipt
}

#[test]
fn offline_fixture_receipt_hash_binds_runner_received_digest() {
    let receipt = valid_receipt();
    assert!(receipt.validate().is_ok());

    let original_sha256 = receipt.receipt_sha256.clone();
    let mut changed = receipt;
    changed.runner_received_digest_sha256 = "44".repeat(32);

    assert_ne!(changed.compute_sha256().unwrap(), original_sha256);
    assert!(changed.validate().is_err());
}

#[test]
fn offline_fixture_receipt_hash_binds_fixed_freshness_clock() {
    let receipt = valid_receipt();
    assert!(receipt.validate().is_ok());

    let original_sha256 = receipt.receipt_sha256.clone();
    let mut changed = receipt;
    changed.fixture_freshness_clock_unix_seconds += 1;

    assert_ne!(changed.compute_sha256().unwrap(), original_sha256);
    assert!(changed.validate().is_err());
}
