use super::*;

const CAPTURE_ID_BYTES: [u8; 16] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x46, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];
const FIXTURE_PAYLOAD: &[u8] = br#"{"fixture":1}"#;
const FIXTURE_SUMMARY_SHA256: &str =
    "a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0";

fn fixture_capture_id() -> RawCaptureInstanceId {
    RawCaptureInstanceId::new(CAPTURE_ID_BYTES).unwrap()
}

#[test]
fn offline_input_identity_names_the_synthetic_fixture_without_relabeling_protocol() {
    let capture_id = fixture_capture_id();
    let alpaca_identity = chunk_input_identity("alpaca", "opra", capture_id, 3, 1, 2).unwrap();
    assert!(
        alpaca_identity.starts_with("mdp-synthetic-offline-fixture:alpaca-opra-trade-v1:capture:")
    );
    assert!(alpaca_identity.ends_with(":source:3:frames:1-2"));

    let jsonl_identity =
        chunk_input_identity("synthetic", "synthetic", capture_id, 3, 1, 2).unwrap();
    assert!(jsonl_identity.starts_with("mdp-capture-pair-v2:"));
    assert!(!jsonl_identity.contains("synthetic-offline-fixture"));
    assert!(chunk_input_identity("alpaca", "sip", capture_id, 3, 1, 2).is_err());
}

#[tokio::test]
async fn synthetic_fake_wire_flows_through_durable_spool_parquet_pair_and_local_readback() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use broker_ports::{
        RawFrameCapture, RawFrameDisposition, RawFrameFinalization, RawFramePayload,
        RawFrameSinkFactory, RawFrameWireEncoding, RawMarketFrame,
    };
    use market_contracts::{
        DecimalString, EventMetadataV1, MarketDataSourceV1, MarketEventEnvelopeV1, MarketEventV1,
        NumericEncodingV1,
    };
    use tempfile::tempdir;

    use crate::archive::{
        ArchiveLimits, ArchivePublisher, LocalRawFrameSpoolFactory, RawFrameSpoolLimits,
    };
    use crate::storage::LocalTestTransport;

    const CONTROL_WIRE: &[u8] = br#"{"T":"success"}"#;
    const WIRE: &[u8] = br#"{"T":"t","S":"QQQ","p":600.25,"s":1}"#;
    const CONTROL_RECEIVED: &str = "2020-01-02T13:29:59Z";
    const RECEIVED: &str = "2020-01-02T13:30:00Z";

    let temp = tempdir().unwrap();
    let spool_factory = LocalRawFrameSpoolFactory::open(
        temp.path().join("raw-spool"),
        RawFrameSpoolLimits::default(),
    )
    .unwrap();
    let sink = spool_factory.create_sink("synthetic", "synthetic").unwrap();
    let capture_id = sink.capture_instance_id();
    let control_received_at = market_contracts::UtcTimestamp::parse(CONTROL_RECEIVED).unwrap();
    let control_capture = RawFrameCapture::new(
        capture_id,
        "synthetic",
        "synthetic",
        EntitlementState::Unknown,
        7,
        1,
        control_received_at.clone(),
        RawFrameWireEncoding::Json,
        RawFramePayload::capture(CONTROL_WIRE.to_vec()).unwrap(),
    )
    .unwrap();
    let control_predecode_ack = sink.persist_before_decode(&control_capture).await.unwrap();
    assert!(control_predecode_ack.matches(&control_capture));
    let control_finalization =
        RawFrameFinalization::new(0, Vec::new(), None, RawFrameDisposition::ControlMessage)
            .unwrap();
    let control_finalization_ack = sink
        .finalize_after_decode(&control_predecode_ack, &control_finalization)
        .await
        .unwrap();
    assert!(control_finalization_ack.matches(&control_predecode_ack, &control_finalization));

    let received_at = market_contracts::UtcTimestamp::parse(RECEIVED).unwrap();
    let capture = RawFrameCapture::new(
        capture_id,
        "synthetic",
        "synthetic",
        EntitlementState::Unknown,
        7,
        2,
        received_at.clone(),
        RawFrameWireEncoding::Json,
        RawFramePayload::capture(WIRE.to_vec()).unwrap(),
    )
    .unwrap();
    let predecode_ack = sink.persist_before_decode(&capture).await.unwrap();
    assert!(predecode_ack.matches(&capture));

    // This is an owner-provided synthetic wire fixture and a deliberately small offline decoder;
    // it does not call Alpaca, OAuth, rclone, or a provider endpoint.
    let wire: serde_json::Value = serde_json::from_slice(capture.payload().as_bytes()).unwrap();
    assert_eq!(wire["T"], "t");
    let symbol = wire["S"].as_str().unwrap().to_owned();
    let price = DecimalString::new(wire["p"].to_string()).unwrap();
    let size = DecimalString::new(wire["s"].to_string()).unwrap();
    let summary = RawFrameFinalization::new(
        1,
        vec![symbol.clone()],
        Some(NumericEncodingV1::DecimalToken),
        RawFrameDisposition::DecodedMarketData,
    )
    .unwrap();
    let final_ack = sink
        .finalize_after_decode(&predecode_ack, &summary)
        .await
        .unwrap();
    assert!(final_ack.matches(&predecode_ack, &summary));

    let source = MarketDataSourceV1::new(
        "synthetic",
        "synthetic",
        EntitlementState::Unknown,
        NumericEncodingV1::DecimalToken,
        None,
    )
    .unwrap();
    let event = MarketEventEnvelopeV1 {
        metadata: EventMetadataV1 {
            schema_version: 1,
            source,
            generation: 41,
            sequence: 1,
            raw_frame_sha256: Some(capture.capture_key().frame_sha256().to_owned()),
            source_timestamp: None,
            received_timestamp: received_at.clone(),
        },
        event: MarketEventV1::StockTrade {
            symbol: symbol.clone(),
            price,
            size,
        },
    };
    let projected_control = RawMarketFrame {
        provider: "synthetic".to_owned(),
        feed: "synthetic".to_owned(),
        entitlement: EntitlementState::Unknown,
        capture_key: Some(control_capture.capture_key().clone()),
        wire_encoding: RawFrameWireEncoding::Json,
        numeric_encoding: None,
        generation: 41,
        frame_sequence: 1,
        received_timestamp_utc: control_received_at,
        event_count: 0,
        symbols: Vec::new(),
        disposition: RawFrameDisposition::ControlMessage,
        payload: control_capture.payload().clone(),
    };
    let projected_frame = RawMarketFrame {
        provider: "synthetic".to_owned(),
        feed: "synthetic".to_owned(),
        entitlement: EntitlementState::Unknown,
        capture_key: Some(capture.capture_key().clone()),
        wire_encoding: RawFrameWireEncoding::Json,
        numeric_encoding: Some(NumericEncodingV1::DecimalToken),
        generation: 41,
        frame_sequence: 2,
        received_timestamp_utc: received_at,
        event_count: 1,
        symbols: vec![symbol],
        disposition: RawFrameDisposition::DecodedMarketData,
        payload: capture.payload().clone(),
    };
    let publisher = ArchivePublisher::local_test(
        LocalTestTransport::new(temp.path().join("local-test-archive")).unwrap(),
        temp.path().join("archive-state"),
        temp.path().join("staging"),
        ArchiveLimits::default(),
    )
    .unwrap();
    publisher
        .publish_local_synthetic_capture_pair_v2(
            &spool_factory,
            capture_id,
            &[projected_control, projected_frame],
            &[event],
        )
        .unwrap();

    let pair_state = temp.path().join("archive-state/capture-pair-v2");
    let rollup_path = pair_state.join(format!(
        "capture-{}.rollup.json",
        hex::encode(capture_id.as_bytes())
    ));
    let rollup: serde_json::Value =
        serde_json::from_slice(&fs::read(rollup_path).unwrap()).unwrap();
    assert_eq!(rollup["pair_verification"], PAIR_VERIFICATION);
    assert_eq!(rollup["source_completeness"], "NOT_ASSERTED");
    assert_eq!(rollup["raw_frame_count"], "2");
    assert_eq!(rollup["normalized_event_count"], "1");
    let receipt_file = fs::read_dir(pair_state)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .find(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .ends_with(".receipt.json")
        })
        .unwrap();
    assert_eq!(
        fs::metadata(receipt_file.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o600
    );
    assert_eq!(
        fs::metadata(temp.path().join("archive-state/capture-pair-v2"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o700
    );
    let receipt_bytes = fs::read(receipt_file.path()).unwrap();
    let receipt: LocalCapturePairChunkReceiptV2 = serde_json::from_slice(&receipt_bytes).unwrap();
    receipt.validate().unwrap();
    assert_eq!(receipt.pair_verification, PAIR_VERIFICATION);
    assert_eq!(
        receipt.raw_frames.parquet_schema_id,
        crate::schema::RAW_JSON_FRAME_SCHEMA_V2_ID
    );
    assert_eq!(
        receipt.normalized_events.parquet_schema_id,
        crate::schema::EVENT_SCHEMA_V3_ID
    );
    assert_eq!(receipt.raw_frames.transport, DatasetTransportV1::LocalTest);
    assert_eq!(
        receipt.normalized_events.transport,
        DatasetTransportV1::LocalTest
    );

    let archive_root = temp.path().join("local-test-archive");
    let readback_staging = temp.path().join("staging");
    let chunk_receipt_name = receipt_file.file_name().to_string_lossy().to_string();
    let reader = crate::archive::capture_pair_v2::reader::LocalCapturePairV2Reader::local_test(
        &archive_root,
        temp.path().join("archive-state"),
        &readback_staging,
        ArchiveLimits::default(),
    )
    .unwrap();
    let verified = reader.verify_chunk(&chunk_receipt_name).unwrap();
    let verified_json = serde_json::to_value(verified).unwrap();
    assert!(
        verified_json["status"] == "VERIFIED_LOCAL_TEST_CHUNK_ONLY",
        "unexpected verified pair status"
    );
    assert!(
        verified_json["source_completeness"] == "NOT_ASSERTED",
        "unexpected source completeness label"
    );
    assert!(
        verified_json["entitlement"] == "unknown",
        "unexpected entitlement label"
    );

    let tight_manifest_limits = ArchiveLimits {
        max_manifest_bytes: 1,
        ..ArchiveLimits::default()
    };
    let capped_reader =
        crate::archive::capture_pair_v2::reader::LocalCapturePairV2Reader::local_test(
            &archive_root,
            temp.path().join("archive-state"),
            &readback_staging,
            tight_manifest_limits,
        )
        .unwrap();
    assert!(matches!(
        capped_reader.verify_chunk(&chunk_receipt_name),
        Err(crate::MarketDataError::InputLimit)
    ));

    let exact_payload_bytes = [CONTROL_WIRE, WIRE].concat();
    let exact_payload_sha256 = hex::encode(Sha256::digest(&exact_payload_bytes));
    for artifact in [&receipt.raw_frames, &receipt.normalized_events] {
        let manifest_path = archive_root
            .join(&artifact.dataset_id)
            .join(&artifact.manifest_object_name);
        let manifest_bytes = fs::read(manifest_path).unwrap();
        let manifest = parse_dataset_manifest_v2_json(&manifest_bytes).unwrap();
        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.source.provider, "synthetic");
        assert_eq!(manifest.source.feed, "synthetic");
        assert_eq!(manifest.source.entitlement, EntitlementState::Unknown);
        let finite = manifest.completion_evidence.finite_batch.unwrap();
        assert_eq!(finite.source_kind, FiniteBatchSourceKindV2::SyntheticReplay);
        assert_eq!(finite.input_sha256, exact_payload_sha256);
        assert_eq!(
            finite.input_size_bytes,
            u64::try_from(exact_payload_bytes.len()).unwrap()
        );
        assert_eq!(finite.input_record_count, 2);
        assert_ne!(finite.input_sha256, receipt.input_chunk_sha256);
        assert!(finite_batch_matches_input(
            &finite,
            &finite.input_identity,
            &exact_payload_sha256,
            u64::try_from(exact_payload_bytes.len()).unwrap(),
            2,
        ));
        let mut composite_digest_as_manifest_sha = finite.clone();
        composite_digest_as_manifest_sha.input_sha256 = receipt.input_chunk_sha256.clone();
        assert!(!finite_batch_matches_input(
            &composite_digest_as_manifest_sha,
            &finite.input_identity,
            &exact_payload_sha256,
            u64::try_from(exact_payload_bytes.len()).unwrap(),
            2,
        ));
        assert_eq!(
            manifest.object.parquet_schema_sha256,
            artifact.parquet_schema_sha256
        );
    }
    let staging_entries: Vec<_> = fs::read_dir(temp.path().join("staging")).unwrap().collect();
    assert_eq!(staging_entries.len(), 1);
    assert_eq!(
        staging_entries[0]
            .as_ref()
            .unwrap()
            .file_name()
            .to_string_lossy(),
        ".pair-readback-budget.lock"
    );
    spool_factory.shutdown().await;
}

#[test]
fn input_digest_matches_fixed_cross_language_fixture() {
    let fixture: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../docs/fixtures/raw-event-pair-input-v2.json"
    ))
    .unwrap();
    assert_eq!(fixture["schema_version"].as_u64(), Some(2));
    let domain_utf8_hex = hex::encode(CHUNK_INPUT_HASH_DOMAIN);
    assert_eq!(
        fixture["domain_utf8_hex"].as_str(),
        Some(domain_utf8_hex.as_str())
    );
    let capture_id_bytes: [u8; 16] = hex::decode(fixture["capture_instance_id"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let capture_id = RawCaptureInstanceId::new(capture_id_bytes).unwrap();
    let source_generation = fixture["source_generation"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let canonical_generation = fixture["canonical_generation"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let frame_sequence = fixture["source_frame_sequence"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let payload = fixture["payload_utf8"].as_str().unwrap().as_bytes();
    let payload_sha256 = hex::encode(Sha256::digest(payload));
    assert_eq!(
        fixture["payload_sha256"].as_str(),
        Some(payload_sha256.as_str())
    );
    let capture_key = RawFrameCaptureKey::new(
        capture_id,
        source_generation,
        frame_sequence,
        payload_sha256.clone(),
    )
    .unwrap();
    let received_at =
        UtcTimestamp::parse(fixture["received_timestamp_utc"].as_str().unwrap()).unwrap();
    let wire_encoding = match fixture["wire_encoding"].as_str().unwrap() {
        "json" => RawFrameWireEncoding::Json,
        _ => panic!("unknown fixture wire encoding"),
    };
    assert_eq!(fixture["wire_encoding_tag"].as_u64(), Some(1));
    let entitlement = match fixture["entitlement"].as_str().unwrap() {
        "unknown" => EntitlementState::Unknown,
        "authorized" => EntitlementState::Authorized,
        "unauthorized" => EntitlementState::Unauthorized,
        _ => panic!("unknown fixture entitlement"),
    };
    assert_eq!(fixture["entitlement_tag"].as_u64(), Some(0));
    let frame = PairChunkFrameInput {
        capture_key: &capture_key,
        canonical_generation,
        received_timestamp_utc: &received_at,
        wire_encoding,
        payload,
        finalization_summary_sha256: fixture["finalization_summary_sha256"].as_str().unwrap(),
    };
    let input = PairChunkInputV2 {
        capture_instance_id: capture_id,
        provider: fixture["provider"].as_str().unwrap(),
        feed: fixture["feed"].as_str().unwrap(),
        entitlement,
        source_generation,
        canonical_generation,
        frames: &[frame],
    };

    let digest = digest_pair_chunk_input_v2(&input).unwrap();
    assert_eq!(hex::encode(digest.manifest_input_sha256), payload_sha256);
    assert_ne!(
        hex::encode(digest.manifest_input_sha256),
        hex::encode(digest.input_chunk_sha256)
    );
    assert_eq!(
        hex::encode(digest.input_chunk_sha256),
        fixture["input_chunk_sha256"].as_str().unwrap()
    );
    assert_eq!(digest.first_source_frame_sequence, 1);
    assert_eq!(digest.last_source_frame_sequence, 1);
    assert_eq!(
        hex::encode(digest.first_frame_sha256),
        capture_key.frame_sha256()
    );
    assert_eq!(
        hex::encode(digest.last_frame_sha256),
        capture_key.frame_sha256()
    );
    assert_eq!(digest.raw_frame_count, 1);
    let payload_bytes = digest.input_payload_bytes.to_string();
    assert_eq!(
        fixture["input_payload_bytes"].as_str(),
        Some(payload_bytes.as_str())
    );
}

#[test]
fn input_digest_binds_every_frame_identity_and_payload_fact() {
    let base_facts = InputDigestFacts {
        capture_id: fixture_capture_id(),
        provider: "synthetic",
        feed: "synthetic",
        entitlement: EntitlementState::Unknown,
        source_generation: 7,
        canonical_generation: 41,
        sequence: 1,
        payload: FIXTURE_PAYLOAD,
        received_at: "2026-10-08T13:30:00Z",
        wire_encoding: RawFrameWireEncoding::Json,
        summary_sha256: FIXTURE_SUMMARY_SHA256,
    };
    let base = input_digest(base_facts);
    let changed_summary = "b0".repeat(32);
    let variants = [
        input_digest(InputDigestFacts {
            provider: "other-provider",
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            feed: "other-feed",
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            entitlement: EntitlementState::Authorized,
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            source_generation: 8,
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            canonical_generation: 42,
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            sequence: 2,
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            payload: b"{\"fixture\":2}",
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            received_at: "2026-10-08T13:30:00.001Z",
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            wire_encoding: RawFrameWireEncoding::MessagePack,
            ..base_facts
        }),
        input_digest(InputDigestFacts {
            summary_sha256: &changed_summary,
            ..base_facts
        }),
    ];

    for variant in variants {
        assert_ne!(variant, base);
    }
}

#[derive(Clone, Copy)]
struct InputDigestFacts<'a> {
    capture_id: RawCaptureInstanceId,
    provider: &'a str,
    feed: &'a str,
    entitlement: EntitlementState,
    source_generation: u64,
    canonical_generation: u64,
    sequence: u64,
    payload: &'a [u8],
    received_at: &'a str,
    wire_encoding: RawFrameWireEncoding,
    summary_sha256: &'a str,
}

fn input_digest(facts: InputDigestFacts<'_>) -> [u8; 32] {
    let frame_sha256 = hex::encode(Sha256::digest(facts.payload));
    let capture_key = RawFrameCaptureKey::new(
        facts.capture_id,
        facts.source_generation,
        facts.sequence,
        frame_sha256,
    )
    .unwrap();
    let received_at = UtcTimestamp::parse(facts.received_at).unwrap();
    let frame = PairChunkFrameInput {
        capture_key: &capture_key,
        canonical_generation: facts.canonical_generation,
        received_timestamp_utc: &received_at,
        wire_encoding: facts.wire_encoding,
        payload: facts.payload,
        finalization_summary_sha256: facts.summary_sha256,
    };
    let input = PairChunkInputV2 {
        capture_instance_id: facts.capture_id,
        provider: facts.provider,
        feed: facts.feed,
        entitlement: facts.entitlement,
        source_generation: facts.source_generation,
        canonical_generation: facts.canonical_generation,
        frames: std::slice::from_ref(&frame),
    };
    digest_pair_chunk_input_v2(&input)
        .unwrap()
        .input_chunk_sha256
}

#[test]
fn input_digest_rejects_identity_sequence_and_payload_mismatches() {
    let capture_id = fixture_capture_id();
    let other_capture_id = RawCaptureInstanceId::new([
        0x10, 0x11, 0x22, 0x33, 0x44, 0x55, 0x46, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ])
    .unwrap();
    let received_at = UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap();
    let payload_sha256 = hex::encode(Sha256::digest(FIXTURE_PAYLOAD));
    let capture_key_1 = RawFrameCaptureKey::new(capture_id, 7, 1, payload_sha256.clone()).unwrap();
    let capture_key_2 = RawFrameCaptureKey::new(capture_id, 7, 3, payload_sha256).unwrap();
    let frames = [
        PairChunkFrameInput {
            capture_key: &capture_key_1,
            canonical_generation: 41,
            received_timestamp_utc: &received_at,
            wire_encoding: RawFrameWireEncoding::Json,
            payload: FIXTURE_PAYLOAD,
            finalization_summary_sha256: FIXTURE_SUMMARY_SHA256,
        },
        PairChunkFrameInput {
            capture_key: &capture_key_2,
            canonical_generation: 41,
            received_timestamp_utc: &received_at,
            wire_encoding: RawFrameWireEncoding::Json,
            payload: FIXTURE_PAYLOAD,
            finalization_summary_sha256: FIXTURE_SUMMARY_SHA256,
        },
    ];
    let input = PairChunkInputV2 {
        capture_instance_id: capture_id,
        provider: "synthetic",
        feed: "synthetic",
        entitlement: EntitlementState::Unknown,
        source_generation: 7,
        canonical_generation: 41,
        frames: &frames,
    };
    assert_eq!(
        digest_pair_chunk_input_v2(&input).unwrap_err(),
        PairReceiptError::SequenceGap
    );

    let payload_sha256 = hex::encode(Sha256::digest(FIXTURE_PAYLOAD));
    let valid_capture_key =
        RawFrameCaptureKey::new(capture_id, 7, 1, payload_sha256.clone()).unwrap();
    let frame = PairChunkFrameInput {
        capture_key: &valid_capture_key,
        canonical_generation: 41,
        received_timestamp_utc: &received_at,
        wire_encoding: RawFrameWireEncoding::Json,
        payload: FIXTURE_PAYLOAD,
        finalization_summary_sha256: FIXTURE_SUMMARY_SHA256,
    };
    let wrong_capture = PairChunkInputV2 {
        capture_instance_id: other_capture_id,
        provider: "synthetic",
        feed: "synthetic",
        entitlement: EntitlementState::Unknown,
        source_generation: 7,
        canonical_generation: 41,
        frames: &[frame],
    };
    assert_eq!(
        digest_pair_chunk_input_v2(&wrong_capture).unwrap_err(),
        PairReceiptError::InvalidInput
    );

    let wrong_payload = PairChunkInputV2 {
        capture_instance_id: capture_id,
        provider: "synthetic",
        feed: "synthetic",
        entitlement: EntitlementState::Unknown,
        source_generation: 7,
        canonical_generation: 41,
        frames: &[PairChunkFrameInput {
            capture_key: frame.capture_key,
            canonical_generation: 41,
            received_timestamp_utc: &received_at,
            wire_encoding: RawFrameWireEncoding::Json,
            payload: b"{\"fixture\":2}",
            finalization_summary_sha256: FIXTURE_SUMMARY_SHA256,
        }],
    };
    assert_eq!(
        digest_pair_chunk_input_v2(&wrong_payload).unwrap_err(),
        PairReceiptError::InvalidInput
    );
}

#[test]
fn chunk_input_rejects_mixed_canonical_generations_and_raw_wire_formats() {
    let capture_id = fixture_capture_id();
    let payload_sha256 = hex::encode(Sha256::digest(FIXTURE_PAYLOAD));
    let key_one = RawFrameCaptureKey::new(capture_id, 7, 1, payload_sha256.clone()).unwrap();
    let key_two = RawFrameCaptureKey::new(capture_id, 7, 2, payload_sha256).unwrap();
    let received_at = UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap();
    let frames = [
        PairChunkFrameInput {
            capture_key: &key_one,
            canonical_generation: 41,
            received_timestamp_utc: &received_at,
            wire_encoding: RawFrameWireEncoding::Json,
            payload: FIXTURE_PAYLOAD,
            finalization_summary_sha256: FIXTURE_SUMMARY_SHA256,
        },
        PairChunkFrameInput {
            capture_key: &key_two,
            canonical_generation: 42,
            received_timestamp_utc: &received_at,
            wire_encoding: RawFrameWireEncoding::Json,
            payload: FIXTURE_PAYLOAD,
            finalization_summary_sha256: FIXTURE_SUMMARY_SHA256,
        },
    ];
    let mixed_generation = PairChunkInputV2 {
        capture_instance_id: capture_id,
        provider: "synthetic",
        feed: "synthetic",
        entitlement: EntitlementState::Unknown,
        source_generation: 7,
        canonical_generation: 41,
        frames: &frames,
    };
    assert_eq!(
        digest_pair_chunk_input_v2(&mixed_generation).unwrap_err(),
        PairReceiptError::InvalidInput
    );

    let mixed_encoding = PairChunkInputV2 {
        frames: &[
            PairChunkFrameInput {
                capture_key: &key_one,
                canonical_generation: 41,
                received_timestamp_utc: &received_at,
                wire_encoding: RawFrameWireEncoding::Json,
                payload: FIXTURE_PAYLOAD,
                finalization_summary_sha256: FIXTURE_SUMMARY_SHA256,
            },
            PairChunkFrameInput {
                capture_key: &key_two,
                canonical_generation: 41,
                received_timestamp_utc: &received_at,
                wire_encoding: RawFrameWireEncoding::MessagePack,
                payload: FIXTURE_PAYLOAD,
                finalization_summary_sha256: FIXTURE_SUMMARY_SHA256,
            },
        ],
        ..mixed_generation
    };
    assert_eq!(
        digest_pair_chunk_input_v2(&mixed_encoding).unwrap_err(),
        PairReceiptError::InvalidInput
    );
}

#[test]
fn receipt_hash_matches_fixed_fixture_and_binds_artifact_identity() {
    let fixture: LocalCapturePairChunkReceiptV2 = serde_json::from_slice(include_bytes!(
        "../../../docs/fixtures/raw-event-pair-receipt-v2.json"
    ))
    .unwrap();
    assert_eq!(
        fixture.compute_receipt_sha256().unwrap(),
        fixture.pair_receipt_sha256
    );
    let json = serde_json::to_value(&fixture).unwrap();
    assert_eq!(json["source_generation"], "7");
    assert_eq!(json["canonical_generation"], "41");
    assert_eq!(json["raw_frames"]["size_bytes"], "4096");
    assert_eq!(json["normalized_events"]["row_count"], "1");

    let mut changed_artifact_identity = fixture.clone();
    changed_artifact_identity.raw_frames.object_id = "local-test:substituted".into();
    assert_ne!(
        changed_artifact_identity.compute_receipt_sha256().unwrap(),
        fixture.pair_receipt_sha256
    );

    let mut changed_schema = fixture.clone();
    changed_schema.normalized_events.parquet_schema_sha256 = "aa".repeat(32);
    assert_ne!(
        changed_schema.compute_receipt_sha256().unwrap(),
        fixture.pair_receipt_sha256
    );

    let mut changed_input = fixture.clone();
    changed_input.input_chunk_sha256 = "bb".repeat(32);
    assert_ne!(
        changed_input.compute_receipt_sha256().unwrap(),
        fixture.pair_receipt_sha256
    );
}
