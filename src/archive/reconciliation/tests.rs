use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use market_contracts::EntitlementState;
use sha2::Digest;

use super::authority::{
    ProviderAdmissionClaimsV1, ProviderAdmissionDatasetBindingV1, ProviderAdmissionEvidenceV1,
    ProviderAdmissionReceiptV1, ProviderPhaseSliceV1, ReconciliationPolicyV1,
    TrustedProviderAdmissionVerifierV1, signed_payload_for_test, verify_provider_admission_v1,
};
use super::authority_types::{AdmissionVerifyErrorV1, UnavailableProviderAdmissionVerifierV1};
use super::logical_index::*;
use super::*;

fn cache_input<'a>(
    request: &'a BarsRequestV1,
    source_scope: &'a CacheQuerySourceScopeV1,
    raw_observations: BarsResponseV1,
    canonical_response: BarsResponseV1,
    page_evidence: ProviderPageEvidenceV1,
) -> CacheBarsResponsesInputV1<'a> {
    CacheBarsResponsesInputV1::new(
        request,
        source_scope,
        raw_observations,
        canonical_response,
        page_evidence,
    )
}

fn cache_input_with_test_phases<'a>(
    request: &'a BarsRequestV1,
    source_scope: &'a CacheQuerySourceScopeV1,
    raw_observations: BarsResponseV1,
    canonical_response: BarsResponseV1,
    page_evidence: ProviderPageEvidenceV1,
) -> CacheBarsResponsesInputV1<'a> {
    let phases = canonical_response
        .segments
        .iter()
        .map(|segment| ProviderPhaseSliceV1 {
            phase: "regular".to_owned(),
            session_instance_id: segment.session_instance_id.clone(),
            start_utc: segment.start_utc.as_str().to_owned(),
            end_exclusive_utc: segment.end_exclusive_utc.as_str().to_owned(),
        })
        .collect::<Vec<_>>();
    CacheBarsResponsesInputV1::new(
        request,
        source_scope,
        raw_observations,
        canonical_response,
        page_evidence,
    )
    .with_phase_slices(&phases)
}

fn logical_pointer_version_on_disk(
    root: &Path,
    identity: &CacheLogicalIdentityV1,
    session_instance_id: &str,
) -> String {
    let logical_dir = root.join("logical").join(identity.fingerprint().unwrap());
    let envelope: serde_json::Value =
        serde_json::from_slice(&fs::read(logical_dir.join("current.json")).unwrap()).unwrap();
    let shards = envelope["payload"]["shards"].as_object().unwrap();
    for reference in shards.values() {
        let content_sha256 = reference["content_sha256"].as_str().unwrap();
        let shard: serde_json::Value = serde_json::from_slice(
            &fs::read(
                logical_dir
                    .join("shards")
                    .join(format!("{content_sha256}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        if let Some(version_id) =
            shard["sessions"][session_instance_id]["current"]["version_id"].as_str()
        {
            return version_id.to_owned();
        }
    }
    panic!("logical session was not found in verified shards");
}

fn row(identity: &str, price: &str, volume: &str) -> CanonicalRowV1 {
    CanonicalRowV1::new(
        identity,
        BTreeMap::from([
            (
                "close".to_owned(),
                CanonicalValueV1::Decimal(price.to_owned()),
            ),
            (
                "volume".to_owned(),
                CanonicalValueV1::Decimal(volume.to_owned()),
            ),
        ]),
    )
    .unwrap()
}

fn segment(provider: &str, feed: &str, entitlement: EntitlementState) -> SourceSegmentV1 {
    SourceSegmentV1 {
        segment_id: "page-1".to_owned(),
        provider: provider.to_owned(),
        feed: feed.to_owned(),
        entitlement,
        session_instance_id: "2026-10-08-regular".to_owned(),
        covered_start_utc: "2026-10-08T13:30:00Z".to_owned(),
        covered_end_exclusive_utc: "2026-10-08T13:31:00Z".to_owned(),
        provenance_id: Some("source-1".to_owned()),
        coverage_status: market_contracts::BarsCoverageStatusV1::Complete,
        source_observation_sha256: "1".repeat(64),
        canonical_observation_sha256: "2".repeat(64),
        lineage_receipt_sha256: "3".repeat(64),
        first_source_sequence: None,
        last_source_sequence: None,
        raw_row_count: 1,
        canonical_row_count: 1,
        continuation_cursor_sha256: None,
    }
}

fn key(provider: &str, feed: &str, entitlement: EntitlementState) -> CacheKeyV1 {
    CacheKeyV1 {
        provider: provider.to_owned(),
        feed: feed.to_owned(),
        entitlement,
        numeric_encoding: "decimal_token".to_owned(),
        adjustment: "raw".to_owned(),
        requested_resolution: "1m".to_owned(),
        effective_resolution: "1m".to_owned(),
        provider_mode: if provider == "futu" {
            "futu_only".to_owned()
        } else {
            "alpaca_only".to_owned()
        },
        source_scope_id: format!("{provider}-{feed}-v1"),
        entitlement_scope_sha256: "5".repeat(64),
        rights_profile_sha256: "8".repeat(64),
        asset_class: "etf".to_owned(),
        symbol: "QQQ".to_owned(),
        session_scope: "regular".to_owned(),
        session_instance_id: "2026-10-08-regular".to_owned(),
        window_start_utc: "2026-10-08T13:30:00Z".to_owned(),
        window_end_exclusive_utc: "2026-10-08T13:31:00Z".to_owned(),
        market_calendar_version: Some("XNYS-2026a".to_owned()),
        request_cursor_sha256: None,
        schema_version: 1,
        schema_id: crate::schema::MARKET_BARS_SCHEMA_V1_ID.to_owned(),
        schema_fingerprint_sha256: crate::schema::fingerprint(
            crate::schema::MARKET_BARS_SCHEMA_V1_ID,
            &crate::schema::arrow_schema(crate::schema::MARKET_BARS_SCHEMA_V1_ID).unwrap(),
        )
        .unwrap(),
    }
}

fn coverage(key: &CacheKeyV1, exhausted: bool, gaps: u64, truncated: bool) -> CoverageEvidenceV1 {
    CoverageEvidenceV1 {
        covered_start_utc: key.window_start_utc.clone(),
        covered_end_exclusive_utc: key.window_end_exclusive_utc.clone(),
        response_observed_at_utc: key.window_end_exclusive_utc.clone(),
        pages_seen: Some(1),
        pages_exhausted: Some(exhausted),
        sequence_gap_count: Some(gaps),
        coverage_gap_count: Some(0),
        truncated,
        segments: vec![segment(&key.provider, &key.feed, key.entitlement)],
    }
}

fn logical_bar_row(
    session_instance_id: &str,
    start_utc: &str,
    end_utc: &str,
    close: &str,
    provenance_id: &str,
) -> BarRowV1 {
    let timestamp = |value: &str| market_contracts::ProtoTimestampV2::parse(value).unwrap();
    BarRowV1 {
        symbol: "QQQ".to_owned(),
        asset_class: market_contracts::AssetClassV1::Etf,
        resolution: market_contracts::BarResolutionV1::OneMinute,
        adjustment: market_contracts::AdjustmentV1::Unadjusted,
        bar_start_utc: timestamp(start_utc),
        bar_end_exclusive_utc: timestamp(end_utc),
        received_at_utc: timestamp(end_utc),
        available_at_utc: timestamp(end_utc),
        open: market_contracts::DecimalString::new(close).unwrap(),
        high: market_contracts::DecimalString::new(close).unwrap(),
        low: market_contracts::DecimalString::new(close).unwrap(),
        close: market_contracts::DecimalString::new(close).unwrap(),
        volume: market_contracts::DecimalString::new("2").unwrap(),
        provenance_id: provenance_id.to_owned(),
        session_instance_id: session_instance_id.to_owned(),
        validation_state: market_contracts::BarsValidationStateV1::Validated,
    }
}

fn logical_segment(
    source_key: &CacheKeyV1,
    session_instance_id: &str,
    start_utc: &str,
    end_utc: &str,
    segment_id: &str,
) -> SourceSegmentV1 {
    let mut value = segment(
        &source_key.provider,
        &source_key.feed,
        source_key.entitlement,
    );
    value.segment_id = segment_id.to_owned();
    value.session_instance_id = session_instance_id.to_owned();
    value.covered_start_utc = start_utc.to_owned();
    value.covered_end_exclusive_utc = end_utc.to_owned();
    value
}

fn admission_test_request() -> BarsRequestV1 {
    BarsRequestV1 {
        symbol: "QQQ".to_owned(),
        asset_class: market_contracts::AssetClassV1::Etf,
        start: market_contracts::ProtoTimestampV2::parse("2026-10-08T13:30:00Z").unwrap(),
        end: market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:00Z").unwrap(),
        resolution: market_contracts::BarResolutionV1::OneMinute,
        session_scope: market_contracts::SessionScopeV1::Regular,
        adjustment: market_contracts::AdjustmentV1::Unadjusted,
        provider_mode: market_contracts::BarsProviderModeV1::AlpacaOnly,
        allow_daily_downgrade: false,
        limit: 1000,
        cursor: None,
    }
}

fn admission_test_scope() -> CacheQuerySourceScopeV1 {
    CacheQuerySourceScopeV1 {
        provider: "alpaca".to_owned(),
        feed: "sip".to_owned(),
        source_scope_id: "alpaca-sip-v1".to_owned(),
        entitlement_scope_sha256: "6".repeat(64),
        rights_profile_sha256: "7".repeat(64),
        entitlement: EntitlementState::Authorized,
        numeric_encoding: market_contracts::NumericEncodingV1::DecimalToken,
        effective_resolution: market_contracts::BarResolutionV1::OneMinute,
        session_instance_ids: vec!["2026-10-08-regular".to_owned()],
        market_calendar_version: Some("XNYS-2026a".to_owned()),
    }
}

fn admission_test_response(close: &str) -> BarsResponseV1 {
    BarsResponseV1 {
        schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
        provider: "alpaca".to_owned(),
        feed: "sip".to_owned(),
        entitlement_status: market_contracts::BarsEntitlementStatusV1::Authorized,
        requested_resolution: market_contracts::BarResolutionV1::OneMinute,
        effective_resolution: market_contracts::BarResolutionV1::OneMinute,
        adjustment: market_contracts::AdjustmentV1::Unadjusted,
        rows: vec![logical_bar_row(
            "2026-10-08-regular",
            "2026-10-08T13:30:00Z",
            "2026-10-08T13:31:00Z",
            close,
            "source-1",
        )],
        coverage_status: market_contracts::BarsCoverageStatusV1::Complete,
        segments: vec![market_contracts::BarCoverageSegmentV1 {
            start_utc: market_contracts::ProtoTimestampV2::parse("2026-10-08T13:30:00Z").unwrap(),
            end_exclusive_utc: market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:00Z")
                .unwrap(),
            coverage_status: market_contracts::BarsCoverageStatusV1::Complete,
            provenance_id: Some("source-1".to_owned()),
            session_instance_id: "2026-10-08-regular".to_owned(),
        }],
        gaps: Vec::new(),
        source_provenance: vec![market_contracts::BarSourceProvenanceV1 {
            provenance_id: "source-1".to_owned(),
            provider: "alpaca".to_owned(),
            feed: "sip".to_owned(),
            source: "synthetic trusted-verifier fixture".to_owned(),
            entitlement_status: market_contracts::BarsEntitlementStatusV1::Authorized,
            numeric_encoding: market_contracts::NumericEncodingV1::DecimalToken,
        }],
        as_of: None,
        fetched_at: market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:01Z").unwrap(),
        verified_at: None,
        cache_status: market_contracts::BarsCacheStatusV1::Miss,
        market_calendar_version: Some("XNYS-2026a".to_owned()),
        continuation_cursor: None,
        truncated: false,
    }
}

fn admission_test_evidence() -> ProviderPageEvidenceV1 {
    ProviderPageEvidenceV1 {
        source_observation_sha256: Some("a".repeat(64)),
        response_observed_at_utc: Some(
            market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:02Z").unwrap(),
        ),
        pages_seen: Some(1),
        pages_exhausted: Some(true),
        sequence_gap_count: None,
        truncated: false,
        ..ProviderPageEvidenceV1::default()
    }
}

fn admission_test_policy() -> ReconciliationPolicyV1 {
    ReconciliationPolicyV1::new(
        "test-reconciliation-v1",
        1,
        SameFeedTolerancePolicyV1::default(),
        SameFeedTolerancePolicyV1::default(),
    )
}

fn admission_test_phases() -> Vec<ProviderPhaseSliceV1> {
    vec![ProviderPhaseSliceV1 {
        phase: "regular".to_owned(),
        session_instance_id: "2026-10-08-regular".to_owned(),
        start_utc: "2026-10-08T13:30:00Z".to_owned(),
        end_exclusive_utc: "2026-10-08T13:31:00Z".to_owned(),
    }]
}

fn admission_test_receipt(
    request: &BarsRequestV1,
    scope: &CacheQuerySourceScopeV1,
    phases: &[ProviderPhaseSliceV1],
    dataset_binding: Option<ProviderAdmissionDatasetBindingV1>,
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
) -> ProviderAdmissionReceiptV1 {
    let key = CacheKeyV1::from_request(request, scope).unwrap();
    let request_cursor_sha256 = dataset_binding
        .as_ref()
        .and(key.request_cursor_sha256.clone());
    let mut receipt = ProviderAdmissionReceiptV1 {
        issuer: "test-issuer".to_owned(),
        key_id: "test-key".to_owned(),
        issued_at_unix_seconds,
        expires_at_unix_seconds,
        claims: ProviderAdmissionClaimsV1 {
            provider: scope.provider.clone(),
            feed: scope.feed.clone(),
            source_scope_id: scope.source_scope_id.clone(),
            entitlement: scope.entitlement,
            entitlement_scope_sha256: scope.entitlement_scope_sha256.clone(),
            rights_profile_sha256: scope.rights_profile_sha256.clone(),
            numeric_encoding: scope.numeric_encoding.as_str().to_owned(),
            symbol: request.symbol.clone(),
            asset_class: request.asset_class,
            requested_resolution: request.resolution,
            effective_resolution: scope.effective_resolution,
            adjustment: request.adjustment,
            session_scope: request.session_scope,
            session_instance_ids: scope.session_instance_ids.clone(),
            window_start_utc: request.start.as_str().to_owned(),
            window_end_exclusive_utc: request.end.as_str().to_owned(),
            market_calendar_version: scope.market_calendar_version.clone(),
            request_cursor_sha256,
            schema_id: key.schema_id,
            schema_fingerprint_sha256: key.schema_fingerprint_sha256,
            authorization_context_sha256: "b".repeat(64),
            sdk_name: "alpaca-rust".to_owned(),
            sdk_version: "3.1.0".to_owned(),
            gateway_version: "mdp-test-1".to_owned(),
            ordered_phase_slices: phases.to_vec(),
            license_scope_id: "alpaca-sip-history-v1".to_owned(),
            license_terms_sha256: "c".repeat(64),
            dataset_binding,
        },
        signature_base64: String::new(),
    };
    let payload = signed_payload_for_test(&receipt).unwrap();
    receipt.signature_base64 = format!(
        "test-signature:{}",
        super::hex_digest(sha2::Sha256::digest(payload).as_slice())
    );
    receipt
}

fn admission_test_evidence_with_receipt(
    receipt: ProviderAdmissionReceiptV1,
    phases: &[ProviderPhaseSliceV1],
) -> ProviderAdmissionEvidenceV1 {
    ProviderAdmissionEvidenceV1 {
        authorization_context_sha256: Some("b".repeat(64)),
        sdk_name: Some("alpaca-rust".to_owned()),
        sdk_version: Some("3.1.0".to_owned()),
        gateway_version: Some("mdp-test-1".to_owned()),
        ordered_phase_slices: phases.to_vec(),
        signed_receipt: Some(receipt),
    }
}

struct DeterministicTestAdmissionVerifier {
    revoked: bool,
}

impl TrustedProviderAdmissionVerifierV1 for DeterministicTestAdmissionVerifier {
    fn verify_signature(
        &self,
        issuer: &str,
        key_id: &str,
        canonical_payload: &[u8],
        signature_base64: &str,
    ) -> std::result::Result<(), AdmissionVerifyErrorV1> {
        let expected = format!(
            "test-signature:{}",
            super::hex_digest(sha2::Sha256::digest(canonical_payload).as_slice())
        );
        if self.revoked
            || issuer != "test-issuer"
            || key_id != "test-key"
            || signature_base64 != expected
        {
            Err(AdmissionVerifyErrorV1::Rejected)
        } else {
            Ok(())
        }
    }
}

#[test]
fn exact_decimal_normalization_and_identical_dedup_are_stable() {
    let normalized =
        normalize_and_deduplicate_rows([row("bar-1", "1.00", "2.0"), row("bar-1", "1", "2")])
            .unwrap();
    assert_eq!(normalized, [row("bar-1", "1", "2")]);
}

#[test]
fn duplicate_identity_with_conflicting_values_is_rejected() {
    assert!(matches!(
        normalize_and_deduplicate_rows([row("bar-1", "1", "2"), row("bar-1", "1.01", "2")]),
        Err(CacheReconciliationError::Conflict)
    ));
}

#[test]
fn provider_cursor_receipt_is_query_bound_expiring_and_redacted() {
    let cache_key = key("alpaca", "sip", EntitlementState::Authorized);
    let secret_page_locator = "synthetic-alpaca-page-locator";
    let input = ProviderCursorCacheInputV1::new(
        ProviderCursorKindV1::Alpaca,
        serde_json::to_string(secret_page_locator).unwrap(),
        1_800_000_120,
    )
    .unwrap();
    let receipt =
        ValidatedProviderCursorReceiptV1::bind_to_key(input, &cache_key, 1_800_000_000).unwrap();
    assert_eq!(receipt.kind(), ProviderCursorKindV1::Alpaca);
    assert_eq!(receipt.payload_json(), "\"synthetic-alpaca-page-locator\"");
    assert_eq!(receipt.expires_at_unix_seconds(), 1_800_000_120);
    receipt.validate_for_cache_key(&cache_key).unwrap();
    assert!(!format!("{receipt:?}").contains(secret_page_locator));

    let mut different_realm = cache_key.clone();
    different_realm.entitlement_scope_sha256 = "7".repeat(64);
    assert!(receipt.validate_for_cache_key(&different_realm).is_err());
    assert!(
        ProviderCursorCacheInputV1::new(
            ProviderCursorKindV1::Futu,
            serde_json::to_string("not-a-valid-cursor").unwrap(),
            1_800_000_120,
        )
        .is_err()
    );
    let expired = ProviderCursorCacheInputV1::new(
        ProviderCursorKindV1::Alpaca,
        "\"expired-locator\"".to_owned(),
        1_800_000_000,
    )
    .unwrap();
    assert!(matches!(
        ValidatedProviderCursorReceiptV1::bind_to_key(expired, &cache_key, 1_800_000_000),
        Err(CacheReconciliationError::Expired)
    ));
}

#[test]
fn cache_root_marker_initializes_an_empty_directory_and_allows_restart() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cache-root");
    std::fs::create_dir(&root).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let first = CacheVersionStoreV1::new(root.clone(), CacheLimitsV1::default()).unwrap();
    drop(first);
    assert_eq!(
        std::fs::read(root.join(".mdp-cache-owner")).unwrap(),
        b"lqepoch.mdp.cache-root.v1\n"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(root.join(".mdp-cache-owner"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    let _second = CacheVersionStoreV1::new(root.clone(), CacheLimitsV1::default()).unwrap();
    assert!(root.join("keys").is_dir());
    assert!(root.join("locks").is_dir());
}

#[test]
fn cache_root_rejects_foreign_nonempty_directory_without_mutating_it() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("foreign-cache");
    std::fs::create_dir(&root).unwrap();
    let foreign_file = root.join("existing-data");
    std::fs::write(&foreign_file, b"foreign-owned-data").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let before_bytes = std::fs::read(&foreign_file).unwrap();
    #[cfg(unix)]
    let before_mode = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(&root).unwrap().permissions().mode() & 0o777
    };

    assert!(matches!(
        CacheVersionStoreV1::new(root.clone(), CacheLimitsV1::default()),
        Err(CacheReconciliationError::ForeignNonEmptyRoot)
    ));

    assert_eq!(std::fs::read(&foreign_file).unwrap(), before_bytes);
    assert!(!root.join(".mdp-cache-owner").exists());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            before_mode
        );
    }
}

#[test]
fn cache_root_rejects_invalid_ownership_marker_without_mutating_it() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("invalid-owned-cache");
    std::fs::create_dir(&root).unwrap();
    let marker = root.join(".mdp-cache-owner");
    std::fs::write(&marker, b"foreign-marker\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let before_marker = std::fs::read(&marker).unwrap();
    #[cfg(unix)]
    let before_mode = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(&root).unwrap().permissions().mode() & 0o777
    };

    assert!(matches!(
        CacheVersionStoreV1::new(root.clone(), CacheLimitsV1::default()),
        Err(CacheReconciliationError::ForeignNonEmptyRoot)
    ));

    assert_eq!(std::fs::read(&marker).unwrap(), before_marker);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            before_mode
        );
    }
}

#[test]
fn concurrent_cache_root_initialization_uses_one_valid_marker() {
    use std::sync::{Arc, Barrier};

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("concurrent-cache");
    let barrier = Arc::new(Barrier::new(4));
    let workers = (0..4)
        .map(|_| {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                CacheVersionStoreV1::new(root, CacheLimitsV1::default())
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    let failures = results
        .iter()
        .enumerate()
        .filter_map(|(index, result)| {
            result
                .as_ref()
                .err()
                .map(|error| format!("worker {index}: {error:?}"))
        })
        .collect::<Vec<_>>();
    assert!(
        results.iter().all(|result| result.is_ok()),
        "concurrent cache-root initializers failed: {failures:?}"
    );
    assert_eq!(
        std::fs::read(root.join(".mdp-cache-owner")).unwrap(),
        b"lqepoch.mdp.cache-root.v1\n"
    );
}

#[test]
fn same_feed_policy_uses_exact_decimal_tolerance_and_rejects_material_changes() {
    let policy = SameFeedTolerancePolicyV1 {
        max_abs_delta_by_field: BTreeMap::from([("close".to_owned(), "0.01".to_owned())]),
        max_changed_rows: 1,
        max_added_rows: 0,
        allow_removed_rows: false,
    };
    let within = compare_same_feed_authoritative(
        &[row("bar-1", "10.00", "2")],
        &[row("bar-1", "10.01", "2")],
        &policy,
    )
    .unwrap();
    assert_eq!(within.changed_rows, 1);
    assert_eq!(within.tolerated_decimal_fields, 1);
    assert!(matches!(
        compare_same_feed_authoritative(
            &[row("bar-1", "10.00", "2")],
            &[row("bar-1", "10.02", "2")],
            &policy
        ),
        Err(CacheReconciliationError::Conflict)
    ));
}

#[test]
fn merged_futu_recent_and_buffer_run_compares_only_the_shared_old_session() {
    let regular_session = "2026-10-08-regular";
    let after_hours_session = "2026-10-08-after-hours";
    let mut futu_key = key("futu", "futu", EntitlementState::Unknown);
    futu_key.session_scope = "extended".to_owned();
    futu_key.session_instance_id = "merged-recent-buffer-window".to_owned();
    futu_key.window_end_exclusive_utc = "2026-10-08T20:01:00Z".to_owned();
    let alpaca_key = key("alpaca", "sip", EntitlementState::Authorized);
    let identity = CacheLogicalIdentityV1::from_key(&futu_key).unwrap();

    let futu_coverage = CoverageEvidenceV1 {
        covered_start_utc: "2026-10-08T13:30:00Z".to_owned(),
        covered_end_exclusive_utc: "2026-10-08T20:01:00Z".to_owned(),
        response_observed_at_utc: "2026-10-08T20:02:00Z".to_owned(),
        pages_seen: Some(1),
        pages_exhausted: Some(true),
        sequence_gap_count: None,
        coverage_gap_count: Some(0),
        truncated: false,
        segments: vec![
            logical_segment(
                &futu_key,
                regular_session,
                "2026-10-08T13:30:00Z",
                "2026-10-08T13:32:00Z",
                "futu-regular",
            ),
            logical_segment(
                &futu_key,
                after_hours_session,
                "2026-10-08T20:00:00Z",
                "2026-10-08T20:01:00Z",
                "futu-after-hours",
            ),
        ],
    };
    let alpaca_coverage = CoverageEvidenceV1 {
        covered_start_utc: "2026-10-08T13:30:00Z".to_owned(),
        covered_end_exclusive_utc: "2026-10-08T13:32:00Z".to_owned(),
        response_observed_at_utc: "2026-10-08T13:33:00Z".to_owned(),
        pages_seen: Some(1),
        pages_exhausted: Some(true),
        sequence_gap_count: None,
        coverage_gap_count: Some(0),
        truncated: false,
        segments: vec![logical_segment(
            &alpaca_key,
            regular_session,
            "2026-10-08T13:30:00Z",
            "2026-10-08T13:32:00Z",
            "alpaca-regular",
        )],
    };
    let futu_rows = vec![
        logical_bar_row(
            regular_session,
            "2026-10-08T13:30:00Z",
            "2026-10-08T13:31:00Z",
            "600.25",
            "futu-regular-source",
        ),
        logical_bar_row(
            regular_session,
            "2026-10-08T13:31:00Z",
            "2026-10-08T13:32:00Z",
            "600.26",
            "futu-regular-source",
        ),
        logical_bar_row(
            after_hours_session,
            "2026-10-08T20:00:00Z",
            "2026-10-08T20:01:00Z",
            "999.00",
            "futu-after-source",
        ),
    ];
    let alpaca_rows = vec![
        logical_bar_row(
            regular_session,
            "2026-10-08T13:30:00Z",
            "2026-10-08T13:31:00Z",
            "600.25",
            "alpaca-regular-source",
        ),
        logical_bar_row(
            regular_session,
            "2026-10-08T13:31:00Z",
            "2026-10-08T13:32:00Z",
            "600.26",
            "alpaca-regular-source",
        ),
    ];
    let futu_version = "1".repeat(64);
    let alpaca_version = "2".repeat(64);
    let comparison = compare_session_overlap(
        &identity,
        regular_session,
        LogicalSessionSnapshotRefV1 {
            key: &futu_key,
            authority: CacheAuthorityV1::Provisional,
            version_id: &futu_version,
            coverage: &futu_coverage,
            rows: &futu_rows,
            evidence_status: CacheLogicalEvidenceStatusV1::ProvisionalObservation,
        },
        LogicalSessionSnapshotRefV1 {
            key: &alpaca_key,
            authority: CacheAuthorityV1::Authoritative,
            version_id: &alpaca_version,
            coverage: &alpaca_coverage,
            rows: &alpaca_rows,
            evidence_status: CacheLogicalEvidenceStatusV1::AdmissionVerifiedAtPublication,
        },
        &SameFeedTolerancePolicyV1::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(comparison.session_instance_id, regular_session);
    assert_eq!(comparison.kind, CacheLogicalComparisonKindV1::CrossProvider);
    assert_eq!(comparison.summary.compared_rows, 2);
    assert_eq!(comparison.summary.changed_rows, 0);
    assert_eq!(comparison.compared_start_utc, "2026-10-08T13:30:00Z");
    assert_eq!(
        comparison.compared_end_exclusive_utc,
        "2026-10-08T13:32:00Z"
    );

    let mut minor_change_rows = alpaca_rows.clone();
    minor_change_rows[1].high = market_contracts::DecimalString::new("600.27").unwrap();
    minor_change_rows[1].close = market_contracts::DecimalString::new("600.27").unwrap();
    let cross_provider_tolerance = SameFeedTolerancePolicyV1 {
        max_abs_delta_by_field: BTreeMap::from([
            ("close".to_owned(), "0.01".to_owned()),
            ("high".to_owned(), "0.01".to_owned()),
        ]),
        max_changed_rows: 1,
        max_added_rows: 0,
        allow_removed_rows: false,
    };
    let tolerated = compare_session_overlap(
        &identity,
        regular_session,
        LogicalSessionSnapshotRefV1 {
            key: &futu_key,
            authority: CacheAuthorityV1::Provisional,
            version_id: &futu_version,
            coverage: &futu_coverage,
            rows: &futu_rows,
            evidence_status: CacheLogicalEvidenceStatusV1::ProvisionalObservation,
        },
        LogicalSessionSnapshotRefV1 {
            key: &alpaca_key,
            authority: CacheAuthorityV1::Authoritative,
            version_id: &alpaca_version,
            coverage: &alpaca_coverage,
            rows: &minor_change_rows,
            evidence_status: CacheLogicalEvidenceStatusV1::AdmissionVerifiedAtPublication,
        },
        &cross_provider_tolerance,
    )
    .unwrap()
    .unwrap();
    assert_eq!(tolerated.summary.changed_rows, 1);
    assert_eq!(tolerated.summary.tolerated_decimal_fields, 2);

    let mut alpaca_merged_key = alpaca_key.clone();
    alpaca_merged_key.session_scope = "extended".to_owned();
    alpaca_merged_key.session_instance_id = "alpaca-merged-window".to_owned();
    alpaca_merged_key.window_end_exclusive_utc = "2026-10-08T20:01:00Z".to_owned();
    let alpaca_merged_coverage = CoverageEvidenceV1 {
        covered_start_utc: "2026-10-08T13:30:00Z".to_owned(),
        covered_end_exclusive_utc: "2026-10-08T20:01:00Z".to_owned(),
        response_observed_at_utc: "2026-10-08T20:02:00Z".to_owned(),
        pages_seen: Some(1),
        pages_exhausted: Some(true),
        sequence_gap_count: None,
        coverage_gap_count: Some(0),
        truncated: false,
        segments: vec![
            logical_segment(
                &alpaca_merged_key,
                regular_session,
                "2026-10-08T13:30:00Z",
                "2026-10-08T13:32:00Z",
                "alpaca-merged-regular",
            ),
            logical_segment(
                &alpaca_merged_key,
                after_hours_session,
                "2026-10-08T20:00:00Z",
                "2026-10-08T20:01:00Z",
                "alpaca-merged-after-hours",
            ),
        ],
    };
    let mut conflicting_rows = alpaca_rows.clone();
    conflicting_rows.push(logical_bar_row(
        after_hours_session,
        "2026-10-08T20:00:00Z",
        "2026-10-08T20:01:00Z",
        "1000.00",
        "alpaca-after-source",
    ));
    let batch = compare_session_batch(
        &identity,
        LogicalSessionSnapshotRefV1 {
            key: &alpaca_merged_key,
            authority: CacheAuthorityV1::Authoritative,
            version_id: &alpaca_version,
            coverage: &alpaca_merged_coverage,
            rows: &conflicting_rows,
            evidence_status: CacheLogicalEvidenceStatusV1::AdmissionVerifiedAtPublication,
        },
        &[
            (
                regular_session.to_owned(),
                LogicalSessionSnapshotRefV1 {
                    key: &futu_key,
                    authority: CacheAuthorityV1::Provisional,
                    version_id: &futu_version,
                    coverage: &futu_coverage,
                    rows: &futu_rows,
                    evidence_status: CacheLogicalEvidenceStatusV1::ProvisionalObservation,
                },
            ),
            (
                after_hours_session.to_owned(),
                LogicalSessionSnapshotRefV1 {
                    key: &futu_key,
                    authority: CacheAuthorityV1::Provisional,
                    version_id: &futu_version,
                    coverage: &futu_coverage,
                    rows: &futu_rows,
                    evidence_status: CacheLogicalEvidenceStatusV1::ProvisionalObservation,
                },
            ),
        ],
        &SameFeedTolerancePolicyV1::default(),
        &SameFeedTolerancePolicyV1::default(),
    )
    .unwrap();
    assert!(batch.comparisons.contains_key(regular_session));
    assert_eq!(
        batch.conflict_sessions,
        BTreeSet::from([after_hours_session.to_owned()])
    );
    assert_eq!(
        batch.cross_provider_conflict_sessions,
        BTreeSet::from([after_hours_session.to_owned()])
    );
}

#[test]
fn paged_gaps_truncation_and_unexhausted_pages_cannot_be_complete() {
    let key = key("alpaca", "sip", EntitlementState::Authorized);
    for evidence in [
        coverage(&key, false, 0, false),
        coverage(&key, true, 1, false),
        coverage(&key, true, 0, true),
    ] {
        evidence.validate_shape(&key).unwrap();
        assert!(matches!(
            evidence.validate_complete(&key),
            Err(CacheReconciliationError::Incomplete)
        ));
    }
    coverage(&key, true, 0, false)
        .validate_complete(&key)
        .unwrap();
    let mut partial_segment = coverage(&key, true, 0, false);
    partial_segment.segments[0].coverage_status = market_contracts::BarsCoverageStatusV1::Partial;
    assert!(matches!(
        partial_segment.validate_complete(&key),
        Err(CacheReconciliationError::Incomplete)
    ));
}

#[test]
fn historical_bars_do_not_require_stream_sequences_when_page_facts_are_complete() {
    let key = key("alpaca", "sip", EntitlementState::Authorized);
    let mut evidence = coverage(&key, true, 0, false);
    evidence.sequence_gap_count = None;
    assert!(
        evidence
            .segments
            .iter()
            .all(|segment| segment.first_source_sequence.is_none())
    );
    evidence.validate_complete(&key).unwrap();
}

#[test]
fn cancelled_cache_read_is_discarded_before_starting_work() {
    let temp = tempfile::tempdir().unwrap();
    let store = CacheVersionStoreV1::new(temp.path(), CacheLimitsV1::default()).unwrap();
    let cancellation = crate::cancellation::CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        store.load_current_cancellable(
            &key("alpaca", "sip", EntitlementState::Authorized),
            1,
            &cancellation,
        ),
        Err(CacheReconciliationError::Cancelled)
    ));
}

#[test]
fn cache_fingerprint_separates_source_and_canonical_dimensions() {
    let base = key("alpaca", "sip", EntitlementState::Authorized);
    let mut variants = Vec::new();
    let mut changed = base.clone();
    changed.provider = "futu".to_owned();
    variants.push(changed);
    let mut changed = base.clone();
    changed.feed = "iex".to_owned();
    variants.push(changed);
    let mut changed = base.clone();
    changed.adjustment = "split".to_owned();
    variants.push(changed);
    let mut changed = base.clone();
    changed.requested_resolution = "5m".to_owned();
    variants.push(changed);
    let mut changed = base.clone();
    changed.effective_resolution = "5m".to_owned();
    variants.push(changed);
    let mut changed = base.clone();
    changed.session_scope = "extended".to_owned();
    variants.push(changed);
    let mut changed = base.clone();
    changed.entitlement = EntitlementState::Unknown;
    variants.push(changed);
    let mut changed = base.clone();
    changed.asset_class = "stock".to_owned();
    variants.push(changed);
    let mut changed = base.clone();
    changed.source_scope_id = "alpaca-iex-v1".to_owned();
    variants.push(changed);
    let mut changed = base.clone();
    changed.entitlement_scope_sha256 = "6".repeat(64);
    variants.push(changed);
    let base = base.fingerprint().unwrap();
    for variant in variants {
        assert_ne!(base, variant.fingerprint().unwrap());
    }

    let mut malformed_asset_class = key("alpaca", "sip", EntitlementState::Authorized);
    malformed_asset_class.asset_class = "crypto".to_owned();
    assert!(matches!(
        malformed_asset_class.validate(),
        Err(CacheReconciliationError::InvalidInput)
    ));
    let mut malformed_schema = key("alpaca", "sip", EntitlementState::Authorized);
    malformed_schema.schema_version = 2;
    assert!(matches!(
        malformed_schema.validate(),
        Err(CacheReconciliationError::InvalidInput)
    ));
}

#[test]
fn request_key_is_available_before_fetch_and_never_contains_the_raw_cursor() {
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarResolutionV1, BarsProviderModeV1, BarsRequestV1,
        NumericEncodingV1, ProtoTimestampV2, SessionScopeV1,
    };

    let request = BarsRequestV1 {
        symbol: "QQQ".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: ProtoTimestampV2::parse("2026-10-08T13:30:00Z").unwrap(),
        end: ProtoTimestampV2::parse("2026-10-08T13:31:00Z").unwrap(),
        resolution: BarResolutionV1::OneMinute,
        session_scope: SessionScopeV1::Regular,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::Auto,
        allow_daily_downgrade: false,
        limit: 1,
        cursor: Some("opaque-provider-cursor-secret".to_owned()),
    };
    let mut scope = CacheQuerySourceScopeV1 {
        provider: "alpaca".to_owned(),
        feed: "sip".to_owned(),
        source_scope_id: "alpaca-sip-v1".to_owned(),
        entitlement_scope_sha256: "6".repeat(64),
        rights_profile_sha256: "7".repeat(64),
        entitlement: EntitlementState::Authorized,
        numeric_encoding: NumericEncodingV1::DecimalToken,
        effective_resolution: BarResolutionV1::OneMinute,
        session_instance_ids: vec!["2026-10-08-regular".to_owned()],
        market_calendar_version: Some("XNYS-2026a".to_owned()),
    };
    let key = CacheKeyV1::from_request(&request, &scope).unwrap();
    assert_eq!(key.asset_class, "etf");
    assert!(key.request_cursor_sha256.is_some());
    assert!(
        !serde_json::to_string(&key)
            .unwrap()
            .contains("opaque-provider-cursor-secret")
    );

    scope.feed = "unknown".to_owned();
    scope.source_scope_id = "alpaca-unknown-feed-v1".to_owned();
    let unknown_feed_key = CacheKeyV1::from_request(&request, &scope).unwrap();
    assert_ne!(
        key.fingerprint().unwrap(),
        unknown_feed_key.fingerprint().unwrap()
    );

    let mut stock_request = request.clone();
    stock_request.asset_class = AssetClassV1::Stock;
    let same_symbol_stock_key = CacheKeyV1::from_request(&stock_request, &scope).unwrap();
    assert_eq!(key.symbol, same_symbol_stock_key.symbol);
    assert_ne!(
        key.fingerprint().unwrap(),
        same_symbol_stock_key.fingerprint().unwrap()
    );

    let mut different_actual_provider = scope.clone();
    different_actual_provider.provider = "futu".to_owned();
    different_actual_provider.feed = "futu".to_owned();
    different_actual_provider.source_scope_id = "futu-v1".to_owned();
    let futu_key = CacheKeyV1::from_request(&request, &different_actual_provider).unwrap();
    assert_ne!(key.fingerprint().unwrap(), futu_key.fingerprint().unwrap());
}

#[test]
fn stable_coverage_identity_survives_new_generations_and_content_hashes() {
    let base = segment("alpaca", "sip", EntitlementState::Authorized);
    let expected = coverage_identity_sha256(std::slice::from_ref(&base)).unwrap();
    let mut refreshed = base.clone();
    refreshed.source_observation_sha256 = "a".repeat(64);
    refreshed.canonical_observation_sha256 = "b".repeat(64);
    refreshed.lineage_receipt_sha256 = "c".repeat(64);
    refreshed.raw_row_count += 10;
    refreshed.canonical_row_count += 10;
    assert_eq!(coverage_identity_sha256(&[refreshed]).unwrap(), expected);
    let mut different_source = base;
    different_source.segment_id = "page-2".to_owned();
    assert_ne!(
        coverage_identity_sha256(&[different_source]).unwrap(),
        expected
    );
}

#[test]
fn production_sources_without_admission_evidence_remain_provisional() {
    for source_key in [
        key("futu", "futu", EntitlementState::Authorized),
        key("alpaca", "sip", EntitlementState::Authorized),
        key("alpaca", "boats", EntitlementState::Unknown),
        key("alpaca", "opra", EntitlementState::Authorized),
    ] {
        assert_eq!(
            super::store::derive_response_authority(
                super::store::CacheAuthorityBasisV1::ProviderAdmissionUnavailable,
                &source_key,
                true,
                &[],
            ),
            CacheAuthorityV1::Provisional
        );
    }
}

#[test]
fn provider_admission_preflight_without_signed_evidence_fails_closed() {
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarResolutionV1, BarsProviderModeV1, BarsRequestV1,
        NumericEncodingV1, ProtoTimestampV2, SessionScopeV1,
    };

    let request = BarsRequestV1 {
        symbol: "QQQ".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: ProtoTimestampV2::parse("2026-10-08T13:30:00Z").unwrap(),
        end: ProtoTimestampV2::parse("2026-10-08T13:31:00Z").unwrap(),
        resolution: BarResolutionV1::OneMinute,
        session_scope: SessionScopeV1::Regular,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::AlpacaOnly,
        allow_daily_downgrade: false,
        limit: 1000,
        cursor: None,
    };
    let scope = CacheQuerySourceScopeV1 {
        provider: "alpaca".to_owned(),
        feed: "sip".to_owned(),
        source_scope_id: "alpaca-sip-v1".to_owned(),
        entitlement_scope_sha256: "6".repeat(64),
        rights_profile_sha256: "7".repeat(64),
        entitlement: EntitlementState::Authorized,
        numeric_encoding: NumericEncodingV1::DecimalToken,
        effective_resolution: BarResolutionV1::OneMinute,
        session_instance_ids: vec!["2026-10-08-regular".to_owned()],
        market_calendar_version: Some("XNYS-2026a".to_owned()),
    };
    let phases = [ProviderPhaseSliceV1 {
        phase: "regular".to_owned(),
        session_instance_id: "2026-10-08-regular".to_owned(),
        start_utc: "2026-10-08T13:30:00Z".to_owned(),
        end_exclusive_utc: "2026-10-08T13:31:00Z".to_owned(),
    }];

    assert!(matches!(
        verify_provider_admission_v1(
            &request,
            &scope,
            &ProviderAdmissionEvidenceV1::default(),
            &UnavailableProviderAdmissionVerifierV1,
            &phases,
            1_800_000_000,
        ),
        Err(CacheReconciliationError::BlockedExternal)
    ));

    let mut unscoped = scope;
    unscoped.rights_profile_sha256 = "not-a-sha256".to_owned();
    assert!(matches!(
        CacheKeyV1::from_request(&request, &unscoped),
        Err(CacheReconciliationError::InvalidInput)
    ));
}

#[test]
fn signed_capability_admission_is_cursor_agnostic_and_expires() {
    let now = 1_800_000_000;
    let mut request = admission_test_request();
    request.cursor = Some("provider-page-2".to_owned());
    let scope = admission_test_scope();
    let phases = admission_test_phases();
    let receipt = admission_test_receipt(&request, &scope, &phases, None, now - 1, now + 60);
    let evidence = admission_test_evidence_with_receipt(receipt, &phases);
    let verifier = DeterministicTestAdmissionVerifier { revoked: false };
    let admission =
        verify_provider_admission_v1(&request, &scope, &evidence, &verifier, &phases, now).unwrap();
    admission
        .validate_for_query(&request, &scope, &phases, now)
        .unwrap();
    assert!(matches!(
        admission.validate_for_dataset_query(&request, &scope, &phases, now),
        Err(CacheReconciliationError::AdmissionMismatch)
    ));
    assert!(matches!(
        admission.validate_for_query(&request, &scope, &phases, now + 60),
        Err(CacheReconciliationError::AdmissionExpired)
    ));

    let mut wrong_phases = phases;
    wrong_phases[0].phase = "after_hours".to_owned();
    assert!(matches!(
        admission.validate_for_query(&request, &scope, &wrong_phases, now),
        Err(CacheReconciliationError::AdmissionMismatch)
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn signed_admission_binds_dataset_and_is_reverified_on_authoritative_reads() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let now = 1_800_000_000;
    let request = admission_test_request();
    let scope = admission_test_scope();
    let phases = admission_test_phases();
    let policy = admission_test_policy();
    let response = admission_test_response("600.25");
    let page_evidence = admission_test_evidence();
    let dataset_binding = provider_admission_dataset_binding_for_bars_responses(
        &request,
        &scope,
        &response,
        &response,
        &page_evidence,
        &policy,
    )
    .unwrap();
    let receipt = admission_test_receipt(
        &request,
        &scope,
        &phases,
        Some(dataset_binding),
        now - 1,
        now + 300,
    );
    let evidence = admission_test_evidence_with_receipt(receipt.clone(), &phases);
    let verifier = DeterministicTestAdmissionVerifier { revoked: false };
    let admission =
        verify_provider_admission_v1(&request, &scope, &evidence, &verifier, &phases, now).unwrap();
    assert_eq!(admission.provider(), "alpaca");
    assert_eq!(
        admission.rights_profile_sha256(),
        scope.rights_profile_sha256
    );

    let mut replayed_request = request.clone();
    replayed_request.cursor = Some("another-provider-page".to_owned());
    assert!(matches!(
        verify_provider_admission_v1(
            &replayed_request,
            &scope,
            &evidence,
            &verifier,
            &phases,
            now,
        ),
        Err(CacheReconciliationError::AdmissionMismatch)
    ));
    let mut wrong_window = request.clone();
    wrong_window.end = market_contracts::ProtoTimestampV2::parse("2026-10-08T13:32:00Z").unwrap();
    assert!(matches!(
        verify_provider_admission_v1(&wrong_window, &scope, &evidence, &verifier, &phases, now),
        Err(CacheReconciliationError::AdmissionMismatch)
    ));
    let mut wrong_rights = scope.clone();
    wrong_rights.rights_profile_sha256 = "9".repeat(64);
    assert!(matches!(
        verify_provider_admission_v1(&request, &wrong_rights, &evidence, &verifier, &phases, now),
        Err(CacheReconciliationError::AdmissionMismatch)
    ));
    let mut bad_runtime = evidence.clone();
    bad_runtime.authorization_context_sha256 = Some("d".repeat(64));
    assert!(matches!(
        verify_provider_admission_v1(&request, &scope, &bad_runtime, &verifier, &phases, now),
        Err(CacheReconciliationError::AdmissionMismatch)
    ));
    let mut bad_signature = evidence.clone();
    bad_signature
        .signed_receipt
        .as_mut()
        .unwrap()
        .signature_base64 = "test-signature:tampered".to_owned();
    assert!(matches!(
        verify_provider_admission_v1(&request, &scope, &bad_signature, &verifier, &phases, now),
        Err(CacheReconciliationError::BlockedExternal)
    ));
    let expired_receipt = admission_test_receipt(
        &request,
        &scope,
        &phases,
        receipt.claims.dataset_binding.clone(),
        now - 1,
        now,
    );
    let expired_evidence = admission_test_evidence_with_receipt(expired_receipt, &phases);
    assert!(matches!(
        verify_provider_admission_v1(&request, &scope, &expired_evidence, &verifier, &phases, now,),
        Err(CacheReconciliationError::AdmissionExpired)
    ));
    let revoked_verifier = DeterministicTestAdmissionVerifier { revoked: true };
    assert!(matches!(
        verify_provider_admission_v1(&request, &scope, &evidence, &revoked_verifier, &phases, now,),
        Err(CacheReconciliationError::BlockedExternal)
    ));

    let temp = tempfile::tempdir().unwrap();
    let store = CacheVersionStoreV1::new(
        temp.path().join("admission-cache"),
        CacheLimitsV1::default(),
    )
    .unwrap();
    let mut changed_data = admission_test_response("600.30");
    changed_data.fetched_at =
        market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:02Z").unwrap();
    assert!(matches!(
        store.publish_bars_responses_with_admission(
            cache_input(
                &request,
                &scope,
                changed_data.clone(),
                changed_data,
                page_evidence.clone(),
            ),
            now,
            &policy,
            &phases,
            &admission,
        ),
        Err(CacheReconciliationError::AdmissionMismatch)
    ));
    let published = store
        .publish_bars_responses_with_admission(
            cache_input(&request, &scope, response.clone(), response, page_evidence),
            now,
            &policy,
            &phases,
            &admission,
        )
        .unwrap();
    assert_eq!(published.authority, CacheAuthorityV1::Authoritative);
    let key = CacheKeyV1::from_request(&request, &scope).unwrap();
    assert!(matches!(
        store.load_current(&key, now + 1),
        Err(CacheReconciliationError::BlockedExternal)
    ));
    let current = store
        .load_current_with_verifier(&key, now + 1, &verifier)
        .unwrap();
    assert_eq!(current.current.authority, CacheAuthorityV1::Authoritative);
    let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();
    assert!(matches!(
        store.load_logical_current(&identity, "2026-10-08-regular", now + 1),
        Err(CacheReconciliationError::BlockedExternal)
    ));
    let logical = store
        .load_logical_current_with_verifier(&identity, "2026-10-08-regular", now + 1, &verifier)
        .unwrap();
    assert_eq!(
        logical.snapshot.current.authority,
        CacheAuthorityV1::Authoritative
    );
    assert!(matches!(
        store.load_current_with_verifier(&key, now + 1, &revoked_verifier),
        Err(CacheReconciliationError::BlockedExternal)
    ));
    let mut second_response = admission_test_response("600.25");
    second_response.fetched_at =
        market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:03Z").unwrap();
    let mut second_page_evidence = admission_test_evidence();
    second_page_evidence.source_observation_sha256 = Some("e".repeat(64));
    second_page_evidence.response_observed_at_utc =
        Some(market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:04Z").unwrap());
    let second_dataset_binding = provider_admission_dataset_binding_for_bars_responses(
        &request,
        &scope,
        &second_response,
        &second_response,
        &second_page_evidence,
        &policy,
    )
    .unwrap();
    let second_receipt = admission_test_receipt(
        &request,
        &scope,
        &phases,
        Some(second_dataset_binding),
        now,
        now + 301,
    );
    let second_evidence = admission_test_evidence_with_receipt(second_receipt, &phases);
    let second_admission = verify_provider_admission_v1(
        &request,
        &scope,
        &second_evidence,
        &verifier,
        &phases,
        now + 1,
    )
    .unwrap();
    let second = store
        .publish_bars_responses_with_admission(
            cache_input(
                &request,
                &scope,
                second_response.clone(),
                second_response,
                second_page_evidence,
            ),
            now + 1,
            &policy,
            &phases,
            &second_admission,
        )
        .unwrap();
    assert_ne!(second.published_version_id, published.published_version_id);
    assert!(matches!(
        store.rollback_to(&key, &published.published_version_id, now + 2),
        Err(CacheReconciliationError::BlockedExternal)
    ));
    let rolled_back = store
        .rollback_to_with_verifier(&key, &published.published_version_id, now + 2, &verifier)
        .unwrap();
    assert_eq!(
        rolled_back.current.unwrap().version_id,
        published.published_version_id
    );
    assert!(matches!(
        store.recover_to(&key, &published.published_version_id, now + 3),
        Err(CacheReconciliationError::BlockedExternal)
    ));
    let recovered = store
        .recover_to_with_verifier(&key, &published.published_version_id, now + 3, &verifier)
        .unwrap();
    assert_eq!(
        recovered.current.unwrap().version_id,
        published.published_version_id
    );
    assert!(matches!(
        store.load_current_with_verifier(&key, now + 300, &verifier),
        Err(CacheReconciliationError::AdmissionExpired)
    ));
}

#[cfg(not(target_os = "linux"))]
#[test]
fn cache_store_reports_bounded_worker_unsupported_without_inline_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let store = CacheVersionStoreV1::new(temp.path(), CacheLimitsV1::default()).unwrap();
    let key = key("alpaca", "sip", EntitlementState::Authorized);
    assert!(matches!(
        store.load_current(&key, 1),
        Err(CacheReconciliationError::Unsupported)
    ));
}

#[cfg(target_os = "linux")]
#[path = "version_store_tests.rs"]
mod version_store_tests;

#[cfg(target_os = "linux")]
#[path = "transaction_tests.rs"]
mod transaction_tests;

#[cfg(target_os = "linux")]
#[test]
fn synthetic_precommit_failure_preserves_another_sources_legacy_current() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("cache");
    let store = CacheVersionStoreV1::new(cache_root.clone(), CacheLimitsV1::default()).unwrap();
    let tolerance = SameFeedTolerancePolicyV1::default();

    let alpaca_request = admission_test_request();
    let alpaca_scope = admission_test_scope();
    let alpaca_response = admission_test_response("600.25");
    let alpaca_key = CacheKeyV1::from_request(&alpaca_request, &alpaca_scope).unwrap();
    let alpaca_version = store
        .publish_bars_responses(
            cache_input(
                &alpaca_request,
                &alpaca_scope,
                alpaca_response.clone(),
                alpaca_response,
                admission_test_evidence(),
            ),
            1_800_000_100,
            &tolerance,
        )
        .unwrap()
        .published_version_id;
    let identity = CacheLogicalIdentityV1::from_key(&alpaca_key).unwrap();
    let logical_dir = cache_root
        .join("logical")
        .join(identity.fingerprint().unwrap());
    let transaction_path = logical_dir.join("transaction.json");
    let alpaca_transaction = fs::read(&transaction_path).unwrap();

    let mut futu_request = alpaca_request.clone();
    futu_request.provider_mode = BarsProviderModeV1::Auto;
    let mut futu_scope = alpaca_scope.clone();
    futu_scope.provider = "futu".to_owned();
    futu_scope.feed = "futu".to_owned();
    futu_scope.source_scope_id = "futu-unknown-history-v1".to_owned();
    futu_scope.entitlement_scope_sha256 = "9".repeat(64);
    futu_scope.entitlement = EntitlementState::Unknown;
    let futu_key = CacheKeyV1::from_request(&futu_request, &futu_scope).unwrap();
    let mut futu_response = admission_test_response("600.26");
    futu_response.provider = "futu".to_owned();
    futu_response.feed = "futu".to_owned();
    futu_response.entitlement_status = market_contracts::BarsEntitlementStatusV1::Unknown;
    futu_response.source_provenance[0].provider = "futu".to_owned();
    futu_response.source_provenance[0].feed = "futu".to_owned();
    futu_response.source_provenance[0].source = "Futu SDK observation".to_owned();
    futu_response.source_provenance[0].entitlement_status =
        market_contracts::BarsEntitlementStatusV1::Unknown;
    let futu_version = store
        .publish_bars_responses(
            cache_input(
                &futu_request,
                &futu_scope,
                futu_response.clone(),
                futu_response,
                admission_test_evidence(),
            ),
            1_800_000_101,
            &tolerance,
        )
        .unwrap()
        .published_version_id;

    // Recreate a legacy source mirror while another provider owns the effective transaction head.
    // This models an existing old-format current that has not yet joined the transaction protocol.
    let futu_key_dir = cache_root
        .join("keys")
        .join(futu_key.fingerprint().unwrap());
    fs::write(&transaction_path, alpaca_transaction).unwrap();
    fs::remove_file(futu_key_dir.join(".transaction-mode")).unwrap();
    fs::remove_file(futu_key_dir.join("transaction-mirror.json")).unwrap();
    assert_eq!(
        store
            .load_current(&futu_key, 1_800_000_102)
            .unwrap()
            .current
            .version_id,
        futu_version
    );

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::BeforeCommitpoint);
    let mut changed_futu_response = admission_test_response("600.27");
    changed_futu_response.provider = "futu".to_owned();
    changed_futu_response.feed = "futu".to_owned();
    changed_futu_response.entitlement_status = market_contracts::BarsEntitlementStatusV1::Unknown;
    changed_futu_response.source_provenance[0].provider = "futu".to_owned();
    changed_futu_response.source_provenance[0].feed = "futu".to_owned();
    changed_futu_response.source_provenance[0].source = "Futu SDK observation".to_owned();
    changed_futu_response.source_provenance[0].entitlement_status =
        market_contracts::BarsEntitlementStatusV1::Unknown;
    assert!(matches!(
        store.publish_bars_responses(
            cache_input(
                &futu_request,
                &futu_scope,
                changed_futu_response.clone(),
                changed_futu_response,
                admission_test_evidence(),
            ),
            1_800_000_103,
            &tolerance,
        ),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    assert!(!futu_key_dir.join(".transaction-mode").exists());
    assert_eq!(
        store
            .load_current(&futu_key, 1_800_000_104)
            .unwrap()
            .current
            .version_id,
        futu_version
    );
    assert_eq!(
        store
            .load_current(&alpaca_key, 1_800_000_104)
            .unwrap()
            .current
            .version_id,
        alpaca_version
    );
}

#[cfg(target_os = "linux")]
#[test]
fn second_session_conflict_keeps_every_existing_logical_pointer() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarCoverageSegmentV1, BarResolutionV1, BarSourceProvenanceV1,
        BarsCacheStatusV1, BarsCoverageStatusV1, BarsEntitlementStatusV1, BarsProviderModeV1,
        BarsRequestV1, BarsResponseV1, NumericEncodingV1, ProtoTimestampV2, SessionScopeV1,
    };

    fn ts(value: &str) -> ProtoTimestampV2 {
        ProtoTimestampV2::parse(value).unwrap()
    }

    let make_request = |start: &str, provider_mode: BarsProviderModeV1| BarsRequestV1 {
        symbol: "QQQ".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: ts(start),
        end: ts("2026-10-08T20:01:00Z"),
        resolution: BarResolutionV1::OneMinute,
        session_scope: SessionScopeV1::All,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode,
        allow_daily_downgrade: false,
        limit: 1000,
        cursor: None,
    };
    let make_source_scope = |provider: &str, feed: &str, entitlement| CacheQuerySourceScopeV1 {
        provider: provider.to_owned(),
        feed: feed.to_owned(),
        source_scope_id: format!("{provider}-{feed}-test-v1"),
        entitlement_scope_sha256: if provider == "futu" {
            "6".repeat(64)
        } else {
            "5".repeat(64)
        },
        rights_profile_sha256: "7".repeat(64),
        entitlement,
        numeric_encoding: NumericEncodingV1::DecimalToken,
        effective_resolution: BarResolutionV1::OneMinute,
        session_instance_ids: vec![
            "2026-10-08-regular".to_owned(),
            "2026-10-08-after-hours".to_owned(),
        ],
        market_calendar_version: Some("XNYS-2026a".to_owned()),
    };
    let make_response = |provider: &str,
                         feed: &str,
                         entitlement_status: BarsEntitlementStatusV1,
                         rows: Vec<BarRowV1>| {
        let mut segments = Vec::new();
        for (session, provenance_id) in [
            ("2026-10-08-regular", "source-regular"),
            ("2026-10-08-after-hours", "source-after-hours"),
        ] {
            let session_rows = rows
                .iter()
                .filter(|row| row.session_instance_id == session)
                .collect::<Vec<_>>();
            if let (Some(first), Some(last)) = (session_rows.first(), session_rows.last()) {
                segments.push(BarCoverageSegmentV1 {
                    start_utc: first.bar_start_utc.clone(),
                    end_exclusive_utc: last.bar_end_exclusive_utc.clone(),
                    coverage_status: BarsCoverageStatusV1::Complete,
                    provenance_id: Some(provenance_id.to_owned()),
                    session_instance_id: session.to_owned(),
                });
            }
        }
        BarsResponseV1 {
            schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
            provider: provider.to_owned(),
            feed: feed.to_owned(),
            entitlement_status,
            requested_resolution: BarResolutionV1::OneMinute,
            effective_resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            rows,
            coverage_status: BarsCoverageStatusV1::Complete,
            segments,
            gaps: Vec::new(),
            source_provenance: vec![
                BarSourceProvenanceV1 {
                    provenance_id: "source-regular".to_owned(),
                    provider: provider.to_owned(),
                    feed: feed.to_owned(),
                    source: "synthetic cache fixture".to_owned(),
                    entitlement_status,
                    numeric_encoding: NumericEncodingV1::DecimalToken,
                },
                BarSourceProvenanceV1 {
                    provenance_id: "source-after-hours".to_owned(),
                    provider: provider.to_owned(),
                    feed: feed.to_owned(),
                    source: "synthetic cache fixture".to_owned(),
                    entitlement_status,
                    numeric_encoding: NumericEncodingV1::DecimalToken,
                },
            ],
            as_of: None,
            fetched_at: ts("2026-10-08T20:02:00Z"),
            verified_at: None,
            cache_status: BarsCacheStatusV1::Miss,
            market_calendar_version: Some("XNYS-2026a".to_owned()),
            continuation_cursor: None,
            truncated: false,
        }
    };
    let page_evidence = ProviderPageEvidenceV1 {
        response_observed_at_utc: Some(ts("2026-10-08T20:02:00Z")),
        pages_seen: Some(1),
        pages_exhausted: Some(true),
        truncated: false,
        ..ProviderPageEvidenceV1::default()
    };
    let initial_request = make_request("2026-10-08T13:30:00Z", BarsProviderModeV1::FutuOnly);
    let candidate_request = make_request("2026-10-08T13:31:00Z", BarsProviderModeV1::AlpacaOnly);
    let futu_scope = make_source_scope("futu", "futu", EntitlementState::Unknown);
    let alpaca_scope = make_source_scope("alpaca", "sip", EntitlementState::Authorized);
    let initial_rows = vec![
        logical_bar_row(
            "2026-10-08-regular",
            "2026-10-08T13:30:00Z",
            "2026-10-08T13:31:00Z",
            "600.25",
            "source-regular",
        ),
        logical_bar_row(
            "2026-10-08-regular",
            "2026-10-08T13:31:00Z",
            "2026-10-08T13:32:00Z",
            "600.26",
            "source-regular",
        ),
        logical_bar_row(
            "2026-10-08-after-hours",
            "2026-10-08T20:00:00Z",
            "2026-10-08T20:01:00Z",
            "600.30",
            "source-after-hours",
        ),
    ];
    let candidate_rows = vec![
        logical_bar_row(
            "2026-10-08-regular",
            "2026-10-08T13:31:00Z",
            "2026-10-08T13:32:00Z",
            "600.26",
            "source-regular",
        ),
        logical_bar_row(
            "2026-10-08-after-hours",
            "2026-10-08T20:00:00Z",
            "2026-10-08T20:01:00Z",
            "999.00",
            "source-after-hours",
        ),
    ];
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("logical-cache");
    let store = CacheVersionStoreV1::new(cache_root.clone(), CacheLimitsV1::default()).unwrap();
    let futu_response = make_response(
        "futu",
        "futu",
        BarsEntitlementStatusV1::Unknown,
        initial_rows,
    );
    let initial = store
        .publish_bars_responses(
            cache_input_with_test_phases(
                &initial_request,
                &futu_scope,
                futu_response.clone(),
                futu_response,
                page_evidence.clone(),
            ),
            1_800_000_000,
            &SameFeedTolerancePolicyV1::default(),
        )
        .unwrap();
    assert_eq!(initial.authority, CacheAuthorityV1::Provisional);
    assert_eq!(
        initial.logical_invalidation.as_ref().unwrap().changes.len(),
        2
    );

    let candidate_response = make_response(
        "alpaca",
        "sip",
        BarsEntitlementStatusV1::Authorized,
        candidate_rows,
    );
    let candidate_key = CacheKeyV1::from_request(&candidate_request, &alpaca_scope).unwrap();
    assert!(matches!(
        store.publish_synthetic_verified_bars_responses(
            cache_input_with_test_phases(
                &candidate_request,
                &alpaca_scope,
                candidate_response.clone(),
                candidate_response,
                page_evidence,
            ),
            1_800_000_901,
            &SameFeedTolerancePolicyV1::default(),
        ),
        Err(CacheReconciliationError::Conflict)
    ));
    assert!(matches!(
        store.load_current(&candidate_key, 1_800_000_011),
        Err(CacheReconciliationError::NotFound)
    ));
    let rejected = store
        .list_verified_versions(&candidate_key, 1_800_000_902)
        .unwrap();
    assert_eq!(rejected.len(), 1);
    assert!(rejected[0].conflict_rejected);
    let candidate_receipt_path = cache_root
        .join("keys")
        .join(candidate_key.fingerprint().unwrap())
        .join("versions")
        .join(&rejected[0].version_id)
        .join("receipt.json");
    let candidate_receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(candidate_receipt_path).unwrap()).unwrap();
    assert!(
        candidate_receipt["logical_comparisons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|comparison| comparison["baseline_evidence_status"]
                == "expired_provisional_historical")
    );

    let initial_key = CacheKeyV1::from_request(&initial_request, &futu_scope).unwrap();
    assert!(matches!(
        store.load_current(&initial_key, 1_800_000_902),
        Err(CacheReconciliationError::Expired)
    ));
    let identity = CacheLogicalIdentityV1::from_key(&initial_key).unwrap();
    let logical_dir = cache_root
        .join("logical")
        .join(identity.fingerprint().unwrap());
    let logical_envelope: serde_json::Value =
        serde_json::from_slice(&fs::read(logical_dir.join("current.json")).unwrap()).unwrap();
    assert_eq!(
        logical_envelope["payload"]["generation"],
        initial.logical_invalidation.as_ref().unwrap().generation
    );
    for session in ["2026-10-08-regular", "2026-10-08-after-hours"] {
        assert_eq!(
            logical_pointer_version_on_disk(&cache_root, &identity, session),
            initial.published_version_id
        );
        assert!(matches!(
            store.load_logical_current(&identity, session, 1_800_000_902),
            Err(CacheReconciliationError::Expired)
        ));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn synthetic_cross_provider_tolerance_allows_minor_candidate_update() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let mut futu_request = admission_test_request();
    futu_request.provider_mode = BarsProviderModeV1::FutuOnly;
    let mut futu_scope = admission_test_scope();
    futu_scope.provider = "futu".to_owned();
    futu_scope.feed = "futu".to_owned();
    futu_scope.source_scope_id = "futu-unknown-history-v1".to_owned();
    futu_scope.entitlement = EntitlementState::Unknown;

    let mut futu_response = admission_test_response("600.25");
    futu_response.provider = "futu".to_owned();
    futu_response.feed = "futu".to_owned();
    futu_response.entitlement_status = market_contracts::BarsEntitlementStatusV1::Unknown;
    futu_response.source_provenance[0].provider = "futu".to_owned();
    futu_response.source_provenance[0].feed = "futu".to_owned();
    futu_response.source_provenance[0].entitlement_status =
        market_contracts::BarsEntitlementStatusV1::Unknown;

    let alpaca_request = admission_test_request();
    let alpaca_scope = admission_test_scope();
    let alpaca_response = admission_test_response("600.26");
    let policy = SameFeedTolerancePolicyV1 {
        max_abs_delta_by_field: BTreeMap::from([
            ("close".to_owned(), "0.01".to_owned()),
            ("high".to_owned(), "0.01".to_owned()),
            ("low".to_owned(), "0.01".to_owned()),
            ("open".to_owned(), "0.01".to_owned()),
        ]),
        max_changed_rows: 1,
        max_added_rows: 0,
        allow_removed_rows: false,
    };
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("minor-conflict-cache");
    let store = CacheVersionStoreV1::new(cache_root, CacheLimitsV1::default()).unwrap();
    let initial = store
        .publish_bars_responses_with_policy(
            cache_input_with_test_phases(
                &futu_request,
                &futu_scope,
                futu_response.clone(),
                futu_response,
                admission_test_evidence(),
            ),
            1_800_000_000,
            &ReconciliationPolicyV1::legacy_same_feed(policy.clone()),
        )
        .unwrap();
    assert_eq!(initial.authority, CacheAuthorityV1::Provisional);

    let candidate = store
        .publish_synthetic_verified_bars_responses(
            cache_input_with_test_phases(
                &alpaca_request,
                &alpaca_scope,
                alpaca_response.clone(),
                alpaca_response,
                admission_test_evidence(),
            ),
            1_800_000_010,
            &policy,
        )
        .unwrap();
    assert_eq!(candidate.authority, CacheAuthorityV1::Authoritative);
    let comparison = candidate.cross_provider_comparison.unwrap();
    assert_eq!(comparison.changed_rows, 1);
    assert_eq!(comparison.tolerated_decimal_fields, 4);
    let invalidation = candidate.logical_invalidation.unwrap();
    assert_eq!(invalidation.changes.len(), 1);
    assert_eq!(
        invalidation.changes[0].session_instance_id,
        "2026-10-08-regular"
    );

    let identity = CacheLogicalIdentityV1::from_key(
        &CacheKeyV1::from_request(&alpaca_request, &alpaca_scope).unwrap(),
    )
    .unwrap();
    let current = store
        .load_logical_current(&identity, "2026-10-08-regular", 1_800_000_011)
        .unwrap();
    assert_eq!(
        current.snapshot.current.version_id,
        candidate.published_version_id
    );
    assert_eq!(
        current.snapshot.rows[0].fields.get("close").unwrap(),
        &CanonicalValueV1::Decimal("600.26".to_owned())
    );
}

#[cfg(target_os = "linux")]
#[test]
fn logical_index_roundtrips_ninety_two_sessions_in_two_shards_after_restart() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let session_ids = (0..92)
        .map(|index| format!("opaque-session-{index:02}"))
        .collect::<Vec<_>>();
    let minute = |offset: usize| {
        let value = 13 * 60 + 30 + offset;
        format!("2026-10-08T{:02}:{:02}:00Z", value / 60, value % 60)
    };
    let request = BarsRequestV1 {
        symbol: "QQQ".to_owned(),
        asset_class: market_contracts::AssetClassV1::Etf,
        start: market_contracts::ProtoTimestampV2::parse(&minute(0)).unwrap(),
        end: market_contracts::ProtoTimestampV2::parse(&minute(session_ids.len())).unwrap(),
        resolution: market_contracts::BarResolutionV1::OneMinute,
        session_scope: market_contracts::SessionScopeV1::All,
        adjustment: market_contracts::AdjustmentV1::Unadjusted,
        provider_mode: market_contracts::BarsProviderModeV1::FutuOnly,
        allow_daily_downgrade: false,
        limit: 1000,
        cursor: None,
    };
    let source_scope = CacheQuerySourceScopeV1 {
        provider: "futu".to_owned(),
        feed: "futu".to_owned(),
        source_scope_id: "futu-unknown-history-v1".to_owned(),
        entitlement_scope_sha256: "6".repeat(64),
        rights_profile_sha256: "7".repeat(64),
        entitlement: EntitlementState::Unknown,
        numeric_encoding: market_contracts::NumericEncodingV1::DecimalToken,
        effective_resolution: market_contracts::BarResolutionV1::OneMinute,
        session_instance_ids: session_ids.clone(),
        market_calendar_version: Some("XNYS-2026a".to_owned()),
    };
    let rows = session_ids
        .iter()
        .enumerate()
        .map(|(index, session)| {
            logical_bar_row(
                session,
                &minute(index),
                &minute(index + 1),
                "600.25",
                "source-1",
            )
        })
        .collect::<Vec<_>>();
    let segments = session_ids
        .iter()
        .enumerate()
        .map(|(index, session)| market_contracts::BarCoverageSegmentV1 {
            start_utc: market_contracts::ProtoTimestampV2::parse(&minute(index)).unwrap(),
            end_exclusive_utc: market_contracts::ProtoTimestampV2::parse(&minute(index + 1))
                .unwrap(),
            coverage_status: market_contracts::BarsCoverageStatusV1::Complete,
            provenance_id: Some("source-1".to_owned()),
            session_instance_id: session.clone(),
        })
        .collect::<Vec<_>>();
    let response = BarsResponseV1 {
        schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
        provider: "futu".to_owned(),
        feed: "futu".to_owned(),
        entitlement_status: market_contracts::BarsEntitlementStatusV1::Unknown,
        requested_resolution: market_contracts::BarResolutionV1::OneMinute,
        effective_resolution: market_contracts::BarResolutionV1::OneMinute,
        adjustment: market_contracts::AdjustmentV1::Unadjusted,
        rows,
        coverage_status: market_contracts::BarsCoverageStatusV1::Complete,
        segments,
        gaps: Vec::new(),
        source_provenance: vec![market_contracts::BarSourceProvenanceV1 {
            provenance_id: "source-1".to_owned(),
            provider: "futu".to_owned(),
            feed: "futu".to_owned(),
            source: "synthetic bounded-index fixture".to_owned(),
            entitlement_status: market_contracts::BarsEntitlementStatusV1::Unknown,
            numeric_encoding: market_contracts::NumericEncodingV1::DecimalToken,
        }],
        as_of: None,
        fetched_at: market_contracts::ProtoTimestampV2::parse(&minute(66)).unwrap(),
        verified_at: None,
        cache_status: market_contracts::BarsCacheStatusV1::Miss,
        market_calendar_version: Some("XNYS-2026a".to_owned()),
        continuation_cursor: None,
        truncated: false,
    };
    let page_evidence = ProviderPageEvidenceV1 {
        source_observation_sha256: Some("a".repeat(64)),
        response_observed_at_utc: Some(
            market_contracts::ProtoTimestampV2::parse(&minute(66)).unwrap(),
        ),
        pages_seen: Some(1),
        pages_exhausted: Some(true),
        truncated: false,
        ..ProviderPageEvidenceV1::default()
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("multi-session-cache");
    let store = CacheVersionStoreV1::new(root.clone(), CacheLimitsV1::default()).unwrap();
    let published = store
        .publish_bars_responses(
            cache_input_with_test_phases(
                &request,
                &source_scope,
                response.clone(),
                response,
                page_evidence,
            ),
            1_800_000_000,
            &SameFeedTolerancePolicyV1::default(),
        )
        .unwrap();
    assert_eq!(published.authority, CacheAuthorityV1::Provisional);
    assert!(!published.logical_index_overflow);
    assert_eq!(
        published
            .logical_invalidation
            .as_ref()
            .unwrap()
            .changes
            .len(),
        92
    );

    let reopened = CacheVersionStoreV1::new(root, CacheLimitsV1::default()).unwrap();
    let key = CacheKeyV1::from_request(&request, &source_scope).unwrap();
    let current = reopened.load_current(&key, 1_800_000_001).unwrap();
    assert_eq!(current.raw_bars_rows.len(), 92);
    let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();
    for session in [session_ids.first().unwrap(), session_ids.last().unwrap()] {
        let logical = reopened
            .load_logical_current(&identity, session, 1_800_000_001)
            .unwrap();
        assert_eq!(logical.session.current.session_instance_id, *session);
        assert_eq!(logical.logical_current.shards.len(), 2);
        assert!(
            logical
                .logical_current
                .shards
                .keys()
                .all(|shard_id| shard_id.starts_with("2026-10-"))
        );
        assert!(logical.logical_current.shards.values().all(|shard| {
            shard.session_count as usize <= MAX_LOGICAL_SESSIONS_PER_SHARD
                && shard.byte_length as usize <= super::logical_index::MAX_LOGICAL_SHARD_BYTES
        }));
        assert_eq!(
            logical.snapshot.current.version_id,
            published.published_version_id
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn missing_calendar_phases_keeps_provisional_artifacts_without_logical_indexing() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let session = "provider-session-opaque-17";
    let request = admission_test_request();
    let mut source_scope = admission_test_scope();
    source_scope.provider = "futu".to_owned();
    source_scope.feed = "futu".to_owned();
    source_scope.source_scope_id = "futu-unknown-history-v1".to_owned();
    source_scope.entitlement = EntitlementState::Unknown;
    source_scope.session_instance_ids = vec![session.to_owned()];
    let mut request = request;
    request.provider_mode = market_contracts::BarsProviderModeV1::FutuOnly;
    let mut response = admission_test_response("600.25");
    response.provider = "futu".to_owned();
    response.feed = "futu".to_owned();
    response.entitlement_status = market_contracts::BarsEntitlementStatusV1::Unknown;
    response.source_provenance[0].provider = "futu".to_owned();
    response.source_provenance[0].feed = "futu".to_owned();
    response.source_provenance[0].entitlement_status =
        market_contracts::BarsEntitlementStatusV1::Unknown;
    response.rows[0].session_instance_id = session.to_owned();
    response.segments[0].session_instance_id = session.to_owned();
    let temp = tempfile::tempdir().unwrap();
    let store = CacheVersionStoreV1::new(
        temp.path().join("missing-phases-cache"),
        CacheLimitsV1::default(),
    )
    .unwrap();
    let published = store
        .publish_bars_responses(
            cache_input(
                &request,
                &source_scope,
                response.clone(),
                response,
                admission_test_evidence(),
            ),
            1_800_000_000,
            &SameFeedTolerancePolicyV1::default(),
        )
        .unwrap();
    assert_eq!(published.authority, CacheAuthorityV1::Provisional);
    assert_eq!(
        published.logical_index_status,
        CacheLogicalIndexStatusV1::PhaseMappingUnavailable
    );
    assert!(!published.logical_index_overflow);
    assert!(published.logical_invalidation.is_none());
    let key = CacheKeyV1::from_request(&request, &source_scope).unwrap();
    assert_eq!(
        store
            .load_current(&key, 1_800_000_001)
            .unwrap()
            .raw_bars_rows
            .len(),
        1
    );
    let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();
    assert!(matches!(
        store.load_logical_current(&identity, session, 1_800_000_001),
        Err(CacheReconciliationError::NotFound)
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn logical_index_keeps_more_than_legacy_capacity_in_bounded_shards() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let session = "opaque-session-new";
    let request = admission_test_request();
    let mut source_scope = admission_test_scope();
    source_scope.provider = "futu".to_owned();
    source_scope.feed = "futu".to_owned();
    source_scope.source_scope_id = "futu-unknown-history-v1".to_owned();
    source_scope.entitlement = EntitlementState::Unknown;
    source_scope.session_instance_ids = vec![session.to_owned()];
    let mut request = request;
    request.provider_mode = market_contracts::BarsProviderModeV1::FutuOnly;
    let key = CacheKeyV1::from_request(&request, &source_scope).unwrap();
    let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();
    let key_fingerprint = key.fingerprint().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("large-logical-cache");
    let store = CacheVersionStoreV1::new(root.clone(), CacheLimitsV1::default()).unwrap();
    let mut sessions = BTreeMap::new();
    let mut session_months = BTreeMap::new();
    for index in 0..super::logical_index::MAX_LOGICAL_SESSIONS {
        let session_id = format!("old-session-{index:04}");
        session_months.insert(session_id.clone(), "2026-10".to_owned());
        sessions.insert(
            session_id.clone(),
            CacheLogicalSessionCurrentV1 {
                current: CacheLogicalVersionRefV1 {
                    key: key.clone(),
                    key_fingerprint_sha256: key_fingerprint.clone(),
                    version_id: format!("{index:064x}"),
                    receipt_sha256: format!("{:064x}", index + 1),
                    session_instance_id: session_id,
                    shard_month: "2026-10".to_owned(),
                    authority: CacheAuthorityV1::Provisional,
                    covered_start_utc: key.window_start_utc.clone(),
                    covered_end_exclusive_utc: key.window_end_exclusive_utc.clone(),
                    created_at_unix_seconds: 1_799_999_000,
                    expires_at_unix_seconds: 1_800_003_000,
                },
                reconciliation_policy_sha256: "b".repeat(64),
                comparison: None,
            },
        );
    }
    store
        .write_logical_current_for_test(&CacheLogicalCurrentV1 {
            identity: identity.clone(),
            generation: 1,
            sessions,
            session_months,
            shards: BTreeMap::new(),
            updated_at_unix_seconds: 1_800_000_000,
        })
        .unwrap();

    let mut response = admission_test_response("600.25");
    response.provider = "futu".to_owned();
    response.feed = "futu".to_owned();
    response.entitlement_status = market_contracts::BarsEntitlementStatusV1::Unknown;
    response.source_provenance[0].provider = "futu".to_owned();
    response.source_provenance[0].feed = "futu".to_owned();
    response.source_provenance[0].entitlement_status =
        market_contracts::BarsEntitlementStatusV1::Unknown;
    response.rows[0].session_instance_id = session.to_owned();
    response.segments[0].session_instance_id = session.to_owned();
    response.fetched_at =
        market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:01Z").unwrap();
    let mut page_evidence = admission_test_evidence();
    page_evidence.response_observed_at_utc =
        Some(market_contracts::ProtoTimestampV2::parse("2026-10-08T13:31:02Z").unwrap());
    let published = store
        .publish_bars_responses(
            cache_input_with_test_phases(
                &request,
                &source_scope,
                response.clone(),
                response,
                page_evidence,
            ),
            1_800_000_001,
            &SameFeedTolerancePolicyV1::default(),
        )
        .unwrap();
    assert!(!published.logical_index_overflow);
    assert_eq!(
        published.logical_index_status,
        CacheLogicalIndexStatusV1::Updated
    );
    let current = store.load_current(&key, 1_800_000_002).unwrap();
    assert_eq!(current.raw_bars_rows.len(), 1);
    assert_eq!(current.current.version_id, published.published_version_id);
    let logical = store
        .load_logical_current(&identity, session, 1_800_000_002)
        .unwrap();
    assert_eq!(logical.logical_current.sessions.len(), 2049);
    assert_eq!(logical.logical_current.shards.len(), 33);
    let versions = store.list_verified_versions(&key, 1_800_000_002).unwrap();
    assert_eq!(versions.len(), 1);
    assert!(!versions[0].logical_index_overflow);
    assert_eq!(
        versions[0].logical_index_status,
        CacheLogicalIndexStatusV1::Updated
    );

    let reopened = CacheVersionStoreV1::new(root, CacheLimitsV1::default()).unwrap();
    assert_eq!(
        reopened
            .load_current(&key, 1_800_000_003)
            .unwrap()
            .raw_bars_rows
            .len(),
        1
    );
    let reopened_logical = reopened
        .load_logical_current(&identity, session, 1_800_000_003)
        .unwrap();
    assert_eq!(reopened_logical.logical_current.sessions.len(), 2049);
    assert_eq!(reopened_logical.logical_current.shards.len(), 33);
}

fn synthetic_alpaca_complete_timeline_fixture(
    missing_index: Option<usize>,
) -> (
    BarsRequestV1,
    CacheQuerySourceScopeV1,
    BarsResponseV1,
    BarsResponseV1,
    Vec<ProviderPhaseSliceV1>,
) {
    use chrono::{Duration, SecondsFormat, Utc};
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarCoverageSegmentV1, BarResolutionV1, BarSourceProvenanceV1,
        BarsCacheStatusV1, BarsCoverageStatusV1, BarsEntitlementStatusV1, BarsProviderModeV1,
        BarsRequestV1, BarsResponseV1, BarsValidationStateV1, NumericEncodingV1, ProtoTimestampV2,
        SessionScopeV1,
    };

    let start = chrono::DateTime::parse_from_rfc3339("2026-10-08T13:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let end = start + Duration::hours(6) + Duration::minutes(30);
    let format = |value: &chrono::DateTime<Utc>| value.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let session = "2026-10-08-regular";
    let request = BarsRequestV1 {
        symbol: "QQQ".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: ProtoTimestampV2::parse(&format(&start)).unwrap(),
        end: ProtoTimestampV2::parse(&format(&end)).unwrap(),
        resolution: BarResolutionV1::OneMinute,
        session_scope: SessionScopeV1::Regular,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::AlpacaOnly,
        allow_daily_downgrade: false,
        limit: 1_000,
        cursor: None,
    };
    let source_scope = CacheQuerySourceScopeV1 {
        provider: "alpaca".to_owned(),
        feed: "sip".to_owned(),
        source_scope_id: "alpaca-sip-v1".to_owned(),
        entitlement_scope_sha256: "6".repeat(64),
        rights_profile_sha256: "7".repeat(64),
        entitlement: EntitlementState::Unknown,
        numeric_encoding: NumericEncodingV1::DecimalToken,
        effective_resolution: BarResolutionV1::OneMinute,
        session_instance_ids: vec![session.to_owned()],
        market_calendar_version: Some("XNYS-2026a".to_owned()),
    };
    let make_response = |row_session: &str| {
        let mut rows = Vec::new();
        for index in 0..390usize {
            if missing_index == Some(index) {
                continue;
            }
            let bar_start = start + Duration::minutes(index as i64);
            let bar_end = bar_start + Duration::minutes(1);
            let mut row = logical_bar_row(
                row_session,
                &format(&bar_start),
                &format(&bar_end),
                "10.25",
                "alpaca-sdk-row",
            );
            row.validation_state = BarsValidationStateV1::Provisional;
            rows.push(row);
        }
        let segments = rows
            .iter()
            .map(|row| BarCoverageSegmentV1 {
                start_utc: row.bar_start_utc.clone(),
                end_exclusive_utc: row.bar_end_exclusive_utc.clone(),
                coverage_status: BarsCoverageStatusV1::Unknown,
                provenance_id: Some(row.provenance_id.clone()),
                session_instance_id: row.session_instance_id.clone(),
            })
            .collect();
        BarsResponseV1 {
            schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
            provider: "alpaca".to_owned(),
            feed: "sip".to_owned(),
            entitlement_status: BarsEntitlementStatusV1::Unknown,
            requested_resolution: BarResolutionV1::OneMinute,
            effective_resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            rows,
            coverage_status: BarsCoverageStatusV1::Unknown,
            segments,
            gaps: Vec::new(),
            source_provenance: vec![BarSourceProvenanceV1 {
                provenance_id: "alpaca-sdk-row".to_owned(),
                provider: "alpaca".to_owned(),
                feed: "sip".to_owned(),
                source: "successful Alpaca stock-bars SDK query".to_owned(),
                entitlement_status: BarsEntitlementStatusV1::Unknown,
                numeric_encoding: NumericEncodingV1::DecimalToken,
            }],
            as_of: None,
            fetched_at: ProtoTimestampV2::parse("2026-10-08T20:00:02Z").unwrap(),
            verified_at: None,
            cache_status: BarsCacheStatusV1::Unknown,
            market_calendar_version: None,
            continuation_cursor: None,
            truncated: false,
        }
    };
    let phases = vec![ProviderPhaseSliceV1 {
        phase: "regular".to_owned(),
        session_instance_id: session.to_owned(),
        start_utc: format(&start),
        end_exclusive_utc: format(&end),
    }];
    (
        request,
        source_scope,
        make_response("provider-local"),
        make_response(session),
        phases,
    )
}

#[test]
fn synthetic_alpaca_complete_rth_timeline_derives_coverage_without_entitlement_claim() {
    let (request, scope, raw, mut canonical, phases) =
        synthetic_alpaca_complete_timeline_fixture(None);
    assert_eq!(raw.rows.len(), 390);
    assert!(raw.validate().is_ok());
    assert!(request.validate_response(&canonical).is_ok());
    assert!(
        super::alpaca_query_access::derive_complete_coverage_from_timeline(
            &request,
            &scope,
            &raw,
            &mut canonical,
            Some(&phases),
            true,
        )
        .unwrap()
    );
    assert_eq!(
        canonical.coverage_status,
        market_contracts::BarsCoverageStatusV1::Complete
    );
    assert_eq!(
        canonical.market_calendar_version.as_deref(),
        Some("XNYS-2026a")
    );
    assert!(
        canonical
            .rows
            .iter()
            .all(|row| row.validation_state == market_contracts::BarsValidationStateV1::Validated)
    );
    assert_eq!(
        canonical.entitlement_status,
        market_contracts::BarsEntitlementStatusV1::Unknown
    );
}

#[test]
fn synthetic_alpaca_missing_minute_stays_provisional_after_sdk_chain_exhaustion() {
    let (request, scope, raw, mut canonical, phases) =
        synthetic_alpaca_complete_timeline_fixture(Some(117));
    assert_eq!(raw.rows.len(), 389);
    canonical.coverage_status = market_contracts::BarsCoverageStatusV1::Complete;
    for segment in &mut canonical.segments {
        segment.coverage_status = market_contracts::BarsCoverageStatusV1::Complete;
    }
    assert!(
        !super::alpaca_query_access::derive_complete_coverage_from_timeline(
            &request,
            &scope,
            &raw,
            &mut canonical,
            Some(&phases),
            true,
        )
        .unwrap()
    );
    assert_eq!(
        canonical.coverage_status,
        market_contracts::BarsCoverageStatusV1::Partial
    );
    assert!(
        canonical
            .segments
            .iter()
            .all(|segment| segment.coverage_status
                == market_contracts::BarsCoverageStatusV1::Partial)
    );
    assert!(canonical
        .rows
        .iter()
        .all(|row| row.validation_state == market_contracts::BarsValidationStateV1::Provisional));
}

#[test]
fn synthetic_alpaca_timeline_without_trusted_phases_or_full_chain_stays_unknown() {
    let (request, scope, raw, mut canonical, phases) =
        synthetic_alpaca_complete_timeline_fixture(None);
    assert!(
        !super::alpaca_query_access::derive_complete_coverage_from_timeline(
            &request,
            &scope,
            &raw,
            &mut canonical,
            None,
            true,
        )
        .unwrap()
    );
    assert!(
        !super::alpaca_query_access::derive_complete_coverage_from_timeline(
            &request,
            &scope,
            &raw,
            &mut canonical,
            Some(&phases),
            false,
        )
        .unwrap()
    );
    assert_eq!(
        canonical.coverage_status,
        market_contracts::BarsCoverageStatusV1::Unknown
    );
}

#[test]
fn synthetic_alpaca_hourly_timeline_accepts_short_final_session_bar() {
    use chrono::{Duration, SecondsFormat, Utc};
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarCoverageSegmentV1, BarResolutionV1, BarSourceProvenanceV1,
        BarsCacheStatusV1, BarsCoverageStatusV1, BarsEntitlementStatusV1, BarsProviderModeV1,
        BarsRequestV1, BarsResponseV1, BarsValidationStateV1, NumericEncodingV1, ProtoTimestampV2,
        SessionScopeV1,
    };

    let start = chrono::DateTime::parse_from_rfc3339("2026-10-08T13:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let end = start + Duration::hours(6) + Duration::minutes(30);
    let format = |value: &chrono::DateTime<Utc>| value.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let session = "2026-10-08-regular";
    let request = BarsRequestV1 {
        symbol: "QQQ".to_owned(),
        asset_class: AssetClassV1::Etf,
        start: ProtoTimestampV2::parse(&format(&start)).unwrap(),
        end: ProtoTimestampV2::parse(&format(&end)).unwrap(),
        resolution: BarResolutionV1::OneHour,
        session_scope: SessionScopeV1::Regular,
        adjustment: AdjustmentV1::Unadjusted,
        provider_mode: BarsProviderModeV1::AlpacaOnly,
        allow_daily_downgrade: false,
        limit: 1_000,
        cursor: None,
    };
    let source_scope = CacheQuerySourceScopeV1 {
        provider: "alpaca".to_owned(),
        feed: "sip".to_owned(),
        source_scope_id: "alpaca-sip-v1".to_owned(),
        entitlement_scope_sha256: "6".repeat(64),
        rights_profile_sha256: "7".repeat(64),
        entitlement: EntitlementState::Unknown,
        numeric_encoding: NumericEncodingV1::DecimalToken,
        effective_resolution: BarResolutionV1::OneHour,
        session_instance_ids: vec![session.to_owned()],
        market_calendar_version: Some("XNYS-2026a".to_owned()),
    };
    let make_response = |row_session: &str| {
        let mut rows = Vec::new();
        for index in 0..7 {
            let bar_start = start + Duration::hours(index);
            let step_end = bar_start + Duration::hours(1);
            let bar_end = std::cmp::min(step_end, end);
            let mut row = logical_bar_row(
                row_session,
                &format(&bar_start),
                &format(&bar_end),
                "10.25",
                "alpaca-sdk-row",
            );
            row.resolution = BarResolutionV1::OneHour;
            row.validation_state = BarsValidationStateV1::Provisional;
            rows.push(row);
        }
        let segments = rows
            .iter()
            .map(|row| BarCoverageSegmentV1 {
                start_utc: row.bar_start_utc.clone(),
                end_exclusive_utc: row.bar_end_exclusive_utc.clone(),
                coverage_status: BarsCoverageStatusV1::Unknown,
                provenance_id: Some(row.provenance_id.clone()),
                session_instance_id: row.session_instance_id.clone(),
            })
            .collect();
        BarsResponseV1 {
            schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
            provider: "alpaca".to_owned(),
            feed: "sip".to_owned(),
            entitlement_status: BarsEntitlementStatusV1::Unknown,
            requested_resolution: BarResolutionV1::OneHour,
            effective_resolution: BarResolutionV1::OneHour,
            adjustment: AdjustmentV1::Unadjusted,
            rows,
            coverage_status: BarsCoverageStatusV1::Unknown,
            segments,
            gaps: Vec::new(),
            source_provenance: vec![BarSourceProvenanceV1 {
                provenance_id: "alpaca-sdk-row".to_owned(),
                provider: "alpaca".to_owned(),
                feed: "sip".to_owned(),
                source: "synthetic pinned-SDK query fixture".to_owned(),
                entitlement_status: BarsEntitlementStatusV1::Unknown,
                numeric_encoding: NumericEncodingV1::DecimalToken,
            }],
            as_of: None,
            fetched_at: ProtoTimestampV2::parse("2026-10-08T20:00:02Z").unwrap(),
            verified_at: None,
            cache_status: BarsCacheStatusV1::Unknown,
            market_calendar_version: None,
            continuation_cursor: None,
            truncated: false,
        }
    };
    let phases = vec![ProviderPhaseSliceV1 {
        phase: "regular".to_owned(),
        session_instance_id: session.to_owned(),
        start_utc: format(&start),
        end_exclusive_utc: format(&end),
    }];
    let raw = make_response("provider-local");
    let mut canonical = make_response(session);
    assert_eq!(raw.rows.len(), 7);
    assert!(raw.validate().is_ok());
    assert!(request.validate_response(&canonical).is_ok());
    assert!(
        super::alpaca_query_access::derive_complete_coverage_from_timeline(
            &request,
            &source_scope,
            &raw,
            &mut canonical,
            Some(&phases),
            true,
        )
        .unwrap()
    );
    assert_eq!(canonical.coverage_status, BarsCoverageStatusV1::Complete);
}

#[test]
fn synthetic_alpaca_page_chain_validates_duplicate_observation_times_and_cursor_links() {
    let mut request = admission_test_request();
    request.end = market_contracts::ProtoTimestampV2::parse("2026-10-08T13:32:00Z").unwrap();
    let scope = CacheQuerySourceScopeV1 {
        entitlement: EntitlementState::Unknown,
        ..admission_test_scope()
    };
    let key = CacheKeyV1::from_request(&request, &scope).unwrap();
    let key_fingerprint = key.fingerprint().unwrap();
    let (start, end) = super::alpaca_query_access::key_request_bounds(&key).unwrap();
    let observed = "2026-10-08T13:31:02Z";
    let receipt = |request_cursor_sha256: Option<String>,
                   next_page_cursor_sha256: Option<String>,
                   response_sha: &str| {
        super::alpaca_query_access::AlpacaQueryAccessReceiptV1 {
            cache_key_fingerprint_sha256: key_fingerprint.clone(),
            cache_request_cursor_sha256: None,
            contract_id: "alpaca-stock-bars-single-symbol-v2/typed-observation-v1".to_owned(),
            method: "GET".to_owned(),
            origin: "https://data.alpaca.markets".to_owned(),
            path_template: "/v2/stocks/{symbol}/bars".to_owned(),
            success_class: "sdk_returned_decoded_response".to_owned(),
            symbol: "QQQ".to_owned(),
            requested_feed: "sip".to_owned(),
            adjustment: "raw".to_owned(),
            currency: "USD".to_owned(),
            asof: "-".to_owned(),
            timeframe: "1Min".to_owned(),
            start_inclusive_utc: start.clone(),
            end_inclusive_utc: end.clone(),
            sort: "asc".to_owned(),
            page_size: 1_000,
            request_cursor_sha256,
            typed_response_sha256: response_sha.to_owned(),
            response_observed_at_utc: observed.to_owned(),
            row_count: 1,
            next_page_cursor_sha256,
            source_observation_sha256: response_sha.to_owned(),
        }
    };
    let receipts = vec![
        receipt(None, Some("b".repeat(64)), &"a".repeat(64)),
        receipt(Some("b".repeat(64)), None, &"c".repeat(64)),
    ];
    let chain_sha =
        super::alpaca_query_access::alpaca_query_access_chain_observation_sha256(&receipts)
            .unwrap();
    let mut coverage = coverage(&key, true, 0, false);
    coverage.covered_end_exclusive_utc = key.window_end_exclusive_utc.clone();
    coverage.response_observed_at_utc = observed.to_owned();
    coverage.pages_seen = Some(2);
    coverage.sequence_gap_count = None;
    coverage.segments[0].coverage_status = market_contracts::BarsCoverageStatusV1::Partial;
    coverage.segments[0].covered_end_exclusive_utc = key.window_end_exclusive_utc.clone();
    coverage.segments[0].source_observation_sha256 = chain_sha;
    coverage.segments[0].raw_row_count = 2;
    coverage.segments[0].canonical_row_count = 2;
    let received_at = market_contracts::ProtoTimestampV2::parse(observed).unwrap();
    let mut first = logical_bar_row(
        "2026-10-08-regular",
        "2026-10-08T13:30:00Z",
        "2026-10-08T13:31:00Z",
        "10.25",
        "source-1",
    );
    first.received_at_utc = received_at.clone();
    let mut second = logical_bar_row(
        "2026-10-08-regular",
        "2026-10-08T13:31:00Z",
        "2026-10-08T13:32:00Z",
        "10.25",
        "source-1",
    );
    second.received_at_utc = received_at;
    let rows = vec![first, second];

    assert!(
        super::alpaca_query_access::stored::validate_stored_query_access_receipts(
            &key,
            &coverage,
            &rows,
            &rows,
            &receipts,
            true,
            &[],
        )
        .is_ok()
    );

    let mut broken_chain = receipts.clone();
    broken_chain[1].request_cursor_sha256 = Some("d".repeat(64));
    assert!(matches!(
        super::alpaca_query_access::stored::validate_stored_query_access_receipts(
            &key,
            &coverage,
            &rows,
            &rows,
            &broken_chain,
            true,
            &[],
        ),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
}
