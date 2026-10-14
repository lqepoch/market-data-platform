use std::collections::BTreeMap;

use market_contracts::EntitlementState;

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
                CacheVersionStoreV1::new(root, CacheLimitsV1::default()).is_ok()
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert!(results.into_iter().all(|success| success));
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
#[test]
fn synthetic_verified_version_store_preserves_raw_rows_and_rolls_back_after_worker_readback() {
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarCoverageSegmentV1, BarResolutionV1, BarRowV1,
        BarSourceProvenanceV1, BarsCacheStatusV1, BarsCoverageStatusV1, BarsEntitlementStatusV1,
        BarsProviderModeV1, BarsRequestV1, BarsResponseV1, BarsValidationStateV1, DecimalString,
        NumericEncodingV1, ProtoTimestampV2, SessionScopeV1,
    };

    fn ts(value: &str) -> ProtoTimestampV2 {
        ProtoTimestampV2::parse(value).unwrap()
    }

    fn test_request() -> BarsRequestV1 {
        BarsRequestV1 {
            symbol: "QQQ".to_owned(),
            asset_class: AssetClassV1::Etf,
            start: ts("2026-10-08T13:30:00Z"),
            end: ts("2026-10-08T13:31:00Z"),
            resolution: BarResolutionV1::OneMinute,
            session_scope: SessionScopeV1::Regular,
            adjustment: AdjustmentV1::Unadjusted,
            provider_mode: BarsProviderModeV1::AlpacaOnly,
            allow_daily_downgrade: false,
            limit: 1000,
            cursor: None,
        }
    }

    fn test_source_scope() -> CacheQuerySourceScopeV1 {
        CacheQuerySourceScopeV1 {
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
        }
    }

    fn test_response(close: &str) -> BarsResponseV1 {
        BarsResponseV1 {
            schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
            provider: "alpaca".to_owned(),
            feed: "sip".to_owned(),
            entitlement_status: BarsEntitlementStatusV1::Authorized,
            requested_resolution: BarResolutionV1::OneMinute,
            effective_resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            rows: vec![BarRowV1 {
                symbol: "QQQ".to_owned(),
                asset_class: AssetClassV1::Etf,
                resolution: BarResolutionV1::OneMinute,
                adjustment: AdjustmentV1::Unadjusted,
                bar_start_utc: ts("2026-10-08T13:30:00Z"),
                bar_end_exclusive_utc: ts("2026-10-08T13:31:00Z"),
                received_at_utc: ts("2026-10-08T13:31:00Z"),
                available_at_utc: ts("2026-10-08T13:31:00.000000001Z"),
                open: DecimalString::new(close).unwrap(),
                high: DecimalString::new(close).unwrap(),
                low: DecimalString::new(close).unwrap(),
                close: DecimalString::new(close).unwrap(),
                volume: DecimalString::new("2").unwrap(),
                provenance_id: "source-1".to_owned(),
                session_instance_id: "2026-10-08-regular".to_owned(),
                validation_state: BarsValidationStateV1::Validated,
            }],
            coverage_status: BarsCoverageStatusV1::Complete,
            segments: vec![BarCoverageSegmentV1 {
                start_utc: ts("2026-10-08T13:30:00Z"),
                end_exclusive_utc: ts("2026-10-08T13:31:00Z"),
                coverage_status: BarsCoverageStatusV1::Complete,
                provenance_id: Some("source-1".to_owned()),
                session_instance_id: "2026-10-08-regular".to_owned(),
            }],
            gaps: Vec::new(),
            source_provenance: vec![BarSourceProvenanceV1 {
                provenance_id: "source-1".to_owned(),
                provider: "alpaca".to_owned(),
                feed: "sip".to_owned(),
                source: "Alpaca SIP".to_owned(),
                entitlement_status: BarsEntitlementStatusV1::Authorized,
                numeric_encoding: NumericEncodingV1::DecimalToken,
            }],
            as_of: None,
            fetched_at: ts("2026-10-08T13:31:01Z"),
            verified_at: None,
            cache_status: BarsCacheStatusV1::Miss,
            market_calendar_version: Some("XNYS-2026a".to_owned()),
            continuation_cursor: None,
            truncated: false,
        }
    }

    fn test_page_evidence() -> ProviderPageEvidenceV1 {
        ProviderPageEvidenceV1 {
            source_observation_sha256: None,
            response_observed_at_utc: None,
            pages_seen: Some(1),
            pages_exhausted: Some(true),
            first_source_sequence: None,
            last_source_sequence: None,
            sequence_gap_count: None,
            truncated: false,
            provider_cursor_cache_input: None,
        }
    }

    let request = test_request();
    let source_scope = test_source_scope();

    let mut malformed_bar = test_response("600.25");
    malformed_bar.rows[0].high = DecimalString::new("599.99").unwrap();
    assert!(
        CacheSnapshotV1::from_bars_response(
            &request,
            &source_scope,
            malformed_bar,
            test_page_evidence(),
        )
        .is_err()
    );
    let mut unresolved_provenance = test_response("600.25");
    unresolved_provenance.rows[0].provenance_id = "missing-source".to_owned();
    assert!(
        CacheSnapshotV1::from_bars_response(
            &request,
            &source_scope,
            unresolved_provenance,
            test_page_evidence(),
        )
        .is_err()
    );
    let valid_snapshot = CacheSnapshotV1::from_bars_response(
        &request,
        &source_scope,
        test_response("600.25"),
        test_page_evidence(),
    )
    .unwrap();
    let mut raw_provider_page = test_response("600.2500");
    raw_provider_page.rows[0].session_instance_id = "provider-local".to_owned();
    raw_provider_page.segments[0].session_instance_id = "provider-local".to_owned();
    let mut filtered_conflict = raw_provider_page.rows[0].clone();
    filtered_conflict.open = DecimalString::new("600.31").unwrap();
    filtered_conflict.high = DecimalString::new("600.31").unwrap();
    filtered_conflict.low = DecimalString::new("600.31").unwrap();
    filtered_conflict.close = DecimalString::new("600.31").unwrap();
    raw_provider_page.rows.push(filtered_conflict);
    let mapped_page = test_response("600.25");
    let mapped_snapshot = CacheSnapshotV1::from_bars_responses(
        &request,
        &source_scope,
        raw_provider_page,
        mapped_page,
        test_page_evidence(),
    )
    .unwrap();
    assert_eq!(mapped_snapshot.raw_response.rows.len(), 2);
    assert_eq!(mapped_snapshot.response.rows.len(), 1);
    assert_eq!(mapped_snapshot.coverage.segments[0].raw_row_count, 2);
    assert_eq!(mapped_snapshot.coverage.segments[0].canonical_row_count, 1);

    let mut wrong_asset_class = valid_snapshot.response.clone();
    wrong_asset_class.rows[0].asset_class = AssetClassV1::Stock;
    let wrong_asset_evidence =
        BarsCacheEvidenceV1::from_response(&request, &wrong_asset_class).unwrap();
    assert!(matches!(
        wrong_asset_evidence.validate_against(
            &valid_snapshot.key,
            &valid_snapshot.coverage,
            &wrong_asset_class.rows,
        ),
        Err(CacheReconciliationError::Conflict)
    ));
    let incomplete_snapshot = CacheSnapshotV1::from_bars_response(
        &request,
        &source_scope,
        test_response("600.25"),
        ProviderPageEvidenceV1::default(),
    )
    .unwrap();
    assert_eq!(
        incomplete_snapshot.response.coverage_status,
        BarsCoverageStatusV1::Partial
    );
    assert!(
        !incomplete_snapshot
            .coverage
            .is_complete_for_key(&incomplete_snapshot.key)
    );
    let mut continuation_response = test_response("600.25");
    continuation_response.continuation_cursor = Some("opaque-page-token".to_owned());
    let continuation_evidence =
        BarsCacheEvidenceV1::from_response(&request, &continuation_response).unwrap();
    assert!(
        !serde_json::to_string(&continuation_evidence)
            .unwrap()
            .contains("opaque-page-token")
    );
    assert!(
        continuation_evidence
            .to_cached_response(
                continuation_response.rows.clone(),
                BarsCacheStatusV1::Hit,
                None,
            )
            .is_err()
    );

    let temp = tempfile::tempdir().unwrap();
    let request = test_request();
    let source_scope = test_source_scope();
    let store =
        CacheVersionStoreV1::new(temp.path().join("cache"), CacheLimitsV1::default()).unwrap();
    let raw_first_response = mapped_snapshot.raw_response.clone();
    let canonical_first_response = mapped_snapshot.response.clone();
    let key = CacheKeyV1::from_request(&request, &source_scope).unwrap();
    let tolerance = SameFeedTolerancePolicyV1 {
        max_abs_delta_by_field: BTreeMap::from([("close".to_owned(), "0.01".to_owned())]),
        max_changed_rows: 1,
        max_added_rows: 0,
        allow_removed_rows: false,
    };
    let cancellation = crate::cancellation::CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        store.publish_bars_responses_cancellable(
            cache_input(
                &request,
                &source_scope,
                test_response("600.25"),
                test_response("600.25"),
                test_page_evidence(),
            ),
            1_800_000_000,
            &tolerance,
            &cancellation,
        ),
        Err(CacheReconciliationError::Cancelled)
    ));
    let first = store
        .publish_bars_responses(
            cache_input(
                &request,
                &source_scope,
                raw_first_response,
                canonical_first_response,
                test_page_evidence(),
            ),
            1_800_000_000,
            &tolerance,
        )
        .unwrap();
    assert_eq!(first.authority, CacheAuthorityV1::Provisional);
    let first_id = first.published_version_id;
    let read = store.load_current(&key, 1_800_000_001).unwrap();
    assert_eq!(read.raw_bars_rows.len(), 2);
    assert_eq!(read.raw_bars_rows[0].session_instance_id, "provider-local");
    assert_eq!(read.raw_bars_rows[0].close.as_str(), "600.2500");
    assert_eq!(read.bars_rows[0].close.as_str(), "600.25");
    assert_eq!(read.current.version_id, first_id);
    assert_eq!(
        read.bars_evidence
            .to_cached_response(read.bars_rows.clone(), BarsCacheStatusV1::Hit, None)
            .unwrap()
            .cache_status,
        BarsCacheStatusV1::Hit
    );

    let mut page_request = request.clone();
    page_request.cursor = Some("provider-page-selection-digest".to_owned());
    let mut raw_page = test_response("600.25");
    raw_page.coverage_status = BarsCoverageStatusV1::Partial;
    raw_page.segments[0].coverage_status = BarsCoverageStatusV1::Partial;
    raw_page.continuation_cursor = Some("outer-mdp-page-cursor".to_owned());
    raw_page.truncated = true;
    let mut canonical_page = raw_page.clone();
    let provider_locator = "synthetic-provider-page-locator";
    let page_evidence = ProviderPageEvidenceV1 {
        source_observation_sha256: None,
        response_observed_at_utc: None,
        pages_seen: Some(1),
        pages_exhausted: Some(false),
        first_source_sequence: None,
        last_source_sequence: None,
        sequence_gap_count: None,
        truncated: true,
        provider_cursor_cache_input: Some(
            ProviderCursorCacheInputV1::new(
                ProviderCursorKindV1::Alpaca,
                serde_json::to_string(provider_locator).unwrap(),
                1_800_000_200,
            )
            .unwrap(),
        ),
    };
    canonical_page.cache_status = BarsCacheStatusV1::Miss;
    let page_publish = store
        .publish_bars_responses(
            cache_input(
                &page_request,
                &source_scope,
                raw_page,
                canonical_page,
                page_evidence,
            ),
            1_800_000_100,
            &tolerance,
        )
        .unwrap();
    assert_eq!(page_publish.authority, CacheAuthorityV1::Provisional);
    let page_key = CacheKeyV1::from_request(&page_request, &source_scope).unwrap();
    let page_read = store.load_current(&page_key, 1_800_000_101).unwrap();
    let cursor_receipt = page_read.provider_cursor_receipt.as_ref().unwrap();
    assert_eq!(cursor_receipt.expires_at_unix_seconds(), 1_800_000_200);
    cursor_receipt.validate_for_cache_key(&page_key).unwrap();
    assert!(!format!("{cursor_receipt:?}").contains(provider_locator));
    let cached_page = page_read
        .bars_evidence
        .to_cached_response(
            page_read.bars_rows.clone(),
            BarsCacheStatusV1::Hit,
            Some("rewrapped-current-selection-cursor".to_owned()),
        )
        .unwrap();
    assert!(cached_page.truncated);
    assert_eq!(
        cached_page.continuation_cursor.as_deref(),
        Some("rewrapped-current-selection-cursor")
    );
    assert!(matches!(
        store.load_current(&page_key, 1_800_000_200),
        Err(CacheReconciliationError::Expired)
    ));

    let mut no_cursor_request = request.clone();
    no_cursor_request.cursor = Some("provider-page-without-replay-cursor".to_owned());
    let mut no_cursor_page = test_response("600.25");
    no_cursor_page.coverage_status = BarsCoverageStatusV1::Partial;
    no_cursor_page.segments[0].coverage_status = BarsCoverageStatusV1::Partial;
    no_cursor_page.continuation_cursor = Some("opaque-provider-page-token".to_owned());
    no_cursor_page.truncated = true;
    let no_cursor_evidence = ProviderPageEvidenceV1 {
        pages_seen: Some(1),
        pages_exhausted: Some(false),
        truncated: true,
        ..ProviderPageEvidenceV1::default()
    };
    store
        .publish_bars_responses(
            cache_input(
                &no_cursor_request,
                &source_scope,
                no_cursor_page.clone(),
                no_cursor_page,
                no_cursor_evidence,
            ),
            1_800_000_100,
            &tolerance,
        )
        .unwrap();
    let no_cursor_key = CacheKeyV1::from_request(&no_cursor_request, &source_scope).unwrap();
    assert!(matches!(
        store.load_current(&no_cursor_key, 1_800_000_101),
        Err(CacheReconciliationError::Expired)
    ));

    let second = store
        .publish_synthetic_verified_bars_responses(
            cache_input(
                &request,
                &source_scope,
                test_response("600.26"),
                test_response("600.26"),
                test_page_evidence(),
            ),
            1_800_000_010,
            &tolerance,
        )
        .unwrap();
    assert_eq!(second.authority, CacheAuthorityV1::Authoritative);
    assert!(second.current_changed);
    let second_id = second.published_version_id.clone();
    let provisional_refresh = store
        .publish_bars_responses(
            cache_input(
                &request,
                &source_scope,
                test_response("600.27"),
                test_response("600.27"),
                test_page_evidence(),
            ),
            1_800_000_012,
            &tolerance,
        )
        .unwrap();
    assert_eq!(provisional_refresh.authority, CacheAuthorityV1::Provisional);
    assert!(!provisional_refresh.current_changed);
    let conflict_response = test_response("600.30");
    assert!(matches!(
        store.publish_bars_responses(
            cache_input(
                &request,
                &source_scope,
                conflict_response.clone(),
                conflict_response,
                test_page_evidence(),
            ),
            1_800_000_015,
            &tolerance,
        ),
        Err(CacheReconciliationError::Conflict)
    ));
    let still_current = store.load_current(&key, 1_800_000_016).unwrap();
    assert_eq!(still_current.current.version_id, second_id);
    assert_eq!(still_current.bars_rows[0].close.as_str(), "600.26");
    let versions = store.list_verified_versions(&key, 1_800_000_020).unwrap();
    assert_eq!(versions.len(), 3);
    assert!(versions.iter().all(|version| version.is_fresh));
    let rollback = store.rollback_to(&key, &first_id, 1_800_000_020).unwrap();
    assert_eq!(
        rollback.current.unwrap().operation,
        CacheCurrentOperationV1::Rollback
    );
    let restored = store.load_current(&key, 1_800_000_021).unwrap();
    assert_eq!(restored.current.version_id, first_id);
    std::fs::remove_file(
        temp.path()
            .join("cache")
            .join("keys")
            .join(key.fingerprint().unwrap())
            .join("current.json"),
    )
    .unwrap();
    let recovered = store.recover_to(&key, &first_id, 1_800_000_022).unwrap();
    assert_eq!(
        recovered.current.unwrap().operation,
        CacheCurrentOperationV1::Recovery
    );
    assert!(matches!(
        store.load_current(&key, 1_800_000_900),
        Err(CacheReconciliationError::Expired)
    ));
    let current = store.load_current(&key, 1_800_000_023).unwrap();
    std::fs::write(&current.raw_parquet_path, b"tampered parquet object").unwrap();
    assert!(matches!(
        store.load_current(&key, 1_800_000_024),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
}
