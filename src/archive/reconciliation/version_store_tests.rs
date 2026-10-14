use super::*;

fn cache_result_variant_for_test<T>(
    result: &std::result::Result<T, CacheReconciliationError>,
) -> String {
    let variant = match result {
        Ok(_) => return "Ok".to_owned(),
        Err(CacheReconciliationError::InvalidInput) => "InvalidInput",
        Err(CacheReconciliationError::Incomplete) => "Incomplete",
        Err(CacheReconciliationError::Conflict) => "Conflict",
        Err(CacheReconciliationError::UnknownOutcome) => "UnknownOutcome",
        Err(CacheReconciliationError::LockHeld) => "LockHeld",
        Err(CacheReconciliationError::LimitExceeded) => "LimitExceeded",
        Err(CacheReconciliationError::Io(_)) => "Io",
        Err(CacheReconciliationError::Json(_)) => "Json",
        Err(CacheReconciliationError::Readback) => "Readback",
        Err(CacheReconciliationError::Cancelled) => "Cancelled",
        Err(CacheReconciliationError::Unsupported) => "Unsupported",
        Err(CacheReconciliationError::NotFound) => "NotFound",
        Err(CacheReconciliationError::Expired) => "Expired",
        Err(CacheReconciliationError::ForeignNonEmptyRoot) => "ForeignNonEmptyRoot",
        Err(CacheReconciliationError::BlockedExternal) => "BlockedExternal",
        Err(CacheReconciliationError::AdmissionMismatch) => "AdmissionMismatch",
        Err(CacheReconciliationError::AdmissionExpired) => "AdmissionExpired",
    };
    format!("Err({variant})")
}

#[cfg(target_os = "linux")]
#[test]
fn synthetic_verified_version_store_preserves_raw_rows_and_rolls_back_after_worker_readback() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
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
    // This fixture sets every OHLC field to the supplied close value, so each replacement
    // changes all four decimal fields by the same delta.
    let tolerance = SameFeedTolerancePolicyV1 {
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
            cache_input_with_test_phases(
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
                test_response("600.26"),
                test_response("600.26"),
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
    assert_eq!(versions.len(), 4);
    assert!(versions.iter().all(|version| version.is_fresh));
    assert!(versions.iter().any(|version| version.conflict_rejected));
    let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();
    let assert_pointer_state = |store: &CacheVersionStoreV1,
                                stage: &str,
                                expected_source: (&str, &str, CacheAuthorityV1),
                                expected_logical: (&str, &str, CacheAuthorityV1),
                                now: u64| {
        let source = store.load_current(&key, now).unwrap();
        let logical = store
            .load_logical_current(&identity, "2026-10-08-regular", now)
            .unwrap();
        assert!(
            source.current.version_id == expected_source.0,
            "{stage}: source version mismatch at now={now}"
        );
        assert!(
            logical.session.current.version_id == expected_logical.0,
            "{stage}: logical version mismatch at now={now}"
        );
        assert!(
            source.bars_rows[0].close.as_str() == expected_source.1,
            "{stage}: source close mismatch at now={now}"
        );
        assert!(
            logical.snapshot.bars_rows[0].close.as_str() == expected_logical.1,
            "{stage}: logical close mismatch at now={now}"
        );
        assert_eq!(
            source.current.authority, expected_source.2,
            "{stage}: source authority"
        );
        assert_eq!(
            logical.snapshot.current.authority, expected_logical.2,
            "{stage}: logical authority"
        );
        if expected_source.1 == expected_logical.1 {
            assert!(
                source.raw_bars_rows_sha256 == logical.snapshot.raw_bars_rows_sha256,
                "{stage}: raw observation lineage differs between equivalent source and logical current"
            );
            assert!(
                source.bars_rows_sha256 == logical.snapshot.bars_rows_sha256,
                "{stage}: canonical rows differ between equivalent source and logical current"
            );
        } else {
            assert!(
                source.bars_rows_sha256 != logical.snapshot.bars_rows_sha256,
                "{stage}: different source and logical rows unexpectedly share a canonical hash"
            );
        }
        assert!(
            source.created_at_unix_seconds <= now && now < source.expires_at_unix_seconds,
            "{stage}: source version is outside its TTL at now={now}"
        );
        assert!(
            logical.snapshot.created_at_unix_seconds <= now
                && now < logical.snapshot.expires_at_unix_seconds,
            "{stage}: logical version is outside its TTL at now={now}"
        );
    };
    let publish_verified = |store: &CacheVersionStoreV1, close: &str, now| {
        let response = test_response(close);
        store.publish_synthetic_verified_bars_responses(
            cache_input_with_test_phases(
                &request,
                &source_scope,
                response.clone(),
                response,
                test_page_evidence(),
            ),
            now,
            &tolerance,
        )
    };

    assert_pointer_state(
        &store,
        "before-same-price-refresh",
        (&second_id, "600.26", CacheAuthorityV1::Authoritative),
        (&second_id, "600.26", CacheAuthorityV1::Authoritative),
        1_800_000_027,
    );

    // A same-price source refresh advances the immutable source and logical versions while
    // preserving identical verified rows, lineage, authority, and TTL.
    let same_price_26 = publish_verified(&store, "600.26", 1_800_000_028).unwrap();
    assert!(same_price_26.current_changed);
    assert_eq!(same_price_26.authority, CacheAuthorityV1::Authoritative);
    assert!(same_price_26.logical_invalidation.is_some());
    let settled_26 = store
        .load_current(&key, 1_800_000_028)
        .unwrap()
        .current
        .version_id
        .clone();
    assert_ne!(settled_26, second_id);
    assert_pointer_state(
        &store,
        "same-price-source-refresh",
        (&settled_26, "600.26", CacheAuthorityV1::Authoritative),
        (&settled_26, "600.26", CacheAuthorityV1::Authoritative),
        1_800_000_028,
    );

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::BeforeCommitpoint);
    let before_commit_result = publish_verified(&store, "600.27", 1_800_000_030);
    let before_commit_result_debug = cache_result_variant_for_test(&before_commit_result);
    assert!(
        matches!(
            before_commit_result,
            Err(CacheReconciliationError::UnknownOutcome)
        ),
        "BeforeCommitpoint returned {before_commit_result_debug}"
    );
    assert_pointer_state(
        &store,
        "before-commitpoint-live",
        (&settled_26, "600.26", CacheAuthorityV1::Authoritative),
        (&settled_26, "600.26", CacheAuthorityV1::Authoritative),
        1_800_000_031,
    );
    let before_commit_reopen =
        CacheVersionStoreV1::new(temp.path().join("cache"), CacheLimitsV1::default()).unwrap();
    assert_pointer_state(
        &before_commit_reopen,
        "before-commitpoint-reopen",
        (&settled_26, "600.26", CacheAuthorityV1::Authoritative),
        (&settled_26, "600.26", CacheAuthorityV1::Authoritative),
        1_800_000_031,
    );

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::AfterCommitpoint);
    let after_commit_result = publish_verified(&store, "600.27", 1_800_000_032);
    let after_commit_result_debug = cache_result_variant_for_test(&after_commit_result);
    assert!(
        matches!(
            after_commit_result,
            Err(CacheReconciliationError::UnknownOutcome)
        ),
        "AfterCommitpoint returned {after_commit_result_debug}"
    );
    let committed_27_read = store.load_current(&key, 1_800_000_033).unwrap();
    let committed_27 = committed_27_read.current.version_id.clone();
    assert_ne!(committed_27, second_id);
    assert_eq!(
        committed_27_read
            .current
            .previous_current_version_id
            .as_deref(),
        Some(settled_26.as_str())
    );
    assert_pointer_state(
        &store,
        "after-commitpoint-live",
        (&committed_27, "600.27", CacheAuthorityV1::Authoritative),
        (&committed_27, "600.27", CacheAuthorityV1::Authoritative),
        1_800_000_033,
    );
    let after_commit_reopen =
        CacheVersionStoreV1::new(temp.path().join("cache"), CacheLimitsV1::default()).unwrap();
    after_commit_reopen
        .settle_pointer_transaction_for_test(&key)
        .unwrap();
    assert_pointer_state(
        &after_commit_reopen,
        "after-commitpoint-reopen-and-settle",
        (&committed_27, "600.27", CacheAuthorityV1::Authoritative),
        (&committed_27, "600.27", CacheAuthorityV1::Authoritative),
        1_800_000_033,
    );

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::AfterSourceMirror);
    let after_source_mirror_result = publish_verified(&store, "600.28", 1_800_000_035);
    let after_source_mirror_result_debug =
        cache_result_variant_for_test(&after_source_mirror_result);
    assert!(
        matches!(
            after_source_mirror_result,
            Err(CacheReconciliationError::UnknownOutcome)
        ),
        "AfterSourceMirror returned {after_source_mirror_result_debug}"
    );
    let committed_28_read = store.load_current(&key, 1_800_000_036).unwrap();
    let committed_28 = committed_28_read.current.version_id.clone();
    assert_ne!(committed_28, committed_27);
    assert_eq!(
        committed_28_read
            .current
            .previous_current_version_id
            .as_deref(),
        Some(committed_27.as_str())
    );
    assert_pointer_state(
        &store,
        "after-source-mirror-live",
        (&committed_28, "600.28", CacheAuthorityV1::Authoritative),
        (&committed_28, "600.28", CacheAuthorityV1::Authoritative),
        1_800_000_036,
    );
    let after_source_mirror_reopen =
        CacheVersionStoreV1::new(temp.path().join("cache"), CacheLimitsV1::default()).unwrap();
    after_source_mirror_reopen
        .settle_pointer_transaction_for_test(&key)
        .unwrap();
    assert_pointer_state(
        &after_source_mirror_reopen,
        "after-source-mirror-reopen-and-settle",
        (&committed_28, "600.28", CacheAuthorityV1::Authoritative),
        (&committed_28, "600.28", CacheAuthorityV1::Authoritative),
        1_800_000_036,
    );

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::AfterLogicalMirror);
    let after_logical_mirror_result = publish_verified(&store, "600.29", 1_800_000_038);
    let after_logical_mirror_result_debug =
        cache_result_variant_for_test(&after_logical_mirror_result);
    assert!(
        matches!(
            after_logical_mirror_result,
            Err(CacheReconciliationError::UnknownOutcome)
        ),
        "AfterLogicalMirror returned {after_logical_mirror_result_debug}"
    );
    let committed_29_read = store.load_current(&key, 1_800_000_039).unwrap();
    let committed_29 = committed_29_read.current.version_id.clone();
    assert_ne!(committed_29, committed_28);
    assert_eq!(
        committed_29_read
            .current
            .previous_current_version_id
            .as_deref(),
        Some(committed_28.as_str())
    );
    assert_pointer_state(
        &store,
        "after-logical-mirror-live",
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        1_800_000_039,
    );
    let after_logical_mirror_reopen =
        CacheVersionStoreV1::new(temp.path().join("cache"), CacheLimitsV1::default()).unwrap();
    assert_pointer_state(
        &after_logical_mirror_reopen,
        "after-logical-mirror-reopen",
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        1_800_000_039,
    );
    after_logical_mirror_reopen
        .settle_pointer_transaction_for_test(&key)
        .unwrap();
    assert_pointer_state(
        &after_logical_mirror_reopen,
        "after-logical-mirror-reopen-and-settle",
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        1_800_000_039,
    );

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::AfterCommitpoint);
    let rollback_fault_result = store.rollback_to(&key, &first_id, 1_800_000_041);
    let rollback_fault_result_debug = cache_result_variant_for_test(&rollback_fault_result);
    assert!(
        matches!(
            rollback_fault_result,
            Err(CacheReconciliationError::UnknownOutcome)
        ),
        "rollback AfterCommitpoint returned {rollback_fault_result_debug}"
    );
    let restored = store.load_current(&key, 1_800_000_042).unwrap();
    assert_eq!(restored.current.version_id, first_id);
    assert_eq!(
        restored.current.operation,
        CacheCurrentOperationV1::Rollback
    );
    assert_eq!(
        restored.current.previous_current_version_id.as_deref(),
        Some(committed_29.as_str())
    );
    assert_eq!(
        restored.current.generation,
        committed_29_read.current.generation + 1
    );
    store.settle_pointer_transaction_for_test(&key).unwrap();
    assert_pointer_state(
        &store,
        "after-rollback-settle",
        (&first_id, "600.25", CacheAuthorityV1::Provisional),
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        1_800_000_042,
    );
    std::fs::remove_file(
        temp.path()
            .join("cache")
            .join("keys")
            .join(key.fingerprint().unwrap())
            .join("current.json"),
    )
    .unwrap();

    // Repair the rollback head before injecting the recovery transaction's mirror failure.
    let initial_recovery = store.recover_to(&key, &first_id, 1_800_000_043).unwrap();
    let initial_recovery_current = initial_recovery.current.unwrap();
    assert_eq!(
        initial_recovery_current.operation,
        CacheCurrentOperationV1::Recovery
    );
    assert_pointer_state(
        &store,
        "after-recovery-live",
        (&first_id, "600.25", CacheAuthorityV1::Provisional),
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        1_800_000_043,
    );

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::AfterLogicalMirror);
    let recovery_fault_result = store.recover_to(&key, &first_id, 1_800_000_044);
    let recovery_fault_result_debug = cache_result_variant_for_test(&recovery_fault_result);
    assert!(
        matches!(
            recovery_fault_result,
            Err(CacheReconciliationError::UnknownOutcome)
        ),
        "recovery AfterLogicalMirror returned {recovery_fault_result_debug}"
    );
    let recovered = store.load_current(&key, 1_800_000_045).unwrap();
    assert_eq!(
        recovered.current.operation,
        CacheCurrentOperationV1::Recovery
    );
    assert_eq!(recovered.current.version_id, first_id);
    assert_eq!(
        recovered.current.previous_current_version_id.as_deref(),
        Some(first_id.as_str())
    );
    assert_eq!(
        recovered.current.generation,
        initial_recovery_current.generation + 1
    );
    assert_pointer_state(
        &store,
        "after-recovery-fault-live",
        (&first_id, "600.25", CacheAuthorityV1::Provisional),
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        1_800_000_045,
    );
    let logical_dir = temp
        .path()
        .join("cache")
        .join("logical")
        .join(identity.fingerprint().unwrap());
    let transaction_path = logical_dir.join("transaction.json");
    let transaction_bytes = fs::read(&transaction_path).unwrap();
    fs::write(&transaction_path, b"corrupt transaction head").unwrap();
    assert!(matches!(
        store.load_current(&key, 1_800_000_046),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    assert!(matches!(
        store.load_logical_current(&identity, "2026-10-08-regular", 1_800_000_046),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    assert!(matches!(
        store.recover_to(&key, &first_id, 1_800_000_047),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    fs::write(&transaction_path, transaction_bytes).unwrap();
    let logical_marker_path = logical_dir.join(".transaction-mode");
    let logical_marker_bytes = fs::read(&logical_marker_path).unwrap();
    fs::remove_file(&logical_marker_path).unwrap();
    assert!(matches!(
        store.load_current(&key, 1_800_000_048),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    fs::write(&logical_marker_path, logical_marker_bytes).unwrap();
    assert_pointer_state(
        &store,
        "after-transaction-marker-restored",
        (&first_id, "600.25", CacheAuthorityV1::Provisional),
        (&committed_29, "600.29", CacheAuthorityV1::Authoritative),
        1_800_000_048,
    );
    assert!(matches!(
        store.load_current(&key, 1_800_000_900),
        Err(CacheReconciliationError::Expired)
    ));
    let current = store.load_current(&key, 1_800_000_049).unwrap();
    std::fs::write(&current.raw_parquet_path, b"tampered parquet object").unwrap();
    assert!(matches!(
        store.load_current(&key, 1_800_000_050),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
}
