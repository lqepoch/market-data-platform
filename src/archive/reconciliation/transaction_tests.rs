use super::*;

#[cfg(target_os = "linux")]
#[test]
fn synthetic_first_transaction_before_base_is_retryable_after_reopen() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("cache");
    let store = CacheVersionStoreV1::new(cache_root.clone(), CacheLimitsV1::default()).unwrap();
    let request = admission_test_request();
    let source_scope = admission_test_scope();
    let key = CacheKeyV1::from_request(&request, &source_scope).unwrap();
    let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::BeforeCommitpoint);
    assert!(matches!(
        store.publish_bars_responses(
            cache_input(
                &request,
                &source_scope,
                admission_test_response("600.25"),
                admission_test_response("600.25"),
                admission_test_evidence(),
            ),
            1_800_000_080,
            &SameFeedTolerancePolicyV1::default(),
        ),
        Err(CacheReconciliationError::UnknownOutcome)
    ));

    let logical_dir = cache_root
        .join("logical")
        .join(identity.fingerprint().unwrap());
    assert!(!logical_dir.join(".transaction-mode").exists());
    assert!(!logical_dir.join("transaction.json").exists());
    let reopened = CacheVersionStoreV1::new(cache_root.clone(), CacheLimitsV1::default()).unwrap();
    assert!(matches!(
        reopened.load_current(&key, 1_800_000_081),
        Err(CacheReconciliationError::NotFound)
    ));
    assert!(matches!(
        reopened.load_logical_current(&identity, "2026-10-08-regular", 1_800_000_081),
        Err(CacheReconciliationError::NotFound)
    ));

    let published = reopened
        .publish_bars_responses(
            cache_input(
                &request,
                &source_scope,
                admission_test_response("600.25"),
                admission_test_response("600.25"),
                admission_test_evidence(),
            ),
            1_800_000_082,
            &SameFeedTolerancePolicyV1::default(),
        )
        .unwrap();
    assert_eq!(
        reopened
            .load_current(&key, 1_800_000_083)
            .unwrap()
            .current
            .version_id,
        published.published_version_id
    );
}

#[cfg(target_os = "linux")]
#[test]
fn synthetic_first_transaction_after_marker_reads_base_and_is_retryable() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("cache");
    let store = CacheVersionStoreV1::new(cache_root.clone(), CacheLimitsV1::default()).unwrap();
    let request = admission_test_request();
    let source_scope = admission_test_scope();
    let response = admission_test_response("600.25");
    let key = CacheKeyV1::from_request(&request, &source_scope).unwrap();
    let identity = CacheLogicalIdentityV1::from_key(&key).unwrap();

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::AfterIdentityMarker);
    assert!(matches!(
        store.publish_bars_responses(
            cache_input(
                &request,
                &source_scope,
                response.clone(),
                response,
                admission_test_evidence(),
            ),
            1_800_000_090,
            &SameFeedTolerancePolicyV1::default(),
        ),
        Err(CacheReconciliationError::UnknownOutcome)
    ));

    let logical_dir = cache_root
        .join("logical")
        .join(identity.fingerprint().unwrap());
    assert!(logical_dir.join(".transaction-mode").exists());
    let transaction_path = logical_dir.join("transaction.json");
    assert!(transaction_path.exists());
    let transaction_bytes = fs::read(&transaction_path).unwrap();
    let marker_path = logical_dir.join(".transaction-mode");
    let marker_bytes = fs::read(&marker_path).unwrap();
    let reopened = CacheVersionStoreV1::new(cache_root, CacheLimitsV1::default()).unwrap();
    assert!(matches!(
        reopened.load_current(&key, 1_800_000_091),
        Err(CacheReconciliationError::NotFound)
    ));
    assert!(matches!(
        reopened.load_logical_current(&identity, "2026-10-08-regular", 1_800_000_091),
        Err(CacheReconciliationError::NotFound)
    ));

    // A verified BASE is an old-state snapshot without the identity marker. Removing the marker
    // before NEW must not turn a normal first-write interruption into an unknown result.
    fs::remove_file(&marker_path).unwrap();
    assert!(matches!(
        reopened.load_current(&key, 1_800_000_091),
        Err(CacheReconciliationError::NotFound)
    ));
    assert!(matches!(
        reopened.load_logical_current(&identity, "2026-10-08-regular", 1_800_000_091),
        Err(CacheReconciliationError::NotFound)
    ));
    fs::write(&marker_path, &marker_bytes).unwrap();

    fs::write(&marker_path, b"corrupt marker").unwrap();
    assert!(matches!(
        reopened.load_current(&key, 1_800_000_092),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    fs::write(&marker_path, &marker_bytes).unwrap();
    fs::remove_file(&transaction_path).unwrap();
    assert!(matches!(
        reopened.load_current(&key, 1_800_000_093),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    fs::write(&transaction_path, &transaction_bytes).unwrap();

    let published = reopened
        .publish_bars_responses(
            cache_input(
                &request,
                &source_scope,
                admission_test_response("600.25"),
                admission_test_response("600.25"),
                admission_test_evidence(),
            ),
            1_800_000_094,
            &SameFeedTolerancePolicyV1::default(),
        )
        .unwrap();
    assert_eq!(
        reopened
            .load_current(&key, 1_800_000_095)
            .unwrap()
            .current
            .version_id,
        published.published_version_id
    );
}

#[cfg(target_os = "linux")]
#[test]
fn synthetic_initial_base_preserves_other_source_logical_and_legacy_currents() {
    let _worker_guard = crate::parquet_worker::serialize_worker_test();
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("cache");
    let store = CacheVersionStoreV1::new(cache_root.clone(), CacheLimitsV1::default()).unwrap();
    let request_a = admission_test_request();
    let scope_a = admission_test_scope();
    let response_a = admission_test_response("600.25");
    let key_a = CacheKeyV1::from_request(&request_a, &scope_a).unwrap();
    let identity = CacheLogicalIdentityV1::from_key(&key_a).unwrap();
    let published_a = store
        .publish_synthetic_verified_bars_responses(
            cache_input_with_test_phases(
                &request_a,
                &scope_a,
                response_a.clone(),
                response_a,
                admission_test_evidence(),
            ),
            1_800_000_070,
            &SameFeedTolerancePolicyV1::default(),
        )
        .unwrap();
    let logical_before = store
        .load_logical_current(&identity, "2026-10-08-regular", 1_800_000_071)
        .unwrap()
        .session
        .current
        .version_id
        .clone();
    assert_eq!(logical_before, published_a.published_version_id);

    // Model the prior on-disk format: source A and its logical root exist, but no transaction
    // marker or head has been installed yet.
    let logical_dir = cache_root
        .join("logical")
        .join(identity.fingerprint().unwrap());
    let key_a_dir = cache_root.join("keys").join(key_a.fingerprint().unwrap());
    fs::remove_file(logical_dir.join("transaction.json")).unwrap();
    fs::remove_file(logical_dir.join(".transaction-mode")).unwrap();
    fs::remove_file(logical_dir.join("transaction-mirror.json")).unwrap();
    fs::remove_file(key_a_dir.join(".transaction-mode")).unwrap();
    fs::remove_file(key_a_dir.join("transaction-mirror.json")).unwrap();
    assert_eq!(
        store
            .load_current(&key_a, 1_800_000_072)
            .unwrap()
            .current
            .version_id,
        published_a.published_version_id
    );

    let mut request_b = request_a.clone();
    request_b.provider_mode = BarsProviderModeV1::Auto;
    let mut scope_b = scope_a.clone();
    scope_b.provider = "futu".to_owned();
    scope_b.feed = "futu".to_owned();
    scope_b.source_scope_id = "futu-unknown-history-v1".to_owned();
    scope_b.entitlement_scope_sha256 = "8".repeat(64);
    scope_b.entitlement = EntitlementState::Unknown;
    let key_b = CacheKeyV1::from_request(&request_b, &scope_b).unwrap();
    assert_eq!(CacheLogicalIdentityV1::from_key(&key_b).unwrap(), identity);

    let mut response_b = admission_test_response("600.25");
    response_b.provider = "futu".to_owned();
    response_b.feed = "futu".to_owned();
    response_b.entitlement_status = market_contracts::BarsEntitlementStatusV1::Unknown;
    response_b.source_provenance[0].provider = "futu".to_owned();
    response_b.source_provenance[0].feed = "futu".to_owned();
    response_b.source_provenance[0].source = "Futu SDK observation".to_owned();
    response_b.source_provenance[0].entitlement_status =
        market_contracts::BarsEntitlementStatusV1::Unknown;

    store.set_transaction_fault_for_test(CacheTransactionFaultV1::AfterBaseCommitpoint);
    assert!(matches!(
        store.publish_bars_responses(
            cache_input_with_test_phases(
                &request_b,
                &scope_b,
                response_b.clone(),
                response_b.clone(),
                admission_test_evidence(),
            ),
            1_800_000_073,
            &SameFeedTolerancePolicyV1::default(),
        ),
        Err(CacheReconciliationError::UnknownOutcome)
    ));

    let reopened = CacheVersionStoreV1::new(cache_root, CacheLimitsV1::default()).unwrap();
    assert!(!logical_dir.join(".transaction-mode").exists());
    let transaction_path = logical_dir.join("transaction.json");
    let base_bytes = fs::read(&transaction_path).unwrap();
    assert!(matches!(
        reopened.load_current(&key_b, 1_800_000_074),
        Err(CacheReconciliationError::NotFound)
    ));
    assert_eq!(
        reopened
            .load_logical_current(&identity, "2026-10-08-regular", 1_800_000_074)
            .unwrap()
            .session
            .current
            .version_id,
        published_a.published_version_id
    );
    assert_eq!(
        reopened
            .load_current(&key_a, 1_800_000_074)
            .unwrap()
            .current
            .version_id,
        published_a.published_version_id
    );
    fs::write(&transaction_path, b"corrupt BASE head").unwrap();
    assert!(matches!(
        reopened.load_current(&key_b, 1_800_000_074),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    assert!(matches!(
        reopened.load_logical_current(&identity, "2026-10-08-regular", 1_800_000_074),
        Err(CacheReconciliationError::UnknownOutcome)
    ));
    fs::write(&transaction_path, &base_bytes).unwrap();

    reopened
        .publish_bars_responses(
            cache_input_with_test_phases(
                &request_b,
                &scope_b,
                response_b.clone(),
                response_b,
                admission_test_evidence(),
            ),
            1_800_000_075,
            &SameFeedTolerancePolicyV1::default(),
        )
        .unwrap();
    assert_eq!(
        reopened
            .load_logical_current(&identity, "2026-10-08-regular", 1_800_000_076)
            .unwrap()
            .session
            .current
            .version_id,
        published_a.published_version_id
    );
    assert_eq!(
        reopened
            .load_current(&key_a, 1_800_000_076)
            .unwrap()
            .current
            .version_id,
        published_a.published_version_id
    );
}
