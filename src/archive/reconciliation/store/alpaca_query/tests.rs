use super::*;

#[test]
fn alpaca_query_access_lease_is_exact_version_short_lived_and_process_local() {
    let directory = tempfile::tempdir().expect("private cache root");
    let now = chrono::Utc::now().timestamp() as u64;
    let observed_at = chrono::DateTime::from_timestamp(now as i64, 0)
        .expect("valid clock")
        .to_rfc3339();
    let key = "a".repeat(64);
    let version = "b".repeat(64);
    let store = CacheVersionStoreV1::new(directory.path(), super::super::CacheLimitsV1::default())
        .expect("cache store");

    assert!(matches!(
        store.require_alpaca_query_access_lease(&key, &version, now),
        Err(CacheReconciliationError::Expired)
    ));
    store
        .grant_alpaca_query_access_lease(key.clone(), version.clone(), now, &observed_at)
        .expect("grant same-process query lease");
    assert!(
        store
            .require_alpaca_query_access_lease(&key, &version, now)
            .is_ok()
    );
    assert!(matches!(
        store.require_alpaca_query_access_lease(&key, &version, now + 60),
        Err(CacheReconciliationError::Expired)
    ));
    assert!(matches!(
        store.require_alpaca_query_access_lease(&key, &"c".repeat(64), now),
        Err(CacheReconciliationError::Expired)
    ));
    store
        .grant_alpaca_query_access_lease(
            key.clone(),
            "c".repeat(64),
            now + 10,
            &chrono::DateTime::from_timestamp((now + 10) as i64, 0)
                .expect("valid clock")
                .to_rfc3339(),
        )
        .expect("grant second version lease");
    assert!(
        store
            .require_alpaca_query_access_lease(&key, &version, now + 10)
            .is_ok()
    );

    let restarted =
        CacheVersionStoreV1::new(directory.path(), super::super::CacheLimitsV1::default())
            .expect("restart cache store");
    assert!(matches!(
        restarted.require_alpaca_query_access_lease(&key, &version, now),
        Err(CacheReconciliationError::Expired)
    ));
}
