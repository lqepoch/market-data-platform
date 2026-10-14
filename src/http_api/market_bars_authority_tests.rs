//! Linux-only HTTP integration coverage for the synthetic admission path.
//!
//! Every source fact in this module is a clearly named synthetic fixture. The signing key is a
//! deterministic test verifier, never a provider credential or production trust root.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use market_contracts::{
    AdjustmentV1, AssetClassV1, BarCoverageSegmentV1, BarResolutionV1, BarRowV1,
    BarSourceProvenanceV1, BarsCacheStatusV1, BarsCoverageStatusV1, BarsEntitlementStatusV1,
    BarsResponseV1, BarsValidationStateV1, DecimalString, EntitlementState, NumericEncodingV1,
    ProtoTimestampV2,
};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use super::{
    MAX_CACHE_INVALIDATION_EVENTS, MarketBarsCacheInvalidationEvent,
    MarketBarsPageAdmissionContext, MarketBarsPageAdmissionEvidenceSource, MarketBarsRuntime,
    SyntheticMarketBarsPageSource, hash_cache_rights_profile,
};
use crate::{
    archive::reconciliation::{
        CacheLimitsV1, CacheVersionStoreV1, SameFeedTolerancePolicyV1,
        authority::{
            AdmissionVerifyErrorV1, ProviderAdmissionClaimsV1, ProviderAdmissionEvidenceV1,
            ProviderAdmissionReceiptV1, ReconciliationPolicyV1, TrustedProviderAdmissionVerifierV1,
            signed_payload_for_test,
        },
    },
    http_api::{ServiceState, app_router, auth::test_support, supervisor::QuerySupervisor},
    market_calendar::LoadedMarketCalendar,
    market_query::providers::{
        MarketBarsProviderRuntime, ProviderCacheIdentity, ProviderPageFetch, ProviderQueryError,
    },
    remote_query::{RemoteArchiveReader, RemoteCacheLimits},
    session_router::{
        AlpacaFeed, CalendarDay, CalendarDayState, CalendarSourceMetadata,
        CalendarVerificationStatus, DataInterval, Provider, SessionInstance, SessionPhase,
        SourceCapability, TrustedCalendarSnapshot, UtcWindow,
    },
    storage::LocalTestTransport,
};

const TEST_CACHE_SCOPE: &str = "synthetic-authority-http-v1";
const TEST_SOURCE_NAME: &str = "SyntheticVerified fixture only; not ProviderVerified";

#[derive(Clone, Copy)]
enum SyntheticFacts {
    KnownSipAuthorized,
    UnknownFeedAndEntitlement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvidenceMutation {
    None,
    WrongPrincipal,
    WrongDataset,
}

struct SyntheticPageSource {
    facts: SyntheticFacts,
    fetch_count: AtomicUsize,
}

impl SyntheticPageSource {
    fn new(facts: SyntheticFacts) -> Self {
        Self {
            facts,
            fetch_count: AtomicUsize::new(0),
        }
    }
}

impl SyntheticMarketBarsPageSource for SyntheticPageSource {
    fn source_capabilities(&self) -> Vec<SourceCapability> {
        vec![SourceCapability {
            provider: Provider::Alpaca,
            feed: Some(AlpacaFeed::Sip),
            phase: Some(SessionPhase::Regular),
            interval: DataInterval::Minute(1),
            historical: true,
            realtime: false,
        }]
    }

    fn cache_identity(
        &self,
        selection: &crate::market_query::providers::ProviderPageSelection,
    ) -> ProviderCacheIdentity {
        let mut identity = selection.cache_identity().clone();
        identity.provider = "alpaca".to_owned();
        identity.source_scope_id = "synthetic-page-source-v1".to_owned();
        identity.requested_feed = Some("sip".to_owned());
        identity.numeric_encoding = NumericEncodingV1::DecimalToken;
        match self.facts {
            SyntheticFacts::KnownSipAuthorized => {
                identity.feed = "sip".to_owned();
                identity.entitlement_status = BarsEntitlementStatusV1::Authorized;
            }
            SyntheticFacts::UnknownFeedAndEntitlement => {
                identity.feed = "unknown".to_owned();
                identity.entitlement_status = BarsEntitlementStatusV1::Unknown;
            }
        }
        identity
    }

    fn fetch_page(
        &self,
        plan: &crate::market_query::planning::MarketBarsPlan,
        selection: &crate::market_query::providers::ProviderPageSelection,
    ) -> Result<ProviderPageFetch, ProviderQueryError> {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        let group = selection.provider_group();
        let start = group.fetch_window.start;
        let end = group.fetch_window.end_exclusive;
        let fetched_at = timestamp_now();
        let (feed, entitlement, validation, coverage) = match self.facts {
            SyntheticFacts::KnownSipAuthorized => (
                "sip",
                BarsEntitlementStatusV1::Authorized,
                BarsValidationStateV1::Validated,
                BarsCoverageStatusV1::Complete,
            ),
            SyntheticFacts::UnknownFeedAndEntitlement => (
                "unknown",
                BarsEntitlementStatusV1::Unknown,
                BarsValidationStateV1::Provisional,
                BarsCoverageStatusV1::Partial,
            ),
        };
        let mut rows = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let bar_end = (cursor + Duration::minutes(1)).min(end);
            let bar_start_utc = timestamp(&cursor.to_rfc3339_opts(SecondsFormat::Nanos, true))?;
            let bar_end_exclusive_utc =
                timestamp(&bar_end.to_rfc3339_opts(SecondsFormat::Nanos, true))?;
            rows.push(BarRowV1 {
                symbol: "SPY".to_owned(),
                asset_class: AssetClassV1::Etf,
                resolution: BarResolutionV1::OneMinute,
                adjustment: AdjustmentV1::Unadjusted,
                bar_start_utc,
                bar_end_exclusive_utc,
                received_at_utc: fetched_at.clone(),
                available_at_utc: fetched_at.clone(),
                open: DecimalString::new("10.00")
                    .map_err(|_| ProviderQueryError::InvalidCorePage)?,
                high: DecimalString::new("11.00")
                    .map_err(|_| ProviderQueryError::InvalidCorePage)?,
                low: DecimalString::new("9.00").map_err(|_| ProviderQueryError::InvalidCorePage)?,
                close: DecimalString::new("10.50")
                    .map_err(|_| ProviderQueryError::InvalidCorePage)?,
                volume: DecimalString::new("100")
                    .map_err(|_| ProviderQueryError::InvalidCorePage)?,
                provenance_id: "synthetic-page-v1".to_owned(),
                session_instance_id: "provider-local".to_owned(),
                validation_state: validation,
            });
            cursor = bar_end;
        }
        let start_utc = timestamp(&start.to_rfc3339_opts(SecondsFormat::Nanos, true))?;
        let end_exclusive_utc = timestamp(&end.to_rfc3339_opts(SecondsFormat::Nanos, true))?;
        let response = BarsResponseV1 {
            schema_version: market_contracts::BARS_SCHEMA_VERSION_V1,
            provider: "alpaca".to_owned(),
            feed: feed.to_owned(),
            entitlement_status: entitlement,
            requested_resolution: BarResolutionV1::OneMinute,
            effective_resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            rows,
            coverage_status: coverage,
            segments: vec![BarCoverageSegmentV1 {
                start_utc,
                end_exclusive_utc,
                coverage_status: coverage,
                provenance_id: Some("synthetic-page-v1".to_owned()),
                session_instance_id: "provider-local".to_owned(),
            }],
            gaps: Vec::new(),
            source_provenance: vec![BarSourceProvenanceV1 {
                provenance_id: "synthetic-page-v1".to_owned(),
                provider: "alpaca".to_owned(),
                feed: feed.to_owned(),
                source: TEST_SOURCE_NAME.to_owned(),
                entitlement_status: entitlement,
                numeric_encoding: NumericEncodingV1::DecimalToken,
            }],
            as_of: None,
            fetched_at: fetched_at.clone(),
            verified_at: None,
            cache_status: BarsCacheStatusV1::Miss,
            market_calendar_version: Some(plan.calendar_version.clone()),
            continuation_cursor: None,
            truncated: false,
        };
        ProviderPageFetch::new(
            response,
            Some("a".repeat(64)),
            Some(fetched_at),
            Some(1),
            Some(true),
            None,
        )
    }
}

struct SyntheticAdmissionEvidenceSource {
    allowed_rights_profile_sha256: String,
    mutation: EvidenceMutation,
    calls: AtomicUsize,
}

impl SyntheticAdmissionEvidenceSource {
    fn new(allowed_rights_profile_sha256: String, mutation: EvidenceMutation) -> Self {
        Self {
            allowed_rights_profile_sha256,
            mutation,
            calls: AtomicUsize::new(0),
        }
    }
}

impl MarketBarsPageAdmissionEvidenceSource for SyntheticAdmissionEvidenceSource {
    fn evidence_for_page(
        &self,
        context: MarketBarsPageAdmissionContext<'_>,
    ) -> Option<ProviderAdmissionEvidenceV1> {
        let MarketBarsPageAdmissionContext {
            request,
            source_scope,
            raw_observations,
            canonical_response,
            dataset_binding,
            phase_slices,
            now_unix_seconds,
            ..
        } = context;
        self.calls.fetch_add(1, Ordering::SeqCst);
        if (self.mutation != EvidenceMutation::WrongPrincipal
            && source_scope.rights_profile_sha256 != self.allowed_rights_profile_sha256)
            || source_scope.provider != "alpaca"
            || source_scope.feed != "sip"
            || source_scope.entitlement != EntitlementState::Authorized
            || raw_observations.feed != "sip"
            || canonical_response.feed != "sip"
            || raw_observations.entitlement_status != BarsEntitlementStatusV1::Authorized
            || canonical_response.entitlement_status != BarsEntitlementStatusV1::Authorized
        {
            return None;
        }
        let mut dataset_binding = dataset_binding;
        if self.mutation == EvidenceMutation::WrongDataset {
            dataset_binding.canonical_rows_sha256 = "e".repeat(64);
        }
        let key =
            crate::archive::reconciliation::CacheKeyV1::from_request(request, source_scope).ok()?;
        let mut receipt = ProviderAdmissionReceiptV1 {
            issuer: "synthetic-fixture-issuer".to_owned(),
            key_id: "synthetic-fixture-key".to_owned(),
            issued_at_unix_seconds: now_unix_seconds.saturating_sub(1),
            expires_at_unix_seconds: now_unix_seconds.saturating_add(300),
            claims: ProviderAdmissionClaimsV1 {
                provider: source_scope.provider.clone(),
                feed: source_scope.feed.clone(),
                source_scope_id: source_scope.source_scope_id.clone(),
                entitlement: source_scope.entitlement,
                entitlement_scope_sha256: source_scope.entitlement_scope_sha256.clone(),
                rights_profile_sha256: if self.mutation == EvidenceMutation::WrongPrincipal {
                    self.allowed_rights_profile_sha256.clone()
                } else {
                    source_scope.rights_profile_sha256.clone()
                },
                numeric_encoding: source_scope.numeric_encoding.as_str().to_owned(),
                symbol: request.symbol.clone(),
                asset_class: request.asset_class,
                requested_resolution: request.resolution,
                effective_resolution: source_scope.effective_resolution,
                adjustment: request.adjustment,
                session_scope: request.session_scope,
                session_instance_ids: source_scope.session_instance_ids.clone(),
                window_start_utc: request.start.as_str().to_owned(),
                window_end_exclusive_utc: request.end.as_str().to_owned(),
                market_calendar_version: source_scope.market_calendar_version.clone(),
                request_cursor_sha256: key.request_cursor_sha256.clone(),
                schema_id: key.schema_id,
                schema_fingerprint_sha256: key.schema_fingerprint_sha256,
                authorization_context_sha256: "b".repeat(64),
                sdk_name: "synthetic-page-source".to_owned(),
                sdk_version: "test-only".to_owned(),
                gateway_version: "test-only".to_owned(),
                ordered_phase_slices: phase_slices.to_vec(),
                license_scope_id: "synthetic-fixture-only".to_owned(),
                license_terms_sha256: "c".repeat(64),
                dataset_binding: Some(dataset_binding),
            },
            signature_base64: String::new(),
        };
        let payload = signed_payload_for_test(&receipt).ok()?;
        receipt.signature_base64 = signature_for_payload(&payload);
        Some(ProviderAdmissionEvidenceV1 {
            authorization_context_sha256: Some("b".repeat(64)),
            sdk_name: Some("synthetic-page-source".to_owned()),
            sdk_version: Some("test-only".to_owned()),
            gateway_version: Some("test-only".to_owned()),
            ordered_phase_slices: phase_slices.to_vec(),
            signed_receipt: Some(receipt),
        })
    }
}

struct SyntheticAdmissionVerifier {
    reject_all: bool,
}

struct UnavailableSyntheticAdmissionVerifier;

impl TrustedProviderAdmissionVerifierV1 for UnavailableSyntheticAdmissionVerifier {
    fn verify_signature(
        &self,
        _issuer: &str,
        _key_id: &str,
        _canonical_payload: &[u8],
        _signature_base64: &str,
    ) -> Result<(), AdmissionVerifyErrorV1> {
        Err(AdmissionVerifyErrorV1::Unavailable)
    }
}

impl TrustedProviderAdmissionVerifierV1 for SyntheticAdmissionVerifier {
    fn verify_signature(
        &self,
        issuer: &str,
        key_id: &str,
        canonical_payload: &[u8],
        signature_base64: &str,
    ) -> Result<(), AdmissionVerifyErrorV1> {
        if self.reject_all
            || issuer != "synthetic-fixture-issuer"
            || key_id != "synthetic-fixture-key"
            || signature_base64 != signature_for_payload(canonical_payload)
        {
            Err(AdmissionVerifyErrorV1::Rejected)
        } else {
            Ok(())
        }
    }
}

struct Harness {
    _temp: tempfile::TempDir,
    app: axum::Router,
    supervisor: QuerySupervisor,
    runtime: Arc<MarketBarsRuntime>,
    source: Arc<SyntheticPageSource>,
    evidence_source: Arc<SyntheticAdmissionEvidenceSource>,
}

async fn harness(facts: SyntheticFacts, reject_admission: bool) -> Harness {
    harness_with_options(facts, reject_admission, EvidenceMutation::None, true).await
}

async fn harness_without_authority(facts: SyntheticFacts) -> Harness {
    harness_with_options(facts, false, EvidenceMutation::None, false).await
}

async fn harness_with_evidence_mutation(
    facts: SyntheticFacts,
    mutation: EvidenceMutation,
) -> Harness {
    harness_with_options(facts, false, mutation, true).await
}

async fn harness_with_options(
    facts: SyntheticFacts,
    reject_admission: bool,
    mutation: EvidenceMutation,
    install_authority: bool,
) -> Harness {
    harness_with_cache_limits(
        facts,
        reject_admission,
        mutation,
        install_authority,
        CacheLimitsV1::default(),
    )
    .await
}

async fn harness_with_cache_limits(
    facts: SyntheticFacts,
    reject_admission: bool,
    mutation: EvidenceMutation,
    install_authority: bool,
    cache_limits: CacheLimitsV1,
) -> Harness {
    let temp = tempfile::tempdir().unwrap();
    let archive_root = temp.path().join("archive");
    std::fs::create_dir_all(&archive_root).unwrap();
    let reader = Arc::new(
        RemoteArchiveReader::local_test_isolated(
            LocalTestTransport::new(archive_root).unwrap(),
            temp.path().join("query-cache"),
            RemoteCacheLimits::default(),
        )
        .unwrap(),
    );
    let (supervisor, query_client) = QuerySupervisor::start(Arc::clone(&reader), None);
    let terminal_rights = rights_for_terminal();
    let evidence_source = Arc::new(SyntheticAdmissionEvidenceSource::new(
        terminal_rights,
        mutation,
    ));
    let source = Arc::new(SyntheticPageSource::new(facts));
    let cache = CacheVersionStoreV1::new(temp.path().join("market-cache"), cache_limits).unwrap();
    let provider = MarketBarsProviderRuntime::from_environment().unwrap();
    let verifier: Arc<dyn TrustedProviderAdmissionVerifierV1> = if reject_admission {
        Arc::new(UnavailableSyntheticAdmissionVerifier)
    } else {
        Arc::new(SyntheticAdmissionVerifier { reject_all: false })
    };
    let mut runtime = MarketBarsRuntime::synthetic_for_test(
        provider,
        cache,
        TEST_CACHE_SCOPE.to_owned(),
        source.clone(),
    );
    if install_authority {
        runtime = runtime.with_verified_admission_context(verifier, Vec::new(), test_policy());
    }
    let runtime = Arc::new(runtime.with_page_admission_evidence_source(evidence_source.clone()));
    let state = Arc::new(ServiceState {
        auth_configured: true,
        market_calendar: Some(LoadedMarketCalendar {
            snapshot: Arc::new(one_day_calendar()),
            input_sha256: "d".repeat(64),
        }),
        market_bars: Some(Arc::clone(&runtime)),
        query_client,
        reader,
        pair_query_available: false,
    });
    let app = app_router(state, Some(test_support::auth_config()));
    Harness {
        _temp: temp,
        app,
        supervisor,
        runtime,
        source,
        evidence_source,
    }
}

#[tokio::test]
async fn synthetic_verified_dataset_flows_through_authenticated_http_and_cold_cache_reverify() {
    let mut harness = harness(SyntheticFacts::KnownSipAuthorized, false).await;
    for expected_cache_status in ["miss", "hit"] {
        let response =
            send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["feed"], "sip");
        assert_eq!(body["entitlement_status"], "authorized");
        assert_eq!(body["cache_status"], expected_cache_status);
        assert_eq!(body["rows"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["rows"][0]["session_instance_id"],
            "XNYS:2026-01-05:regular"
        );
        assert_eq!(body["verified_at"], serde_json::Value::Null);
        assert!(
            body["source_provenance"][0]["source"]
                .as_str()
                .unwrap()
                .contains("not ProviderVerified")
        );
    }
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 1);
    assert_eq!(harness.evidence_source.calls.load(Ordering::SeqCst), 1);

    let terminal_rights = rights_for_terminal();
    let events = harness
        .runtime
        .cache_invalidation_events_for_rights_profile(&terminal_rights);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].reread_status, "reread_verified");
    assert_eq!(
        events[0].current_authority.as_deref(),
        Some("authoritative")
    );
    assert!(
        harness
            .runtime
            .cache_invalidation_events_for_rights_profile(&rights_for_research())
            .is_empty()
    );

    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn authoritative_old_session_is_reused_for_a_narrower_fetch_key() {
    let mut harness = harness(SyntheticFacts::KnownSipAuthorized, false).await;
    let first = send_bars_request_for_window(
        &harness.app,
        &test_support::terminal_token("market:read"),
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:32:00Z",
        "alpaca_only",
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(response_json(first).await["cache_status"], "miss");

    let reused = send_bars_request_for_window(
        &harness.app,
        &test_support::terminal_token("market:read"),
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:31:00Z",
        "alpaca_only",
    )
    .await;
    assert_eq!(reused.status(), StatusCode::OK);
    let body = response_json(reused).await;
    assert_eq!(body["cache_status"], "hit");
    assert_eq!(body["provider"], "alpaca");
    assert_eq!(body["feed"], "sip");
    assert_eq!(body["coverage_status"], "complete");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        DateTime::parse_from_rfc3339(body["rows"][0]["bar_start_utc"].as_str().unwrap())
            .unwrap()
            .timestamp(),
        1_767_623_400
    );
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 1);
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn logical_old_cache_does_not_cross_futu_only_mode() {
    let mut harness = harness(SyntheticFacts::KnownSipAuthorized, false).await;
    let first = send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
    assert_eq!(first.status(), StatusCode::OK);
    let _ = response_json(first).await;

    let futu_only = send_bars_request_for_window(
        &harness.app,
        &test_support::terminal_token("market:read"),
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:31:00Z",
        "futu_only",
    )
    .await;
    assert_eq!(futu_only.status(), StatusCode::PARTIAL_CONTENT);
    let body = response_json(futu_only).await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 0);
    assert_ne!(body["cache_status"], "hit");
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 1);
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn logical_old_cache_does_not_cross_verified_principal_rights() {
    let mut harness = harness(SyntheticFacts::KnownSipAuthorized, false).await;
    let first = send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
    assert_eq!(first.status(), StatusCode::OK);
    let _ = response_json(first).await;

    let second = send_bars_request_for_window(
        &harness.app,
        &test_support::research_token("market:read"),
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:31:00Z",
        "alpaca_only",
    )
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let body = response_json(second).await;
    assert_eq!(body["cache_status"], "miss");
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 2);
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_old_page_does_not_seed_logical_cache_reuse() {
    let mut harness = harness(SyntheticFacts::UnknownFeedAndEntitlement, false).await;
    let first = send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
    assert_eq!(first.status(), StatusCode::PARTIAL_CONTENT);
    let _ = response_json(first).await;

    let second = send_bars_request_for_window(
        &harness.app,
        &test_support::terminal_token("market:read"),
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:31:00Z",
        "alpaca_only",
    )
    .await;
    assert_eq!(second.status(), StatusCode::PARTIAL_CONTENT);
    let body = response_json(second).await;
    assert_eq!(body["coverage_status"], "partial");
    assert_eq!(body["cache_status"], "miss");
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 2);
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn expired_logical_authority_is_a_miss_and_refetches() {
    let limits = CacheLimitsV1 {
        ttl: std::time::Duration::from_secs(1),
        ..CacheLimitsV1::default()
    };
    let mut harness = harness_with_cache_limits(
        SyntheticFacts::KnownSipAuthorized,
        false,
        EvidenceMutation::None,
        true,
        limits,
    )
    .await;
    let first = send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
    assert_eq!(first.status(), StatusCode::OK);
    let _ = response_json(first).await;

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let second = send_bars_request_for_window(
        &harness.app,
        &test_support::terminal_token("market:read"),
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:31:00Z",
        "alpaca_only",
    )
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let body = response_json(second).await;
    assert_eq!(body["cache_status"], "miss");
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 2);
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_provider_facts_never_reach_the_attestor_or_claim_authority() {
    let mut harness = harness(SyntheticFacts::UnknownFeedAndEntitlement, false).await;
    let response =
        send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = response_json(response).await;
    assert_eq!(body["feed"], "unknown");
    assert_eq!(body["entitlement_status"], "unknown");
    assert_eq!(body["coverage_status"], "partial");
    assert_eq!(body["verified_at"], serde_json::Value::Null);
    assert_eq!(harness.evidence_source.calls.load(Ordering::SeqCst), 0);

    assert!(
        harness
            .runtime
            .cache_invalidation_events_for_rights_profile(&rights_for_terminal())
            .is_empty(),
        "partial source pages do not advance the complete-session logical index"
    );
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn valid_synthetic_receipt_is_not_enough_when_the_runtime_verifier_is_unavailable() {
    let mut harness = harness(SyntheticFacts::KnownSipAuthorized, true).await;
    let response =
        send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["feed"], "sip");
    assert_eq!(body["verified_at"], serde_json::Value::Null);
    assert_eq!(harness.evidence_source.calls.load(Ordering::SeqCst), 1);
    let events = harness
        .runtime
        .cache_invalidation_events_for_rights_profile(&rights_for_terminal());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].current_authority.as_deref(), Some("provisional"));
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn default_runtime_without_injected_authority_can_only_publish_provisional_pages() {
    let mut harness = harness_without_authority(SyntheticFacts::KnownSipAuthorized).await;
    let response =
        send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["feed"], "sip");
    assert_eq!(body["verified_at"], serde_json::Value::Null);
    assert_eq!(harness.evidence_source.calls.load(Ordering::SeqCst), 0);
    let events = harness
        .runtime
        .cache_invalidation_events_for_rights_profile(&rights_for_terminal());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].current_authority.as_deref(), Some("provisional"));
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn signed_receipt_for_another_principal_is_rejected() {
    let mut harness = harness_with_evidence_mutation(
        SyntheticFacts::KnownSipAuthorized,
        EvidenceMutation::WrongPrincipal,
    )
    .await;
    let response =
        send_bars_request(&harness.app, &test_support::research_token("market:read")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["feed"], "sip");
    assert_eq!(body["verified_at"], serde_json::Value::Null);
    assert_eq!(harness.evidence_source.calls.load(Ordering::SeqCst), 1);
    assert!(
        harness
            .runtime
            .cache_invalidation_events_for_rights_profile(&rights_for_terminal())
            .is_empty()
    );
    let events = harness
        .runtime
        .cache_invalidation_events_for_rights_profile(&rights_for_research());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].current_authority.as_deref(), Some("provisional"));
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn correctly_signed_receipt_for_a_different_dataset_is_rejected() {
    let mut harness = harness_with_evidence_mutation(
        SyntheticFacts::KnownSipAuthorized,
        EvidenceMutation::WrongDataset,
    )
    .await;
    let response =
        send_bars_request(&harness.app, &test_support::terminal_token("market:read")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["feed"], "sip");
    assert_eq!(body["verified_at"], serde_json::Value::Null);
    assert_eq!(harness.evidence_source.calls.load(Ordering::SeqCst), 1);
    let events = harness
        .runtime
        .cache_invalidation_events_for_rights_profile(&rights_for_terminal());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].current_authority.as_deref(), Some("provisional"));
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn verified_principal_rights_partition_prevents_cross_principal_cache_reuse() {
    let mut harness = harness(SyntheticFacts::KnownSipAuthorized, false).await;
    for token in [
        test_support::terminal_token("market:read"),
        test_support::research_token("market:read"),
    ] {
        let response = send_bars_request(&harness.app, &token).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(harness.source.fetch_count.load(Ordering::SeqCst), 2);
    assert_eq!(harness.evidence_source.calls.load(Ordering::SeqCst), 2);

    let terminal_events = harness
        .runtime
        .cache_invalidation_events_for_rights_profile(&rights_for_terminal());
    let research_events = harness
        .runtime
        .cache_invalidation_events_for_rights_profile(&rights_for_research());
    assert_eq!(terminal_events.len(), 1);
    assert_eq!(
        terminal_events[0].current_authority.as_deref(),
        Some("authoritative")
    );
    assert_eq!(research_events.len(), 1);
    assert_eq!(
        research_events[0].current_authority.as_deref(),
        Some("provisional")
    );
    assert_ne!(
        terminal_events[0].identity_fingerprint_sha256,
        research_events[0].identity_fingerprint_sha256
    );
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_status_returns_principal_scoped_bounded_and_sanitized_events_over_http() {
    let mut harness = harness(SyntheticFacts::KnownSipAuthorized, false).await;
    let terminal_token = test_support::terminal_token("market:read");
    let bar_response = send_bars_request(&harness.app, &terminal_token).await;
    assert_eq!(bar_response.status(), StatusCode::OK);
    let _ = response_json(bar_response).await;

    let terminal_response = send_cache_status_request(&harness.app, &terminal_token).await;
    assert_eq!(terminal_response.status(), StatusCode::OK);
    assert_no_store(&terminal_response);
    let terminal_body = response_json(terminal_response).await;
    assert_eq!(terminal_body["schema_version"], "market-cache-status-v1");
    assert_eq!(terminal_body["cache_status"], "not_inspected");
    assert_eq!(terminal_body["invalidation_events_status"], "available");
    assert_eq!(
        terminal_body["canonical_market_cache_configured"].as_bool(),
        Some(true)
    );
    assert!(terminal_body["runtime"].is_object());
    assert_eq!(terminal_body["volatile"].as_bool(), Some(true));
    assert_eq!(terminal_body["restart_lost"].as_bool(), Some(true));
    assert_eq!(terminal_body["coverage_asserted"].as_bool(), Some(false));
    assert_eq!(
        terminal_body["invalidation_events"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        terminal_body["invalidation_events"][0]["reread_status"],
        "reread_verified"
    );

    // Exercise the ring bound through the same HTTP projection; all fixture events remain in
    // the terminal partition and contain no real provider identity or credentials.
    let terminal_rights = rights_for_terminal();
    for generation in 0..(MAX_CACHE_INVALIDATION_EVENTS as u64 + 1) {
        harness
            .runtime
            .record_cache_invalidation_event(MarketBarsCacheInvalidationEvent {
                rights_profile_sha256: terminal_rights.clone(),
                identity_fingerprint_sha256: Some("a".repeat(64)),
                session_instance_id: Some("synthetic-session".to_owned()),
                logical_generation: Some(generation),
                logical_updated_at_unix_seconds: Some(1_800_000_000 + generation),
                reread_status: "reread_verified",
                current_provider: Some("synthetic".to_owned()),
                current_feed: Some("synthetic".to_owned()),
                current_authority: Some("provisional".to_owned()),
            });
    }
    let bounded_response = send_cache_status_request(&harness.app, &terminal_token).await;
    assert_eq!(bounded_response.status(), StatusCode::OK);
    let bounded_body = response_json(bounded_response).await;
    let events = bounded_body["invalidation_events"].as_array().unwrap();
    assert_eq!(events.len(), MAX_CACHE_INVALIDATION_EVENTS);
    assert_eq!(events[0]["logical_generation"], "1");
    assert_eq!(events[events.len() - 1]["logical_generation"], "128");
    let serialized = bounded_body.to_string();
    for private_field in [
        "rights_profile_sha256",
        "identity_fingerprint_sha256",
        "profile",
        "cache_scope_id",
        "entitlement_scope_sha256",
        "credential",
        "token",
        "rows",
        "sha256",
    ] {
        assert!(
            !serialized.contains(private_field),
            "private field {private_field} appeared in cache-event HTTP response"
        );
    }

    let research_response =
        send_cache_status_request(&harness.app, &test_support::research_token("market:read")).await;
    assert_eq!(research_response.status(), StatusCode::OK);
    let research_body = response_json(research_response).await;
    assert!(
        research_body["invalidation_events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    harness.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_status_requires_market_read_and_returns_empty_events_without_runtime() {
    let mut harness = harness(SyntheticFacts::KnownSipAuthorized, false).await;
    let unauthorized =
        send_cache_status_request(&harness.app, &test_support::terminal_token("market:write"))
            .await;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_no_store(&unauthorized);
    harness.supervisor.shutdown().await.unwrap();

    let (_temp, app, mut supervisor) = app_without_market_bars().await;
    let unavailable =
        send_cache_status_request(&app, &test_support::terminal_token("market:read")).await;
    assert_eq!(unavailable.status(), StatusCode::OK);
    assert_no_store(&unavailable);
    let body = response_json(unavailable).await;
    assert_eq!(body["invalidation_events_status"], "unavailable");
    assert_eq!(
        body["canonical_market_cache_configured"].as_bool(),
        Some(false)
    );
    assert_eq!(body["volatile"].as_bool(), Some(true));
    assert_eq!(body["restart_lost"].as_bool(), Some(true));
    assert_eq!(body["coverage_asserted"].as_bool(), Some(false));
    assert!(body["invalidation_events"].as_array().unwrap().is_empty());
    supervisor.shutdown().await.unwrap();
}

async fn app_without_market_bars() -> (tempfile::TempDir, axum::Router, QuerySupervisor) {
    let temp = tempfile::tempdir().unwrap();
    let archive_root = temp.path().join("archive");
    std::fs::create_dir_all(&archive_root).unwrap();
    let reader = Arc::new(
        RemoteArchiveReader::local_test_isolated(
            LocalTestTransport::new(archive_root).unwrap(),
            temp.path().join("query-cache"),
            RemoteCacheLimits::default(),
        )
        .unwrap(),
    );
    let (supervisor, query_client) = QuerySupervisor::start(Arc::clone(&reader), None);
    let state = Arc::new(ServiceState {
        auth_configured: true,
        market_calendar: None,
        market_bars: None,
        query_client,
        reader,
        pair_query_available: false,
    });
    let app = app_router(state, Some(test_support::auth_config()));
    (temp, app, supervisor)
}

async fn send_cache_status_request(app: &axum::Router, token: &str) -> axum::response::Response {
    let request = Request::builder()
        .method("GET")
        .uri("/v1/market/cache/status")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap()
}

fn assert_no_store(response: &axum::response::Response) {
    assert!(
        response
            .headers()
            .get_all(axum::http::header::CACHE_CONTROL)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|directive| directive.trim().eq_ignore_ascii_case("no-store")),
        "cache event responses must be marked no-store"
    );
}

async fn send_bars_request(app: &axum::Router, token: &str) -> axum::response::Response {
    send_bars_request_for_window(
        app,
        token,
        "2026-01-05T14:30:00Z",
        "2026-01-05T14:32:00Z",
        "alpaca_only",
    )
    .await
}

async fn send_bars_request_for_window(
    app: &axum::Router,
    token: &str,
    start: &str,
    end: &str,
    provider_mode: &str,
) -> axum::response::Response {
    let uri = format!(
        "/v1/market/bars?symbol=SPY&asset_class=etf&start={start}&end={end}&resolution=1m&session_scope=regular&adjustment=unadjusted&provider_mode={provider_mode}&allow_daily_downgrade=false&limit=10"
    );
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap()
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn one_day_calendar() -> TrustedCalendarSnapshot {
    let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let day_start = trade_date.and_hms_opt(5, 0, 0).unwrap().and_utc();
    let day_end = day_start + Duration::days(1);
    let regular_start = DateTime::parse_from_rfc3339("2026-01-05T14:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let regular_end = DateTime::parse_from_rfc3339("2026-01-05T21:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let session_id = "XNYS:2026-01-05".to_owned();
    TrustedCalendarSnapshot {
        source_id: "XNYS".to_owned(),
        version: "nyse.v1".to_owned(),
        source_metadata: CalendarSourceMetadata {
            source_name: "NYSE Holidays & Trading Hours".to_owned(),
            source_reference: "https://www.nyse.com/trade/hours-calendars".to_owned(),
            generator_package: "pandas_market_calendars".to_owned(),
            generator_version: "5.4.0".to_owned(),
            valid_from: NaiveDate::from_ymd_opt(1990, 1, 1).unwrap(),
            valid_to: NaiveDate::from_ymd_opt(2031, 12, 31).unwrap(),
            official_verified_from: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            official_verified_through: NaiveDate::from_ymd_opt(2028, 12, 31).unwrap(),
            verification_status: "verified_through_official_fixture".to_owned(),
            artifact_checksum: "a".repeat(64),
            pinned_input_sha256: "b".repeat(64),
            generated_at: "2026-09-04T00:00:00Z".to_owned(),
            source_git_commit: "c".repeat(40),
            compiler_config_sha256: "d".repeat(64),
            next_review_at: NaiveDate::from_ymd_opt(2026, 12, 15).unwrap(),
        },
        coverage_start: trade_date,
        coverage_end_exclusive: trade_date.succ_opt().unwrap(),
        days: vec![CalendarDay {
            trade_date,
            day_window: UtcWindow {
                start: day_start,
                end_exclusive: day_end,
            },
            state: CalendarDayState::Open {
                session_id: session_id.clone(),
            },
            verification_status: CalendarVerificationStatus::Verified,
        }],
        instances: vec![
            SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: "XNYS:2026-01-05:pre".to_owned(),
                trade_date,
                phase: SessionPhase::Pre,
                window: UtcWindow {
                    start: day_start + Duration::hours(4),
                    end_exclusive: regular_start,
                },
            },
            SessionInstance {
                session_id: session_id.clone(),
                session_instance_id: "XNYS:2026-01-05:regular".to_owned(),
                trade_date,
                phase: SessionPhase::Regular,
                window: UtcWindow {
                    start: regular_start,
                    end_exclusive: regular_end,
                },
            },
            SessionInstance {
                session_id,
                session_instance_id: "XNYS:2026-01-05:after".to_owned(),
                trade_date,
                phase: SessionPhase::After,
                window: UtcWindow {
                    start: regular_end,
                    end_exclusive: day_start + Duration::hours(20),
                },
            },
        ],
    }
}

fn timestamp(value: &str) -> Result<ProtoTimestampV2, ProviderQueryError> {
    ProtoTimestampV2::parse(value).map_err(|_| ProviderQueryError::InvalidCorePage)
}

fn timestamp_now() -> ProtoTimestampV2 {
    ProtoTimestampV2::parse(&Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true))
        .expect("UTC test clock is a valid Core timestamp")
}

fn test_policy() -> ReconciliationPolicyV1 {
    ReconciliationPolicyV1::legacy_same_feed(SameFeedTolerancePolicyV1::default())
}

fn signature_for_payload(payload: &[u8]) -> String {
    format!(
        "synthetic-signature:{}",
        hex::encode(Sha256::digest(payload))
    )
}

fn rights_for_terminal() -> String {
    let principal = test_support::terminal_principal();
    hash_cache_rights_profile(TEST_CACHE_SCOPE, principal.principal_scope_sha256())
}

fn rights_for_research() -> String {
    let principal = test_support::research_principal();
    hash_cache_rights_profile(TEST_CACHE_SCOPE, principal.principal_scope_sha256())
}
