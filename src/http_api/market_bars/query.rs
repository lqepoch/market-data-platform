//! Bounded-supervisor query orchestration and provisional cache adapter.

use std::sync::Arc;

use chrono::SecondsFormat;
use market_contracts::{
    BarsCacheStatusV1, BarsEntitlementStatusV1, BarsRequestV1, BarsResponseV1, EntitlementState,
    ProtoTimestampV2,
};

use super::super::{QueryFailure, auth::VerifiedMarketPrincipal};
use super::response::{
    planning_error_to_market_data, provider_error_is_http_fatal,
    provider_query_error_to_market_data, response_for_cache_page, response_for_provider_failure,
    response_from_page, response_without_provider,
};
use super::{
    MarketBarsRuntime, PROVIDER_DEADLINE, current_timestamp, hash_cache_entitlement_scope,
    unix_seconds_now,
};

struct PreparedMarketBars {
    plan: crate::market_query::planning::MarketBarsPlan,
    selection: Option<crate::market_query::providers::ProviderPageSelection>,
    cache_request: Option<BarsRequestV1>,
    cache_scope: Option<crate::archive::reconciliation::CacheQuerySourceScopeV1>,
    cache_hit: Option<BarsResponseV1>,
    cache_status: BarsCacheStatusV1,
}

enum ProviderFetchOutcome {
    NoProviderGroup,
    CacheHit(BarsResponseV1),
    Provider(crate::market_query::providers::ProviderPageFetch),
}

pub(super) async fn run_market_bars_query(
    query_client: super::QueryClient,
    runtime: Arc<MarketBarsRuntime>,
    request: BarsRequestV1,
    calendar: crate::market_calendar::LoadedMarketCalendar,
    principal: VerifiedMarketPrincipal,
    gateway_delegation: Option<String>,
) -> std::result::Result<BarsResponseV1, QueryFailure> {
    let prepare_runtime = Arc::clone(&runtime);
    let prepare_calendar = Arc::clone(&calendar.snapshot);
    let prepare_request = request.clone();
    let principal_scope_sha256 = principal.principal_scope_sha256().to_owned();
    let cache_profile = runtime.cache_scope_id.clone();
    let prepare = move |cancellation: crate::cancellation::CancellationToken| {
        if cancellation.is_cancelled() {
            return Err(crate::MarketDataError::Storage(
                crate::error::StorageFailure::Cancelled,
            ));
        }
        let now = current_timestamp().map_err(|_| crate::MarketDataError::InvalidInput)?;
        let as_of = prepare_runtime
            .provider
            .planning_as_of(&prepare_request, &now)
            .map_err(provider_query_error_to_market_data)?;
        let capabilities = prepare_runtime.provider.source_capabilities();
        let plan = crate::market_query::planning::plan_market_bars(
            &prepare_request,
            &as_of,
            &capabilities,
            &prepare_calendar,
        )
        .map_err(planning_error_to_market_data)?;
        if cancellation.is_cancelled() {
            return Err(crate::MarketDataError::Storage(
                crate::error::StorageFailure::Cancelled,
            ));
        }

        let selection = if plan.provider_groups.is_empty() {
            None
        } else {
            Some(
                prepare_runtime
                    .provider
                    .select_page(&prepare_request, &plan)
                    .map_err(provider_query_error_to_market_data)?,
            )
        };
        let (cache_request, cache_scope, cache_hit, cache_status) = match selection.as_ref() {
            Some(selection) => cache_preflight(
                &prepare_runtime,
                &prepare_request,
                &plan,
                selection,
                &principal_scope_sha256,
                cache_profile.as_deref(),
                &cancellation,
            ),
            None => (
                None,
                None,
                None,
                if prepare_runtime.cache.is_some() {
                    BarsCacheStatusV1::Bypass
                } else {
                    BarsCacheStatusV1::Unavailable
                },
            ),
        };
        Ok(PreparedMarketBars {
            plan,
            selection,
            cache_request,
            cache_scope,
            cache_hit,
            cache_status,
        })
    };

    let provider_runtime = Arc::clone(&runtime);
    let delegation = gateway_delegation;
    let provider = move |prepared: Arc<PreparedMarketBars>,
                         cancellation: crate::cancellation::CancellationToken| async move {
        if cancellation.is_cancelled() {
            return Err(crate::market_query::providers::ProviderQueryError::Timeout);
        }
        if let Some(response) = prepared.cache_hit.clone() {
            return Ok(ProviderFetchOutcome::CacheHit(response));
        }
        let Some(selection) = prepared.selection.as_ref() else {
            return Ok(ProviderFetchOutcome::NoProviderGroup);
        };
        let context = crate::market_query::providers::ProviderRequestContext::new(
            tokio::time::Instant::now() + PROVIDER_DEADLINE,
            delegation,
        );
        provider_runtime
            .provider
            .fetch_provider_page_with_evidence(&prepared.plan, selection, context)
            .await
            .map(ProviderFetchOutcome::Provider)
    };

    let finalize_runtime = Arc::clone(&runtime);
    let finalize_request = request.clone();
    let planner = move |prepared: Arc<PreparedMarketBars>,
                        fetched: std::result::Result<
        ProviderFetchOutcome,
        crate::market_query::providers::ProviderQueryError,
    >,
                        cancellation: crate::cancellation::CancellationToken| {
        if cancellation.is_cancelled() {
            return Err(crate::MarketDataError::Storage(
                crate::error::StorageFailure::Cancelled,
            ));
        }
        let response = match fetched {
            Ok(ProviderFetchOutcome::NoProviderGroup) => {
                response_without_provider(&finalize_request, &prepared.plan, prepared.cache_status)?
            }
            Ok(ProviderFetchOutcome::CacheHit(page)) => response_from_page(
                &finalize_request,
                &prepared.plan,
                prepared.selection.as_ref(),
                page,
                prepared.cache_status,
            )?,
            Ok(ProviderFetchOutcome::Provider(page)) => {
                let selection =
                    prepared
                        .selection
                        .as_ref()
                        .ok_or(crate::MarketDataError::Provider(
                            crate::error::ProviderFailure::InvalidResponse,
                        ))?;
                let raw_page = page.response.clone();
                let cache_page = response_for_cache_page(
                    &finalize_request,
                    &prepared.plan,
                    selection,
                    raw_page.clone(),
                    prepared.cache_status,
                )?;
                let response = response_from_page(
                    &finalize_request,
                    &prepared.plan,
                    Some(selection),
                    page.response,
                    prepared.cache_status,
                )?;
                publish_cache_observation(
                    &finalize_runtime,
                    &prepared,
                    raw_page,
                    cache_page,
                    page.evidence,
                    &cancellation,
                );
                response
            }
            Err(error) if provider_error_is_http_fatal(error) => {
                return Err(provider_query_error_to_market_data(error));
            }
            Err(error) => response_for_provider_failure(
                &finalize_request,
                &prepared.plan,
                prepared.selection.as_ref(),
                error,
                prepared.cache_status,
            )?,
        };
        finalize_request.validate_response(&response).map_err(|_| {
            crate::MarketDataError::Provider(crate::error::ProviderFailure::InvalidResponse)
        })?;
        Ok(response)
    };

    query_client.market_bars(prepare, provider, planner).await
}

fn cache_preflight(
    runtime: &MarketBarsRuntime,
    request: &BarsRequestV1,
    plan: &crate::market_query::planning::MarketBarsPlan,
    selection: &crate::market_query::providers::ProviderPageSelection,
    principal_scope_sha256: &str,
    cache_profile: Option<&str>,
    cancellation: &crate::cancellation::CancellationToken,
) -> (
    Option<BarsRequestV1>,
    Option<crate::archive::reconciliation::CacheQuerySourceScopeV1>,
    Option<BarsResponseV1>,
    BarsCacheStatusV1,
) {
    let Some(cache) = runtime.cache.as_ref() else {
        return (None, None, None, BarsCacheStatusV1::Unavailable);
    };
    let Some(profile) = cache_profile else {
        return (None, None, None, BarsCacheStatusV1::Bypass);
    };
    if selection.cache_policy()
        != crate::market_query::providers::ProviderCachePolicy::ProvisionalOnly
        || cancellation.is_cancelled()
    {
        return (None, None, None, BarsCacheStatusV1::Bypass);
    }
    let identity = selection.cache_identity();
    if identity.session_instance_ids.is_empty() {
        return (None, None, None, BarsCacheStatusV1::Bypass);
    }
    let Some(cache_request) =
        cache_request_for_group(request, selection.provider_group(), selection.cache_key())
    else {
        return (None, None, None, BarsCacheStatusV1::Unavailable);
    };
    let calendar_version = identity
        .market_calendar_version
        .as_deref()
        .unwrap_or(&plan.calendar_version);
    let source_scope = crate::archive::reconciliation::CacheQuerySourceScopeV1 {
        provider: identity.provider.clone(),
        feed: identity.feed.clone(),
        source_scope_id: identity.source_scope_id.clone(),
        entitlement_scope_sha256: hash_cache_entitlement_scope(
            profile,
            principal_scope_sha256,
            &identity.provider,
            &identity.feed,
            &identity.source_scope_id,
            calendar_version,
        ),
        entitlement: entitlement_state(identity.entitlement_status),
        numeric_encoding: identity.numeric_encoding,
        effective_resolution: identity.effective_resolution,
        session_instance_ids: identity.session_instance_ids.clone(),
        market_calendar_version: Some(plan.calendar_version.clone()),
    };
    let key = match crate::archive::reconciliation::CacheKeyV1::from_request(
        &cache_request,
        &source_scope,
    ) {
        Ok(key) => key,
        Err(_) => {
            return (
                Some(cache_request),
                Some(source_scope),
                None,
                BarsCacheStatusV1::Unavailable,
            );
        }
    };
    let now_unix_seconds = unix_seconds_now();
    match cache.load_current_cancellable(&key, now_unix_seconds, cancellation) {
        Ok(read) => {
            let continuation = match (
                read.bars_evidence.truncated,
                read.provider_cursor_receipt.as_ref(),
            ) {
                (false, None) => Some(None),
                (true, Some(receipt)) => selection
                    .cursor_after_cached_provider_page(receipt, &key, now_unix_seconds)
                    .ok()
                    .map(Some),
                // A truncated cache page cannot be replayed without its separately validated
                // provider cursor receipt; an unexpected receipt for an exhausted page is also
                // treated as a cache miss.
                _ => None,
            };
            let hit = continuation
                .and_then(|continuation| {
                    read.bars_evidence
                        .to_cached_response(read.bars_rows, BarsCacheStatusV1::Hit, continuation)
                        .ok()
                })
                .filter(|response| cache_request.validate_response(response).is_ok());
            if let Some(response) = hit {
                (
                    Some(cache_request),
                    Some(source_scope),
                    Some(response),
                    BarsCacheStatusV1::Hit,
                )
            } else {
                (
                    Some(cache_request),
                    Some(source_scope),
                    None,
                    BarsCacheStatusV1::Miss,
                )
            }
        }
        Err(
            crate::archive::reconciliation::CacheReconciliationError::NotFound
            | crate::archive::reconciliation::CacheReconciliationError::Expired,
        ) => (
            Some(cache_request),
            Some(source_scope),
            None,
            BarsCacheStatusV1::Miss,
        ),
        Err(_) => (
            Some(cache_request),
            Some(source_scope),
            None,
            BarsCacheStatusV1::Unavailable,
        ),
    }
}

fn cache_request_for_group(
    request: &BarsRequestV1,
    group: &crate::market_query::planning::ProviderQueryGroup,
    page_identity: &str,
) -> Option<BarsRequestV1> {
    let mut cache_request = request.clone();
    cache_request.start = ProtoTimestampV2::parse(
        &group
            .fetch_window
            .start
            .to_rfc3339_opts(SecondsFormat::Nanos, true),
    )
    .ok()?;
    cache_request.end = ProtoTimestampV2::parse(
        &group
            .fetch_window
            .end_exclusive
            .to_rfc3339_opts(SecondsFormat::Nanos, true),
    )
    .ok()?;
    cache_request.cursor = Some(page_identity.to_owned());
    cache_request.validate().ok()?;
    Some(cache_request)
}

fn entitlement_state(value: BarsEntitlementStatusV1) -> EntitlementState {
    match value {
        BarsEntitlementStatusV1::Authorized => EntitlementState::Authorized,
        BarsEntitlementStatusV1::Unauthorized => EntitlementState::Unauthorized,
        BarsEntitlementStatusV1::Unknown | BarsEntitlementStatusV1::Mixed => {
            EntitlementState::Unknown
        }
    }
}

fn publish_cache_observation(
    runtime: &MarketBarsRuntime,
    prepared: &PreparedMarketBars,
    raw_page: BarsResponseV1,
    cache_page: BarsResponseV1,
    evidence: crate::archive::reconciliation::ProviderPageEvidenceV1,
    cancellation: &crate::cancellation::CancellationToken,
) {
    let (Some(cache), Some(request), Some(scope)) = (
        runtime.cache.as_ref(),
        prepared.cache_request.as_ref(),
        prepared.cache_scope.as_ref(),
    ) else {
        return;
    };
    if prepared.cache_status != BarsCacheStatusV1::Miss || cancellation.is_cancelled() {
        return;
    }
    // Keep SDK-normalized provider rows in the raw Parquet view and the trusted-calendar mapping
    // in the canonical view. The cache facade verifies their exact bar identity relation; it must
    // not treat provider-local session IDs as trusted calendar evidence.
    if cache
        .publish_bars_responses_cancellable(
            crate::archive::reconciliation::CacheBarsResponsesInputV1::new(
                request, scope, raw_page, cache_page, evidence,
            ),
            unix_seconds_now(),
            &crate::archive::reconciliation::SameFeedTolerancePolicyV1::default(),
            cancellation,
        )
        .is_err()
    {
        tracing::warn!("market-bars provisional cache publication was unavailable");
    }
}
