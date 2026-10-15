//! Bounded-supervisor query orchestration and provisional cache adapter.

use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
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
    hash_cache_rights_profile, unix_seconds_now,
};

struct PreparedMarketBars {
    plan: crate::market_query::planning::MarketBarsPlan,
    selection: Option<crate::market_query::providers::ProviderPageSelection>,
    cache_identity: Option<crate::market_query::providers::ProviderCacheIdentity>,
    cache_request: Option<BarsRequestV1>,
    cache_scope: Option<crate::archive::reconciliation::CacheQuerySourceScopeV1>,
    cache_hit: Option<CachedMarketBarsPage>,
    cache_status: BarsCacheStatusV1,
}

#[derive(Clone)]
struct CachedMarketBarsPage {
    response: BarsResponseV1,
    verification: Option<VerifiedCachePageEvidence>,
}

#[derive(Clone)]
struct VerifiedCachePageEvidence {
    verified_at: ProtoTimestampV2,
    canonical_rows: Vec<market_contracts::BarRowV1>,
}

#[derive(Clone, Copy, Debug)]
enum VerifiedCacheReadPhase {
    ExactCacheRead,
    LogicalCacheRead,
    PublishedReadback,
}

enum ProviderFetchOutcome {
    NoProviderGroup,
    CacheHit(CachedMarketBarsPage),
    Provider(crate::market_query::providers::ProviderPageFetch),
}

struct CachePreflightContext<'a> {
    runtime: &'a MarketBarsRuntime,
    request: &'a BarsRequestV1,
    plan: &'a crate::market_query::planning::MarketBarsPlan,
    selection: &'a crate::market_query::providers::ProviderPageSelection,
    identity: &'a crate::market_query::providers::ProviderCacheIdentity,
    principal_scope_sha256: &'a str,
    cache_profile: Option<&'a str>,
    cancellation: &'a crate::cancellation::CancellationToken,
}

struct LogicalCacheLookupContext<'a> {
    runtime: &'a MarketBarsRuntime,
    request: &'a BarsRequestV1,
    plan: &'a crate::market_query::planning::MarketBarsPlan,
    selection: &'a crate::market_query::providers::ProviderPageSelection,
    current_identity: &'a crate::market_query::providers::ProviderCacheIdentity,
    current_key: &'a crate::archive::reconciliation::CacheKeyV1,
    current_scope: &'a crate::archive::reconciliation::CacheQuerySourceScopeV1,
    cancellation: &'a crate::cancellation::CancellationToken,
    now_unix_seconds: u64,
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
    let prepare_calendar = calendar;
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
        let now_unix_seconds = unix_seconds_now();
        // Every opaque cursor is tied to a verified principal, even when no operator cache
        // profile is configured. An empty profile does not grant provider admission.
        let rights_profile_sha256 = hash_cache_rights_profile(
            cache_profile.as_deref().unwrap_or(""),
            &principal_scope_sha256,
        );
        let as_of = prepare_runtime
            .provider
            .planning_as_of(&prepare_request, &now, &rights_profile_sha256)
            .map_err(provider_query_error_to_market_data)?;
        // Calendar trust and revocation use the current wall clock on every request. The outer
        // cursor keeps its original `as_of`, but the current signed overlay selection is part of
        // the plan version, so an expired/revoked/changed overlay cannot resume an old plan.
        let requested_window = request_window(&prepare_request)?;
        let calendar_selection = prepare_calendar.snapshot_for_instrument(
            prepare_request.asset_class,
            &prepare_request.symbol,
            requested_window,
        );
        // Route attempts are selected from the server-owned policy and exact trusted calendar,
        // independently of the optional cache profile. An A04 admission can still affect cache
        // authority later, but absence of one does not prevent a configured provisional attempt.
        #[cfg(test)]
        let capabilities = if let Some(source) = prepare_runtime.synthetic_page_source.as_ref() {
            source.source_capabilities()
        } else {
            prepare_runtime.provider.source_capabilities_for_request(
                &prepare_request,
                &as_of,
                calendar_selection.snapshot.as_ref(),
            )
        };
        #[cfg(not(test))]
        let capabilities = prepare_runtime.provider.source_capabilities_for_request(
            &prepare_request,
            &as_of,
            calendar_selection.snapshot.as_ref(),
        );
        let capabilities = filter_overnight_capabilities(capabilities, &calendar_selection);
        let plan = crate::market_query::planning::plan_market_bars(
            &prepare_request,
            &as_of,
            &capabilities,
            calendar_selection.snapshot.as_ref(),
        )
        .map_err(planning_error_to_market_data)?;
        if cancellation.is_cancelled() {
            return Err(crate::MarketDataError::Storage(
                crate::error::StorageFailure::Cancelled,
            ));
        }

        #[cfg(test)]
        let selection = if plan.provider_groups.is_empty() {
            None
        } else if prepare_runtime.synthetic_page_source.is_some() {
            let selection = prepare_runtime
                .provider
                .select_page_at_synthetic_for_test(
                    &prepare_request,
                    &plan,
                    &rights_profile_sha256,
                    now_unix_seconds,
                )
                .map_err(provider_query_error_to_market_data)?;
            Some(selection)
        } else {
            let selection = prepare_runtime
                .provider
                .select_page_at(
                    &prepare_request,
                    &plan,
                    &rights_profile_sha256,
                    now_unix_seconds,
                )
                .map_err(provider_query_error_to_market_data)?;
            Some(selection)
        };
        #[cfg(not(test))]
        let selection = if plan.provider_groups.is_empty() {
            None
        } else {
            let selection = prepare_runtime
                .provider
                .select_page_at(
                    &prepare_request,
                    &plan,
                    &rights_profile_sha256,
                    now_unix_seconds,
                )
                .map_err(provider_query_error_to_market_data)?;
            Some(selection)
        };
        #[cfg(test)]
        let cache_identity = selection.as_ref().map(|selection| {
            prepare_runtime.synthetic_page_source.as_ref().map_or_else(
                || selection.cache_identity().clone(),
                |source| source.cache_identity(selection),
            )
        });
        #[cfg(not(test))]
        let cache_identity = selection
            .as_ref()
            .map(|selection| selection.cache_identity().clone());
        let (cache_request, cache_scope, cache_hit, cache_status) =
            match (selection.as_ref(), cache_identity.as_ref()) {
                (Some(selection), Some(identity)) => cache_preflight(CachePreflightContext {
                    runtime: &prepare_runtime,
                    request: &prepare_request,
                    plan: &plan,
                    selection,
                    identity,
                    principal_scope_sha256: &principal_scope_sha256,
                    cache_profile: cache_profile.as_deref(),
                    cancellation: &cancellation,
                }),
                (Some(_), None) => (
                    None,
                    None,
                    None,
                    if prepare_runtime.cache.is_some() {
                        BarsCacheStatusV1::Bypass
                    } else {
                        BarsCacheStatusV1::Unavailable
                    },
                ),
                (None, _) => (
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
            cache_identity,
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
        #[cfg(test)]
        if let Some(source) = provider_runtime.synthetic_page_source.as_ref() {
            return source
                .fetch_page(&prepared.plan, selection)
                .map(ProviderFetchOutcome::Provider);
        }
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
            Ok(ProviderFetchOutcome::CacheHit(page)) => {
                let mut response = response_from_page(
                    &finalize_request,
                    &prepared.plan,
                    prepared.selection.as_ref(),
                    page.response,
                    prepared.cache_status,
                )?;
                if let (Some(verification), Some(identity)) =
                    (page.verification, prepared.cache_identity.as_ref())
                {
                    super::response::attach_verified_at_from_cache(
                        &mut response,
                        &verification.verified_at,
                        &identity.provider,
                        &identity.feed,
                        &verification.canonical_rows,
                    );
                }
                response
            }
            Ok(ProviderFetchOutcome::Provider(page)) => {
                let selection =
                    prepared
                        .selection
                        .as_ref()
                        .ok_or(crate::MarketDataError::Provider(
                            crate::error::ProviderFailure::InvalidResponse,
                        ))?;
                let page_cache_status = provider_page_cache_status(
                    selection.provider_group().source,
                    page.response.rows.len(),
                    prepared.cache_status,
                );
                let raw_page = page.response.clone();
                let cache_page = response_for_cache_page(
                    &finalize_request,
                    &prepared.plan,
                    selection,
                    raw_page.clone(),
                    page_cache_status,
                )?;
                let response = response_from_page(
                    &finalize_request,
                    &prepared.plan,
                    Some(selection),
                    page.response,
                    page_cache_status,
                )?;
                let verification = publish_cache_observation(
                    &finalize_runtime,
                    &prepared,
                    raw_page,
                    cache_page,
                    page.evidence,
                    &cancellation,
                );
                let mut response = response;
                if let (Some(verification), Some(identity)) =
                    (verification, prepared.cache_identity.as_ref())
                {
                    super::response::attach_verified_at_from_cache(
                        &mut response,
                        &verification.verified_at,
                        &identity.provider,
                        &identity.feed,
                        &verification.canonical_rows,
                    );
                }
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
    context: CachePreflightContext<'_>,
) -> (
    Option<BarsRequestV1>,
    Option<crate::archive::reconciliation::CacheQuerySourceScopeV1>,
    Option<CachedMarketBarsPage>,
    BarsCacheStatusV1,
) {
    let CachePreflightContext {
        runtime,
        request,
        plan,
        selection,
        identity,
        principal_scope_sha256,
        cache_profile,
        cancellation,
    } = context;
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
        rights_profile_sha256: hash_cache_rights_profile(profile, principal_scope_sha256),
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
    let read_result = if cancellation.is_cancelled() {
        Err(crate::archive::reconciliation::CacheReconciliationError::Cancelled)
    } else if let Some(authority) = runtime.authority.as_ref() {
        // An authoritative current pointer must be cold-verified against the currently injected
        // issuer/revocation verifier on every read. This synchronous method still runs inside the
        // supervisor's joined blocking prepare phase; cancellation is checked on both sides.
        let result =
            cache.load_current_with_verifier(&key, now_unix_seconds, authority.verifier.as_ref());
        if cancellation.is_cancelled() {
            Err(crate::archive::reconciliation::CacheReconciliationError::Cancelled)
        } else {
            result
        }
    } else {
        cache.load_current_cancellable(&key, now_unix_seconds, cancellation)
    };
    let exact_result = match read_result {
        Ok(read) => {
            let verification = verified_cache_page_evidence(
                &read,
                now_unix_seconds,
                VerifiedCacheReadPhase::ExactCacheRead,
            );
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
                    Some(cache_request.clone()),
                    Some(source_scope.clone()),
                    Some(CachedMarketBarsPage {
                        response,
                        verification,
                    }),
                    BarsCacheStatusV1::Hit,
                )
            } else {
                (
                    Some(cache_request.clone()),
                    Some(source_scope.clone()),
                    None,
                    BarsCacheStatusV1::Miss,
                )
            }
        }
        Err(
            crate::archive::reconciliation::CacheReconciliationError::NotFound
            | crate::archive::reconciliation::CacheReconciliationError::Expired,
        ) => (
            Some(cache_request.clone()),
            Some(source_scope.clone()),
            None,
            BarsCacheStatusV1::Miss,
        ),
        Err(_) => (
            Some(cache_request.clone()),
            Some(source_scope.clone()),
            None,
            BarsCacheStatusV1::Unavailable,
        ),
    };
    if exact_result.2.is_some() || exact_result.3 != BarsCacheStatusV1::Miss {
        return exact_result;
    }
    let logical_hit = logical_authoritative_cache_page(LogicalCacheLookupContext {
        runtime,
        request,
        plan,
        selection,
        current_identity: identity,
        current_key: &key,
        current_scope: &source_scope,
        cancellation,
        now_unix_seconds,
    });
    match logical_hit {
        Some(page) => (
            Some(cache_request),
            Some(source_scope),
            Some(page),
            BarsCacheStatusV1::Hit,
        ),
        None => exact_result,
    }
}

/// Reuse one cold-reverified authoritative Alpaca session when its fetch key differs from the
/// current request. This deliberately supports one complete current session only; multi-session,
/// partial, truncated, or over-limit cases continue through the normal provider path.
fn logical_authoritative_cache_page(
    context: LogicalCacheLookupContext<'_>,
) -> Option<CachedMarketBarsPage> {
    use crate::{
        archive::reconciliation::CacheAuthorityV1,
        session_router::{AlpacaFeed, PlannedSource},
    };

    let LogicalCacheLookupContext {
        runtime,
        request,
        plan,
        selection,
        current_identity,
        current_key,
        current_scope,
        cancellation,
        now_unix_seconds,
    } = context;
    if cancellation.is_cancelled()
        || !logical_cache_mode_allows_alpaca(request.provider_mode)
        || selection.cache_policy()
            != crate::market_query::providers::ProviderCachePolicy::ProvisionalOnly
        || current_identity.provider != "alpaca"
        || current_identity.market_calendar_version.as_deref()
            != Some(plan.calendar_version.as_str())
        || current_scope.provider != "alpaca"
        || current_scope.market_calendar_version.as_deref() != Some(plan.calendar_version.as_str())
        || current_scope.rights_profile_sha256 != current_key.rights_profile_sha256
    {
        return None;
    }
    let expected_feed = match selection.provider_group().source {
        PlannedSource::Alpaca {
            feed: AlpacaFeed::Sip,
        } => "sip",
        PlannedSource::Alpaca {
            feed: AlpacaFeed::Boats,
        } => "boats",
        PlannedSource::Futu | PlannedSource::YFinanceDailyResearch => return None,
    };
    if current_identity.requested_feed.as_deref() != Some(expected_feed)
        || selection.provider_group().slices_recent_to_old.len() != 1
        || !group_contains_only_old_verified_slices(selection.provider_group())
    {
        return None;
    }
    let slice = selection.provider_group().slices_recent_to_old.first()?;
    let session_instance_id = slice.session_instance_id.as_deref()?;
    if slice.source != Some(selection.provider_group().source)
        || current_identity.session_instance_ids.len() != 1
        || current_identity
            .session_instance_ids
            .first()
            .map(String::as_str)
            != Some(session_instance_id)
    {
        return None;
    }

    let cache = runtime.cache.as_ref()?;
    let authority = runtime.authority.as_ref()?;
    let logical_identity =
        crate::archive::reconciliation::logical_index::CacheLogicalIdentityV1::from_key(
            current_key,
        )
        .ok()?;
    if logical_identity.market_calendar_version.as_deref() != Some(plan.calendar_version.as_str())
        || logical_identity.rights_profile_sha256 != current_scope.rights_profile_sha256
    {
        return None;
    }
    let read = cache
        .load_logical_current_with_verifier(
            &logical_identity,
            session_instance_id,
            now_unix_seconds,
            authority.verifier.as_ref(),
        )
        .ok()?;
    if cancellation.is_cancelled()
        || read.session.current.authority != CacheAuthorityV1::Authoritative
        || read.snapshot.current.authority != CacheAuthorityV1::Authoritative
        || read.snapshot.conflict_rejected
        || !logical_identity
            .matches_key(&read.session.current.key)
            .ok()?
    {
        return None;
    }

    let cached_key = &read.session.current.key;
    let asset_class = serde_json::to_value(request.asset_class)
        .ok()?
        .as_str()?
        .to_owned();
    let session_scope = serde_json::to_value(request.session_scope)
        .ok()?
        .as_str()?
        .to_owned();
    let adjustment = serde_json::to_value(request.adjustment)
        .ok()?
        .as_str()?
        .to_owned();
    if !logical_cache_source_matches(
        expected_feed,
        &cached_key.provider,
        &cached_key.feed,
        cached_key.entitlement,
    ) || cached_key.numeric_encoding != current_identity.numeric_encoding.as_str()
        || cached_key.source_scope_id != current_identity.source_scope_id
        || cached_key.rights_profile_sha256 != current_scope.rights_profile_sha256
        || cached_key.symbol != request.symbol
        || cached_key.asset_class != asset_class
        || cached_key.requested_resolution != request.resolution.as_str()
        || cached_key.effective_resolution != selection.provider_group().resolution.as_str()
        || cached_key.adjustment != adjustment
        || cached_key.session_scope != session_scope
        || !matches!(cached_key.provider_mode.as_str(), "auto" | "alpaca_only")
        || cached_key.market_calendar_version.as_deref() != Some(plan.calendar_version.as_str())
        || read.session.current.session_instance_id != session_instance_id
    {
        return None;
    }

    let covered_start = parse_utc_timestamp(&read.session.current.covered_start_utc)?;
    let covered_end = parse_utc_timestamp(&read.session.current.covered_end_exclusive_utc)?;
    if covered_start > slice.window.start || covered_end < slice.window.end_exclusive {
        return None;
    }

    let mut cached_page = read
        .snapshot
        .bars_evidence
        .to_cached_response(
            read.snapshot.bars_rows.clone(),
            BarsCacheStatusV1::Hit,
            None,
        )
        .ok()?;
    if cached_page.provider != "alpaca"
        || cached_page.feed != expected_feed
        || cached_page.entitlement_status != BarsEntitlementStatusV1::Authorized
        || cached_page.requested_resolution != request.resolution
        || cached_page.effective_resolution != selection.provider_group().resolution
        || cached_page.adjustment != request.adjustment
        || cached_page.market_calendar_version.as_deref() != Some(plan.calendar_version.as_str())
        || cached_page.truncated
    {
        return None;
    }
    let provenance = cached_page
        .source_provenance
        .iter()
        .map(|item| (item.provenance_id.clone(), item.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let valid_provenance = |id: &str| {
        provenance.get(id).is_some_and(|item| {
            item.provider == "alpaca"
                && item.feed == expected_feed
                && item.entitlement_status == BarsEntitlementStatusV1::Authorized
                && item.numeric_encoding == current_identity.numeric_encoding
        })
    };

    let mut clipped_segments = Vec::new();
    let mut used_provenance = std::collections::BTreeSet::new();
    for segment in &cached_page.segments {
        let start = parse_utc_timestamp(segment.start_utc.as_str())?;
        let end = parse_utc_timestamp(segment.end_exclusive_utc.as_str())?;
        let intersects = start < slice.window.end_exclusive && slice.window.start < end;
        if !intersects {
            continue;
        }
        if segment.session_instance_id != session_instance_id
            || segment.coverage_status != market_contracts::BarsCoverageStatusV1::Complete
        {
            return None;
        }
        let provenance_id = segment.provenance_id.as_deref()?;
        if !valid_provenance(provenance_id) {
            return None;
        }
        used_provenance.insert(provenance_id.to_owned());
        let clipped_start = start.max(slice.window.start);
        let clipped_end = end.min(slice.window.end_exclusive);
        if clipped_start >= clipped_end {
            return None;
        }
        clipped_segments.push((
            clipped_start,
            clipped_end,
            market_contracts::BarCoverageSegmentV1 {
                start_utc: format_utc_timestamp(clipped_start)?,
                end_exclusive_utc: format_utc_timestamp(clipped_end)?,
                coverage_status: segment.coverage_status,
                provenance_id: segment.provenance_id.clone(),
                session_instance_id: segment.session_instance_id.clone(),
            },
        ));
    }
    clipped_segments.sort_by_key(|(start, end, _)| (*start, *end));
    let mut coverage_cursor = slice.window.start;
    for (start, end, _) in &clipped_segments {
        if *start != coverage_cursor || *end <= *start {
            return None;
        }
        coverage_cursor = *end;
    }
    if clipped_segments.is_empty() || coverage_cursor != slice.window.end_exclusive {
        return None;
    }
    let clipped_segments = clipped_segments
        .into_iter()
        .map(|(_, _, segment)| segment)
        .collect::<Vec<_>>();

    for gap in &cached_page.gaps {
        let start = parse_utc_timestamp(gap.start_utc.as_str())?;
        let end = parse_utc_timestamp(gap.end_exclusive_utc.as_str())?;
        if start < slice.window.end_exclusive && slice.window.start < end {
            return None;
        }
    }
    let mut rows_by_start = std::collections::BTreeMap::new();
    for row in cached_page.rows.drain(..) {
        let start = parse_utc_timestamp(row.bar_start_utc.as_str())?;
        let end = parse_utc_timestamp(row.bar_end_exclusive_utc.as_str())?;
        let intersects = start < slice.window.end_exclusive && slice.window.start < end;
        if !intersects {
            continue;
        }
        if row.session_instance_id != session_instance_id
            || start < slice.window.start
            || end > slice.window.end_exclusive
            || row.symbol != request.symbol
            || row.asset_class != request.asset_class
            || row.resolution != selection.provider_group().resolution
            || row.adjustment != request.adjustment
            || row.validation_state != market_contracts::BarsValidationStateV1::Validated
            || !valid_provenance(&row.provenance_id)
        {
            return None;
        }
        used_provenance.insert(row.provenance_id.clone());
        if let Some(previous) = rows_by_start.insert(start, row.clone())
            && previous != row
        {
            return None;
        }
    }
    if rows_by_start.is_empty() || rows_by_start.len() > usize::from(request.limit) {
        return None;
    }
    let rows = rows_by_start.into_values().collect::<Vec<_>>();
    if rows.windows(2).any(|pair| {
        parse_utc_timestamp(pair[0].bar_end_exclusive_utc.as_str())
            .zip(parse_utc_timestamp(pair[1].bar_start_utc.as_str()))
            .is_none_or(|(left_end, right_start)| left_end > right_start)
    }) {
        return None;
    }

    let source_provenance = cached_page
        .source_provenance
        .into_iter()
        .filter(|item| used_provenance.contains(&item.provenance_id))
        .collect::<Vec<_>>();
    if source_provenance.is_empty() {
        return None;
    }
    cached_page.rows = rows;
    cached_page.segments = clipped_segments;
    cached_page.gaps.clear();
    cached_page.source_provenance = source_provenance;
    cached_page.coverage_status = market_contracts::BarsCoverageStatusV1::Complete;
    cached_page.continuation_cursor = None;
    cached_page.truncated = false;
    cached_page.validate().ok()?;
    request.validate_response(&cached_page).ok()?;
    let mut verification = verified_cache_page_evidence(
        &read.snapshot,
        now_unix_seconds,
        VerifiedCacheReadPhase::LogicalCacheRead,
    );
    if let Some(verification) = verification.as_mut() {
        verification.canonical_rows.clone_from(&cached_page.rows);
    }
    Some(CachedMarketBarsPage {
        response: cached_page,
        verification,
    })
}

fn parse_utc_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn verified_cache_page_evidence(
    read: &crate::archive::reconciliation::CacheReadResultV1,
    now_unix_seconds: u64,
    phase: VerifiedCacheReadPhase,
) -> Option<VerifiedCachePageEvidence> {
    use market_contracts::{BarsCoverageStatusV1, BarsValidationStateV1};

    let verified_at = verified_at_from_cache_receipt(
        read.current.authority,
        read.created_at_unix_seconds,
        now_unix_seconds,
    );
    let coverage_complete = read.coverage.is_complete_for_key(&read.key);
    let canonical_rows_validated = !read.bars_rows.is_empty()
        && read
            .bars_rows
            .iter()
            .all(|row| row.validation_state == BarsValidationStateV1::Validated);
    let canonical_summary_complete = read.bars_evidence.coverage_status
        == BarsCoverageStatusV1::Complete
        && !read.bars_evidence.truncated
        && read.bars_evidence.gaps.is_empty()
        && !read.bars_evidence.segments.is_empty()
        && read
            .bars_evidence
            .segments
            .iter()
            .all(|segment| segment.coverage_status == BarsCoverageStatusV1::Complete);
    if verified_at.is_none()
        || !coverage_complete
        || !canonical_summary_complete
        || !canonical_rows_validated
    {
        #[cfg(test)]
        eprintln!(
            "synthetic market-bars verified_at diagnostic: phase={phase:?} authority={:?} has_verified_receipt={} coverage_complete={coverage_complete} canonical_summary_complete={canonical_summary_complete} canonical_rows_validated={canonical_rows_validated} canonical_row_count={}",
            read.current.authority,
            verified_at.is_some(),
            read.bars_rows.len(),
        );
        #[cfg(not(test))]
        let _ = phase;
        return None;
    }
    Some(VerifiedCachePageEvidence {
        verified_at: verified_at?,
        canonical_rows: read.bars_rows.clone(),
    })
}

fn verified_at_from_cache_receipt(
    authority: crate::archive::reconciliation::CacheAuthorityV1,
    created_at_unix_seconds: u64,
    now_unix_seconds: u64,
) -> Option<ProtoTimestampV2> {
    if authority != crate::archive::reconciliation::CacheAuthorityV1::Authoritative
        || created_at_unix_seconds > now_unix_seconds
    {
        return None;
    }
    let seconds = i64::try_from(created_at_unix_seconds).ok()?;
    format_utc_timestamp(DateTime::<Utc>::from_timestamp(seconds, 0)?)
}

fn request_window(request: &BarsRequestV1) -> crate::Result<crate::session_router::UtcWindow> {
    let start =
        parse_utc_timestamp(request.start.as_str()).ok_or(crate::MarketDataError::InvalidInput)?;
    let end_exclusive =
        parse_utc_timestamp(request.end.as_str()).ok_or(crate::MarketDataError::InvalidInput)?;
    if start >= end_exclusive {
        return Err(crate::MarketDataError::InvalidInput);
    }
    Ok(crate::session_router::UtcWindow {
        start,
        end_exclusive,
    })
}

fn filter_overnight_capabilities(
    capabilities: Vec<crate::session_router::SourceCapability>,
    calendar: &crate::market_calendar::CalendarSnapshotSelection,
) -> Vec<crate::session_router::SourceCapability> {
    let overlay_applied =
        calendar.status == crate::market_calendar::OvernightOverlayStatus::Applied;
    capabilities
        .into_iter()
        .filter(|capability| {
            capability.phase != Some(crate::session_router::SessionPhase::Over)
                || (overlay_applied
                    && calendar
                        .allowed_overnight_routes
                        .iter()
                        .any(|route| route.matches_source_capability(capability)))
        })
        .collect()
}

fn format_utc_timestamp(value: DateTime<Utc>) -> Option<ProtoTimestampV2> {
    ProtoTimestampV2::parse(&value.to_rfc3339_opts(SecondsFormat::Nanos, true)).ok()
}

fn logical_cache_mode_allows_alpaca(mode: market_contracts::BarsProviderModeV1) -> bool {
    matches!(
        mode,
        market_contracts::BarsProviderModeV1::Auto
            | market_contracts::BarsProviderModeV1::AlpacaOnly
    )
}

fn logical_cache_source_matches(
    target_feed: &str,
    actual_provider: &str,
    actual_feed: &str,
    entitlement: EntitlementState,
) -> bool {
    actual_provider == "alpaca"
        && actual_feed == target_feed
        && entitlement == EntitlementState::Authorized
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

fn provider_page_cache_status(
    source: crate::session_router::PlannedSource,
    row_count: usize,
    current: BarsCacheStatusV1,
) -> BarsCacheStatusV1 {
    if row_count == 0
        && selected_alpaca_feed(source).is_some()
        && current != BarsCacheStatusV1::Unavailable
    {
        BarsCacheStatusV1::Bypass
    } else {
        current
    }
}

fn selected_alpaca_feed(source: crate::session_router::PlannedSource) -> Option<&'static str> {
    match source {
        crate::session_router::PlannedSource::Alpaca {
            feed: crate::session_router::AlpacaFeed::Sip,
        } => Some("sip"),
        crate::session_router::PlannedSource::Alpaca {
            feed: crate::session_router::AlpacaFeed::Boats,
        } => Some("boats"),
        crate::session_router::PlannedSource::Futu
        | crate::session_router::PlannedSource::YFinanceDailyResearch => None,
    }
}

fn alpaca_query_access_publication_is_eligible(
    selection: &crate::market_query::providers::ProviderPageSelection,
    identity: Option<&crate::market_query::providers::ProviderCacheIdentity>,
    scope: &crate::archive::reconciliation::CacheQuerySourceScopeV1,
    raw: &BarsResponseV1,
    canonical: &BarsResponseV1,
    evidence: &crate::archive::reconciliation::ProviderPageEvidenceV1,
    phase_slices: &[crate::archive::reconciliation::authority::ProviderPhaseSliceV1],
) -> bool {
    let Some(expected_feed) = selected_alpaca_feed(selection.provider_group().source) else {
        return false;
    };
    let Some(identity) = identity else {
        return false;
    };
    let page_count = evidence.alpaca_query_access_pages.len();
    let provenance_matches = |response: &BarsResponseV1| {
        !response.source_provenance.is_empty()
            && response.source_provenance.iter().all(|provenance| {
                provenance.provider == "alpaca"
                    && provenance.feed == expected_feed
                    && provenance.entitlement_status == BarsEntitlementStatusV1::Unknown
                    && provenance.numeric_encoding
                        == market_contracts::NumericEncodingV1::DecimalToken
            })
    };

    group_contains_only_old_verified_slices(selection.provider_group())
        && !phase_slices.is_empty()
        && (1..=crate::archive::reconciliation::alpaca_query_access::MAX_ALPACA_QUERY_ACCESS_PAGES)
            .contains(&page_count)
        && u32::try_from(page_count).ok() == evidence.pages_seen
        && evidence.response_observed_at_utc.is_some()
        && evidence
            .source_observation_sha256
            .as_deref()
            .is_some_and(|digest| {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        && evidence.truncated == raw.truncated
        && evidence.truncated == canonical.truncated
        && identity.provider == "alpaca"
        && identity.feed == expected_feed
        && identity.requested_feed.as_deref() == Some(expected_feed)
        && identity.entitlement_status == BarsEntitlementStatusV1::Unknown
        && identity.numeric_encoding == market_contracts::NumericEncodingV1::DecimalToken
        && identity.effective_resolution == selection.provider_group().resolution
        && identity.session_instance_ids == scope.session_instance_ids
        && identity.market_calendar_version.as_deref() == scope.market_calendar_version.as_deref()
        && identity.source_scope_id == scope.source_scope_id
        && scope.provider == "alpaca"
        && scope.feed == expected_feed
        && scope.entitlement == EntitlementState::Unknown
        && scope.numeric_encoding == market_contracts::NumericEncodingV1::DecimalToken
        && scope.effective_resolution == selection.provider_group().resolution
        && raw.provider == "alpaca"
        && raw.feed == expected_feed
        && raw.entitlement_status == BarsEntitlementStatusV1::Unknown
        && canonical.provider == "alpaca"
        && canonical.feed == expected_feed
        && canonical.entitlement_status == BarsEntitlementStatusV1::Unknown
        && !raw.rows.is_empty()
        && !canonical.rows.is_empty()
        && provenance_matches(raw)
        && provenance_matches(canonical)
}

fn publish_cache_observation(
    runtime: &MarketBarsRuntime,
    prepared: &PreparedMarketBars,
    raw_page: BarsResponseV1,
    cache_page: BarsResponseV1,
    evidence: crate::archive::reconciliation::ProviderPageEvidenceV1,
    cancellation: &crate::cancellation::CancellationToken,
) -> Option<VerifiedCachePageEvidence> {
    let (Some(cache), Some(request), Some(scope)) = (
        runtime.cache.as_ref(),
        prepared.cache_request.as_ref(),
        prepared.cache_scope.as_ref(),
    ) else {
        return None;
    };
    if prepared.cache_status != BarsCacheStatusV1::Miss || cancellation.is_cancelled() {
        return None;
    }
    let selection = prepared.selection.as_ref()?;
    if selected_alpaca_feed(selection.provider_group().source).is_some() && raw_page.rows.is_empty()
    {
        return None;
    }
    let phase_slices = provider_phase_slices(selection).unwrap_or_default();
    let fallback_policy =
        crate::archive::reconciliation::authority::ReconciliationPolicyV1::legacy_same_feed(
            crate::archive::reconciliation::SameFeedTolerancePolicyV1::default(),
        );
    let policy = runtime
        .authority
        .as_ref()
        .map_or(&fallback_policy, |authority| {
            &authority.reconciliation_policy
        });
    let now_unix_seconds = unix_seconds_now();

    let input = || {
        crate::archive::reconciliation::CacheBarsResponsesInputV1::new(
            request,
            scope,
            raw_page.clone(),
            cache_page.clone(),
            evidence.clone(),
        )
        .with_phase_slices(&phase_slices)
    };

    if authority_publish_is_eligible(
        selection.provider_group(),
        prepared.cache_identity.as_ref(),
        scope,
        &raw_page,
        &cache_page,
        &evidence,
        &phase_slices,
    ) && let (Some(authority), Some(evidence_source)) = (
        runtime.authority.as_ref(),
        runtime.page_admission_evidence_source.as_ref(),
    ) {
        let admission_evidence =
            super::MarketBarsPageAdmissionContext::new(super::MarketBarsPageAdmissionInput {
                request,
                source_scope: scope,
                raw_observations: &raw_page,
                canonical_response: &cache_page,
                page_evidence: &evidence,
                phase_slices: &phase_slices,
                policy,
                now_unix_seconds,
            })
            .and_then(|context| evidence_source.evidence_for_page(context));
        if let Some(admission_evidence) = admission_evidence {
            match crate::archive::reconciliation::authority::verify_provider_admission_v1(
                request,
                scope,
                &admission_evidence,
                authority.verifier.as_ref(),
                &phase_slices,
                now_unix_seconds,
            ) {
                Ok(admission) if admission.dataset_binding().is_some() => {
                    if cancellation.is_cancelled() {
                        return None;
                    }
                    match cache.publish_bars_responses_with_admission(
                        input(),
                        now_unix_seconds,
                        policy,
                        &phase_slices,
                        &admission,
                    ) {
                        Ok(result) => {
                            consume_cache_invalidation(
                                runtime,
                                &result,
                                &scope.rights_profile_sha256,
                                now_unix_seconds,
                            );
                            return authoritative_page_after_publish(
                                runtime,
                                cache,
                                request,
                                scope,
                                &result,
                                cancellation,
                            );
                        }
                        Err(
                            crate::archive::reconciliation::CacheReconciliationError::AdmissionMismatch
                            | crate::archive::reconciliation::CacheReconciliationError::AdmissionExpired
                            | crate::archive::reconciliation::CacheReconciliationError::BlockedExternal
                            | crate::archive::reconciliation::CacheReconciliationError::InvalidInput,
                        ) => {
                            // A signed receipt that does not match these exact observations is
                            // not an authority grant. The immutable page may still be retained
                            // through the ordinary provisional path below.
                            tracing::warn!("market-bars admission did not match the fetched page");
                        }
                        Err(_) => {
                            // Conflicts and uncertain/failed atomic commits must not be retried as
                            // a lower-authority write. Leave their audit/current state intact.
                            tracing::warn!("market-bars signed cache publication was unavailable");
                            return None;
                        }
                    }
                }
                Ok(_) => {
                    tracing::warn!("market-bars admission evidence was not dataset-bound");
                }
                Err(_) => {
                    tracing::warn!("market-bars admission evidence was rejected");
                }
            }
        }
    }

    // This local query-access basis is independent of the signed admission path above. It records
    // only a real, non-empty SDK page for the selected feed; entitlement remains Unknown. The A04
    // publisher revalidates the opaque witness chain and grants only its short same-process lease.
    if selected_alpaca_feed(selection.provider_group().source).is_some()
        && scope.entitlement == EntitlementState::Unknown
        && alpaca_query_access_publication_is_eligible(
            selection,
            prepared.cache_identity.as_ref(),
            scope,
            &raw_page,
            &cache_page,
            &evidence,
            &phase_slices,
        )
    {
        match cache.publish_bars_responses_with_alpaca_query_access_cancellable(
            input(),
            now_unix_seconds,
            policy,
            cancellation,
        ) {
            Ok(result) => {
                consume_cache_invalidation(
                    runtime,
                    &result,
                    &scope.rights_profile_sha256,
                    now_unix_seconds,
                );
                return authoritative_page_after_publish(
                    runtime,
                    cache,
                    request,
                    scope,
                    &result,
                    cancellation,
                );
            }
            Err(_) => {
                tracing::warn!("market-bars Alpaca query-access cache publication was unavailable")
            }
        }
        return None;
    }

    // Keep SDK-normalized provider rows in the raw Parquet view and the trusted-calendar mapping
    // in the canonical view. The cache facade verifies their exact bar identity relation; it must
    // not treat provider-local session IDs as trusted calendar evidence.
    if cancellation.is_cancelled() {
        return None;
    }
    match cache.publish_bars_responses_with_policy(input(), now_unix_seconds, policy) {
        Ok(result) => consume_cache_invalidation(
            runtime,
            &result,
            &scope.rights_profile_sha256,
            now_unix_seconds,
        ),
        Err(_) => tracing::warn!("market-bars provisional cache publication was unavailable"),
    }
    None
}

fn authoritative_page_after_publish(
    runtime: &MarketBarsRuntime,
    cache: &crate::archive::reconciliation::CacheVersionStoreV1,
    request: &BarsRequestV1,
    scope: &crate::archive::reconciliation::CacheQuerySourceScopeV1,
    result: &crate::archive::reconciliation::CachePublishResultV1,
    cancellation: &crate::cancellation::CancellationToken,
) -> Option<VerifiedCachePageEvidence> {
    use crate::archive::reconciliation::CacheAuthorityV1;

    if cancellation.is_cancelled() || result.authority != CacheAuthorityV1::Authoritative {
        return None;
    }
    let published_current = result.current.as_ref()?;
    if published_current.authority != CacheAuthorityV1::Authoritative
        || published_current.version_id != result.published_version_id
    {
        return None;
    }
    let key = crate::archive::reconciliation::CacheKeyV1::from_request(request, scope).ok()?;
    let now_unix_seconds = unix_seconds_now();
    let read = if let Some(authority) = runtime.authority.as_ref() {
        cache.load_current_with_verifier(&key, now_unix_seconds, authority.verifier.as_ref())
    } else {
        cache.load_current_cancellable(&key, now_unix_seconds, cancellation)
    }
    .ok()?;
    if cancellation.is_cancelled() || &read.current != published_current {
        return None;
    }
    verified_cache_page_evidence(
        &read,
        unix_seconds_now(),
        VerifiedCacheReadPhase::PublishedReadback,
    )
}

fn provider_phase_slices(
    selection: &crate::market_query::providers::ProviderPageSelection,
) -> Option<Vec<crate::archive::reconciliation::authority::ProviderPhaseSliceV1>> {
    let mut slices = selection
        .provider_group()
        .slices_recent_to_old
        .iter()
        .map(|slice| {
            let session_instance_id = slice.session_instance_id.as_ref()?.clone();
            let phase = match slice.phase {
                Some(crate::session_router::SessionPhase::Pre) => "pre",
                Some(crate::session_router::SessionPhase::Regular) => "regular",
                Some(crate::session_router::SessionPhase::After) => "after",
                Some(crate::session_router::SessionPhase::Over) => "overnight",
                None if slice.interval == crate::session_router::DataInterval::Daily => "daily",
                None => return None,
            };
            Some((
                slice.window.start,
                crate::archive::reconciliation::authority::ProviderPhaseSliceV1 {
                    phase: phase.to_owned(),
                    session_instance_id,
                    start_utc: slice
                        .window
                        .start
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                    end_exclusive_utc: slice
                        .window
                        .end_exclusive
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                },
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    slices.sort_by_key(|(start, _)| *start);
    Some(slices.into_iter().map(|(_, slice)| slice).collect())
}

fn authority_publish_is_eligible(
    group: &crate::market_query::planning::ProviderQueryGroup,
    cache_identity: Option<&crate::market_query::providers::ProviderCacheIdentity>,
    scope: &crate::archive::reconciliation::CacheQuerySourceScopeV1,
    raw: &BarsResponseV1,
    canonical: &BarsResponseV1,
    evidence: &crate::archive::reconciliation::ProviderPageEvidenceV1,
    phase_slices: &[crate::archive::reconciliation::authority::ProviderPhaseSliceV1],
) -> bool {
    use crate::session_router::{AlpacaFeed, PlannedSource};
    use market_contracts::{BarsCoverageStatusV1, BarsEntitlementStatusV1};

    let Some(identity) = cache_identity else {
        return false;
    };
    let PlannedSource::Alpaca {
        feed: requested_feed,
    } = group.source
    else {
        return false;
    };
    let expected_feed = match requested_feed {
        AlpacaFeed::Sip => "sip",
        AlpacaFeed::Boats => "boats",
    };
    let old_verified_slices = group_contains_only_old_verified_slices(group);
    let exact_source_facts = |response: &BarsResponseV1| {
        response.provider == scope.provider
            && response.feed == scope.feed
            && response.entitlement_status == BarsEntitlementStatusV1::Authorized
            && response.effective_resolution == scope.effective_resolution
            && !response.source_provenance.is_empty()
            && response.source_provenance.iter().all(|provenance| {
                provenance.provider == scope.provider
                    && provenance.feed == scope.feed
                    && provenance.entitlement_status == BarsEntitlementStatusV1::Authorized
                    && provenance.numeric_encoding == scope.numeric_encoding
            })
    };
    let complete_page = |response: &BarsResponseV1| {
        response.coverage_status == BarsCoverageStatusV1::Complete
            && !response.rows.is_empty()
            && !response.segments.is_empty()
            && response.gaps.is_empty()
            && !response.truncated
            && response.rows.iter().all(|row| {
                row.validation_state == market_contracts::BarsValidationStateV1::Validated
            })
            && response
                .segments
                .iter()
                .all(|segment| segment.coverage_status == BarsCoverageStatusV1::Complete)
    };

    old_verified_slices
        && !phase_slices.is_empty()
        && scope.provider == "alpaca"
        && scope.feed == expected_feed
        && scope.entitlement == EntitlementState::Authorized
        && identity.provider == scope.provider
        && identity.requested_feed.as_deref() == Some(expected_feed)
        && identity.feed == scope.feed
        && identity.source_scope_id == scope.source_scope_id
        && identity.entitlement_status == BarsEntitlementStatusV1::Authorized
        && identity.numeric_encoding == scope.numeric_encoding
        && identity.effective_resolution == scope.effective_resolution
        && identity.session_instance_ids == scope.session_instance_ids
        && identity.market_calendar_version.as_deref() == scope.market_calendar_version.as_deref()
        && evidence
            .source_observation_sha256
            .as_deref()
            .is_some_and(|digest| {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        && evidence.pages_seen.is_some_and(|pages| pages > 0)
        && evidence.pages_exhausted == Some(true)
        && !evidence.truncated
        && exact_source_facts(raw)
        && exact_source_facts(canonical)
        && complete_page(raw)
        && complete_page(canonical)
}

fn group_contains_only_old_verified_slices(
    group: &crate::market_query::planning::ProviderQueryGroup,
) -> bool {
    use crate::session_router::SessionRank;

    !group.slices_recent_to_old.is_empty()
        && group.slices_recent_to_old.iter().all(|slice| {
            slice.rank == Some(SessionRank::Old)
                && slice.historical
                && !slice.realtime
                && !slice.provisional
                && slice.calendar_verification_status
                    == crate::session_router::CalendarVerificationStatus::Verified
        })
}

fn consume_cache_invalidation(
    runtime: &MarketBarsRuntime,
    result: &crate::archive::reconciliation::CachePublishResultV1,
    rights_profile_sha256: &str,
    now_unix_seconds: u64,
) {
    let (Some(cache), Some(invalidation)) =
        (runtime.cache.as_ref(), result.logical_invalidation.as_ref())
    else {
        return;
    };
    for change in &invalidation.changes {
        let identity =
            crate::archive::reconciliation::logical_index::CacheLogicalIdentityV1::from_key(
                &change.current_version.key,
            );
        let reread = identity.and_then(|identity| match change.current_version.authority {
            crate::archive::reconciliation::CacheAuthorityV1::Authoritative => runtime
                .authority
                .as_ref()
                .ok_or(crate::archive::reconciliation::CacheReconciliationError::BlockedExternal)
                .and_then(|authority| {
                    cache.load_logical_current_with_verifier(
                        &identity,
                        &change.session_instance_id,
                        now_unix_seconds,
                        authority.verifier.as_ref(),
                    )
                }),
            crate::archive::reconciliation::CacheAuthorityV1::Provisional => {
                cache.load_logical_current(&identity, &change.session_instance_id, now_unix_seconds)
            }
        });
        let event = match reread {
            Ok(read)
                if read.logical_current.generation >= invalidation.generation
                    && read.session.current.session_instance_id == change.session_instance_id =>
            {
                let current = &read.session.current;
                let authority = match current.authority {
                    crate::archive::reconciliation::CacheAuthorityV1::Authoritative => {
                        "authoritative"
                    }
                    crate::archive::reconciliation::CacheAuthorityV1::Provisional => "provisional",
                };
                super::MarketBarsCacheInvalidationEvent {
                    rights_profile_sha256: rights_profile_sha256.to_owned(),
                    identity_fingerprint_sha256: Some(
                        invalidation.identity_fingerprint_sha256.clone(),
                    ),
                    session_instance_id: Some(change.session_instance_id.clone()),
                    logical_generation: Some(read.logical_current.generation),
                    logical_updated_at_unix_seconds: Some(
                        read.logical_current.updated_at_unix_seconds,
                    ),
                    reread_status: "reread_verified",
                    current_provider: Some(current.key.provider.clone()),
                    current_feed: Some(current.key.feed.clone()),
                    current_authority: Some(authority.to_owned()),
                }
            }
            _ => super::MarketBarsCacheInvalidationEvent {
                rights_profile_sha256: rights_profile_sha256.to_owned(),
                identity_fingerprint_sha256: None,
                session_instance_id: None,
                logical_generation: None,
                logical_updated_at_unix_seconds: None,
                reread_status: "reread_unavailable",
                current_provider: None,
                current_feed: None,
                current_authority: None,
            },
        };
        runtime.record_cache_invalidation_event(event);
    }
}

#[cfg(test)]
mod authority_eligibility_tests {
    use chrono::{NaiveDate, TimeZone, Utc};

    use crate::{
        market_query::planning::ProviderQueryGroup,
        session_router::{
            AlpacaFeed, CalendarVerificationStatus, DataInterval, PlannedSlice, PlannedSource,
            SessionPhase, SessionRank, UtcWindow,
        },
    };

    use super::{
        group_contains_only_old_verified_slices, logical_cache_mode_allows_alpaca,
        logical_cache_source_matches, provider_page_cache_status, verified_at_from_cache_receipt,
    };

    fn slice(rank: SessionRank) -> PlannedSlice {
        let start = Utc.with_ymd_and_hms(2026, 1, 5, 14, 30, 0).unwrap();
        PlannedSlice {
            symbol: "SPY".to_owned(),
            trade_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            session_id: "XNYS:2026-01-05".to_owned(),
            session_instance_id: Some("XNYS:2026-01-05:regular".to_owned()),
            phase: Some(SessionPhase::Regular),
            rank: Some(rank),
            interval: DataInterval::Minute(1),
            window: UtcWindow {
                start,
                end_exclusive: start + chrono::Duration::minutes(1),
            },
            calendar_verification_status: CalendarVerificationStatus::Verified,
            source: Some(PlannedSource::Alpaca {
                feed: AlpacaFeed::Sip,
            }),
            source_label: Some("synthetic eligibility test".to_owned()),
            historical: true,
            realtime: false,
            provisional: false,
            gap: None,
        }
    }

    fn group(slice: PlannedSlice) -> ProviderQueryGroup {
        ProviderQueryGroup {
            source: PlannedSource::Alpaca {
                feed: AlpacaFeed::Sip,
            },
            resolution: market_contracts::BarResolutionV1::OneMinute,
            adjustment: market_contracts::AdjustmentV1::Unadjusted,
            fetch_window: slice.window,
            slices_recent_to_old: vec![slice],
        }
    }

    #[test]
    fn only_verified_historical_old_slices_can_enter_authoritative_cache_path() {
        assert!(group_contains_only_old_verified_slices(&group(slice(
            SessionRank::Old
        ))));
        assert!(!group_contains_only_old_verified_slices(&group(slice(
            SessionRank::Buffer
        ))));
        assert!(!group_contains_only_old_verified_slices(&group(slice(
            SessionRank::New
        ))));
        assert!(!group_contains_only_old_verified_slices(
            &ProviderQueryGroup {
                slices_recent_to_old: Vec::new(),
                ..group(slice(SessionRank::Old))
            }
        ));

        let mut provisional = slice(SessionRank::Old);
        provisional.provisional = true;
        assert!(!group_contains_only_old_verified_slices(&group(
            provisional
        )));

        let mut unverified_calendar = slice(SessionRank::Old);
        unverified_calendar.calendar_verification_status = CalendarVerificationStatus::Provisional;
        assert!(!group_contains_only_old_verified_slices(&group(
            unverified_calendar
        )));
    }

    #[test]
    fn logical_cache_respects_provider_mode_and_exact_actual_feed() {
        use market_contracts::{BarsProviderModeV1, EntitlementState};

        assert!(logical_cache_mode_allows_alpaca(BarsProviderModeV1::Auto));
        assert!(logical_cache_mode_allows_alpaca(
            BarsProviderModeV1::AlpacaOnly
        ));
        assert!(!logical_cache_mode_allows_alpaca(
            BarsProviderModeV1::FutuOnly
        ));
        assert!(!logical_cache_mode_allows_alpaca(
            BarsProviderModeV1::Research
        ));

        assert!(logical_cache_source_matches(
            "sip",
            "alpaca",
            "sip",
            EntitlementState::Authorized,
        ));
        assert!(logical_cache_source_matches(
            "boats",
            "alpaca",
            "boats",
            EntitlementState::Authorized,
        ));
        assert!(!logical_cache_source_matches(
            "sip",
            "alpaca",
            "boats",
            EntitlementState::Authorized,
        ));
        assert!(!logical_cache_source_matches(
            "boats",
            "alpaca",
            "sip",
            EntitlementState::Authorized,
        ));
        assert!(!logical_cache_source_matches(
            "sip",
            "futu",
            "futu",
            EntitlementState::Authorized,
        ));
        assert!(!logical_cache_source_matches(
            "sip",
            "alpaca",
            "unknown",
            EntitlementState::Unknown,
        ));
        assert!(!logical_cache_source_matches(
            "boats",
            "alpaca",
            "boats",
            EntitlementState::Unknown,
        ));
    }

    #[test]
    fn empty_alpaca_pages_bypass_cache_without_changing_other_routes() {
        use market_contracts::BarsCacheStatusV1;

        let sip = PlannedSource::Alpaca {
            feed: AlpacaFeed::Sip,
        };
        let boats = PlannedSource::Alpaca {
            feed: AlpacaFeed::Boats,
        };

        assert_eq!(
            provider_page_cache_status(sip, 0, BarsCacheStatusV1::Miss),
            BarsCacheStatusV1::Bypass
        );
        assert_eq!(
            provider_page_cache_status(boats, 0, BarsCacheStatusV1::Miss),
            BarsCacheStatusV1::Bypass
        );
        assert_eq!(
            provider_page_cache_status(sip, 1, BarsCacheStatusV1::Miss),
            BarsCacheStatusV1::Miss
        );
        assert_eq!(
            provider_page_cache_status(sip, 0, BarsCacheStatusV1::Unavailable),
            BarsCacheStatusV1::Unavailable
        );
        assert_eq!(
            provider_page_cache_status(PlannedSource::Futu, 0, BarsCacheStatusV1::Miss),
            BarsCacheStatusV1::Miss
        );
    }

    #[test]
    fn cache_receipt_timestamp_requires_authority_and_cannot_be_from_the_future() {
        use crate::archive::reconciliation::CacheAuthorityV1;

        let created_at = 1_800_000_000;
        let verified_at = verified_at_from_cache_receipt(
            CacheAuthorityV1::Authoritative,
            created_at,
            created_at + 1,
        )
        .expect("authoritative persisted receipt timestamp");
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(verified_at.as_str())
                .expect("canonical UTC timestamp")
                .timestamp(),
            i64::try_from(created_at).expect("fixture timestamp fits")
        );
        assert!(verified_at.as_str().ends_with('Z'));
        assert_eq!(
            verified_at_from_cache_receipt(
                CacheAuthorityV1::Provisional,
                created_at,
                created_at + 1,
            ),
            None
        );
        assert_eq!(
            verified_at_from_cache_receipt(
                CacheAuthorityV1::Authoritative,
                created_at + 1,
                created_at,
            ),
            None
        );
    }
}
