//! Alpaca's exact-query cache path. The Broker witness establishes one successful SDK query;
//! complete source authority is derived here from the entire exhausted page chain and the
//! canonical row timeline. A separate short-lived process-local lease gates later cache reads.

use crate::cancellation::CancellationToken;

use super::{
    CacheAuthorityBasisV1, CacheBarsResponsesInputV1, CachePublishContextV1, CachePublishResultV1,
    CacheVersionStoreV1,
};
use crate::archive::reconciliation::authority::ReconciliationPolicyV1;
use crate::archive::reconciliation::{
    CacheKeyV1, CacheReconciliationError, Result,
    alpaca_query_access::{derive_complete_coverage_from_timeline, validate_query_access_pages},
};

const MAX_QUERY_ACCESS_LEASE_SECONDS: u64 = 60;
const MAX_QUERY_ACCESS_LEASES: usize = 4_096;

impl CacheVersionStoreV1 {
    /// Persist a bounded SDK-observation chain and canonical row set after rechecking the opaque
    /// Broker witness. Entitlement stays Unknown. Coverage is promoted only when exact response
    /// rows tile every trusted calendar phase with no gaps and the SDK chain is exhausted.
    ///
    /// Successful publication creates a same-process lease for the exact cache key/current
    /// version. The witness itself remains a local observation receipt and is never sufficient
    /// for cold-start cache reads or for asserting a Plus/license entitlement.
    pub(crate) fn publish_bars_responses_with_alpaca_query_access(
        &self,
        input: CacheBarsResponsesInputV1<'_>,
        now_unix_seconds: u64,
        policy: &ReconciliationPolicyV1,
    ) -> Result<CachePublishResultV1> {
        policy.validate()?;
        input
            .raw_observations
            .validate()
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        input
            .request
            .validate_response(&input.raw_observations)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        input
            .request
            .validate_response(&input.canonical_response)
            .map_err(|_| CacheReconciliationError::InvalidInput)?;
        let validation = validate_query_access_pages(
            input.request,
            input.source_scope,
            &input.raw_observations,
            &input.canonical_response,
            &input.page_evidence,
            now_unix_seconds,
        )?;
        let key = CacheKeyV1::from_request(input.request, input.source_scope)?;
        let key_fingerprint = key.fingerprint()?;
        let phase_slices = input.phase_slices;
        let mut canonical_response = input.canonical_response;
        derive_complete_coverage_from_timeline(
            input.request,
            input.source_scope,
            &input.raw_observations,
            &mut canonical_response,
            phase_slices.as_deref(),
            validation.full_provider_chain,
        )?;
        let CacheBarsResponsesInputV1 {
            request,
            source_scope,
            raw_observations,
            page_evidence,
            ..
        } = input;
        let input = CacheBarsResponsesInputV1 {
            request,
            source_scope,
            raw_observations,
            canonical_response,
            page_evidence,
            phase_slices: phase_slices.clone(),
        };
        let result = self.publish_bars_responses_with_basis(
            input,
            CachePublishContextV1 {
                now_unix_seconds,
                policy,
                authority_basis: CacheAuthorityBasisV1::AlpacaQueryAccessObserved,
                admission: None,
                phase_slices: phase_slices.as_deref(),
                alpaca_query_access_receipts: Some(&validation.receipts),
                alpaca_full_provider_chain: validation.full_provider_chain,
            },
        )?;
        if result
            .current
            .as_ref()
            .is_some_and(|current| current.version_id == result.published_version_id)
        {
            self.grant_alpaca_query_access_lease(
                key_fingerprint,
                result.published_version_id.clone(),
                now_unix_seconds,
                validation
                    .receipts
                    .last()
                    .ok_or(CacheReconciliationError::AdmissionMismatch)?
                    .response_observed_at_utc
                    .as_str(),
            )?;
        }
        Ok(result)
    }

    /// Supervisor-friendly wrapper for the synchronous bounded finalizer. Once atomic cache
    /// publication starts it finishes and is joined; cancellation after that point suppresses
    /// the response and forces the caller to reload rather than retry blindly.
    pub(crate) fn publish_bars_responses_with_alpaca_query_access_cancellable(
        &self,
        input: CacheBarsResponsesInputV1<'_>,
        now_unix_seconds: u64,
        policy: &ReconciliationPolicyV1,
        cancellation: &CancellationToken,
    ) -> Result<CachePublishResultV1> {
        if cancellation.is_cancelled() {
            return Err(CacheReconciliationError::Cancelled);
        }
        let result =
            self.publish_bars_responses_with_alpaca_query_access(input, now_unix_seconds, policy)?;
        if cancellation.is_cancelled() {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(result)
    }

    fn grant_alpaca_query_access_lease(
        &self,
        key_fingerprint: String,
        version_id: String,
        now_unix_seconds: u64,
        observed_at_utc: &str,
    ) -> Result<()> {
        let observed_seconds = chrono::DateTime::parse_from_rfc3339(observed_at_utc)
            .map_err(|_| CacheReconciliationError::AdmissionMismatch)?
            .timestamp();
        let lease_limit = now_unix_seconds
            .checked_add(MAX_QUERY_ACCESS_LEASE_SECONDS.min(self.limits.ttl.as_secs()))
            .ok_or(CacheReconciliationError::LimitExceeded)?;
        let observation_limit = u64::try_from(observed_seconds)
            .ok()
            .and_then(|seconds| seconds.checked_add(MAX_QUERY_ACCESS_LEASE_SECONDS))
            .ok_or(CacheReconciliationError::AdmissionExpired)?;
        let expires_at = lease_limit.min(observation_limit);
        if expires_at <= now_unix_seconds {
            return Ok(());
        }
        let mut leases = self
            .alpaca_query_access_leases
            .lock()
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        leases.retain(|_, expiry| *expiry > now_unix_seconds);
        let lease_identity = (key_fingerprint, version_id);
        if !leases.contains_key(&lease_identity) && leases.len() >= MAX_QUERY_ACCESS_LEASES {
            // Keep active leases intact. A saturated lease table creates a cache miss for this
            // key, while the freshly fetched response can still be returned to the caller.
            return Ok(());
        }
        leases.insert(lease_identity, expires_at);
        Ok(())
    }

    pub(super) fn require_alpaca_query_access_lease(
        &self,
        key_fingerprint: &str,
        version_id: &str,
        now_unix_seconds: u64,
    ) -> Result<()> {
        let leases = self
            .alpaca_query_access_leases
            .lock()
            .map_err(|_| CacheReconciliationError::UnknownOutcome)?;
        match leases.get(&(key_fingerprint.to_owned(), version_id.to_owned())) {
            Some(expires_at) if *expires_at > now_unix_seconds => Ok(()),
            _ => Err(CacheReconciliationError::Expired),
        }
    }
}

#[cfg(test)]
mod tests;
