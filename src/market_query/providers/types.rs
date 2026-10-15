use std::fmt;

use alpaca_rest_read::AlpacaStockBarsQueryAccessWitness;
use market_contracts::{BarsResponseV1, ProtoTimestampV2};
use zeroize::Zeroizing;

use crate::archive::reconciliation::{AlpacaStockBarsQueryWitnessV1, ProviderPageEvidenceV1};

use super::error::ProviderQueryError;

/// Request-scoped deadline and optional, already-issued Gateway delegation.
///
/// The delegation is forwarded only to the fixed loopback Futu Gateway route. It is never used
/// for MDP authentication, provider selection, cursor hashing, or cache identity.
pub struct ProviderRequestContext {
    deadline: tokio::time::Instant,
    gateway_delegation: Option<Zeroizing<String>>,
}

impl ProviderRequestContext {
    /// Build an owned context suitable for a bounded `move` closure.
    #[must_use]
    pub fn new(deadline: tokio::time::Instant, gateway_delegation: Option<String>) -> Self {
        Self {
            deadline,
            gateway_delegation: gateway_delegation.map(Zeroizing::new),
        }
    }

    pub(crate) const fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    pub(crate) fn gateway_delegation(&self) -> Option<&str> {
        self.gateway_delegation.as_deref().map(String::as_str)
    }

    pub(crate) fn remaining(&self) -> Result<std::time::Duration, ProviderQueryError> {
        self.deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(ProviderQueryError::Timeout)
    }
}

impl fmt::Debug for ProviderRequestContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderRequestContext")
            .field("deadline", &self.deadline)
            .field("has_gateway_delegation", &self.gateway_delegation.is_some())
            .finish()
    }
}

/// One Core-valid provider page together with source-observation lineage facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderPageFetch {
    /// The raw provider observation projected into the frozen Core Bars V1 envelope.
    pub response: BarsResponseV1,
    /// Facts observed while reading the provider page; sequence fields remain absent for history.
    pub(crate) evidence: ProviderPageEvidenceV1,
}

impl ProviderPageFetch {
    pub(crate) fn new(
        response: BarsResponseV1,
        source_observation_sha256: Option<String>,
        response_observed_at_utc: Option<ProtoTimestampV2>,
        pages_seen: Option<u32>,
        pages_exhausted: Option<bool>,
        provider_cursor_cache_input: Option<
            crate::archive::reconciliation::ProviderCursorCacheInputV1,
        >,
    ) -> Result<Self, ProviderQueryError> {
        let truncated = response.truncated;
        if response.validate().is_err() {
            return Err(ProviderQueryError::InvalidCorePage);
        }
        Ok(Self {
            response,
            evidence: ProviderPageEvidenceV1 {
                source_observation_sha256,
                response_observed_at_utc,
                pages_seen,
                pages_exhausted,
                first_source_sequence: None,
                last_source_sequence: None,
                sequence_gap_count: None,
                truncated,
                provider_cursor_cache_input,
                alpaca_query_access_pages: Vec::new(),
            },
        })
    }

    pub(crate) fn with_alpaca_query_access(
        mut self,
        witness: &AlpacaStockBarsQueryAccessWitness,
        expected_request_cursor_sha256: Option<&str>,
    ) -> Result<Self, ProviderQueryError> {
        let evidence = AlpacaStockBarsQueryWitnessV1::from_broker_witness(
            witness,
            expected_request_cursor_sha256,
            &self.response.rows,
        )
        .map_err(|_| ProviderQueryError::MalformedResponse)?;
        self.evidence.alpaca_query_access_pages.push(evidence);
        Ok(self)
    }
}

/// Provider cache identity derived from the selected implementation route.
///
/// It contains no account, credential, entitlement, or principal identity. The HTTP owner must
/// combine it with its operator cache scope and verified principal partition before cache access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCacheIdentity {
    pub provider: String,
    /// Selected route partition. Alpaca uses the requested SIP/BOATS selector here; page
    /// provenance remains `unknown` unless a matching same-page SDK witness supports that route.
    pub feed: String,
    /// Stable adapter/route identity; not an account or entitlement identity.
    pub source_scope_id: String,
    pub requested_feed: Option<String>,
    pub entitlement_status: market_contracts::BarsEntitlementStatusV1,
    pub numeric_encoding: market_contracts::NumericEncodingV1,
    pub effective_resolution: market_contracts::BarResolutionV1,
    pub session_instance_ids: Vec<String>,
    pub market_calendar_version: Option<String>,
}

/// Baseline cache policy for a provider route before operator/principal scoping is applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCachePolicy {
    /// Eligible for provisional observation caching when the HTTP owner supplies explicit scope.
    ProvisionalOnly,
    /// Do not cache as Core bars because the returned adjustment semantics are unknown.
    BypassAdjustmentSemanticsUnverified,
}
