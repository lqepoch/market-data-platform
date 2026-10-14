//! Bounded read-only executors for planned market-bars provider groups.
//!
//! The runtime advertises only configured Alpaca adapter routes to the planner. Those
//! `SourceCapability` values mean that this process can attempt the documented historical API
//! call; they do not prove an account plan, feed entitlement, actual response feed, or historical
//! completeness. Futu transport is implemented against the existing loopback Gateway route but is
//! not planner-eligible until separately reviewed provider evidence is supplied. Yahoo remains a
//! provider-native research observation path because its adjustment semantics cannot satisfy the
//! strict Core Bars V1 adjustment contract.

mod alpaca;
mod cursor;
mod error;
mod futu;
mod projection;
mod types;
mod yfinance;

use std::{env, fmt};

use alpaca_rest_read::{AlpacaRestCredentials, AlpacaRestLimits, AlpacaRestReadClient};
use market_contracts::{BarsRequestV1, ProtoTimestampV2};
use zeroize::Zeroizing;

use crate::{
    market_query::planning::MarketBarsPlan,
    session_router::{
        AlpacaFeed, DataInterval, PlannedSource, Provider, SessionPhase, SourceCapability,
    },
};

pub use cursor::{ProviderPageSelection, planning_as_of, select_page};
pub use error::{ProviderConfigError, ProviderQueryError};
pub use types::{
    ProviderCacheIdentity, ProviderCachePolicy, ProviderPageFetch, ProviderRequestContext,
};
pub use yfinance::YFinanceResearchObservationPage;

/// Runtime clients for read-only Alpaca history, the existing Futu Gateway, and the isolated
/// provider-native Yahoo research bridge.
pub struct MarketBarsProviderRuntime {
    alpaca: Option<AlpacaRestReadClient>,
    futu: futu::FutuGateway,
    yfinance: yfinance::YFinanceBridge,
}

impl fmt::Debug for MarketBarsProviderRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketBarsProviderRuntime")
            .field("alpaca_configured", &self.alpaca.is_some())
            .field("futu_gateway", &self.futu)
            .field("yfinance_bridge", &self.yfinance)
            .finish()
    }
}

impl MarketBarsProviderRuntime {
    /// Read optional server-side provider configuration. Missing provider credentials leave that
    /// provider unavailable; a partial Alpaca pair or malformed setting fails initialization.
    ///
    /// This function does not make a network request and never enables order APIs.
    pub fn from_environment() -> Result<Self, ProviderConfigError> {
        let alpaca = alpaca_client_from_environment()?;
        let futu_port = parse_port("MDP_FUTU_GATEWAY_PORT", 8_080)?;
        let futu = futu::FutuGateway::new(futu_port)?;
        let yfinance = yfinance::YFinanceBridge::from_environment()?;
        Ok(Self {
            alpaca,
            futu,
            yfinance,
        })
    }

    /// Return planner capabilities that this process can attempt using configured credentials.
    ///
    /// The entries are static code-path coverage only. They make no claim that credentials are
    /// accepted, that an account has SIP/BOATS or Plus access, or that Basic-plan recency limits
    /// are satisfied. The response provenance therefore remains `unknown` until the provider
    /// supplies evidence. No Futu or Yahoo capability is advertised here.
    #[must_use]
    pub fn source_capabilities(&self) -> Vec<SourceCapability> {
        alpaca_source_capabilities(self.alpaca.is_some())
    }

    /// Read the pinned snapshot time from the opaque cursor, or return the caller's current time
    /// for a first page. This keeps session planning stable across pagination.
    pub fn planning_as_of(
        &self,
        request: &BarsRequestV1,
        observed_now: &ProtoTimestampV2,
    ) -> Result<ProtoTimestampV2, ProviderQueryError> {
        cursor::planning_as_of(request, observed_now)
    }

    /// Select the current provider group and expose only its safe cache identity to the handler.
    pub fn select_page(
        &self,
        request: &BarsRequestV1,
        plan: &MarketBarsPlan,
    ) -> Result<ProviderPageSelection, ProviderQueryError> {
        cursor::select_page(request, plan)
    }

    /// Fetch one provider page and retain source-observation evidence for the cache writer.
    ///
    /// The owned context is suitable for the HTTP supervisor's `move` closure. Its Gateway token
    /// is forwarded only to the fixed loopback Futu route and is redacted from `Debug` output.
    pub async fn fetch_provider_page_with_evidence(
        &self,
        plan: &MarketBarsPlan,
        selection: &ProviderPageSelection,
        context: ProviderRequestContext,
    ) -> Result<ProviderPageFetch, ProviderQueryError> {
        selection.validate_for(plan)?;
        let group = selection.provider_group();
        match group.source {
            PlannedSource::Alpaca { .. } => {
                let client = self
                    .alpaca
                    .as_ref()
                    .ok_or(ProviderQueryError::MissingCredentials)?;
                alpaca::fetch_page(client, plan, group, selection, &context).await
            }
            PlannedSource::Futu => self.futu.fetch_page(plan, group, selection, &context).await,
            PlannedSource::YFinanceDailyResearch => {
                Err(ProviderQueryError::AdjustmentSemanticsUnverified)
            }
        }
    }

    /// Fetch one strict Core Bars V1 page when cache-lineage facts are not needed by the caller.
    pub async fn fetch_provider_page(
        &self,
        plan: &MarketBarsPlan,
        selection: &ProviderPageSelection,
        context: ProviderRequestContext,
    ) -> Result<market_contracts::BarsResponseV1, ProviderQueryError> {
        self.fetch_provider_page_with_evidence(plan, selection, context)
            .await
            .map(|page| page.response)
    }

    /// Fetch isolated Yahoo daily research observations. The result intentionally does not
    /// implement `BarsResponseV1` because the provider does not confirm raw/unadjusted semantics.
    pub async fn fetch_yfinance_research_observations(
        &self,
        request: &BarsRequestV1,
        context: ProviderRequestContext,
    ) -> Result<YFinanceResearchObservationPage, ProviderQueryError> {
        self.yfinance
            .fetch_research_observations(request, &context)
            .await
    }
}

fn alpaca_source_capabilities(configured: bool) -> Vec<SourceCapability> {
    if !configured {
        return Vec::new();
    }

    let mut capabilities = Vec::with_capacity(17);
    for interval in [
        DataInterval::Daily,
        DataInterval::Minute(1),
        DataInterval::Minute(5),
        DataInterval::Minute(15),
        DataInterval::Minute(60),
    ] {
        if interval == DataInterval::Daily {
            capabilities.push(SourceCapability {
                provider: Provider::Alpaca,
                feed: Some(AlpacaFeed::Sip),
                phase: None,
                interval,
                historical: true,
                realtime: false,
            });
            continue;
        }

        for phase in [
            SessionPhase::Pre,
            SessionPhase::Regular,
            SessionPhase::After,
        ] {
            capabilities.push(SourceCapability {
                provider: Provider::Alpaca,
                feed: Some(AlpacaFeed::Sip),
                phase: Some(phase),
                interval,
                historical: true,
                realtime: false,
            });
        }
        capabilities.push(SourceCapability {
            provider: Provider::Alpaca,
            feed: Some(AlpacaFeed::Boats),
            phase: Some(SessionPhase::Over),
            interval,
            historical: true,
            realtime: false,
        });
    }
    capabilities
}

fn alpaca_client_from_environment() -> Result<Option<AlpacaRestReadClient>, ProviderConfigError> {
    let key = optional_secret("ALPACA_KEY")?;
    let secret = optional_secret("ALPACA_SECRET")?;
    let (mut key, mut secret) = match (key, secret) {
        (None, None) => return Ok(None),
        (Some(key), Some(secret)) => (key, secret),
        _ => return Err(ProviderConfigError::IncompleteAlpacaCredentials),
    };

    // Move each value directly into the connector's own zeroizing credential wrapper. Leaving an
    // empty string in these environment wrappers avoids creating a second plain-text copy.
    let key = std::mem::take(&mut *key);
    let secret = std::mem::take(&mut *secret);
    let credentials =
        AlpacaRestCredentials::new(key, secret).map_err(|_| ProviderConfigError::InvalidValue)?;
    AlpacaRestReadClient::new(credentials, AlpacaRestLimits::default())
        .map(Some)
        .map_err(|_| ProviderConfigError::ClientInitialization)
}

fn optional_secret(name: &str) -> Result<Option<Zeroizing<String>>, ProviderConfigError> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(Some(Zeroizing::new(value))),
        Ok(_) => Err(ProviderConfigError::InvalidValue),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(ProviderConfigError::InvalidValue),
    }
}

pub(super) fn parse_port(name: &str, default: u16) -> Result<u16, ProviderConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u16>()
            .ok()
            .filter(|port| *port > 0)
            .ok_or(ProviderConfigError::InvalidValue),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ProviderConfigError::InvalidValue),
    }
}

#[cfg(test)]
mod tests {
    use crate::session_router::{AlpacaFeed, DataInterval, Provider, SessionPhase};

    use super::alpaca_source_capabilities;

    #[test]
    fn advertises_only_configured_alpaca_code_paths_and_never_entitlement() {
        assert!(alpaca_source_capabilities(false).is_empty());
        let capabilities = alpaca_source_capabilities(true);
        assert_eq!(capabilities.len(), 17);
        assert!(capabilities.iter().all(|capability| {
            capability.provider == Provider::Alpaca && capability.historical && !capability.realtime
        }));
        assert!(capabilities.iter().any(|capability| {
            capability.feed == Some(AlpacaFeed::Sip)
                && capability.phase == Some(SessionPhase::Regular)
                && capability.interval == DataInterval::Minute(15)
        }));
        assert!(capabilities.iter().any(|capability| {
            capability.feed == Some(AlpacaFeed::Boats)
                && capability.phase == Some(SessionPhase::Over)
                && capability.interval == DataInterval::Minute(1)
        }));
        assert!(!capabilities.iter().any(|capability| {
            capability.feed == Some(AlpacaFeed::Boats)
                && capability.phase != Some(SessionPhase::Over)
        }));
    }
}
