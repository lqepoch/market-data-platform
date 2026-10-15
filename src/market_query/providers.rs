//! Bounded read-only executors for planned market-bars provider groups.
//!
//! The runtime advertises only configured adapter routes that its backend policy permits as
//! attempts. These `SourceCapability` values do not prove an account plan, feed entitlement, actual
//! response feed, realtime transport, or historical completeness. Futu is exposed only when its
//! loopback history Gateway port is explicitly configured and only for permitted NEW/BUFFER
//! slices; Yahoo remains provider-native research because its adjustment semantics cannot satisfy
//! the strict Core Bars V1 contract. Outer pagination cursors use a runtime-owned HMAC key; the
//! HTTP adapter supplies a rights-profile digest derived from its verified principal.

mod admission;
mod alpaca;
mod cursor;
mod error;
mod futu;
mod projection;
mod provider_policy;
pub(crate) mod research_cursor;
mod types;
mod yfinance;

use std::{env, fmt};

use alpaca_rest_read::{AlpacaRestCredentials, AlpacaRestLimits, AlpacaRestReadClient};
use market_contracts::{BarsRequestV1, ProtoTimestampV2};
use zeroize::Zeroizing;

use crate::{
    archive::reconciliation::authority::VerifiedProviderAdmissionV1,
    market_query::planning::MarketBarsPlan,
    session_router::{
        AlpacaFeed, DataInterval, PlannedSource, Provider, SessionPhase, SourceCapability,
        TrustedCalendarSnapshot,
    },
};

use self::provider_policy::ProviderAttemptPolicy;

pub use cursor::ProviderPageSelection;
pub use error::{ProviderConfigError, ProviderQueryError};
pub use types::{
    ProviderCacheIdentity, ProviderCachePolicy, ProviderPageFetch, ProviderRequestContext,
};
pub use yfinance::{
    YFinanceResearchAdjustment, YFinanceResearchObservationPage, YFinanceResearchWindowRequest,
};

/// Independent page selection for the provider-native Yahoo research endpoint.
pub(crate) struct YFinanceResearchPageSelection {
    pub(crate) window: research_cursor::ResearchWindow,
    pub(crate) page_index: u32,
    request_sha256: String,
}

/// Isolated Yahoo research runtime. This does not initialize Alpaca/Futu, Core bars, or cache
/// authority; a configured bridge remains a provisional display-only observation source.
pub(crate) struct YFinanceResearchRuntime {
    bridge: yfinance::YFinanceBridge,
    cursor_signer: cursor::CursorSigner,
}

impl fmt::Debug for YFinanceResearchRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("YFinanceResearchRuntime")
            .field("bridge", &self.bridge)
            .field("cursor_signer", &"[REDACTED]")
            .finish()
    }
}

impl YFinanceResearchRuntime {
    /// Build the independent endpoint only when its explicit default-off feature flag is true.
    pub(crate) fn from_environment() -> Result<Option<Self>, ProviderConfigError> {
        let enabled = match env::var("MDP_YFINANCE_RESEARCH_API_ENABLED") {
            Ok(value) => parse_research_api_enabled(Some(&value))?,
            Err(env::VarError::NotPresent) => parse_research_api_enabled(None)?,
            Err(env::VarError::NotUnicode(_)) => return Err(ProviderConfigError::InvalidValue),
        };
        if !enabled {
            return Ok(None);
        }
        Ok(Some(Self {
            bridge: yfinance::YFinanceBridge::from_environment()?,
            cursor_signer: cursor::CursorSigner::from_environment()?,
        }))
    }

    pub(crate) fn select_page(
        &self,
        request: &YFinanceResearchWindowRequest,
        cursor: Option<&str>,
        principal_scope_sha256: &str,
        now_unix_seconds: u64,
    ) -> Result<YFinanceResearchPageSelection, ProviderQueryError> {
        let request_sha256 = research_cursor::request_sha256(
            &request.symbols,
            request.asset_class,
            request.start_utc,
            request.end_utc,
            request.adjustment,
        );
        let (window, page_index) = research_cursor::select_window(
            &self.cursor_signer,
            cursor,
            &request_sha256,
            principal_scope_sha256,
            request.start_utc,
            request.end_utc,
            now_unix_seconds,
        )?;
        Ok(YFinanceResearchPageSelection {
            window,
            page_index,
            request_sha256,
        })
    }

    pub(crate) fn cursor_for_previous_page(
        &self,
        selection: &YFinanceResearchPageSelection,
        full_start_utc: chrono::DateTime<chrono::Utc>,
        principal_scope_sha256: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<String>, ProviderQueryError> {
        let Some(window) = research_cursor::previous_page(selection.window, full_start_utc) else {
            return Ok(None);
        };
        let page_index = selection
            .page_index
            .checked_add(1)
            .ok_or(ProviderQueryError::InvalidCursor)?;
        research_cursor::issue_cursor(
            &self.cursor_signer,
            &selection.request_sha256,
            principal_scope_sha256,
            window,
            page_index,
            now_unix_seconds,
        )
        .map(Some)
    }

    pub(crate) async fn fetch_window(
        &self,
        request: &YFinanceResearchWindowRequest,
        context: &ProviderRequestContext,
    ) -> Result<YFinanceResearchObservationPage, ProviderQueryError> {
        self.bridge.fetch_research_window(request, context).await
    }
}

fn parse_research_api_enabled(value: Option<&str>) -> Result<bool, ProviderConfigError> {
    match value {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(ProviderConfigError::InvalidValue),
    }
}

/// Runtime clients for read-only Alpaca history, the existing Futu Gateway, and the isolated
/// provider-native Yahoo research bridge.
pub struct MarketBarsProviderRuntime {
    alpaca: Option<AlpacaRestReadClient>,
    futu: Option<futu::FutuGateway>,
    yfinance: yfinance::YFinanceBridge,
    futu_admissions: Vec<VerifiedProviderAdmissionV1>,
    provider_attempt_policy: ProviderAttemptPolicy,
    cursor_signer: cursor::CursorSigner,
}

impl fmt::Debug for MarketBarsProviderRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketBarsProviderRuntime")
            .field("alpaca_configured", &self.alpaca.is_some())
            .field("futu_gateway_configured", &self.futu.is_some())
            .field("yfinance_bridge", &self.yfinance)
            .field("futu_admission_count", &self.futu_admissions.len())
            .finish()
    }
}

impl MarketBarsProviderRuntime {
    /// Read optional server-side provider configuration. Missing provider credentials leave that
    /// provider unavailable; a partial Alpaca pair or malformed setting fails initialization.
    /// The cursor signing key persists only when `MDP_MARKET_CURSOR_SIGNING_KEY` is set;
    /// otherwise a process-local CSPRNG key invalidates outstanding cursors on restart.
    ///
    /// This function does not make a network request and never enables order APIs.
    pub fn from_environment() -> Result<Self, ProviderConfigError> {
        let cursor_signer = cursor::CursorSigner::from_environment()?;
        let alpaca = alpaca_client_from_environment()?;
        let futu = optional_port("MDP_FUTU_GATEWAY_PORT")?
            .map(futu::FutuGateway::new)
            .transpose()?;
        let provider_attempt_policy =
            ProviderAttemptPolicy::from_futu_gateway_configuration(futu.is_some());
        let yfinance = yfinance::YFinanceBridge::from_environment()?;
        let runtime = Self {
            alpaca,
            futu,
            yfinance,
            futu_admissions: Vec::new(),
            provider_attempt_policy,
            cursor_signer,
        };
        Ok(runtime.with_verified_provider_admissions(Vec::new()))
    }

    /// Attach receipts already verified by the independent A04 trust boundary.
    ///
    /// This internal injection point does not parse receipts, trust environment flags, or create
    /// admissions. These receipts affect verified cache authority only; the owner policy separately
    /// controls whether a configured Futu history adapter may be attempted provisionally.
    pub(crate) fn with_verified_provider_admissions(
        mut self,
        admissions: Vec<VerifiedProviderAdmissionV1>,
    ) -> Self {
        self.futu_admissions = admissions;
        self
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

    /// Whether the explicit loopback Futu history path is configured for provisional attempts.
    /// This does not describe reachability, provider rights, realtime access, or planner coverage.
    pub(crate) const fn futu_history_attempt_configured(&self) -> bool {
        self.provider_attempt_policy
            .futu_history_attempts_configured()
    }

    /// Return request-scoped route-attempt capabilities for the exact query and trusted calendar.
    /// The policy is server-owned and does not use request fields to grant provider authority.
    /// Futu capabilities represent only bounded historical endpoint attempts; they never claim
    /// realtime transport, actual response feed, entitlement, completeness, or archive authority.
    #[must_use]
    pub fn source_capabilities_for_request(
        &self,
        request: &BarsRequestV1,
        as_of: &ProtoTimestampV2,
        calendar: &TrustedCalendarSnapshot,
    ) -> Vec<SourceCapability> {
        admission::source_capabilities_for_attempt(
            request,
            as_of,
            calendar,
            self.source_capabilities(),
            self.provider_attempt_policy,
        )
    }

    /// Read the pinned snapshot time from a signed cursor, or return the caller's current time for
    /// a first page. This keeps planning stable while binding continuations to the verified
    /// principal rights profile supplied by the authenticated HTTP adapter.
    pub fn planning_as_of(
        &self,
        request: &BarsRequestV1,
        observed_now: &ProtoTimestampV2,
        rights_profile_sha256: &str,
    ) -> Result<ProtoTimestampV2, ProviderQueryError> {
        cursor::planning_as_of(
            request,
            observed_now,
            &self.cursor_signer,
            rights_profile_sha256,
        )
    }

    /// Select a provider run and bind its cursor to the verified principal rights profile.
    /// `rights_profile_sha256` must come from the authenticated HTTP adapter, not request input.
    /// Valid A04 receipts bind cursor continuity, while the server-owned attempt policy permits
    /// configured NEW/BUFFER Futu history attempts without receipts; neither path grants cache
    /// promotion authority.
    pub fn select_page_at(
        &self,
        request: &BarsRequestV1,
        plan: &MarketBarsPlan,
        rights_profile_sha256: &str,
        now_unix_seconds: u64,
    ) -> Result<ProviderPageSelection, ProviderQueryError> {
        let admissions = admission::validate_plan_admissions_for_attempt(
            request,
            plan,
            rights_profile_sha256,
            &self.futu_admissions,
            now_unix_seconds,
            self.provider_attempt_policy,
        )?;
        cursor::select_page_with_admissions(
            request,
            plan,
            admissions,
            rights_profile_sha256,
            &self.cursor_signer,
            now_unix_seconds,
        )
    }

    /// Select a synthetic provider run in unit tests without manufacturing an admission receipt.
    /// This test-only path uses the runtime's real signer and cursor rules but must never be used
    /// as authorization to perform provider I/O.
    #[cfg(test)]
    pub(crate) fn select_page_at_synthetic_for_test(
        &self,
        request: &BarsRequestV1,
        plan: &MarketBarsPlan,
        rights_profile_sha256: &str,
        now_unix_seconds: u64,
    ) -> Result<ProviderPageSelection, ProviderQueryError> {
        cursor::select_page_at_synthetic_for_test(
            request,
            plan,
            rights_profile_sha256,
            &self.cursor_signer,
            now_unix_seconds,
        )
    }

    /// Compatibility name for callers that distinguish request admission from clock selection.
    pub fn select_page_for_request(
        &self,
        request: &BarsRequestV1,
        plan: &MarketBarsPlan,
        rights_profile_sha256: &str,
        now_unix_seconds: u64,
    ) -> Result<ProviderPageSelection, ProviderQueryError> {
        self.select_page_at(request, plan, rights_profile_sha256, now_unix_seconds)
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
            PlannedSource::Futu => {
                if !self.provider_attempt_policy.permits_futu_plan(plan) {
                    return Err(ProviderQueryError::ProviderCapabilityUnavailable);
                }
                let futu = self
                    .futu
                    .as_ref()
                    .ok_or(ProviderQueryError::ProviderCapabilityUnavailable)?;
                futu.fetch_page(plan, group, selection, &context).await
            }
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

    /// Construct adapter clients for selection-only tests without making network requests.
    #[cfg(test)]
    pub(crate) fn configured_sources_for_selection_test() -> Self {
        let credentials = AlpacaRestCredentials::new("synthetic-test-key", "synthetic-test-secret")
            .expect("fixed test-only credentials satisfy the client configuration");
        let alpaca = AlpacaRestReadClient::new(credentials, AlpacaRestLimits::default())
            .expect("fixed test-only Alpaca client configuration");
        Self {
            alpaca: Some(alpaca),
            futu: Some(futu::FutuGateway::new(1).expect("nonzero test-only loopback port")),
            yfinance: yfinance::YFinanceBridge::disabled_for_test()
                .expect("fixed disabled research bridge configuration"),
            futu_admissions: Vec::new(),
            provider_attempt_policy: ProviderAttemptPolicy::from_futu_gateway_configuration(true),
            cursor_signer: cursor::CursorSigner::for_test(),
        }
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

fn optional_port(name: &str) -> Result<Option<u16>, ProviderConfigError> {
    match env::var(name) {
        Ok(value) => parse_optional_port_value(Some(&value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(ProviderConfigError::InvalidValue),
    }
}

fn parse_optional_port_value(value: Option<&str>) -> Result<Option<u16>, ProviderConfigError> {
    value
        .map(|value| {
            value
                .parse::<u16>()
                .ok()
                .filter(|port| *port > 0)
                .ok_or(ProviderConfigError::InvalidValue)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use crate::session_router::{AlpacaFeed, DataInterval, Provider, SessionPhase};

    use super::{
        ProviderConfigError, alpaca_source_capabilities, parse_optional_port_value,
        parse_research_api_enabled,
    };

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

    #[test]
    fn futu_history_attempt_requires_an_explicit_valid_gateway_port() {
        assert_eq!(parse_optional_port_value(None), Ok(None));
        assert_eq!(parse_optional_port_value(Some("8080")), Ok(Some(8_080)));
        assert_eq!(
            parse_optional_port_value(Some("0")),
            Err(ProviderConfigError::InvalidValue)
        );
        assert_eq!(
            parse_optional_port_value(Some("not-a-port")),
            Err(ProviderConfigError::InvalidValue)
        );
    }

    #[test]
    fn yahoo_research_api_feature_is_default_off_and_strictly_boolean() {
        assert_eq!(parse_research_api_enabled(None), Ok(false));
        assert_eq!(parse_research_api_enabled(Some("false")), Ok(false));
        assert_eq!(parse_research_api_enabled(Some("true")), Ok(true));
        assert_eq!(
            parse_research_api_enabled(Some("yes")),
            Err(ProviderConfigError::InvalidValue)
        );
    }
}
