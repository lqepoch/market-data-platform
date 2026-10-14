use std::collections::BTreeSet;

use chrono::SecondsFormat;
use market_contracts::{
    AdjustmentV1, AssetClassV1, BarsRequestV1, EntitlementState, NumericEncodingV1,
    ProtoTimestampV2,
};
use sha2::{Digest, Sha256};

use crate::{
    archive::reconciliation::{ProviderPhaseSliceV1, VerifiedProviderAdmissionV1},
    market_query::planning::{MarketBarsPlan, plan_market_bars},
    session_router::{
        DataInterval, PlannedSlice, PlannedSource, Provider, SessionPhase, SourceCapability,
        TrustedCalendarSnapshot,
    },
};

use super::error::ProviderQueryError;

const FUTU_SOURCE_SCOPE_ID: &str = "futu-gateway-history-v1";

/// Opaque A04 receipts that were matched to a precise query and calendar scope.
pub(super) struct ValidatedFutuAdmissions {
    pub(super) admissions: Vec<VerifiedProviderAdmissionV1>,
    pub(super) rights_profile_sha256: String,
    pub(super) binding_sha256: Option<String>,
    covered_session_instance_ids: BTreeSet<String>,
}

impl ValidatedFutuAdmissions {
    fn empty(rights_profile_sha256: &str) -> Self {
        Self {
            admissions: Vec::new(),
            rights_profile_sha256: rights_profile_sha256.to_owned(),
            binding_sha256: None,
            covered_session_instance_ids: BTreeSet::new(),
        }
    }

    pub(super) fn covers_run(&self, run: &crate::market_query::planning::ProviderRun) -> bool {
        run.group.slices_recent_to_old.iter().all(|slice| {
            slice
                .session_instance_id
                .as_ref()
                .is_some_and(|id| self.covered_session_instance_ids.contains(id))
        })
    }

    fn covers_slices(&self, slices: &[&PlannedSlice]) -> bool {
        !slices.is_empty()
            && slices.iter().all(|slice| {
                slice
                    .session_instance_id
                    .as_ref()
                    .is_some_and(|id| self.covered_session_instance_ids.contains(id))
            })
    }
}

/// Keep generic routing closed. Futu capabilities are added only when opaque A04 receipts cover
/// every Futu-routed phase in the exact request/calendar snapshot.
pub(super) fn source_capabilities_for_request(
    request: &BarsRequestV1,
    as_of: &ProtoTimestampV2,
    calendar: &TrustedCalendarSnapshot,
    rights_profile_sha256: &str,
    now_unix_seconds: u64,
    alpaca_capabilities: Vec<SourceCapability>,
    admissions: &[VerifiedProviderAdmissionV1],
) -> Vec<SourceCapability> {
    if !request_supports_futu(request)
        || admissions.is_empty()
        || !valid_sha256(rights_profile_sha256)
    {
        return alpaca_capabilities;
    }

    let candidate_capabilities = futu_probe_capabilities();
    let Ok(plan) = plan_market_bars(request, as_of, &candidate_capabilities, calendar) else {
        return alpaca_capabilities;
    };
    let validated = evaluate_admissions(
        request,
        &plan,
        rights_profile_sha256,
        admissions,
        now_unix_seconds,
    );

    let mut capabilities = alpaca_capabilities;
    for capability in candidate_capabilities {
        let slices = plan
            .slices_recent_to_old
            .iter()
            .filter(|slice| {
                slice.source == Some(PlannedSource::Futu)
                    && slice.phase == capability.phase
                    && slice.interval == capability.interval
            })
            .collect::<Vec<_>>();
        if validated.covers_slices(&slices) {
            capabilities.push(capability);
        }
    }
    capabilities
}

/// Bind selected Futu slices to trusted, query-scoped receipts. An empty Futu slice set is valid
/// for Alpaca-only/research plans and returns an empty admission set.
pub(super) fn validate_plan_admissions(
    request: &BarsRequestV1,
    plan: &MarketBarsPlan,
    rights_profile_sha256: &str,
    admissions: &[VerifiedProviderAdmissionV1],
    now_unix_seconds: u64,
) -> Result<ValidatedFutuAdmissions, ProviderQueryError> {
    let validated = evaluate_admissions(
        request,
        plan,
        rights_profile_sha256,
        admissions,
        now_unix_seconds,
    );
    let futu_slices = plan
        .slices_recent_to_old
        .iter()
        .filter(|slice| slice.source == Some(PlannedSource::Futu))
        .collect::<Vec<_>>();
    if futu_slices.is_empty() || validated.covers_slices(&futu_slices) {
        Ok(validated)
    } else {
        Err(ProviderQueryError::ProviderCapabilityUnavailable)
    }
}

fn evaluate_admissions(
    request: &BarsRequestV1,
    plan: &MarketBarsPlan,
    rights_profile_sha256: &str,
    admissions: &[VerifiedProviderAdmissionV1],
    now_unix_seconds: u64,
) -> ValidatedFutuAdmissions {
    let mut validated = ValidatedFutuAdmissions::empty(rights_profile_sha256);
    if !request_supports_futu(request)
        || !valid_sha256(rights_profile_sha256)
        || &plan.request != request
    {
        return validated;
    }

    let futu_slices = plan
        .slices_recent_to_old
        .iter()
        .filter(|slice| slice.source == Some(PlannedSource::Futu))
        .collect::<Vec<_>>();
    if futu_slices.is_empty() {
        return validated;
    }
    let available_ids = futu_slices
        .iter()
        .filter_map(|slice| slice.session_instance_id.as_deref())
        .collect::<BTreeSet<_>>();
    if available_ids.len() != futu_slices.len() {
        return validated;
    }

    let mut admitted_receipts = BTreeSet::new();
    let mut common_scope: Option<ScopeIdentity> = None;
    for admission in admissions {
        let scope = admission.source_scope();
        if !admission_matches_request(
            admission,
            scope,
            request,
            plan,
            rights_profile_sha256,
            now_unix_seconds,
        ) || scope.session_instance_ids.is_empty()
        {
            continue;
        }
        let scope_ids = scope
            .session_instance_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if scope_ids.len() != scope.session_instance_ids.len()
            || !scope_ids.is_subset(&available_ids)
        {
            continue;
        }
        let selected_slices = futu_slices
            .iter()
            .copied()
            .filter(|slice| {
                slice
                    .session_instance_id
                    .as_deref()
                    .is_some_and(|id| scope_ids.contains(id))
            })
            .collect::<Vec<_>>();
        let selected_ids = selected_slices
            .iter()
            .filter_map(|slice| slice.session_instance_id.as_deref())
            .collect::<BTreeSet<_>>();
        if selected_ids != scope_ids {
            continue;
        }
        let Some(phase_slices) = phase_slices_for(&selected_slices) else {
            continue;
        };
        if admission
            .validate_for_query(request, scope, &phase_slices, now_unix_seconds)
            .is_err()
        {
            continue;
        }
        let receipt_sha256 = admission.receipt_sha256();
        if admitted_receipts.contains(receipt_sha256) {
            continue;
        }
        let identity = ScopeIdentity::from_admission(scope, admission);
        if common_scope
            .as_ref()
            .is_some_and(|current| current != &identity)
        {
            continue;
        }
        common_scope.get_or_insert(identity);
        admitted_receipts.insert(receipt_sha256.to_owned());
        validated
            .covered_session_instance_ids
            .extend(scope.session_instance_ids.iter().cloned());
        validated.admissions.push(admission.clone());
    }

    if !validated.admissions.is_empty() {
        let mut receipt_hashes = validated
            .admissions
            .iter()
            .map(|admission| admission.receipt_sha256().to_owned())
            .collect::<Vec<_>>();
        receipt_hashes.sort();
        receipt_hashes.dedup();
        validated.binding_sha256 = Some(digest_strings(&receipt_hashes));
    }
    validated
}

fn admission_matches_request(
    admission: &VerifiedProviderAdmissionV1,
    scope: &crate::archive::reconciliation::CacheQuerySourceScopeV1,
    request: &BarsRequestV1,
    plan: &MarketBarsPlan,
    rights_profile_sha256: &str,
    now_unix_seconds: u64,
) -> bool {
    scope.provider == "futu"
        && scope.feed == "futu"
        && scope.source_scope_id == FUTU_SOURCE_SCOPE_ID
        && scope.entitlement == EntitlementState::Authorized
        && scope.rights_profile_sha256 == rights_profile_sha256
        && admission.entitlement_scope_sha256() == scope.entitlement_scope_sha256
        && scope.numeric_encoding == NumericEncodingV1::BinaryFloat64ShortestDecimal
        && scope.effective_resolution == plan.effective_resolution
        && scope.market_calendar_version.as_deref() == Some(plan.calendar_version.as_str())
        && admission.provider() == "futu"
        && admission.feed() == "futu"
        && admission.entitlement() == EntitlementState::Authorized
        && admission.rights_profile_sha256() == rights_profile_sha256
        && admission.symbol() == request.symbol.as_str()
        && admission.asset_class() == request.asset_class
        && admission.requested_resolution() == request.resolution
        && admission.resolution() == plan.effective_resolution
        && admission.adjustment() == request.adjustment
        && admission.session_scope() == request.session_scope
        && admission.session_instance_ids() == scope.session_instance_ids.as_slice()
        && admission.numeric_encoding() == "binary_float64_shortest_decimal"
        && admission.window_start_utc() == request.start.as_str()
        && admission.window_end_exclusive_utc() == request.end.as_str()
        && admission.market_calendar_version() == Some(plan.calendar_version.as_str())
        && valid_sha256(admission.authorization_context_sha256())
        && !admission.license_scope_id().is_empty()
        && valid_sha256(admission.license_terms_sha256())
        && !admission.sdk_name().is_empty()
        && !admission.sdk_version().is_empty()
        && !admission.gateway_version().is_empty()
        && admission.is_valid_at(now_unix_seconds)
}

fn request_supports_futu(request: &BarsRequestV1) -> bool {
    request.asset_class == AssetClassV1::Stock
        && request.adjustment == AdjustmentV1::Unadjusted
        && matches!(
            request.provider_mode,
            market_contracts::BarsProviderModeV1::Auto
                | market_contracts::BarsProviderModeV1::FutuOnly
        )
}

fn phase_slices_for(slices: &[&PlannedSlice]) -> Option<Vec<ProviderPhaseSliceV1>> {
    let mut phases = slices
        .iter()
        .map(|slice| {
            Some(ProviderPhaseSliceV1 {
                phase: phase_label(slice.phase).to_owned(),
                session_instance_id: slice.session_instance_id.clone()?,
                start_utc: slice
                    .window
                    .start
                    .to_rfc3339_opts(SecondsFormat::Nanos, true),
                end_exclusive_utc: slice
                    .window
                    .end_exclusive
                    .to_rfc3339_opts(SecondsFormat::Nanos, true),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    phases.sort_by(|left, right| {
        left.start_utc
            .cmp(&right.start_utc)
            .then_with(|| left.end_exclusive_utc.cmp(&right.end_exclusive_utc))
            .then_with(|| left.session_instance_id.cmp(&right.session_instance_id))
    });
    Some(phases)
}

fn phase_label(phase: Option<SessionPhase>) -> &'static str {
    match phase {
        None => "daily",
        Some(SessionPhase::Pre) => "pre",
        Some(SessionPhase::Regular) => "regular",
        Some(SessionPhase::After) => "after",
        Some(SessionPhase::Over) => "overnight",
    }
}

fn futu_probe_capabilities() -> Vec<SourceCapability> {
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
                provider: Provider::Futu,
                feed: None,
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
            SessionPhase::Over,
        ] {
            capabilities.push(SourceCapability {
                provider: Provider::Futu,
                feed: None,
                phase: Some(phase),
                interval,
                historical: true,
                realtime: false,
            });
        }
    }
    capabilities
}

#[derive(Eq, PartialEq)]
struct ScopeIdentity {
    provider: String,
    feed: String,
    source_scope_id: String,
    entitlement_scope_sha256: String,
    rights_profile_sha256: String,
    entitlement: EntitlementState,
    numeric_encoding: NumericEncodingV1,
    effective_resolution: market_contracts::BarResolutionV1,
    market_calendar_version: Option<String>,
    authorization_context_sha256: String,
    sdk_name: String,
    sdk_version: String,
    gateway_version: String,
    issuer: String,
    key_id: String,
    license_scope_id: String,
    license_terms_sha256: String,
}

impl ScopeIdentity {
    fn from_admission(
        scope: &crate::archive::reconciliation::CacheQuerySourceScopeV1,
        admission: &VerifiedProviderAdmissionV1,
    ) -> Self {
        Self {
            provider: scope.provider.clone(),
            feed: scope.feed.clone(),
            source_scope_id: scope.source_scope_id.clone(),
            entitlement_scope_sha256: scope.entitlement_scope_sha256.clone(),
            rights_profile_sha256: scope.rights_profile_sha256.clone(),
            entitlement: scope.entitlement,
            numeric_encoding: scope.numeric_encoding,
            effective_resolution: scope.effective_resolution,
            market_calendar_version: scope.market_calendar_version.clone(),
            authorization_context_sha256: admission.authorization_context_sha256().to_owned(),
            sdk_name: admission.sdk_name().to_owned(),
            sdk_version: admission.sdk_version().to_owned(),
            gateway_version: admission.gateway_version().to_owned(),
            issuer: admission.issuer().to_owned(),
            key_id: admission.key_id().to_owned(),
            license_scope_id: admission.license_scope_id().to_owned(),
            license_terms_sha256: admission.license_terms_sha256().to_owned(),
        }
    }
}

fn digest_strings(values: &[String]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"mdp-futu-admission-set-v1\0");
    for value in values {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn now_unix_seconds() -> Result<u64, ProviderQueryError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .ok_or(ProviderQueryError::InvalidCursor)
}
