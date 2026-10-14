//! Signed provider-specific overnight calendar overlays.
//!
//! XNYS cash calendars remain strict and never contain overnight phases. An embedding owner may
//! independently inject a signed overlay whose exact content is bound to one cash snapshot. The
//! overlay adds only explicit, signed windows; it does not establish market-data entitlement.

use std::{collections::BTreeSet, fmt};

use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use market_contracts::AssetClassV1;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    archive::reconciliation::authority::TrustedProviderAdmissionVerifierV1,
    session_router::{
        AlpacaFeed, CalendarDay, CalendarDayState, Provider, SessionInstance, SessionPhase,
        TrustedCalendarSnapshot, UtcWindow, validate_calendar_snapshot,
    },
};

const OVERLAY_DOMAIN: &[u8] = b"lqepoch.mdp.trusted-overnight-calendar.v1\0";
const CONTENT_FINGERPRINT_DOMAIN: &[u8] = b"lqepoch.mdp.overnight-calendar-content.v1\0";
const COMPOSITE_CALENDAR_DOMAIN: &[u8] = b"lqepoch.mdp.composite-market-calendar.v1\0";
const CASH_CALENDAR_DOMAIN: &[u8] = b"lqepoch.mdp.cash-market-calendar.v1\0";
const OVERLAY_SCHEMA_ID: &str = "mdp.trusted-overnight-calendar.v1";
const NEW_YORK_TIMEZONE: &str = "America/New_York";
const MAX_OVERLAY_BYTES: usize = 2 * 1024 * 1024;
// 8,192 windows cover more than 30 years at roughly 252 trading dates per year.
const MAX_OVERLAY_WINDOWS: usize = 8_192;
const MAX_ELIGIBLE_SYMBOLS: usize = 10_000;
const MAX_ISSUER_BYTES: usize = 128;
const MAX_DIMENSION_BYTES: usize = 256;
const MAX_SIGNATURE_BYTES: usize = 4096;
const MAX_LEASE_SECONDS: u64 = 366 * 24 * 60 * 60;

/// A bounded provider/feed route that the signed calendar explicitly covers.
///
/// `feed` is deliberately not `Option<AlpacaFeed>`: the wire claim distinguishes the Futu
/// timetable (`futu`) from Alpaca BOATS, while `matches_source_capability` maps Futu to the Core
/// capability shape `(Provider::Futu, None)`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OvernightRouteV1 {
    pub(crate) provider: Provider,
    pub(crate) feed: OvernightRouteFeedV1,
}

impl OvernightRouteV1 {
    pub(crate) fn matches_source_capability(
        self,
        capability: &crate::session_router::SourceCapability,
    ) -> bool {
        if capability.phase != Some(SessionPhase::Over) {
            return false;
        }
        match (self.provider, self.feed) {
            (Provider::Alpaca, OvernightRouteFeedV1::Boats) => {
                capability.provider == Provider::Alpaca
                    && capability.feed == Some(AlpacaFeed::Boats)
            }
            (Provider::Futu, OvernightRouteFeedV1::Futu) => {
                capability.provider == Provider::Futu && capability.feed.is_none()
            }
            _ => false,
        }
    }

    fn is_supported(self) -> bool {
        matches!(
            (self.provider, self.feed),
            (Provider::Alpaca, OvernightRouteFeedV1::Boats)
                | (Provider::Futu, OvernightRouteFeedV1::Futu)
        )
    }

    fn canonical_key(self) -> &'static str {
        match (self.provider, self.feed) {
            (Provider::Alpaca, OvernightRouteFeedV1::Boats) => "alpaca/boats",
            (Provider::Futu, OvernightRouteFeedV1::Futu) => "futu/futu",
            _ => "unsupported/unsupported",
        }
    }
}

/// Wire-level feed identity for an overnight timetable or compatible market-data route.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OvernightRouteFeedV1 {
    Boats,
    Futu,
}

/// Status returned to HTTP for the cash-calendar/overnight-overlay selection.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OvernightOverlayStatus {
    NoOverlay,
    Applied,
    InvalidArtifact,
    RawShaMismatch,
    CashBindingMismatch,
    TrustRejected,
    NotYetValid,
    Expired,
    Revoked,
    NotEligible,
    NoWindowCoverage,
}

/// Public calendar component identity. This contains no receipt, key, or signature material.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CalendarComponentsV1 {
    pub(crate) cash: CashCalendarComponentV1,
    /// Exact SHA-256 of the supplied signed artifact bytes, when bytes were available to hash.
    pub(crate) overnight_input_sha256: Option<String>,
    /// Signed public calendar identity, present only after its signature and content validated.
    pub(crate) overnight: Option<OvernightCalendarComponentV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CashCalendarComponentV1 {
    pub(crate) source_id: String,
    pub(crate) version: String,
    pub(crate) input_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OvernightCalendarComponentV1 {
    pub(crate) source_provider: Provider,
    pub(crate) source_feed: OvernightRouteFeedV1,
    pub(crate) source_id: String,
    pub(crate) version: String,
    pub(crate) source_reference: String,
    pub(crate) content_fingerprint_sha256: String,
}

/// Snapshot plus explicit overlay-selection evidence consumed by the bars/session adapters.
#[derive(Clone, Debug)]
pub(crate) struct CalendarSnapshotSelection {
    pub(crate) snapshot: std::sync::Arc<TrustedCalendarSnapshot>,
    pub(crate) status: OvernightOverlayStatus,
    pub(crate) allowed_overnight_routes: Vec<OvernightRouteV1>,
    pub(crate) components: CalendarComponentsV1,
}

/// A signature-verified overlay. Fields remain private and the type is deliberately not
/// deserializable: runtime consumers cannot construct trusted overlay authority from JSON.
pub(crate) struct VerifiedOvernightCalendarOverlay {
    claims: SignedOvernightCalendarClaimsV1,
    signature_base64: String,
    input_sha256: String,
    content_fingerprint_sha256: String,
}

impl fmt::Debug for VerifiedOvernightCalendarOverlay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedOvernightCalendarOverlay")
            .field(
                "calendar_source_id",
                &self.claims.content.calendar_source_id,
            )
            .field("calendar_version", &self.claims.content.calendar_version)
            .field("input_sha256", &self.input_sha256)
            .field(
                "content_fingerprint_sha256",
                &self.content_fingerprint_sha256,
            )
            .field("signature_base64", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SignedOvernightCalendarArtifactV1 {
    claims: SignedOvernightCalendarClaimsV1,
    signature_base64: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SignedOvernightCalendarClaimsV1 {
    schema_id: String,
    authority_role: String,
    issuer: String,
    key_id: String,
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    revoked_at_unix_seconds: Option<u64>,
    content: OvernightCalendarContentV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct OvernightCalendarContentV1 {
    cash_source_id: String,
    cash_version: String,
    cash_input_sha256: String,
    source_provider: Provider,
    source_feed: OvernightRouteFeedV1,
    calendar_source_id: String,
    calendar_version: String,
    calendar_source_reference: String,
    timezone: String,
    compatible_routes: Vec<OvernightRouteV1>,
    eligible_asset_classes: Vec<AssetClassV1>,
    eligible_symbols: Vec<String>,
    windows: Vec<OvernightWindowV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct OvernightWindowV1 {
    trade_date: NaiveDate,
    session_id: String,
    session_instance_id: String,
    start_utc: String,
    end_exclusive_utc: String,
}

#[derive(Clone, Debug)]
pub(crate) struct OverlayVerificationFailure {
    pub(crate) status: OvernightOverlayStatus,
    pub(crate) input_sha256: Option<String>,
}

/// Verifies exact bytes, signature/domain, cash binding, eligibility bounds, and each explicit
/// UTC OVER window. Time validity is checked again at every selection using current wall clock.
pub(crate) fn verify_signed_overnight_overlay(
    bytes: &[u8],
    expected_raw_sha256: &str,
    cash_snapshot: &TrustedCalendarSnapshot,
    cash_input_sha256: &str,
    verifier: &dyn TrustedProviderAdmissionVerifierV1,
) -> Result<VerifiedOvernightCalendarOverlay, OverlayVerificationFailure> {
    if bytes.len() > MAX_OVERLAY_BYTES {
        return Err(failure(OvernightOverlayStatus::InvalidArtifact, None));
    }
    let input_sha256 = sha256(bytes);
    if !valid_sha256(expected_raw_sha256) || input_sha256 != expected_raw_sha256 {
        return Err(failure(
            OvernightOverlayStatus::RawShaMismatch,
            Some(input_sha256),
        ));
    }
    let artifact: SignedOvernightCalendarArtifactV1 =
        serde_json::from_slice(bytes).map_err(|_| {
            failure(
                OvernightOverlayStatus::InvalidArtifact,
                Some(input_sha256.clone()),
            )
        })?;
    validate_signed_claims_shape(&artifact.claims)
        .map_err(|status| failure(status, Some(input_sha256.clone())))?;
    if artifact.signature_base64.is_empty() || artifact.signature_base64.len() > MAX_SIGNATURE_BYTES
    {
        return Err(failure(
            OvernightOverlayStatus::InvalidArtifact,
            Some(input_sha256),
        ));
    }
    let payload = canonical_signed_payload(&artifact.claims).map_err(|_| {
        failure(
            OvernightOverlayStatus::InvalidArtifact,
            Some(input_sha256.clone()),
        )
    })?;
    if verifier
        .verify_signature(
            &artifact.claims.issuer,
            &artifact.claims.key_id,
            &payload,
            &artifact.signature_base64,
        )
        .is_err()
    {
        return Err(failure(
            OvernightOverlayStatus::TrustRejected,
            Some(input_sha256),
        ));
    }
    validate_cash_binding(&artifact.claims.content, cash_snapshot, cash_input_sha256)
        .map_err(|status| failure(status, Some(input_sha256.clone())))?;
    validate_windows(&artifact.claims.content.windows, cash_snapshot)
        .map_err(|status| failure(status, Some(input_sha256.clone())))?;
    let content_fingerprint_sha256 =
        fingerprint_content(&artifact.claims.content).map_err(|_| {
            failure(
                OvernightOverlayStatus::InvalidArtifact,
                Some(input_sha256.clone()),
            )
        })?;
    let mut merged = cash_snapshot.clone();
    merged
        .instances
        .extend(instances_from_claims(&artifact.claims.content.windows)?);
    merged
        .instances
        .sort_by_key(|instance| (instance.window.start, instance.window.end_exclusive));
    if validate_calendar_snapshot(&merged).is_err() {
        return Err(failure(
            OvernightOverlayStatus::InvalidArtifact,
            Some(input_sha256),
        ));
    }
    Ok(VerifiedOvernightCalendarOverlay {
        claims: artifact.claims,
        signature_base64: artifact.signature_base64,
        input_sha256,
        content_fingerprint_sha256,
    })
}

impl VerifiedOvernightCalendarOverlay {
    pub(crate) fn component(&self) -> OvernightCalendarComponentV1 {
        let content = &self.claims.content;
        OvernightCalendarComponentV1 {
            source_provider: content.source_provider,
            source_feed: content.source_feed,
            source_id: content.calendar_source_id.clone(),
            version: content.calendar_version.clone(),
            source_reference: content.calendar_source_reference.clone(),
            content_fingerprint_sha256: self.content_fingerprint_sha256.clone(),
        }
    }

    pub(crate) fn input_sha256(&self) -> &str {
        &self.input_sha256
    }

    pub(crate) fn compatible_routes(&self) -> &[OvernightRouteV1] {
        &self.claims.content.compatible_routes
    }

    pub(crate) fn is_eligible(&self, asset_class: AssetClassV1, symbol: &str) -> bool {
        self.claims
            .content
            .eligible_asset_classes
            .contains(&asset_class)
            && self
                .claims
                .content
                .eligible_symbols
                .binary_search_by(|candidate| candidate.as_str().cmp(symbol))
                .is_ok()
    }

    pub(crate) fn time_status(&self, now_unix_seconds: u64) -> Option<OvernightOverlayStatus> {
        let claims = &self.claims;
        if claims.issued_at_unix_seconds > now_unix_seconds {
            return Some(OvernightOverlayStatus::NotYetValid);
        }
        if claims
            .revoked_at_unix_seconds
            .is_some_and(|revoked_at| revoked_at <= now_unix_seconds)
        {
            return Some(OvernightOverlayStatus::Revoked);
        }
        if now_unix_seconds >= claims.expires_at_unix_seconds {
            return Some(OvernightOverlayStatus::Expired);
        }
        None
    }

    pub(crate) fn signature_is_currently_trusted(
        &self,
        verifier: &dyn TrustedProviderAdmissionVerifierV1,
    ) -> bool {
        canonical_signed_payload(&self.claims).is_ok_and(|payload| {
            verifier
                .verify_signature(
                    &self.claims.issuer,
                    &self.claims.key_id,
                    &payload,
                    &self.signature_base64,
                )
                .is_ok()
        })
    }

    pub(crate) fn merged_snapshot(
        &self,
        cash_snapshot: &TrustedCalendarSnapshot,
        cash_input_sha256: &str,
    ) -> Option<TrustedCalendarSnapshot> {
        let mut snapshot = cash_snapshot.clone();
        snapshot.version = composite_calendar_version(
            cash_snapshot,
            cash_input_sha256,
            &self.claims.content,
            &self.content_fingerprint_sha256,
        )
        .ok()?;
        snapshot
            .instances
            .extend(instances_from_claims(&self.claims.content.windows).ok()?);
        snapshot
            .instances
            .sort_by_key(|instance| (instance.window.start, instance.window.end_exclusive));
        validate_calendar_snapshot(&snapshot).ok()?;
        Some(snapshot)
    }
}

fn validate_signed_claims_shape(
    claims: &SignedOvernightCalendarClaimsV1,
) -> Result<(), OvernightOverlayStatus> {
    if claims.schema_id != OVERLAY_SCHEMA_ID
        || claims.authority_role != "market-calendar:overnight"
        || !valid_text(&claims.issuer, MAX_ISSUER_BYTES)
        || !valid_text(&claims.key_id, MAX_ISSUER_BYTES)
        || claims.expires_at_unix_seconds <= claims.issued_at_unix_seconds
        || claims.expires_at_unix_seconds - claims.issued_at_unix_seconds > MAX_LEASE_SECONDS
        || claims.revoked_at_unix_seconds.is_some_and(|revoked| {
            revoked < claims.issued_at_unix_seconds || revoked > claims.expires_at_unix_seconds
        })
    {
        return Err(OvernightOverlayStatus::InvalidArtifact);
    }
    let content = &claims.content;
    if !valid_sha256(&content.cash_input_sha256)
        || !valid_text(&content.cash_source_id, MAX_DIMENSION_BYTES)
        || !valid_text(&content.cash_version, MAX_DIMENSION_BYTES)
        || !valid_calendar_label(&content.calendar_source_id)
        || !valid_calendar_label(&content.calendar_version)
        || !valid_public_reference(&content.calendar_source_reference)
        || content.timezone != NEW_YORK_TIMEZONE
        || !valid_source_pair(content.source_provider, content.source_feed)
        || content.compatible_routes.is_empty()
        || content.compatible_routes.len() > 2
        || content.eligible_asset_classes.is_empty()
        || content.eligible_asset_classes.len() > 2
        || content.eligible_symbols.is_empty()
        || content.eligible_symbols.len() > MAX_ELIGIBLE_SYMBOLS
        || content.windows.is_empty()
        || content.windows.len() > MAX_OVERLAY_WINDOWS
    {
        return Err(OvernightOverlayStatus::InvalidArtifact);
    }
    let mut routes = BTreeSet::new();
    let mut prior_route: Option<&str> = None;
    for route in &content.compatible_routes {
        let route_key = route.canonical_key();
        if !route.is_supported()
            || !routes.insert(route_key)
            || prior_route.is_some_and(|prior| prior >= route_key)
        {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        }
        prior_route = Some(route_key);
    }
    let mut asset_classes = BTreeSet::new();
    let mut prior_asset_class = None;
    for asset_class in &content.eligible_asset_classes {
        let rank = if *asset_class == AssetClassV1::Stock {
            0
        } else if *asset_class == AssetClassV1::Etf {
            1
        } else {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        };
        if !asset_classes.insert(rank) || prior_asset_class.is_some_and(|prior| prior >= rank) {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        }
        prior_asset_class = Some(rank);
    }
    let mut prior_symbol: Option<&str> = None;
    for symbol in &content.eligible_symbols {
        if !valid_symbol(symbol) || prior_symbol.is_some_and(|prior| prior >= symbol.as_str()) {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        }
        prior_symbol = Some(symbol);
    }
    Ok(())
}

fn validate_cash_binding(
    content: &OvernightCalendarContentV1,
    cash_snapshot: &TrustedCalendarSnapshot,
    cash_input_sha256: &str,
) -> Result<(), OvernightOverlayStatus> {
    if content.cash_source_id != cash_snapshot.source_id
        || content.cash_version != cash_snapshot.version
        || content.cash_input_sha256 != cash_input_sha256
        || !valid_sha256(cash_input_sha256)
    {
        return Err(OvernightOverlayStatus::CashBindingMismatch);
    }
    Ok(())
}

fn validate_windows(
    windows: &[OvernightWindowV1],
    cash_snapshot: &TrustedCalendarSnapshot,
) -> Result<(), OvernightOverlayStatus> {
    let mut session_instance_ids = cash_snapshot
        .instances
        .iter()
        .map(|instance| instance.session_instance_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut prior_end: Option<DateTime<Utc>> = None;
    for window in windows {
        if !valid_text(&window.session_id, MAX_DIMENSION_BYTES)
            || !valid_text(&window.session_instance_id, MAX_DIMENSION_BYTES)
            || !session_instance_ids.insert(window.session_instance_id.as_str())
        {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        }
        let start = parse_canonical_utc(&window.start_utc)
            .map_err(|_| OvernightOverlayStatus::InvalidArtifact)?;
        let end = parse_canonical_utc(&window.end_exclusive_utc)
            .map_err(|_| OvernightOverlayStatus::InvalidArtifact)?;
        if start >= end || prior_end.is_some_and(|prior| start < prior) {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        }
        let Some(day_index) = cash_snapshot
            .days
            .binary_search_by_key(&window.trade_date, |day| day.trade_date)
            .ok()
        else {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        };
        let day = &cash_snapshot.days[day_index];
        let CalendarDayState::Open { session_id } = &day.state else {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        };
        if session_id != &window.session_id {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        }
        let expected = expected_overnight_window(&cash_snapshot.days, day_index)
            .ok_or(OvernightOverlayStatus::InvalidArtifact)?;
        if start < expected.start || end > expected.end_exclusive {
            return Err(OvernightOverlayStatus::InvalidArtifact);
        }
        prior_end = Some(end);
    }
    Ok(())
}

/// Computes the 20:00 previous-New-York-day through 04:00 trade-date band from trusted adjacent
/// civil-day UTC bounds. This keeps spring/fall DST transitions tied to the signed cash snapshot.
fn expected_overnight_window(days: &[CalendarDay], day_index: usize) -> Option<UtcWindow> {
    let trade_day = days.get(day_index)?;
    let prior_day = days.get(day_index.checked_sub(1)?)?;
    let prior_day_length = duration_seconds(prior_day.day_window)?;
    let trade_day_length = duration_seconds(trade_day.day_window)?;
    let hours = 60 * 60;
    if (prior_day_length != 23 * hours
        && prior_day_length != 24 * hours
        && prior_day_length != 25 * hours)
        || (trade_day_length != 23 * hours
            && trade_day_length != 24 * hours
            && trade_day_length != 25 * hours)
        || prior_day.trade_date.succ_opt() != Some(trade_day.trade_date)
        || prior_day.day_window.end_exclusive != trade_day.day_window.start
    {
        return None;
    }
    Some(UtcWindow {
        start: prior_day.day_window.start + Duration::seconds(prior_day_length - 4 * hours),
        end_exclusive: trade_day.day_window.start
            + Duration::seconds(trade_day_length - 20 * hours),
    })
}

fn duration_seconds(window: UtcWindow) -> Option<i64> {
    let seconds = (window.end_exclusive - window.start).num_seconds();
    (window.start < window.end_exclusive && seconds > 0).then_some(seconds)
}

fn instances_from_claims(
    windows: &[OvernightWindowV1],
) -> Result<Vec<SessionInstance>, OverlayVerificationFailure> {
    windows
        .iter()
        .map(|window| {
            let start = parse_canonical_utc(&window.start_utc)
                .map_err(|_| failure(OvernightOverlayStatus::InvalidArtifact, None))?;
            let end_exclusive = parse_canonical_utc(&window.end_exclusive_utc)
                .map_err(|_| failure(OvernightOverlayStatus::InvalidArtifact, None))?;
            Ok(SessionInstance {
                session_id: window.session_id.clone(),
                session_instance_id: window.session_instance_id.clone(),
                trade_date: window.trade_date,
                phase: SessionPhase::Over,
                window: UtcWindow {
                    start,
                    end_exclusive,
                },
            })
        })
        .collect()
}

fn valid_source_pair(provider: Provider, feed: OvernightRouteFeedV1) -> bool {
    matches!(
        (provider, feed),
        (Provider::Alpaca, OvernightRouteFeedV1::Boats)
            | (Provider::Futu, OvernightRouteFeedV1::Futu)
    )
}

fn valid_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol.len() <= 32
        && symbol.trim() == symbol
        && symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || b".-".contains(&byte))
}

fn valid_calendar_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_DIMENSION_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn valid_public_reference(value: &str) -> bool {
    let Some(authority_and_path) = value.strip_prefix("https://") else {
        return false;
    };
    let authority = authority_and_path.split('/').next().unwrap_or_default();
    valid_text(value, 2048)
        && !authority.is_empty()
        && !value.contains('?')
        && !value.contains('#')
        && value.bytes().all(|byte| !byte.is_ascii_whitespace())
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_canonical_utc(value: &str) -> Result<DateTime<Utc>, ()> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| ())?
        .with_timezone(&Utc);
    let canonical = parsed.to_rfc3339_opts(SecondsFormat::Nanos, true);
    (canonical == value).then_some(parsed).ok_or(())
}

fn canonical_signed_payload(
    claims: &SignedOvernightCalendarClaimsV1,
) -> Result<Vec<u8>, serde_json::Error> {
    let encoded = serde_json::to_vec(claims)?;
    let mut payload = Vec::with_capacity(OVERLAY_DOMAIN.len() + encoded.len());
    payload.extend_from_slice(OVERLAY_DOMAIN);
    payload.extend_from_slice(&encoded);
    Ok(payload)
}

fn fingerprint_content(content: &OvernightCalendarContentV1) -> Result<String, serde_json::Error> {
    let encoded = serde_json::to_vec(content)?;
    Ok(hash_domain(CONTENT_FINGERPRINT_DOMAIN, &encoded))
}

fn composite_calendar_version(
    cash: &TrustedCalendarSnapshot,
    cash_input_sha256: &str,
    overlay: &OvernightCalendarContentV1,
    content_fingerprint_sha256: &str,
) -> Result<String, serde_json::Error> {
    let dimensions = (
        cash.source_id.as_str(),
        cash.version.as_str(),
        cash_input_sha256,
        overlay.calendar_source_id.as_str(),
        overlay.calendar_version.as_str(),
        content_fingerprint_sha256,
    );
    let encoded = serde_json::to_vec(&dimensions)?;
    Ok(format!(
        "composite.v1.{}",
        hash_domain(COMPOSITE_CALENDAR_DOMAIN, &encoded)
    ))
}

pub(super) fn stable_cash_calendar_version(
    cash: &TrustedCalendarSnapshot,
    cash_input_sha256: &str,
) -> String {
    let digest = hash_fields(
        CASH_CALENDAR_DOMAIN,
        &[
            cash.source_id.as_str(),
            cash.version.as_str(),
            cash_input_sha256,
        ],
    );
    format!("cash.v1.{digest}")
}

fn hash_fields(domain: &[u8], fields: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn hash_domain(domain: &[u8], bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn failure(
    status: OvernightOverlayStatus,
    input_sha256: Option<String>,
) -> OverlayVerificationFailure {
    OverlayVerificationFailure {
        status,
        input_sha256,
    }
}

#[cfg(test)]
#[path = "overnight_overlay_test_support.rs"]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "overnight_overlay_tests.rs"]
mod tests;
