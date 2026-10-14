use std::fmt;

use market_contracts::{
    BarsEntitlementStatusV1, BarsRequestV1, NumericEncodingV1, ProtoTimestampV2,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    archive::reconciliation::{
        CacheKeyV1, ProviderCursorCacheInputV1, ProviderCursorKindV1,
        ValidatedProviderCursorReceiptV1,
    },
    market_query::planning::{MarketBarsPlan, ProviderQueryGroup, ProviderRun, next_provider_run},
    session_router::{AlpacaFeed, PlannedSource},
};

use super::{
    error::ProviderQueryError,
    projection::parse_proto,
    types::{ProviderCacheIdentity, ProviderCachePolicy},
};

const CURSOR_PREFIX: &str = "mdp1:";
const CURSOR_TTL_SECS: u64 = 30 * 60;
const MAX_CURSOR_BYTES: usize = market_contracts::MAX_BARS_CURSOR_BYTES_V1;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorEnvelope {
    version: u8,
    request_sha256: String,
    plan_sha256: String,
    as_of_utc: String,
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    group_index: usize,
    timeline_position: usize,
    provider_cursor: Option<ProviderCursor>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum ProviderCursor {
    Alpaca(alpaca_rest_read::StockBarsCursor),
    Futu(String),
}

impl fmt::Debug for ProviderCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Alpaca(_) => "Alpaca([REDACTED])",
            Self::Futu(_) => "Futu([REDACTED])",
        })
    }
}

/// Opaque, request-bound selection of the current planned provider group and page.
pub struct ProviderPageSelection {
    request_sha256: String,
    plan_sha256: String,
    as_of_utc: String,
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    run: ProviderRun,
    timeline_position: usize,
    next_run_group_index: Option<usize>,
    provider_cursor: Option<ProviderCursor>,
    cache_key: String,
    cache_identity: ProviderCacheIdentity,
    cache_policy: ProviderCachePolicy,
}

impl fmt::Debug for ProviderPageSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderPageSelection")
            .field("group_index", &self.run.group_index)
            .field("timeline_position", &self.timeline_position)
            .field("run_window", &self.run.group.fetch_window)
            .field("has_provider_cursor", &self.provider_cursor.is_some())
            .field("cache_key", &self.cache_key)
            .finish()
    }
}

impl ProviderPageSelection {
    /// Return the selected group index within the frozen plan.
    #[must_use]
    pub const fn group_index(&self) -> usize {
        self.run.group_index
    }

    /// Return the plan timeline position for this run's opaque continuation.
    #[must_use]
    pub const fn timeline_position(&self) -> usize {
        self.timeline_position
    }

    /// Return the clipped provider group for this temporal run.
    #[must_use]
    pub const fn provider_group(&self) -> &ProviderQueryGroup {
        &self.run.group
    }

    /// Return the clipped run for trusted plan mapping.
    #[must_use]
    pub const fn provider_run(&self) -> &ProviderRun {
        &self.run
    }

    /// Return a digest suitable for this exact planned source group cache request.
    #[must_use]
    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }

    /// Return actual-source facts and the planned route identity for safe cache scoping.
    #[must_use]
    pub const fn cache_identity(&self) -> &ProviderCacheIdentity {
        &self.cache_identity
    }

    /// Return the route's baseline cache policy before operator and principal partitioning.
    #[must_use]
    pub const fn cache_policy(&self) -> ProviderCachePolicy {
        self.cache_policy
    }

    /// Build the next temporal run's cursor after a cache hit or an exhausted provider page.
    pub fn cursor_for_next_group(&self) -> Result<Option<String>, ProviderQueryError> {
        let Some(next_group_index) = self.next_run_group_index else {
            return Ok(None);
        };
        encode_envelope(&self.envelope(next_group_index, self.run.next_timeline_position, None))
            .map(Some)
            .map_err(|_| ProviderQueryError::InvalidCursor)
    }

    /// Rewrap a cache-verified provider-local cursor in this request's current plan envelope.
    ///
    /// The raw provider token is consumed only inside this module. The caller supplies the exact
    /// cache key used for the hit so both the persisted receipt and this group's stable cache
    /// identity are checked before a new outer continuation is returned.
    pub(crate) fn cursor_after_cached_provider_page(
        &self,
        receipt: &ValidatedProviderCursorReceiptV1,
        cache_key: &CacheKeyV1,
        now_unix_seconds: u64,
    ) -> Result<String, ProviderQueryError> {
        receipt
            .validate_for_cache_key(cache_key)
            .map_err(|_| ProviderQueryError::CursorQueryMismatch)?;

        let expected_cursor_hash = digest_text(&self.cache_key);
        let expected_provider = match receipt.kind() {
            ProviderCursorKindV1::Alpaca => "alpaca",
            ProviderCursorKindV1::Futu => "futu",
        };
        if cache_key.request_cursor_sha256.as_deref() != Some(expected_cursor_hash.as_str())
            || cache_key.provider != self.cache_identity.provider
            || cache_key.feed != self.cache_identity.feed
            || cache_key.source_scope_id != self.cache_identity.source_scope_id
            || cache_key.effective_resolution != self.cache_identity.effective_resolution.as_str()
            || cache_key.numeric_encoding != self.cache_identity.numeric_encoding.as_str()
            || self.cache_identity.provider != expected_provider
        {
            return Err(ProviderQueryError::CursorQueryMismatch);
        }

        if now_unix_seconds < self.issued_at_unix_seconds {
            return Err(ProviderQueryError::InvalidCursor);
        }
        let expires_at_unix_seconds = receipt
            .expires_at_unix_seconds()
            .min(self.expires_at_unix_seconds);
        if now_unix_seconds >= expires_at_unix_seconds
            || self.issued_at_unix_seconds >= expires_at_unix_seconds
        {
            return Err(ProviderQueryError::CursorExpired);
        }

        let provider_cursor = match receipt.kind() {
            ProviderCursorKindV1::Alpaca => {
                let cursor: alpaca_rest_read::StockBarsCursor =
                    serde_json::from_str(receipt.payload_json())
                        .map_err(|_| ProviderQueryError::InvalidCursor)?;
                ProviderCursor::Alpaca(cursor)
            }
            ProviderCursorKindV1::Futu => {
                let cursor: String = serde_json::from_str(receipt.payload_json())
                    .map_err(|_| ProviderQueryError::InvalidCursor)?;
                if !valid_futu_cursor(&cursor) {
                    return Err(ProviderQueryError::InvalidCursor);
                }
                ProviderCursor::Futu(cursor)
            }
        };

        let mut envelope = self.envelope(
            self.run.group_index,
            self.timeline_position,
            Some(provider_cursor),
        );
        envelope.expires_at_unix_seconds = expires_at_unix_seconds;
        encode_envelope(&envelope).map_err(|_| ProviderQueryError::InvalidCursor)
    }

    pub(super) fn provider_cursor(&self) -> Option<&ProviderCursor> {
        self.provider_cursor.as_ref()
    }

    pub(super) fn validate_for(&self, plan: &MarketBarsPlan) -> Result<(), ProviderQueryError> {
        if self.plan_sha256 != plan_digest(plan)?
            || self.as_of_utc != plan.as_of.as_str()
            || self.timeline_position > plan.slices_recent_to_old.len()
        {
            return Err(ProviderQueryError::CursorPlanChanged);
        }
        let run = next_provider_run(plan, self.timeline_position)
            .map_err(|_| ProviderQueryError::CursorPlanChanged)?
            .ok_or(ProviderQueryError::CursorPlanChanged)?;
        if run != self.run {
            return Err(ProviderQueryError::CursorPlanChanged);
        }
        Ok(())
    }

    pub(super) fn cursor_for_provider_page(
        &self,
        provider_cursor: ProviderCursor,
    ) -> Result<String, ProviderQueryError> {
        encode_envelope(&self.envelope(
            self.run.group_index,
            self.timeline_position,
            Some(provider_cursor),
        ))
        .map_err(|_| ProviderQueryError::InvalidCursor)
    }

    fn envelope(
        &self,
        group_index: usize,
        timeline_position: usize,
        provider_cursor: Option<ProviderCursor>,
    ) -> CursorEnvelope {
        CursorEnvelope {
            version: 1,
            request_sha256: self.request_sha256.clone(),
            plan_sha256: self.plan_sha256.clone(),
            as_of_utc: self.as_of_utc.clone(),
            issued_at_unix_seconds: self.issued_at_unix_seconds,
            expires_at_unix_seconds: self.expires_at_unix_seconds,
            group_index,
            timeline_position,
            provider_cursor,
        }
    }
}

/// Keep the original plan snapshot time stable across all pages of a cursor.
pub fn planning_as_of(
    request: &BarsRequestV1,
    observed_now: &ProtoTimestampV2,
) -> Result<ProtoTimestampV2, ProviderQueryError> {
    request
        .validate()
        .map_err(|_| ProviderQueryError::CursorQueryMismatch)?;
    let Some(cursor) = request.cursor.as_deref() else {
        return Ok(observed_now.clone());
    };
    let envelope = decode_envelope(cursor)?;
    let request_sha256 = request_digest(request)?;
    if envelope.request_sha256 != request_sha256 {
        return Err(ProviderQueryError::CursorQueryMismatch);
    }
    let observed_unix = unix_seconds(observed_now)?;
    if observed_unix >= envelope.expires_at_unix_seconds
        || envelope
            .expires_at_unix_seconds
            .saturating_sub(envelope.issued_at_unix_seconds)
            > CURSOR_TTL_SECS
    {
        return Err(ProviderQueryError::CursorExpired);
    }
    if observed_unix < envelope.issued_at_unix_seconds {
        return Err(ProviderQueryError::InvalidCursor);
    }
    ProtoTimestampV2::parse(&envelope.as_of_utc).map_err(|_| ProviderQueryError::InvalidCursor)
}

/// Select the current provider group or validate and consume its continuation cursor.
pub fn select_page(
    request: &BarsRequestV1,
    plan: &MarketBarsPlan,
) -> Result<ProviderPageSelection, ProviderQueryError> {
    request
        .validate()
        .map_err(|_| ProviderQueryError::CursorQueryMismatch)?;
    if request_identity(request)? != request_identity(&plan.request)? {
        return Err(ProviderQueryError::CursorQueryMismatch);
    }
    let request_sha256 = request_digest(request)?;
    let plan_sha256 = plan_digest(plan)?;
    let (
        timeline_position,
        expected_group_index,
        provider_cursor,
        issued_at_unix_seconds,
        expires_at_unix_seconds,
    ) = if let Some(cursor) = request.cursor.as_deref() {
        let envelope = decode_envelope(cursor)?;
        if envelope.request_sha256 != request_sha256 {
            return Err(ProviderQueryError::CursorQueryMismatch);
        }
        if envelope.plan_sha256 != plan_sha256 || envelope.as_of_utc != plan.as_of.as_str() {
            return Err(ProviderQueryError::CursorPlanChanged);
        }
        let now = current_unix_seconds()?;
        if now >= envelope.expires_at_unix_seconds {
            return Err(ProviderQueryError::CursorExpired);
        }
        if now < envelope.issued_at_unix_seconds {
            return Err(ProviderQueryError::InvalidCursor);
        }
        if envelope.timeline_position > plan.slices_recent_to_old.len() {
            return Err(ProviderQueryError::InvalidCursor);
        }
        let run = next_provider_run(plan, envelope.timeline_position)
            .map_err(|_| ProviderQueryError::CursorPlanChanged)?
            .ok_or(ProviderQueryError::InvalidCursor)?;
        if envelope.group_index != run.group_index {
            return Err(ProviderQueryError::CursorQueryMismatch);
        }
        validate_cursor_source(envelope.provider_cursor.as_ref(), run.group.source)?;
        (
            envelope.timeline_position,
            Some(envelope.group_index),
            envelope.provider_cursor,
            envelope.issued_at_unix_seconds,
            envelope.expires_at_unix_seconds,
        )
    } else {
        let issued = unix_seconds(&plan.as_of)?;
        (
            0,
            None,
            None,
            issued,
            issued.saturating_add(CURSOR_TTL_SECS),
        )
    };

    let run = next_provider_run(plan, timeline_position)
        .map_err(|_| ProviderQueryError::ProviderGroupUnavailable)?
        .ok_or(ProviderQueryError::ProviderGroupUnavailable)?;
    if expected_group_index.is_some_and(|index| index != run.group_index) {
        return Err(ProviderQueryError::CursorQueryMismatch);
    }
    let next_run_group_index = next_provider_run(plan, run.next_timeline_position)
        .map_err(|_| ProviderQueryError::ProviderGroupUnavailable)?
        .map(|next_run| next_run.group_index);
    let (cache_identity, cache_policy) = cache_identity(plan, &run.group)?;
    // The outer continuation token is bound to `as_of` and the complete plan. The cache key is a
    // separate, stable identity for the actual group page: omit the moving plan clock, outer
    // request bounds and opaque MDP cursor, while retaining the normalized request and exact
    // coalesced provider window. Include the provider cursor only as part of the digest input; its
    // raw value is never returned as the cache key or logged.
    let slices = run
        .group
        .slices_recent_to_old
        .iter()
        .map(|slice| {
            (
                slice.trade_date,
                slice.session_id.as_str(),
                slice.phase,
                slice.interval,
                slice.window,
                slice.calendar_verification_status,
                slice.source,
            )
        })
        .collect::<Vec<_>>();
    let cache_key = digest_json(&(
        (
            "market-bars-cache-key-v1",
            plan.request.symbol.as_str(),
            plan.request.asset_class,
            plan.request.resolution,
            plan.request.session_scope,
            plan.request.provider_mode,
            plan.request.allow_daily_downgrade,
            plan.request.limit,
        ),
        (
            run.group.source,
            run.group.resolution,
            run.group.adjustment,
            run.group.fetch_window,
            timeline_position,
            plan.calendar_source_id.as_str(),
            plan.calendar_version.as_str(),
            slices,
            provider_cursor.as_ref(),
        ),
    ))?;
    Ok(ProviderPageSelection {
        request_sha256,
        plan_sha256,
        as_of_utc: plan.as_of.as_str().to_owned(),
        issued_at_unix_seconds,
        expires_at_unix_seconds,
        run,
        timeline_position,
        next_run_group_index,
        provider_cursor,
        cache_key,
        cache_identity,
        cache_policy,
    })
}

pub(super) fn validate_provider_cursor(
    selection: &ProviderPageSelection,
    source: PlannedSource,
) -> Result<(), ProviderQueryError> {
    validate_cursor_source(selection.provider_cursor.as_ref(), source)
}

fn validate_cursor_source(
    cursor: Option<&ProviderCursor>,
    source: PlannedSource,
) -> Result<(), ProviderQueryError> {
    match (cursor, source) {
        (None, _) => Ok(()),
        (Some(ProviderCursor::Alpaca(_)), PlannedSource::Alpaca { .. }) => Ok(()),
        (Some(ProviderCursor::Futu(token)), PlannedSource::Futu) if valid_futu_cursor(token) => {
            Ok(())
        }
        _ => Err(ProviderQueryError::CursorQueryMismatch),
    }
}

pub(super) fn alpaca_cursor(
    selection: &ProviderPageSelection,
) -> Result<Option<alpaca_rest_read::StockBarsCursor>, ProviderQueryError> {
    match selection.provider_cursor() {
        None => Ok(None),
        Some(ProviderCursor::Alpaca(cursor)) => Ok(Some(cursor.clone())),
        Some(ProviderCursor::Futu(_)) => Err(ProviderQueryError::CursorQueryMismatch),
    }
}

pub(super) fn futu_cursor(
    selection: &ProviderPageSelection,
) -> Result<Option<&str>, ProviderQueryError> {
    match selection.provider_cursor() {
        None => Ok(None),
        Some(ProviderCursor::Futu(cursor)) if valid_futu_cursor(cursor) => Ok(Some(cursor)),
        _ => Err(ProviderQueryError::CursorQueryMismatch),
    }
}

pub(super) fn make_alpaca_cursor(
    selection: &ProviderPageSelection,
    cursor: alpaca_rest_read::StockBarsCursor,
) -> Result<String, ProviderQueryError> {
    selection.cursor_for_provider_page(ProviderCursor::Alpaca(cursor))
}

pub(super) fn alpaca_cursor_cache_input(
    selection: &ProviderPageSelection,
    cursor: &alpaca_rest_read::StockBarsCursor,
) -> Result<ProviderCursorCacheInputV1, ProviderQueryError> {
    let payload = serde_json::to_string(cursor).map_err(|_| ProviderQueryError::InvalidCursor)?;
    ProviderCursorCacheInputV1::new(
        ProviderCursorKindV1::Alpaca,
        payload,
        selection.expires_at_unix_seconds,
    )
    .map_err(|_| ProviderQueryError::InvalidCursor)
}

pub(super) fn make_futu_cursor(
    selection: &ProviderPageSelection,
    cursor: String,
) -> Result<String, ProviderQueryError> {
    if !valid_futu_cursor(&cursor) {
        return Err(ProviderQueryError::MalformedResponse);
    }
    selection.cursor_for_provider_page(ProviderCursor::Futu(cursor))
}

pub(super) fn futu_cursor_cache_input(
    selection: &ProviderPageSelection,
    cursor: &str,
) -> Result<ProviderCursorCacheInputV1, ProviderQueryError> {
    if !valid_futu_cursor(cursor) {
        return Err(ProviderQueryError::MalformedResponse);
    }
    let payload = serde_json::to_string(cursor).map_err(|_| ProviderQueryError::InvalidCursor)?;
    ProviderCursorCacheInputV1::new(
        ProviderCursorKindV1::Futu,
        payload,
        selection.expires_at_unix_seconds,
    )
    .map_err(|_| ProviderQueryError::InvalidCursor)
}

fn cache_identity(
    plan: &MarketBarsPlan,
    group: &ProviderQueryGroup,
) -> Result<(ProviderCacheIdentity, ProviderCachePolicy), ProviderQueryError> {
    let (provider, feed, source_scope_id, requested_feed, encoding, policy) = match group.source {
        PlannedSource::Alpaca { feed } => {
            let feed_label = match feed {
                AlpacaFeed::Sip => "sip",
                AlpacaFeed::Boats => "boats",
            };
            (
                "alpaca",
                "unknown",
                format!("alpaca-rest-stock-bars-{feed_label}-request-v1"),
                Some(feed_label.to_owned()),
                NumericEncodingV1::DecimalToken,
                ProviderCachePolicy::ProvisionalOnly,
            )
        }
        PlannedSource::Futu => (
            "futu",
            "futu",
            "futu-gateway-history-v1".to_owned(),
            None,
            NumericEncodingV1::BinaryFloat64ShortestDecimal,
            ProviderCachePolicy::ProvisionalOnly,
        ),
        PlannedSource::YFinanceDailyResearch => (
            "yfinance",
            "unknown",
            "yfinance-research-bridge-v1".to_owned(),
            None,
            NumericEncodingV1::BinaryFloat64ShortestDecimal,
            ProviderCachePolicy::BypassAdjustmentSemanticsUnverified,
        ),
    };
    let mut session_instance_ids = group
        .slices_recent_to_old
        .iter()
        .filter_map(|slice| slice.session_instance_id.clone())
        .collect::<Vec<_>>();
    session_instance_ids.sort();
    session_instance_ids.dedup();
    if session_instance_ids.is_empty() {
        return Err(ProviderQueryError::ProviderGroupUnavailable);
    }
    Ok((
        ProviderCacheIdentity {
            provider: provider.to_owned(),
            feed: feed.to_owned(),
            source_scope_id,
            requested_feed,
            entitlement_status: BarsEntitlementStatusV1::Unknown,
            numeric_encoding: encoding,
            effective_resolution: group.resolution,
            session_instance_ids,
            market_calendar_version: Some(plan.calendar_version.clone()),
        },
        policy,
    ))
}

fn request_identity(request: &BarsRequestV1) -> Result<Vec<u8>, ProviderQueryError> {
    let mut request = request.clone();
    request.cursor = None;
    serde_json::to_vec(&request).map_err(|_| ProviderQueryError::InvalidCursor)
}

fn request_digest(request: &BarsRequestV1) -> Result<String, ProviderQueryError> {
    Ok(hex::encode(Sha256::digest(request_identity(request)?)))
}

fn plan_digest(plan: &MarketBarsPlan) -> Result<String, ProviderQueryError> {
    let groups = plan
        .provider_groups
        .iter()
        .map(|group| {
            (
                group.source,
                group.resolution,
                group.adjustment,
                group.fetch_window,
                &group.slices_recent_to_old,
            )
        })
        .collect::<Vec<_>>();
    let calendar_evidence = plan
        .calendar_verification_status
        .iter()
        .map(|evidence| {
            (
                evidence.trade_date,
                evidence.session_id.as_str(),
                evidence.session_instance_id.as_deref(),
                evidence.phase,
                evidence.window,
                evidence.verification_status,
                evidence.provisional,
            )
        })
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&(
        request_identity(&plan.request)?,
        plan.as_of.as_str(),
        plan.requested_window,
        plan.effective_resolution,
        plan.calendar_source_id.as_str(),
        plan.calendar_version.as_str(),
        calendar_evidence,
        &plan.slices_recent_to_old,
        groups,
        &plan.gaps,
        plan.provisional,
    ))
    .map_err(|_| ProviderQueryError::InvalidCursor)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn digest_json(value: &impl Serialize) -> Result<String, ProviderQueryError> {
    let bytes = serde_json::to_vec(value).map_err(|_| ProviderQueryError::InvalidCursor)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn digest_text(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn encode_envelope(envelope: &CursorEnvelope) -> Result<String, ProviderQueryError> {
    let body = serde_json::to_vec(envelope).map_err(|_| ProviderQueryError::InvalidCursor)?;
    let token = format!("{CURSOR_PREFIX}{}", hex::encode(body));
    if token.len() > MAX_CURSOR_BYTES {
        return Err(ProviderQueryError::InvalidCursor);
    }
    Ok(token)
}

fn decode_envelope(token: &str) -> Result<CursorEnvelope, ProviderQueryError> {
    let encoded = token
        .strip_prefix(CURSOR_PREFIX)
        .filter(|encoded| !encoded.is_empty() && encoded.len() % 2 == 0)
        .ok_or(ProviderQueryError::InvalidCursor)?;
    if token.len() > MAX_CURSOR_BYTES || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ProviderQueryError::InvalidCursor);
    }
    let body = hex::decode(encoded).map_err(|_| ProviderQueryError::InvalidCursor)?;
    let envelope: CursorEnvelope =
        serde_json::from_slice(&body).map_err(|_| ProviderQueryError::InvalidCursor)?;
    if envelope.version != 1
        || !valid_digest(&envelope.request_sha256)
        || !valid_digest(&envelope.plan_sha256)
        || envelope.expires_at_unix_seconds <= envelope.issued_at_unix_seconds
        || envelope
            .expires_at_unix_seconds
            .saturating_sub(envelope.issued_at_unix_seconds)
            > CURSOR_TTL_SECS
    {
        return Err(ProviderQueryError::InvalidCursor);
    }
    ProtoTimestampV2::parse(&envelope.as_of_utc).map_err(|_| ProviderQueryError::InvalidCursor)?;
    Ok(envelope)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_futu_cursor(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn unix_seconds(timestamp: &ProtoTimestampV2) -> Result<u64, ProviderQueryError> {
    let value = parse_proto(timestamp)?;
    u64::try_from(value.timestamp()).map_err(|_| ProviderQueryError::InvalidCursor)
}

fn current_unix_seconds() -> Result<u64, ProviderQueryError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_secs()).ok())
        .ok_or(ProviderQueryError::InvalidCursor)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, NaiveDate, Utc};
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarResolutionV1, BarsProviderModeV1, BarsRequestV1,
        ProtoTimestampV2, SessionScopeV1,
    };

    use crate::{
        market_query::planning::{MarketBarsPlan, ProviderQueryGroup},
        session_router::{
            AlpacaFeed, CalendarVerificationStatus, DataInterval, PlannedSlice, PlannedSource,
            SessionRank, UtcWindow,
        },
    };

    use super::{ProviderCursor, select_page};

    fn fixture(as_of: &str) -> (BarsRequestV1, MarketBarsPlan) {
        let start = DateTime::parse_from_rfc3339("2026-10-13T13:30:00Z")
            .expect("valid fixture timestamp")
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-10-13T20:00:00Z")
            .expect("valid fixture timestamp")
            .with_timezone(&Utc);
        let timestamp = |value: DateTime<Utc>| {
            ProtoTimestampV2::parse(&value.to_rfc3339()).expect("valid Core timestamp")
        };
        let window = UtcWindow {
            start,
            end_exclusive: end,
        };
        let request = BarsRequestV1 {
            symbol: "AAPL".to_owned(),
            asset_class: AssetClassV1::Stock,
            start: timestamp(start),
            end: timestamp(end),
            resolution: BarResolutionV1::OneDay,
            session_scope: SessionScopeV1::Regular,
            adjustment: AdjustmentV1::Unadjusted,
            provider_mode: BarsProviderModeV1::FutuOnly,
            allow_daily_downgrade: false,
            limit: 10,
            cursor: None,
        };
        let slice = PlannedSlice {
            symbol: "AAPL".to_owned(),
            trade_date: NaiveDate::from_ymd_opt(2026, 10, 13).expect("valid fixture date"),
            session_id: "XNYS-2026-10-13".to_owned(),
            session_instance_id: Some("XNYS-2026-10-13-regular".to_owned()),
            phase: None,
            rank: Some(SessionRank::Old),
            interval: DataInterval::Daily,
            window,
            calendar_verification_status: CalendarVerificationStatus::Verified,
            source: Some(PlannedSource::Futu),
            source_label: Some("Futu".to_owned()),
            historical: true,
            realtime: false,
            provisional: false,
            gap: None,
        };
        let plan = MarketBarsPlan {
            request: request.clone(),
            as_of: ProtoTimestampV2::parse(as_of).expect("valid as-of timestamp"),
            requested_window: window,
            effective_resolution: BarResolutionV1::OneDay,
            calendar_source_id: "fixture-calendar".to_owned(),
            calendar_version: "fixture-v1".to_owned(),
            calendar_verification_status: Vec::new(),
            slices_recent_to_old: vec![slice.clone()],
            provider_groups: vec![ProviderQueryGroup {
                source: PlannedSource::Futu,
                resolution: BarResolutionV1::OneDay,
                adjustment: AdjustmentV1::Unadjusted,
                fetch_window: window,
                slices_recent_to_old: vec![slice],
            }],
            gaps: Vec::new(),
            provisional: false,
        };
        (request, plan)
    }

    #[test]
    fn cache_key_ignores_the_plan_clock_but_changes_with_provider_page_cursor() {
        let first_as_of = Utc::now();
        let later_as_of = first_as_of + Duration::seconds(1);
        let (request, first_plan) = fixture(&first_as_of.to_rfc3339());
        let first = select_page(&request, &first_plan).expect("first-page selection");

        let (request_after_clock, next_plan) = fixture(&later_as_of.to_rfc3339());
        let after_clock =
            select_page(&request_after_clock, &next_plan).expect("new plan selection");
        assert_eq!(first.cache_key(), after_clock.cache_key());

        let next_cursor = first
            .cursor_for_provider_page(ProviderCursor::Futu("a".repeat(32)))
            .expect("encode provider continuation");
        let mut next_request = request;
        next_request.cursor = Some(next_cursor);
        let next_page = select_page(&next_request, &first_plan).expect("continued page selection");
        assert_ne!(first.cache_key(), next_page.cache_key());
    }

    #[test]
    fn provider_cursor_stays_redacted_from_cache_identity_and_debug_output() {
        let (request, plan) = fixture("2026-10-14T19:00:00Z");
        let selection = select_page(&request, &plan).expect("first-page selection");
        let provider_cursor = "provider-page-secret-token-12345";
        let outer_cursor = selection
            .cursor_for_provider_page(ProviderCursor::Futu(provider_cursor.to_owned()))
            .expect("outer cursor");
        let cache_input =
            super::futu_cursor_cache_input(&selection, provider_cursor).expect("cache input");

        assert!(!outer_cursor.contains(provider_cursor));
        assert!(!selection.cache_key().contains(provider_cursor));
        assert!(!format!("{cache_input:?}").contains(provider_cursor));
    }

    #[test]
    fn exhausted_provider_page_advances_to_next_temporal_source_run() {
        let as_of = Utc::now().to_rfc3339();
        let (mut request, mut plan) = fixture(&as_of);
        request.provider_mode = market_contracts::BarsProviderModeV1::Auto;
        plan.request.provider_mode = request.provider_mode;

        let first_slice = plan.slices_recent_to_old[0].clone();
        let prior_start = first_slice.window.start - chrono::Duration::days(1);
        let prior_end = first_slice.window.end_exclusive - chrono::Duration::days(1);
        request.start =
            ProtoTimestampV2::parse(&prior_start.to_rfc3339()).expect("valid prior request start");
        plan.request.start = request.start.clone();
        plan.requested_window.start = prior_start;
        let mut prior_slice = first_slice.clone();
        prior_slice.trade_date = NaiveDate::from_ymd_opt(2026, 10, 12).expect("valid date");
        prior_slice.session_id = "XNYS-2026-10-12".to_owned();
        prior_slice.session_instance_id = Some("XNYS-2026-10-12-regular".to_owned());
        prior_slice.window = UtcWindow {
            start: prior_start,
            end_exclusive: prior_end,
        };
        prior_slice.source = Some(PlannedSource::Alpaca {
            feed: AlpacaFeed::Sip,
        });
        prior_slice.source_label = Some("Alpaca SIP".to_owned());

        let futu_group = plan.provider_groups[0].clone();
        let alpaca_group = ProviderQueryGroup {
            source: PlannedSource::Alpaca {
                feed: AlpacaFeed::Sip,
            },
            resolution: futu_group.resolution,
            adjustment: futu_group.adjustment,
            fetch_window: prior_slice.window,
            slices_recent_to_old: vec![prior_slice.clone()],
        };
        plan.slices_recent_to_old.push(prior_slice);
        plan.provider_groups = vec![futu_group, alpaca_group];

        let first = select_page(&request, &plan).expect("first temporal run");
        assert_eq!(first.group_index(), 0);
        assert_eq!(first.timeline_position(), 0);
        assert_eq!(first.provider_group().source, PlannedSource::Futu);

        request.cursor = Some(
            first
                .cursor_for_next_group()
                .expect("encode next temporal run")
                .expect("second run exists"),
        );
        let second = select_page(&request, &plan).expect("second temporal run");
        assert_eq!(second.group_index(), 1);
        assert_eq!(second.timeline_position(), 1);
        assert_eq!(
            second.provider_group().source,
            PlannedSource::Alpaca {
                feed: AlpacaFeed::Sip
            }
        );
        assert!(
            second
                .cursor_for_next_group()
                .expect("there is no third run")
                .is_none()
        );
    }
}
