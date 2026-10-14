//! Bounded provider-neutral, per-session reconciliation index.
//!
//! Immutable raw/canonical cache versions retain their exact provider query windows and hashes.
//! This small index maps a stable rights/instrument/calendar/schema partition to complete
//! session slices so a merged provider response can be compared with one session from another
//! provider. Every candidate session is compared before the single index manifest is replaced.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    CacheAuthorityV1, CacheKeyV1, CacheReconciliationError, CoverageEvidenceV1, Result,
    SameFeedComparisonV1, SameFeedTolerancePolicyV1, canonical_rows_from_bars,
    compare_same_feed_authoritative, hex_digest, valid_dimension, valid_sha256,
};

const LOGICAL_IDENTITY_DOMAIN: &[u8] = b"lqepoch.mdp.cache-logical-partition-identity.v1\0";
/// One bounded index can cover multiple years of daily sessions while remaining small enough to
/// read and replace atomically. Candidates that exceed this cap retain their immutable cache
/// version and report logical-index overflow without changing existing session pointers.
pub(crate) const MAX_LOGICAL_SESSIONS: usize = 2048;
pub(crate) const MAX_LOGICAL_INDEX_BYTES: usize = 4 * 1024 * 1024;

/// Provider-neutral identity shared by source pages for the same authorized instrument query.
/// Provider, feed, entitlement realm, request range, and cursor deliberately stay out of this
/// key so a trusted source can reconcile a different feed's complete session slice. The rights
/// profile is supplied by the verified HTTP principal/operator scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheLogicalIdentityV1 {
    pub rights_profile_sha256: String,
    pub asset_class: String,
    pub symbol: String,
    pub requested_resolution: String,
    pub effective_resolution: String,
    pub adjustment: String,
    pub market_calendar_version: Option<String>,
    pub schema_version: u32,
    pub schema_id: String,
    pub schema_fingerprint_sha256: String,
}

impl CacheLogicalIdentityV1 {
    pub(crate) fn from_key(key: &CacheKeyV1) -> Result<Self> {
        key.validate()?;
        let identity = Self {
            rights_profile_sha256: key.rights_profile_sha256.clone(),
            asset_class: key.asset_class.clone(),
            symbol: key.symbol.clone(),
            requested_resolution: key.requested_resolution.clone(),
            effective_resolution: key.effective_resolution.clone(),
            adjustment: key.adjustment.clone(),
            market_calendar_version: key.market_calendar_version.clone(),
            schema_version: key.schema_version,
            schema_id: key.schema_id.clone(),
            schema_fingerprint_sha256: key.schema_fingerprint_sha256.clone(),
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn matches_key(&self, key: &CacheKeyV1) -> Result<bool> {
        key.validate()?;
        Ok(self.rights_profile_sha256 == key.rights_profile_sha256
            && self.asset_class == key.asset_class
            && self.symbol == key.symbol
            && self.requested_resolution == key.requested_resolution
            && self.effective_resolution == key.effective_resolution
            && self.adjustment == key.adjustment
            && self.market_calendar_version == key.market_calendar_version
            && self.schema_version == key.schema_version
            && self.schema_id == key.schema_id
            && self.schema_fingerprint_sha256 == key.schema_fingerprint_sha256)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if !valid_sha256(&self.rights_profile_sha256)
            || !matches!(self.asset_class.as_str(), "stock" | "etf")
            || !valid_dimension(&self.symbol)
            || !valid_dimension(&self.requested_resolution)
            || !valid_dimension(&self.effective_resolution)
            || !valid_dimension(&self.adjustment)
            || self
                .market_calendar_version
                .as_deref()
                .is_some_and(|value| !valid_dimension(value))
            || self.schema_version == 0
            || !valid_dimension(&self.schema_id)
            || !valid_sha256(&self.schema_fingerprint_sha256)
        {
            return Err(CacheReconciliationError::InvalidInput);
        }
        Ok(())
    }

    pub(crate) fn fingerprint(&self) -> Result<String> {
        self.validate()?;
        let payload = serde_json::to_vec(self)?;
        let mut digest = Sha256::new();
        digest.update(LOGICAL_IDENTITY_DOMAIN);
        digest.update(payload);
        Ok(hex_digest(digest.finalize().as_slice()))
    }
}

/// Immutable source version reference for one complete session slice inside a larger query.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheLogicalVersionRefV1 {
    pub key: CacheKeyV1,
    pub key_fingerprint_sha256: String,
    pub version_id: String,
    pub receipt_sha256: String,
    pub session_instance_id: String,
    pub authority: CacheAuthorityV1,
    pub covered_start_utc: String,
    pub covered_end_exclusive_utc: String,
    pub created_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
}

pub(crate) struct CacheLogicalVersionSourceV1<'a> {
    pub key: &'a CacheKeyV1,
    pub key_fingerprint_sha256: &'a str,
    pub version_id: &'a str,
    pub receipt_sha256: &'a str,
    pub authority: CacheAuthorityV1,
    pub created_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
    pub coverage: &'a CoverageEvidenceV1,
}

impl CacheLogicalVersionRefV1 {
    pub(crate) fn validate(&self, identity: &CacheLogicalIdentityV1, session: &str) -> Result<()> {
        self.key.validate()?;
        let start = parse_utc(&self.covered_start_utc)?;
        let end = parse_utc(&self.covered_end_exclusive_utc)?;
        let key_start = parse_utc(&self.key.window_start_utc)?;
        let key_end = parse_utc(&self.key.window_end_exclusive_utc)?;
        if !identity.matches_key(&self.key)?
            || self.session_instance_id != session
            || start >= end
            || start < key_start
            || end > key_end
            || self.key.fingerprint()? != self.key_fingerprint_sha256
            || !valid_dimension(&self.session_instance_id)
            || !valid_sha256(&self.version_id)
            || !valid_sha256(&self.receipt_sha256)
            || self.expires_at_unix_seconds <= self.created_at_unix_seconds
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(())
    }
}

/// Evidence for one source's complete canonical data in a specific session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheLogicalComparisonV1 {
    pub session_instance_id: String,
    pub compared_start_utc: String,
    pub compared_end_exclusive_utc: String,
    pub baseline_provider: String,
    pub baseline_feed: String,
    pub baseline_version_id: String,
    pub baseline_authority: CacheAuthorityV1,
    pub candidate_provider: String,
    pub candidate_feed: String,
    pub candidate_version_id: String,
    pub candidate_authority: CacheAuthorityV1,
    pub kind: CacheLogicalComparisonKindV1,
    pub summary: SameFeedComparisonV1,
}

impl CacheLogicalComparisonV1 {
    pub(crate) fn validate(&self) -> Result<()> {
        let start = parse_utc(&self.compared_start_utc)?;
        let end = parse_utc(&self.compared_end_exclusive_utc)?;
        if !valid_dimension(&self.session_instance_id)
            || start >= end
            || !valid_dimension(&self.baseline_provider)
            || !valid_dimension(&self.baseline_feed)
            || !valid_sha256(&self.baseline_version_id)
            || !valid_dimension(&self.candidate_provider)
            || !valid_dimension(&self.candidate_feed)
            || !valid_sha256(&self.candidate_version_id)
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CacheLogicalComparisonKindV1 {
    SameFeed,
    CrossProvider,
}

#[derive(Clone, Copy)]
pub(crate) struct LogicalSessionSnapshotRefV1<'a> {
    pub key: &'a CacheKeyV1,
    pub authority: CacheAuthorityV1,
    pub version_id: &'a str,
    pub coverage: &'a CoverageEvidenceV1,
    pub rows: &'a [market_contracts::BarRowV1],
}

/// Pure result of comparing every existing session in one candidate before the store mutates a
/// current pointer. Conflicts are collected so the caller can persist one rejected immutable
/// candidate and leave the whole old manifest untouched.
pub(crate) struct CacheLogicalBatchComparisonV1 {
    pub comparisons: BTreeMap<String, CacheLogicalComparisonV1>,
    pub incomplete_sessions: BTreeSet<String>,
    pub conflict_sessions: BTreeSet<String>,
    pub cross_provider_conflict_sessions: BTreeSet<String>,
}

pub(crate) fn compare_session_batch(
    identity: &CacheLogicalIdentityV1,
    candidate: LogicalSessionSnapshotRefV1<'_>,
    baselines: &[(String, LogicalSessionSnapshotRefV1<'_>)],
    same_feed_policy: &SameFeedTolerancePolicyV1,
    cross_provider_policy: &SameFeedTolerancePolicyV1,
) -> Result<CacheLogicalBatchComparisonV1> {
    if baselines.len() > MAX_LOGICAL_SESSIONS {
        return Err(CacheReconciliationError::LimitExceeded);
    }
    let mut result = CacheLogicalBatchComparisonV1 {
        comparisons: BTreeMap::new(),
        incomplete_sessions: BTreeSet::new(),
        conflict_sessions: BTreeSet::new(),
        cross_provider_conflict_sessions: BTreeSet::new(),
    };
    for (session, baseline) in baselines {
        let same_feed = baseline.key.provider == candidate.key.provider
            && baseline.key.feed == candidate.key.feed;
        let policy = if same_feed {
            same_feed_policy
        } else {
            cross_provider_policy
        };
        match compare_session_overlap(identity, session, *baseline, candidate, policy) {
            Ok(Some(comparison)) => {
                result.comparisons.insert(session.clone(), comparison);
            }
            Ok(None) => {
                result.incomplete_sessions.insert(session.clone());
            }
            Err(CacheReconciliationError::Conflict) => {
                result.conflict_sessions.insert(session.clone());
                if !same_feed {
                    result
                        .cross_provider_conflict_sessions
                        .insert(session.clone());
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(result)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheLogicalSessionCurrentV1 {
    pub current: CacheLogicalVersionRefV1,
    pub reconciliation_policy_sha256: String,
    pub comparison: Option<CacheLogicalComparisonV1>,
}

impl CacheLogicalSessionCurrentV1 {
    pub(crate) fn validate(&self, identity: &CacheLogicalIdentityV1, session: &str) -> Result<()> {
        self.current.validate(identity, session)?;
        if !valid_sha256(&self.reconciliation_policy_sha256) {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        if let Some(comparison) = self.comparison.as_ref() {
            comparison.validate()?;
            if comparison.session_instance_id != session {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
        }
        Ok(())
    }
}

/// Complete logical index partition. All session replacements for one candidate are serialized
/// into this bounded manifest and committed by one atomic rename.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheLogicalCurrentV1 {
    pub identity: CacheLogicalIdentityV1,
    pub generation: u64,
    pub sessions: BTreeMap<String, CacheLogicalSessionCurrentV1>,
    pub updated_at_unix_seconds: u64,
}

impl CacheLogicalCurrentV1 {
    pub(crate) fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if self.generation == 0
            || self.sessions.is_empty()
            || self.sessions.len() > MAX_LOGICAL_SESSIONS
        {
            return Err(CacheReconciliationError::UnknownOutcome);
        }
        for (session, entry) in &self.sessions {
            if !valid_dimension(session) {
                return Err(CacheReconciliationError::UnknownOutcome);
            }
            entry.validate(&self.identity, session)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CacheLogicalSessionChangeV1 {
    pub session_instance_id: String,
    pub invalidated_version: Option<CacheLogicalVersionRefV1>,
    pub current_version: CacheLogicalVersionRefV1,
}

/// Invalidation covers every changed session in one atomic logical-index commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CacheLogicalInvalidationV1 {
    pub identity_fingerprint_sha256: String,
    pub changes: Vec<CacheLogicalSessionChangeV1>,
    pub generation: u64,
    pub updated_at_unix_seconds: u64,
}

/// A checksummed pointer and its re-read, immutable source version for one session.
#[derive(Clone, Debug)]
pub(crate) struct CacheLogicalReadResultV1 {
    pub logical_current: CacheLogicalCurrentV1,
    pub session: CacheLogicalSessionCurrentV1,
    pub snapshot: super::CacheReadResultV1,
}

/// Return a complete, contiguous canonical range for one real session. Partial, gapped, empty,
/// overlapping, or disjoint segments cannot seed or advance the logical index.
pub(crate) fn session_coverage_bounds(
    coverage: &CoverageEvidenceV1,
    session_instance_id: &str,
) -> Result<
    Option<(
        market_contracts::UtcTimestamp,
        market_contracts::UtcTimestamp,
    )>,
> {
    let mut ranges = coverage
        .segments
        .iter()
        .filter(|segment| segment.session_instance_id == session_instance_id)
        .map(|segment| {
            let start = parse_utc(&segment.covered_start_utc)?;
            let end = parse_utc(&segment.covered_end_exclusive_utc)?;
            Ok((start, end, segment.coverage_status))
        })
        .collect::<Result<Vec<_>>>()?;
    if ranges.is_empty()
        || ranges
            .iter()
            .any(|(_, _, status)| *status != market_contracts::BarsCoverageStatusV1::Complete)
    {
        return Ok(None);
    }
    ranges.sort_by(|left, right| left.0.cmp(&right.0));
    for pair in ranges.windows(2) {
        if pair[0].1 != pair[1].0 {
            return Ok(None);
        }
    }
    let start = ranges
        .first()
        .map(|range| range.0.clone())
        .ok_or(CacheReconciliationError::UnknownOutcome)?;
    let end = ranges
        .last()
        .map(|range| range.1.clone())
        .ok_or(CacheReconciliationError::UnknownOutcome)?;
    Ok(Some((start, end)))
}

pub(crate) fn version_ref_for_session(
    source: CacheLogicalVersionSourceV1<'_>,
    session_instance_id: &str,
) -> Result<Option<CacheLogicalVersionRefV1>> {
    let Some((start, end)) = session_coverage_bounds(source.coverage, session_instance_id)? else {
        return Ok(None);
    };
    let reference = CacheLogicalVersionRefV1 {
        key: source.key.clone(),
        key_fingerprint_sha256: source.key_fingerprint_sha256.to_owned(),
        version_id: source.version_id.to_owned(),
        receipt_sha256: source.receipt_sha256.to_owned(),
        session_instance_id: session_instance_id.to_owned(),
        authority: source.authority,
        covered_start_utc: start.as_str().to_owned(),
        covered_end_exclusive_utc: end.as_str().to_owned(),
        created_at_unix_seconds: source.created_at_unix_seconds,
        expires_at_unix_seconds: source.expires_at_unix_seconds,
    };
    reference.validate(
        &CacheLogicalIdentityV1::from_key(source.key)?,
        session_instance_id,
    )?;
    Ok(Some(reference))
}

/// Compare exact rows only within the shared half-open interval of complete segments for one
/// session. Source rows that straddle the comparison boundary are rejected rather than clipped.
pub(crate) fn compare_session_overlap(
    identity: &CacheLogicalIdentityV1,
    session_instance_id: &str,
    baseline: LogicalSessionSnapshotRefV1<'_>,
    candidate: LogicalSessionSnapshotRefV1<'_>,
    policy: &SameFeedTolerancePolicyV1,
) -> Result<Option<CacheLogicalComparisonV1>> {
    if !identity.matches_key(baseline.key)?
        || !identity.matches_key(candidate.key)?
        || session_source_mismatch(baseline, session_instance_id)
        || session_source_mismatch(candidate, session_instance_id)
    {
        return Err(CacheReconciliationError::AdmissionMismatch);
    }
    let Some((baseline_start, baseline_end)) =
        session_coverage_bounds(baseline.coverage, session_instance_id)?
    else {
        return Ok(None);
    };
    let Some((candidate_start, candidate_end)) =
        session_coverage_bounds(candidate.coverage, session_instance_id)?
    else {
        return Ok(None);
    };
    let start = if baseline_start > candidate_start {
        baseline_start
    } else {
        candidate_start
    };
    let end = if baseline_end < candidate_end {
        baseline_end
    } else {
        candidate_end
    };
    if start >= end {
        return Ok(None);
    }
    let baseline_rows = session_rows_in_range(baseline.rows, session_instance_id, &start, &end)?;
    let candidate_rows = session_rows_in_range(candidate.rows, session_instance_id, &start, &end)?;
    if baseline_rows.is_empty() || candidate_rows.is_empty() {
        return Ok(None);
    }
    let summary = compare_same_feed_authoritative(&baseline_rows, &candidate_rows, policy)?;
    let same_feed =
        baseline.key.provider == candidate.key.provider && baseline.key.feed == candidate.key.feed;
    let comparison = CacheLogicalComparisonV1 {
        session_instance_id: session_instance_id.to_owned(),
        compared_start_utc: start.as_str().to_owned(),
        compared_end_exclusive_utc: end.as_str().to_owned(),
        baseline_provider: baseline.key.provider.clone(),
        baseline_feed: baseline.key.feed.clone(),
        baseline_version_id: baseline.version_id.to_owned(),
        baseline_authority: baseline.authority,
        candidate_provider: candidate.key.provider.clone(),
        candidate_feed: candidate.key.feed.clone(),
        candidate_version_id: candidate.version_id.to_owned(),
        candidate_authority: candidate.authority,
        kind: if same_feed {
            CacheLogicalComparisonKindV1::SameFeed
        } else {
            CacheLogicalComparisonKindV1::CrossProvider
        },
        summary,
    };
    comparison.validate()?;
    Ok(Some(comparison))
}

fn session_source_mismatch(
    snapshot: LogicalSessionSnapshotRefV1<'_>,
    session_instance_id: &str,
) -> bool {
    snapshot
        .coverage
        .segments
        .iter()
        .filter(|segment| segment.session_instance_id == session_instance_id)
        .any(|segment| {
            segment.provider != snapshot.key.provider
                || segment.feed != snapshot.key.feed
                || segment.entitlement != snapshot.key.entitlement
        })
}

fn session_rows_in_range(
    rows: &[market_contracts::BarRowV1],
    session_instance_id: &str,
    range_start: &market_contracts::UtcTimestamp,
    range_end: &market_contracts::UtcTimestamp,
) -> Result<Vec<super::CanonicalRowV1>> {
    let mut selected = Vec::new();
    for row in rows
        .iter()
        .filter(|row| row.session_instance_id == session_instance_id)
    {
        let start = parse_utc(row.bar_start_utc.as_str())?;
        let end = parse_utc(row.bar_end_exclusive_utc.as_str())?;
        if start < *range_end && end > *range_start {
            if start < *range_start || end > *range_end {
                return Err(CacheReconciliationError::Incomplete);
            }
            selected.push(row.clone());
        }
    }
    canonical_rows_from_bars(&selected)
}

fn parse_utc(value: &str) -> Result<market_contracts::UtcTimestamp> {
    market_contracts::UtcTimestamp::parse_for_arrow_nanoseconds(value)
        .map_err(|_| CacheReconciliationError::UnknownOutcome)
}
