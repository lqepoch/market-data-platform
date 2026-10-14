//! Deterministic source planning over a caller-supplied, verified US equity calendar snapshot.
//!
//! This module does not fetch a calendar, infer holidays, create provider clients, or establish
//! provider entitlement. Callers must load and verify the calendar snapshot and source
//! capabilities before invoking the planner. The snapshot supplies explicit New York day bounds
//! and UTC phase windows, so DST, holidays, and early closes remain owned by the calendar source.

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

const MAX_CALENDAR_DAYS: usize = 50_000;
const MAX_CALENDAR_INSTANCES: usize = MAX_CALENDAR_DAYS * 4;
const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const YFINANCE_DAILY_RESEARCH_LABEL: &str = "YFinance daily (research-only)";

/// NY trade-date cutoff for intraday Alpaca/Futu planning.
pub fn intraday_cutoff_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2016, 1, 1).expect("valid constant date")
}

/// The 2016-01-01 New York civil-day boundary expressed in UTC (EST, UTC-05:00).
pub fn intraday_cutoff_utc() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2016-01-01T05:00:00Z")
        .expect("valid constant timestamp")
        .with_timezone(&Utc)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    UsStock,
    UsEtf,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    Pre,
    Regular,
    After,
    Over,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataInterval {
    Daily,
    Minute(u16),
    Second(u16),
    Trades,
}

impl DataInterval {
    const fn is_daily(self) -> bool {
        matches!(self, Self::Daily)
    }

    fn validate(self) -> bool {
        match self {
            Self::Daily | Self::Trades => true,
            Self::Minute(value) | Self::Second(value) => value > 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceMode {
    Auto,
    AlpacaOnly,
    FutuOnly,
    Research,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Futu,
    Alpaca,
    YFinance,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaFeed {
    Sip,
    Boats,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannedSource {
    Futu,
    Alpaca { feed: AlpacaFeed },
    YFinanceDailyResearch,
}

impl PlannedSource {
    pub const fn provider(self) -> Provider {
        match self {
            Self::Futu => Provider::Futu,
            Self::Alpaca { .. } => Provider::Alpaca,
            Self::YFinanceDailyResearch => Provider::YFinance,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Futu => "Futu",
            Self::Alpaca {
                feed: AlpacaFeed::Sip,
            } => "Alpaca SIP",
            Self::Alpaca {
                feed: AlpacaFeed::Boats,
            } => "Alpaca BOATS",
            Self::YFinanceDailyResearch => YFINANCE_DAILY_RESEARCH_LABEL,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionRank {
    New,
    Buffer,
    Old,
}

/// A half-open UTC range. It is authoritative input from the calendar adapter.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UtcWindow {
    pub start: DateTime<Utc>,
    pub end_exclusive: DateTime<Utc>,
}

impl UtcWindow {
    fn is_valid(self) -> bool {
        self.start < self.end_exclusive
    }

    fn intersection(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end_exclusive = self.end_exclusive.min(other.end_exclusive);
        (start < end_exclusive).then_some(Self {
            start,
            end_exclusive,
        })
    }

    fn contains(self, instant: DateTime<Utc>) -> bool {
        self.start <= instant && instant < self.end_exclusive
    }
}

/// One NY calendar date. `day_window` is local midnight-to-midnight expressed in UTC and may be
/// 23, 24, or 25 hours across daylight-saving transitions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CalendarDayState {
    Open { session_id: String },
    Closed { reason: Option<String> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarDay {
    pub trade_date: NaiveDate,
    pub day_window: UtcWindow,
    pub state: CalendarDayState,
    pub verification_status: CalendarVerificationStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInstance {
    pub session_id: String,
    pub session_instance_id: String,
    pub trade_date: NaiveDate,
    pub phase: SessionPhase,
    pub window: UtcWindow,
}

/// Verified calendar data passed in by the service/API layer.
///
/// `days` must contain every New York date from `coverage_start` through the day before
/// `coverage_end_exclusive`, including explicit closed dates. Instances are only the phases
/// attested by the source, globally chronological. The planner checks bounds, sort order,
/// identities, and shape; source authenticity and version/hash verification belong to the loader.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedCalendarSnapshot {
    pub source_id: String,
    pub version: String,
    pub source_metadata: CalendarSourceMetadata,
    pub coverage_start: NaiveDate,
    pub coverage_end_exclusive: NaiveDate,
    pub days: Vec<CalendarDay>,
    pub instances: Vec<SessionInstance>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCapability {
    pub provider: Provider,
    pub feed: Option<AlpacaFeed>,
    /// `None` means a date-level daily request; otherwise the capability is phase-specific.
    pub phase: Option<SessionPhase>,
    pub interval: DataInterval,
    pub historical: bool,
    pub realtime: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct PlanRequest<'a> {
    pub symbol: &'a str,
    pub instrument: InstrumentKind,
    /// Caller-requested UTC half-open window.
    pub start_utc: DateTime<Utc>,
    pub end_utc_exclusive: DateTime<Utc>,
    pub as_of_utc: DateTime<Utc>,
    pub interval: DataInterval,
    pub source_mode: SourceMode,
    pub allow_daily_downgrade: bool,
    pub capabilities: &'a [SourceCapability],
    pub calendar: &'a TrustedCalendarSnapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DowngradeReason {
    Pre2016History,
    ResearchDailyOnly,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    FutureSession,
    SourceModeExcludesRequiredProvider,
    MissingProviderCapability,
    MissingCalendarPhase,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CalendarVerificationStatus {
    Verified,
    Provisional,
    ModeledWeekend,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarSourceMetadata {
    pub source_name: String,
    pub source_reference: String,
    pub generator_package: String,
    pub generator_version: String,
    pub valid_from: NaiveDate,
    pub valid_to: NaiveDate,
    pub official_verified_from: NaiveDate,
    pub official_verified_through: NaiveDate,
    pub verification_status: String,
    pub artifact_checksum: String,
    pub pinned_input_sha256: String,
    pub generated_at: String,
    pub source_git_commit: String,
    pub compiler_config_sha256: String,
    pub next_review_at: NaiveDate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedSlice {
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub session_id: String,
    /// Phase instance for intraday requests; latest relevant phase identity for a daily request.
    pub session_instance_id: Option<String>,
    pub phase: Option<SessionPhase>,
    pub rank: Option<SessionRank>,
    pub interval: DataInterval,
    pub window: UtcWindow,
    pub calendar_verification_status: CalendarVerificationStatus,
    /// Planned route, even when its explicit provider capability is missing.
    pub source: Option<PlannedSource>,
    pub source_label: Option<String>,
    pub historical: bool,
    /// True only when `as_of_utc` is inside this exact trusted phase and a live capability exists.
    pub realtime: bool,
    /// True for a live daily bar or a just-finished Futu window while the market is closed.
    pub provisional: bool,
    pub gap: Option<GapReason>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionPlan {
    pub symbol: String,
    pub instrument: InstrumentKind,
    pub requested_window: UtcWindow,
    pub requested_interval: DataInterval,
    pub effective_interval: DataInterval,
    pub daily_downgrade: bool,
    pub downgrade_reason: Option<DowngradeReason>,
    pub calendar_source_id: String,
    pub calendar_version: String,
    pub calendar_source_metadata: CalendarSourceMetadata,
    pub active_session_instance_id: Option<String>,
    pub slices: Vec<PlannedSlice>,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PlanError {
    #[error("symbol must be a nonempty canonical US ticker")]
    InvalidSymbol,
    #[error("requested UTC window must be nonempty and half-open")]
    InvalidWindow,
    #[error("interval must be positive")]
    InvalidInterval,
    #[error("calendar source id or version is invalid")]
    InvalidCalendarMetadata,
    #[error("calendar coverage is invalid or exceeds the configured bounds")]
    InvalidCalendarCoverage,
    #[error("calendar date rows are not contiguous or have invalid day windows")]
    InvalidCalendarDay,
    #[error("calendar phase instances are duplicated, overlapping, or unsorted")]
    InvalidSessionInstances,
    #[error("requested or as-of time lies outside calendar coverage")]
    OutsideCalendarCoverage,
    #[error("intraday data before 2016-01-01 NY is unsupported")]
    UnsupportedPre2016Intraday { first_trade_date: NaiveDate },
    #[error("research mode supports daily history only")]
    UnsupportedResearchIntraday,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CalendarArtifactError {
    #[error("expected SHA-256 must be 64 lowercase hexadecimal characters")]
    InvalidExpectedSha256,
    #[error("native XNYS snapshot exceeds the 16 MiB bound")]
    SnapshotTooLarge,
    #[error("native XNYS snapshot does not match the operator-pinned SHA-256")]
    SnapshotSha256Mismatch,
    #[error("native XNYS snapshot JSON is invalid: {0}")]
    InvalidJson(String),
    #[error("native snapshot does not retain supported XNYS source metadata")]
    UnsupportedSourceMetadata,
    #[error("native XNYS snapshot contains unsupported or fabricated phase identities")]
    UnsupportedPhaseSet,
    #[error("native XNYS snapshot is structurally invalid: {0}")]
    InvalidSnapshot(PlanError),
}

/// Parse an adapter-generated native snapshot after checking its operator-pinned exact bytes.
///
/// The offline `tools/xnys_snapshot_adapter.py` consumes the Stock XNYS artifact and uses the
/// standard-library New York timezone database to construct continuous civil-day bounds. This
/// function deliberately accepts that native snapshot, not the upstream artifact, so the runtime
/// never reconstructs timezone rules or imports private loader code.
pub fn parse_calendar_snapshot(
    bytes: &[u8],
    expected_sha256: &str,
) -> Result<TrustedCalendarSnapshot, CalendarArtifactError> {
    if !valid_sha256(expected_sha256) {
        return Err(CalendarArtifactError::InvalidExpectedSha256);
    }
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(CalendarArtifactError::SnapshotTooLarge);
    }
    let actual_sha256 = hex::encode(Sha256::digest(bytes));
    if actual_sha256 != expected_sha256 {
        return Err(CalendarArtifactError::SnapshotSha256Mismatch);
    }
    let snapshot: TrustedCalendarSnapshot = serde_json::from_slice(bytes)
        .map_err(|error| CalendarArtifactError::InvalidJson(error.to_string()))?;
    let metadata = &snapshot.source_metadata;
    if snapshot.source_id != "XNYS"
        || snapshot.version != "nyse.v1"
        || metadata.source_name != "NYSE Holidays & Trading Hours"
        || metadata.source_reference != "https://www.nyse.com/trade/hours-calendars"
        || metadata.generator_package != "pandas_market_calendars"
        || metadata.verification_status != "verified_through_official_fixture"
    {
        return Err(CalendarArtifactError::UnsupportedSourceMetadata);
    }
    validate_calendar_snapshot(&snapshot).map_err(CalendarArtifactError::InvalidSnapshot)?;
    let mut phases_by_session: BTreeMap<&str, BTreeSet<SessionPhase>> = BTreeMap::new();
    for instance in &snapshot.instances {
        phases_by_session
            .entry(instance.session_id.as_str())
            .or_default()
            .insert(instance.phase);
    }
    let expected_phases: BTreeSet<_> = [
        SessionPhase::Pre,
        SessionPhase::Regular,
        SessionPhase::After,
    ]
    .into_iter()
    .collect();
    for day in &snapshot.days {
        if let CalendarDayState::Open { session_id } = &day.state
            && phases_by_session.get(session_id.as_str()) != Some(&expected_phases)
        {
            return Err(CalendarArtifactError::UnsupportedPhaseSet);
        }
    }
    Ok(snapshot)
}

/// Produce a deterministic read plan without opening a provider or synthesizing calendar rows.
pub fn plan_sessions(request: PlanRequest<'_>) -> Result<SessionPlan, PlanError> {
    validate_request(&request)?;
    validate_calendar(request.calendar)?;
    let requested_window = UtcWindow {
        start: request.start_utc,
        end_exclusive: request.end_utc_exclusive,
    };
    let first_day = request
        .calendar
        .days
        .first()
        .ok_or(PlanError::InvalidCalendarCoverage)?;
    let last_day = request
        .calendar
        .days
        .last()
        .ok_or(PlanError::InvalidCalendarCoverage)?;
    let coverage_window = UtcWindow {
        start: first_day.day_window.start,
        end_exclusive: last_day.day_window.end_exclusive,
    };
    if requested_window.start < coverage_window.start
        || requested_window.end_exclusive > coverage_window.end_exclusive
        || request.as_of_utc < coverage_window.start
        || request.as_of_utc >= coverage_window.end_exclusive
    {
        return Err(PlanError::OutsideCalendarCoverage);
    }

    let selected_days: Vec<_> = request
        .calendar
        .days
        .iter()
        .filter_map(|day| Some((day, day.day_window.intersection(requested_window)?)))
        .collect();
    let selected_instances: Vec<_> = request
        .calendar
        .instances
        .iter()
        .filter_map(|instance| Some((instance, instance.window.intersection(requested_window)?)))
        .collect();
    let instances_by_session = instances_by_session(&request.calendar.instances);
    let pre_cutoff_day = (request.start_utc < intraday_cutoff_utc()).then(|| {
        request
            .calendar
            .days
            .iter()
            .find(|day| day.day_window.contains(request.start_utc))
            .map(|day| day.trade_date)
            .unwrap_or(request.start_utc.date_naive())
    });
    let research_intraday =
        request.source_mode == SourceMode::Research && !request.interval.is_daily();

    let (effective_interval, downgrade_reason) = if !request.interval.is_daily()
        && let Some(first_trade_date) = pre_cutoff_day
    {
        if !request.allow_daily_downgrade {
            return Err(PlanError::UnsupportedPre2016Intraday { first_trade_date });
        }
        (DataInterval::Daily, Some(DowngradeReason::Pre2016History))
    } else if research_intraday {
        if !request.allow_daily_downgrade {
            return Err(PlanError::UnsupportedResearchIntraday);
        }
        (
            DataInterval::Daily,
            Some(DowngradeReason::ResearchDailyOnly),
        )
    } else {
        (request.interval, None)
    };

    let rank_context = rank_context(request.as_of_utc, &request.calendar.instances);
    let mut slices = Vec::new();
    if effective_interval.is_daily() {
        for (day, window) in selected_days {
            let CalendarDayState::Open { session_id } = &day.state else {
                continue;
            };
            let session_instances = instances_by_session
                .get(session_id.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let reference = session_instances
                .iter()
                .copied()
                .filter(|instance| instance.window.start <= request.as_of_utc)
                .max_by_key(|instance| instance.window.start);
            let Some(reference) = reference else {
                let gap = if !session_instances.is_empty() {
                    GapReason::FutureSession
                } else {
                    GapReason::MissingCalendarPhase
                };
                slices.push(build_slice(
                    request,
                    SliceSpec {
                        trade_date: day.trade_date,
                        session_id,
                        session_instance_id: None,
                        phase: None,
                        rank: None,
                        interval: effective_interval,
                        window,
                        calendar_verification_status: day.verification_status,
                        source: Err(gap),
                        active: false,
                        market_closed: rank_context.active_index.is_none(),
                        daily: true,
                    },
                ));
                continue;
            };
            let rank = rank_context.rank(reference);
            let source = choose_source(
                request.source_mode,
                day.trade_date,
                effective_interval,
                None,
                rank,
            );
            slices.push(build_slice(
                request,
                SliceSpec {
                    trade_date: day.trade_date,
                    session_id,
                    session_instance_id: Some(&reference.session_instance_id),
                    phase: None,
                    rank,
                    interval: effective_interval,
                    window,
                    calendar_verification_status: day.verification_status,
                    source,
                    active: reference.window.contains(request.as_of_utc),
                    market_closed: rank_context.active_index.is_none(),
                    daily: true,
                },
            ));
        }
    } else {
        for (instance, window) in selected_instances {
            let rank = rank_context.rank(instance);
            let source = choose_source(
                request.source_mode,
                instance.trade_date,
                effective_interval,
                Some(instance.phase),
                rank,
            );
            slices.push(build_slice(
                request,
                SliceSpec {
                    trade_date: instance.trade_date,
                    session_id: &instance.session_id,
                    session_instance_id: Some(&instance.session_instance_id),
                    phase: Some(instance.phase),
                    rank,
                    interval: effective_interval,
                    window,
                    calendar_verification_status: calendar_verification_for(
                        request.calendar,
                        instance.trade_date,
                    )?,
                    source,
                    active: instance.window.contains(request.as_of_utc),
                    market_closed: rank_context.active_index.is_none(),
                    daily: false,
                },
            ));
        }
        for (day, requested_day_window) in &selected_days {
            let CalendarDayState::Open { session_id } = &day.state else {
                continue;
            };
            for window in uncovered_windows(*requested_day_window, &request.calendar.instances) {
                slices.push(build_slice(
                    request,
                    SliceSpec {
                        trade_date: day.trade_date,
                        session_id,
                        session_instance_id: None,
                        phase: None,
                        rank: None,
                        interval: effective_interval,
                        window,
                        calendar_verification_status: day.verification_status,
                        source: Err(GapReason::MissingCalendarPhase),
                        active: false,
                        market_closed: false,
                        daily: false,
                    },
                ));
            }
        }
    }

    slices.sort_by_key(|slice| (slice.window.start, slice.trade_date, slice.phase));
    Ok(SessionPlan {
        symbol: request.symbol.to_owned(),
        instrument: request.instrument,
        requested_window,
        requested_interval: request.interval,
        effective_interval,
        daily_downgrade: downgrade_reason.is_some(),
        downgrade_reason,
        calendar_source_id: request.calendar.source_id.clone(),
        calendar_version: request.calendar.version.clone(),
        calendar_source_metadata: request.calendar.source_metadata.clone(),
        active_session_instance_id: rank_context.active_index.map(|index| {
            request.calendar.instances[index]
                .session_instance_id
                .clone()
        }),
        slices,
    })
}

fn validate_request(request: &PlanRequest<'_>) -> Result<(), PlanError> {
    if request.symbol.is_empty()
        || request.symbol.trim() != request.symbol
        || request.symbol.len() > 32
        || !request
            .symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || b".-".contains(&byte))
    {
        return Err(PlanError::InvalidSymbol);
    }
    if request.start_utc >= request.end_utc_exclusive {
        return Err(PlanError::InvalidWindow);
    }
    if !request.interval.validate() {
        return Err(PlanError::InvalidInterval);
    }
    Ok(())
}

fn validate_calendar(calendar: &TrustedCalendarSnapshot) -> Result<(), PlanError> {
    let metadata = &calendar.source_metadata;
    if !valid_label(&calendar.source_id)
        || !valid_label(&calendar.version)
        || !valid_label(&metadata.source_name)
        || !valid_label(&metadata.source_reference)
        || !valid_label(&metadata.generator_package)
        || !valid_label(&metadata.generator_version)
        || !valid_label(&metadata.verification_status)
        || !valid_git_commit(&metadata.source_git_commit)
        || calendar.coverage_start < metadata.valid_from
        || calendar.coverage_end_exclusive
            > metadata.valid_to.succ_opt().unwrap_or(metadata.valid_to)
        || metadata.official_verified_from < metadata.valid_from
        || metadata.official_verified_through > metadata.valid_to
        || metadata.official_verified_from > metadata.official_verified_through
        || !valid_sha256(&metadata.artifact_checksum)
        || !valid_sha256(&metadata.pinned_input_sha256)
        || !valid_sha256(&metadata.compiler_config_sha256)
        || DateTime::parse_from_rfc3339(&metadata.generated_at).is_err()
        || metadata.next_review_at < metadata.valid_from
        || metadata.next_review_at > metadata.valid_to
    {
        return Err(PlanError::InvalidCalendarMetadata);
    }
    if calendar.coverage_start >= calendar.coverage_end_exclusive
        || calendar.days.is_empty()
        || calendar.days.len() > MAX_CALENDAR_DAYS
        || calendar.instances.len() > MAX_CALENDAR_INSTANCES
    {
        return Err(PlanError::InvalidCalendarCoverage);
    }
    let expected_day_count = calendar
        .coverage_end_exclusive
        .signed_duration_since(calendar.coverage_start)
        .num_days();
    if expected_day_count < 0
        || usize::try_from(expected_day_count).ok() != Some(calendar.days.len())
    {
        return Err(PlanError::InvalidCalendarCoverage);
    }

    let mut open_sessions = BTreeMap::new();
    let mut prior_day: Option<&CalendarDay> = None;
    let mut expected_date = calendar.coverage_start;
    for day in &calendar.days {
        let day_seconds = day
            .day_window
            .end_exclusive
            .signed_duration_since(day.day_window.start)
            .num_seconds();
        if day.trade_date != expected_date
            || !day.day_window.is_valid()
            || !matches!(day_seconds, 82_800 | 86_400 | 90_000)
        {
            return Err(PlanError::InvalidCalendarDay);
        }
        if let Some(prior) = prior_day
            && prior.day_window.end_exclusive != day.day_window.start
        {
            return Err(PlanError::InvalidCalendarDay);
        }
        match &day.state {
            CalendarDayState::Open { session_id } => {
                if !valid_label(session_id)
                    || open_sessions
                        .insert(session_id.as_str(), day.trade_date)
                        .is_some()
                {
                    return Err(PlanError::InvalidCalendarDay);
                }
            }
            CalendarDayState::Closed { reason } => {
                if reason.as_ref().is_some_and(|value| !valid_label(value)) {
                    return Err(PlanError::InvalidCalendarDay);
                }
            }
        }
        let is_weekend = matches!(
            day.trade_date.weekday(),
            chrono::Weekday::Sat | chrono::Weekday::Sun
        );
        if is_weekend
            != matches!(
                day.verification_status,
                CalendarVerificationStatus::ModeledWeekend
            )
            || (!is_weekend
                && (day.trade_date >= metadata.official_verified_from
                    && day.trade_date <= metadata.official_verified_through)
                    != matches!(
                        day.verification_status,
                        CalendarVerificationStatus::Verified
                    ))
            || (is_weekend && !matches!(day.state, CalendarDayState::Closed { .. }))
        {
            return Err(PlanError::InvalidCalendarDay);
        }
        prior_day = Some(day);
        expected_date = expected_date
            .succ_opt()
            .ok_or(PlanError::InvalidCalendarCoverage)?;
    }
    if expected_date != calendar.coverage_end_exclusive {
        return Err(PlanError::InvalidCalendarCoverage);
    }

    let coverage_window = UtcWindow {
        start: calendar.days[0].day_window.start,
        end_exclusive: calendar.days[calendar.days.len() - 1]
            .day_window
            .end_exclusive,
    };
    let mut ids = BTreeSet::new();
    let mut phases_by_session: BTreeMap<&str, BTreeSet<SessionPhase>> = BTreeMap::new();
    let mut prior_instance: Option<&SessionInstance> = None;
    for instance in &calendar.instances {
        if !valid_label(&instance.session_id)
            || !valid_label(&instance.session_instance_id)
            || !instance.window.is_valid()
            || !coverage_window.contains(instance.window.start)
            || instance.window.end_exclusive > coverage_window.end_exclusive
            || !ids.insert(instance.session_instance_id.as_str())
            || open_sessions.get(instance.session_id.as_str()) != Some(&instance.trade_date)
        {
            return Err(PlanError::InvalidSessionInstances);
        }
        if let Some(prior) = prior_instance
            && (prior.window.start >= instance.window.start
                || prior.window.end_exclusive > instance.window.start)
        {
            return Err(PlanError::InvalidSessionInstances);
        }
        let phases = phases_by_session
            .entry(instance.session_id.as_str())
            .or_default();
        if !phases.insert(instance.phase) {
            return Err(PlanError::InvalidSessionInstances);
        }
        prior_instance = Some(instance);
    }
    if phases_by_session
        .keys()
        .any(|session_id| !open_sessions.contains_key(session_id))
    {
        return Err(PlanError::InvalidSessionInstances);
    }
    Ok(())
}

/// Validate a caller-loaded snapshot before it is used as runtime calendar authority.
pub fn validate_calendar_snapshot(calendar: &TrustedCalendarSnapshot) -> Result<(), PlanError> {
    validate_calendar(calendar)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_git_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn instances_by_session(instances: &[SessionInstance]) -> BTreeMap<&str, Vec<&SessionInstance>> {
    let mut result: BTreeMap<&str, Vec<&SessionInstance>> = BTreeMap::new();
    for instance in instances {
        result
            .entry(instance.session_id.as_str())
            .or_default()
            .push(instance);
    }
    result
}

fn calendar_verification_for(
    calendar: &TrustedCalendarSnapshot,
    trade_date: NaiveDate,
) -> Result<CalendarVerificationStatus, PlanError> {
    calendar
        .days
        .binary_search_by_key(&trade_date, |day| day.trade_date)
        .ok()
        .map(|index| calendar.days[index].verification_status)
        .ok_or(PlanError::InvalidSessionInstances)
}

fn uncovered_windows(
    requested_day_window: UtcWindow,
    instances: &[SessionInstance],
) -> Vec<UtcWindow> {
    let mut uncovered = Vec::new();
    let first_relevant = instances
        .partition_point(|instance| instance.window.end_exclusive <= requested_day_window.start);
    let mut cursor = requested_day_window.start;
    for instance in &instances[first_relevant..] {
        if instance.window.start >= requested_day_window.end_exclusive {
            break;
        }
        let Some(intersection) = instance.window.intersection(requested_day_window) else {
            continue;
        };
        if intersection.start > cursor {
            uncovered.push(UtcWindow {
                start: cursor,
                end_exclusive: intersection.start,
            });
        }
        cursor = cursor.max(intersection.end_exclusive);
        if cursor >= requested_day_window.end_exclusive {
            return uncovered;
        }
    }
    if cursor < requested_day_window.end_exclusive {
        uncovered.push(UtcWindow {
            start: cursor,
            end_exclusive: requested_day_window.end_exclusive,
        });
    }
    uncovered
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        && value.trim() == value
}

struct RankContext<'a> {
    index_by_id: BTreeMap<&'a str, usize>,
    active_index: Option<usize>,
    last_completed_index: Option<usize>,
}

impl RankContext<'_> {
    fn rank(&self, instance: &SessionInstance) -> Option<SessionRank> {
        let index = *self
            .index_by_id
            .get(instance.session_instance_id.as_str())?;
        if let Some(active) = self.active_index {
            return match active.checked_sub(index) {
                Some(0) => Some(SessionRank::New),
                Some(1) => Some(SessionRank::Buffer),
                Some(_) => Some(SessionRank::Old),
                None => None,
            };
        }
        let completed = self.last_completed_index?;
        match completed.checked_sub(index) {
            Some(0) => Some(SessionRank::Buffer),
            Some(_) => Some(SessionRank::Old),
            None => None,
        }
    }
}

fn rank_context(as_of_utc: DateTime<Utc>, instances: &[SessionInstance]) -> RankContext<'_> {
    let active_index = instances
        .iter()
        .position(|instance| instance.window.contains(as_of_utc));
    let last_completed_index = instances
        .iter()
        .rposition(|instance| instance.window.end_exclusive <= as_of_utc);
    let index_by_id = instances
        .iter()
        .enumerate()
        .map(|(index, instance)| (instance.session_instance_id.as_str(), index))
        .collect();
    RankContext {
        index_by_id,
        active_index,
        last_completed_index,
    }
}

#[derive(Clone, Copy)]
struct SourceChoice {
    source: PlannedSource,
    required_provider: Provider,
    required_feed: Option<AlpacaFeed>,
}

fn choose_source(
    mode: SourceMode,
    trade_date: NaiveDate,
    interval: DataInterval,
    phase: Option<SessionPhase>,
    rank: Option<SessionRank>,
) -> Result<SourceChoice, GapReason> {
    let pre_cutoff = trade_date < intraday_cutoff_date();
    if rank.is_none() {
        return Err(GapReason::FutureSession);
    }
    if pre_cutoff && interval.is_daily() {
        return match mode {
            SourceMode::Auto | SourceMode::Research => Ok(SourceChoice {
                source: PlannedSource::YFinanceDailyResearch,
                required_provider: Provider::YFinance,
                required_feed: None,
            }),
            SourceMode::AlpacaOnly | SourceMode::FutuOnly => {
                Err(GapReason::SourceModeExcludesRequiredProvider)
            }
        };
    }
    if mode == SourceMode::Research {
        return Ok(SourceChoice {
            source: PlannedSource::YFinanceDailyResearch,
            required_provider: Provider::YFinance,
            required_feed: None,
        });
    }
    let provider = match mode {
        SourceMode::Auto => match rank {
            Some(SessionRank::New | SessionRank::Buffer) => Provider::Futu,
            Some(SessionRank::Old) => Provider::Alpaca,
            None => return Err(GapReason::FutureSession),
        },
        SourceMode::AlpacaOnly => Provider::Alpaca,
        SourceMode::FutuOnly => Provider::Futu,
        SourceMode::Research => unreachable!("research handled above"),
    };
    let feed = (provider == Provider::Alpaca).then_some(if phase == Some(SessionPhase::Over) {
        AlpacaFeed::Boats
    } else {
        AlpacaFeed::Sip
    });
    let source = match provider {
        Provider::Futu => PlannedSource::Futu,
        Provider::Alpaca => PlannedSource::Alpaca {
            feed: feed.expect("Alpaca feed is selected"),
        },
        Provider::YFinance => PlannedSource::YFinanceDailyResearch,
    };
    Ok(SourceChoice {
        source,
        required_provider: provider,
        required_feed: feed,
    })
}

#[derive(Clone, Copy)]
struct SliceSpec<'a> {
    trade_date: NaiveDate,
    session_id: &'a str,
    session_instance_id: Option<&'a str>,
    phase: Option<SessionPhase>,
    rank: Option<SessionRank>,
    interval: DataInterval,
    window: UtcWindow,
    calendar_verification_status: CalendarVerificationStatus,
    source: Result<SourceChoice, GapReason>,
    active: bool,
    market_closed: bool,
    daily: bool,
}

fn build_slice(request: PlanRequest<'_>, spec: SliceSpec<'_>) -> PlannedSlice {
    let source_choice = spec.source.ok();
    let source = source_choice.map(|choice| choice.source);
    let capability = source_choice.and_then(|choice| {
        request.capabilities.iter().find(|capability| {
            capability.provider == choice.required_provider
                && capability.feed == choice.required_feed
                && capability.phase == spec.phase
                && capability.interval == spec.interval
        })
    });
    let has_access = capability.is_some_and(|capability| {
        capability.historical
            || (spec.active && spec.window.contains(request.as_of_utc) && capability.realtime)
    });
    let gap = match spec.source {
        Err(reason) => Some(reason),
        Ok(_) if !has_access => Some(GapReason::MissingProviderCapability),
        Ok(_) => None,
    };
    let historical = capability.is_some_and(|capability| capability.historical);
    let realtime = source.is_some()
        && gap.is_none()
        && spec.active
        && spec.window.contains(request.as_of_utc)
        && capability.is_some_and(|capability| capability.realtime);
    let closed_recent_futu = spec.market_closed
        && spec.rank == Some(SessionRank::Buffer)
        && source == Some(PlannedSource::Futu);
    let live_daily_bar = spec.daily
        && spec.rank == Some(SessionRank::New)
        && source == Some(PlannedSource::Futu)
        && spec.active;
    PlannedSlice {
        symbol: request.symbol.to_owned(),
        trade_date: spec.trade_date,
        session_id: spec.session_id.to_owned(),
        session_instance_id: spec.session_instance_id.map(str::to_owned),
        phase: spec.phase,
        rank: spec.rank,
        interval: spec.interval,
        window: spec.window,
        calendar_verification_status: spec.calendar_verification_status,
        source,
        source_label: source.map(|source| source.label().to_owned()),
        historical,
        realtime,
        provisional: closed_recent_futu || live_daily_bar,
        gap,
    }
}

#[cfg(test)]
#[path = "session_router_tests.rs"]
mod tests;
