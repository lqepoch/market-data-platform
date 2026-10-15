//! Backend-owned policy for provisional provider route attempts.
//!
//! This policy decides whether a configured adapter may be attempted for a planned slice. It does
//! not assert provider entitlement, actual feed, live/realtime transport, completeness, or cache
//! authority. Those facts remain separately evidenced by the provider response and A04 admission.

use crate::{
    market_query::planning::MarketBarsPlan,
    session_router::{DataInterval, PlannedSlice, PlannedSource, SessionPhase, SessionRank},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProviderAttemptPolicy {
    futu_history_attempts_configured: bool,
}

impl ProviderAttemptPolicy {
    pub(crate) const fn from_futu_gateway_configuration(configured: bool) -> Self {
        Self {
            futu_history_attempts_configured: configured,
        }
    }

    pub(crate) const fn futu_history_attempts_configured(self) -> bool {
        self.futu_history_attempts_configured
    }

    pub(crate) fn permits_futu_slices(self, slices: &[&PlannedSlice]) -> bool {
        self.futu_history_attempts_configured
            && !slices.is_empty()
            && slices.iter().all(|slice| {
                slice.source == Some(PlannedSource::Futu)
                    && slice.gap.is_none()
                    && slice.historical
                    && !slice.realtime
                    && matches!(slice.rank, Some(SessionRank::New | SessionRank::Buffer))
                    && supported_interval(slice.interval)
            })
    }

    pub(crate) fn permits_futu_capability(
        self,
        phase: Option<SessionPhase>,
        interval: DataInterval,
        slices: &[PlannedSlice],
    ) -> bool {
        let matching = slices
            .iter()
            .filter(|slice| {
                slice.source == Some(PlannedSource::Futu)
                    && slice.phase == phase
                    && slice.interval == interval
            })
            .collect::<Vec<_>>();
        self.permits_futu_slices(&matching)
    }

    pub(crate) fn permits_futu_plan(self, plan: &MarketBarsPlan) -> bool {
        let slices = plan
            .slices_recent_to_old
            .iter()
            .filter(|slice| slice.source == Some(PlannedSource::Futu) && slice.gap.is_none())
            .collect::<Vec<_>>();
        self.permits_futu_slices(&slices)
    }
}

fn supported_interval(interval: DataInterval) -> bool {
    matches!(
        interval,
        DataInterval::Daily
            | DataInterval::Minute(1)
            | DataInterval::Minute(5)
            | DataInterval::Minute(15)
            | DataInterval::Minute(60)
    )
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone, Utc};

    use crate::session_router::{
        CalendarVerificationStatus, DataInterval, PlannedSlice, PlannedSource, SessionPhase,
        SessionRank, UtcWindow,
    };

    use super::ProviderAttemptPolicy;

    fn futu_slice(rank: SessionRank) -> PlannedSlice {
        let start = Utc.with_ymd_and_hms(2026, 1, 5, 14, 30, 0).unwrap();
        PlannedSlice {
            symbol: "SPY".to_owned(),
            trade_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            session_id: "XNYS:2026-01-05".to_owned(),
            session_instance_id: Some(format!("XNYS:2026-01-05:{rank:?}")),
            phase: Some(SessionPhase::Regular),
            rank: Some(rank),
            interval: DataInterval::Minute(1),
            window: UtcWindow {
                start,
                end_exclusive: start + chrono::Duration::minutes(1),
            },
            calendar_verification_status: CalendarVerificationStatus::Verified,
            source: Some(PlannedSource::Futu),
            source_label: Some("Futu".to_owned()),
            historical: true,
            realtime: false,
            provisional: false,
            gap: None,
        }
    }

    #[test]
    fn only_explicitly_configured_historical_new_and_buffer_futu_attempts_are_permitted() {
        let new = futu_slice(SessionRank::New);
        let buffer = futu_slice(SessionRank::Buffer);
        let old = futu_slice(SessionRank::Old);
        let configured = ProviderAttemptPolicy::from_futu_gateway_configuration(true);
        let unconfigured = ProviderAttemptPolicy::from_futu_gateway_configuration(false);

        assert!(configured.permits_futu_slices(&[&new]));
        assert!(configured.permits_futu_slices(&[&new, &buffer]));
        assert!(!configured.permits_futu_slices(&[&old]));
        assert!(!configured.permits_futu_slices(&[]));
        assert!(!unconfigured.permits_futu_slices(&[&new]));
    }

    #[test]
    fn futu_only_source_mode_cannot_reuse_a_phase_capability_for_old_slices() {
        let policy = ProviderAttemptPolicy::from_futu_gateway_configuration(true);
        let mut new = futu_slice(SessionRank::New);
        let mut old = futu_slice(SessionRank::Old);
        old.session_instance_id = Some("XNYS:2026-01-02:regular".to_owned());
        old.trade_date = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        let candidate_slices = vec![new.clone(), old];

        assert!(!policy.permits_futu_capability(
            Some(SessionPhase::Regular),
            DataInterval::Minute(1),
            &candidate_slices,
        ));

        new.phase = Some(SessionPhase::Pre);
        let only_recent_phase = vec![new];
        assert!(policy.permits_futu_capability(
            Some(SessionPhase::Pre),
            DataInterval::Minute(1),
            &only_recent_phase,
        ));
    }

    #[test]
    fn policy_rejects_realtime_claims_and_non_adapter_intervals() {
        let policy = ProviderAttemptPolicy::from_futu_gateway_configuration(true);
        let mut realtime = futu_slice(SessionRank::New);
        realtime.realtime = true;
        let mut unsupported = futu_slice(SessionRank::New);
        unsupported.interval = DataInterval::Minute(2);

        assert!(!policy.permits_futu_slices(&[&realtime]));
        assert!(!policy.permits_futu_slices(&[&unsupported]));
    }
}
