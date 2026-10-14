//! Startup calendar admission. Pinned cash input and signed provider calendars are not
//! market-data entitlement evidence; only a trusted owner may inject an overnight overlay.

use std::{
    fs::File,
    io::Read,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};

use crate::session_router::{TrustedCalendarSnapshot, parse_calendar_snapshot};
use crate::{
    archive::reconciliation::authority::TrustedProviderAdmissionVerifierV1,
    session_router::UtcWindow,
};

pub(crate) mod overnight_overlay;
#[cfg(test)]
pub(crate) use overnight_overlay::OvernightRouteFeedV1;
pub(crate) use overnight_overlay::{
    CalendarComponentsV1, CalendarSnapshotSelection, OvernightOverlayStatus, OvernightRouteV1,
};
use overnight_overlay::{VerifiedOvernightCalendarOverlay, verify_signed_overnight_overlay};

pub const MAX_MARKET_CALENDAR_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct LoadedMarketCalendar {
    snapshot: Arc<TrustedCalendarSnapshot>,
    input_sha256: String,
    cash_selection_snapshot: Arc<TrustedCalendarSnapshot>,
    overnight_overlay: OvernightOverlayState,
}

#[derive(Clone)]
enum OvernightOverlayState {
    NotConfigured,
    Rejected {
        status: OvernightOverlayStatus,
        input_sha256: Option<String>,
    },
    Verified {
        overlay: Arc<VerifiedOvernightCalendarOverlay>,
        verifier: Arc<dyn TrustedProviderAdmissionVerifierV1>,
    },
}

impl LoadedMarketCalendar {
    /// Both variables are operator startup configuration, never request parameters.
    /// An omitted calendar keeps market planning unavailable without affecting legacy archives.
    pub fn from_environment() -> crate::Result<Option<Self>> {
        match (
            std::env::var_os("MDP_MARKET_CALENDAR_FILE"),
            std::env::var("MDP_MARKET_CALENDAR_SHA256"),
        ) {
            (None, Err(std::env::VarError::NotPresent)) => Ok(None),
            (Some(path), Ok(expected)) => Self::load(Path::new(&path), &expected).map(Some),
            _ => Err(crate::MarketDataError::InvalidInput),
        }
    }

    pub fn load(path: &Path, expected_sha256: &str) -> crate::Result<Self> {
        if !canonical_sha256(expected_sha256) {
            return Err(crate::MarketDataError::InvalidInput);
        }
        let file = File::open(path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() > MAX_MARKET_CALENDAR_BYTES {
            return Err(crate::MarketDataError::InvalidInput);
        }
        let mut bytes = Vec::new();
        file.take(MAX_MARKET_CALENDAR_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_MARKET_CALENDAR_BYTES
            || hex::encode(Sha256::digest(&bytes)) != expected_sha256
        {
            return Err(crate::MarketDataError::InvalidInput);
        }
        // Stock's artifact must first pass the source-owned offline adapter. Runtime admission
        // accepts only its native, pinned snapshot; it never invents timezone or night sessions.
        let snapshot = parse_calendar_snapshot(&bytes, expected_sha256)
            .map_err(|_| crate::MarketDataError::InvalidInput)?;
        let cash_selection_snapshot = Arc::new(cash_snapshot_with_stable_version(
            &snapshot,
            expected_sha256,
        ));
        Ok(Self {
            snapshot: Arc::new(snapshot),
            input_sha256: expected_sha256.to_owned(),
            cash_selection_snapshot,
            overnight_overlay: OvernightOverlayState::NotConfigured,
        })
    }

    /// Installs an independently signed provider-specific OVER timetable. There is no
    /// environment, HTTP, or request path that can create this authority. Invalid input leaves
    /// the cash snapshot usable and is surfaced by `snapshot_for_instrument` as a status. The
    /// injected verifier must use an independently reviewed issuer/key policy for the
    /// `market-calendar:overnight` role; reusing A04's generic signature primitive does not grant
    /// provider-rights issuers calendar-authority by itself.
    pub(crate) fn with_signed_overnight_overlay(
        mut self,
        artifact_bytes: &[u8],
        expected_raw_sha256: &str,
        verifier: Arc<dyn TrustedProviderAdmissionVerifierV1>,
    ) -> Self {
        self.overnight_overlay = match verify_signed_overnight_overlay(
            artifact_bytes,
            expected_raw_sha256,
            self.raw_snapshot(),
            self.input_sha256(),
            verifier.as_ref(),
        ) {
            Ok(overlay) => OvernightOverlayState::Verified {
                overlay: Arc::new(overlay),
                verifier,
            },
            Err(failure) => OvernightOverlayState::Rejected {
                status: failure.status,
                input_sha256: failure.input_sha256,
            },
        };
        self
    }

    /// Selects a cash snapshot with any currently trusted, eligible, explicitly signed OVER
    /// instances. Current wall clock and trust policy are rechecked for every request.
    pub(crate) fn snapshot_for_instrument(
        &self,
        asset_class: market_contracts::AssetClassV1,
        symbol: &str,
        requested_window: UtcWindow,
    ) -> CalendarSnapshotSelection {
        let now_unix_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        self.snapshot_for_instrument_at_inner(
            asset_class,
            symbol,
            requested_window,
            now_unix_seconds,
        )
    }

    /// Returns the validated cash calendar with a version bound to its exact pinned input.
    /// Sessions requests have no instrument or requested window, but still need the same
    /// stable calendar identity used by bars selections and cursor/cache keys.
    pub(crate) fn cash_snapshot_for_selection(&self) -> Arc<TrustedCalendarSnapshot> {
        Arc::clone(&self.cash_selection_snapshot)
    }

    /// Read-only access to the original admitted snapshot, for provenance inspection only.
    pub(crate) fn raw_snapshot(&self) -> &TrustedCalendarSnapshot {
        &self.snapshot
    }

    /// Read-only exact native-input pin. Selections expose the same value in component metadata.
    pub(crate) fn input_sha256(&self) -> &str {
        &self.input_sha256
    }

    #[cfg(test)]
    pub(crate) fn snapshot_for_instrument_at(
        &self,
        asset_class: market_contracts::AssetClassV1,
        symbol: &str,
        requested_window: UtcWindow,
        now_unix_seconds: u64,
    ) -> CalendarSnapshotSelection {
        self.snapshot_for_instrument_at_inner(
            asset_class,
            symbol,
            requested_window,
            now_unix_seconds,
        )
    }

    fn snapshot_for_instrument_at_inner(
        &self,
        asset_class: market_contracts::AssetClassV1,
        symbol: &str,
        requested_window: UtcWindow,
        now_unix_seconds: u64,
    ) -> CalendarSnapshotSelection {
        use overnight_overlay::{CalendarComponentsV1, CashCalendarComponentV1};

        let cash = CashCalendarComponentV1 {
            source_id: self.snapshot.source_id.clone(),
            version: self.snapshot.version.clone(),
            input_sha256: self.input_sha256.clone(),
        };
        let cash_only = |status, input_sha256, overnight| CalendarSnapshotSelection {
            snapshot: self.cash_snapshot_for_selection(),
            status,
            allowed_overnight_routes: Vec::new(),
            components: CalendarComponentsV1 {
                cash: cash.clone(),
                overnight_input_sha256: input_sha256,
                overnight,
            },
        };
        match &self.overnight_overlay {
            OvernightOverlayState::NotConfigured => {
                cash_only(OvernightOverlayStatus::NoOverlay, None, None)
            }
            OvernightOverlayState::Rejected {
                status,
                input_sha256,
            } => cash_only(*status, input_sha256.clone(), None),
            OvernightOverlayState::Verified { overlay, verifier } => {
                let input_sha256 = Some(overlay.input_sha256().to_owned());
                let component = Some(overlay.component());
                if !overlay.signature_is_currently_trusted(verifier.as_ref()) {
                    return cash_only(
                        OvernightOverlayStatus::TrustRejected,
                        input_sha256,
                        component,
                    );
                }
                if let Some(status) = overlay.time_status(now_unix_seconds) {
                    return cash_only(status, input_sha256, component);
                }
                if !overlay.is_eligible(asset_class, symbol) {
                    return cash_only(OvernightOverlayStatus::NotEligible, input_sha256, component);
                }
                let Some(first_day) = self.snapshot.days.first() else {
                    return cash_only(
                        OvernightOverlayStatus::InvalidArtifact,
                        input_sha256,
                        component,
                    );
                };
                let Some(last_day) = self.snapshot.days.last() else {
                    return cash_only(
                        OvernightOverlayStatus::InvalidArtifact,
                        input_sha256,
                        component,
                    );
                };
                let cash_coverage = UtcWindow {
                    start: first_day.day_window.start,
                    end_exclusive: last_day.day_window.end_exclusive,
                };
                if requested_window.start >= requested_window.end_exclusive
                    || requested_window.start >= cash_coverage.end_exclusive
                    || requested_window.end_exclusive <= cash_coverage.start
                {
                    return cash_only(
                        OvernightOverlayStatus::NoWindowCoverage,
                        input_sha256,
                        component,
                    );
                }
                let Some(snapshot) = overlay.merged_snapshot(&self.snapshot, &self.input_sha256)
                else {
                    return cash_only(
                        OvernightOverlayStatus::InvalidArtifact,
                        input_sha256,
                        component,
                    );
                };
                CalendarSnapshotSelection {
                    snapshot: Arc::new(snapshot),
                    status: OvernightOverlayStatus::Applied,
                    allowed_overnight_routes: overlay.compatible_routes().to_vec(),
                    components: CalendarComponentsV1 {
                        cash,
                        overnight_input_sha256: input_sha256,
                        overnight: component,
                    },
                }
            }
        }
    }

    /// Explicit test-only cash fixture construction; production must use the pinned loader.
    #[cfg(test)]
    pub(crate) fn synthetic_cash_snapshot_for_tests(
        snapshot: TrustedCalendarSnapshot,
        input_sha256: impl Into<String>,
    ) -> Self {
        let input_sha256 = input_sha256.into();
        let cash_selection_snapshot =
            Arc::new(cash_snapshot_with_stable_version(&snapshot, &input_sha256));
        Self {
            snapshot: Arc::new(snapshot),
            input_sha256,
            cash_selection_snapshot,
            overnight_overlay: OvernightOverlayState::NotConfigured,
        }
    }
}

fn cash_snapshot_with_stable_version(
    snapshot: &TrustedCalendarSnapshot,
    input_sha256: &str,
) -> TrustedCalendarSnapshot {
    let mut snapshot = snapshot.clone();
    snapshot.version = overnight_overlay::stable_cash_calendar_version(&snapshot, input_sha256);
    snapshot
}

fn canonical_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unpinned_corrupted_and_oversized_calendars() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("calendar.json");
        std::fs::write(&path, b"{}").unwrap();
        assert!(LoadedMarketCalendar::load(&path, "").is_err());
        assert!(LoadedMarketCalendar::load(&path, &"a".repeat(64)).is_err());
        let malformed_hash = hex::encode(Sha256::digest(b"{}"));
        assert!(LoadedMarketCalendar::load(&path, &malformed_hash).is_err());
        File::create(&path)
            .unwrap()
            .set_len(MAX_MARKET_CALENDAR_BYTES + 1)
            .unwrap();
        assert!(LoadedMarketCalendar::load(&path, &malformed_hash).is_err());
    }
}
