use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::archive::reconciliation::authority::AdmissionVerifyErrorV1;

pub(crate) struct SyntheticOverlayVerifier {
    revoked: AtomicBool,
}

impl SyntheticOverlayVerifier {
    pub(crate) fn set_revoked(&self, revoked: bool) {
        self.revoked.store(revoked, Ordering::SeqCst);
    }
}

impl TrustedProviderAdmissionVerifierV1 for SyntheticOverlayVerifier {
    fn verify_signature(
        &self,
        issuer: &str,
        key_id: &str,
        canonical_payload: &[u8],
        signature_base64: &str,
    ) -> Result<(), AdmissionVerifyErrorV1> {
        let expected = format!("synthetic-overlay:{}", sha256(canonical_payload));
        if self.revoked.load(Ordering::SeqCst)
            || issuer != "synthetic-overlay-issuer"
            || key_id != "synthetic-overlay-key"
            || signature_base64 != expected
        {
            Err(AdmissionVerifyErrorV1::Rejected)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SyntheticOvernightWindow {
    pub(crate) trade_date: NaiveDate,
    pub(crate) session_id: String,
    pub(crate) session_instance_id: String,
    pub(crate) window: UtcWindow,
}

pub(crate) struct SyntheticOverlayArtifact {
    pub(crate) bytes: Vec<u8>,
    pub(crate) verifier: std::sync::Arc<SyntheticOverlayVerifier>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn signed_overlay_for_tests(
    cash: &TrustedCalendarSnapshot,
    cash_input_sha256: &str,
    source_provider: Provider,
    source_feed: OvernightRouteFeedV1,
    compatible_routes: Vec<OvernightRouteV1>,
    eligible_asset_classes: Vec<AssetClassV1>,
    eligible_symbols: Vec<String>,
    windows: Vec<SyntheticOvernightWindow>,
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    revoked_at_unix_seconds: Option<u64>,
) -> SyntheticOverlayArtifact {
    let claims = SignedOvernightCalendarClaimsV1 {
        schema_id: OVERLAY_SCHEMA_ID.to_owned(),
        authority_role: "market-calendar:overnight".to_owned(),
        issuer: "synthetic-overlay-issuer".to_owned(),
        key_id: "synthetic-overlay-key".to_owned(),
        issued_at_unix_seconds,
        expires_at_unix_seconds,
        revoked_at_unix_seconds,
        content: OvernightCalendarContentV1 {
            cash_source_id: cash.source_id.clone(),
            cash_version: cash.version.clone(),
            cash_input_sha256: cash_input_sha256.to_owned(),
            source_provider,
            source_feed,
            calendar_source_id: "synthetic-overnight-calendar".to_owned(),
            calendar_version: "synthetic.v1".to_owned(),
            calendar_source_reference: "https://example.invalid/overnight-calendar".to_owned(),
            timezone: NEW_YORK_TIMEZONE.to_owned(),
            compatible_routes,
            eligible_asset_classes,
            eligible_symbols,
            windows: windows
                .into_iter()
                .map(|window| OvernightWindowV1 {
                    trade_date: window.trade_date,
                    session_id: window.session_id,
                    session_instance_id: window.session_instance_id,
                    start_utc: window
                        .window
                        .start
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                    end_exclusive_utc: window
                        .window
                        .end_exclusive
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                })
                .collect(),
        },
    };
    let payload = canonical_signed_payload(&claims).expect("synthetic claims serialize");
    let artifact = SignedOvernightCalendarArtifactV1 {
        claims,
        signature_base64: format!("synthetic-overlay:{}", sha256(&payload)),
    };
    SyntheticOverlayArtifact {
        bytes: serde_json::to_vec(&artifact).expect("synthetic artifact serializes"),
        verifier: std::sync::Arc::new(SyntheticOverlayVerifier {
            revoked: AtomicBool::new(false),
        }),
    }
}
