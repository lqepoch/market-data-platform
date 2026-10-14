use std::sync::atomic::{AtomicBool, Ordering};

use aws_lc_rs::hmac;

use super::*;
use crate::archive::reconciliation::authority::AdmissionVerifyErrorV1;

// This fixed key and domain are compiled only into tests and convey no provider authority.
const TEST_SIGNATURE_DOMAIN: &[u8] = b"lqepoch.mdp.synthetic-overnight-calendar.test-hmac.v1\0";
const TEST_HMAC_KEY: &[u8] = b"synthetic-only-market-calendar-test-key-v1";
const TEST_SIGNATURE_PREFIX: &str = "synthetic-hmac-sha256-v1:";

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
        if self.revoked.load(Ordering::SeqCst) {
            return Err(AdmissionVerifyErrorV1::Rejected);
        }
        let Some(tag_hex) = signature_base64.strip_prefix(TEST_SIGNATURE_PREFIX) else {
            return Err(AdmissionVerifyErrorV1::Rejected);
        };
        let tag = hex::decode(tag_hex).map_err(|_| AdmissionVerifyErrorV1::Rejected)?;
        let authenticated_payload = test_signature_payload(canonical_payload);
        let key = hmac::Key::new(hmac::HMAC_SHA256, TEST_HMAC_KEY);
        hmac::verify(&key, &authenticated_payload, &tag)
            .map_err(|_| AdmissionVerifyErrorV1::Rejected)?;
        if issuer != "synthetic-test-issuer" || key_id != "synthetic-test-key" {
            return Err(AdmissionVerifyErrorV1::Rejected);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SyntheticOvernightWindow {
    pub(crate) trade_date: NaiveDate,
    pub(crate) session_id: String,
    pub(crate) session_instance_id: String,
    pub(crate) window: UtcWindow,
}

/// Test-only artifact whose tagged hex HMAC is accepted only by `SyntheticOverlayVerifier`.
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
        issuer: "synthetic-test-issuer".to_owned(),
        key_id: "synthetic-test-key".to_owned(),
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
        signature_base64: synthetic_signature_for_tests(&payload),
    };
    SyntheticOverlayArtifact {
        bytes: serde_json::to_vec(&artifact).expect("synthetic artifact serializes"),
        verifier: std::sync::Arc::new(SyntheticOverlayVerifier {
            revoked: AtomicBool::new(false),
        }),
    }
}

pub(crate) fn resign_with_identity_for_tests(bytes: &[u8], issuer: &str, key_id: &str) -> Vec<u8> {
    let mut artifact: SignedOvernightCalendarArtifactV1 =
        serde_json::from_slice(bytes).expect("synthetic artifact parses");
    artifact.claims.issuer = issuer.to_owned();
    artifact.claims.key_id = key_id.to_owned();
    let payload = canonical_signed_payload(&artifact.claims).expect("synthetic claims serialize");
    artifact.signature_base64 = synthetic_signature_for_tests(&payload);
    serde_json::to_vec(&artifact).expect("synthetic artifact serializes")
}

fn synthetic_signature_for_tests(payload: &[u8]) -> String {
    let authenticated_payload = test_signature_payload(payload);
    let key = hmac::Key::new(hmac::HMAC_SHA256, TEST_HMAC_KEY);
    let tag = hmac::sign(&key, &authenticated_payload);
    format!("{TEST_SIGNATURE_PREFIX}{}", hex::encode(tag.as_ref()))
}

fn test_signature_payload(payload: &[u8]) -> Vec<u8> {
    let mut authenticated_payload = Vec::with_capacity(TEST_SIGNATURE_DOMAIN.len() + payload.len());
    authenticated_payload.extend_from_slice(TEST_SIGNATURE_DOMAIN);
    authenticated_payload.extend_from_slice(payload);
    authenticated_payload
}
