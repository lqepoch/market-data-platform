//! Domain-separated HMAC cursor for the provider-native Yahoo research endpoint.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{cursor::CursorSigner, error::ProviderQueryError};

pub(crate) const MAX_RESEARCH_CURSOR_BYTES: usize = 8 * 1024;
const CURSOR_TTL_SECS: u64 = 30 * 60;
const PAGE_DAYS: i64 = 3_650;
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResearchWindow {
    pub(crate) start_utc: DateTime<Utc>,
    pub(crate) end_utc: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct YFinanceResearchCursorClaims {
    version: u8,
    request_sha256: String,
    principal_scope_sha256: String,
    page_index: u32,
    page_start_utc: String,
    page_end_utc: String,
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
}

pub(crate) fn request_sha256(
    symbols: &[String],
    asset_class: market_contracts::AssetClassV1,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    adjustment: super::yfinance::YFinanceResearchAdjustment,
) -> String {
    let adjustment = match adjustment {
        super::yfinance::YFinanceResearchAdjustment::Unadjusted => "unadjusted",
        super::yfinance::YFinanceResearchAdjustment::Adjusted => "adjusted",
    };
    let asset_class = match asset_class {
        market_contracts::AssetClassV1::Stock => "stock",
        market_contracts::AssetClassV1::Etf => "etf",
    };
    let mut digest = Sha256::new();
    digest.update(b"mdp-yfinance-research-request-v1\0");
    digest_field(&mut digest, asset_class.as_bytes());
    digest_field(&mut digest, adjustment.as_bytes());
    digest_field(&mut digest, start_utc.to_rfc3339().as_bytes());
    digest_field(&mut digest, end_utc.to_rfc3339().as_bytes());
    digest.update((symbols.len() as u64).to_be_bytes());
    for symbol in symbols {
        digest_field(&mut digest, symbol.as_bytes());
    }
    hex::encode(digest.finalize())
}

pub(crate) fn newest_page(full_start: DateTime<Utc>, full_end: DateTime<Utc>) -> ResearchWindow {
    let page_start = full_end - Duration::days(PAGE_DAYS);
    ResearchWindow {
        start_utc: full_start.max(page_start),
        end_utc: full_end,
    }
}

pub(crate) fn previous_page(
    current: ResearchWindow,
    full_start: DateTime<Utc>,
) -> Option<ResearchWindow> {
    (current.start_utc > full_start).then(|| {
        let page_end = current.start_utc;
        ResearchWindow {
            start_utc: full_start.max(page_end - Duration::days(PAGE_DAYS)),
            end_utc: page_end,
        }
    })
}

pub(crate) fn select_window(
    signer: &CursorSigner,
    token: Option<&str>,
    request_sha256: &str,
    principal_scope_sha256: &str,
    full_start: DateTime<Utc>,
    full_end: DateTime<Utc>,
    now_unix_seconds: u64,
) -> Result<(ResearchWindow, u32), ProviderQueryError> {
    let Some(token) = token else {
        return Ok((newest_page(full_start, full_end), 0));
    };
    let claims = signer.verify_yfinance_research_cursor(token)?;
    if claims.version != VERSION
        || claims.request_sha256 != request_sha256
        || claims.principal_scope_sha256 != principal_scope_sha256
    {
        return Err(ProviderQueryError::CursorQueryMismatch);
    }
    if claims.issued_at_unix_seconds == 0
        || claims.issued_at_unix_seconds > now_unix_seconds
        || claims.expires_at_unix_seconds <= now_unix_seconds
        || claims.expires_at_unix_seconds <= claims.issued_at_unix_seconds
        || claims.expires_at_unix_seconds - claims.issued_at_unix_seconds != CURSOR_TTL_SECS
    {
        return Err(ProviderQueryError::CursorExpired);
    }
    if claims.page_index == 0 || claims.page_index > 9 {
        return Err(ProviderQueryError::InvalidCursor);
    }
    let page_start = parse_timestamp(&claims.page_start_utc)?;
    let page_end = parse_timestamp(&claims.page_end_utc)?;
    let expected_end = full_end - Duration::days(PAGE_DAYS * i64::from(claims.page_index));
    let expected_start = full_start.max(expected_end - Duration::days(PAGE_DAYS));
    if page_start != expected_start
        || page_end != expected_end
        || page_start < full_start
        || page_end > full_end
        || page_start >= page_end
    {
        return Err(ProviderQueryError::InvalidCursor);
    }
    Ok((
        ResearchWindow {
            start_utc: page_start,
            end_utc: page_end,
        },
        claims.page_index,
    ))
}

pub(crate) fn issue_cursor(
    signer: &CursorSigner,
    request_sha256: &str,
    principal_scope_sha256: &str,
    next_window: ResearchWindow,
    page_index: u32,
    now_unix_seconds: u64,
) -> Result<String, ProviderQueryError> {
    if !valid_digest(request_sha256)
        || !valid_digest(principal_scope_sha256)
        || !(1..=9).contains(&page_index)
        || next_window.start_utc >= next_window.end_utc
        || next_window
            .end_utc
            .signed_duration_since(next_window.start_utc)
            > Duration::days(PAGE_DAYS)
        || now_unix_seconds == 0
    {
        return Err(ProviderQueryError::InvalidCursor);
    }
    let claims = YFinanceResearchCursorClaims {
        version: VERSION,
        request_sha256: request_sha256.to_owned(),
        principal_scope_sha256: principal_scope_sha256.to_owned(),
        page_index,
        page_start_utc: next_window.start_utc.to_rfc3339(),
        page_end_utc: next_window.end_utc.to_rfc3339(),
        issued_at_unix_seconds: now_unix_seconds,
        expires_at_unix_seconds: now_unix_seconds.saturating_add(CURSOR_TTL_SECS),
    };
    signer.sign_yfinance_research_cursor(&claims)
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, ProviderQueryError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ProviderQueryError::InvalidCursor)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest_field(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, Utc};

    use super::{ResearchWindow, issue_cursor, newest_page, previous_page, select_window};
    use crate::market_query::providers::{
        cursor::CursorSigner, yfinance::YFinanceResearchAdjustment,
    };

    fn timestamp(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("valid fixture timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn newest_first_windows_are_contiguous_and_cover_100_year_bounds() {
        let full_start = timestamp("1970-01-01T00:00:00Z");
        let full_end = timestamp("2026-10-15T12:00:00Z");
        let newest = newest_page(full_start, full_end);
        assert_eq!(newest.end_utc, full_end);
        assert_eq!(newest.end_utc - newest.start_utc, Duration::days(3_650));
        let older = previous_page(newest, full_start).expect("older page exists");
        assert_eq!(older.end_utc, newest.start_utc);
        assert_eq!(older.end_utc - older.start_utc, Duration::days(3_650));

        let short = ResearchWindow {
            start_utc: full_start,
            end_utc: full_start + Duration::days(10),
        };
        assert!(previous_page(short, full_start).is_none());
    }

    #[test]
    fn signed_cursor_binds_request_principal_expiry_and_exact_older_window() {
        let signer = CursorSigner::for_test();
        let request_sha256 = "a".repeat(64);
        let principal_sha256 = "b".repeat(64);
        let full_start = timestamp("2010-01-01T00:00:00Z");
        let full_end = timestamp("2026-01-01T00:00:00Z");
        let first = newest_page(full_start, full_end);
        let next = previous_page(first, full_start).expect("older page exists");
        let token = issue_cursor(
            &signer,
            &request_sha256,
            &principal_sha256,
            next,
            1,
            1_800_000_000,
        )
        .expect("sign opaque research cursor");
        assert!(!token.contains("2010-01-01"));
        assert_eq!(
            select_window(
                &signer,
                Some(&token),
                &request_sha256,
                &principal_sha256,
                full_start,
                full_end,
                1_800_000_001,
            )
            .expect("verify cursor"),
            (next, 1)
        );
        assert!(
            select_window(
                &signer,
                Some(&token),
                &"c".repeat(64),
                &principal_sha256,
                full_start,
                full_end,
                1_800_000_001,
            )
            .is_err()
        );
        let replacement = if token.ends_with('0') { '1' } else { '0' };
        let tampered = format!("{}{}", &token[..token.len() - 1], replacement);
        assert!(
            select_window(
                &signer,
                Some(&tampered),
                &request_sha256,
                &principal_sha256,
                full_start,
                full_end,
                1_800_000_001,
            )
            .is_err()
        );
        assert!(
            select_window(
                &signer,
                Some(&token),
                &request_sha256,
                &"d".repeat(64),
                full_start,
                full_end,
                1_800_000_001,
            )
            .is_err()
        );
        assert!(
            select_window(
                &signer,
                Some(&token),
                &request_sha256,
                &principal_sha256,
                full_start,
                full_end,
                1_800_002_000,
            )
            .is_err()
        );
    }

    #[test]
    fn query_hash_binds_full_range_adjustment_and_symbol_order() {
        let symbols = vec!["AAPL".to_owned(), "SPY".to_owned()];
        let start = timestamp("2010-01-01T00:00:00Z");
        let end = timestamp("2026-01-01T00:00:00Z");
        let base = super::request_sha256(
            &symbols,
            market_contracts::AssetClassV1::Stock,
            start,
            end,
            YFinanceResearchAdjustment::Unadjusted,
        );
        assert_ne!(
            base,
            super::request_sha256(
                &symbols,
                market_contracts::AssetClassV1::Stock,
                start,
                end,
                YFinanceResearchAdjustment::Adjusted,
            )
        );
        assert_ne!(
            base,
            super::request_sha256(
                &["SPY".to_owned(), "AAPL".to_owned()],
                market_contracts::AssetClassV1::Stock,
                start,
                end,
                YFinanceResearchAdjustment::Unadjusted,
            )
        );
    }
}
