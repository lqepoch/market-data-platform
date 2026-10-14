//! Startup-only calendar admission. A pinned file is not market-data entitlement evidence.

use std::{fs::File, io::Read, path::Path, sync::Arc};

use sha2::{Digest, Sha256};

use crate::session_router::{TrustedCalendarSnapshot, parse_calendar_snapshot};

pub const MAX_MARKET_CALENDAR_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct LoadedMarketCalendar {
    pub snapshot: Arc<TrustedCalendarSnapshot>,
    pub input_sha256: String,
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
        Ok(Self {
            snapshot: Arc::new(snapshot),
            input_sha256: expected_sha256.to_owned(),
        })
    }
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
