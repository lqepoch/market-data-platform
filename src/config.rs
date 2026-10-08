//! Runtime configuration. Provider credentials and SDK endpoints are owned by broker-connectors.

use std::{fmt, path::PathBuf, time::Duration};

pub const DEFAULT_DRIVE_OPERATION_TIMEOUT: Duration = Duration::from_secs(30 * 60);
pub const MAX_DRIVE_OPERATION_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaFeed {
    Sip,
    Opra,
}

impl AlpacaFeed {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sip => "sip",
            Self::Opra => "opra",
        }
    }
}

#[derive(Clone)]
pub struct DriveConfig {
    pub remote: String,
    pub root_folder_id: String,
    pub rclone_config: PathBuf,
    pub dataset_prefix: String,
    /// Wall-clock limit for one rclone operation, independent of rclone's idle timeout.
    pub operation_timeout: Duration,
}

impl fmt::Debug for DriveConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DriveConfig")
            .field("remote", &"configured")
            .field("root_folder_id", &"redacted")
            .field("rclone_config", &"redacted")
            .field("dataset_prefix", &self.dataset_prefix)
            .field("operation_timeout", &self.operation_timeout)
            .finish()
    }
}

impl DriveConfig {
    pub fn validate(&self) -> crate::Result<()> {
        let valid_remote = !self.remote.is_empty()
            && self.remote.len() <= 64
            && self
                .remote
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
        let valid_root = !self.root_folder_id.is_empty()
            && self.root_folder_id.len() <= 256
            && self
                .root_folder_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
        let valid_prefix = !self.dataset_prefix.is_empty()
            && self.dataset_prefix.len() <= 128
            && self
                .dataset_prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
        if !valid_remote
            || !valid_root
            || !valid_prefix
            || !self.rclone_config.is_file()
            || self.operation_timeout.is_zero()
            || self.operation_timeout > MAX_DRIVE_OPERATION_TIMEOUT
        {
            return Err(crate::MarketDataError::InvalidInput);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rclone_identifiers_reject_path_and_argument_injection() {
        let config = DriveConfig {
            remote: "drive:arbitrary".into(),
            root_folder_id: "root".into(),
            rclone_config: PathBuf::from("/not-read-by-validation"),
            dataset_prefix: "mdp".into(),
            operation_timeout: DEFAULT_DRIVE_OPERATION_TIMEOUT,
        };
        assert!(config.validate().is_err());
        let config = DriveConfig {
            remote: "drive".into(),
            dataset_prefix: "../escape".into(),
            ..config
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn primary_market_feeds_are_explicit() {
        assert_eq!(AlpacaFeed::Sip.as_str(), "sip");
        assert_eq!(AlpacaFeed::Opra.as_str(), "opra");
    }

    #[test]
    fn drive_debug_hides_remote_root_and_config_location() {
        let config = DriveConfig {
            remote: "customer-private-remote".into(),
            root_folder_id: "private-root-id-marker".into(),
            rclone_config: PathBuf::from("/private/customer/rclone.conf"),
            dataset_prefix: "mdp".into(),
            operation_timeout: DEFAULT_DRIVE_OPERATION_TIMEOUT,
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("customer-private-remote"));
        assert!(!debug.contains("private-root-id-marker"));
        assert!(!debug.contains("rclone.conf"));
    }

    #[test]
    fn drive_operation_deadline_must_be_finite_and_bounded() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut config = DriveConfig {
            remote: "drive".into(),
            root_folder_id: "root".into(),
            rclone_config: file.path().to_path_buf(),
            dataset_prefix: "mdp".into(),
            operation_timeout: Duration::from_secs(1),
        };
        assert!(config.validate().is_ok());
        config.operation_timeout = Duration::ZERO;
        assert!(config.validate().is_err());
        config.operation_timeout = MAX_DRIVE_OPERATION_TIMEOUT + Duration::from_secs(1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn drive_debug_exposes_deadline_but_redacts_configuration_location() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let config = DriveConfig {
            remote: "drive".into(),
            root_folder_id: "root".into(),
            rclone_config: file.path().to_path_buf(),
            dataset_prefix: "mdp".into(),
            operation_timeout: Duration::from_secs(4321),
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("4321s"));
        assert!(!debug.contains(file.path().to_string_lossy().as_ref()));
    }
}
