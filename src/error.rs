use thiserror::Error;

pub type Result<T, E = MarketDataError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum MarketDataError {
    #[error("invalid input or configuration")]
    InvalidInput,
    #[error("provider request failed ({0})")]
    Provider(ProviderFailure),
    #[error("market data contract validation failed")]
    Contract,
    #[error("Parquet operation failed")]
    Parquet,
    #[error("Parquet physical schema does not match the trusted core descriptor")]
    ParquetSchema,
    #[error("storage operation failed ({0})")]
    Storage(StorageFailure),
    #[error("publication outcome is unknown; reconciliation is required")]
    UnknownOutcome,
    #[error("immutable object conflicts with an existing object")]
    Conflict,
    #[error("bounded ingestion queue is full")]
    QueueFull,
    #[error("bounded collection channel is closed")]
    QueueClosed,
    #[error("gap ledger reached its configured maximum size")]
    GapLedgerFull,
    #[error("provider/feed source cardinality exceeded the configured bound")]
    SourceLimit,
    #[error("bounded collection writer worker limit reached")]
    WriterLimit,
    #[error("collection writer was poisoned after an unreconciled failure")]
    WriterPoisoned,
    #[error("dataset publication lacks authorized source or exact numeric evidence")]
    PublicationNotAuthorized,
    #[error("raw market frame exceeds the shared size bound")]
    FrameTooLarge,
    #[error("input exceeded the configured byte or record budget")]
    InputLimit,
    #[error("market-data window is incomplete or lacks required evidence")]
    IncompleteWindow,
    #[error("mixed provider provenance in one immutable dataset")]
    MixedProvenance,
    #[error("event symbol is outside the requested dataset")]
    UnexpectedSymbol,
    #[error("event type is not supported by the requested transform")]
    UnsupportedEvent,
    #[error("exact decimal aggregation overflowed the shared range")]
    DecimalOverflow,
    #[error("publisher lock is already held")]
    LockHeld,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderFailure {
    MissingCredentials,
    Unauthorized,
    Forbidden,
    RateLimited,
    HttpStatus(u16),
    Transport,
    InvalidResponse,
}

impl std::fmt::Display for ProviderFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCredentials => f.write_str("missing_credentials"),
            Self::Unauthorized => f.write_str("unauthorized"),
            Self::Forbidden => f.write_str("forbidden"),
            Self::RateLimited => f.write_str("rate_limited"),
            Self::HttpStatus(code) => write!(f, "http_status_{code}"),
            Self::Transport => f.write_str("transport"),
            Self::InvalidResponse => f.write_str("invalid_response"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageFailure {
    Spawn,
    CommandFailed,
    MalformedListing,
    ReadbackFailed,
    InvalidManifest,
    ReceiptFailed,
}

impl std::fmt::Display for StorageFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn => f.write_str("spawn"),
            Self::CommandFailed => f.write_str("command_failed"),
            Self::MalformedListing => f.write_str("malformed_listing"),
            Self::ReadbackFailed => f.write_str("readback_failed"),
            Self::InvalidManifest => f.write_str("invalid_manifest"),
            Self::ReceiptFailed => f.write_str("receipt_failed"),
        }
    }
}
