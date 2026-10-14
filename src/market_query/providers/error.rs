use thiserror::Error;

/// Redaction-safe errors emitted by the read-only provider executor.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderQueryError {
    #[error("provider is disabled")]
    ProviderDisabled,
    #[error("provider credentials are unavailable")]
    MissingCredentials,
    #[error("invalid provider configuration")]
    InvalidConfiguration,
    #[error("Gateway delegation is required")]
    GatewayDelegationMissing,
    #[error("provider rejected authentication")]
    Unauthorized,
    #[error("provider denied access")]
    Forbidden,
    #[error("provider rate limit exceeded")]
    RateLimited,
    #[error("provider request timed out")]
    Timeout,
    #[error("provider is unavailable")]
    ProviderUnavailable,
    #[error("provider response is malformed")]
    MalformedResponse,
    #[error("provider response exceeded the configured bound")]
    ResponseTooLarge,
    #[error("provider page limit was reached before exhaustion")]
    PartialPageLimit,
    #[error("provider cursor is invalid")]
    InvalidCursor,
    #[error("provider cursor expired")]
    CursorExpired,
    #[error("provider cursor does not match the query")]
    CursorQueryMismatch,
    #[error("provider cursor plan changed")]
    CursorPlanChanged,
    #[error("provider does not support the requested adjustment")]
    UnsupportedAdjustment,
    #[error("provider adjustment semantics are unverified")]
    AdjustmentSemanticsUnverified,
    #[error("provider group is unavailable")]
    ProviderGroupUnavailable,
    #[error("provider capability is unavailable")]
    ProviderCapabilityUnavailable,
    #[error("provider page failed Core validation")]
    InvalidCorePage,
}

/// Configuration failures safe to return during service initialization.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderConfigError {
    #[error("provider credentials must be configured as a pair")]
    IncompleteAlpacaCredentials,
    #[error("invalid provider configuration")]
    InvalidValue,
    #[error("provider client could not be initialized")]
    ClientInitialization,
    #[error("market cursor signing key could not be generated")]
    CursorKeyGeneration,
}
