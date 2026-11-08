/// An invalid adapter registration or policy, never an untrusted payload echo.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigurationError {
    /// A principal identity or permission alias was malformed or exceeded a bound.
    #[error("invalid principal or permission")]
    InvalidPrincipal,
    /// A wire contract name/version was invalid or already registered.
    #[error("invalid or duplicate RPC contract")]
    InvalidContract,
    /// The registry, concurrency or duration budget exceeded supported bounds.
    #[error("invalid RPC budget")]
    InvalidBudget,
}

/// A typed client call failed. Serialization diagnostics never expose payload data.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The bearer token, contract or timeout was invalid.
    #[error("invalid RPC client input")]
    InvalidInput,
    /// Request serialization failed without exposing its error text.
    #[error("request serialization failed")]
    Serialization,
    /// The request exceeded the message/JSON budget before transmission.
    #[error("RPC message size limit exceeded")]
    TooLarge,
    /// The remote reply did not match the expected JSON reply schema.
    #[error("invalid RPC response JSON")]
    InvalidResponse,
    /// Standard gRPC status, including deadline, authorization and admission errors.
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
}
