//! Explicit typed body codecs and pre-custody canonical projections.
//!
//! No runtime type activation, `Any` payloads or foreign library identities.
//! Bodies are encoded from registered Rust models, not from a JSON value passed
//! off as a universal binary schema. See the repository serialization guide for
//! allocation trust, framing and the deliberately limited compatibility surface.

mod bounds;
mod canonical;
pub mod cloudevents;
mod codecs;
mod http_attributes;
mod protection;
mod registry;
mod strict_json;

pub use bounds::Limits;
pub use codecs::{BodyCodec, Json, MessagePack, MessagePackLayout, Protobuf};
pub use registry::{EndpointCodecs, Registry};

/// Payload-free codec/mapping failure. No failed message has acquired custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Encoded, canonical, declared-length, depth, node or header bound exceeded.
    #[error("serialization limit exceeded")]
    Limits,
    /// Malformed, truncated, duplicate or conflicting wire data.
    #[error("invalid serialized message")]
    Malformed,
    /// No explicitly registered media type, contract, schema or event alias.
    #[error("unsupported serialized contract")]
    Unsupported,
    /// Duplicate registration or invalid local configuration.
    #[error("invalid serialization configuration")]
    Configuration,
    /// Trusted application projection cannot represent this model losslessly.
    #[error("message cannot be represented by the canonical contract")]
    Projection,
    /// This plaintext-only body profile cannot compose with a protected envelope.
    /// Decrypt/authenticate through the separately configured ingress boundary;
    /// applications must reject unsupported combined configuration at startup.
    #[error("selected body codec and protected-envelope composition is unsupported")]
    UnsupportedProtection,
    /// Untrusted external metadata attempted to populate a reserved field.
    #[error("reserved external metadata")]
    Reserved,
}
