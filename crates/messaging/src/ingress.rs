use std::error::Error;

use chrono::{DateTime, Utc};
use raddle::wire::{Envelope, EnvelopeLimits, WireError};

use crate::{BoxFuture, BusError, MessageBus, MessageContext, TrustedOrigin};

/// Typed pre-custody codec boundary. Implementations must decrypt, validate and
/// upcast as required BEFORE returning the canonical application envelope.
/// Retain the original message ID and canonical content across key/nonce changes.
/// Envelope headers and encrypted transport metadata never authenticate origin.
pub trait IngressDecoder: Send + Sync {
    /// Concrete codec failure, retained without converting it into handler retry state.
    type Error: Error + Send + Sync + 'static;

    /// Returns canonical, validated content, not encrypted bytes wrapped for storage.
    /// `origin` is independently authenticated application state, not decoded claims.
    fn decode<'a>(
        &'a self,
        bytes: &'a [u8],
        origin: &'a TrustedOrigin,
    ) -> BoxFuture<'a, Result<Envelope, Self::Error>>;
}

/// Plain validated JSON ingress. Encrypted inputs need an explicit decrypting
/// IngressDecoder instead; this type does not infer encryption from arbitrary headers.
pub struct JsonIngress {
    limits: EnvelopeLimits,
}
impl JsonIngress {
    /// Uses explicit framing/metadata bounds before dispatch or SQL custody.
    pub fn new(limits: EnvelopeLimits) -> Self {
        Self { limits }
    }
}
impl IngressDecoder for JsonIngress {
    type Error = WireError;
    fn decode<'a>(
        &'a self,
        bytes: &'a [u8],
        _: &'a TrustedOrigin,
    ) -> BoxFuture<'a, Result<Envelope, WireError>> {
        Box::pin(async move { Envelope::from_slice(bytes, self.limits) })
    }
}

#[cfg(feature = "serialization-ingress")]
impl IngressDecoder for raddle_serialization::EndpointCodecs {
    type Error = raddle_serialization::Error;

    fn decode<'a>(
        &'a self,
        bytes: &'a [u8],
        _: &'a TrustedOrigin,
    ) -> BoxFuture<'a, Result<Envelope, Self::Error>> {
        // Selected typed decoding and any explicit upcasting complete before
        // identity comparison, inbox custody or handler invocation. Decoded
        // metadata never supplies the independently authenticated origin.
        Box::pin(async move { self.decode(bytes) })
    }
}

#[cfg(feature = "crypto-ingress")]
impl<P: raddle_crypto::KeyProvider> IngressDecoder for raddle_crypto::EncryptionCodec<P> {
    type Error = raddle_crypto::CryptoError;

    fn decode<'a>(
        &'a self,
        bytes: &'a [u8],
        _: &'a TrustedOrigin,
    ) -> BoxFuture<'a, Result<Envelope, Self::Error>> {
        // Authentication and receive-required enforcement finish before dispatch
        // opens a custody transaction. Key/nonce changes are not inbox identity.
        Box::pin(self.decode_bytes(bytes))
    }
}

/// Codec rejection is separate from post-decode routing/custody failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IngressError<E: Error + 'static> {
    /// Nothing was admitted or staged by the bus; retain the typed codec failure.
    #[error("pre-custody message decoding failed")]
    Decode(#[source] E),
    /// Canonical envelope failed final validation.
    #[error("canonical message envelope failed validation")]
    Invalid(#[source] WireError),
    /// Canonical message reached routing or custody, which failed.
    #[error("decoded message admission failed")]
    Admission(#[source] BusError),
}

impl MessageBus {
    /// Applies the pre-custody decoder before any identity comparison, inbox write
    /// or route execution. Re-encrypted retries therefore compare canonical content.
    /// The caller owns broker acknowledgement and must not acknowledge failed custody.
    pub async fn receive<D: IngressDecoder>(
        &self,
        decoder: &D,
        bytes: &[u8],
        origin: TrustedOrigin,
        now: DateTime<Utc>,
        require_route: bool,
    ) -> Result<(), IngressError<D::Error>> {
        let envelope = decoder
            .decode(bytes, &origin)
            .await
            .map_err(IngressError::Decode)?;
        envelope
            .validate(EnvelopeLimits::default())
            .map_err(IngressError::Invalid)?;
        self.dispatch(MessageContext::new(origin, envelope, now), require_route)
            .await
            .map_err(IngressError::Admission)
    }
}
