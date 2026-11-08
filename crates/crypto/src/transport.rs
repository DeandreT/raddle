use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;

use crate::{CryptoError, EncryptionCodec, KeyProvider};

/// Confirmed-publication wrapper. Encryption finishes before the underlying
/// transport sees any bytes; success retains that transport's confirm meaning,
/// not handler completion. Constructing another wrapper does not mutate core.
pub struct EncryptedTransport<T, P> {
    transport: T,
    codec: EncryptionCodec<P>,
}
impl<T: Transport, P: KeyProvider> EncryptedTransport<T, P> {
    /// Owns both collaborators; application decides their lifetime/rotation.
    pub fn new(transport: T, codec: EncryptionCodec<P>) -> Self {
        Self { transport, codec }
    }
    /// Returns the owned collaborators for explicit transport shutdown.
    pub fn into_parts(self) -> (T, EncryptionCodec<P>) {
        (self.transport, self.codec)
    }
    /// Receiver-side codec paired with this wrapper; run before dispatch.
    pub async fn receive(&self, wire: &[u8]) -> Result<Envelope, CryptoError> {
        self.codec.decode_bytes(wire).await
    }
    /// Encodes before publishing while retaining detailed crypto errors for
    /// applications that distinguish key failures from transport refusal.
    pub async fn publish_encrypted(
        &self,
        destination: &str,
        envelope: &Envelope,
    ) -> Result<(), EncryptedSendError> {
        let wire = self
            .codec
            .encode(envelope)
            .await
            .map_err(EncryptedSendError::Cryptography)?;
        self.transport
            .publish(destination, &wire)
            .await
            .map_err(EncryptedSendError::Transport)
    }
}
/// Detailed wrapper failure, without payload or key material.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum EncryptedSendError {
    /// No publication began.
    #[error("message encryption failed: {0}")]
    Cryptography(CryptoError),
    /// Underlying confirmed publication failed; acceptance may be uncertain.
    #[error("encrypted publication failed: {0}")]
    Transport(TransportError),
}
impl<T: Transport, P: KeyProvider> Transport for EncryptedTransport<T, P> {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.publish_encrypted(destination, envelope)
            .await
            .map_err(|error| match error {
                EncryptedSendError::Cryptography(_) => TransportError::Rejected,
                EncryptedSendError::Transport(error) => error,
            })
    }
}
