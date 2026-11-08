//! AES-256-GCM payload encryption with application-owned asynchronous keys.
//!
//! Native framing is versioned independently from .NET Wolverine framing.
//! Native framing authenticates the complete immutable envelope metadata,
//! key ID and inner content type. Integrity does not establish authorization,
//! tenant membership or freshness; exact replay still needs inbox deduplication.

mod codec;
mod keys;
mod transport;

pub use codec::{CryptoError, EncryptionCodec, EncryptionPolicy, Selection};
pub use keys::{KeyId, KeyMaterial, KeyProvider, KeyProviderError, RotatingKeys, SecretKey};
pub use transport::{EncryptedSendError, EncryptedTransport};
