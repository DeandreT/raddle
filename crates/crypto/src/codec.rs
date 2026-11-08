use std::collections::BTreeSet;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use raddle::wire::{Envelope, EnvelopeLimits};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use zeroize::Zeroizing;

use crate::{KeyId, KeyMaterial, KeyProvider};

const MARKER: &str = "raddle-aes256gcm-v2";
const CONTENT_TYPE: &str = "application/json";

/// Pre-handler crypto errors have a payload-free surface. Receivers should
/// dead-letter directly, bypassing ordinary handler retries and terminal faults.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CryptoError {
    /// Unknown/unavailable key or mismatched provider response.
    #[error("encryption key unavailable")]
    KeyUnavailable,
    /// Invalid authentication tag, authenticated metadata or wrong key.
    #[error("message authentication failed")]
    Authentication,
    /// Malformed or unsupported framing/content type.
    #[error("invalid encrypted message framing")]
    Framing,
    /// Encoded or plaintext byte/header limit exceeded.
    #[error("message exceeds encryption limits")]
    Limits,
    /// Plaintext arrived for a receive-required contract.
    #[error("encrypted message is required")]
    Required,
    /// Operating-system nonce generation failed; publication never begins.
    #[error("secure randomness unavailable")]
    Randomness,
    /// Attempt to encrypt an already encrypted wire copy.
    #[error("message encryption already applied")]
    AlreadyEncrypted,
}

/// Explicit global/selective opt-in, independently configured for send/receive.
#[derive(Clone, Debug, Default)]
pub enum Selection {
    /// Encryption is not selected.
    #[default]
    None,
    /// Every contract requires payload protection.
    All,
    /// These stable message contracts. Because native faults share one wire
    /// contract, any nonempty selection also protects all native faults.
    Contracts(BTreeSet<String>),
}
impl Selection {
    fn matches(&self, contract: &str) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::Contracts(names) => {
                names.contains(contract) || (contract == "raddle.fault" && !names.is_empty())
            }
        }
    }
}

/// Immutable headers, correlation and replay identity are authenticated. This
/// policy still does not establish authorization or trusted tenant membership.
#[derive(Clone, Debug, Default)]
pub struct EncryptionPolicy {
    /// Opt-in outgoing protection.
    pub send: Selection,
    /// Reject plaintext before handler decoding for selected contracts.
    pub receive_required: Selection,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EncryptedBody {
    format: String,
    key_id: String,
    inner_content_type: String,
    nonce: String,
    ciphertext: String,
    // Fault pairing must be decidable before decrypting plaintext. It is
    // authenticated with the message type and verified against the inner body.
    fault_source: Option<String>,
}

/// Caller-owned codec. No mutable configuration, global setup or key cache.
pub struct EncryptionCodec<P> {
    provider: P,
    policy: EncryptionPolicy,
    limits: EnvelopeLimits,
}
impl<P: KeyProvider> EncryptionCodec<P> {
    /// Constructs one immutable encryption layer with explicit wire limits.
    pub fn new(provider: P, policy: EncryptionPolicy, limits: EnvelopeLimits) -> Self {
        Self {
            provider,
            policy,
            limits,
        }
    }

    /// Makes a structured validated wire copy. Never mutates the live local
    /// envelope, so failures/retries cannot accidentally replay ciphertext.
    pub async fn encode(&self, envelope: &Envelope) -> Result<Envelope, CryptoError> {
        envelope
            .validate(self.limits)
            .map_err(|_| CryptoError::Limits)?;
        let mut value = serde_json::to_value(envelope).map_err(|_| CryptoError::Framing)?;
        let payload = value.get("payload").ok_or(CryptoError::Framing)?;
        if has_marker(payload) {
            return Err(CryptoError::AlreadyEncrypted);
        }
        let fault_source = fault_source(envelope.contract(), payload).map(str::to_owned);
        if !self.policy.send.matches(envelope.contract()) {
            return Ok(envelope.clone());
        }
        let material = self
            .provider
            .active()
            .await
            .map_err(|_| CryptoError::KeyUnavailable)?;
        let plaintext =
            Zeroizing::new(serde_json::to_vec(payload).map_err(|_| CryptoError::Framing)?);
        let mut nonce = [0; 12];
        // A fresh 96-bit OS-random nonce per encryption. This is probabilistic
        // uniqueness, not a global replay registry; rotate before GCM key-use
        // limits. A retry encrypts anew rather than reusing a mutable wire copy.
        getrandom::fill(&mut nonce).map_err(|_| CryptoError::Randomness)?;
        let aad = aad(
            envelope,
            material.id().as_str(),
            CONTENT_TYPE,
            fault_source.as_deref(),
        )?;
        let ciphertext = seal(&material, &nonce, &plaintext, &aad)?;
        value["payload"] = serde_json::to_value(EncryptedBody {
            format: MARKER.to_owned(),
            key_id: material.id().as_str().to_owned(),
            inner_content_type: CONTENT_TYPE.to_owned(),
            nonce: STANDARD.encode(nonce),
            ciphertext: STANDARD.encode(ciphertext),
            fault_source,
        })
        .map_err(|_| CryptoError::Framing)?;
        validated(value, self.limits)
    }

    /// Parses bounded wire framing, resolves a key and authenticates BEFORE
    /// deserializing the protected message body. Unknown keys/tamper/required
    /// plaintext never reach typed handlers. Dropping cancels provider work.
    pub async fn decode_bytes(&self, bytes: &[u8]) -> Result<Envelope, CryptoError> {
        let envelope = Envelope::from_slice(bytes, self.limits).map_err(|_| CryptoError::Limits)?;
        self.decode(&envelope).await
    }

    /// Receiver codec for a transport's already validated envelope.
    pub async fn decode(&self, envelope: &Envelope) -> Result<Envelope, CryptoError> {
        envelope
            .validate(self.limits)
            .map_err(|_| CryptoError::Limits)?;
        let mut value = serde_json::to_value(envelope).map_err(|_| CryptoError::Framing)?;
        let payload = value.get("payload").ok_or(CryptoError::Framing)?;
        if !has_marker(payload) {
            if self.policy.receive_required.matches(envelope.contract()) {
                return Err(CryptoError::Required);
            }
            return Ok(envelope.clone());
        }
        let body: EncryptedBody =
            serde_json::from_value(payload.clone()).map_err(|_| CryptoError::Framing)?;
        if body.format != MARKER
            || body.inner_content_type != CONTENT_TYPE
            || body.nonce.len() != 16
            || body.fault_source.as_ref().is_some_and(|s| s.len() > 256)
        {
            return Err(CryptoError::Framing);
        }
        let id = KeyId::new(body.key_id).map_err(|_| CryptoError::Framing)?;
        let material = self
            .provider
            .lookup(&id)
            .await
            .map_err(|_| CryptoError::KeyUnavailable)?;
        if material.id() != &id {
            return Err(CryptoError::KeyUnavailable);
        }
        let nonce: [u8; 12] = STANDARD
            .decode(body.nonce)
            .map_err(|_| CryptoError::Framing)?
            .try_into()
            .map_err(|_| CryptoError::Framing)?;
        let ciphertext = STANDARD
            .decode(body.ciphertext)
            .map_err(|_| CryptoError::Framing)?;
        if ciphertext.len() < 16 {
            return Err(CryptoError::Framing);
        }
        let aad = aad(
            envelope,
            id.as_str(),
            &body.inner_content_type,
            body.fault_source.as_deref(),
        )?;
        let cipher = Aes256Gcm::new_from_slice(material.key().bytes())
            .map_err(|_| CryptoError::KeyUnavailable)?;
        let plaintext = Zeroizing::new(
            cipher
                .decrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: &ciphertext,
                        aad: &aad,
                    },
                )
                .map_err(|_| CryptoError::Authentication)?,
        );
        if plaintext.len() > self.limits.max_bytes {
            return Err(CryptoError::Limits);
        }
        let inner: Value = serde_json::from_slice(&plaintext).map_err(|_| CryptoError::Framing)?;
        if fault_source(envelope.contract(), &inner) != body.fault_source.as_deref() {
            return Err(CryptoError::Authentication);
        }
        value["payload"] = inner;
        validated(value, self.limits)
    }
}

fn has_marker(value: &Value) -> bool {
    value
        .get("format")
        .is_some_and(|format| format.as_str().is_some_and(|s| s.starts_with("raddle-aes")))
}
fn fault_source<'a>(contract: &str, value: &'a Value) -> Option<&'a str> {
    (contract == "raddle.fault")
        .then(|| value.get("source_contract").and_then(Value::as_str))
        .flatten()
}
fn validated(value: Value, limits: EnvelopeLimits) -> Result<Envelope, CryptoError> {
    let bytes = serde_json::to_vec(&value).map_err(|_| CryptoError::Framing)?;
    Envelope::from_slice(&bytes, limits).map_err(|_| CryptoError::Limits)
}
fn aad(
    envelope: &Envelope,
    id: &str,
    content_type: &str,
    fault_source: Option<&str>,
) -> Result<Vec<u8>, CryptoError> {
    // Fixed-order tuple plus a sorted BTreeMap: independent of JSON object
    // ordering, whitespace and serde_json's optional preserve_order feature.
    // Crypto framing lives in payload, so no application headers are excluded.
    // Broker delivery tags, locks and mutable delivery options are not envelope
    // fields and deliberately do not enter this immutable authenticated input.
    serde_json::to_vec(&(
        MARKER,
        envelope.id(),
        envelope.contract(),
        envelope.version(),
        envelope.created_at(),
        envelope.correlation_id(),
        envelope.causation_id(),
        envelope.headers(),
        fault_source,
        id,
        content_type,
    ))
    .map_err(|_| CryptoError::Framing)
}
fn seal(
    material: &KeyMaterial,
    nonce: &[u8; 12],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes256Gcm::new_from_slice(material.key().bytes())
        .map_err(|_| CryptoError::KeyUnavailable)?;
    cipher
        .encrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| CryptoError::Authentication)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::SecretKey;
    use std::sync::Arc;

    #[test]
    fn nist_aes256_gcm_known_answer() {
        // NIST GCMVS AES-256, all-zero key/IV, one all-zero block, no AAD.
        let material = KeyMaterial::new(
            KeyId::new("nist").unwrap(),
            Arc::new(SecretKey::new([0; 32])),
        );
        let actual = seal(&material, &[0; 12], &[0; 16], &[]).unwrap();
        assert_eq!(
            actual,
            [
                0xce, 0xa7, 0x40, 0x3d, 0x4d, 0x60, 0x6b, 0x6e, 0x07, 0x4e, 0xc5, 0xd3, 0xba, 0xf3,
                0x9d, 0x18, 0xd0, 0xd1, 0xc8, 0xa7, 0x99, 0x99, 0x6b, 0xf0, 0x26, 0x5b, 0x98, 0xb5,
                0xd4, 0x8a, 0xb9, 0x19
            ]
        );
    }
}
