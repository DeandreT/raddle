//! Stable, bounded wire contracts and explicit one-version-at-a-time upcasting.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::SubscriptionName;

/// A serialized contract, independent of a Rust module or type name.
pub trait WireMessage: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Stable lowercase contract identifier, following subscription-name syntax.
    const NAME: &'static str;
    /// Positive wire schema version. Rust package versions are independent.
    const VERSION: u32;
}

/// Untrusted message validation or contract compatibility failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WireError {
    /// Invalid envelope metadata; never includes the payload.
    #[error("invalid envelope: {0}")]
    Invalid(&'static str),
    /// JSON framing or payload conversion failed.
    #[error("invalid JSON message")]
    Json(#[from] serde_json::Error),
    /// A different contract or unsupported version was received.
    #[error("message contract or version is not supported")]
    Unsupported,
    /// An upcaster was registered twice.
    #[error("upcaster already registered")]
    DuplicateUpcaster,
}

/// Limits applied before dispatch or durable acceptance.
#[derive(Clone, Copy, Debug)]
pub struct EnvelopeLimits {
    /// Maximum encoded envelope size, including metadata.
    pub max_bytes: usize,
    /// Maximum number of application headers.
    pub max_headers: usize,
}

impl Default for EnvelopeLimits {
    fn default() -> Self {
        Self {
            max_bytes: 1_048_576,
            max_headers: 32,
        }
    }
}

/// A stable message identity and JSON body. Tenant and authorization are not trusted headers.
///
/// Construction and decoding validate metadata and sizes. Fields are private so
/// the same checks cannot be bypassed by deserializing directly into this type.
#[derive(Clone, Serialize)]
pub struct Envelope {
    id: Uuid,
    contract: String,
    version: u32,
    created_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
    causation_id: Option<Uuid>,
    headers: BTreeMap<String, String>,
    payload: Value,
}

impl std::fmt::Debug for Envelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Envelope")
            .field("id", &self.id)
            .field("contract", &self.contract)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UntrustedEnvelope {
    id: Uuid,
    contract: String,
    version: u32,
    created_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
    causation_id: Option<Uuid>,
    headers: BTreeMap<String, String>,
    payload: Value,
}

impl Envelope {
    /// Encodes a typed contract with a new stable identity.
    pub fn encode<M: WireMessage>(message: &M) -> Result<Self, WireError> {
        Self::with_identity(Uuid::new_v4(), Utc::now(), message)
    }

    /// Encodes using an existing identity and time, useful for deterministic tests.
    pub fn with_identity<M: WireMessage>(
        id: Uuid,
        created_at: DateTime<Utc>,
        message: &M,
    ) -> Result<Self, WireError> {
        let envelope = Self {
            id,
            contract: M::NAME.to_owned(),
            version: M::VERSION,
            created_at,
            correlation_id: None,
            causation_id: None,
            headers: BTreeMap::new(),
            payload: serde_json::to_value(message)?,
        };
        envelope.validate(EnvelopeLimits::default())?;
        Ok(envelope)
    }

    /// Parses untrusted bytes, bounding allocation by checking framing size first.
    pub fn from_slice(bytes: &[u8], limits: EnvelopeLimits) -> Result<Self, WireError> {
        if bytes.len() > limits.max_bytes {
            return Err(WireError::Invalid("envelope exceeds byte limit"));
        }
        let raw: UntrustedEnvelope = serde_json::from_slice(bytes)?;
        let envelope = Self {
            id: raw.id,
            contract: raw.contract,
            version: raw.version,
            created_at: raw.created_at,
            correlation_id: raw.correlation_id,
            causation_id: raw.causation_id,
            headers: raw.headers,
            payload: raw.payload,
        };
        envelope.validate(limits)?;
        Ok(envelope)
    }

    /// Encodes a validated envelope for persistence or transport.
    pub fn to_vec(&self) -> Result<Vec<u8>, WireError> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Canonical application content, excluding retry-independent identity and creation time.
    /// Persistence adapters use this for scoped HTTP idempotency comparisons.
    pub fn content_bytes(&self) -> Result<Vec<u8>, WireError> {
        Ok(serde_json::to_vec(&(
            &self.contract,
            self.version,
            self.correlation_id,
            self.causation_id,
            &self.headers,
            &self.payload,
        ))?)
    }

    /// Validates explicit limits without trusting application headers.
    pub fn validate(&self, limits: EnvelopeLimits) -> Result<(), WireError> {
        if self.id.is_nil() || self.version == 0 {
            return Err(WireError::Invalid("nil identity or zero version"));
        }
        SubscriptionName::try_new(self.contract.clone())
            .map_err(|_| WireError::Invalid("invalid contract name"))?;
        if self.headers.len() > limits.max_headers {
            return Err(WireError::Invalid("too many headers"));
        }
        for (key, value) in &self.headers {
            if key.is_empty()
                || key.len() > 64
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                || value.len() > 2048
            {
                return Err(WireError::Invalid("invalid application header"));
            }
        }
        if self.to_vec()?.len() > limits.max_bytes {
            return Err(WireError::Invalid("envelope exceeds byte limit"));
        }
        Ok(())
    }

    /// Adds correlation without changing message identity, checking the full byte limit.
    pub fn correlated(
        mut self,
        correlation_id: Uuid,
        causation_id: Uuid,
    ) -> Result<Self, WireError> {
        self.correlation_id = Some(correlation_id);
        self.causation_id = Some(causation_id);
        self.validate(EnvelopeLimits::default())?;
        Ok(self)
    }

    /// Returns a new envelope with a validated application header.
    pub fn with_header(mut self, key: String, value: String) -> Result<Self, WireError> {
        self.headers.insert(key, value);
        self.validate(EnvelopeLimits::default())?;
        Ok(self)
    }

    /// Stable message identity; retries retain it.
    pub fn id(&self) -> Uuid {
        self.id
    }
    /// Stable contract name.
    pub fn contract(&self) -> &str {
        &self.contract
    }
    /// Wire schema version.
    pub fn version(&self) -> u32 {
        self.version
    }
    /// Original creation time.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    /// Conversation identity, when supplied.
    pub fn correlation_id(&self) -> Option<Uuid> {
        self.correlation_id
    }
    /// Identity of the message that caused this one.
    pub fn causation_id(&self) -> Option<Uuid> {
        self.causation_id
    }
    /// Bounded application metadata, not authenticated claims.
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }

    /// Decodes an exact matching contract without implicit schema conversion.
    pub fn decode<M: WireMessage>(&self) -> Result<M, WireError> {
        if self.contract != M::NAME || self.version != M::VERSION {
            return Err(WireError::Unsupported);
        }
        Ok(serde_json::from_value(self.payload.clone())?)
    }
}

type Upcaster = Box<dyn Fn(Value) -> Result<Value, WireError> + Send + Sync>;

/// Explicit pure JSON migrations. Each migration advances exactly one version.
#[derive(Default)]
pub struct Codecs {
    upcasters: BTreeMap<(String, u32), Upcaster>,
}

impl Codecs {
    /// Registers a one-version migration. Duplicate registration is rejected.
    pub fn upcast<F>(&mut self, name: &str, from_version: u32, migrate: F) -> Result<(), WireError>
    where
        F: Fn(Value) -> Result<Value, WireError> + Send + Sync + 'static,
    {
        SubscriptionName::try_new(name).map_err(|_| WireError::Invalid("invalid contract name"))?;
        if from_version == 0 || from_version == u32::MAX {
            return Err(WireError::Invalid("invalid source version"));
        }
        let key = (name.to_owned(), from_version);
        if self.upcasters.contains_key(&key) {
            return Err(WireError::DuplicateUpcaster);
        }
        self.upcasters.insert(key, Box::new(migrate));
        Ok(())
    }

    /// Applies only registered migrations; unknown future versions are refused.
    pub fn decode<M: WireMessage>(&self, envelope: &Envelope) -> Result<M, WireError> {
        if envelope.contract != M::NAME
            || envelope.version > M::VERSION
            || M::VERSION == 0
            || M::VERSION - envelope.version > 32
        {
            return Err(WireError::Unsupported);
        }
        let mut value = envelope.payload.clone();
        for version in envelope.version..M::VERSION {
            value = self
                .upcasters
                .get(&(M::NAME.to_owned(), version))
                .ok_or(WireError::Unsupported)?(value)?;
        }
        Ok(serde_json::from_value(value)?)
    }
}
