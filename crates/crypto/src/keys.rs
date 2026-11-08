use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};

use zeroize::Zeroizing;

/// Bounded public key identifier, not key material.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct KeyId(String);
impl KeyId {
    /// Identifiers use 1..=128 ASCII alphanumeric, dash, underscore or dot.
    pub fn new(value: impl Into<String>) -> Result<Self, KeyProviderError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(KeyProviderError);
        }
        Ok(Self(value))
    }
    /// The identifier is safe to place in authenticated wire metadata.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 256-bit key whose owned storage is zeroized on Drop. Never serializable;
/// Debug is permanently redacted. Providers own lifetime and rotation.
pub struct SecretKey(Zeroizing<[u8; 32]>);
impl SecretKey {
    /// Imports application-managed key bytes. Callers must clear their copies.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }
    /// Generates key material from the operating system's CSPRNG.
    pub fn generate() -> Result<Self, KeyProviderError> {
        let mut bytes = Zeroizing::new([0; 32]);
        getrandom::fill(bytes.as_mut()).map_err(|_| KeyProviderError)?;
        Ok(Self(bytes))
    }
    pub(crate) fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}
impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey([REDACTED])")
    }
}

/// A provider-selected key and its authenticated identifier.
#[derive(Clone, Debug)]
pub struct KeyMaterial {
    id: KeyId,
    key: Arc<SecretKey>,
}
impl KeyMaterial {
    /// Pairs an identifier with private provider-owned key material.
    pub fn new(id: KeyId, key: Arc<SecretKey>) -> Self {
        Self { id, key }
    }
    /// Public identifier; key bytes deliberately have no public accessor.
    pub fn id(&self) -> &KeyId {
        &self.id
    }
    pub(crate) fn key(&self) -> &SecretKey {
        &self.key
    }
}

/// Unknown/unavailable/invalid keys share a redacted error surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("encryption key is unavailable or invalid")]
pub struct KeyProviderError;

/// Async provider boundary permits application KMS/secret-store integration.
/// A lookup must return the requested ID or fail, never silently use active.
/// Dropping the future cancels this operation without caching partial results.
pub trait KeyProvider: Send + Sync {
    /// Selects the currently active key for a new publication.
    fn active(&self) -> impl Future<Output = Result<KeyMaterial, KeyProviderError>> + Send;
    /// Resolves a historical key for receive after rotation.
    fn lookup(
        &self,
        id: &KeyId,
    ) -> impl Future<Output = Result<KeyMaterial, KeyProviderError>> + Send;
}
impl<T: KeyProvider> KeyProvider for Arc<T> {
    async fn active(&self) -> Result<KeyMaterial, KeyProviderError> {
        (**self).active().await
    }
    async fn lookup(&self, id: &KeyId) -> Result<KeyMaterial, KeyProviderError> {
        (**self).lookup(id).await
    }
}

struct State {
    keys: BTreeMap<KeyId, Arc<SecretKey>>,
    used_ids: BTreeSet<KeyId>,
    active: Option<KeyId>,
}

/// Actual bounded in-process rotation provider. No implicit global key cache,
/// network access or background task. Clones share one application-owned ring.
#[derive(Clone)]
pub struct RotatingKeys {
    state: Arc<Mutex<State>>,
    capacity: usize,
    lifetime_id_budget: usize,
}
impl RotatingKeys {
    /// Keeps at most 1..=1024 retained keys and remembers at most 4096 IDs for
    /// this provider's lifetime. Retirement frees live-key capacity, not IDs.
    pub fn new(capacity: usize) -> Result<Self, KeyProviderError> {
        Self::with_lifetime_id_budget(capacity, 4096)
    }
    /// Sets an explicit lifetime budget from live capacity through 65536 IDs.
    /// Exhaustion fails closed; retired IDs cannot be rebound, even to identical
    /// bytes. A replacement/external provider must preserve identity uniqueness
    /// across its own lifetimes while old ciphertext may still be received.
    pub fn with_lifetime_id_budget(
        capacity: usize,
        lifetime_id_budget: usize,
    ) -> Result<Self, KeyProviderError> {
        if !(1..=1024).contains(&capacity) || !(capacity..=65536).contains(&lifetime_id_budget) {
            return Err(KeyProviderError);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                keys: BTreeMap::new(),
                used_ids: BTreeSet::new(),
                active: None,
            })),
            capacity,
            lifetime_id_budget,
        })
    }
    /// Adds new immutable key identity. Reusing an ID, even with the same
    /// bytes, is refused to prevent rotation ambiguity and nonce accounting.
    pub fn insert(
        &self,
        id: KeyId,
        key: SecretKey,
        activate: bool,
    ) -> Result<(), KeyProviderError> {
        let mut state = self.state.lock().map_err(|_| KeyProviderError)?;
        if state.keys.len() == self.capacity
            || state.used_ids.len() == self.lifetime_id_budget
            || state.used_ids.contains(&id)
        {
            return Err(KeyProviderError);
        }
        state.used_ids.insert(id.clone());
        state.keys.insert(id.clone(), Arc::new(key));
        if activate {
            state.active = Some(id);
        }
        Ok(())
    }
    /// Changes active selection while retaining old keys for in-flight work.
    pub fn activate(&self, id: &KeyId) -> Result<(), KeyProviderError> {
        let mut state = self.state.lock().map_err(|_| KeyProviderError)?;
        if !state.keys.contains_key(id) {
            return Err(KeyProviderError);
        }
        state.active = Some(id.clone());
        Ok(())
    }
    /// Explicit retirement refuses the active key; outstanding Arc references
    /// keep the old key alive only until their in-flight operation completes.
    pub fn retire(&self, id: &KeyId) -> Result<(), KeyProviderError> {
        let mut state = self.state.lock().map_err(|_| KeyProviderError)?;
        if state.active.as_ref() == Some(id) || state.keys.remove(id).is_none() {
            return Err(KeyProviderError);
        }
        Ok(())
    }
}
impl KeyProvider for RotatingKeys {
    async fn active(&self) -> Result<KeyMaterial, KeyProviderError> {
        let state = self.state.lock().map_err(|_| KeyProviderError)?;
        let id = state.active.as_ref().ok_or(KeyProviderError)?;
        Ok(KeyMaterial::new(
            id.clone(),
            state.keys.get(id).ok_or(KeyProviderError)?.clone(),
        ))
    }
    async fn lookup(&self, id: &KeyId) -> Result<KeyMaterial, KeyProviderError> {
        let state = self.state.lock().map_err(|_| KeyProviderError)?;
        Ok(KeyMaterial::new(
            id.clone(),
            state.keys.get(id).ok_or(KeyProviderError)?.clone(),
        ))
    }
}
