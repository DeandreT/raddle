//! Bounded typed routing to explicitly configured endpoint aliases, never URIs.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroUsize};

use crate::wire::WireMessage;
use crate::{InvalidSubscriptionName, SubscriptionName};

/// A real wire contract identity: validated stable name plus positive version.
///
/// Rust `TypeId` and diagnostic type names are never used as route keys.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct WireMessageType {
    name: SubscriptionName,
    version: NonZeroU32,
}

impl WireMessageType {
    /// Validates the contract declared by a concrete wire message.
    pub fn of<M: WireMessage>() -> Result<Self, RoutingError> {
        let name = SubscriptionName::try_new(M::NAME.to_owned())
            .map_err(|_| RoutingError::InvalidContract)?;
        let version = NonZeroU32::new(M::VERSION).ok_or(RoutingError::InvalidContract)?;
        Ok(Self { name, version })
    }

    /// Stable contract name, independent of Rust source organization.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
    /// Positive schema version used for exact route matching.
    pub fn version(&self) -> NonZeroU32 {
        self.version
    }
}

/// A validated symbolic endpoint name, not a connection string or network URI.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct EndpointAlias(SubscriptionName);

impl EndpointAlias {
    /// Uses the bounded lowercase subscription-name syntax; URI punctuation fails.
    pub fn try_new(value: impl Into<String>) -> Result<Self, InvalidSubscriptionName> {
        SubscriptionName::try_new(value).map(Self)
    }
    /// Configured symbolic name suitable for explicit adapter resolution.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// A rejected registration or unresolved typed route; failures change nothing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RoutingError {
    /// The message declares an invalid stable name or zero schema version.
    #[error("invalid wire contract")]
    InvalidContract,
    /// The application has not configured the requested endpoint alias.
    #[error("endpoint alias is not configured")]
    UnknownEndpoint,
    /// The same endpoint or contract/endpoint binding already exists.
    #[error("duplicate route or endpoint")]
    Duplicate,
    /// An endpoint, binding, or fan-out bound would be exceeded.
    #[error("routing capacity exhausted")]
    Capacity,
    /// No route matches the message's exact stable name and version.
    #[error("wire contract has no configured route")]
    Unregistered,
}

/// Explicit finite configuration for endpoint admission, total bindings, and fan-out.
/// Defaults to 64 endpoints, 256 bindings, and 16 aliases per contract.
#[derive(Clone, Copy, Debug)]
pub struct RouteLimits {
    endpoints: NonZeroUsize,
    bindings: NonZeroUsize,
    fan_out: NonZeroUsize,
}

impl RouteLimits {
    /// Sets independent positive limits; no allocation is reserved up front.
    pub fn new(endpoints: NonZeroUsize, bindings: NonZeroUsize, fan_out: NonZeroUsize) -> Self {
        Self {
            endpoints,
            bindings,
            fan_out,
        }
    }
}

impl Default for RouteLimits {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::MIN.saturating_add(63),
            NonZeroUsize::MIN.saturating_add(255),
            NonZeroUsize::MIN.saturating_add(15),
        )
    }
}

/// Bounded contract-to-alias routes, assembled by the application.
///
/// Resolution returns configured aliases in registration order, not arbitrary
/// inbound reply addresses. Adapters still own the alias-to-endpoint mapping,
/// authorization, serialization, and confirmed publication. This table performs
/// no network I/O and does not infer routes or trust envelope headers.
#[derive(Debug)]
pub struct RouteTable {
    limits: RouteLimits,
    endpoints: BTreeSet<EndpointAlias>,
    routes: BTreeMap<WireMessageType, Vec<EndpointAlias>>,
    bindings: usize,
}

impl RouteTable {
    /// Creates empty application-owned configuration with explicit bounds.
    pub fn new(limits: RouteLimits) -> Self {
        Self {
            limits,
            endpoints: BTreeSet::new(),
            routes: BTreeMap::new(),
            bindings: 0,
        }
    }

    /// Admits an alias that the application has separately configured in its adapter.
    pub fn endpoint(&mut self, endpoint: EndpointAlias) -> Result<(), RoutingError> {
        if self.endpoints.contains(&endpoint) {
            return Err(RoutingError::Duplicate);
        }
        if self.endpoints.len() >= self.limits.endpoints.get() {
            return Err(RoutingError::Capacity);
        }
        self.endpoints.insert(endpoint);
        Ok(())
    }

    /// Registers an exact typed wire contract to an already admitted alias.
    pub fn route<M: WireMessage>(&mut self, endpoint: &EndpointAlias) -> Result<(), RoutingError> {
        let message = WireMessageType::of::<M>()?;
        if !self.endpoints.contains(endpoint) {
            return Err(RoutingError::UnknownEndpoint);
        }
        if let Some(routes) = self.routes.get(&message) {
            if routes.contains(endpoint) {
                return Err(RoutingError::Duplicate);
            }
            if routes.len() >= self.limits.fan_out.get() {
                return Err(RoutingError::Capacity);
            }
        }
        if self.bindings >= self.limits.bindings.get() {
            return Err(RoutingError::Capacity);
        }
        self.routes
            .entry(message)
            .or_default()
            .push(endpoint.clone());
        self.bindings += 1;
        Ok(())
    }

    /// Resolves a typed message to exact configured aliases, preserving registration order.
    pub fn resolve<M: WireMessage>(
        &self,
    ) -> Result<impl ExactSizeIterator<Item = &EndpointAlias>, RoutingError> {
        let message = WireMessageType::of::<M>()?;
        self.routes
            .get(&message)
            .map(|routes| routes.iter())
            .ok_or(RoutingError::Unregistered)
    }
}

impl Default for RouteTable {
    fn default() -> Self {
        Self::new(RouteLimits::default())
    }
}
