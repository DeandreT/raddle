use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use raddle::routing::EndpointAlias;
use raddle::wire::WireMessage;

use super::projection::Projection;
use super::{Key, MessageDeclaration, MessageRole, MessageSelector, RoutingError, key, name};

/// Conventional local behavior; explicit routes to local aliases are unaffected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum LocalRouting {
    /// Stop after accumulated non-pass-through contributions, including local ones.
    #[default]
    Terminating,
    /// Continue from local routing into external conventions.
    Additive,
    /// Do not contribute conventional-local bindings.
    Disabled,
}

/// Independent source continuation and contribution semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceOptions {
    /// False stops when accumulated non-pass-through contributions are nonempty,
    /// even if this source itself found nothing.
    pub additive: bool,
    /// Excludes this source's routes from that accumulated stop decision.
    pub pass_through: bool,
}
impl SourceOptions {
    /// Ordinary additive source: its contributions stop a later terminating stage.
    pub const ADDITIVE: Self = Self {
        additive: true,
        pass_through: false,
    };
    /// Adds routes without changing later built-in stop decisions.
    pub const PASS_THROUGH: Self = Self {
        additive: true,
        pass_through: true,
    };
    /// Ends the chain when any non-pass-through contribution has accumulated.
    pub const TERMINATING: Self = Self {
        additive: false,
        pass_through: false,
    };
}

/// Pure bounded startup routing extension. Only configured aliases may be returned.
/// No callback is run during durable retry or diagnostic inspection.
pub trait RouteSource: Send + Sync + 'static {
    /// At most 16 contributions per declaration, including duplicates.
    fn routes(&self, message: &MessageDeclaration) -> Result<Vec<EndpointAlias>, RoutingError>;
}
impl<F> RouteSource for F
where
    F: Fn(&MessageDeclaration) -> Result<Vec<EndpointAlias>, RoutingError> + Send + Sync + 'static,
{
    fn routes(&self, message: &MessageDeclaration) -> Result<Vec<EndpointAlias>, RoutingError> {
        self(message)
    }
}

#[derive(Clone)]
pub(crate) struct Source {
    pub id: String,
    pub description: String,
    pub options: SourceOptions,
    pub source: Arc<dyn RouteSource>,
}

/// Finite additive configuration for [`crate::BusBuilder::build_routed`].
/// Existing builder routes stay explicit; legacy handler-created bindings stay
/// pinned in that same stage. Only routed registrations add conventional locals.
#[derive(Clone, Default)]
pub struct RoutingConfig {
    pub(crate) messages: BTreeMap<Key, MessageDeclaration>,
    pub(crate) custom: Vec<Source>,
    pub(crate) explicit: Vec<Source>,
    pub(crate) conventions: Vec<Source>,
    pub(crate) locals: BTreeMap<Key, Vec<String>>,
    pub(crate) projections: Vec<Arc<Projection>>,
    pub(crate) local: LocalRouting,
    ids: BTreeSet<String>,
}
impl RoutingConfig {
    /// Adds concrete metadata. Conflicting serializer ownership is never merged.
    pub fn declare(&mut self, declaration: MessageDeclaration) -> Result<(), RoutingError> {
        if self.messages.len() >= 1024 || self.messages.contains_key(&declaration.key) {
            return Err(RoutingError::Invalid(
                "duplicate declaration or contract capacity exceeded",
            ));
        }
        self.messages.insert(declaration.key.clone(), declaration);
        Ok(())
    }
    pub(crate) fn handled<M: WireMessage>(&mut self) -> Result<(), RoutingError> {
        let declaration = MessageDeclaration::of::<M>(MessageRole::Handled)?;
        if let Some(existing) = self.messages.get_mut(&declaration.key) {
            if existing.concrete != declaration.concrete {
                return Err(RoutingError::Invalid("conflicting concrete contract owner"));
            }
            existing.role = MessageRole::Handled;
            Ok(())
        } else {
            self.declare(declaration)
        }
    }
    /// Selects conventional local behavior; default is terminating.
    pub fn local_routing(&mut self, mode: LocalRouting) {
        self.local = mode;
    }
    /// Adds an explicitly classified conventional-local route. Registration does
    /// not create its receiving owner. Final build validates all local handlers.
    pub fn local_route<M: WireMessage>(&mut self, endpoint: &str) -> Result<(), RoutingError> {
        name(endpoint)?;
        let key = key::<M>()?;
        if !self.messages.contains_key(&key) {
            return Err(RoutingError::Invalid("local route needs a declaration"));
        }
        let routes = self.locals.entry(key).or_default();
        if routes.iter().any(|route| route == endpoint) {
            return Ok(());
        }
        if routes.len() >= 16 {
            return Err(RoutingError::Invalid("local fanout exceeded"));
        }
        routes.push(endpoint.to_owned());
        Ok(())
    }
    /// Adds an explicit selector rule; a rule never broadcasts undeclared types.
    pub fn rule(
        &mut self,
        id: &str,
        description: &str,
        selector: MessageSelector,
        targets: Vec<EndpointAlias>,
    ) -> Result<(), RoutingError> {
        if targets.len() > 16 {
            return Err(RoutingError::Invalid("source fanout exceeded"));
        }
        self.add(
            id,
            description,
            SourceOptions::ADDITIVE,
            move |message: &MessageDeclaration| {
                Ok(if selector.matches(message) {
                    targets.clone()
                } else {
                    Vec::new()
                })
            },
            1,
        )
    }
    /// Consulted before all built-ins, in registration order.
    pub fn source(
        &mut self,
        id: &str,
        description: &str,
        options: SourceOptions,
        source: impl RouteSource,
    ) -> Result<(), RoutingError> {
        self.add(id, description, options, source, 0)
    }
    /// Last-stage conventional sender discovery. No listener/provisioning occurs.
    pub fn convention(
        &mut self,
        id: &str,
        description: &str,
        source: impl RouteSource,
    ) -> Result<(), RoutingError> {
        self.add(id, description, SourceOptions::ADDITIVE, source, 2)
    }
    fn add(
        &mut self,
        id: &str,
        description: &str,
        options: SourceOptions,
        source: impl RouteSource,
        stage: u8,
    ) -> Result<(), RoutingError> {
        self.validate_id(id, description)?;
        let source = Source {
            id: id.into(),
            description: description.into(),
            options,
            source: Arc::new(source),
        };
        self.ids.insert(id.into());
        match stage {
            0 => self.custom.push(source),
            1 => self.explicit.push(source),
            _ => self.conventions.push(source),
        }
        Ok(())
    }
    fn validate_id(&self, id: &str, description: &str) -> Result<(), RoutingError> {
        name(id)?;
        if matches!(
            id,
            "transforms" | "explicit" | "local" | "legacy-explicit" | "direct"
        ) || self.ids.len() >= 32
            || self.ids.contains(id)
            || description.is_empty()
            || description.len() > 512
        {
            return Err(RoutingError::Invalid(
                "duplicate source, invalid description or rule capacity exceeded",
            ));
        }
        Ok(())
    }
    /// Pure plaintext `From -> To` publication projection. The versioned stable
    /// ID is part of deterministic child identity; bump it when semantics change.
    /// This never transforms RPC, decrypts protected roots or performs upcasting.
    pub fn transform<From: WireMessage, To: WireMessage>(
        &mut self,
        id: &str,
        version: u32,
        map: impl Fn(From) -> Result<To, RoutingError> + Send + Sync + 'static,
    ) -> Result<(), RoutingError> {
        self.validate_id(id, "typed publication projection")?;
        if version == 0 || self.projections.len() >= 16 {
            return Err(RoutingError::Invalid(
                "invalid projection version or capacity",
            ));
        }
        let from = key::<From>()?;
        let to = key::<To>()?;
        if !self.messages.contains_key(&from) || !self.messages.contains_key(&to) {
            return Err(RoutingError::Invalid(
                "projection contracts must be declared",
            ));
        }
        if self.messages[&from].concrete != std::any::TypeId::of::<From>()
            || self.messages[&to].concrete != std::any::TypeId::of::<To>()
        {
            return Err(RoutingError::Invalid(
                "conflicting projection contract owner",
            ));
        }
        self.projections
            .push(Arc::new(Projection::new(id, version, from, to, map)));
        self.ids.insert(id.into());
        Ok(())
    }
}
