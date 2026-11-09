use std::any::TypeId;
use std::collections::BTreeSet;
use std::sync::Arc;

use raddle::routing::WireMessageType;
use raddle::wire::WireMessage;

use super::{Key, RoutingError, key, name};

/// Declared native use, not inferred from a CLR or Rust type hierarchy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MessageRole {
    /// An explicitly registered concrete receiving contract.
    Handled,
    /// A concrete contract the application may send or publish.
    Outgoing,
    /// A concrete publication contract without a required local handler.
    PublishOnly,
}

/// Bounded concrete metadata. Module/family labels are selection metadata only;
/// wire identity always remains the concrete stable contract and schema version.
#[derive(Clone, Debug)]
pub struct MessageDeclaration {
    pub(crate) key: Key,
    pub(crate) concrete: TypeId,
    contract: WireMessageType,
    pub(crate) role: MessageRole,
    module: Option<String>,
    families: BTreeSet<String>,
}

impl MessageDeclaration {
    /// Declares one concrete serializer identity, without executing a handler.
    pub fn of<M: WireMessage>(role: MessageRole) -> Result<Self, RoutingError> {
        Ok(Self {
            key: key::<M>()?,
            concrete: TypeId::of::<M>(),
            contract: WireMessageType::of::<M>()
                .map_err(|_| RoutingError::Invalid("invalid contract"))?,
            role,
            module: None,
            families: BTreeSet::new(),
        })
    }
    /// Explicit Rust-native module label, matched at `::` segment boundaries.
    pub fn module(mut self, module: &str) -> Result<Self, RoutingError> {
        validate_module(module)?;
        self.module = Some(module.to_owned());
        Ok(self)
    }
    /// Adds one of at most 16 explicit family labels; never changes wire identity.
    pub fn family(mut self, family: &str) -> Result<Self, RoutingError> {
        name(family)?;
        if self.families.len() >= 16 || !self.families.insert(family.to_owned()) {
            return Err(RoutingError::Invalid(
                "duplicate family or capacity exceeded",
            ));
        }
        Ok(self)
    }
    /// Exact validated wire contract/version.
    pub fn contract(&self) -> &WireMessageType {
        &self.contract
    }
    /// Explicit native use.
    pub fn role(&self) -> MessageRole {
        self.role
    }
    /// Optional module metadata, not a discovered type path.
    pub fn module_name(&self) -> Option<&str> {
        self.module.as_deref()
    }
    /// Whether this concrete declaration belongs to an explicit native family.
    pub fn in_family(&self, family: &str) -> bool {
        self.families.contains(family)
    }
}

type Predicate = Arc<dyn Fn(&MessageDeclaration) -> bool + Send + Sync>;

/// Selection over only the application's finite declared inventory.
#[derive(Clone)]
pub struct MessageSelector(Selector);

#[derive(Clone)]
enum Selector {
    Exact(Key),
    Family(String),
    Module(String),
    Predicate(Predicate),
}

impl MessageSelector {
    /// Matches one concrete stable contract/version.
    pub fn exact<M: WireMessage>() -> Result<Self, RoutingError> {
        Ok(Self(Selector::Exact(key::<M>()?)))
    }
    /// Matches explicit family members, not all application messages.
    pub fn family(family: &str) -> Result<Self, RoutingError> {
        name(family)?;
        Ok(Self(Selector::Family(family.to_owned())))
    }
    /// Matches exactly this module and its `::` descendants.
    pub fn module(module: &str) -> Result<Self, RoutingError> {
        validate_module(module)?;
        Ok(Self(Selector::Module(module.to_owned())))
    }
    /// Pure startup predicate. The containing rule supplies its diagnostic name.
    pub fn predicate(
        predicate: impl Fn(&MessageDeclaration) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self(Selector::Predicate(Arc::new(predicate)))
    }
    pub(crate) fn matches(&self, message: &MessageDeclaration) -> bool {
        match &self.0 {
            Selector::Exact(key) => key == &message.key,
            Selector::Family(family) => message.in_family(family),
            Selector::Module(module) => message.module.as_ref().is_some_and(|candidate| {
                candidate == module
                    || candidate
                        .strip_prefix(module)
                        .is_some_and(|suffix| suffix.starts_with("::"))
            }),
            Selector::Predicate(predicate) => predicate(message),
        }
    }
}

fn validate_module(module: &str) -> Result<(), RoutingError> {
    if module.len() > 256
        || module.split("::").any(|part| {
            !part
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
                || !part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
    {
        return Err(RoutingError::Invalid("invalid module label"));
    }
    Ok(())
}
