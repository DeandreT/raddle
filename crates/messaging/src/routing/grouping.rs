use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use raddle::grouping::{Dispatch, GroupId};
use raddle::wire::{Envelope, WireMessage};

use super::{Key, MessageDeclaration, MessageRole, MessageSelector, RoutingError};
use crate::TrustedOrigin;

/// Fall-through is distinct from a match with an empty logical identity.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RuleMatch {
    /// Consult the next registered rule.
    NoMatch,
    /// Stop, including when the group contains an empty string.
    Matched(GroupId),
}

type Extract = dyn Fn(&Envelope, &TrustedOrigin) -> Result<RuleMatch, RoutingError> + Send + Sync;

/// Pure synchronous bounded extraction. Callbacks must not block or perform I/O.
#[derive(Clone)]
pub struct GroupingRule(Arc<Extract>);
impl GroupingRule {
    /// Explicit custom native rule; protection is checked before calling it.
    pub fn custom(
        rule: impl Fn(&Envelope, &TrustedOrigin) -> Result<RuleMatch, RoutingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self(Arc::new(rule))
    }
    /// Exact typed native accessor. Other contracts fall through without decoding.
    pub fn typed<M: WireMessage>(
        extract: impl Fn(&M) -> RuleMatch + Send + Sync + 'static,
    ) -> Self {
        Self::custom(move |envelope, _| {
            if envelope.contract() != M::NAME || envelope.version() != M::VERSION {
                return Ok(RuleMatch::NoMatch);
            }
            Ok(extract(
                &envelope.decode::<M>().map_err(|_| RoutingError::Grouping)?,
            ))
        })
    }
    /// Uses independently authenticated tenant identity, never an envelope header.
    pub fn tenant() -> Self {
        Self::custom(|_, origin| Ok(RuleMatch::Matched(GroupId::new(origin.tenant()))))
    }
    /// First present property wins; present null matches empty, absent falls through.
    /// Other JSON values use compact canonical JSON spelling, not CLR ToString.
    pub fn properties(names: Vec<String>) -> Result<Self, RoutingError> {
        if names.is_empty()
            || names.len() > 16
            || names.iter().any(|n| n.is_empty() || n.len() > 128)
        {
            return Err(RoutingError::Invalid("invalid grouping property names"));
        }
        Ok(Self::custom(move |envelope, _| {
            let payload = envelope
                .plaintext_payload()
                .map_err(|_| RoutingError::Protected)?;
            for name in &names {
                if let Some(value) = payload.get(name) {
                    let group = match value {
                        serde_json::Value::Null => String::new(),
                        serde_json::Value::String(s) => s.clone(),
                        value => serde_json::to_string(&canonical_value(value))
                            .map_err(|_| RoutingError::Grouping)?,
                    };
                    return Ok(RuleMatch::Matched(GroupId::new(group)));
                }
            }
            Ok(RuleMatch::NoMatch)
        }))
    }
    /// Explicit native saga/aggregate/stream correlation registration. The native
    /// application owns the accessor; no foreign reflection/ORM inference occurs.
    pub fn inferred<M: WireMessage>(
        identity: impl Fn(&M) -> Option<GroupId> + Send + Sync + 'static,
    ) -> Self {
        Self::typed(move |message| identity(message).map_or(RuleMatch::NoMatch, RuleMatch::Matched))
    }
    /// Native sequenced-message registration. Null orders distribute independently
    /// using the stable original message ID, rather than becoming one empty group.
    pub fn sequenced<M: WireMessage>(
        order: impl Fn(&M) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self::custom(move |envelope, origin| {
            if envelope.contract() != M::NAME || envelope.version() != M::VERSION {
                return Ok(RuleMatch::NoMatch);
            }
            let message = envelope.decode::<M>().map_err(|_| RoutingError::Grouping)?;
            Ok(RuleMatch::Matched(GroupId::new(
                order(&message)
                    .unwrap_or_else(|| format!("sequence:{}:{}", origin.tenant(), envelope.id())),
            )))
        })
    }
}

/// Ordered additive native rules/exemptions, frozen by the existing resolver.
#[derive(Clone, Default)]
pub struct GroupingConfig {
    rules: Vec<GroupingRule>,
    exemptions: Vec<MessageSelector>,
    pub(super) topologies: Vec<TopologyGrouping>,
    pub(super) propagate: bool,
}
impl GroupingConfig {
    pub(super) fn configured(&self) -> bool {
        !self.rules.is_empty()
            || !self.exemptions.is_empty()
            || !self.topologies.is_empty()
            || self.propagate
    }
    /// Appends; repeated calls never reset earlier application rules.
    pub fn rule(&mut self, rule: GroupingRule) -> Result<(), RoutingError> {
        if self.rules.len() >= 32 {
            return Err(RoutingError::Invalid("grouping rule capacity exceeded"));
        }
        self.rules.push(rule);
        Ok(())
    }
    /// Additive concrete/family/predicate exemption from local slots only.
    pub fn exempt(&mut self, selector: MessageSelector) -> Result<(), RoutingError> {
        if self.exemptions.len() >= 32 {
            return Err(RoutingError::Invalid("exemption capacity exceeded"));
        }
        self.exemptions.push(selector);
        Ok(())
    }
    /// Requests originating-group fallback for returned children's absent key.
    /// Build refuses this policy until prepared returned custody is supported.
    pub fn propagate_group_to_partition_key(&mut self) {
        self.propagate = true;
    }
}

#[derive(Clone)]
pub(super) struct TopologyGrouping {
    pub selector: MessageSelector,
    pub rules: Vec<GroupingRule>,
}

#[derive(Clone, Default)]
struct Program {
    rules: Vec<GroupingRule>,
    exempt: bool,
    overlap: bool,
}

#[derive(Clone, Default)]
pub(super) struct GroupingPlans {
    programs: BTreeMap<Key, Program>,
}
impl GroupingPlans {
    pub fn compile(
        config: &GroupingConfig,
        messages: &BTreeMap<Key, MessageDeclaration>,
        handled: &BTreeSet<Key>,
    ) -> Result<Self, RoutingError> {
        if config.propagate {
            return Err(RoutingError::Invalid(
                "group-to-key propagation requires prepared returned custody",
            ));
        }
        let mut plans = Self {
            programs: BTreeMap::new(),
        };
        for (key, message) in messages {
            let program = catch_unwind(AssertUnwindSafe(|| {
                let owners: Vec<_> = config
                    .topologies
                    .iter()
                    .filter(|t| !t.rules.is_empty() && t.selector.matches(message))
                    .collect();
                let overlap = owners.len() > 1;
                if overlap && (message.role() == MessageRole::Handled || handled.contains(key)) {
                    return Err(RoutingError::Invalid(
                        "overlapping topology grouping owners",
                    ));
                }
                let mut rules = owners
                    .first()
                    .map_or_else(Vec::new, |owner| owner.rules.clone());
                rules.extend(config.rules.iter().cloned());
                Ok(Program {
                    rules,
                    exempt: config.exemptions.iter().any(|s| s.matches(message)),
                    overlap,
                })
            }))
            .map_err(|_| RoutingError::Callback)??;
            plans.programs.insert(key.clone(), program);
        }
        Ok(plans)
    }
    pub fn prepare(
        &self,
        envelope: &Envelope,
        origin: &TrustedOrigin,
        enabled: bool,
    ) -> Result<Envelope, RoutingError> {
        let program = self
            .programs
            .get(&(envelope.contract().into(), envelope.version()));
        if program.is_some_and(|p| p.overlap) {
            return Err(RoutingError::Invalid(
                "overlapping topology grouping owners",
            ));
        }
        let enabled = enabled || self.active(&(envelope.contract().into(), envelope.version()));
        if !enabled && envelope.dispatch().is_none() {
            return Ok(envelope.clone());
        }
        envelope
            .require_plaintext()
            .map_err(|_| RoutingError::Protected)?;
        let input = envelope.dispatch();
        if input.is_some_and(Dispatch::is_resolved) {
            return Ok(envelope.clone());
        }
        let mut group = input.and_then(Dispatch::group).cloned();
        if group.is_none()
            && let Some(program) = program
        {
            for rule in &program.rules {
                match catch_unwind(AssertUnwindSafe(|| (rule.0)(envelope, origin)))
                    .map_err(|_| RoutingError::Grouping)??
                {
                    RuleMatch::NoMatch => {}
                    RuleMatch::Matched(value) => {
                        group = Some(value);
                        break;
                    }
                }
            }
        }
        envelope
            .clone()
            .with_dispatch(Dispatch::resolved(
                group,
                input.and_then(Dispatch::partition_key).map(str::to_owned),
            ))
            .map_err(|_| RoutingError::Grouping)
    }
    pub fn exempt(&self, envelope: &Envelope) -> bool {
        self.programs
            .get(&(envelope.contract().into(), envelope.version()))
            .is_some_and(|p| p.exempt)
    }
    pub fn active(&self, key: &Key) -> bool {
        self.programs
            .get(key)
            .is_some_and(|p| !p.rules.is_empty() || p.overlap)
    }
    pub fn check(&self, key: &Key) -> Result<(), RoutingError> {
        if self.programs.get(key).is_some_and(|p| p.overlap) {
            return Err(RoutingError::Invalid(
                "overlapping topology grouping owners",
            ));
        }
        Ok(())
    }
}

// Stable even if a consumer enables serde_json/preserve_order. This is transient
// bounded payload traversal, not retained per-business-key scheduler state.
fn canonical_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => serde_json::Value::Object(
            object
                .iter()
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(key, value)| (key.clone(), canonical_value(value)))
                .collect(),
        ),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_value).collect())
        }
        value => value.clone(),
    }
}
