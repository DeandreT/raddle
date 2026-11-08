//! Opt-in classic, separated and sticky registration on the real application bus.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use raddle::wire::WireMessage;
use raddle_postgres::{FailureDisposition, RetryPolicy, Store};

use crate::bus::Endpoint;
use crate::composition::{Chain, Composed, CompositionFailure};
use crate::scoped::{Failure, Flush, ScopedHandler};
use crate::{
    BusBuilder, BusError, DurableEndpoint, LocalQueue, QueueMode, QueueRunner, QueueSettings,
    ScopedRegistration,
};

/// Native multiple-handler behavior. Ordinary Runtime registrations are unchanged.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum MultipleHandlerBehavior {
    /// Version-six contract: selected groups share one transaction, in order.
    #[default]
    Classic,
    /// Each unassigned group gets its own named subscription and transaction.
    Separated,
}

/// Startup diagnostic for a real endpoint combining distinct native group IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CombinedHandlers {
    /// Stable wire contract, not a CLR or Rust class-name claim.
    pub contract: String,
    /// Endpoint whose selected chain combines groups.
    pub endpoint: String,
    /// Explicit native identities in execution order.
    pub groups: Vec<String>,
}

/// Default custody for automatically created non-sticky endpoints.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub enum Custody {
    /// Bounded volatile memory; failures are observed, not automatically retried.
    #[default]
    Local,
    /// PostgreSQL subscriptions, with independent persisted retry/completion state.
    Durable,
}

type Classifier<E> = Arc<dyn Fn(&Failure<E>) -> FailureDisposition + Send + Sync>;

/// One selected endpoint's execution/error configuration, never global mutation.
pub struct EndpointOptions<E: std::error::Error + 'static> {
    /// Override for this selected chain (1ms through one hour). None keeps the
    /// configured bus per-message/global pre-commit budget.
    pub timeout: Option<Duration>,
    /// Bounded memory settings; only Buffered is supported for scoped execution.
    pub queue: QueueSettings,
    /// SQL receiving parallelism, independent of memory admission capacity.
    pub parallelism: u32,
    /// Persisted SQL retry budget/backoff for this receiving owner.
    pub retry: RetryPolicy,
    classifier: Classifier<E>,
}
impl<E: std::error::Error + 'static> Default for EndpointOptions<E> {
    fn default() -> Self {
        Self {
            timeout: None,
            queue: QueueSettings {
                mode: QueueMode::Buffered,
                capacity: 64,
                parallelism: 1,
            },
            parallelism: 1,
            retry: RetryPolicy::default(),
            classifier: Arc::new(|failure| {
                if matches!(failure, Failure::Wire(_)) {
                    FailureDisposition::Reject
                } else {
                    FailureDisposition::Retry
                }
            }),
        }
    }
}
impl<E: std::error::Error + 'static> EndpointOptions<E> {
    /// Supplies an explicit endpoint-only typed failure classifier.
    pub fn classify(
        mut self,
        classifier: impl Fn(&Failure<E>) -> FailureDisposition + Send + Sync + 'static,
    ) -> Self {
        self.classifier = Arc::new(classifier);
        self
    }
    fn validate(&self) -> Result<(), BusError> {
        if self.timeout.is_some_and(|timeout| {
            timeout < Duration::from_millis(1) || timeout > Duration::from_secs(3600)
        }) || !(1..=256).contains(&self.parallelism)
            || self.queue.mode != QueueMode::Buffered
            || !(1..=65536).contains(&self.queue.capacity)
            || !(1..=256).contains(&self.queue.parallelism)
        {
            return Err(BusError::Invalid("invalid composed endpoint options"));
        }
        Ok(())
    }
}

/// Application-owned receiving blocks returned by registration. No task starts
/// here. Attach local runners and durable endpoints to the application's Host;
/// bind the frozen message bus to durable endpoints if contextual routing is used.
#[derive(Default)]
pub struct DispatchOwners {
    /// Newly created bounded local queues and their sole runners.
    pub local: Vec<(String, LocalQueue, QueueRunner)>,
    /// Newly configured durable receiving owners; do not also create another
    /// DurableEndpoint for these aliases.
    pub durable: Vec<(String, DurableEndpoint)>,
}

/// Frozen global typed registration plus explicit receiving ownership.
pub struct DispatchRegistration<M, C: Chain<M>> {
    /// Global unassigned chain for typed scoped Invoke in classic mode. None for
    /// separated or all-sticky sets: these have no combined global execution.
    /// Sticky endpoint overrides never replace the global chain.
    pub global: Option<ScopedRegistration<M, Composed<C>>>,
    /// Receivers created during registration, never hidden background producers.
    pub owners: DispatchOwners,
}

/// Typed handler set with explicit identities and optional sticky assignments.
/// Registration defaults to classic composition, never changes low-level Runtime.
pub struct HandlerSet<M, C: Chain<M>> {
    chain: Arc<C>,
    sticky: BTreeMap<String, String>,
    behavior: MultipleHandlerBehavior,
    custody: Custody,
    options: BTreeMap<String, EndpointOptions<CompositionFailure<C::Error>>>,
    marker: PhantomData<fn(M)>,
}
impl<M, C> HandlerSet<M, C>
where
    M: WireMessage + Send + Sync + 'static,
    C: Chain<M>,
{
    /// Takes a typed Group or recursive Then chain.
    pub fn new(chain: C) -> Self {
        Self {
            chain: Arc::new(chain),
            sticky: BTreeMap::new(),
            behavior: MultipleHandlerBehavior::default(),
            custody: Custody::default(),
            options: BTreeMap::new(),
            marker: PhantomData,
        }
    }
    /// Chooses independent subscriptions instead of classic composition.
    pub fn behavior(mut self, behavior: MultipleHandlerBehavior) -> Self {
        self.behavior = behavior;
        self
    }
    /// Chooses custody for absent default/separated endpoints. A missing sticky
    /// alias always creates a local queue, as specified by sticky resolution.
    pub fn custody(mut self, custody: Custody) -> Self {
        self.custody = custody;
        self
    }
    /// Assigns one native group exclusively to an endpoint. Reassignment is an error.
    pub fn sticky(mut self, group: &str, endpoint: &str) -> Result<Self, BusError> {
        if self.sticky.contains_key(group) {
            return Err(BusError::Invalid("duplicate sticky assignment"));
        }
        self.sticky.insert(group.to_owned(), endpoint.to_owned());
        Ok(self)
    }
    /// Sets execution/retry configuration for exactly one selected endpoint.
    pub fn endpoint_options(
        mut self,
        endpoint: &str,
        options: EndpointOptions<CompositionFailure<C::Error>>,
    ) -> Result<Self, BusError> {
        options.validate()?;
        if self.options.contains_key(endpoint) {
            return Err(BusError::Invalid("duplicate endpoint options"));
        }
        self.options.insert(endpoint.to_owned(), options);
        Ok(self)
    }
    /// Registers routes, selected chains and owners atomically in the builder.
    /// Existing named local/durable endpoints win over fallback creation. Remote
    /// producer aliases cannot host handlers and are rejected, not provisioned.
    pub fn register(
        self,
        builder: &mut BusBuilder,
        default: &str,
        store: Store,
        flush: Arc<dyn Flush>,
    ) -> Result<DispatchRegistration<M, C>, BusError> {
        let mut ids = Vec::new();
        self.chain.identities(&mut ids);
        if ids.is_empty() || ids.len() > 16 {
            return Err(BusError::Invalid("handler group count outside 1..=16"));
        }
        let unique: BTreeSet<_> = ids.iter().collect();
        if unique.len() != ids.len() {
            return Err(BusError::Invalid("duplicate native handler identity"));
        }
        for id in &ids {
            validate_name(id)?;
        }
        validate_name(default)?;
        if self.sticky.keys().any(|id| !ids.contains(id)) {
            return Err(BusError::Invalid("unknown sticky handler identity"));
        }
        let global: Vec<_> = ids
            .iter()
            .filter(|id| !self.sticky.contains_key(*id))
            .cloned()
            .collect();
        let mut selected: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for id in &ids {
            let endpoint = if let Some(endpoint) = self.sticky.get(id) {
                endpoint.as_str()
            } else if self.behavior == MultipleHandlerBehavior::Separated {
                id.as_str()
            } else {
                default
            };
            if !self.sticky.contains_key(id) && self.sticky.values().any(|name| name == endpoint) {
                return Err(BusError::Invalid(
                    "leftover endpoint collides with sticky selection",
                ));
            }
            validate_name(endpoint)?;
            selected
                .entry(endpoint.to_owned())
                .or_default()
                .push(id.clone());
        }
        if self.behavior == MultipleHandlerBehavior::Separated
            && selected.values().any(|groups| groups.len() != 1)
        {
            return Err(BusError::Invalid(
                "separated endpoints require exactly one native handler group",
            ));
        }
        // Only classic mode installs a global leftover chain. Separated routes
        // must already select one group or an explicit endpoint-only override;
        // a second global chain would duplicate committed independent work.
        if self.behavior == MultipleHandlerBehavior::Separated || !global.is_empty() {
            for name in builder.route_names::<M>()? {
                if !selected.contains_key(&name)
                    && !matches!(builder.endpoints.get(&name), Some(Endpoint::Remote(_)))
                    && !builder.has_endpoint_handler::<M>(&name)?
                {
                    if self.behavior == MultipleHandlerBehavior::Separated {
                        return Err(BusError::Invalid(
                            "separated receiving routes need an explicit group binding or endpoint handler",
                        ));
                    }
                    selected.insert(name, global.clone());
                }
            }
        }
        if self.options.keys().any(|name| !selected.contains_key(name)) {
            return Err(BusError::Invalid("options for unused selected endpoint"));
        }
        // Work on a configuration snapshot; late errors cannot leave partial routes.
        let mut candidate = builder.clone();
        let mut owners = DispatchOwners::default();
        let mut options = self.options;
        for (name, groups) in selected {
            let options = options.remove(&name).unwrap_or_default();
            let handler = Arc::new(Composed::new(Arc::clone(&self.chain), groups.clone()));
            let sticky = self.sticky.values().any(|endpoint| endpoint == &name);
            let durable = !sticky && matches!(self.custody, Custody::Durable);
            install::<M, _>(
                &mut candidate,
                &name,
                handler,
                &store,
                Arc::clone(&flush),
                options,
                durable,
                &mut owners,
            )?;
            if groups.len() > 1 {
                candidate.combined.push(CombinedHandlers {
                    contract: M::NAME.into(),
                    endpoint: name,
                    groups,
                });
            }
        }
        let global = (self.behavior == MultipleHandlerBehavior::Classic && !global.is_empty())
            .then(|| ScopedRegistration {
                handler: Arc::new(Composed::new(self.chain, global)),
                store,
                flush,
                marker: PhantomData,
            });
        *builder = candidate;
        Ok(DispatchRegistration { global, owners })
    }

    /// Atomically registers conventional-local bindings with the selected handlers.
    /// All declared receiving targets are checked, including suppressed sources.
    /// Legacy registrations and explicit pins remain unchanged. Configure routing
    /// sources and their named endpoints before this operation.
    pub fn register_routed(
        self,
        builder: &mut BusBuilder,
        routing: &mut crate::routing::RoutingConfig,
        default: &str,
        store: Store,
        flush: Arc<dyn Flush>,
    ) -> Result<DispatchRegistration<M, C>, BusError> {
        let mut candidate = builder.clone();
        let mut config = routing.clone();
        config.handled::<M>()?;
        let original = candidate.route_names::<M>()?;
        let preview = candidate.routing_preview(&config)?;
        let key = (M::NAME.to_owned(), M::VERSION);
        // Reuse the existing selected-chain preflight, but expose every raw
        // candidate to it, not just the route source that currently wins.
        for endpoint in preview.targets.get(&key).into_iter().flatten() {
            candidate.ensure_route::<M>(endpoint)?;
        }
        let registration = self.register(&mut candidate, default, store, flush)?;
        conventionalize::<M>(&mut candidate, &mut config, &original)?;
        let resolver = candidate.routing_preview(&config)?;
        candidate.validate_routing(&resolver)?;
        *builder = candidate;
        *routing = config;
        Ok(registration)
    }
}

fn conventionalize<M: WireMessage>(
    builder: &mut BusBuilder,
    config: &mut crate::routing::RoutingConfig,
    original: &[String],
) -> Result<(), BusError> {
    let key = (M::NAME.to_owned(), M::VERSION);
    let mut receivers = BTreeSet::new();
    for endpoint in builder.route_names::<M>()? {
        if !matches!(builder.endpoints.get(&endpoint), Some(Endpoint::Remote(_))) {
            if !builder.has_endpoint_handler::<M>(&endpoint)? {
                return Err(BusError::Invalid("routed receiving target is unbound"));
            }
            receivers.insert(endpoint.clone());
            config.local_route::<M>(&endpoint)?;
        }
    }
    // Only pins added by this operation are reclassified. Existing exact calls,
    // including legacy handler-created pins, retain their historical fanout.
    if let Some(routes) = builder.routes.get_mut(&key) {
        routes.retain(|name| original.contains(name));
        if routes.is_empty() {
            builder.routes.remove(&key);
        }
    }
    builder.routed_receivers.insert(key, receivers);
    Ok(())
}

fn validate_name(name: &str) -> Result<(), BusError> {
    raddle::SubscriptionName::try_new(name)
        .map_err(|_| BusError::Invalid("invalid native handler/endpoint identity"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn install<M: WireMessage, H: ScopedHandler<M>>(
    builder: &mut BusBuilder,
    name: &str,
    handler: Arc<H>,
    store: &Store,
    flush: Arc<dyn Flush>,
    options: EndpointOptions<H::Error>,
    durable_default: bool,
    owners: &mut DispatchOwners,
) -> Result<(), BusError> {
    options.validate()?;
    validate_name(name)?;
    if !builder.endpoints.contains_key(name) {
        if durable_default {
            builder.durable(name, store.clone())?;
        } else {
            let (queue, runner) = LocalQueue::new(options.queue)?;
            builder.local(name, queue.clone())?;
            owners.local.push((name.to_owned(), queue, runner));
        }
    }
    match builder.endpoints.get(name) {
        Some(Endpoint::Local(_)) => {
            builder.scoped::<M, _>(name, handler, store.clone(), flush)?;
            if let Some(timeout) = options.timeout {
                builder.endpoint_timeout::<M>(name, timeout)?;
            }
        }
        Some(Endpoint::Durable(endpoint_store)) => {
            if builder.selected_durable.contains_key(name) {
                return Err(BusError::Invalid(
                    "durable receiving owner already selected",
                ));
            }
            let mut endpoint = DurableEndpoint::new(
                endpoint_store.clone(),
                name,
                options.parallelism,
                options.retry,
            )?;
            let classifier = options.classifier;
            endpoint.handle_with_policy::<M, H, _>(
                handler,
                flush,
                options.timeout.unwrap_or(builder.handler_timeout::<M>()?),
                move |failure| classifier(failure),
            )?;
            owners.durable.push((name.to_owned(), endpoint));
            builder
                .selected_durable
                .insert(name.to_owned(), (M::NAME.to_owned(), M::VERSION));
        }
        _ => {
            return Err(BusError::Invalid(
                "selected handler needs a local or durable endpoint",
            ));
        }
    }
    builder.ensure_route::<M>(name)?;
    Ok(())
}

impl BusBuilder {
    /// Inspects distinct selected native groups combined into logical handlers.
    pub fn combined_handlers(&self) -> &[CombinedHandlers] {
        &self.combined
    }
    /// Explicit startup assertion. Several phases of one group are not counted.
    pub fn assert_no_combined_handlers(&self) -> Result<(), BusError> {
        if self.combined.is_empty() {
            Ok(())
        } else {
            Err(BusError::Invalid(
                "multiple native handler groups combined; inspect combined_handlers",
            ))
        }
    }
    /// Registers a pre-built endpoint-only override; missing names create both a
    /// bounded local queue and its route. Other endpoints and ordinary Invoke
    /// retain their global handler. An existing durable alias returns its owner.
    pub fn sticky_handler<M: WireMessage, H: ScopedHandler<M>>(
        &mut self,
        endpoint: &str,
        handler: Arc<H>,
        store: Store,
        flush: Arc<dyn Flush>,
        options: EndpointOptions<H::Error>,
    ) -> Result<DispatchOwners, BusError> {
        let mut candidate = self.clone();
        let mut owners = DispatchOwners::default();
        install::<M, H>(
            &mut candidate,
            endpoint,
            handler,
            &store,
            flush,
            options,
            false,
            &mut owners,
        )?;
        *self = candidate;
        Ok(owners)
    }

    /// Routed pre-built endpoint-only override. Builder, metadata, provenance and
    /// returned receiving owners are committed together, or remain unchanged.
    #[allow(clippy::too_many_arguments)]
    pub fn sticky_handler_routed<M: WireMessage, H: ScopedHandler<M>>(
        &mut self,
        routing: &mut crate::routing::RoutingConfig,
        endpoint: &str,
        handler: Arc<H>,
        store: Store,
        flush: Arc<dyn Flush>,
        options: EndpointOptions<H::Error>,
    ) -> Result<DispatchOwners, BusError> {
        let mut candidate = self.clone();
        let mut config = routing.clone();
        config.handled::<M>()?;
        let original = candidate.route_names::<M>()?;
        let owners = candidate.sticky_handler::<M, H>(endpoint, handler, store, flush, options)?;
        conventionalize::<M>(&mut candidate, &mut config, &original)?;
        let resolver = candidate.routing_preview(&config)?;
        candidate.validate_routing(&resolver)?;
        *self = candidate;
        *routing = config;
        Ok(owners)
    }
}
