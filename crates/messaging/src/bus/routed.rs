use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{Delivery, DeliveryKind};

use super::{BusBuilder, BusError, Endpoint, MessageBus, key};
use crate::effects::Outgoing;
use crate::routing::{Resolver, Route, RoutingConfig, RoutingError, RoutingExplanation};
use crate::{Cancellation, MessageContext, TrustedOrigin};

impl BusBuilder {
    /// Freezes declared layered publication plans and explanations. No I/O or
    /// tasks start here. Legacy route/handler registrations remain explicit pins;
    /// Invoke retains exactly the legacy route view, not publication projections.
    pub fn build_routed(mut self, config: RoutingConfig) -> Result<MessageBus, BusError> {
        let resolver = self.routing_preview(&config)?;
        self.validate_routing(&resolver)?;
        self.resolver = resolver;
        Ok(MessageBus(Arc::new(self)))
    }
    pub(crate) fn routing_preview(&self, config: &RoutingConfig) -> Result<Resolver, BusError> {
        // Actual ownership tightens grouping validation without rewriting the
        // declaration metadata observed by existing routing-source callbacks.
        let handled = self
            .handlers
            .keys()
            .map(|(_, key)| key.clone())
            .chain(self.selected_durable.values().cloned())
            .collect();
        for target in config.locals.values().flatten() {
            if matches!(self.endpoints.get(target), Some(Endpoint::Remote(_))) {
                return Err(RoutingError::Invalid(
                    "conventional local binding needs a receiving endpoint",
                )
                .into());
            }
        }
        Ok(Resolver::compile(
            config,
            &self.routes,
            &self.endpoints.keys().cloned().collect(),
            &handled,
        )?)
    }
    pub(crate) fn validate_routing(&self, resolver: &Resolver) -> Result<(), BusError> {
        for (key, targets) in &resolver.targets {
            for target in targets {
                if resolver.grouping_active(key)
                    && !matches!(self.endpoints.get(target), Some(Endpoint::Local(queue)) if queue.is_partitioned())
                {
                    return Err(BusError::Invalid(
                        "native grouping requires partitioned buffered receivers",
                    ));
                }
                if matches!(self.endpoints.get(target), Some(Endpoint::Local(_)))
                    && !self.handlers.contains_key(&(target.clone(), key.clone()))
                {
                    return Err(
                        RoutingError::Invalid("receiving target has no concrete handler").into(),
                    );
                }
                if let Some(allowed) = self.routed_receivers.get(key)
                    && !matches!(self.endpoints.get(target), Some(Endpoint::Remote(_)))
                    && !allowed.contains(target)
                {
                    return Err(RoutingError::Invalid("late unbound receiving target").into());
                }
            }
        }
        Ok(())
    }
}

impl MessageBus {
    pub(super) fn require_unpartitioned_execution(
        &self,
        envelope: &Envelope,
    ) -> Result<(), BusError> {
        envelope.require_legacy_dispatch()?;
        let key = (envelope.contract().to_owned(), envelope.version());
        if self.0.resolver.grouping_active(&key) || self.0.resolver.routes(&key).iter().any(|route| matches!(&self.0.endpoints[&route.destination],Endpoint::Local(queue) if queue.is_partitioned())) {
            return Err(BusError::Invalid("grouped contracts require buffered Send/Publish execution"));
        }
        Ok(())
    }
    /// Sends with explicit logical group/transport-key overrides. Only configured
    /// partitioned buffered receivers support dispatch metadata in this increment.
    pub async fn send_with_options<M: WireMessage>(
        &self,
        message: &M,
        origin: TrustedOrigin,
        options: raddle::grouping::DispatchOptions,
    ) -> Result<(), BusError> {
        self.dispatch(
            MessageContext::new(
                origin,
                with_options(Envelope::encode(message)?, options)?,
                Utc::now(),
            ),
            true,
        )
        .await
    }
    /// Publishes through the same compiled resolver with explicit overrides.
    pub async fn publish_with_options<M: WireMessage>(
        &self,
        message: &M,
        origin: TrustedOrigin,
        options: raddle::grouping::DispatchOptions,
    ) -> Result<(), BusError> {
        self.dispatch(
            MessageContext::new(
                origin,
                with_options(Envelope::encode(message)?, options)?,
                Utc::now(),
            ),
            false,
        )
        .await
    }
    /// Frozen publication plan and exact legacy invocation view. This executes
    /// neither discovery nor conversion callbacks and promises no handler success.
    pub fn explain<M: WireMessage>(&self) -> Result<RoutingExplanation, BusError> {
        Ok(self.0.resolver.explain(&key::<M>()?))
    }
    /// Explicit configured destination, bypassing publication selectors and
    /// projections. Confirmation means this endpoint's normal custody guarantee.
    pub async fn send_to<M: WireMessage>(
        &self,
        endpoint: &str,
        message: &M,
        origin: TrustedOrigin,
    ) -> Result<(), BusError> {
        self.send_to_with_options(endpoint, message, origin, Default::default())
            .await
    }
    /// Explicit configured buffered destination with native dispatch overrides.
    pub async fn send_to_with_options<M: WireMessage>(
        &self,
        endpoint: &str,
        message: &M,
        origin: TrustedOrigin,
        options: raddle::grouping::DispatchOptions,
    ) -> Result<(), BusError> {
        if !self.0.endpoints.contains_key(endpoint) {
            return Err(BusError::Invalid("unknown endpoint"));
        }
        let context = MessageContext::new(
            origin,
            with_options(Envelope::encode(message)?, options)?,
            Utc::now(),
        );
        let route = Route::direct(endpoint.into(), key::<M>()?, "direct");
        self.dispatch_routes(context, &[route], true).await
    }
    pub(super) async fn dispatch_routes(
        &self,
        context: MessageContext,
        routes: &[Route],
        required: bool,
    ) -> Result<(), BusError> {
        if required && routes.is_empty() {
            return Err(BusError::NoRoute);
        }
        let context = context.with_bus(self.clone());
        let mut prepared = BTreeMap::new();
        let mut admissions = Vec::new();
        // Resolve and capability-check every branch before admitting any. A later
        // unsupported profile must not leave an earlier buffer acknowledging it.
        let grouped = routes.iter().any(|route| matches!(&self.0.endpoints[&route.destination], Endpoint::Local(queue) if queue.is_partitioned()));
        for route in routes {
            let envelope = prepared
                .entry(route.path_key())
                .or_insert_with(|| {
                    let envelope =
                        route.prepare(context.envelope(), context.origin(), &self.0.propagated)?;
                    self.0
                        .resolver
                        .prepare_group(&envelope, context.origin(), grouped)
                })
                .clone();
            if let Ok(envelope) = &envelope {
                self.check_dispatch_destination(&route.destination, envelope)?;
            }
            admissions.push((route, envelope));
        }
        let mut failure = None;
        for (route, envelope) in admissions {
            let result = match envelope {
                Ok(envelope) => {
                    self.admit(&route.destination, context.clone().with_envelope(envelope))
                        .await
                }
                Err(error) => Err(BusError::Routing(error)),
            };
            if let Err(error) = result
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    fn check_dispatch_destination(
        &self,
        destination: &str,
        envelope: &Envelope,
    ) -> Result<(), BusError> {
        if envelope.dispatch().is_some()
            && !matches!(&self.0.endpoints[destination], Endpoint::Local(queue) if queue.is_partitioned())
        {
            return Err(BusError::Invalid(
                "native dispatch unsupported by destination",
            ));
        }
        Ok(())
    }
    async fn admit(&self, destination: &str, context: MessageContext) -> Result<(), BusError> {
        self.check_dispatch_destination(destination, context.envelope())?;
        let key = (
            context.envelope().contract().to_owned(),
            context.envelope().version(),
        );
        match &self.0.endpoints[destination] {
            Endpoint::Policy(queue) => {
                queue
                    .submit(context.with_cancellation(Cancellation::new()))
                    .await?
            }
            Endpoint::Local(queue) => {
                let handler = Arc::clone(
                    self.0
                        .handlers
                        .get(&(destination.into(), key.clone()))
                        .ok_or(BusError::Invalid("local route has no handler"))?,
                );
                let context = context.with_cancellation(Cancellation::new());
                let timeout = self
                    .0
                    .endpoint_timeouts
                    .get(&(destination.into(), key.clone()))
                    .copied()
                    .or_else(|| self.0.timeouts.get(&key).copied())
                    .unwrap_or(self.0.timeout);
                if queue.is_partitioned() {
                    let envelope = context.envelope().clone();
                    let tenant = context.origin().tenant().to_owned();
                    let exempt = self.0.resolver.exempt(&envelope);
                    queue
                        .submit_envelope(&envelope, &tenant, exempt, async move {
                            handler(context, tokio::time::Instant::now() + timeout).await
                        })
                        .await?;
                } else {
                    let deadline = tokio::time::Instant::now() + timeout;
                    queue
                        .submit(async move { handler(context, deadline).await })
                        .await?;
                }
            }
            Endpoint::Durable(store) => {
                let mut scope = store.begin().await?;
                scope
                    .stage_received(
                        context.origin().source(),
                        context.origin().tenant(),
                        context.envelope(),
                        &[Delivery::new(DeliveryKind::Local, destination, Utc::now())],
                    )
                    .await?;
                scope.commit(Utc::now()).await?;
            }
            Endpoint::Remote(transport) => {
                transport.publish(destination, context.envelope()).await?
            }
        }
        Ok(())
    }
    /// Resolves all payload branches once, before transaction staging. Any
    /// volatile destination or failed projection rejects the whole returned unit.
    pub fn intents<M: WireMessage>(
        &self,
        message: &M,
        context: &MessageContext,
        required: bool,
    ) -> Result<Vec<Outgoing>, BusError> {
        let envelope = context.outgoing(Envelope::encode(message)?, &self.0.propagated)?;
        self.envelope_intents(envelope, context, required)
    }
    pub(super) fn envelope_intents(
        &self,
        envelope: Envelope,
        context: &MessageContext,
        required: bool,
    ) -> Result<Vec<Outgoing>, BusError> {
        envelope.require_legacy_dispatch()?;
        context.envelope().require_legacy_dispatch()?;
        let key = (envelope.contract().to_owned(), envelope.version());
        let routes = self.0.resolver.routes(&key);
        if self.0.resolver.grouping_active(&key) || routes.iter().any(|route| matches!(&self.0.endpoints[&route.destination], Endpoint::Local(queue) if queue.is_partitioned())) {
            return Err(BusError::Invalid("native grouping returned intents require future prepared custody"));
        }
        if required && routes.is_empty() {
            return Err(BusError::NoRoute);
        }
        let mut outputs: Vec<Outgoing> = Vec::new();
        let mut prepared = BTreeMap::new();
        for route in routes {
            let kind = match &self.0.endpoints[&route.destination] {
                Endpoint::Local(_) | Endpoint::Policy(_) => return Err(BusError::NonAtomicRoute),
                Endpoint::Durable(_) => DeliveryKind::Local,
                Endpoint::Remote(_) => DeliveryKind::Remote,
            };
            let path = route.path_key();
            let index = if let Some(index) = prepared.get(&path) {
                *index
            } else {
                let output = route.prepare(&envelope, context.origin(), &self.0.propagated)?;
                let output = self
                    .0
                    .resolver
                    .prepare_group(&output, context.origin(), false)?;
                output.require_legacy_dispatch()?;
                let index = outputs.len();
                outputs.push(Outgoing {
                    envelope: output,
                    deliveries: Vec::new(),
                });
                prepared.insert(path, index);
                index
            };
            outputs[index]
                .deliveries
                .push(Delivery::new(kind, &route.destination, context.now()));
        }
        Ok(outputs)
    }
}

fn with_options(
    envelope: Envelope,
    options: raddle::grouping::DispatchOptions,
) -> Result<Envelope, raddle::wire::WireError> {
    if options.group.is_none() && options.partition_key.is_none() {
        return Ok(envelope);
    }
    envelope.with_dispatch(raddle::grouping::Dispatch::explicit(options))
}
