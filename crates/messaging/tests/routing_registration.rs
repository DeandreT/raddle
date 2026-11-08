//! Joint handler/routing snapshots preserve receiving ownership and legacy pins.
#![allow(clippy::unwrap_used)]

use raddle::RuntimeBuilder;
use raddle::routing::EndpointAlias;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_messaging::composition::{Group, Then};
use raddle_messaging::effects::HandlerOutput;
use raddle_messaging::handler_routing::{EndpointOptions, HandlerSet, MultipleHandlerBehavior};
use raddle_messaging::routing::{MessageDeclaration, MessageRole, MessageSelector, RoutingConfig};
use raddle_messaging::scoped::{HandleFuture, LocalOnlyFlush, ScopedHandler};
use raddle_messaging::{
    BoxFuture, BusBuilder, LocalQueue, QueueMode, QueueSettings, TrustedOrigin,
};
use raddle_postgres::{Scope, Store};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct Work;
impl WireMessage for Work {
    const NAME: &'static str = "routing.work";
    const VERSION: u32 = 1;
}
#[derive(Debug, thiserror::Error)]
#[error("fixture")]
struct Fault;
struct Handler;
impl ScopedHandler<Work> for Handler {
    type Resources = ();
    type Reply = ();
    type Error = Fault;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a raddle_messaging::MessageContext,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        _: &'a Work,
        _: &'a mut (),
        _: &'a mut Scope<'static>,
        _: &'a raddle_messaging::MessageContext,
    ) -> HandleFuture<'a, (), (), Fault> {
        Box::pin(async { Ok(HandlerOutput::reply(())) })
    }
}
#[derive(Clone)]
struct Remote;
impl Transport for Remote {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        Ok(())
    }
}
fn store() -> Store {
    Store::new(
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused/unused")
            .unwrap(),
    )
}
fn config() -> RoutingConfig {
    let mut config = RoutingConfig::default();
    config
        .declare(MessageDeclaration::of::<Work>(MessageRole::Handled).unwrap())
        .unwrap();
    config
}
fn settings() -> QueueSettings {
    QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 8,
        parallelism: 1,
    }
}
fn set() -> HandlerSet<Work, Then<Group<Handler>, Group<Handler>>> {
    HandlerSet::new(Then::new(
        Group::new("a", Arc::new(Handler)),
        Group::new("b", Arc::new(Handler)),
    ))
}

#[tokio::test]
async fn legacy_separated_remote_plus_a_b_stays_additive_in_routed_build() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.remote("upstream", Remote).unwrap();
    builder.route::<Work>("upstream").unwrap();
    let registration = set()
        .behavior(MultipleHandlerBehavior::Separated)
        .register(&mut builder, "unused", store(), Arc::new(LocalOnlyFlush))
        .unwrap();
    let bus = builder.build_routed(config()).unwrap();
    assert_eq!(
        bus.explain::<Work>()
            .unwrap()
            .routes
            .iter()
            .map(|r| r.endpoint.as_str())
            .collect::<Vec<_>>(),
        ["upstream", "a", "b"]
    );
    bus.send(
        &Work,
        TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        registration
            .owners
            .local
            .iter()
            .all(|(_, q, _)| q.snapshot().buffered == 1)
    );
}

#[tokio::test]
async fn routed_separated_locals_are_suppressed_by_explicit_but_owners_are_preserved() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.remote("upstream", Remote).unwrap();
    builder.route::<Work>("upstream").unwrap();
    let mut config = config();
    let registration = set()
        .behavior(MultipleHandlerBehavior::Separated)
        .register_routed(
            &mut builder,
            &mut config,
            "unused",
            store(),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    assert!(registration.global.is_none());
    assert_eq!(registration.owners.local.len(), 2);
    let bus = builder.build_routed(config).unwrap();
    assert_eq!(bus.explain::<Work>().unwrap().routes.len(), 1);
    bus.send(
        &Work,
        TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        registration
            .owners
            .local
            .iter()
            .all(|(_, q, _)| q.snapshot().buffered == 0)
    );
}

#[tokio::test]
async fn suppressed_ambiguous_receiving_target_rejects_both_snapshots_atomically() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let (queue, _runner) = LocalQueue::new(settings()).unwrap();
    builder.local("ambiguous", queue).unwrap();
    builder.remote("upstream", Remote).unwrap();
    builder.route::<Work>("upstream").unwrap();
    let mut config = config();
    config
        .convention(
            "suppressed",
            "Still validate the receiving target",
            |_: &MessageDeclaration| Ok(vec![EndpointAlias::try_new("ambiguous").unwrap()]),
        )
        .unwrap();
    assert!(
        set()
            .behavior(MultipleHandlerBehavior::Separated)
            .register_routed(
                &mut builder,
                &mut config,
                "unused",
                store(),
                Arc::new(LocalOnlyFlush)
            )
            .is_err()
    );
    assert!(builder.combined_handlers().is_empty());
    // No a/b endpoints or auto local bindings escaped the failed candidate.
    let registration = set()
        .behavior(MultipleHandlerBehavior::Separated)
        .register(&mut builder, "unused", store(), Arc::new(LocalOnlyFlush))
        .unwrap();
    assert_eq!(registration.owners.local.len(), 2);
    assert_eq!(builder.build().explain::<Work>().unwrap().routes.len(), 3);
    // Reusing config independently proves failure did not add conventional locals.
    let mut fresh = BusBuilder::new(RuntimeBuilder::default().build());
    fresh.remote("ambiguous", Remote).unwrap();
    let bus = fresh.build_routed(config).unwrap();
    assert_eq!(bus.explain::<Work>().unwrap().routes.len(), 1);
}

#[tokio::test]
async fn prebuilt_override_sticky_dedup_classic_and_late_unbound_guard() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let mut config = config();
    let owners = builder
        .sticky_handler_routed::<Work, _>(
            &mut config,
            "override",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    assert_eq!(owners.local.len(), 1);
    let registration = set()
        .sticky("a", "sticky")
        .unwrap()
        .register_routed(
            &mut builder,
            &mut config,
            "default",
            store(),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    assert!(registration.global.is_some());
    let bus = builder.clone().build_routed(config.clone()).unwrap();
    let explanation = bus.explain::<Work>().unwrap();
    assert_eq!(explanation.routes.len(), 3);
    let mut names: Vec<_> = explanation
        .routes
        .iter()
        .map(|r| r.endpoint.as_str())
        .collect();
    names.sort();
    assert_eq!(names, ["default", "override", "sticky"]);
    let (queue, _runner) = LocalQueue::new(settings()).unwrap();
    builder.local("late", queue).unwrap();
    config
        .rule(
            "late-route",
            "Cannot add an unselected receiver after binding",
            MessageSelector::exact::<Work>().unwrap(),
            vec![EndpointAlias::try_new("late").unwrap()],
        )
        .unwrap();
    assert!(builder.build_routed(config).is_err());
}
