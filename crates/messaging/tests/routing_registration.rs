//! Joint handler/routing snapshots preserve receiving ownership and legacy pins.
#![allow(clippy::unwrap_used)]

use raddle::RuntimeBuilder;
use raddle::routing::EndpointAlias;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_messaging::composition::{Group, Then};
use raddle_messaging::effects::HandlerOutput;
use raddle_messaging::handler_routing::{EndpointOptions, HandlerSet, MultipleHandlerBehavior};
use raddle_messaging::routing::RoutingError;
use raddle_messaging::routing::{MessageDeclaration, MessageRole, MessageSelector, RoutingConfig};
use raddle_messaging::scoped::{HandleFuture, LocalOnlyFlush, ScopedHandler};
use raddle_messaging::{
    BoxFuture, BusBuilder, BusError, LocalQueue, QueueMode, QueueSettings, TrustedOrigin,
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

#[tokio::test]
async fn sequential_routed_prebuilt_preserve_prior_receivers() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let mut config = config();
    let first = builder
        .sticky_handler_routed::<Work, _>(
            &mut config,
            "first",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    let second = builder.sticky_handler_routed::<Work, _>(
        &mut config,
        "second",
        Arc::new(Handler),
        store(),
        Arc::new(LocalOnlyFlush),
        EndpointOptions::default(),
    );
    assert!(
        second.is_ok(),
        "second independent pre-built registration rejected: {:?}",
        second.as_ref().err()
    );
    let second = second.unwrap();
    assert_eq!(first.local.len(), 1);
    assert_eq!(second.local.len(), 1);
    let before = builder
        .clone()
        .build_routed(config.clone())
        .unwrap()
        .explain::<Work>()
        .unwrap();
    assert_eq!(
        before
            .routes
            .iter()
            .map(|route| route.endpoint.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert!(
        builder
            .sticky_handler_routed::<Work, _>(
                &mut config,
                "first",
                Arc::new(Handler),
                store(),
                Arc::new(LocalOnlyFlush),
                EndpointOptions::default(),
            )
            .is_err()
    );
    let bus = builder.build_routed(config).unwrap();
    assert_eq!(bus.explain::<Work>().unwrap(), before);
    bus.send(
        &Work,
        TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(first.local[0].1.snapshot().buffered, 1);
    assert_eq!(second.local[0].1.snapshot().buffered, 1);
}

async fn append_prebuilt_to_routed_set(behavior: MultipleHandlerBehavior) {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let mut config = config();
    let selected = set()
        .behavior(behavior)
        .register_routed(
            &mut builder,
            &mut config,
            "combined",
            store(),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    let additional = builder.sticky_handler_routed::<Work, _>(
        &mut config,
        "additional",
        Arc::new(Handler),
        store(),
        Arc::new(LocalOnlyFlush),
        EndpointOptions::default(),
    );
    assert!(
        additional.is_ok(),
        "pre-built registration after routed {behavior:?} set rejected: {:?}",
        additional.as_ref().err()
    );
    let additional = additional.unwrap();
    assert_eq!(additional.local.len(), 1);
    let expected: &[&str] = if behavior == MultipleHandlerBehavior::Classic {
        assert!(selected.global.is_some());
        assert_eq!(builder.combined_handlers().len(), 1);
        &["combined", "additional"]
    } else {
        assert!(selected.global.is_none());
        builder.assert_no_combined_handlers().unwrap();
        &["a", "b", "additional"]
    };
    let bus = builder.build_routed(config).unwrap();
    assert_eq!(
        bus.explain::<Work>()
            .unwrap()
            .routes
            .iter()
            .map(|route| route.endpoint.as_str())
            .collect::<Vec<_>>(),
        expected
    );
    bus.send(
        &Work,
        TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        selected
            .owners
            .local
            .iter()
            .all(|(_, queue, _)| queue.snapshot().buffered == 1)
    );
    assert_eq!(additional.local[0].1.snapshot().buffered, 1);
}

#[tokio::test]
async fn routed_classic_set_then_prebuilt_preserve_prior_receivers() {
    append_prebuilt_to_routed_set(MultipleHandlerBehavior::Classic).await;
}

#[tokio::test]
async fn routed_separated_set_then_prebuilt_preserve_prior_receivers() {
    append_prebuilt_to_routed_set(MultipleHandlerBehavior::Separated).await;
}

#[tokio::test]
async fn cumulative_receivers_still_reject_suppressed_unowned_targets_atomically() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let mut config = config();
    let first = builder
        .sticky_handler_routed::<Work, _>(
            &mut config,
            "first",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    let second = builder
        .sticky_handler_routed::<Work, _>(
            &mut config,
            "second",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    builder.durable("unowned", store()).unwrap();
    let before = builder
        .clone()
        .build_routed(config.clone())
        .unwrap()
        .explain::<Work>()
        .unwrap();
    let mut suppressed = config.clone();
    suppressed
        .convention(
            "late-unowned",
            "Suppressed is not permission to take custody",
            |_: &MessageDeclaration| Ok(vec![EndpointAlias::try_new("unowned").unwrap()]),
        )
        .unwrap();
    let failure = builder.sticky_handler_routed::<Work, _>(
        &mut suppressed,
        "additional",
        Arc::new(Handler),
        store(),
        Arc::new(LocalOnlyFlush),
        EndpointOptions::default(),
    );
    assert!(matches!(
        failure,
        Err(BusError::Routing(RoutingError::Invalid(
            "late unbound receiving target"
        )))
    ));
    assert_eq!(
        builder
            .clone()
            .build_routed(config.clone())
            .unwrap()
            .explain::<Work>()
            .unwrap(),
        before
    );
    assert_eq!(first.local[0].1.snapshot().buffered, 0);
    assert_eq!(second.local[0].1.snapshot().buffered, 0);

    // The failed registration must not add `additional` to either snapshot.
    // Legitimately bind the previously unowned target, then reuse the rejected
    // config: any leaked local binding would now fail on its missing endpoint.
    let bound = builder
        .sticky_handler_routed::<Work, _>(
            &mut config,
            "unowned",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    assert_eq!(bound.durable.len(), 1);
    assert_eq!(
        builder
            .clone()
            .build_routed(suppressed)
            .unwrap()
            .explain::<Work>()
            .unwrap()
            .routes,
        before.routes
    );
    let retried = builder
        .sticky_handler_routed::<Work, _>(
            &mut config,
            "additional",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    assert_eq!(retried.local.len(), 1);
    assert_eq!(
        builder
            .build_routed(config)
            .unwrap()
            .explain::<Work>()
            .unwrap()
            .routes
            .len(),
        4
    );
}

#[derive(Serialize, Deserialize)]
struct WorkV2;
impl WireMessage for WorkV2 {
    const NAME: &'static str = Work::NAME;
    const VERSION: u32 = 2;
}
impl ScopedHandler<WorkV2> for Handler {
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
        _: &'a WorkV2,
        _: &'a mut (),
        _: &'a mut Scope<'static>,
        _: &'a raddle_messaging::MessageContext,
    ) -> HandleFuture<'a, (), (), Fault> {
        Box::pin(async { Ok(HandlerOutput::reply(())) })
    }
}

#[tokio::test]
async fn cumulative_receiving_ownership_is_scoped_to_concrete_contract_version() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let mut config = config();
    let first = builder
        .sticky_handler_routed::<Work, _>(
            &mut config,
            "first",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    builder.durable("versioned", store()).unwrap();
    let versioned = builder
        .sticky_handler_routed::<WorkV2, _>(
            &mut config,
            "versioned",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    let second = builder
        .sticky_handler_routed::<Work, _>(
            &mut config,
            "second",
            Arc::new(Handler),
            store(),
            Arc::new(LocalOnlyFlush),
            EndpointOptions::default(),
        )
        .unwrap();
    assert_eq!(versioned.durable.len(), 1);
    assert_eq!(first.local.len() + second.local.len(), 2);
    let bus = builder.clone().build_routed(config.clone()).unwrap();
    assert_eq!(bus.explain::<Work>().unwrap().routes.len(), 2);
    assert_eq!(bus.explain::<WorkV2>().unwrap().routes[0].version, 2);
    assert_eq!(
        bus.explain::<WorkV2>().unwrap().routes[0].endpoint,
        "versioned"
    );
    config
        .convention(
            "wrong-version",
            "Another schema's receiver is not an owned receiver",
            |message: &MessageDeclaration| {
                Ok(if message.contract().version().get() == 1 {
                    vec![EndpointAlias::try_new("versioned").unwrap()]
                } else {
                    Vec::new()
                })
            },
        )
        .unwrap();
    assert!(matches!(
        builder.build_routed(config),
        Err(BusError::Routing(RoutingError::Invalid(
            "late unbound receiving target"
        )))
    ));
}
