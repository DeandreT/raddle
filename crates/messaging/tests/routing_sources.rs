//! Actual publication, ingress and intent paths over one compiled native resolver.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{TimeZone, Utc};
use raddle::routing::EndpointAlias;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, RequestFamily, RuntimeBuilder};
use raddle_messaging::routing::{
    LocalRouting, MessageDeclaration, MessageRole, MessageSelector, RoutingConfig, RoutingError,
    SourceOptions,
};
use raddle_messaging::{
    BoxFuture, BusBuilder, BusError, JsonIngress, LocalQueue, MessageContext, QueueMode,
    QueueSettings, RemoteInvoker, TrustedOrigin,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order(i32);
impl WireMessage for Order {
    const NAME: &'static str = "routing.order";
    const VERSION: u32 = 1;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Other(i32);
impl WireMessage for Other {
    const NAME: &'static str = "routing.other";
    const VERSION: u32 = 1;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct V2(i32);
impl WireMessage for V2 {
    const NAME: &'static str = "routing.order";
    const VERSION: u32 = 2;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reply(i32);
impl WireMessage for Reply {
    const NAME: &'static str = "routing.reply";
    const VERSION: u32 = 1;
}
impl Request for Order {
    type Reply = Reply;
    type Error = Infallible;
}

type History = Arc<Mutex<Vec<(String, Envelope)>>>;
#[derive(Clone, Default)]
struct Recorder(History);
impl Transport for Recorder {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.0
            .lock()
            .unwrap()
            .push((destination.into(), envelope.clone()));
        if destination == "broken" {
            Err(TransportError::Transient)
        } else {
            Ok(())
        }
    }
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant-a", "source-a", None).unwrap()
}
fn alias(name: &str) -> EndpointAlias {
    EndpointAlias::try_new(name).unwrap()
}
fn config() -> RoutingConfig {
    let mut config = RoutingConfig::default();
    config
        .declare(
            MessageDeclaration::of::<Order>(MessageRole::Outgoing)
                .unwrap()
                .module("app::orders::events")
                .unwrap()
                .family("orders")
                .unwrap(),
        )
        .unwrap();
    config
        .declare(
            MessageDeclaration::of::<Other>(MessageRole::PublishOnly)
                .unwrap()
                .module("app::orders_extra")
                .unwrap(),
        )
        .unwrap();
    config
        .declare(
            MessageDeclaration::of::<V2>(MessageRole::PublishOnly)
                .unwrap()
                .module("app::orders")
                .unwrap()
                .family("orders")
                .unwrap(),
        )
        .unwrap();
    config
}
fn builder(recorder: &Recorder) -> BusBuilder {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    for name in ["custom", "explicit", "convention", "projected", "broken"] {
        builder.remote(name, recorder.clone()).unwrap();
    }
    let (queue, _runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Inline,
        capacity: 8,
        parallelism: 1,
    })
    .unwrap();
    builder.local("local", queue).unwrap();
    let captured = recorder.clone();
    builder
        .handle::<Order, _, _>("local", move |_, context| {
            let captured = captured.clone();
            async move {
                captured
                    .0
                    .lock()
                    .unwrap()
                    .push(("local".into(), context.envelope().clone()));
                Ok(())
            }
        })
        .unwrap();
    builder
}
fn names(recorder: &Recorder) -> Vec<String> {
    recorder
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|(name, _)| name.clone())
        .collect()
}
fn rule(config: &mut RoutingConfig, target: &str) {
    config
        .rule(
            "order-rule",
            "Exact order publication",
            MessageSelector::exact::<Order>().unwrap(),
            vec![alias(target)],
        )
        .unwrap();
}
fn conventions(config: &mut RoutingConfig) {
    config
        .convention(
            "external-convention",
            "Declared external convention",
            |_: &MessageDeclaration| Ok(vec![alias("convention")]),
        )
        .unwrap();
}

#[tokio::test]
async fn custom_accumulation_pass_through_and_empty_terminating_stages() {
    for (options, expected) in [
        (SourceOptions::ADDITIVE, vec!["custom"]),
        (SourceOptions::TERMINATING, vec!["custom"]),
        (SourceOptions::PASS_THROUGH, vec!["custom", "explicit"]),
    ] {
        let recorder = Recorder::default();
        let builder = builder(&recorder);
        let mut config = config();
        config
            .source(
                "custom-source",
                "An application source",
                options,
                |_: &MessageDeclaration| Ok(vec![alias("custom")]),
            )
            .unwrap();
        rule(&mut config, "explicit");
        conventions(&mut config);
        let bus = builder.build_routed(config).unwrap();
        let explanation = bus.explain::<Order>().unwrap();
        assert_eq!(
            explanation
                .routes
                .iter()
                .map(|r| r.endpoint.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        bus.send(&Order(1), origin()).await.unwrap();
        bus.publish(&Order(2), origin()).await.unwrap();
        let encoded = Envelope::encode(&Order(3)).unwrap();
        bus.receive(
            &JsonIngress::new(Default::default()),
            &encoded.to_vec().unwrap(),
            origin(),
            Utc::now(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(names(&recorder), expected.repeat(3));
        let parent =
            MessageContext::new(origin(), Envelope::encode(&Other(0)).unwrap(), Utc::now());
        let intents = bus.intents(&Order(4), &parent, true).unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(
            intents[0]
                .deliveries
                .iter()
                .map(|d| d.destination.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        if options == SourceOptions::ADDITIVE {
            let transforms = explanation
                .steps
                .iter()
                .find(|step| step.source == "transforms")
                .unwrap();
            assert!(!transforms.skipped);
            assert_eq!(transforms.contributions, 0);
            assert_eq!(transforms.blocking, 1);
            assert!(
                explanation
                    .steps
                    .iter()
                    .find(|step| step.source == "explicit")
                    .unwrap()
                    .skipped
            );
        }
    }
}

#[tokio::test]
async fn no_match_pass_through_empty_chain_and_no_subscriber_defaults() {
    for contributes in [false, true] {
        let recorder = Recorder::default();
        let builder = builder(&recorder);
        let mut config = config();
        config
            .source(
                "empty-or-pass",
                "Pass through never blocks",
                SourceOptions::PASS_THROUGH,
                move |_: &MessageDeclaration| {
                    Ok(if contributes {
                        vec![alias("custom")]
                    } else {
                        Vec::new()
                    })
                },
            )
            .unwrap();
        conventions(&mut config);
        let bus = builder.build_routed(config).unwrap();
        bus.send(&Order(1), origin()).await.unwrap();
        assert_eq!(
            names(&recorder),
            if contributes {
                vec!["custom", "convention"]
            } else {
                vec!["convention"]
            }
        );
    }
    let bus = BusBuilder::new(RuntimeBuilder::default().build())
        .build_routed(config())
        .unwrap();
    assert!(matches!(
        bus.send(&Order(1), origin()).await,
        Err(BusError::NoRoute)
    ));
    bus.publish(&Order(1), origin()).await.unwrap();
    assert!(bus.explain::<Order>().unwrap().routes.is_empty());
}

#[tokio::test]
async fn duplicates_count_before_dedup_and_frozen_callbacks_do_not_run_during_execution() {
    let recorder = Recorder::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = config();
    config
        .source(
            "pass",
            "First pass contribution",
            SourceOptions::PASS_THROUGH,
            |_: &MessageDeclaration| Ok(vec![alias("custom")]),
        )
        .unwrap();
    let count = Arc::clone(&calls);
    config
        .source(
            "normal",
            "Duplicate blocking contribution",
            SourceOptions::ADDITIVE,
            move |_: &MessageDeclaration| {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(vec![alias("custom"), alias("custom")])
            },
        )
        .unwrap();
    conventions(&mut config);
    let bus = builder(&recorder).build_routed(config).unwrap();
    let before = calls.load(Ordering::SeqCst);
    let explanation = bus.explain::<Order>().unwrap();
    assert_eq!(explanation.routes.len(), 1);
    assert_eq!(explanation.routes[0].sources, ["pass", "normal"]);
    assert_eq!(explanation.steps[1].contributions, 2);
    assert_eq!(explanation.steps[2].blocking, 2);
    bus.publish(&Order(1), origin()).await.unwrap();
    assert_eq!(names(&recorder), ["custom"]);
    assert_eq!(calls.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn explicit_local_additive_and_disabled_semantics() {
    for (mode, explicit, expected) in [
        (LocalRouting::Terminating, false, vec!["local"]),
        (LocalRouting::Additive, false, vec!["local", "convention"]),
        (LocalRouting::Disabled, false, vec!["convention"]),
        (LocalRouting::Additive, true, vec!["explicit"]),
    ] {
        let recorder = Recorder::default();
        let mut config = config();
        config.local_route::<Order>("local").unwrap();
        config.local_routing(mode);
        if explicit {
            rule(&mut config, "explicit");
        }
        conventions(&mut config);
        let bus = builder(&recorder).build_routed(config).unwrap();
        bus.publish(&Order(1), origin()).await.unwrap();
        assert_eq!(names(&recorder), expected);
    }
    // Disabled conventional local does not disable an explicit local queue route.
    let recorder = Recorder::default();
    let mut builder = builder(&recorder);
    let (queue, _runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Inline,
        capacity: 1,
        parallelism: 1,
    })
    .unwrap();
    builder.local("actual-local", queue).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let called = count.clone();
    builder
        .handle::<Order, _, _>("actual-local", move |_, _| {
            let called = called.clone();
            async move {
                called.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    builder.route::<Order>("actual-local").unwrap();
    let mut config = config();
    config.local_routing(LocalRouting::Disabled);
    builder
        .build_routed(config)
        .unwrap()
        .send(&Order(1), origin())
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn selectors_are_finite_concrete_versioned_and_module_boundary_aware() {
    for selector in [
        MessageSelector::family("orders").unwrap(),
        MessageSelector::module("app::orders").unwrap(),
        MessageSelector::predicate(|message| message.role() == MessageRole::PublishOnly),
    ] {
        let recorder = Recorder::default();
        let mut config = config();
        config
            .rule(
                "selection",
                "Bounded declared selection",
                selector,
                vec![alias("explicit")],
            )
            .unwrap();
        let bus = builder(&recorder).build_routed(config).unwrap();
        bus.publish(&Order(1), origin()).await.unwrap();
        bus.publish(&Other(2), origin()).await.unwrap();
        bus.publish(&V2(3), origin()).await.unwrap();
        bus.publish(&Reply(4), origin()).await.unwrap(); // undeclared
        let contracts: Vec<_> = recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|(_, e)| (e.contract().to_owned(), e.version()))
            .collect();
        assert_eq!(contracts.len(), 2);
        assert!(contracts.contains(&("routing.order".into(), 2)));
        assert!(!contracts.contains(&("routing.reply".into(), 1)));
    }
    let recorder = Recorder::default();
    let mut config = config();
    rule(&mut config, "explicit");
    let bus = builder(&recorder).build_routed(config).unwrap();
    bus.publish(&V2(1), origin()).await.unwrap();
    assert!(names(&recorder).is_empty());
}

#[tokio::test]
async fn transform_priority_mixed_intents_stable_identity_and_origin_namespace() {
    let recorder = Recorder::default();
    let mut config = config();
    config
        .source(
            "pass",
            "Retain original publication",
            SourceOptions::PASS_THROUGH,
            |message: &MessageDeclaration| {
                Ok(
                    if message.contract().name() == Order::NAME
                        && message.contract().version().get() == 1
                    {
                        vec![alias("custom")]
                    } else {
                        Vec::new()
                    },
                )
            },
        )
        .unwrap();
    config
        .transform::<Order, Other>("to-other", 1, |m| Ok(Other(m.0 * 2)))
        .unwrap();
    rule(&mut config, "explicit");
    config
        .rule(
            "target",
            "Target publication",
            MessageSelector::exact::<Other>().unwrap(),
            vec![alias("projected")],
        )
        .unwrap();
    let mut builder = builder(&recorder);
    for header in [
        "application",
        "raddle-reply-to",
        "raddle-request-token",
        "raddle-reply-mode",
    ] {
        builder.propagate_header(header).unwrap();
    }
    let bus = builder.build_routed(config).unwrap();
    let time = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut root = Envelope::with_identity(Uuid::from_u128(1), time, &Order(3))
        .unwrap()
        .correlated(Uuid::from_u128(2), Uuid::from_u128(3))
        .unwrap();
    for header in [
        "application",
        "unlisted",
        "raddle-reply-to",
        "raddle-request-token",
        "raddle-reply-mode",
    ] {
        root = root.with_header(header.into(), "value".into()).unwrap();
    }
    for _ in 0..2 {
        bus.dispatch(MessageContext::new(origin(), root.clone(), time), true)
            .await
            .unwrap();
    }
    let first_id = {
        let observations = recorder.0.lock().unwrap();
        assert_eq!(
            observations
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["custom", "projected", "custom", "projected"]
        );
        let projected = &observations[1].1;
        assert_eq!(projected.id(), observations[3].1.id());
        assert_ne!(projected.id(), root.id());
        assert_eq!(projected.decode::<Other>().unwrap().0, 6);
        assert_eq!(projected.created_at(), time);
        assert_eq!(projected.correlation_id(), root.correlation_id());
        assert_eq!(projected.causation_id(), root.causation_id());
        assert_eq!(
            projected
                .headers()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["application"]
        );
        let first_id = projected.id();
        assert_eq!(
            first_id,
            Uuid::parse_str("eeb38438-06d6-879b-8a55-551e2808b970").unwrap()
        );
        first_id
    };
    for other_origin in [
        TrustedOrigin::authenticated("tenant-b", "source-a", None).unwrap(),
        TrustedOrigin::authenticated("tenant-a", "source-b", None).unwrap(),
    ] {
        bus.dispatch(MessageContext::new(other_origin, root.clone(), time), true)
            .await
            .unwrap();
        assert_ne!(first_id, recorder.0.lock().unwrap().last().unwrap().1.id());
    }
    let parent = MessageContext::new(origin(), Envelope::encode(&Reply(0)).unwrap(), time);
    assert!(matches!(
        bus.intent(&Order(3), &parent, true),
        Err(BusError::Routing(RoutingError::MultipleEnvelopes))
    ));
    let intents = bus.intents(&Order(3), &parent, true).unwrap();
    assert_eq!(intents.len(), 2);
    assert_eq!(
        intents[1].envelope.causation_id(),
        Some(parent.envelope().id())
    );
    let explanation = bus.explain::<Order>().unwrap();
    assert_eq!(explanation.routes[1].projections, ["to-other@1"]);
    assert_eq!(
        explanation
            .routes
            .iter()
            .map(|r| r.endpoint.as_str())
            .collect::<Vec<_>>(),
        ["custom", "projected"]
    );
}

#[tokio::test]
async fn unresolvable_transform_falls_through_and_failed_inputs_do_not_stop_neighbors() {
    let recorder = Recorder::default();
    let mut config = config();
    config
        .transform::<Order, Other>("to-other", 1, |m| Ok(Other(m.0)))
        .unwrap();
    rule(&mut config, "explicit");
    let bus = builder(&recorder).build_routed(config).unwrap();
    bus.send(&Order(1), origin()).await.unwrap();
    assert_eq!(names(&recorder), ["explicit"]);

    for failed in [1, 2, 3] {
        let recorder = Recorder::default();
        let mut config = self::config();
        config
            .transform::<Order, Other>("fallible", 1, move |m| {
                if m.0 == failed {
                    return Err(RoutingError::Projection);
                }
                if m.0 == -1 {
                    panic!("input-specific projection panic");
                }
                Ok(Other(m.0))
            })
            .unwrap();
        config
            .rule(
                "target",
                "Target publication",
                MessageSelector::exact::<Other>().unwrap(),
                vec![alias("projected")],
            )
            .unwrap();
        let bus = builder(&recorder).build_routed(config).unwrap();
        for id in [1, 2, 3, -1, 4] {
            assert_eq!(
                bus.send(&Order(id), origin()).await.is_ok(),
                id != failed && id != -1
            );
        }
        assert_eq!(names(&recorder).len(), 3);
        // Invalid typed source content is also scoped to the input.
        let malformed = Envelope::with_identity(Uuid::new_v4(), Utc::now(), &Order(5)).unwrap();
        let mut value = serde_json::to_value(&malformed).unwrap();
        value["payload"] = serde_json::json!({"wrong":true});
        let malformed =
            Envelope::from_slice(&serde_json::to_vec(&value).unwrap(), Default::default()).unwrap();
        assert!(
            bus.receive(
                &JsonIngress::new(Default::default()),
                &malformed.to_vec().unwrap(),
                origin(),
                Utc::now(),
                true
            )
            .await
            .is_err()
        );
        bus.send(&Order(6), origin()).await.unwrap();
        assert_eq!(names(&recorder).len(), 4);
    }
}

#[tokio::test]
async fn failing_destinations_do_not_skip_healthy_siblings_or_new_work() {
    for broken in 0..3 {
        let recorder = Recorder::default();
        let mut builder = builder(&recorder);
        let mut routes = vec!["explicit", "projected"];
        routes.insert(broken, "broken");
        for route in routes {
            builder.route::<Order>(route).unwrap();
        }
        let bus = builder.build_routed(config()).unwrap();
        for id in [1, 2] {
            assert!(bus.send(&Order(id), origin()).await.is_err());
        }
        assert_eq!(names(&recorder).len(), 6);
    }
}

#[tokio::test]
async fn malformed_and_unroutable_inputs_at_every_position_leave_current_and_new_work_healthy() {
    for failed in 0..3 {
        for failure in 0..3 {
            let recorder = Recorder::default();
            let mut config = self::config();
            config
                .transform::<Order, Other>("canonical", 1, |m| Ok(Other(m.0)))
                .unwrap();
            config
                .rule(
                    "target",
                    "Canonical target",
                    MessageSelector::exact::<Other>().unwrap(),
                    vec![alias("projected")],
                )
                .unwrap();
            let bus = builder(&recorder).build_routed(config).unwrap();
            for position in 0..3 {
                let root = Envelope::encode(&Order(position)).unwrap();
                let bytes = if position == failed {
                    if failure == 0 {
                        b"not-json".to_vec()
                    } else if failure == 1 {
                        Envelope::from_parts(
                            root.metadata(),
                            serde_json::json!({"wrong":"shape"}),
                            Default::default(),
                        )
                        .unwrap()
                        .to_vec()
                        .unwrap()
                    } else {
                        Envelope::encode(&Reply(position))
                            .unwrap()
                            .to_vec()
                            .unwrap()
                    }
                } else {
                    root.to_vec().unwrap()
                };
                assert_eq!(
                    bus.receive(
                        &JsonIngress::new(Default::default()),
                        &bytes,
                        origin(),
                        Utc::now(),
                        true
                    )
                    .await
                    .is_ok(),
                    position != failed
                );
            }
            bus.send(&Order(4), origin()).await.unwrap();
            let history = recorder.0.lock().unwrap();
            assert_eq!(history.len(), 3);
            let numbers: Vec<_> = history
                .iter()
                .map(|(_, e)| e.decode::<Other>().unwrap().0)
                .collect();
            assert!(!numbers.contains(&failed));
            assert_eq!(numbers.last(), Some(&4));
        }
    }
}

#[test]
fn startup_rejects_cycles_undeclared_unknown_conflicting_and_unbounded_sources() {
    let recorder = Recorder::default();
    let mut config = config();
    config
        .transform::<Order, Other>("a", 1, |m| Ok(Other(m.0)))
        .unwrap();
    config
        .transform::<Other, Order>("b", 1, |m| Ok(Order(m.0)))
        .unwrap();
    assert!(builder(&recorder).build_routed(config).is_err());
    let mut config = self::config();
    config
        .source(
            "unknown",
            "Even suppressed outputs must be valid",
            SourceOptions::ADDITIVE,
            |_: &MessageDeclaration| Ok(vec![alias("not-configured")]),
        )
        .unwrap();
    assert!(builder(&recorder).build_routed(config).is_err());
    let mut config = self::config();
    config
        .source(
            "large",
            "Bound each source",
            SourceOptions::ADDITIVE,
            |_: &MessageDeclaration| Ok(vec![alias("explicit"); 17]),
        )
        .unwrap();
    assert!(builder(&recorder).build_routed(config).is_err());
    let mut config = self::config();
    config
        .source(
            "panic",
            "Startup panic is an error",
            SourceOptions::ADDITIVE,
            |_: &MessageDeclaration| -> Result<Vec<EndpointAlias>, RoutingError> {
                panic!("startup callback");
            },
        )
        .unwrap();
    assert!(builder(&recorder).build_routed(config).is_err());
    let mut config = self::config();
    assert!(
        config
            .declare(MessageDeclaration::of::<Order>(MessageRole::Handled).unwrap())
            .is_err()
    );
    assert!(MessageSelector::module("app::::orders").is_err());
    assert!(config.local_route::<Reply>("local").is_err());
}

struct Rpc(Arc<AtomicUsize>);
impl RemoteInvoker for Rpc {
    fn request<'a>(
        &'a self,
        destination: &'a str,
        _: &'a MessageContext,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Envelope, BusError>> {
        Box::pin(async move {
            assert_eq!(destination, "explicit");
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Envelope::encode(&Reply(99))?)
        })
    }
}
#[tokio::test]
async fn local_family_and_exact_remote_invoke_are_not_publication_routing() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut family =
        RequestFamily::new(|m: Order, _| async move { Ok::<_, Infallible>(Reply(m.0)) });
    family.member::<Order>(|m| m).unwrap();
    let mut runtime = RuntimeBuilder::default();
    runtime.handle_family(family).unwrap();
    for local in [true, false] {
        let recorder = Recorder::default();
        let mut builder = if local {
            BusBuilder::new(runtime.build())
        } else {
            builder(&recorder)
        };
        if local {
            for name in ["explicit", "custom"] {
                builder.remote(name, recorder.clone()).unwrap();
            }
        }
        builder.route::<Order>("explicit").unwrap();
        builder.remote_invoker(Arc::new(Rpc(calls.clone())), true);
        let mut config = self::config();
        config
            .source(
                "publication-only",
                "Do not rewrite requests",
                SourceOptions::TERMINATING,
                |_: &MessageDeclaration| Ok(vec![alias("custom")]),
            )
            .unwrap();
        let bus = builder.build_routed(config).unwrap();
        assert_eq!(
            bus.invoke(Order(7), origin()).await.unwrap().0,
            if local { 7 } else { 99 }
        );
        assert_eq!(bus.explain::<Order>().unwrap().invocation, ["explicit"]);
        bus.publish(&Order(8), origin()).await.unwrap();
        assert_eq!(names(&recorder), ["custom"]);
        // RuntimeBuilder is consumed; use an empty runtime for the second pass.
        runtime = RuntimeBuilder::default();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn direct_destination_uses_configured_alias_not_selectors() {
    let recorder = Recorder::default();
    let mut config = config();
    rule(&mut config, "explicit");
    let bus = builder(&recorder).build_routed(config).unwrap();
    bus.send_to("custom", &Order(1), origin()).await.unwrap();
    assert_eq!(names(&recorder), ["custom"]);
    assert!(
        bus.send_to("tcp://untrusted", &Order(2), origin())
            .await
            .is_err()
    );
}

#[derive(Serialize, Deserialize)]
struct Version<const V: u32>(i32);
impl<const V: u32> WireMessage for Version<V> {
    const NAME: &'static str = "routing.chain";
    const VERSION: u32 = V;
}

#[tokio::test]
async fn bounded_multi_hop_projection_keeps_ordered_path_and_target_body() {
    let recorder = Recorder::default();
    let mut config = RoutingConfig::default();
    config
        .declare(MessageDeclaration::of::<Version<1>>(MessageRole::Outgoing).unwrap())
        .unwrap();
    config
        .declare(MessageDeclaration::of::<Version<2>>(MessageRole::Outgoing).unwrap())
        .unwrap();
    config
        .declare(MessageDeclaration::of::<Version<3>>(MessageRole::PublishOnly).unwrap())
        .unwrap();
    config
        .transform::<Version<1>, Version<2>>("first", 1, |m| Ok(Version(m.0 + 1)))
        .unwrap();
    config
        .transform::<Version<2>, Version<3>>("second", 2, |m| Ok(Version(m.0 * 2)))
        .unwrap();
    config
        .rule(
            "target",
            "Final target",
            MessageSelector::exact::<Version<3>>().unwrap(),
            vec![alias("projected")],
        )
        .unwrap();
    let bus = builder(&recorder).build_routed(config).unwrap();
    bus.send(&Version::<1>(3), origin()).await.unwrap();
    assert_eq!(
        recorder.0.lock().unwrap()[0]
            .1
            .decode::<Version<3>>()
            .unwrap()
            .0,
        8
    );
    let explanation = bus.explain::<Version<1>>().unwrap();
    assert_eq!(explanation.routes[0].projections, ["first@1", "second@2"]);
    assert_eq!(explanation.sources["target"], "Final target");
}

#[test]
fn depth_effective_fanout_and_source_limits_are_enforced_independently() {
    let mut config = RoutingConfig::default();
    macro_rules! declarations { ($($v:literal),*) => { $(config.declare(MessageDeclaration::of::<Version<$v>>(MessageRole::Outgoing).unwrap()).unwrap();)* }; }
    declarations!(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
    macro_rules! transitions { ($(($from:literal,$to:literal)),*) => { $(config.transform::<Version<$from>,Version<$to>>(&format!("step-{}",$from),1,|m| Ok(Version(m.0))).unwrap();)* }; }
    transitions!(
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 6),
        (6, 7),
        (7, 8),
        (8, 9),
        (9, 10),
        (10, 11),
        (11, 12),
        (12, 13),
        (13, 14),
        (14, 15),
        (15, 16),
        (16, 17)
    );
    assert!(
        BusBuilder::new(RuntimeBuilder::default().build())
            .build_routed(config)
            .is_err()
    );

    let recorder = Recorder::default();
    let mut builder = builder(&recorder);
    let mut config = self::config();
    let names: Vec<_> = (0..17).map(|i| format!("endpoint-{i}")).collect();
    for name in &names {
        builder.remote(name, recorder.clone()).unwrap();
    }
    let targets: Vec<_> = names[..16].iter().map(|name| alias(name)).collect();
    config
        .source(
            "sixteen",
            "Sixteen pass-through outputs",
            SourceOptions::PASS_THROUGH,
            move |_: &MessageDeclaration| Ok(targets.clone()),
        )
        .unwrap();
    config
        .source(
            "seventeenth",
            "One more distinct output",
            SourceOptions::PASS_THROUGH,
            |_: &MessageDeclaration| Ok(vec![alias("endpoint-16")]),
        )
        .unwrap();
    assert!(builder.build_routed(config).is_err());
    let mut config = RoutingConfig::default();
    for id in 0..32 {
        config
            .source(
                &format!("source-{id}"),
                "Bounded source",
                SourceOptions::ADDITIVE,
                |_: &MessageDeclaration| Ok(Vec::new()),
            )
            .unwrap();
    }
    assert!(
        config
            .source(
                "too-many",
                "Capacity failure",
                SourceOptions::ADDITIVE,
                |_: &MessageDeclaration| Ok(Vec::new())
            )
            .is_err()
    );
    assert!(
        RoutingConfig::default()
            .source(
                "explicit",
                "Reserved name",
                SourceOptions::ADDITIVE,
                |_: &MessageDeclaration| Ok(Vec::new())
            )
            .is_err()
    );
}
