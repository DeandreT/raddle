//! Actual compiled-rule execution through bounded buffered receiving owners.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::grouping::{DispatchOptions, GroupId};
use raddle::routing::EndpointAlias;
use raddle::sequencing::SlotSet;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::routing::{
    GroupingRule, MessageDeclaration, MessageRole, MessageSelector, RoutingConfig, RuleMatch,
};
use raddle_messaging::{
    BusBuilder, JsonIngress, LocalQueue, MessageContext, QueueMode, QueueSettings, TrustedOrigin,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize)]
struct Work {
    id: usize,
    key: Option<String>,
}
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant", "source", None).unwrap()
}
fn aliases() -> Vec<EndpointAlias> {
    vec![EndpointAlias::try_new("orders").unwrap()]
}
async fn next(receiver: &mut mpsc::Receiver<Envelope>) -> Envelope {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}
fn config(role: MessageRole) -> RoutingConfig {
    let mut config = RoutingConfig::default();
    config
        .declare(
            MessageDeclaration::of::<Work>(role)
                .unwrap()
                .family("orders")
                .unwrap(),
        )
        .unwrap();
    config
}
fn builder() -> (
    BusBuilder,
    raddle_messaging::QueueRunner,
    mpsc::Receiver<Envelope>,
) {
    let (queue, runner) = LocalQueue::partitioned(
        QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 32,
            parallelism: 4,
        },
        SlotSet::new("orders", 7).unwrap(),
    )
    .unwrap();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("orders", queue).unwrap();
    let (tx, rx) = mpsc::channel(32);
    builder
        .handle::<Work, _, _>("orders", move |_, context| {
            let tx = tx.clone();
            async move {
                tx.send(context.envelope().clone()).await.unwrap();
                Ok(())
            }
        })
        .unwrap();
    (builder, runner, rx)
}

#[tokio::test]
async fn priorities_null_absent_explicit_empty_and_appended_rules_execute() {
    let (builder, runner, mut received) = builder();
    let mut config = config(MessageRole::Handled);
    config
        .grouping()
        .rule(GroupingRule::properties(vec!["key".into()]).unwrap())
        .unwrap();
    config.grouping().rule(GroupingRule::tenant()).unwrap();
    let reached = Arc::new(AtomicUsize::new(0));
    let observed = reached.clone();
    config
        .grouping()
        .rule(GroupingRule::custom(move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(RuleMatch::Matched(GroupId::new("unreached")))
        }))
        .unwrap();
    config
        .topology(
            "topology",
            MessageSelector::family("orders").unwrap(),
            vec![],
            vec![GroupingRule::typed::<Work>(|work| {
                work.key.as_ref().map_or(RuleMatch::NoMatch, |key| {
                    RuleMatch::Matched(GroupId::new(format!("topology:{key}")))
                })
            })],
            aliases(),
        )
        .unwrap();
    let bus = builder.build_routed(config).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    bus.send(
        &Work {
            id: 1,
            key: Some("blue".into()),
        },
        origin(),
    )
    .await
    .unwrap();
    bus.publish_with_options(
        &Work {
            id: 2,
            key: Some("blue".into()),
        },
        origin(),
        DispatchOptions {
            group: Some(GroupId::new("")),
            partition_key: Some("explicit-key".into()),
        },
    )
    .await
    .unwrap();
    let base = Envelope::encode(&Work { id: 3, key: None }).unwrap();
    for (id, payload) in [(3, json!({"id":3,"key":null})), (4, json!({"id":4}))] {
        let mut metadata = base.metadata();
        metadata.id = uuid::Uuid::from_u128(id);
        let envelope = Envelope::from_parts(metadata, payload, Default::default()).unwrap();
        bus.receive(
            &JsonIngress::new(Default::default()),
            &envelope.to_vec().unwrap(),
            origin(),
            Utc::now(),
            true,
        )
        .await
        .unwrap();
    }
    let mut values = Vec::new();
    for _ in 0..4 {
        let envelope = next(&mut received).await;
        let dispatch = envelope.dispatch().unwrap();
        values.push((
            envelope.decode::<Work>().unwrap().id,
            dispatch.group().unwrap().as_str().to_owned(),
            dispatch.partition_key().map(str::to_owned),
        ));
    }
    values.sort();
    assert_eq!(
        values,
        vec![
            (1, "topology:blue".into(), None),
            (2, "".into(), Some("explicit-key".into())),
            (3, "".into(), None),
            (4, "tenant".into(), None)
        ]
    );
    assert_eq!(reached.load(Ordering::SeqCst), 0);
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn overlap_checks_membership_not_runtime_match_or_explicit_override() {
    for role in [MessageRole::Handled, MessageRole::PublishOnly] {
        let (builder, _runner, _received) = builder();
        let mut config = config(role);
        for id in ["one", "two"] {
            config
                .topology(
                    id,
                    MessageSelector::exact::<Work>().unwrap(),
                    vec![],
                    vec![GroupingRule::custom(|_, _| Ok(RuleMatch::NoMatch))],
                    aliases(),
                )
                .unwrap();
        }
        let result = builder.build_routed(config);
        // A real receiving registration wins even over inaccurate PublishOnly metadata.
        assert!(result.is_err());
    }
    let outgoing_builder = BusBuilder::new(RuntimeBuilder::default().build());
    let mut outgoing = config(MessageRole::PublishOnly);
    for id in ["one", "two"] {
        outgoing
            .topology(
                id,
                MessageSelector::exact::<Work>().unwrap(),
                vec![],
                vec![GroupingRule::custom(|_, _| Ok(RuleMatch::NoMatch))],
                vec![],
            )
            .unwrap();
    }
    let bus = outgoing_builder.build_routed(outgoing).unwrap();
    assert!(matches!(
        bus.publish_with_options(
            &Work { id: 1, key: None },
            origin(),
            DispatchOptions {
                group: Some(GroupId::new("override")),
                partition_key: None
            }
        )
        .await,
        Err(raddle_messaging::BusError::Routing(_))
    ));
    let (builder, _runner, _received) = builder();
    let mut config = config(MessageRole::Handled);
    config
        .topology(
            "one",
            MessageSelector::exact::<Work>().unwrap(),
            vec![],
            vec![GroupingRule::tenant()],
            aliases(),
        )
        .unwrap();
    config
        .topology(
            "excluded",
            MessageSelector::family("orders").unwrap(),
            vec![MessageSelector::exact::<Work>().unwrap()],
            vec![GroupingRule::tenant()],
            aliases(),
        )
        .unwrap();
    assert!(
        config
            .topology(
                "one",
                MessageSelector::exact::<Work>().unwrap(),
                vec![],
                vec![GroupingRule::tenant()],
                aliases()
            )
            .is_err()
    );
    assert!(builder.build_routed(config).is_ok());
}

#[test]
fn grouping_needs_declared_inventory_and_unavailable_propagation_fails_at_build() {
    let (mut bus, _runner, _received) = builder();
    bus.route::<Work>("orders").unwrap();
    let mut missing = RoutingConfig::default();
    missing.grouping().rule(GroupingRule::tenant()).unwrap();
    assert!(bus.clone().build_routed(missing).is_err());
    let mut propagation = config(MessageRole::Handled);
    propagation.grouping().propagate_group_to_partition_key();
    assert!(bus.build_routed(propagation).is_err());
}

#[test]
fn actual_handler_validation_does_not_reclassify_metadata_seen_by_routing_sources() {
    let (builder, _runner, _received) = builder();
    let mut config = config(MessageRole::PublishOnly);
    config
        .source(
            "observer",
            "Declared roles stay unchanged",
            raddle_messaging::routing::SourceOptions::ADDITIVE,
            |message: &MessageDeclaration| {
                assert_eq!(message.role(), MessageRole::PublishOnly);
                Ok(Vec::new())
            },
        )
        .unwrap();
    config
        .topology(
            "one",
            MessageSelector::exact::<Work>().unwrap(),
            vec![],
            vec![GroupingRule::tenant()],
            aliases(),
        )
        .unwrap();
    assert!(builder.build_routed(config).is_ok());
}

#[tokio::test]
async fn protected_framing_refuses_before_any_custom_or_permissive_typed_extraction() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (builder, runner, mut received) = builder();
    let mut config = config(MessageRole::Handled);
    let observed = calls.clone();
    config
        .grouping()
        .rule(GroupingRule::custom(move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(RuleMatch::NoMatch)
        }))
        .unwrap();
    let observed = calls.clone();
    config
        .grouping()
        .rule(GroupingRule::typed::<Work>(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            RuleMatch::Matched(GroupId::new("bad"))
        }))
        .unwrap();
    config
        .rule(
            "route",
            "Route",
            MessageSelector::exact::<Work>().unwrap(),
            aliases(),
        )
        .unwrap();
    let bus = builder.build_routed(config).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    // Poison first/middle/last, with corrected retry and new healthy neighbors.
    for id in 0..3 {
        let healthy = Envelope::encode(&Work { id, key: None }).unwrap();
        let protected = Envelope::from_parts(
            healthy.metadata(),
            json!({"id":id,"key":null,"format":"raddle-aes-test"}),
            Default::default(),
        )
        .unwrap();
        assert!(
            bus.dispatch(MessageContext::new(origin(), protected, Utc::now()), true)
                .await
                .is_err()
        );
        bus.dispatch(MessageContext::new(origin(), healthy, Utc::now()), true)
            .await
            .unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    for _ in 0..3 {
        next(&mut received).await;
    }
    assert!(received.try_recv().is_err());
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn sequenced_null_is_stable_but_not_shared_empty_and_inferred_falls_through() {
    let (builder, runner, mut received) = builder();
    let mut config = config(MessageRole::Handled);
    config
        .grouping()
        .rule(GroupingRule::inferred::<Work>(|work| {
            work.key.as_ref().map(GroupId::new)
        }))
        .unwrap();
    config
        .grouping()
        .rule(GroupingRule::sequenced::<Work>(|_| None))
        .unwrap();
    config
        .rule(
            "route",
            "Route",
            MessageSelector::exact::<Work>().unwrap(),
            aliases(),
        )
        .unwrap();
    let bus = builder.build_routed(config).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    let envelope = Envelope::encode(&Work { id: 1, key: None }).unwrap();
    for envelope in [
        envelope.clone(),
        envelope,
        Envelope::encode(&Work { id: 2, key: None }).unwrap(),
        Envelope::encode(&Work {
            id: 3,
            key: Some("native-stream".into()),
        })
        .unwrap(),
    ] {
        bus.dispatch(MessageContext::new(origin(), envelope, Utc::now()), true)
            .await
            .unwrap();
    }
    for id in 100..132 {
        let envelope = Envelope::with_identity(
            uuid::Uuid::from_u128(id as u128),
            Utc::now(),
            &Work { id, key: None },
        )
        .unwrap();
        bus.dispatch(MessageContext::new(origin(), envelope, Utc::now()), true)
            .await
            .unwrap();
    }
    let mut groups = std::collections::BTreeMap::<usize, Vec<String>>::new();
    for _ in 0..36 {
        let envelope = next(&mut received).await;
        groups
            .entry(envelope.decode::<Work>().unwrap().id)
            .or_default()
            .push(
                envelope
                    .dispatch()
                    .unwrap()
                    .group()
                    .unwrap()
                    .as_str()
                    .into(),
            );
    }
    assert_eq!(groups[&1][0], groups[&1][1]);
    assert_ne!(groups[&1][0], groups[&2][0]);
    assert!(!groups[&1][0].is_empty());
    assert_eq!(groups[&3], vec!["native-stream"]);
    let slots = SlotSet::new("orders", 7).unwrap();
    let selected: std::collections::BTreeSet<_> = groups
        .range(100..)
        .map(|(_, values)| slots.slot("tenant", Some(&GroupId::new(&values[0])), uuid::Uuid::nil()))
        .collect();
    assert!(
        selected.len() > 1,
        "null sequence orders must not collapse into one shared group/slot"
    );
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
