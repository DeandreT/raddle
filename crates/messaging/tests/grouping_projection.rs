//! Final-contract projection grouping and input-local corrected retry behavior.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::grouping::{Dispatch, GroupId};
use raddle::routing::EndpointAlias;
use raddle::sequencing::SlotSet;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::routing::{
    GroupingRule, MessageDeclaration, MessageRole, MessageSelector, RoutingConfig, RoutingError,
    RuleMatch,
};
use raddle_messaging::{
    BusBuilder, LocalQueue, MessageContext, QueueError, QueueMode, QueueSettings, TrustedOrigin,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc};

#[derive(Serialize, Deserialize)]
struct Work {
    id: usize,
}
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Child {
    id: usize,
    key: String,
}
impl WireMessage for Child {
    const NAME: &'static str = "group.child";
    const VERSION: u32 = 1;
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant", "source", None).unwrap()
}
fn alias() -> Vec<EndpointAlias> {
    vec![EndpointAlias::try_new("orders").unwrap()]
}
fn queue() -> (LocalQueue, raddle_messaging::QueueRunner) {
    LocalQueue::partitioned(
        QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 16,
            parallelism: 2,
        },
        SlotSet::new("orders", 7).unwrap(),
    )
    .unwrap()
}
async fn next<T>(rx: &mut mpsc::Receiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn join(task: tokio::task::JoinHandle<Result<(), raddle_host::HostError>>) {
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn resolved_none_is_not_inherited_by_new_projected_identity_but_empty_and_key_are() {
    let (queue, runner) = queue();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("orders", queue).unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    builder
        .handle::<Child, _, _>("orders", move |_, context| {
            let tx = tx.clone();
            async move {
                tx.send(context.envelope().clone()).await.unwrap();
                Ok(())
            }
        })
        .unwrap();
    let mut config = RoutingConfig::default();
    config
        .declare(MessageDeclaration::of::<Work>(MessageRole::PublishOnly).unwrap())
        .unwrap();
    config
        .declare(MessageDeclaration::of::<Child>(MessageRole::Handled).unwrap())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    config
        .topology(
            "child",
            MessageSelector::exact::<Child>().unwrap(),
            vec![],
            vec![GroupingRule::typed::<Child>(move |child| {
                observed.fetch_add(1, Ordering::SeqCst);
                RuleMatch::Matched(GroupId::new(&child.key))
            })],
            alias(),
        )
        .unwrap();
    config
        .transform::<Work, Child>("projection", 1, |work| {
            Ok(Child {
                id: work.id,
                key: "child-group".into(),
            })
        })
        .unwrap();
    let bus = builder.build_routed(config).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    for group in [None, Some(GroupId::new(""))] {
        let root = Envelope::encode(&Work { id: 1 })
            .unwrap()
            .with_dispatch(Dispatch::resolved(
                group.clone(),
                Some("explicit-key".into()),
            ))
            .unwrap();
        bus.dispatch(
            MessageContext::new(origin(), root.clone(), Utc::now()),
            true,
        )
        .await
        .unwrap();
        let child = next(&mut rx).await;
        assert_ne!(child.id(), root.id());
        assert_eq!(
            child.dispatch().unwrap().partition_key(),
            Some("explicit-key")
        );
        assert_eq!(
            child.dispatch().unwrap().group().unwrap().as_str(),
            group.as_ref().map_or("child-group", GroupId::as_str)
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown.shutdown();
    join(task).await;
}

#[tokio::test]
async fn bad_grouping_input_at_start_middle_end_does_not_gate_neighbors_or_corrected_retry() {
    for (failed, should_panic) in [
        (0, false),
        (1, false),
        (2, false),
        (0, true),
        (1, true),
        (2, true),
    ] {
        let (queue, runner) = queue();
        let observed = queue.clone();
        let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
        builder.local("orders", queue).unwrap();
        let (tx, mut rx) = mpsc::channel(16);
        builder
            .handle::<Work, _, _>("orders", move |work, _| {
                let tx = tx.clone();
                async move {
                    tx.send(work.id).await.unwrap();
                    Ok(())
                }
            })
            .unwrap();
        let corrected = Arc::new(AtomicBool::new(false));
        let fixed = corrected.clone();
        let mut config = RoutingConfig::default();
        config
            .declare(MessageDeclaration::of::<Work>(MessageRole::Handled).unwrap())
            .unwrap();
        config
            .grouping()
            .rule(GroupingRule::custom(move |envelope, _| {
                let work = envelope
                    .decode::<Work>()
                    .map_err(|_| RoutingError::Grouping)?;
                if work.id == failed && !fixed.load(Ordering::SeqCst) {
                    assert!(!should_panic, "synthetic grouping callback panic");
                    return Err(RoutingError::Grouping);
                }
                Ok(RuleMatch::Matched(GroupId::new("shared")))
            }))
            .unwrap();
        config
            .rule(
                "route",
                "Route",
                MessageSelector::exact::<Work>().unwrap(),
                alias(),
            )
            .unwrap();
        let bus = builder.build_routed(config).unwrap();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, "orders").unwrap();
        let shutdown = host.handle();
        let task = tokio::spawn(host.run());
        let inputs: Vec<_> = (0..3)
            .map(|id| Envelope::encode(&Work { id }).unwrap())
            .collect();
        for (id, envelope) in inputs.iter().enumerate() {
            assert_eq!(
                bus.dispatch(
                    MessageContext::new(origin(), envelope.clone(), Utc::now()),
                    true
                )
                .await
                .is_ok(),
                id != failed
            );
        }
        let mut seen = vec![next(&mut rx).await, next(&mut rx).await];
        assert!(!seen.contains(&failed));
        corrected.store(true, Ordering::SeqCst);
        bus.dispatch(
            MessageContext::new(origin(), inputs[failed].clone(), Utc::now()),
            true,
        )
        .await
        .unwrap();
        seen.push(next(&mut rx).await);
        bus.send(&Work { id: 3 }, origin()).await.unwrap();
        seen.push(next(&mut rx).await);
        seen.sort();
        assert_eq!(seen, vec![0, 1, 2, 3]);
        shutdown.shutdown();
        join(task).await;
        assert_eq!(observed.snapshot().completed, 4);
        assert_eq!(observed.snapshot().failed, 0);
    }
}

#[tokio::test]
async fn handler_failure_retries_only_explicit_failed_input_and_does_not_replay_successes() {
    for failed in 0..3 {
        let (queue, runner) = queue();
        let observed = queue.clone();
        let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
        builder.local("orders", queue).unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        let corrected = Arc::new(AtomicBool::new(false));
        let fixed = corrected.clone();
        builder
            .handle::<Work, _, _>("orders", move |work, _| {
                let tx = tx.clone();
                let fixed = fixed.clone();
                async move {
                    tx.send(work.id).await.unwrap();
                    if work.id == failed && !fixed.load(Ordering::SeqCst) {
                        Err(QueueError::Handler)
                    } else {
                        Ok(())
                    }
                }
            })
            .unwrap();
        let mut config = RoutingConfig::default();
        config
            .declare(MessageDeclaration::of::<Work>(MessageRole::Handled).unwrap())
            .unwrap();
        config.grouping().rule(GroupingRule::tenant()).unwrap();
        config
            .rule(
                "route",
                "Route",
                MessageSelector::exact::<Work>().unwrap(),
                alias(),
            )
            .unwrap();
        let bus = builder.build_routed(config).unwrap();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, "orders").unwrap();
        let shutdown = host.handle();
        let task = tokio::spawn(host.run());
        let inputs: Vec<_> = (0..3)
            .map(|id| Envelope::encode(&Work { id }).unwrap())
            .collect();
        for envelope in &inputs {
            bus.dispatch(
                MessageContext::new(origin(), envelope.clone(), Utc::now()),
                true,
            )
            .await
            .unwrap();
        }
        let mut seen = vec![
            next(&mut rx).await,
            next(&mut rx).await,
            next(&mut rx).await,
        ];
        corrected.store(true, Ordering::SeqCst);
        bus.dispatch(
            MessageContext::new(origin(), inputs[failed].clone(), Utc::now()),
            true,
        )
        .await
        .unwrap();
        seen.push(next(&mut rx).await);
        assert_eq!(seen, vec![0, 1, 2, failed]);
        shutdown.shutdown();
        join(task).await;
        assert_eq!(observed.snapshot().failed, 1);
        assert_eq!(observed.snapshot().completed, 3);
    }
}

#[tokio::test]
async fn concrete_family_and_predicate_exemptions_are_additive_and_allow_same_group_overlap() {
    for selector in [
        MessageSelector::exact::<Child>().unwrap(),
        MessageSelector::family("telemetry").unwrap(),
        MessageSelector::predicate(|message| message.in_family("telemetry")),
    ] {
        let (queue, runner) = LocalQueue::partitioned(
            QueueSettings {
                mode: QueueMode::Buffered,
                capacity: 8,
                parallelism: 2,
            },
            SlotSet::new("orders", 1).unwrap(),
        )
        .unwrap();
        let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
        builder.local("orders", queue).unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        let gate = Arc::new(Semaphore::new(0));
        let held = gate.clone();
        let work_tx = tx.clone();
        builder
            .handle::<Work, _, _>("orders", move |_, _| {
                let tx = work_tx.clone();
                let gate = held.clone();
                async move {
                    tx.send(1).await.unwrap();
                    gate.acquire().await.unwrap().forget();
                    Ok(())
                }
            })
            .unwrap();
        builder
            .handle::<Child, _, _>("orders", move |_, _| {
                let tx = tx.clone();
                async move {
                    tx.send(2).await.unwrap();
                    Ok(())
                }
            })
            .unwrap();
        let mut config = RoutingConfig::default();
        config
            .declare(MessageDeclaration::of::<Work>(MessageRole::Handled).unwrap())
            .unwrap();
        config
            .declare(
                MessageDeclaration::of::<Child>(MessageRole::Handled)
                    .unwrap()
                    .family("telemetry")
                    .unwrap(),
            )
            .unwrap();
        config.grouping().rule(GroupingRule::tenant()).unwrap();
        config.grouping().exempt(selector).unwrap();
        config
            .grouping()
            .exempt(MessageSelector::predicate(|_| false))
            .unwrap();
        config
            .rule(
                "route",
                "Route",
                MessageSelector::predicate(|_| true),
                alias(),
            )
            .unwrap();
        let bus = builder.build_routed(config).unwrap();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, "orders").unwrap();
        let shutdown = host.handle();
        let task = tokio::spawn(host.run());
        bus.send(&Work { id: 1 }, origin()).await.unwrap();
        assert_eq!(next(&mut rx).await, 1);
        bus.send(
            &Child {
                id: 2,
                key: "unused".into(),
            },
            origin(),
        )
        .await
        .unwrap();
        assert_eq!(next(&mut rx).await, 2);
        gate.add_permits(1);
        shutdown.shutdown();
        join(task).await;
    }
}
