//! Barrier-driven buffered group, collision and exemption execution.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::grouping::{Dispatch, DispatchOptions, GroupId};
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
use tokio::sync::{Semaphore, mpsc};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Work {
    id: usize,
    key: Option<String>,
}
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Telemetry {
    id: usize,
}
impl WireMessage for Telemetry {
    const NAME: &'static str = "group.telemetry";
    const VERSION: u32 = 1;
}

fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant", "source", None).unwrap()
}
fn settings(capacity: usize, parallelism: usize) -> QueueSettings {
    QueueSettings {
        mode: QueueMode::Buffered,
        capacity,
        parallelism,
    }
}
fn config() -> RoutingConfig {
    let mut config = RoutingConfig::default();
    config
        .declare(
            MessageDeclaration::of::<Work>(MessageRole::Handled)
                .unwrap()
                .family("orders")
                .unwrap(),
        )
        .unwrap();
    config
        .declare(
            MessageDeclaration::of::<Telemetry>(MessageRole::Handled)
                .unwrap()
                .family("telemetry")
                .unwrap(),
        )
        .unwrap();
    config
}
fn route(config: &mut RoutingConfig) {
    config
        .rule(
            "all",
            "Test receiving owner",
            MessageSelector::predicate(|_| true),
            vec![EndpointAlias::try_new("orders").unwrap()],
        )
        .unwrap();
}
async fn next<T>(receiver: &mut mpsc::Receiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn send_publish_direct_receive_share_slots_and_exemptions_overlap() {
    let slots = SlotSet::new("orders", 7).unwrap();
    let (queue, runner) = LocalQueue::partitioned(settings(16, 3), slots.clone()).unwrap();
    let observed = queue.clone();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("orders", queue).unwrap();
    let (sender, mut receiver) = mpsc::channel(32);
    let gate = Arc::new(Semaphore::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let work_sender = sender.clone();
    let work_gate = gate.clone();
    let work_active = active.clone();
    let work_peak = peak.clone();
    builder
        .handle::<Work, _, _>("orders", move |work, context| {
            let sender = work_sender.clone();
            let gate = work_gate.clone();
            let active = work_active.clone();
            let peak = work_peak.clone();
            async move {
                let same = context
                    .envelope()
                    .dispatch()
                    .unwrap()
                    .group()
                    .unwrap()
                    .as_str()
                    == "hot";
                if same {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                }
                sender.send(work.id).await.unwrap();
                gate.acquire().await.unwrap().forget();
                if same {
                    active.fetch_sub(1, Ordering::SeqCst);
                }
                Ok(())
            }
        })
        .unwrap();
    let telemetry_gate = gate.clone();
    builder
        .handle::<Telemetry, _, _>("orders", move |work, _| {
            let sender = sender.clone();
            let gate = telemetry_gate.clone();
            async move {
                sender.send(work.id).await.unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            }
        })
        .unwrap();
    let mut config = config();
    route(&mut config);
    config
        .grouping()
        .rule(GroupingRule::properties(vec!["key".into()]).unwrap())
        .unwrap();
    config
        .grouping()
        .rule(GroupingRule::custom(|_, _| {
            Ok(RuleMatch::Matched(GroupId::new("hot")))
        }))
        .unwrap();
    config
        .grouping()
        .exempt(MessageSelector::family("telemetry").unwrap())
        .unwrap();
    let bus = builder.build_routed(config).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let host_task = tokio::spawn(host.run());
    bus.send(
        &Work {
            id: 1,
            key: Some("hot".into()),
        },
        origin(),
    )
    .await
    .unwrap();
    assert_eq!(next(&mut receiver).await, 1);
    // A dominant hot prefix must not consume active capacity or hide accepted cold work.
    for id in 2..6 {
        bus.publish(
            &Work {
                id,
                key: Some("hot".into()),
            },
            origin(),
        )
        .await
        .unwrap();
    }
    let hot_slot = slots.slot("tenant", Some(&GroupId::new("hot")), Uuid::nil());
    let cold = (0..100)
        .map(|n| format!("cold-{n}"))
        .find(|key| slots.slot("tenant", Some(&GroupId::new(key)), Uuid::nil()) != hot_slot)
        .unwrap();
    bus.send_to(
        "orders",
        &Work {
            id: 6,
            key: Some(cold),
        },
        origin(),
    )
    .await
    .unwrap();
    let telemetry = Envelope::encode(&Telemetry { id: 7 }).unwrap();
    bus.receive(
        &JsonIngress::new(Default::default()),
        &telemetry.to_vec().unwrap(),
        origin(),
        Utc::now(),
        true,
    )
    .await
    .unwrap();
    let mut entered = vec![next(&mut receiver).await, next(&mut receiver).await];
    entered.sort();
    assert_eq!(entered, [6, 7]);
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert!(observed.snapshot().buffered <= 16);
    assert!(observed.snapshot().active <= 3);
    gate.add_permits(32);
    shutdown.shutdown();
    host_task.await.unwrap().unwrap();
    assert_eq!(observed.snapshot().completed, 7);
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(observed.snapshot().buffered, 0);
    assert_eq!(observed.snapshot().active, 0);
}

#[tokio::test]
async fn collision_shared_queue_owner_and_pause_drain_are_real_execution() {
    let slots = SlotSet::new("collision", 1).unwrap();
    let (a, ar) = LocalQueue::partitioned(settings(3, 2), slots.clone()).unwrap();
    let (b, br) = LocalQueue::partitioned(settings(3, 2), slots).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    ar.attach(&mut host, "a").unwrap();
    br.attach(&mut host, "b").unwrap();
    let (entered, mut entries) = mpsc::channel(4);
    let gate = Arc::new(Semaphore::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    for (queue, key, id) in [(&a, "one", 1), (&b, "two", 2)] {
        let envelope = Envelope::encode(&Work { id, key: None })
            .unwrap()
            .with_dispatch(Dispatch::resolved(Some(GroupId::new(key)), None))
            .unwrap();
        let entered = entered.clone();
        let gate = gate.clone();
        let active = active.clone();
        let peak = peak.clone();
        queue
            .submit_envelope(&envelope, "tenant", false, async move {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                entered.send(id).await.unwrap();
                gate.acquire().await.unwrap().forget();
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap();
    }
    next(&mut entries).await;
    a.pause().unwrap();
    b.pause().unwrap();
    shutdown.shutdown();
    gate.add_permits(2);
    task.await.unwrap().unwrap();
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(a.snapshot().completed + b.snapshot().completed, 2);
}

#[tokio::test]
async fn unsupported_profiles_refuse_before_any_buffer_or_transport_admission() {
    let (queue, _runner) =
        LocalQueue::partitioned(settings(8, 2), SlotSet::new("orders", 7).unwrap()).unwrap();
    let observed = queue.clone();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("orders", queue).unwrap();
    builder
        .handle::<Work, _, _>("orders", |_, _| async { Ok(()) })
        .unwrap();
    let (plain, _runner) = LocalQueue::new(settings(8, 2)).unwrap();
    let plain_observed = plain.clone();
    builder.local("plain", plain).unwrap();
    builder
        .handle::<Work, _, _>("plain", |_, _| async { Ok(()) })
        .unwrap();
    builder.route::<Work>("orders").unwrap();
    builder.route::<Work>("plain").unwrap();
    let bus = builder.build();
    assert!(
        bus.send_with_options(
            &Work { id: 1, key: None },
            origin(),
            DispatchOptions {
                group: Some(GroupId::new("g")),
                partition_key: None
            }
        )
        .await
        .is_err()
    );
    assert_eq!(observed.snapshot().buffered, 0);
    assert_eq!(plain_observed.snapshot().buffered, 0);
    let context = MessageContext::new(
        origin(),
        Envelope::encode(&Work { id: 0, key: None }).unwrap(),
        Utc::now(),
    );
    assert!(
        bus.intents(&Work { id: 2, key: None }, &context, true)
            .is_err()
    );
    assert!(
        LocalQueue::partitioned(
            QueueSettings {
                mode: QueueMode::Inline,
                capacity: 1,
                parallelism: 1
            },
            SlotSet::new("invalid", 1).unwrap()
        )
        .is_err()
    );
}
