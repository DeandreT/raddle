//! Native property spelling, trusted tenant isolation and execution-time budgets.
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::grouping::{Dispatch, GroupId};
use raddle::routing::EndpointAlias;
use raddle::sequencing::SlotSet;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::routing::{
    GroupingRule, MessageDeclaration, MessageRole, MessageSelector, RoutingConfig,
};
use raddle_messaging::{
    BusBuilder, LocalQueue, MessageContext, QueueMode, QueueSettings, TrustedOrigin,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct Payload(Value);
impl WireMessage for Payload {
    const NAME: &'static str = "group.payload";
    const VERSION: u32 = 1;
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("trusted-tenant", "source", None).unwrap()
}
async fn next<T>(rx: &mut mpsc::Receiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
fn config() -> RoutingConfig {
    let mut config = RoutingConfig::default();
    config
        .declare(MessageDeclaration::of::<Payload>(MessageRole::Handled).unwrap())
        .unwrap();
    config
        .rule(
            "route",
            "Route",
            MessageSelector::exact::<Payload>().unwrap(),
            vec![EndpointAlias::try_new("orders").unwrap()],
        )
        .unwrap();
    config
}

#[tokio::test]
async fn first_present_property_all_native_values_and_untrusted_tenant_header_execute() {
    let (queue, runner) = LocalQueue::partitioned(
        QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 16,
            parallelism: 4,
        },
        SlotSet::new("orders", 7).unwrap(),
    )
    .unwrap();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("orders", queue).unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    builder
        .handle::<Payload, _, _>("orders", move |_, context| {
            let tx = tx.clone();
            async move {
                tx.send(
                    context
                        .envelope()
                        .dispatch()
                        .unwrap()
                        .group()
                        .unwrap()
                        .as_str()
                        .to_owned(),
                )
                .await
                .unwrap();
                Ok(())
            }
        })
        .unwrap();
    let mut config = config();
    config
        .grouping()
        .rule(GroupingRule::properties(vec!["primary".into(), "secondary".into()]).unwrap())
        .unwrap();
    config.grouping().rule(GroupingRule::tenant()).unwrap();
    let bus = builder.build_routed(config).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    for (payload, expected) in [
        (json!({"primary":null,"secondary":"later"}), ""),
        (json!({"primary":"","secondary":"later"}), ""),
        (json!({"secondary":"fallback"}), "fallback"),
        (json!({}), "trusted-tenant"),
        (json!({"primary":17}), "17"),
        (json!({"primary":true}), "true"),
        (json!({"primary":[1,null]}), "[1,null]"),
        (json!({"primary":{"z":1,"a":2}}), "{\"a\":2,\"z\":1}"),
    ] {
        let envelope = Envelope::encode(&Payload(payload))
            .unwrap()
            .with_header("tenant".into(), "spoofed".into())
            .unwrap();
        bus.dispatch(MessageContext::new(origin(), envelope, Utc::now()), true)
            .await
            .unwrap();
        assert_eq!(next(&mut rx).await, expected);
    }
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn waiting_for_a_busy_slot_does_not_consume_the_buffered_handler_deadline() {
    let (queue, runner) = LocalQueue::partitioned(
        QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 4,
            parallelism: 2,
        },
        SlotSet::new("orders", 1).unwrap(),
    )
    .unwrap();
    let observed = queue.clone();
    let gate = Arc::new(Semaphore::new(0));
    let held = gate.clone();
    let (tx, mut rx) = mpsc::channel(4);
    let first_tx = tx.clone();
    let envelope = Envelope::encode(&Payload(Value::Null))
        .unwrap()
        .with_dispatch(Dispatch::resolved(Some(GroupId::new("g")), None))
        .unwrap();
    queue
        .submit_envelope(&envelope, "trusted-tenant", false, async move {
            first_tx.send(0).await.unwrap();
            held.acquire().await.unwrap().forget();
            Ok(())
        })
        .await
        .unwrap();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.timeout(Duration::from_millis(100)).unwrap();
    builder.local("orders", queue).unwrap();
    builder
        .handle::<Payload, _, _>("orders", move |_, _| {
            let tx = tx.clone();
            async move {
                tx.send(1).await.unwrap();
                Ok(())
            }
        })
        .unwrap();
    let bus = builder.build_routed(config()).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    assert_eq!(next(&mut rx).await, 0);
    bus.send(&Payload(Value::Null), origin()).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(observed.snapshot().buffered, 1);
    gate.add_permits(1);
    assert_eq!(next(&mut rx).await, 1);
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(observed.snapshot().completed, 2);
    assert_eq!(observed.snapshot().failed, 0);
}

#[tokio::test(start_paused = true)]
async fn timed_out_handler_releases_its_slot_without_cancelling_healthy_neighbor() {
    let (queue, runner) = LocalQueue::partitioned(
        QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 4,
            parallelism: 2,
        },
        SlotSet::new("orders", 1).unwrap(),
    )
    .unwrap();
    let observed = queue.clone();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.timeout(Duration::from_millis(100)).unwrap();
    builder.local("orders", queue).unwrap();
    let (tx, mut rx) = mpsc::channel(4);
    builder
        .handle::<Payload, _, _>("orders", move |payload, _| {
            let tx = tx.clone();
            async move {
                let id = payload.0.as_u64().unwrap();
                tx.send(id).await.unwrap();
                if id == 0 {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        })
        .unwrap();
    let mut config = config();
    config.grouping().rule(GroupingRule::tenant()).unwrap();
    let bus = builder.build_routed(config).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    bus.send(&Payload(json!(0)), origin()).await.unwrap();
    assert_eq!(next(&mut rx).await, 0);
    bus.send(&Payload(json!(1)), origin()).await.unwrap();
    tokio::time::advance(Duration::from_millis(200)).await;
    assert_eq!(next(&mut rx).await, 1);
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(observed.snapshot().failed, 1);
    assert_eq!(observed.snapshot().completed, 1);
    assert_eq!(observed.snapshot().cancelled, 0);
    assert_eq!(observed.snapshot().active, 0);
}

#[cfg(feature = "crypto-ingress")]
#[tokio::test]
async fn real_encrypted_frame_is_refused_before_group_callbacks_then_canonical_ingress_groups() {
    use raddle_crypto::{
        EncryptionCodec, EncryptionPolicy, KeyId, RotatingKeys, SecretKey, Selection,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    let keys = RotatingKeys::new(1).unwrap();
    keys.insert(
        KeyId::new("test").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    let codec = EncryptionCodec::new(
        keys,
        EncryptionPolicy {
            send: Selection::All,
            receive_required: Selection::All,
        },
        Default::default(),
    );
    let protected = codec
        .encode(&Envelope::encode(&Payload(json!({"primary":"group"}))).unwrap())
        .await
        .unwrap();
    let (queue, runner) = LocalQueue::partitioned(
        QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 4,
            parallelism: 2,
        },
        SlotSet::new("orders", 7).unwrap(),
    )
    .unwrap();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("orders", queue).unwrap();
    let (tx, mut rx) = mpsc::channel(4);
    builder
        .handle::<Payload, _, _>("orders", move |_, context| {
            let tx = tx.clone();
            async move {
                tx.send(
                    context
                        .envelope()
                        .dispatch()
                        .unwrap()
                        .group()
                        .unwrap()
                        .as_str()
                        .to_owned(),
                )
                .await
                .unwrap();
                Ok(())
            }
        })
        .unwrap();
    let mut config = config();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    config
        .grouping()
        .rule(GroupingRule::custom(move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(raddle_messaging::routing::RuleMatch::NoMatch)
        }))
        .unwrap();
    let observed = calls.clone();
    config
        .grouping()
        .rule(GroupingRule::typed::<Payload>(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            raddle_messaging::routing::RuleMatch::NoMatch
        }))
        .unwrap();
    config
        .grouping()
        .rule(GroupingRule::properties(vec!["primary".into()]).unwrap())
        .unwrap();
    let bus = builder.build_routed(config).unwrap();
    assert!(
        bus.receive(
            &raddle_messaging::JsonIngress::new(Default::default()),
            &protected.to_vec().unwrap(),
            origin(),
            Utc::now(),
            true
        )
        .await
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    bus.receive(
        &codec,
        &protected.to_vec().unwrap(),
        origin(),
        Utc::now(),
        true,
    )
    .await
    .unwrap();
    assert_eq!(next(&mut rx).await, "group");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
