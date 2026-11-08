//! Distinct push and Core queue-group protocol contracts against a real fixture.
#![allow(clippy::unwrap_used)]

use raddle::wire::{Envelope, WireMessage};
use raddle_nats::{
    Bounds, Core, JetStream, Provision, PublishOptions, PushConfig, Route, Source, StreamConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Order(u32);
impl WireMessage for Order {
    const NAME: &'static str = "test.order";
    const VERSION: u32 = 1;
}

#[tokio::test]
#[ignore = "requires disposable real NATS fixture"]
async fn push_flow_is_a_distinct_non_lease_custody_api() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{prefix}.orders");
    let core = Core::connect(
        &std::env::var("RADDLE_NATS_URL").unwrap(),
        raddle_nats::ConnectOptions::new(),
        vec![Route {
            alias: "orders".into(),
            subject: subject.clone(),
        }],
        Bounds::default(),
    )
    .await
    .unwrap();
    let jet = JetStream::new(core.clone(), None).unwrap();
    jet.provision_stream(
        StreamConfig {
            name: prefix.clone(),
            subjects: vec![format!("{prefix}.>")],
            ..StreamConfig::default()
        },
        Provision::Create,
    )
    .await
    .unwrap();
    let source = Source {
        client: "fixture".into(),
        tenant: "trusted".into(),
        endpoint: "push".into(),
    };
    let mut receiver = jet
        .push_receiver(
            &prefix,
            PushConfig {
                durable_name: Some("push".into()),
                deliver_subject: format!("_INBOX.raddle{prefix}"),
                filter_subject: subject,
                ack_wait: Duration::from_secs(5),
                max_ack_pending: 8,
                flow_control: true,
                idle_heartbeat: Duration::from_millis(250),
                ..PushConfig::default()
            },
            Provision::Create,
            source,
        )
        .await
        .unwrap();
    let envelope = Envelope::encode(&Order(1)).unwrap();
    jet.send("orders", &envelope, PublishOptions::default())
        .await
        .unwrap();
    let mut message = receiver
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(message.decode().unwrap().id(), envelope.id());
    message.complete().await.unwrap();
    assert!(message.complete().await.is_err());
    println!(
        "flow-controlled push source explicitly acknowledges custody without inventing lease admission"
    );
    drop(receiver);
    drop(jet);
    core.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real NATS fixture"]
async fn core_queue_groups_share_one_delivery_not_a_durable_copy() {
    let subject = format!("t{}.orders", uuid::Uuid::new_v4().simple());
    let core = Core::connect(
        &std::env::var("RADDLE_NATS_URL").unwrap(),
        raddle_nats::ConnectOptions::new(),
        vec![Route {
            alias: "orders".into(),
            subject: subject.clone(),
        }],
        Bounds::default(),
    )
    .await
    .unwrap();
    let source = Source {
        client: "fixture".into(),
        tenant: "trusted".into(),
        endpoint: "queue".into(),
    };
    let mut a = core
        .subscribe(&subject, Some("workers"), source.clone())
        .await
        .unwrap();
    let mut b = core
        .subscribe(&subject, Some("workers"), source)
        .await
        .unwrap();
    for i in 0..16 {
        core.send("orders", &Envelope::encode(&Order(i)).unwrap())
            .await
            .unwrap();
    }
    let mut counts = [0, 0];
    let mut ids = std::collections::BTreeSet::new();
    for _ in 0..16 {
        for (i, receiver) in [&mut a, &mut b].into_iter().enumerate() {
            if let Some(message) = receiver.receive(Duration::from_millis(20)).await.unwrap() {
                counts[i] += 1;
                assert!(ids.insert(message.decode().unwrap().id()));
            }
        }
    }
    assert_eq!(counts.iter().sum::<usize>(), 16);
    assert!(counts.into_iter().all(|count| count > 0));
    drop(a);
    drop(b);
    core.close().await.unwrap();
}
