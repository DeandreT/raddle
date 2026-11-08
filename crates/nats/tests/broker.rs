//! Real Core/JetStream broker contracts against an explicitly configured fixture.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use raddle::transport::Transport;
use raddle::wire::{Envelope, WireMessage};
use raddle_nats::{
    Bounds, ConsumerConfig, Core, Error, JetStream, Provision, PublishOptions, RetentionPolicy,
    Route, ScheduleResult, Source, StreamConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct Order(u32);
impl WireMessage for Order {
    const NAME: &'static str = "test.order";
    const VERSION: u32 = 1;
}
fn source() -> Source {
    Source {
        client: "fixture".into(),
        tenant: "trusted".into(),
        endpoint: "orders".into(),
    }
}
fn bounds() -> Bounds {
    Bounds {
        operation_timeout: Duration::from_secs(3),
        prefetch: 8,
        ..Bounds::default()
    }
}
async fn core(routes: Vec<Route>) -> Core {
    let url = std::env::var("RADDLE_NATS_URL").expect("fixture URL");
    Core::connect(&url, raddle_nats::ConnectOptions::new(), routes, bounds())
        .await
        .unwrap()
}
fn route(alias: &str, subject: &str) -> Route {
    Route {
        alias: alias.into(),
        subject: subject.into(),
    }
}
fn consumer(name: &str, filter: &str) -> ConsumerConfig {
    ConsumerConfig {
        durable_name: Some(name.into()),
        filter_subject: filter.into(),
        ack_wait: Duration::from_secs(5),
        max_ack_pending: 8,
        ..ConsumerConfig::default()
    }
}

#[tokio::test]
#[ignore = "requires disposable real NATS fixture"]
async fn core_protocol_does_not_claim_durable_acceptance() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{prefix}.orders");
    let bus = core(vec![
        route("orders", &subject),
        route("empty", &format!("{prefix}.none.absent")),
    ])
    .await;
    let mut receiver = bus
        .subscribe(&format!("{prefix}.*"), Some("workers"), source())
        .await
        .unwrap();
    let message = Envelope::encode(&Order(1)).unwrap();
    bus.send("orders", &message).await.unwrap();
    let received = receiver
        .receive(Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.decode().unwrap().id(), message.id());
    assert_eq!(received.source().tenant, "trusted");
    assert!(matches!(
        bus.request("empty", &message).await,
        Err(Error::NoResponders)
    ));
    // A responder sees the original inbox and can only respond to an actual NATS inbox.
    let request = bus.request("orders", &message);
    let response = async {
        let request = receiver
            .receive(Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        request
            .respond(&Envelope::encode(&Order(2)).unwrap())
            .await
            .unwrap();
    };
    let (result, ()) = tokio::join!(request, response);
    assert_eq!(result.unwrap().decode::<Order>().unwrap().0, 2);
    drop(receiver);
    bus.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real NATS JetStream fixture"]
async fn jetstream_puback_redelivery_dlq_lease_and_resume() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{prefix}.orders");
    let dlq = format!("{prefix}.dead");
    let core = core(vec![
        route("orders", &subject),
        route("dead", &dlq),
        route("missing", "unretained.none"),
    ])
    .await;
    let jet = JetStream::new(core.clone(), None)
        .unwrap()
        .with_dedup_scope(source())
        .unwrap();
    let config = StreamConfig {
        name: prefix.clone(),
        subjects: vec![format!("{prefix}.>")],
        retention: RetentionPolicy::Limits,
        num_replicas: 1,
        ..StreamConfig::default()
    };
    jet.provision_stream(config.clone(), Provision::Create)
        .await
        .unwrap();
    jet.provision_stream(config.clone(), Provision::Verify)
        .await
        .unwrap();
    let mut drift = config.clone();
    drift.retention = RetentionPolicy::Interest;
    assert!(matches!(
        jet.provision_stream(drift, Provision::Verify).await,
        Err(Error::Drift)
    ));
    let mut receiver = jet
        .receiver(
            &prefix,
            consumer("orders", &subject),
            Provision::Create,
            source(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    let mut dead = jet
        .receiver(
            &prefix,
            consumer("dead", &dlq),
            Provision::Create,
            source(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    let envelope = Envelope::encode(&Order(3)).unwrap();
    let accepted = jet
        .send("orders", &envelope, PublishOptions::default())
        .await
        .unwrap();
    let duplicate = jet
        .send("orders", &envelope, PublishOptions::default())
        .await
        .unwrap();
    assert_eq!(accepted.sequence, duplicate.sequence);
    assert!(duplicate.duplicate);
    let mut held = receiver
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let position = held.position().sequence;
    held.progress().await.unwrap();
    held.retry(Some(Duration::from_millis(10))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut redelivery = receiver
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(redelivery.position().sequence, position);
    assert_eq!(redelivery.decode().unwrap().id(), envelope.id());
    assert!(redelivery.position().attempt > 1);
    assert!(redelivery.dead_letter("unknown").await.is_err());
    redelivery.dead_letter("dead").await.unwrap();
    let mut copy = dead
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(copy.decode().unwrap().id(), envelope.id());
    copy.complete().await.unwrap();
    drop(receiver);
    let mut resumed = jet
        .receiver(
            &prefix,
            consumer("orders", &subject),
            Provision::Verify,
            source(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert!(
        resumed
            .receive(1, Duration::from_millis(100))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(jet.publish("unknown", &envelope).await.is_err());
    assert!(jet.publish("missing", &envelope).await.is_err());
    println!(
        "JetStream accepted PubAck, duplicate, progress, NAK redelivery, confirmed DLQ then TERM, durable resume"
    );
    drop(resumed);
    drop(dead);
    drop(jet);
    core.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real NATS JetStream fixture"]
async fn invalid_first_middle_last_isolate_and_correct_only_failed() {
    let url = std::env::var("RADDLE_NATS_URL").unwrap();
    let peer = async_nats::connect(&url).await.unwrap();
    let native = async_nats::jetstream::new(peer.clone());
    println!(
        "NATS server {} fixture {}",
        peer.server_info().version,
        std::env::var("RADDLE_FIXTURE_IMAGE").unwrap_or_default()
    );
    for failed in 0..3 {
        let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
        let subject = format!("{prefix}.orders");
        let bus = core(vec![
            route("orders", &subject),
            route("dead", &format!("{prefix}.dead")),
        ])
        .await;
        let jet = JetStream::new(bus.clone(), None).unwrap();
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
        let mut receiver = jet
            .receiver(
                &prefix,
                consumer("orders", &subject),
                Provision::Create,
                source(),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        let messages: Vec<_> = (0..3)
            .map(|i| Envelope::encode(&Order(i)).unwrap())
            .collect();
        for (i, message) in messages.iter().enumerate() {
            if i == failed {
                let mut broken = serde_json::to_value(message).unwrap();
                broken["version"] = 0.into();
                native
                    .publish(subject.clone(), serde_json::to_vec(&broken).unwrap().into())
                    .await
                    .unwrap()
                    .await
                    .unwrap();
            } else {
                jet.send("orders", message, PublishOptions::default())
                    .await
                    .unwrap();
            }
        }
        let received = receiver.receive(3, Duration::from_secs(2)).await.unwrap();
        assert_eq!(received.len(), 3);
        let mut successful = Vec::new();
        for mut held in received {
            match held.decode() {
                Ok(envelope) => {
                    successful.push(envelope.id());
                    held.complete().await.unwrap();
                }
                Err(_) => {
                    held.dead_letter("dead").await.unwrap();
                }
            }
        }
        assert_eq!(successful.len(), 2);
        jet.send("orders", &messages[failed], PublishOptions::default())
            .await
            .unwrap();
        let mut corrected = receiver
            .receive(1, Duration::from_secs(2))
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(corrected.decode().unwrap().id(), messages[failed].id());
        assert!(!successful.contains(&corrected.decode().unwrap().id()));
        corrected.complete().await.unwrap();
        assert!(
            receiver
                .receive(1, Duration::from_millis(50))
                .await
                .unwrap()
                .is_empty()
        );
        drop(receiver);
        drop(jet);
        bus.close().await.unwrap();
    }
    peer.drain().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real NATS JetStream fixture"]
async fn schedules_are_native_or_explicitly_require_durable_fallback() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{prefix}.orders");
    let bus = core(vec![route("orders", &subject)]).await;
    let jet = JetStream::new(bus.clone(), None).unwrap();
    let mut config = StreamConfig {
        name: prefix.clone(),
        subjects: vec![format!("{prefix}.>")],
        num_replicas: 1,
        ..StreamConfig::default()
    };
    jet.provision_stream(config.clone(), Provision::Create)
        .await
        .unwrap();
    let envelope = Envelope::encode(&Order(99)).unwrap();
    let at = chrono::Utc::now() + chrono::Duration::seconds(2);
    assert_eq!(
        jet.schedule(&prefix, "orders", &envelope, at)
            .await
            .unwrap(),
        ScheduleResult::RequiresDurableScheduler
    );
    config.allow_message_schedules = true;
    jet.provision_stream(config, Provision::Update)
        .await
        .unwrap();
    let mut receiver = jet
        .receiver(
            &prefix,
            consumer("orders", &subject),
            Provision::Create,
            source(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    let result = jet
        .schedule(
            &prefix,
            "orders",
            &envelope,
            chrono::Utc::now() + chrono::Duration::seconds(2),
        )
        .await
        .unwrap();
    match result {
        ScheduleResult::Native(receipt) => {
            assert_eq!(receipt.stream, prefix);
            let mut held = receiver
                .receive(1, Duration::from_secs(5))
                .await
                .unwrap()
                .pop()
                .expect("scheduled delivery");
            assert_eq!(held.decode().unwrap().id(), envelope.id());
            held.complete().await.unwrap();
            println!("native schedule delivered with original identity");
        }
        ScheduleResult::RequiresDurableScheduler => {
            panic!("enabled limits stream on this fixture must schedule natively")
        }
        _ => panic!("unknown scheduling decision"),
    }
    drop(receiver);
    drop(jet);
    bus.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real NATS JetStream fixture"]
async fn native_schedules_scope_equal_uuids_and_unscoped_controls_never_replace() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{prefix}.orders");
    let bus = core(vec![route("orders", &subject)]).await;
    let jet = JetStream::new(bus.clone(), None).unwrap();
    jet.provision_stream(
        StreamConfig {
            name: prefix.clone(),
            subjects: vec![format!("{prefix}.>")],
            allow_message_schedules: true,
            ..StreamConfig::default()
        },
        Provision::Create,
    )
    .await
    .unwrap();
    let configured = |tenant: &str| Source {
        tenant: tenant.into(),
        ..source()
    };
    let a = jet
        .clone()
        .with_dedup_scope(configured("tenant-a"))
        .unwrap();
    let b = jet
        .clone()
        .with_dedup_scope(configured("tenant-b"))
        .unwrap();
    let id = uuid::Uuid::new_v4();
    let created = chrono::Utc::now();
    let original_a = Envelope::with_identity(id, created, &Order(1)).unwrap();
    let original_b = Envelope::with_identity(id, created, &Order(2)).unwrap();
    let mut receiver_config = consumer("audit", &subject);
    receiver_config.ack_wait = Duration::from_secs(10);
    let mut receiver = jet
        .receiver(
            &prefix,
            receiver_config,
            Provision::Create,
            source(),
            Duration::from_secs(20),
        )
        .await
        .unwrap();
    let at = chrono::Utc::now() + chrono::Duration::seconds(3);
    for (client, envelope, duplicate) in [
        (&a, &original_a, false),
        (&b, &original_b, false),
        (&a, &original_a, true),
        (&b, &original_b, true),
        (&jet, &original_a, false),
        (&jet, &original_b, false),
    ] {
        let ScheduleResult::Native(receipt) = client
            .schedule(&prefix, "orders", envelope, at)
            .await
            .unwrap()
        else {
            panic!("native schedule required")
        };
        assert_eq!(receipt.duplicate, duplicate);
    }
    let mut values = Vec::new();
    for _ in 0..4 {
        let mut held = receiver
            .receive(1, Duration::from_secs(6))
            .await
            .unwrap()
            .pop()
            .expect("scheduled publication");
        let envelope = held.decode().unwrap();
        assert_eq!(envelope.id(), id);
        values.push(envelope.decode::<Order>().unwrap().0);
        held.complete().await.unwrap();
    }
    values.sort();
    assert_eq!(values, vec![1, 1, 2, 2]);
    assert!(
        receiver
            .receive(1, Duration::from_millis(100))
            .await
            .unwrap()
            .is_empty()
    );
    println!(
        "equal UUID schedule controls survive across trusted tenants; scoped retries deduplicate and unscoped nonces never replace controls"
    );
    drop(receiver);
    drop(a);
    drop(b);
    drop(jet);
    bus.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real NATS JetStream fixture"]
async fn deleted_consumer_and_expired_queued_receipts_cannot_admit_or_ack_work() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{prefix}.orders");
    let bus = core(vec![route("orders", &subject)]).await;
    let jet = JetStream::new(bus.clone(), None).unwrap();
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
    let mut lost = jet
        .receiver(
            &prefix,
            consumer("lost", &subject),
            Provision::Create,
            source(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    let envelope = Envelope::encode(&Order(1)).unwrap();
    jet.send("orders", &envelope, PublishOptions::default())
        .await
        .unwrap();
    let mut held = lost
        .receive(1, Duration::from_secs(1))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let peer = async_nats::connect(std::env::var("RADDLE_NATS_URL").unwrap())
        .await
        .unwrap();
    let native = async_nats::jetstream::new(peer.clone());
    native
        .get_stream(&prefix)
        .await
        .unwrap()
        .delete_consumer("lost")
        .await
        .unwrap();
    assert!(held.progress().await.is_err());
    assert!(!held.is_owned());
    assert!(held.decode().is_err());
    assert!(held.complete().await.is_err());
    // A fresh independent consumer still processes the retained order normally.
    let mut short = consumer("short", &subject);
    short.ack_wait = Duration::from_secs(1);
    let mut receiver = jet
        .receiver(
            &prefix,
            short,
            Provision::Create,
            source(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    let mut queued = receiver
        .receive(1, Duration::from_millis(200))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let sequence = queued.position().sequence;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(!queued.is_owned());
    assert!(queued.decode().is_err());
    assert!(queued.complete().await.is_err());
    let mut redelivery = receiver
        .receive(1, Duration::from_millis(200))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(redelivery.position().sequence, sequence);
    assert!(redelivery.position().attempt > 1);
    assert_eq!(redelivery.decode().unwrap().id(), envelope.id());
    redelivery.complete().await.unwrap();
    println!(
        "deleted-consumer progress cannot fabricate ownership; expired queued copy refuses admission/ack and only broker redelivery proceeds"
    );
    drop(receiver);
    drop(lost);
    drop(jet);
    peer.drain().await.unwrap();
    bus.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real NATS JetStream fixture"]
async fn equal_uuids_in_trusted_tenant_namespaces_do_not_suppress_other_work() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{prefix}.orders");
    let bus = core(vec![route("orders", &subject)]).await;
    let unscoped = JetStream::new(bus.clone(), None).unwrap();
    unscoped
        .provision_stream(
            StreamConfig {
                name: prefix.clone(),
                subjects: vec![format!("{prefix}.>")],
                ..StreamConfig::default()
            },
            Provision::Create,
        )
        .await
        .unwrap();
    let configured = |tenant: &str| Source {
        client: "fixture".into(),
        tenant: tenant.into(),
        endpoint: "orders".into(),
    };
    let a = unscoped
        .clone()
        .with_dedup_scope(configured("tenant-a"))
        .unwrap();
    let b = unscoped
        .clone()
        .with_dedup_scope(configured("tenant-b"))
        .unwrap();
    let clients = raddle_nats::Clients::new(
        vec![],
        vec![
            ("fixture".into(), "tenant-a".into(), a),
            ("fixture".into(), "tenant-b".into(), b),
        ],
    )
    .unwrap();
    let a = clients.jetstream("fixture", "tenant-a").unwrap();
    let b = clients.jetstream("fixture", "tenant-b").unwrap();
    assert!(clients.jetstream("fixture", "unconfigured").is_err());
    let id = uuid::Uuid::new_v4();
    let created = chrono::Utc::now();
    // Payload/header claims deliberately name the wrong tenant. Only configuration
    // supplies the namespace, and both publishers use the SAME physical subject/stream.
    let original_a = Envelope::with_identity(id, created, &Order(1))
        .unwrap()
        .with_header("tenant-id".into(), "tenant-b".into())
        .unwrap();
    let original_b = Envelope::with_identity(id, created, &Order(2))
        .unwrap()
        .with_header("tenant-id".into(), "tenant-a".into())
        .unwrap();
    let accepted_a = a
        .send("orders", &original_a, PublishOptions::default())
        .await
        .unwrap();
    let accepted_b = b
        .send("orders", &original_b, PublishOptions::default())
        .await
        .unwrap();
    assert!(!accepted_a.duplicate && !accepted_b.duplicate);
    assert_ne!(accepted_a.sequence, accepted_b.sequence);
    let retry_a = a
        .send("orders", &original_a, PublishOptions::default())
        .await
        .unwrap();
    let retry_b = b
        .send("orders", &original_b, PublishOptions::default())
        .await
        .unwrap();
    assert!(retry_a.duplicate && retry_b.duplicate);
    assert_eq!(retry_a.sequence, accepted_a.sequence);
    assert_eq!(retry_b.sequence, accepted_b.sequence);
    let mut options = PublishOptions {
        deduplication_id: Some("ignored-override".into()),
        ..PublishOptions::default()
    };
    options
        .headers
        .insert("Nats-Msg-Id", "same-explicit-logical-key");
    let header_a = a
        .send("orders", &original_a, options.clone())
        .await
        .unwrap();
    let header_b = b
        .send("orders", &original_b, options.clone())
        .await
        .unwrap();
    assert!(!header_a.duplicate && !header_b.duplicate);
    assert_ne!(header_a.sequence, header_b.sequence);
    options.deduplication_id = Some("changed-but-header-still-wins".into());
    assert!(
        a.send("orders", &original_a, options.clone())
            .await
            .unwrap()
            .duplicate
    );
    assert!(
        b.send("orders", &original_b, options.clone())
            .await
            .unwrap()
            .duplicate
    );
    assert!(unscoped.send("orders", &original_a, options).await.is_err());
    let first = unscoped
        .send("orders", &original_a, PublishOptions::default())
        .await
        .unwrap();
    let second = unscoped
        .send("orders", &original_a, PublishOptions::default())
        .await
        .unwrap();
    assert!(!first.duplicate && !second.duplicate);
    assert_ne!(first.sequence, second.sequence);
    let mut audit = unscoped
        .receiver(
            &prefix,
            consumer("audit", &subject),
            Provision::Create,
            source(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    let received = audit.receive(6, Duration::from_secs(2)).await.unwrap();
    assert_eq!(received.len(), 6);
    let mut values = Vec::new();
    for mut held in received {
        let envelope = held.decode().unwrap();
        assert_eq!(envelope.id(), id);
        assert_eq!(held.source().tenant, "trusted");
        values.push(envelope.decode::<Order>().unwrap().0);
        held.complete().await.unwrap();
    }
    values.sort();
    assert_eq!(values, vec![1, 1, 1, 1, 2, 2]);
    assert!(
        audit
            .receive(1, Duration::from_millis(50))
            .await
            .unwrap()
            .is_empty()
    );
    println!(
        "same physical stream/subject: equal original UUIDs survive tenant-a and tenant-b; scoped retries and explicit logical keys deduplicate only their own namespace; unscoped sends never infer tenancy"
    );
    drop(audit);
    drop(clients);
    drop(unscoped);
    bus.close().await.unwrap();
}
