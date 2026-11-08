//! Real Mosquitto MQTT5 acceptance and independently failed order contracts.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::task::Poll;
use std::time::Duration;

use raddle::transport::Transport;
use raddle::wire::{Envelope, WireMessage};
use raddle_mqtt::{
    Acceptance, Bounds, Error, MqttOptions, PublishOptions, Publisher, QoS, Route, Session, Source,
};
use rumqttc::Outgoing;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{AsyncClient, Event, EventLoop};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order(u32);
impl WireMessage for Order {
    const NAME: &'static str = "test.order";
    const VERSION: u32 = 1;
}
fn source(tenant: &str) -> Source {
    Source {
        client: "fixture".into(),
        tenant: tenant.into(),
        endpoint: "orders".into(),
    }
}
fn port() -> u16 {
    std::env::var("RADDLE_MQTT_PORT")
        .expect("fixture port")
        .parse()
        .unwrap()
}
fn routes(prefix: &str) -> Vec<Route> {
    ["orders", "reply", "dead"]
        .into_iter()
        .map(|alias| Route {
            alias: alias.into(),
            topic: format!("{prefix}/{alias}"),
        })
        .collect()
}
fn options() -> MqttOptions {
    let mut options = MqttOptions::new(
        format!("fixture-{}", uuid::Uuid::new_v4().simple()),
        "127.0.0.1",
        port(),
    );
    options
        .set_clean_start(false)
        .set_session_expiry_interval(Some(60));
    options
}
fn session(prefix: &str, tenant: &str) -> Session {
    Session::new(
        options(),
        source(tenant),
        routes(prefix),
        Bounds {
            timeout: Duration::from_secs(3),
            prefetch: 8,
            ..Bounds::default()
        },
    )
    .unwrap()
}
async fn raw_publish(client: &AsyncClient, eventloop: &mut EventLoop, topic: &str, body: Vec<u8>) {
    raw_publish_qos(client, eventloop, topic, body, QoS::AtLeastOnce).await;
}
async fn raw_publish_qos(
    client: &AsyncClient,
    eventloop: &mut EventLoop,
    topic: &str,
    body: Vec<u8>,
    qos: QoS,
) {
    client.try_publish(topic, qos, false, body).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut packet = None;
    loop {
        match tokio::time::timeout_at(deadline, eventloop.poll())
            .await
            .unwrap()
            .unwrap()
        {
            Event::Outgoing(Outgoing::Publish(id)) => packet = Some(id),
            Event::Incoming(Packet::PubAck(ack))
                if qos == QoS::AtLeastOnce && packet == Some(ack.pkid) =>
            {
                break;
            }
            Event::Incoming(Packet::PubComp(ack))
                if qos == QoS::ExactlyOnce && packet == Some(ack.pkid) =>
            {
                break;
            }
            _ => {}
        }
    }
}

#[tokio::test]
#[ignore = "requires disposable real Mosquitto MQTT5 fixture"]
async fn qos_acknowledgements_retained_clear_wildcards_reply_and_tenants() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let mut publisher = session(&prefix, "tenant-a");
    let mut receiver = session(&prefix, "tenant-a");
    let mut unrelated = session(&format!("{prefix}-other"), "tenant-b");
    assert_ne!(publisher.client_id(), receiver.client_id());
    receiver
        .subscribe(&format!("{prefix}/+"), QoS::ExactlyOnce)
        .await
        .unwrap();
    unrelated
        .subscribe(&format!("{prefix}-other/+"), QoS::AtLeastOnce)
        .await
        .unwrap();
    let envelope = Envelope::encode(&Order(1)).unwrap();
    assert_eq!(
        publisher
            .publish(
                "orders",
                &envelope,
                QoS::AtMostOnce,
                PublishOptions::default()
            )
            .await
            .unwrap(),
        Acceptance::Unconfirmed
    );
    let zero = receiver
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(zero.qos(), QoS::AtMostOnce);
    drop(zero);
    let publish_options = PublishOptions {
        retain: true,
        reply_alias: Some("reply".into()),
        correlation: Some(vec![1, 2, 3]),
        user_properties: vec![("tenant".into(), "untrusted-foreign".into())],
        ..PublishOptions::default()
    };
    assert_eq!(
        publisher
            .publish("orders", &envelope, QoS::AtLeastOnce, publish_options)
            .await
            .unwrap(),
        Acceptance::PubAck
    );
    let mut held = receiver
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(held.decode().unwrap().id(), envelope.id());
    assert_eq!(held.source().tenant, "tenant-a");
    assert_eq!(held.user_properties()[0].0, "tenant");
    assert!(
        receiver
            .respond(&held, "dead", &envelope, QoS::AtLeastOnce)
            .await
            .is_err()
    );
    receiver
        .respond(
            &held,
            "reply",
            &Envelope::encode(&Order(2)).unwrap(),
            QoS::AtLeastOnce,
        )
        .await
        .unwrap();
    receiver.complete(&mut held).await.unwrap();
    drop(held);
    // Drain the self-received reply before checking the later QoS2 delivery.
    let mut reply = receiver
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reply.decode().unwrap().decode::<Order>().unwrap().0, 2);
    receiver.complete(&mut reply).await.unwrap();
    drop(reply);
    let mut retained = session(&prefix, "tenant-a");
    retained
        .subscribe(&format!("{prefix}/orders"), QoS::AtLeastOnce)
        .await
        .unwrap();
    let mut snapshot = retained
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(snapshot.retained());
    retained.complete(&mut snapshot).await.unwrap();
    drop(snapshot);
    publisher
        .clear_retained("orders", QoS::AtLeastOnce)
        .await
        .unwrap();
    // Clearing a retained value is a live empty publish to existing subscribers.
    for client in [&mut receiver, &mut retained] {
        let mut clear = client
            .receive(1, Duration::from_secs(2))
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(clear.decode().is_err());
        client.complete(&mut clear).await.unwrap();
    }
    let mut fresh = session(&prefix, "tenant-a");
    fresh
        .subscribe(&format!("{prefix}/orders"), QoS::AtLeastOnce)
        .await
        .unwrap();
    assert!(
        fresh
            .receive(1, Duration::from_millis(100))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        publisher
            .publish(
                "orders",
                &envelope,
                QoS::ExactlyOnce,
                PublishOptions::default()
            )
            .await
            .unwrap(),
        Acceptance::PubComp
    );
    let mut qos2 = receiver
        .receive(1, Duration::from_secs(2))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(qos2.qos(), QoS::ExactlyOnce);
    receiver.complete(&mut qos2).await.unwrap();
    drop(qos2);
    assert!(
        unrelated
            .receive(1, Duration::from_millis(100))
            .await
            .unwrap()
            .is_empty()
    );
    println!(
        "MQTT5 actual QoS0 write, QoS1 PubAck, QoS2 PubComp, retained clear, wildcard, reply alias and tenant topic isolation"
    );
    for client in [publisher, receiver, retained, fresh, unrelated] {
        client.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires disposable real Mosquitto MQTT5 fixture"]
async fn malformed_first_middle_last_quarantine_then_correct_only_failed() {
    let (peer, mut peer_loop) = AsyncClient::new(options(), 8);
    for failed in 0..3 {
        let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
        let mut receiver = session(&prefix, "trusted");
        receiver
            .subscribe(&format!("{prefix}/orders"), QoS::AtLeastOnce)
            .await
            .unwrap();
        let messages: Vec<_> = (0..3)
            .map(|i| Envelope::encode(&Order(i)).unwrap())
            .collect();
        for (i, message) in messages.iter().enumerate() {
            let mut raw = serde_json::to_value(message).unwrap();
            if i == failed {
                raw["version"] = 0.into();
            }
            raw_publish(
                &peer,
                &mut peer_loop,
                &format!("{prefix}/orders"),
                serde_json::to_vec(&raw).unwrap(),
            )
            .await;
        }
        let mut successful = Vec::new();
        let mut quarantine_topic = None;
        for _ in 0..3 {
            let mut held = receiver
                .receive(1, Duration::from_secs(2))
                .await
                .unwrap()
                .pop()
                .unwrap();
            match held.decode() {
                Ok(message) => {
                    successful.push(message.id());
                    receiver.complete(&mut held).await.unwrap();
                }
                Err(_) => {
                    quarantine_topic = Some(
                        receiver
                            .quarantine(&mut held, "dead", "invalid-envelope")
                            .await
                            .unwrap(),
                    );
                }
            }
        }
        assert_eq!(successful.len(), 2);
        // No quarantine subscriber existed when the broker accepted the snapshot.
        // A later wildcard subscriber must recover the raw poison, not just a notification.
        let topic = quarantine_topic.unwrap();
        assert!(topic.starts_with(&format!("{prefix}/dead/.quarantine/")));
        let mut audit = session(&prefix, "trusted-audit");
        audit
            .subscribe(&format!("{prefix}/dead/#"), QoS::AtLeastOnce)
            .await
            .unwrap();
        let mut snapshot = audit
            .receive(1, Duration::from_secs(2))
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(snapshot.retained());
        assert!(snapshot.decode().is_err());
        let mut expected = serde_json::to_value(&messages[failed]).unwrap();
        expected["version"] = 0.into();
        let decoded =
            raddle_mqtt::QuarantineSnapshot::from_slice(snapshot.raw(), 1_048_576).unwrap();
        assert_eq!(decoded.payload(), serde_json::to_vec(&expected).unwrap());
        audit.complete(&mut snapshot).await.unwrap();
        drop(snapshot);
        audit.close().await.unwrap();
        receiver
            .clear_retained_topic(&topic, QoS::AtLeastOnce)
            .await
            .unwrap();
        let mut fresh = session(&prefix, "trusted-audit");
        fresh
            .subscribe(&format!("{prefix}/dead/#"), QoS::AtLeastOnce)
            .await
            .unwrap();
        assert!(
            fresh
                .receive(1, Duration::from_millis(50))
                .await
                .unwrap()
                .is_empty()
        );
        fresh.close().await.unwrap();
        raw_publish(
            &peer,
            &mut peer_loop,
            &format!("{prefix}/orders"),
            messages[failed].to_vec().unwrap(),
        )
        .await;
        let mut corrected = receiver
            .receive(1, Duration::from_secs(2))
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(corrected.decode().unwrap().id(), messages[failed].id());
        assert!(!successful.contains(&corrected.decode().unwrap().id()));
        receiver.complete(&mut corrected).await.unwrap();
        assert!(
            receiver
                .receive(1, Duration::from_millis(50))
                .await
                .unwrap()
                .is_empty()
        );
        receiver.close().await.unwrap();
    }
    // Peer event loop has no internal worker and drops its socket here.
    drop(peer_loop);
    drop(peer);
}

#[tokio::test]
#[ignore = "requires disposable real Mosquitto MQTT5 fixture"]
async fn cancelled_publication_poison_and_confirmed_transport_refuses_qos0() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let mut client = session(&prefix, "trusted");
    let envelope = Envelope::encode(&Order(1)).unwrap();
    let mut publication = Box::pin(client.publish(
        "orders",
        &envelope,
        QoS::AtLeastOnce,
        PublishOptions::default(),
    ));
    std::future::poll_fn(|cx| {
        assert!(publication.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(publication);
    assert!(client.is_unresolved());
    assert!(matches!(
        client
            .publish(
                "orders",
                &envelope,
                QoS::AtLeastOnce,
                PublishOptions::default()
            )
            .await,
        Err(Error::Unresolved)
    ));
    drop(client);
    assert!(
        Publisher::new(
            session(&prefix, "trusted"),
            QoS::AtMostOnce,
            Duration::from_secs(3)
        )
        .is_err()
    );
    let publisher = Publisher::new(
        session(&prefix, "trusted"),
        QoS::AtLeastOnce,
        Duration::from_secs(3),
    )
    .unwrap();
    publisher.publish("orders", &envelope).await.unwrap();
    assert!(publisher.publish("untrusted-uri", &envelope).await.is_err());
    publisher.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real Mosquitto MQTT5 fixture"]
async fn named_tenant_clients_do_not_treat_envelope_uuid_as_global_protocol_dedup() {
    let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
    let mut audit = session(&prefix, "audit");
    audit
        .subscribe(&format!("{prefix}/orders"), QoS::ExactlyOnce)
        .await
        .unwrap();
    let a = session(&prefix, "tenant-a");
    let b = session(&prefix, "tenant-b");
    assert_ne!(a.client_id(), b.client_id());
    let mut clients = raddle_mqtt::Clients::new(vec![
        ("fixture".into(), "tenant-a".into(), a),
        ("fixture".into(), "tenant-b".into(), b),
    ])
    .unwrap();
    let original = Envelope::encode(&Order(1)).unwrap();
    let other = Envelope::with_identity(original.id(), original.created_at(), &Order(2)).unwrap();
    assert_eq!(original.id(), other.id());
    for (tenant, envelope) in [
        ("tenant-a", &original),
        ("tenant-b", &other),
        ("tenant-a", &original),
    ] {
        assert_eq!(
            clients
                .get_mut("fixture", tenant)
                .unwrap()
                .publish(
                    "orders",
                    envelope,
                    QoS::ExactlyOnce,
                    PublishOptions::default()
                )
                .await
                .unwrap(),
            Acceptance::PubComp
        );
    }
    let mut values = Vec::new();
    for _ in 0..3 {
        let mut held = audit
            .receive(1, Duration::from_secs(2))
            .await
            .unwrap()
            .pop()
            .unwrap();
        let envelope = held.decode().unwrap();
        assert_eq!(envelope.id(), original.id());
        assert_eq!(held.source().tenant, "audit");
        values.push(envelope.decode::<Order>().unwrap().0);
        audit.complete(&mut held).await.unwrap();
    }
    values.sort();
    assert_eq!(values, vec![1, 1, 2]);
    println!(
        "two named tenant client IDs preserve identical UUID work on one MQTT topic; a new QoS2 exchange is not application-envelope retry deduplication"
    );
    assert!(
        clients
            .close()
            .await
            .into_iter()
            .all(|result| result.is_ok())
    );
    audit.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable real Mosquitto MQTT5 fixture"]
async fn empty_poison_retains_recoverable_evidence_before_source_completion() {
    for qos in [QoS::AtLeastOnce, QoS::ExactlyOnce] {
        let prefix = format!("t{}", uuid::Uuid::new_v4().simple());
        let mut receiver = session(&prefix, "trusted");
        receiver
            .subscribe(&format!("{prefix}/orders"), qos)
            .await
            .unwrap();
        let (peer, mut eventloop) = AsyncClient::new(options(), 8);
        raw_publish_qos(
            &peer,
            &mut eventloop,
            &format!("{prefix}/orders"),
            Vec::new(),
            qos,
        )
        .await;
        let mut poison = receiver
            .receive(1, Duration::from_secs(2))
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(poison.raw().is_empty());
        assert!(poison.decode().is_err());
        let retained_topic = receiver
            .quarantine(&mut poison, "dead", "empty-input")
            .await
            .unwrap();
        assert!(matches!(
            receiver.complete(&mut poison).await,
            Err(Error::Lost)
        ));
        // This subscriber did not exist at publication time. A nonempty frame must be
        // retained, not interpreted as MQTT's zero-byte retained deletion operation.
        let mut audit = session(&prefix, "trusted-audit");
        audit
            .subscribe(&format!("{prefix}/dead/#"), QoS::AtLeastOnce)
            .await
            .unwrap();
        let mut snapshot = audit
            .receive(1, Duration::from_secs(2))
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(snapshot.retained());
        assert!(!snapshot.raw().is_empty());
        let original =
            raddle_mqtt::QuarantineSnapshot::from_slice(snapshot.raw(), 1_048_576).unwrap();
        assert!(original.payload().is_empty());
        // Replay exactly the recovered original bytes, not an invented replacement.
        raw_publish_qos(
            &peer,
            &mut eventloop,
            &format!("{prefix}/orders"),
            original.payload().to_vec(),
            qos,
        )
        .await;
        audit.complete(&mut snapshot).await.unwrap();
        let mut replay = receiver
            .receive(1, Duration::from_secs(2))
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(replay.raw().is_empty());
        receiver.complete(&mut replay).await.unwrap();
        receiver
            .clear_retained_topic(&retained_topic, QoS::AtLeastOnce)
            .await
            .unwrap();
        drop(snapshot);
        drop(poison);
        drop(replay);
        receiver.close().await.unwrap();
        audit.close().await.unwrap();
        drop(eventloop);
        drop(peer);
    }
}
