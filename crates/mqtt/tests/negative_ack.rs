//! Deterministic negative reason-code protocol tests supplement real broker tests.
#![allow(clippy::unwrap_used)]

use raddle::wire::{Envelope, WireMessage};
use raddle_mqtt::{
    Acceptance, Bounds, Error, MqttOptions, PublishOptions, QoS, Route, Session, Source,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Serialize, Deserialize)]
struct Order(u32);
impl WireMessage for Order {
    const NAME: &'static str = "test.order";
    const VERSION: u32 = 1;
}

async fn packet(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let header = socket.read_u8().await.unwrap();
    let mut remaining = 0usize;
    let mut multiplier = 1;
    for _ in 0..4 {
        let byte = socket.read_u8().await.unwrap();
        remaining += usize::from(byte & 127) * multiplier;
        if byte & 128 == 0 {
            break;
        }
        multiplier *= 128;
    }
    assert!(remaining <= 1_048_576);
    let mut body = vec![0; remaining];
    socket.read_exact(&mut body).await.unwrap();
    (header, body)
}

// This deliberately controlled protocol peer supplements, not replaces, Mosquitto
// qualification. It deterministically emits MQTT5 negative reason codes without
// relying on a broker's ACL policy or logging any authentication material.
#[tokio::test]
async fn negative_puback_first_middle_last_does_not_confirm_or_block_other_orders() {
    for failed in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (header, _) = packet(&mut socket).await;
            assert_eq!(header >> 4, 1);
            socket.write_all(&[0x20, 0x03, 0, 0, 0]).await.unwrap();
            let mut ids = Vec::new();
            for i in 0..3 {
                let (header, body) = packet(&mut socket).await;
                assert_eq!(header, 0x32);
                let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
                let offset = 2 + length;
                let pkid = u16::from_be_bytes([body[offset], body[offset + 1]]);
                assert_ne!(pkid, 0);
                ids.push(pkid);
                let reason = if i == failed { 0x87 } else { 0 };
                socket
                    .write_all(&[0x40, 0x04, (pkid >> 8) as u8, pkid as u8, reason, 0])
                    .await
                    .unwrap();
            }
            let _ = socket.read_u8().await;
            ids
        });
        let mut session = Session::new(
            MqttOptions::new(format!("negative-{failed}"), "127.0.0.1", port),
            Source {
                client: "peer".into(),
                tenant: "trusted".into(),
                endpoint: "orders".into(),
            },
            vec![Route {
                alias: "orders".into(),
                topic: "orders".into(),
            }],
            Bounds {
                timeout: Duration::from_secs(3),
                ..Bounds::default()
            },
        )
        .unwrap();
        for i in 0..3 {
            let envelope = Envelope::encode(&Order(i)).unwrap();
            let result = session
                .publish(
                    "orders",
                    &envelope,
                    QoS::AtLeastOnce,
                    PublishOptions::default(),
                )
                .await;
            if i == failed {
                assert!(matches!(result, Err(Error::Refused)));
            } else {
                assert_eq!(result.unwrap(), Acceptance::PubAck);
            }
            assert!(!session.is_unresolved());
        }
        drop(session);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), peer)
                .await
                .unwrap()
                .unwrap()
                .len(),
            3
        );
    }
}

async fn accept_subscription(listener: &TcpListener) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    assert_eq!(packet(&mut socket).await.0 >> 4, 1);
    // A fresh native broker session, explicitly without session continuity.
    socket.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
    let (header, subscribe) = packet(&mut socket).await;
    assert_eq!(header, 0x82);
    socket
        .write_all(&[0x90, 4, subscribe[0], subscribe[1], 0, 1])
        .await
        .unwrap();
    socket
}

async fn incoming_packet_one(socket: &mut TcpStream, envelope: &Envelope) {
    let mut body = vec![0, 6];
    body.extend_from_slice(b"orders");
    body.extend_from_slice(&[0, 1, 0]); // packet ID 1; no MQTT5 properties
    body.extend_from_slice(&envelope.to_vec().unwrap());
    let mut bytes = vec![0x32];
    let mut remaining = body.len();
    loop {
        let mut digit = (remaining % 128) as u8;
        remaining /= 128;
        if remaining != 0 {
            digit |= 128;
        }
        bytes.push(digit);
        if remaining == 0 {
            break;
        }
    }
    bytes.extend_from_slice(&body);
    socket.write_all(&bytes).await.unwrap();
}

#[tokio::test]
async fn receive_disconnect_fails_closed_and_old_receipt_cannot_ack_reused_packet_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (held_ready, held_ready_rx) = tokio::sync::oneshot::channel();
    let (old_attempted, old_attempted_rx) = tokio::sync::oneshot::channel();
    let (no_stale_ack, no_stale_ack_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut old = accept_subscription(&listener).await;
        incoming_packet_one(&mut old, &Envelope::encode(&Order(1)).unwrap()).await;
        held_ready_rx.await.unwrap();
        old.shutdown().await.unwrap();
        drop(old);
        let mut fresh = accept_subscription(&listener).await;
        incoming_packet_one(&mut fresh, &Envelope::encode(&Order(2)).unwrap()).await;
        old_attempted_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), packet(&mut fresh))
                .await
                .is_err(),
            "old receipt acknowledged a new unprocessed packet"
        );
        no_stale_ack.send(()).unwrap();
        let (header, body) = packet(&mut fresh).await;
        assert_eq!(header, 0x40);
        assert_eq!(&body[..2], &[0, 1]);
    });
    let new_session = || {
        Session::new(
            MqttOptions::new("reused-packet-id", "127.0.0.1", port),
            Source {
                client: "peer".into(),
                tenant: "trusted".into(),
                endpoint: "orders".into(),
            },
            vec![Route {
                alias: "orders".into(),
                topic: "orders".into(),
            }],
            Bounds {
                timeout: Duration::from_secs(3),
                ..Bounds::default()
            },
        )
        .unwrap()
    };
    let mut original = new_session();
    original
        .subscribe("orders", QoS::AtLeastOnce)
        .await
        .unwrap();
    let mut old = original
        .receive(1, Duration::from_secs(3))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(old.decode().unwrap().decode::<Order>().unwrap().0, 1);
    held_ready.send(()).unwrap();
    assert!(matches!(
        original.receive(1, Duration::from_secs(3)).await,
        Err(Error::Uncertain)
    ));
    assert!(original.is_unresolved());
    assert!(matches!(
        original.complete(&mut old).await,
        Err(Error::Unresolved)
    ));
    assert!(matches!(
        original.receive(1, Duration::from_secs(1)).await,
        Err(Error::Unresolved)
    ));
    let mut replacement = new_session();
    replacement
        .subscribe("orders", QoS::AtLeastOnce)
        .await
        .unwrap();
    let mut fresh = replacement
        .receive(1, Duration::from_secs(3))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(fresh.decode().unwrap().decode::<Order>().unwrap().0, 2);
    assert!(matches!(
        replacement.complete(&mut old).await,
        Err(Error::Lost)
    ));
    old_attempted.send(()).unwrap();
    no_stale_ack_rx.await.unwrap();
    replacement.complete(&mut fresh).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
    drop(fresh);
    drop(old);
    drop(replacement);
    drop(original);
}
