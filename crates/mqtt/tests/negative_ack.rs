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
