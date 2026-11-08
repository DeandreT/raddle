//! Refuse unsupported selected-body/crypto composition; retain legacy required
//! JSON-inner encryption and pre-custody decryption without policy changes.
#![cfg(all(feature = "serialization-ingress", feature = "crypto-ingress"))]
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::Utc;
use prost::Message;
use raddle::RuntimeBuilder;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_crypto::{
    EncryptedTransport, EncryptionCodec, EncryptionPolicy, KeyId, RotatingKeys, SecretKey,
    Selection,
};
use raddle_messaging::{BusBuilder, IngressError, MessageBus, MessageContext, TrustedOrigin};
use raddle_postgres::Store;
use raddle_serialization::{
    BodyCodec, EndpointCodecs, Error, Limits, MessagePack, MessagePackLayout, Protobuf, Registry,
};
use raddle_tcp::{Consumer, Delivery, Endpoint, Peer, Publisher, Receiver};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct Open(Value);
impl WireMessage for Open {
    const NAME: &'static str = "serialization.protection";
    const VERSION: u32 = 1;
}
#[derive(Clone, PartialEq, Message)]
struct ProtoText {
    #[prost(string, tag = "1")]
    text: String,
}

fn selected(media: &str) -> Arc<EndpointCodecs> {
    let mut registry = Registry::default();
    registry.json::<Open>().unwrap();
    registry
        .register::<Open, Open, _, _, _>(MessagePack(MessagePackLayout::Map), Ok, Ok)
        .unwrap();
    registry
        .register::<ProtoText, Open, _, _, _>(
            Protobuf,
            |m| Ok(Open(json!({"text":m.text}))),
            |m| {
                Ok(ProtoText {
                    text: m
                        .0
                        .get("text")
                        .and_then(Value::as_str)
                        .ok_or(Error::Projection)?
                        .to_owned(),
                })
            },
        )
        .unwrap();
    registry.set_default(media).unwrap();
    Arc::new(EndpointCodecs::new(Arc::new(registry), Limits::default()).unwrap())
}
fn keys() -> RotatingKeys {
    let keys = RotatingKeys::new(1).unwrap();
    keys.insert(
        KeyId::new("serialization-boundary").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    keys
}
fn crypto(keys: &RotatingKeys) -> EncryptionCodec<RotatingKeys> {
    EncryptionCodec::new(
        keys.clone(),
        EncryptionPolicy {
            send: Selection::All,
            receive_required: Selection::All,
        },
        Default::default(),
    )
}
fn publisher(address: SocketAddr) -> Publisher {
    Publisher::try_new(
        BTreeMap::from([(
            "inbox".into(),
            Endpoint::try_new("127.0.0.1", address.port()).unwrap(),
        )]),
        Default::default(),
    )
    .unwrap()
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("configured-tenant", "crypto-boundary", None).unwrap()
}

#[tokio::test]
async fn actual_required_crypto_frames_are_refused_before_selected_publication_for_every_default() {
    let keys = keys();
    let plain = Envelope::encode(&Open(json!({"text":"plain 雪"}))).unwrap();
    let encrypted = crypto(&keys).encode(&plain).await.unwrap();
    assert_eq!(
        serde_json::to_value(&encrypted).unwrap()["payload"]["inner_content_type"],
        "application/json"
    );
    for media in ["application/json", "binary/messagepack", "binary/protobuf"] {
        let profile = selected(media);
        assert_eq!(
            profile.encode(&encrypted),
            Err(Error::UnsupportedProtection)
        );
        assert!(profile.encode(&plain).is_ok());
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let transport = EncryptedTransport::new(
            publisher(listener.local_addr().unwrap())
                .with_codecs("inbox", profile)
                .unwrap(),
            crypto(&keys),
        );
        assert_eq!(
            transport.publish("inbox", &plain).await,
            Err(TransportError::Rejected)
        );
        assert!(
            matches!(listener.into_std().unwrap().accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock)
        );
    }
    assert_eq!(
        crypto(&keys)
            .decode(&encrypted)
            .await
            .unwrap()
            .content_bytes()
            .unwrap(),
        plain.content_bytes().unwrap()
    );
}

struct Admission {
    bus: MessageBus,
    crypto: Option<EncryptionCodec<RotatingKeys>>,
    reached: AtomicUsize,
}
impl Consumer for Admission {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        self.reached.fetch_add(1, Ordering::SeqCst);
        let accepted = if let Some(crypto) = &self.crypto {
            let bytes = envelope.to_vec().unwrap();
            self.bus
                .receive(crypto, &bytes, origin(), Utc::now(), true)
                .await
                .is_ok()
        } else {
            self.bus
                .dispatch(MessageContext::new(origin(), envelope, Utc::now()), true)
                .await
                .is_ok()
        };
        if accepted {
            Delivery::Accepted
        } else {
            Delivery::Rejected
        }
    }
}
async fn raw(address: SocketAddr, magic: &[u8; 5], frame: &[u8]) -> u8 {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(magic).await.unwrap();
    socket.write_u16(1).await.unwrap();
    socket.write_u32(frame.len() as u32).await.unwrap();
    socket.write_all(frame).await.unwrap();
    socket.read_u8().await.unwrap()
}
fn packet(encrypted: &Envelope, media: &str, body: &[u8]) -> Vec<u8> {
    let header =
        serde_json::to_vec(&json!({"metadata":encrypted.metadata(),"content_type":media})).unwrap();
    let mut packet = b"RDSB\x01".to_vec();
    packet.extend_from_slice(&(header.len() as u32).to_be_bytes());
    packet.extend_from_slice(&header);
    packet.extend_from_slice(body);
    packet
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cipher_frames_never_reach_custody_even_with_value_models_and_legacy_json_inner_stays_required(
    pool: PgPool,
) {
    let keys = keys();
    let plain = Envelope::encode(&Open(json!({"text":"plain 雪"}))).unwrap();
    let encrypted = crypto(&keys).encode(&plain).await.unwrap();
    let payload = serde_json::to_value(&encrypted).unwrap()["payload"].clone();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.durable("inbox", Store::new(pool.clone())).unwrap();
    builder.route::<Open>("inbox").unwrap();
    let bus = builder.build();
    let profile = selected("binary/messagepack");
    let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
        .await
        .unwrap()
        .with_codecs(Arc::clone(&profile));
    let address = receiver.local_addr().unwrap();
    let stop = CancellationToken::new();
    let admission = Arc::new(Admission {
        bus: bus.clone(),
        crypto: None,
        reached: AtomicUsize::new(0),
    });
    let running = tokio::spawn(receiver.run(Arc::clone(&admission), stop.clone()));
    let json_body = serde_json::to_vec(&payload).unwrap();
    let binary_body = MessagePack(MessagePackLayout::Map)
        .encode(&Open(payload), Limits::default())
        .unwrap();
    for (media, body) in [
        ("application/json", json_body),
        ("binary/messagepack", binary_body),
    ] {
        let frame = packet(&encrypted, media, &body);
        assert!(matches!(
            bus.receive(profile.as_ref(), &frame, origin(), Utc::now(), true)
                .await,
            Err(IngressError::Decode(Error::UnsupportedProtection))
        ));
        assert_eq!(raw(address, b"RDTC\x02", &frame).await, 2);
    }
    // Legacy compatibility must not bypass an explicitly selected plaintext-only
    // profile. Default receivers without selection keep their original behavior.
    assert_eq!(
        raw(address, b"RDTC\x01", &encrypted.to_vec().unwrap()).await,
        2
    );
    assert_eq!(admission.reached.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    publisher(address)
        .with_codecs("inbox", profile)
        .unwrap()
        .publish("inbox", &plain)
        .await
        .unwrap();
    assert_eq!(admission.reached.load(Ordering::SeqCst), 1);
    stop.cancel();
    running.await.unwrap().unwrap();

    let legacy = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
        .await
        .unwrap();
    let address = legacy.local_addr().unwrap();
    let stop = CancellationToken::new();
    let legacy_admission = Arc::new(Admission {
        bus,
        crypto: Some(crypto(&keys)),
        reached: AtomicUsize::new(0),
    });
    let running = tokio::spawn(legacy.run(legacy_admission, stop.clone()));
    let original = Envelope::encode(&Open(json!({"text":"required JSON inner"}))).unwrap();
    assert_eq!(
        publisher(address).publish("inbox", &original).await,
        Err(TransportError::Rejected)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE message_id=$1")
            .bind(original.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    EncryptedTransport::new(publisher(address), crypto(&keys))
        .publish("inbox", &original)
        .await
        .unwrap();
    let bytes: Vec<u8> = sqlx::query_scalar("SELECT envelope FROM raddle_jobs WHERE message_id=$1")
        .bind(original.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        Envelope::from_slice(&bytes, Default::default())
            .unwrap()
            .content_bytes()
            .unwrap(),
        original.content_bytes().unwrap()
    );
    stop.cancel();
    running.await.unwrap().unwrap();
}
