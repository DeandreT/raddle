//! Opt-in production TCP body framing, replies and explicit version refusal.
#![cfg(feature = "serialization")]
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use prost::Message;
use raddle::wire::{Envelope, WireMessage};
use raddle_serialization::{
    EndpointCodecs, Limits, MessagePack, MessagePackLayout, Protobuf, Registry,
};
use raddle_tcp::{Consumer, Delivery, Endpoint, Peer, Publisher, Receiver};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
struct Word {
    text: String,
}
impl WireMessage for Word {
    const NAME: &'static str = "tcp.codec-word";
    const VERSION: u32 = 1;
}
#[derive(Clone, PartialEq, Message)]
struct ProtoWord {
    #[prost(string, tag = "1")]
    text: String,
}
fn codecs(media: &str) -> Arc<EndpointCodecs> {
    let mut app = Registry::default();
    app.json::<Word>().unwrap();
    app.register::<Word, Word, _, _, _>(MessagePack(MessagePackLayout::Map), Ok, Ok)
        .unwrap();
    app.register::<ProtoWord, Word, _, _, _>(
        Protobuf,
        |m| Ok(Word { text: m.text }),
        |m| Ok(ProtoWord { text: m.text }),
    )
    .unwrap();
    app.set_default(media).unwrap();
    Arc::new(EndpointCodecs::new(Arc::new(app), Limits::default()).unwrap())
}
struct Echo(AtomicUsize);
impl Consumer for Echo {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        self.0.fetch_add(1, Ordering::SeqCst);
        let message = envelope.decode::<Word>().unwrap();
        Delivery::Reply(
            Envelope::encode(&Word {
                text: message.text.chars().rev().collect(),
            })
            .unwrap()
            .correlated(envelope.id(), envelope.id())
            .unwrap(),
        )
    }
}

#[tokio::test]
async fn selected_real_socket_request_reply_decodes_each_direction_and_preserves_correlation() {
    for media in ["application/json", "binary/messagepack", "binary/protobuf"] {
        // Requests and replies deliberately use different defaults; incoming
        // lookup must use the advertised media, not the local sending default.
        let receiving = codecs("binary/protobuf");
        let sending = codecs(media);
        let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
            .await
            .unwrap()
            .with_codecs(receiving);
        let address = receiver.local_addr().unwrap();
        let stop = CancellationToken::new();
        let consumer = Arc::new(Echo(AtomicUsize::new(0)));
        let running = tokio::spawn(receiver.run(Arc::clone(&consumer), stop.clone()));
        let publisher = Publisher::try_new(
            BTreeMap::from([(
                "echo".into(),
                Endpoint::try_new("127.0.0.1", address.port()).unwrap(),
            )]),
            Default::default(),
        )
        .unwrap()
        .with_codecs("echo", sending)
        .unwrap();
        let request = Envelope::encode(&Word {
            text: "雪abc".into(),
        })
        .unwrap();
        let reply = publisher
            .request("echo", &request, Word::NAME)
            .await
            .unwrap();
        assert_eq!(reply.decode::<Word>().unwrap().text, "cba雪");
        assert_eq!(reply.correlation_id(), Some(request.id()));
        assert_eq!(reply.causation_id(), Some(request.id()));
        assert_eq!(consumer.0.load(Ordering::SeqCst), 1);
        stop.cancel();
        running.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn unconfigured_receiver_refuses_selected_protocol_without_invoking_consumer_or_falling_back()
{
    let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
        .await
        .unwrap();
    let address = receiver.local_addr().unwrap();
    let stop = CancellationToken::new();
    let consumer = Arc::new(Echo(AtomicUsize::new(0)));
    let running = tokio::spawn(receiver.run(Arc::clone(&consumer), stop.clone()));
    let publisher = Publisher::try_new(
        BTreeMap::from([(
            "echo".into(),
            Endpoint::try_new("127.0.0.1", address.port()).unwrap(),
        )]),
        Default::default(),
    )
    .unwrap()
    .with_codecs("echo", codecs("binary/messagepack"))
    .unwrap();
    let result = publisher
        .publish_batch(
            "echo",
            &[Envelope::encode(&Word { text: "x".into() }).unwrap()],
        )
        .await
        .unwrap();
    assert!(result[0].is_err());
    assert_eq!(consumer.0.load(Ordering::SeqCst), 0);
    stop.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn oversized_declared_selected_frame_closes_connection_without_allocating_or_custody() {
    let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
        .await
        .unwrap()
        .with_codecs(codecs("binary/protobuf"));
    let address = receiver.local_addr().unwrap();
    let stop = CancellationToken::new();
    let consumer = Arc::new(Echo(AtomicUsize::new(0)));
    let running = tokio::spawn(receiver.run(Arc::clone(&consumer), stop.clone()));
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(b"RDTC\x02").await.unwrap();
    socket.write_u16(1).await.unwrap();
    socket.write_u32(u32::MAX).await.unwrap();
    assert!(socket.read_u8().await.is_err());
    assert_eq!(consumer.0.load(Ordering::SeqCst), 0);
    stop.cancel();
    running.await.unwrap().unwrap();
}
