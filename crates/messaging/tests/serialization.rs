//! Actual production TCP codec selection, typed bus handlers and SQL custody.
#![cfg(feature = "serialization-ingress")]
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use prost::Message;
use raddle::RuntimeBuilder;
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_messaging::effects::HandlerOutput;
use raddle_messaging::scoped::{LocalOnlyFlush, ScopedHandler};
use raddle_messaging::{
    BoxFuture, BusBuilder, DurableEndpoint, LocalQueue, MessageBus, MessageContext, QueueMode,
    QueueSettings, TrustedOrigin,
};
use raddle_postgres::{RetryPolicy, Scope, Store};
use raddle_serialization::{
    BodyCodec, EndpointCodecs, Error, Json, Limits, MessagePack, MessagePackLayout, Protobuf,
    Registry,
};
use raddle_tcp::{Consumer, Delivery, Endpoint, Peer, Publisher, Receipt, Receiver};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Input {
    value: u32,
    label: String,
}
impl WireMessage for Input {
    const NAME: &'static str = "serialization.input";
    const VERSION: u32 = 1;
}

#[derive(Clone, PartialEq, Message)]
struct ProtoInput {
    #[prost(uint32, tag = "1")]
    value: u32,
    #[prost(string, tag = "2")]
    label: String,
}
#[derive(Serialize, Deserialize)]
struct ForeignJson {
    number: u32,
    renamed: String,
}

struct TextCodec;
impl BodyCodec<String> for TextCodec {
    fn content_type(&self) -> &'static str {
        "text/plain"
    }
    fn encode(&self, model: &String, _: Limits) -> Result<Vec<u8>, Error> {
        Ok(model.as_bytes().to_vec())
    }
    fn decode(&self, bytes: &[u8], _: Limits) -> Result<String, Error> {
        String::from_utf8(bytes.to_vec()).map_err(|_| Error::Malformed)
    }
}
fn codecs(media: &str, foreign_json: bool) -> Arc<EndpointCodecs> {
    let mut registry = Registry::default();
    registry.json::<Input>().unwrap();
    registry
        .register::<Input, Input, _, _, _>(MessagePack(MessagePackLayout::Positional), Ok, Ok)
        .unwrap();
    registry
        .register::<ProtoInput, Input, _, _, _>(
            Protobuf,
            |m| {
                Ok(Input {
                    value: m.value,
                    label: m.label,
                })
            },
            |m| {
                Ok(ProtoInput {
                    value: m.value,
                    label: m.label,
                })
            },
        )
        .unwrap();
    registry
        .register::<String, Input, _, _, _>(
            TextCodec,
            |text| {
                let (value, label) = text.split_once(':').ok_or(Error::Malformed)?;
                Ok(Input {
                    value: value.parse().map_err(|_| Error::Malformed)?,
                    label: label.into(),
                })
            },
            |m| Ok(format!("{}:{}", m.value, m.label)),
        )
        .unwrap();
    registry.set_default("binary/messagepack").unwrap();
    let mut endpoint = EndpointCodecs::new(Arc::new(registry), Limits::default()).unwrap();
    if foreign_json {
        endpoint
            .local_mut()
            .register::<ForeignJson, Input, _, _, _>(
                Json,
                |m| {
                    Ok(Input {
                        value: m.number,
                        label: m.renamed,
                    })
                },
                |m| {
                    Ok(ForeignJson {
                        number: m.value,
                        renamed: m.label,
                    })
                },
            )
            .unwrap();
    }
    endpoint.set_default(media).unwrap();
    Arc::new(endpoint)
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("configured-tenant", "configured-socket", None).unwrap()
}
struct BusConsumer(MessageBus);
impl Consumer for BusConsumer {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        match self
            .0
            .dispatch(MessageContext::new(origin(), envelope, Utc::now()), true)
            .await
        {
            Ok(()) => Delivery::Accepted,
            Err(_) => Delivery::Retry,
        }
    }
}
fn publisher(address: SocketAddr, codecs: Arc<EndpointCodecs>) -> Publisher {
    Publisher::try_new(
        BTreeMap::from([(
            "orders".into(),
            Endpoint::try_new("127.0.0.1", address.port()).unwrap(),
        )]),
        Default::default(),
    )
    .unwrap()
    .with_codecs("orders", codecs)
    .unwrap()
}
async fn malformed_batch(address: SocketAddr, frames: &[Vec<u8>], bad: usize) -> Vec<u8> {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(b"RDTC\x02").await.unwrap();
    socket.write_u16(frames.len() as u16).await.unwrap();
    let mut results = Vec::new();
    for (index, frame) in frames.iter().enumerate() {
        let mut frame = frame.clone();
        if index == bad {
            let len = u32::from_be_bytes(frame[5..9].try_into().unwrap()) as usize;
            frame.truncate(9 + len);
            frame.push(0xff);
        }
        socket.write_u32(frame.len() as u32).await.unwrap();
        socket.write_all(&frame).await.unwrap();
        results.push(socket.read_u8().await.unwrap());
    }
    results
}

#[tokio::test]
async fn shipping_socket_selected_decoders_reach_typed_message_bus_handler_with_failure_isolation()
{
    for (media, foreign) in [
        ("application/json", true),
        ("text/plain", false),
        ("binary/messagepack", false),
        ("binary/protobuf", false),
    ] {
        let codecs = codecs(media, foreign);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut bus = BusBuilder::new(RuntimeBuilder::default().build());
        let (queue, _runner) = LocalQueue::new(QueueSettings {
            mode: QueueMode::Inline,
            capacity: 4,
            parallelism: 1,
        })
        .unwrap();
        bus.local("typed", queue).unwrap();
        bus.route::<Input>("typed").unwrap();
        let handled = Arc::clone(&seen);
        bus.handle::<Input, _, _>("typed", move |model, context| {
            assert_eq!(context.origin().tenant(), "configured-tenant");
            assert_eq!(context.origin().source(), "configured-socket");
            assert_eq!(model.label, "雪");
            handled.lock().unwrap().push(model.value);
            async { Ok(()) }
        })
        .unwrap();
        let bus = bus.build();
        // The optional pre-custody messaging hook uses the same production codec.
        let direct = Envelope::encode(&Input {
            value: 0,
            label: "雪".into(),
        })
        .unwrap();
        bus.receive(
            codecs.as_ref(),
            &codecs.encode(&direct).unwrap(),
            origin(),
            Utc::now(),
            true,
        )
        .await
        .unwrap();
        seen.lock().unwrap().clear();
        let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
            .await
            .unwrap()
            .with_codecs(Arc::clone(&codecs));
        let address = receiver.local_addr().unwrap();
        let stop = CancellationToken::new();
        let running = tokio::spawn(receiver.run(Arc::new(BusConsumer(bus)), stop.clone()));
        let sender = publisher(address, Arc::clone(&codecs));
        for bad in 0..3 {
            let messages: Vec<_> = (1..=3)
                .map(|value| {
                    Envelope::encode(&Input {
                        value,
                        label: "雪".into(),
                    })
                    .unwrap()
                    .with_header("tenant".into(), "untrusted-tenant".into())
                    .unwrap()
                })
                .collect();
            let frames: Vec<_> = messages.iter().map(|e| codecs.encode(e).unwrap()).collect();
            let results = malformed_batch(address, &frames, bad).await;
            assert_eq!(
                results,
                (0..3)
                    .map(|i| if i == bad { 2 } else { 0 })
                    .collect::<Vec<_>>()
            );
            let accepted = seen.lock().unwrap().clone();
            assert_eq!(
                accepted,
                (1..=3).filter(|v| *v != bad as u32 + 1).collect::<Vec<_>>()
            );
            let retry = sender
                .publish_batch("orders", std::slice::from_ref(&messages[bad]))
                .await
                .unwrap();
            assert!(matches!(retry.as_slice(), [Ok(Receipt::Accepted)]));
            let mut counts = seen.lock().unwrap().clone();
            counts.sort();
            assert_eq!(counts, [1, 2, 3]);
            seen.lock().unwrap().clear();
        }
        stop.cancel();
        running.await.unwrap().unwrap();
    }
}

struct SqlHandler;
impl ScopedHandler<Input> for SqlHandler {
    type Resources = ();
    type Reply = ();
    type Error = Infallible;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Infallible>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        message: &'a Input,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<(), ()>, Infallible>> {
        Box::pin(async move {
            // Primary key makes a repeated business invocation observable.
            assert_eq!(context.origin().tenant(), "configured-tenant");
            assert_eq!(context.origin().source(), "configured-socket");
            sqlx::query("INSERT INTO serialization_business(id,value) VALUES($1,$2)")
                .bind(context.envelope().id())
                .bind(message.value as i32)
                .execute(scope.connection())
                .await
                .unwrap();
            Ok(HandlerOutput::reply(()))
        })
    }
}

#[derive(Serialize, Deserialize)]
struct CurrentInput {
    value: u32,
    label: String,
    normalized: bool,
}
impl WireMessage for CurrentInput {
    const NAME: &'static str = Input::NAME;
    const VERSION: u32 = 2;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn selected_canonicalizer_finishes_upcasting_before_socket_custody_identity_comparison(
    pool: PgPool,
) {
    let mut upcasters = Codecs::default();
    upcasters
        .upcast(Input::NAME, 1, |mut value| {
            value["normalized"] = true.into();
            Ok(value)
        })
        .unwrap();
    let receiving = Arc::try_unwrap(codecs("binary/messagepack", false))
        .ok()
        .unwrap()
        .with_canonicalizer(move |envelope| {
            let current = upcasters
                .decode::<CurrentInput>(&envelope)
                .map_err(|_| Error::Projection)?;
            envelope
                .rebody(&current, Limits::default().canonical)
                .map_err(|_| Error::Projection)
        });
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder
        .durable("current", Store::new(pool.clone()))
        .unwrap();
    builder.route::<CurrentInput>("current").unwrap();
    let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
        .await
        .unwrap()
        .with_codecs(Arc::new(receiving));
    let address = receiver.local_addr().unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(receiver.run(Arc::new(BusConsumer(builder.build())), stop.clone()));
    let message = Envelope::encode(&Input {
        value: 9,
        label: "雪".into(),
    })
    .unwrap();
    for media in ["binary/protobuf", "application/json"] {
        let sender = publisher(address, codecs(media, false));
        assert!(matches!(
            sender
                .publish_batch("orders", std::slice::from_ref(&message))
                .await
                .unwrap()
                .as_slice(),
            [Ok(Receipt::Accepted)]
        ));
    }
    let rows: Vec<Vec<u8>> =
        sqlx::query_scalar("SELECT envelope FROM raddle_jobs WHERE message_id=$1")
            .bind(message.id())
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    let stored = Envelope::from_slice(&rows[0], Default::default()).unwrap();
    assert_eq!(stored.version(), 2);
    assert_eq!(stored.id(), message.id());
    assert_eq!(stored.created_at(), message.created_at());
    assert!(stored.decode::<CurrentInput>().unwrap().normalized);
    stop.cancel();
    running.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn selected_socket_custody_is_canonical_before_sql_and_corrected_retry_runs_only_failed_work(
    pool: PgPool,
) {
    sqlx::query("CREATE TABLE serialization_business(id uuid PRIMARY KEY,value integer NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    for media in [
        "application/json",
        "text/plain",
        "binary/messagepack",
        "binary/protobuf",
    ] {
        let codecs = codecs(media, false);
        let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
        builder.durable("canonical", store.clone()).unwrap();
        builder.route::<Input>("canonical").unwrap();
        let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
            .await
            .unwrap()
            .with_codecs(Arc::clone(&codecs));
        let address = receiver.local_addr().unwrap();
        let stop = CancellationToken::new();
        let running =
            tokio::spawn(receiver.run(Arc::new(BusConsumer(builder.build())), stop.clone()));
        let sender = publisher(address, Arc::clone(&codecs));
        let mut worker =
            DurableEndpoint::new(store.clone(), "canonical", 3, RetryPolicy::default()).unwrap();
        worker
            .handle::<Input, _>(Arc::new(SqlHandler), Arc::new(LocalOnlyFlush))
            .unwrap();
        for bad in 0..3 {
            let messages: Vec<_> = (1..=3)
                .map(|value| {
                    Envelope::encode(&Input {
                        value,
                        label: "雪".into(),
                    })
                    .unwrap()
                    .with_header("tenant".into(), "untrusted-tenant".into())
                    .unwrap()
                })
                .collect();
            let frames: Vec<_> = messages.iter().map(|e| codecs.encode(e).unwrap()).collect();
            assert_eq!(
                malformed_batch(address, &frames, bad).await,
                (0..3)
                    .map(|i| if i == bad { 2 } else { 0 })
                    .collect::<Vec<_>>()
            );
            for (i, message) in messages.iter().enumerate() {
                let row: Option<(String, String, Vec<u8>)> = sqlx::query_as(
                    "SELECT source,tenant,envelope FROM raddle_jobs WHERE message_id=$1",
                )
                .bind(message.id())
                .fetch_optional(&pool)
                .await
                .unwrap();
                if i == bad {
                    assert!(row.is_none());
                } else {
                    let (source, tenant, bytes) = row.unwrap();
                    assert_eq!(source, "configured-socket");
                    assert_eq!(tenant, "configured-tenant");
                    let stored = Envelope::from_slice(&bytes, Default::default()).unwrap();
                    assert_eq!(
                        stored.content_bytes().unwrap(),
                        message.content_bytes().unwrap()
                    );
                }
            }
            assert!(matches!(
                sender
                    .publish_batch("orders", std::slice::from_ref(&messages[bad]))
                    .await
                    .unwrap()
                    .as_slice(),
                [Ok(Receipt::Accepted)]
            ));
            assert_eq!(worker.run_once().await.unwrap(), 3);
            let ids: Vec<Uuid> = messages.iter().map(Envelope::id).collect();
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM serialization_business WHERE id=ANY($1)"
                )
                .bind(&ids)
                .fetch_one(&pool)
                .await
                .unwrap(),
                3
            );
            // An explicit duplicate through a different body encoding still
            // deduplicates canonical custody; no second business invocation.
            let alternative = publisher(
                address,
                self::codecs(
                    if media == "binary/protobuf" {
                        "application/json"
                    } else {
                        "binary/protobuf"
                    },
                    false,
                ),
            );
            assert!(matches!(
                alternative
                    .publish_batch("orders", std::slice::from_ref(&messages[bad]))
                    .await
                    .unwrap()
                    .as_slice(),
                [Ok(Receipt::Accepted)]
            ));
            assert_eq!(worker.run_once().await.unwrap(), 0);
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM raddle_jobs WHERE message_id=ANY($1)"
                )
                .bind(&ids)
                .fetch_one(&pool)
                .await
                .unwrap(),
                3
            );
        }
        stop.cancel();
        running.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn production_publisher_endpoint_override_changes_observed_socket_bytes_and_legacy_sibling_stays_json()
 {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let (queue, _runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Inline,
        capacity: 4,
        parallelism: 1,
    })
    .unwrap();
    builder.local("typed", queue).unwrap();
    builder.route::<Input>("typed").unwrap();
    let handled = Arc::clone(&seen);
    builder
        .handle::<Input, _, _>("typed", move |m, _| {
            handled.lock().unwrap().push(m.value);
            async { Ok(()) }
        })
        .unwrap();
    let receive = codecs("binary/messagepack", false);
    let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Default::default())
        .await
        .unwrap()
        .with_codecs(Arc::clone(&receive));
    let address = receiver.local_addr().unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(receiver.run(Arc::new(BusConsumer(builder.build())), stop.clone()));
    // Transparent byte recorder forwards every request to the real production
    // receiver/bus. It is not a fake peer supplying custody by itself.
    let proxy = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    let recording = tokio::spawn(async move {
        let mut captured = Vec::new();
        for _ in 0..3 {
            let (mut sender, _) = proxy.accept().await.unwrap();
            let mut receiver = TcpStream::connect(address).await.unwrap();
            let mut magic = [0; 5];
            sender.read_exact(&mut magic).await.unwrap();
            receiver.write_all(&magic).await.unwrap();
            let count = sender.read_u16().await.unwrap();
            receiver.write_u16(count).await.unwrap();
            assert_eq!(count, 1);
            let length = sender.read_u32().await.unwrap();
            assert!(length < 1_048_576);
            let mut frame = vec![0; length as usize];
            sender.read_exact(&mut frame).await.unwrap();
            receiver.write_u32(length).await.unwrap();
            receiver.write_all(&frame).await.unwrap();
            let status = receiver.read_u8().await.unwrap();
            assert_eq!(status, 0);
            sender.write_u8(status).await.unwrap();
            captured.push((magic, frame));
        }
        captured
    });
    let endpoint = Endpoint::try_new("127.0.0.1", proxy_addr.port()).unwrap();
    let sender = Publisher::try_new(
        BTreeMap::from([
            ("app".into(), endpoint.clone()),
            ("local".into(), endpoint.clone()),
            ("legacy".into(), endpoint),
        ]),
        Default::default(),
    )
    .unwrap()
    .with_codecs("app", receive)
    .unwrap()
    .with_codecs("local", codecs("text/plain", false))
    .unwrap();
    let message = Envelope::encode(&Input {
        value: 7,
        label: "雪".into(),
    })
    .unwrap();
    for alias in ["app", "local", "legacy"] {
        assert!(matches!(
            sender
                .publish_batch(alias, std::slice::from_ref(&message))
                .await
                .unwrap()
                .as_slice(),
            [Ok(Receipt::Accepted)]
        ));
    }
    let captured = recording.await.unwrap();
    for (i, media) in [(0, "binary/messagepack"), (1, "text/plain")] {
        assert_eq!(&captured[i].0, b"RDTC\x02");
        let bytes = &captured[i].1;
        let header_len = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let header: serde_json::Value = serde_json::from_slice(&bytes[9..9 + header_len]).unwrap();
        assert_eq!(header["content_type"], media);
        assert_eq!(
            &bytes[9 + header_len..],
            if i == 0 {
                &b"\x92\x07\xa3\xe9\x9b\xaa"[..]
            } else {
                "7:雪".as_bytes()
            }
        );
    }
    assert_eq!(&captured[2].0, b"RDTC\x01");
    assert_eq!(captured[2].1, message.to_vec().unwrap());
    assert_eq!(*seen.lock().unwrap(), [7, 7, 7]);
    stop.cancel();
    running.await.unwrap().unwrap();
}
