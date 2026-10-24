//! Complete transactional request/reply using a real database and AMQP sockets.
#![allow(clippy::unwrap_used)]

use std::error::Error;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::types::FieldTable;
use lapin::{Connection as AmqpConnection, ConnectionProperties};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_postgres::{
    ConversationStatus, Delivery, DeliveryKind, HandlerFailure, OutboxWorker, ReplyDisposition,
    RequestOptions, RetryPolicy, Store, Worker,
};
use raddle_rabbitmq::{Connection, Receiver, Route};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct Request(u32);
impl WireMessage for Request {
    const NAME: &'static str = "test.request";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Reply(u32);
impl WireMessage for Reply {
    const NAME: &'static str = "test.reply";
    const VERSION: u32 = 1;
}

async fn custody(
    receiver: &mut Receiver,
    store: &Store,
    subscription: &str,
) -> Result<(), Box<dyn Error>> {
    let mut held = receiver
        .receive(Duration::from_secs(5))
        .await?
        .ok_or("missing message")?;
    let envelope = held.decode()?;
    let mut scope = store.begin().await?;
    // The stable route snapshot must survive redelivery: not a new wall-clock due time.
    let delivery = Delivery::new(DeliveryKind::Local, subscription, envelope.created_at());
    scope
        .stage("trusted.rabbitmq", "tenant", &envelope, &[delivery])
        .await?;
    scope.commit(Utc::now()).await?;
    held.complete().await?;
    Ok(())
}

#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires isolated DATABASE_URL and RADDLE_RABBITMQ_TEST_URI"]
async fn durable_conversation_completes_after_real_broker_custody(
    pool: PgPool,
) -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")?;
    let admin = AmqpConnection::connect(&uri, ConnectionProperties::default()).await?;
    let channel = admin.create_channel().await?;
    let requests = format!("raddle-requests-{}", uuid::Uuid::new_v4());
    let replies = format!("raddle-replies-{}", uuid::Uuid::new_v4());
    for queue in [&requests, &replies] {
        channel
            .queue_declare(
                queue.to_owned().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
    }
    let connection = Connection::connect(&uri).await?;
    let result = async {
        let store = Store::new(pool.clone());
        let publisher = connection
            .publisher([
                Route {
                    alias: "requests".into(),
                    exchange: String::new(),
                    routing_key: requests.clone(),
                },
                Route {
                    alias: "replies".into(),
                    exchange: String::new(),
                    routing_key: replies.clone(),
                },
            ])
            .await?;
        let outbox = OutboxWorker::new(store.clone(), publisher, RetryPolicy::default());
        let request = Envelope::encode(&Request(21))?;
        let mut scope = store.begin().await?;
        scope
            .request(
                "tenant",
                &request,
                &RequestOptions {
                    destination: "requests".into(),
                    reply_endpoint: "replies".into(),
                    responder: "trusted.rabbitmq".into(),
                    reply_contract: Reply::NAME.into(),
                    deadline: Utc::now() + TimeDelta::minutes(1),
                },
            )
            .await?;
        scope.commit(Utc::now()).await?;
        assert_eq!(outbox.run_once(4).await?, 1);
        let mut request_receiver = connection.receiver(&requests, 1).await?;
        custody(&mut request_receiver, &store, "requests").await?;
        request_receiver.close().await?;
        let mut worker = Worker::new(store.clone(), RetryPolicy::default());
        worker.handle::<Request, _>("requests", Codecs::default(), |message, scope| {
            Box::pin(async move {
                let original = scope
                    .envelope()
                    .map_err(|_| HandlerFailure::Reject)?
                    .ok_or(HandlerFailure::Reject)?;
                let reply =
                    Envelope::encode(&Reply(message.0 * 2)).map_err(|_| HandlerFailure::Reject)?;
                scope
                    .reply(
                        "tenant",
                        &original,
                        reply,
                        &["replies".parse().unwrap()],
                        Utc::now(),
                    )
                    .await
                    .map_err(|_| HandlerFailure::Retry)
            })
        })?;
        worker.handle::<Reply, _>("replies", Codecs::default(), |_, scope| {
            Box::pin(async move {
                let reply = scope
                    .envelope()
                    .map_err(|_| HandlerFailure::Reject)?
                    .ok_or(HandlerFailure::Reject)?;
                match scope
                    .accept_reply("tenant", "trusted.rabbitmq", &reply, Utc::now())
                    .await
                {
                    Ok(ReplyDisposition::Accepted) => Ok(()),
                    _ => Err(HandlerFailure::Reject),
                }
            })
        })?;
        assert_eq!(worker.run_once(4).await?, 1);
        assert_eq!(outbox.run_once(4).await?, 1);
        let mut reply_receiver = connection.receiver(&replies, 1).await?;
        custody(&mut reply_receiver, &store, "replies").await?;
        reply_receiver.close().await?;
        assert_eq!(worker.run_once(4).await?, 1);
        let result = store
            .conversation("tenant", request.id(), Utc::now())
            .await?
            .ok_or("missing conversation")?;
        assert_eq!(result.status, ConversationStatus::Completed);
        assert_eq!(
            result.reply.ok_or("missing reply")?.decode::<Reply>()?.0,
            42
        );
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    for queue in [requests, replies] {
        channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await?;
    }
    connection.close().await?;
    admin.close(200, "finished".into()).await?;
    result
}
