//! Runs only against an explicitly selected disposable local RabbitMQ instance.
#![allow(clippy::unwrap_used)]

use std::error::Error;
use std::time::Duration;

use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::types::FieldTable;
use lapin::{Connection as AmqpConnection, ConnectionProperties};
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_rabbitmq::{Connection, Requeue, Route};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Probe(u32);
impl WireMessage for Probe {
    const NAME: &'static str = "test.rabbitmq";
    const VERSION: u32 = 1;
}

#[tokio::test]
#[ignore = "requires RADDLE_RABBITMQ_TEST_URI for an isolated loopback broker"]
async fn confirms_returns_prefetch_and_explicit_settlement() -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")?;
    let connection = Connection::connect(&uri).await?;
    let admin = AmqpConnection::connect(&uri, ConnectionProperties::default()).await?;
    let channel = admin.create_channel().await?;
    let queue = format!("raddle-test-{}", uuid::Uuid::new_v4());
    channel
        .queue_declare(
            queue.clone().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    let result = async {
        let publisher = connection
            .publisher([
                Route {
                    alias: "queue".into(),
                    exchange: String::new(),
                    routing_key: queue.clone(),
                },
                Route {
                    alias: "missing".into(),
                    exchange: String::new(),
                    routing_key: format!("{queue}-missing"),
                },
            ])
            .await?;
        let first = Envelope::encode(&Probe(1))?;
        let second = Envelope::encode(&Probe(2))?;
        assert_eq!(
            publisher.publish("unknown", &first).await,
            Err(TransportError::Rejected)
        );
        assert_eq!(
            publisher.publish("missing", &first).await,
            Err(TransportError::Rejected)
        );
        // Concurrent confirms must retain the mandatory return on its own publication.
        let (routed, returned) = tokio::join!(
            publisher.publish("queue", &first),
            publisher.publish("missing", &second)
        );
        routed?;
        assert_eq!(returned, Err(TransportError::Rejected));
        publisher.publish("queue", &second).await?;
        let mut receiver = connection.receiver(&queue, 1).await?;
        let mut held = receiver
            .receive(Duration::from_secs(5))
            .await?
            .ok_or("missing delivery")?;
        assert_eq!(held.decode()?.id(), first.id());
        assert!(
            receiver
                .receive(Duration::from_millis(100))
                .await?
                .is_none(),
            "prefetch=1 must stop before acknowledgment"
        );
        held.reject(Requeue::Yes).await?;
        assert!(held.complete().await.is_err());
        let mut redelivered = receiver
            .receive(Duration::from_secs(5))
            .await?
            .ok_or("missing requeue")?;
        assert_eq!(redelivered.decode()?.id(), first.id());
        redelivered.complete().await?;
        let held = receiver
            .receive(Duration::from_secs(5))
            .await?
            .ok_or("missing second delivery")?;
        assert_eq!(held.decode()?.id(), second.id());
        // Closing a channel without acknowledging preserves work for another consumer.
        drop(held);
        receiver.close().await?;
        let mut receiver = connection.receiver(&queue, 1).await?;
        let mut held = receiver
            .receive(Duration::from_secs(5))
            .await?
            .ok_or("missing channel-close requeue")?;
        assert_eq!(held.decode()?.id(), second.id());
        held.reject(Requeue::No).await?;
        assert!(
            receiver
                .receive(Duration::from_millis(100))
                .await?
                .is_none()
        );
        receiver.close().await?;
        publisher.close().await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    // Cleanup is attempted even if the adapter returned an error.
    let deletion = channel
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await;
    let adapter_close = connection.close().await;
    let admin_close = admin.close(200, "finished".into()).await;
    result?;
    deletion?;
    adapter_close?;
    admin_close?;
    Ok(())
}
