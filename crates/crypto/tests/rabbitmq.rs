//! Explicit opt-in qualification against an existing disposable local broker.
#![cfg(feature = "rabbitmq-tests")]
#![allow(clippy::unwrap_used)]

use std::error::Error;
use std::time::Duration;

use lapin::options::{
    ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{Connection as AmqpConnection, ConnectionProperties};
use raddle::transport::Transport;
use raddle::wire::{Envelope, EnvelopeLimits, WireMessage};
use raddle_crypto::{
    EncryptedTransport, EncryptionCodec, EncryptionPolicy, KeyId, RotatingKeys, SecretKey,
    Selection,
};
use raddle_rabbitmq::{Connection, Requeue, Route};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Probe {
    number: u32,
}
impl WireMessage for Probe {
    const NAME: &'static str = "crypto.probe";
    const VERSION: u32 = 1;
}

#[tokio::test]
#[ignore = "requires RADDLE_RABBITMQ_TEST_URI for an isolated local broker"]
async fn confirmed_encryption_preserves_context_and_rejects_before_handler()
-> Result<(), Box<dyn Error>> {
    let uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")?;
    let connection = Connection::connect(&uri).await?;
    let admin = AmqpConnection::connect(&uri, ConnectionProperties::default()).await?;
    let channel = admin.create_channel().await?;
    let queue = format!("raddle-crypto-{}", Uuid::new_v4());
    let dead_exchange = format!("{queue}-dead");
    let dead_queue = format!("{queue}-dead-queue");
    channel
        .exchange_declare(
            dead_exchange.clone().into(),
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_declare(
            dead_queue.clone().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            dead_queue.clone().into(),
            dead_exchange.clone().into(),
            "crypto-failure".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    let mut arguments = FieldTable::default();
    arguments.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString(dead_exchange.clone().into()),
    );
    arguments.insert(
        "x-dead-letter-routing-key".into(),
        AMQPValue::LongString("crypto-failure".into()),
    );
    channel
        .queue_declare(
            queue.clone().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            arguments,
        )
        .await?;
    let result = async {
        let publisher = connection
            .publisher([Route {
                alias: "probe".into(),
                exchange: String::new(),
                routing_key: queue.clone(),
            }])
            .await?;
        let raw = connection
            .publisher([Route {
                alias: "probe".into(),
                exchange: String::new(),
                routing_key: queue.clone(),
            }])
            .await?;
        let keys = RotatingKeys::new(1)?;
        keys.insert(KeyId::new("ephemeral-test")?, SecretKey::generate()?, true)?;
        let codec = EncryptionCodec::new(
            keys.clone(),
            EncryptionPolicy {
                send: Selection::All,
                receive_required: Selection::All,
            },
            EnvelopeLimits::default(),
        );
        let decoder = EncryptionCodec::new(
            keys,
            EncryptionPolicy {
                send: Selection::All,
                receive_required: Selection::All,
            },
            EnvelopeLimits::default(),
        );
        let transport = EncryptedTransport::new(publisher, codec);
        let original = Envelope::encode(&Probe { number: 42 })?
            .correlated(Uuid::new_v4(), Uuid::new_v4())?
            .with_header("context".into(), "untrusted-original".into())?;
        let before = original.to_vec()?;
        // No consumer exists yet: confirmation is broker acceptance, not handler
        // execution or application completion.
        transport.publish("probe", &original).await?;
        assert_eq!(original.to_vec()?, before);
        let mut receiver = connection.receiver(&queue, 1).await?;
        let mut held = receiver
            .receive(Duration::from_secs(10))
            .await?
            .ok_or("missing ciphertext")?;
        let encrypted = held.decode()?;
        let decoded = decoder.decode(&encrypted).await?;
        assert_eq!(decoded.to_vec()?, before);
        assert_eq!(decoded.decode::<Probe>()?.number, 42);
        held.complete().await?;
        let mut handler_calls = 1;
        let mut changed = serde_json::to_value(&encrypted)?;
        changed["payload"]["ciphertext"] = json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        let tampered =
            Envelope::from_slice(&serde_json::to_vec(&changed)?, EnvelopeLimits::default())?;
        let mut changed = serde_json::to_value(&encrypted)?;
        changed["payload"]["key_id"] = json!("unknown");
        let unknown =
            Envelope::from_slice(&serde_json::to_vec(&changed)?, EnvelopeLimits::default())?;
        for bad in [&original, &tampered, &unknown] {
            raw.publish("probe", bad).await?;
            let mut held = receiver
                .receive(Duration::from_secs(10))
                .await?
                .ok_or("missing negative case")?;
            if decoder.decode(&held.decode()?).await.is_ok() {
                handler_calls += 1;
            }
            held.reject(Requeue::No).await?;
        }
        assert_eq!(handler_calls, 1);
        let mut dead_receiver = connection.receiver(&dead_queue, 3).await?;
        for _ in 0..3 {
            let mut rejected = dead_receiver
                .receive(Duration::from_secs(10))
                .await?
                .ok_or("crypto failure did not reach DLQ")?;
            assert_eq!(rejected.decode()?.id(), original.id());
            rejected.complete().await?;
        }
        dead_receiver.close().await?;
        receiver.close().await?;
        raw.close().await?;
        let (publisher, _) = transport.into_parts();
        publisher.close().await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    let deletion = channel
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await;
    let dead_deletion = channel
        .queue_delete(dead_queue.into(), QueueDeleteOptions::default())
        .await;
    let exchange_deletion = channel
        .exchange_delete(dead_exchange.into(), ExchangeDeleteOptions::default())
        .await;
    let close = connection.close().await;
    let admin_close = admin.close(200, "finished".into()).await;
    result?;
    deletion?;
    dead_deletion?;
    exchange_deletion?;
    close?;
    admin_close?;
    Ok(())
}
