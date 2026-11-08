//! Decrypt-before-inbox correctness against isolated real PostgreSQL databases.
#![cfg(feature = "postgres-tests")]
#![allow(clippy::unwrap_used)]

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::Utc;
use raddle::wire::{Codecs, Envelope, EnvelopeLimits, WireMessage};
use raddle_crypto::{EncryptionCodec, EncryptionPolicy, KeyId, RotatingKeys, SecretKey, Selection};
use raddle_postgres::{Delivery, DeliveryKind, HandlerFailure, RetryPolicy, Store, Worker};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Probe {
    number: u32,
}
impl WireMessage for Probe {
    const NAME: &'static str = "durable.crypto.probe";
    const VERSION: u32 = 1;
}

fn codec(keys: RotatingKeys) -> EncryptionCodec<RotatingKeys> {
    EncryptionCodec::new(
        keys,
        EncryptionPolicy {
            send: Selection::All,
            receive_required: Selection::All,
        },
        EnvelopeLimits::default(),
    )
}
fn refused_wires(encrypted: &Envelope, plaintext: &Envelope) -> Vec<Envelope> {
    let source = serde_json::to_value(encrypted).unwrap();
    let mut values = Vec::new();
    for field in ["id", "correlation_id", "causation_id"] {
        let mut value = source.clone();
        value[field] = json!(Uuid::new_v4());
        values.push(value);
    }
    for (field, content) in [
        ("version", json!(2)),
        ("created_at", json!("2020-01-01T00:00:00Z")),
        ("contract", json!("forged.contract")),
    ] {
        let mut value = source.clone();
        value[field] = content;
        values.push(value);
    }
    let mut headers = source.clone();
    headers["headers"]["context"] = json!("forged");
    values.push(headers);
    let mut body = source.clone();
    body["payload"]["ciphertext"] = json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    values.push(body);
    let mut key = source;
    key["payload"]["key_id"] = json!("unknown");
    values.push(key);
    let mut envelopes: Vec<_> = values
        .into_iter()
        .map(|value| {
            Envelope::from_slice(
                &serde_json::to_vec(&value).unwrap(),
                EnvelopeLimits::default(),
            )
            .unwrap()
        })
        .collect();
    envelopes.push(plaintext.clone());
    envelopes
}
async fn accept_wire(
    codec: &EncryptionCodec<RotatingKeys>,
    store: &Store,
    wire: &[u8],
    delivery: &Delivery,
    staging_calls: &AtomicUsize,
) -> Result<(Uuid, Vec<u8>), Box<dyn Error>> {
    // Authentication, requirements and byte/header validation MUST precede
    // both transaction creation and Store's canonical content fingerprint.
    let plain = codec.decode_bytes(wire).await?;
    staging_calls.fetch_add(1, Ordering::SeqCst);
    let mut scope = store.begin().await?;
    let ids = scope
        .stage(
            "crypto-receive",
            "test-tenant",
            &plain,
            std::slice::from_ref(delivery),
        )
        .await?;
    scope.commit(Utc::now()).await?;
    Ok((ids[0], plain.to_vec()?))
}
async fn execute_once(pool: &PgPool, store: Store) {
    let mut worker = Worker::new(store, RetryPolicy::default());
    worker
        .handle::<Probe, _>("handler", Codecs::default(), |probe, scope| {
            Box::pin(async move {
                sqlx::query("INSERT INTO crypto_handler_calls(number) VALUES($1)")
                    .bind(i64::from(probe.number))
                    .execute(scope.connection())
                    .await
                    .map_err(|_| HandlerFailure::Reject)?;
                Ok(())
            })
        })
        .unwrap();
    assert_eq!(worker.run_once(16).await.unwrap(), 1);
    assert_eq!(worker.run_once(16).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM crypto_handler_calls WHERE number=42")
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL through DATABASE_URL"]
async fn random_nonce_and_rotated_key_retries_deduplicate_after_decryption(pool: PgPool) {
    sqlx::query("CREATE TABLE crypto_handler_calls(number bigint NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    let keys = RotatingKeys::new(2).unwrap();
    keys.insert(
        KeyId::new("first").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    let codec = codec(keys.clone());
    let original = Envelope::encode(&Probe { number: 42 })
        .unwrap()
        .correlated(Uuid::new_v4(), Uuid::new_v4())
        .unwrap()
        .with_header("context".into(), "untrusted-original".into())
        .unwrap();
    let first = codec.encode(&original).await.unwrap();
    let retry = codec.encode(&original).await.unwrap();
    keys.insert(
        KeyId::new("rotated").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    let rotated = codec.encode(&original).await.unwrap();
    let first_value = serde_json::to_value(&first).unwrap();
    let retry_value = serde_json::to_value(&retry).unwrap();
    assert_ne!(
        first_value["payload"]["nonce"],
        retry_value["payload"]["nonce"]
    );
    assert_ne!(first.to_vec().unwrap(), retry.to_vec().unwrap());
    assert_ne!(first.to_vec().unwrap(), rotated.to_vec().unwrap());
    // Delivery fingerprint inputs are immutable too. Replacing available_at
    // with processing-time now on every retry would create another conflict.
    let delivery = Delivery::new(DeliveryKind::Local, "handler", original.created_at());
    let staging_calls = AtomicUsize::new(0);
    let mut accepted = Vec::new();
    for wire in [&first, &retry, &rotated, &first] {
        let (job, bytes) = accept_wire(
            &codec,
            &store,
            &wire.to_vec().unwrap(),
            &delivery,
            &staging_calls,
        )
        .await
        .unwrap();
        assert_eq!(bytes, original.to_vec().unwrap());
        accepted.push(job);
    }
    assert!(accepted.iter().all(|id| *id == accepted[0]));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let stored: Vec<u8> = sqlx::query_scalar("SELECT envelope FROM raddle_jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored, original.to_vec().unwrap());
    let plain = Envelope::encode(&Probe { number: 99 }).unwrap();
    for refused in refused_wires(&retry, &plain) {
        assert!(
            accept_wire(
                &codec,
                &store,
                &refused.to_vec().unwrap(),
                &delivery,
                &staging_calls
            )
            .await
            .is_err()
        );
    }
    assert_eq!(staging_calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM crypto_handler_calls")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    // Demonstrate the actual rejected ordering in a rolled-back control scope.
    let mut unsafe_scope = store.begin().await.unwrap();
    unsafe_scope
        .stage(
            "ciphertext-control",
            "test-tenant",
            &first,
            std::slice::from_ref(&delivery),
        )
        .await
        .unwrap();
    assert!(matches!(
        unsafe_scope
            .stage(
                "ciphertext-control",
                "test-tenant",
                &retry,
                std::slice::from_ref(&delivery)
            )
            .await,
        Err(raddle_postgres::Error::IdentityConflict)
    ));
    unsafe_scope.rollback().await.unwrap();
    execute_once(&pool, store).await;
}

#[cfg(feature = "rabbitmq-tests")]
#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL and RADDLE_RABBITMQ_TEST_URI"]
async fn rabbit_retry_rotation_decrypts_before_canonical_durable_staging(
    pool: PgPool,
) -> Result<(), Box<dyn Error>> {
    use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
    use lapin::types::FieldTable;
    use lapin::{Connection as AmqpConnection, ConnectionProperties};
    use raddle::transport::Transport;
    use raddle_crypto::EncryptedTransport;
    use raddle_rabbitmq::{Connection, Requeue, Route};
    use std::time::Duration;

    sqlx::query("CREATE TABLE crypto_handler_calls(number bigint NOT NULL)")
        .execute(&pool)
        .await?;
    let store = Store::new(pool.clone());
    let uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")?;
    let connection = Connection::connect(&uri).await?;
    let admin = AmqpConnection::connect(&uri, ConnectionProperties::default()).await?;
    let channel = admin.create_channel().await?;
    let queue = format!("raddle-crypto-inbox-{}", Uuid::new_v4());
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
        let routes = || {
            [Route {
                alias: "input".into(),
                exchange: String::new(),
                routing_key: queue.clone(),
            }]
        };
        let publisher = connection.publisher(routes()).await?;
        let raw = connection.publisher(routes()).await?;
        let keys = RotatingKeys::new(2)?;
        keys.insert(KeyId::new("old").unwrap(), SecretKey::generate()?, true)?;
        let wrapper = EncryptedTransport::new(publisher, codec(keys.clone()));
        let receive_codec = codec(keys.clone());
        let original = Envelope::encode(&Probe { number: 42 })?
            .correlated(Uuid::new_v4(), Uuid::new_v4())?
            .with_header("context".into(), "untrusted-original".into())?;
        let delivery = Delivery::new(DeliveryKind::Local, "handler", original.created_at());
        wrapper.publish("input", &original).await?;
        wrapper.publish("input", &original).await?;
        keys.insert(KeyId::new("new")?, SecretKey::generate()?, true)?;
        wrapper.publish("input", &original).await?;
        let mut receiver = connection.receiver(&queue, 1).await?;
        let staging_calls = AtomicUsize::new(0);
        let mut wire_copies = Vec::new();
        let mut jobs = Vec::new();
        for _ in 0..3 {
            let mut held = receiver
                .receive(Duration::from_secs(10))
                .await?
                .ok_or("missing retry publication")?;
            let wire = held.decode()?;
            let (job, canonical) = accept_wire(
                &receive_codec,
                &store,
                &wire.to_vec()?,
                &delivery,
                &staging_calls,
            )
            .await?;
            assert_eq!(canonical, original.to_vec()?);
            jobs.push(job);
            wire_copies.push(wire);
            held.complete().await?;
        }
        assert!(jobs.iter().all(|id| *id == jobs[0]));
        assert_ne!(
            serde_json::to_value(&wire_copies[0])?["payload"]["nonce"],
            serde_json::to_value(&wire_copies[1])?["payload"]["nonce"]
        );
        assert_eq!(
            serde_json::to_value(&wire_copies[2])?["payload"]["key_id"],
            "new"
        );
        for rejected in refused_wires(&wire_copies[0], &original) {
            raw.publish("input", &rejected).await?;
            let mut held = receiver
                .receive(Duration::from_secs(10))
                .await?
                .ok_or("missing refused wire")?;
            assert!(
                accept_wire(
                    &receive_codec,
                    &store,
                    &held.decode()?.to_vec()?,
                    &delivery,
                    &staging_calls
                )
                .await
                .is_err()
            );
            held.reject(Requeue::No).await?;
        }
        assert_eq!(staging_calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
                .fetch_one(&pool)
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM crypto_handler_calls")
                .fetch_one(&pool)
                .await?,
            0
        );
        execute_once(&pool, store).await;
        receiver.close().await?;
        raw.close().await?;
        let (publisher, _) = wrapper.into_parts();
        publisher.close().await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    let deletion = channel
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await;
    let close = connection.close().await;
    let admin_close = admin.close(200, "finished".into()).await;
    result?;
    deletion?;
    close?;
    admin_close?;
    Ok(())
}
