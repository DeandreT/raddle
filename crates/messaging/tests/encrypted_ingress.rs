//! Actual encryption through the bus's pre-custody boundary and PostgreSQL worker.
#![cfg(feature = "crypto-ingress")]
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_crypto::{EncryptionCodec, EncryptionPolicy, KeyId, RotatingKeys, SecretKey, Selection};
use raddle_messaging::{BusBuilder, IngressError, TrustedOrigin};
use raddle_postgres::{HandlerFailure, RetryPolicy, Store, Worker};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
struct Input(u32);
impl WireMessage for Input {
    const NAME: &'static str = "bus.encrypted-input";
    const VERSION: u32 = 1;
}

fn origin(tenant: &str) -> TrustedOrigin {
    TrustedOrigin::authenticated(tenant, "trusted-transport", None).unwrap()
}

fn tampered(wire: &Envelope) -> Vec<Vec<u8>> {
    let original = serde_json::to_value(wire).unwrap();
    let mut results = Vec::new();
    for name in ["id", "correlation_id", "causation_id"] {
        let mut value = original.clone();
        value[name] = serde_json::json!(Uuid::new_v4());
        results.push(serde_json::to_vec(&value).unwrap());
    }
    for (name, replacement) in [
        ("contract", serde_json::json!("forged.contract")),
        ("version", serde_json::json!(2)),
        ("created_at", serde_json::json!("2020-01-01T00:00:00Z")),
    ] {
        let mut value = original.clone();
        value[name] = replacement;
        results.push(serde_json::to_vec(&value).unwrap());
    }
    let mut headers = original.clone();
    headers["headers"]["tenant"] = serde_json::json!("forged-tenant");
    results.push(serde_json::to_vec(&headers).unwrap());
    let mut key = original.clone();
    key["payload"]["key_id"] = serde_json::json!("unknown-key");
    results.push(serde_json::to_vec(&key).unwrap());
    let mut body = original;
    body["payload"]["ciphertext"] =
        serde_json::json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    results.push(serde_json::to_vec(&body).unwrap());
    results
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn authenticated_crypto_ingress_is_canonical_tenant_scoped_and_isolates_bad_neighbors(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    sqlx::query("CREATE TABLE encrypted_bus_calls(number bigint NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let keys = RotatingKeys::new(2).unwrap();
    keys.insert(
        KeyId::new("first").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    let codec = EncryptionCodec::new(
        keys.clone(),
        EncryptionPolicy {
            send: Selection::All,
            receive_required: Selection::All,
        },
        Default::default(),
    );
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.durable("inbox", store.clone()).unwrap();
    builder.route::<Input>("inbox").unwrap();
    let bus = builder.build();
    let original = Envelope::encode(&Input(42))
        .unwrap()
        .with_header("tenant".into(), "untrusted-tenant".into())
        .unwrap();
    let first = codec.encode(&original).await.unwrap();
    let retry = codec.encode(&original).await.unwrap();
    assert_ne!(first.to_vec().unwrap(), retry.to_vec().unwrap());
    keys.insert(
        KeyId::new("rotated").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    let rotated = codec.encode(&original).await.unwrap();
    let mut refused = tampered(&first);
    refused.push(original.to_vec().unwrap());
    for bytes in &refused {
        assert!(matches!(
            bus.receive(&codec, bytes, origin("tenant-a"), Utc::now(), true)
                .await,
            Err(IngressError::Decode(_))
        ));
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    // Successful neighbors stay staged while a malformed middle item is refused.
    for wire in [&first, &retry, &rotated, &first] {
        bus.receive(
            &codec,
            &wire.to_vec().unwrap(),
            origin("tenant-a"),
            Utc::now(),
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            bus.receive(&codec, &refused[0], origin("tenant-a"), Utc::now(), true)
                .await,
            Err(IngressError::Decode(_))
        ));
    }
    // The same original ID belongs to distinct inbox namespaces, not to a tenant
    // supplied by the authenticated but non-authoritative envelope headers.
    bus.receive(
        &codec,
        &retry.to_vec().unwrap(),
        origin("tenant-b"),
        Utc::now(),
        true,
    )
    .await
    .unwrap();
    let rows: Vec<(String, String, Vec<u8>)> =
        sqlx::query_as("SELECT source,tenant,envelope FROM raddle_jobs ORDER BY tenant")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
    for (index, (source, tenant, bytes)) in rows.iter().enumerate() {
        assert_eq!(source, "trusted-transport");
        assert_eq!(tenant, if index == 0 { "tenant-a" } else { "tenant-b" });
        assert_eq!(bytes, &original.to_vec().unwrap());
    }
    let mut worker = Worker::new(store, RetryPolicy::default());
    worker
        .handle::<Input, _>("inbox", Codecs::default(), |input, scope| {
            Box::pin(async move {
                sqlx::query("INSERT INTO encrypted_bus_calls(number) VALUES($1)")
                    .bind(i64::from(input.0))
                    .execute(scope.connection())
                    .await
                    .map_err(|_| HandlerFailure::Reject)?;
                Ok(())
            })
        })
        .unwrap();
    assert_eq!(worker.run_once(16).await.unwrap(), 2);
    assert_eq!(worker.run_once(16).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM encrypted_bus_calls WHERE number=42")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}
