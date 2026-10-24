//! Durable HTTP idempotency is checked against an isolated PostgreSQL database.
#![allow(clippy::unwrap_used)]

use axum::body::Body;
use axum::http::{HeaderMap, Request};
use http_body_util::BodyExt;
use raddle::RuntimeBuilder;
use raddle::wire::WireMessage;
use raddle_axum::{Api, ApiBuilder, Permission, Principal};
use raddle_postgres::Store;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use tower::ServiceExt;

#[derive(Serialize, Deserialize, JsonSchema)]
struct Command {
    value: u32,
}
impl WireMessage for Command {
    const NAME: &'static str = "test.command";
    const VERSION: u32 = 1;
}

fn api(pool: PgPool) -> Api {
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), |_: &HeaderMap| {
        Principal::try_new("tenant", "test", [Permission::Invoke])
    });
    builder
        .durable::<Command>("/commands", Store::new(pool), "commands")
        .unwrap();
    builder.build()
}
async fn submit(api: &Api, key: &str, value: u32) -> (u16, Value) {
    let request = Request::builder()
        .uri("/commands")
        .method("POST")
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer fixture")
        .header("Idempotency-Key", key)
        .body(Body::from(format!("{{\"value\":{value}}}")))
        .unwrap();
    let response = api.router().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let body =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, body)
}

#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn concurrent_duplicates_have_one_durable_effect_and_conflicts_are_rejected(pool: PgPool) {
    let api = api(pool.clone());
    let (a, b) = tokio::join!(submit(&api, "same-key", 42), submit(&api, "same-key", 42));
    assert_eq!(a.0, 202);
    assert_eq!(b.0, 202);
    assert_eq!(a.1, b.1);
    assert_eq!(submit(&api, "same-key", 43).await.0, 409);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_idempotency")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let store = Store::new(pool);
    let cutoff = chrono::Utc::now() + chrono::TimeDelta::seconds(1);
    assert_eq!(
        store
            .prune_idempotency("tenant", cutoff, 100)
            .await
            .unwrap(),
        0
    );
    let claim = store
        .claim(
            raddle_postgres::DeliveryKind::Local,
            1,
            std::time::Duration::from_secs(60),
            chrono::Utc::now(),
        )
        .await
        .unwrap()
        .remove(0);
    store.complete(&claim, chrono::Utc::now()).await.unwrap();
    assert_eq!(
        store.prune_idempotency("other", cutoff, 100).await.unwrap(),
        0
    );
    assert_eq!(
        store
            .prune_idempotency("tenant", cutoff, 100)
            .await
            .unwrap(),
        1
    );
}
