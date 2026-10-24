//! Actual subprocess exit tests for rollback and confirmed-publication uncertainty.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{self, Command};
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{Delivery, DeliveryKind, Store};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgPool};

#[derive(Serialize, Deserialize)]
struct Message {
    value: u32,
}
impl WireMessage for Message {
    const NAME: &'static str = "test.crash";
    const VERSION: u32 = 1;
}

// This helper is invoked by name in a new process. Without the explicit private
// environment it performs no I/O, so ordinary `cargo test` remains self-contained.
#[tokio::test]
async fn subprocess_probe() {
    let Ok(url) = std::env::var("RADDLE_CRASH_TEST_URL") else {
        return;
    };
    let options: PgConnectOptions = url.parse().unwrap();
    let pool = PgPool::connect_with(options.disable_statement_logging())
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    match std::env::var("RADDLE_CRASH_TEST_PHASE").unwrap().as_str() {
        "before-commit" => {
            let mut scope = store.begin().await.unwrap();
            sqlx::query("INSERT INTO business VALUES(1)")
                .execute(scope.connection())
                .await
                .unwrap();
            scope
                .stage(
                    "test",
                    "tenant",
                    &Envelope::encode(&Message { value: 1 }).unwrap(),
                    &[Delivery::new(DeliveryKind::Remote, "remote", Utc::now())],
                )
                .await
                .unwrap();
            process::exit(23); // Deliberately bypass all Rust destructors.
        }
        "after-accept" => {
            let claim = store
                .claim(DeliveryKind::Remote, 1, Duration::from_secs(1), Utc::now())
                .await
                .unwrap()
                .remove(0);
            // A separately committed receipt stands in for a broker's durable
            // acceptance; it cannot roll back with the publisher's state.
            sqlx::query("INSERT INTO receipts(message_id) VALUES($1)")
                .bind(claim.envelope().unwrap().id())
                .execute(&pool)
                .await
                .unwrap();
            process::exit(24); // Acceptance occurred; outbox completion did not.
        }
        _ => panic!("unknown fault phase"),
    }
}

fn crash(pool: &PgPool, phase: &str, code: i32) {
    let result = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "subprocess_probe", "--nocapture"])
        .env(
            "RADDLE_CRASH_TEST_URL",
            pool.connect_options().to_url_lossy().as_str(),
        )
        .env("RADDLE_CRASH_TEST_PHASE", phase)
        .output()
        .unwrap();
    assert_eq!(
        result.status.code(),
        Some(code),
        "fault subprocess did not reach its intended exit point"
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn process_exit_rolls_back_business_and_outgoing_messages(pool: PgPool) {
    sqlx::query("CREATE TABLE business(id integer)")
        .execute(&pool)
        .await
        .unwrap();
    crash(&pool, "before-commit", 23);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn process_exit_after_acceptance_republishes_the_same_identity(pool: PgPool) {
    sqlx::query("CREATE TABLE receipts(message_id uuid)")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    let envelope = Envelope::encode(&Message { value: 1 }).unwrap();
    let mut scope = store.begin().await.unwrap();
    scope
        .stage(
            "test",
            "tenant",
            &envelope,
            &[Delivery::new(DeliveryKind::Remote, "remote", Utc::now())],
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    crash(&pool, "after-accept", 24);
    let claim = store
        .claim(
            DeliveryKind::Remote,
            1,
            Duration::from_secs(60),
            Utc::now() + TimeDelta::seconds(2),
        )
        .await
        .unwrap()
        .remove(0);
    assert_eq!(claim.envelope().unwrap().id(), envelope.id());
    sqlx::query("INSERT INTO receipts(message_id) VALUES($1)")
        .bind(claim.envelope().unwrap().id())
        .execute(&pool)
        .await
        .unwrap();
    store
        .complete(&claim, Utc::now() + TimeDelta::seconds(2))
        .await
        .unwrap();
    let receipts: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT message_id FROM receipts")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(receipts, vec![envelope.id(), envelope.id()]);
}
