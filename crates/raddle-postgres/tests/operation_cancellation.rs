//! Cancelling borrowed transactional work cannot leave partial effects committable.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{Delivery, DeliveryKind, Error, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

async fn wait_for_lock(pool: &PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                .bind(pid).fetch_one(pool).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
}

#[derive(Serialize, Deserialize)]
struct Command;
impl WireMessage for Command {
    const NAME: &'static str = "cancellation.command";
    const VERSION: u32 = 1;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelled_idempotent_staging_poisons_the_scope_after_its_key_was_written(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    sqlx::query("INSERT INTO raddle_quotas(tenant) VALUES('tenant')")
        .execute(&pool)
        .await
        .unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM raddle_quotas WHERE tenant='tenant' FOR UPDATE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut scope = store.begin().await.unwrap();
    let envelope = Envelope::encode(&Command).unwrap();
    let deliveries = [Delivery::new(DeliveryKind::Local, "handler", Utc::now())];
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(scope.connection())
        .await
        .unwrap();
    let mut operation =
        Box::pin(scope.stage_idempotent("test", "tenant", "key", &envelope, &deliveries));
    tokio::select! {
        result = &mut operation => panic!("quota lock did not block staging: {result:?}"),
        () = wait_for_lock(&pool, pid) => {},
    }
    drop(operation);
    // SQLx must drain the cancelled statement before reusing its connection.
    // Release the independent blocker before querying the partially written scope.
    blocker.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_idempotency WHERE key='key'")
            .fetch_one(scope.connection())
            .await
            .unwrap(),
        1,
        "the cancelled future must actually have written part of the operation"
    );
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_idempotency")
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

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelled_multi_delivery_staging_cannot_commit_before_its_capacity_check(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    store.set_capacity("tenant", 1).await.unwrap();
    // The first delivery reaches storage; the second blocks before the final
    // capacity check. A cancelled SQLx statement may finish after the lock drops.
    sqlx::query("CREATE FUNCTION block_second_delivery() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.destination='second' THEN PERFORM pg_advisory_xact_lock(4242); END IF; RETURN NEW; END $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER second_delivery BEFORE INSERT ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION block_second_delivery()")
        .execute(&pool).await.unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(4242)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut scope = store.begin().await.unwrap();
    let envelope = Envelope::encode(&Command).unwrap();
    let deliveries = [
        Delivery::new(DeliveryKind::Local, "first", Utc::now()),
        Delivery::new(DeliveryKind::Local, "second", Utc::now()),
    ];
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(scope.connection())
        .await
        .unwrap();
    let mut operation = Box::pin(scope.stage("test", "tenant", &envelope, &deliveries));
    tokio::select! {
        result = &mut operation => panic!("second delivery did not block staging: {result:?}"),
        () = wait_for_lock(&pool, pid) => {},
    }
    drop(operation);
    blocker.rollback().await.unwrap();
    assert!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(scope.connection())
            .await
            .unwrap()
            >= 1,
        "the cancelled batch must have written a delivery before its budget check"
    );
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}
