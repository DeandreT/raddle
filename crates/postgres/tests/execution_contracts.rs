//! Coordinated regressions for durable execution and transaction isolation.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{
    ConversationStatus, Delivery, DeliveryKind, Error, FailureDisposition, JobStatus,
    RequestOptions, RetryPolicy, Scope, Store,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct Message;

impl WireMessage for Message {
    const NAME: &'static str = "test.execution-contract";
    const VERSION: u32 = 1;
}

// Advisory locks are database-scoped; each SQLx test owns an isolated database.
const GATE: i64 = 210021;

async fn install_update_gate(pool: &PgPool, table: &str) {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION test_update_gate() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_advisory_xact_lock({GATE}); RETURN NULL; END $$;
         CREATE TRIGGER test_update_gate AFTER UPDATE ON {table}
         FOR EACH STATEMENT EXECUTE FUNCTION test_update_gate()"
    ))
    .execute(pool)
    .await
    .unwrap();
}

async fn wait_for_gate(pool: &PgPool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory'
                 AND objid=$1::bigint::oid AND NOT granted
                 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))",
            )
            .bind(GATE)
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn enqueue(store: &Store, max_attempts: u32) -> uuid::Uuid {
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let mut delivery = Delivery::new(DeliveryKind::Local, "handler", now);
    delivery.max_attempts = max_attempts;
    let id = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Message).unwrap(),
            &[delivery],
        )
        .await
        .unwrap()[0];
    scope.commit(now).await.unwrap();
    id
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn exhausted_claim_unlocked_after_sweep_is_never_reclaimed(pool: PgPool) {
    let store = Store::new(pool.clone());
    let id = enqueue(&store, 1).await;
    let now = Utc::now();
    let claim = store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(1), now)
        .await
        .unwrap()
        .remove(0);
    let execution = store.execution(&claim, now).await.unwrap();
    install_update_gate(&pool, "raddle_jobs").await;
    let mut gate = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(GATE)
        .execute(&mut *gate)
        .await
        .unwrap();

    let future = now + TimeDelta::seconds(2);
    let (claims, ()) = tokio::join!(
        store.claim(DeliveryKind::Local, 1, Duration::from_secs(1), future),
        async {
            // The sweep skipped the execution's row lock, then reached its
            // statement trigger. Release that row before candidate selection.
            wait_for_gate(&pool).await;
            execution.rollback().await.unwrap();
            gate.rollback().await.unwrap();
        }
    );
    assert!(claims.unwrap().is_empty());
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().attempts,
        1
    );
    assert!(
        store
            .claim(DeliveryKind::Local, 1, Duration::from_secs(1), future)
            .await
            .unwrap()
            .is_empty()
    );
    let dead = store.inspect("tenant", id).await.unwrap().unwrap();
    assert_eq!(dead.status, JobStatus::Dead);
    assert_eq!(dead.last_error.as_deref(), Some("attempts-exhausted"));
}

#[test]
fn execution_timeout_cannot_disable_postgres_statement_timeout() {
    for timeout in [Duration::from_nanos(1), Duration::from_micros(999)] {
        assert!(matches!(
            RetryPolicy::try_new(Duration::from_secs(1), Duration::from_secs(5), timeout),
            Err(Error::Invalid(_))
        ));
    }
    assert!(
        RetryPolicy::try_new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_millis(1)
        )
        .is_ok()
    );
}

async fn old_snapshot(pool: &PgPool) -> Scope<'static> {
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("SELECT count(*) FROM raddle_jobs")
        .execute(&mut *transaction)
        .await
        .unwrap();
    Scope::enroll(transaction)
}

async fn assert_stale_admission_fails(mut scope: Scope<'static>) {
    let now = Utc::now();
    let result = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Message).unwrap(),
            &[Delivery::new(DeliveryKind::Local, "handler", now)],
        )
        .await;
    assert!(matches!(result, Err(Error::Database(ref error))
        if error.as_database_error().and_then(|error| error.code()).as_deref() == Some("40001")));
    assert!(matches!(scope.commit(now).await, Err(Error::RollbackOnly)));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn repeatable_read_old_snapshot_cannot_over_admit_after_staging(pool: PgPool) {
    sqlx::query("CREATE TABLE business_effects(value integer)")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    store.set_capacity("tenant", 1).await.unwrap();
    let mut scope = old_snapshot(&pool).await;
    sqlx::query("INSERT INTO business_effects VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    enqueue(&store, 5).await;
    assert_stale_admission_fails(scope).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business_effects")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn repeatable_read_old_snapshot_cannot_over_admit_after_replay(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.set_capacity("tenant", 1).await.unwrap();
    let id = enqueue(&store, 5).await;
    let claim = store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(30), Utc::now())
        .await
        .unwrap()
        .remove(0);
    store
        .fail(
            &claim,
            FailureDisposition::Reject,
            RetryPolicy::default(),
            "rejected",
            Utc::now(),
        )
        .await
        .unwrap();
    let scope = old_snapshot(&pool).await;
    assert!(
        store
            .replay("tenant", id, "test", "retry", Utc::now())
            .await
            .unwrap()
    );
    assert_stale_admission_fails(scope).await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn application_owned_stronger_isolation_remains_supported(pool: PgPool) {
    sqlx::query("CREATE TABLE business_effects(value integer)")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    for isolation in ["REPEATABLE READ", "SERIALIZABLE"] {
        store.set_capacity(isolation, 1).await.unwrap();
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query(&format!("SET TRANSACTION ISOLATION LEVEL {isolation}"))
            .execute(&mut *transaction)
            .await
            .unwrap();
        let mut scope = Scope::enroll(transaction);
        sqlx::query("INSERT INTO business_effects VALUES(1)")
            .execute(scope.connection())
            .await
            .unwrap();
        let message = Envelope::encode(&Message).unwrap();
        let delivery = Delivery::new(DeliveryKind::Local, "handler", Utc::now());
        let ids = scope
            .stage("test", isolation, &message, std::slice::from_ref(&delivery))
            .await
            .unwrap();
        assert_eq!(
            scope
                .stage("test", isolation, &message, &[delivery])
                .await
                .unwrap(),
            ids
        );
        scope.commit(Utc::now()).await.unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business_effects")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn lookup_never_returns_expired_pending_insertion(pool: PgPool) {
    let store = Store::new(pool.clone());
    install_update_gate(&pool, "raddle_conversations").await;
    let request = Envelope::encode(&Message).unwrap();
    let deadline = request.created_at() + TimeDelta::seconds(1);
    let mut insertion = store.begin().await.unwrap();
    insertion
        .request(
            "tenant",
            &request,
            &RequestOptions {
                destination: "handler".into(),
                reply_endpoint: "replies".into(),
                responder: "handler".into(),
                reply_contract: Message::NAME.into(),
                deadline,
            },
        )
        .await
        .unwrap();
    let mut gate = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(GATE)
        .execute(&mut *gate)
        .await
        .unwrap();
    let now = deadline + TimeDelta::seconds(1);
    let lookup = store.conversation("tenant", request.id(), now);
    tokio::pin!(lookup);
    let observed = tokio::select! {
        result = &mut lookup => {
            // A lookup that never saw the uncommitted insertion may return None.
            insertion.commit(now).await.unwrap();
            gate.rollback().await.unwrap();
            result.unwrap()
        }
        () = wait_for_gate(&pool) => {
            // An UPDATE-first lookup missed the row in its expiry sweep. Make
            // the expired insertion visible before its following SELECT.
            insertion.commit(now).await.unwrap();
            gate.rollback().await.unwrap();
            lookup.await.unwrap()
        }
    };
    if let Some(conversation) = observed {
        assert_eq!(conversation.status, ConversationStatus::TimedOut);
    }
    assert_eq!(
        store
            .conversation("tenant", request.id(), now)
            .await
            .unwrap()
            .unwrap()
            .status,
        ConversationStatus::TimedOut
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM raddle_conversations WHERE id=$1")
            .bind(request.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "timed-out"
    );
}
