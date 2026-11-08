//! Typed saga dispatch on the worker's existing transaction and trusted tenant.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_postgres::{
    CorrelationKey, Delivery, DeliveryKind, HandlerFailure, RetryPolicy, SagaInitialization,
    SagaState, Store, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct Change {
    correlation: String,
    tenant: String,
    fail: bool,
    finish: bool,
}
impl WireMessage for Change {
    const NAME: &'static str = "test.saga-change";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct State {
    count: u32,
}
impl SagaState for State {
    const NAME: &'static str = "test.dispatch-saga";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Effect(u32);
impl WireMessage for Effect {
    const NAME: &'static str = "test.saga-effect";
    const VERSION: u32 = 1;
}

async fn enqueue(store: &Store, correlation: &str, fail: bool, finish: bool) {
    let mut scope = store.begin().await.unwrap();
    let envelope = Envelope::encode(&Change {
        correlation: correlation.into(),
        tenant: "forged".into(),
        fail,
        finish,
    })
    .unwrap()
    .with_header("tenant".into(), "also-forged".into())
    .unwrap();
    scope
        .stage(
            "ingress",
            "trusted",
            &envelope,
            &[Delivery::new(DeliveryKind::Local, "changes", Utc::now())],
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
}

fn worker(store: Store, initialization: SagaInitialization<State>) -> Worker {
    let mut worker = Worker::new(
        store,
        RetryPolicy::try_new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            Duration::from_secs(5),
        )
        .unwrap(),
    );
    worker
        .handle_saga::<Change, State, _, _>(
            "changes",
            Codecs::default(),
            |message| CorrelationKey::new(&message.correlation).map_err(|_| HandlerFailure::Reject),
            initialization,
            |message, saga, scope| {
                Box::pin(async move {
                    saga.state_mut().count += 1;
                    let tenant = scope.tenant().unwrap().to_owned();
                    let outgoing = Envelope::encode(&Effect(saga.state().count))
                        .map_err(|_| HandlerFailure::Reject)?;
                    scope
                        .stage(
                            "saga",
                            &tenant,
                            &outgoing,
                            &[Delivery::new(DeliveryKind::Remote, "effects", Utc::now())],
                        )
                        .await
                        .map_err(|_| HandlerFailure::Retry)?;
                    scope
                        .schedule_saga_timeout(saga, "timeouts", Utc::now() + TimeDelta::hours(1))
                        .await
                        .map_err(|_| HandlerFailure::Retry)?;
                    if message.fail {
                        return Err(HandlerFailure::Reject);
                    }
                    if message.finish {
                        saga.finish();
                    }
                    Ok(())
                })
            },
        )
        .unwrap();
    worker
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn state_outgoing_timeout_and_inbox_completion_commit_together(pool: PgPool) {
    let store = Store::new(pool.clone());
    enqueue(&store, "order", false, false).await;
    let worker = worker(
        store.clone(),
        SagaInitialization::Start(|| State { count: 0 }),
    );
    assert_eq!(worker.run_once(1).await.unwrap(), 1);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_sagas WHERE tenant='trusted' AND (state->>'count')::int=1 AND timeout_generation=1 AND timeout_due IS NOT NULL").fetch_one(&pool).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE source='saga' AND tenant='trusted'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE source='raddle.sagas' AND tenant='trusted'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE source='ingress' AND status='completed'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_sagas WHERE tenant <> 'trusted'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn failed_transition_rolls_back_state_outgoing_and_timeout_but_neighbors_run(pool: PgPool) {
    let store = Store::new(pool.clone());
    enqueue(&store, "before", false, false).await;
    enqueue(&store, "bad", true, false).await;
    enqueue(&store, "after", false, false).await;
    let worker = worker(
        store.clone(),
        SagaInitialization::Start(|| State { count: 0 }),
    );
    assert_eq!(worker.run_once(3).await.unwrap(), 3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_sagas")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_sagas WHERE correlation='bad'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE source='saga'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE source='raddle.sagas'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE source='ingress' AND status='dead'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    // A corrected new incoming message processes only the failed correlation.
    enqueue(&store, "bad", false, false).await;
    assert_eq!(worker.run_once(3).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_sagas WHERE (state->>'count')::int=1"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn existing_only_and_completed_tombstones_are_noops(pool: PgPool) {
    let store = Store::new(pool.clone());
    enqueue(&store, "unknown", false, false).await;
    let existing = worker(store.clone(), SagaInitialization::ExistingOnly);
    existing.run_once(1).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_sagas")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    enqueue(&store, "finished", false, true).await;
    let starter = worker(
        store.clone(),
        SagaInitialization::Start(|| State { count: 0 }),
    );
    starter.run_once(1).await.unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT completed FROM raddle_sagas")
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    // Tombstones remain harmless even after a state-schema deployment changes.
    sqlx::query("UPDATE raddle_sagas SET schema_version=2,state='{}'")
        .execute(&pool)
        .await
        .unwrap();
    enqueue(&store, "finished", false, false).await;
    starter.run_once(1).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE source='saga'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE source='ingress' AND status='completed'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn existing_only_transitions_existing_state_and_validates_selector(pool: PgPool) {
    let store = Store::new(pool.clone());
    let mut scope = store.begin().await.unwrap();
    let mut saga = scope
        .saga("trusted", "existing", || State { count: 5 })
        .await
        .unwrap();
    scope.save_saga(&mut saga).await.unwrap();
    scope.commit(Utc::now()).await.unwrap();
    enqueue(&store, "existing", false, false).await;
    enqueue(&store, "", false, false).await;
    let worker = worker(store, SagaInitialization::ExistingOnly);
    worker.run_once(2).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT (state->>'count')::int FROM raddle_sagas")
            .fetch_one(&pool)
            .await
            .unwrap(),
        6
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE source='ingress' AND status='dead'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[test]
fn correlation_is_bounded_and_rejects_control_characters() {
    for value in [
        "".to_owned(),
        "x".repeat(257),
        "order\n1".to_owned(),
        "order\0".to_owned(),
    ] {
        assert!(CorrelationKey::new(value).is_err());
    }
    assert_eq!(CorrelationKey::new("order-1").unwrap().as_str(), "order-1");
}
