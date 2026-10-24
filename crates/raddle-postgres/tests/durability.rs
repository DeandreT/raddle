//! Opt-in tests allocate isolated databases on a disposable PostgreSQL server.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_postgres::{
    Delivery, DeliveryKind, Error, HandlerFailure, JobStatus, OutboxWorker, RetryPolicy, Scope,
    Store, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct Order {
    number: i32,
}
impl WireMessage for Order {
    const NAME: &'static str = "orders.place";
    const VERSION: u32 = 1;
}

async fn enqueue(store: &Store, kind: DeliveryKind, destination: &str) -> uuid::Uuid {
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let ids = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Order { number: 42 }).unwrap(),
            &[Delivery::new(kind, destination, now)],
        )
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    ids[0]
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn business_writes_and_effects_share_one_transaction(pool: PgPool) {
    sqlx::query("CREATE TABLE orders(number integer PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    let envelope = Envelope::encode(&Order { number: 1 }).unwrap();
    let delivery = Delivery::new(DeliveryKind::Local, "audit", Utc::now());
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    sqlx::query("INSERT INTO orders VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope
        .stage("test", "tenant", &envelope, std::slice::from_ref(&delivery))
        .await
        .unwrap();
    scope.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM orders")
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
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    sqlx::query("INSERT INTO orders VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope
        .stage("test", "tenant", &envelope, &[delivery])
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM orders")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    store.migrate().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn duplicate_conflicts_poison_the_entire_fanout(pool: PgPool) {
    let store = Store::new(pool.clone());
    let now = Utc::now();
    let envelope = Envelope::encode(&Order { number: 1 }).unwrap();
    let delivery = Delivery::new(DeliveryKind::Local, "audit", now);
    let mut scope = store.begin().await.unwrap();
    let original = scope
        .stage("test", "tenant", &envelope, std::slice::from_ref(&delivery))
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    assert_eq!(
        scope
            .stage("test", "tenant", &envelope, std::slice::from_ref(&delivery))
            .await
            .unwrap(),
        original
    );
    scope.commit(now).await.unwrap();
    let changed =
        Envelope::with_identity(envelope.id(), envelope.created_at(), &Order { number: 2 })
            .unwrap();
    let mut scope = store.begin().await.unwrap();
    let new_subscriber = Delivery::new(DeliveryKind::Local, "other", now);
    assert!(matches!(
        scope
            .stage("test", "tenant", &changed, &[new_subscriber, delivery])
            .await,
        Err(Error::IdentityConflict)
    ));
    assert!(matches!(scope.commit(now).await, Err(Error::RollbackOnly)));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn stale_generation_cannot_commit_or_complete(pool: PgPool) {
    let store = Store::new(pool);
    let id = enqueue(&store, DeliveryKind::Local, "audit").await;
    let now = Utc::now();
    let first = store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(1), now)
        .await
        .unwrap()
        .remove(0);
    let second = store
        .claim(
            DeliveryKind::Local,
            1,
            Duration::from_secs(60),
            now + TimeDelta::seconds(2),
        )
        .await
        .unwrap()
        .remove(0);
    assert!(matches!(
        store.execution(&first, now).await,
        Err(Error::StaleClaim)
    ));
    assert!(matches!(
        store.complete(&first, now).await,
        Err(Error::StaleClaim)
    ));
    let scope = store
        .execution(&second, now + TimeDelta::seconds(2))
        .await
        .unwrap();
    scope.commit(now + TimeDelta::seconds(2)).await.unwrap();
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Completed
    );
    assert!(store.inspect("other-tenant", id).await.unwrap().is_none());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn active_database_execution_is_not_stolen_after_lease_time(pool: PgPool) {
    let store = Store::new(pool);
    enqueue(&store, DeliveryKind::Local, "audit").await;
    let now = Utc::now();
    let claim = store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(1), now)
        .await
        .unwrap()
        .remove(0);
    let scope = store.execution(&claim, now).await.unwrap();
    let recovered = tokio::time::timeout(
        Duration::from_secs(2),
        store.claim(
            DeliveryKind::Local,
            1,
            Duration::from_secs(1),
            now + TimeDelta::seconds(3),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(recovered.is_empty());
    scope.commit(now + TimeDelta::seconds(4)).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn repeated_crashes_exhaust_attempts_and_replay_is_audited(pool: PgPool) {
    let store = Store::new(pool.clone());
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let mut delivery = Delivery::new(DeliveryKind::Local, "audit", now);
    delivery.max_attempts = 1;
    let id = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Order { number: 1 }).unwrap(),
            &[delivery],
        )
        .await
        .unwrap()[0];
    scope.commit(now).await.unwrap();
    store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(1), now)
        .await
        .unwrap();
    assert!(
        store
            .claim(
                DeliveryKind::Local,
                1,
                Duration::from_secs(1),
                now + TimeDelta::seconds(2)
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Dead
    );
    assert!(
        !store
            .replay("other", id, "operator", "retry", now)
            .await
            .unwrap()
    );
    assert!(
        store
            .replay("tenant", id, "operator", "fixed handler", now)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_replay_audit")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn per_key_serialization_and_delayed_delivery(pool: PgPool) {
    let store = Store::new(pool);
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    for number in 0..3 {
        let mut delivery = Delivery::new(DeliveryKind::Local, "audit", now);
        delivery.partition_key = Some("order-42".into());
        if number == 2 {
            delivery.available_at = now + TimeDelta::hours(1);
        }
        scope
            .stage(
                "test",
                "tenant",
                &Envelope::encode(&Order { number }).unwrap(),
                &[delivery],
            )
            .await
            .unwrap();
    }
    scope.commit(now).await.unwrap();
    let claims = store
        .claim(DeliveryKind::Local, 10, Duration::from_secs(60), now)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert!(
        store
            .claim(DeliveryKind::Local, 10, Duration::from_secs(60), now)
            .await
            .unwrap()
            .is_empty()
    );
    store.complete(&claims[0], now).await.unwrap();
    let next = store
        .claim(DeliveryKind::Local, 10, Duration::from_secs(60), now)
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    store.complete(&next[0], now).await.unwrap();
    assert!(
        store
            .claim(DeliveryKind::Local, 10, Duration::from_secs(60), now)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .claim(
                DeliveryKind::Local,
                10,
                Duration::from_secs(60),
                now + TimeDelta::hours(2)
            )
            .await
            .unwrap()
            .len(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn successful_sibling_is_not_replayed_when_other_subscriber_fails(pool: PgPool) {
    let store = Store::new(pool.clone());
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let ids = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Order { number: 1 }).unwrap(),
            &[
                Delivery::new(DeliveryKind::Local, "good", now),
                Delivery::new(DeliveryKind::Local, "bad", now),
            ],
        )
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut worker = Worker::new(store.clone(), RetryPolicy::default());
    let counted = Arc::clone(&calls);
    worker
        .handle::<Order, _>("good", Codecs::default(), move |_, scope| {
            counted.fetch_add(1, Ordering::SeqCst);
            assert_eq!(scope.tenant(), Some("tenant"));
            Box::pin(async { Ok(()) })
        })
        .unwrap();
    worker
        .handle::<Order, _>("bad", Codecs::default(), |_, _| {
            Box::pin(async { Err(HandlerFailure::Retry) })
        })
        .unwrap();
    assert_eq!(worker.run_once(10).await.unwrap(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .inspect("tenant", ids[0])
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Completed
    );
    assert_eq!(
        store
            .inspect("tenant", ids[1])
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Ready
    );
    sqlx::query("UPDATE raddle_jobs SET available_at=now() WHERE status='ready'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(worker.run_once(10).await.unwrap(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct Uncertain {
    calls: Arc<AtomicUsize>,
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn capacity_is_atomic_and_duplicates_do_not_consume_another_slot(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.set_capacity("tenant", 1).await.unwrap();
    let now = Utc::now();
    let message = Envelope::encode(&Order { number: 1 }).unwrap();
    let delivery = Delivery::new(DeliveryKind::Local, "audit", now);
    let mut scope = store.begin().await.unwrap();
    scope
        .stage("test", "tenant", &message, std::slice::from_ref(&delivery))
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    let mut duplicate = store.begin().await.unwrap();
    duplicate
        .stage("test", "tenant", &message, std::slice::from_ref(&delivery))
        .await
        .unwrap();
    duplicate.commit(now).await.unwrap();
    let mut overflow = store.begin().await.unwrap();
    assert!(matches!(
        overflow
            .stage(
                "test",
                "tenant",
                &Envelope::encode(&Order { number: 2 }).unwrap(),
                &[delivery]
            )
            .await,
        Err(Error::Capacity)
    ));
    assert!(matches!(
        overflow.commit(now).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn duplicate_custody_does_not_wait_on_running_handler_row(pool: PgPool) {
    let store = Store::new(pool);
    let now = Utc::now();
    let message = Envelope::encode(&Order { number: 1 }).unwrap();
    let delivery = Delivery::new(DeliveryKind::Local, "audit", now);
    let mut scope = store.begin().await.unwrap();
    scope
        .stage("test", "tenant", &message, std::slice::from_ref(&delivery))
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    let claim = store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(60), now)
        .await
        .unwrap()
        .remove(0);
    let running = store.execution(&claim, now).await.unwrap();
    let mut duplicate = store.begin().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        duplicate.stage("test", "tenant", &message, &[delivery]),
    )
    .await
    .unwrap()
    .unwrap();
    duplicate.commit(now).await.unwrap();
    running.commit(now).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn concurrent_claims_are_disjoint(pool: PgPool) {
    let store = Store::new(pool);
    for _ in 0..4 {
        enqueue(&store, DeliveryKind::Local, "audit").await;
    }
    let now = Utc::now();
    let (first, second) = tokio::join!(
        store.claim(DeliveryKind::Local, 2, Duration::from_secs(60), now),
        store.claim(DeliveryKind::Local, 2, Duration::from_secs(60), now)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.len() + second.len(), 4);
    assert!(
        first
            .iter()
            .all(|a| second.iter().all(|b| a.id() != b.id()))
    );
}
impl Transport for Uncertain {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(TransportError::Transient)
        } else {
            Ok(())
        }
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn uncertain_publish_retries_without_changing_identity(pool: PgPool) {
    let store = Store::new(pool.clone());
    let id = enqueue(&store, DeliveryKind::Remote, "orders").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = OutboxWorker::new(
        store.clone(),
        Uncertain {
            calls: Arc::clone(&calls),
        },
        RetryPolicy::default(),
    );
    worker.run_once(10).await.unwrap();
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Ready
    );
    sqlx::query("UPDATE raddle_jobs SET available_at=now() WHERE status='ready'")
        .execute(&pool)
        .await
        .unwrap();
    worker.run_once(10).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Completed
    );
}
