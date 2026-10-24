//! Real-file SQLite transaction, restart, fencing and worker conformance.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_sqlite::{
    Delivery, DeliveryKind, Error, FailureDisposition, OutboxWorker, Store, Worker,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Message {
    value: u32,
}
impl WireMessage for Message {
    const NAME: &'static str = "test.message";
    const VERSION: u32 = 1;
}

async fn fixture() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path().join("messages.sqlite"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    (directory, store)
}
async fn enqueue(store: &Store, kind: DeliveryKind, destination: &str) -> uuid::Uuid {
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let ids = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Message { value: 42 }).unwrap(),
            &[Delivery::new(kind, destination, now)],
        )
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    ids[0]
}

#[tokio::test]
async fn atomic_application_writes_and_restart_recovery() {
    let (directory, store) = fixture().await;
    let mut setup = store.begin().await.unwrap();
    sqlx::query("CREATE TABLE business(value INTEGER)")
        .execute(setup.connection())
        .await
        .unwrap();
    setup.commit(Utc::now()).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    sqlx::query("INSERT INTO business VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    let id = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Message { value: 1 }).unwrap(),
            &[Delivery::new(DeliveryKind::Local, "audit", Utc::now())],
        )
        .await
        .unwrap()[0];
    scope.rollback().await.unwrap();
    assert!(store.inspect("tenant", id).await.unwrap().is_none());
    let mut scope = store.begin().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
            .fetch_one(scope.connection())
            .await
            .unwrap(),
        0
    );
    scope.rollback().await.unwrap();
    let id = enqueue(&store, DeliveryKind::Local, "audit").await;
    store.close().await;
    let reopened = Store::open(directory.path().join("messages.sqlite"))
        .await
        .unwrap();
    reopened.migrate().await.unwrap();
    assert_eq!(
        reopened
            .inspect("tenant", id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "ready"
    );
    reopened.close().await;
}

#[tokio::test]
async fn duplicate_content_is_idempotent_but_conflicts_rollback_all_effects() {
    let (_directory, store) = fixture().await;
    let now = Utc::now();
    let envelope = Envelope::encode(&Message { value: 1 }).unwrap();
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
    let mut scope = store.begin().await.unwrap();
    let changed =
        Envelope::with_identity(envelope.id(), envelope.created_at(), &Message { value: 2 })
            .unwrap();
    assert!(matches!(
        scope
            .stage(
                "test",
                "tenant",
                &changed,
                &[Delivery::new(DeliveryKind::Local, "other", now), delivery]
            )
            .await,
        Err(Error::IdentityConflict)
    ));
    assert!(matches!(scope.commit(now).await, Err(Error::RollbackOnly)));
    assert_eq!(
        store
            .claim(DeliveryKind::Local, 10, Duration::from_secs(60), now)
            .await
            .unwrap()
            .len(),
        1
    );
    store.close().await;
}

#[tokio::test]
async fn claims_are_fenced_and_dead_jobs_have_audited_replay() {
    let (_directory, store) = fixture().await;
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
        store.complete(&first, now).await,
        Err(Error::StaleClaim)
    ));
    store
        .fail(&second, FailureDisposition::Reject, now)
        .await
        .unwrap();
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "dead"
    );
    assert!(
        !store
            .replay("other", id, "operator", "fixed", now)
            .await
            .unwrap()
    );
    assert!(
        store
            .replay("tenant", id, "operator", "fixed", now)
            .await
            .unwrap()
    );
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "ready"
    );
    store.close().await;
}

#[tokio::test]
async fn typed_worker_commits_completion_and_releases_capacity() {
    let (_directory, store) = fixture().await;
    store.set_capacity("tenant", 1).await.unwrap();
    let id = enqueue(&store, DeliveryKind::Local, "audit").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut worker = Worker::try_new(store.clone(), Duration::from_secs(5)).unwrap();
    worker
        .handle::<Message, _>("audit", Codecs::default(), move |message, _| {
            observed.fetch_add(message.value as usize, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        })
        .unwrap();
    assert!(worker.run_once().await.unwrap());
    assert!(!worker.run_once().await.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 42);
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "completed"
    );
    enqueue(&store, DeliveryKind::Local, "audit").await;
    store.close().await;
}

struct Flaky {
    calls: Arc<AtomicUsize>,
}
impl Transport for Flaky {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(TransportError::Transient)
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn confirmed_outbox_retries_uncertain_sends() {
    let (_directory, store) = fixture().await;
    let id = enqueue(&store, DeliveryKind::Remote, "out").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = OutboxWorker::try_new(
        store.clone(),
        Flaky {
            calls: Arc::clone(&calls),
        },
        Duration::from_secs(5),
    )
    .unwrap();
    worker.run_once().await.unwrap();
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "ready"
    );
    let mut scope = store.begin().await.unwrap();
    sqlx::query("UPDATE raddle_jobs SET available_at=0 WHERE status='ready'")
        .execute(scope.connection())
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    worker.run_once().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "completed"
    );
    store.close().await;
}

#[tokio::test]
async fn separate_pools_cannot_claim_the_same_delivery() {
    let (directory, first) = fixture().await;
    let second = Store::open(directory.path().join("messages.sqlite"))
        .await
        .unwrap();
    for _ in 0..4 {
        enqueue(&first, DeliveryKind::Local, "audit").await;
    }
    let now = Utc::now();
    let (a, b) = tokio::join!(
        first.claim(DeliveryKind::Local, 2, Duration::from_secs(60), now),
        second.claim(DeliveryKind::Local, 2, Duration::from_secs(60), now)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.len() + b.len(), 4);
    assert!(
        a.iter()
            .all(|a| b.iter().all(|b| a.id().unwrap() != b.id().unwrap()))
    );
    second.close().await;
    first.close().await;
}

#[tokio::test]
async fn expiry_attempt_exhaustion_and_keyed_scheduling() {
    let (_directory, store) = fixture().await;
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let mut expired = Delivery::new(DeliveryKind::Local, "audit", now);
    expired.expires_at = Some(now);
    let expired_id = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Message { value: 0 }).unwrap(),
            &[expired],
        )
        .await
        .unwrap()[0];
    for value in 1..=2 {
        let mut delivery = Delivery::new(DeliveryKind::Local, "audit", now);
        delivery.partition_key = Some("order".into());
        delivery.max_attempts = 1;
        if value == 2 {
            delivery.available_at = now + TimeDelta::seconds(10);
        }
        scope
            .stage(
                "test",
                "tenant",
                &Envelope::encode(&Message { value }).unwrap(),
                &[delivery],
            )
            .await
            .unwrap();
    }
    scope.commit(now).await.unwrap();
    let claims = store
        .claim(DeliveryKind::Local, 10, Duration::from_secs(1), now)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims[0]
            .envelope()
            .unwrap()
            .decode::<Message>()
            .unwrap()
            .value,
        1
    );
    assert_eq!(
        store
            .inspect("tenant", expired_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "dead"
    );
    assert!(
        !store
            .replay("tenant", expired_id, "operator", "retry", now)
            .await
            .unwrap()
    );
    assert!(
        store
            .claim(
                DeliveryKind::Local,
                10,
                Duration::from_secs(1),
                now + TimeDelta::seconds(2)
            )
            .await
            .unwrap()
            .is_empty()
    );
    let claims = store
        .claim(
            DeliveryKind::Local,
            10,
            Duration::from_secs(5),
            now + TimeDelta::seconds(11),
        )
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims[0]
            .envelope()
            .unwrap()
            .decode::<Message>()
            .unwrap()
            .value,
        2
    );
    store.close().await;
}

#[tokio::test]
async fn capacity_refusal_rolls_back_fanout_and_retention_is_tenant_scoped() {
    let (_directory, store) = fixture().await;
    store.set_capacity("tenant", 1).await.unwrap();
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        scope
            .stage(
                "test",
                "tenant",
                &Envelope::encode(&Message { value: 1 }).unwrap(),
                &[
                    Delivery::new(DeliveryKind::Local, "one", now),
                    Delivery::new(DeliveryKind::Local, "two", now),
                ]
            )
            .await,
        Err(Error::Capacity)
    ));
    assert!(matches!(scope.commit(now).await, Err(Error::RollbackOnly)));
    assert!(
        store
            .claim(DeliveryKind::Local, 10, Duration::from_secs(5), now)
            .await
            .unwrap()
            .is_empty()
    );
    let id = enqueue(&store, DeliveryKind::Local, "one").await;
    let claim = store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(5), Utc::now())
        .await
        .unwrap()
        .remove(0);
    store.complete(&claim, Utc::now()).await.unwrap();
    let cutoff = Utc::now() + TimeDelta::seconds(1);
    assert_eq!(store.prune_completed("other", cutoff, 1).await.unwrap(), 0);
    assert_eq!(store.prune_completed("tenant", cutoff, 1).await.unwrap(), 1);
    assert!(store.inspect("tenant", id).await.unwrap().is_none());
    store.close().await;
}

#[tokio::test]
async fn cancelled_worker_rolls_back_business_writes_and_recovers_its_claim() {
    let (_directory, store) = fixture().await;
    let mut scope = store.begin().await.unwrap();
    sqlx::query("CREATE TABLE business(value INTEGER)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let id = enqueue(&store, DeliveryKind::Local, "audit").await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = Arc::clone(&entered);
    let mut worker = Worker::try_new(store.clone(), Duration::from_secs(30)).unwrap();
    worker
        .handle::<Message, _>("audit", Codecs::default(), move |_, scope| {
            let signal = Arc::clone(&signal);
            Box::pin(async move {
                assert_eq!(scope.tenant(), Some("tenant"));
                sqlx::query("INSERT INTO business VALUES(1)")
                    .execute(scope.connection())
                    .await
                    .unwrap();
                signal.notify_one();
                std::future::pending().await
            })
        })
        .unwrap();
    let task = tokio::spawn(async move { worker.run_once().await });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let mut scope = store.begin().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
            .fetch_one(scope.connection())
            .await
            .unwrap(),
        0
    );
    scope.rollback().await.unwrap();
    let claims = store
        .claim(
            DeliveryKind::Local,
            1,
            Duration::from_secs(5),
            Utc::now() + TimeDelta::seconds(36),
        )
        .await
        .unwrap();
    assert_eq!(claims[0].id().unwrap(), id);
    store.close().await;
}

#[tokio::test]
async fn replay_cannot_bypass_the_pending_capacity() {
    let (_directory, store) = fixture().await;
    store.set_capacity("tenant", 1).await.unwrap();
    let dead = enqueue(&store, DeliveryKind::Local, "audit").await;
    let claim = store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(60), Utc::now())
        .await
        .unwrap()
        .remove(0);
    store
        .fail(&claim, FailureDisposition::Reject, Utc::now())
        .await
        .unwrap();
    enqueue(&store, DeliveryKind::Local, "audit").await;
    assert!(matches!(
        store
            .replay("tenant", dead, "operator", "retry", Utc::now())
            .await,
        Err(Error::Capacity)
    ));
    assert_eq!(
        store.inspect("tenant", dead).await.unwrap().unwrap().status,
        "dead"
    );
    store.close().await;
}
