//! Failed-only retries and ownership loss through the shipping SQLite pollers.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_sqlite::{
    Claim, Delivery, DeliveryKind, Error, FailureDisposition, OutboxWorker, Scope, Store, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteOperation;

#[derive(Serialize, Deserialize)]
struct Message {
    value: u32,
}

impl WireMessage for Message {
    const NAME: &'static str = "test.sqlite.poll";
    const VERSION: u32 = 1;
}

#[derive(Serialize, Deserialize)]
struct Unknown;

impl WireMessage for Unknown {
    const NAME: &'static str = "test.sqlite.unregistered";
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

async fn enqueue(
    store: &Store,
    value: u32,
    kind: DeliveryKind,
    destination: &str,
) -> (uuid::Uuid, Envelope, Delivery) {
    let now = Utc::now();
    let envelope = Envelope::encode(&Message { value }).unwrap();
    let delivery = Delivery::new(kind, destination, now);
    let mut scope = store.begin().await.unwrap();
    let id = scope
        .stage("test", "tenant", &envelope, std::slice::from_ref(&delivery))
        .await
        .unwrap()[0];
    scope.commit(now).await.unwrap();
    (id, envelope, delivery)
}

#[tokio::test]
async fn failed_first_middle_last_replays_only_failed_work_and_retains_new_successes() {
    for failed in 0..3 {
        let (_directory, store) = fixture().await;
        let mut setup = store.begin().await.unwrap();
        sqlx::query("CREATE TABLE business(value INTEGER PRIMARY KEY)")
            .execute(setup.connection())
            .await
            .unwrap();
        setup.commit(Utc::now()).await.unwrap();
        let mut jobs = Vec::new();
        for value in 0..3 {
            jobs.push(enqueue(&store, value, DeliveryKind::Local, "owned").await);
        }
        let corrected = Arc::new(AtomicBool::new(false));
        let allowed = Arc::clone(&corrected);
        let mut worker = Worker::try_new(store.clone(), Duration::from_secs(2)).unwrap();
        worker
            .handle::<Message, _>("owned", Codecs::default(), move |message, scope| {
                let reject = message.value == failed && !allowed.load(Ordering::SeqCst);
                Box::pin(async move {
                    if reject {
                        return Err(FailureDisposition::Reject);
                    }
                    sqlx::query("INSERT INTO business VALUES(?)")
                        .bind(message.value)
                        .execute(scope.connection())
                        .await
                        .map_err(|_| FailureDisposition::Reject)?;
                    Ok(())
                })
            })
            .unwrap();
        for _ in 0..3 {
            assert!(worker.run_once().await.unwrap());
        }
        for (value, (id, _, _)) in jobs.iter().enumerate() {
            let job = store.inspect("tenant", *id).await.unwrap().unwrap();
            assert_eq!(
                job.status,
                if value as u32 == failed {
                    "dead"
                } else {
                    "completed"
                }
            );
            assert_eq!(job.attempts, 1);
        }
        jobs.push(enqueue(&store, 3, DeliveryKind::Local, "owned").await);
        assert!(worker.run_once().await.unwrap());
        assert!(!worker.run_once().await.unwrap());
        corrected.store(true, Ordering::SeqCst);
        assert!(
            !store
                .replay(
                    "other",
                    jobs[failed as usize].0,
                    "operator",
                    "corrected",
                    Utc::now()
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .replay(
                    "tenant",
                    jobs[failed as usize].0,
                    "operator",
                    "corrected",
                    Utc::now()
                )
                .await
                .unwrap()
        );
        assert!(worker.run_once().await.unwrap());
        assert!(!worker.run_once().await.unwrap());
        for (id, envelope, delivery) in &jobs {
            let mut duplicate = store.begin().await.unwrap();
            assert_eq!(
                duplicate
                    .stage("test", "tenant", envelope, std::slice::from_ref(delivery))
                    .await
                    .unwrap(),
                vec![*id]
            );
            duplicate.commit(Utc::now()).await.unwrap();
            let job = store.inspect("tenant", *id).await.unwrap().unwrap();
            assert_eq!((job.status.as_str(), job.attempts), ("completed", 1));
        }
        let mut inspect = store.begin().await.unwrap();
        let values: Vec<i64> = sqlx::query_scalar("SELECT value FROM business ORDER BY value")
            .fetch_all(inspect.connection())
            .await
            .unwrap();
        assert_eq!(values, vec![0, 1, 2, 3]);
        let audits: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_replay_audit")
            .fetch_one(inspect.connection())
            .await
            .unwrap();
        assert_eq!(audits, 1);
        inspect.rollback().await.unwrap();
        store.close().await;
    }
}

#[tokio::test]
async fn unknown_contracts_are_rejected_only_inside_owned_destinations() {
    let (_directory, store) = fixture().await;
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let envelope = Envelope::encode(&Unknown).unwrap();
    let ids = scope
        .stage(
            "test",
            "tenant",
            &envelope,
            &[
                Delivery::new(DeliveryKind::Local, "other", now),
                Delivery::new(DeliveryKind::Local, "owned", now),
            ],
        )
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    let mut worker = Worker::try_new(store.clone(), Duration::from_secs(2)).unwrap();
    worker
        .handle::<Message, _>("owned", Codecs::default(), |_, _| {
            Box::pin(async { Ok(()) })
        })
        .unwrap();
    assert!(worker.run_once().await.unwrap());
    assert_eq!(
        store
            .inspect("tenant", ids[0])
            .await
            .unwrap()
            .unwrap()
            .status,
        "ready"
    );
    assert_eq!(
        store
            .inspect("tenant", ids[1])
            .await
            .unwrap()
            .unwrap()
            .status,
        "dead"
    );
    let healthy = enqueue(&store, 7, DeliveryKind::Local, "owned").await.0;
    assert!(worker.run_once().await.unwrap());
    assert_eq!(
        store
            .inspect("tenant", healthy)
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    store.close().await;
}

struct Takeover {
    store: Store,
    failed: u32,
    acknowledged: bool,
    remaining: AtomicBool,
    successor: Mutex<Option<Claim>>,
    calls: Mutex<BTreeMap<u32, Vec<uuid::Uuid>>>,
}

impl Transport for Takeover {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        let value = envelope.decode::<Message>().unwrap().value;
        self.calls
            .lock()
            .unwrap()
            .entry(value)
            .or_default()
            .push(envelope.id());
        if value == self.failed && self.remaining.swap(false, Ordering::SeqCst) {
            let successor = self
                .store
                .claim_for(
                    DeliveryKind::Remote,
                    destination,
                    1,
                    Duration::from_secs(60),
                    Utc::now() + TimeDelta::seconds(20),
                )
                .await
                .unwrap()
                .remove(0);
            assert_eq!(successor.envelope().unwrap().id(), envelope.id());
            *self.successor.lock().unwrap() = Some(successor);
            if !self.acknowledged {
                return Err(TransportError::Transient);
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn stale_outbox_settlement_does_not_stop_neighbors_or_modify_successor_custody() {
    for acknowledged in [false, true] {
        for failed in 0..3 {
            let (_directory, store) = fixture().await;
            let mut jobs = Vec::new();
            for value in 0..3 {
                jobs.push(enqueue(&store, value, DeliveryKind::Remote, "out").await);
            }
            let transport = Takeover {
                store: store.clone(),
                failed,
                acknowledged,
                remaining: AtomicBool::new(true),
                successor: Mutex::new(None),
                calls: Mutex::new(BTreeMap::new()),
            };
            let worker =
                OutboxWorker::try_new(store.clone(), transport, Duration::from_secs(2)).unwrap();
            for _ in 0..3 {
                assert!(worker.run_once().await.unwrap());
            }
            let retained = store
                .inspect("tenant", jobs[failed as usize].0)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((retained.status.as_str(), retained.attempts), ("active", 2));
            jobs.push(enqueue(&store, 3, DeliveryKind::Remote, "out").await);
            assert!(worker.run_once().await.unwrap());
            assert!(!worker.run_once().await.unwrap());
            let transport = worker.into_transport();
            let successor = transport.successor.lock().unwrap().take().unwrap();
            // The successor publisher uses the same canonical identity and explicit
            // confirmed-publication boundary. This is not an exactly-once wire claim.
            transport
                .publish("out", &successor.envelope().unwrap())
                .await
                .unwrap();
            store
                .complete(&successor, Utc::now() + TimeDelta::seconds(20))
                .await
                .unwrap();
            for (value, (id, envelope, _)) in jobs.iter().enumerate() {
                assert_eq!(
                    store.inspect("tenant", *id).await.unwrap().unwrap().status,
                    "completed"
                );
                let calls = transport.calls.lock().unwrap();
                let observed = &calls[&(value as u32)];
                assert_eq!(observed.len(), if value as u32 == failed { 2 } else { 1 });
                assert!(observed.iter().all(|id| *id == envelope.id()));
            }
            store.close().await;
        }
    }
}

struct Release(Arc<(Mutex<bool>, Condvar)>);

impl Drop for Release {
    fn drop(&mut self) {
        let (released, wake) = &*self.0;
        *released.lock().unwrap() = true;
        wake.notify_all();
    }
}

#[tokio::test]
async fn local_claim_expiring_after_selection_remains_a_local_observed_outcome() {
    let (_directory, store) = fixture().await;
    let id = enqueue(&store, 0, DeliveryKind::Local, "owned").await.0;
    let barrier = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(Arc::clone(&barrier));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut sender = Some(sender);
    let mut setup = store.begin().await.unwrap();
    setup
        .connection()
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |event| {
            if event.table == "raddle_jobs"
                && event.operation == SqliteOperation::Update
                && let Some(sender) = sender.take()
            {
                let _ = sender.send(());
                let (released, wake) = &*barrier;
                let mut guard = released.lock().unwrap();
                while !*guard {
                    guard = wake.wait(guard).unwrap();
                }
            }
        });
    setup.commit(Utc::now()).await.unwrap();
    let mut worker = Worker::try_new(store.clone(), Duration::from_millis(1)).unwrap();
    worker
        .handle::<Message, _>("owned", Codecs::default(), |_, _| {
            Box::pin(async { Ok(()) })
        })
        .unwrap();
    let (result, ()) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(worker.run_once(), async move {
            receiver.await.unwrap();
            tokio::time::sleep(Duration::from_millis(5250)).await;
            drop(release);
        })
    })
    .await
    .unwrap();
    assert!(result.unwrap());
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "active"
    );
    // A fresh handler budget avoids conflating ownership loss with a deliberately
    // tiny one-millisecond business deadline on a loaded host.
    let mut retry = Worker::try_new(store.clone(), Duration::from_secs(2)).unwrap();
    retry
        .handle::<Message, _>("owned", Codecs::default(), |_, _| {
            Box::pin(async { Ok(()) })
        })
        .unwrap();
    assert!(retry.run_once().await.unwrap());
    let healthy = enqueue(&store, 1, DeliveryKind::Local, "owned").await.0;
    assert!(retry.run_once().await.unwrap());
    for id in [id, healthy] {
        assert_eq!(
            store.inspect("tenant", id).await.unwrap().unwrap().status,
            "completed"
        );
    }
    store.close().await;
}

struct ClosePool(Store);

impl Transport for ClosePool {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        self.0.close().await;
        Ok(())
    }
}

#[tokio::test]
async fn real_database_failures_are_not_reclassified_as_local_claim_loss() {
    let (directory, store) = fixture().await;
    let remote = enqueue(&store, 0, DeliveryKind::Remote, "out").await.0;
    let outbox = OutboxWorker::try_new(
        store.clone(),
        ClosePool(store.clone()),
        Duration::from_secs(2),
    )
    .unwrap();
    assert!(matches!(
        outbox.run_once().await,
        Err(Error::Database(sqlx::Error::PoolClosed))
    ));
    let reopened = Store::open(directory.path().join("messages.sqlite"))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .inspect("tenant", remote)
            .await
            .unwrap()
            .unwrap()
            .status,
        "active"
    );
    let mut worker = Worker::try_new(reopened.clone(), Duration::from_secs(2)).unwrap();
    worker
        .handle::<Message, _>("owned", Codecs::default(), |_, _| {
            Box::pin(async { Ok(()) })
        })
        .unwrap();
    let mut corrupt = reopened.begin().await.unwrap();
    sqlx::query("DROP TABLE raddle_jobs")
        .execute(corrupt.connection())
        .await
        .unwrap();
    corrupt.commit(Utc::now()).await.unwrap();
    assert!(matches!(worker.run_once().await, Err(Error::Database(_))));
    reopened.close().await;
}

#[tokio::test]
async fn two_scoped_pools_do_not_claim_the_same_delivery() {
    let (directory, first) = fixture().await;
    let second = Store::open(directory.path().join("messages.sqlite"))
        .await
        .unwrap();
    let now = Utc::now();
    let other = enqueue(&first, 9, DeliveryKind::Local, "other").await.0;
    let mut ids = Vec::new();
    for value in 0..4 {
        ids.push(enqueue(&first, value, DeliveryKind::Local, "owned").await.0);
    }
    let (a, b) = tokio::join!(
        first.claim_for(
            DeliveryKind::Local,
            "owned",
            2,
            Duration::from_secs(30),
            now + TimeDelta::seconds(1)
        ),
        second.claim_for(
            DeliveryKind::Local,
            "owned",
            2,
            Duration::from_secs(30),
            now + TimeDelta::seconds(1)
        )
    );
    let claimed = a
        .unwrap()
        .into_iter()
        .chain(b.unwrap())
        .map(|claim| claim.id().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(claimed, ids.into_iter().collect());
    assert_eq!(
        first
            .inspect("tenant", other)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        0
    );
    second.close().await;
    first.close().await;
}

#[derive(Clone, Copy, Debug)]
enum Swallowed {
    InvalidStage,
    CancelledStage,
    ExplicitRollback,
}

async fn cancel_partial_child_stage(scope: &mut Scope<'static>) {
    let barrier = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(Arc::clone(&barrier));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut sender = Some(sender);
    scope
        .connection()
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |event| {
            if event.table == "raddle_jobs"
                && event.operation == SqliteOperation::Insert
                && let Some(sender) = sender.take()
            {
                let _ = sender.send(());
                let (released, wake) = &*barrier;
                let mut guard = released.lock().unwrap();
                while !*guard {
                    guard = wake.wait(guard).unwrap();
                }
            }
        });
    let child = Envelope::encode(&Message { value: 99 }).unwrap();
    let now = Utc::now();
    let deliveries = [
        Delivery::new(DeliveryKind::Remote, "child-a", now),
        Delivery::new(DeliveryKind::Remote, "child-b", now),
    ];
    {
        let mut pending = Box::pin(scope.stage("test", "tenant", &child, &deliveries));
        tokio::select! {
            ready = receiver => ready.unwrap(),
            result = &mut pending => panic!("child stage completed before insert gate: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("child insert gate timed out"),
        }
    }
    drop(release);
    scope
        .connection()
        .lock_handle()
        .await
        .unwrap()
        .remove_update_hook();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE kind='remote'")
            .fetch_one(scope.connection())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn confirmed_rollback_only_commits_retry_failed_orders_without_stopping_neighbors() {
    for mode in [
        Swallowed::InvalidStage,
        Swallowed::CancelledStage,
        Swallowed::ExplicitRollback,
    ] {
        for failed in 0..3 {
            let (_directory, store) = fixture().await;
            let mut setup = store.begin().await.unwrap();
            sqlx::query("CREATE TABLE business(value INTEGER PRIMARY KEY)")
                .execute(setup.connection())
                .await
                .unwrap();
            setup.commit(Utc::now()).await.unwrap();
            let mut jobs = Vec::new();
            for value in 0..3 {
                jobs.push(enqueue(&store, value, DeliveryKind::Local, "owned").await);
            }
            let corrected = Arc::new(AtomicBool::new(false));
            let swallowed = Arc::new(AtomicUsize::new(0));
            let fixed = Arc::clone(&corrected);
            let observed = Arc::clone(&swallowed);
            let mut worker = Worker::try_new(store.clone(), Duration::from_secs(5)).unwrap();
            worker
                .handle::<Message, _>("owned", Codecs::default(), move |message, scope| {
                    let fixed = Arc::clone(&fixed);
                    let observed = Arc::clone(&observed);
                    Box::pin(async move {
                        sqlx::query("INSERT INTO business VALUES(?)")
                            .bind(message.value)
                            .execute(scope.connection())
                            .await
                            .unwrap();
                        if message.value == failed && !fixed.load(Ordering::SeqCst) {
                            match mode {
                                Swallowed::InvalidStage => {
                                    let child = Envelope::encode(&Message { value: 99 }).unwrap();
                                    let invalid =
                                        Delivery::new(DeliveryKind::Remote, "", Utc::now());
                                    assert!(matches!(
                                        scope.stage("test", "tenant", &child, &[invalid]).await,
                                        Err(Error::Invalid(_))
                                    ));
                                }
                                Swallowed::CancelledStage => {
                                    cancel_partial_child_stage(scope).await
                                }
                                Swallowed::ExplicitRollback => scope.mark_rollback_only(),
                            }
                            // Prove the handler actually returned success after the
                            // poison operation, rather than timing out beforehand.
                            observed.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(())
                    })
                })
                .unwrap();
            for position in 0..3 {
                assert!(
                    worker.run_once().await.unwrap(),
                    "{mode:?}, position {position}"
                );
                if position == failed {
                    let id = jobs[failed as usize].0;
                    let job = store.inspect("tenant", id).await.unwrap().unwrap();
                    assert_eq!((job.status.as_str(), job.attempts), ("ready", 1));
                    let mut inspect = store.begin().await.unwrap();
                    let due: i64 =
                        sqlx::query_scalar("SELECT available_at FROM raddle_jobs WHERE id=?")
                            .bind(id.to_string())
                            .fetch_one(inspect.connection())
                            .await
                            .unwrap();
                    assert!(due > jobs[failed as usize].2.available_at.timestamp_millis());
                    // Hold this already-recorded retry outside the wall-clock
                    // duration of the neighbor phase, then advance it once fixed.
                    assert_eq!(
                        sqlx::query(
                            "UPDATE raddle_jobs SET available_at=? WHERE id=? AND status='ready'"
                        )
                        .bind((Utc::now() + TimeDelta::hours(1)).timestamp_millis())
                        .bind(id.to_string())
                        .execute(inspect.connection())
                        .await
                        .unwrap()
                        .rows_affected(),
                        1
                    );
                    inspect.commit(Utc::now()).await.unwrap();
                }
            }
            assert_eq!(swallowed.load(Ordering::SeqCst), 1);
            jobs.push(enqueue(&store, 3, DeliveryKind::Local, "owned").await);
            assert!(worker.run_once().await.unwrap());
            assert!(!worker.run_once().await.unwrap());
            let mut inspect = store.begin().await.unwrap();
            let values: Vec<i64> = sqlx::query_scalar("SELECT value FROM business ORDER BY value")
                .fetch_all(inspect.connection())
                .await
                .unwrap();
            assert_eq!(
                values,
                (0..4)
                    .filter(|value| *value != i64::from(failed))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM raddle_jobs WHERE kind='remote'"
                )
                .fetch_one(inspect.connection())
                .await
                .unwrap(),
                0
            );
            inspect.rollback().await.unwrap();
            corrected.store(true, Ordering::SeqCst);
            let mut advance = store.begin().await.unwrap();
            assert_eq!(
                sqlx::query("UPDATE raddle_jobs SET available_at=0 WHERE id=? AND status='ready'")
                    .bind(jobs[failed as usize].0.to_string())
                    .execute(advance.connection())
                    .await
                    .unwrap()
                    .rows_affected(),
                1
            );
            advance.commit(Utc::now()).await.unwrap();
            assert!(worker.run_once().await.unwrap());
            assert!(!worker.run_once().await.unwrap());
            for (value, (id, _, _)) in jobs.iter().enumerate() {
                let job = store.inspect("tenant", *id).await.unwrap().unwrap();
                assert_eq!(job.status, "completed");
                assert_eq!(job.attempts, if value as u32 == failed { 2 } else { 1 });
            }
            let mut inspect = store.begin().await.unwrap();
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
                    .fetch_one(inspect.connection())
                    .await
                    .unwrap(),
                4
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_replay_audit")
                    .fetch_one(inspect.connection())
                    .await
                    .unwrap(),
                0
            );
            inspect.rollback().await.unwrap();
            store.close().await;
        }
    }
}

#[tokio::test]
async fn commit_database_errors_are_not_reclassified_as_confirmed_rollback() {
    let (_directory, store) = fixture().await;
    let mut setup = store.begin().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(setup.connection())
            .await
            .unwrap(),
        1
    );
    sqlx::query("CREATE TABLE parents(id INTEGER PRIMARY KEY)")
        .execute(setup.connection())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE children(parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)").execute(setup.connection()).await.unwrap();
    setup.commit(Utc::now()).await.unwrap();
    let id = enqueue(&store, 404, DeliveryKind::Local, "owned").await.0;
    let mut worker = Worker::try_new(store.clone(), Duration::from_secs(5)).unwrap();
    worker
        .handle::<Message, _>("owned", Codecs::default(), |message, scope| {
            Box::pin(async move {
                sqlx::query("INSERT INTO children VALUES(?)")
                    .bind(message.value)
                    .execute(scope.connection())
                    .await
                    .unwrap();
                Ok(())
            })
        })
        .unwrap();
    match worker.run_once().await {
        Err(Error::Database(sqlx::Error::Database(error))) => {
            assert_eq!(error.code().as_deref(), Some("787"))
        }
        other => panic!("expected deferred commit error, got {other:?}"),
    }
    let job = store.inspect("tenant", id).await.unwrap().unwrap();
    assert_eq!((job.status.as_str(), job.attempts), ("active", 1));
    let mut inspect = store.begin().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM children")
            .fetch_one(inspect.connection())
            .await
            .unwrap(),
        0
    );
    inspect.rollback().await.unwrap();
    store.close().await;
}
