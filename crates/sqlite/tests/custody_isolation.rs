//! Real-file cancellation, destination admission and live-owner settlement.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_sqlite::{Delivery, DeliveryKind, Error, FailureDisposition, Store, Worker};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteOperation;

#[derive(Serialize, Deserialize)]
struct Message {
    value: u32,
}

impl WireMessage for Message {
    const NAME: &'static str = "test.sqlite.custody";
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

async fn stage(store: &Store, delivery: Delivery) -> uuid::Uuid {
    let now = delivery.available_at;
    let mut scope = store.begin().await.unwrap();
    let envelope = Envelope::encode(&Message { value: 7 }).unwrap();
    let id = scope
        .stage("test", "tenant", &envelope, &[delivery])
        .await
        .unwrap()[0];
    scope.commit(now).await.unwrap();
    id
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
async fn cancelled_borrowed_stage_rolls_back_partial_fanout_and_business() {
    let (_directory, store) = fixture().await;
    let now = Utc::now();
    let mut setup = store.begin().await.unwrap();
    sqlx::query("CREATE TABLE business(value INTEGER)")
        .execute(setup.connection())
        .await
        .unwrap();
    setup.commit(now).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    sqlx::query("INSERT INTO business VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();

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
    let envelope = Envelope::encode(&Message { value: 9 }).unwrap();
    let deliveries = [
        Delivery::new(DeliveryKind::Local, "first", now),
        Delivery::new(DeliveryKind::Local, "second", now),
    ];
    {
        let mut pending = Box::pin(scope.stage("test", "tenant", &envelope, &deliveries));
        tokio::select! {
            ready = receiver => ready.unwrap(),
            result = &mut pending => panic!("stage completed before its first insert gate: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("staging gate timed out"),
        }
        // Cancel just this borrowed operation, not the retained Scope owner.
    }
    drop(release);
    scope
        .connection()
        .lock_handle()
        .await
        .unwrap()
        .remove_update_hook();
    let partial: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_jobs")
        .fetch_one(scope.connection())
        .await
        .unwrap();
    assert_eq!(
        partial, 1,
        "the cancelled operation must have partly staged"
    );
    assert!(matches!(scope.commit(now).await, Err(Error::RollbackOnly)));
    let mut inspect = store.begin().await.unwrap();
    for table in ["raddle_jobs", "business"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(inspect.connection())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    inspect.rollback().await.unwrap();
    store.close().await;
}

#[tokio::test]
async fn workers_leave_unregistered_destinations_and_their_sweeps_untouched() {
    for registered in [false, true] {
        let (_directory, store) = fixture().await;
        let now = Utc::now();
        let unrelated = stage(&store, Delivery::new(DeliveryKind::Local, "other", now)).await;
        let mut expired = Delivery::new(DeliveryKind::Local, "other", now);
        expired.expires_at = Some(now);
        let expired = stage(&store, expired).await;
        let exhausted = stage(&store, Delivery::new(DeliveryKind::Local, "other", now)).await;
        let mut setup = store.begin().await.unwrap();
        sqlx::query("UPDATE raddle_jobs SET attempts=max_attempts WHERE id=?")
            .bind(exhausted.to_string())
            .execute(setup.connection())
            .await
            .unwrap();
        setup.commit(now).await.unwrap();
        let owned = stage(&store, Delivery::new(DeliveryKind::Local, "owned", now)).await;
        let mut worker = Worker::try_new(store.clone(), Duration::from_secs(2)).unwrap();
        if registered {
            worker
                .handle::<Message, _>("owned", Codecs::default(), |_, _| {
                    Box::pin(async { Ok(()) })
                })
                .unwrap();
        }
        assert_eq!(worker.run_once().await.unwrap(), registered);
        for id in [unrelated, expired, exhausted] {
            assert_eq!(
                store.inspect("tenant", id).await.unwrap().unwrap().status,
                "ready"
            );
        }
        assert_eq!(
            store
                .inspect("tenant", unrelated)
                .await
                .unwrap()
                .unwrap()
                .attempts,
            0
        );
        assert_eq!(
            store
                .inspect("tenant", expired)
                .await
                .unwrap()
                .unwrap()
                .attempts,
            0
        );
        assert_eq!(
            store
                .inspect("tenant", exhausted)
                .await
                .unwrap()
                .unwrap()
                .attempts,
            5
        );
        assert_eq!(
            store
                .inspect("tenant", owned)
                .await
                .unwrap()
                .unwrap()
                .status,
            if registered { "completed" } else { "ready" }
        );
        assert!(!worker.run_once().await.unwrap());
        store.close().await;
    }
}

#[tokio::test]
async fn scoped_claim_retains_legacy_claim_and_scopes_expiry_and_partition_order() {
    let (_directory, store) = fixture().await;
    let now = Utc::now();
    let other = stage(&store, Delivery::new(DeliveryKind::Local, "other", now)).await;
    let mut expired = Delivery::new(DeliveryKind::Local, "owned", now);
    expired.expires_at = Some(now);
    let expired = stage(&store, expired).await;
    let mut keyed = Delivery::new(DeliveryKind::Local, "owned", now);
    keyed.partition_key = Some("order".into());
    let first = stage(&store, keyed.clone()).await;
    let second = stage(&store, keyed).await;
    let claims = store
        .claim_for(
            DeliveryKind::Local,
            "owned",
            16,
            Duration::from_secs(30),
            now,
        )
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].id().unwrap(), first);
    assert_eq!(
        store
            .inspect("tenant", expired)
            .await
            .unwrap()
            .unwrap()
            .status,
        "dead"
    );
    assert_eq!(
        store
            .inspect("tenant", other)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        0
    );
    store.complete(&claims[0], now).await.unwrap();
    let next = store
        .claim_for(
            DeliveryKind::Local,
            "owned",
            16,
            Duration::from_secs(30),
            now,
        )
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].id().unwrap(), second);
    let legacy = store
        .claim(DeliveryKind::Local, 16, Duration::from_secs(30), now)
        .await
        .unwrap();
    assert_eq!(legacy.len(), 1);
    assert_eq!(legacy[0].id().unwrap(), other);
    assert!(matches!(
        store
            .claim_for(DeliveryKind::Local, "", 1, Duration::from_secs(1), now)
            .await,
        Err(Error::Invalid(_))
    ));
    store.close().await;
}

#[tokio::test]
async fn expired_or_superseded_owners_cannot_retry_reject_or_complete() {
    for disposition in [FailureDisposition::Retry, FailureDisposition::Reject] {
        for offset in [0, 1000] {
            let (_directory, store) = fixture().await;
            let now = Utc::now();
            let id = stage(&store, Delivery::new(DeliveryKind::Local, "owned", now)).await;
            let claim = store
                .claim_for(DeliveryKind::Local, "owned", 1, Duration::from_secs(1), now)
                .await
                .unwrap()
                .remove(0);
            let mut inspect = store.begin().await.unwrap();
            let until: i64 = sqlx::query_scalar("SELECT lease_until FROM raddle_jobs WHERE id=?")
                .bind(id.to_string())
                .fetch_one(inspect.connection())
                .await
                .unwrap();
            inspect.rollback().await.unwrap();
            let expired =
                DateTime::from_timestamp_millis(until).unwrap() + TimeDelta::milliseconds(offset);
            assert!(matches!(
                store.fail(&claim, disposition, expired).await,
                Err(Error::StaleClaim)
            ));
            assert!(matches!(
                store.complete(&claim, expired).await,
                Err(Error::StaleClaim)
            ));
            assert_eq!(
                store.inspect("tenant", id).await.unwrap().unwrap().status,
                "active"
            );
            let successor = store
                .claim_for(
                    DeliveryKind::Local,
                    "owned",
                    1,
                    Duration::from_secs(30),
                    expired,
                )
                .await
                .unwrap()
                .remove(0);
            assert!(matches!(
                store.fail(&claim, disposition, expired).await,
                Err(Error::StaleClaim)
            ));
            assert!(matches!(
                store.complete(&claim, expired).await,
                Err(Error::StaleClaim)
            ));
            assert_eq!(
                store.inspect("tenant", id).await.unwrap().unwrap().attempts,
                2
            );
            store.complete(&successor, expired).await.unwrap();
            assert_eq!(
                store.inspect("tenant", id).await.unwrap().unwrap().status,
                "completed"
            );
            store.close().await;
        }
    }
}

#[tokio::test]
async fn local_completion_at_expiry_rolls_back_business_with_its_claim() {
    let (_directory, store) = fixture().await;
    let now = Utc::now();
    let id = stage(&store, Delivery::new(DeliveryKind::Local, "owned", now)).await;
    let claim = store
        .claim(DeliveryKind::Local, 1, Duration::from_secs(1), now)
        .await
        .unwrap()
        .remove(0);
    let mut scope = store.execution(&claim, now).await.unwrap();
    let until: i64 = sqlx::query_scalar("SELECT lease_until FROM raddle_jobs WHERE id=?")
        .bind(id.to_string())
        .fetch_one(scope.connection())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE late_business(value INTEGER)")
        .execute(scope.connection())
        .await
        .unwrap();
    sqlx::query("INSERT INTO late_business VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    assert!(matches!(
        scope
            .commit(DateTime::from_timestamp_millis(until).unwrap())
            .await,
        Err(Error::StaleClaim)
    ));
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "active"
    );
    let mut inspect = store.begin().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_master WHERE name='late_business'"
        )
        .fetch_one(inspect.connection())
        .await
        .unwrap(),
        0
    );
    inspect.rollback().await.unwrap();
    store.close().await;
}

#[tokio::test]
async fn writer_waits_advance_the_callers_logical_clock_before_admission_and_settlement() {
    for operation in ["retry", "reject", "complete", "execution"] {
        let (directory, store) = fixture().await;
        let second = Store::open(directory.path().join("messages.sqlite"))
            .await
            .unwrap();
        let now = Utc::now() + TimeDelta::days(1);
        let id = stage(&store, Delivery::new(DeliveryKind::Local, "owned", now)).await;
        let claim = store
            .claim(DeliveryKind::Local, 1, Duration::from_millis(40), now)
            .await
            .unwrap()
            .remove(0);
        let blocker = second.begin().await.unwrap();
        let action = async {
            match operation {
                "retry" => store.fail(&claim, FailureDisposition::Retry, now).await,
                "reject" => store.fail(&claim, FailureDisposition::Reject, now).await,
                "complete" => store.complete(&claim, now).await,
                "execution" => match store.execution(&claim, now).await {
                    Ok(scope) => scope.rollback().await,
                    Err(error) => Err(error),
                },
                _ => unreachable!(),
            }
        };
        let (result, ()) = tokio::join!(action, async move {
            tokio::time::sleep(Duration::from_millis(160)).await;
            blocker.rollback().await.unwrap();
        });
        assert!(
            matches!(result, Err(Error::StaleClaim)),
            "{operation}: {result:?}"
        );
        assert_eq!(
            store.inspect("tenant", id).await.unwrap().unwrap().status,
            "active"
        );
        second.close().await;
        store.close().await;
    }
}

#[tokio::test]
async fn a_long_writer_wait_does_not_issue_an_already_expired_new_lease() {
    let (directory, store) = fixture().await;
    let second = Store::open(directory.path().join("messages.sqlite"))
        .await
        .unwrap();
    let now = Utc::now() + TimeDelta::days(1);
    let id = stage(&store, Delivery::new(DeliveryKind::Local, "owned", now)).await;
    let blocker = second.begin().await.unwrap();
    let (claims, ()) = tokio::join!(
        store.claim_for(
            DeliveryKind::Local,
            "owned",
            1,
            Duration::from_millis(80),
            now
        ),
        async move {
            tokio::time::sleep(Duration::from_millis(160)).await;
            blocker.rollback().await.unwrap();
        }
    );
    let claim = claims.unwrap().remove(0);
    assert_eq!(claim.id().unwrap(), id);
    // Read the actual resulting lease rather than inferring it from wall time.
    let mut inspect = store.begin().await.unwrap();
    let until: i64 = sqlx::query_scalar("SELECT lease_until FROM raddle_jobs WHERE id=?")
        .bind(id.to_string())
        .fetch_one(inspect.connection())
        .await
        .unwrap();
    inspect.rollback().await.unwrap();
    assert!(until >= now.timestamp_millis() + 200);
    second.close().await;
    store.close().await;
}

#[tokio::test]
async fn terminal_rejection_accepts_the_full_supported_datetime_range() {
    let (_directory, store) = fixture().await;
    let now = DateTime::<Utc>::MAX_UTC - TimeDelta::seconds(1);
    let id = stage(&store, Delivery::new(DeliveryKind::Local, "owned", now)).await;
    let claim = store
        .claim(DeliveryKind::Local, 1, Duration::from_millis(900), now)
        .await
        .unwrap()
        .remove(0);
    store
        .fail(
            &claim,
            FailureDisposition::Reject,
            now + TimeDelta::milliseconds(800),
        )
        .await
        .unwrap();
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "dead"
    );
    store.close().await;
}
