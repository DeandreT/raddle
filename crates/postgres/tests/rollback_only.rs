//! A handler reporting success cannot commit a poisoned scope or stop its neighbors.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use chrono::Utc;
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_postgres::{
    Delivery, DeliveryKind, Error, JobStatus, RetryPolicy, Scope, Store, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

const GATE: i64 = 58001;

#[derive(Serialize, Deserialize)]
struct Work(i32);
impl WireMessage for Work {
    const NAME: &'static str = "test.rollback-only";
    const VERSION: u32 = 1;
}

#[derive(Clone, Copy, Debug)]
enum Poison {
    Invalid,
    CancelIdempotent,
    CancelBatch,
    LostConnection,
}

struct Handler {
    pool: PgPool,
    poison: Poison,
    victim: i32,
    corrected: AtomicBool,
    calls: Mutex<BTreeMap<i32, usize>>,
    partial: AtomicUsize,
}
impl Handler {
    fn new(pool: &PgPool, poison: Poison, victim: i32) -> Self {
        Self {
            pool: pool.clone(),
            poison,
            victim,
            corrected: AtomicBool::new(false),
            calls: Mutex::new(BTreeMap::new()),
            partial: AtomicUsize::new(0),
        }
    }
    async fn apply(&self, work: Work, scope: &mut Scope<'static>) {
        *self.calls.lock().unwrap().entry(work.0).or_default() += 1;
        sqlx::query("INSERT INTO business(id) VALUES($1)")
            .bind(work.0)
            .execute(scope.connection())
            .await
            .unwrap();
        scope
            .stage(
                "source",
                "tenant",
                &Envelope::encode(&work).unwrap(),
                &[remote("direct")],
            )
            .await
            .unwrap();
        if work.0 != self.victim || self.corrected.load(Ordering::SeqCst) {
            return;
        }
        match self.poison {
            Poison::CancelIdempotent | Poison::CancelBatch => self.cancel_stage(scope, &work).await,
            Poison::Invalid | Poison::LostConnection => {
                assert!(matches!(
                    scope
                        .stage("", "tenant", &Envelope::encode(&work).unwrap(), &[])
                        .await,
                    Err(Error::Invalid(_))
                ));
                if matches!(self.poison, Poison::LostConnection) {
                    // Only this owned test execution connection is terminated.
                    let _ = sqlx::query("SELECT pg_terminate_backend(pg_backend_pid())")
                        .execute(scope.connection())
                        .await;
                }
            }
        }
        // Deliberately swallow the inner failure, not the worker's outer deadline.
    }
    async fn cancel_stage(&self, scope: &mut Scope<'static>, work: &Work) {
        let mut blocker = self.pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(GATE)
            .execute(&mut *blocker)
            .await
            .unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(scope.connection())
            .await
            .unwrap();
        let envelope = Envelope::encode(&Work(work.0 + 1000)).unwrap();
        let deliveries = [remote("partial-child"), remote("blocked-child")];
        let key = work.0.to_string();
        let mut operation = Box::pin(async {
            match self.poison {
                Poison::CancelIdempotent => scope
                    .stage_idempotent("source", "tenant", &key, &envelope, &deliveries)
                    .await
                    .map(|_| ()),
                _ => scope
                    .stage("source", "tenant", &envelope, &deliveries)
                    .await
                    .map(|_| ()),
            }
        });
        tokio::select! {
            result = &mut operation => panic!("stage did not wait at the test gate: {result:?}"),
            () = wait_for_lock(&self.pool, pid) => {},
        }
        drop(operation);
        blocker.rollback().await.unwrap();
        // Querying the transaction drains SQLx's in-flight statement. Prove real
        // partial work, not a never-polled future or an outer handler timeout.
        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM raddle_jobs WHERE message_id=$1 AND destination='partial-child'",
        )
        .bind(envelope.id())
        .fetch_one(scope.connection())
        .await
        .unwrap();
        assert_eq!(rows, 1);
        if matches!(self.poison, Poison::CancelIdempotent) {
            let keys: i64 =
                sqlx::query_scalar("SELECT count(*) FROM raddle_idempotency WHERE key=$1")
                    .bind(key)
                    .fetch_one(scope.connection())
                    .await
                    .unwrap();
            assert_eq!(keys, 1);
        }
        self.partial.fetch_add(1, Ordering::SeqCst);
    }
    fn worker(self: &Arc<Self>, store: &Store, destination: &str) -> Worker {
        let mut worker = Worker::new(store.clone(), retry());
        let handler = Arc::clone(self);
        worker
            .handle::<Work, _>(destination, Codecs::default(), move |work, scope| {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    handler.apply(work, scope).await;
                    Ok(())
                })
            })
            .unwrap();
        worker
    }
}

async fn wait_for_lock(pool: &PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND datname=current_database() AND wait_event_type='Lock')").bind(pid).fetch_one(pool).await.unwrap();
            if waiting { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
}
fn remote(destination: &str) -> Delivery {
    Delivery::new(DeliveryKind::Remote, destination, Utc::now())
}
fn retry() -> RetryPolicy {
    RetryPolicy::try_new(
        Duration::from_millis(1),
        Duration::from_millis(1),
        Duration::from_secs(30),
    )
    .unwrap()
}
async fn setup(pool: &PgPool) -> Store {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    sqlx::raw_sql("CREATE TABLE business(id integer PRIMARY KEY); CREATE FUNCTION block_child() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.destination='blocked-child' THEN PERFORM pg_advisory_xact_lock(58001); END IF; RETURN NEW; END $$; CREATE TRIGGER block_child BEFORE INSERT ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION block_child();").execute(pool).await.unwrap();
    store
}
async fn stage(store: &Store, destination: &str, value: i32) -> Uuid {
    let mut scope = store.begin().await.unwrap();
    let mut delivery = Delivery::new(DeliveryKind::Local, destination, Utc::now());
    delivery.max_attempts = 2;
    let id = scope
        .stage(
            "source",
            "tenant",
            &Envelope::encode(&Work(value)).unwrap(),
            &[delivery],
        )
        .await
        .unwrap()[0];
    scope.commit(Utc::now()).await.unwrap();
    id
}
async fn status(store: &Store, id: Uuid) -> JobStatus {
    store.inspect("tenant", id).await.unwrap().unwrap().status
}
async fn effects(pool: &PgPool, expected: &[i32]) {
    let business: Vec<i32> = sqlx::query_scalar("SELECT id FROM business ORDER BY id")
        .fetch_all(pool)
        .await
        .unwrap();
    assert_eq!(business, expected);
    let direct: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raddle_jobs WHERE destination='direct'")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(direct, expected.len() as i64);
    let partial: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raddle_jobs WHERE destination IN ('partial-child','blocked-child')",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_idempotency")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!((partial, keys), (0, 0));
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn poisoned_success_and_cancelled_inner_staging_retry_only_failed_first_middle_last(
    pool: PgPool,
) {
    let store = setup(&pool).await;
    let mut expected = Vec::new();
    for (mode, poison) in [
        Poison::Invalid,
        Poison::CancelIdempotent,
        Poison::CancelBatch,
    ]
    .into_iter()
    .enumerate()
    {
        for position in 0..3 {
            let base = (mode * 100 + position * 10) as i32;
            let name = format!("rollback-{base}");
            let handler = Arc::new(Handler::new(&pool, poison, base + position as i32));
            let worker = handler.worker(&store, &name);
            let mut ids = Vec::new();
            for index in 0..3 {
                ids.push(stage(&store, &name, base + index).await);
            }
            assert_eq!(worker.run_once(3).await.unwrap(), 3);
            for (index, id) in ids.iter().enumerate() {
                assert_eq!(
                    status(&store, *id).await,
                    if index == position {
                        JobStatus::Ready
                    } else {
                        JobStatus::Completed
                    }
                );
            }
            // New custody arrives only after the first batch joined.
            ids.push(stage(&store, &name, base + 3).await);
            tokio::time::timeout(Duration::from_secs(30), async {
                while status(&store, ids[position]).await != JobStatus::Dead
                    || status(&store, ids[3]).await != JobStatus::Completed
                {
                    worker.run_once(3).await.unwrap();
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                store
                    .inspect("tenant", ids[position])
                    .await
                    .unwrap()
                    .unwrap()
                    .attempts,
                2
            );
            expected.extend((base..base + 4).filter(|id| *id != handler.victim));
            expected.sort_unstable();
            effects(&pool, &expected).await;
            if !matches!(poison, Poison::Invalid) {
                assert_eq!(handler.partial.load(Ordering::SeqCst), 2);
            }
            handler.corrected.store(true, Ordering::SeqCst);
            assert!(
                store
                    .replay(
                        "tenant",
                        ids[position],
                        "operator",
                        "corrected failed input",
                        Utc::now()
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(worker.run_once(3).await.unwrap(), 1);
            assert_eq!(status(&store, ids[position]).await, JobStatus::Completed);
            {
                let calls = handler.calls.lock().unwrap();
                for index in 0..4 {
                    assert_eq!(
                        calls[&(base + index as i32)],
                        if index == position { 3 } else { 1 }
                    );
                }
            }
            expected.push(handler.victim);
            expected.sort_unstable();
            effects(&pool, &expected).await;
        }
    }
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn poisoned_rollback_connection_error_propagates_after_healthy_siblings_join(pool: PgPool) {
    let store = setup(&pool).await;
    let handler = Arc::new(Handler::new(&pool, Poison::LostConnection, 2));
    let worker = handler.worker(&store, "lost-rollback");
    let first = stage(&store, "lost-rollback", 1).await;
    let failed = stage(&store, "lost-rollback", 2).await;
    let last = stage(&store, "lost-rollback", 3).await;
    assert!(matches!(worker.run_once(3).await, Err(Error::Database(_))));
    assert_eq!(status(&store, first).await, JobStatus::Completed);
    assert_eq!(status(&store, last).await, JobStatus::Completed);
    assert_eq!(status(&store, failed).await, JobStatus::Active);
    assert_eq!(handler.calls.lock().unwrap()[&2], 1);
    effects(&pool, &[1, 3]).await;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn confirmed_rollback_settlement_database_error_is_not_suppressed(pool: PgPool) {
    let store = setup(&pool).await;
    sqlx::raw_sql("CREATE FUNCTION refuse_rollback_retry() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.last_error='rollback-only' THEN RAISE EXCEPTION 'refused retry settlement'; END IF; RETURN NEW; END $$; CREATE TRIGGER refuse_rollback_retry BEFORE UPDATE ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION refuse_rollback_retry();").execute(&pool).await.unwrap();
    let handler = Arc::new(Handler::new(&pool, Poison::Invalid, 2));
    let worker = handler.worker(&store, "lost-settlement");
    let first = stage(&store, "lost-settlement", 1).await;
    let failed = stage(&store, "lost-settlement", 2).await;
    let last = stage(&store, "lost-settlement", 3).await;
    let error = worker.run_once(3).await.unwrap_err();
    assert!(
        matches!(error, Error::Database(ref error) if error.as_database_error().and_then(|error| error.code()).as_deref() == Some("P0001"))
    );
    assert_eq!(status(&store, first).await, JobStatus::Completed);
    assert_eq!(status(&store, last).await, JobStatus::Completed);
    assert_eq!(status(&store, failed).await, JobStatus::Active);
    assert_eq!(handler.calls.lock().unwrap()[&2], 1);
    effects(&pool, &[1, 3]).await;
}
