//! SQLite VM interruption, rollback, and connection lifecycle regression tests.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_sqlite::{Delivery, DeliveryKind, FailureDisposition, Store, Worker};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Connection, SqlitePool};

// Finite even on the unfixed implementation, but far longer than the worker budget.
const EXPENSIVE_QUERY: &str = "WITH RECURSIVE numbers(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM numbers WHERE n<50000000) SELECT sum(n) FROM numbers";
const REUSE_QUERY: &str = "WITH RECURSIVE numbers(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM numbers WHERE n<100000) SELECT sum(n) FROM numbers";

#[derive(Serialize, Deserialize)]
struct Message;
impl WireMessage for Message {
    const NAME: &'static str = "test.deadline";
    const VERSION: u32 = 1;
}

async fn fixture() -> (tempfile::TempDir, Store, SqlitePool) {
    let directory = tempfile::tempdir().unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(directory.path().join("deadline.sqlite"))
                .create_if_missing(true),
        )
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    sqlx::query("CREATE TABLE business(value INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    // A connection-local marker proves normal cleanup reuses the actual connection.
    sqlx::query("CREATE TEMP TABLE connection_marker(value INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    (directory, store, pool)
}

async fn enqueue(store: &Store) -> uuid::Uuid {
    let mut scope = store.begin().await.unwrap();
    let ids = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Message).unwrap(),
            &[Delivery::new(DeliveryKind::Local, "audit", Utc::now())],
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    ids[0]
}

async fn assert_pool_usable(pool: &SqlitePool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut connection = pool.acquire().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
                .fetch_one(&mut *connection)
                .await
                .unwrap(),
            0
        );
        // More VM instructions than a progress-handler interval, after its old deadline.
        assert_eq!(
            sqlx::query_scalar::<_, i64>(REUSE_QUERY)
                .fetch_one(&mut *connection)
                .await
                .unwrap(),
            5_000_050_000
        );
        // A custom BEGIN also verifies SQLx's transaction-depth counter is clean,
        // rather than silently creating a savepoint on a stale checkout.
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("INSERT INTO business VALUES(2)")
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn deadline_interrupts_sqlite_and_reuses_the_clean_connection() {
    let (_directory, store, pool) = fixture().await;
    let id = enqueue(&store).await;
    let mut worker = Worker::try_new(store.clone(), Duration::from_millis(40)).unwrap();
    worker
        .handle::<Message, _>("audit", Codecs::default(), |_, scope| {
            Box::pin(async move {
                sqlx::query("INSERT INTO business VALUES(1)")
                    .execute(scope.connection())
                    .await
                    .unwrap();
                // Even if the application classifies an interrupted query as permanent,
                // the execution deadline must remain a retry, not a rejection.
                sqlx::query(EXPENSIVE_QUERY)
                    .execute(scope.connection())
                    .await
                    .map_err(|_| FailureDisposition::Reject)?;
                Ok(())
            })
        })
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), worker.run_once())
            .await
            .unwrap()
            .unwrap()
    );
    let job = store.inspect("tenant", id).await.unwrap().unwrap();
    assert_eq!(job.status, "ready");
    assert_eq!(job.attempts, 1);
    assert_pool_usable(&pool).await;
    sqlx::query("SELECT * FROM connection_marker")
        .execute(&pool)
        .await
        .unwrap();
    store.close().await;
}

#[tokio::test]
async fn dropping_execution_interrupts_running_sqlite_and_preserves_recovery() {
    let (_directory, store, pool) = fixture().await;
    let id = enqueue(&store).await;
    let started = Arc::new(tokio::sync::Notify::new());
    let signal = Arc::clone(&started);
    // Cancellation must not wait for this much longer execution budget.
    let mut worker = Worker::try_new(store.clone(), Duration::from_secs(30)).unwrap();
    worker
        .handle::<Message, _>("audit", Codecs::default(), move |_, scope| {
            let signal = Arc::clone(&signal);
            Box::pin(async move {
                sqlx::query("INSERT INTO business VALUES(1)")
                    .execute(scope.connection())
                    .await
                    .unwrap();
                signal.notify_one();
                sqlx::query(EXPENSIVE_QUERY)
                    .execute(scope.connection())
                    .await
                    .map_err(|_| FailureDisposition::Retry)?;
                Ok(())
            })
        })
        .unwrap();
    let task = tokio::spawn(async move { worker.run_once().await });
    started.notified().await;
    // Give SQLx's blocking thread time to enter sqlite3_step before aborting.
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_pool_usable(&pool).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_temp_master WHERE name='connection_marker'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "a cancelled execution must discard its guarded checkout"
    );
    let job = store.inspect("tenant", id).await.unwrap().unwrap();
    assert_eq!(job.status, "active");
    assert_eq!(job.attempts, 1);
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
async fn successful_and_failed_handlers_remove_the_progress_callback() {
    for disposition in [
        None,
        Some(FailureDisposition::Retry),
        Some(FailureDisposition::Reject),
    ] {
        let (_directory, store, pool) = fixture().await;
        let id = enqueue(&store).await;
        let mut worker = Worker::try_new(store.clone(), Duration::from_millis(100)).unwrap();
        worker
            .handle::<Message, _>("audit", Codecs::default(), move |_, _| {
                Box::pin(async move { disposition.map_or(Ok(()), Err) })
            })
            .unwrap();
        assert!(worker.run_once().await.unwrap());
        let expected = match disposition {
            None => "completed",
            Some(FailureDisposition::Retry) => "ready",
            Some(_) => "dead",
        };
        assert_eq!(
            store.inspect("tenant", id).await.unwrap().unwrap().status,
            expected
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_pool_usable(&pool).await;
        sqlx::query("SELECT * FROM connection_marker")
            .execute(&pool)
            .await
            .unwrap();
        store.close().await;
    }
}

#[tokio::test]
async fn rust_future_timeout_also_removes_the_progress_callback() {
    let (_directory, store, pool) = fixture().await;
    let id = enqueue(&store).await;
    let mut worker = Worker::try_new(store.clone(), Duration::from_millis(40)).unwrap();
    worker
        .handle::<Message, _>("audit", Codecs::default(), |_, scope| {
            Box::pin(async move {
                sqlx::query("INSERT INTO business VALUES(1)")
                    .execute(scope.connection())
                    .await
                    .unwrap();
                std::future::pending().await
            })
        })
        .unwrap();
    assert!(worker.run_once().await.unwrap());
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "ready"
    );
    assert_pool_usable(&pool).await;
    sqlx::query("SELECT * FROM connection_marker")
        .execute(&pool)
        .await
        .unwrap();
    store.close().await;
}

#[tokio::test]
async fn interrupted_write_and_swallowed_query_error_cannot_commit() {
    let (_directory, store, pool) = fixture().await;
    let id = enqueue(&store).await;
    let mut worker = Worker::try_new(store.clone(), Duration::from_millis(40)).unwrap();
    worker
        .handle::<Message, _>("audit", Codecs::default(), |_, scope| {
            Box::pin(async move {
                sqlx::query("INSERT INTO business VALUES(1)")
                    .execute(scope.connection())
                    .await
                    .unwrap();
                // SQLite may roll back the entire transaction when this write is
                // interrupted, even though the handler mistakenly returns success.
                let _ = sqlx::query("WITH RECURSIVE numbers(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM numbers WHERE n<50000000) INSERT INTO business SELECT sum(n) FROM numbers")
                    .execute(scope.connection())
                    .await;
                // Do not let a tiny follow-up write autocommit after SQLite's
                // implicit rollback, even if the handler ignores both failures.
                let _ = sqlx::query("INSERT INTO business VALUES(99)")
                    .execute(scope.connection())
                    .await;
                Ok(())
            })
        })
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), worker.run_once())
            .await
            .unwrap()
            .unwrap()
    );
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        "ready"
    );
    assert_pool_usable(&pool).await;
    store.close().await;
}
