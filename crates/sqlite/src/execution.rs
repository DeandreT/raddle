use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use sqlx::pool::PoolConnection;
use sqlx::sqlite::SqliteTransactionManager;
use sqlx::{Sqlite, SqliteConnection, SqlitePool, TransactionManager};

// Own the pool checkout, rather than only a SQLx Transaction, so cancellation can
// quarantine a connection whose progress callback cannot be removed synchronously.
pub(crate) struct Execution {
    connection: PoolConnection<Sqlite>,
    cancelled: Arc<AtomicBool>,
    interrupted: Arc<AtomicBool>,
    deadline: Option<Instant>,
    reusable: bool,
}

impl Execution {
    pub(crate) async fn begin(pool: &SqlitePool) -> Result<Self, sqlx::Error> {
        let mut execution = Self {
            connection: pool.acquire().await?,
            cancelled: Arc::new(AtomicBool::new(false)),
            interrupted: Arc::new(AtomicBool::new(false)),
            deadline: None,
            reusable: false,
        };
        SqliteTransactionManager::begin(&mut execution.connection, Some("BEGIN IMMEDIATE".into()))
            .await?;
        Ok(execution)
    }

    pub(crate) fn connection(&mut self) -> &mut SqliteConnection {
        &mut self.connection
    }

    pub(crate) async fn set_deadline(&mut self, timeout: Duration) -> Result<(), sqlx::Error> {
        let mut handle = self.connection.lock_handle().await?;
        let deadline = Instant::now() + timeout;
        let cancelled = Arc::clone(&self.cancelled);
        let interrupted = Arc::clone(&self.interrupted);
        // Check every VM instruction: after an interrupted write automatically
        // rolls back, even a tiny follow-up statement must not escape the expired
        // budget and autocommit. SQLite checks this on its own blocking thread.
        handle.set_progress_handler(1, move || {
            let keep_running = !cancelled.load(Ordering::Relaxed) && Instant::now() < deadline;
            if !keep_running {
                interrupted.store(true, Ordering::Relaxed);
            }
            keep_running
        });
        self.deadline = Some(deadline);
        Ok(())
    }

    pub(crate) async fn stop_deadline(&mut self) -> Result<bool, sqlx::Error> {
        // A handler may have dropped an unfinished SQL future before returning.
        // Interrupt that work before waiting for exclusive access to its handle.
        self.cancelled.store(true, Ordering::Relaxed);
        let mut handle = self.connection.lock_handle().await?;
        handle.remove_progress_handler();
        Ok(self.interrupted.load(Ordering::Relaxed)
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline))
    }

    pub(crate) async fn commit(mut self) -> Result<(), sqlx::Error> {
        SqliteTransactionManager::commit(&mut self.connection).await?;
        self.reusable = true;
        Ok(())
    }

    pub(crate) async fn rollback(mut self) -> Result<(), sqlx::Error> {
        match SqliteTransactionManager::rollback(&mut self.connection).await {
            Ok(()) => self.reusable = true,
            Err(error) if !self.interrupted.load(Ordering::Relaxed) => return Err(error),
            Err(_) => {}
        }
        // An interrupted write can make SQLite roll back automatically. SQLx's
        // rollback then fails and its transaction-depth counter remains stale.
        // Discard that checkout: closing also rolls back any surviving transaction,
        // without relying on private SQLite state APIs or error-message matching.
        Ok(())
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        if !self.reusable {
            // SQLx closes the connection after any interrupted VM call finishes.
            // Closing rolls back writes and destroys the callback. The pool never
            // receives a connection with a live callback or unfinished transaction.
            self.connection.close_on_drop();
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::time::Duration;

    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    use super::Execution;

    fn assert_interrupted(error: sqlx::Error) {
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
            Some("9"),
            "expected SQLite SQLITE_INTERRUPT, got {error}"
        );
    }

    #[tokio::test]
    async fn expired_deadline_refuses_followup_autocommit_after_interrupted_write() {
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
        sqlx::query("CREATE TABLE business(value INTEGER)")
            .execute(&pool)
            .await
            .unwrap();

        let mut execution = Execution::begin(&pool).await.unwrap();
        sqlx::query("INSERT INTO business VALUES(1)")
            .execute(execution.connection())
            .await
            .unwrap();
        execution
            .set_deadline(Duration::from_millis(40))
            .await
            .unwrap();

        // No outer Rust timeout can skip the follow-up: await SQLite's own
        // interruption, then issue the tiny write before removing the callback.
        let error = sqlx::query("WITH RECURSIVE numbers(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM numbers WHERE n<50000000) INSERT INTO business SELECT sum(n) FROM numbers")
            .execute(execution.connection())
            .await
            .unwrap_err();
        assert_interrupted(error);
        let error = sqlx::query("INSERT INTO business VALUES(99)")
            .execute(execution.connection())
            .await
            .unwrap_err();
        assert_interrupted(error);

        assert!(execution.stop_deadline().await.unwrap());
        // Check before explicit cleanup: neither the original transaction nor
        // the attempted follow-up autocommit may have left business writes.
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
                .fetch_one(execution.connection())
                .await
                .unwrap(),
            0
        );
        execution.rollback().await.unwrap();

        // The replacement checkout must also have clean SQLx transaction state.
        let transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        transaction.rollback().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        pool.close().await;
    }
}
