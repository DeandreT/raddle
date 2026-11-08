//! Runs actual PostgreSQL local/outbox workers against an explicitly disposable DB.
use std::sync::Arc;
use std::time::Duration;

use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;
use raddle_host::{Host, PollSettings, RestartPolicy};
use raddle_postgres::{OutboxWorker, RetryPolicy, Store, Worker};

// This example never discards remote messages or pretends they were delivered.
// Replace this transport with a configured confirmed broker publisher.
struct UnconfiguredTransport;
impl Transport for UnconfiguredTransport {
    async fn publish(
        &self,
        _destination: &str,
        _envelope: &Envelope,
    ) -> Result<(), TransportError> {
        Err(TransportError::Transient)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("RADDLE_EXAMPLE_ALLOW_DISPOSABLE_DATABASE").as_deref() != Ok("yes") {
        return Err(
            "set RADDLE_EXAMPLE_ALLOW_DISPOSABLE_DATABASE=yes for an isolated example database"
                .into(),
        );
    }
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let store = Store::new(pool.clone());
    store.migrate().await?;
    let policy = RetryPolicy::try_new(
        Duration::from_millis(10),
        Duration::from_secs(1),
        Duration::from_secs(2),
    )?;
    // Register application-specific transactional handlers before starting.
    let local = Arc::new(Worker::new(store.clone(), policy));
    let outbox = Arc::new(OutboxWorker::new(store, UnconfiguredTransport, policy));
    let polling = PollSettings::new(16, Duration::from_millis(50))?;
    let mut host = Host::new(Duration::from_secs(5))?;
    host.service("local", RestartPolicy::NEVER, move |context| {
        let worker = Arc::clone(&local);
        async move { raddle_host::postgres_worker(&worker, polling, context).await }
    })?;
    host.service("outbox", RestartPolicy::NEVER, move |context| {
        let worker = Arc::clone(&outbox);
        async move { raddle_host::postgres_outbox(&worker, polling, context).await }
    })?;
    let handle = host.handle();
    // A real application's signal listener calls the same explicit shutdown API.
    let (result, ()) = tokio::join!(host.run(), async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        handle.shutdown();
    });
    result?;
    pool.close().await;
    Ok(())
}
