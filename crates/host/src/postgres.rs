use std::time::Duration;

use raddle::transport::Transport;
use raddle_postgres::{OutboxWorker, Worker};

use crate::{HostError, ServiceContext, ServiceFailure};

/// Validated bounded PostgreSQL polling settings.
#[derive(Clone, Copy, Debug)]
pub struct PollSettings {
    limit: u32,
    interval: Duration,
}

impl PollSettings {
    /// Accepts 1..=256 claims and an idle interval in 1ms..=60s.
    pub fn new(limit: u32, interval: Duration) -> Result<Self, HostError> {
        if !(1..=256).contains(&limit)
            || !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&interval)
        {
            return Err(HostError::Invalid("invalid PostgreSQL polling bounds"));
        }
        Ok(Self { limit, interval })
    }
}

/// Hosts a real transactional worker. Readiness follows its first successful
/// database batch, not task startup. A batch drains before cooperative shutdown.
///
/// Keep handler registration and migrations outside the service factory. Forced
/// host abort may leave leases for recovery; it does not undo committed work.
pub async fn postgres_worker(
    worker: &Worker,
    settings: PollSettings,
    context: ServiceContext,
) -> Result<(), ServiceFailure> {
    if *context.shutdown().borrow() {
        return Ok(());
    }
    worker.run_once(settings.limit).await.map_err(classify)?;
    context.set_ready(true);
    worker
        .run(settings.limit, settings.interval, context.shutdown())
        .await
        .map_err(classify)
}

/// Hosts a real confirmed outbox worker through bounded `run_once` calls.
/// Readiness means a database batch succeeded, **not** that an idle transport
/// connection has been probed. Applications needing broker readiness must probe
/// it explicitly in their service before reporting readiness instead.
pub async fn postgres_outbox<T: Transport>(
    worker: &OutboxWorker<T>,
    settings: PollSettings,
    mut context: ServiceContext,
) -> Result<(), ServiceFailure> {
    loop {
        if *context.shutdown().borrow() {
            return Ok(());
        }
        worker.run_once(settings.limit).await.map_err(classify)?;
        context.set_ready(true);
        tokio::select! {
            biased;
            _ = context.cancelled() => return Ok(()),
            _ = tokio::time::sleep(settings.interval) => {}
        }
    }
}

fn classify(error: raddle_postgres::Error) -> ServiceFailure {
    match error {
        raddle_postgres::Error::Database(_) | raddle_postgres::Error::StaleClaim => {
            ServiceFailure::Transient
        }
        _ => ServiceFailure::Permanent,
    }
}
