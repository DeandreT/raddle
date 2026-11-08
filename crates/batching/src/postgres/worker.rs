use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use raddle_postgres::Scope;

use super::{BatchClaim, BatchStore, Error, SettlementReport};
use crate::{BatchView, ExecutionQueue, Resolution};

/// Explicit borrowed transaction boundary; the concrete domain error is retained.
pub type HandlerFuture<'a, E> = Pin<Box<dyn Future<Output = Result<Resolution, E>> + Send + 'a>>;

/// Bounded isolation trigger. The caller additionally matches its typed error.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Isolation {
    /// Preserve aggregate retries without automatic probing.
    Disabled,
    /// One size-one sweep after this many matching typed failures (1..=1000).
    After(u32),
}

/// Report of a caller-driven execution pass, not an implicit transport ack.
#[derive(Debug, Default)]
pub struct RunReport {
    /// A durable batch was claimed (an empty interval does not call the handler).
    pub claimed: bool,
    /// All remaining originals expired in the gap between claim and execution.
    pub expired: bool,
    /// Committed settlements; deferred quarantines remain visibly pending.
    pub settlements: Vec<SettlementReport>,
    /// Handler returned an opaque typed error; its business transaction rolled back.
    pub handler_failures: usize,
    /// Execution deadline expired; timed-out business effects were rolled back.
    pub timeouts: usize,
    /// Individual input units attempted during the one-time sweep.
    pub probes: usize,
}

impl BatchStore {
    /// Executes one already assembled batch. Assembly is separately driven for
    /// explicit tenant/group lanes. No polling loop or task is installed.
    ///
    /// Cancellation drops the owned SQLx scope and leaves the claim recoverable;
    /// no failure settlement runs after caller cancellation. A typed error or
    /// timeout rolls back before retry/probing. Errors are never persisted as
    /// formatted strings. External effects require their own idempotency.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_once<E, H, P>(
        &self,
        queue: &ExecutionQueue,
        now: DateTime<Utc>,
        lease: Duration,
        timeout: Duration,
        retry_at: DateTime<Utc>,
        isolation: Isolation,
        mut handler: H,
        matches_isolation: P,
    ) -> Result<RunReport, Error>
    where
        H: for<'a> FnMut(BatchView<'a, Envelope>, &'a mut Scope<'static>) -> HandlerFuture<'a, E>,
        P: Fn(&E) -> bool,
    {
        super::execution_timeout(timeout)?;
        if let Isolation::After(after) = isolation
            && !(1..=1000).contains(&after)
        {
            return Err(Error::Invalid("probe threshold"));
        }
        let started = std::time::Instant::now();
        let Some(claim) = self.claim(queue, now, lease).await? else {
            return Ok(RunReport::default());
        };
        let mut report = RunReport {
            claimed: true,
            ..RunReport::default()
        };
        let clock = || super::until(now, started.elapsed());
        match self.execution(&claim, clock()?, timeout).await {
            Ok(mut execution) => {
                let (members, scope) = execution.parts();
                let outcome = tokio::time::timeout(timeout, handler(members, scope)).await;
                match outcome {
                    Ok(Ok(resolution)) => {
                        report
                            .settlements
                            .push(execution.settle(resolution, retry_at, clock()?).await?);
                        return Ok(report);
                    }
                    Ok(Err(error)) => {
                        execution.rollback().await?;
                        report.handler_failures += 1;
                        let probe = match isolation {
                            Isolation::After(after) if matches_isolation(&error) => {
                                self.isolate_once(&claim, after, clock()?).await?
                            }
                            _ => false,
                        };
                        if !probe {
                            let execution = self.execution(&claim, clock()?, timeout).await?;
                            let resolution = Resolution::named(execution.members(), [])?;
                            report
                                .settlements
                                .push(execution.settle(resolution, retry_at, clock()?).await?);
                            return Ok(report);
                        }
                    }
                    Err(_) => {
                        execution.rollback().await?;
                        report.timeouts += 1;
                        let execution = self.execution(&claim, clock()?, timeout).await?;
                        let resolution = Resolution::named(execution.members(), [])?;
                        report
                            .settlements
                            .push(execution.settle(resolution, retry_at, clock()?).await?);
                        return Ok(report);
                    }
                }
            }
            Err(Error::Probing) => {}
            Err(Error::Expired) => {
                report.expired = true;
                return Ok(report);
            }
            Err(error) => return Err(error),
        }
        self.run_probes(&claim, timeout, retry_at, &clock, &mut handler, &mut report)
            .await?;
        Ok(report)
    }

    async fn run_probes<E, H, C>(
        &self,
        claim: &BatchClaim,
        timeout: Duration,
        retry_at: DateTime<Utc>,
        clock: &C,
        handler: &mut H,
        report: &mut RunReport,
    ) -> Result<(), Error>
    where
        H: for<'a> FnMut(BatchView<'a, Envelope>, &'a mut Scope<'static>) -> HandlerFuture<'a, E>,
        C: Fn() -> Result<DateTime<Utc>, Error>,
    {
        // This is bounded actual work, never a wait/poll loop. Each committed
        // probe advances persisted membership, so crashes resume only unfinished
        // work. Once the sweep ends, future claims retry the reduced batch only.
        for _ in 0..256 {
            let mut execution = match self.probe(claim, clock()?, timeout).await {
                Ok(Some(execution)) => execution,
                Ok(None) => break,
                Err(Error::Expired) => {
                    report.expired = true;
                    break;
                }
                Err(error) => return Err(error),
            };
            report.probes += 1;
            let (members, scope) = execution.parts();
            let outcome = tokio::time::timeout(timeout, handler(members, scope)).await;
            match outcome {
                Ok(Ok(resolution)) => report
                    .settlements
                    .push(execution.settle(resolution, retry_at, clock()?).await?),
                outcome => {
                    if outcome.is_err() {
                        report.timeouts += 1;
                    } else {
                        report.handler_failures += 1;
                    }
                    execution.rollback().await?;
                    let execution = self
                        .probe(claim, clock()?, timeout)
                        .await?
                        .ok_or(Error::Stale)?;
                    let resolution = Resolution::named(execution.members(), [])?;
                    report
                        .settlements
                        .push(execution.settle(resolution, retry_at, clock()?).await?);
                }
            }
            // Settlement closes the fence after the last probe. Do not attempt
            // another execution using that now-stale claim.
            let live: bool = sqlx::query_scalar("SELECT status='active' AND owner=$2 AND generation=$3 FROM raddle_batches WHERE id=$1")
                .bind(claim.id).bind(claim.owner).bind(claim.generation).fetch_one(&self.pool).await?;
            if !live {
                break;
            }
        }
        Ok(())
    }
}
