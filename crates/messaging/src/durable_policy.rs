use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle_policies::{
    AttemptCounters, Direction, DriverError, Execution, ExecutionMode, PolicyExecutorDriver,
    Transition,
};
use raddle_postgres::endpoint_policies::PolicySettlement;
use raddle_postgres::{Claim, Error, Store};
use tokio::sync::watch;

use crate::durable::ClaimOutcome;
use crate::policy::{AttemptFailure, Permit};
use crate::{Cancellation, DurableEndpoint, EndpointPolicies, MessageContext, TrustedOrigin};

struct Driver<'a> {
    store: &'a Store,
    claim: &'a Claim,
    policies: &'a EndpointPolicies,
    clock: tokio::time::Instant,
    error: Option<Error>,
}
impl Driver<'_> {
    fn wall(&self, due: Duration) -> Result<DateTime<Utc>, DriverError> {
        let delay = due.saturating_sub(self.clock.elapsed());
        Utc::now()
            .checked_add_signed(
                chrono::Duration::from_std(delay).map_err(|_| DriverError::Invalid)?,
            )
            .ok_or(DriverError::Invalid)
    }
    fn result(&mut self, result: Result<(), Error>) -> Result<(), DriverError> {
        result.map_err(|error| {
            self.error = Some(error);
            DriverError::Custody
        })
    }
    fn outcome(&mut self) -> Result<ClaimOutcome, Error> {
        match self.error.take() {
            Some(Error::StaleClaim) => Ok(ClaimOutcome::OwnershipLost),
            Some(error) => Err(error),
            None => Err(Error::Invalid("policy driver state failed")),
        }
    }
}
impl PolicyExecutorDriver for Driver<'_> {
    async fn transition(&mut self, transition: Transition) -> Result<(), DriverError> {
        let settlement = match transition {
            Transition::QueueAt(due) => PolicySettlement::QueueAt(self.wall(due)?),
            Transition::Discard => PolicySettlement::Discard,
            Transition::DeadLetter { replay_denied } => {
                if replay_denied {
                    PolicySettlement::ReplayDenied
                } else {
                    PolicySettlement::DeadLetter
                }
            }
            Transition::RetryInline => {
                // Executor has already checked explicit replay authorization.
                let result = self.store.policy_rolled_back(self.claim, Utc::now()).await;
                return self.result(result);
            }
            Transition::Retain => return Ok(()),
            _ => return Err(DriverError::Invalid),
        };
        let result = self
            .store
            .settle_policy(self.claim, settlement, Utc::now())
            .await;
        self.result(result)
    }
    async fn pause_until(&mut self, until: Duration) -> Result<(), DriverError> {
        let wall = self.wall(until)?;
        let result = self.store.pause_policy(self.claim, wall, Utc::now()).await;
        self.result(result)?;
        let mut admission = self.policies.admission.lock().await;
        admission.paused_until = admission.paused_until.max(until);
        Ok(())
    }
}

impl DurableEndpoint {
    pub(crate) async fn process_policy_claim(
        &self,
        claim: Claim,
        shutdown: Cancellation,
    ) -> Result<ClaimOutcome, Error> {
        let policies = self
            .policies
            .as_ref()
            .ok_or(Error::Invalid("missing endpoint policies"))?;
        let mut driver = Driver {
            store: &self.store,
            claim: &claim,
            policies,
            clock: self.started,
            error: None,
        };
        let envelope = match claim.envelope() {
            Ok(envelope) => envelope,
            Err(Error::Wire(_)) => return terminal(&mut driver).await,
            Err(error) => return Err(error),
        };
        let Some((handler, replay)) = self
            .policy_handlers
            .get(&(envelope.contract().to_owned(), envelope.version()))
        else {
            // Unknown input bypasses ordinary error policies and handler counters.
            let original = envelope.to_vec()?;
            return match self
                .unknown
                .lock()
                .await
                .handle(&mut driver, &original)
                .await
            {
                Ok(_) => Ok(ClaimOutcome::Quarantined),
                Err(_) => driver.outcome(),
            };
        };
        let origin = match TrustedOrigin::authenticated(claim.tenant(), claim.source(), None) {
            Ok(origin) => origin,
            Err(_) => return terminal(&mut driver).await,
        };
        loop {
            if shutdown.is_cancelled() {
                policies.cancel_probe().await;
                return Ok(ClaimOutcome::Recoverable);
            }
            let now = self.started.elapsed();
            let permit = if self.control.is_paused() {
                Permit::Limited(now)
            } else {
                policies
                    .acquire(&self.name, envelope.contract(), now)
                    .await
                    .map_err(|_| Error::Invalid("policy admission failed"))?
            };
            let permit = match permit {
                Permit::Granted(permit) => permit,
                Permit::Limited(until) => {
                    if until > self.started.elapsed() && driver.pause_until(until).await.is_err() {
                        return driver.outcome();
                    }
                    return match driver.transition(Transition::QueueAt(until)).await {
                        Ok(()) => Ok(ClaimOutcome::Recoverable),
                        Err(_) => driver.outcome(),
                    };
                }
            };
            let deadline = tokio::time::Instant::now() + self.lease_timeout;
            let attempts = match self
                .store
                .begin_policy_attempt(
                    &claim,
                    self.lease_timeout + Duration::from_secs(5),
                    Utc::now(),
                )
                .await
            {
                Ok(attempts) => attempts,
                Err(error) => {
                    policies.cancel_probe().await;
                    return if matches!(error, Error::StaleClaim) {
                        Ok(ClaimOutcome::OwnershipLost)
                    } else {
                        Err(error)
                    };
                }
            };
            let scope = match self.store.policy_execution(&claim, Utc::now()).await {
                Ok(scope) => scope,
                Err(error) => {
                    policies.cancel_probe().await;
                    return if matches!(error, Error::StaleClaim) {
                        Ok(ClaimOutcome::OwnershipLost)
                    } else {
                        Err(error)
                    };
                }
            };
            let mut context = MessageContext::new(origin.clone(), envelope.clone(), Utc::now())
                .with_cancellation(shutdown.child());
            if let Some(bus) = &self.bus {
                context = context.with_bus(bus.clone());
            }
            // Never drop this future on shutdown: it owns rollback and Finally.
            let result = handler(scope, context, deadline).await;
            drop(permit);
            if matches!(&result, Err(AttemptFailure::Cancelled))
                && matches!(
                    replay,
                    raddle_policies::ReplaySafety::Idempotent
                        | raddle_policies::ReplaySafety::Transactional
                )
                && let Err(error) = self.store.policy_rolled_back(&claim, Utc::now()).await
            {
                policies.cancel_probe().await;
                return if matches!(error, Error::StaleClaim) {
                    Ok(ClaimOutcome::OwnershipLost)
                } else {
                    Err(error)
                };
            }
            match result {
                Ok(()) => {
                    // A committed root remains successful even if shutdown raced
                    // its commit or post-commit flush. No parent replay.
                    policies
                        .record(self.started.elapsed(), None)
                        .await
                        .map_err(|_| Error::Invalid("policy circuit failed"))?;
                    return Ok(ClaimOutcome::Completed);
                }
                Err(AttemptFailure::Cancelled) => {
                    policies.cancel_probe().await;
                    return Ok(ClaimOutcome::Recoverable);
                }
                Err(AttemptFailure::Infrastructure(error)) => {
                    policies.cancel_probe().await;
                    return if matches!(error, Error::StaleClaim) {
                        Ok(ClaimOutcome::OwnershipLost)
                    } else {
                        Err(error)
                    };
                }
                Err(AttemptFailure::InvalidInput) => {
                    policies.cancel_probe().await;
                    return terminal(&mut driver).await;
                }
                Err(AttemptFailure::Application(error)) => {
                    if shutdown.is_cancelled() {
                        policies.cancel_probe().await;
                        return Ok(ClaimOutcome::Recoverable);
                    }
                    if let Some(until) = policies
                        .record(self.started.elapsed(), Some(&*error))
                        .await
                        .map_err(|_| Error::Invalid("policy circuit failed"))?
                        && driver.pause_until(until).await.is_err()
                    {
                        return driver.outcome();
                    }
                    let (sender, mut stop) = watch::channel(false);
                    let execution = Execution {
                        endpoint: &self.name,
                        contract: envelope.contract(),
                        direction: Direction::Handling,
                        attempts: AttemptCounters {
                            handling: attempts,
                            sending: 0,
                        },
                        replay: *replay,
                        mode: ExecutionMode::Queued,
                        now: self.started.elapsed(),
                        jitter_sample: jitter(&claim, attempts),
                    };
                    let transition = {
                        let failed =
                            policies
                                .executor
                                .failed(&mut driver, execution, &*error, &mut stop);
                        tokio::pin!(failed);
                        tokio::select! {
                        biased;
                        () = shutdown.cancelled() => { sender.send_replace(true); failed.await }
                        result = &mut failed => result,
                        }
                    };
                    match transition {
                        Ok(Transition::RetryInline) => {
                            tokio::task::yield_now().await;
                        }
                        Ok(Transition::Retain) => return Ok(ClaimOutcome::Recoverable),
                        Ok(_) => return Ok(ClaimOutcome::FailureRecorded),
                        Err(_) => return driver.outcome(),
                    }
                }
            }
        }
    }
}

async fn terminal(driver: &mut Driver<'_>) -> Result<ClaimOutcome, Error> {
    match driver
        .transition(Transition::DeadLetter {
            replay_denied: false,
        })
        .await
    {
        Ok(()) => Ok(ClaimOutcome::Quarantined),
        Err(_) => driver.outcome(),
    }
}

// Stable per-delivery/attempt mixing; not cryptographic randomness or identity.
fn jitter(claim: &Claim, attempt: u64) -> u64 {
    claim
        .id()
        .as_bytes()
        .iter()
        .fold(attempt.wrapping_mul(0x9e3779b97f4a7c15), |state, byte| {
            state.rotate_left(7) ^ u64::from(*byte)
        })
}
