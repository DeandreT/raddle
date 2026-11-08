use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Codecs, WireMessage};

use crate::{DeliveryKind, Error, FailureDisposition, RetryPolicy, Scope, Store};

/// Handler retry disposition. Domain error messages are not persisted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HandlerFailure {
    /// Retry with bounded backoff.
    Retry,
    /// Quarantine immediately.
    Reject,
}

/// A borrowed transaction handler future, boxed only at the registry boundary.
pub type HandlerFuture<'scope> =
    Pin<Box<dyn Future<Output = Result<(), HandlerFailure>> + Send + 'scope>>;
type Handler = Box<
    dyn for<'scope> Fn(
            &'scope raddle::wire::Envelope,
            &'scope mut Scope<'static>,
        ) -> HandlerFuture<'scope>
        + Send
        + Sync,
>;

/// Registered transactional local handlers. No network effects belong in handlers.
pub struct Worker {
    store: Store,
    handlers: HashMap<(String, String), Handler>,
    policy: RetryPolicy,
}

impl Worker {
    /// Builds a worker using the application's store and bounded execution policy.
    pub fn new(store: Store, policy: RetryPolicy) -> Self {
        Self {
            store,
            handlers: HashMap::new(),
            policy,
        }
    }

    /// Registers a typed handler and its pure schema migrations.
    pub fn handle<M, F>(
        &mut self,
        subscription: &str,
        codecs: Codecs,
        handler: F,
    ) -> Result<(), Error>
    where
        M: WireMessage,
        F: for<'scope> Fn(M, &'scope mut Scope<'static>) -> HandlerFuture<'scope>
            + Send
            + Sync
            + 'static,
    {
        raddle::SubscriptionName::try_new(subscription)
            .map_err(|_| Error::Invalid("invalid subscription"))?;
        let key = (subscription.to_owned(), M::NAME.to_owned());
        if self.handlers.contains_key(&key) {
            return Err(Error::Invalid("duplicate transactional handler"));
        }
        self.handlers.insert(
            key,
            Box::new(move |envelope, scope| match codecs.decode::<M>(envelope) {
                Ok(message) => handler(message, scope),
                Err(_) => Box::pin(async { Err(HandlerFailure::Reject) }),
            }),
        );
        Ok(())
    }

    /// Processes one bounded batch from registered destination queues only.
    /// Other queues, including their expiry sweep, remain untouched. Workers
    /// sharing a destination must register its complete contract set.
    /// Returns the number of observed claims, not successful executions. Lost
    /// claims belong to recovery, not this poller; other errors propagate after
    /// the current batch joins and leave remaining leases recoverable.
    pub async fn run_once(&self, limit: u32) -> Result<usize, Error> {
        use futures_util::stream::StreamExt;

        let destinations = self
            .handlers
            .keys()
            .map(|(destination, _)| destination.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let claims = self
            .store
            .claim_registered(
                &destinations,
                limit,
                self.policy.timeout() + Duration::from_secs(5),
                Utc::now(),
            )
            .await?;
        let count = claims.len();
        let results = futures_util::stream::iter(claims)
            .map(|claim| async move {
                let envelope = match claim.envelope() {
                    Ok(envelope) => envelope,
                    Err(_) => {
                        return self
                            .store
                            .fail(
                                &claim,
                                FailureDisposition::Reject,
                                self.policy,
                                "invalid-envelope",
                                Utc::now(),
                            )
                            .await;
                    }
                };
                let Some(handler) = self.handlers.get(&(
                    claim.destination().to_owned(),
                    envelope.contract().to_owned(),
                )) else {
                    return self
                        .store
                        .fail(
                            &claim,
                            FailureDisposition::Reject,
                            self.policy,
                            "unregistered-contract",
                            Utc::now(),
                        )
                        .await;
                };
                let mut scope = self.store.execution(&claim, Utc::now()).await?;
                sqlx::query("SELECT set_config('statement_timeout', $1, true)")
                    .bind(self.policy.timeout().as_millis().to_string())
                    .execute(scope.connection())
                    .await?;
                match tokio::time::timeout(self.policy.timeout(), handler(&envelope, &mut scope))
                    .await
                {
                    Ok(Ok(())) => scope.commit(Utc::now()).await,
                    outcome => {
                        scope.rollback().await?;
                        let permanent = matches!(outcome, Ok(Err(HandlerFailure::Reject)));
                        self.store
                            .fail(
                                &claim,
                                if permanent {
                                    FailureDisposition::Reject
                                } else {
                                    FailureDisposition::Retry
                                },
                                self.policy,
                                if permanent { "rejected" } else { "retry" },
                                Utc::now(),
                            )
                            .await
                    }
                }
            })
            .buffer_unordered(limit as usize)
            .collect::<Vec<_>>()
            .await;
        finish_batch(count, results)
    }

    /// Polls until shutdown is requested; in-flight batches are joined before return.
    pub async fn run(
        &self,
        limit: u32,
        interval: Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), Error> {
        if interval.is_zero() {
            return Err(Error::Invalid("zero polling interval"));
        }
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.run_once(limit).await?;
            tokio::select! {
                _ = tokio::time::sleep(interval) => {},
                changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
        }
    }
}

/// Confirmed outbox publishing with stable identities and fenced completion.
pub struct OutboxWorker<T> {
    store: Store,
    transport: T,
    policy: RetryPolicy,
}

impl<T: Transport> OutboxWorker<T> {
    /// Uses a transport that acknowledges broker acceptance explicitly.
    pub fn new(store: Store, transport: T, policy: RetryPolicy) -> Self {
        Self {
            store,
            transport,
            policy,
        }
    }

    /// Returns the owned transport for explicit shutdown after batch futures finish.
    /// This worker starts no background tasks; consuming it cannot race a borrowed batch.
    pub fn into_transport(self) -> T {
        self.transport
    }

    /// Publishes a bounded batch outside database transactions.
    /// Returns observed claims, not confirmed completions. Lost claims are left
    /// to their current owner; other errors propagate after the batch joins.
    pub async fn run_once(&self, limit: u32) -> Result<usize, Error> {
        self.run_once_inner(None, limit).await
    }

    /// Flushes one configured destination without claiming unrelated endpoints.
    /// Counts observed claims and handles ownership loss as in `run_once`.
    pub async fn run_once_for(&self, destination: &str, limit: u32) -> Result<usize, Error> {
        self.run_once_inner(Some(destination), limit).await
    }

    async fn run_once_inner(&self, destination: Option<&str>, limit: u32) -> Result<usize, Error> {
        use futures_util::stream::StreamExt;

        let lease = self.policy.timeout() + Duration::from_secs(5);
        let claims = match destination {
            Some(destination) => {
                self.store
                    .claim_for(DeliveryKind::Remote, destination, limit, lease, Utc::now())
                    .await?
            }
            None => {
                self.store
                    .claim(DeliveryKind::Remote, limit, lease, Utc::now())
                    .await?
            }
        };
        let count = claims.len();
        let results = futures_util::stream::iter(claims)
            .map(|claim| async move {
                let envelope = match claim.envelope() {
                    Ok(envelope) => envelope,
                    Err(_) => {
                        return self
                            .store
                            .fail(
                                &claim,
                                FailureDisposition::Reject,
                                self.policy,
                                "invalid-envelope",
                                Utc::now(),
                            )
                            .await;
                    }
                };
                match tokio::time::timeout(
                    self.policy.timeout(),
                    self.transport.publish(claim.destination(), &envelope),
                )
                .await
                {
                    Ok(Ok(())) => self.store.complete(&claim, Utc::now()).await,
                    outcome => {
                        self.store
                            .fail(
                                &claim,
                                if matches!(outcome, Ok(Err(TransportError::Rejected))) {
                                    FailureDisposition::Reject
                                } else {
                                    FailureDisposition::Retry
                                },
                                self.policy,
                                "publish-failed",
                                Utc::now(),
                            )
                            .await
                    }
                }
            })
            .buffer_unordered(limit as usize)
            .collect::<Vec<_>>()
            .await;
        finish_batch(count, results)
    }
}

fn finish_batch(count: usize, results: Vec<Result<(), Error>>) -> Result<usize, Error> {
    for result in results {
        match result {
            // Queued executions and publications outside a transaction can
            // lose ownership. Fencing leaves their successor claim untouched.
            Ok(()) | Err(Error::StaleClaim) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use crate::Error;

    use super::finish_batch;

    #[test]
    fn ownership_loss_in_any_result_position_counts_observed_claims() {
        // buffer_unordered produces completion order, not claim/input order.
        for stale in 0..3 {
            let results = (0..3)
                .map(|index| {
                    if index == stale {
                        Err(Error::StaleClaim)
                    } else {
                        Ok(())
                    }
                })
                .collect();
            assert!(matches!(finish_batch(3, results), Ok(3)));
        }
        assert!(matches!(
            finish_batch(2, vec![Err(Error::StaleClaim), Err(Error::StaleClaim)]),
            Ok(2)
        ));
    }

    #[test]
    fn ownership_loss_never_hides_other_errors_in_any_result_order() {
        for stale in 0..3 {
            for failed in 0..3 {
                if stale == failed {
                    continue;
                }
                let results = (0..3)
                    .map(|index| match index {
                        index if index == stale => Err(Error::StaleClaim),
                        index if index == failed => Err(Error::Database(sqlx::Error::PoolClosed)),
                        _ => Ok(()),
                    })
                    .collect();
                assert!(matches!(finish_batch(3, results), Err(Error::Database(_))));
            }
        }
        for error in [
            Error::RollbackOnly,
            Error::Invalid("test"),
            Error::Capacity,
            Error::IdentityConflict,
        ] {
            assert!(finish_batch(2, vec![Err(Error::StaleClaim), Err(error)]).is_err());
        }
    }
}
