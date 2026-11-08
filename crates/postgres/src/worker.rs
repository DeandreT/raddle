use std::collections::HashMap;
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

    /// Processes one bounded batch; errors leave remaining leases recoverable.
    pub async fn run_once(&self, limit: u32) -> Result<usize, Error> {
        use futures_util::stream::StreamExt;

        let claims = self
            .store
            .claim(
                DeliveryKind::Local,
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
        for result in results {
            result?;
        }
        Ok(count)
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
    pub async fn run_once(&self, limit: u32) -> Result<usize, Error> {
        use futures_util::stream::StreamExt;

        let claims = self
            .store
            .claim(
                DeliveryKind::Remote,
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
        for result in results {
            result?;
        }
        Ok(count)
    }
}
