use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Codecs, Envelope, WireMessage};

use crate::{Claim, DeliveryKind, Error, FailureDisposition, Scope, Store};

/// A borrowed transactional handler, boxed at registration only.
pub type HandlerFuture<'scope> =
    Pin<Box<dyn Future<Output = Result<(), FailureDisposition>> + Send + 'scope>>;
type Handler = Box<
    dyn for<'scope> Fn(&'scope Envelope, &'scope mut Scope<'static>) -> HandlerFuture<'scope>
        + Send
        + Sync,
>;

/// Single-writer local dispatcher with bounded execution time.
pub struct Worker {
    store: Store,
    handlers: HashMap<(String, String), Handler>,
    timeout: Duration,
}
impl Worker {
    /// Uses one active SQLite transaction at a time. The handler budget covers
    /// cooperative Rust futures and SQLite VM work, not writer acquisition,
    /// filesystem I/O, native SQLite functions, or blocking application code.
    pub fn try_new(store: Store, timeout: Duration) -> Result<Self, Error> {
        if timeout.is_zero() || timeout > Duration::from_secs(3600) {
            return Err(Error::Invalid("invalid timeout"));
        }
        Ok(Self {
            store,
            handlers: HashMap::new(),
            timeout,
        })
    }
    /// Registers a typed local handler and explicit upcasters.
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
            return Err(Error::Invalid("duplicate handler"));
        }
        self.handlers.insert(
            key,
            Box::new(move |envelope, scope| match codecs.decode::<M>(envelope) {
                Ok(message) => handler(message, scope),
                Err(_) => Box::pin(async { Err(FailureDisposition::Reject) }),
            }),
        );
        Ok(())
    }
    /// Observes at most one claim from registered destination queues only.
    /// Other destinations, including their expiry sweep, remain untouched. Workers
    /// sharing one destination must register its complete contract set. Ownership
    /// loss stays local; other errors propagate. `true` does not imply success.
    ///
    /// The deadline interrupts SQLite VM work.
    /// Dropping the future interrupts in-flight SQL and closes its connection,
    /// rolling back writes; the claim remains recoverable after lease expiry.
    pub async fn run_once(&self) -> Result<bool, Error> {
        let destinations = self
            .handlers
            .keys()
            .map(|(destination, _)| destination.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let Some(claim) = self
            .store
            .claim_registered(
                &destinations,
                self.timeout + Duration::from_secs(5),
                Utc::now(),
            )
            .await?
            .pop()
        else {
            return Ok(false);
        };
        observed(self.process(claim).await)
    }

    async fn process(&self, claim: Claim) -> Result<(), Error> {
        let envelope = match claim.envelope() {
            Ok(value) => value,
            Err(_) => {
                self.store
                    .fail(&claim, FailureDisposition::Reject, Utc::now())
                    .await?;
                return Ok(());
            }
        };
        let Some(handler) = self.handlers.get(&(
            claim.destination().to_owned(),
            envelope.contract().to_owned(),
        )) else {
            self.store
                .fail(&claim, FailureDisposition::Reject, Utc::now())
                .await?;
            return Ok(());
        };
        let mut scope = self
            .store
            .worker_execution(&claim, Utc::now(), self.timeout)
            .await?;
        let result = tokio::time::timeout(self.timeout, handler(&envelope, &mut scope)).await;
        let expired = scope.stop_deadline().await?;
        match (result, expired) {
            (Ok(Ok(())), false) => scope.commit(Utc::now()).await?,
            (result, expired) => {
                scope.rollback().await?;
                self.store
                    .fail(
                        &claim,
                        if expired {
                            FailureDisposition::Retry
                        } else {
                            result
                                .ok()
                                .and_then(Result::err)
                                .unwrap_or(FailureDisposition::Retry)
                        },
                        Utc::now(),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

/// One-at-a-time confirmed publication avoids claiming work that can expire in a local queue.
pub struct OutboxWorker<T> {
    store: Store,
    transport: T,
    timeout: Duration,
}
impl<T: Transport> OutboxWorker<T> {
    /// Sets a positive bounded publication timeout.
    pub fn try_new(store: Store, transport: T, timeout: Duration) -> Result<Self, Error> {
        if timeout.is_zero() || timeout > Duration::from_secs(3600) {
            return Err(Error::Invalid("invalid timeout"));
        }
        Ok(Self {
            store,
            transport,
            timeout,
        })
    }

    /// Returns the owned transport for explicit shutdown after batch futures finish.
    /// This worker starts no background tasks; consuming it cannot race a borrowed batch.
    pub fn into_transport(self) -> T {
        self.transport
    }

    /// Publishes at most one due message and preserves its identity across retries.
    /// `true` reports an observed claim, including ownership loss. A successor's
    /// custody remains untouched; genuine database and configuration errors propagate.
    pub async fn run_once(&self) -> Result<bool, Error> {
        let Some(claim) = self
            .store
            .claim(
                DeliveryKind::Remote,
                1,
                self.timeout + Duration::from_secs(5),
                Utc::now(),
            )
            .await?
            .pop()
        else {
            return Ok(false);
        };
        observed(self.process(claim).await)
    }

    async fn process(&self, claim: Claim) -> Result<(), Error> {
        let envelope = match claim.envelope() {
            Ok(value) => value,
            Err(_) => {
                self.store
                    .fail(&claim, FailureDisposition::Reject, Utc::now())
                    .await?;
                return Ok(());
            }
        };
        match tokio::time::timeout(
            self.timeout,
            self.transport.publish(claim.destination(), &envelope),
        )
        .await
        {
            Ok(Ok(())) => self.store.complete(&claim, Utc::now()).await?,
            result => {
                self.store
                    .fail(
                        &claim,
                        if matches!(result, Ok(Err(TransportError::Rejected))) {
                            FailureDisposition::Reject
                        } else {
                            FailureDisposition::Retry
                        },
                        Utc::now(),
                    )
                    .await?
            }
        }
        Ok(())
    }
}

fn observed(result: Result<(), Error>) -> Result<bool, Error> {
    match result {
        Ok(()) | Err(Error::StaleClaim) => Ok(true),
        Err(error) => Err(error),
    }
}
