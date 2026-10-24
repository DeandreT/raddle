use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Codecs, Envelope, WireMessage};

use crate::{DeliveryKind, Error, FailureDisposition, Scope, Store};

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
    /// Uses one active SQLite transaction at a time.
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
    /// Executes at most one local job. Dropping the future rolls back in-flight writes.
    pub async fn run_once(&self) -> Result<bool, Error> {
        let Some(claim) = self
            .store
            .claim(
                DeliveryKind::Local,
                1,
                self.timeout + Duration::from_secs(5),
                Utc::now(),
            )
            .await?
            .pop()
        else {
            return Ok(false);
        };
        let envelope = match claim.envelope() {
            Ok(value) => value,
            Err(_) => {
                self.store
                    .fail(&claim, FailureDisposition::Reject, Utc::now())
                    .await?;
                return Ok(true);
            }
        };
        let Some(handler) = self.handlers.get(&(
            claim.destination().to_owned(),
            envelope.contract().to_owned(),
        )) else {
            self.store
                .fail(&claim, FailureDisposition::Reject, Utc::now())
                .await?;
            return Ok(true);
        };
        let mut scope = self.store.execution(&claim, Utc::now()).await?;
        match tokio::time::timeout(self.timeout, handler(&envelope, &mut scope)).await {
            Ok(Ok(())) => scope.commit(Utc::now()).await?,
            result => {
                scope.rollback().await?;
                self.store
                    .fail(
                        &claim,
                        result
                            .ok()
                            .and_then(Result::err)
                            .unwrap_or(FailureDisposition::Retry),
                        Utc::now(),
                    )
                    .await?;
            }
        }
        Ok(true)
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
    /// Publishes at most one due message and preserves its identity across retries.
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
        let envelope = match claim.envelope() {
            Ok(value) => value,
            Err(_) => {
                self.store
                    .fail(&claim, FailureDisposition::Reject, Utc::now())
                    .await?;
                return Ok(true);
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
        Ok(true)
    }
}
