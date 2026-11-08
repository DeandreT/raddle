use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::stream::{self, StreamExt};
use raddle::wire::WireMessage;
use raddle_host::{Host, HostError, RestartPolicy, ServiceFailure};
use raddle_postgres::{DeliveryKind, Error, FailureDisposition, RetryPolicy, Scope, Store};
use tokio::sync::{Mutex, watch};

use crate::scoped::{self, Failure, Flush, ScopedHandler};
use crate::{BoxFuture, Cancellation, MessageContext, TrustedOrigin};

type Handler = Box<
    dyn Fn(Scope<'static>, MessageContext) -> BoxFuture<'static, Result<(), FailureDisposition>>
        + Send
        + Sync,
>;

/// Idempotent pause/resume of one durable receiving block, not the entire store.
#[derive(Clone)]
pub struct DurableControl(watch::Sender<bool>);
impl DurableControl {
    /// Stops new claims; active root transactions finish their current attempt.
    pub fn pause(&self) {
        self.0.send_replace(true);
    }
    /// Resumes claims without starting another worker.
    pub fn resume(&self) {
        self.0.send_replace(false);
    }
    /// Explicit receiving pause state.
    pub fn is_paused(&self) -> bool {
        *self.0.borrow()
    }
}

/// One named durable queue and typed handler registry. Independent subscriptions
/// never steal each other's leases. All business writes, inbox completion and
/// returned outbox/local child intents commit on the same SQLx connection.
pub struct DurableEndpoint {
    store: Store,
    name: String,
    handlers: BTreeMap<(String, u32), Handler>,
    policy: RetryPolicy,
    timeout: Duration,
    lease_timeout: Duration,
    parallelism: u32,
    control: DurableControl,
    receiving: Mutex<()>,
    bus: Option<crate::MessageBus>,
}

impl DurableEndpoint {
    /// Creates an endpoint with separate claim/execution admission. The global
    /// worker retry policy does not replace its 60s default handler deadline.
    pub fn new(
        store: Store,
        name: &str,
        parallelism: u32,
        policy: RetryPolicy,
    ) -> Result<Self, Error> {
        raddle::SubscriptionName::try_new(name)
            .map_err(|_| Error::Invalid("invalid endpoint name"))?;
        if !(1..=256).contains(&parallelism) {
            return Err(Error::Invalid("invalid endpoint parallelism"));
        }
        Ok(Self {
            store,
            name: name.to_owned(),
            handlers: BTreeMap::new(),
            policy,
            timeout: Duration::from_secs(60),
            lease_timeout: Duration::from_secs(60),
            parallelism,
            control: DurableControl(watch::channel(false).0),
            receiving: Mutex::new(()),
            bus: None,
        })
    }
    /// Sets the global pre-commit budget before typed handlers are registered.
    pub fn timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        if !self.handlers.is_empty()
            || timeout < Duration::from_millis(1)
            || timeout > Duration::from_secs(3600)
        {
            return Err(Error::Invalid("invalid or late endpoint timeout"));
        }
        self.timeout = timeout;
        self.lease_timeout = timeout;
        Ok(())
    }
    /// Controls one receiving block; repeated pause/resume never creates workers.
    pub fn control(&self) -> DurableControl {
        self.control.clone()
    }

    /// Binds the same frozen application routes used for durable admission.
    /// Handlers can return `context.returned(...)` instead of assembling aliases.
    pub fn message_bus(&mut self, bus: crate::MessageBus) {
        self.bus = Some(bus);
    }

    /// Registers a scoped handler with default root failure classification.
    /// Wire failures reject; other root failures retry. Post-commit errors never
    /// enter this retry classifier because the parent is already completed.
    pub fn handle<M, H>(&mut self, handler: Arc<H>, flush: Arc<dyn Flush>) -> Result<(), Error>
    where
        M: WireMessage,
        H: ScopedHandler<M>,
    {
        self.handle_with_policy::<M, H, _>(handler, flush, self.timeout, |failure| {
            if matches!(failure, Failure::Wire(_)) {
                FailureDisposition::Reject
            } else {
                FailureDisposition::Retry
            }
        })
    }

    /// Registers a per-message deadline and explicit typed failure classifier.
    /// This is the policy integration boundary; it has no runtime service lookup.
    pub fn handle_with_policy<M, H, F>(
        &mut self,
        handler: Arc<H>,
        flush: Arc<dyn Flush>,
        timeout: Duration,
        classify: F,
    ) -> Result<(), Error>
    where
        M: WireMessage,
        H: ScopedHandler<M>,
        F: Fn(&Failure<H::Error>) -> FailureDisposition + Send + Sync + 'static,
    {
        let key = (M::NAME.to_owned(), M::VERSION);
        raddle::routing::WireMessageType::of::<M>()
            .map_err(|_| Error::Invalid("invalid wire contract"))?;
        if timeout < Duration::from_millis(1)
            || timeout > Duration::from_secs(3600)
            || self.handlers.contains_key(&key)
            || self.handlers.len() >= 1024
        {
            return Err(Error::Invalid(
                "invalid/duplicate durable handler or timeout",
            ));
        }
        let classify = Arc::new(classify);
        self.handlers.insert(
            key,
            Box::new(move |scope, context| {
                let handler = Arc::clone(&handler);
                let flush = Arc::clone(&flush);
                let classify = Arc::clone(&classify);
                Box::pin(async move {
                    scoped::run::<M, H>(
                        handler,
                        scope,
                        context,
                        tokio::time::Instant::now() + timeout,
                        flush,
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| classify(&error))
                })
            }),
        );
        self.lease_timeout = self.lease_timeout.max(timeout);
        Ok(())
    }

    /// Executes one endpoint-local batch, joining every independent result.
    /// Infrastructure failures are reported only after healthy siblings finish.
    pub async fn run_once(&self) -> Result<usize, Error> {
        self.run_batch(Cancellation::new()).await
    }

    async fn run_batch(&self, cancellation: Cancellation) -> Result<usize, Error> {
        let _receiving = self.receiving.lock().await;
        if self.control.is_paused() || cancellation.is_cancelled() {
            return Ok(0);
        }
        let claims = self
            .store
            .claim_for(
                DeliveryKind::Local,
                &self.name,
                self.parallelism,
                self.lease_timeout + Duration::from_secs(5),
                Utc::now(),
            )
            .await?;
        let count = claims.len();
        let results = stream::iter(claims)
            .map(|claim| {
                let shutdown = cancellation.clone();
                let cancellation = cancellation.child();
                async move {
                    let envelope = claim.envelope()?;
                    let Some(handler) = self
                        .handlers
                        .get(&(envelope.contract().to_owned(), envelope.version()))
                    else {
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
                    let origin =
                        TrustedOrigin::authenticated(claim.tenant(), claim.source(), None)?;
                    let mut context = MessageContext::new(origin, envelope, Utc::now())
                        .with_cancellation(cancellation.clone());
                    if let Some(bus) = &self.bus {
                        context = context.with_bus(bus.clone());
                    }
                    let scope = self.store.execution(&claim, Utc::now()).await?;
                    match handler(scope, context).await {
                        Ok(()) => Ok(()),
                        Err(_) if shutdown.is_cancelled() => Ok(()), // retain recoverable lease, not a fabricated terminal failure
                        Err(disposition) => {
                            self.store
                                .fail(
                                    &claim,
                                    disposition,
                                    self.policy,
                                    "handler-failed",
                                    Utc::now(),
                                )
                                .await
                        }
                    }
                }
            })
            .buffer_unordered(self.parallelism as usize)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result?;
        }
        Ok(count)
    }

    /// Attaches the receiving owner. Host stop cancels unfinished pre-commit
    /// phases, joins Finally/disposal, and leaves leases recoverable. Buffered
    /// queues instead drain their accepted memory jobs to completion.
    pub fn attach(self, host: &mut Host, interval: Duration) -> Result<(), HostError> {
        if interval.is_zero() {
            return Err(HostError::Invalid("zero durable poll interval"));
        }
        let name = self.name.clone();
        let endpoint = Arc::new(self);
        host.service(&name, RestartPolicy::NEVER, move |mut context| {
            let endpoint = Arc::clone(&endpoint);
            async move {
                context.set_ready(true);
                loop {
                    let cancellation = Cancellation::new();
                    let batch = endpoint.run_batch(cancellation.clone());
                    tokio::pin!(batch);
                    tokio::select! {
                        biased;
                        () = context.cancelled() => {
                            cancellation.cancel();
                            batch.await.map_err(|_| ServiceFailure::Transient)?;
                            return Ok(());
                        }
                        result = &mut batch => { result.map_err(|_| ServiceFailure::Transient)?; }
                    }
                    tokio::select! {
                        biased;
                        () = context.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(interval) => {},
                    }
                }
            }
        })
    }
}
