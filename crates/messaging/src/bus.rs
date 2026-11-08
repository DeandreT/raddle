use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireError, WireMessage};
use raddle::{ExecutionError, Request, Runtime};
use raddle_postgres::{Delivery, DeliveryKind, Store};
use tokio::sync::oneshot;

use crate::effects::Outgoing;
use crate::scoped::{self, Committed, Failure, Flush, ScopedHandler};
use crate::{BoxFuture, LocalQueue, MessageContext, QueueError, QueueMode, TrustedOrigin};

/// Application routing/admission failure, deliberately separate from typed handler errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BusError {
    /// Send or remote invoke requires a configured route.
    #[error("message has no application route")]
    NoRoute,
    /// Remote request/reply requires exactly one configured destination.
    #[error("remote invocation has multiple destinations")]
    Ambiguous,
    /// A configured route cannot do request/reply or remote invocation is disabled.
    #[error("remote invocation is disabled or unavailable")]
    RemoteDisabled,
    /// A remote peer reported handler failure (not a successful reply).
    #[error("remote handler failed")]
    RemoteFailure,
    /// Underlying confirmed transport error; acceptance may be uncertain.
    #[error("message transport failed")]
    Transport(#[from] TransportError),
    /// Invalid/duplicate/unsupported configuration; no partial registration is applied.
    #[error("invalid message bus configuration: {0}")]
    Invalid(&'static str),
    /// A return intent cannot atomically use an in-memory queue.
    #[error("atomic returned messages require durable routes")]
    NonAtomicRoute,
    /// Local queue admission failed.
    #[error("local queue admission failed")]
    Queue(#[from] QueueError),
    /// Message encoding/decoding failed.
    #[error("message envelope failed validation")]
    Wire(#[from] WireError),
    /// Durable staging failed; the enclosing transaction was rolled back.
    #[error("durable message admission failed")]
    Storage(#[from] raddle_postgres::Error),
}

/// Local domain failures stay typed; remote failures are acknowledgements, not deserialized exceptions.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InvokeError<E: std::error::Error + 'static> {
    /// Original typed local execution result.
    #[error("local invocation failed: {0}")]
    Local(#[from] ExecutionError<E>),
    /// Routing, remote negative acknowledgement or transport error.
    #[error("application invocation failed: {0}")]
    Bus(#[from] BusError),
    /// Total budget (including admission and middleware) expired.
    #[error("application invocation deadline expired")]
    TimedOut,
    /// The explicit invocation signal was cancelled.
    #[error("application invocation cancelled")]
    Cancelled,
}

/// Overrides for one invocation. Total execution defaults to 60 seconds; remote
/// request/reply waits at most 5 seconds unless explicitly overridden.
#[derive(Clone, Copy, Debug)]
pub struct InvokeOptions {
    /// Total admission/handler budget; None selects type/global default.
    pub timeout: Option<Duration>,
    /// Remote request/reply budget (also bounded by the total budget).
    pub remote_timeout: Duration,
    /// Explicitly forbid falling back to a remote peer.
    pub allow_remote: bool,
}
impl Default for InvokeOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            remote_timeout: Duration::from_secs(5),
            allow_remote: true,
        }
    }
}

/// Public request/reply transport adapter. Implementations correlate and clean up
/// late replies, authenticate responders, and return `RemoteFailure` for a negative
/// acknowledgement. Dropping this future must release its response registration.
pub trait RemoteInvoker: Send + Sync + 'static {
    /// Uses configured aliases only, never an inbound response URI as authority.
    fn request<'a>(
        &'a self,
        destination: &'a str,
        context: &'a MessageContext,
        reply_contract: &'a str,
    ) -> BoxFuture<'a, Result<Envelope, BusError>>;
}

trait Publisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        destination: &'a str,
        envelope: &'a Envelope,
    ) -> BoxFuture<'a, Result<(), TransportError>>;
}
struct TypedPublisher<T>(T);
impl<T: Transport> Publisher for TypedPublisher<T> {
    fn publish<'a>(
        &'a self,
        destination: &'a str,
        envelope: &'a Envelope,
    ) -> BoxFuture<'a, Result<(), TransportError>> {
        Box::pin(self.0.publish(destination, envelope))
    }
}

enum Endpoint {
    Local(LocalQueue),
    Durable(Store),
    Remote(Arc<dyn Publisher>),
}
type Key = (String, u32);
type Handler = Arc<
    dyn Fn(MessageContext, tokio::time::Instant) -> BoxFuture<'static, Result<(), QueueError>>
        + Send
        + Sync,
>;

/// Typed registration shared by routed Send/Publish and scoped invocation. Its
/// handler/resources are never retrieved through an untyped service locator.
pub struct ScopedRegistration<M, H> {
    handler: Arc<H>,
    store: Store,
    flush: Arc<dyn Flush>,
    marker: std::marker::PhantomData<fn(M)>,
}

/// Finite explicit registration; no service locator, scanning, reflection or code generation.
pub struct BusBuilder {
    runtime: Runtime,
    endpoints: BTreeMap<String, Endpoint>,
    routes: BTreeMap<Key, Vec<String>>,
    handlers: BTreeMap<(String, Key), Handler>,
    remote: Option<Arc<dyn RemoteInvoker>>,
    timeout: Duration,
    timeouts: BTreeMap<Key, Duration>,
    allow_remote: bool,
    propagated: BTreeSet<String>,
    execution: Option<LocalQueue>,
}

impl BusBuilder {
    /// Keeps the low-level runtime's registration/typed domain results unchanged.
    pub fn new(runtime: Runtime) -> Self {
        Self {
            runtime,
            endpoints: BTreeMap::new(),
            routes: BTreeMap::new(),
            handlers: BTreeMap::new(),
            remote: None,
            timeout: Duration::from_secs(60),
            timeouts: BTreeMap::new(),
            allow_remote: true,
            propagated: BTreeSet::new(),
            execution: None,
        }
    }
    fn endpoint(&mut self, name: &str, endpoint: Endpoint) -> Result<(), BusError> {
        raddle::SubscriptionName::try_new(name)
            .map_err(|_| BusError::Invalid("invalid endpoint name"))?;
        if self.endpoints.len() >= 256 || self.endpoints.contains_key(name) {
            return Err(BusError::Invalid("duplicate endpoint or capacity exceeded"));
        }
        self.endpoints.insert(name.to_owned(), endpoint);
        Ok(())
    }
    /// Adds an inline/buffered named local queue. Attach its runner to the host separately.
    pub fn local(&mut self, name: &str, queue: LocalQueue) -> Result<(), BusError> {
        self.endpoint(name, Endpoint::Local(queue))
    }
    /// Adds PostgreSQL custody, using the alias as an independent subscription.
    /// Register the matching handler on `DurableEndpoint` and attach it to the host.
    pub fn durable(&mut self, name: &str, store: Store) -> Result<(), BusError> {
        self.endpoint(name, Endpoint::Durable(store))
    }
    /// Adds a confirmed transport adapter. Encryption can wrap this typed Transport boundary.
    pub fn remote<T: Transport + 'static>(
        &mut self,
        name: &str,
        transport: T,
    ) -> Result<(), BusError> {
        self.endpoint(name, Endpoint::Remote(Arc::new(TypedPublisher(transport))))
    }
    /// Sets the public remote invocation adapter and global remote fallback switch.
    pub fn remote_invoker(&mut self, remote: Arc<dyn RemoteInvoker>, enabled: bool) {
        self.remote = Some(remote);
        self.allow_remote = enabled;
    }
    /// Sets a global deadline, validated before freezing configuration.
    pub fn timeout(&mut self, timeout: Duration) -> Result<(), BusError> {
        validate_timeout(timeout)?;
        self.timeout = timeout;
        Ok(())
    }
    /// Sets an exact stable contract override, not a diagnostic type-name rule.
    pub fn message_timeout<M: WireMessage>(&mut self, timeout: Duration) -> Result<(), BusError> {
        validate_timeout(timeout)?;
        self.timeouts.insert(key::<M>()?, timeout);
        Ok(())
    }
    /// Allowlists one application header for outgoing correlation propagation.
    pub fn propagate_header(&mut self, name: &str) -> Result<(), BusError> {
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(BusError::Invalid("invalid propagated header name"));
        }
        if self.propagated.len() >= 32 {
            return Err(BusError::Invalid("header capacity exceeded"));
        }
        self.propagated.insert(name.to_owned());
        Ok(())
    }
    /// Adds one exact contract-to-endpoint route, with at most 16 destinations.
    pub fn route<M: WireMessage>(&mut self, endpoint: &str) -> Result<(), BusError> {
        let key = key::<M>()?;
        if !self.endpoints.contains_key(endpoint) {
            return Err(BusError::Invalid("unknown endpoint"));
        }
        if self.routes.len() >= 1024 && !self.routes.contains_key(&key) {
            return Err(BusError::Invalid("route capacity exceeded"));
        }
        let routes = self.routes.entry(key).or_default();
        if routes.len() >= 16 || routes.iter().any(|route| route == endpoint) {
            return Err(BusError::Invalid("duplicate route or fanout exceeded"));
        }
        routes.push(endpoint.to_owned());
        Ok(())
    }
    /// Explicit typed local handler. Returned failures affect only this queued job.
    pub fn handle<M, F, Fut>(&mut self, endpoint: &str, handler: F) -> Result<(), BusError>
    where
        M: WireMessage,
        F: Fn(M, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), QueueError>> + Send + 'static,
    {
        if !matches!(self.endpoints.get(endpoint), Some(Endpoint::Local(_))) {
            return Err(BusError::Invalid("handler needs a local endpoint"));
        }
        let key = (endpoint.to_owned(), key::<M>()?);
        if self.handlers.contains_key(&key) {
            return Err(BusError::Invalid("duplicate handler"));
        }
        let handler = Arc::new(handler);
        self.handlers.insert(
            key,
            Arc::new(move |context, deadline| {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    let message = context
                        .envelope()
                        .decode::<M>()
                        .map_err(|_| QueueError::Handler)?;
                    let cancellation = context.cancellation().clone();
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => Err(QueueError::Cancelled),
                        _ = tokio::time::sleep_until(deadline) => { cancellation.cancel(); Err(QueueError::Handler) },
                        result = handler(message, context) => result,
                    }
                })
            }),
        );
        Ok(())
    }
    /// Registers a scoped transactional handler on a buffered endpoint. Routed
    /// messages automatically execute phases and returned work; the typed handle
    /// invokes exactly the same registration and preserves its reply/error types.
    pub fn scoped<M, H>(
        &mut self,
        endpoint: &str,
        handler: Arc<H>,
        store: Store,
        flush: Arc<dyn Flush>,
    ) -> Result<ScopedRegistration<M, H>, BusError>
    where
        M: WireMessage,
        H: ScopedHandler<M>,
    {
        if !matches!(self.endpoints.get(endpoint), Some(Endpoint::Local(queue)) if queue.mode() == QueueMode::Buffered)
        {
            return Err(BusError::Invalid(
                "scoped routes require managed buffered execution",
            ));
        }
        let key = (endpoint.to_owned(), key::<M>()?);
        if self.handlers.contains_key(&key) {
            return Err(BusError::Invalid("duplicate handler"));
        }
        let registration = ScopedRegistration {
            handler: Arc::clone(&handler),
            store: store.clone(),
            flush: Arc::clone(&flush),
            marker: std::marker::PhantomData,
        };
        self.handlers.insert(
            key,
            Arc::new(move |context, deadline| {
                let handler = Arc::clone(&handler);
                let store = store.clone();
                let flush = Arc::clone(&flush);
                Box::pin(async move {
                    let scope = store.begin().await.map_err(|_| QueueError::Handler)?;
                    scoped::run::<M, H>(handler, scope, context, deadline, flush)
                        .await
                        .map(|_| ())
                        .map_err(|error| {
                            if matches!(error, Failure::Cancelled) {
                                QueueError::Cancelled
                            } else {
                                QueueError::Handler
                            }
                        })
                })
            }),
        );
        Ok(registration)
    }

    /// Designates a separate bounded host-owned admission queue for scoped Invoke.
    pub fn execution_queue(&mut self, queue: LocalQueue) -> Result<(), BusError> {
        if queue.mode() != QueueMode::Buffered {
            return Err(BusError::Invalid(
                "scoped execution requires managed buffering",
            ));
        }
        self.execution = Some(queue);
        Ok(())
    }
    /// Freezes registrations. This opens no connections and starts no tasks.
    pub fn build(self) -> MessageBus {
        MessageBus(Arc::new(self))
    }
}

/// Application bus. Configuration is immutable and all asynchronous local work is
/// owned by explicit host-attached queues. No global singleton is introduced.
#[derive(Clone)]
pub struct MessageBus(Arc<BusBuilder>);

impl std::fmt::Debug for MessageBus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MessageBus")
            .field("endpoints", &self.0.endpoints.len())
            .field("routes", &self.0.routes.len())
            .finish_non_exhaustive()
    }
}

impl MessageBus {
    /// Local-first typed invocation. Remote serialization is opt-in through WireMessage.
    pub async fn invoke<M>(
        &self,
        message: M,
        origin: TrustedOrigin,
    ) -> Result<M::Reply, InvokeError<M::Error>>
    where
        M: Request + WireMessage,
        M::Reply: WireMessage,
    {
        self.invoke_with(message, origin, InvokeOptions::default())
            .await
    }

    /// Uses a whole-operation deadline, including local admission/middleware or remote I/O.
    pub async fn invoke_with<M>(
        &self,
        message: M,
        origin: TrustedOrigin,
        options: InvokeOptions,
    ) -> Result<M::Reply, InvokeError<M::Error>>
    where
        M: Request + WireMessage,
        M::Reply: WireMessage,
    {
        let context = MessageContext::new(
            origin,
            Envelope::encode(&message).map_err(BusError::from)?,
            Utc::now(),
        );
        self.invoke_context(message, context, options).await
    }

    /// Receives explicit validated metadata and a caller-controlled cancellation signal.
    /// The message must encode to the same contract/content as its supplied envelope.
    pub async fn invoke_context<M>(
        &self,
        message: M,
        context: MessageContext,
        options: InvokeOptions,
    ) -> Result<M::Reply, InvokeError<M::Error>>
    where
        M: Request + WireMessage,
        M::Reply: WireMessage,
    {
        // Compare typed application content only: ingress owns metadata, not a
        // newly encoded envelope. This also validates the exact declared contract.
        let supplied = context.envelope().decode::<M>().map_err(BusError::from)?;
        if Envelope::encode(&supplied)
            .map_err(BusError::from)?
            .content_bytes()
            .map_err(BusError::from)?
            != Envelope::encode(&message)
                .map_err(BusError::from)?
                .content_bytes()
                .map_err(BusError::from)?
        {
            return Err(BusError::Invalid("message and context differ").into());
        }
        let timeout = options.timeout.unwrap_or(self.deadline::<M>()?);
        validate_timeout(timeout)?;
        validate_timeout(options.remote_timeout)?;
        let execution = async {
            if self.0.runtime.has_request::<M>() {
                return self
                    .0
                    .runtime
                    .invoke(message)
                    .await
                    .map_err(InvokeError::Local);
            }
            if !options.allow_remote || !self.0.allow_remote {
                return Err(BusError::RemoteDisabled.into());
            }
            let routes = self.0.routes.get(&key::<M>()?).ok_or(BusError::NoRoute)?;
            if routes.len() != 1 {
                return Err(BusError::Ambiguous.into());
            }
            let remote = self.0.remote.as_ref().ok_or(BusError::RemoteDisabled)?;
            if !matches!(self.0.endpoints.get(&routes[0]), Some(Endpoint::Remote(_))) {
                return Err(BusError::RemoteDisabled.into());
            }
            let reply = tokio::time::timeout(
                options.remote_timeout,
                remote.request(&routes[0], &context, M::Reply::NAME),
            )
            .await
            .map_err(|_| InvokeError::TimedOut)??;
            reply
                .decode::<M::Reply>()
                .map_err(|error| BusError::Wire(error).into())
        };
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => Err(InvokeError::Cancelled),
            result = tokio::time::timeout(timeout, execution) => result.unwrap_or(Err(InvokeError::TimedOut)),
        }
    }

    /// Enqueues routed work; no route is an error. Local buffered handlers are not awaited.
    pub async fn send<M: WireMessage>(
        &self,
        message: &M,
        origin: TrustedOrigin,
    ) -> Result<(), BusError> {
        let context = MessageContext::new(origin, Envelope::encode(message)?, Utc::now());
        self.dispatch(context, true).await
    }
    /// Same application routing as Send; no subscriber is a successful no-op.
    pub async fn publish<M: WireMessage>(
        &self,
        message: &M,
        origin: TrustedOrigin,
    ) -> Result<(), BusError> {
        let context = MessageContext::new(origin, Envelope::encode(message)?, Utc::now());
        self.dispatch(context, false).await
    }

    /// Dispatches an ingress/contextual envelope. Trusted identity remains separate.
    /// Fanout admissions are independent; failure of one destination does not undo
    /// already accepted siblings. Use returned intents for required SQL atomicity.
    pub async fn dispatch(
        &self,
        context: MessageContext,
        require_route: bool,
    ) -> Result<(), BusError> {
        let context = context.with_bus(self.clone());
        let key = (
            context.envelope().contract().to_owned(),
            context.envelope().version(),
        );
        let Some(routes) = self.0.routes.get(&key) else {
            return if require_route {
                Err(BusError::NoRoute)
            } else {
                Ok(())
            };
        };
        let mut failure = None;
        for destination in routes {
            let result = async {
                match &self.0.endpoints[destination] {
                    Endpoint::Local(queue) => {
                        let handler = Arc::clone(
                            self.0
                                .handlers
                                .get(&(destination.clone(), key.clone()))
                                .ok_or(BusError::Invalid("local route has no handler"))?,
                        );
                        // Custody creates an independent execution. A timeout in
                        // one subscriber must not cancel another subscriber.
                        let context = context
                            .clone()
                            .with_cancellation(crate::Cancellation::new());
                        let timeout = self.0.timeouts.get(&key).copied().unwrap_or(self.0.timeout);
                        let deadline = tokio::time::Instant::now() + timeout;
                        queue
                            .submit(async move { handler(context, deadline).await })
                            .await?;
                    }
                    Endpoint::Durable(store) => {
                        let mut scope = store.begin().await?;
                        scope
                            .stage_received(
                                context.origin().source(),
                                context.origin().tenant(),
                                context.envelope(),
                                &[Delivery::new(DeliveryKind::Local, destination, Utc::now())],
                            )
                            .await?;
                        scope.commit(Utc::now()).await?;
                    }
                    Endpoint::Remote(transport) => {
                        transport.publish(destination, context.envelope()).await?
                    }
                }
                Ok::<(), BusError>(())
            }
            .await;
            if let Err(error) = result
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// Resolves returned messages using the same route table as Send/Publish.
    /// Rejects in-memory routes rather than pretending they can join SQL commit.
    pub fn intent<M: WireMessage>(
        &self,
        message: &M,
        context: &MessageContext,
        required: bool,
    ) -> Result<Outgoing, BusError> {
        let envelope = context.outgoing(Envelope::encode(message)?, &self.0.propagated)?;
        let mut deliveries = Vec::new();
        if let Some(routes) = self.0.routes.get(&key::<M>()?) {
            for destination in routes {
                let kind = match &self.0.endpoints[destination] {
                    Endpoint::Local(_) => return Err(BusError::NonAtomicRoute),
                    Endpoint::Durable(_) => DeliveryKind::Local,
                    Endpoint::Remote(_) => DeliveryKind::Remote,
                };
                deliveries.push(Delivery::new(kind, destination, context.now()));
            }
        } else if required {
            return Err(BusError::NoRoute);
        }
        Ok(Outgoing {
            envelope,
            deliveries,
        })
    }

    pub(crate) fn deadline<M: WireMessage>(&self) -> Result<Duration, BusError> {
        Ok(self
            .0
            .timeouts
            .get(&key::<M>()?)
            .copied()
            .unwrap_or(self.0.timeout))
    }

    /// Offline inspection of the effective global/per-message execution budget.
    pub fn timeout_for<M: WireMessage>(&self) -> Result<Duration, BusError> {
        self.deadline::<M>()
    }

    /// Executes a typed scoped handler under host ownership. Dropping the caller
    /// signals cancellation; the host still drives Finally and async disposal.
    /// The result distinguishes a committed root from a later flush/callback error.
    pub async fn execute<M, H>(
        &self,
        handler: Arc<H>,
        context: MessageContext,
        store: Store,
        flush: Arc<dyn Flush>,
    ) -> Result<Committed<H::Reply>, Failure<H::Error>>
    where
        M: WireMessage,
        H: ScopedHandler<M>,
    {
        let context = context.with_bus(self.clone());
        let queue = self.0.execution.as_ref().ok_or(Failure::Closed)?;
        let timeout = self.deadline::<M>().map_err(|_| Failure::Closed)?;
        let deadline = tokio::time::Instant::now() + timeout;
        let (sender, receiver) = oneshot::channel();
        let _guard = CancelOnDrop(context.cancellation().clone());
        let cancellation = context.cancellation().clone();
        let job_cancellation = cancellation.clone();
        let submit = queue.submit(async move {
            if job_cancellation.is_cancelled() {
                let _ = sender.send(Err(Failure::Cancelled));
                return Err(QueueError::Cancelled);
            }
            let result = match store.begin().await {
                Ok(scope) => scoped::run::<M, H>(handler, scope, context, deadline, flush).await,
                Err(error) => Err(Failure::Database(error)),
            };
            let outcome = match &result {
                Ok(_) => Ok(()),
                Err(Failure::Cancelled) => Err(QueueError::Cancelled),
                Err(_) => Err(QueueError::Handler),
            };
            let _ = sender.send(result);
            outcome
        });
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(Failure::Cancelled),
            _ = tokio::time::sleep_until(deadline) => return Err(Failure::TimedOut),
            result = submit => result.map_err(|_| Failure::Closed)?,
        }
        // Once admitted, let the owner report the real transaction outcome.
        // A caller timeout during commit/flush must not masquerade as rollback.
        // The pre-commit deadline still includes the time spent in this queue;
        // Finally/disposal and an in-progress SQL commit are deliberately joined.
        receiver.await.map_err(|_| Failure::Closed)?
    }
}

impl MessageBus {
    /// Typed scoped invocation through a registration also used by routed
    /// Send/Publish. Unlike ordinary Runtime invocation, returned effects run
    /// automatically in the root transaction before the successful response.
    pub async fn invoke_scoped<M, H>(
        &self,
        registration: &ScopedRegistration<M, H>,
        context: MessageContext,
    ) -> Result<Committed<H::Reply>, Failure<H::Error>>
    where
        M: WireMessage,
        H: ScopedHandler<M>,
    {
        self.execute::<M, H>(
            Arc::clone(&registration.handler),
            context,
            registration.store.clone(),
            Arc::clone(&registration.flush),
        )
        .await
    }
}

struct CancelOnDrop(crate::Cancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn key<M: WireMessage>() -> Result<Key, BusError> {
    raddle::routing::WireMessageType::of::<M>()
        .map_err(|_| BusError::Invalid("invalid contract"))?;
    Ok((M::NAME.to_owned(), M::VERSION))
}
fn validate_timeout(timeout: Duration) -> Result<(), BusError> {
    if timeout < Duration::from_millis(1) || timeout > Duration::from_secs(3600) {
        return Err(BusError::Invalid("deadline outside 1ms..=1h"));
    }
    Ok(())
}
