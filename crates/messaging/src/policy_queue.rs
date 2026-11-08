//! Sequential, bounded volatile policy queues owned by the application Host.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::FutureExt;

use raddle::wire::WireMessage;
use raddle_host::{Host, HostError, RestartPolicy, ServiceFailure};
use raddle_policies::{
    Direction, DriverError, InMemoryEndpoint, MemoryDelivery, PolicyError, ReplaySafety,
    UnknownMessageHandler,
};
use raddle_postgres::Store;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use uuid::Uuid;

use crate::policy::AttemptFailure;
use crate::scoped::{self, Flush, ScopedHandler};
use crate::{BoxFuture, Cancellation, EndpointPolicies, MessageContext};

type Key = (String, u32);
type Handler =
    Arc<dyn Fn(MessageContext) -> BoxFuture<'static, Result<(), AttemptFailure>> + Send + Sync>;
struct Registered {
    handler: Handler,
    replay: ReplaySafety,
    valid: fn(&raddle::wire::Envelope) -> bool,
}
type Registry = BTreeMap<Key, Registered>;
type IdentityIndex = BTreeMap<Uuid, (crate::TrustedOrigin, Vec<u8>)>;
type SharedIdentities = Arc<StdMutex<IdentityIndex>>;

enum Command {
    Admit(Box<MessageContext>, OwnedSemaphorePermit),
    Retry(Uuid, oneshot::Sender<Result<(), DriverError>>),
    Resume(Uuid, ReplaySafety, oneshot::Sender<Result<(), DriverError>>),
}

/// Frozen sending and inspection handle. Capacity includes retained completed,
/// failed and interrupted identities, not just waiting work. Process exit loses
/// all custody; no broker acknowledgement or durability claim is implied.
#[derive(Clone)]
pub struct PolicyQueue {
    name: String,
    sender: mpsc::Sender<Command>,
    capacity: Arc<Semaphore>,
    state: Arc<StdMutex<Vec<MemoryDelivery>>>,
    identities: SharedIdentities,
}
impl PolicyQueue {
    /// Explicit configured name; bus aliases must match it.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Nonblocking bounded memory custody. A full retained-identity budget is an
    /// explicit refusal rather than an indefinitely waiting hidden backlog.
    pub async fn submit(&self, context: MessageContext) -> Result<(), DriverError> {
        context
            .envelope()
            .validate(Default::default())
            .map_err(|_| DriverError::Invalid)?;
        let bytes = context
            .envelope()
            .to_vec()
            .map_err(|_| DriverError::Invalid)?;
        let id = context.envelope().id();
        let mut identities = self.identities.lock().map_err(|_| DriverError::Custody)?;
        if let Some((origin, original)) = identities.get(&id) {
            return if origin == context.origin() && *original == bytes {
                Ok(())
            } else {
                Err(DriverError::Invalid)
            };
        }
        let permit = Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| DriverError::Custody)?;
        let origin = context.origin().clone();
        self.sender
            .try_send(Command::Admit(Box::new(context), permit))
            .map_err(|_| DriverError::Custody)?;
        identities.insert(id, (origin, bytes));
        Ok(())
    }
    /// Retained delivery states with redacted envelope Debug.
    pub fn deliveries(&self) -> Result<Vec<MemoryDelivery>, DriverError> {
        Ok(self.state.lock().map_err(|_| DriverError::Custody)?.clone())
    }
    /// Operator retry only for the specified dead letter, never its siblings.
    pub async fn retry_dead_letter(&self, id: Uuid) -> Result<(), DriverError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Retry(id, sender))
            .await
            .map_err(|_| DriverError::Custody)?;
        receiver.await.map_err(|_| DriverError::Custody)?
    }
    /// Explicit authorization for interrupted, potentially uncertain effects.
    pub async fn resume_interrupted(
        &self,
        id: Uuid,
        replay: ReplaySafety,
    ) -> Result<(), DriverError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Resume(id, replay, sender))
            .await
            .map_err(|_| DriverError::Custody)?;
        receiver.await.map_err(|_| DriverError::Custody)?
    }
}

/// Explicit typed error/handler registration before the queue is frozen.
/// This receiving block is sequential; configure independent named queues for
/// independent parallelism. No future, worker or connection starts at registration.
pub struct PolicyQueueBuilder {
    name: String,
    capacity: usize,
    timeout: Duration,
    policies: EndpointPolicies,
    handlers: Registry,
    unknown: UnknownMessageHandler,
}
impl PolicyQueueBuilder {
    /// Constructs one application-owned queue. Each attempt has its own budget.
    pub fn new(
        name: &str,
        capacity: usize,
        timeout: Duration,
        policies: EndpointPolicies,
    ) -> Result<Self, DriverError> {
        raddle::SubscriptionName::try_new(name).map_err(|_| DriverError::Invalid)?;
        if !(1..=65536).contains(&capacity)
            || !(Duration::from_millis(1)..=Duration::from_secs(3600)).contains(&timeout)
        {
            return Err(DriverError::Invalid);
        }
        Ok(Self {
            name: name.to_owned(),
            capacity,
            timeout,
            policies,
            handlers: BTreeMap::new(),
            unknown: UnknownMessageHandler::new(Default::default(), Default::default())
                .map_err(|_| DriverError::Invalid)?,
        })
    }
    /// Configures unknown-message observers/disposition independently of retries.
    pub fn unknown_messages(&mut self, unknown: UnknownMessageHandler) {
        self.unknown = unknown;
    }
    fn register<M: WireMessage>(
        &mut self,
        handler: Handler,
        replay: ReplaySafety,
    ) -> Result<(), DriverError> {
        raddle::routing::WireMessageType::of::<M>().map_err(|_| DriverError::Invalid)?;
        let key = (M::NAME.to_owned(), M::VERSION);
        if self.handlers.len() >= 1024
            || self.handlers.contains_key(&key)
            || replay == ReplaySafety::AtLeastOnceSend
        {
            return Err(DriverError::Invalid);
        }
        self.handlers.insert(
            key,
            Registered {
                handler,
                replay,
                valid: |envelope| envelope.decode::<M>().is_ok(),
            },
        );
        Ok(())
    }
    /// Registers the exact application error type; message, endpoint and global
    /// filters inspect it directly, never a Display string or erased queue error.
    pub fn handle<M, E, F, Fut>(
        &mut self,
        handler: F,
        replay: ReplaySafety,
    ) -> Result<(), DriverError>
    where
        M: WireMessage,
        E: std::error::Error + Send + Sync + 'static,
        F: Fn(M, MessageContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let timeout = self.timeout;
        self.register::<M>(Arc::new(move |context| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let message = context.envelope().decode::<M>().map_err(|_| AttemptFailure::InvalidInput)?;
                tokio::select! {
                    biased;
                    () = context.cancellation().cancelled() => Err(AttemptFailure::Cancelled),
                    _ = tokio::time::sleep(timeout) => { context.cancellation().cancel(); Err(AttemptFailure::Application(Box::new(PolicyTimeout))) },
                    result = AssertUnwindSafe(async { handler(message, context.clone()).await }).catch_unwind() => match result {
                        Ok(result) => result.map_err(|error| AttemptFailure::Application(Box::new(error))),
                        Err(_) => Err(AttemptFailure::Application(Box::new(scoped::Failure::<E>::Panicked))),
                    },
                }
            })
        }), replay)
    }
    /// Runs the original scoped pipeline with fresh resources/SQL per attempt.
    /// Uncertain commit/rollback retains only that delivery for explicit
    /// reconciliation; shared scope-admission errors still fail the service.
    /// Post-commit errors never replay the parent.
    pub fn scoped<M, H>(
        &mut self,
        handler: Arc<H>,
        store: Store,
        flush: Arc<dyn Flush>,
        replay: ReplaySafety,
    ) -> Result<(), DriverError>
    where
        M: WireMessage,
        H: ScopedHandler<M>,
    {
        let timeout = self.timeout;
        self.register::<M>(
            Arc::new(move |context| {
                let handler = Arc::clone(&handler);
                let store = store.clone();
                let flush = Arc::clone(&flush);
                Box::pin(async move {
                    let deadline = tokio::time::Instant::now() + timeout;
                    let scope = store
                        .begin()
                        .await
                        .map_err(AttemptFailure::Infrastructure)?;
                    scoped::run_strict::<M, H>(handler, scope, context, deadline, flush)
                        .await
                        .map(|_| ())
                        .map_err(AttemptFailure::scoped)
                })
            }),
            replay,
        )
    }
    /// Freezes configuration and returns the bus handle plus its sole Host owner.
    pub fn build(mut self) -> Result<(PolicyQueue, PolicyQueueRunner), DriverError> {
        let mut endpoint =
            InMemoryEndpoint::new(self.name.clone(), Direction::Handling, self.capacity)?;
        if let Some(circuit) = self.policies.admission.get_mut().circuit.take() {
            endpoint = endpoint.with_circuit(circuit)?;
        }
        if let Some(quotas) = self.policies.quotas {
            endpoint = endpoint.with_quotas(quotas);
        }
        let (sender, receiver) = mpsc::channel(self.capacity);
        let state = Arc::new(StdMutex::new(Vec::new()));
        let identities = Arc::new(StdMutex::new(BTreeMap::new()));
        let queue = PolicyQueue {
            name: self.name,
            sender,
            capacity: Arc::new(Semaphore::new(self.capacity)),
            state: Arc::clone(&state),
            identities: Arc::clone(&identities),
        };
        let runner = PolicyQueueRunner {
            queue: endpoint,
            executor: self.policies.executor,
            handlers: self.handlers,
            receiver,
            contexts: BTreeMap::new(),
            state,
            unknown: self.unknown,
            identities,
        };
        Ok((queue, runner))
    }
}

/// Typed local policy timeout, distinct from shutdown cancellation.
#[derive(Debug, thiserror::Error)]
#[error("local policy attempt timed out")]
pub struct PolicyTimeout;

/// Sole owned receiving block; cannot be started twice or silently rebuilt empty.
pub struct PolicyQueueRunner {
    queue: InMemoryEndpoint,
    executor: raddle_policies::PolicyExecutor,
    handlers: Registry,
    receiver: mpsc::Receiver<Command>,
    contexts: BTreeMap<Uuid, (MessageContext, OwnedSemaphorePermit)>,
    state: Arc<StdMutex<Vec<MemoryDelivery>>>,
    unknown: UnknownMessageHandler,
    identities: SharedIdentities,
}
impl PolicyQueueRunner {
    /// Attaches actual queued execution. Stop cancels pre-commit phases and joins
    /// Finally/disposal; unfinished states remain in the final snapshot rather
    /// than being acknowledged. Forced Host grace expiry cannot promise cleanup.
    pub fn attach(self, host: &mut Host, name: &str) -> Result<(), HostError> {
        let runner = Arc::new(Mutex::new(Some(self)));
        host.service(name, RestartPolicy::NEVER, move |mut context| {
            let runner = Arc::clone(&runner);
            async move {
                let mut runner = runner.lock().await.take().ok_or(ServiceFailure::Permanent)?;
                let shutdown = Cancellation::new();
                let (sender, mut stop) = watch::channel(false);
                let clock = tokio::time::Instant::now();
                context.set_ready(true);
                loop {
                    while let Ok(command) = runner.receiver.try_recv() { runner.command(command, clock.elapsed()).map_err(|_| ServiceFailure::Transient)?; }
                    let result = {
                        let run = runner.once(clock.elapsed(), &mut stop, &shutdown);
                        tokio::pin!(run);
                        tokio::select! {
                        biased;
                        () = context.cancelled() => { shutdown.cancel(); sender.send_replace(true); run.await }
                        result = &mut run => result,
                        }
                    };
                    result.map_err(|_| ServiceFailure::Transient)?;
                    if shutdown.is_cancelled() { break; }
                    tokio::select! {
                        biased;
                        () = context.cancelled() => { shutdown.cancel(); sender.send_replace(true); break; }
                        command = runner.receiver.recv() => match command { Some(command) => runner.command(command, clock.elapsed()).map_err(|_| ServiceFailure::Transient)?, None => break },
                        _ = tokio::time::sleep(Duration::from_millis(1)) => {},
                    }
                }
                runner.receiver.close();
                while let Some(command) = runner.receiver.recv().await { runner.command(command, clock.elapsed()).map_err(|_| ServiceFailure::Transient)?; }
                runner.snapshot().map_err(|_| ServiceFailure::Transient)?;
                Ok(())
            }
        })
    }
    fn command(&mut self, command: Command, now: Duration) -> Result<(), DriverError> {
        match command {
            Command::Admit(context, permit) => {
                let id = context.envelope().id();
                let replay = self
                    .handlers
                    .get(&(
                        context.envelope().contract().to_owned(),
                        context.envelope().version(),
                    ))
                    .map(|registered| registered.replay)
                    .unwrap_or_default();
                let mut delivery = MemoryDelivery::new(context.envelope().clone());
                delivery.replay = replay;
                // Duplicate admission never destroys an already accepted sibling.
                if self.contexts.contains_key(&id) {
                    return Ok(());
                }
                self.queue.enqueue(delivery)?;
                self.contexts.insert(id, (*context, permit));
            }
            Command::Retry(id, response) => {
                let _ = response.send(self.queue.retry_dead_letter(id, now));
            }
            Command::Resume(id, replay, response) => {
                let _ = response.send(self.queue.resume_interrupted(id, now, replay));
            }
        }
        self.snapshot()
    }
    fn snapshot(&self) -> Result<(), DriverError> {
        *self.state.lock().map_err(|_| DriverError::Custody)? = self.queue.deliveries().to_vec();
        Ok(())
    }
    async fn once(
        &mut self,
        now: Duration,
        stop: &mut watch::Receiver<bool>,
        shutdown: &Cancellation,
    ) -> Result<(), DriverError> {
        let next = self.queue.next_ready(now);
        let active_id = next.map(|delivery| delivery.envelope.id());
        if next.is_some_and(|delivery| {
            self.handlers
                .get(&(
                    delivery.envelope.contract().to_owned(),
                    delivery.envelope.version(),
                ))
                .is_some_and(|registered| !(registered.valid)(&delivery.envelope))
        }) {
            self.queue.reject_next(now).await?;
            return self.snapshot();
        }
        if let Some(delivery) = next
            && !self.handlers.contains_key(&(
                delivery.envelope.contract().to_owned(),
                delivery.envelope.version(),
            ))
        {
            let original = delivery
                .envelope
                .to_vec()
                .map_err(|_| DriverError::Invalid)?;
            self.queue
                .handle_unknown(&mut self.unknown, &original, now)
                .await?;
            return self.snapshot();
        }
        let handlers = &self.handlers;
        let contexts = &self.contexts;
        // run_joined already retains outer errors without a policy/circuit
        // sample. Distinguish this one typed outcome from shared infrastructure
        // failure without changing the lower-level driver's error contract.
        let uncertain = AtomicBool::new(false);
        let observed_uncertain = &uncertain;
        let mut result = self
            .queue
            .run_joined(&self.executor, now, stop, |envelope| {
                let entry = contexts.get(&envelope.id());
                let handler = handlers.get(&(envelope.contract().to_owned(), envelope.version()));
                let message_id = envelope.id();
                async move {
                    let (context, _) = entry.ok_or(DriverError::Custody)?;
                    let registered = handler.ok_or(DriverError::Invalid)?;
                    let context = context.clone().with_cancellation(shutdown.child());
                    let outcome = AssertUnwindSafe(async { (registered.handler)(context).await })
                        .catch_unwind()
                        .await
                        .map_err(|_| DriverError::Custody)?;
                    match outcome {
                        Ok(()) => Ok(Ok(())),
                        Err(AttemptFailure::Application(error)) => Ok(Err(error)),
                        Err(AttemptFailure::Uncertain(error)) => {
                            tracing::error!(%message_id, error = %error,
                                "policy transaction uncertain; retaining memory custody pending explicit reconciliation");
                            observed_uncertain.store(true, Ordering::Relaxed);
                            Err(DriverError::Custody)
                        }
                        // A failed fresh scope admission or other shared error is
                        // still reported by the Host, never blanket-suppressed.
                        Err(AttemptFailure::Infrastructure(_)) => Err(DriverError::Custody),
                        Err(AttemptFailure::InvalidInput) => Err(DriverError::Invalid),
                        Err(AttemptFailure::Cancelled) => {
                            Ok(Err(Box::new(Cancelled) as PolicyError))
                        }
                    }
                }
            })
            .await;
        if uncertain.load(Ordering::Relaxed)
            && matches!(result, Err(DriverError::Custody))
            && active_id.is_some_and(|id| {
                self.queue.deliveries().iter().any(|delivery| {
                    delivery.envelope.id() == id
                        && delivery.status == raddle_policies::MemoryStatus::Interrupted
                })
            })
        {
            // Only suppress the typed uncertain outcome AFTER the driver has
            // actually retained it. Any failed retention/state error propagates.
            result = Ok(true);
        }
        if result.is_err() {
            let _ = self.queue.recover_interrupted();
        }
        // Admission acknowledgements can arrive through the bounded channel
        // while this owner executes a handler. Merge that accepted frontier
        // before giving a recovered item its actual application queue-tail slot.
        let identities = Arc::clone(&self.identities);
        let _admission = identities.lock().map_err(|_| DriverError::Custody)?;
        let pending = self.receiver.len();
        for _ in 0..pending {
            if let Ok(command) = self.receiver.try_recv() {
                self.command(command, now)?;
            }
        }
        if let Some(id) = active_id
            && self.queue.deliveries().iter().any(|delivery| {
                delivery.envelope.id() == id
                    && delivery.status == raddle_policies::MemoryStatus::Ready
            })
        {
            self.queue.move_ready_to_tail(id)?;
        }
        self.snapshot()?;
        result.map(|_| ())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("managed policy execution cancelled")]
struct Cancelled;
