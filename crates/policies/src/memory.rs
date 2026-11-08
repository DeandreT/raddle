use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use raddle::wire::Envelope;
use tokio::sync::watch;

use crate::driver;
use crate::{
    AttemptCounters, CircuitAdmission, CircuitBreaker, Direction, DriverError, Execution,
    ExecutionMode, PolicyError, PolicyExecutor, PolicyExecutorDriver, Transition,
};

/// Explicit repeat-effect authorization. Nothing infers this from a handler.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReplaySafety {
    /// Do not repeat potentially non-idempotent effects.
    #[default]
    Never,
    /// Application promises a stable idempotency key guards repeated effects.
    Idempotent,
    /// Application rolls back all effects before returning a failed attempt.
    Transactional,
    /// Sending may duplicate accepted work; receiver must tolerate duplicates.
    AtLeastOnceSend,
}
impl ReplaySafety {
    pub(crate) fn permits(self, direction: Direction) -> bool {
        match self {
            Self::Never => false,
            Self::AtLeastOnceSend => direction == Direction::Sending,
            Self::Idempotent | Self::Transactional => true,
        }
    }
}

/// Per-delivery custody and attempt state, independent of successful siblings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryStatus {
    /// Queued and waiting for its due time.
    Ready,
    /// Handler or publisher owns a live claim.
    Active,
    /// Confirmed publication or successful handler execution.
    Completed,
    /// Shutdown interrupted an attempt. Custody is retained, but resumption
    /// requires explicit authorization because external effects may be uncertain.
    Interrupted,
    /// Deliberate terminal discard.
    Discarded,
    /// Terminal quarantine, optionally because repeated effects were forbidden.
    DeadLetter {
        /// Quarantined rather than replaying unauthorized effects.
        replay_denied: bool,
    },
}

/// An independently scheduled work item; Debug never exposes body or headers.
#[derive(Clone, Debug)]
pub struct MemoryDelivery {
    /// Original envelope, unchanged by policy execution.
    pub envelope: Envelope,
    /// Independent sending/handling attempts.
    pub attempts: AttemptCounters,
    /// Current custody state.
    pub status: MemoryStatus,
    /// Monotonic time when next eligible.
    pub due: Duration,
    /// Explicit repeat authorization, defaulting to no retries.
    pub replay: ReplaySafety,
}
impl MemoryDelivery {
    /// Constructs a ready delivery with no implicit replay authorization.
    pub fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            attempts: AttemptCounters::default(),
            status: MemoryStatus::Ready,
            due: Duration::ZERO,
            replay: ReplaySafety::Never,
        }
    }
}

/// Real bounded, volatile endpoint driver. One owned active claim at a time;
/// completed state is retained, so correcting one failed delivery never replays
/// successful siblings. It is not durable and spawns no background task.
pub struct InMemoryEndpoint {
    name: String,
    direction: Direction,
    capacity: usize,
    deliveries: Vec<MemoryDelivery>,
    queue: VecDeque<usize>,
    active: Option<usize>,
    paused_until: Duration,
    circuit: Option<CircuitBreaker>,
    quotas: Option<Arc<crate::ScopedQuotas>>,
    jitter: Box<dyn FnMut() -> Result<u64, DriverError> + Send>,
}

enum AttemptOutcome {
    Completed(Result<(), PolicyError>),
    Limited(Option<Duration>),
    Cancelled,
}
impl InMemoryEndpoint {
    /// Capacity bounds queued and retained terminal deliveries together.
    pub fn new(
        name: impl Into<String>,
        direction: Direction,
        capacity: usize,
    ) -> Result<Self, DriverError> {
        let name = name.into();
        raddle::SubscriptionName::try_new(name.clone()).map_err(|_| DriverError::Invalid)?;
        if capacity == 0 || capacity > 1_000_000 {
            return Err(DriverError::Invalid);
        }
        Ok(Self {
            name,
            direction,
            capacity,
            deliveries: Vec::new(),
            queue: VecDeque::new(),
            active: None,
            paused_until: Duration::ZERO,
            circuit: None,
            quotas: None,
            jitter: Box::new(|| {
                let mut bytes = [0; 8];
                getrandom::fill(&mut bytes).map_err(|_| DriverError::Invalid)?;
                Ok(u64::from_le_bytes(bytes))
            }),
        })
    }
    /// Installs endpoint-local receive sampling; sending uses sending policies.
    pub fn with_circuit(mut self, circuit: CircuitBreaker) -> Result<Self, DriverError> {
        if self.direction != Direction::Handling {
            return Err(DriverError::Invalid);
        }
        self.circuit = Some(circuit);
        Ok(self)
    }

    /// Installs immutable startup-owned, in-process admission policies. Limits
    /// are checked before beginning attempts; no handler executes when limited.
    pub fn with_quotas(mut self, quotas: Arc<crate::ScopedQuotas>) -> Self {
        self.quotas = Some(quotas);
        self
    }
    /// Overrides the OS-random jitter sampler, e.g. for deterministic tests.
    /// Samples cover the full u64 domain; no pending work is reconfigured.
    pub fn with_jitter_source(
        mut self,
        source: impl FnMut() -> Result<u64, DriverError> + Send + 'static,
    ) -> Self {
        self.jitter = Box::new(source);
        self
    }
    /// Enqueues one validated identity. Duplicate identities are refused.
    pub fn enqueue(&mut self, delivery: MemoryDelivery) -> Result<(), DriverError> {
        if self.deliveries.len() == self.capacity
            || delivery.status != MemoryStatus::Ready
            || self
                .deliveries
                .iter()
                .any(|d| d.envelope.id() == delivery.envelope.id())
        {
            return Err(DriverError::Custody);
        }
        delivery
            .envelope
            .validate(Default::default())
            .map_err(|_| DriverError::Invalid)?;
        self.queue.push_back(self.deliveries.len());
        self.deliveries.push(delivery);
        Ok(())
    }
    /// Redacted envelope Debug keeps diagnostic inspection payload-free.
    pub fn deliveries(&self) -> &[MemoryDelivery] {
        &self.deliveries
    }
    /// The next eligible queue identity in actual queue-tail order. An endpoint
    /// pause or active claim makes no additional work eligible.
    pub fn next_ready(&self, now: Duration) -> Option<&MemoryDelivery> {
        if self.active.is_some() || now < self.paused_until {
            return None;
        }
        self.queue.iter().find_map(|index| {
            let delivery = &self.deliveries[*index];
            (delivery.due <= now).then_some(delivery)
        })
    }
    /// Places an already-ready delivery behind newly merged admissions without
    /// changing its due time or counters. Managed receivers use this after
    /// merging buffered custody accepted during the previous active operation.
    pub fn move_ready_to_tail(&mut self, id: uuid::Uuid) -> Result<(), DriverError> {
        if self.active.is_some() {
            return Err(DriverError::Custody);
        }
        let position = self
            .queue
            .iter()
            .position(|index| {
                self.deliveries[*index].envelope.id() == id
                    && self.deliveries[*index].status == MemoryStatus::Ready
            })
            .ok_or(DriverError::Custody)?;
        let index = self.queue.remove(position).ok_or(DriverError::Custody)?;
        self.queue.push_back(index);
        Ok(())
    }
    /// Quarantines the next eligible item after application wire/body validation
    /// failed. This bypasses ordinary handler policies and attempt counters.
    pub async fn reject_next(&mut self, now: Duration) -> Result<(), DriverError> {
        let id = self
            .next_ready(now)
            .map(|d| d.envelope.id())
            .ok_or(DriverError::Custody)?;
        let position = self
            .queue
            .iter()
            .position(|index| self.deliveries[*index].envelope.id() == id)
            .ok_or(DriverError::Custody)?;
        let index = self.queue.remove(position).ok_or(DriverError::Custody)?;
        self.active = Some(index);
        self.deliveries[index].status = MemoryStatus::Active;
        self.transition(Transition::DeadLetter {
            replay_denied: false,
        })
        .await
    }
    /// Explicit operator retry affects only a failed item, retaining attempts.
    pub fn retry_dead_letter(&mut self, id: uuid::Uuid, now: Duration) -> Result<(), DriverError> {
        let index = self
            .deliveries
            .iter()
            .position(|d| d.envelope.id() == id)
            .ok_or(DriverError::Custody)?;
        let delivery = &mut self.deliveries[index];
        if !matches!(delivery.status, MemoryStatus::DeadLetter { .. }) {
            return Err(DriverError::Custody);
        }
        delivery.status = MemoryStatus::Ready;
        delivery.due = now;
        self.queue.push_back(index);
        Ok(())
    }

    /// Explicitly authorizes resumption after cancellation. Never infers that
    /// interrupted non-idempotent effects were rolled back.
    pub fn resume_interrupted(
        &mut self,
        id: uuid::Uuid,
        now: Duration,
        replay: ReplaySafety,
    ) -> Result<(), DriverError> {
        let index = self
            .deliveries
            .iter()
            .position(|d| d.envelope.id() == id)
            .ok_or(DriverError::Custody)?;
        let delivery = &mut self.deliveries[index];
        if delivery.status != MemoryStatus::Interrupted || !replay.permits(self.direction) {
            return Err(DriverError::Invalid);
        }
        delivery.replay = replay;
        delivery.status = MemoryStatus::Ready;
        delivery.due = now;
        self.queue.push_back(index);
        Ok(())
    }

    /// Recovers the owned claim after the `run_once` future was dropped without
    /// a stop signal. No effects are replayed; explicit resumption is required.
    pub fn recover_interrupted(&mut self) -> Result<(), DriverError> {
        let index = self.active.take().ok_or(DriverError::Custody)?;
        self.deliveries[index].status = MemoryStatus::Interrupted;
        if let Some(circuit) = &mut self.circuit {
            circuit.cancel_probe();
        }
        Ok(())
    }
    /// Settles a received unknown contract without invoking an ordinary handler
    /// policy or incrementing handling attempts. Original bytes must identify
    /// an already-enqueued ready delivery; exact bytes reach the tracker.
    pub async fn handle_unknown(
        &mut self,
        handler: &mut crate::UnknownMessageHandler,
        original: &[u8],
        now: Duration,
    ) -> Result<crate::UnknownMessageReport, DriverError> {
        if self.active.is_some() {
            return Err(DriverError::Custody);
        }
        let incoming =
            Envelope::from_slice(original, Default::default()).map_err(|_| DriverError::Invalid)?;
        let position = self
            .queue
            .iter()
            .position(|index| {
                self.deliveries[*index].envelope.id() == incoming.id()
                    && self.deliveries[*index].due <= now
            })
            .ok_or(DriverError::Custody)?;
        let index = self.queue[position];
        if incoming.to_vec().map_err(|_| DriverError::Invalid)?
            != self.deliveries[index]
                .envelope
                .to_vec()
                .map_err(|_| DriverError::Invalid)?
        {
            return Err(DriverError::Invalid);
        }
        self.queue.remove(position);
        self.active = Some(index);
        self.deliveries[index].status = MemoryStatus::Active;
        handler.handle(self, original).await
    }
    /// Executes at most one queued item (plus explicitly authorized inline
    /// retries). Cancellation drops the running future and retains the item.
    /// Closures receive owned wire copies, not mutable endpoint messages. If
    /// this future itself is dropped, call `recover_interrupted` before reuse.
    pub async fn run_once<F, Fut>(
        &mut self,
        executor: &PolicyExecutor,
        now: Duration,
        stop: &mut watch::Receiver<bool>,
        mut operation: F,
    ) -> Result<bool, DriverError>
    where
        F: FnMut(Envelope) -> Fut + Send,
        Fut: Future<Output = Result<(), PolicyError>> + Send,
    {
        self.run_managed(executor, now, stop, false, move |envelope| {
            let operation = operation(envelope);
            async move { Ok(operation.await) }
        })
        .await
    }

    /// Managed application boundary: join the operation's cancellation cleanup
    /// instead of dropping it. The owner must propagate stop into that operation.
    /// An outer driver error retains uncertain work without applying a handler
    /// policy or sampling the circuit. An inner error is a typed handler failure.
    /// Successful commit wins over a concurrent stop; it is never replayed.
    /// Quota admission uses the quota-owned clock shared across managed owners;
    /// the supplied clock still controls this endpoint's scheduling and circuit.
    pub async fn run_joined<F, Fut>(
        &mut self,
        executor: &PolicyExecutor,
        now: Duration,
        stop: &mut watch::Receiver<bool>,
        operation: F,
    ) -> Result<bool, DriverError>
    where
        F: FnMut(Envelope) -> Fut + Send,
        Fut: Future<Output = Result<Result<(), PolicyError>, DriverError>> + Send,
    {
        self.run_managed(executor, now, stop, true, operation).await
    }

    async fn run_managed<F, Fut>(
        &mut self,
        executor: &PolicyExecutor,
        now: Duration,
        stop: &mut watch::Receiver<bool>,
        joined: bool,
        mut operation: F,
    ) -> Result<bool, DriverError>
    where
        F: FnMut(Envelope) -> Fut + Send,
        Fut: Future<Output = Result<Result<(), PolicyError>, DriverError>> + Send,
    {
        if driver::stopped(stop) || now < self.paused_until {
            return Ok(false);
        }
        if self.active.is_some() {
            return Err(DriverError::Custody);
        }
        let Some(position) = self
            .queue
            .iter()
            .position(|i| self.deliveries[*i].due <= now)
        else {
            return Ok(false);
        };
        let index = self.queue.remove(position).ok_or(DriverError::Custody)?;
        self.active = Some(index);
        self.deliveries[index].status = MemoryStatus::Active;
        let quota = self
            .quotas
            .as_ref()
            .and_then(|quotas| {
                quotas.select(&self.name, self.deliveries[index].envelope.contract())
            })
            .cloned();
        let started = tokio::time::Instant::now();
        let mut executed = false;
        loop {
            let current = now
                .checked_add(started.elapsed())
                .ok_or(DriverError::Invalid)?;
            if driver::stopped(stop) {
                if let Some(circuit) = &mut self.circuit {
                    circuit.cancel_probe();
                }
                self.transition(Transition::Retain).await?;
                return Ok(executed);
            }
            if current < self.paused_until {
                self.transition(Transition::QueueAt(self.paused_until))
                    .await?;
                return Ok(executed);
            }
            if let Some(circuit) = &mut self.circuit
                && let CircuitAdmission::Paused(until) = circuit.admission(current)
            {
                self.transition(Transition::QueueAt(until)).await?;
                return Ok(executed);
            }
            let outcome = tokio::select! {
                biased;
                _ = driver::wait_for_stop(stop), if !joined => AttemptOutcome::Cancelled,
                outcome = async {
                    // Admission belongs to each actual call, not to the outer
                    // delivery/retry loop. Release concurrency admission before
                    // recovery/cooldown; never acquire a second permit while
                    // retaining the first one.
                    let _permit = match &quota {
                        Some(quota) => {
                            let (admission, quota_now) = if joined {
                                quota.acquire_current().map_err(|_| DriverError::Invalid)?
                            } else { (quota.acquire(current).map_err(|_| DriverError::Invalid)?, current) };
                            match admission {
                            crate::QuotaAdmission::Granted(permit) => Some(permit),
                            crate::QuotaAdmission::Limited { retry_at } => {
                                let retry_at = if joined {
                                    let local_now = now.checked_add(started.elapsed()).ok_or(DriverError::Invalid)?;
                                    retry_at.map(|due| local_now.checked_add(due.saturating_sub(quota_now)).ok_or(DriverError::Invalid)).transpose()?
                                } else { retry_at };
                                return Ok(AttemptOutcome::Limited(retry_at));
                            }
                        }},
                        None => None,
                    };
                    self.deliveries[index].attempts.begin(self.direction).map_err(|_| DriverError::Invalid)?;
                    executed = true;
                    let result = match operation(self.deliveries[index].envelope.clone()).await {
                        Ok(result) => result,
                        Err(error) => {
                            if let Some(circuit) = &mut self.circuit { circuit.cancel_probe(); }
                            self.transition(Transition::Retain).await?;
                            return Err(error);
                        }
                    };
                    Ok::<_, DriverError>(AttemptOutcome::Completed(result))
                } => outcome?,
            };
            let result = match outcome {
                AttemptOutcome::Completed(result) => result,
                AttemptOutcome::Limited(retry_at) => {
                    if let Some(circuit) = &mut self.circuit {
                        circuit.cancel_probe();
                    }
                    let due = retry_at.unwrap_or(current);
                    if retry_at.is_some() {
                        self.paused_until = self.paused_until.max(due);
                    }
                    self.transition(Transition::QueueAt(due)).await?;
                    return Ok(executed);
                }
                AttemptOutcome::Cancelled => {
                    if let Some(circuit) = &mut self.circuit {
                        circuit.cancel_probe();
                    }
                    self.transition(Transition::Retain).await?;
                    return Ok(executed);
                }
            };
            if result.is_err() && driver::stopped(stop) {
                if let Some(circuit) = &mut self.circuit {
                    circuit.cancel_probe();
                }
                self.transition(Transition::Retain).await?;
                return Ok(executed);
            }
            let current = now
                .checked_add(started.elapsed())
                .ok_or(DriverError::Invalid)?;
            if let Some(circuit) = &mut self.circuit
                && let Some(until) = circuit
                    .record(
                        current,
                        result
                            .as_ref()
                            .err()
                            .map(|e| &**e as &(dyn std::error::Error + 'static)),
                        false,
                    )
                    .map_err(|_| DriverError::Invalid)?
            {
                self.paused_until = self.paused_until.max(until);
            }
            match result {
                Ok(()) => {
                    self.deliveries[index].status = MemoryStatus::Completed;
                    self.active = None;
                    return Ok(true);
                }
                Err(error) => {
                    let endpoint = self.name.clone();
                    let contract = self.deliveries[index].envelope.contract().to_owned();
                    let context = Execution {
                        endpoint: &endpoint,
                        contract: &contract,
                        direction: self.direction,
                        attempts: self.deliveries[index].attempts,
                        replay: self.deliveries[index].replay,
                        mode: ExecutionMode::Queued,
                        now: current,
                        jitter_sample: (self.jitter)()?,
                    };
                    let transition = executor.failed(self, context, &*error, stop).await?;
                    if transition != Transition::RetryInline {
                        return Ok(true);
                    }
                    // An explicitly indefinite immediate policy still yields
                    // to shutdown and other endpoint work on the executor.
                    tokio::task::yield_now().await;
                    if driver::stopped(stop) {
                        self.transition(Transition::Retain).await?;
                        return Ok(true);
                    }
                }
            }
        }
    }
}
impl PolicyExecutorDriver for InMemoryEndpoint {
    async fn transition(&mut self, transition: Transition) -> Result<(), DriverError> {
        let index = self.active.ok_or(DriverError::Custody)?;
        let delivery = &mut self.deliveries[index];
        match transition {
            Transition::RetryInline => return Ok(()),
            Transition::QueueAt(due) => {
                delivery.status = MemoryStatus::Ready;
                delivery.due = due;
                self.queue.push_back(index);
            }
            Transition::Retain => {
                delivery.status = MemoryStatus::Interrupted;
            }
            Transition::StopInline => {
                delivery.status = MemoryStatus::Ready;
                self.queue.push_front(index);
            }
            Transition::Discard => delivery.status = MemoryStatus::Discarded,
            Transition::DeadLetter { replay_denied } => {
                delivery.status = MemoryStatus::DeadLetter { replay_denied }
            }
        }
        self.active = None;
        Ok(())
    }
    async fn pause_until(&mut self, until: Duration) -> Result<(), DriverError> {
        self.paused_until = self.paused_until.max(until);
        Ok(())
    }
}
