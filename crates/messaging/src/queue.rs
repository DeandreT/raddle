use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use futures_util::FutureExt;
use futures_util::stream::{FuturesUnordered, StreamExt};
use raddle::sequencing::SlotSet;
use raddle::wire::Envelope;
use raddle_host::{Host, HostError, RestartPolicy, ServiceContext, ServiceFailure};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, watch};

use crate::BoxFuture;

/// The local handoff boundary. Durable queues are PostgreSQL subscriptions,
/// configured separately through `BusBuilder::durable`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueueMode {
    /// Runs in the sender's future; completion is the admission acknowledgement.
    Inline,
    /// Acknowledges bounded memory custody before execution; process exit can lose work.
    Buffered,
}

/// Independent memory-admission and active-execution bounds for one named queue.
#[derive(Clone, Copy, Debug)]
pub struct QueueSettings {
    /// Inline or host-owned buffered execution.
    pub mode: QueueMode,
    /// Maximum waiting jobs (not active execution count).
    pub capacity: usize,
    /// Maximum active jobs; one provides sequential execution.
    pub parallelism: usize,
}

/// Admission/execution result without user payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum QueueError {
    /// The queue has stopped admitting work.
    #[error("local queue is closed")]
    Closed,
    /// No valid bounded queue can be constructed.
    #[error("invalid local queue settings")]
    Invalid,
    /// One job failed; siblings are still executed.
    #[error("local message execution failed")]
    Handler,
    /// Explicit caller/host cancellation, not a handler or sending failure.
    #[error("local message execution cancelled")]
    Cancelled,
}

struct State {
    accepting: AtomicBool,
    active: AtomicUsize,
    completed: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    paused: watch::Sender<bool>,
}

/// Independently sampled payload-free queue observations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueSnapshot {
    /// Whether new work can be accepted.
    pub accepting: bool,
    /// Explicit receiving/execution pause.
    pub paused: bool,
    /// Waiting buffered jobs; inline has no buffer.
    pub buffered: usize,
    /// Active handlers, bounded independently of buffering.
    pub active: usize,
    /// Successful jobs since this process constructed the queue.
    pub completed: u64,
    /// Failed jobs; not an automatic retry count.
    pub failed: u64,
    /// Explicitly cancelled jobs, separate from success and handler failures.
    pub cancelled: u64,
}

type Operation = BoxFuture<'static, Result<(), QueueError>>;
struct Job {
    operation: Operation,
    slot: Option<usize>,
    waiting: OwnedSemaphorePermit,
}
struct BufferedPermit {
    channel: mpsc::OwnedPermit<Job>,
    waiting: OwnedSemaphorePermit,
    slot: Option<usize>,
}
impl BufferedPermit {
    fn send(self, operation: Operation) {
        self.channel.send(Job {
            operation,
            slot: self.slot,
            waiting: self.waiting,
        });
    }
}

/// Cloneable sending/control handle; no task is created by constructing a queue.
#[derive(Clone)]
pub struct LocalQueue {
    sender: mpsc::Sender<Job>,
    state: Arc<State>,
    settings: QueueSettings,
    inline_admission: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    slots: Option<Arc<SlotSet>>,
}

/// Single ownership of the receiving block prevents simultaneous/double starts.
pub struct QueueRunner {
    receiver: mpsc::Receiver<Job>,
    state: Arc<State>,
    parallelism: usize,
    waiting: Arc<Semaphore>,
    slots: Option<Arc<SlotSet>>,
}

impl LocalQueue {
    /// Creates the sending handle and its sole receiving owner.
    pub fn new(settings: QueueSettings) -> Result<(Self, QueueRunner), QueueError> {
        Self::create(settings, None)
    }

    /// Creates a buffered queue sharing finite execution ownership. Inline is refused.
    pub fn partitioned(
        settings: QueueSettings,
        slots: Arc<SlotSet>,
    ) -> Result<(Self, QueueRunner), QueueError> {
        if settings.mode != QueueMode::Buffered {
            return Err(QueueError::Invalid);
        }
        Self::create(settings, Some(slots))
    }

    fn create(
        settings: QueueSettings,
        slots: Option<Arc<SlotSet>>,
    ) -> Result<(Self, QueueRunner), QueueError> {
        if !(1..=65536).contains(&settings.capacity) || !(1..=256).contains(&settings.parallelism) {
            return Err(QueueError::Invalid);
        }
        let (sender, receiver) = mpsc::channel(settings.capacity);
        let waiting = Arc::new(Semaphore::new(settings.capacity));
        let state = Arc::new(State {
            accepting: AtomicBool::new(true),
            active: AtomicUsize::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            paused: watch::channel(false).0,
        });
        Ok((
            Self {
                sender,
                state: Arc::clone(&state),
                settings,
                inline_admission: Arc::new(Semaphore::new(settings.parallelism)),
                waiting: Arc::clone(&waiting),
                slots: slots.clone(),
            },
            QueueRunner {
                receiver,
                state,
                parallelism: settings.parallelism,
                waiting,
                slots,
            },
        ))
    }

    /// Current mode; scoped executions require host-managed Buffered custody.
    pub fn mode(&self) -> QueueMode {
        self.settings.mode
    }

    /// Stops taking buffered work into execution, without dropping accepted work.
    /// Inline execution has no receiving block, so pause is rejected explicitly.
    pub fn pause(&self) -> Result<(), QueueError> {
        if self.settings.mode == QueueMode::Inline {
            return Err(QueueError::Invalid);
        }
        self.state.paused.send_replace(true);
        Ok(())
    }

    /// Idempotently resumes a paused buffered receiver.
    pub fn resume(&self) {
        self.state.paused.send_replace(false);
    }

    /// Samples queue counters, not a transactional cross-counter snapshot.
    pub fn snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            accepting: self.state.accepting.load(Ordering::Acquire)
                && (self.settings.mode == QueueMode::Inline || !self.sender.is_closed()),
            paused: *self.state.paused.borrow(),
            buffered: self.settings.capacity - self.waiting.available_permits(),
            active: self.state.active.load(Ordering::Acquire),
            completed: self.state.completed.load(Ordering::Relaxed),
            failed: self.state.failed.load(Ordering::Relaxed),
            cancelled: self.state.cancelled.load(Ordering::Relaxed),
        }
    }

    /// Waits for bounded admission, not handler completion in buffered mode.
    /// Dropping a waiting submit does not enqueue a job. Once admitted, the host
    /// owns its future and joins its cleanup even if the sender disappears.
    pub async fn submit<F>(&self, job: F) -> Result<(), QueueError>
    where
        F: Future<Output = Result<(), QueueError>> + Send + 'static,
    {
        if self.slots.is_some() {
            return Err(QueueError::Invalid);
        }
        self.submit_class(None, job).await
    }

    /// Admits a canonical envelope to this buffered slot owner. Exemption is an
    /// explicit application decision; it affects execution only, never custody.
    pub async fn submit_envelope<F>(
        &self,
        envelope: &Envelope,
        tenant: &str,
        exempt: bool,
        job: F,
    ) -> Result<(), QueueError>
    where
        F: Future<Output = Result<(), QueueError>> + Send + 'static,
    {
        if tenant.is_empty() || tenant.len() > 128 {
            return Err(QueueError::Invalid);
        }
        envelope
            .require_plaintext()
            .map_err(|_| QueueError::Invalid)?;
        let slots = self.slots.as_ref().ok_or(QueueError::Invalid)?;
        let dispatch = envelope
            .dispatch()
            .filter(|d| d.is_resolved())
            .ok_or(QueueError::Invalid)?;
        let slot = (!exempt).then(|| slots.slot(tenant, dispatch.group(), envelope.id()));
        self.submit_class(slot, job).await
    }

    pub(crate) fn is_partitioned(&self) -> bool {
        self.slots.is_some()
    }
    pub(crate) fn slot_set(&self) -> Option<&Arc<SlotSet>> {
        self.slots.as_ref()
    }

    async fn submit_class<F>(&self, slot: Option<usize>, job: F) -> Result<(), QueueError>
    where
        F: Future<Output = Result<(), QueueError>> + Send + 'static,
    {
        if !self.state.accepting.load(Ordering::Acquire) {
            return Err(QueueError::Closed);
        }
        if self.settings.mode == QueueMode::Inline {
            let _permit = self
                .inline_admission
                .acquire()
                .await
                .map_err(|_| QueueError::Closed)?;
            if !self.state.accepting.load(Ordering::Acquire) {
                return Err(QueueError::Closed);
            }
            // Its caller owns cancellation, but admission is independently bounded.
            return measured(Arc::clone(&self.state), Box::pin(job)).await;
        }
        let mut permit = self.reserve_buffered().await?;
        permit.slot = slot;
        permit.send(Box::pin(job));
        Ok(())
    }

    async fn reserve_buffered(&self) -> Result<BufferedPermit, QueueError> {
        let waiting = Arc::clone(&self.waiting)
            .acquire_owned()
            .await
            .map_err(|_| QueueError::Closed)?;
        let permit = self
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| QueueError::Closed)?;
        if !self.state.accepting.load(Ordering::Acquire) {
            return Err(QueueError::Closed);
        }
        Ok(BufferedPermit {
            channel: permit,
            waiting,
            slot: None,
        })
    }
}

impl QueueRunner {
    /// Registers a single-use host service. Restarting a consumed in-memory
    /// receiver is forbidden rather than silently losing its accepted jobs.
    pub fn attach(self, host: &mut Host, name: &str) -> Result<(), HostError> {
        let runner = Arc::new(Mutex::new(Some(self)));
        host.service(name, RestartPolicy::NEVER, move |context| {
            let runner = Arc::clone(&runner);
            async move {
                let runner = runner
                    .lock()
                    .await
                    .take()
                    .ok_or(ServiceFailure::Permanent)?;
                runner.run(context).await
            }
        })
    }

    /// Runs until host shutdown, then closes admission and drains queued and
    /// active work, ignoring pause during drain. The host enforces its grace bound.
    pub async fn run(mut self, mut context: ServiceContext) -> Result<(), ServiceFailure> {
        let _running = Running(Arc::clone(&self.state));
        let mut paused = self.state.paused.subscribe();
        let mut active = FuturesUnordered::new();
        let mut pending = VecDeque::<Job>::new();
        let mut changes = self.slots.as_ref().map(|slots| slots.subscribe());
        context.set_ready(true);
        loop {
            if !*paused.borrow() {
                self.launch(&mut pending, &mut active);
            }
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                _ = paused.changed() => {},
                Some(_) = active.next(), if !active.is_empty() => {},
                _ = slot_changed(&mut changes) => {},
                job = self.receiver.recv(), if !*paused.borrow() => {
                    match job {
                        Some(job) => pending.push_back(job),
                        None => break,
                    }
                }
            }
        }
        context.set_ready(false);
        self.state.accepting.store(false, Ordering::Release);
        self.waiting.close();
        self.receiver.close();
        // A sender can have passed admission while still holding a reserved
        // channel permit. Only recv(None), not an empty buffer, proves that all
        // those senders have either handed over their jobs or released custody.
        let mut receiving = true;
        while receiving || !pending.is_empty() || !active.is_empty() {
            self.launch(&mut pending, &mut active);
            tokio::select! {
                Some(_) = active.next(), if !active.is_empty() => {},
                _ = slot_changed(&mut changes) => {},
                job = self.receiver.recv(), if receiving => {
                    match job {
                        Some(job) => pending.push_back(job),
                        None => receiving = false,
                    }
                }
            }
        }
        Ok(())
    }

    fn launch(&self, pending: &mut VecDeque<Job>, active: &mut FuturesUnordered<Operation>) {
        while active.len() < self.parallelism {
            let selected = pending
                .iter()
                .enumerate()
                .find_map(|(position, job)| match job.slot {
                    Some(slot) => self
                        .slots
                        .as_ref()?
                        .try_reserve(slot)
                        .map(|lease| (position, Some(lease))),
                    None => Some((position, None)),
                });
            let Some((position, lease)) = selected else {
                break;
            };
            let Some(job) = pending.remove(position) else {
                break;
            };
            drop(job.waiting);
            let state = Arc::clone(&self.state);
            active.push(Box::pin(async move {
                let _lease = lease;
                measured(state, job.operation).await
            }));
        }
    }
}

async fn slot_changed(changes: &mut Option<watch::Receiver<u64>>) {
    match changes {
        Some(changes) => {
            let _ = changes.changed().await;
        }
        None => std::future::pending::<()>().await,
    }
}

struct Active(Arc<State>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Running(Arc<State>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.accepting.store(false, Ordering::Release);
    }
}

async fn measured(state: Arc<State>, job: Operation) -> Result<(), QueueError> {
    state.active.fetch_add(1, Ordering::AcqRel);
    let _guard = Active(Arc::clone(&state));
    let result = AssertUnwindSafe(job)
        .catch_unwind()
        .await
        .unwrap_or(Err(QueueError::Handler));
    match &result {
        Ok(()) => {
            state.completed.fetch_add(1, Ordering::Relaxed);
        }
        Err(QueueError::Cancelled) => {
            state.cancelled.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            state.failed.fetch_add(1, Ordering::Relaxed);
        }
    }
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use raddle_host::Host;
    use tokio::sync::{Notify, Semaphore};

    use super::{LocalQueue, QueueMode, QueueSettings};
    use raddle::grouping::{Dispatch, GroupId};
    use raddle::sequencing::SlotSet;
    use raddle::wire::Envelope;

    fn configured(
        partitioned: bool,
        capacity: usize,
        parallelism: usize,
    ) -> (LocalQueue, super::QueueRunner) {
        let settings = QueueSettings {
            mode: QueueMode::Buffered,
            capacity,
            parallelism,
        };
        if partitioned {
            LocalQueue::partitioned(settings, SlotSet::new("reserved", 1).unwrap()).unwrap()
        } else {
            LocalQueue::new(settings).unwrap()
        }
    }

    #[tokio::test]
    async fn partitioned_shutdown_drains_reserved_sender_and_polls_cleanup() {
        reserved_drain(true).await;
        reserved_cleanup(true).await;
    }

    #[tokio::test]
    async fn shutdown_drains_a_job_sent_through_a_previously_admitted_permit_after_close() {
        reserved_drain(false).await;
    }
    async fn reserved_drain(partitioned: bool) {
        let (queue, runner) = configured(partitioned, 2, 1);
        // Stop the real submit path between its accepting check and permit.send.
        let permit = queue.reserve_buffered().await.unwrap();
        assert!(queue.state.accepting.load(Ordering::Acquire));
        let entered = Arc::new(Notify::new());
        let started = Arc::clone(&entered);
        queue
            .submit_class(partitioned.then_some(0), async move {
                started.notify_one();
                Ok(())
            })
            .await
            .unwrap();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, "reserved").unwrap();
        let shutdown = host.handle();
        let host_task = tokio::spawn(host.run());
        entered.notified().await;
        queue.pause().unwrap();
        shutdown.shutdown();
        queue.sender.closed().await;

        let executed = Arc::new(AtomicBool::new(false));
        let ran = Arc::clone(&executed);
        permit.send(Box::pin(async move {
            ran.store(true, Ordering::Release);
            Ok(())
        }));
        host_task.await.unwrap().unwrap();
        assert!(executed.load(Ordering::Acquire));
        assert_eq!(queue.snapshot().completed, 2);
        assert_eq!(queue.snapshot().active, 0);
    }

    #[tokio::test]
    async fn shutdown_polls_active_cleanup_while_waiting_for_a_reserved_permit_to_be_released() {
        reserved_cleanup(false).await;
    }
    async fn reserved_cleanup(partitioned: bool) {
        let (queue, runner) = configured(partitioned, 2, 2);
        let permit = queue.reserve_buffered().await.unwrap();
        assert!(queue.state.accepting.load(Ordering::Acquire));
        let entered = Arc::new(Notify::new());
        let cleaned = Arc::new(Notify::new());
        let cleanup_gate = Arc::new(Semaphore::new(0));
        let started = Arc::clone(&entered);
        let finished = Arc::clone(&cleaned);
        let gate = Arc::clone(&cleanup_gate);
        queue
            .submit_class(partitioned.then_some(0), async move {
                started.notify_one();
                gate.acquire().await.unwrap().forget();
                finished.notify_one();
                Ok(())
            })
            .await
            .unwrap();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, "reserved-cleanup").unwrap();
        let shutdown = host.handle();
        let host_task = tokio::spawn(host.run());
        entered.notified().await;
        shutdown.shutdown();
        queue.sender.closed().await;
        cleanup_gate.add_permits(1);
        cleaned.notified().await;
        // Completion of active work is not completion of the closed receiver:
        // it still owes the sender a chance to release its reserved slot.
        assert!(!host_task.is_finished());
        drop(permit);
        host_task.await.unwrap().unwrap();
        assert_eq!(queue.snapshot().completed, 1);
    }

    #[tokio::test]
    async fn grouped_pending_channel_and_reserved_share_one_waiting_budget_and_cancel_safely() {
        let (queue, runner) = configured(true, 2, 2);
        let envelope = Envelope::from_parts_with_dispatch(
            raddle::wire::EnvelopeMetadata {
                id: uuid::Uuid::new_v4(),
                contract: "group.work".into(),
                version: 1,
                created_at: chrono::Utc::now(),
                correlation_id: None,
                causation_id: None,
                headers: Default::default(),
            },
            serde_json::Value::Null,
            Some(Dispatch::resolved(Some(GroupId::new("g")), None)),
            Default::default(),
        )
        .unwrap();
        let entered = Arc::new(Notify::new());
        let gate = Arc::new(Semaphore::new(0));
        let started = entered.clone();
        let held = gate.clone();
        queue
            .submit_envelope(&envelope, "tenant", false, async move {
                started.notify_one();
                held.acquire().await.unwrap().forget();
                Ok(())
            })
            .await
            .unwrap();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, "bounded").unwrap();
        let shutdown = host.handle();
        let task = tokio::spawn(host.run());
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        let mut reserved = queue.reserve_buffered().await.unwrap();
        reserved.slot = Some(0);
        queue
            .submit_envelope(&envelope, "tenant", false, async { Ok(()) })
            .await
            .unwrap();
        assert_eq!(queue.snapshot().buffered, 2);
        let executed = Arc::new(AtomicBool::new(false));
        let ran = executed.clone();
        {
            let waiting = queue.submit_envelope(&envelope, "tenant", false, async move {
                ran.store(true, Ordering::SeqCst);
                Ok(())
            });
            tokio::pin!(waiting);
            assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        }
        assert_eq!(queue.snapshot().buffered, 2);
        queue.pause().unwrap();
        shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(5), queue.sender.closed())
            .await
            .unwrap();
        reserved.send(Box::pin(async { Ok(()) }));
        gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!executed.load(Ordering::SeqCst));
        assert_eq!(queue.snapshot().completed, 3);
        assert_eq!(queue.snapshot().buffered, 0);
    }

    #[tokio::test]
    async fn grouped_cancel_and_panic_release_slots_for_healthy_neighbors() {
        let (queue, runner) = configured(true, 4, 2);
        queue
            .submit_class(Some(0), async { Err(super::QueueError::Cancelled) })
            .await
            .unwrap();
        queue
            .submit_class(Some(0), async {
                panic!("owned test panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
            .unwrap();
        let entered = Arc::new(Notify::new());
        let ran = entered.clone();
        queue
            .submit_class(Some(0), async move {
                ran.notify_one();
                Ok(())
            })
            .await
            .unwrap();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, "releases").unwrap();
        let shutdown = host.handle();
        let task = tokio::spawn(host.run());
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(queue.snapshot().cancelled, 1);
        assert_eq!(queue.snapshot().failed, 1);
        assert_eq!(queue.snapshot().completed, 1);
        assert_eq!(queue.snapshot().active, 0);
    }

    #[tokio::test]
    async fn grouped_pause_resume_retains_work_after_its_sending_handle_disappears() {
        let (queue, runner) = configured(true, 4, 2);
        let entered = Arc::new(Notify::new());
        let ready = entered.clone();
        queue
            .submit_class(Some(0), async move {
                ready.notify_one();
                Ok(())
            })
            .await
            .unwrap();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, "pause").unwrap();
        let shutdown = host.handle();
        let task = tokio::spawn(host.run());
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        queue.pause().unwrap();
        let executed = Arc::new(AtomicBool::new(false));
        let ran = executed.clone();
        let finished = Arc::new(Notify::new());
        let done = finished.clone();
        {
            let sender = queue.clone();
            sender
                .submit_class(Some(0), async move {
                    ran.store(true, Ordering::SeqCst);
                    done.notify_one();
                    Ok(())
                })
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;
        assert!(queue.snapshot().paused);
        assert_eq!(queue.snapshot().buffered, 1);
        assert!(!executed.load(Ordering::SeqCst));
        queue.resume();
        tokio::time::timeout(Duration::from_secs(5), finished.notified())
            .await
            .unwrap();
        shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(executed.load(Ordering::SeqCst));
        assert_eq!(queue.snapshot().completed, 2);
    }
}
