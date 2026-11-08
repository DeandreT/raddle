use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use futures_util::FutureExt;
use futures_util::stream::{FuturesUnordered, StreamExt};
use raddle_host::{Host, HostError, RestartPolicy, ServiceContext, ServiceFailure};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

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

type Job = BoxFuture<'static, Result<(), QueueError>>;

/// Cloneable sending/control handle; no task is created by constructing a queue.
#[derive(Clone)]
pub struct LocalQueue {
    sender: mpsc::Sender<Job>,
    state: Arc<State>,
    settings: QueueSettings,
    inline_admission: Arc<Semaphore>,
}

/// Single ownership of the receiving block prevents simultaneous/double starts.
pub struct QueueRunner {
    receiver: mpsc::Receiver<Job>,
    state: Arc<State>,
    parallelism: usize,
}

impl LocalQueue {
    /// Creates the sending handle and its sole receiving owner.
    pub fn new(settings: QueueSettings) -> Result<(Self, QueueRunner), QueueError> {
        if !(1..=65536).contains(&settings.capacity) || !(1..=256).contains(&settings.parallelism) {
            return Err(QueueError::Invalid);
        }
        let (sender, receiver) = mpsc::channel(settings.capacity);
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
            },
            QueueRunner {
                receiver,
                state,
                parallelism: settings.parallelism,
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
            buffered: self.sender.max_capacity() - self.sender.capacity(),
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
        let permit = self.reserve_buffered().await?;
        permit.send(Box::pin(job));
        Ok(())
    }

    async fn reserve_buffered(&self) -> Result<mpsc::Permit<'_, Job>, QueueError> {
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| QueueError::Closed)?;
        if !self.state.accepting.load(Ordering::Acquire) {
            return Err(QueueError::Closed);
        }
        Ok(permit)
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
        context.set_ready(true);
        loop {
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                _ = paused.changed() => {},
                Some(_) = active.next(), if !active.is_empty() => {},
                job = self.receiver.recv(), if !*paused.borrow() && active.len() < self.parallelism => {
                    match job {
                        Some(job) => active.push(measured(Arc::clone(&self.state), job)),
                        None => break,
                    }
                }
            }
        }
        context.set_ready(false);
        self.state.accepting.store(false, Ordering::Release);
        self.receiver.close();
        // A sender can have passed admission while still holding a reserved
        // channel permit. Only recv(None), not an empty buffer, proves that all
        // those senders have either handed over their jobs or released custody.
        let mut receiving = true;
        while receiving || !active.is_empty() {
            tokio::select! {
                Some(_) = active.next(), if !active.is_empty() => {},
                job = self.receiver.recv(), if receiving && active.len() < self.parallelism => {
                    match job {
                        Some(job) => active.push(measured(Arc::clone(&self.state), job)),
                        None => receiving = false,
                    }
                }
            }
        }
        Ok(())
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

async fn measured(state: Arc<State>, job: Job) -> Result<(), QueueError> {
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

    #[tokio::test]
    async fn shutdown_drains_a_job_sent_through_a_previously_admitted_permit_after_close() {
        let (queue, runner) = LocalQueue::new(QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 2,
            parallelism: 1,
        })
        .unwrap();
        // Stop the real submit path between its accepting check and permit.send.
        let permit = queue.reserve_buffered().await.unwrap();
        assert!(queue.state.accepting.load(Ordering::Acquire));
        let entered = Arc::new(Notify::new());
        let started = Arc::clone(&entered);
        queue
            .submit(async move {
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
        let (queue, runner) = LocalQueue::new(QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 2,
            parallelism: 2,
        })
        .unwrap();
        let permit = queue.reserve_buffered().await.unwrap();
        assert!(queue.state.accepting.load(Ordering::Acquire));
        let entered = Arc::new(Notify::new());
        let cleaned = Arc::new(Notify::new());
        let cleanup_gate = Arc::new(Semaphore::new(0));
        let started = Arc::clone(&entered);
        let finished = Arc::clone(&cleaned);
        let gate = Arc::clone(&cleanup_gate);
        queue
            .submit(async move {
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
}
