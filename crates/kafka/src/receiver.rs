use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, Message, OwnedMessage};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::offsets::{Commit, Ledger, Token};
use crate::{
    ClientSettings, CommitStrategy, ConfigurationWarning, ConsumerSettings, Error, Limits,
};

type Partition = (String, i32);
type PollTask = JoinHandle<Result<Option<OwnedMessage>, Error>>;
type CommitTask = JoinHandle<Vec<Result<(), Error>>>;

#[derive(Clone, Copy)]
enum Operation {
    Store,
    Commit,
}

#[derive(Default)]
struct Ownership {
    generations: Mutex<BTreeMap<Partition, Uuid>>,
    full: AtomicBool,
    failed: AtomicBool,
}

impl Ownership {
    fn ensure_live(&self) -> Result<(), Error> {
        if self.failed.load(Ordering::Acquire) {
            Err(Error::ReceiverFenced)
        } else {
            Ok(())
        }
    }

    fn poll_error(&self, error: KafkaError) -> Result<(), Error> {
        if matches!(
            error,
            KafkaError::NoMessageReceived | KafkaError::PartitionEOF(_)
        ) {
            return Ok(());
        }
        // A native record/batch error can advance the SDK's local fetch position
        // without returning the failed record's partition/offset to this API.
        // Do not commit any successor past that unknown hole. Only explicit
        // receiver recovery may resume; ordinary invalid envelopes retain their
        // own known per-record barrier instead of entering this shared failure.
        self.failed.store(true, Ordering::Release);
        Err(Error::ReceiverFenced)
    }
}

struct Context(Arc<Ownership>);
impl ClientContext for Context {
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
    fn error(&self, _: KafkaError, _: &str) {}
}
impl ConsumerContext for Context {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Ok(mut generations) = self.0.generations.lock() else {
            self.0.failed.store(true, Ordering::Release);
            return;
        };
        match rebalance {
            Rebalance::Revoke(partitions) => {
                // Incremental revocation must preserve every unaffected owner.
                for partition in partitions.elements() {
                    generations.remove(&(partition.topic().to_owned(), partition.partition()));
                }
            }
            Rebalance::Error(_) => {
                generations.clear();
            }
            Rebalance::Assign(_) => {}
        }
    }
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Assign(partitions) = rebalance else {
            return;
        };
        let Ok(mut generations) = self.0.generations.lock() else {
            self.0.failed.store(true, Ordering::Release);
            return;
        };
        for partition in partitions.elements() {
            let key = (partition.topic().to_owned(), partition.partition());
            if generations.len() >= 1024 && !generations.contains_key(&key) {
                self.0.failed.store(true, Ordering::Release);
                return;
            }
            generations.entry(key).or_insert_with(Uuid::new_v4);
        }
        drop(generations);
        if self.0.full.load(Ordering::Acquire)
            && (consumer.pause(partitions).is_err()
                || partitions
                    .elements()
                    .iter()
                    .any(|entry| entry.error().is_err()))
        {
            self.0.failed.store(true, Ordering::Release);
        }
    }
}

struct PartitionState {
    generation: Uuid,
    ledger: Ledger<()>,
    completions: usize,
}
struct PendingCommit {
    entries: Vec<(Partition, Uuid, Commit)>,
    operation: Operation,
    task: CommitTask,
}

/// One bounded native record, tied to its original receiver and assignment.
/// Headers remain untrusted input. Dropping it does not acknowledge custody;
/// its unfinished partition prefix remains a barrier until assignment recovery.
pub struct Received {
    owner: Uuid,
    generation: Uuid,
    token: Token,
    partition: Partition,
    message: OwnedMessage,
    validity: Result<(), Error>,
}
impl Received {
    /// Exact backend message, including binary keys, headers and null tombstones.
    pub fn message(&self) -> &OwnedMessage {
        &self.message
    }
    /// Applies configured application byte/header bounds. Invalid records still
    /// own an offset barrier: quarantine durably before explicitly completing
    /// them. A refusal is never an implicit discard or positive acknowledgement.
    pub fn validate(&self) -> Result<(), Error> {
        self.validity
    }
}

/// Observed settlement, not an assertion of exactly-once business execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Settlement {
    /// Custody completed, but an earlier record or the flush policy defers commit.
    Deferred,
    /// A safe next-read position was stored locally, not broker-confirmed.
    /// Auto-commit may subsequently persist it; call flush for confirmation.
    Stored,
    /// The broker confirmed a safe prefix covering this original record.
    Committed,
}

/// Caller-polled native receiving with bounded retained records. Poll regularly
/// while processing to service membership callbacks; native heartbeats do not
/// remove Kafka's max-poll requirement. This is not a host-owned handler runner.
pub struct Receiver {
    consumer: Arc<BaseConsumer<Context>>,
    ownership: Arc<Ownership>,
    owner: Uuid,
    limits: Limits,
    partitions: BTreeMap<Partition, PartitionState>,
    poll: Option<PollTask>,
    commit: Option<PendingCommit>,
    incidental: Option<Received>,
    strategy: CommitStrategy,
    last_flush: Instant,
    warnings: Vec<ConfigurationWarning>,
}

impl Receiver {
    /// Connects to configured subscriptions without provisioning topics. This
    /// All strategies disable automatic storage of fetched records. Only safe
    /// custody prefixes can enter the native local store or broker commit.
    pub async fn connect(
        settings: ClientSettings,
        subscription: ConsumerSettings,
    ) -> Result<Self, Error> {
        let resolved = subscription.resolve(&settings)?;
        tokio::task::spawn_blocking(move || {
            let ownership = Arc::new(Ownership::default());
            let consumer: BaseConsumer<Context> = resolved
                .native
                .create_with_context(Context(Arc::clone(&ownership)))
                .map_err(|_| Error::Configuration)?;
            let topics: Vec<_> = subscription
                .topics
                .iter()
                .map(|topic| topic.as_str())
                .collect();
            consumer
                .subscribe(&topics)
                .map_err(|_| Error::Configuration)?;
            Ok(Self {
                consumer: Arc::new(consumer),
                ownership,
                owner: Uuid::new_v4(),
                limits: settings.limits,
                partitions: BTreeMap::new(),
                poll: None,
                commit: None,
                incidental: None,
                strategy: subscription.strategy,
                last_flush: Instant::now(),
                warnings: resolved.warnings,
            })
        })
        .await
        .map_err(|_| Error::Native)?
    }

    /// Number of observed records retained behind the safe commit watermark.
    pub fn pending(&self) -> usize {
        self.partitions
            .values()
            .map(|state| state.ledger.len())
            .sum()
    }

    /// KIP-848 normalization warnings for application diagnostics. Each removed
    /// explicit classic setting appears once; values/credentials are not exposed.
    pub fn configuration_warnings(&self) -> &[ConfigurationWarning] {
        &self.warnings
    }

    fn synchronize(&mut self) -> Result<(), Error> {
        self.ownership.ensure_live()?;
        let generations = self
            .ownership
            .generations
            .lock()
            .map_err(|_| Error::Native)?;
        self.partitions.retain(|key, state| {
            if generations.get(key) == Some(&state.generation) {
                true
            } else {
                drop(state.ledger.fence());
                false
            }
        });
        if self
            .incidental
            .as_ref()
            .is_some_and(|record| generations.get(&record.partition) != Some(&record.generation))
        {
            self.incidental = None;
        }
        Ok(())
    }

    fn observe(&mut self, message: OwnedMessage) -> Result<Received, Error> {
        self.synchronize()?;
        let mut bytes = message
            .payload()
            .map_or(0, <[u8]>::len)
            .checked_add(message.key().map_or(0, <[u8]>::len));
        let mut validity = Ok(());
        if let Some(headers) = message.headers() {
            if headers.count() > self.limits.max_headers {
                validity = Err(Error::Rejected);
            } else {
                for header in headers.iter() {
                    bytes = bytes
                        .and_then(|bytes| bytes.checked_add(header.key.len()))
                        .and_then(|bytes| bytes.checked_add(header.value.map_or(0, <[u8]>::len)));
                }
            }
        }
        if bytes.is_none_or(|bytes| bytes > self.limits.max_record_bytes) {
            validity = Err(Error::Rejected);
        }
        let partition = (message.topic().to_owned(), message.partition());
        let generation = *self
            .ownership
            .generations
            .lock()
            .map_err(|_| Error::Native)?
            .get(&partition)
            .ok_or(Error::Stale)?;
        let state = match self.partitions.entry(partition.clone()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(PartitionState {
                generation,
                ledger: Ledger::new(self.limits.max_pending + 1)
                    .map_err(|_| Error::Configuration)?,
                completions: 0,
            }),
        };
        let token = state
            .ledger
            .observe(message.offset(), ())
            .map_err(|_| Error::Native)?;
        Ok(Received {
            owner: self.owner,
            generation,
            token,
            partition,
            message,
            validity,
        })
    }

    async fn harvest_poll(&mut self) -> Result<(), Error> {
        if let Some(task) = &mut self.poll {
            // Keep the handle inside self while awaiting. Canceling receive does
            // not discard a fetched record or let another native poll overtake it.
            let result = task.await;
            self.poll = None;
            let result = result.map_err(|_| {
                self.ownership.failed.store(true, Ordering::Release);
                Error::Native
            })?;
            self.synchronize()?;
            if let Some(message) = result? {
                if self.incidental.is_some() {
                    return Err(Error::Capacity);
                }
                self.incidental = Some(self.observe(message)?);
            }
        }
        Ok(())
    }

    /// Polls for at most one short native interval. None is not end-of-stream.
    /// A single extra incidental slot handles returns around pause/rebalance.
    /// At full capacity, complete existing custody before calling again.
    pub async fn receive(&mut self) -> Result<Option<Received>, Error> {
        self.finish_commit().await?;
        self.harvest_poll().await?;
        self.synchronize()?;
        self.flush_if_due().await?;
        if self.incidental.is_some() {
            if self.pending() > self.limits.max_pending {
                return Err(Error::Capacity);
            }
            return Ok(self.incidental.take());
        }
        let full = self.pending() >= self.limits.max_pending;
        self.ownership.full.store(full, Ordering::Release);
        let consumer = Arc::clone(&self.consumer);
        let ownership = Arc::clone(&self.ownership);
        self.poll = Some(tokio::task::spawn_blocking(move || {
            let assignment = consumer.assignment().map_err(|_| Error::Native)?;
            if !assignment.elements().is_empty() {
                let result = if full {
                    consumer.pause(&assignment)
                } else {
                    consumer.resume(&assignment)
                };
                result.map_err(|_| Error::Native)?;
                if assignment
                    .elements()
                    .iter()
                    .any(|entry| entry.error().is_err())
                {
                    return Err(Error::Native);
                }
            }
            match consumer.poll(Duration::from_millis(50)) {
                None => Ok(None),
                Some(Ok(message)) => Ok(Some(message.detach())),
                Some(Err(error)) => {
                    ownership.poll_error(error)?;
                    Ok(None)
                }
            }
        }));
        self.harvest_poll().await?;
        self.flush_if_due().await?;
        if full {
            return Err(Error::Capacity);
        }
        Ok(self.incidental.take())
    }

    /// Call only after application processing or durable inbox commit. Marks one
    /// observed record, then applies the configured store/commit policy, never a
    /// later offset past unfinished work. Repeating it does not rerun business work.
    pub async fn complete(&mut self, record: &Received) -> Result<Settlement, Error> {
        self.harvest_poll().await?;
        self.finish_commit().await?;
        self.synchronize()?;
        if record.owner != self.owner || self.consumer.assignment_lost() {
            return Err(Error::Stale);
        }
        let state = self
            .partitions
            .get_mut(&record.partition)
            .ok_or(Error::Stale)?;
        if state.generation != record.generation {
            return Err(Error::Stale);
        }
        let new = state
            .ledger
            .complete(record.token)
            .map_err(|_| Error::Stale)?;
        if new && !matches!(self.strategy, CommitStrategy::StoreThenAutoFlush(_)) {
            state.completions = state.completions.saturating_add(1);
        }
        match self.strategy {
            CommitStrategy::StoreThenAutoFlush(_) => self.settle(Operation::Store).await?,
            _ => self.flush_if_due().await?,
        }
        let state = self.partitions.get(&record.partition).ok_or(Error::Stale)?;
        if state.generation != record.generation {
            return Err(Error::Stale);
        }
        Ok(
            if state
                .ledger
                .confirmed_offset()
                .is_some_and(|offset| offset > record.message.offset())
            {
                Settlement::Committed
            } else if state
                .ledger
                .stored_offset()
                .is_some_and(|offset| offset > record.message.offset())
            {
                Settlement::Stored
            } else {
                Settlement::Deferred
            },
        )
    }

    /// Retries uncertain offset commits without resubmitting successful handlers.
    /// Explicitly confirms all safe positions, including locally stored ones,
    /// regardless of the configured count/interval. Never commits fetched state.
    pub async fn flush(&mut self) -> Result<(), Error> {
        self.settle(Operation::Commit).await
    }

    async fn flush_if_due(&mut self) -> Result<(), Error> {
        let completed = self
            .partitions
            .values()
            .map(|state| state.completions)
            .fold(0usize, usize::saturating_add);
        if commit_due(
            self.strategy,
            completed,
            self.pending() >= self.limits.max_pending,
            self.last_flush.elapsed(),
        ) {
            self.settle(Operation::Commit).await?;
        }
        Ok(())
    }

    async fn settle(&mut self, operation: Operation) -> Result<(), Error> {
        self.harvest_poll().await?;
        self.finish_commit().await?;
        self.synchronize()?;
        let mut entries = Vec::new();
        for (partition, state) in &mut self.partitions {
            let commit = match operation {
                Operation::Store => state.ledger.begin_store(),
                Operation::Commit => state.ledger.begin_commit(),
            };
            if let Some(commit) = commit {
                entries.push((partition.clone(), state.generation, commit));
            }
        }
        if entries.is_empty() {
            return Ok(());
        }
        let consumer = Arc::clone(&self.consumer);
        let ownership = Arc::clone(&self.ownership);
        // The blocking worker owns only an immutable operation snapshot. The
        // original attempts and their handle stay in self across cancellation.
        let auto_store = matches!(self.strategy, CommitStrategy::StoreThenAutoFlush(_));
        let requests = entries
            .iter()
            .map(|(partition, generation, commit)| {
                (partition.clone(), *generation, commit.next_offset())
            })
            .collect::<Vec<_>>();
        self.commit = Some(PendingCommit {
            entries,
            operation,
            task: tokio::task::spawn_blocking(move || {
                requests
                    .into_iter()
                    .map(|(partition, generation, offset)| {
                        ownership.ensure_live()?;
                        if consumer.assignment_lost()
                            || ownership
                                .generations
                                .lock()
                                .map_err(|_| Error::Native)?
                                .get(&partition)
                                != Some(&generation)
                        {
                            return Err(Error::Stale);
                        }
                        let mut offsets = TopicPartitionList::new();
                        offsets
                            .add_partition_offset(&partition.0, partition.1, Offset::Offset(offset))
                            .map_err(|_| Error::Native)?;
                        // One partition per Sync request: the safe wrapper does not
                        // expose a multi-partition broker result on the input TPL.
                        // Partial successes must not acknowledge failing partitions.
                        if auto_store && matches!(operation, Operation::Commit) {
                            // Keep the auto-committer's local position at least as
                            // recent as this explicit safe-prefix commit.
                            consumer
                                .store_offsets(&offsets)
                                .map_err(|_| Error::Uncertain)?;
                            if offsets
                                .elements()
                                .iter()
                                .any(|entry| entry.error().is_err())
                            {
                                return Err(Error::Uncertain);
                            }
                        }
                        match operation {
                            Operation::Store => consumer.store_offsets(&offsets),
                            Operation::Commit => consumer.commit(&offsets, CommitMode::Sync),
                        }
                        .map_err(|_| Error::Uncertain)?;
                        if offsets
                            .elements()
                            .iter()
                            .any(|entry| entry.error().is_err())
                        {
                            return Err(Error::Uncertain);
                        }
                        Ok(())
                    })
                    .collect()
            }),
        });
        self.finish_commit().await
    }

    async fn finish_commit(&mut self) -> Result<(), Error> {
        let Some(pending) = &mut self.commit else {
            return Ok(());
        };
        let outcome = (&mut pending.task).await;
        let Some(pending) = self.commit.take() else {
            return Err(Error::Native);
        };
        let outcomes = outcome.map_err(|_| {
            self.ownership.failed.store(true, Ordering::Release);
            Error::Native
        })?;
        self.synchronize()?;
        if outcomes.len() != pending.entries.len() {
            self.ownership.failed.store(true, Ordering::Release);
            return Err(Error::ReceiverFenced);
        }
        let mut failure = None;
        let mut confirmed = false;
        for ((partition, generation, commit), outcome) in pending.entries.into_iter().zip(outcomes)
        {
            if let Some(state) = self.partitions.get_mut(&partition)
                && state.generation == generation
            {
                match pending.operation {
                    Operation::Store => state.ledger.finish_store(commit, outcome.is_ok()),
                    Operation::Commit => state.ledger.finish_commit(commit, outcome.is_ok()),
                }
                .map_err(|_| Error::Native)?;
                confirmed |= outcome.is_ok();
                if let Err(error) = outcome {
                    failure.get_or_insert(error);
                }
            } else {
                failure.get_or_insert(Error::Stale);
            }
        }
        if confirmed && matches!(pending.operation, Operation::Commit) {
            self.last_flush = Instant::now();
            if failure.is_none() {
                // The count measures new custody completions since a successful
                // flush, not completed records still held behind another record.
                // Uncertain attempts keep the count eligible for offset-only retry.
                for state in self.partitions.values_mut() {
                    state.completions = 0;
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// Joins outstanding native work and leaves unfinished records uncommitted.
    /// Native close/Drop is performed off the async worker. Destruction itself
    /// is not hard-deadline-cancellable. Await this method fully: cancellation
    /// during its preliminary flush can drop self on the caller's thread;
    /// cancellation after the close worker starts does not stop that worker.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        let settlement = self.flush().await;
        tokio::task::spawn_blocking(move || {
            let close = self.consumer.close_queue().map_err(|_| Error::Native);
            let deadline = Instant::now() + self.limits.deadline;
            while close.is_ok() && !self.consumer.closed() && Instant::now() < deadline {
                let _ = self.consumer.poll(Duration::from_millis(50));
            }
            let closed = self.consumer.closed();
            drop(self);
            close?;
            if !closed {
                return Err(Error::Uncertain);
            }
            settlement
        })
        .await
        .map_err(|_| Error::Native)?
    }
}

fn commit_due(strategy: CommitStrategy, completed: usize, full: bool, elapsed: Duration) -> bool {
    match strategy {
        CommitStrategy::PerMessage => completed > 0,
        CommitStrategy::BatchCount(count) => {
            completed >= usize::from(count) || (full && completed > 0)
        }
        CommitStrategy::Interval(interval) => elapsed >= interval,
        CommitStrategy::StoreThenAutoFlush(_) => false,
    }
}

#[cfg(test)]
#[path = "receiver_contracts.rs"]
mod contracts;

#[cfg(test)]
mod tests {
    use super::Ownership;
    use crate::Error;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    #[test]
    fn opaque_native_record_errors_latch_settlement_fence() {
        for error in [
            KafkaError::MessageConsumption(RDKafkaErrorCode::BadMessage),
            KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::BadMessage),
            KafkaError::Global(RDKafkaErrorCode::Unknown),
        ] {
            let owner = Ownership::default();
            assert_eq!(owner.ensure_live(), Ok(()));
            assert_eq!(owner.poll_error(error), Err(Error::ReceiverFenced));
            assert_eq!(owner.ensure_live(), Err(Error::ReceiverFenced));
            assert_eq!(owner.poll_error(KafkaError::PartitionEOF(0)), Ok(()));
            assert_eq!(owner.ensure_live(), Err(Error::ReceiverFenced));
        }
    }

    #[test]
    fn empty_polls_and_partition_end_do_not_fence_receiving() {
        let owner = Ownership::default();
        assert_eq!(owner.poll_error(KafkaError::NoMessageReceived), Ok(()));
        assert_eq!(owner.poll_error(KafkaError::PartitionEOF(0)), Ok(()));
        assert_eq!(owner.ensure_live(), Ok(()));
    }
}
