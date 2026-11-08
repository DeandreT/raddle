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
use crate::{ClientSettings, CommitStrategy, ConsumerSettings, Error, Limits};

type Partition = (String, i32);
type PollTask = JoinHandle<Result<Option<OwnedMessage>, Error>>;
type CommitTask = JoinHandle<Result<(), Error>>;

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
}
struct PendingCommit {
    entries: Vec<(Partition, Uuid, Commit)>,
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
pub enum Settlement {
    /// Custody completed, but an earlier observed record blocks offset commit.
    Deferred,
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
}

impl Receiver {
    /// Connects to configured subscriptions without provisioning topics. This
    /// receiver currently requires PerMessage settlement; other provider offset
    /// strategies are rejected rather than silently approximated.
    pub async fn connect(
        settings: ClientSettings,
        subscription: ConsumerSettings,
    ) -> Result<Self, Error> {
        if subscription.strategy != CommitStrategy::PerMessage {
            return Err(Error::Configuration);
        }
        let native = subscription.native_config(&settings)?;
        tokio::task::spawn_blocking(move || {
            let ownership = Arc::new(Ownership::default());
            let consumer: BaseConsumer<Context> = native
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
        if full {
            return Err(Error::Capacity);
        }
        Ok(self.incidental.take())
    }

    /// Call only after application processing or durable inbox commit. Marks one
    /// observed record, then confirms the safe completed prefix, never a later
    /// offset past unfinished work. Repeating this call does not rerun business work.
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
        state
            .ledger
            .complete(record.token)
            .map_err(|_| Error::Stale)?;
        self.flush().await?;
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
            } else {
                Settlement::Deferred
            },
        )
    }

    /// Retries uncertain offset commits without resubmitting successful handlers.
    /// Automatic native offset storing and automatic commits remain disabled.
    pub async fn flush(&mut self) -> Result<(), Error> {
        self.harvest_poll().await?;
        self.finish_commit().await?;
        self.synchronize()?;
        let mut offsets = TopicPartitionList::new();
        let mut entries = Vec::new();
        for (partition, state) in &mut self.partitions {
            if let Some(commit) = state.ledger.begin_commit() {
                offsets
                    .add_partition_offset(
                        &partition.0,
                        partition.1,
                        Offset::Offset(commit.next_offset()),
                    )
                    .map_err(|_| Error::Native)?;
                entries.push((partition.clone(), state.generation, commit));
            }
        }
        if entries.is_empty() {
            return Ok(());
        }
        let consumer = Arc::clone(&self.consumer);
        self.commit = Some(PendingCommit {
            entries,
            task: tokio::task::spawn_blocking(move || {
                if consumer.assignment_lost() {
                    return Err(Error::Stale);
                }
                consumer
                    .commit(&offsets, CommitMode::Sync)
                    .map_err(|_| Error::Uncertain)?;
                if offsets
                    .elements()
                    .iter()
                    .any(|entry| entry.error().is_err())
                {
                    return Err(Error::Uncertain);
                }
                Ok(())
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
        let outcome = outcome.map_err(|_| {
            self.ownership.failed.store(true, Ordering::Release);
            Error::Native
        })?;
        self.synchronize()?;
        let mut stale = false;
        for (partition, generation, commit) in pending.entries {
            if let Some(state) = self.partitions.get_mut(&partition)
                && state.generation == generation
            {
                state
                    .ledger
                    .finish_commit(commit, outcome.is_ok())
                    .map_err(|_| Error::Native)?;
            } else {
                stale = true;
            }
        }
        outcome?;
        if stale {
            return Err(Error::Stale);
        }
        Ok(())
    }

    /// Joins outstanding native work and leaves unfinished records uncommitted.
    /// Native close/Drop is performed off the async worker. Destruction itself
    /// is not hard-deadline-cancellable; dropping this future does not cancel it.
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
