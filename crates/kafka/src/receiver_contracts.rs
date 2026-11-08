//! Controlled owner-task outcomes, not a substitute for the socket contracts.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use rdkafka::message::{Message, OwnedMessage, Timestamp};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::{Context, Operation, Ownership, PartitionState, PendingCommit, Receiver, commit_due};
use crate::offsets::Ledger;
use crate::{CommitStrategy, Error, Limits, Settlement};

fn receiver() -> Receiver {
    let ownership = Arc::new(Ownership::default());
    let consumer: BaseConsumer<Context> = ClientConfig::new()
        .set("group.id", "controlled-owner")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create_with_context(Context(Arc::clone(&ownership)))
        .unwrap();
    Receiver {
        consumer: Arc::new(consumer),
        ownership,
        owner: Uuid::new_v4(),
        limits: Limits::default(),
        partitions: BTreeMap::new(),
        poll: None,
        commit: None,
        incidental: None,
        strategy: CommitStrategy::PerMessage,
        last_flush: Instant::now(),
        warnings: Vec::new(),
    }
}

fn message(partition: i32, offset: i64) -> OwnedMessage {
    OwnedMessage::new(
        Some(b"value".to_vec()),
        None,
        "controlled".into(),
        Timestamp::NotAvailable,
        partition,
        offset,
        None,
    )
}

fn own(receiver: &Receiver, partition: i32) -> Uuid {
    let generation = Uuid::new_v4();
    receiver
        .ownership
        .generations
        .lock()
        .unwrap()
        .insert(("controlled".into(), partition), generation);
    generation
}

#[test]
fn flush_policy_uses_unique_completions_capacity_and_elapsed_time() {
    assert!(!commit_due(
        CommitStrategy::BatchCount(3),
        2,
        false,
        Duration::ZERO
    ));
    assert!(commit_due(
        CommitStrategy::BatchCount(3),
        3,
        false,
        Duration::ZERO
    ));
    assert!(commit_due(
        CommitStrategy::BatchCount(1000),
        1,
        true,
        Duration::ZERO
    ));
    assert!(!commit_due(
        CommitStrategy::BatchCount(1000),
        0,
        true,
        Duration::ZERO
    ));
    let interval = Duration::from_secs(1);
    assert!(!commit_due(
        CommitStrategy::Interval(interval),
        1,
        false,
        interval / 2
    ));
    assert!(commit_due(
        CommitStrategy::Interval(interval),
        1,
        false,
        interval
    ));
    assert!(!commit_due(
        CommitStrategy::StoreThenAutoFlush(interval),
        100,
        true,
        interval
    ));
}

#[tokio::test]
async fn canceled_poll_keeps_one_handle_and_eventual_record() {
    let mut receiver = receiver();
    own(&receiver, 0);
    let (send, receive) = oneshot::channel();
    receiver.poll = Some(tokio::spawn(async move { receive.await.unwrap() }));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), receiver.receive())
            .await
            .is_err()
    );
    assert!(receiver.poll.is_some());
    assert_eq!(receiver.pending(), 0);
    send.send(Ok(Some(message(0, 7)))).unwrap();
    let record = receiver.receive().await.unwrap().unwrap();
    assert_eq!(record.message().offset(), 7);
    assert!(receiver.poll.is_none());
    assert_eq!(receiver.pending(), 1);
    receiver.shutdown().await.unwrap();
}

#[tokio::test]
async fn successful_prefix_flush_resets_count_without_recounting_retained_completions() {
    let mut receiver = receiver();
    receiver.strategy = CommitStrategy::BatchCount(3);
    own(&receiver, 0);
    let records = [
        receiver.observe(message(0, 100)).unwrap(),
        receiver.observe(message(0, 101)).unwrap(),
        receiver.observe(message(0, 102)).unwrap(),
        receiver.observe(message(0, 103)).unwrap(),
    ];
    let state = receiver.partitions.get_mut(&records[0].partition).unwrap();
    for index in [0, 2, 3] {
        assert!(state.ledger.complete(records[index].token).unwrap());
        state.completions += 1;
    }
    let commit = state.ledger.begin_commit().unwrap();
    assert_eq!(commit.next_offset(), 101);
    receiver.commit = Some(PendingCommit {
        entries: vec![(records[0].partition.clone(), state.generation, commit)],
        operation: Operation::Commit,
        task: tokio::spawn(async { vec![Ok(())] }),
    });
    receiver.finish_commit().await.unwrap();
    assert_eq!(receiver.partitions[&records[0].partition].completions, 0);
    assert_eq!(
        receiver.complete(&records[1]).await.unwrap(),
        Settlement::Deferred
    );
    let state = receiver.partitions.get_mut(&records[0].partition).unwrap();
    assert_eq!(state.completions, 1);
    assert_eq!(state.ledger.confirmed_offset(), Some(101));
    let commit = state.ledger.begin_commit().unwrap();
    assert_eq!(commit.next_offset(), 104);
    receiver.commit = Some(PendingCommit {
        entries: vec![(records[0].partition.clone(), state.generation, commit)],
        operation: Operation::Commit,
        task: tokio::spawn(async { vec![Ok(())] }),
    });
    receiver.finish_commit().await.unwrap();
    assert_eq!(receiver.pending(), 0);
    receiver.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_commit_wait_keeps_attempt_and_partial_results_retry_only_failed_state() {
    let mut receiver = receiver();
    own(&receiver, 0);
    own(&receiver, 1);
    let first = receiver.observe(message(0, 4)).unwrap();
    let other = receiver.observe(message(1, 8)).unwrap();
    let mut entries = Vec::new();
    for (partition, state) in &mut receiver.partitions {
        let record = if partition.1 == 0 { &first } else { &other };
        state.ledger.complete(record.token).unwrap();
        entries.push((
            partition.clone(),
            state.generation,
            state.ledger.begin_commit().unwrap(),
        ));
    }
    let (send, receive) = oneshot::channel();
    receiver.commit = Some(PendingCommit {
        entries,
        operation: Operation::Commit,
        task: tokio::spawn(async move { receive.await.unwrap() }),
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(10), receiver.finish_commit())
            .await
            .is_err()
    );
    assert!(receiver.commit.is_some());
    assert_eq!(receiver.pending(), 2);
    send.send(vec![Ok(()), Err(Error::Uncertain)]).unwrap();
    assert_eq!(receiver.finish_commit().await, Err(Error::Uncertain));
    assert_eq!(receiver.pending(), 1);
    receiver.strategy = CommitStrategy::BatchCount(1000);
    assert_eq!(
        receiver.complete(&first).await.unwrap(),
        Settlement::Committed
    );
    let state = receiver.partitions.get_mut(&other.partition).unwrap();
    let retry = state.ledger.begin_commit().unwrap();
    assert_eq!(retry.next_offset(), 9);
    receiver.commit = Some(PendingCommit {
        entries: vec![(other.partition.clone(), other.generation, retry)],
        operation: Operation::Commit,
        task: tokio::spawn(async { vec![Ok(())] }),
    });
    receiver.finish_commit().await.unwrap();
    assert_eq!(
        receiver.complete(&other).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(receiver.pending(), 0);
    receiver.shutdown().await.unwrap();
}

#[tokio::test]
async fn delayed_old_commit_cannot_confirm_reacquired_partition_and_preserves_other_owner() {
    let mut receiver = receiver();
    own(&receiver, 0);
    own(&receiver, 1);
    let old = receiver.observe(message(0, 3)).unwrap();
    let retained = receiver.observe(message(1, 9)).unwrap();
    let state = receiver.partitions.get_mut(&old.partition).unwrap();
    state.ledger.complete(old.token).unwrap();
    let commit = state.ledger.begin_commit().unwrap();
    receiver.commit = Some(PendingCommit {
        entries: vec![(old.partition.clone(), old.generation, commit)],
        operation: Operation::Commit,
        task: tokio::spawn(async { vec![Ok(())] }),
    });
    let generation = own(&receiver, 0);
    receiver.synchronize().unwrap();
    receiver.partitions.insert(
        old.partition.clone(),
        PartitionState {
            generation,
            ledger: Ledger::new(2).unwrap(),
            completions: 0,
        },
    );
    let fresh = receiver.observe(message(0, 3)).unwrap();
    assert_eq!(receiver.finish_commit().await, Err(Error::Stale));
    assert_eq!(receiver.complete(&old).await, Err(Error::Stale));
    let state = receiver.partitions.get(&fresh.partition).unwrap();
    assert_eq!(state.ledger.confirmed_offset(), None);
    assert_eq!(state.ledger.ready_offset(), None);
    assert_eq!(
        receiver
            .partitions
            .get(&retained.partition)
            .unwrap()
            .generation,
        retained.generation
    );
    assert_eq!(receiver.pending(), 2);
    receiver.shutdown().await.unwrap();
}
