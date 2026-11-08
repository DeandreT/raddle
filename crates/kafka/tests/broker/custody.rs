use std::time::{Duration, Instant};

use raddle_kafka::{
    Admin, ClientSettings, CommitStrategy, ConsumerSettings, Error, Limits, Membership, Publisher,
    Receiver, Record, Settlement, Topic, TopicSpec,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use uuid::Uuid;

use super::{config, name, next, route, settings};

pub(super) async fn committed(group: &str, topic: &Topic, partition: i32) -> Option<i64> {
    let mut config = config();
    config
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    let topic = topic.as_str().to_owned();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = config.create().unwrap();
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition(&topic, partition);
        let result = consumer
            .committed_offsets(offsets, Duration::from_secs(10))
            .unwrap();
        let entry = result.find_partition(&topic, partition).unwrap();
        entry.error().unwrap();
        match entry.offset() {
            Offset::Offset(offset) => Some(offset),
            Offset::Invalid => None,
            unexpected => panic!("unexpected committed position {unexpected:?}"),
        }
    })
    .await
    .unwrap()
}

fn subscription(group: &str, topic: &Topic, strategy: CommitStrategy) -> ConsumerSettings {
    ConsumerSettings::try_new(
        group,
        vec![topic.clone()],
        Membership::Classic,
        None,
        strategy,
    )
    .unwrap()
    .from_beginning()
}

fn bounded(capacity: usize) -> ClientSettings {
    ClientSettings::try_new(
        config(),
        Limits {
            max_pending: capacity,
            deadline: Duration::from_secs(20),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn all_strategies_preserve_incomplete_barriers_and_flush_gracefully_below_threshold() {
    for strategy in [
        CommitStrategy::StoreThenAutoFlush(Duration::from_millis(100)),
        CommitStrategy::PerMessage,
        CommitStrategy::BatchCount(2),
        CommitStrategy::Interval(Duration::from_secs(1)),
    ] {
        let topic = name("four-strategies");
        let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
        let admin = Admin::connect(&settings()).await.unwrap();
        admin.declare(&specs).await.unwrap();
        let publisher = Publisher::connect(settings(), route(&topic)).await.unwrap();
        for value in [b"first".as_slice(), b"middle", b"last", b"tail"] {
            publisher
                .publish_record("input", Record::value(None, value, &[]))
                .await
                .unwrap();
        }
        let group = format!("raddle-strategy-{}", Uuid::new_v4());
        let mut receiver = Receiver::connect(bounded(3), subscription(&group, &topic, strategy))
            .await
            .unwrap();
        let first = next(&mut receiver).await;
        let middle = next(&mut receiver).await;
        let last = next(&mut receiver).await;
        for _ in 0..3 {
            assert!(matches!(receiver.receive().await, Err(Error::Capacity)));
            assert_eq!(receiver.pending(), 3);
        }
        assert_eq!(
            receiver.complete(&last).await.unwrap(),
            Settlement::Deferred
        );
        assert_eq!(committed(&group, &topic, 0).await, None);
        let settlement = receiver.complete(&first).await.unwrap();
        if matches!(strategy, CommitStrategy::StoreThenAutoFlush(_)) {
            assert_eq!(settlement, Settlement::Stored);
            let deadline = Instant::now() + Duration::from_secs(10);
            while committed(&group, &topic, 0).await != Some(1) {
                assert!(
                    Instant::now() < deadline,
                    "auto-commit never flushed the safe stored prefix"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            // The SDK's later uncorrelated callback is not evidence for this
            // receiver's generation. A repeat stays conservatively Stored.
            assert_eq!(receiver.complete(&first).await.unwrap(), Settlement::Stored);
        }
        receiver.flush().await.unwrap();
        assert_eq!(committed(&group, &topic, 0).await, Some(1));
        receiver.complete(&middle).await.unwrap();
        receiver.flush().await.unwrap();
        assert_eq!(committed(&group, &topic, 0).await, Some(3));
        let tail = next(&mut receiver).await;
        assert_eq!(tail.message().payload(), Some(b"tail".as_slice()));
        let outcome = receiver.complete(&tail).await.unwrap();
        if matches!(strategy, CommitStrategy::BatchCount(_)) {
            assert_eq!(outcome, Settlement::Deferred);
            assert_eq!(committed(&group, &topic, 0).await, Some(3));
        }
        receiver.shutdown().await.unwrap();
        assert_eq!(committed(&group, &topic, 0).await, Some(4));
        publisher
            .publish_record("input", Record::value(None, b"restart", &[]))
            .await
            .unwrap();
        let mut restarted = Receiver::connect(bounded(3), subscription(&group, &topic, strategy))
            .await
            .unwrap();
        let fresh = next(&mut restarted).await;
        assert_eq!(fresh.message().payload(), Some(b"restart".as_slice()));
        restarted.complete(&fresh).await.unwrap();
        restarted.shutdown().await.unwrap();
        publisher.shutdown().await.unwrap();
        admin.delete_managed(&specs).await.unwrap();
        admin.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn oversized_count_threshold_flushes_at_full_admission_without_deadlock() {
    let topic = name("capacity-flush");
    let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    let admin = Admin::connect(&settings()).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let publisher = Publisher::connect(settings(), route(&topic)).await.unwrap();
    for value in [b"one".as_slice(), b"two", b"three"] {
        publisher
            .publish_record("input", Record::value(None, value, &[]))
            .await
            .unwrap();
    }
    let group = format!("raddle-capacity-{}", Uuid::new_v4());
    let mut receiver = Receiver::connect(
        bounded(2),
        subscription(&group, &topic, CommitStrategy::BatchCount(1000)),
    )
    .await
    .unwrap();
    let first = next(&mut receiver).await;
    let last = next(&mut receiver).await;
    assert_eq!(
        receiver.complete(&last).await.unwrap(),
        Settlement::Deferred
    );
    assert_eq!(
        receiver.complete(&first).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(receiver.pending(), 0);
    assert_eq!(committed(&group, &topic, 0).await, Some(2));
    let tail = next(&mut receiver).await;
    assert_eq!(
        receiver.complete(&tail).await.unwrap(),
        Settlement::Deferred
    );
    receiver.shutdown().await.unwrap();
    assert_eq!(committed(&group, &topic, 0).await, Some(3));
    publisher.shutdown().await.unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn elapsed_interval_flushes_an_idle_tail_through_regular_caller_polls() {
    let topic = name("idle-flush");
    let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    let admin = Admin::connect(&settings()).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let publisher = Publisher::connect(settings(), route(&topic)).await.unwrap();
    for value in [b"reset-clock".as_slice(), b"idle-tail"] {
        publisher
            .publish_record("input", Record::value(None, value, &[]))
            .await
            .unwrap();
    }
    let group = format!("raddle-idle-{}", Uuid::new_v4());
    let mut receiver = Receiver::connect(
        bounded(3),
        subscription(
            &group,
            &topic,
            CommitStrategy::Interval(Duration::from_secs(2)),
        ),
    )
    .await
    .unwrap();
    let first = next(&mut receiver).await;
    let tail = next(&mut receiver).await;
    receiver.complete(&first).await.unwrap();
    receiver.flush().await.unwrap();
    // Include the broker probe in elapsed time, rather than assuming that RPC
    // stays fast on a shared, heavily loaded qualification host.
    let start = Instant::now();
    assert_eq!(
        receiver.complete(&tail).await.unwrap(),
        Settlement::Deferred
    );
    assert_eq!(committed(&group, &topic, 0).await, Some(1));
    let deadline = start + Duration::from_secs(10);
    while receiver.pending() != 0 {
        assert!(receiver.receive().await.unwrap().is_none());
        assert!(Instant::now() < deadline, "idle tail did not flush");
    }
    assert!(start.elapsed() >= Duration::from_secs(1));
    assert_eq!(committed(&group, &topic, 0).await, Some(2));
    assert_eq!(
        receiver.complete(&tail).await.unwrap(),
        Settlement::Committed
    );
    receiver.shutdown().await.unwrap();
    publisher.shutdown().await.unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn count_flush_confirms_other_partition_without_crossing_a_blocked_prefix() {
    let topic = name("independent-prefix");
    let specs = [TopicSpec::managed(topic.clone(), 2, 1).unwrap()];
    let admin = Admin::connect(&settings()).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let producer: FutureProducer = config().create().unwrap();
    for (partition, payload) in [
        (0, b"blocked".as_slice()),
        (0, b"later"),
        (1, b"independent"),
    ] {
        producer
            .send(
                FutureRecord::<(), [u8]>::to(topic.as_str())
                    .partition(partition)
                    .payload(payload),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    let group = format!("raddle-independent-{}", Uuid::new_v4());
    let mut receiver = Receiver::connect(
        bounded(4),
        subscription(&group, &topic, CommitStrategy::BatchCount(2)),
    )
    .await
    .unwrap();
    let records = [
        next(&mut receiver).await,
        next(&mut receiver).await,
        next(&mut receiver).await,
    ];
    let blocked = records
        .iter()
        .find(|record| record.message().partition() == 0 && record.message().offset() == 0)
        .unwrap();
    let later = records
        .iter()
        .find(|record| record.message().partition() == 0 && record.message().offset() == 1)
        .unwrap();
    let independent = records
        .iter()
        .find(|record| record.message().partition() == 1)
        .unwrap();
    assert_eq!(
        receiver.complete(later).await.unwrap(),
        Settlement::Deferred
    );
    // Repeating a completion must not make the count threshold fire early.
    assert_eq!(
        receiver.complete(later).await.unwrap(),
        Settlement::Deferred
    );
    assert_eq!(
        receiver.complete(independent).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(committed(&group, &topic, 0).await, None);
    assert_eq!(committed(&group, &topic, 1).await, Some(1));
    assert_eq!(receiver.pending(), 2);
    assert_eq!(
        receiver.complete(blocked).await.unwrap(),
        Settlement::Deferred
    );
    receiver.flush().await.unwrap();
    assert_eq!(committed(&group, &topic, 0).await, Some(2));
    assert_eq!(receiver.pending(), 0);
    receiver.shutdown().await.unwrap();
    tokio::task::spawn_blocking(move || drop(producer))
        .await
        .unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn count_threshold_counts_new_completions_not_retained_completed_records() {
    let topic = name("completion-count");
    let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    let admin = Admin::connect(&settings()).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let publisher = Publisher::connect(settings(), route(&topic)).await.unwrap();
    for payload in [b"zero".as_slice(), b"one", b"two", b"three"] {
        publisher
            .publish_record("input", Record::value(None, payload, &[]))
            .await
            .unwrap();
    }
    let group = format!("raddle-count-reset-{}", Uuid::new_v4());
    let mut receiver = Receiver::connect(
        bounded(8),
        subscription(&group, &topic, CommitStrategy::BatchCount(3)),
    )
    .await
    .unwrap();
    let records = [
        next(&mut receiver).await,
        next(&mut receiver).await,
        next(&mut receiver).await,
        next(&mut receiver).await,
    ];
    for index in [0, 2, 3] {
        assert_eq!(
            receiver.complete(&records[index]).await.unwrap(),
            Settlement::Deferred
        );
    }
    assert_eq!(committed(&group, &topic, 0).await, Some(1));
    assert_eq!(receiver.pending(), 3);
    // Two retained completions were already counted in the preceding flush.
    // This is only one new completion, so it must wait for explicit/tail flush.
    assert_eq!(
        receiver.complete(&records[1]).await.unwrap(),
        Settlement::Deferred
    );
    assert_eq!(committed(&group, &topic, 0).await, Some(1));
    receiver.flush().await.unwrap();
    assert_eq!(committed(&group, &topic, 0).await, Some(4));
    assert_eq!(receiver.pending(), 0);
    receiver.shutdown().await.unwrap();
    publisher.shutdown().await.unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}
