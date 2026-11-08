//! Fault-injected native SDK calls. This is not broker interoperability proof.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::time::Duration;

use raddle_kafka::{
    ClientSettings, CommitStrategy, ConsumerSettings, Error, Limits, Membership, Publisher,
    Received, Receiver, Record, Settlement, Topic,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};

fn settings(servers: &str) -> ClientSettings {
    configured_settings(servers, |_| {})
}

fn configured_settings(servers: &str, configure: impl FnOnce(&mut ClientConfig)) -> ClientSettings {
    let mut native = ClientConfig::new();
    native
        .set("bootstrap.servers", servers)
        .set("security.protocol", "plaintext");
    configure(&mut native);
    ClientSettings::try_new(
        native,
        Limits {
            max_pending: 2,
            deadline: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap()
}

async fn next(receiver: &mut Receiver) -> Received {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(record) = receiver.receive().await.unwrap() {
                return record;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_delayed_and_denied_commits_preserve_custody_across_canceled_waits() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("custody", 1, 1).unwrap();
    let topic = Topic::try_new("custody").unwrap();
    let servers = cluster.bootstrap_servers();
    let publisher = Publisher::connect(
        settings(&servers),
        BTreeMap::from([("input".into(), topic.clone())]),
    )
    .await
    .unwrap();
    for payload in [b"first".as_slice(), b"last"] {
        publisher
            .publish_record("input", Record::value(None, payload, &[]))
            .await
            .unwrap();
    }
    let subscription = ConsumerSettings::try_new(
        "controlled-native",
        vec![topic],
        Membership::Classic,
        None,
        CommitStrategy::PerMessage,
    )
    .unwrap()
    .from_beginning();
    let mut receiver = Receiver::connect(settings(&servers), subscription)
        .await
        .unwrap();
    let first = next(&mut receiver).await;
    let last = next(&mut receiver).await;
    cluster
        .broker_round_trip_time(1, Duration::from_millis(500))
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), receiver.complete(&first))
            .await
            .is_err()
    );
    assert_eq!(receiver.pending(), 2);
    // A second canceled call must join the original outstanding native commit,
    // not start a competing commit/poll or discard the original result.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), receiver.receive())
            .await
            .is_err()
    );
    assert_eq!(receiver.pending(), 2);
    cluster.broker_round_trip_time(1, Duration::ZERO).unwrap();
    assert_eq!(
        receiver.complete(&first).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(receiver.pending(), 1);

    cluster.request_errors(
        RDKafkaApiKey::OffsetCommit,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    );
    assert_eq!(receiver.complete(&last).await, Err(Error::Uncertain));
    assert_eq!(receiver.pending(), 1);
    assert_eq!(last.message().payload(), Some(b"last".as_slice()));
    // Correcting the fault needs only offset retry, not new receipt/handling.
    cluster.clear_request_errors(RDKafkaApiKey::OffsetCommit);
    receiver.flush().await.unwrap();
    assert_eq!(receiver.pending(), 0);
    assert_eq!(
        receiver.complete(&last).await.unwrap(),
        Settlement::Committed
    );
    receiver.shutdown().await.unwrap();
    publisher.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_partial_commit_failure_does_not_hide_an_independent_success() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("partial", 2, 1).unwrap();
    let servers = cluster.bootstrap_servers();
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &servers);
    let producer: FutureProducer = config.create().unwrap();
    for partition in 0..2 {
        producer
            .send(
                FutureRecord::<(), [u8]>::to("partial")
                    .partition(partition)
                    .payload(b"custody"),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    let settings = ClientSettings::try_new(
        {
            config.set("security.protocol", "plaintext");
            config
        },
        Limits {
            max_pending: 4,
            deadline: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap();
    let subscription = ConsumerSettings::try_new(
        "partial-native",
        vec![Topic::try_new("partial").unwrap()],
        Membership::Classic,
        None,
        CommitStrategy::BatchCount(1000),
    )
    .unwrap()
    .from_beginning();
    let mut receiver = Receiver::connect(settings, subscription).await.unwrap();
    let records = [next(&mut receiver).await, next(&mut receiver).await];
    for record in &records {
        assert_eq!(
            receiver.complete(record).await.unwrap(),
            Settlement::Deferred
        );
    }
    cluster.request_errors(
        RDKafkaApiKey::OffsetCommit,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    );
    assert_eq!(receiver.flush().await, Err(Error::Uncertain));
    assert_eq!(receiver.pending(), 1);
    // Native requests are ordered by partition; only partition zero was denied.
    let first = records
        .iter()
        .find(|record| record.message().partition() == 0)
        .unwrap();
    let second = records
        .iter()
        .find(|record| record.message().partition() == 1)
        .unwrap();
    assert_eq!(
        receiver.complete(second).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(
        receiver.complete(first).await.unwrap(),
        Settlement::Deferred
    );
    cluster.clear_request_errors(RDKafkaApiKey::OffsetCommit);
    receiver.flush().await.unwrap();
    assert_eq!(
        receiver.complete(first).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(receiver.pending(), 0);
    receiver.shutdown().await.unwrap();
    tokio::task::spawn_blocking(move || drop(producer))
        .await
        .unwrap();
}

async fn recovering_next(receiver: &mut Receiver, attempts: usize) -> Option<Received> {
    let mut empty = 0;
    let mut errors = 0;
    for _ in 0..attempts {
        match receiver.receive().await {
            Ok(Some(record)) => return Some(record),
            Ok(None) => empty += 1,
            Err(Error::Uncertain | Error::Stale | Error::Capacity) => errors += 1,
            Err(error) => panic!("unexpected receiving failure: {error}"),
        }
    }
    eprintln!(
        "recovery ended: empty={empty}, offset/capacity errors={errors}, pending={}",
        receiver.pending()
    );
    None
}

async fn committed(servers: &str, group: &str, topic: &str, partition: i32) -> Option<i64> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", servers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    let topic = topic.to_owned();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = config.create().unwrap();
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition(&topic, partition);
        let offsets = consumer
            .committed_offsets(offsets, Duration::from_secs(10))
            .unwrap();
        let entry = offsets.find_partition(&topic, partition).unwrap();
        entry.error().unwrap();
        match entry.offset() {
            Offset::Offset(value) => Some(value),
            Offset::Invalid => None,
            unexpected => panic!("unexpected committed offset: {unexpected:?}"),
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn illegal_generation_commit_failure_still_polls_revoke_and_rejoins() {
    for canceled in [false, true] {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("generation", 1, 1).unwrap();
        let topic = Topic::try_new("generation").unwrap();
        let servers = cluster.bootstrap_servers();
        let publisher = Publisher::connect(
            settings(&servers),
            BTreeMap::from([("input".into(), topic.clone())]),
        )
        .await
        .unwrap();
        publisher
            .publish_record("input", Record::value(None, b"already-custodied", &[]))
            .await
            .unwrap();
        if canceled {
            publisher
                .publish_record("input", Record::value(None, b"unfinished-before-loss", &[]))
                .await
                .unwrap();
        }
        let subscription = ConsumerSettings::try_new(
            "generation-native",
            vec![topic],
            Membership::Classic,
            None,
            CommitStrategy::PerMessage,
        )
        .unwrap()
        .from_beginning();
        // The SDK mock waits session.timeout.ms - 1000 on a repeat classic
        // join. Bound that fixture delay, without changing adapter defaults.
        let consumer = configured_settings(&servers, |native| {
            native
                .set("session.timeout.ms", "6000")
                .set("heartbeat.interval.ms", "1000");
        });
        let mut receiver = Receiver::connect(consumer, subscription).await.unwrap();
        let original = next(&mut receiver).await;
        let unfinished = if canceled {
            Some(next(&mut receiver).await)
        } else {
            None
        };
        if canceled {
            assert_eq!(receiver.pending(), 2);
        }
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION],
        );
        if canceled {
            cluster
                .broker_round_trip_time(1, Duration::from_millis(500))
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(20), receiver.complete(&original))
                    .await
                    .is_err()
            );
            cluster.broker_round_trip_time(1, Duration::ZERO).unwrap();
            assert!(matches!(
                receiver.receive().await,
                Err(Error::Uncertain | Error::Stale)
            ));
        } else {
            assert_eq!(receiver.complete(&original).await, Err(Error::Uncertain));
        }
        assert_eq!(
            committed(&servers, "generation-native", "generation", 0).await,
            None
        );
        cluster.clear_request_errors(RDKafkaApiKey::OffsetCommit);
        let recovered =
            tokio::time::timeout(Duration::from_secs(25), recovering_next(&mut receiver, 400))
                .await
                .unwrap()
                .unwrap_or_else(|| {
                    panic!("commit failure prevented polling the queued revoke and rejoining (canceled={canceled})")
                });
        assert_eq!(recovered.message().offset(), original.message().offset());
        assert_eq!(recovered.message().payload(), original.message().payload());
        assert_eq!(receiver.complete(&original).await, Err(Error::Stale));
        if let Some(unfinished) = &unfinished {
            assert_eq!(receiver.complete(unfinished).await, Err(Error::Stale));
        }
        // Reassignment legitimately redelivers the uncommitted broker record.
        // Reuse the already-held application custody, without invoking a handler.
        assert_eq!(
            receiver.complete(&recovered).await.unwrap(),
            Settlement::Committed
        );
        if let Some(unfinished) = unfinished {
            assert_eq!(
                committed(&servers, "generation-native", "generation", 0).await,
                Some(1)
            );
            let replayed = next(&mut receiver).await;
            assert_eq!(replayed.message().offset(), unfinished.message().offset());
            assert_eq!(replayed.message().payload(), unfinished.message().payload());
            assert_eq!(
                receiver.complete(&replayed).await.unwrap(),
                Settlement::Committed
            );
        }
        publisher
            .publish_record("input", Record::value(None, b"after-rejoin", &[]))
            .await
            .unwrap();
        let tail = next(&mut receiver).await;
        assert_eq!(tail.message().payload(), Some(b"after-rejoin".as_slice()));
        assert_eq!(
            receiver.complete(&tail).await.unwrap(),
            Settlement::Committed
        );
        assert_eq!(receiver.pending(), 0);
        receiver.shutdown().await.unwrap();
        publisher.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_partition_commit_denial_does_not_starve_independent_fetching() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("denied", 2, 1).unwrap();
    let servers = cluster.bootstrap_servers();
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &servers);
    let producer: FutureProducer = config.create().unwrap();
    producer
        .send(
            FutureRecord::<(), [u8]>::to("denied")
                .partition(0)
                .payload(b"blocked"),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    let subscription = ConsumerSettings::try_new(
        "denied-native",
        vec![Topic::try_new("denied").unwrap()],
        Membership::Classic,
        None,
        CommitStrategy::PerMessage,
    )
    .unwrap()
    .from_beginning();
    let mut receiver = Receiver::connect(settings(&servers), subscription)
        .await
        .unwrap();
    let blocked = next(&mut receiver).await;
    assert_eq!(blocked.message().partition(), 0);
    let denial = RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED;
    cluster.request_errors(RDKafkaApiKey::OffsetCommit, &[denial]);
    assert_eq!(receiver.complete(&blocked).await, Err(Error::Uncertain));
    for expected in 0..2 {
        cluster.request_errors(RDKafkaApiKey::OffsetCommit, &[denial; 128]);
        producer
            .send(
                FutureRecord::<(), [u8]>::to("denied")
                    .partition(1)
                    .payload(b"independent"),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        let record =
            tokio::time::timeout(Duration::from_secs(15), recovering_next(&mut receiver, 32))
                .await
                .unwrap()
                .unwrap_or_else(|| {
                    panic!("persistent offset denial prevented fetching another partition")
                });
        assert_eq!(record.message().partition(), 1);
        assert_eq!(record.message().offset(), expected);
        assert_eq!(
            committed(&servers, "denied-native", "denied", 0).await,
            None
        );
        // The serialized requests target partition zero then one. Keep zero
        // denied while acknowledging only this independent partition's custody.
        cluster.clear_request_errors(RDKafkaApiKey::OffsetCommit);
        cluster.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[denial, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR],
        );
        assert_eq!(receiver.complete(&record).await, Err(Error::Uncertain));
        assert_eq!(
            committed(&servers, "denied-native", "denied", 1).await,
            Some(expected + 1)
        );
        assert_eq!(receiver.pending(), 1);
    }
    assert_eq!(
        committed(&servers, "denied-native", "denied", 0).await,
        None
    );
    cluster.clear_request_errors(RDKafkaApiKey::OffsetCommit);
    receiver.flush().await.unwrap();
    assert_eq!(
        receiver.complete(&blocked).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(receiver.pending(), 0);
    receiver.shutdown().await.unwrap();
    tokio::task::spawn_blocking(move || drop(producer))
        .await
        .unwrap();
}
