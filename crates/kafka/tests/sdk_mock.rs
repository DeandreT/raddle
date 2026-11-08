//! Fault-injected native SDK calls. This is not broker interoperability proof.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::time::Duration;

use raddle_kafka::{
    ClientSettings, CommitStrategy, ConsumerSettings, Error, Limits, Membership, Publisher,
    Received, Receiver, Record, Settlement, Topic,
};
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};

fn settings(servers: &str) -> ClientSettings {
    let mut native = ClientConfig::new();
    native
        .set("bootstrap.servers", servers)
        .set("security.protocol", "plaintext");
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
