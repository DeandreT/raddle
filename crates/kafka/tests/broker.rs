//! Native Kafka 4 socket contracts; no cloud or foreign runtime is involved.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use raddle::wire::{Envelope, WireMessage};
use raddle_kafka::{
    Admin, ClientSettings, CommitStrategy, ConsumerSettings, Error, Header, Limits, Membership,
    Publisher, Received, Receiver, Record, Settlement, Topic, TopicSpec, TopologyOutcome,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[path = "broker/custody.rs"]
mod custody;
#[path = "broker/membership.rs"]
mod membership;
#[path = "broker/transactions.rs"]
mod transactions;

fn config() -> ClientConfig {
    let servers = std::env::var("RADDLE_KAFKA_TEST_BROKERS").unwrap();
    // The fixture is an explicit synthetic loopback broker; these tests never
    // acquire provisioning authority over arbitrary production clusters.
    assert!(servers.starts_with("127.0.0.1:"));
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", servers)
        .set("security.protocol", "plaintext")
        .set("enable.idempotence", "true")
        .set("isolation.level", "read_committed");
    config
}

fn settings() -> ClientSettings {
    ClientSettings::try_new(
        config(),
        Limits {
            deadline: Duration::from_secs(20),
            ..Default::default()
        },
    )
    .unwrap()
}

fn name(label: &str) -> Topic {
    Topic::try_new(format!("raddle-{label}-{}", Uuid::new_v4())).unwrap()
}

fn route(topic: &Topic) -> BTreeMap<String, Topic> {
    BTreeMap::from([("input".into(), topic.clone())])
}

async fn next(receiver: &mut Receiver) -> Received {
    tokio::time::timeout(Duration::from_secs(30), async {
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
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn native_receiver_commits_only_completed_prefix_and_fences_other_receiver_handles() {
    let topic = name("custody");
    let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    let admin = Admin::connect(&settings()).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let publisher = Publisher::connect(settings(), route(&topic)).await.unwrap();
    for value in [
        b"first".as_slice(),
        b"middle".as_slice(),
        b"last".as_slice(),
    ] {
        publisher
            .publish_record("input", Record::value(None, value, &[]))
            .await
            .unwrap();
    }
    let group = format!("raddle-custody-{}", Uuid::new_v4());
    let subscription = || {
        ConsumerSettings::try_new(
            &group,
            vec![topic.clone()],
            Membership::Classic,
            None,
            CommitStrategy::PerMessage,
        )
        .unwrap()
        .from_beginning()
    };
    let mut receiver = Receiver::connect(settings(), subscription()).await.unwrap();
    let first = next(&mut receiver).await;
    let middle = next(&mut receiver).await;
    let last = next(&mut receiver).await;
    first.validate().unwrap();
    assert_eq!(first.message().payload(), Some(b"first".as_slice()));
    assert_eq!(
        receiver.complete(&last).await.unwrap(),
        Settlement::Deferred
    );
    assert_eq!(
        receiver.complete(&middle).await.unwrap(),
        Settlement::Deferred
    );
    assert_eq!(receiver.pending(), 3);
    assert_eq!(
        receiver.complete(&first).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(receiver.pending(), 0);
    receiver.shutdown().await.unwrap();
    publisher
        .publish_record("input", Record::value(None, b"tail", &[]))
        .await
        .unwrap();
    let mut successor = Receiver::connect(settings(), subscription()).await.unwrap();
    let tail = next(&mut successor).await;
    assert_eq!(tail.message().payload(), Some(b"tail".as_slice()));
    assert_eq!(successor.complete(&first).await, Err(Error::Stale));
    assert_eq!(successor.pending(), 1);
    assert_eq!(
        successor.complete(&tail).await.unwrap(),
        Settlement::Committed
    );
    successor.shutdown().await.unwrap();
    publisher.shutdown().await.unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}

#[derive(Deserialize, Serialize)]
struct Input(u32);
impl WireMessage for Input {
    const NAME: &'static str = "kafka.socket-input";
    const VERSION: u32 = 1;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn confirmed_publications_preserve_binary_keys_duplicate_headers_empty_values_and_tombstones()
{
    let topic = name("records");
    let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    let admin = Admin::connect(&settings()).await.unwrap();
    let outcomes = admin.declare(&specs).await.unwrap();
    assert_eq!(outcomes[0].result, Ok(TopologyOutcome::Created));
    let publisher = Publisher::connect(settings(), route(&topic)).await.unwrap();
    let headers = [
        Header::try_new("duplicate", None).unwrap(),
        Header::try_new("duplicate", Some(Vec::new())).unwrap(),
        Header::try_new("binary", Some(vec![0, 255, 2])).unwrap(),
    ];
    let key = [0, 255, 7];
    let envelope = Envelope::encode(&Input(42)).unwrap();
    let bytes = envelope.to_vec().unwrap();
    let first = publisher
        .publish_record("input", Record::value(Some(&key), &bytes, &headers))
        .await
        .unwrap();
    let empty = publisher
        .publish_record("input", Record::value(Some(&key), b"", &[]))
        .await
        .unwrap();
    let tombstone = publisher
        .publish_record("input", Record::tombstone(&key, &[]))
        .await
        .unwrap();
    assert_eq!(first.partition, 0);
    assert!(first.offset < empty.offset && empty.offset < tombstone.offset);
    let topic_name = topic.as_str().to_owned();
    let native = config();
    let received = tokio::task::spawn_blocking(move || {
        let mut native = native;
        native
            .set("group.id", format!("raddle-probe-{}", Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer: BaseConsumer = native.create().unwrap();
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(&topic_name, 0, Offset::Offset(first.offset))
            .unwrap();
        consumer.assign(&assignment).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut records = Vec::new();
        while records.len() < 3 && Instant::now() < deadline {
            if let Some(result) = consumer.poll(Duration::from_millis(100)) {
                let message = result.unwrap();
                let headers: Vec<_> = message
                    .headers()
                    .map(|headers| {
                        headers
                            .iter()
                            .map(|header| (header.key.to_owned(), header.value.map(<[u8]>::to_vec)))
                            .collect()
                    })
                    .unwrap_or_default();
                records.push((
                    message.key().map(<[u8]>::to_vec),
                    message.payload().map(<[u8]>::to_vec),
                    headers,
                ));
            }
        }
        records
    })
    .await
    .unwrap();
    assert_eq!(received.len(), 3);
    assert_eq!(received[0].0, Some(key.to_vec()));
    assert_eq!(received[0].1.as_deref(), Some(bytes.as_slice()));
    assert_eq!(
        received[0].2,
        [
            ("duplicate".into(), None),
            ("duplicate".into(), Some(Vec::new())),
            ("binary".into(), Some(vec![0, 255, 2]))
        ]
    );
    assert_eq!(received[1].1, Some(Vec::new()));
    assert_eq!(received[2].1, None);
    publisher.shutdown().await.unwrap();
    assert_eq!(
        admin.delete_managed(&specs).await.unwrap()[0].result,
        Ok(TopologyOutcome::Deleted)
    );
    admin.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn missing_external_topic_and_invalid_records_do_not_block_valid_neighbors_or_create_resources()
 {
    let topic = name("isolation");
    let missing = name("external-missing");
    let admin = Admin::connect(&settings()).await.unwrap();
    let managed = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    assert_eq!(
        admin.declare(&managed).await.unwrap()[0].result,
        Ok(TopologyOutcome::Created)
    );
    let mut routes = route(&topic);
    routes.insert("missing".into(), missing.clone());
    let publisher = Publisher::connect(settings(), routes).await.unwrap();
    let first = publisher
        .publish_record("input", Record::value(None, b"first", &[]))
        .await
        .unwrap();
    assert!(matches!(
        publisher
            .publish_record("missing", Record::value(None, b"no-topic", &[]))
            .await,
        Err(Error::MissingTopic)
    ));
    let oversized = vec![0; Limits::default().max_record_bytes + 1];
    assert_eq!(
        publisher
            .publish_record("input", Record::value(None, &oversized, &[]))
            .await,
        Err(Error::Rejected)
    );
    let last = publisher
        .publish_record("input", Record::value(None, b"last", &[]))
        .await
        .unwrap();
    assert_eq!(last.offset, first.offset + 1);
    let external = [
        TopicSpec::external(missing.clone()),
        TopicSpec::external(topic.clone()),
    ];
    let checked = admin.declare(&external).await.unwrap();
    assert_eq!(checked[0].result, Err(Error::MissingTopic));
    assert_eq!(checked[1].result, Ok(TopologyOutcome::ExternalPresent));
    let deletion = admin.delete_managed(&external).await.unwrap();
    assert!(
        deletion
            .iter()
            .all(|result| result.result == Ok(TopologyOutcome::ExternalUntouched))
    );
    let corrected = [TopicSpec::managed(missing.clone(), 1, 1).unwrap()];
    assert_eq!(
        admin.declare(&corrected).await.unwrap()[0].result,
        Ok(TopologyOutcome::Created)
    );
    let retry = publisher
        .publish_record("missing", Record::value(None, b"no-topic", &[]))
        .await
        .unwrap();
    assert_eq!(retry.offset, 0);
    publisher.shutdown().await.unwrap();
    admin.delete_managed(&managed).await.unwrap();
    admin.delete_managed(&corrected).await.unwrap();
    admin.shutdown().await.unwrap();
}
