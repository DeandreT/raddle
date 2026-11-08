//! Transactional production is fixture-only; the adapter does not add an EOS engine.

use std::time::{Duration, Instant};

use raddle_kafka::{
    Admin, ClientSettings, CommitStrategy, ConsumerSettings, Limits, Membership, Publisher,
    Receiver, Record, Settlement, TopicSpec,
};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use uuid::Uuid;

use super::{config, name, next, route, settings};

async fn send(producer: &FutureProducer, topic: &str, value: &[u8]) -> i64 {
    producer
        .send(
            FutureRecord::<(), [u8]>::to(topic)
                .partition(0)
                .payload(value),
            Duration::from_secs(10),
        )
        .await
        .unwrap()
        .offset
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated Kafka 4 via RADDLE_KAFKA_TEST_BROKERS"]
async fn default_uncommitted_reads_aborted_and_open_transactions_while_opt_in_committed_filters_them()
 {
    let topic = name("isolation-modes");
    let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    let admin = Admin::connect(&settings()).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let publisher = Publisher::connect(settings(), route(&topic)).await.unwrap();
    publisher
        .publish_record("input", Record::value(None, b"before", &[]))
        .await
        .unwrap();
    let mut native = config();
    native.set(
        "transactional.id",
        format!("raddle-fixture-{}", Uuid::new_v4()),
    );
    let producer: FutureProducer = native.create().unwrap();
    let transaction = producer.clone();
    tokio::task::spawn_blocking(move || transaction.init_transactions(Duration::from_secs(20)))
        .await
        .unwrap()
        .unwrap();
    producer.begin_transaction().unwrap();
    let aborted = send(&producer, topic.as_str(), b"aborted").await;
    let transaction = producer.clone();
    tokio::task::spawn_blocking(move || transaction.abort_transaction(Duration::from_secs(20)))
        .await
        .unwrap()
        .unwrap();
    producer.begin_transaction().unwrap();
    let committed = send(&producer, topic.as_str(), b"committed").await;
    let transaction = producer.clone();
    tokio::task::spawn_blocking(move || transaction.commit_transaction(Duration::from_secs(20)))
        .await
        .unwrap()
        .unwrap();
    producer.begin_transaction().unwrap();
    let open = send(&producer, topic.as_str(), b"open").await;
    let after = publisher
        .publish_record("input", Record::value(None, b"after-open", &[]))
        .await
        .unwrap()
        .offset;
    assert!(aborted < committed && committed < open && open < after);

    let subscription = |label: &str| {
        ConsumerSettings::try_new(
            format!("raddle-{label}-{}", Uuid::new_v4()),
            vec![topic.clone()],
            Membership::Classic,
            None,
            CommitStrategy::PerMessage,
        )
        .unwrap()
        .from_beginning()
    };
    let mut uncommitted_config = config();
    uncommitted_config.remove("isolation.level");
    let default = ClientSettings::try_new(uncommitted_config, Limits::default()).unwrap();
    let mut uncommitted = Receiver::connect(default, subscription("uncommitted"))
        .await
        .unwrap();
    let mut observed = Vec::new();
    for expected in [
        b"before".as_slice(),
        b"aborted",
        b"committed",
        b"open",
        b"after-open",
    ] {
        let record = next(&mut uncommitted).await;
        assert_eq!(record.message().payload(), Some(expected));
        observed.push(record);
    }
    assert_eq!(
        uncommitted.complete(&observed[4]).await.unwrap(),
        Settlement::Deferred
    );
    for record in &observed[..4] {
        uncommitted.complete(record).await.unwrap();
    }
    assert_eq!(uncommitted.pending(), 0);

    let mut isolated = Receiver::connect(settings(), subscription("committed"))
        .await
        .unwrap();
    let first = next(&mut isolated).await;
    let second = next(&mut isolated).await;
    assert_eq!(first.message().payload(), Some(b"before".as_slice()));
    assert_eq!(second.message().payload(), Some(b"committed".as_slice()));
    assert_eq!(second.message().offset(), committed);
    assert!(second.message().offset() > first.message().offset() + 1);
    assert_eq!(
        isolated.complete(&second).await.unwrap(),
        Settlement::Deferred
    );
    assert_eq!(
        isolated.complete(&first).await.unwrap(),
        Settlement::Committed
    );
    let until = Instant::now() + Duration::from_millis(600);
    while Instant::now() < until {
        assert!(
            isolated.receive().await.unwrap().is_none(),
            "read_committed crossed an open transaction's LSO"
        );
    }
    let transaction = producer.clone();
    tokio::task::spawn_blocking(move || transaction.abort_transaction(Duration::from_secs(20)))
        .await
        .unwrap()
        .unwrap();
    let tail = next(&mut isolated).await;
    assert_eq!(tail.message().payload(), Some(b"after-open".as_slice()));
    assert_eq!(tail.message().offset(), after);
    assert_eq!(
        isolated.complete(&tail).await.unwrap(),
        Settlement::Committed
    );
    isolated.shutdown().await.unwrap();
    uncommitted.shutdown().await.unwrap();
    tokio::task::spawn_blocking(move || drop(producer))
        .await
        .unwrap();
    publisher.shutdown().await.unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}
