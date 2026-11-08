//! Actual CRC-only Fetch corruption. Never run against an ordinary broker fixture.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use raddle_kafka::{
    Admin, ClientSettings, CommitStrategy, ConsumerSettings, Error, Limits, Membership, Publisher,
    Received, Receiver, Record, Settlement, Topic, TopicSpec,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::{Value, json};
use uuid::Uuid;

fn config() -> ClientConfig {
    let address: SocketAddr = std::env::var("RADDLE_KAFKA_TEST_BROKERS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(address.ip().is_loopback() && address.is_ipv4());
    let mut native = ClientConfig::new();
    native
        .set("bootstrap.servers", address.to_string())
        .set("security.protocol", "plaintext");
    native
}

fn settings(client: Option<(&str, bool)>) -> ClientSettings {
    let mut native = config();
    if let Some((client, check)) = client {
        native
            .set("client.id", client)
            .set("check.crcs", if check { "true" } else { "false" });
    }
    ClientSettings::try_new(
        native,
        Limits {
            deadline: Duration::from_secs(20),
            ..Default::default()
        },
    )
    .unwrap()
}

async fn control(operation: &str, target: Value) -> Value {
    let address: SocketAddr = std::env::var("RADDLE_KAFKA_CRC_CONTROL")
        .unwrap_or_else(|_| {
            panic!(
                "run conformance/kafka/run.py test --mode crc; an ordinary broker is insufficient"
            )
        })
        .parse()
        .unwrap();
    assert!(address.ip().is_loopback() && address.is_ipv4());
    let request = json!({
        "op": operation,
        "token": std::env::var("RADDLE_KAFKA_CRC_TOKEN").unwrap(),
        "target": target,
    });
    tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        serde_json::to_writer(&mut stream, &request).unwrap();
        stream.write_all(b"\n").unwrap();
        let mut line = String::new();
        BufReader::new(std::io::Read::take(stream, 65536))
            .read_line(&mut line)
            .unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert!(
            response.get("error").is_none(),
            "control refused: {response}"
        );
        response
    })
    .await
    .unwrap()
}

async fn arm(client: &str, topic: &Topic, offset: i64) {
    let state = control(
        "arm",
        json!({"client": client, "topic": topic.as_str(), "partition": 0, "offset": offset}),
    )
    .await;
    assert_eq!(state["armed"], true);
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

fn subscription(group: &str, topic: &Topic) -> ConsumerSettings {
    ConsumerSettings::try_new(
        group,
        vec![topic.clone()],
        Membership::Classic,
        None,
        CommitStrategy::PerMessage,
    )
    .unwrap()
    .from_beginning()
}

async fn committed(group: &str, topic: &Topic) -> i64 {
    let mut native = config();
    native
        .set("group.id", group)
        .set("enable.auto.commit", "false");
    let topic = topic.as_str().to_owned();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = native.create().unwrap();
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition(&topic, 0);
        let result = consumer
            .committed_offsets(offsets, Duration::from_secs(10))
            .unwrap();
        let entry = result.find_partition(&topic, 0).unwrap();
        entry.error().unwrap();
        match entry.offset() {
            Offset::Offset(offset) => offset,
            other => panic!("expected established broker watermark, got {other:?}"),
        }
    })
    .await
    .unwrap()
}

async fn receiver_case(check_crcs: bool) {
    let suffix = Uuid::new_v4();
    let topic = Topic::try_new(format!("raddle-crc-{suffix}")).unwrap();
    let client = format!("raddle-crc-receiver-{check_crcs}");
    let group = format!("raddle-crc-group-{suffix}");
    let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    let admin = Admin::connect(&settings(None)).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let publisher = Publisher::connect(
        settings(None),
        BTreeMap::from([("input".into(), topic.clone())]),
    )
    .await
    .unwrap();
    let baseline = publisher
        .publish_record("input", Record::value(None, b"healthy", &[]))
        .await
        .unwrap();
    assert_eq!(baseline.offset, 0);
    let mut receiver = Receiver::connect(
        settings(Some((&client, check_crcs))),
        subscription(&group, &topic),
    )
    .await
    .unwrap();
    let healthy = next(&mut receiver).await;
    assert_eq!(healthy.message().payload(), Some(b"healthy".as_slice()));
    assert_eq!(
        receiver.complete(&healthy).await.unwrap(),
        Settlement::Committed
    );
    assert_eq!(committed(&group, &topic).await, 1);
    let held_offset = publisher
        .publish_record("input", Record::value(None, b"unfinished", &[]))
        .await
        .unwrap()
        .offset;
    let held = next(&mut receiver).await;
    assert_eq!(held.message().offset(), held_offset);
    assert_eq!(held_offset, 1);
    assert_eq!(receiver.pending(), 1);

    // Arm before publishing, not after an arbitrary sleep: SDK background Fetch
    // cannot prefetch this as-yet nonexistent, separately confirmed batch.
    arm(&client, &topic, held_offset + 1).await;
    let body = b"unchanged\0binary\xffrecord";
    let target = publisher
        .publish_record("input", Record::value(Some(b"\0\xffkey"), body, &[]))
        .await
        .unwrap();
    assert_eq!(target.offset, held_offset + 1);
    if check_crcs {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match receiver.receive().await {
                    Err(Error::ReceiverFenced) => break,
                    Ok(None) => {}
                    Ok(Some(_)) => panic!("CRC-corrupted batch reached the application"),
                    Err(error) => panic!("unexpected native outcome: {error:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(receiver.complete(&held).await, Err(Error::ReceiverFenced));
        assert_eq!(receiver.flush().await, Err(Error::ReceiverFenced));
        assert!(matches!(
            receiver.receive().await,
            Err(Error::ReceiverFenced)
        ));
        assert_eq!(receiver.pending(), 1);
        assert_eq!(committed(&group, &topic).await, 1);
        assert_eq!(receiver.shutdown().await, Err(Error::ReceiverFenced));
        assert_eq!(committed(&group, &topic).await, 1);
        control("disarm", Value::Null).await;
        let recovery_client = "raddle-crc-recovery";
        let mut recovery = Receiver::connect(
            settings(Some((recovery_client, true))),
            subscription(&group, &topic),
        )
        .await
        .unwrap();
        let replayed_held = next(&mut recovery).await;
        let replayed_target = next(&mut recovery).await;
        assert_eq!(replayed_held.message().offset(), held_offset);
        assert_eq!(replayed_held.message().payload(), held.message().payload());
        assert_eq!(replayed_target.message().offset(), target.offset);
        assert_eq!(replayed_target.message().payload(), Some(body.as_slice()));
        assert_eq!(
            replayed_target.message().key(),
            Some(b"\0\xffkey".as_slice())
        );
        assert_eq!(recovery.complete(&held).await, Err(Error::Stale));
        assert_eq!(
            recovery.complete(&replayed_target).await.unwrap(),
            Settlement::Deferred
        );
        assert_eq!(committed(&group, &topic).await, 1);
        assert_eq!(
            recovery.complete(&replayed_held).await.unwrap(),
            Settlement::Committed
        );
        recovery.shutdown().await.unwrap();
    } else {
        // Causal control ONLY. This is not a supported workaround for corruption.
        let unchanged = next(&mut receiver).await;
        assert_eq!(unchanged.message().offset(), target.offset);
        assert_eq!(unchanged.message().payload(), Some(body.as_slice()));
        assert_eq!(unchanged.message().key(), Some(b"\0\xffkey".as_slice()));
        unchanged.validate().unwrap();
        assert_eq!(
            receiver.complete(&unchanged).await.unwrap(),
            Settlement::Deferred
        );
        assert_eq!(committed(&group, &topic).await, 1);
        assert_eq!(
            receiver.complete(&held).await.unwrap(),
            Settlement::Committed
        );
        receiver.shutdown().await.unwrap();
    }
    assert_eq!(committed(&group, &topic).await, 3);
    let status = control("status", Value::Null).await;
    assert_eq!(status["armed"], false);
    assert_eq!(status["errors"], json!([]));
    let receipt = status["receipts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["client"] == client)
        .unwrap();
    assert_eq!(receipt["base_offset"], target.offset);
    assert_eq!(receipt["fetch_version"], 4);
    assert_eq!(receipt["changed_bytes"], 1);
    eprintln!(
        "CRC receiver check.crcs={check_crcs}: held=1 target=2 watermark_before=1 watermark_after=3; fence_and_recovery={check_crcs}; wire={receipt}"
    );
    publisher.shutdown().await.unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}

async fn native_bad_message() {
    let topic = Topic::try_new(format!("raddle-crc-native-{}", Uuid::new_v4())).unwrap();
    let specs = [TopicSpec::managed(topic.clone(), 1, 1).unwrap()];
    let admin = Admin::connect(&settings(None)).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let publisher = Publisher::connect(
        settings(None),
        BTreeMap::from([("input".into(), topic.clone())]),
    )
    .await
    .unwrap();
    let target = publisher
        .publish_record("input", Record::value(None, b"native-proof", &[]))
        .await
        .unwrap();
    let client = "raddle-crc-native-probe";
    arm(client, &topic, target.offset).await;
    let mut native = config();
    native
        .set("client.id", client)
        .set("check.crcs", "true")
        .set("group.id", format!("raddle-crc-probe-{}", Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    let native_error = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = native.create().unwrap();
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(topic.as_str(), 0, Offset::Offset(target.offset))
            .unwrap();
        consumer.assign(&assignment).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Err(error)) => return error,
                Some(Ok(_)) => panic!("native CRC check accepted the corrupted record"),
                None => {}
            }
        }
        panic!("native client never reported the wire CRC failure")
    })
    .await
    .unwrap();
    assert_eq!(
        native_error,
        KafkaError::MessageConsumption(RDKafkaErrorCode::BadMessage)
    );
    eprintln!("CRC native SDK observation: {native_error:?}");
    publisher.shutdown().await.unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires run-owned Kafka 4 CRC proxy: python3 conformance/kafka/run.py test --mode crc"]
async fn actual_crc_wire_corruption_fences_held_custody_and_recovers_without_offset_skip() {
    // Fail before any broker activity when accidentally selected on normal sockets.
    let initial = control("status", Value::Null).await;
    assert_eq!(initial["receipts"], json!([]));
    receiver_case(true).await;
    receiver_case(false).await;
    native_bad_message().await;
    let status = control("status", Value::Null).await;
    assert_eq!(status["receipts"].as_array().unwrap().len(), 3);
    assert_eq!(status["errors"], json!([]));
    assert_eq!(status["armed"], false);
    assert!(status["fetches"].as_u64().unwrap() >= 3);
    for receipt in status["receipts"].as_array().unwrap() {
        assert_eq!(receipt["magic"], 2);
        assert_eq!(receipt["changed_bits"], 1);
        assert_ne!(receipt["crc_before"], receipt["crc_after"]);
    }
}
