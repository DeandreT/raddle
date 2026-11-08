//! Broker-reported group type, not an inference from cooperative rebalance mode.

use std::process::Command;
use std::time::{Duration, Instant};

use raddle_kafka::{
    Admin, CommitStrategy, ConsumerSettings, Error, Membership, Receiver, Settlement, TopicSpec,
};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use uuid::Uuid;

use super::{config, name, next, settings};

async fn cli(script: &str, arguments: Vec<String>) -> String {
    let container = std::env::var("RADDLE_KAFKA_TEST_CONTAINER").unwrap();
    assert!(
        (12..=64).contains(&container.len())
            && container.bytes().all(|byte| byte.is_ascii_hexdigit())
    );
    let script = format!("/opt/kafka/bin/{script}");
    tokio::task::spawn_blocking(move || {
        // Bound the external diagnostic process, not the receiver's native Drop.
        let output = Command::new("timeout")
            .args([
                "60s",
                "docker",
                "exec",
                &container,
                &script,
                "--bootstrap-server",
                "localhost:9092",
            ])
            .args(arguments)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "Kafka CLI failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires owned Kafka 4 container via conformance/kafka/run.py"]
async fn classic_cooperative_and_kip848_join_with_broker_observed_group_types() {
    let topic = name("membership");
    let specs = [TopicSpec::managed(topic.clone(), 2, 1).unwrap()];
    let admin = Admin::connect(&settings()).await.unwrap();
    admin.declare(&specs).await.unwrap();
    let producer: FutureProducer = config().create().unwrap();
    for partition in 0..2 {
        producer
            .send(
                FutureRecord::<(), [u8]>::to(topic.as_str())
                    .partition(partition)
                    .payload(b"membership"),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    for (membership, group_type, assignor) in [
        (Membership::Classic, "Classic", "range"),
        (Membership::Cooperative, "Classic", "cooperative-sticky"),
        (Membership::Consumer, "Consumer", "uniform"),
    ] {
        let group = format!("raddle-membership-{}", Uuid::new_v4());
        let subscription = || {
            ConsumerSettings::try_new(
                &group,
                vec![topic.clone()],
                membership,
                None,
                CommitStrategy::PerMessage,
            )
            .unwrap()
            .from_beginning()
        };
        let mut first = Receiver::connect(settings(), subscription()).await.unwrap();
        let originals = [next(&mut first).await, next(&mut first).await];
        assert_ne!(
            originals[0].message().partition(),
            originals[1].message().partition()
        );
        let mut second = Receiver::connect(settings(), subscription()).await.unwrap();
        let mut newly_assigned = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(40);
        let moved = loop {
            if let Some(record) = first.receive().await.unwrap() {
                newly_assigned.push(record);
            }
            if let Some(record) = second.receive().await.unwrap() {
                break record;
            }
            assert!(
                Instant::now() < deadline,
                "second consumer never acquired a partition"
            );
        };
        assert_eq!(moved.message().payload(), Some(b"membership".as_slice()));
        // Harvest the first client's revoke before checking its old handles.
        if let Some(record) = first.receive().await.unwrap() {
            newly_assigned.push(record);
        }
        let revoked = originals
            .iter()
            .find(|record| record.message().partition() == moved.message().partition())
            .unwrap();
        let retained = originals
            .iter()
            .find(|record| record.message().partition() != moved.message().partition())
            .unwrap();
        assert_eq!(first.complete(revoked).await, Err(Error::Stale));
        match membership {
            Membership::Classic => {
                // Eager rebalancing revokes/reacquires every partition, including
                // one assigned back to the same client. Its old handle is stale.
                assert_eq!(first.complete(retained).await, Err(Error::Stale));
                let fresh = if let Some(record) = newly_assigned.pop() {
                    record
                } else {
                    next(&mut first).await
                };
                assert_eq!(fresh.message().partition(), retained.message().partition());
                assert_eq!(first.complete(&fresh).await.unwrap(), Settlement::Committed);
            }
            Membership::Cooperative | Membership::Consumer => {
                assert!(
                    newly_assigned.is_empty(),
                    "unaffected partition was unnecessarily replayed"
                );
                assert_eq!(
                    first.complete(retained).await.unwrap(),
                    Settlement::Committed
                );
            }
        }
        assert_eq!(
            second.complete(&moved).await.unwrap(),
            Settlement::Committed
        );
        let groups = cli("kafka-groups.sh", vec!["--list".into()]).await;
        println!("Kafka 4 group-type observation for {membership:?}:\n{groups}");
        let row = groups
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>())
            .find(|fields| fields.first() == Some(&group.as_str()))
            .unwrap();
        assert_eq!(row.get(1), Some(&group_type), "{groups}");
        let state = cli(
            "kafka-consumer-groups.sh",
            vec![
                "--describe".into(),
                "--group".into(),
                group,
                "--state".into(),
            ],
        )
        .await;
        println!("Kafka 4 assignor observation for {membership:?}:\n{state}");
        assert!(
            state.split_whitespace().any(|field| field == assignor),
            "{state}"
        );
        assert!(
            state.split_whitespace().any(|field| field == "Stable"),
            "{state}"
        );
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
    }
    tokio::task::spawn_blocking(move || drop(producer))
        .await
        .unwrap();
    admin.delete_managed(&specs).await.unwrap();
    admin.shutdown().await.unwrap();
}
