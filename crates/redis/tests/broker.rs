//! Real Redis command, ownership, scheduling and failure-isolation contracts.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use raddle::wire::{Envelope, WireMessage};
use raddle_redis::{
    AckMode, Broker, Config, Durability, Error, GroupStart, Ownership, Route, Source,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct Order(u32);
impl WireMessage for Order {
    const NAME: &'static str = "test.order";
    const VERSION: u32 = 1;
}
fn source() -> Source {
    Source {
        client: "fixture".into(),
        tenant: "trusted".into(),
        endpoint: "orders".into(),
    }
}
fn config() -> Config {
    Config {
        prefetch: 8,
        timeout: Duration::from_secs(3),
        acknowledgement: AckMode::DeleteWhenAcknowledged,
        durability: Durability::FsyncAlways,
        ..Config::default()
    }
}
fn route(alias: &str, stream: &str) -> Route {
    Route {
        alias: alias.into(),
        stream: stream.into(),
    }
}
async fn peer() -> redis::aio::MultiplexedConnection {
    redis::Client::open(std::env::var("RADDLE_REDIS_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn broker(routes: Vec<Route>) -> Broker {
    Broker::connect(
        &std::env::var("RADDLE_REDIS_URL").unwrap(),
        config(),
        routes,
    )
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires disposable real Redis 8.2+ fixture"]
async fn command_capability_atomic_ack_claim_fencing_and_durable_views() {
    let prefix = format!("{{t{}}}", uuid::Uuid::new_v4().simple());
    let stream = format!("{prefix}:orders");
    let bus = broker(vec![
        route("orders", &stream),
        route("dead", &format!("{prefix}:dead")),
    ])
    .await;
    assert!(bus.capabilities().atomic_ack_delete);
    assert!(bus.capabilities().append_only);
    assert_eq!(bus.capabilities().append_fsync, "always");
    let mut raw = peer().await;
    let info: String = redis::cmd("INFO")
        .arg("server")
        .query_async(&mut raw)
        .await
        .unwrap();
    println!(
        "{}; XACKDEL=true; AOF=always; noeviction",
        info.lines()
            .find(|l| l.starts_with("redis_version:"))
            .unwrap()
    );
    let mut a = bus
        .receiver(
            "orders",
            "workers",
            source(),
            Ownership::Owned,
            GroupStart::Beginning,
        )
        .await
        .unwrap();
    let mut b = bus
        .receiver(
            "orders",
            "workers",
            source(),
            Ownership::External,
            GroupStart::Beginning,
        )
        .await
        .unwrap();
    let message = Envelope::encode(&Order(1)).unwrap();
    let receipt = bus.send("orders", &message).await.unwrap();
    let mut first = a
        .receive(1, Duration::from_secs(1), Duration::from_millis(10))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(first.id(), receipt.id);
    first.progress().await.unwrap();
    let pending = a.pending(8).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].owner, a.consumer_id());
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut claimed = b
        .receive(1, Duration::from_secs(1), Duration::from_millis(10))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id(), first.id());
    assert!(claimed.attempt() > first.attempt());
    assert!(matches!(first.progress().await, Err(Error::Lost)));
    assert!(matches!(first.complete().await, Err(Error::Lost)));
    assert_eq!(claimed.decode().unwrap().id(), message.id());
    claimed.complete().await.unwrap();
    assert!(a.pending(8).await.unwrap().is_empty());
    let length: usize = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut raw)
        .await
        .unwrap();
    assert_eq!(
        length, 0,
        "native atomic XACKDEL removed sole-group acknowledged body"
    );
    println!(
        "actual XACKDEL, fenced XAUTOCLAIM, stale-owner refusal and empty pending view verified"
    );
}

#[tokio::test]
#[ignore = "requires disposable real Redis fixture"]
async fn invalid_first_middle_last_preserve_success_and_confirm_dlq_before_ack() {
    let mut raw = peer().await;
    for failed in 0..3 {
        let prefix = format!("{{t{}}}", uuid::Uuid::new_v4().simple());
        let stream = format!("{prefix}:orders");
        let dlq = format!("{prefix}:dead");
        let bus = broker(vec![route("orders", &stream), route("dead", &dlq)]).await;
        let mut receiver = bus
            .receiver(
                "orders",
                "workers",
                source(),
                Ownership::Owned,
                GroupStart::Beginning,
            )
            .await
            .unwrap();
        let messages: Vec<_> = (0..3)
            .map(|i| Envelope::encode(&Order(i)).unwrap())
            .collect();
        for (i, message) in messages.iter().enumerate() {
            if i == failed {
                let mut broken = serde_json::to_value(message).unwrap();
                broken["id"] = serde_json::Value::String(uuid::Uuid::nil().to_string());
                let _: String = redis::cmd("XADD")
                    .arg(&stream)
                    .arg("*")
                    .arg("body")
                    .arg(serde_json::to_vec(&broken).unwrap())
                    .query_async(&mut raw)
                    .await
                    .unwrap();
            } else {
                bus.send("orders", message).await.unwrap();
            }
        }
        let received = receiver
            .receive(3, Duration::from_secs(2), Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(received.len(), 3);
        let mut success = Vec::new();
        for mut held in received {
            if let Ok(message) = held.decode() {
                success.push(message.id());
                held.complete().await.unwrap();
            } else {
                held.dead_letter("dead", "invalid-envelope").await.unwrap();
            }
        }
        assert_eq!(success.len(), 2);
        assert!(receiver.pending(8).await.unwrap().is_empty());
        let length: usize = redis::cmd("XLEN")
            .arg(&dlq)
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(length, 1);
        bus.send("orders", &messages[failed]).await.unwrap();
        let mut corrected = receiver
            .receive(1, Duration::from_secs(2), Duration::from_secs(30))
            .await
            .unwrap()
            .pop()
            .unwrap();
        let id = corrected.decode().unwrap().id();
        assert_eq!(id, messages[failed].id());
        assert!(!success.contains(&id));
        corrected.complete().await.unwrap();
        assert!(
            receiver
                .receive(1, Duration::from_millis(100), Duration::from_secs(30))
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
#[ignore = "requires disposable real Redis fixture"]
async fn scheduler_bad_target_first_middle_last_retains_only_failures() {
    let mut raw = peer().await;
    for failed in 0..3 {
        let prefix = format!("{{t{}}}", uuid::Uuid::new_v4().simple());
        let streams: Vec<_> = (0..3).map(|i| format!("{prefix}:{i}")).collect();
        let bus = broker(
            (0..3)
                .map(|i| route(&format!("target{i}"), &streams[i]))
                .collect(),
        )
        .await;
        let namespace = format!("{prefix}:schedule");
        for i in 0..3 {
            // Sorted identity gives deterministic first/middle/last positions at the same score.
            let message = Envelope::with_identity(
                uuid::Uuid::from_u128(i as u128 + 1),
                chrono_time(),
                &Order(i as u32),
            )
            .unwrap();
            bus.schedule(&namespace, &format!("target{i}"), &message, 100)
                .await
                .unwrap();
            bus.schedule(&namespace, &format!("target{i}"), &message, 100)
                .await
                .unwrap();
            assert!(
                bus.schedule(&namespace, &format!("target{i}"), &message, 101)
                    .await
                    .is_err()
            );
        }
        let _: () = redis::cmd("SET")
            .arg(&streams[failed])
            .arg("wrong-type")
            .query_async(&mut raw)
            .await
            .unwrap();
        let mut total = (0, 0);
        for _ in 0..3 {
            let pass = bus.release_due(&namespace, 100, 1).await.unwrap();
            total.0 += pass.released;
            total.1 += pass.failed;
        }
        assert_eq!(
            total,
            (2, 1),
            "one poison scheduled entry cannot starve one-member pages"
        );
        let _: usize = redis::cmd("DEL")
            .arg(&streams[failed])
            .query_async(&mut raw)
            .await
            .unwrap();
        let retry = bus.release_due(&namespace, 1100, 3).await.unwrap();
        assert_eq!((retry.released, retry.failed), (1, 0));
        assert_eq!(
            bus.release_due(&namespace, 1100, 3).await.unwrap().released,
            0
        );
        for stream in &streams {
            let length: usize = redis::cmd("XLEN")
                .arg(stream)
                .query_async(&mut raw)
                .await
                .unwrap();
            assert_eq!(length, 1);
        }
    }
}

fn chrono_time() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

#[tokio::test]
#[ignore = "requires disposable real Redis fixture"]
async fn explicit_scheduling_namespaces_do_not_coalesce_equal_cross_tenant_uuids() {
    let prefix = format!("{{t{}}}", uuid::Uuid::new_v4().simple());
    let stream = format!("{prefix}:orders");
    let bus = broker(vec![route("orders", &stream)]).await;
    let namespace_a = format!("{prefix}:tenant-a:schedule");
    let namespace_b = format!("{prefix}:tenant-b:schedule");
    let id = uuid::Uuid::new_v4();
    let created = chrono_time();
    let a = Envelope::with_identity(id, created, &Order(1)).unwrap();
    let b = Envelope::with_identity(id, created, &Order(2)).unwrap();
    for _ in 0..2 {
        bus.schedule(&namespace_a, "orders", &a, 100).await.unwrap();
        bus.schedule(&namespace_b, "orders", &b, 100).await.unwrap();
    }
    assert_eq!(
        bus.release_due(&namespace_a, 100, 8)
            .await
            .unwrap()
            .released,
        1
    );
    assert_eq!(
        bus.release_due(&namespace_b, 100, 8)
            .await
            .unwrap()
            .released,
        1
    );
    assert_eq!(
        bus.release_due(&namespace_a, 100, 8)
            .await
            .unwrap()
            .released,
        0
    );
    assert_eq!(
        bus.release_due(&namespace_b, 100, 8)
            .await
            .unwrap()
            .released,
        0
    );
    let mut audit = bus
        .receiver(
            "orders",
            "audit",
            source(),
            Ownership::Owned,
            GroupStart::Beginning,
        )
        .await
        .unwrap();
    let received = audit
        .receive(2, Duration::from_secs(2), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(received.len(), 2);
    let mut values = Vec::new();
    for mut held in received {
        let envelope = held.decode().unwrap();
        assert_eq!(envelope.id(), id);
        values.push(envelope.decode::<Order>().unwrap().0);
        held.complete().await.unwrap();
    }
    values.sort();
    assert_eq!(values, vec![1, 2]);
    println!(
        "caller-configured scheduler namespaces preserve equal UUID work on one physical Redis stream and suppress only same-namespace schedule retries"
    );
}

#[tokio::test]
#[ignore = "requires disposable real Redis fixture"]
async fn failed_dlq_pending_entry_does_not_starve_new_one_member_pages() {
    let prefix = format!("{{t{}}}", uuid::Uuid::new_v4().simple());
    let stream = format!("{prefix}:orders");
    let dead = format!("{prefix}:dead");
    let bus = broker(vec![route("orders", &stream), route("dead", &dead)]).await;
    let mut raw = peer().await;
    let intended = Envelope::encode(&Order(1)).unwrap();
    let mut broken = serde_json::to_value(&intended).unwrap();
    broken["version"] = 0.into();
    let _: String = redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("body")
        .arg(serde_json::to_vec(&broken).unwrap())
        .query_async(&mut raw)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(&dead)
        .arg("wrong-type")
        .query_async(&mut raw)
        .await
        .unwrap();
    let mut receiver = bus
        .receiver(
            "orders",
            "workers",
            source(),
            Ownership::Owned,
            GroupStart::Beginning,
        )
        .await
        .unwrap();
    let mut failed = receiver
        .receive(1, Duration::from_secs(1), Duration::from_millis(10))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(failed.decode().is_err());
    assert!(
        failed
            .dead_letter("dead", "invalid-envelope")
            .await
            .is_err()
    );
    drop(failed);
    let valid_a = Envelope::encode(&Order(2)).unwrap();
    let valid_b = Envelope::encode(&Order(3)).unwrap();
    bus.send("orders", &valid_a).await.unwrap();
    bus.send("orders", &valid_b).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut first = receiver
        .receive(1, Duration::from_secs(1), Duration::from_millis(10))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(first.decode().unwrap().id(), valid_a.id());
    first.complete().await.unwrap();
    let retry = receiver
        .receive(1, Duration::from_secs(1), Duration::from_millis(10))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(retry.decode().is_err());
    retry.retry();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut second = receiver
        .receive(1, Duration::from_secs(1), Duration::from_millis(10))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.decode().unwrap().id(), valid_b.id());
    second.complete().await.unwrap();
    assert_eq!(receiver.pending(8).await.unwrap().len(), 1);
    let _: usize = redis::cmd("DEL")
        .arg(&dead)
        .query_async(&mut raw)
        .await
        .unwrap();
    let mut poison = receiver
        .receive(1, Duration::from_secs(1), Duration::from_millis(10))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(poison.decode().is_err());
    poison
        .dead_letter("dead", "invalid-envelope")
        .await
        .unwrap();
    bus.send("orders", &intended).await.unwrap();
    let mut corrected = receiver
        .receive(1, Duration::from_secs(1), Duration::from_millis(10))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(corrected.decode().unwrap().id(), intended.id());
    corrected.complete().await.unwrap();
    assert!(receiver.pending(8).await.unwrap().is_empty());
    println!(
        "failed DLQ copy retains the poison PEL entry while one-member pages process unrelated new orders; corrected retry only processes the failed identity"
    );
}

#[tokio::test]
#[ignore = "requires disposable real Redis 8.2+ fixture"]
async fn atomic_ack_delete_preserves_another_groups_unread_copy() {
    let prefix = format!("{{t{}}}", uuid::Uuid::new_v4().simple());
    let stream = format!("{prefix}:orders");
    let bus = broker(vec![route("orders", &stream)]).await;
    let mut workers = bus
        .receiver(
            "orders",
            "workers",
            source(),
            Ownership::Owned,
            GroupStart::Beginning,
        )
        .await
        .unwrap();
    let mut audit = bus
        .receiver(
            "orders",
            "audit",
            source(),
            Ownership::Owned,
            GroupStart::Beginning,
        )
        .await
        .unwrap();
    let envelope = Envelope::encode(&Order(1)).unwrap();
    bus.send("orders", &envelope).await.unwrap();
    let mut first = workers
        .receive(1, Duration::from_secs(1), Duration::from_secs(30))
        .await
        .unwrap()
        .pop()
        .unwrap();
    first.complete().await.unwrap();
    assert!(workers.pending(8).await.unwrap().is_empty());
    let mut raw = peer().await;
    let length: usize = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut raw)
        .await
        .unwrap();
    assert_eq!(
        length, 1,
        "ACKED must retain a body needed by another group"
    );
    let mut other = audit
        .receive(1, Duration::from_secs(1), Duration::from_secs(30))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(other.decode().unwrap().id(), envelope.id());
    other.complete().await.unwrap();
    let length: usize = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut raw)
        .await
        .unwrap();
    assert_eq!(
        length, 0,
        "last group acknowledgement permits native deletion"
    );
    println!(
        "native XACKDEL ACKED settles each group without destroying another group's unread work"
    );
}
