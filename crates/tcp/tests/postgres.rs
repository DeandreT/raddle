//! Real receiver-custody commits, rollback and corrected retries over TCP.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::transport::Transport;
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{Delivery as StoredDelivery, DeliveryKind, Store};
use raddle_tcp::{Consumer, Delivery, Endpoint, Limits, Peer, Publisher, Receipt, Receiver};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, Serialize)]
struct Order(u32);
impl WireMessage for Order {
    const NAME: &'static str = "tcp.order";
    const VERSION: u32 = 1;
}

struct Inbox {
    store: Store,
    failing: AtomicU32,
    suppress_ack: AtomicBool,
    committed: Notify,
}

impl Inbox {
    async fn commit(&self, envelope: &Envelope) -> Result<Delivery, raddle_postgres::Error> {
        let mut scope = self.store.begin().await?;
        // Source and tenant are configured on the receiver, not selected by the
        // envelope's untrusted headers or remote socket address.
        scope
            .stage(
                "tcp.orders",
                "configured-tenant",
                envelope,
                &[StoredDelivery::new(
                    DeliveryKind::Local,
                    "orders",
                    Utc::now(),
                )],
            )
            .await?;
        if envelope
            .decode::<Order>()
            .map_or(true, |order| order.0 == self.failing.load(Ordering::SeqCst))
        {
            scope.rollback().await?;
            return Ok(Delivery::Retry);
        }
        scope.commit(Utc::now()).await?;
        Ok(Delivery::Accepted)
    }
}

impl Consumer for Inbox {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        let disposition = self.commit(&envelope).await.unwrap_or(Delivery::Retry);
        if matches!(disposition, Delivery::Accepted) && self.suppress_ack.load(Ordering::SeqCst) {
            self.committed.notify_one();
            // Model loss of the commit acknowledgement without undoing custody.
            std::future::pending().await
        } else {
            disposition
        }
    }
}

#[sqlx::test(migrations = false)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn socket_ack_follows_inbox_commit_and_failed_siblings_roll_back(pool: PgPool) {
    Store::new(pool.clone()).migrate().await.unwrap();
    for failing in [1, 2, 3] {
        let inbox = Arc::new(Inbox {
            store: Store::new(pool.clone()),
            failing: AtomicU32::new(failing),
            suppress_ack: AtomicBool::new(false),
            committed: Notify::new(),
        });
        let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default())
            .await
            .unwrap();
        let address = receiver.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let running = tokio::spawn(receiver.run(Arc::clone(&inbox), cancellation.clone()));
        let publisher = Publisher::try_new(
            BTreeMap::from([(
                "orders".into(),
                Endpoint::try_new("127.0.0.1", address.port()).unwrap(),
            )]),
            Limits::default(),
        )
        .unwrap();
        let envelopes: Vec<_> = (1..=3)
            .map(|n| Envelope::encode(&Order(n)).unwrap())
            .collect();
        let results = publisher.publish_batch("orders", &envelopes).await.unwrap();
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Ok(Receipt::Accepted)))
                .count(),
            2
        );
        // A fresh connection sees every acknowledged item already committed, and
        // the explicitly rolled-back item has no persisted inbox or job state.
        for (position, envelope) in envelopes.iter().enumerate() {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND message_id=$2",
            )
            .bind("configured-tenant")
            .bind(envelope.id())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(count, i64::from(position + 1 != failing as usize));
        }
        inbox.failing.store(0, Ordering::SeqCst);
        publisher
            .publish("orders", &envelopes[failing as usize - 1])
            .await
            .unwrap();
        // Explicit duplicate submission retains a single job per original ID.
        publisher
            .publish("orders", &envelopes[failing as usize - 1])
            .await
            .unwrap();
        for envelope in &envelopes {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND message_id=$2",
            )
            .bind("configured-tenant")
            .bind(envelope.id())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(count, 1);
        }
        cancellation.cancel();
        running.await.unwrap().unwrap();
    }
}

#[sqlx::test(migrations = false)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn commit_without_observed_ack_and_original_identity_retry_keep_one_durable_job(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let inbox = Arc::new(Inbox {
        store,
        failing: AtomicU32::new(0),
        suppress_ack: AtomicBool::new(true),
        committed: Notify::new(),
    });
    let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default())
        .await
        .unwrap();
    let address = receiver.local_addr().unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(receiver.run(Arc::clone(&inbox), stop.clone()));
    let publisher = Arc::new(
        Publisher::try_new(
            BTreeMap::from([(
                "orders".into(),
                Endpoint::try_new("127.0.0.1", address.port()).unwrap(),
            )]),
            Limits::default(),
        )
        .unwrap(),
    );
    let envelope = Envelope::encode(&Order(42)).unwrap();
    let sending = Arc::clone(&publisher);
    let request = envelope.clone();
    let first = tokio::spawn(async move { sending.publish("orders", &request).await });
    tokio::time::timeout(Duration::from_secs(10), inbox.committed.notified())
        .await
        .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND message_id=$2")
            .bind("configured-tenant")
            .bind(envelope.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    inbox.suppress_ack.store(false, Ordering::SeqCst);
    publisher.publish("orders", &envelope).await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND message_id=$2")
            .bind("configured-tenant")
            .bind(envelope.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    stop.cancel();
    running.await.unwrap().unwrap();
}
