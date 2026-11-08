//! Real transaction/claim/outbox integration against an isolated SQLx test DB.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_host::{Host, PollSettings, RestartPolicy};
use raddle_postgres::{
    Delivery, DeliveryKind, JobStatus, OutboxWorker, RetryPolicy, Store, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::Notify;

#[derive(Serialize, Deserialize)]
struct Probe;
impl WireMessage for Probe {
    const NAME: &'static str = "test.host";
    const VERSION: u32 = 1;
}

struct Confirmed(Arc<AtomicUsize>);
impl Transport for Confirmed {
    async fn publish(
        &self,
        _destination: &str,
        _envelope: &Envelope,
    ) -> Result<(), TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn hosted_workers_execute_transactions_and_confirm_outbox_before_shutdown(pool: PgPool) {
    sqlx::query("CREATE TABLE host_effect (id integer PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let jobs = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Probe).unwrap(),
            &[
                Delivery::new(DeliveryKind::Local, "local", now),
                Delivery::new(DeliveryKind::Remote, "remote", now),
            ],
        )
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler_entered = Arc::clone(&entered);
    let handler_release = Arc::clone(&release);
    let mut local = Worker::new(store.clone(), RetryPolicy::default());
    local
        .handle::<Probe, _>("local", Codecs::default(), move |_, scope| {
            let entered = Arc::clone(&handler_entered);
            let release = Arc::clone(&handler_release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                sqlx::query("INSERT INTO host_effect VALUES (1)")
                    .execute(scope.connection())
                    .await
                    .map_err(|_| raddle_postgres::HandlerFailure::Retry)?;
                Ok(())
            })
        })
        .unwrap();
    let local = Arc::new(local);
    let publications = Arc::new(AtomicUsize::new(0));
    let outbox = Arc::new(OutboxWorker::new(
        store.clone(),
        Confirmed(Arc::clone(&publications)),
        RetryPolicy::default(),
    ));
    let settings = PollSettings::new(16, Duration::from_millis(5)).unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    host.service("local", RestartPolicy::NEVER, move |context| {
        let local = Arc::clone(&local);
        async move { raddle_host::postgres_worker(&local, settings, context).await }
    })
    .unwrap();
    host.service("outbox", RestartPolicy::NEVER, move |context| {
        let outbox = Arc::clone(&outbox);
        async move { raddle_host::postgres_outbox(&outbox, settings, context).await }
    })
    .unwrap();
    let handle = host.handle();
    assert!(!handle.snapshot().ready);
    let run = tokio::spawn(host.run());
    let started = tokio::time::timeout(Duration::from_secs(10), entered.notified()).await;
    let prematurely_ready = handle.snapshot().ready;
    release.notify_one();
    let ready = tokio::time::timeout(Duration::from_secs(10), async {
        while !handle.snapshot().ready {
            tokio::task::yield_now().await;
        }
    })
    .await;
    handle.shutdown();
    run.await.unwrap().unwrap();
    started.unwrap();
    assert!(
        !prematurely_ready,
        "an uncommitted first batch is not ready"
    );
    ready.unwrap();
    assert_eq!(publications.load(Ordering::SeqCst), 1);
    let effects: i64 = sqlx::query_scalar("SELECT count(*) FROM host_effect")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(effects, 1);
    for id in jobs {
        assert_eq!(
            store.inspect("tenant", id).await.unwrap().unwrap().status,
            JobStatus::Completed
        );
    }
    assert!(!handle.snapshot().ready);
}
