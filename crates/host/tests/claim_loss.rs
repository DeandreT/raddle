//! Ownership loss is local to a claim, not a reason to restart hosted pollers.
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_host::{Host, HostError, PollSettings, RestartPolicy, ServiceFailure, ServiceState};
use raddle_postgres::{
    Claim, Delivery, DeliveryKind, JobStatus, OutboxWorker, RetryPolicy, Store, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::{PgListener, PgPoolOptions};
use tokio::sync::{Notify, mpsc};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(30);
const FAILED_COMPLETION: i64 = 210035;

#[derive(Serialize, Deserialize)]
struct Probe(u32);

impl WireMessage for Probe {
    const NAME: &'static str = "test.claim-loss";
    const VERSION: u32 = 1;
}

async fn single_connection_pool(control: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with((*control.connect_options()).clone())
        .await
        .unwrap()
}

async fn completion_listener(pool: &PgPool) -> PgListener {
    sqlx::raw_sql(
        "CREATE TABLE claim_effect (input integer PRIMARY KEY);
         CREATE FUNCTION notify_completion() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_notify('claim_completed', NEW.id::text); RETURN NEW; END $$;
         CREATE TRIGGER notify_completion AFTER UPDATE ON raddle_jobs
         FOR EACH ROW WHEN (NEW.status = 'completed') EXECUTE FUNCTION notify_completion()",
    )
    .execute(pool)
    .await
    .unwrap();
    let mut listener = PgListener::connect_with(pool).await.unwrap();
    listener.listen("claim_completed").await.unwrap();
    listener
}

async fn stage(store: &Store, kind: DeliveryKind, input: u32) -> Uuid {
    let mut scope = store.begin().await.unwrap();
    let id = scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Probe(input)).unwrap(),
            &[Delivery::new(kind, "owned", Utc::now())],
        )
        .await
        .unwrap()[0];
    scope.commit(Utc::now()).await.unwrap();
    id
}

async fn wait_completed(pool: &PgPool, listener: &mut PgListener, ids: &[Uuid]) {
    // Notifications are committed with the update. No lease-duration sleeps or
    // assumptions about UPDATE RETURNING order are needed.
    tokio::time::timeout(DEADLINE, async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM raddle_jobs WHERE id=ANY($1) AND status='completed'",
            )
            .bind(ids)
            .fetch_one(pool)
            .await
            .unwrap();
            if count == ids.len() as i64 {
                return;
            }
            listener.recv().await.unwrap();
        }
    })
    .await
    .unwrap();
}

async fn snapshot(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT row_to_json(j)::text FROM raddle_jobs j WHERE id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn reclaim(control: &PgPool, kind: DeliveryKind, id: Uuid) -> Claim {
    // Only this unlocked row expires. The first local handler keeps its row
    // lock; this test never tries to steal an executing transaction.
    let changed = sqlx::query(
        "UPDATE raddle_jobs SET lease_until=clock_timestamp()-interval '1 second'
         WHERE id=$1 AND status='active'",
    )
    .bind(id)
    .execute(control)
    .await
    .unwrap();
    assert_eq!(changed.rows_affected(), 1);
    let mut claims = Store::new(control.clone())
        .claim_for(kind, "owned", 3, Duration::from_secs(3600), Utc::now())
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let claim = claims.remove(0);
    assert_eq!(claim.id(), id);
    claim
}

fn settings() -> PollSettings {
    PollSettings::new(3, Duration::from_millis(1)).unwrap()
}

fn with_sibling() -> Host {
    let mut host = Host::new(DEADLINE).unwrap();
    host.service("sibling", RestartPolicy::NEVER, |mut context| async move {
        context.set_ready(true);
        context.cancelled().await;
        Ok(())
    })
    .unwrap();
    host
}

async fn assert_ready_without_restart(handle: &raddle_host::HostHandle) {
    tokio::time::timeout(DEADLINE, async {
        while !handle.snapshot().ready {
            assert!(
                handle.snapshot().running,
                "host stopped after ownership loss"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for service in handle.snapshot().services {
        assert_eq!(service.attempt, 1);
        assert_eq!(service.state, ServiceState::Ready);
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn local_queued_claim_loss_keeps_host_and_later_work_alive(control: PgPool) {
    let pool = single_connection_pool(&control).await;
    let store = Store::new(pool.clone());
    let mut listener = completion_listener(&control).await;
    let mut ids = Vec::new();
    for input in 0..3 {
        ids.push(stage(&store, DeliveryKind::Local, input).await);
    }
    let (entered, mut started) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let gate = Arc::clone(&release);
    let calls = Arc::new(Mutex::new(HashMap::<u32, usize>::new()));
    let observed = Arc::clone(&calls);
    let mut worker = Worker::new(store.clone(), RetryPolicy::default());
    worker
        .handle::<Probe, _>("owned", Codecs::default(), move |Probe(input), scope| {
            let first = {
                let mut calls = observed.lock().unwrap();
                let first = calls.is_empty();
                *calls.entry(input).or_default() += 1;
                first
            };
            let entered = entered.clone();
            let release = Arc::clone(&gate);
            Box::pin(async move {
                if first {
                    entered.send(input).unwrap();
                    release.notified().await;
                }
                sqlx::query("INSERT INTO claim_effect VALUES ($1)")
                    .bind(input as i32)
                    .execute(scope.connection())
                    .await
                    .unwrap();
                Ok(())
            })
        })
        .unwrap();
    let worker = Arc::new(worker);
    let mut host = with_sibling();
    host.service("worker", RestartPolicy::NEVER, move |context| {
        let worker = Arc::clone(&worker);
        async move { raddle_host::postgres_worker(&worker, settings(), context).await }
    })
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    let first = tokio::time::timeout(DEADLINE, started.recv())
        .await
        .unwrap()
        .unwrap();
    // Select by actual handler input, not the claim query's unspecified order.
    let victim = (0..3).find(|input| *input != first).unwrap();
    let successor = reclaim(&control, DeliveryKind::Local, ids[victim as usize]).await;
    let held = snapshot(&control, successor.id()).await;
    release.notify_one();
    let healthy: Vec<_> = ids
        .iter()
        .copied()
        .filter(|id| *id != successor.id())
        .collect();
    wait_completed(&control, &mut listener, &healthy).await;
    assert_ready_without_restart(&handle).await;
    let late = stage(&store, DeliveryKind::Local, 3).await;
    wait_completed(&control, &mut listener, &[late]).await;
    assert_ready_without_restart(&handle).await;
    assert_eq!(snapshot(&control, successor.id()).await, held);
    assert_eq!(calls.lock().unwrap().get(&victim), None);
    assert_eq!(calls.lock().unwrap().len(), 3);
    assert!(calls.lock().unwrap().values().all(|count| *count == 1));
    handle.shutdown();
    run.await.unwrap().unwrap();

    let recovery = Store::new(control.clone());
    let mut scope = recovery.execution(&successor, Utc::now()).await.unwrap();
    sqlx::query("INSERT INTO claim_effect VALUES ($1)")
        .bind(victim as i32)
        .execute(scope.connection())
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    ids.push(late);
    for id in ids {
        let job = recovery.inspect("tenant", id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.attempts, if id == successor.id() { 2 } else { 1 });
    }
    let effects: i64 = sqlx::query_scalar("SELECT count(*) FROM claim_effect")
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(effects, 4);
    pool.close().await;
}

struct PublicationGate {
    input: u32,
    entered: mpsc::UnboundedSender<()>,
    release: Notify,
    calls: Mutex<HashMap<Uuid, usize>>,
}

struct Confirmed(Arc<PublicationGate>);

impl Transport for Confirmed {
    async fn publish(&self, _destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        let first = {
            let mut calls = self.0.calls.lock().unwrap();
            let count = calls.entry(envelope.id()).or_default();
            *count += 1;
            *count == 1
        };
        let Probe(input) = Codecs::default().decode(envelope).unwrap();
        if input == self.0.input && first {
            self.0.entered.send(()).unwrap();
            self.0.release.notified().await;
        }
        Ok(())
    }
}

fn publication_gate(input: u32) -> (Arc<PublicationGate>, mpsc::UnboundedReceiver<()>) {
    let (entered, started) = mpsc::unbounded_channel();
    (
        Arc::new(PublicationGate {
            input,
            entered,
            release: Notify::new(),
            calls: Mutex::new(HashMap::new()),
        }),
        started,
    )
}

async fn outbox_claim_loss(control: PgPool, victim: u32) {
    let pool = single_connection_pool(&control).await;
    let store = Store::new(pool.clone());
    let mut listener = completion_listener(&control).await;
    let mut ids = Vec::new();
    for input in 0..3 {
        ids.push(stage(&store, DeliveryKind::Remote, input).await);
    }
    let (gate, mut entered) = publication_gate(victim);
    let outbox = Arc::new(OutboxWorker::new(
        store.clone(),
        Confirmed(Arc::clone(&gate)),
        RetryPolicy::default(),
    ));
    let mut host = with_sibling();
    let hosted = Arc::clone(&outbox);
    host.service("outbox", RestartPolicy::NEVER, move |context| {
        let outbox = Arc::clone(&hosted);
        async move { raddle_host::postgres_outbox(&outbox, settings(), context).await }
    })
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    tokio::time::timeout(DEADLINE, entered.recv())
        .await
        .unwrap()
        .unwrap();
    let successor = reclaim(&control, DeliveryKind::Remote, ids[victim as usize]).await;
    let held = snapshot(&control, successor.id()).await;
    gate.release.notify_one();
    let healthy: Vec<_> = ids
        .iter()
        .copied()
        .filter(|id| *id != successor.id())
        .collect();
    wait_completed(&control, &mut listener, &healthy).await;
    assert_ready_without_restart(&handle).await;
    let late = stage(&store, DeliveryKind::Remote, 3).await;
    wait_completed(&control, &mut listener, &[late]).await;
    assert_ready_without_restart(&handle).await;
    assert_eq!(snapshot(&control, successor.id()).await, held);
    assert_eq!(gate.calls.lock().unwrap().len(), 4);
    assert!(gate.calls.lock().unwrap().values().all(|count| *count == 1));
    handle.shutdown();
    run.await.unwrap().unwrap();

    // Recover the owned fake, then model acceptance by the successor. A second
    // publish retains the wire identity and only its new claim may complete.
    let transport = Arc::try_unwrap(outbox).ok().unwrap().into_transport();
    let envelope = successor.envelope().unwrap();
    transport
        .publish(successor.destination(), &envelope)
        .await
        .unwrap();
    let recovery = Store::new(control);
    recovery.complete(&successor, Utc::now()).await.unwrap();
    assert_eq!(gate.calls.lock().unwrap()[&envelope.id()], 2);
    ids.push(late);
    for id in ids {
        let job = recovery.inspect("tenant", id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.attempts, if id == successor.id() { 2 } else { 1 });
    }
    pool.close().await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn outbox_first_input_loss_keeps_host_alive(pool: PgPool) {
    outbox_claim_loss(pool, 0).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn outbox_middle_input_loss_keeps_host_alive(pool: PgPool) {
    outbox_claim_loss(pool, 1).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn outbox_last_input_loss_keeps_host_alive(pool: PgPool) {
    outbox_claim_loss(pool, 2).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn destination_outbox_batch_counts_lost_claims_without_completing_them(control: PgPool) {
    let pool = single_connection_pool(&control).await;
    let store = Store::new(pool.clone());
    let mut ids = Vec::new();
    for input in 0..3 {
        ids.push(stage(&store, DeliveryKind::Remote, input).await);
    }
    let (gate, mut entered) = publication_gate(1);
    let worker = OutboxWorker::new(store, Confirmed(Arc::clone(&gate)), RetryPolicy::default());
    let run = tokio::spawn(async move { worker.run_once_for("owned", 3).await });
    tokio::time::timeout(DEADLINE, entered.recv())
        .await
        .unwrap()
        .unwrap();
    let successor = reclaim(&control, DeliveryKind::Remote, ids[1]).await;
    let held = snapshot(&control, successor.id()).await;
    gate.release.notify_one();
    let count = tokio::time::timeout(DEADLINE, run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(count, 3, "a lost claim was observed, not completed");
    assert_eq!(snapshot(&control, successor.id()).await, held);
    let recovery = Store::new(control);
    for id in [ids[0], ids[2]] {
        assert_eq!(
            recovery
                .inspect("tenant", id)
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );
    }
    assert_eq!(gate.calls.lock().unwrap().len(), 3);
    assert!(gate.calls.lock().unwrap().values().all(|count| *count == 1));
    recovery.complete(&successor, Utc::now()).await.unwrap();
    pool.close().await;
}

async fn database_error_still_stops_host_after_join(control: PgPool, kind: DeliveryKind) {
    let mut listener = completion_listener(&control).await;
    let pool = PgPoolOptions::new()
        .min_connections(2)
        .max_connections(2)
        .connect_with((*control.connect_options()).clone())
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    let bad = stage(&store, kind, 0).await;
    let healthy = stage(&store, kind, 1).await;
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION fail_completion() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           PERFORM pg_advisory_lock({FAILED_COMPLETION});
           RAISE EXCEPTION 'test database failure';
         END $$;
         CREATE TRIGGER fail_completion BEFORE UPDATE ON raddle_jobs
         FOR EACH ROW WHEN (NEW.status='completed' AND NEW.id='{bad}')
         EXECUTE FUNCTION fail_completion()"
    ))
    .execute(&control)
    .await
    .unwrap();
    let (gate, mut entered) = publication_gate(1);
    let mut host = with_sibling();
    match kind {
        DeliveryKind::Local => {
            let mut worker = Worker::new(store.clone(), RetryPolicy::default());
            let gate = Arc::clone(&gate);
            worker
                .handle::<Probe, _>("owned", Codecs::default(), move |Probe(input), _scope| {
                    let gate = Arc::clone(&gate);
                    Box::pin(async move {
                        if input == 1 {
                            gate.entered.send(()).unwrap();
                            gate.release.notified().await;
                        }
                        Ok(())
                    })
                })
                .unwrap();
            let worker = Arc::new(worker);
            host.service("database", RestartPolicy::NEVER, move |context| {
                let worker = Arc::clone(&worker);
                async move { raddle_host::postgres_worker(&worker, settings(), context).await }
            })
            .unwrap();
        }
        DeliveryKind::Remote => {
            let outbox = Arc::new(OutboxWorker::new(
                store.clone(),
                Confirmed(Arc::clone(&gate)),
                RetryPolicy::default(),
            ));
            host.service("database", RestartPolicy::NEVER, move |context| {
                let outbox = Arc::clone(&outbox);
                async move { raddle_host::postgres_outbox(&outbox, settings(), context).await }
            })
            .unwrap();
        }
        _ => unreachable!(),
    }
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    tokio::time::timeout(DEADLINE, entered.recv())
        .await
        .unwrap()
        .unwrap();
    // A session lock survives the failed transaction. Once that backend has
    // returned idle, its completion has returned a Database error to the batch.
    // The healthy local handler holds one connection; an outbox publication
    // holds none. Preopening both connections makes the idle count unambiguous.
    let expected_idle = if kind == DeliveryKind::Local { 1 } else { 2 };
    tokio::time::timeout(DEADLINE, async {
        loop {
            assert!(
                !run.is_finished(),
                "database error cancelled the batch sibling"
            );
            let failed: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory'
                 AND objid=$1::bigint::oid AND granted
                 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))",
            )
            .bind(FAILED_COMPLETION)
            .fetch_one(&control)
            .await
            .unwrap();
            if failed && pool.size() == 2 && pool.num_idle() == expected_idle {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !run.is_finished(),
        "database error must not cancel a healthy batch sibling"
    );
    gate.release.notify_one();
    let result = tokio::time::timeout(DEADLINE, run).await.unwrap().unwrap();
    assert!(matches!(
        result,
        Err(HostError::ServiceFailed {
            failure: ServiceFailure::Transient,
            ..
        })
    ));
    wait_completed(&control, &mut listener, &[healthy]).await;
    assert_eq!(
        store.inspect("tenant", bad).await.unwrap().unwrap().status,
        JobStatus::Active
    );
    assert!(
        handle
            .snapshot()
            .services
            .iter()
            .all(|service| service.attempt == 1)
    );
    assert!(!handle.snapshot().running);
    // Closing these two owned connections also releases the test's session lock.
    pool.close().await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn local_database_error_propagates_after_healthy_sibling_joins(pool: PgPool) {
    database_error_still_stops_host_after_join(pool, DeliveryKind::Local).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn outbox_database_error_propagates_after_healthy_sibling_joins(pool: PgPool) {
    database_error_still_stops_host_after_join(pool, DeliveryKind::Remote).await;
}
