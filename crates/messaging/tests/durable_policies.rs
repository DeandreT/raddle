//! Real PostgreSQL execution, custody and cleanup for opt-in endpoint policies.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::effects::HandlerOutput;
use raddle_messaging::scoped::{
    CommitReceipt, Completion, Failure, Flush, LocalOnlyFlush, Phase, PostCommitError,
    ScopedHandler,
};
use raddle_messaging::{BoxFuture, DurableEndpoint, EndpointPolicies, MessageContext};
use raddle_policies::{
    Action, CircuitBreaker, CircuitOptions, ErrorFilter, Jitter, Plan, PolicySet, ReplaySafety,
    Rule, Step, Tail,
};
use raddle_postgres::{Delivery, DeliveryKind, JobStatus, RetryPolicy, Scope, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Work(i32);
impl WireMessage for Work {
    const NAME: &'static str = "test.durable-policy";
    const VERSION: u32 = 1;
}
#[derive(Debug, thiserror::Error)]
#[error("typed failure")]
struct TypedFailure;

struct Handler {
    failed: i32,
    corrected: AtomicBool,
    calls: AtomicUsize,
    finalized: AtomicUsize,
    disposed: AtomicUsize,
    exceptions: AtomicUsize,
    after_commit: AtomicUsize,
    trace: Mutex<Vec<&'static str>>,
    entered: Notify,
    block: bool,
    duplicate_at_commit: bool,
    outgoing: bool,
    terminate_backend: bool,
    invalid_outgoing: Option<i32>,
}
impl Handler {
    fn new(failed: i32) -> Self {
        Self {
            failed,
            corrected: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            finalized: AtomicUsize::new(0),
            disposed: AtomicUsize::new(0),
            exceptions: AtomicUsize::new(0),
            after_commit: AtomicUsize::new(0),
            trace: Mutex::new(Vec::new()),
            entered: Notify::new(),
            block: false,
            duplicate_at_commit: false,
            outgoing: false,
            terminate_backend: false,
            invalid_outgoing: None,
        }
    }
}
impl ScopedHandler<Work> for Handler {
    type Resources = ();
    type Reply = ();
    type Error = TypedFailure;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            assert_eq!(context.origin().tenant(), "tenant");
            assert_eq!(context.origin().source(), "source");
            self.trace.lock().await.push("create");
            Ok(())
        })
    }
    fn phase<'a>(
        &'a self,
        phase: Phase,
        _: &'a Work,
        _: &'a mut (),
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            self.trace.lock().await.push(match phase {
                Phase::Before => "before",
                Phase::Load => "load",
                Phase::Validate => "validate",
                Phase::After => "after",
                Phase::PostProcess => "post",
                _ => "unknown",
            });
            Ok(())
        })
    }
    fn handle<'a>(
        &'a self,
        work: &'a Work,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<(), ()>, TypedFailure>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.trace.lock().await.push("handle");
            sqlx::query("INSERT INTO business(id) VALUES($1)")
                .bind(work.0)
                .execute(scope.connection())
                .await
                .map_err(|_| TypedFailure)?;
            if self.terminate_backend && work.0 == 42 {
                let _ = sqlx::query("SELECT pg_terminate_backend(pg_backend_pid())")
                    .execute(scope.connection())
                    .await;
                return Err(TypedFailure);
            }
            if self.duplicate_at_commit && work.0 == 42 {
                sqlx::query("INSERT INTO business(id) VALUES($1)")
                    .bind(work.0)
                    .execute(scope.connection())
                    .await
                    .map_err(|_| TypedFailure)?;
            }
            self.entered.notify_one();
            if self.block && !self.corrected.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if work.0 == self.failed && !self.corrected.load(Ordering::SeqCst) {
                Err(TypedFailure)
            } else {
                let mut output = HandlerOutput::reply(());
                if self.outgoing || self.invalid_outgoing == Some(work.0) {
                    output
                        .effects
                        .push(raddle_messaging::effects::Effect::Outgoing(
                            raddle_messaging::effects::Outgoing {
                                envelope: Envelope::encode(&Work(work.0 + 1000))
                                    .map_err(|_| TypedFailure)?,
                                deliveries: vec![Delivery::new(
                                    DeliveryKind::Remote,
                                    if self.invalid_outgoing == Some(work.0) {
                                        "invalid alias"
                                    } else {
                                        "remote"
                                    },
                                    Utc::now(),
                                )],
                            },
                        ))
                        .map_err(|_| TypedFailure)?;
                }
                Ok(output)
            }
        })
    }
    fn on_exception<'a>(
        &'a self,
        _: &'a Failure<TypedFailure>,
        _: &'a mut (),
        _: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.exceptions.fetch_add(1, Ordering::SeqCst);
            self.trace.lock().await.push("exception");
        })
    }
    fn finally<'a>(
        &'a self,
        _: Completion,
        _: &'a mut (),
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            self.finalized.fetch_add(1, Ordering::SeqCst);
            self.trace.lock().await.push("finally");
            Ok(())
        })
    }
    fn dispose<'a>(
        &'a self,
        _: (),
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            self.disposed.fetch_add(1, Ordering::SeqCst);
            self.trace.lock().await.push("dispose");
            Ok(())
        })
    }
    fn after_commit<'a>(
        &'a self,
        _: &'a mut (),
        _: &'a CommitReceipt,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            self.after_commit.fetch_add(1, Ordering::SeqCst);
            self.trace.lock().await.push("after-commit");
            Ok(())
        })
    }
}

fn policies(action: Action) -> EndpointPolicies {
    let mut policies = PolicySet::default();
    policies.global(Rule {
        filter: ErrorFilter::typed::<TypedFailure>(),
        plan: Plan::new(
            vec![Step::new(action)],
            Tail::Terminal(Action::DeadLetter),
            Jitter::None,
        )
        .unwrap(),
    });
    EndpointPolicies::new(policies)
}
fn endpoint(
    store: &Store,
    name: &str,
    parallelism: u32,
    handler: Arc<Handler>,
    policies: EndpointPolicies,
    flush: Arc<dyn Flush>,
) -> DurableEndpoint {
    let mut endpoint =
        DurableEndpoint::new(store.clone(), name, parallelism, RetryPolicy::default()).unwrap();
    endpoint.policies(policies).unwrap();
    endpoint
        .handle_policy::<Work, _>(handler, flush, ReplaySafety::Transactional)
        .unwrap();
    endpoint
}
async fn stage(store: &Store, name: &str, value: i32) -> Uuid {
    let mut scope = store.begin().await.unwrap();
    let mut delivery = Delivery::new(DeliveryKind::Local, name, Utc::now());
    delivery.max_attempts = 1; // Opt-in policy owns its budget, not this legacy default.
    let id = scope
        .stage(
            "source",
            "tenant",
            &Envelope::encode(&Work(value)).unwrap(),
            &[delivery],
        )
        .await
        .unwrap()[0];
    scope.commit(Utc::now()).await.unwrap();
    id
}
async fn schema(pool: &PgPool) {
    sqlx::query("CREATE TABLE business(id integer UNIQUE DEFERRABLE INITIALLY DEFERRED)")
        .execute(pool)
        .await
        .unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn retries_rollback_sql_and_replay_only_failed_first_middle_last(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    for failed in 0..3 {
        let name = format!("position-{failed}");
        let offset = failed * 10;
        let handler = Arc::new(Handler::new(offset + failed));
        let endpoint = endpoint(
            &store,
            &name,
            3,
            Arc::clone(&handler),
            policies(Action::Requeue),
            Arc::new(LocalOnlyFlush),
        );
        let mut ids = Vec::new();
        for value in 0..3 {
            ids.push(stage(&store, &name, offset + value).await);
        }
        assert_eq!(endpoint.run_once().await.unwrap(), 3);
        assert_eq!(endpoint.run_once().await.unwrap(), 1);
        assert_eq!(handler.calls.load(Ordering::SeqCst), 4);
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM business WHERE id >= $1 AND id < $1+3")
                .bind(offset)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rows, 2);
        for (index, id) in ids.iter().enumerate() {
            assert_eq!(
                store.inspect("tenant", *id).await.unwrap().unwrap().status,
                if index == failed as usize {
                    JobStatus::Dead
                } else {
                    JobStatus::Completed
                }
            );
        }
        handler.corrected.store(true, Ordering::SeqCst);
        assert!(
            store
                .replay(
                    "tenant",
                    ids[failed as usize],
                    "operator",
                    "corrected",
                    Utc::now()
                )
                .await
                .unwrap()
        );
        assert_eq!(endpoint.run_once().await.unwrap(), 1);
        assert_eq!(endpoint.run_once().await.unwrap(), 0);
        assert_eq!(handler.calls.load(Ordering::SeqCst), 5);
        assert_eq!(handler.finalized.load(Ordering::SeqCst), 5);
        assert_eq!(handler.disposed.load(Ordering::SeqCst), 5);
        let attempts: i64 =
            sqlx::query_scalar("SELECT handling FROM raddle_policy_attempts WHERE job_id=$1")
                .bind(ids[failed as usize])
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(attempts, 3);
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn immediate_continuation_runs_original_pipeline_and_cleanup_each_attempt(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let handler = Arc::new(Handler::new(7));
    // An observational failure hook does not swallow or bypass the policy.
    let endpoint = endpoint(
        &store,
        "inline-retry",
        1,
        Arc::clone(&handler),
        policies(Action::Immediate),
        Arc::new(LocalOnlyFlush),
    );
    let id = stage(&store, "inline-retry", 7).await;
    assert_eq!(endpoint.run_once().await.unwrap(), 1);
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Dead
    );
    assert_eq!(
        *handler.trace.lock().await,
        [
            "create",
            "before",
            "load",
            "validate",
            "handle",
            "exception",
            "finally",
            "dispose",
            "create",
            "before",
            "load",
            "validate",
            "handle",
            "exception",
            "finally",
            "dispose"
        ]
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM business")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

struct FailedFlush;
impl Flush for FailedFlush {
    fn flush<'a>(&'a self, _: &'a CommitReceipt) -> BoxFuture<'a, Result<(), PostCommitError>> {
        Box::pin(async { Err(PostCommitError::Flush) })
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn real_host_committed_parent_is_not_retried_on_flush_failure(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let mut h = Handler::new(-1);
    h.outgoing = true;
    let handler = Arc::new(h);
    let endpoint = endpoint(
        &store,
        "host-flush",
        1,
        Arc::clone(&handler),
        policies(Action::Immediate),
        Arc::new(FailedFlush),
    );
    let id = stage(&store, "host-flush", 42).await;
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    endpoint
        .attach(&mut host, Duration::from_millis(1))
        .unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if handler.disposed.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Completed
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(handler.after_commit.load(Ordering::SeqCst), 0);
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raddle_jobs WHERE destination='remote' AND status='ready'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, 1);
    let worker =
        raddle_postgres::OutboxWorker::new(store.clone(), Confirmed, RetryPolicy::default());
    assert_eq!(worker.run_once_for("remote", 1).await.unwrap(), 1);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *handler.trace.lock().await,
        [
            "create", "before", "load", "validate", "handle", "after", "post", "finally", "dispose"
        ]
    );
}

struct Confirmed;
impl raddle::transport::Transport for Confirmed {
    async fn publish(
        &self,
        _: &str,
        _: &Envelope,
    ) -> Result<(), raddle::transport::TransportError> {
        Ok(())
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn real_host_shutdown_joins_rollback_finally_and_does_not_trip_circuit(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let mut h = Handler::new(-1);
    h.block = true;
    let handler = Arc::new(h);
    let circuit = CircuitBreaker::new(CircuitOptions {
        minimum_samples: 1,
        ..Default::default()
    })
    .unwrap();
    let endpoint = endpoint(
        &store,
        "host-stop",
        1,
        Arc::clone(&handler),
        policies(Action::Immediate).with_circuit(circuit),
        Arc::new(LocalOnlyFlush),
    );
    let id = stage(&store, "host-stop", 42).await;
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    endpoint
        .attach(&mut host, Duration::from_millis(1))
        .unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    handler.entered.notified().await;
    handle.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Active
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(handler.finalized.load(Ordering::SeqCst), 1);
    assert_eq!(handler.disposed.load(Ordering::SeqCst), 1);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM business")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    let paused: bool = sqlx::query_scalar("SELECT paused_until>clock_timestamp() FROM raddle_endpoint_policy_pauses WHERE destination='host-stop'").fetch_one(&pool).await.unwrap();
    assert!(!paused);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn commit_failure_retains_custody_reports_infrastructure_after_healthy_siblings(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let mut h = Handler::new(-1);
    h.duplicate_at_commit = true;
    let handler = Arc::new(h);
    let endpoint = endpoint(
        &store,
        "uncertain",
        3,
        Arc::clone(&handler),
        policies(Action::Immediate),
        Arc::new(LocalOnlyFlush),
    );
    let first = stage(&store, "uncertain", 41).await;
    let id = stage(&store, "uncertain", 42).await;
    let last = stage(&store, "uncertain", 43).await;
    assert!(matches!(
        endpoint.run_once().await,
        Err(raddle_postgres::Error::Database(_))
    ));
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Active
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 3);
    assert_eq!(handler.finalized.load(Ordering::SeqCst), 3);
    assert_eq!(handler.disposed.load(Ordering::SeqCst), 3);
    assert_eq!(
        store
            .inspect("tenant", first)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Completed
    );
    assert_eq!(
        store.inspect("tenant", last).await.unwrap().unwrap().status,
        JobStatus::Completed
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM business")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 2);
    let future = Utc::now() + chrono::Duration::minutes(5);
    assert!(
        store
            .claim_policy_for("uncertain", 1, Duration::from_secs(30), future)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .recover_policy_uncertain(
                "tenant",
                id,
                raddle_postgres::endpoint_policies::PolicyRecovery::RolledBack,
                "operator",
                "verified rollback",
                future
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .claim_policy_for("uncertain", 1, Duration::from_secs(30), future)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn buffered_bus_scoped_policy_retries_and_host_cancellation_join_original_cleanup(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let handler = Arc::new(Handler::new(7));
    let mut builder = raddle_messaging::PolicyQueueBuilder::new(
        "buffered",
        8,
        Duration::from_secs(60),
        policies(Action::Immediate),
    )
    .unwrap();
    builder
        .scoped::<Work, _>(
            Arc::clone(&handler),
            store.clone(),
            Arc::new(LocalOnlyFlush),
            ReplaySafety::Transactional,
        )
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    let mut bus =
        raddle_messaging::BusBuilder::new(raddle::RuntimeBuilder::try_new(1).unwrap().build());
    bus.policy_queue(queue.clone()).unwrap();
    bus.route::<Work>("buffered").unwrap();
    let bus = bus.build();
    bus.send(
        &Work(7),
        raddle_messaging::TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
    )
    .await
    .unwrap();
    bus.send(
        &Work(8),
        raddle_messaging::TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
    )
    .await
    .unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "buffered").unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if handler.disposed.load(Ordering::SeqCst) == 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.shutdown();
    task.await.unwrap().unwrap();
    let states = queue.deliveries().unwrap();
    assert!(matches!(
        states[0].status,
        raddle_policies::MemoryStatus::DeadLetter { .. }
    ));
    assert_eq!(states[1].status, raddle_policies::MemoryStatus::Completed);
    let rows: Vec<i32> = sqlx::query_scalar("SELECT id FROM business")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows, vec![8]);

    let mut h = Handler::new(-1);
    h.block = true;
    let handler = Arc::new(h);
    let mut builder = raddle_messaging::PolicyQueueBuilder::new(
        "buffered-stop",
        8,
        Duration::from_secs(60),
        policies(Action::Immediate),
    )
    .unwrap();
    builder
        .scoped::<Work, _>(
            Arc::clone(&handler),
            store,
            Arc::new(LocalOnlyFlush),
            ReplaySafety::Transactional,
        )
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    queue
        .submit(MessageContext::new(
            raddle_messaging::TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
            Envelope::encode(&Work(9)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "buffered-stop").unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    handler.entered.notified().await;
    handle.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(
        queue.deliveries().unwrap()[0].status,
        raddle_policies::MemoryStatus::Interrupted
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(handler.disposed.load(Ordering::SeqCst), 1);
    assert_eq!(handler.finalized.load(Ordering::SeqCst), 1);
    let rows: Vec<i32> = sqlx::query_scalar("SELECT id FROM business")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows, vec![8]);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn policy_circuit_pauses_only_its_endpoint_before_inline_retry(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let handler = Arc::new(Handler::new(7));
    let circuit = CircuitBreaker::new(CircuitOptions {
        minimum_samples: 1,
        ..Default::default()
    })
    .unwrap();
    let failed = endpoint(
        &store,
        "failed",
        1,
        Arc::clone(&handler),
        policies(Action::Immediate).with_circuit(circuit),
        Arc::new(LocalOnlyFlush),
    );
    let healthy = endpoint(
        &store,
        "healthy",
        1,
        Arc::clone(&handler),
        policies(Action::Immediate),
        Arc::new(LocalOnlyFlush),
    );
    let failed_id = stage(&store, "failed", 7).await;
    let healthy_id = stage(&store, "healthy", 8).await;
    assert_eq!(failed.run_once().await.unwrap(), 1);
    assert_eq!(failed.run_once().await.unwrap(), 0);
    assert_eq!(
        store
            .inspect("tenant", failed_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Ready
    );
    assert_eq!(healthy.run_once().await.unwrap(), 1);
    assert_eq!(
        store
            .inspect("tenant", healthy_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Completed
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn refused_replay_and_lost_terminal_transition_never_open_automatic_retry_gap(pool: PgPool) {
    schema(&pool).await;
    sqlx::raw_sql("CREATE FUNCTION refuse_terminal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='dead' THEN RAISE EXCEPTION 'scripted settlement failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER refuse_terminal BEFORE UPDATE ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION refuse_terminal();").execute(&pool).await.unwrap();
    let store = Store::new(pool.clone());
    let handler = Arc::new(Handler::new(7));
    let mut endpoint =
        DurableEndpoint::new(store.clone(), "unsafe", 1, RetryPolicy::default()).unwrap();
    endpoint.policies(policies(Action::Immediate)).unwrap();
    endpoint
        .handle_policy::<Work, _>(
            Arc::clone(&handler),
            Arc::new(LocalOnlyFlush),
            ReplaySafety::Never,
        )
        .unwrap();
    let id = stage(&store, "unsafe", 7).await;
    assert!(matches!(
        endpoint.run_once().await,
        Err(raddle_postgres::Error::Database(_))
    ));
    let future = Utc::now() + chrono::Duration::minutes(5);
    assert!(
        store
            .claim_policy_for("unsafe", 1, Duration::from_secs(30), future)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Active
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn policy_settlement_rechecks_database_clock_after_row_lock_wait(pool: PgPool) {
    let store = Store::new(pool.clone());
    let id = stage(&store, "expiry", 1).await;
    let now = Utc::now();
    let claim = store
        .claim_policy_for("expiry", 1, Duration::from_secs(3), now)
        .await
        .unwrap()
        .remove(0);
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM raddle_jobs WHERE id=$1 FOR UPDATE")
        .bind(id)
        .execute(&mut *lock)
        .await
        .unwrap();
    let live: bool =
        sqlx::query_scalar("SELECT lease_until>clock_timestamp() FROM raddle_jobs WHERE id=$1")
            .bind(id)
            .fetch_one(&mut *lock)
            .await
            .unwrap();
    assert!(live);
    let settling = store.clone();
    let task = tokio::spawn(async move {
        settling
            .settle_policy(
                &claim,
                raddle_postgres::endpoint_policies::PolicySettlement::DeadLetter,
                now,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async { loop {
        let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id FROM raddle_jobs%FOR UPDATE')").fetch_one(&pool).await.unwrap();
        if waiting { break; } tokio::task::yield_now().await;
    }}).await.unwrap();
    sqlx::query("SELECT pg_sleep(3.1)")
        .execute(&mut *lock)
        .await
        .unwrap();
    lock.rollback().await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(raddle_postgres::Error::StaleClaim)
    ));
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Active
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn policy_execution_rechecks_database_clock_after_pool_wait(pool: PgPool) {
    let store = Store::new(pool.clone());
    let id = stage(&store, "pool-expiry", 1).await;
    let single = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let single_store = Store::new(single.clone());
    let mut held = single.acquire().await.unwrap();
    let now = Utc::now();
    let claim = store
        .claim_policy_for("pool-expiry", 1, Duration::from_secs(3), now)
        .await
        .unwrap()
        .remove(0);
    let pending = single_store.policy_execution(&claim, now);
    tokio::pin!(pending);
    tokio::select! { biased; _ = &mut pending => panic!("execution entered while the only pool connection was held"), () = tokio::task::yield_now() => {} }
    sqlx::query("SELECT pg_sleep(3.1)")
        .execute(&mut *held)
        .await
        .unwrap();
    drop(held);
    assert!(matches!(
        pending.await,
        Err(raddle_postgres::Error::StaleClaim)
    ));
    let state = store.inspect("tenant", id).await.unwrap().unwrap();
    assert_eq!(state.status, JobStatus::Active);
    assert_eq!(state.attempts, 0);
    single.close().await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn rollback_io_failure_is_not_reclassified_as_a_retryable_handler_error(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let mut h = Handler::new(-1);
    h.terminate_backend = true;
    let handler = Arc::new(h);
    let endpoint = endpoint(
        &store,
        "rollback-io",
        1,
        Arc::clone(&handler),
        policies(Action::Immediate),
        Arc::new(LocalOnlyFlush),
    );
    let id = stage(&store, "rollback-io", 42).await;
    assert!(matches!(
        endpoint.run_once().await,
        Err(raddle_postgres::Error::Database(_))
    ));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(handler.finalized.load(Ordering::SeqCst), 1);
    assert_eq!(handler.disposed.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Active
    );
    assert!(
        store
            .claim_policy_for(
                "rollback-io",
                1,
                Duration::from_secs(30),
                Utc::now() + chrono::Duration::minutes(5)
            )
            .await
            .unwrap()
            .is_empty()
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM business")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn unsafe_replay_refusal_is_persisted_without_repeating_the_handler(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let handler = Arc::new(Handler::new(7));
    let mut endpoint =
        DurableEndpoint::new(store.clone(), "never", 1, RetryPolicy::default()).unwrap();
    endpoint.policies(policies(Action::Immediate)).unwrap();
    endpoint
        .handle_policy::<Work, _>(
            Arc::clone(&handler),
            Arc::new(LocalOnlyFlush),
            ReplaySafety::Never,
        )
        .unwrap();
    let id = stage(&store, "never", 7).await;
    assert_eq!(endpoint.run_once().await.unwrap(), 1);
    let state = store.inspect("tenant", id).await.unwrap().unwrap();
    assert_eq!(state.status, JobStatus::Dead);
    assert_eq!(state.last_error.as_deref(), Some("policy-replay-denied"));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn quota_is_reacquired_before_each_durable_inline_retry_without_advancing_denied_attempts(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let handler = Arc::new(Handler::new(7));
    let mut quotas = raddle_policies::ScopedQuotas::default();
    quotas
        .message(
            Work::NAME.to_owned(),
            raddle_policies::Quota::new(raddle_policies::QuotaKind::FixedWindow {
                permits: 1,
                window: Duration::from_secs(60),
            })
            .unwrap(),
        )
        .unwrap();
    let endpoint = endpoint(
        &store,
        "quota",
        1,
        Arc::clone(&handler),
        policies(Action::Immediate).with_quotas(Arc::new(quotas)),
        Arc::new(LocalOnlyFlush),
    );
    let id = stage(&store, "quota", 7).await;
    assert_eq!(endpoint.run_once().await.unwrap(), 1);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Ready
    );
    let attempts: i64 =
        sqlx::query_scalar("SELECT handling FROM raddle_policy_attempts WHERE job_id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, 1);
    let pending: bool =
        sqlx::query_scalar("SELECT available_at>clock_timestamp() FROM raddle_jobs WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(pending);
    let paused: bool = sqlx::query_scalar("SELECT paused_until>clock_timestamp() FROM raddle_endpoint_policy_pauses WHERE destination='quota'").fetch_one(&pool).await.unwrap();
    assert!(paused);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn malformed_returned_intent_is_order_local_after_proven_rollback_in_both_owners(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    for failed in 0..3 {
        let offset = failed * 10;
        let mut h = Handler::new(-1);
        h.invalid_outgoing = Some(offset + failed);
        let handler = Arc::new(h);
        let name = format!("invalid-durable-{failed}");
        let endpoint = endpoint(
            &store,
            &name,
            3,
            Arc::clone(&handler),
            EndpointPolicies::new(PolicySet::default()),
            Arc::new(LocalOnlyFlush),
        );
        let mut ids = Vec::new();
        for index in 0..3 {
            ids.push(stage(&store, &name, offset + index).await);
        }
        assert_eq!(endpoint.run_once().await.unwrap(), 3);
        for (index, id) in ids.iter().enumerate() {
            assert_eq!(
                store.inspect("tenant", *id).await.unwrap().unwrap().status,
                if index == failed as usize {
                    JobStatus::Dead
                } else {
                    JobStatus::Completed
                }
            );
        }
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM business WHERE id BETWEEN $1 AND $1+2")
                .bind(offset)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rows, 2);

        let offset = 100 + failed * 10;
        let mut h = Handler::new(-1);
        h.invalid_outgoing = Some(offset + failed);
        let handler = Arc::new(h);
        let name = format!("invalid-buffered-{failed}");
        let mut builder = raddle_messaging::PolicyQueueBuilder::new(
            &name,
            8,
            Duration::from_secs(5),
            EndpointPolicies::new(PolicySet::default()),
        )
        .unwrap();
        builder
            .scoped::<Work, _>(
                Arc::clone(&handler),
                store.clone(),
                Arc::new(LocalOnlyFlush),
                ReplaySafety::Transactional,
            )
            .unwrap();
        let (queue, runner) = builder.build().unwrap();
        for index in 0..3 {
            queue
                .submit(MessageContext::new(
                    raddle_messaging::TrustedOrigin::authenticated("tenant", "source", None)
                        .unwrap(),
                    Envelope::encode(&Work(offset + index)).unwrap(),
                    Utc::now(),
                ))
                .await
                .unwrap();
        }
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        runner.attach(&mut host, &name).unwrap();
        let stop = host.handle();
        let task = tokio::spawn(host.run());
        let states = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let states = queue.deliveries().unwrap();
                if states.len() == 3
                    && states.iter().all(|delivery| {
                        matches!(
                            delivery.status,
                            raddle_policies::MemoryStatus::Completed
                                | raddle_policies::MemoryStatus::DeadLetter { .. }
                        )
                    })
                {
                    break states;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.shutdown();
        task.await.unwrap().unwrap();
        for (index, state) in states.iter().enumerate() {
            if index == failed as usize {
                assert!(matches!(
                    state.status,
                    raddle_policies::MemoryStatus::DeadLetter { .. }
                ));
            } else {
                assert_eq!(state.status, raddle_policies::MemoryStatus::Completed);
            }
        }
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM business WHERE id BETWEEN $1 AND $1+2")
                .bind(offset)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(handler.calls.load(Ordering::SeqCst), 3);
        assert_eq!(handler.disposed.load(Ordering::SeqCst), 3);
    }
}
