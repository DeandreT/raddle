//! Uncertain attempts retain custody, not ownership of the receiving service.
use super::{Handler, TypedFailure, Work, schema, stage};
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use chrono::Utc;
use raddle::wire::Envelope;
use raddle_host::{Host, HostError};
use raddle_messaging::effects::HandlerOutput;
use raddle_messaging::scoped::{
    CommitReceipt, Completion, Failure, LocalOnlyFlush, Phase, ScopedHandler,
};
use raddle_messaging::{
    BoxFuture, Cancellation, DurableEndpoint, EndpointPolicies, MessageContext, PolicyQueue,
    PolicyQueueBuilder, TrustedOrigin,
};
use raddle_policies::{
    Action, CircuitBreaker, CircuitOptions, ErrorFilter, Jitter, MemoryStatus, Plan, PolicySet,
    ReplaySafety, Rule, Step, Tail,
};
use raddle_postgres::endpoint_policies::PolicyRecovery;
use raddle_postgres::{JobStatus, RetryPolicy, Scope, Store};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
enum Fault {
    RollbackConnection,
    CommitConnection,
}

struct FaultHandler {
    inner: Handler,
    victim: i32,
    fault: Fault,
    corrected: AtomicBool,
    contexts: Mutex<Vec<Cancellation>>,
    backends: Mutex<Vec<(i32, i32)>>,
}
impl FaultHandler {
    fn new(victim: i32, fault: Fault) -> Self {
        Self {
            inner: Handler::new(-1),
            victim,
            fault,
            corrected: AtomicBool::new(false),
            contexts: Mutex::new(Vec::new()),
            backends: Mutex::new(Vec::new()),
        }
    }
    async fn assert_isolated(&self, later: i32) {
        assert_eq!(self.inner.uncertain_completions.load(Ordering::SeqCst), 1);
        assert!(
            self.contexts
                .lock()
                .await
                .iter()
                .all(|token| !token.is_cancelled()),
            "one failure must not cancel the shared parent or siblings"
        );
        let backends = self.backends.lock().await;
        let failed = backends
            .iter()
            .find(|(work, _)| *work == self.victim)
            .unwrap()
            .1;
        let fresh = backends.iter().find(|(work, _)| *work == later).unwrap().1;
        assert_ne!(
            failed, fresh,
            "new healthy work must not reuse the dead connection"
        );
    }
}
impl ScopedHandler<Work> for FaultHandler {
    type Resources = ();
    type Reply = ();
    type Error = TypedFailure;
    fn create<'a>(
        &'a self,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            self.contexts
                .lock()
                .await
                .push(context.cancellation().clone());
            self.inner.create(scope, context).await
        })
    }
    fn phase<'a>(
        &'a self,
        phase: Phase,
        work: &'a Work,
        resources: &'a mut (),
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        self.inner.phase(phase, work, resources, scope, context)
    }
    fn handle<'a>(
        &'a self,
        work: &'a Work,
        resources: &'a mut (),
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<(), ()>, TypedFailure>> {
        Box::pin(async move {
            let output = self.inner.handle(work, resources, scope, context).await?;
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(scope.connection())
                .await
                .map_err(|_| TypedFailure)?;
            self.backends.lock().await.push((work.0, pid));
            if work.0 == self.victim && !self.corrected.load(Ordering::SeqCst) {
                match self.fault {
                    Fault::RollbackConnection => {
                        let _ = sqlx::query("SELECT pg_terminate_backend(pg_backend_pid())")
                            .execute(scope.connection())
                            .await;
                        return Err(TypedFailure);
                    }
                    Fault::CommitConnection => {
                        sqlx::query("SELECT set_config('raddle.test_commit_fault','yes',true)")
                            .execute(scope.connection())
                            .await
                            .map_err(|_| TypedFailure)?;
                    }
                }
            }
            Ok(output)
        })
    }
    fn on_exception<'a>(
        &'a self,
        error: &'a Failure<TypedFailure>,
        resources: &'a mut (),
        context: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        self.inner.on_exception(error, resources, context)
    }
    fn finally<'a>(
        &'a self,
        completion: Completion,
        resources: &'a mut (),
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        self.inner.finally(completion, resources, context)
    }
    fn dispose<'a>(
        &'a self,
        resources: (),
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        self.inner.dispose(resources, context)
    }
    fn after_commit<'a>(
        &'a self,
        resources: &'a mut (),
        receipt: &'a CommitReceipt,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        self.inner.after_commit(resources, receipt, context)
    }
}

fn policies(circuit: bool) -> EndpointPolicies {
    let mut set = PolicySet::default();
    set.global(Rule {
        filter: ErrorFilter::any(),
        plan: Plan::new(
            vec![Step::new(Action::Immediate)],
            Tail::Terminal(Action::DeadLetter),
            Jitter::None,
        )
        .unwrap(),
    });
    let policies = EndpointPolicies::new(set);
    if circuit {
        policies.with_circuit(
            CircuitBreaker::new(CircuitOptions {
                minimum_samples: 1,
                ..Default::default()
            })
            .unwrap(),
        )
    } else {
        policies
    }
}
fn durable(
    store: Store,
    name: &str,
    parallelism: u32,
    handler: Arc<FaultHandler>,
) -> DurableEndpoint {
    let mut endpoint =
        DurableEndpoint::new(store, name, parallelism, RetryPolicy::default()).unwrap();
    endpoint.policies(policies(parallelism == 1)).unwrap();
    endpoint
        .handle_policy::<Work, _>(
            handler,
            Arc::new(LocalOnlyFlush),
            ReplaySafety::Transactional,
        )
        .unwrap();
    endpoint
}
fn local(
    store: Store,
    name: &str,
    handler: Arc<FaultHandler>,
) -> (PolicyQueue, raddle_messaging::PolicyQueueRunner) {
    let mut builder =
        PolicyQueueBuilder::new(name, 16, Duration::from_secs(30), policies(true)).unwrap();
    builder
        .scoped::<Work, _>(
            handler,
            store,
            Arc::new(LocalOnlyFlush),
            ReplaySafety::Transactional,
        )
        .unwrap();
    builder.build().unwrap()
}
async fn submit(queue: &PolicyQueue, work: i32) -> (Uuid, MessageContext) {
    let context = MessageContext::new(
        TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
        Envelope::encode(&Work(work)).unwrap(),
        Utc::now(),
    );
    let id = context.envelope().id();
    queue.submit(context.clone()).await.unwrap();
    (id, context)
}
async fn wait<F, Fut>(task: &JoinHandle<Result<(), HostError>>, mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            assert!(
                !task.is_finished(),
                "post-admission uncertainty stopped the NEVER Host"
            );
            if ready().await {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
}
async fn completed(store: &Store, ids: &[Uuid]) -> bool {
    for id in ids {
        if store.inspect("tenant", *id).await.unwrap().unwrap().status != JobStatus::Completed {
            return false;
        }
    }
    true
}
async fn tainted(pool: &PgPool, id: Uuid) {
    let state: (String, bool, i64, bool) = sqlx::query_as("SELECT j.status,p.uncertain,p.handling,j.completed_at IS NOT NULL FROM raddle_jobs j JOIN raddle_policy_attempts p ON p.job_id=j.id WHERE j.id=$1")
        .bind(id).fetch_one(pool).await.unwrap();
    assert_eq!(state, ("active".to_owned(), true, 1, false));
}
async fn setup(pool: &PgPool) {
    schema(pool).await;
    // This trigger terminates only its own disposable execution connection, and
    // only when the test explicitly opts that transaction into a COMMIT fault.
    sqlx::raw_sql("CREATE FUNCTION lose_test_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('raddle.test_commit_fault',true)='yes' THEN PERFORM pg_terminate_backend(pg_backend_pid()); END IF; RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER lose_test_commit AFTER INSERT ON business DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION lose_test_commit();").execute(pool).await.unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn durable_uncertainty_keeps_never_host_current_and_new_work_alive_and_recovery_audited(
    pool: PgPool,
) {
    setup(&pool).await;
    let store = Store::new(pool.clone());
    for (mode, fault) in [Fault::RollbackConnection, Fault::CommitConnection]
        .into_iter()
        .enumerate()
    {
        for parallelism in [1, 3] {
            for position in 0..3usize {
                let base = mode as i32 * 1000 + parallelism as i32 * 100 + position as i32 * 10;
                let victim = base + position as i32;
                let name = format!("uncertain-{base}");
                let sibling_name = format!("sibling-{base}");
                let handler = Arc::new(FaultHandler::new(victim, fault));
                let sibling_handler = Arc::new(FaultHandler::new(-1, fault));
                let mut ids = Vec::new();
                for i in 0..3 {
                    ids.push(stage(&store, &name, base + i).await);
                }
                let good: Vec<_> = ids
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != position)
                    .map(|(_, id)| *id)
                    .collect();
                let mut host = Host::new(Duration::from_secs(10)).unwrap();
                durable(store.clone(), &name, parallelism, Arc::clone(&handler))
                    .attach(&mut host, Duration::from_millis(5))
                    .unwrap();
                durable(
                    store.clone(),
                    &sibling_name,
                    1,
                    Arc::clone(&sibling_handler),
                )
                .attach(&mut host, Duration::from_millis(5))
                .unwrap();
                let stop = host.handle();
                let task = tokio::spawn(host.run());
                wait(&task, || async {
                    completed(&store, &good).await
                        && handler.inner.disposed.load(Ordering::SeqCst) == 3
                })
                .await;
                tainted(&pool, ids[position]).await;
                assert!(
                    !store
                        .recover_policy_uncertain(
                            "tenant",
                            ids[position],
                            PolicyRecovery::RolledBack,
                            "operator",
                            "not expired",
                            Utc::now()
                        )
                        .await
                        .unwrap()
                );
                // Expire only this finished test attempt's lease, not its marker.
                sqlx::query("UPDATE raddle_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1").bind(ids[position]).execute(&pool).await.unwrap();
                let later = stage(&store, &name, base + 3).await;
                let sibling = stage(&store, &sibling_name, base + 4).await;
                wait(&task, || async {
                    completed(&store, &[later, sibling]).await
                })
                .await;
                tainted(&pool, ids[position]).await; // no automatic lease-recovery replay
                handler.assert_isolated(base + 3).await;
                assert!(
                    !store
                        .recover_policy_uncertain(
                            "wrong-tenant",
                            ids[position],
                            PolicyRecovery::RolledBack,
                            "operator",
                            "wrong tenant",
                            Utc::now()
                        )
                        .await
                        .unwrap()
                );
                assert!(
                    store
                        .recover_policy_uncertain(
                            "tenant",
                            ids[position],
                            PolicyRecovery::RolledBack,
                            "",
                            "missing actor",
                            Utc::now()
                        )
                        .await
                        .is_err()
                );
                let rolled_back: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM business WHERE id=$1")
                        .bind(victim)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                assert_eq!(rolled_back, 0);
                handler.corrected.store(true, Ordering::SeqCst);
                assert!(
                    store
                        .recover_policy_uncertain(
                            "tenant",
                            ids[position],
                            PolicyRecovery::RolledBack,
                            "test-operator",
                            "verified rollback and corrected handler",
                            Utc::now()
                        )
                        .await
                        .unwrap()
                );
                wait(&task, || async {
                    completed(&store, &ids).await
                        && handler.inner.disposed.load(Ordering::SeqCst) == 5
                })
                .await;
                let audit: (String, String) =
                    sqlx::query_as("SELECT actor,reason FROM raddle_replay_audit WHERE job_id=$1")
                        .bind(ids[position])
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                assert_eq!(
                    audit,
                    (
                        "test-operator".to_owned(),
                        "verified rollback and corrected handler".to_owned()
                    )
                );
                assert_eq!(handler.inner.calls.load(Ordering::SeqCst), 5);
                assert_eq!(handler.inner.after_commit.load(Ordering::SeqCst), 4);
                assert_eq!(handler.inner.finalized.load(Ordering::SeqCst), 5);
                assert_eq!(handler.inner.disposed.load(Ordering::SeqCst), 5);
                assert_eq!(sibling_handler.inner.calls.load(Ordering::SeqCst), 1);
                let counts: Vec<(i32,i64)> = sqlx::query_as("SELECT id,count(*) FROM business WHERE id BETWEEN $1 AND $1+4 GROUP BY id ORDER BY id").bind(base).fetch_all(&pool).await.unwrap();
                assert_eq!(
                    counts,
                    (base..base + 5).map(|id| (id, 1)).collect::<Vec<_>>()
                );
                let attempts: i64 = sqlx::query_scalar(
                    "SELECT handling FROM raddle_policy_attempts WHERE job_id=$1",
                )
                .bind(ids[position])
                .fetch_one(&pool)
                .await
                .unwrap();
                assert_eq!(attempts, 2);
                stop.shutdown();
                task.await.unwrap().unwrap();
            }
        }
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn local_uncertainty_retains_volatile_custody_without_stopping_never_host_or_replaying_siblings(
    pool: PgPool,
) {
    setup(&pool).await;
    let store = Store::new(pool.clone());
    for (mode, fault) in [Fault::RollbackConnection, Fault::CommitConnection]
        .into_iter()
        .enumerate()
    {
        for position in 0..3usize {
            let base = mode as i32 * 1000 + position as i32 * 10;
            let name = format!("local-uncertain-{base}");
            let sibling_name = format!("local-sibling-{base}");
            let handler = Arc::new(FaultHandler::new(base + position as i32, fault));
            let sibling_handler = Arc::new(FaultHandler::new(-1, fault));
            let (queue, runner) = local(store.clone(), &name, Arc::clone(&handler));
            let (sibling, sibling_runner) =
                local(store.clone(), &sibling_name, Arc::clone(&sibling_handler));
            let mut inputs = Vec::new();
            for i in 0..3 {
                inputs.push(submit(&queue, base + i).await);
            }
            let mut host = Host::new(Duration::from_secs(10)).unwrap();
            runner.attach(&mut host, &name).unwrap();
            sibling_runner.attach(&mut host, &sibling_name).unwrap();
            let stop = host.handle();
            let task = tokio::spawn(host.run());
            wait(&task, || async {
                let states = queue.deliveries().unwrap();
                states.len() == 3
                    && states.iter().enumerate().all(|(i, s)| {
                        s.status
                            == if i == position {
                                MemoryStatus::Interrupted
                            } else {
                                MemoryStatus::Completed
                            }
                    })
            })
            .await;
            assert!(queue.retry_dead_letter(inputs[position].0).await.is_err());
            assert!(
                queue
                    .resume_interrupted(inputs[position].0, ReplaySafety::Never)
                    .await
                    .is_err()
            );
            queue.submit(inputs[position].1.clone()).await.unwrap(); // duplicate custody is not replay
            submit(&queue, base + 3).await;
            submit(&sibling, base + 4).await;
            wait(&task, || async {
                queue.deliveries().unwrap().len() == 4
                    && queue.deliveries().unwrap()[3].status == MemoryStatus::Completed
                    && sibling.deliveries().unwrap().len() == 1
                    && sibling.deliveries().unwrap()[0].status == MemoryStatus::Completed
            })
            .await;
            assert_eq!(
                queue.deliveries().unwrap()[position].status,
                MemoryStatus::Interrupted
            );
            assert_eq!(handler.inner.calls.load(Ordering::SeqCst), 4);
            handler.assert_isolated(base + 3).await;
            let markers: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_policy_attempts")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(
                markers, 0,
                "local custody must not claim a durable taint marker"
            );
            let rolled_back: i64 = sqlx::query_scalar("SELECT count(*) FROM business WHERE id=$1")
                .bind(handler.victim)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(rolled_back, 0);
            handler.corrected.store(true, Ordering::SeqCst);
            // The operator explicitly attests transactional safety after checking
            // this real server rollback. This memory API is not durable audit.
            queue
                .resume_interrupted(inputs[position].0, ReplaySafety::Transactional)
                .await
                .unwrap();
            wait(&task, || async {
                queue
                    .deliveries()
                    .unwrap()
                    .iter()
                    .all(|s| s.status == MemoryStatus::Completed)
            })
            .await;
            let states = queue.deliveries().unwrap();
            for (i, state) in states.iter().enumerate() {
                assert_eq!(state.attempts.handling, if i == position { 2 } else { 1 });
            }
            assert_eq!(handler.inner.calls.load(Ordering::SeqCst), 5);
            assert_eq!(handler.inner.finalized.load(Ordering::SeqCst), 5);
            assert_eq!(handler.inner.disposed.load(Ordering::SeqCst), 5);
            assert_eq!(handler.inner.after_commit.load(Ordering::SeqCst), 4);
            assert_eq!(sibling_handler.inner.calls.load(Ordering::SeqCst), 1);
            let counts: Vec<(i32,i64)> = sqlx::query_as("SELECT id,count(*) FROM business WHERE id BETWEEN $1 AND $1+4 GROUP BY id ORDER BY id").bind(base).fetch_all(&pool).await.unwrap();
            assert_eq!(
                counts,
                (base..base + 5).map(|id| (id, 1)).collect::<Vec<_>>()
            );
            stop.shutdown();
            task.await.unwrap().unwrap();
        }
    }
}

async fn secondary(pool: &PgPool) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap()
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn genuine_shared_claim_pool_failure_still_propagates_after_an_uncertain_attempt(
    pool: PgPool,
) {
    setup(&pool).await;
    let own_pool = secondary(&pool).await;
    let store = Store::new(own_pool.clone());
    let inspector = Store::new(pool.clone());
    let handler = Arc::new(FaultHandler::new(1, Fault::RollbackConnection));
    let failed = stage(&store, "shared-claim", 1).await;
    let healthy = stage(&store, "shared-claim", 2).await;
    let mut host = Host::new(Duration::from_secs(10)).unwrap();
    durable(store.clone(), "shared-claim", 1, Arc::clone(&handler))
        .attach(&mut host, Duration::from_millis(5))
        .unwrap();
    let task = tokio::spawn(host.run());
    wait(&task, || async {
        completed(&inspector, &[healthy]).await
            && handler.inner.disposed.load(Ordering::SeqCst) == 2
    })
    .await;
    tainted(&pool, failed).await;
    // Close only this endpoint's own pool, never the shared fixture/server.
    // Its next actual claim acquisition must still fail the NEVER service.
    own_pool.close().await;
    assert!(matches!(
        store.begin().await,
        Err(raddle_postgres::Error::Database(sqlx::Error::PoolClosed))
    ));
    assert!(
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    tainted(&pool, failed).await;
    assert_eq!(handler.inner.calls.load(Ordering::SeqCst), 2);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn genuine_shared_scope_admission_failure_still_propagates_in_the_local_runner(pool: PgPool) {
    setup(&pool).await;
    let own_pool = secondary(&pool).await;
    let handler = Arc::new(FaultHandler::new(1, Fault::RollbackConnection));
    let (queue, runner) = local(
        Store::new(own_pool.clone()),
        "shared-scope",
        Arc::clone(&handler),
    );
    submit(&queue, 1).await;
    submit(&queue, 2).await;
    let mut host = Host::new(Duration::from_secs(10)).unwrap();
    runner.attach(&mut host, "shared-scope").unwrap();
    let task = tokio::spawn(host.run());
    wait(&task, || async {
        let states = queue.deliveries().unwrap();
        states.len() == 2
            && states[0].status == MemoryStatus::Interrupted
            && states[1].status == MemoryStatus::Completed
    })
    .await;
    own_pool.close().await;
    assert!(matches!(
        own_pool.acquire().await,
        Err(sqlx::Error::PoolClosed)
    ));
    submit(&queue, 3).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let states = queue.deliveries().unwrap();
    assert_eq!(states[0].status, MemoryStatus::Interrupted);
    assert_eq!(states[1].status, MemoryStatus::Completed);
    assert_eq!(states[2].status, MemoryStatus::Interrupted);
    assert_eq!(
        handler.inner.calls.load(Ordering::SeqCst),
        2,
        "fresh scope admission failed before business work"
    );
}
