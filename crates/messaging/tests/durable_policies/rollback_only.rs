//! Confirmed aborts retain typed policy selection, unlike uncertain completion.
use super::{TypedFailure, Work, schema};
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use raddle_host::{Host, HostError, HostHandle, RestartPolicy};
use raddle_messaging::effects::{Effect, HandlerOutput, Outgoing};
use raddle_messaging::scoped::{
    CommitReceipt, Completion, Failure, Flush, LocalOnlyFlush, PostCommitError, ScopedHandler,
};
use raddle_messaging::{
    BoxFuture, DurableEndpoint, EndpointPolicies, MessageContext, PolicyQueue, PolicyQueueBuilder,
    TrustedOrigin,
};
use raddle_policies::{
    Action, ErrorFilter, Jitter, MemoryStatus, Plan, PolicySet, ReplaySafety, Rule, Step, Tail,
};
use raddle_postgres::{Delivery, DeliveryKind, Error, JobStatus, RetryPolicy, Scope, Store};
use sqlx::PgPool;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[path = "rollback_only_cases.rs"]
mod cases;
#[path = "rollback_only_faults.rs"]
mod faults;

const GATE: i64 = 58002;
#[derive(Clone, Copy, Debug)]
enum Poison {
    Invalid,
    CancelIdempotent,
    CancelBatch,
    LostConnection,
}
#[derive(Clone, Copy, Debug)]
enum Owner {
    Durable,
    Local,
}

struct Handler {
    pool: PgPool,
    victim: i32,
    poison: Poison,
    returned: bool,
    corrected: AtomicBool,
    calls: Mutex<BTreeMap<i32, u64>>,
    partial: AtomicUsize,
    created: AtomicUsize,
    rolled_back: AtomicUsize,
    uncertain: AtomicUsize,
    unexpected: AtomicUsize,
    finalized: AtomicUsize,
    failed: AtomicUsize,
    disposed: AtomicUsize,
    after_commit: AtomicUsize,
    cancelled: AtomicUsize,
}
impl Handler {
    fn new(pool: &PgPool, victim: i32, poison: Poison, returned: bool) -> Self {
        Self {
            pool: pool.clone(),
            victim,
            poison,
            returned,
            corrected: AtomicBool::new(false),
            calls: Mutex::new(BTreeMap::new()),
            partial: AtomicUsize::new(0),
            created: AtomicUsize::new(0),
            rolled_back: AtomicUsize::new(0),
            uncertain: AtomicUsize::new(0),
            unexpected: AtomicUsize::new(0),
            finalized: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            disposed: AtomicUsize::new(0),
            after_commit: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
        }
    }
    async fn poison(&self, scope: &mut Scope<'static>, work: &Work) {
        match self.poison {
            Poison::Invalid | Poison::LostConnection => {
                assert!(matches!(
                    scope.stage("", "tenant", &child(work.0, false), &[]).await,
                    Err(Error::Invalid(_))
                ));
                if matches!(self.poison, Poison::LostConnection) {
                    let _ = sqlx::query("SELECT pg_terminate_backend(pg_backend_pid())")
                        .execute(scope.connection())
                        .await;
                }
            }
            Poison::CancelIdempotent | Poison::CancelBatch => {
                let mut blocker = self.pool.begin().await.unwrap();
                sqlx::query("SELECT pg_advisory_xact_lock($1)")
                    .bind(GATE)
                    .execute(&mut *blocker)
                    .await
                    .unwrap();
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(scope.connection())
                    .await
                    .unwrap();
                let envelope = Envelope::encode(&Work(work.0 + 20000)).unwrap();
                let deliveries = [
                    delivery(DeliveryKind::Remote, "partial-child"),
                    delivery(DeliveryKind::Remote, "blocked-child"),
                ];
                let key = work.0.to_string();
                let mut operation = Box::pin(async {
                    match self.poison {
                        Poison::CancelIdempotent => scope
                            .stage_idempotent("source", "tenant", &key, &envelope, &deliveries)
                            .await
                            .map(|_| ()),
                        _ => scope
                            .stage("source", "tenant", &envelope, &deliveries)
                            .await
                            .map(|_| ()),
                    }
                });
                tokio::select! {
                    result = &mut operation => panic!("staging did not block: {result:?}"),
                    () = wait_for_lock(&self.pool, pid) => {},
                }
                drop(operation);
                blocker.rollback().await.unwrap();
                // Drain the outstanding statement and prove partial writes before
                // returning success. The message cancellation token is untouched.
                let partial: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_jobs WHERE message_id=$1 AND destination='partial-child'").bind(envelope.id()).fetch_one(scope.connection()).await.unwrap();
                assert_eq!(partial, 1);
                if matches!(self.poison, Poison::CancelIdempotent) {
                    let keys: i64 =
                        sqlx::query_scalar("SELECT count(*) FROM raddle_idempotency WHERE key=$1")
                            .bind(key)
                            .fetch_one(scope.connection())
                            .await
                            .unwrap();
                    assert_eq!(keys, 1);
                }
                self.partial.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    fn assert_cleanup(&self, attempts: usize, failed: usize, successful: usize) {
        assert_eq!(self.created.load(Ordering::SeqCst), attempts);
        assert_eq!(self.finalized.load(Ordering::SeqCst), attempts);
        assert_eq!(self.disposed.load(Ordering::SeqCst), attempts);
        assert_eq!(self.failed.load(Ordering::SeqCst), failed);
        assert_eq!(self.after_commit.load(Ordering::SeqCst), successful);
        assert_eq!(self.unexpected.load(Ordering::SeqCst), 0);
        assert_eq!(self.cancelled.load(Ordering::SeqCst), 0);
    }
}
impl ScopedHandler<Work> for Handler {
    type Resources = ();
    type Reply = ();
    type Error = TypedFailure;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            self.created.fetch_add(1, Ordering::SeqCst);
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
            *self.calls.lock().await.entry(work.0).or_default() += 1;
            sqlx::query("INSERT INTO business(id) VALUES($1)")
                .bind(work.0)
                .execute(scope.connection())
                .await
                .unwrap();
            scope
                .stage(
                    "source",
                    "tenant",
                    &child(work.0, false),
                    &[delivery(DeliveryKind::Remote, "direct")],
                )
                .await
                .unwrap();
            if work.0 == self.victim && !self.corrected.load(Ordering::SeqCst) {
                self.poison(scope, work).await;
            }
            let mut output = HandlerOutput::reply(());
            if self.returned {
                output
                    .effects
                    .push(Effect::Outgoing(Outgoing {
                        envelope: child(work.0, true),
                        deliveries: vec![delivery(DeliveryKind::Local, "returned")],
                    }))
                    .unwrap();
            }
            Ok(output)
        })
    }
    fn on_exception<'a>(
        &'a self,
        error: &'a Failure<TypedFailure>,
        _: &'a mut (),
        _: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            match error {
                Failure::Database(Error::RollbackOnly) => {
                    self.rolled_back.fetch_add(1, Ordering::SeqCst);
                }
                Failure::UncertainDatabase(Error::Database(_)) => {
                    self.uncertain.fetch_add(1, Ordering::SeqCst);
                }
                _ => {
                    self.unexpected.fetch_add(1, Ordering::SeqCst);
                }
            }
        })
    }
    fn finally<'a>(
        &'a self,
        completion: Completion,
        _: &'a mut (),
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            self.finalized.fetch_add(1, Ordering::SeqCst);
            if completion == Completion::Failed {
                self.failed.fetch_add(1, Ordering::SeqCst);
            }
            if context.cancellation().is_cancelled() {
                self.cancelled.fetch_add(1, Ordering::SeqCst);
            }
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
            Ok(())
        })
    }
}

#[derive(Default)]
struct TrackedFlush(AtomicUsize);
impl Flush for TrackedFlush {
    fn flush<'a>(
        &'a self,
        receipt: &'a CommitReceipt,
    ) -> BoxFuture<'a, Result<(), PostCommitError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            LocalOnlyFlush.flush(receipt).await
        })
    }
}
fn child(value: i32, returned: bool) -> Envelope {
    Envelope::with_identity(
        Uuid::from_u128(value as u128 + if returned { 100000 } else { 200000 }),
        DateTime::<Utc>::UNIX_EPOCH,
        &Work(value),
    )
    .unwrap()
}
fn delivery(kind: DeliveryKind, name: &str) -> Delivery {
    Delivery::new(kind, name, DateTime::<Utc>::UNIX_EPOCH)
}
async fn setup(pool: &PgPool) -> Store {
    schema(pool).await;
    sqlx::raw_sql("CREATE FUNCTION block_child() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.destination='blocked-child' THEN PERFORM pg_advisory_xact_lock(58002); END IF; RETURN NEW; END $$; CREATE TRIGGER block_child BEFORE INSERT ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION block_child();").execute(pool).await.unwrap();
    Store::new(pool.clone())
}
async fn wait_for_lock(pool: &PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND datname=current_database() AND wait_event_type='Lock')").bind(pid).fetch_one(pool).await.unwrap();
            if waiting { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
}
fn policies(action: Action) -> EndpointPolicies {
    let mut set = PolicySet::default();
    let plan = if action == Action::Discard {
        Plan::new(Vec::new(), Tail::Terminal(Action::Discard), Jitter::None)
    } else {
        Plan::new(
            vec![Step::new(action)],
            Tail::Terminal(Action::DeadLetter),
            Jitter::None,
        )
    }
    .unwrap();
    set.global(Rule {
        // Matching the concrete wrapper proves classification has not erased or
        // replaced the error with an untyped retry/terminal shortcut.
        filter: ErrorFilter::predicate(|error| {
            matches!(
                error.downcast_ref::<Failure<TypedFailure>>(),
                Some(Failure::Database(Error::RollbackOnly))
            )
        }),
        plan,
    });
    EndpointPolicies::new(set)
}

struct Running {
    queue: Option<PolicyQueue>,
    release: Option<oneshot::Sender<()>>,
    stop: HostHandle,
    task: Option<JoinHandle<Result<(), HostError>>>,
    witness_stopped: Arc<AtomicBool>,
}
impl Running {
    fn prepare(
        owner: Owner,
        store: &Store,
        name: &str,
        handler: Arc<Handler>,
        flush: Arc<TrackedFlush>,
        action: Action,
        replay: ReplaySafety,
    ) -> Self {
        let mut host = Host::new(Duration::from_secs(10)).unwrap();
        let witness_stopped = Arc::new(AtomicBool::new(false));
        let witness = Arc::clone(&witness_stopped);
        host.service("witness", RestartPolicy::NEVER, move |mut context| {
            let witness = Arc::clone(&witness);
            async move {
                context.set_ready(true);
                context.cancelled().await;
                witness.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
        let queue = match owner {
            Owner::Durable => {
                let mut endpoint =
                    DurableEndpoint::new(store.clone(), name, 3, RetryPolicy::default()).unwrap();
                endpoint.timeout(Duration::from_secs(60)).unwrap();
                endpoint.policies(policies(action)).unwrap();
                endpoint
                    .handle_policy::<Work, _>(handler, flush, replay)
                    .unwrap();
                endpoint
                    .attach(&mut host, Duration::from_millis(2))
                    .unwrap();
                None
            }
            Owner::Local => {
                let mut builder =
                    PolicyQueueBuilder::new(name, 16, Duration::from_secs(60), policies(action))
                        .unwrap();
                builder
                    .scoped::<Work, _>(handler, store.clone(), flush, replay)
                    .unwrap();
                let (queue, runner) = builder.build().unwrap();
                runner.attach(&mut host, name).unwrap();
                Some(queue)
            }
        };
        let stop = host.handle();
        let (release, start) = oneshot::channel();
        let task = tokio::spawn(async move {
            // Accept the complete initial cohort before ANY Host polling. This
            // is not a post-claim pause and cannot consume handling attempts.
            tokio::time::timeout(Duration::from_secs(60), start)
                .await
                .map_err(|_| HostError::Invalid("test Host startup gate timed out"))?
                .map_err(|_| HostError::Invalid("test Host startup gate closed"))?;
            host.run().await
        });
        Self {
            queue,
            release: Some(release),
            stop,
            task: Some(task),
            witness_stopped,
        }
    }
    async fn start(&mut self, handler: &Handler) {
        self.alive();
        assert_eq!(handler.created.load(Ordering::SeqCst), 0);
        assert!(handler.calls.lock().await.is_empty());
        self.release.take().unwrap().send(()).unwrap();
        // Actual typed handler progress, not an endpoint readiness notification,
        // proves that the gate released the shipping receiving owner.
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                self.alive();
                if !handler.calls.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    fn alive(&self) {
        assert!(!self.task.as_ref().unwrap().is_finished());
        assert!(!self.witness_stopped.load(Ordering::SeqCst));
    }
    async fn admit(&self, store: &Store, name: &str, value: i32) -> Uuid {
        if let Some(queue) = &self.queue {
            let envelope = Envelope::encode(&Work(value)).unwrap();
            let id = envelope.id();
            queue
                .submit(MessageContext::new(
                    TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
                    envelope,
                    Utc::now(),
                ))
                .await
                .unwrap();
            id
        } else {
            super::stage(store, name, value).await
        }
    }
    async fn state(&self, store: &Store, id: Uuid) -> Option<MemoryStatus> {
        if let Some(queue) = &self.queue {
            queue
                .deliveries()
                .unwrap()
                .iter()
                .find(|delivery| delivery.envelope.id() == id)
                .map(|delivery| delivery.status)
        } else {
            store
                .inspect("tenant", id)
                .await
                .unwrap()
                .map(|job| match job.status {
                    JobStatus::Completed => MemoryStatus::Completed,
                    JobStatus::Dead => MemoryStatus::DeadLetter {
                        replay_denied: job.last_error.as_deref() == Some("policy-replay-denied"),
                    },
                    JobStatus::Ready => MemoryStatus::Ready,
                    _ => MemoryStatus::Active,
                })
        }
    }
    async fn wait(&self, store: &Store, expected: &[(Uuid, MemoryStatus)]) {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                self.alive();
                let mut ready = true;
                for (id, state) in expected {
                    ready &= self.state(store, *id).await == Some(*state);
                }
                if ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    async fn retry(&self, store: &Store, id: Uuid) {
        if let Some(queue) = &self.queue {
            queue.retry_dead_letter(id).await.unwrap();
        } else {
            assert!(
                store
                    .replay(
                        "tenant",
                        id,
                        "operator",
                        "corrected failed input",
                        Utc::now()
                    )
                    .await
                    .unwrap()
            );
        }
    }
    async fn attempts(&self, pool: &PgPool, id: Uuid) -> u64 {
        if let Some(queue) = &self.queue {
            queue
                .deliveries()
                .unwrap()
                .iter()
                .find(|delivery| delivery.envelope.id() == id)
                .unwrap()
                .attempts
                .handling
        } else {
            sqlx::query_scalar::<_, i64>(
                "SELECT handling FROM raddle_policy_attempts WHERE job_id=$1",
            )
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap() as u64
        }
    }
    async fn wait_cleanup(&self, handler: &Handler, attempts: usize) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while handler.disposed.load(Ordering::SeqCst) != attempts {
                self.alive();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    async fn stop(mut self) {
        self.stop.shutdown();
        let mut task = self.task.take().unwrap();
        match tokio::time::timeout(Duration::from_secs(20), &mut task).await {
            Ok(result) => result.unwrap().unwrap(),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("Host did not drain after shutdown");
            }
        }
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.shutdown();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn effects(pool: &PgPool, base: i32, expected: &[i32], returned: bool) {
    let business: Vec<i32> =
        sqlx::query_scalar("SELECT id FROM business WHERE id BETWEEN $1 AND $1+3 ORDER BY id")
            .bind(base)
            .fetch_all(pool)
            .await
            .unwrap();
    assert_eq!(business, expected);
    for value in base..base + 4 {
        for (is_returned, destination) in [(false, "direct"), (true, "returned")] {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM raddle_jobs WHERE message_id=$1 AND destination=$2",
            )
            .bind(child(value, is_returned).id())
            .bind(destination)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(
                count,
                i64::from(expected.contains(&value) && (!is_returned || returned))
            );
        }
    }
    let partial: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raddle_jobs WHERE destination IN ('partial-child','blocked-child')",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_idempotency")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!((partial, keys), (0, 0));
}
