//! Native bus execution, real SQL custody and explicit receiving ownership.
#![allow(clippy::unwrap_used)]

#[path = "handler_dispatch/separated_routes.rs"]
mod separated_routes;

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, RuntimeBuilder};
use raddle_host::Host;
use raddle_messaging::composition::{Group, Then};
use raddle_messaging::effects::{Effect, Effects, HandlerOutput, Outgoing};
use raddle_messaging::handler_routing::{
    Custody, DispatchOwners, EndpointOptions, HandlerSet, MultipleHandlerBehavior,
};
use raddle_messaging::scoped::{
    CommitReceipt, Completion, Failure, LocalOnlyFlush, Phase, PostCommitError, ScopedHandler,
};
use raddle_messaging::{
    BoxFuture, BusBuilder, LocalQueue, MessageBus, MessageContext, QueueMode, QueueSettings,
    TrustedOrigin,
};
use raddle_postgres::{
    Delivery, DeliveryKind, FailureDisposition, RetryPolicy, SagaState, Scope, Store,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::{Notify, Semaphore};

#[derive(Serialize, Deserialize)]
struct Work(i32);
impl WireMessage for Work {
    const NAME: &'static str = "test.handler-dispatch";
    const VERSION: u32 = 1;
}
impl Request for Work {
    type Reply = Response;
    type Error = Fault;
}
#[derive(Debug, Serialize, Deserialize)]
struct Response(i32);
impl WireMessage for Response {
    const NAME: &'static str = "test.handler-response";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Child(i32, i32);
impl WireMessage for Child {
    const NAME: &'static str = "test.handler-child";
    const VERSION: u32 = 1;
}
#[derive(Debug, thiserror::Error)]
#[error("scripted member failure")]
struct Fault;

#[derive(Serialize, Deserialize)]
struct Counter(i32);
impl SagaState for Counter {
    const NAME: &'static str = "test.handler-saga";
    const VERSION: u32 = 1;
}

struct Resource<const N: i32> {
    backend: i32,
    loaded: Option<i32>,
}
struct Member<const N: i32> {
    fail: AtomicI32,
    blocked: AtomicI32,
    after_fail: AtomicBool,
    finally_panic: AtomicBool,
    dispose_fail: AtomicBool,
    create_blocked: AtomicBool,
    requires_first: AtomicBool,
    created: AtomicUsize,
    entered: Notify,
    disposing: Notify,
    dispose_gate: Option<Arc<Semaphore>>,
    log: Arc<Mutex<Vec<(i32, String)>>>,
}
impl<const N: i32> Member<N> {
    fn new(log: &Arc<Mutex<Vec<(i32, String)>>>) -> Self {
        Self {
            fail: AtomicI32::new(-1),
            blocked: AtomicI32::new(-1),
            after_fail: AtomicBool::new(false),
            finally_panic: AtomicBool::new(false),
            dispose_fail: AtomicBool::new(false),
            create_blocked: AtomicBool::new(false),
            requires_first: AtomicBool::new(false),
            created: AtomicUsize::new(0),
            entered: Notify::new(),
            disposing: Notify::new(),
            dispose_gate: None,
            log: Arc::clone(log),
        }
    }
    fn record(&self, phase: impl Into<String>) {
        self.log.lock().unwrap().push((N, phase.into()));
    }
    fn reply(&self, id: i32) -> i32 {
        id * 10 + N
    }
}
impl<const N: i32> ScopedHandler<Work> for Member<N> {
    type Resources = Resource<N>;
    type Reply = i32;
    type Error = Fault;
    fn create<'a>(
        &'a self,
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<Self::Resources, Fault>> {
        Box::pin(async move {
            if self.create_blocked.load(Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.created.fetch_add(1, Ordering::SeqCst);
            self.record("Create");
            Ok(Resource {
                backend: sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(scope.connection())
                    .await
                    .map_err(|_| Fault)?,
                loaded: None,
            })
        })
    }
    fn phase<'a>(
        &'a self,
        phase: Phase,
        work: &'a Work,
        resource: &'a mut Resource<N>,
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async move {
            self.record(format!("{phase:?}"));
            let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(scope.connection())
                .await
                .map_err(|_| Fault)?;
            assert_eq!(backend, resource.backend);
            if phase == Phase::Load {
                resource.loaded = Some(work.0 * 10 + N);
            }
            if phase == Phase::Validate {
                assert_eq!(resource.loaded, Some(work.0 * 10 + N));
            }
            if phase == Phase::After {
                assert_eq!(resource.loaded, Some(work.0 * 10 + N + 100));
            }
            Ok(())
        })
    }
    fn handle<'a>(
        &'a self,
        work: &'a Work,
        resource: &'a mut Resource<N>,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<i32, Resource<N>>, Fault>> {
        Box::pin(async move {
            self.record(format!("Handle:{}", work.0));
            if self.requires_first.load(Ordering::SeqCst) {
                assert_eq!(
                    sqlx::query_scalar::<_, i64>(
                        "SELECT count(*) FROM facts WHERE id=$1 AND member=1 AND phase='inline'"
                    )
                    .bind(work.0)
                    .fetch_one(scope.connection())
                    .await
                    .map_err(|_| Fault)?,
                    1
                );
            }
            sqlx::query("INSERT INTO facts(id,member,phase,backend,tx) VALUES($1,$2,'handle',pg_backend_pid(),txid_current())").bind(work.0).bind(N).execute(scope.connection()).await.map_err(|_| Fault)?;
            assert_eq!(resource.loaded, Some(work.0 * 10 + N));
            if self.blocked.load(Ordering::SeqCst) == work.0 {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            if N != 3 && self.fail.load(Ordering::SeqCst) == work.0 {
                return Err(Fault);
            }
            if N == 3 {
                let mut saga = scope
                    .saga(context.origin().tenant(), &work.0.to_string(), || {
                        Counter(0)
                    })
                    .await
                    .map_err(|_| Fault)?;
                saga.state_mut().0 += 1;
                scope.save_saga(&mut saga).await.map_err(|_| Fault)?;
                if self.fail.load(Ordering::SeqCst) == work.0 {
                    return Err(Fault);
                }
            }
            let id = work.0;
            let inline = Effect::inline(move |resource: &mut Resource<N>, scope, _| {
                Box::pin(async move {
                    assert_eq!(resource.loaded, Some(id * 10 + N));
                    resource.loaded = Some(id * 10 + N + 100);
                    sqlx::query("INSERT INTO facts(id,member,phase,backend,tx) VALUES($1,$2,'inline',pg_backend_pid(),txid_current())").bind(id).bind(N).execute(scope.connection()).await.map_err(raddle_postgres::Error::from)?;
                    Ok(())
                })
            });
            let outgoing = Effect::Outgoing(Outgoing {
                envelope: Envelope::encode(&Child(id, N)).map_err(|_| Fault)?,
                deliveries: vec![Delivery::new(DeliveryKind::Local, "children", Utc::now())],
            });
            Ok(HandlerOutput {
                reply: self.reply(id),
                effects: Effects::returned((inline, outgoing)).map_err(|_| Fault)?,
            })
        })
    }
    fn on_exception<'a>(
        &'a self,
        _: &'a Failure<Fault>,
        _: &'a mut Resource<N>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.record("Exception");
        })
    }
    fn after_commit<'a>(
        &'a self,
        _: &'a mut Resource<N>,
        _: &'a CommitReceipt,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async move {
            self.record("AfterCommit");
            if self.after_fail.load(Ordering::SeqCst) {
                Err(Fault)
            } else {
                Ok(())
            }
        })
    }
    fn finally<'a>(
        &'a self,
        completion: Completion,
        _: &'a mut Resource<N>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async move {
            self.record(format!("Finally:{completion:?}"));
            assert!(
                !self.finally_panic.load(Ordering::SeqCst),
                "scripted finally panic"
            );
            Ok(())
        })
    }
    fn dispose<'a>(
        &'a self,
        _: Resource<N>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async move {
            self.disposing.notify_one();
            if let Some(gate) = &self.dispose_gate {
                gate.acquire().await.unwrap().forget();
            }
            self.record("Dispose");
            if self.dispose_fail.load(Ordering::SeqCst) {
                Err(Fault)
            } else {
                Ok(())
            }
        })
    }
}

fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant", "test", None).unwrap()
}
fn context(id: i32) -> MessageContext {
    MessageContext::new(origin(), Envelope::encode(&Work(id)).unwrap(), Utc::now())
}
fn settings() -> QueueSettings {
    QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 8,
        parallelism: 1,
    }
}
fn builder() -> BusBuilder {
    BusBuilder::new(RuntimeBuilder::default().build())
}
fn executions(builder: &mut BusBuilder, host: &mut Host) {
    let (queue, runner) = LocalQueue::new(settings()).unwrap();
    builder.execution_queue(queue).unwrap();
    runner.attach(host, "invocations").unwrap();
}
fn attach(mut owners: DispatchOwners, bus: &MessageBus, host: &mut Host) {
    for (name, _, runner) in owners.local {
        runner.attach(host, &name).unwrap();
    }
    for (_, mut endpoint) in owners.durable.drain(..) {
        endpoint.message_bus(bus.clone());
        endpoint.attach(host, Duration::from_millis(10)).unwrap();
    }
}
async fn schema(pool: &PgPool) {
    sqlx::query("CREATE TABLE facts(id integer,member integer,phase text,backend integer,tx bigint,PRIMARY KEY(id,member,phase,tx))").execute(pool).await.unwrap();
}
async fn rows(pool: &PgPool) -> Vec<(i32, i32)> {
    sqlx::query_as("SELECT id,member FROM facts WHERE phase='handle' ORDER BY id,member")
        .fetch_all(pool)
        .await
        .unwrap()
}
async fn wait_rows(pool: &PgPool, count: usize) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if rows(pool).await.len() == count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
fn retry<E: std::error::Error>() -> EndpointOptions<E> {
    let mut options = EndpointOptions::default();
    options.retry = RetryPolicy::try_new(
        Duration::from_millis(1),
        Duration::from_millis(2),
        Duration::from_secs(20),
    )
    .unwrap();
    options.parallelism = 3;
    options
}

#[tokio::test]
async fn startup_distinguishes_groups_from_phases_and_configuration_is_atomic() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let one = Group::new("one", Arc::new(Member::<1>::new(&log)));
    // Store construction needs no live connection; registration performs no I/O.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused/unused")
        .unwrap();
    let store = Store::new(pool);
    let mut bus = builder();
    let one = HandlerSet::<Work, _>::new(one)
        .register(&mut bus, "default", store.clone(), Arc::new(LocalOnlyFlush))
        .unwrap();
    assert!(bus.assert_no_combined_handlers().is_ok());
    drop(one);
    let chain = Then::new(
        Group::new("one", Arc::new(Member::<1>::new(&log))),
        Group::new("two", Arc::new(Member::<2>::new(&log))),
    );
    let two = HandlerSet::<Work, _>::new(chain)
        .register(
            &mut bus,
            "combined",
            store.clone(),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    assert!(bus.assert_no_combined_handlers().is_err());
    assert_eq!(bus.combined_handlers()[0].groups, ["one", "two"]);
    drop(two);
    let invalid = Then::new(
        Group::new("same", Arc::new(Member::<1>::new(&log))),
        Group::new("same", Arc::new(Member::<2>::new(&log))),
    );
    assert!(
        HandlerSet::<Work, _>::new(invalid)
            .register(&mut bus, "invalid", store, Arc::new(LocalOnlyFlush))
            .is_err()
    );
    assert_eq!(bus.combined_handlers().len(), 1);
    assert!(log.lock().unwrap().is_empty());
}

struct ProducerOnly;
impl Transport for ProducerOnly {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        Ok(())
    }
}

#[tokio::test]
async fn outgoing_remote_routes_remain_producers_while_leftovers_get_a_real_local_queue() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused/unused")
        .unwrap();
    let mut builder = builder();
    builder.remote("upstream", ProducerOnly).unwrap();
    builder.route::<Work>("upstream").unwrap();
    let registration =
        HandlerSet::<Work, _>::new(Group::new("handler", Arc::new(Member::<1>::new(&log))))
            .register(
                &mut builder,
                "local",
                Store::new(pool),
                Arc::new(LocalOnlyFlush),
            )
            .unwrap();
    assert_eq!(registration.owners.local.len(), 1);
    assert!(registration.owners.durable.is_empty());
    let bus = builder.build();
    bus.send(&Work(1), origin()).await.unwrap();
    assert_eq!(registration.owners.local[0].1.snapshot().buffered, 1);
    assert!(log.lock().unwrap().is_empty()); // No host/SQL connection or hidden producer task.
}

#[derive(Serialize, Deserialize)]
struct InvalidWork {
    unexpected: String,
}
impl WireMessage for InvalidWork {
    const NAME: &'static str = Work::NAME;
    const VERSION: u32 = Work::VERSION;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn default_selected_policy_rejects_bad_typed_input_without_blocking_valid_neighbors(
    pool: PgPool,
) {
    schema(&pool).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let handler = Arc::new(Member::<1>::new(&log));
    let mut builder = builder();
    let mut options = EndpointOptions::default();
    options.parallelism = 3;
    let reg = HandlerSet::<Work, _>::new(Group::new("handler", Arc::clone(&handler)))
        .custody(Custody::Durable)
        .endpoint_options("roots", options)
        .unwrap()
        .register(
            &mut builder,
            "roots",
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    let bus = builder.build();
    bus.send(&Work(1), origin()).await.unwrap();
    bus.dispatch(
        MessageContext::new(
            origin(),
            Envelope::encode(&InvalidWork {
                unexpected: "invalid typed shape".into(),
            })
            .unwrap(),
            Utc::now(),
        ),
        true,
    )
    .await
    .unwrap();
    bus.send(&Work(2), origin()).await.unwrap();
    assert_eq!(reg.owners.durable[0].1.run_once().await.unwrap(), 3);
    assert_eq!(rows(&pool).await, [(1, 1), (2, 1)]);
    assert_eq!(handler.created.load(Ordering::SeqCst), 2);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raddle_jobs WHERE destination='roots' AND status='dead' AND attempts=1").fetch_one(&pool).await.unwrap(),1);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raddle_jobs WHERE destination='roots' AND status='completed' AND attempts=1").fetch_one(&pool).await.unwrap(),2);
}

#[tokio::test]
async fn late_remote_selection_failure_does_not_leak_earlier_created_queue_or_route() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused/unused")
        .unwrap();
    let mut builder = builder();
    builder.remote("z", ProducerOnly).unwrap();
    let chain = Then::new(
        Group::new("a", Arc::new(Member::<1>::new(&log))),
        Group::new("z", Arc::new(Member::<2>::new(&log))),
    );
    assert!(
        HandlerSet::<Work, _>::new(chain)
            .behavior(MultipleHandlerBehavior::Separated)
            .register(
                &mut builder,
                "unused",
                Store::new(pool),
                Arc::new(LocalOnlyFlush)
            )
            .is_err()
    );
    assert!(builder.route::<Work>("a").is_err());
    builder.route::<Work>("z").unwrap();
    assert!(log.lock().unwrap().is_empty());
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn classic_bus_rolls_back_earlier_sql_inline_and_outgoing_then_runs_healthy_work(
    pool: PgPool,
) {
    schema(&pool).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::new(Member::<1>::new(&log));
    let b = Arc::new(Member::<2>::new(&log));
    b.fail.store(2, Ordering::SeqCst);
    b.requires_first.store(true, Ordering::SeqCst);
    let mut builder = builder();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    executions(&mut builder, &mut host);
    let reg = HandlerSet::<Work, _>::new(Then::new(
        Group::new("a", Arc::clone(&a)),
        Group::new("b", Arc::clone(&b)),
    ))
    .register(
        &mut builder,
        "classic",
        Store::new(pool.clone()),
        Arc::new(LocalOnlyFlush),
    )
    .unwrap();
    assert_eq!(builder.combined_handlers().len(), 1);
    let bus = builder.build();
    attach(reg.owners, &bus, &mut host);
    let stop = host.handle();
    let task = tokio::spawn(host.run());
    bus.send(&Work(1), origin()).await.unwrap();
    wait_rows(&pool, 2).await;
    assert!(
        bus.invoke_scoped(reg.global.as_ref().unwrap(), context(2))
            .await
            .is_err()
    );
    assert_eq!(rows(&pool).await, [(1, 1), (1, 2)]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    b.fail.store(-1, Ordering::SeqCst);
    let result = bus
        .invoke_scoped(reg.global.as_ref().unwrap(), context(2))
        .await
        .unwrap();
    assert_eq!(result.reply, (Some(21), Some(22)));
    assert!(result.post_commit.is_ok());
    bus.send(&Work(3), origin()).await.unwrap();
    wait_rows(&pool, 6).await;
    stop.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(DISTINCT tx) FROM facts WHERE id=2")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(DISTINCT backend) FROM facts WHERE id=2")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let log = log.lock().unwrap();
    let start = log
        .iter()
        .position(|e| *e == (1, "Handle:1".into()))
        .unwrap();
    let next = log
        .iter()
        .position(|e| *e == (2, "Handle:1".into()))
        .unwrap();
    assert!(start < next);
    assert_eq!(
        log.iter().filter(|(_, phase)| phase == "Dispose").count(),
        8
    );
    assert!(log.contains(&(2, "Exception".into())));
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn separated_bus_retains_siblings_and_saga_across_first_middle_last_failed_only_retry(
    pool: PgPool,
) {
    schema(&pool).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::new(Member::<1>::new(&log));
    let b = Arc::new(Member::<2>::new(&log));
    let saga = Arc::new(Member::<3>::new(&log));
    let chain = Then::new(
        Then::new(
            Group::new("a", Arc::clone(&a)),
            Group::new("b", Arc::clone(&b)),
        ),
        Group::new("saga", Arc::clone(&saga)),
    );
    let mut builder = builder();
    let mut set = HandlerSet::<Work, _>::new(chain)
        .behavior(MultipleHandlerBehavior::Separated)
        .custody(Custody::Durable);
    for name in ["a", "b", "saga"] {
        set = set.endpoint_options(name, retry()).unwrap();
    }
    let reg = set
        .register(
            &mut builder,
            "unused",
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    assert!(reg.owners.local.is_empty());
    assert_eq!(reg.owners.durable.len(), 3);
    assert!(builder.assert_no_combined_handlers().is_ok());
    let bus = builder.build();
    let endpoints = reg.owners.durable;
    for position in 0..3 {
        let first = position * 3 + 1;
        let failed = first + position;
        b.fail.store(failed, Ordering::SeqCst);
        for id in first..first + 3 {
            bus.send(&Work(id), origin()).await.unwrap();
        }
        // Real independently claimed SQL subscriptions, not callback loops.
        for (_, endpoint) in &endpoints {
            assert_eq!(endpoint.run_once().await.unwrap(), 3);
        }
        assert_eq!(rows(&pool).await.len(), (position as usize + 1) * 9 - 1);
        let successes: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_jobs WHERE destination IN ('a','b','saga') AND status='completed'").fetch_one(&pool).await.unwrap();
        assert_eq!(successes, i64::from((position + 1) * 9 - 1));
        b.fail.store(-1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        for (_, endpoint) in &endpoints {
            endpoint.run_once().await.unwrap();
        }
        assert_eq!(rows(&pool).await.len(), (position as usize + 1) * 9);
    }
    assert_eq!(a.created.load(Ordering::SeqCst), 9);
    assert_eq!(saga.created.load(Ordering::SeqCst), 9);
    assert_eq!(b.created.load(Ordering::SeqCst), 12);
    let states: Vec<serde_json::Value> = sqlx::query_scalar("SELECT state FROM raddle_sagas")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(states.len(), 9);
    assert!(states.iter().all(|value| value == &serde_json::json!(1)));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE destination='children'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        27
    );
    // Fail the saga itself *after* its enrolled state write. Its rollback and
    // corrected retry must retain both successful ordinary siblings.
    saga.fail.store(10, Ordering::SeqCst);
    bus.send(&Work(10), origin()).await.unwrap();
    for (_, endpoint) in &endpoints {
        assert_eq!(endpoint.run_once().await.unwrap(), 1);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_sagas WHERE correlation='10'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    saga.fail.store(-1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(5)).await;
    for (name, endpoint) in &endpoints {
        assert_eq!(
            endpoint.run_once().await.unwrap(),
            usize::from(name == "saga")
        );
    }
    assert_eq!(a.created.load(Ordering::SeqCst), 10);
    assert_eq!(b.created.load(Ordering::SeqCst), 13);
    assert_eq!(saga.created.load(Ordering::SeqCst), 11);
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT state FROM raddle_sagas WHERE correlation='10'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        serde_json::json!(1)
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn sticky_resolves_existing_and_missing_queue_with_only_selected_groups_and_leftovers(
    pool: PgPool,
) {
    schema(&pool).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::new(Member::<1>::new(&log));
    let b = Arc::new(Member::<2>::new(&log));
    let global = Arc::new(Member::<3>::new(&log));
    let chain = Then::new(
        Then::new(
            Group::new("a", Arc::clone(&a)),
            Group::new("b", Arc::clone(&b)),
        ),
        Group::new("global", Arc::clone(&global)),
    );
    let mut builder = builder();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    let (existing, runner) = LocalQueue::new(settings()).unwrap();
    builder.local("a", existing.clone()).unwrap();
    runner.attach(&mut host, "a").unwrap();
    let (other, runner) = LocalQueue::new(settings()).unwrap();
    builder.local("other", other).unwrap();
    builder.route::<Work>("other").unwrap();
    runner.attach(&mut host, "other").unwrap();
    let reg = HandlerSet::<Work, _>::new(chain)
        .sticky("a", "a")
        .unwrap()
        .sticky("b", "b")
        .unwrap()
        .register(
            &mut builder,
            "default",
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    assert_eq!(
        reg.owners
            .local
            .iter()
            .map(|(name, _, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["b", "default"]
    );
    assert!(builder.assert_no_combined_handlers().is_ok());
    let bus = builder.build();
    attach(reg.owners, &bus, &mut host);
    let stop = host.handle();
    let task = tokio::spawn(host.run());
    bus.send(&Work(1), origin()).await.unwrap();
    wait_rows(&pool, 4).await;
    stop.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(rows(&pool).await, [(1, 1), (1, 2), (1, 3), (1, 3)]);
    assert_eq!(a.created.load(Ordering::SeqCst), 1);
    assert_eq!(b.created.load(Ordering::SeqCst), 1);
    assert_eq!(global.created.load(Ordering::SeqCst), 2);
    assert_eq!(existing.snapshot().completed, 1);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn prebuilt_override_is_endpoint_only_with_its_own_policy_and_failed_only_retry(
    pool: PgPool,
) {
    schema(&pool).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::new(Member::<1>::new(&log));
    let global = Arc::new(Member::<2>::new(&log));
    let ordinary = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&ordinary);
    let mut runtime = RuntimeBuilder::default();
    let global_request = Arc::clone(&global);
    runtime
        .handle(move |work: Work| {
            called.fetch_add(1, Ordering::SeqCst);
            let reply = global_request.reply(work.0);
            async move { Ok::<_, Fault>(Response(reply)) }
        })
        .unwrap();
    let mut builder = BusBuilder::new(runtime.build());
    let store = Store::new(pool.clone());
    for name in ["a", "b"] {
        builder.durable(name, store.clone()).unwrap();
        builder.route::<Work>(name).unwrap();
    }
    let classified = Arc::new(AtomicUsize::new(0));
    let classified_a = Arc::clone(&classified);
    let timed_out = Arc::new(AtomicUsize::new(0));
    let timeout_a = Arc::clone(&timed_out);
    let mut a_options = retry().classify(move |failure| {
        classified_a.fetch_add(1, Ordering::SeqCst);
        if matches!(failure, Failure::TimedOut) {
            timeout_a.fetch_add(1, Ordering::SeqCst);
        }
        FailureDisposition::Retry
    });
    a_options.timeout = Some(Duration::from_secs(2));
    let owners_a = builder
        .sticky_handler::<Work, _>(
            "a",
            Arc::clone(&a),
            store.clone(),
            Arc::new(LocalOnlyFlush),
            a_options,
        )
        .unwrap();
    let reg = HandlerSet::<Work, _>::new(Group::new("global", Arc::clone(&global)))
        .custody(Custody::Durable)
        .endpoint_options("b", retry().classify(|_| FailureDisposition::Reject))
        .unwrap()
        .endpoint_options("default", retry().classify(|_| FailureDisposition::Reject))
        .unwrap()
        .register(&mut builder, "default", store, Arc::new(LocalOnlyFlush))
        .unwrap();
    let bus = builder.build();
    let mut endpoints = owners_a.durable;
    endpoints.extend(reg.owners.durable);
    a.fail.store(2, Ordering::SeqCst);
    for id in 1..=3 {
        bus.send(&Work(id), origin()).await.unwrap();
    }
    for (_, endpoint) in &endpoints {
        assert_eq!(endpoint.run_once().await.unwrap(), 3);
    }
    assert_eq!(rows(&pool).await.len(), 8);
    assert_eq!(classified.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM facts WHERE phase='handle' AND member=1 AND id=2"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    a.fail.store(-1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(5)).await;
    for (name, endpoint) in &endpoints {
        assert_eq!(endpoint.run_once().await.unwrap(), usize::from(name == "a"));
    }
    assert_eq!(rows(&pool).await.len(), 9);
    assert_eq!(a.created.load(Ordering::SeqCst), 4);
    assert_eq!(global.created.load(Ordering::SeqCst), 6);
    assert_eq!(bus.invoke(Work(4), origin()).await.unwrap().0, 42);
    assert_eq!(ordinary.load(Ordering::SeqCst), 1);
    // The same failure on global B/default uses Reject, not A's Retry policy.
    global.fail.store(5, Ordering::SeqCst);
    bus.send(&Work(5), origin()).await.unwrap();
    for (_, endpoint) in &endpoints {
        endpoint.run_once().await.unwrap();
    }
    assert_eq!(classified.load(Ordering::SeqCst), 1);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raddle_jobs WHERE destination IN ('b','default') AND status='dead'").fetch_one(&pool).await.unwrap(),2);
    // A's timeout rolls back A only; B/default and ordinary Invoke remain global.
    a.blocked.store(7, Ordering::SeqCst);
    bus.send(&Work(7), origin()).await.unwrap();
    for (_, endpoint) in &endpoints {
        endpoint.run_once().await.unwrap();
    }
    assert_eq!(timed_out.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM facts WHERE id=7 AND phase='handle' AND member=1"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM facts WHERE id=7 AND phase='handle' AND member=2"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
    assert_eq!(bus.invoke(Work(7), origin()).await.unwrap().0, 72);
    a.blocked.store(-1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(5)).await;
    for (name, endpoint) in &endpoints {
        assert_eq!(endpoint.run_once().await.unwrap(), usize::from(name == "a"));
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn composed_cancellation_joins_prior_resource_cleanup_during_later_construction(
    pool: PgPool,
) {
    schema(&pool).await;
    for during_create in [true, false] {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(Semaphore::new(0));
        let mut first = Member::<1>::new(&log);
        first.dispose_gate = Some(Arc::clone(&gate));
        let a = Arc::new(first);
        let b = Arc::new(Member::<2>::new(&log));
        b.create_blocked.store(during_create, Ordering::SeqCst);
        if !during_create {
            b.blocked.store(1, Ordering::SeqCst);
        }
        let mut builder = builder();
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        executions(&mut builder, &mut host);
        let reg = HandlerSet::<Work, _>::new(Then::new(
            Group::new("a", Arc::clone(&a)),
            Group::new("b", Arc::clone(&b)),
        ))
        .register(
            &mut builder,
            "default",
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
        let bus = builder.build();
        attach(reg.owners, &bus, &mut host);
        let stop = host.handle();
        let mut task = tokio::spawn(host.run());
        let call = tokio::spawn(async move {
            bus.invoke_scoped(reg.global.as_ref().unwrap(), context(1))
                .await
        });
        b.entered.notified().await;
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        a.disposing.notified().await;
        assert!(rows(&pool).await.is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        stop.shutdown();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut task)
                .await
                .is_err()
        );
        gate.add_permits(1);
        task.await.unwrap().unwrap();
        let log = log.lock().unwrap();
        assert!(log.contains(&(1, "Finally:Failed".into())));
        assert!(log.contains(&(1, "Dispose".into())));
        if during_create {
            assert!(!log.iter().any(|(member, _)| *member == 2));
        } else {
            assert!(log.contains(&(2, "Finally:Failed".into())));
            assert!(log.contains(&(2, "Dispose".into())));
        }
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn separated_host_keeps_current_and_new_later_work_live_while_only_failure_retries(
    pool: PgPool,
) {
    schema(&pool).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::new(Member::<1>::new(&log));
    let b = Arc::new(Member::<2>::new(&log));
    let saga = Arc::new(Member::<3>::new(&log));
    b.fail.store(1, Ordering::SeqCst);
    let chain = Then::new(
        Then::new(
            Group::new("a", Arc::clone(&a)),
            Group::new("b", Arc::clone(&b)),
        ),
        Group::new("saga", Arc::clone(&saga)),
    );
    let mut builder = builder();
    let mut set = HandlerSet::<Work, _>::new(chain)
        .behavior(MultipleHandlerBehavior::Separated)
        .custody(Custody::Durable);
    for name in ["a", "b", "saga"] {
        let mut options = retry();
        if name == "b" {
            // Leave time to prove healthy current/later work before correcting
            // the failed order; do not exhaust the five-attempt inbox budget.
            options.retry = RetryPolicy::try_new(
                Duration::from_secs(10),
                Duration::from_secs(10),
                Duration::from_secs(30),
            )
            .unwrap();
        }
        set = set.endpoint_options(name, options).unwrap();
    }
    let reg = set
        .register(
            &mut builder,
            "unused",
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    let bus = builder.build();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    attach(reg.owners, &bus, &mut host);
    let stop = host.handle();
    let task = tokio::spawn(host.run());
    bus.send(&Work(1), origin()).await.unwrap();
    bus.send(&Work(2), origin()).await.unwrap();
    wait_rows(&pool, 5).await;
    bus.send(&Work(3), origin()).await.unwrap();
    wait_rows(&pool, 8).await;
    assert!(stop.snapshot().running);
    assert!(
        stop.snapshot()
            .services
            .iter()
            .all(|status| status.attempt == 1)
    );
    b.fail.store(-1, Ordering::SeqCst);
    wait_rows(&pool, 9).await;
    stop.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(a.created.load(Ordering::SeqCst), 3);
    assert_eq!(saga.created.load(Ordering::SeqCst), 3);
    assert!(b.created.load(Ordering::SeqCst) >= 4);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raddle_jobs WHERE destination IN ('a','b','saga') AND status='completed'").fetch_one(&pool).await.unwrap(),9);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE destination IN ('a','saga') AND attempts<>1"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn postcommit_and_panicking_cleanup_do_not_skip_other_groups_or_replay_roots_under_host(
    pool: PgPool,
) {
    schema(&pool).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::new(Member::<1>::new(&log));
    let b = Arc::new(Member::<2>::new(&log));
    a.after_fail.store(true, Ordering::SeqCst);
    b.finally_panic.store(true, Ordering::SeqCst);
    b.dispose_fail.store(true, Ordering::SeqCst);
    let mut builder = builder();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    executions(&mut builder, &mut host);
    let reg = HandlerSet::<Work, _>::new(Then::new(
        Group::new("a", Arc::clone(&a)),
        Group::new("b", Arc::clone(&b)),
    ))
    .custody(Custody::Durable)
    .register(
        &mut builder,
        "roots",
        Store::new(pool.clone()),
        Arc::new(LocalOnlyFlush),
    )
    .unwrap();
    let bus = builder.build();
    attach(reg.owners, &bus, &mut host);
    let stop = host.handle();
    let task = tokio::spawn(host.run());
    bus.send(&Work(1), origin()).await.unwrap();
    bus.send(&Work(2), origin()).await.unwrap();
    wait_rows(&pool, 4).await;
    a.after_fail.store(false, Ordering::SeqCst);
    b.finally_panic.store(false, Ordering::SeqCst);
    b.dispose_fail.store(false, Ordering::SeqCst);
    bus.send(&Work(3), origin()).await.unwrap();
    wait_rows(&pool, 6).await;
    let committed = bus
        .invoke_scoped(reg.global.as_ref().unwrap(), context(4))
        .await
        .unwrap();
    assert!(committed.post_commit.is_ok());
    a.after_fail.store(true, Ordering::SeqCst);
    let committed = bus
        .invoke_scoped(reg.global.as_ref().unwrap(), context(5))
        .await
        .unwrap();
    assert_eq!(committed.post_commit, Err(PostCommitError::Callback));
    a.after_fail.store(false, Ordering::SeqCst);
    b.dispose_fail.store(true, Ordering::SeqCst);
    let committed = bus
        .invoke_scoped(reg.global.as_ref().unwrap(), context(6))
        .await
        .unwrap();
    assert_eq!(committed.post_commit, Err(PostCommitError::Cleanup));
    stop.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raddle_jobs WHERE destination='roots' AND status='completed' AND attempts=1").fetch_one(&pool).await.unwrap(),3);
    let log = log.lock().unwrap();
    assert_eq!(
        log.iter().filter(|(_, phase)| phase == "Dispose").count(),
        12
    );
    assert_eq!(
        log.iter()
            .filter(|(_, phase)| phase == "AfterCommit")
            .count(),
        12
    );
}
