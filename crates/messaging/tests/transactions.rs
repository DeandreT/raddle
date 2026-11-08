//! Real PostgreSQL transaction/phase tests; each sqlx test gets an isolated database.
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::effects::{Effect, EffectError, Effects, HandlerOutput, Outgoing};
use raddle_messaging::scoped::{
    CommitReceipt, Completion, ConfirmedFlush, Failure, Flush, LocalOnlyFlush, Phase,
    PostCommitError, ScopedHandler,
};
use raddle_messaging::{
    BoxFuture, BusBuilder, DurableEndpoint, LocalQueue, MessageBus, MessageContext, QueueMode,
    QueueSettings, TrustedOrigin,
};
use raddle_postgres::{Delivery, DeliveryKind, JobStatus, RetryPolicy, Scope, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::{Notify, Semaphore};

#[derive(Serialize, Deserialize)]
struct Input(i32);
impl WireMessage for Input {
    const NAME: &'static str = "test.tx-root";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Child(i32);
impl WireMessage for Child {
    const NAME: &'static str = "test.tx-child";
    const VERSION: u32 = 1;
}

#[derive(Debug, thiserror::Error)]
#[error("test phase failed")]
struct TestError;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Success,
    InlineFail,
    CommitFail,
    AfterCommitFail,
    Cancel,
    StageFail,
    Panic,
}

struct Resources {
    identity: u64,
    loaded: Option<i32>,
    backend: i32,
    singleton: Arc<AtomicU64>,
}
struct Scenario {
    pool: PgPool,
    log: Arc<Mutex<Vec<String>>>,
    singleton: Arc<AtomicU64>,
    mode: Mode,
    fail_id: Arc<AtomicI32>,
    entered: Arc<Notify>,
    disposing: Arc<Notify>,
    dispose_gate: Option<Arc<Semaphore>>,
    outgoing: bool,
    routed: bool,
}
impl Scenario {
    fn new(pool: PgPool, mode: Mode, outgoing: bool) -> Self {
        Self {
            pool,
            log: Arc::new(Mutex::new(Vec::new())),
            singleton: Arc::new(AtomicU64::new(0)),
            mode,
            fail_id: Arc::new(AtomicI32::new(-1)),
            entered: Arc::new(Notify::new()),
            disposing: Arc::new(Notify::new()),
            dispose_gate: None,
            outgoing,
            routed: false,
        }
    }
    fn record(&self, phase: &str) {
        self.log.lock().unwrap().push(phase.to_owned());
    }
}

impl ScopedHandler<Input> for Scenario {
    type Resources = Resources;
    type Reply = i32;
    type Error = TestError;

    fn create<'a>(
        &'a self,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<Resources, TestError>> {
        Box::pin(async move {
            self.record("Create");
            assert_eq!(context.origin().tenant(), "tenant-a");
            let backend = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(scope.connection())
                .await
                .map_err(|_| TestError)?;
            let identity = self.singleton.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Resources {
                identity,
                loaded: None,
                backend,
                singleton: Arc::clone(&self.singleton),
            })
        })
    }
    fn phase<'a>(
        &'a self,
        phase: Phase,
        message: &'a Input,
        resources: &'a mut Resources,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TestError>> {
        Box::pin(async move {
            self.record(&format!("{phase:?}"));
            let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(scope.connection())
                .await
                .map_err(|_| TestError)?;
            assert_eq!(backend, resources.backend);
            assert!(Arc::ptr_eq(&resources.singleton, &self.singleton));
            assert!(resources.identity > 0);
            assert_eq!(context.origin().tenant(), "tenant-a");
            assert_eq!(context.origin().source(), "ingress");
            if phase == Phase::Load {
                resources.loaded = Some(message.0 * 2);
            }
            if phase == Phase::Validate {
                assert_eq!(resources.loaded, Some(message.0 * 2));
            }
            Ok(())
        })
    }
    fn handle<'a>(
        &'a self,
        message: &'a Input,
        resources: &'a mut Resources,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<i32, Resources>, TestError>> {
        Box::pin(async move {
            self.record("Handle");
            sqlx::query("INSERT INTO business(id, tenant) VALUES($1,'tenant-a')")
                .bind(message.0)
                .execute(scope.connection())
                .await
                .map_err(|_| TestError)?;
            if self.fail_id.load(Ordering::SeqCst) == message.0 {
                return Err(TestError);
            }
            assert!(self.mode != Mode::Panic, "isolated scoped handler panic");
            if self.mode == Mode::CommitFail {
                sqlx::query("INSERT INTO deferred_refs(id) VALUES(999)")
                    .execute(scope.connection())
                    .await
                    .map_err(|_| TestError)?;
            }
            if self.mode == Mode::Cancel && message.0 == 1 {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            let fail = self.mode == Mode::InlineFail;
            let log = Arc::clone(&self.log);
            let id = message.0;
            let inline = Effect::inline(move |resources: &mut Resources, scope, context| {
                Box::pin(async move {
                    log.lock().unwrap().push("Inline".into());
                    assert_eq!(resources.loaded, Some(id * 2));
                    assert_eq!(context.origin().tenant(), "tenant-a");
                    sqlx::query("INSERT INTO inline_business(id) VALUES($1)")
                        .bind(id)
                        .execute(scope.connection())
                        .await
                        .map_err(raddle_postgres::Error::from)?;
                    if fail {
                        return Err(EffectError::Failed("scripted-inline"));
                    }
                    Ok(())
                })
            });
            let outgoing = if self.routed {
                Some(Effect::Outgoing(
                    context.returned(&Child(id), true).map_err(|_| TestError)?,
                ))
            } else if self.outgoing {
                Some(Effect::Outgoing(Outgoing {
                    envelope: Envelope::encode(&Child(id)).map_err(|_| TestError)?,
                    deliveries: vec![
                        Delivery::new(DeliveryKind::Remote, "remote-child", Utc::now()),
                        Delivery::new(DeliveryKind::Local, "children", Utc::now()),
                    ],
                }))
            } else {
                None
            };
            let invalid = if self.mode == Mode::StageFail {
                Some(Effect::Outgoing(Outgoing {
                    envelope: Envelope::encode(&Child(id + 1000)).map_err(|_| TestError)?,
                    deliveries: vec![Delivery::new(
                        DeliveryKind::Remote,
                        "not a configured alias",
                        Utc::now(),
                    )],
                }))
            } else {
                None
            };
            Ok(HandlerOutput {
                reply: resources.loaded.unwrap(),
                effects: Effects::returned((inline, outgoing, invalid)).map_err(|_| TestError)?,
            })
        })
    }
    fn on_exception<'a>(
        &'a self,
        _: &'a Failure<TestError>,
        _: &'a mut Resources,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.record("OnException");
        })
    }
    fn after_commit<'a>(
        &'a self,
        _: &'a mut Resources,
        receipt: &'a CommitReceipt,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TestError>> {
        Box::pin(async move {
            self.record("AfterCommit");
            assert!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
                    .fetch_one(&self.pool)
                    .await
                    .unwrap()
                    > 0
            );
            for (id, _) in &receipt.remote {
                assert_eq!(
                    Store::new(self.pool.clone())
                        .inspect("tenant-a", *id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    JobStatus::Completed
                );
            }
            if self.mode == Mode::AfterCommitFail {
                Err(TestError)
            } else {
                Ok(())
            }
        })
    }
    fn finally<'a>(
        &'a self,
        completion: Completion,
        _: &'a mut Resources,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TestError>> {
        Box::pin(async move {
            self.record(&format!("Finally:{completion:?}"));
            Ok(())
        })
    }
    fn dispose<'a>(
        &'a self,
        _: Resources,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TestError>> {
        Box::pin(async move {
            self.disposing.notify_one();
            if let Some(gate) = &self.dispose_gate {
                gate.acquire().await.unwrap().forget();
            }
            self.record("Dispose");
            Ok(())
        })
    }
}

struct ConfirmingTransport {
    pool: PgPool,
    log: Arc<Mutex<Vec<String>>>,
    fail: Arc<AtomicBool>,
}
impl Transport for ConfirmingTransport {
    async fn publish(&self, _: &str, envelope: &Envelope) -> Result<(), TransportError> {
        // A separate connection MUST see the business state before any send.
        let child = envelope.decode::<Child>().unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business WHERE id=$1")
                .bind(child.0)
                .fetch_one(&self.pool)
                .await
                .unwrap(),
            1
        );
        self.log.lock().unwrap().push("Flush".into());
        if self.fail.load(Ordering::SeqCst) {
            Err(TransportError::Transient)
        } else {
            Ok(())
        }
    }
}

struct ChildHandler(Arc<AtomicI32>);
impl ScopedHandler<Child> for ChildHandler {
    type Resources = ();
    type Reply = ();
    type Error = TestError;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TestError>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        message: &'a Child,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<(), ()>, TestError>> {
        Box::pin(async move {
            sqlx::query("INSERT INTO child_business(id) VALUES($1)")
                .bind(message.0)
                .execute(scope.connection())
                .await
                .map_err(|_| TestError)?;
            if self.0.load(Ordering::SeqCst) == message.0 {
                return Err(TestError);
            }
            Ok(HandlerOutput::reply(()))
        })
    }
}

async fn schema(pool: &PgPool) {
    sqlx::raw_sql("CREATE TABLE business(id integer PRIMARY KEY,tenant text NOT NULL); CREATE TABLE inline_business(id integer PRIMARY KEY); CREATE TABLE deferred_refs(id integer REFERENCES business(id) DEFERRABLE INITIALLY DEFERRED); CREATE TABLE child_business(id integer PRIMARY KEY)")
        .execute(pool).await.unwrap();
}
fn context(id: i32) -> MessageContext {
    MessageContext::new(
        TrustedOrigin::authenticated("tenant-a", "ingress", Some("alice".into())).unwrap(),
        Envelope::encode(&Input(id)).unwrap(),
        Utc::now(),
    )
}
fn bus() -> (MessageBus, Host) {
    let (queue, runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 8,
        parallelism: 2,
    })
    .unwrap();
    let mut host = Host::new(Duration::from_secs(10)).unwrap();
    runner.attach(&mut host, "executions").unwrap();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.execution_queue(queue).unwrap();
    (builder.build(), host)
}
fn flush(handler: &Scenario, failing: Arc<AtomicBool>) -> Arc<dyn Flush> {
    Arc::new(ConfirmedFlush::new(
        Store::new(handler.pool.clone()),
        ConfirmingTransport {
            pool: handler.pool.clone(),
            log: Arc::clone(&handler.log),
            fail: failing,
        },
        RetryPolicy::try_new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_secs(5),
        )
        .unwrap(),
    ))
}
async fn counts(pool: &PgPool) -> (i64, i64, i64) {
    (
        sqlx::query_scalar("SELECT count(*) FROM business")
            .fetch_one(pool)
            .await
            .unwrap(),
        sqlx::query_scalar("SELECT count(*) FROM inline_business")
            .fetch_one(pool)
            .await
            .unwrap(),
        sqlx::query_scalar("SELECT count(*) FROM raddle_jobs")
            .fetch_one(pool)
            .await
            .unwrap(),
    )
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn successful_commit_flush_aftercommit_finally_disposal_are_in_order(pool: PgPool) {
    schema(&pool).await;
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::Success, true));
    let confirmed = flush(&handler, Arc::new(AtomicBool::new(false)));
    let (bus, host) = bus();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    let committed = bus
        .execute::<Input, _>(
            Arc::clone(&handler),
            context(1),
            Store::new(pool.clone()),
            confirmed,
        )
        .await
        .unwrap();
    assert_eq!(committed.reply, 2);
    assert_eq!(committed.post_commit, Ok(()));
    assert_eq!(counts(&pool).await, (1, 1, 2));
    assert_eq!(
        *handler.log.lock().unwrap(),
        [
            "Create",
            "Before",
            "Load",
            "Validate",
            "Handle",
            "Inline",
            "After",
            "PostProcess",
            "Flush",
            "AfterCommit",
            "Finally:Committed",
            "Dispose"
        ]
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn one_registered_scoped_handler_is_used_by_routed_send_and_typed_invoke(pool: PgPool) {
    schema(&pool).await;
    let mut handler = Scenario::new(pool.clone(), Mode::Success, false);
    handler.routed = true;
    let handler = Arc::new(handler);
    let (queue, runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 8,
        parallelism: 1,
    })
    .unwrap();
    let (invocations, invocation_runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 8,
        parallelism: 1,
    })
    .unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "routed").unwrap();
    invocation_runner.attach(&mut host, "invocations").unwrap();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("routed", queue).unwrap();
    builder.route::<Input>("routed").unwrap();
    builder
        .durable("children", Store::new(pool.clone()))
        .unwrap();
    builder.route::<Child>("children").unwrap();
    builder.execution_queue(invocations).unwrap();
    let registration = builder
        .scoped::<Input, _>(
            "routed",
            Arc::clone(&handler),
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    let bus = builder.build();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    bus.send(
        &Input(1),
        TrustedOrigin::authenticated("tenant-a", "ingress", None).unwrap(),
    )
    .await
    .unwrap();
    let response = bus.invoke_scoped(&registration, context(2)).await.unwrap();
    assert_eq!(response.reply, 4);
    assert!(response.post_commit.is_ok());
    shutdown.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(counts(&pool).await, (2, 2, 2));
    assert_eq!(handler.singleton.load(Ordering::SeqCst), 2);
    assert_eq!(
        handler
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|phase| phase.as_str() == "Dispose")
            .count(),
        2
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn inline_failure_rolls_back_business_and_outgoing_and_disposes(pool: PgPool) {
    schema(&pool).await;
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::InlineFail, true));
    let (bus, host) = bus();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    assert!(matches!(
        bus.execute::<Input, _>(
            Arc::clone(&handler),
            context(1),
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush)
        )
        .await,
        Err(Failure::Effect(_))
    ));
    assert_eq!(counts(&pool).await, (0, 0, 0));
    assert_eq!(
        *handler.log.lock().unwrap(),
        [
            "Create",
            "Before",
            "Load",
            "Validate",
            "Handle",
            "Inline",
            "OnException",
            "Finally:Failed",
            "Dispose"
        ]
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn real_deferred_commit_failure_never_flushes_or_calls_aftercommit(pool: PgPool) {
    schema(&pool).await;
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::CommitFail, true));
    let confirmed = flush(&handler, Arc::new(AtomicBool::new(false)));
    let (bus, host) = bus();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    assert!(matches!(
        bus.execute::<Input, _>(
            Arc::clone(&handler),
            context(1),
            Store::new(pool.clone()),
            confirmed
        )
        .await,
        Err(Failure::Database(_))
    ));
    assert_eq!(counts(&pool).await, (0, 0, 0));
    assert_eq!(
        *handler.log.lock().unwrap(),
        [
            "Create",
            "Before",
            "Load",
            "Validate",
            "Handle",
            "Inline",
            "After",
            "PostProcess",
            "OnException",
            "Finally:Failed",
            "Dispose"
        ]
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn failure_staging_second_intent_rolls_back_first_intent_and_business(pool: PgPool) {
    schema(&pool).await;
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::StageFail, true));
    let (bus, host) = bus();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    assert!(matches!(
        bus.execute::<Input, _>(
            Arc::clone(&handler),
            context(1),
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush)
        )
        .await,
        Err(Failure::Database(_))
    ));
    assert_eq!(counts(&pool).await, (0, 0, 0));
    assert!(
        !handler
            .log
            .lock()
            .unwrap()
            .iter()
            .any(|phase| phase == "AfterCommit")
    );
    assert_eq!(
        handler.log.lock().unwrap().last().map(String::as_str),
        Some("Dispose")
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn handler_panic_rolls_back_and_still_runs_finally_and_async_disposal(pool: PgPool) {
    schema(&pool).await;
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::Panic, true));
    let (bus, host) = bus();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    assert!(matches!(
        bus.execute::<Input, _>(
            Arc::clone(&handler),
            context(1),
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush)
        )
        .await,
        Err(Failure::Panicked)
    ));
    assert_eq!(counts(&pool).await, (0, 0, 0));
    assert_eq!(
        handler.log.lock().unwrap().last().map(String::as_str),
        Some("Dispose")
    );
    let healthy = Arc::new(Scenario::new(pool.clone(), Mode::Success, false));
    assert!(
        bus.execute::<Input, _>(
            healthy,
            context(2),
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush)
        )
        .await
        .unwrap()
        .post_commit
        .is_ok()
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn flush_failure_is_committed_not_root_retry_and_callback_failure_also_keeps_commit(
    pool: PgPool,
) {
    schema(&pool).await;
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::Success, true));
    let confirmed = flush(&handler, Arc::new(AtomicBool::new(true)));
    let (bus, host) = bus();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    let committed = bus
        .execute::<Input, _>(
            Arc::clone(&handler),
            context(1),
            Store::new(pool.clone()),
            confirmed,
        )
        .await
        .unwrap();
    assert_eq!(committed.post_commit, Err(PostCommitError::Flush));
    assert_eq!(counts(&pool).await, (1, 1, 2));
    assert!(
        !handler
            .log
            .lock()
            .unwrap()
            .iter()
            .any(|s| s == "AfterCommit")
    );
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::AfterCommitFail, false));
    let committed = bus
        .execute::<Input, _>(
            Arc::clone(&handler),
            context(2),
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush),
        )
        .await
        .unwrap();
    assert_eq!(committed.post_commit, Err(PostCommitError::Callback));
    assert_eq!(counts(&pool).await, (2, 2, 2));
    assert_eq!(
        handler.log.lock().unwrap().last().map(String::as_str),
        Some("Dispose")
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn dropped_caller_cancels_root_but_host_joins_async_disposal(pool: PgPool) {
    schema(&pool).await;
    let gate = Arc::new(Semaphore::new(0));
    let mut handler = Scenario::new(pool.clone(), Mode::Cancel, false);
    handler.dispose_gate = Some(Arc::clone(&gate));
    let handler = Arc::new(handler);
    let (bus, host) = bus();
    let shutdown = host.handle();
    let mut host_task = tokio::spawn(host.run());
    let call_handler = Arc::clone(&handler);
    let store = Store::new(pool.clone());
    let call = tokio::spawn(async move {
        bus.execute::<Input, _>(call_handler, context(1), store, Arc::new(LocalOnlyFlush))
            .await
    });
    handler.entered.notified().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    handler.disposing.notified().await;
    assert_eq!(counts(&pool).await, (0, 0, 0));
    shutdown.shutdown();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut host_task)
            .await
            .is_err()
    );
    gate.add_permits(1);
    host_task.await.unwrap().unwrap();
    assert_eq!(
        *handler.log.lock().unwrap(),
        [
            "Create",
            "Before",
            "Load",
            "Validate",
            "Handle",
            "OnException",
            "Finally:Failed",
            "Dispose"
        ]
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancellation_of_an_admitted_waiting_job_is_not_success_or_handler_failure(pool: PgPool) {
    schema(&pool).await;
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::Success, false));
    let (queue, runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 2,
        parallelism: 1,
    })
    .unwrap();
    queue.pause().unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "waiting").unwrap();
    let shutdown = host.handle();
    let host_task = tokio::spawn(host.run());
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.execution_queue(queue.clone()).unwrap();
    let bus = builder.build();
    let context = context(1);
    let cancellation = context.cancellation().clone();
    let call = bus.execute::<Input, _>(
        Arc::clone(&handler),
        context,
        Store::new(pool.clone()),
        Arc::new(LocalOnlyFlush),
    );
    tokio::pin!(call);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut call)
            .await
            .is_err()
    );
    assert_eq!(queue.snapshot().buffered, 1);
    cancellation.cancel();
    queue.resume();
    assert!(matches!(call.await, Err(Failure::Cancelled)));
    shutdown.shutdown();
    host_task.await.unwrap().unwrap();
    assert_eq!(counts(&pool).await, (0, 0, 0));
    assert!(handler.log.lock().unwrap().is_empty());
    assert_eq!(queue.snapshot().cancelled, 1);
    assert_eq!(queue.snapshot().completed, 0);
    assert_eq!(queue.snapshot().failed, 0);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn a_durable_message_timeout_does_not_cancel_a_healthy_sibling(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    for id in [1, 2] {
        let mut scope = store.begin().await.unwrap();
        scope
            .stage(
                "ingress",
                "tenant-a",
                &Envelope::encode(&Input(id)).unwrap(),
                &[Delivery::new(DeliveryKind::Local, "isolated", Utc::now())],
            )
            .await
            .unwrap();
        scope.commit(Utc::now()).await.unwrap();
    }
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::Cancel, false));
    let mut endpoint = DurableEndpoint::new(store, "isolated", 2, RetryPolicy::default()).unwrap();
    endpoint.timeout(Duration::from_secs(2)).unwrap();
    endpoint
        .handle::<Input, _>(Arc::clone(&handler), Arc::new(LocalOnlyFlush))
        .unwrap();
    assert_eq!(endpoint.run_once().await.unwrap(), 2);
    assert_eq!(
        sqlx::query_as::<_, (i32, String)>("SELECT id,tenant FROM business")
            .fetch_all(&pool)
            .await
            .unwrap(),
        [(2, "tenant-a".into())]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE status='ready'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE status='completed'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        handler
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|phase| phase.as_str() == "Dispose")
            .count(),
        2
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn durable_message_override_can_exceed_global_budget_without_shortening_its_lease(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let mut scope = store.begin().await.unwrap();
    scope
        .stage(
            "ingress",
            "tenant-a",
            &Envelope::encode(&Input(2)).unwrap(),
            &[Delivery::new(DeliveryKind::Local, "override", Utc::now())],
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut endpoint = DurableEndpoint::new(store, "override", 1, RetryPolicy::default()).unwrap();
    endpoint.timeout(Duration::from_millis(1)).unwrap();
    endpoint
        .handle_with_policy::<Input, _, _>(
            Arc::new(Scenario::new(pool.clone(), Mode::Success, false)),
            Arc::new(LocalOnlyFlush),
            Duration::from_secs(2),
            |_| raddle_postgres::FailureDisposition::Retry,
        )
        .unwrap();
    assert_eq!(endpoint.run_once().await.unwrap(), 1);
    assert_eq!(counts(&pool).await, (1, 1, 1));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "completed"
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn first_middle_last_failure_and_corrected_retry_only_replays_failed_inbox(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let policy = RetryPolicy::try_new(
        Duration::from_millis(1),
        Duration::from_millis(2),
        Duration::from_secs(5),
    )
    .unwrap();
    let handler = Arc::new(Scenario::new(pool.clone(), Mode::Success, true));
    let mut endpoint = DurableEndpoint::new(store.clone(), "roots", 3, policy).unwrap();
    // Deliberately fail remote sends: this must not retry any completed parent.
    endpoint
        .handle::<Input, _>(
            Arc::clone(&handler),
            flush(&handler, Arc::new(AtomicBool::new(true))),
        )
        .unwrap();
    let pause = endpoint.control();
    pause.pause();
    pause.pause();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.durable("roots", store.clone()).unwrap();
    builder.route::<Input>("roots").unwrap();
    let bus = builder.build();
    for position in 0..3 {
        let first = position * 3 + 1;
        let failed = first + position;
        handler.fail_id.store(failed, Ordering::SeqCst);
        for id in first..first + 3 {
            bus.send(
                &Input(id),
                TrustedOrigin::authenticated("tenant-a", "ingress", None).unwrap(),
            )
            .await
            .unwrap();
        }
        if position == 0 {
            assert_eq!(endpoint.run_once().await.unwrap(), 0);
            pause.resume();
            pause.resume();
        }
        assert_eq!(endpoint.run_once().await.unwrap(), 3);
        let persisted = counts(&pool).await;
        assert_eq!(persisted.0, i64::from(position * 3 + 2));
        assert_eq!(persisted.1, persisted.0);
        let failed_job: (uuid::Uuid, i32) = sqlx::query_as(
            "SELECT id,attempts FROM raddle_jobs WHERE destination='roots' AND status='ready'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(failed_job.1, 1);
        handler.fail_id.store(-1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(endpoint.run_once().await.unwrap(), 1);
        assert_eq!(counts(&pool).await.0, i64::from((position + 1) * 3));
        assert_eq!(
            store
                .inspect("tenant-a", failed_job.0)
                .await
                .unwrap()
                .unwrap()
                .attempts,
            2
        );
        assert_eq!(endpoint.run_once().await.unwrap(), 0);
    }
    let roots: Vec<(String, i32)> =
        sqlx::query_as("SELECT status,attempts FROM raddle_jobs WHERE destination='roots'")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(roots.iter().all(|(status, _)| status == "completed"));
    assert_eq!(
        roots.iter().filter(|(_, attempts)| *attempts == 2).count(),
        3
    );
    assert_eq!(handler.singleton.load(Ordering::SeqCst), 12); // nine successes plus exactly three failed attempts
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE destination='children' AND status='ready'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        9
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE destination='remote-child' AND status <> 'completed'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        9
    );
    // A new receiving owner recovers only persisted child intent. Child failure
    // does not replay its committed parent or already successful siblings.
    let failed_child = Arc::new(AtomicI32::new(1));
    let mut children = DurableEndpoint::new(store.clone(), "children", 9, policy).unwrap();
    children
        .handle::<Child, _>(
            Arc::new(ChildHandler(Arc::clone(&failed_child))),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    assert_eq!(children.run_once().await.unwrap(), 9);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM child_business")
            .fetch_one(&pool)
            .await
            .unwrap(),
        8
    );
    failed_child.store(-1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(children.run_once().await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM child_business")
            .fetch_one(&pool)
            .await
            .unwrap(),
        9
    );
    assert_eq!(handler.singleton.load(Ordering::SeqCst), 12);
    assert_eq!(endpoint.run_once().await.unwrap(), 0);
}
