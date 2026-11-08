//! Shared caller cancellation never becomes execution-owned cancellation.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::effects::HandlerOutput;
use raddle_messaging::scoped::{Completion, Failure, HandleFuture, LocalOnlyFlush, ScopedHandler};
use raddle_messaging::{
    BoxFuture, BusBuilder, Cancellation, LocalQueue, MessageContext, QueueMode, QueueSettings,
    TrustedOrigin,
};
use raddle_postgres::{Scope, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::{Semaphore, mpsc};

#[derive(Clone, Copy, Serialize, Deserialize)]
enum Mode {
    Success,
    DomainFailure,
    Hold,
}
#[derive(Serialize, Deserialize)]
struct Work {
    id: u32,
    mode: Mode,
}
impl WireMessage for Work {
    const NAME: &'static str = "test.shared-cancellation-primary";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Sibling(Work);
impl WireMessage for Sibling {
    const NAME: &'static str = "test.shared-cancellation-sibling";
    const VERSION: u32 = 1;
}
trait TestWork: WireMessage {
    fn work(&self) -> &Work;
}
impl TestWork for Work {
    fn work(&self) -> &Work {
        self
    }
}
impl TestWork for Sibling {
    fn work(&self) -> &Work {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("scripted domain failure")]
struct DomainError;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Event {
    Started(u32),
    Finally(u32),
    Disposing(u32),
    Disposed(u32),
}

struct Handler {
    events: mpsc::UnboundedSender<Event>,
    tokens: Mutex<BTreeMap<u32, Cancellation>>,
    release_sibling: Semaphore,
    dispose_primary: Semaphore,
}
impl<M: TestWork> ScopedHandler<M> for Handler {
    type Resources = u32;
    type Reply = u32;
    type Error = DomainError;

    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<u32, DomainError>> {
        Box::pin(async move {
            let id = context.envelope().decode::<M>().unwrap().work().id;
            self.tokens
                .lock()
                .unwrap()
                .insert(id, context.cancellation().clone());
            Ok(id)
        })
    }
    fn handle<'a>(
        &'a self,
        message: &'a M,
        _: &'a mut u32,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> HandleFuture<'a, u32, u32, DomainError> {
        Box::pin(async move {
            let work = message.work();
            sqlx::query("INSERT INTO business(id) VALUES($1)")
                .bind(work.id as i32)
                .execute(scope.connection())
                .await
                .map_err(|_| DomainError)?;
            self.events.send(Event::Started(work.id)).unwrap();
            match work.mode {
                Mode::Success => Ok(HandlerOutput::reply(work.id)),
                Mode::DomainFailure => Err(DomainError),
                Mode::Hold if work.id == 2 => {
                    self.release_sibling.acquire().await.unwrap().forget();
                    Ok(HandlerOutput::reply(work.id))
                }
                Mode::Hold => {
                    context.cancellation().cancelled().await;
                    Err(DomainError)
                }
            }
        })
    }
    fn finally<'a>(
        &'a self,
        _: Completion,
        id: &'a mut u32,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), DomainError>> {
        Box::pin(async move {
            self.events.send(Event::Finally(*id)).unwrap();
            Ok(())
        })
    }
    fn dispose<'a>(
        &'a self,
        id: u32,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), DomainError>> {
        Box::pin(async move {
            self.events.send(Event::Disposing(id)).unwrap();
            if id == 1 {
                self.dispose_primary.acquire().await.unwrap().forget();
            }
            self.events.send(Event::Disposed(id)).unwrap();
            Ok(())
        })
    }
}

#[derive(Clone, Copy)]
enum Case {
    Success,
    DomainFailure,
    Timeout,
    Abandonment,
    ParentCancellation,
}

async fn is_pending<F: Future + Unpin>(future: &mut F) -> bool {
    future::poll_fn(|context| Poll::Ready(Pin::new(&mut *future).poll(context).is_pending())).await
}

async fn observed(
    receiver: &mut mpsc::UnboundedReceiver<Event>,
    seen: &mut Vec<Event>,
    wanted: Event,
) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while !seen.contains(&wanted) {
            seen.push(receiver.recv().await.unwrap());
        }
    })
    .await
    .unwrap();
}

fn context<M: WireMessage>(message: &M, parent: &Cancellation) -> MessageContext {
    MessageContext::new(
        TrustedOrigin::authenticated("tenant", "caller", None).unwrap(),
        Envelope::encode(message).unwrap(),
        Utc::now(),
    )
    .with_cancellation(parent.clone())
}

async fn exercise(pool: PgPool, case: Case) {
    sqlx::query("CREATE TABLE business(id integer PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let (events, mut receiver) = mpsc::unbounded_channel();
    let handler = Arc::new(Handler {
        events,
        tokens: Mutex::new(BTreeMap::new()),
        release_sibling: Semaphore::new(0),
        dispose_primary: Semaphore::new(0),
    });
    let (queue, runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 4,
        parallelism: 2,
    })
    .unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "shared-parent").unwrap();
    let shutdown = host.handle();
    let mut host_task = tokio::spawn(host.run());
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.execution_queue(queue).unwrap();
    builder
        .message_timeout::<Sibling>(Duration::from_secs(3600))
        .unwrap();
    let bus = builder.build();
    let parent = Cancellation::new();
    let mode = match case {
        Case::Success => Mode::Success,
        Case::DomainFailure => Mode::DomainFailure,
        _ => Mode::Hold,
    };
    let first_context = context(&Work { id: 1, mode }, &parent);
    let first_bus = bus.clone();
    let first_handler = Arc::clone(&handler);
    let first_store = Store::new(pool.clone());
    let mut first = Some(tokio::spawn(async move {
        first_bus
            .execute::<Work, _>(
                first_handler,
                first_context,
                first_store,
                Arc::new(LocalOnlyFlush),
            )
            .await
    }));
    let second_context = context(
        &Sibling(Work {
            id: 2,
            mode: Mode::Hold,
        }),
        &parent,
    );
    let sibling_handler = Arc::clone(&handler);
    let sibling_store = Store::new(pool.clone());
    let sibling = tokio::spawn(async move {
        bus.execute::<Sibling, _>(
            sibling_handler,
            second_context,
            sibling_store,
            Arc::new(LocalOnlyFlush),
        )
        .await
    });
    let mut seen = Vec::new();
    observed(&mut receiver, &mut seen, Event::Started(1)).await;
    observed(&mut receiver, &mut seen, Event::Started(2)).await;

    match case {
        Case::Timeout => {
            // Advance only after both real SQL writes and factories completed.
            // Resume real time before awaiting rollback/disposal or querying SQL.
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(61)).await;
            tokio::time::resume();
        }
        Case::Abandonment => {
            let call = first.take().unwrap();
            call.abort();
            assert!(call.await.unwrap_err().is_cancelled());
        }
        Case::ParentCancellation => parent.cancel(),
        _ => {}
    }
    observed(&mut receiver, &mut seen, Event::Disposing(1)).await;
    assert!(seen.contains(&Event::Finally(1)));
    let primary_token = handler.tokens.lock().unwrap()[&1].clone();
    let sibling_token = handler.tokens.lock().unwrap()[&2].clone();
    if matches!(case, Case::ParentCancellation) {
        assert!(sibling_token.is_cancelled());
        assert!(matches!(sibling.await.unwrap(), Err(Failure::Cancelled)));
    } else {
        assert!(!parent.is_cancelled());
        assert!(!sibling_token.is_cancelled());
        assert!(!sibling.is_finished());
        if matches!(case, Case::Timeout | Case::Abandonment) {
            assert!(primary_token.is_cancelled());
        }
        if let Some(call) = &first {
            assert!(!call.is_finished(), "response preceded async disposal");
        }
        // Normal results must not mutate either the caller's signal or their
        // execution child as an incidental consequence of dropping the guard.
        if matches!(case, Case::Success | Case::DomainFailure) {
            handler.dispose_primary.add_permits(1);
            let result = first.take().unwrap().await.unwrap();
            match case {
                Case::Success => assert_eq!(result.unwrap().reply, 1),
                Case::DomainFailure => assert!(matches!(result, Err(Failure::Handler(_)))),
                _ => unreachable!(),
            }
            assert!(!primary_token.is_cancelled());
            assert!(!parent.is_cancelled());
            assert!(!sibling_token.is_cancelled());
        }
        handler.release_sibling.add_permits(1);
        assert_eq!(sibling.await.unwrap().unwrap().reply, 2);
        assert!(!parent.is_cancelled());
    }
    shutdown.shutdown();
    if !matches!(case, Case::Success | Case::DomainFailure) {
        assert!(
            is_pending(&mut host_task).await,
            "host abandoned async disposal"
        );
        handler.dispose_primary.add_permits(1);
    }
    if let Some(call) = first {
        let result = call.await.unwrap();
        if matches!(case, Case::Timeout) {
            assert!(matches!(result, Err(Failure::TimedOut)));
        } else {
            assert!(matches!(result, Err(Failure::Cancelled)));
        }
    }
    host_task.await.unwrap().unwrap();
    observed(&mut receiver, &mut seen, Event::Disposed(1)).await;
    observed(&mut receiver, &mut seen, Event::Disposed(2)).await;
    assert!(seen.contains(&Event::Finally(2)));
    let ids = sqlx::query_scalar::<_, i32>("SELECT id FROM business ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    match case {
        Case::Success => assert_eq!(ids, [1, 2]),
        Case::ParentCancellation => assert!(ids.is_empty()),
        _ => assert_eq!(ids, [2]),
    }
    parent.cancel();
    assert!(primary_token.is_cancelled());
    assert!(sibling_token.is_cancelled());
}

#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn successful_execution_does_not_cancel_shared_parent_or_sibling(pool: PgPool) {
    exercise(pool, Case::Success).await;
}
#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn domain_failure_does_not_cancel_shared_parent_or_sibling(pool: PgPool) {
    exercise(pool, Case::DomainFailure).await;
}
#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn execution_timeout_does_not_cancel_shared_parent_or_sibling(pool: PgPool) {
    exercise(pool, Case::Timeout).await;
}
#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn caller_abandonment_cancels_only_its_child_and_host_joins_cleanup(pool: PgPool) {
    exercise(pool, Case::Abandonment).await;
}
#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn shared_parent_cancellation_still_reaches_both_executions_and_joins_cleanup(pool: PgPool) {
    exercise(pool, Case::ParentCancellation).await;
}
