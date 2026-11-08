//! Ordered middleware, cancellation, owned retry, and typed routing regressions.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures_util::future::{self, BoxFuture};
use raddle::pipeline::{Middleware, Next, Pipeline};
use raddle::{
    Event, EventDelivery, ExecutionContext, ExecutionError, Request, RuntimeBuilder,
    SubscriptionName,
};
use tokio::sync::{Mutex, Notify};

#[derive(Debug, thiserror::Error)]
#[error("rejected")]
struct Rejected;

struct Read(u32);
impl Request for Read {
    type Reply = u32;
    type Error = Rejected;
}

struct Observe {
    entry: &'static str,
    exit: &'static str,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl Middleware<Read, u32, Rejected> for Observe {
    fn call<'execution>(
        &'execution self,
        message: Read,
        context: ExecutionContext,
        next: Next<'execution, Read, u32, Rejected>,
    ) -> BoxFuture<'execution, Result<u32, Rejected>> {
        Box::pin(async move {
            self.trace.lock().await.push(self.entry);
            let result = next.run(message, context).await;
            self.trace.lock().await.push(self.exit);
            result.map(|value| value + 1)
        })
    }
}

#[tokio::test]
async fn layers_are_ordered_async_and_preserve_typed_results() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let terminal_trace = Arc::clone(&trace);
    let pipeline = Pipeline::new(move |message: Read, context| {
        let trace = Arc::clone(&terminal_trace);
        async move {
            assert_eq!(context.depth(), 0);
            assert_eq!(context.correlation_id(), context.execution_id());
            trace.lock().await.push("handler");
            Ok(message.0)
        }
    })
    .layer(Observe {
        entry: "a-in",
        exit: "a-out",
        trace: Arc::clone(&trace),
    })
    .layer(Observe {
        entry: "b-in",
        exit: "b-out",
        trace: Arc::clone(&trace),
    });
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder.handle_pipeline(pipeline).unwrap();
    assert_eq!(builder.build().invoke(Read(40)).await.unwrap(), 42);
    assert_eq!(
        *trace.lock().await,
        ["a-in", "b-in", "handler", "b-out", "a-out"]
    );
}

#[tokio::test]
async fn validation_short_circuits_and_outer_layers_observe_failure() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(AtomicUsize::new(0));
    let terminal_called = Arc::clone(&called);
    let pipeline = Pipeline::new(move |message: Read, _| {
        terminal_called.fetch_add(1, Ordering::SeqCst);
        async move { Ok(message.0) }
    })
    .layer(Observe {
        entry: "in",
        exit: "out",
        trace: Arc::clone(&trace),
    })
    .validate(|message: Read, _| async move {
        tokio::task::yield_now().await;
        if message.0 == 0 {
            Err(Rejected)
        } else {
            Ok(Read(message.0 + 1))
        }
    });
    let mut builder = RuntimeBuilder::default();
    builder.handle_pipeline(pipeline).unwrap();
    let runtime = builder.build();
    assert!(matches!(
        runtime.invoke(Read(0)).await,
        Err(ExecutionError::Handler(Rejected))
    ));
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert_eq!(*trace.lock().await, ["in", "out"]);
    assert_eq!(runtime.invoke(Read(40)).await.unwrap(), 42);
}

#[tokio::test]
async fn dropping_inflight_validation_releases_admission() {
    let entered = Arc::new(Notify::new());
    let validation_entered = Arc::clone(&entered);
    let pipeline = Pipeline::new(|message: Read, _| async move { Ok(message.0) }).validate(
        move |message: Read, _| {
            let entered = Arc::clone(&validation_entered);
            async move {
                if message.0 == 0 {
                    entered.notify_one();
                    future::pending::<()>().await;
                }
                Ok(message)
            }
        },
    );
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder.handle_pipeline(pipeline).unwrap();
    let runtime = builder.build();
    let mut first = Box::pin(runtime.invoke(Read(0)));
    tokio::select! {
        _ = entered.notified() => {},
        result = &mut first => panic!("unexpected result: {result:?}"),
    }
    drop(first);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), runtime.invoke(Read(42)))
            .await
            .unwrap()
            .unwrap(),
        42
    );
}

struct Notice;
impl Event for Notice {
    type Error = Rejected;
}

#[tokio::test]
async fn event_validation_is_independent_and_shares_runtime_capacity() {
    let called = Arc::new(AtomicUsize::new(0));
    let success_called = Arc::clone(&called);
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .subscribe_pipeline(
            SubscriptionName::try_new("reject").unwrap(),
            Pipeline::new(|_: EventDelivery<Notice>, _| async { Ok(()) })
                .validate(|_, _| async { Err(Rejected) }),
        )
        .unwrap();
    builder
        .subscribe_pipeline(
            SubscriptionName::try_new("success").unwrap(),
            Pipeline::new(move |_: EventDelivery<Notice>, _| {
                success_called.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }),
        )
        .unwrap();
    let report = builder.build().publish(Notice).await;
    let outcomes: Vec<_> = report.outcomes().collect();
    assert!(matches!(
        outcomes[0].result(),
        Err(ExecutionError::Handler(Rejected))
    ));
    assert!(outcomes[1].result().is_ok());
    assert_eq!(called.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_request_and_event_validation_share_the_same_admission_bound() {
    async fn validate<T>(
        message: T,
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
    ) -> Result<T, Rejected> {
        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
        maximum.fetch_max(current, Ordering::SeqCst);
        tokio::task::yield_now().await;
        active.fetch_sub(1, Ordering::SeqCst);
        Ok(message)
    }
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let request_active = Arc::clone(&active);
    let request_maximum = Arc::clone(&maximum);
    let event_active = Arc::clone(&active);
    let event_maximum = Arc::clone(&maximum);
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .handle_pipeline(
            Pipeline::new(|message: Read, _| async move { Ok(message.0) }).validate(
                move |message, _| {
                    validate(
                        message,
                        Arc::clone(&request_active),
                        Arc::clone(&request_maximum),
                    )
                },
            ),
        )
        .unwrap();
    builder
        .subscribe_pipeline(
            SubscriptionName::try_new("validate-event").unwrap(),
            Pipeline::new(|_: EventDelivery<Notice>, _| async { Ok(()) }).validate(
                move |message, _| {
                    validate(
                        message,
                        Arc::clone(&event_active),
                        Arc::clone(&event_maximum),
                    )
                },
            ),
        )
        .unwrap();
    let runtime = builder.build();
    let (first, event, last) = tokio::join!(
        runtime.invoke(Read(1)),
        runtime.publish(Notice),
        runtime.invoke(Read(2))
    );
    assert_eq!(first.unwrap(), 1);
    assert!(event.is_success());
    assert_eq!(last.unwrap(), 2);
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn middleware_closures_and_non_sync_owned_requests_are_supported() {
    struct Owned(std::cell::Cell<u32>);
    impl Request for Owned {
        type Reply = u32;
        type Error = Infallible;
    }
    let pipeline = Pipeline::new(|message: Owned, _| async move { Ok(message.0.get()) })
        .validate(|message: Owned, _| async move {
            message.0.set(message.0.get() + 1);
            Ok(message)
        })
        .layer_fn(
            |message: Owned, context, next: Next<'_, Owned, u32, Infallible>| {
                next.run(message, context)
            },
        );
    let mut builder = RuntimeBuilder::default();
    builder.handle_pipeline(pipeline).unwrap();
    assert_eq!(
        builder
            .build()
            .invoke(Owned(std::cell::Cell::new(41)))
            .await
            .unwrap(),
        42
    );
}

#[tokio::test]
async fn a_layer_can_short_circuit_with_a_typed_success() {
    let pipeline = Pipeline::new(|_: Read, _| async { panic!("short-circuited handler ran") })
        .layer_fn(|message: Read, _context, _next| Box::pin(async move { Ok(message.0) }));
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder.handle_pipeline(pipeline).unwrap();
    assert_eq!(builder.build().invoke(Read(42)).await.unwrap(), 42);
}

#[tokio::test]
async fn cancellation_of_waiting_pipeline_and_close_do_not_run_validation() {
    let entered = Arc::new(Notify::new());
    let called = Arc::new(AtomicUsize::new(0));
    let validation_called = Arc::clone(&called);
    let handler_entered = Arc::clone(&entered);
    let pipeline = Pipeline::new(move |message: Read, _| {
        let entered = Arc::clone(&handler_entered);
        async move {
            if message.0 == 0 {
                entered.notify_one();
                future::pending::<()>().await;
            }
            Ok(message.0)
        }
    })
    .validate(move |message: Read, _| {
        validation_called.fetch_add(1, Ordering::SeqCst);
        async { Ok(message) }
    });
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder.handle_pipeline(pipeline).unwrap();
    let runtime = builder.build();
    let mut first = Box::pin(runtime.invoke(Read(0)));
    tokio::select! { _ = entered.notified() => {}, _ = &mut first => panic!("pending handler returned") }
    let mut waiting = Box::pin(runtime.invoke(Read(1)));
    assert!(
        future::poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context)))
            .await
            .is_pending()
    );
    drop(waiting);
    runtime.close();
    assert!(matches!(
        runtime.invoke(Read(2)).await,
        Err(ExecutionError::Closed)
    ));
    assert_eq!(called.load(Ordering::SeqCst), 1);
    drop(first);
}

#[tokio::test]
async fn retry_requires_owned_safe_state_and_stops_after_success() {
    use std::num::NonZeroUsize;

    use raddle::retry::{self, Attempt, RetryPolicy, RetrySafety};

    struct Owned {
        stable_key: String,
        pending: usize,
    }
    let called = AtomicUsize::new(0);
    let report = retry::retry_owned(
        Owned {
            stable_key: "order-42".to_owned(),
            pending: 0,
        },
        RetryPolicy::new(NonZeroUsize::new(3).unwrap(), Duration::from_millis(1)),
        |mut state, attempt| {
            called.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(state.stable_key, "order-42");
                state.pending += 1;
                if attempt < 3 {
                    Attempt::Retry {
                        state,
                        error: Rejected,
                        safety: RetrySafety::Idempotent,
                        after: Duration::from_millis(1),
                    }
                } else {
                    Attempt::Complete(state.pending)
                }
            }
        },
    )
    .await;
    assert_eq!(report.attempts(), 3);
    assert_eq!(report.into_result().unwrap(), 3);
    assert_eq!(called.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retry_distinguishes_terminal_failure_exhaustion_and_excessive_delay() {
    use std::num::NonZeroUsize;

    use raddle::retry::{self, Attempt, RetryError, RetryPolicy, RetrySafety};

    let terminal = retry::retry_owned((), RetryPolicy::default(), |(), _| async {
        Attempt::<(), _, ()>::Failed(Rejected)
    })
    .await;
    assert!(matches!(
        terminal.result(),
        Err(RetryError::Failed(Rejected))
    ));
    assert_eq!(terminal.attempts(), 1);
    let exhausted = retry::retry_owned((), RetryPolicy::default(), |(), _| async {
        Attempt::<(), _, _>::Retry {
            state: (),
            error: Rejected,
            safety: RetrySafety::RolledBack,
            after: Duration::ZERO,
        }
    })
    .await;
    assert!(matches!(
        exhausted.result(),
        Err(RetryError::Exhausted(Rejected))
    ));
    assert_eq!(exhausted.attempts(), 1);
    let excessive = retry::retry_owned(
        (),
        RetryPolicy::new(NonZeroUsize::new(2).unwrap(), Duration::ZERO),
        |(), _| async {
            Attempt::<(), _, _>::Retry {
                state: (),
                error: Rejected,
                safety: RetrySafety::Idempotent,
                after: Duration::from_secs(1),
            }
        },
    )
    .await;
    assert!(matches!(
        excessive.result(),
        Err(RetryError::DelayExceeded(Rejected))
    ));
    assert_eq!(excessive.attempts(), 1);
}

#[tokio::test]
async fn cancelling_retry_delay_does_not_start_another_attempt() {
    use std::num::NonZeroUsize;

    use raddle::retry::{self, Attempt, RetryPolicy, RetrySafety};

    let called = AtomicUsize::new(0);
    let mut retry = Box::pin(retry::retry_owned(
        (),
        RetryPolicy::new(NonZeroUsize::new(2).unwrap(), Duration::from_secs(60)),
        |(), _| {
            called.fetch_add(1, Ordering::SeqCst);
            async {
                Attempt::<(), _, _>::Retry {
                    state: (),
                    error: Rejected,
                    safety: RetrySafety::RolledBack,
                    after: Duration::from_secs(60),
                }
            }
        },
    ));
    assert!(
        future::poll_fn(|context| Poll::Ready(retry.as_mut().poll(context)))
            .await
            .is_pending()
    );
    drop(retry);
    assert_eq!(called.load(Ordering::SeqCst), 1);
}

#[derive(serde::Deserialize, serde::Serialize)]
struct WireOne;
impl raddle::wire::WireMessage for WireOne {
    const NAME: &'static str = "orders.accepted";
    const VERSION: u32 = 1;
}
#[derive(serde::Deserialize, serde::Serialize)]
struct WireTwo;
impl raddle::wire::WireMessage for WireTwo {
    const NAME: &'static str = "orders.accepted";
    const VERSION: u32 = 2;
}

#[test]
fn typed_routes_match_stable_contract_and_version_and_only_configured_aliases() {
    use std::num::NonZeroUsize;

    use raddle::routing::{EndpointAlias, RouteLimits, RouteTable, RoutingError, WireMessageType};

    let one = NonZeroUsize::MIN;
    let two = NonZeroUsize::new(2).unwrap();
    let a = EndpointAlias::try_new("orders-primary".to_owned()).unwrap();
    let b = EndpointAlias::try_new("orders-audit".to_owned()).unwrap();
    assert!(EndpointAlias::try_new("https://untrusted.example".to_owned()).is_err());
    let mut routes = RouteTable::new(RouteLimits::new(two, two, two));
    assert_eq!(
        routes.route::<WireOne>(&a),
        Err(RoutingError::UnknownEndpoint)
    );
    routes.endpoint(a.clone()).unwrap();
    routes.endpoint(b.clone()).unwrap();
    assert_eq!(routes.endpoint(a.clone()), Err(RoutingError::Duplicate));
    assert_eq!(
        routes.endpoint(EndpointAlias::try_new("extra".to_owned()).unwrap()),
        Err(RoutingError::Capacity)
    );
    routes.route::<WireOne>(&b).unwrap();
    routes.route::<WireOne>(&a).unwrap();
    assert_eq!(routes.route::<WireOne>(&a), Err(RoutingError::Duplicate));
    assert_eq!(
        routes
            .resolve::<WireOne>()
            .unwrap()
            .map(|alias| alias.as_str())
            .collect::<Vec<_>>(),
        ["orders-audit", "orders-primary"]
    );
    assert!(matches!(
        routes.resolve::<WireTwo>(),
        Err(RoutingError::Unregistered)
    ));
    assert_eq!(routes.route::<WireTwo>(&a), Err(RoutingError::Capacity));
    assert_eq!(
        WireMessageType::of::<WireOne>().unwrap().name(),
        "orders.accepted"
    );
    assert_eq!(WireMessageType::of::<WireOne>().unwrap().version().get(), 1);
    let mut limited = RouteTable::new(RouteLimits::new(two, two, one));
    limited.endpoint(a.clone()).unwrap();
    limited.endpoint(b.clone()).unwrap();
    limited.route::<WireOne>(&a).unwrap();
    assert_eq!(limited.route::<WireOne>(&b), Err(RoutingError::Capacity));
    // A failed fan-out append did not consume the other binding slot.
    limited.route::<WireTwo>(&b).unwrap();
}

#[test]
fn routes_reject_invalid_declared_wire_contracts_without_consuming_capacity() {
    use raddle::routing::{EndpointAlias, RouteTable, RoutingError};

    #[derive(serde::Deserialize, serde::Serialize)]
    struct Invalid;
    impl raddle::wire::WireMessage for Invalid {
        const NAME: &'static str = "arbitrary://uri";
        const VERSION: u32 = 1;
    }
    #[derive(serde::Deserialize, serde::Serialize)]
    struct Zero;
    impl raddle::wire::WireMessage for Zero {
        const NAME: &'static str = "valid.name";
        const VERSION: u32 = 0;
    }
    let a = EndpointAlias::try_new("allowed".to_owned()).unwrap();
    let mut routes = RouteTable::default();
    routes.endpoint(a.clone()).unwrap();
    assert_eq!(
        routes.route::<Invalid>(&a),
        Err(RoutingError::InvalidContract)
    );
    assert_eq!(routes.route::<Zero>(&a), Err(RoutingError::InvalidContract));
    routes.route::<WireOne>(&a).unwrap();
}
