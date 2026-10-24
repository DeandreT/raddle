//! Public API, ownership, independent failure, and cancellation contracts.
// Test failures intentionally panic with contextual assertions.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::cell::Cell;
use std::convert::Infallible;
use std::future::{self, Future};
use std::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use raddle::{
    ConfigurationError, Event, EventDelivery, ExecutionError, RegistrationError, Request,
    RuntimeBuilder, SubscriptionName,
};
use tokio::sync::{Barrier, Notify, Semaphore, mpsc};
use tokio::time;

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
#[error("order {order} was rejected")]
struct Rejected {
    order: u64,
}

struct PlaceOrder(u64);

impl Request for PlaceOrder {
    type Reply = u64;
    type Error = Rejected;
}

struct NonSyncRequest(Cell<u64>);

impl Request for NonSyncRequest {
    type Reply = Cell<u64>;
    type Error = Infallible;
}

// Intentionally not Clone: fan-out must not require copying the payload.
struct OrderPlaced(u64);

impl Event for OrderPlaced {
    type Error = Rejected;
}

struct AnotherEvent;

impl Event for AnotherEvent {
    type Error = Infallible;
}

fn name(value: &str) -> SubscriptionName {
    value.parse().expect("test subscription is valid")
}

async fn accept(order: PlaceOrder) -> Result<u64, Rejected> {
    Ok(order.0)
}

#[tokio::test]
async fn ordinary_function_preserves_typed_reply() {
    let mut builder = RuntimeBuilder::default();
    builder.handle(accept).unwrap();
    let runtime = builder.build();
    assert_eq!(runtime.invoke(PlaceOrder(42)).await.unwrap(), 42);
}

#[tokio::test]
async fn request_and_reply_need_not_be_sync_or_clone() {
    let mut builder = RuntimeBuilder::default();
    builder
        .handle(|request: NonSyncRequest| async move { Ok(request.0) })
        .unwrap();
    let result = builder
        .build()
        .invoke(NonSyncRequest(Cell::new(17)))
        .await
        .unwrap();
    assert_eq!(result.get(), 17);
}

#[tokio::test]
async fn captured_application_state_is_explicit_and_shared() {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = Arc::clone(&calls);
    let mut builder = RuntimeBuilder::default();
    builder
        .handle(move |order: PlaceOrder| {
            state.fetch_add(1, Ordering::SeqCst);
            async move { Ok(order.0) }
        })
        .unwrap();
    let runtime = builder.build();
    runtime.invoke(PlaceOrder(1)).await.unwrap();
    runtime.clone().invoke(PlaceOrder(2)).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn domain_failure_retains_its_concrete_type_and_fields() {
    let mut builder = RuntimeBuilder::default();
    builder
        .handle(|order: PlaceOrder| async move { Err(Rejected { order: order.0 }) })
        .unwrap();
    assert!(matches!(
        builder.build().invoke(PlaceOrder(99)).await,
        Err(ExecutionError::Handler(Rejected { order: 99 }))
    ));
}

#[tokio::test]
async fn missing_request_is_reported_without_dispatch() {
    assert!(matches!(
        RuntimeBuilder::default()
            .build()
            .invoke(PlaceOrder(1))
            .await,
        Err(ExecutionError::Unregistered { .. })
    ));
}

#[tokio::test]
async fn duplicate_request_does_not_replace_the_original() {
    let mut builder = RuntimeBuilder::default();
    builder.handle(accept).unwrap();
    assert!(matches!(
        builder.handle(|_: PlaceOrder| async { Ok(999) }),
        Err(RegistrationError::DuplicateRequest { .. })
    ));
    assert_eq!(builder.build().invoke(PlaceOrder(1)).await.unwrap(), 1);
}

#[test]
fn concurrency_is_validated_without_semaphore_panics() {
    for invalid in [0, usize::MAX, Semaphore::MAX_PERMITS + 1] {
        assert!(matches!(
            RuntimeBuilder::try_new(invalid),
            Err(ConfigurationError::InvalidConcurrency { requested, .. }) if requested == invalid
        ));
    }
    assert!(RuntimeBuilder::try_new(1).is_ok());
    assert!(RuntimeBuilder::try_new(Semaphore::MAX_PERMITS).is_ok());
}

#[tokio::test]
async fn unobserved_event_is_a_successful_empty_publication() {
    let runtime = RuntimeBuilder::default().build();
    runtime.close();
    let report = runtime.publish(OrderPlaced(1)).await;
    assert!(report.is_success());
    assert_eq!(report.outcomes().len(), 0);
}

#[tokio::test]
async fn subscriptions_are_independent_and_failures_do_not_short_circuit() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut builder = RuntimeBuilder::default();
    builder
        .subscribe(
            name("reject"),
            |event: EventDelivery<OrderPlaced>| async move {
                Err(Rejected {
                    order: event.message().0,
                })
            },
        )
        .unwrap();
    builder
        .subscribe(name("accept"), move |event: EventDelivery<OrderPlaced>| {
            observed.store(event.message().0 as usize, Ordering::SeqCst);
            async { Ok(()) }
        })
        .unwrap();
    let report = builder.build().publish(OrderPlaced(45)).await;
    assert!(!report.is_success());
    assert_eq!(calls.load(Ordering::SeqCst), 45);
    let mut outcomes = report.into_iter();
    let failure = outcomes.next().unwrap();
    assert_eq!(failure.name().as_str(), "reject");
    assert!(matches!(
        failure.into_result(),
        Err(ExecutionError::Handler(Rejected { order: 45 }))
    ));
    assert!(outcomes.next().unwrap().result().is_ok());
    assert!(outcomes.next().is_none());
}

#[tokio::test]
async fn subscription_names_are_unique_across_event_types() {
    let mut builder = RuntimeBuilder::default();
    builder
        .subscribe(name("audit"), |_: EventDelivery<OrderPlaced>| async {
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        builder.subscribe(name("audit"), |_: EventDelivery<AnotherEvent>| async {
            Ok(())
        }),
        Err(RegistrationError::DuplicateSubscription { .. })
    ));
    let runtime = builder.build();
    assert_eq!(runtime.publish(OrderPlaced(1)).await.outcomes().len(), 1);
    assert_eq!(runtime.publish(AnotherEvent).await.outcomes().len(), 0);
}

#[tokio::test]
async fn event_subscribers_share_one_payload_and_run_concurrently() {
    let barrier = Arc::new(Barrier::new(3));
    let (sender, mut receiver) = mpsc::channel(2);
    let mut builder = RuntimeBuilder::try_new(2).unwrap();
    for label in ["first", "second"] {
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        builder
            .subscribe(name(label), move |event: EventDelivery<OrderPlaced>| {
                let barrier = Arc::clone(&barrier);
                let sender = sender.clone();
                async move {
                    let address = std::ptr::from_ref(event.message()) as usize;
                    sender.send(address).await.unwrap();
                    barrier.wait().await;
                    Ok(())
                }
            })
            .unwrap();
    }
    let runtime = builder.build();
    let task = tokio::spawn(async move { runtime.publish(OrderPlaced(1)).await });
    time::timeout(Duration::from_secs(5), barrier.wait())
        .await
        .expect("both subscribers must reach the barrier concurrently");
    assert_eq!(receiver.recv().await, receiver.recv().await);
    assert!(task.await.unwrap().is_success());
}

#[tokio::test]
async fn report_order_is_registration_order_not_completion_order() {
    let release_first = Arc::new(Notify::new());
    let signal = Arc::clone(&release_first);
    let mut builder = RuntimeBuilder::try_new(2).unwrap();
    builder
        .subscribe(name("first"), move |_: EventDelivery<OrderPlaced>| {
            let signal = Arc::clone(&signal);
            async move {
                signal.notified().await;
                Ok(())
            }
        })
        .unwrap();
    builder
        .subscribe(name("second"), move |_: EventDelivery<OrderPlaced>| {
            release_first.notify_one();
            async { Ok(()) }
        })
        .unwrap();
    let report = time::timeout(
        Duration::from_secs(5),
        builder.build().publish(OrderPlaced(1)),
    )
    .await
    .expect("the second handler must wake the first");
    let labels: Vec<_> = report
        .outcomes()
        .map(|outcome| outcome.name().as_str())
        .collect();
    assert_eq!(labels, ["first", "second"]);
}

#[tokio::test]
async fn clones_share_one_concurrency_limit() {
    let gate = Arc::new(Semaphore::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (started, mut receiver) = mpsc::channel(8);
    let mut builder = RuntimeBuilder::try_new(2).unwrap();
    {
        let gate = Arc::clone(&gate);
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        builder
            .handle(move |order: PlaceOrder| {
                let gate = Arc::clone(&gate);
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let started = started.clone();
                async move {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    started.send(()).await.unwrap();
                    let _permit = gate.acquire().await.unwrap();
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(order.0)
                }
            })
            .unwrap();
    }
    let runtime = builder.build();
    let mut tasks = Vec::new();
    for id in 0..8 {
        let runtime = runtime.clone();
        tasks.push(tokio::spawn(
            async move { runtime.invoke(PlaceOrder(id)).await },
        ));
    }
    for _ in 0..2 {
        time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    gate.add_permits(8);
    for task in tasks {
        time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn canceling_an_active_request_releases_its_permit() {
    let started = Arc::new(Notify::new());
    let signal = Arc::clone(&started);
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .handle(move |order: PlaceOrder| {
            let signal = Arc::clone(&signal);
            async move {
                if order.0 == 0 {
                    signal.notify_one();
                    future::pending::<()>().await;
                }
                Ok(order.0)
            }
        })
        .unwrap();
    let runtime = builder.build();
    let worker = runtime.clone();
    let task = tokio::spawn(async move { worker.invoke(PlaceOrder(0)).await });
    time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        time::timeout(Duration::from_secs(5), runtime.invoke(PlaceOrder(7)))
            .await
            .unwrap()
            .unwrap(),
        7
    );
}

#[tokio::test]
async fn canceled_waiters_do_not_consume_later_admission() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    {
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        builder
            .handle(move |order: PlaceOrder| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                async move {
                    if order.0 == 0 {
                        entered.notify_one();
                        release.notified().await;
                    }
                    Ok(order.0)
                }
            })
            .unwrap();
    }
    let runtime = builder.build();
    let worker = runtime.clone();
    let active = tokio::spawn(async move { worker.invoke(PlaceOrder(0)).await });
    time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    {
        let mut waiting = pin::pin!(runtime.invoke(PlaceOrder(1)));
        future::poll_fn(|context| {
            assert!(waiting.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        // Dropping this polled future removes the queued semaphore acquisition.
    }
    release.notify_one();
    assert_eq!(active.await.unwrap().unwrap(), 0);
    assert_eq!(
        time::timeout(Duration::from_secs(5), runtime.invoke(PlaceOrder(2)))
            .await
            .unwrap()
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn requests_and_event_subscribers_share_admission() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder.handle(accept).unwrap();
    {
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        builder
            .subscribe(name("blocking"), move |_: EventDelivery<OrderPlaced>| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                }
            })
            .unwrap();
    }
    let runtime = builder.build();
    let publisher = runtime.clone();
    let publication = tokio::spawn(async move { publisher.publish(OrderPlaced(1)).await });
    time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let mut request = pin::pin!(runtime.invoke(PlaceOrder(2)));
    future::poll_fn(|context| {
        assert!(request.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    release.notify_one();
    assert!(publication.await.unwrap().is_success());
    assert_eq!(
        time::timeout(Duration::from_secs(5), request)
            .await
            .unwrap()
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn canceling_publication_releases_all_active_permits() {
    let entered = Arc::new(Barrier::new(3));
    let mut builder = RuntimeBuilder::try_new(2).unwrap();
    builder.handle(accept).unwrap();
    for label in ["first", "second"] {
        let entered = Arc::clone(&entered);
        builder
            .subscribe(name(label), move |_: EventDelivery<OrderPlaced>| {
                let entered = Arc::clone(&entered);
                async move {
                    entered.wait().await;
                    future::pending::<()>().await;
                    Ok(())
                }
            })
            .unwrap();
    }
    let runtime = builder.build();
    let publisher = runtime.clone();
    let task = tokio::spawn(async move { publisher.publish(OrderPlaced(1)).await });
    time::timeout(Duration::from_secs(5), entered.wait())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        time::timeout(Duration::from_secs(5), runtime.invoke(PlaceOrder(8)))
            .await
            .unwrap()
            .is_ok()
    );
}

#[tokio::test]
async fn close_wakes_waiting_requests_without_canceling_active_ones() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    {
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        builder
            .handle(move |order: PlaceOrder| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(order.0)
                }
            })
            .unwrap();
    }
    let runtime = builder.build();
    let worker = runtime.clone();
    let active = tokio::spawn(async move { worker.invoke(PlaceOrder(1)).await });
    time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let mut waiting = pin::pin!(runtime.invoke(PlaceOrder(2)));
    future::poll_fn(|context| {
        assert!(waiting.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    runtime.clone().close();
    assert!(runtime.is_closed());
    assert!(matches!(waiting.await, Err(ExecutionError::Closed)));
    release.notify_one();
    assert_eq!(active.await.unwrap().unwrap(), 1);
}

#[tokio::test]
async fn closed_publication_reports_each_subscriber_without_calling_it() {
    let mut builder = RuntimeBuilder::default();
    builder
        .subscribe(name("never"), |_: EventDelivery<OrderPlaced>| async {
            panic!("closed runtimes must not enter a subscriber");
        })
        .unwrap();
    let runtime = builder.build();
    runtime.close();
    let report = runtime.publish(OrderPlaced(0)).await;
    assert!(matches!(
        report.into_iter().next().unwrap().into_result(),
        Err(ExecutionError::Closed)
    ));
}

#[tokio::test]
async fn panics_propagate_and_release_admission() {
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .handle(|order: PlaceOrder| async move {
            assert_ne!(order.0, 0, "intentional handler panic");
            Ok(order.0)
        })
        .unwrap();
    let runtime = builder.build();
    let worker = runtime.clone();
    let task = tokio::spawn(async move { worker.invoke(PlaceOrder(0)).await });
    assert!(task.await.unwrap_err().is_panic());
    assert_eq!(
        time::timeout(Duration::from_secs(5), runtime.invoke(PlaceOrder(1)))
            .await
            .unwrap()
            .unwrap(),
        1
    );
}

#[test]
fn public_runtime_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<raddle::Runtime>();
    assert_send_sync::<raddle::RuntimeBuilder>();
}
