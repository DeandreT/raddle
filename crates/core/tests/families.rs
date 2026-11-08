//! Declared families execute through the ordinary bounded, typed local runtime.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::cell::Cell;
use std::future::{self, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use raddle::pipeline::Pipeline;
use raddle::{
    Event, EventDelivery, EventFamily, ExecutionContext, ExecutionError, FamilyRegistrationError,
    RegistrationError, Request, RequestFamily, RuntimeBuilder, SubscriptionName,
};

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("rejected {0}")]
struct Rejected(u32);

// The request is not Clone; neither it nor its reply is Sync.
struct Add(Cell<u32>);
struct Multiply(u32);
struct Missing;
impl Request for Add {
    type Reply = Cell<u32>;
    type Error = Rejected;
}
impl Request for Multiply {
    type Reply = Cell<u32>;
    type Error = Rejected;
}
impl Request for Missing {
    type Reply = Cell<u32>;
    type Error = Rejected;
}

enum Arithmetic {
    Add(Add),
    Multiply(Multiply),
}
impl Arithmetic {
    fn value(self) -> u32 {
        match self {
            Self::Add(Add(value)) => value.get() + 1,
            Self::Multiply(Multiply(value)) => value * 2,
        }
    }
}

async fn calculate(message: Arithmetic, _: ExecutionContext) -> Result<Cell<u32>, Rejected> {
    let value = message.value();
    if value == 0 {
        Err(Rejected(value))
    } else {
        Ok(Cell::new(value))
    }
}

struct Behavior {
    calls: AtomicUsize,
    drops: Arc<AtomicUsize>,
}
impl Behavior {
    fn record(&self) -> usize {
        assert_eq!(self.drops.load(Ordering::SeqCst), 0);
        self.calls.fetch_add(1, Ordering::SeqCst) + 1
    }
}
impl Drop for Behavior {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn one_nonclone_behavior_handles_concretes_with_typed_results_and_context() {
    let drops = Arc::new(AtomicUsize::new(0));
    let behavior = Behavior {
        calls: AtomicUsize::new(0),
        drops: Arc::clone(&drops),
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);
    let mut family = RequestFamily::new(move |message: Arithmetic, context| {
        let calls = behavior.record();
        observed.lock().unwrap().push((calls, context));
        calculate(message, context)
    });
    family.member::<Add>(Arithmetic::Add).unwrap();
    family.member::<Multiply>(Arithmetic::Multiply).unwrap();
    let mut builder = RuntimeBuilder::default();
    builder.handle_family(family).unwrap();
    let runtime = builder.build();
    assert!(runtime.has_request::<Add>());
    assert!(runtime.has_request::<Multiply>());
    assert!(!runtime.has_request::<Missing>());
    let context = ExecutionContext::new();
    assert_eq!(
        runtime
            .invoke_with_context(Add(Cell::new(4)), context)
            .await
            .unwrap()
            .get(),
        5
    );
    assert_eq!(runtime.invoke(Multiply(6)).await.unwrap().get(), 12);
    assert!(matches!(
        runtime.invoke(Multiply(0)).await,
        Err(ExecutionError::Handler(Rejected(0)))
    ));
    assert!(matches!(
        runtime.invoke(Missing).await,
        Err(ExecutionError::Unregistered { .. })
    ));
    let entries = seen.lock().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], (1, context));
    assert_eq!(entries[2].0, 3);
    drop(entries);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(runtime);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn request_declaration_duplicate_keeps_the_original_converter() {
    let mut family = RequestFamily::new(calculate);
    family.member::<Add>(Arithmetic::Add).unwrap();
    assert!(matches!(
        family.member::<Add>(|_| Arithmetic::Multiply(Multiply(100))),
        Err(FamilyRegistrationError::DuplicateMember { .. })
    ));
    family.member::<Multiply>(Arithmetic::Multiply).unwrap();
    let mut builder = RuntimeBuilder::default();
    builder.handle_family(family).unwrap();
    assert!(matches!(
        builder.handle(|_: Add| async { Ok(Cell::new(999)) }),
        Err(RegistrationError::DuplicateRequest { .. })
    ));
    assert_eq!(
        builder
            .build()
            .invoke(Add(Cell::new(1)))
            .await
            .unwrap()
            .get(),
        2
    );
}

#[tokio::test]
async fn whole_request_group_conflict_at_either_end_changes_nothing() {
    for conflict_first in [true, false] {
        let mut builder = RuntimeBuilder::default();
        builder
            .handle(|_: Multiply| async { Ok(Cell::new(77)) })
            .unwrap();
        let mut family = RequestFamily::new(calculate);
        if conflict_first {
            family.member::<Multiply>(Arithmetic::Multiply).unwrap();
            family.member::<Add>(Arithmetic::Add).unwrap();
        } else {
            family.member::<Add>(Arithmetic::Add).unwrap();
            family.member::<Multiply>(Arithmetic::Multiply).unwrap();
        }
        assert!(matches!(
            builder.handle_family(family),
            Err(FamilyRegistrationError::Registration(
                RegistrationError::DuplicateRequest { .. }
            ))
        ));
        let runtime = builder.build();
        assert_eq!(runtime.invoke(Multiply(5)).await.unwrap().get(), 77);
        assert!(!runtime.has_request::<Add>());
        assert!(matches!(
            runtime.invoke(Add(Cell::new(1))).await,
            Err(ExecutionError::Unregistered { .. })
        ));
    }
}

#[tokio::test]
async fn overlapping_families_reject_the_entire_second_group() {
    let mut first = RequestFamily::new(calculate);
    first.member::<Multiply>(Arithmetic::Multiply).unwrap();
    let mut second = RequestFamily::new(|_: Arithmetic, _| async { Ok(Cell::new(999)) });
    second.member::<Add>(Arithmetic::Add).unwrap();
    second.member::<Multiply>(Arithmetic::Multiply).unwrap();
    let mut builder = RuntimeBuilder::default();
    builder.handle_family(first).unwrap();
    assert!(builder.handle_family(second).is_err());
    let runtime = builder.build();
    assert_eq!(runtime.invoke(Multiply(3)).await.unwrap().get(), 6);
    assert!(!runtime.has_request::<Add>());
}

#[test]
fn empty_groups_are_rejected_without_reserving_anything() {
    let mut builder = RuntimeBuilder::default();
    assert!(matches!(
        builder.handle_family(RequestFamily::new(calculate)),
        Err(FamilyRegistrationError::Empty)
    ));
    assert!(matches!(
        builder.subscribe_family(EventFamily::new(|_: Changes, _| async {
            Ok::<(), Rejected>(())
        })),
        Err(FamilyRegistrationError::Empty)
    ));
}

struct Payload {
    value: u32,
    drops: Arc<AtomicUsize>,
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Created(Payload);
struct Updated(Payload);
struct Unobserved;
impl Event for Created {
    type Error = Rejected;
}
impl Event for Updated {
    type Error = Rejected;
}
impl Event for Unobserved {
    type Error = Rejected;
}
enum Changes {
    Created(Arc<Created>),
    Updated(Arc<Updated>),
}
impl Changes {
    fn details(&self) -> (u32, usize) {
        match self {
            Self::Created(event) => (event.0.value, Arc::as_ptr(event) as usize),
            Self::Updated(event) => (event.0.value, Arc::as_ptr(event) as usize),
        }
    }
}
fn name(value: &str) -> SubscriptionName {
    value.parse().unwrap()
}

#[tokio::test]
async fn event_variants_share_the_source_and_fail_independently_with_later_work() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&observations);
    let mut healthy = EventFamily::new(move |change: Changes, context| {
        observed
            .lock()
            .unwrap()
            .push(("healthy", change.details(), context));
        async { Ok::<(), Rejected>(()) }
    });
    healthy
        .member::<Created>(name("healthy-created"), Changes::Created)
        .unwrap();
    healthy
        .member::<Updated>(name("healthy-updated"), Changes::Updated)
        .unwrap();
    let observed = Arc::clone(&observations);
    let mut sometimes_failing = EventFamily::new(move |change: Changes, context| {
        let (value, address) = change.details();
        observed
            .lock()
            .unwrap()
            .push(("fallible", (value, address), context));
        async move {
            if value == 0 {
                Err(Rejected(value))
            } else {
                Ok(())
            }
        }
    });
    sometimes_failing
        .member::<Created>(name("fallible-created"), Changes::Created)
        .unwrap();
    sometimes_failing
        .member::<Updated>(name("fallible-updated"), Changes::Updated)
        .unwrap();
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder.subscribe_family(sometimes_failing).unwrap();
    builder.subscribe_family(healthy).unwrap();
    let observed = Arc::clone(&observations);
    builder
        .subscribe_with_context(
            name("ordinary-created"),
            move |event: EventDelivery<Created>, context| {
                observed.lock().unwrap().push((
                    "ordinary",
                    (
                        event.message().0.value,
                        std::ptr::from_ref(event.message()) as usize,
                    ),
                    context,
                ));
                async { Ok(()) }
            },
        )
        .unwrap();
    let runtime = builder.build();
    let drops = Arc::new(AtomicUsize::new(0));
    // Fail at the start, middle and end; corrected work remains independent.
    for value in [0, 1, 0, 2, 0, 3] {
        for updated in [false, true] {
            observations.lock().unwrap().clear();
            let payload = Payload {
                value,
                drops: Arc::clone(&drops),
            };
            let context = ExecutionContext::new();
            let report = if updated {
                runtime
                    .publish_with_context(Updated(payload), context)
                    .await
            } else {
                runtime
                    .publish_with_context(Created(payload), context)
                    .await
            };
            let labels: Vec<_> = report
                .outcomes()
                .map(|outcome| outcome.name().as_str())
                .collect();
            assert_eq!(
                labels,
                if updated {
                    vec!["fallible-updated", "healthy-updated"]
                } else {
                    vec!["fallible-created", "healthy-created", "ordinary-created"]
                }
            );
            let mut outcomes = report.into_iter();
            let fallible = outcomes.next().unwrap().into_result();
            if value == 0 {
                assert!(matches!(
                    fallible,
                    Err(ExecutionError::Handler(Rejected(0)))
                ));
            } else {
                assert!(fallible.is_ok());
            }
            assert!(outcomes.all(|outcome| outcome.result().is_ok()));
            let seen = observations.lock().unwrap();
            assert_eq!(seen.len(), if updated { 2 } else { 3 });
            assert!(seen.iter().all(|(_, details, actual_context)| {
                *details == seen[0].1 && details.0 == value && *actual_context == context
            }));
        }
    }
    assert_eq!(drops.load(Ordering::SeqCst), 12);
    assert!(
        runtime
            .publish(Unobserved)
            .await
            .outcomes()
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn event_declaration_conflicts_preserve_members_and_names() {
    let mut family =
        EventFamily::new(|change: Changes, _| async move { Err(Rejected(change.details().0)) });
    family
        .member::<Created>(name("created"), Changes::Created)
        .unwrap();
    assert!(matches!(
        family.member::<Created>(name("unused"), |_| {
            Changes::Updated(Arc::new(Updated(Payload {
                value: 999,
                drops: Arc::new(AtomicUsize::new(0)),
            })))
        }),
        Err(FamilyRegistrationError::DuplicateMember { .. })
    ));
    assert!(matches!(
        family.member::<Updated>(name("created"), Changes::Updated),
        Err(FamilyRegistrationError::Registration(
            RegistrationError::DuplicateSubscription { .. }
        ))
    ));
    family
        .member::<Updated>(name("updated"), Changes::Updated)
        .unwrap();
    let mut builder = RuntimeBuilder::default();
    builder.subscribe_family(family).unwrap();
    // A rejected member must not reserve its supplied name.
    builder
        .subscribe(name("unused"), |_: EventDelivery<Unobserved>| async {
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        builder.subscribe(name("updated"), |_: EventDelivery<Unobserved>| async {
            Ok(())
        }),
        Err(RegistrationError::DuplicateSubscription { .. })
    ));
    let runtime = builder.build();
    for updated in [false, true] {
        let payload = Payload {
            value: 7,
            drops: Arc::new(AtomicUsize::new(0)),
        };
        let report = if updated {
            runtime.publish(Updated(payload)).await
        } else {
            runtime.publish(Created(payload)).await
        };
        assert_eq!(report.outcomes().len(), 1);
        assert!(matches!(
            report.into_iter().next().unwrap().into_result(),
            Err(ExecutionError::Handler(Rejected(7)))
        ));
    }
}

#[tokio::test]
async fn whole_event_group_name_conflict_changes_neither_subscribers_nor_names() {
    for conflict_first in [true, false] {
        let mut builder = RuntimeBuilder::default();
        builder
            .subscribe(name("taken"), |_: EventDelivery<Updated>| async {
                Err(Rejected(88))
            })
            .unwrap();
        let mut family = EventFamily::new(|_: Changes, _| async { Ok::<(), Rejected>(()) });
        if conflict_first {
            family
                .member::<Updated>(name("taken"), Changes::Updated)
                .unwrap();
            family
                .member::<Created>(name("free"), Changes::Created)
                .unwrap();
        } else {
            family
                .member::<Created>(name("free"), Changes::Created)
                .unwrap();
            family
                .member::<Updated>(name("taken"), Changes::Updated)
                .unwrap();
        }
        assert!(matches!(
            builder.subscribe_family(family),
            Err(FamilyRegistrationError::Registration(
                RegistrationError::DuplicateSubscription { .. }
            ))
        ));
        // All names from the failed group remain free, not only all handlers.
        builder
            .subscribe(name("free"), |_: EventDelivery<Unobserved>| async {
                Ok(())
            })
            .unwrap();
        let runtime = builder.build();
        let payload = || Payload {
            value: 1,
            drops: Arc::new(AtomicUsize::new(0)),
        };
        assert!(
            runtime
                .publish(Created(payload()))
                .await
                .outcomes()
                .next()
                .is_none()
        );
        let report = runtime.publish(Updated(payload())).await;
        assert_eq!(report.outcomes().len(), 1);
        assert!(matches!(
            report.into_iter().next().unwrap().into_result(),
            Err(ExecutionError::Handler(Rejected(88)))
        ));
    }
}

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn drop_cancels_family_work_and_waiters_under_shared_admission() {
    let entered = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let converted = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&entered);
    let current = Arc::clone(&active);
    let mut family = RequestFamily::new(move |message: Arithmetic, _| {
        let value = message.value();
        count.fetch_add(1, Ordering::SeqCst);
        let current = Arc::clone(&current);
        async move {
            current.fetch_add(1, Ordering::SeqCst);
            let _guard = Active(current);
            if value == 1 {
                future::pending::<()>().await;
            }
            Ok(Cell::new(value))
        }
    });
    let conversions = Arc::clone(&converted);
    family
        .member::<Add>(move |message| {
            conversions.fetch_add(1, Ordering::SeqCst);
            Arithmetic::Add(message)
        })
        .unwrap();
    let conversions = Arc::clone(&converted);
    family
        .member::<Multiply>(move |message| {
            conversions.fetch_add(1, Ordering::SeqCst);
            Arithmetic::Multiply(message)
        })
        .unwrap();
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder.handle_family(family).unwrap();
    builder
        .handle_pipeline(Pipeline::new(|_: Missing, _| async { Ok(Cell::new(9)) }))
        .unwrap();
    let runtime = builder.build();
    {
        let mut running = std::pin::pin!(runtime.invoke(Add(Cell::new(0))));
        future::poll_fn(|cx| {
            assert!(running.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(active.load(Ordering::SeqCst), 1);
        {
            let mut waiting = std::pin::pin!(runtime.invoke(Multiply(3)));
            let mut ordinary = std::pin::pin!(runtime.invoke(Missing));
            future::poll_fn(|cx| {
                assert!(waiting.as_mut().poll(cx).is_pending());
                assert!(ordinary.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(entered.load(Ordering::SeqCst), 1);
            assert_eq!(converted.load(Ordering::SeqCst), 1);
        }
    }
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.invoke(Multiply(4)).await.unwrap().get(), 8);
    assert_eq!(runtime.invoke(Missing).await.unwrap().get(), 9);
    runtime.close();
    assert!(matches!(
        runtime.invoke(Add(Cell::new(1))).await,
        Err(ExecutionError::Closed)
    ));
    assert_eq!(entered.load(Ordering::SeqCst), 2);
    assert_eq!(converted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn dropping_family_publication_releases_admission_and_shared_event() {
    let active = Arc::new(AtomicUsize::new(0));
    let current = Arc::clone(&active);
    let mut family = EventFamily::new(move |message: Changes, _| {
        let current = Arc::clone(&current);
        async move {
            current.fetch_add(1, Ordering::SeqCst);
            let _guard = Active(current);
            future::pending::<()>().await;
            let _keep_alive = message;
            Ok::<(), Rejected>(())
        }
    });
    family
        .member::<Created>(name("created"), Changes::Created)
        .unwrap();
    family
        .member::<Updated>(name("updated"), Changes::Updated)
        .unwrap();
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder.subscribe_family(family).unwrap();
    builder
        .handle(|_: Missing| async { Ok(Cell::new(5)) })
        .unwrap();
    let runtime = builder.build();
    let drops = Arc::new(AtomicUsize::new(0));
    {
        let mut publication = std::pin::pin!(runtime.publish(Created(Payload {
            value: 1,
            drops: Arc::clone(&drops)
        })));
        future::poll_fn(|cx| {
            assert!(publication.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.invoke(Missing).await.unwrap().get(), 5);
    runtime.close();
    let report = runtime
        .publish(Updated(Payload {
            value: 2,
            drops: Arc::clone(&drops),
        }))
        .await;
    assert!(matches!(
        report.into_iter().next().unwrap().into_result(),
        Err(ExecutionError::Closed)
    ));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}
