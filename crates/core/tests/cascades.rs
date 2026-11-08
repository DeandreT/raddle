//! Bounded cascade execution, effect isolation, and caller-owned cancellation.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::future;
use raddle::cascade::{
    Cascade, CascadeLimit, CascadeLimits, Effect, EffectFailure, EffectResult, Effects,
};
use raddle::{Event, ExecutionError, Request, RuntimeBuilder, SubscriptionName};
use tokio::sync::{Mutex, Notify};

#[derive(Debug, thiserror::Error)]
#[error("bad order {0}")]
struct BadOrder(usize);

struct Work {
    id: usize,
    fail: bool,
}
impl Request for Work {
    type Reply = usize;
    type Error = BadOrder;
}

fn effects(work: impl IntoIterator<Item = Work>) -> Effects {
    let mut batch = Effects::default();
    for message in work {
        batch.push(Effect::command(message)).unwrap();
    }
    batch
}

#[tokio::test]
async fn failures_at_start_middle_end_do_not_block_or_repeat_successful_effects() {
    for failed in 0..3 {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler_seen = Arc::clone(&seen);
        let mut builder = RuntimeBuilder::try_new(1).unwrap();
        builder
            .handle(move |message: Work| {
                let seen = Arc::clone(&handler_seen);
                async move {
                    seen.lock().await.push(message.id);
                    if message.fail {
                        Err(BadOrder(message.id))
                    } else {
                        Ok(message.id)
                    }
                }
            })
            .unwrap();
        let runtime = builder.build();
        let report = runtime
            .cascade(
                effects((0..3).map(|id| Work {
                    id,
                    fail: id == failed,
                })),
                CascadeLimits::default(),
            )
            .await;
        let results: Vec<_> = report.outcomes().collect();
        assert_eq!(results.len(), 3);
        assert!(!report.is_success());
        for (index, outcome) in results.iter().enumerate() {
            match outcome.result() {
                EffectResult::Succeeded(value) => {
                    assert_eq!(value.downcast_ref::<usize>(), Some(&index))
                }
                EffectResult::Failed(EffectFailure::Command(error)) => assert!(
                    matches!(error.downcast_ref::<ExecutionError<BadOrder>>(), Some(ExecutionError::Handler(BadOrder(id))) if *id == failed)
                ),
                other => panic!("unexpected outcome: {other:?}"),
            }
        }
        // Recovery is explicitly just the corrected failed order, not a replay
        // of the original batch. Successful order state is never resubmitted.
        assert!(
            runtime
                .cascade(
                    effects([Work {
                        id: failed,
                        fail: false
                    }]),
                    CascadeLimits::default()
                )
                .await
                .is_success()
        );
        assert_eq!(*seen.lock().await, [0, 1, 2, failed]);
    }
}

struct Tree(usize);
impl Request for Tree {
    type Reply = Cascade<usize>;
    type Error = BadOrder;
}

#[tokio::test]
async fn ordinary_invoke_returns_cascading_intent_without_implicitly_running_it() {
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .handle(|_: Tree| async {
            Ok(Cascade::new(
                42,
                effects([Work {
                    id: 42,
                    fail: false,
                }]),
            ))
        })
        .unwrap();
    // Work deliberately has no handler: ordinary invoke must not dispatch it.
    let (reply, pending) = builder.build().invoke(Tree(1)).await.unwrap().into_parts();
    assert_eq!(reply, 42);
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn cascade_releases_parent_permit_and_runs_breadth_first_with_context() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler_seen = Arc::clone(&seen);
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .handle_with_context(move |message: Tree, context| {
            let seen = Arc::clone(&handler_seen);
            async move {
                seen.lock().await.push((message.0, context));
                let mut batch = Effects::default();
                if message.0 < 3 {
                    batch.push(Effect::cascading(Tree(message.0 * 2))).unwrap();
                    batch
                        .push(Effect::cascading(Tree(message.0 * 2 + 1)))
                        .unwrap();
                }
                Ok(Cascade::new(message.0, batch))
            }
        })
        .unwrap();
    let runtime = builder.build();
    let report = tokio::time::timeout(
        Duration::from_secs(2),
        runtime.invoke_cascade(Tree(1), CascadeLimits::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(report.is_success());
    assert_eq!(*report.reply(), 1);
    let seen = seen.lock().await;
    assert_eq!(
        seen.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    let root = seen[0].1;
    for (_, context) in seen.iter().skip(1) {
        assert_eq!(context.correlation_id(), root.correlation_id());
        assert_ne!(context.execution_id(), root.execution_id());
        assert!(context.parent_id().is_some());
    }
    assert_eq!(seen[1].1.parent_id(), Some(root.execution_id()));
    assert_eq!(seen[3].1.parent_id(), Some(seen[1].1.execution_id()));
    let outcomes: Vec<_> = report.outcomes().collect();
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome.parent())
            .collect::<Vec<_>>(),
        [None, None, Some(0), Some(0)]
    );
}

#[tokio::test]
async fn cyclic_results_terminate_at_both_depth_and_total_effect_limits() {
    for depth_limited in [true, false] {
        let called = Arc::new(AtomicUsize::new(0));
        let handler_called = Arc::clone(&called);
        let mut builder = RuntimeBuilder::try_new(1).unwrap();
        builder
            .handle(move |_: Tree| {
                handler_called.fetch_add(1, Ordering::SeqCst);
                async {
                    let mut batch = Effects::default();
                    batch.push(Effect::cascading(Tree(0))).unwrap();
                    Ok(Cascade::new(0, batch))
                }
            })
            .unwrap();
        let two = NonZeroUsize::new(2).unwrap();
        let many = NonZeroUsize::new(10).unwrap();
        let limits = if depth_limited {
            CascadeLimits::new(many, two)
        } else {
            CascadeLimits::new(two, many)
        };
        let report = builder
            .build()
            .invoke_cascade(Tree(0), limits)
            .await
            .unwrap();
        assert_eq!(called.load(Ordering::SeqCst), 3); // root plus two admitted children
        let outcomes: Vec<_> = report.outcomes().collect();
        assert_eq!(outcomes.len(), 3);
        let expected = if depth_limited {
            CascadeLimit::Depth
        } else {
            CascadeLimit::Effects
        };
        assert!(
            matches!(outcomes[2].result(), EffectResult::Rejected { reason, .. } if *reason == expected)
        );
    }
}

struct Notice;
impl Event for Notice {
    type Error = BadOrder;
}

#[tokio::test]
async fn partial_event_failure_keeps_all_subscriber_outcomes_without_retry_or_continuation() {
    let called = Arc::new(AtomicUsize::new(0));
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    for index in 0..3 {
        let called = Arc::clone(&called);
        builder
            .subscribe::<Notice, _, _>(
                SubscriptionName::try_new(format!("subscriber-{index}")).unwrap(),
                move |_| {
                    called.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if index == 1 {
                            Err(BadOrder(index))
                        } else {
                            Ok(())
                        }
                    }
                },
            )
            .unwrap();
    }
    let mut batch = Effects::default();
    batch
        .push(Effect::event_then(Notice, |_| {
            panic!("failed event must not continue")
        }))
        .unwrap();
    let report = builder
        .build()
        .cascade(batch, CascadeLimits::default())
        .await;
    match report.outcomes().next().unwrap().result() {
        EffectResult::Failed(EffectFailure::Subscribers(value)) => {
            let subscribers = value
                .downcast_ref::<raddle::PublishReport<BadOrder>>()
                .unwrap();
            assert_eq!(
                subscribers
                    .outcomes()
                    .map(|outcome| outcome.result().is_ok())
                    .collect::<Vec<_>>(),
                [true, false, true]
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
    assert_eq!(called.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn successful_event_continuation_runs_commands_without_parent_admission() {
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .subscribe::<Notice, _, _>(SubscriptionName::try_new("one").unwrap(), |_| async {
            Ok(())
        })
        .unwrap();
    builder
        .handle(|work: Work| async move { Ok(work.id) })
        .unwrap();
    let mut batch = Effects::default();
    batch
        .push(Effect::event_then(Notice, |report| {
            assert!(report.is_success());
            effects([Work {
                id: 42,
                fail: false,
            }])
        }))
        .unwrap();
    let report = tokio::time::timeout(
        Duration::from_secs(2),
        builder.build().cascade(batch, CascadeLimits::default()),
    )
    .await
    .unwrap();
    assert!(report.is_success());
    assert_eq!(report.outcomes().len(), 2);
}

#[tokio::test]
async fn cancelled_cascade_drops_queued_effects_and_releases_current_permit() {
    let entered = Arc::new(Notify::new());
    let called = Arc::new(AtomicUsize::new(0));
    let handler_entered = Arc::clone(&entered);
    let handler_called = Arc::clone(&called);
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .handle(move |message: Work| {
            let entered = Arc::clone(&handler_entered);
            handler_called.fetch_add(1, Ordering::SeqCst);
            async move {
                if message.id == 0 {
                    entered.notify_one();
                    future::pending::<()>().await;
                }
                Ok(message.id)
            }
        })
        .unwrap();
    let runtime = builder.build();
    let mut run = Box::pin(runtime.cascade(
        effects([Work { id: 0, fail: false }, Work { id: 1, fail: false }]),
        CascadeLimits::default(),
    ));
    tokio::select! { _ = entered.notified() => {}, _ = &mut run => panic!("pending cascade returned") }
    drop(run);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(2),
            runtime.invoke(Work { id: 2, fail: false })
        )
        .await
        .unwrap()
        .unwrap(),
        2
    );
    assert_eq!(called.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn root_failure_and_unregistered_effect_are_isolated_and_batches_are_bounded() {
    let mut builder = RuntimeBuilder::default();
    builder
        .handle(|_: Tree| async { Err(BadOrder(99)) })
        .unwrap();
    let runtime = builder.build();
    assert!(matches!(
        runtime
            .invoke_cascade(Tree(0), CascadeLimits::default())
            .await,
        Err(ExecutionError::Handler(BadOrder(99)))
    ));
    let mut batch = Effects::new(NonZeroUsize::MIN);
    batch
        .push(Effect::command(Work { id: 1, fail: false }))
        .unwrap();
    let rejected = batch
        .push(Effect::command(Work { id: 2, fail: false }))
        .unwrap_err();
    assert!(!batch.is_empty());
    assert_eq!(batch.len(), 1);
    drop(rejected); // returned ownership; never dispatched
    let report = runtime.cascade(batch, CascadeLimits::default()).await;
    assert!(
        matches!(report.outcomes().next().unwrap().result(), EffectResult::Failed(EffectFailure::Command(error)) if matches!(error.downcast_ref::<ExecutionError<BadOrder>>(), Some(ExecutionError::Unregistered { .. })))
    );
}

#[tokio::test]
async fn rejected_effects_retain_ownership_and_can_be_recovered_without_replaying_success() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler_seen = Arc::clone(&seen);
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    builder
        .handle(move |message: Work| {
            let seen = Arc::clone(&handler_seen);
            async move {
                seen.lock().await.push(message.id);
                Ok(message.id)
            }
        })
        .unwrap();
    let runtime = builder.build();
    let report = runtime
        .cascade(
            effects([Work { id: 1, fail: false }, Work { id: 2, fail: false }]),
            CascadeLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN),
        )
        .await;
    assert!(!report.is_success());
    let mut recovery = Effects::default();
    for outcome in report.into_parts().1 {
        if let EffectResult::Rejected { reason, effect } = outcome.into_result() {
            assert_eq!(reason, CascadeLimit::Effects);
            recovery.push(effect).unwrap();
        }
    }
    assert_eq!(recovery.len(), 1);
    assert!(
        runtime
            .cascade(recovery, CascadeLimits::default())
            .await
            .is_success()
    );
    assert_eq!(*seen.lock().await, [1, 2]);
}
