//! Combined retry admission, pause jitter and half-open cancellation regressions.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use raddle::wire::{Envelope, WireMessage};
use raddle_policies::{
    Action, CircuitBreaker, CircuitOptions, Direction, ErrorFilter, InMemoryEndpoint, Jitter,
    MemoryDelivery, MemoryStatus, Plan, PolicyError, PolicyExecutor, PolicySet, Quota,
    QuotaAdmission, QuotaKind, ReplaySafety, Rule, ScopedQuotas, Step, Tail,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Serialize, Deserialize)]
struct Work;
impl WireMessage for Work {
    const NAME: &'static str = "quota.work";
    const VERSION: u32 = 1;
}

fn failure() -> PolicyError {
    Box::new(std::io::Error::other("test failure"))
}
fn executor(steps: Vec<Step>, tail: Tail, jitter: Jitter) -> PolicyExecutor {
    let mut policies = PolicySet::default();
    policies.global(Rule {
        filter: ErrorFilter::any(),
        plan: Plan::new(steps, tail, jitter).unwrap(),
    });
    PolicyExecutor::new(policies)
}
fn endpoint(direction: Direction, quota: Quota) -> InMemoryEndpoint {
    let mut quotas = ScopedQuotas::default();
    quotas.endpoint("a".into(), quota).unwrap();
    let mut endpoint = InMemoryEndpoint::new("a", direction, 1)
        .unwrap()
        .with_quotas(Arc::new(quotas));
    let mut delivery = MemoryDelivery::new(Envelope::encode(&Work).unwrap());
    delivery.replay = ReplaySafety::Idempotent;
    endpoint.enqueue(delivery).unwrap();
    endpoint
}
fn time_quotas() -> [QuotaKind; 3] {
    let window = Duration::from_secs(10);
    [
        QuotaKind::TokenBucket {
            capacity: 1,
            refill: 1,
            period: window,
        },
        QuotaKind::FixedWindow { permits: 1, window },
        QuotaKind::SlidingWindow { permits: 1, window },
    ]
}

#[tokio::test(start_paused = true)]
async fn every_finite_and_indefinite_inline_attempt_obeys_each_time_quota() {
    for kind in time_quotas() {
        for direction in [Direction::Handling, Direction::Sending] {
            for indefinite in [false, true] {
                let mut endpoint = endpoint(direction, Quota::new(kind).unwrap());
                let executor = if indefinite {
                    executor(
                        vec![],
                        Tail::Repeat(Step::new(Action::Immediate)),
                        Jitter::None,
                    )
                } else {
                    executor(
                        vec![Step::new(Action::Immediate); 2],
                        Tail::Terminal(Action::DeadLetter),
                        Jitter::None,
                    )
                };
                let (_sender, mut stop) = watch::channel(false);
                let calls = AtomicUsize::new(0);
                for attempt in 1..=3 {
                    let now = Duration::from_secs((attempt - 1) * 10);
                    assert!(
                        endpoint
                            .run_once(&executor, now, &mut stop, |_| {
                                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                                async move { if call < 3 { Err(failure()) } else { Ok(()) } }
                            })
                            .await
                            .unwrap()
                    );
                    assert_eq!(calls.load(Ordering::SeqCst), attempt as usize);
                    assert_eq!(endpoint.deliveries()[0].attempts.get(direction), attempt);
                    if attempt < 3 {
                        assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Ready);
                        assert_eq!(endpoint.deliveries()[0].due, now + Duration::from_secs(10));
                    }
                }
                assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Completed);
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn quota_refusals_neither_trip_nor_dilute_the_receive_circuit() {
    for kind in time_quotas() {
        let circuit = CircuitBreaker::new(CircuitOptions {
            minimum_samples: 2,
            failure_basis_points: 10000,
            window: Duration::from_secs(1000),
            pause: Duration::from_secs(50),
            ..Default::default()
        })
        .unwrap();
        let mut endpoint = endpoint(Direction::Handling, Quota::new(kind).unwrap())
            .with_circuit(circuit)
            .unwrap();
        let executor = executor(
            vec![],
            Tail::Repeat(Step::new(Action::Immediate)),
            Jitter::None,
        );
        let (_sender, mut stop) = watch::channel(false);
        endpoint
            .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
                Err(failure())
            })
            .await
            .unwrap();
        assert_eq!(endpoint.deliveries()[0].attempts.handling, 1);
        assert_eq!(endpoint.deliveries()[0].due, Duration::from_secs(10));
        assert!(
            endpoint
                .run_once(&executor, Duration::from_secs(10), &mut stop, |_| async {
                    Err(failure())
                })
                .await
                .unwrap()
        );
        assert_eq!(endpoint.deliveries()[0].attempts.handling, 2);
        assert_eq!(endpoint.deliveries()[0].due, Duration::from_secs(60));
        assert!(
            !endpoint
                .run_once(&executor, Duration::from_secs(20), &mut stop, |_| async {
                    panic!("quota refusal diluted circuit observations");
                    #[allow(unreachable_code)]
                    Ok(())
                })
                .await
                .unwrap()
        );
        assert!(
            endpoint
                .run_once(&executor, Duration::from_secs(60), &mut stop, |_| async {
                    Ok(())
                })
                .await
                .unwrap()
        );
        assert_eq!(endpoint.deliveries()[0].attempts.handling, 3);
    }
}

#[tokio::test(start_paused = true)]
async fn cooldown_rechecks_current_window_before_actual_execution() {
    for kind in time_quotas() {
        for delay in [6, 10] {
            let quota = Quota::new(kind).unwrap();
            let mut endpoint = endpoint(Direction::Handling, quota.clone());
            let executor = executor(
                vec![Step::new(Action::Cooldown(Duration::from_secs(delay)))],
                Tail::Terminal(Action::DeadLetter),
                Jitter::None,
            );
            let (_sender, mut stop) = watch::channel(false);
            let calls = AtomicUsize::new(0);
            endpoint
                .run_once(&executor, Duration::ZERO, &mut stop, |_| {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    async move { if call == 0 { Err(failure()) } else { Ok(()) } }
                })
                .await
                .unwrap();
            if delay == 6 {
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(endpoint.deliveries()[0].attempts.handling, 1);
                assert_eq!(endpoint.deliveries()[0].due, Duration::from_secs(10));
                endpoint
                    .run_once(&executor, Duration::from_secs(10), &mut stop, |_| async {
                        Ok(())
                    })
                    .await
                    .unwrap();
            } else {
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            }
            assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Completed);
            assert_eq!(endpoint.deliveries()[0].attempts.handling, 2);
            assert!(matches!(
                quota.acquire(Duration::from_secs(10)).unwrap(),
                QuotaAdmission::Limited { .. }
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn concurrency_one_is_released_before_retry_and_cooldown() {
    for action in [Action::Immediate, Action::Cooldown(Duration::from_secs(1))] {
        let quota = Quota::new(QuotaKind::Concurrency { permits: 1 }).unwrap();
        let mut endpoint = endpoint(Direction::Handling, quota.clone());
        let executor = executor(
            vec![Step::new(action); 2],
            Tail::Terminal(Action::DeadLetter),
            Jitter::None,
        );
        let (_sender, mut stop) = watch::channel(false);
        let calls = AtomicUsize::new(0);
        endpoint
            .run_once(&executor, Duration::ZERO, &mut stop, |_| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move { if call < 2 { Err(failure()) } else { Ok(()) } }
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Completed);
        assert!(matches!(
            quota.acquire(Duration::from_secs(2)).unwrap(),
            QuotaAdmission::Granted(_)
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn another_operation_can_acquire_concurrency_during_inline_cooldown() {
    let quota = Quota::new(QuotaKind::Concurrency { permits: 1 }).unwrap();
    let mut endpoint = endpoint(Direction::Handling, quota.clone());
    let executor = executor(
        vec![Step::new(Action::Cooldown(Duration::from_secs(1)))],
        Tail::Terminal(Action::DeadLetter),
        Jitter::None,
    );
    let (_sender, mut stop) = watch::channel(false);
    let (begun, mut began) = watch::channel(false);
    let calls = AtomicUsize::new(0);
    let run = endpoint.run_once(&executor, Duration::ZERO, &mut stop, |_| {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        begun.send_replace(true);
        async move { if call == 0 { Err(failure()) } else { Ok(()) } }
    });
    let inspect = async {
        began.changed().await.unwrap();
        assert!(matches!(
            quota.acquire(Duration::ZERO).unwrap(),
            QuotaAdmission::Granted(_)
        ));
    };
    let (result, ()) = tokio::join!(run, inspect);
    assert!(result.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Completed);
}

#[tokio::test(start_paused = true)]
async fn limited_and_interrupted_half_open_probes_are_released_without_failure_samples() {
    for drop_future in [false, true] {
        let quota = Quota::new(QuotaKind::Concurrency { permits: 1 }).unwrap();
        let held = quota.acquire(Duration::ZERO).unwrap();
        let mut circuit = CircuitBreaker::new(CircuitOptions {
            minimum_samples: 1,
            failure_basis_points: 10000,
            pause: Duration::from_secs(5),
            ..Default::default()
        })
        .unwrap();
        circuit
            .record(Duration::ZERO, Some(&*failure()), false)
            .unwrap();
        let mut endpoint = endpoint(Direction::Handling, quota.clone())
            .with_circuit(circuit)
            .unwrap();
        let executor = executor(vec![], Tail::Terminal(Action::DeadLetter), Jitter::None);
        let (sender, mut stop) = watch::channel(false);
        assert!(
            !endpoint
                .run_once(&executor, Duration::from_secs(5), &mut stop, |_| async {
                    panic!("quota denied probe executed");
                    #[allow(unreachable_code)]
                    Ok(())
                })
                .await
                .unwrap()
        );
        assert_eq!(endpoint.deliveries()[0].attempts.handling, 0);
        drop(held);
        if drop_future {
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(1),
                    endpoint.run_once(&executor, Duration::from_secs(5), &mut stop, |_| {
                        std::future::pending()
                    })
                )
                .await
                .is_err()
            );
            endpoint.recover_interrupted().unwrap();
        } else {
            let (begun, mut began) = watch::channel(false);
            let run = endpoint.run_once(&executor, Duration::from_secs(5), &mut stop, |_| {
                begun.send_replace(true);
                std::future::pending()
            });
            let cancel = async {
                began.changed().await.unwrap();
                sender.send_replace(true);
            };
            let (result, ()) = tokio::join!(run, cancel);
            assert!(result.unwrap());
            sender.send_replace(false);
        }
        assert_eq!(endpoint.deliveries()[0].attempts.handling, 1);
        assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Interrupted);
        let id = endpoint.deliveries()[0].envelope.id();
        endpoint
            .resume_interrupted(id, Duration::from_secs(5), ReplaySafety::Idempotent)
            .unwrap();
        assert!(
            endpoint
                .run_once(&executor, Duration::from_secs(5), &mut stop, |_| async {
                    Ok(())
                })
                .await
                .unwrap()
        );
        assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Completed);
        assert_eq!(endpoint.deliveries()[0].attempts.handling, 2);
        assert!(matches!(
            quota.acquire(Duration::from_secs(5)).unwrap(),
            QuotaAdmission::Granted(_)
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn finite_and_repeated_pause_requeue_jitter_applies_once_at_both_bounds() {
    for repeat in [false, true] {
        for (sample, pause) in [(0, 5), (u64::MAX, 10)] {
            let step = Step::new(Action::Requeue).pausing(Duration::from_secs(5));
            let plan = if repeat {
                Plan::new(vec![], Tail::Repeat(step), Jitter::Full)
            } else {
                Plan::new(vec![step], Tail::Terminal(Action::DeadLetter), Jitter::Full)
            }
            .unwrap();
            assert_eq!(
                plan.step(1, sample).unwrap().pause,
                Some(Duration::from_secs(pause))
            );
            if repeat {
                assert_eq!(
                    plan.step(1000, sample).unwrap().pause,
                    Some(Duration::from_secs(pause))
                );
            }
            let mut policies = PolicySet::default();
            policies.global(Rule {
                filter: ErrorFilter::any(),
                plan,
            });
            let mut endpoint = endpoint(
                Direction::Handling,
                Quota::new(QuotaKind::Concurrency { permits: 1 }).unwrap(),
            )
            .with_jitter_source(move || Ok(sample));
            let (_sender, mut stop) = watch::channel(false);
            let executor = PolicyExecutor::new(policies);
            endpoint
                .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
                    Err(failure())
                })
                .await
                .unwrap();
            assert!(
                !endpoint
                    .run_once(
                        &executor,
                        Duration::from_secs(pause - 1),
                        &mut stop,
                        |_| async {
                            panic!("jittered pause bypassed");
                            #[allow(unreachable_code)]
                            Ok(())
                        }
                    )
                    .await
                    .unwrap()
            );
            endpoint
                .run_once(
                    &executor,
                    Duration::from_secs(pause),
                    &mut stop,
                    |_| async { Ok(()) },
                )
                .await
                .unwrap();
            assert_eq!(endpoint.deliveries()[0].attempts.handling, 2);
        }
    }
    let composite = Plan::new(
        vec![Step::new(Action::Schedule(Duration::from_secs(3))).pausing(Duration::from_secs(5))],
        Tail::Terminal(Action::DeadLetter),
        Jitter::Full,
    )
    .unwrap();
    let step = composite.step(1, u64::MAX).unwrap();
    assert_eq!(step.action, Action::Schedule(Duration::from_secs(6)));
    assert_eq!(step.pause, Some(Duration::from_secs(10)));
}
