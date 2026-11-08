//! Executable policy, cancellation, circuit and scoped quota contracts.
#![allow(clippy::unwrap_used)]

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use raddle::wire::{Envelope, WireMessage};
use raddle_policies::{
    Action, AttemptCounters, CircuitAdmission, CircuitBreaker, CircuitOptions, Direction,
    ErrorFilter, Execution, ExecutionMode, InMemoryEndpoint, Jitter, MemoryDelivery, MemoryStatus,
    Plan, PolicyError, PolicyExecutor, PolicyExecutorDriver, PolicySet, ReplaySafety, Rule, Step,
    Tail, Transition,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Serialize, Deserialize)]
struct Work(u32);
impl WireMessage for Work {
    const NAME: &'static str = "work";
    const VERSION: u32 = 1;
}

fn error() -> PolicyError {
    Box::new(io::Error::other("sanitized test failure"))
}
fn plan(actions: Vec<Step>, tail: Tail) -> Plan {
    Plan::new(actions, tail, Jitter::None).unwrap()
}
fn terminal(action: Action) -> Rule {
    Rule {
        filter: ErrorFilter::any(),
        plan: plan(vec![], Tail::Terminal(action)),
    }
}
fn context<'a>(endpoint: &'a str, contract: &'a str, attempts: u64) -> Execution<'a> {
    Execution {
        endpoint,
        contract,
        direction: Direction::Handling,
        attempts: AttemptCounters {
            handling: attempts,
            sending: 0,
        },
        replay: ReplaySafety::Idempotent,
        mode: ExecutionMode::Queued,
        now: Duration::ZERO,
        jitter_sample: 0,
    }
}
#[derive(Default)]
struct Recorder {
    transitions: Vec<Transition>,
    pauses: Vec<Duration>,
}
impl PolicyExecutorDriver for Recorder {
    async fn transition(&mut self, t: Transition) -> Result<(), raddle_policies::DriverError> {
        self.transitions.push(t);
        Ok(())
    }
    async fn pause_until(&mut self, t: Duration) -> Result<(), raddle_policies::DriverError> {
        self.pauses.push(t);
        Ok(())
    }
}

#[test]
fn filters_scopes_and_attempt_counters_are_explicit() {
    #[derive(Debug, thiserror::Error)]
    #[error("outer")]
    struct Outer(#[source] io::Error);
    let wrapped = Outer(io::Error::other("inner"));
    assert!(ErrorFilter::typed::<io::Error>().cause().matches(&wrapped));
    assert!(!ErrorFilter::typed::<io::Error>().matches(&wrapped));
    assert!(
        ErrorFilter::typed::<Outer>()
            .and(ErrorFilter::typed::<io::Error>().cause())
            .matches(&wrapped)
    );
    assert!(
        ErrorFilter::predicate(|e| e.is::<Outer>())
            .or(ErrorFilter::typed::<io::Error>())
            .matches(&wrapped)
    );
    let mut policies = PolicySet::default();
    policies.global(terminal(Action::DeadLetter));
    policies.endpoint("a", terminal(Action::Discard));
    policies.message(
        "work",
        Rule {
            filter: ErrorFilter::typed::<Outer>(),
            plan: plan(
                vec![Step::new(Action::Immediate)],
                Tail::Terminal(Action::DeadLetter),
            ),
        },
    );
    assert_eq!(
        policies
            .select(Direction::Handling, "a", "work", &wrapped)
            .unwrap()
            .step(1, 0)
            .unwrap()
            .action,
        Action::Immediate
    );
    assert_eq!(
        policies
            .select(Direction::Handling, "a", "work", &io::Error::other("x"))
            .unwrap()
            .step(1, 0)
            .unwrap()
            .action,
        Action::Discard
    );
    assert_eq!(
        policies
            .select(Direction::Sending, "a", "work", &wrapped)
            .unwrap()
            .step(1, 0)
            .unwrap()
            .action,
        Action::Discard
    );
    assert_eq!(
        policies
            .select(Direction::Handling, "b", "other", &wrapped)
            .unwrap()
            .step(1, 0)
            .unwrap()
            .action,
        Action::DeadLetter
    );
    let mut attempts = AttemptCounters::default();
    assert_eq!(attempts.begin(Direction::Sending).unwrap(), 1);
    assert_eq!(attempts.begin(Direction::Handling).unwrap(), 1);
    assert_eq!(attempts.begin(Direction::Sending).unwrap(), 2);
    assert_eq!(attempts.handling, 1);
}

#[tokio::test(start_paused = true)]
async fn every_action_is_executed_and_indefinite_tails_keep_pause() {
    let actions = [
        Action::Immediate,
        Action::Cooldown(Duration::from_secs(1)),
        Action::Requeue,
        Action::Schedule(Duration::from_secs(3)),
    ];
    let mut policies = PolicySet::default();
    policies.global(Rule {
        filter: ErrorFilter::any(),
        plan: plan(
            actions.into_iter().map(Step::new).collect(),
            Tail::Repeat(Step::new(Action::Requeue).pausing(Duration::from_secs(5))),
        ),
    });
    let executor = PolicyExecutor::new(policies);
    let (_sender, mut stop) = watch::channel(false);
    let mut recorder = Recorder::default();
    for attempt in 1..=6 {
        executor
            .failed(
                &mut recorder,
                context("a", "work", attempt),
                &*error(),
                &mut stop,
            )
            .await
            .unwrap();
    }
    assert_eq!(
        recorder.transitions,
        [
            Transition::RetryInline,
            Transition::RetryInline,
            Transition::QueueAt(Duration::ZERO),
            Transition::QueueAt(Duration::from_secs(3)),
            Transition::QueueAt(Duration::ZERO),
            Transition::QueueAt(Duration::ZERO)
        ]
    );
    assert_eq!(recorder.pauses, [Duration::from_secs(5); 2]);
    for terminal_action in [Action::Discard, Action::DeadLetter] {
        let mut policies = PolicySet::default();
        policies.global(terminal(terminal_action));
        let mut recorder = Recorder::default();
        PolicyExecutor::new(policies)
            .failed(&mut recorder, context("a", "work", 1), &*error(), &mut stop)
            .await
            .unwrap();
        assert_eq!(
            recorder.transitions[0],
            if terminal_action == Action::Discard {
                Transition::Discard
            } else {
                Transition::DeadLetter {
                    replay_denied: false,
                }
            }
        );
    }
}

#[tokio::test(start_paused = true)]
async fn inline_never_applies_queued_actions_and_unsafe_replay_is_refused() {
    let (_sender, mut stop) = watch::channel(false);
    for action in [
        Action::Requeue,
        Action::Schedule(Duration::from_secs(1)),
        Action::Discard,
        Action::DeadLetter,
    ] {
        let p = if matches!(action, Action::Discard | Action::DeadLetter) {
            plan(vec![], Tail::Terminal(action))
        } else {
            plan(vec![Step::new(action)], Tail::Terminal(Action::DeadLetter))
        };
        let mut policies = PolicySet::default();
        policies.global(Rule {
            filter: ErrorFilter::any(),
            plan: p,
        });
        let executor = PolicyExecutor::new(policies);
        let mut ctx = context("a", "work", 1);
        ctx.mode = ExecutionMode::Inline;
        assert_eq!(
            executor
                .failed(&mut Recorder::default(), ctx, &*error(), &mut stop)
                .await
                .unwrap(),
            Transition::StopInline
        );
    }
    let mut policies = PolicySet::default();
    policies.global(Rule {
        filter: ErrorFilter::any(),
        plan: plan(
            vec![Step::new(Action::Immediate)],
            Tail::Terminal(Action::DeadLetter),
        ),
    });
    let executor = PolicyExecutor::new(policies);
    let mut ctx = context("a", "work", 1);
    ctx.replay = ReplaySafety::Never;
    assert_eq!(
        executor
            .failed(&mut Recorder::default(), ctx, &*error(), &mut stop)
            .await
            .unwrap(),
        Transition::DeadLetter {
            replay_denied: true
        }
    );
}

#[tokio::test]
async fn failed_first_middle_last_and_corrected_retries_only_failed_delivery() {
    for direction in [Direction::Handling, Direction::Sending] {
        for bad in 0..3 {
            let executor = PolicyExecutor::new(PolicySet::default());
            let mut endpoint = InMemoryEndpoint::new("orders", direction, 3).unwrap();
            let calls = Arc::new(Mutex::new([0; 3]));
            let mut ids = Vec::new();
            for i in 0..3 {
                let envelope = Envelope::encode(&Work(i)).unwrap();
                ids.push(envelope.id());
                endpoint.enqueue(MemoryDelivery::new(envelope)).unwrap();
            }
            let (_sender, mut stop) = watch::channel(false);
            for _ in 0..3 {
                let calls = calls.clone();
                endpoint
                    .run_once(&executor, Duration::ZERO, &mut stop, move |message| {
                        let calls = calls.clone();
                        async move {
                            let Work(i) = message.decode().unwrap();
                            calls.lock().unwrap()[i as usize] += 1;
                            if i == bad { Err(error()) } else { Ok(()) }
                        }
                    })
                    .await
                    .unwrap();
            }
            assert_eq!(*calls.lock().unwrap(), [1; 3]);
            for (i, delivery) in endpoint.deliveries().iter().enumerate() {
                assert_eq!(
                    delivery.status,
                    if i == bad as usize {
                        MemoryStatus::DeadLetter {
                            replay_denied: false,
                        }
                    } else {
                        MemoryStatus::Completed
                    }
                );
            }
            endpoint
                .retry_dead_letter(ids[bad as usize], Duration::ZERO)
                .unwrap();
            let observed = calls.clone();
            endpoint
                .run_once(&executor, Duration::ZERO, &mut stop, move |message| {
                    let observed = observed.clone();
                    async move {
                        let Work(i) = message.decode().unwrap();
                        observed.lock().unwrap()[i as usize] += 1;
                        Ok(())
                    }
                })
                .await
                .unwrap();
            let mut expected = [1; 3];
            expected[bad as usize] = 2;
            assert_eq!(*calls.lock().unwrap(), expected);
            assert!(
                endpoint
                    .deliveries()
                    .iter()
                    .all(|d| d.status == MemoryStatus::Completed)
            );
            for delivery in endpoint.deliveries() {
                assert_eq!(
                    delivery.attempts.get(if direction == Direction::Handling {
                        Direction::Sending
                    } else {
                        Direction::Handling
                    }),
                    0
                );
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn queue_tail_scheduling_exhaustion_and_endpoint_pause_preserve_identity() {
    let mut policies = PolicySet::default();
    policies.global(Rule {
        filter: ErrorFilter::any(),
        plan: plan(
            vec![
                Step::new(Action::Requeue),
                Step::new(Action::Schedule(Duration::from_secs(10)))
                    .pausing(Duration::from_secs(5)),
            ],
            Tail::Terminal(Action::Discard),
        ),
    });
    let executor = PolicyExecutor::new(policies);
    let mut endpoint = InMemoryEndpoint::new("a", Direction::Handling, 2).unwrap();
    let mut failing = MemoryDelivery::new(Envelope::encode(&Work(0)).unwrap());
    failing.replay = ReplaySafety::Idempotent;
    let id = failing.envelope.id();
    endpoint.enqueue(failing).unwrap();
    endpoint
        .enqueue(MemoryDelivery::new(Envelope::encode(&Work(1)).unwrap()))
        .unwrap();
    let (_sender, mut stop) = watch::channel(false);
    endpoint
        .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
            Err(error())
        })
        .await
        .unwrap();
    endpoint
        .run_once(
            &executor,
            Duration::ZERO,
            &mut stop,
            |envelope| async move {
                assert_eq!(envelope.decode::<Work>().unwrap().0, 1);
                Ok(())
            },
        )
        .await
        .unwrap();
    endpoint
        .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
            Err(error())
        })
        .await
        .unwrap();
    assert!(
        !endpoint
            .run_once(&executor, Duration::from_secs(6), &mut stop, |_| async {
                Ok(())
            })
            .await
            .unwrap()
    );
    endpoint
        .run_once(&executor, Duration::from_secs(10), &mut stop, |_| async {
            Err(error())
        })
        .await
        .unwrap();
    assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Discarded);
    assert_eq!(endpoint.deliveries()[0].attempts.handling, 3);
    assert_eq!(endpoint.deliveries()[0].envelope.id(), id);
    assert_eq!(endpoint.deliveries()[1].attempts.handling, 1);
}

#[tokio::test]
async fn shutdown_interrupts_cooldown_and_inflight_work_without_terminal_settlement() {
    let mut policies = PolicySet::default();
    policies.global(Rule {
        filter: ErrorFilter::any(),
        plan: plan(
            vec![Step::new(Action::Cooldown(Duration::from_secs(3600)))],
            Tail::Terminal(Action::DeadLetter),
        ),
    });
    let executor = PolicyExecutor::new(policies);
    for during_operation in [false, true] {
        let mut endpoint = InMemoryEndpoint::new("a", Direction::Handling, 1).unwrap();
        let mut delivery = MemoryDelivery::new(Envelope::encode(&Work(0)).unwrap());
        delivery.replay = ReplaySafety::Idempotent;
        endpoint.enqueue(delivery).unwrap();
        let (sender, mut stop) = watch::channel(false);
        let (begun, mut began) = watch::channel(false);
        let task = async {
            endpoint
                .run_once(&executor, Duration::ZERO, &mut stop, |_| {
                    begun.send_replace(true);
                    async move {
                        if during_operation {
                            std::future::pending::<()>().await;
                        }
                        Err(error())
                    }
                })
                .await
                .unwrap();
        };
        let cancel = async {
            began.changed().await.unwrap();
            tokio::task::yield_now().await;
            sender.send_replace(true);
        };
        tokio::join!(task, cancel);
        assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Interrupted);
        assert_eq!(endpoint.deliveries()[0].attempts.handling, 1);
        let id = endpoint.deliveries()[0].envelope.id();
        assert!(
            endpoint
                .resume_interrupted(id, Duration::ZERO, ReplaySafety::Never)
                .is_err()
        );
        endpoint
            .resume_interrupted(id, Duration::ZERO, ReplaySafety::Idempotent)
            .unwrap();
    }
}

#[test]
fn circuit_ratio_window_exclusions_probe_and_reset() {
    let options = CircuitOptions {
        minimum_samples: 2,
        failure_basis_points: 5000,
        window: Duration::from_secs(10),
        pause: Duration::from_secs(5),
        maximum_samples: 3,
        exclude: Some(ErrorFilter::typed::<io::Error>()),
        ..Default::default()
    };
    let mut circuit = CircuitBreaker::new(options).unwrap();
    assert_eq!(
        circuit
            .record(Duration::ZERO, Some(&*error()), false)
            .unwrap(),
        None
    );
    assert_eq!(circuit.samples(), 0);
    #[derive(Debug, thiserror::Error)]
    #[error("bad")]
    struct Bad;
    circuit.record(Duration::ZERO, Some(&Bad), true).unwrap();
    assert_eq!(circuit.samples(), 0);
    circuit.record(Duration::ZERO, Some(&Bad), false).unwrap();
    assert_eq!(
        circuit
            .record(Duration::from_secs(10), None, false)
            .unwrap(),
        None
    );
    assert_eq!(circuit.samples(), 1);
    assert_eq!(
        circuit
            .record(Duration::from_secs(10), Some(&Bad), false)
            .unwrap(),
        Some(Duration::from_secs(15))
    );
    assert_eq!(circuit.samples(), 0);
    assert_eq!(
        circuit.admission(Duration::from_secs(14)),
        CircuitAdmission::Paused(Duration::from_secs(15))
    );
    assert_eq!(
        circuit.admission(Duration::from_secs(15)),
        CircuitAdmission::Probe
    );
    assert!(matches!(
        circuit.admission(Duration::from_secs(15)),
        CircuitAdmission::Paused(_)
    ));
    circuit
        .record(Duration::from_secs(15), None, false)
        .unwrap();
    assert_eq!(
        circuit.admission(Duration::from_secs(15)),
        CircuitAdmission::Open
    );
    assert_eq!(circuit.samples(), 1);
}

#[test]
fn jitter_bounds_and_overflow_never_shorten_base_delay() {
    let base = Duration::from_secs(10);
    for strategy in [
        Jitter::None,
        Jitter::Full,
        Jitter::BoundedPercent(25),
        Jitter::Exponential,
    ] {
        assert_eq!(strategy.delay(base, 2, 0), base);
        assert!(strategy.delay(base, 2, u64::MAX) >= base);
    }
    assert_eq!(
        Jitter::Full.delay(base, 1, u64::MAX),
        Duration::from_secs(20)
    );
    assert_eq!(
        Jitter::BoundedPercent(25).delay(base, 1, u64::MAX),
        Duration::from_millis(12500)
    );
    assert_eq!(
        Jitter::Exponential.delay(base, 2, u64::MAX),
        Duration::from_secs(50)
    );
    assert_eq!(
        Jitter::Exponential.delay(Duration::MAX, u64::MAX, u64::MAX),
        Duration::MAX
    );
    assert!(Plan::new(vec![], Tail::Terminal(Action::Requeue), Jitter::None).is_err());
    assert!(
        Plan::new(
            vec![],
            Tail::Terminal(Action::Discard),
            Jitter::BoundedPercent(0)
        )
        .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn real_endpoint_circuit_pauses_only_failed_endpoint_then_probes_and_resets() {
    let circuit = CircuitBreaker::new(CircuitOptions {
        minimum_samples: 1,
        failure_basis_points: 10000,
        pause: Duration::from_secs(5),
        ..Default::default()
    })
    .unwrap();
    let mut failed = InMemoryEndpoint::new("failed", Direction::Handling, 2)
        .unwrap()
        .with_circuit(circuit)
        .unwrap();
    let mut healthy = InMemoryEndpoint::new("healthy", Direction::Handling, 1).unwrap();
    for i in 0..2 {
        failed
            .enqueue(MemoryDelivery::new(Envelope::encode(&Work(i)).unwrap()))
            .unwrap();
    }
    healthy
        .enqueue(MemoryDelivery::new(Envelope::encode(&Work(2)).unwrap()))
        .unwrap();
    let executor = PolicyExecutor::new(PolicySet::default());
    let (_sender, mut stop) = watch::channel(false);
    failed
        .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
            Err(error())
        })
        .await
        .unwrap();
    assert!(
        !failed
            .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
                panic!("paused endpoint must not execute");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
            .unwrap()
    );
    healthy
        .run_once(&executor, Duration::ZERO, &mut stop, |_| async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(healthy.deliveries()[0].status, MemoryStatus::Completed);
    assert!(
        failed
            .run_once(&executor, Duration::from_secs(5), &mut stop, |_| async {
                Ok(())
            })
            .await
            .unwrap()
    );
    assert_eq!(failed.deliveries()[1].status, MemoryStatus::Completed);
}

#[tokio::test(start_paused = true)]
async fn real_endpoint_quotas_reschedule_before_counting_attempts() {
    use raddle_policies::{Quota, QuotaKind, ScopedQuotas};
    let mut quotas = ScopedQuotas::default();
    quotas
        .endpoint(
            "a".into(),
            Quota::new(QuotaKind::FixedWindow {
                permits: 1,
                window: Duration::from_secs(10),
            })
            .unwrap(),
        )
        .unwrap();
    let mut endpoint = InMemoryEndpoint::new("a", Direction::Handling, 2)
        .unwrap()
        .with_quotas(Arc::new(quotas));
    for i in 0..2 {
        endpoint
            .enqueue(MemoryDelivery::new(Envelope::encode(&Work(i)).unwrap()))
            .unwrap();
    }
    let executor = PolicyExecutor::new(PolicySet::default());
    let (_sender, mut stop) = watch::channel(false);
    endpoint
        .run_once(&executor, Duration::ZERO, &mut stop, |_| async { Ok(()) })
        .await
        .unwrap();
    assert!(
        !endpoint
            .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
                panic!("limited delivery must not execute");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
            .unwrap()
    );
    assert_eq!(endpoint.deliveries()[1].attempts.handling, 0);
    assert_eq!(endpoint.deliveries()[1].due, Duration::from_secs(10));
    endpoint
        .run_once(&executor, Duration::from_secs(10), &mut stop, |_| async {
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(endpoint.deliveries()[1].attempts.handling, 1);
}

#[tokio::test]
async fn dropped_execution_keeps_custody_and_requires_explicit_recovery() {
    let mut endpoint = InMemoryEndpoint::new("a", Direction::Handling, 1).unwrap();
    endpoint
        .enqueue(MemoryDelivery::new(Envelope::encode(&Work(0)).unwrap()))
        .unwrap();
    let executor = PolicyExecutor::new(PolicySet::default());
    let (_sender, mut stop) = watch::channel(false);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(1),
            endpoint.run_once(&executor, Duration::ZERO, &mut stop, |_| {
                std::future::pending()
            })
        )
        .await
        .is_err()
    );
    assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Active);
    endpoint.recover_interrupted().unwrap();
    assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Interrupted);
    assert!(
        !endpoint
            .run_once(&executor, Duration::ZERO, &mut stop, |_| async { Ok(()) })
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn indefinite_inline_retries_yield_to_shutdown_and_false_watch_updates_do_not_stop() {
    let mut policies = PolicySet::default();
    policies.global(Rule {
        filter: ErrorFilter::any(),
        plan: plan(vec![], Tail::Repeat(Step::new(Action::Immediate))),
    });
    let executor = PolicyExecutor::new(policies);
    let mut endpoint = InMemoryEndpoint::new("a", Direction::Handling, 1).unwrap();
    let mut delivery = MemoryDelivery::new(Envelope::encode(&Work(0)).unwrap());
    delivery.replay = ReplaySafety::Idempotent;
    endpoint.enqueue(delivery).unwrap();
    let (sender, mut stop) = watch::channel(false);
    sender.send_replace(false);
    let (begun, mut began) = watch::channel(false);
    let run = endpoint.run_once(&executor, Duration::ZERO, &mut stop, |_| {
        begun.send_replace(true);
        async { Err(error()) }
    });
    let cancel = async {
        began.changed().await.unwrap();
        tokio::task::yield_now().await;
        sender.send_replace(true);
    };
    let (result, ()) = tokio::join!(run, cancel);
    assert!(result.unwrap());
    assert!(endpoint.deliveries()[0].attempts.handling >= 1);
    assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Interrupted);
}

#[tokio::test(start_paused = true)]
async fn seeded_jitter_is_applied_by_the_actual_driver_to_scheduled_work() {
    let mut policies = PolicySet::default();
    policies.global(Rule {
        filter: ErrorFilter::any(),
        plan: Plan::new(
            vec![Step::new(Action::Schedule(Duration::from_secs(10)))],
            Tail::Terminal(Action::DeadLetter),
            Jitter::Full,
        )
        .unwrap(),
    });
    let mut endpoint = InMemoryEndpoint::new("a", Direction::Handling, 1)
        .unwrap()
        .with_jitter_source(|| Ok(u64::MAX));
    let mut delivery = MemoryDelivery::new(Envelope::encode(&Work(1)).unwrap());
    delivery.replay = ReplaySafety::Idempotent;
    endpoint.enqueue(delivery).unwrap();
    let (_sender, mut stop) = watch::channel(false);
    endpoint
        .run_once(
            &PolicyExecutor::new(policies),
            Duration::ZERO,
            &mut stop,
            |_| async { Err(error()) },
        )
        .await
        .unwrap();
    assert_eq!(endpoint.deliveries()[0].due, Duration::from_secs(20));
}
