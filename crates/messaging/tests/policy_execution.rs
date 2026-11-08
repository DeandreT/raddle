//! Actual Host-owned bus queue policy execution and failed-only recovery.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::{
    BusBuilder, EndpointPolicies, MessageContext, PolicyQueue, PolicyQueueBuilder, TrustedOrigin,
};
use raddle_policies::{
    Action, ErrorFilter, Jitter, MemoryDelivery, MemoryStatus, Plan, PolicySet, ReplaySafety, Rule,
    Step, Tail,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize)]
struct Work(u32);
impl WireMessage for Work {
    const NAME: &'static str = "test.policy-work";
    const VERSION: u32 = 1;
}
#[derive(Debug, thiserror::Error)]
#[error("scripted typed failure")]
struct TypedFailure;

fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("trusted-tenant", "trusted-source", None).unwrap()
}
fn rule(steps: Vec<Step>, tail: Action) -> Rule {
    Rule {
        filter: ErrorFilter::typed::<TypedFailure>(),
        plan: Plan::new(steps, Tail::Terminal(tail), Jitter::None).unwrap(),
    }
}
async fn wait(
    queue: &PolicyQueue,
    condition: impl Fn(&[MemoryDelivery]) -> bool,
) -> Vec<MemoryDelivery> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let states = queue.deliveries().unwrap();
            if condition(&states) {
                return states;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn host_bus_queue_requeues_only_failed_delivery_at_first_middle_and_last() {
    for failed in 0..3 {
        let mut policies = PolicySet::default();
        policies.global(rule(vec![Step::new(Action::Requeue)], Action::DeadLetter));
        let mut builder = PolicyQueueBuilder::new(
            "orders",
            8,
            Duration::from_secs(1),
            EndpointPolicies::new(policies),
        )
        .unwrap();
        let corrected = Arc::new(AtomicBool::new(false));
        let executions = Arc::new(Mutex::new(Vec::new()));
        let fixed = Arc::clone(&corrected);
        let calls = Arc::clone(&executions);
        builder
            .handle::<Work, TypedFailure, _, _>(
                move |work, context| {
                    let fixed = Arc::clone(&fixed);
                    let calls = Arc::clone(&calls);
                    async move {
                        assert_eq!(context.origin().tenant(), "trusted-tenant");
                        assert_eq!(context.origin().source(), "trusted-source");
                        assert!(context.bus().is_some());
                        calls.lock().await.push(work.0);
                        if work.0 == failed && !fixed.load(Ordering::SeqCst) {
                            Err(TypedFailure)
                        } else {
                            Ok(())
                        }
                    }
                },
                ReplaySafety::Idempotent,
            )
            .unwrap();
        let (queue, runner) = builder.build().unwrap();
        let mut bus = BusBuilder::new(RuntimeBuilder::try_new(1).unwrap().build());
        bus.policy_queue(queue.clone()).unwrap();
        bus.route::<Work>("orders").unwrap();
        let bus = bus.build();
        for value in 0..3 {
            bus.send(&Work(value), origin()).await.unwrap();
        }
        let mut host = Host::new(Duration::from_secs(2)).unwrap();
        runner.attach(&mut host, "orders").unwrap();
        let handle = host.handle();
        let task = tokio::spawn(host.run());
        let states = wait(&queue, |states| {
            states.len() == 3
                && states.iter().all(|d| {
                    matches!(
                        d.status,
                        MemoryStatus::Completed | MemoryStatus::DeadLetter { .. }
                    )
                })
        })
        .await;
        assert_eq!(*executions.lock().await, vec![0, 1, 2, failed]);
        let dead = &states[failed as usize];
        assert_eq!(dead.attempts.handling, 2);
        corrected.store(true, Ordering::SeqCst);
        queue.retry_dead_letter(dead.envelope.id()).await.unwrap();
        let states = wait(&queue, |states| {
            states.iter().all(|d| d.status == MemoryStatus::Completed)
        })
        .await;
        for (index, state) in states.iter().enumerate() {
            assert_eq!(
                state.attempts.handling,
                if index == failed as usize { 3 } else { 1 }
            );
        }
        assert_eq!(*executions.lock().await, vec![0, 1, 2, failed, failed]);
        handle.shutdown();
        task.await.unwrap().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn message_rule_precedes_endpoint_and_global_and_scheduled_counter_survives() {
    let mut policies = PolicySet::default();
    policies.global(rule(vec![], Action::Discard));
    policies.endpoint("orders", rule(vec![], Action::DeadLetter));
    policies.message(
        Work::NAME,
        rule(
            vec![
                Step::new(Action::Schedule(Duration::from_secs(20)))
                    .pausing(Duration::from_secs(10)),
            ],
            Action::Discard,
        ),
    );
    let mut builder = PolicyQueueBuilder::new(
        "orders",
        4,
        Duration::from_secs(1),
        EndpointPolicies::new(policies),
    )
    .unwrap();
    builder
        .handle::<Work, TypedFailure, _, _>(
            |_, _| async { Err(TypedFailure) },
            ReplaySafety::Idempotent,
        )
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    queue
        .submit(MessageContext::new(
            origin(),
            Envelope::encode(&Work(0)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    let states = wait(&queue, |s| s.len() == 1 && s[0].attempts.handling == 1).await;
    assert_eq!(states[0].status, MemoryStatus::Ready);
    tokio::time::advance(Duration::from_secs(19)).await;
    assert_eq!(queue.deliveries().unwrap()[0].attempts.handling, 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    let states = wait(&queue, |s| s[0].status == MemoryStatus::Discarded).await;
    assert_eq!(states[0].attempts.handling, 2);
    handle.shutdown();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn default_replay_refusal_and_cancellation_retain_distinct_outcomes() {
    let mut policies = PolicySet::default();
    policies.global(rule(vec![Step::new(Action::Immediate)], Action::DeadLetter));
    let mut builder = PolicyQueueBuilder::new(
        "orders",
        4,
        Duration::from_secs(1),
        EndpointPolicies::new(policies),
    )
    .unwrap();
    builder
        .handle::<Work, TypedFailure, _, _>(|_, _| async { Err(TypedFailure) }, ReplaySafety::Never)
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    let envelope = Envelope::encode(&Work(0)).unwrap();
    queue
        .submit(MessageContext::new(origin(), envelope.clone(), Utc::now()))
        .await
        .unwrap();
    // Same identity cannot be silently moved to a different trusted tenant.
    assert!(
        queue
            .submit(MessageContext::new(
                TrustedOrigin::authenticated("other", "trusted-source", None).unwrap(),
                envelope,
                Utc::now()
            ))
            .await
            .is_err()
    );
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    let states = wait(&queue, |s| {
        s.len() == 1
            && matches!(
                s[0].status,
                MemoryStatus::DeadLetter {
                    replay_denied: true
                }
            )
    })
    .await;
    assert_eq!(states[0].attempts.handling, 1);
    handle.shutdown();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn host_stop_interrupts_policy_cooldown_without_another_attempt() {
    let mut policies = PolicySet::default();
    policies.global(rule(
        vec![Step::new(Action::Cooldown(Duration::from_secs(60)))],
        Action::DeadLetter,
    ));
    let mut builder = PolicyQueueBuilder::new(
        "cooldown",
        4,
        Duration::from_secs(1),
        EndpointPolicies::new(policies),
    )
    .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = Arc::clone(&entered);
    builder
        .handle::<Work, TypedFailure, _, _>(
            move |_, _| {
                let notify = Arc::clone(&notify);
                async move {
                    notify.notify_one();
                    Err(TypedFailure)
                }
            },
            ReplaySafety::Idempotent,
        )
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    queue
        .submit(MessageContext::new(
            origin(),
            Envelope::encode(&Work(0)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "cooldown").unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    entered.notified().await;
    handle.shutdown();
    task.await.unwrap().unwrap();
    let states = queue.deliveries().unwrap();
    assert_eq!(states[0].attempts.handling, 1);
    assert_eq!(states[0].status, MemoryStatus::Interrupted);
}

#[tokio::test]
async fn actual_unknown_queue_runs_all_observers_and_bypasses_handler_attempts() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut unknown = raddle_policies::UnknownMessageHandler::new(
        raddle_policies::UnknownMessageOptions {
            disposition: raddle_policies::FailureDisposition::DeadLetter,
            tracking_capacity: 4,
        },
        Default::default(),
    )
    .unwrap();
    for fail in [true, false] {
        let calls = Arc::clone(&calls);
        unknown
            .observe(move |_| {
                let calls = Arc::clone(&calls);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if fail {
                        Err(raddle_policies::PolicyErrorCode)
                    } else {
                        Ok(())
                    }
                })
            })
            .unwrap();
    }
    let mut builder = PolicyQueueBuilder::new(
        "unknown",
        4,
        Duration::from_secs(1),
        EndpointPolicies::new(PolicySet::default()),
    )
    .unwrap();
    builder.unknown_messages(unknown);
    let (queue, runner) = builder.build().unwrap();
    queue
        .submit(MessageContext::new(
            origin(),
            Envelope::encode(&Work(0)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "unknown").unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    let states = wait(&queue, |s| {
        s.len() == 1 && matches!(s[0].status, MemoryStatus::DeadLetter { .. })
    })
    .await;
    assert_eq!(states[0].attempts.handling, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    handle.shutdown();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unsupported_handling_authorization_and_parallel_circuit_are_rejected_at_startup() {
    let mut queue = PolicyQueueBuilder::new(
        "startup",
        4,
        Duration::from_secs(1),
        EndpointPolicies::new(PolicySet::default()),
    )
    .unwrap();
    assert!(
        queue
            .handle::<Work, TypedFailure, _, _>(
                |_, _| async { Ok(()) },
                ReplaySafety::AtLeastOnceSend
            )
            .is_err()
    );
    queue
        .handle::<Work, TypedFailure, _, _>(|_, _| async { Ok(()) }, ReplaySafety::Never)
        .unwrap();
    // Lazy pool proves this refusal is startup validation, not a database probe.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    let mut endpoint = raddle_messaging::DurableEndpoint::new(
        raddle_postgres::Store::new(pool),
        "startup",
        2,
        raddle_postgres::RetryPolicy::default(),
    )
    .unwrap();
    let circuit = raddle_policies::CircuitBreaker::new(Default::default()).unwrap();
    assert!(
        endpoint
            .policies(EndpointPolicies::new(PolicySet::default()).with_circuit(circuit))
            .is_err()
    );
    endpoint
        .policies(EndpointPolicies::new(PolicySet::default()))
        .unwrap();
}

#[tokio::test]
async fn typed_handler_panic_is_quarantined_without_stopping_healthy_siblings() {
    let mut builder = PolicyQueueBuilder::new(
        "panic-isolation",
        8,
        Duration::from_secs(1),
        EndpointPolicies::new(PolicySet::default()),
    )
    .unwrap();
    builder
        .handle::<Work, TypedFailure, _, _>(
            |work, _| async move {
                if work.0 == 1 {
                    panic!("scripted handler panic");
                }
                Ok(())
            },
            ReplaySafety::Never,
        )
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    for value in 0..3 {
        queue
            .submit(MessageContext::new(
                origin(),
                Envelope::encode(&Work(value)).unwrap(),
                Utc::now(),
            ))
            .await
            .unwrap();
    }
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "panic-isolation").unwrap();
    let handle = host.handle();
    let task = tokio::spawn(host.run());
    let states = wait(&queue, |s| {
        s.len() == 3
            && s.iter().all(|d| {
                matches!(
                    d.status,
                    MemoryStatus::Completed | MemoryStatus::DeadLetter { .. }
                )
            })
    })
    .await;
    assert_eq!(states[0].status, MemoryStatus::Completed);
    assert!(matches!(states[1].status, MemoryStatus::DeadLetter { .. }));
    assert_eq!(states[2].status, MemoryStatus::Completed);
    assert!(states.iter().all(|d| d.attempts.handling == 1));
    handle.shutdown();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn shared_quota_uses_one_clock_across_queues_started_at_different_times() {
    let quota = raddle_policies::Quota::new(raddle_policies::QuotaKind::FixedWindow {
        permits: 1,
        window: Duration::from_secs(60),
    })
    .unwrap();
    let mut scopes = raddle_policies::ScopedQuotas::default();
    scopes.message(Work::NAME.to_owned(), quota).unwrap();
    let scopes = Arc::new(scopes);
    let mut first = PolicyQueueBuilder::new(
        "first",
        4,
        Duration::from_secs(1),
        EndpointPolicies::new(PolicySet::default()).with_quotas(Arc::clone(&scopes)),
    )
    .unwrap();
    first
        .handle::<Work, TypedFailure, _, _>(|_, _| async { Ok(()) }, ReplaySafety::Never)
        .unwrap();
    let (first, runner) = first.build().unwrap();
    first
        .submit(MessageContext::new(
            origin(),
            Envelope::encode(&Work(1)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "first").unwrap();
    let stop_first = host.handle();
    let task_first = tokio::spawn(host.run());
    wait(&first, |s| {
        s.len() == 1 && s[0].status == MemoryStatus::Completed
    })
    .await;
    tokio::time::advance(Duration::from_secs(10)).await;
    let mut second = PolicyQueueBuilder::new(
        "second",
        4,
        Duration::from_secs(1),
        EndpointPolicies::new(PolicySet::default()).with_quotas(scopes),
    )
    .unwrap();
    second
        .handle::<Work, TypedFailure, _, _>(|_, _| async { Ok(()) }, ReplaySafety::Never)
        .unwrap();
    let (second, runner) = second.build().unwrap();
    second
        .submit(MessageContext::new(
            origin(),
            Envelope::encode(&Work(2)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "second").unwrap();
    let stop_second = host.handle();
    let task_second = tokio::spawn(host.run());
    let states = wait(&second, |s| {
        s.len() == 1 && s[0].due >= Duration::from_secs(49)
    })
    .await;
    assert_eq!(states[0].attempts.handling, 0);
    tokio::time::advance(Duration::from_secs(49)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert_eq!(second.deliveries().unwrap()[0].attempts.handling, 0);
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    let states = second.deliveries().unwrap();
    assert_eq!(states[0].status, MemoryStatus::Completed);
    assert_eq!(states[0].attempts.handling, 1);
    stop_first.shutdown();
    stop_second.shutdown();
    task_first.await.unwrap().unwrap();
    task_second.await.unwrap().unwrap();
}

#[tokio::test]
async fn requeue_tail_includes_custody_accepted_while_the_failed_handler_was_running() {
    let mut policies = PolicySet::default();
    policies.global(rule(vec![Step::new(Action::Requeue)], Action::DeadLetter));
    let mut builder = PolicyQueueBuilder::new(
        "live-tail",
        8,
        Duration::from_secs(5),
        EndpointPolicies::new(policies),
    )
    .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let executions = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (signal, gate, calls) = (
        Arc::clone(&entered),
        Arc::clone(&release),
        Arc::clone(&executions),
    );
    builder
        .handle::<Work, TypedFailure, _, _>(
            move |work, _| {
                let signal = Arc::clone(&signal);
                let gate = Arc::clone(&gate);
                let calls = Arc::clone(&calls);
                let attempts = Arc::clone(&attempts);
                async move {
                    calls.lock().await.push(work.0);
                    if work.0 == 0 {
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            signal.notify_one();
                            gate.acquire().await.unwrap().forget();
                        }
                        Err(TypedFailure)
                    } else {
                        Ok(())
                    }
                }
            },
            ReplaySafety::Idempotent,
        )
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    queue
        .submit(MessageContext::new(
            origin(),
            Envelope::encode(&Work(0)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "live-tail").unwrap();
    let stop = host.handle();
    let task = tokio::spawn(host.run());
    entered.notified().await;
    queue
        .submit(MessageContext::new(
            origin(),
            Envelope::encode(&Work(1)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    release.add_permits(1);
    wait(&queue, |s| {
        s.len() == 2
            && s.iter().all(|d| {
                matches!(
                    d.status,
                    MemoryStatus::Completed | MemoryStatus::DeadLetter { .. }
                )
            })
    })
    .await;
    assert_eq!(*executions.lock().await, vec![0, 1, 0]);
    stop.shutdown();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn oversized_canonical_input_is_refused_before_custody_without_faulting_the_receiver() {
    let mut builder = PolicyQueueBuilder::new(
        "bounded-input",
        4,
        Duration::from_secs(1),
        EndpointPolicies::new(PolicySet::default()),
    )
    .unwrap();
    builder
        .handle::<Work, TypedFailure, _, _>(|_, _| async { Ok(()) }, ReplaySafety::Never)
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    let mut raw: serde_json::Value =
        serde_json::from_slice(&Envelope::encode(&Work(0)).unwrap().to_vec().unwrap()).unwrap();
    raw["payload"] = serde_json::Value::String("x".repeat(1_100_000));
    let oversized = Envelope::from_slice(
        &serde_json::to_vec(&raw).unwrap(),
        raddle::wire::EnvelopeLimits {
            max_bytes: 2_000_000,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        queue
            .submit(MessageContext::new(origin(), oversized, Utc::now()))
            .await
            .is_err()
    );
    queue
        .submit(MessageContext::new(
            origin(),
            Envelope::encode(&Work(1)).unwrap(),
            Utc::now(),
        ))
        .await
        .unwrap();
    let mut host = Host::new(Duration::from_secs(2)).unwrap();
    runner.attach(&mut host, "bounded-input").unwrap();
    let stop = host.handle();
    let task = tokio::spawn(host.run());
    wait(&queue, |s| {
        s.len() == 1 && s[0].status == MemoryStatus::Completed
    })
    .await;
    stop.shutdown();
    task.await.unwrap().unwrap();
}
