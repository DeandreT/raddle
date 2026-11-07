//! Bounded restart, readiness, cancellation, and joined shutdown regressions.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use raddle_host::{
    Host, HostError, PanicPolicy, PollSettings, RestartPolicy, ServiceFailure, ServiceState,
};
use tokio::sync::{Notify, mpsc};

fn host() -> Host {
    Host::new(Duration::from_millis(100)).unwrap()
}
fn retries(count: u8, panics: PanicPolicy) -> RestartPolicy {
    RestartPolicy::new(
        count,
        Duration::from_millis(1),
        Duration::from_millis(4),
        panics,
    )
    .unwrap()
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn registration_is_inert_and_validates_all_bounds() {
    assert!(Host::new(Duration::ZERO).is_err());
    assert!(Host::new(Duration::MAX).is_err());
    assert!(
        RestartPolicy::new(
            33,
            Duration::from_millis(1),
            Duration::from_secs(1),
            PanicPolicy::Restart
        )
        .is_err()
    );
    assert!(
        RestartPolicy::new(
            1,
            Duration::ZERO,
            Duration::from_secs(1),
            PanicPolicy::Restart
        )
        .is_err()
    );
    assert!(
        RestartPolicy::new(
            1,
            Duration::from_secs(2),
            Duration::from_secs(1),
            PanicPolicy::Restart
        )
        .is_err()
    );
    assert!(
        RestartPolicy::new(
            1,
            Duration::from_secs(1),
            Duration::MAX,
            PanicPolicy::Restart
        )
        .is_err()
    );
    assert!(PollSettings::new(0, Duration::from_millis(1)).is_err());
    assert!(PollSettings::new(257, Duration::from_millis(1)).is_err());
    assert!(PollSettings::new(1, Duration::ZERO).is_err());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut host = host();
    for invalid in ["", "bad name", "Uppercase", "../bad", &"a".repeat(129)] {
        assert!(
            host.service(invalid, RestartPolicy::NEVER, |_| async { Ok(()) })
                .is_err()
        );
    }
    for i in 0..256 {
        let calls = Arc::clone(&calls);
        host.service(&format!("service-{i}"), RestartPolicy::NEVER, move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .unwrap();
    }
    assert!(
        host.service("extra", RestartPolicy::NEVER, |_| async { Ok(()) })
            .is_err()
    );
    assert!(!host.handle().snapshot().ready);
    let run = host.run();
    drop(run);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut host = self::host();
    host.service("duplicate", RestartPolicy::NEVER, |_| async { Ok(()) })
        .unwrap();
    assert!(
        host.service("duplicate", RestartPolicy::NEVER, |_| async { Ok(()) })
            .is_err()
    );
}

#[tokio::test]
async fn empty_and_already_stopped_hosts_start_nothing() {
    host().run().await.unwrap();
    let mut host = host();
    host.service("not-started", RestartPolicy::NEVER, |_| async {
        panic!("must not start")
    })
    .unwrap();
    let handle = host.handle();
    handle.shutdown();
    host.run().await.unwrap();
    assert!(!handle.snapshot().ready);
    assert_eq!(handle.snapshot().services[0].attempt, 0);
}

#[tokio::test]
async fn early_handles_observe_later_registrations_without_false_readiness() {
    let mut host = host();
    let handle = host.handle();
    host.service("ready", RestartPolicy::NEVER, |mut context| async move {
        context.set_ready(true);
        context.cancelled().await;
        Ok(())
    })
    .unwrap();
    host.service(
        "not-ready",
        RestartPolicy::NEVER,
        |mut context| async move {
            context.cancelled().await;
            Ok(())
        },
    )
    .unwrap();
    let run = tokio::spawn(host.run());
    until(|| handle.snapshot().services[0].state == ServiceState::Ready).await;
    assert_eq!(handle.snapshot().services.len(), 2);
    assert!(!handle.snapshot().ready);
    handle.shutdown();
    run.await.unwrap().unwrap();
}

#[tokio::test]
async fn task_creation_is_not_readiness_and_shutdown_joins_cleanup() {
    let finished = Arc::new(AtomicUsize::new(0));
    let mut host = host();
    let completed = Arc::clone(&finished);
    host.service("worker", RestartPolicy::NEVER, move |mut context| {
        let completed = Arc::clone(&completed);
        async move {
            context.cancelled().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    until(|| handle.snapshot().running).await;
    assert!(!handle.snapshot().ready);
    handle.shutdown();
    run.await.unwrap().unwrap();
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    assert!(!handle.snapshot().running);
    assert_eq!(handle.snapshot().services[0].state, ServiceState::Stopped);
}

#[tokio::test]
async fn every_restart_resets_readiness_and_stale_context_cannot_restore_it() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let exit_first = Arc::new(Notify::new());
    let (contexts, mut received) = mpsc::unbounded_channel();
    let mut host = host();
    let exit = Arc::clone(&exit_first);
    host.service(
        "worker",
        retries(1, PanicPolicy::FailFast),
        move |context| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            let contexts = contexts.clone();
            let exit = Arc::clone(&exit);
            async move {
                let mut shutdown = context.shutdown();
                contexts.send(context).unwrap();
                if attempt == 0 {
                    exit.notified().await;
                    Err(ServiceFailure::Transient)
                } else {
                    while !*shutdown.borrow_and_update() {
                        if shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                    Ok(())
                }
            }
        },
    )
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    let old = received.recv().await.unwrap();
    assert!(!handle.snapshot().ready);
    old.set_ready(true);
    assert!(handle.snapshot().ready);
    old.set_ready(false);
    assert!(!handle.snapshot().ready);
    old.set_ready(true);
    exit_first.notify_one();
    let new = received.recv().await.unwrap();
    assert_eq!(handle.snapshot().services[0].attempt, 2);
    assert!(!handle.snapshot().ready);
    old.set_ready(true);
    assert!(!handle.snapshot().ready);
    new.set_ready(true);
    assert!(handle.snapshot().ready);
    handle.shutdown();
    new.set_ready(true);
    assert!(!handle.snapshot().ready);
    run.await.unwrap().unwrap();
    new.set_ready(true);
    assert_eq!(handle.snapshot().services[0].state, ServiceState::Stopped);
}

#[tokio::test]
async fn transient_restarts_are_finite_and_success_does_not_restart() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut host = host();
    let called = Arc::clone(&calls);
    host.service("retry", retries(2, PanicPolicy::FailFast), move |_| {
        called.fetch_add(1, Ordering::SeqCst);
        async { Err(ServiceFailure::Transient) }
    })
    .unwrap();
    let handle = host.handle();
    assert!(matches!(
        host.run().await,
        Err(HostError::ServiceFailed {
            failure: ServiceFailure::Transient,
            ..
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(handle.snapshot().services[0].state, ServiceState::Failed);
    let mut host = self::host();
    host.service("success", retries(32, PanicPolicy::Restart), |_| async {
        Ok(())
    })
    .unwrap();
    let handle = host.handle();
    host.run().await.unwrap();
    assert_eq!(handle.snapshot().services[0].attempt, 1);
}

#[tokio::test]
async fn permanent_failure_never_restarts_and_joins_siblings() {
    let sibling_done = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Notify::new());
    let mut host = host();
    let sibling = Arc::clone(&sibling_done);
    let started = Arc::clone(&gate);
    host.service("sibling", RestartPolicy::NEVER, move |mut context| {
        let done = Arc::clone(&sibling);
        let started = Arc::clone(&started);
        async move {
            started.notify_one();
            context.cancelled().await;
            done.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    host.service("bad", retries(32, PanicPolicy::Restart), move |_| {
        let gate = Arc::clone(&gate);
        async move {
            gate.notified().await;
            Err(ServiceFailure::Permanent)
        }
    })
    .unwrap();
    let handle = host.handle();
    assert!(matches!(
        host.run().await,
        Err(HostError::ServiceFailed {
            failure: ServiceFailure::Permanent,
            ..
        })
    ));
    assert_eq!(sibling_done.load(Ordering::SeqCst), 1);
    assert_eq!(handle.snapshot().services[1].attempt, 1);
}

#[tokio::test]
async fn panic_recovery_requires_opt_in_and_is_bounded_even_for_factory_panics() {
    for (policy, expected) in [(PanicPolicy::FailFast, 1), (PanicPolicy::Restart, 3)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut host = host();
        let called = Arc::clone(&calls);
        host.service("panic", retries(2, policy), move |_| {
            called.fetch_add(1, Ordering::SeqCst);
            assert_eq!(1, 2, "factory panic");
            async { Ok(()) }
        })
        .unwrap();
        assert!(matches!(
            host.run().await,
            Err(HostError::ServiceFailed {
                failure: ServiceFailure::Panicked,
                ..
            })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), expected);
    }
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn deadline_abort_is_joined_before_return() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let mut host = Host::new(Duration::from_millis(5)).unwrap();
    let count = Arc::clone(&dropped);
    let notify = Arc::clone(&started);
    host.service("stuck", RestartPolicy::NEVER, move |_| {
        let count = Arc::clone(&count);
        let notify = Arc::clone(&notify);
        async move {
            let _guard = DropCount(count);
            notify.notify_one();
            std::future::pending().await
        }
    })
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    started.notified().await;
    handle.shutdown();
    assert!(matches!(
        run.await.unwrap(),
        Err(HostError::ShutdownTimeout { aborted: 1 })
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_run_aborts_children_and_clears_readiness() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut host = host();
    let count = Arc::clone(&dropped);
    host.service("cancelled", RestartPolicy::NEVER, move |context| {
        let count = Arc::clone(&count);
        async move {
            let _guard = DropCount(count);
            context.set_ready(true);
            std::future::pending().await
        }
    })
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    until(|| handle.snapshot().ready).await;
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    until(|| dropped.load(Ordering::SeqCst) == 1).await;
    assert!(!handle.snapshot().ready);
    assert_eq!(handle.snapshot().services[0].state, ServiceState::Stopped);
}

#[tokio::test]
async fn shutdown_interrupts_backoff_without_another_factory_call() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut host = host();
    let called = Arc::clone(&calls);
    let policy = RestartPolicy::new(
        2,
        Duration::from_secs(60),
        Duration::from_secs(60),
        PanicPolicy::Restart,
    )
    .unwrap();
    host.service("backoff", policy, move |_| {
        called.fetch_add(1, Ordering::SeqCst);
        async { Err(ServiceFailure::Transient) }
    })
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    until(|| handle.snapshot().services[0].state == ServiceState::Backoff).await;
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn zero_restart_budget_is_fail_fast() {
    let mut host = host();
    host.service("zero", RestartPolicy::NEVER, |_| async {
        Err(ServiceFailure::Transient)
    })
    .unwrap();
    let handle = host.handle();
    assert!(host.run().await.is_err());
    assert_eq!(handle.snapshot().services[0].attempt, 1);
}

#[tokio::test]
async fn errors_during_cooperative_shutdown_are_not_silently_successful() {
    let mut host = host();
    host.service(
        "drain-failure",
        RestartPolicy::NEVER,
        |mut context| async move {
            context.set_ready(true);
            context.cancelled().await;
            Err(ServiceFailure::Permanent)
        },
    )
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    until(|| handle.snapshot().ready).await;
    handle.shutdown();
    assert!(matches!(
        run.await.unwrap(),
        Err(HostError::ServiceFailed {
            failure: ServiceFailure::Permanent,
            ..
        })
    ));
    assert_eq!(handle.snapshot().services[0].state, ServiceState::Failed);
}
