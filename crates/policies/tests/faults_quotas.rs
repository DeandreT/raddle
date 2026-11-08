//! Bounded fault metadata and shared in-process quota admission.
#![allow(clippy::unwrap_used)]

use std::io;
use std::time::Duration;

use raddle::wire::{Envelope, WireMessage};
use raddle_policies::{
    AttemptCounters, ExceptionNode, FailureDisposition, FailureOrigin, Fault, FaultOptions, Quota,
    QuotaAdmission, QuotaKind, ScopedQuotas, UnknownMessageOptions,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Work(u32);
impl WireMessage for Work {
    const NAME: &'static str = "work";
    const VERSION: u32 = 1;
}

#[test]
fn terminal_fault_bypass_recursion_redaction_and_original_payload() {
    let envelope = Envelope::encode(&Work(42))
        .unwrap()
        .with_header("source".into(), "not-trusted-attribution".into())
        .unwrap();
    let options = FaultOptions {
        enabled: true,
        include_discard: false,
    };
    for disposition in [FailureDisposition::DeadLetter, FailureDisposition::Expired] {
        assert!(options.should_emit(disposition, FailureOrigin::Handler, false, &envelope));
        for origin in [
            FailureOrigin::Sending,
            FailureOrigin::Cryptography,
            FailureOrigin::UnknownMessage,
        ] {
            assert!(!options.should_emit(disposition, origin, false, &envelope));
        }
        assert!(!options.should_emit(disposition, FailureOrigin::Handler, true, &envelope));
    }
    assert!(!FaultOptions::default().should_emit(
        FailureDisposition::DeadLetter,
        FailureOrigin::Handler,
        false,
        &envelope
    ));
    assert!(!options.should_emit(
        FailureDisposition::Retry,
        FailureOrigin::Handler,
        false,
        &envelope
    ));
    assert!(!options.should_emit(
        FailureDisposition::Discard,
        FailureOrigin::Handler,
        false,
        &envelope
    ));
    assert!(
        FaultOptions {
            include_discard: true,
            ..options
        }
        .should_emit(
            FailureDisposition::Discard,
            FailureOrigin::Handler,
            false,
            &envelope
        )
    );
    #[derive(Debug)]
    struct Cycle;
    impl std::fmt::Display for Cycle {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("must not leak")
        }
    }
    impl std::error::Error for Cycle {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self)
        }
    }
    let exception = ExceptionNode::capture(&Cycle, &|_| "é".repeat(1000));
    let mut depth = 0;
    let mut node = Some(&exception);
    while let Some(current) = node {
        depth += 1;
        assert_eq!(current.summary.len(), 1024);
        node = current.cause.as_deref();
    }
    assert_eq!(depth, 10);
    let fault = Fault::<Work>::from_envelope(
        &envelope,
        AttemptCounters {
            handling: 3,
            sending: 1,
        },
        100,
        "configured-source".into(),
        Some("application-resolved-tenant".into()),
        FailureDisposition::DeadLetter,
        exception,
    )
    .unwrap();
    assert_eq!(fault.message.0, 42);
    assert_eq!(fault.message_id, envelope.id());
    assert_eq!(fault.source, "configured-source");
    assert_eq!(fault.attempts.handling, 3);
    assert_eq!(fault.attempts.sending, 1);
    assert!(!format!("{fault:?}").contains("application-resolved"));
    let wire = Envelope::encode(&fault).unwrap();
    assert!(!options.should_emit(
        FailureDisposition::DeadLetter,
        FailureOrigin::Handler,
        false,
        &wire
    ));
    assert_eq!(
        UnknownMessageOptions::default().disposition,
        FailureDisposition::Discard
    );
    let redacted = ExceptionNode::capture(&io::Error::other("sensitive"), &|_| "redacted".into());
    assert!(
        !serde_json::to_string(&redacted)
            .unwrap()
            .contains("sensitive")
    );
}

fn granted(admission: QuotaAdmission) -> bool {
    matches!(admission, QuotaAdmission::Granted(_))
}

#[test]
fn bounded_time_quotas_have_exact_boundaries_and_independent_endpoint_state() {
    let window = Duration::from_secs(10);
    for kind in [
        QuotaKind::FixedWindow { permits: 2, window },
        QuotaKind::SlidingWindow { permits: 2, window },
        QuotaKind::TokenBucket {
            capacity: 2,
            refill: 2,
            period: window,
        },
    ] {
        let quota = Quota::new(kind).unwrap();
        assert!(granted(quota.acquire(Duration::ZERO).unwrap()));
        assert!(granted(quota.acquire(Duration::ZERO).unwrap()));
        assert!(
            matches!(quota.acquire(Duration::from_secs(9)).unwrap(), QuotaAdmission::Limited { retry_at: Some(t) } if t == window)
        );
        assert!(granted(quota.acquire(window).unwrap()));
        assert!(quota.acquire(Duration::ZERO).is_err());
    }
    let sliding = Quota::new(QuotaKind::SlidingWindow { permits: 1, window }).unwrap();
    assert!(granted(sliding.acquire(Duration::from_secs(9)).unwrap()));
    assert!(
        matches!(sliding.acquire(Duration::from_secs(10)).unwrap(), QuotaAdmission::Limited { retry_at: Some(t) } if t == Duration::from_secs(19))
    );
    let token = Quota::new(QuotaKind::TokenBucket {
        capacity: 2,
        refill: 1,
        period: window,
    })
    .unwrap();
    assert!(granted(token.acquire(Duration::ZERO).unwrap()));
    assert!(granted(token.acquire(Duration::ZERO).unwrap()));
    assert!(granted(token.acquire(Duration::from_secs(1000)).unwrap()));
    assert!(granted(token.acquire(Duration::from_secs(1000)).unwrap()));
    assert!(matches!(
        token.acquire(Duration::from_secs(1000)).unwrap(),
        QuotaAdmission::Limited { .. }
    ));
    let mut scoped = ScopedQuotas::default();
    scoped
        .message(
            "work".into(),
            Quota::new(QuotaKind::FixedWindow { permits: 1, window }).unwrap(),
        )
        .unwrap();
    scoped
        .endpoint(
            "a".into(),
            Quota::new(QuotaKind::FixedWindow { permits: 2, window }).unwrap(),
        )
        .unwrap();
    assert!(granted(
        scoped
            .select("a", "work")
            .unwrap()
            .acquire(Duration::ZERO)
            .unwrap()
    ));
    assert!(granted(
        scoped
            .select("a", "work")
            .unwrap()
            .acquire(Duration::ZERO)
            .unwrap()
    ));
    assert!(granted(
        scoped
            .select("b", "work")
            .unwrap()
            .acquire(Duration::ZERO)
            .unwrap()
    ));
    assert!(matches!(
        scoped
            .select("a", "work")
            .unwrap()
            .acquire(Duration::ZERO)
            .unwrap(),
        QuotaAdmission::Limited { .. }
    ));
    assert!(scoped.select("unconfigured", "unknown").is_none());
}

#[test]
fn concurrency_permits_release_on_drop_and_clones_share_capacity() {
    let quota = Quota::new(QuotaKind::Concurrency { permits: 1 }).unwrap();
    let other = quota.clone();
    let first = quota.acquire(Duration::ZERO).unwrap();
    assert!(matches!(
        other.acquire(Duration::ZERO).unwrap(),
        QuotaAdmission::Limited { retry_at: None }
    ));
    drop(first);
    assert!(granted(other.acquire(Duration::ZERO).unwrap()));
    assert!(
        Quota::new(QuotaKind::SlidingWindow {
            permits: 1_000_001,
            window: Duration::from_secs(1)
        })
        .is_err()
    );
    assert!(
        Quota::new(QuotaKind::FixedWindow {
            permits: 1,
            window: Duration::ZERO
        })
        .is_err()
    );
}

#[test]
fn concurrent_callers_cannot_overrun_shared_quota() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    let quota = Quota::new(QuotaKind::Concurrency { permits: 3 }).unwrap();
    let attempted = Arc::new(Barrier::new(8));
    let admitted = Arc::new(AtomicUsize::new(0));
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let quota = quota.clone();
            let attempted = attempted.clone();
            let admitted = admitted.clone();
            std::thread::spawn(move || {
                let result = quota.acquire(Duration::ZERO).unwrap();
                if matches!(&result, QuotaAdmission::Granted(_)) {
                    admitted.fetch_add(1, Ordering::SeqCst);
                }
                attempted.wait();
                drop(result);
            })
        })
        .collect();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(admitted.load(Ordering::SeqCst), 3);
    assert!(granted(quota.acquire(Duration::ZERO).unwrap()));
}
