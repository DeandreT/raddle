//! Deterministic portable assembly, custody-view and routing contracts.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use raddle_batching::{
    Assembler, BatchKey, Combination, Decision, Diagnostic, ExecutionQueue, Member, MemberContext,
    Options, Resolution, ResolutionError, RoutePlan, RoutingError,
};
use uuid::Uuid;

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}
fn key(tenant: &str, group: Option<&str>) -> BatchKey {
    BatchKey::new(tenant, group.map(str::to_owned)).unwrap()
}
fn member(value: u32, coalesce: Option<&str>, expiry: Option<DateTime<Utc>>) -> Member<u32> {
    let id = Uuid::new_v4();
    Member {
        context: MemberContext {
            delivery_id: id,
            id,
            attempt: 3,
            sent_at: now() - TimeDelta::seconds(1),
            expires_at: expiry,
        },
        value,
        coalesce: coalesce.map(str::to_owned),
    }
}

#[test]
fn defaults_size_time_empty_and_pending_through_execution() {
    let options = Options::default();
    assert_eq!(options.size(), 100);
    assert_eq!(options.wait(), Duration::from_millis(250));
    let mut assembler = Assembler::new(options);
    assert!(assembler.flush_due(now()).0.is_empty());
    for value in 0..99 {
        assembler
            .push(key("a", None), member(value, None, None), now())
            .unwrap();
    }
    assert!(
        assembler
            .flush_due(now() + TimeDelta::milliseconds(249))
            .0
            .is_empty()
    );
    let (partial, expired) = assembler.flush_due(now() + TimeDelta::milliseconds(250));
    assert!(expired.is_empty());
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0].members().len(), 99);
    assert_eq!(assembler.accumulating(), 0);
    assert_eq!(assembler.pending(), 99);
    drop(partial); // Volatile cleanup, NOT an implied transport acknowledgement.
    assert_eq!(assembler.pending(), 0);
    for value in 0..100 {
        assembler
            .push(key("a", None), member(value, None, None), now())
            .unwrap();
    }
    assert_eq!(assembler.flush_due(now()).0[0].members().len(), 100);
    assert_eq!(assembler.pending(), 0);
}

#[test]
fn concurrent_first_use_split_tenants_groups_and_custom_shape() {
    let assembler = Arc::new(Mutex::new(Assembler::new(Options::default())));
    let threads: Vec<_> = (0..20)
        .map(|i| {
            let assembler = Arc::clone(&assembler);
            std::thread::spawn(move || {
                assembler
                    .lock()
                    .unwrap()
                    .push(
                        key(
                            if i % 2 == 0 { "a" } else { "b" },
                            Some(if i < 10 { "x" } else { "y" }),
                        ),
                        member(i, None, None),
                        now(),
                    )
                    .unwrap();
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let mut assembler = assembler.lock().unwrap();
    let (batches, _) = assembler.drain(now());
    assert_eq!(batches.len(), 4);
    struct Shape {
        count: usize,
        sum: u32,
    }
    for batch in &batches {
        let shape = Shape {
            count: batch.input().len(),
            sum: batch.input().iter().map(|member| member.value).sum(),
        };
        assert_eq!(shape.count, 5);
        assert!(shape.sum > 0);
    }
    assert_eq!(assembler.pending(), 20);
    drop(batches);
    assert_eq!(assembler.pending(), 0);
}

#[test]
fn capacity_full_bucket_and_expired_admission_return_owned_originals() {
    let options = Options::new(2, Duration::from_millis(10), 2).unwrap();
    let mut assembler = Assembler::new(options);
    let expired = assembler
        .push(key("a", None), member(9, None, Some(now())), now())
        .unwrap_err();
    assert_eq!(expired.reason, "expired");
    assert_eq!(expired.member.value, 9);
    assembler
        .push(key("a", None), member(1, None, None), now())
        .unwrap();
    assembler
        .push(key("a", None), member(2, None, None), now())
        .unwrap();
    let rejected = assembler
        .push(key("b", None), member(3, None, None), now())
        .unwrap_err();
    assert_eq!(rejected.reason, "capacity");
    let (batches, _) = assembler.drain(now());
    assert_eq!(assembler.pending(), 2);
    assert_eq!(
        assembler
            .push(key("b", None), rejected.member, now())
            .unwrap_err()
            .reason,
        "capacity"
    );
    drop(batches);
    assert_eq!(assembler.pending(), 0);
    assert!(Options::new(0, Duration::from_millis(1), 1).is_err());
    assert!(Options::new(2, Duration::from_nanos(1), 2).is_err());
}

#[test]
fn coalescing_retains_all_original_context_and_poison_custody() {
    let mut assembler = Assembler::new(Options::default());
    for (value, coalesce) in [(1, "A"), (10, "B"), (2, "A")] {
        assembler
            .push(key("a", None), member(value, Some(coalesce), None), now())
            .unwrap();
    }
    let batch = assembler.drain(now()).0.remove(0);
    assert_eq!(
        batch
            .input()
            .iter()
            .map(|member| member.value)
            .collect::<Vec<_>>(),
        [10, 2]
    );
    assert_eq!(batch.members().len(), 3);
    assert!(
        batch
            .members()
            .iter()
            .all(|member| member.context.attempt == 3 && member.context.sent_at < now())
    );
    let poison = batch.members()[2].context.delivery_id;
    let b = batch.members()[1].context.delivery_id;
    let resolution = Resolution::named(
        batch.members(),
        [
            (poison, Decision::Poison("bad-a".into())),
            (b, Decision::Commit),
        ],
    )
    .unwrap();
    assert_eq!(
        resolution
            .decisions()
            .values()
            .filter(|decision| matches!(decision, Decision::Poison(_)))
            .count(),
        2
    );
    assert_eq!(resolution.decisions()[&b], Decision::Commit);
    assert_eq!(
        Resolution::named(
            batch.members(),
            [
                (poison, Decision::Commit),
                (batch.members()[0].context.delivery_id, Decision::Retry)
            ]
        )
        .unwrap_err(),
        ResolutionError::Coalescing
    );
    assert_eq!(
        Resolution::named(batch.members(), [(Uuid::new_v4(), Decision::Commit)]).unwrap_err(),
        ResolutionError::Unknown
    );
}

#[test]
fn preassembly_expiry_individual_postassembly_latest_or_unbounded() {
    let mut assembler = Assembler::new(Options::default());
    assembler
        .push(
            key("a", None),
            member(1, None, Some(now() + TimeDelta::milliseconds(10))),
            now(),
        )
        .unwrap();
    assembler
        .push(
            key("a", None),
            member(2, None, Some(now() + TimeDelta::seconds(1))),
            now(),
        )
        .unwrap();
    let (batches, expired) = assembler.flush_due(now() + TimeDelta::milliseconds(250));
    assert_eq!(expired.len(), 1);
    assert_eq!(batches[0].members()[0].value, 2);
    let mut assembler = Assembler::new(Options::default());
    assembler
        .push(
            key("a", None),
            member(1, None, Some(now() + TimeDelta::milliseconds(10))),
            now(),
        )
        .unwrap();
    assembler
        .push(
            key("a", None),
            member(2, None, Some(now() + TimeDelta::seconds(1))),
            now(),
        )
        .unwrap();
    let batch = assembler.drain(now()).0.remove(0);
    assert!(!batch.expired(now() + TimeDelta::milliseconds(500)));
    assert!(batch.expired(now() + TimeDelta::seconds(1)));
    assert_eq!(batch.members().len(), 2); // Partial expiry does not shed assembled originals.
    assembler
        .push(key("b", None), member(3, None, None), now())
        .unwrap();
    assert_eq!(assembler.drain(now()).0[0].expires_at(), None);
}

#[test]
fn classic_shadow_and_assertion_separated_independent_destinations() {
    let batches = || {
        vec![
            ExecutionQueue::Independent("first-batch".into()),
            ExecutionQueue::Independent("second-batch".into()),
        ]
    };
    let classic = RoutePlan::build(
        Combination::Classic,
        Some("direct".into()),
        batches(),
        false,
        false,
    )
    .unwrap();
    assert_eq!(classic.destinations(), ["direct"]);
    assert_eq!(
        classic.diagnostics(),
        [
            Diagnostic::Shadowed("first-batch".into()),
            Diagnostic::Shadowed("second-batch".into())
        ]
    );
    assert_eq!(
        RoutePlan::build(
            Combination::Classic,
            Some("direct".into()),
            batches(),
            true,
            false
        )
        .unwrap_err(),
        RoutingError::Conflict
    );
    let separated = RoutePlan::build(
        Combination::Separated,
        Some("direct".into()),
        batches(),
        true,
        false,
    )
    .unwrap();
    assert_eq!(
        separated.destinations(),
        ["direct", "first-batch", "second-batch"]
    );
    assert_eq!(
        RoutePlan::build(Combination::Separated, None, batches(), false, true).unwrap_err(),
        RoutingError::Unsequenced
    );
    assert_eq!(
        RoutePlan::build(
            Combination::Separated,
            Some("first-batch".into()),
            batches(),
            false,
            false
        )
        .unwrap_err(),
        RoutingError::Destination
    );
    assert_eq!(
        RoutePlan::build(Combination::Classic, None, batches(), false, false)
            .unwrap()
            .destinations()
            .len(),
        2
    );
}

#[test]
fn named_poison_first_middle_last_corrected_only_uncommitted_retry() {
    for poison in 0..3 {
        let members: Vec<_> = (0..3).map(|value| member(value, None, None)).collect();
        let decisions = members.iter().enumerate().map(|(i, member)| {
            (
                member.context.delivery_id,
                if i == poison {
                    Decision::Retry
                } else {
                    Decision::Commit
                },
            )
        });
        let resolution = Resolution::named(&members, decisions).unwrap();
        let retried: Vec<_> = members
            .into_iter()
            .filter(|member| resolution.decisions()[&member.context.delivery_id] == Decision::Retry)
            .collect();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].value, poison as u32);
        assert_eq!(Resolution::committed(&retried).decisions().len(), 1);
    }
}

#[test]
fn registry_type_total_listener_counts_keep_execution_but_exclude_in_process_origin() {
    let registry = raddle_batching::Registry::default();
    let mut listener = registry
        .assembler::<u32>(Some("listener"), Options::default())
        .unwrap();
    let mut in_process = registry.assembler::<u32>(None, Options::default()).unwrap();
    listener
        .push(key("a", None), member(1, None, None), now())
        .unwrap();
    listener
        .push(
            key("a", None),
            member(2, None, Some(now() + TimeDelta::milliseconds(1))),
            now(),
        )
        .unwrap();
    in_process
        .push(key("a", None), member(3, None, None), now())
        .unwrap();
    let (batches, expired) = listener.drain(now() + TimeDelta::milliseconds(2));
    assert_eq!(expired[0].member().value, 2);
    let snapshot = registry.snapshot().unwrap();
    assert_eq!(snapshot.total, 3);
    assert_eq!(snapshot.by_type.len(), 1);
    assert_eq!(snapshot.by_type[0].pending, 3);
    assert_eq!(snapshot.by_listener["listener"], 2);
    drop(listener);
    drop(in_process); // Dropped accumulation must not leave phantom counts.
    assert_eq!(registry.snapshot().unwrap().total, 2);
    drop(expired); // Caller has settled the individual expiration.
    assert_eq!(registry.snapshot().unwrap().total, 1);
    drop(batches); // Caller has settled or cancelled volatile execution.
    assert_eq!(registry.snapshot().unwrap().total, 0);
}
