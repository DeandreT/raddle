//! Direct policy-driver admission cannot silently claim grouped execution.
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use raddle::grouping::{Dispatch, GroupId};
use raddle::wire::{Envelope, WireMessage};
use raddle_policies::{Direction, DriverError, InMemoryEndpoint, MemoryDelivery, PolicyExecutor};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Work;
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}

#[tokio::test]
async fn direct_enqueue_refuses_without_retaining_identity_or_starting_an_attempt() {
    let mut endpoint = InMemoryEndpoint::new("policy", Direction::Handling, 4).unwrap();
    let calls = AtomicUsize::new(0);
    let original = Envelope::encode(&Work).unwrap();
    for group in [
        None,
        Some(GroupId::new("")),
        Some(GroupId::new("secret-group")),
    ] {
        let envelope = original
            .clone()
            .with_dispatch(Dispatch::resolved(group, None))
            .unwrap();
        assert!(matches!(
            endpoint.enqueue(MemoryDelivery::new(envelope)),
            Err(DriverError::Invalid)
        ));
        assert!(endpoint.deliveries().is_empty());
        let (_sender, mut stop) = tokio::sync::watch::channel(false);
        assert!(
            !endpoint
                .run_joined(
                    &PolicyExecutor::new(Default::default()),
                    Duration::ZERO,
                    &mut stop,
                    |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async { Ok(Ok(())) }
                    }
                )
                .await
                .unwrap()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // Rejection did not consume capacity or retain the same message identity.
    endpoint.enqueue(MemoryDelivery::new(original)).unwrap();
    assert_eq!(endpoint.deliveries().len(), 1);
    assert_eq!(endpoint.deliveries()[0].attempts.handling, 0);
}
