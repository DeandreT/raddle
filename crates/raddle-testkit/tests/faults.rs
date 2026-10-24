//! Deterministic clock and transport fault contracts.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_testkit::{ManualClock, ScriptedTransport, SendOutcome};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Probe;
impl WireMessage for Probe {
    const NAME: &'static str = "test.probe";
    const VERSION: u32 = 1;
}

#[test]
fn clock_advance_is_monotonic_and_overflow_does_not_mutate() {
    let start = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
    let mut clock = ManualClock::new(start);
    assert!(clock.advance(Duration::from_secs(2)).unwrap() > start);
    let mut clock = ManualClock::new(DateTime::<Utc>::MAX_UTC);
    assert!(clock.advance(Duration::from_secs(1)).is_err());
    assert_eq!(clock.now(), DateTime::<Utc>::MAX_UTC);
}

#[tokio::test]
async fn confirmation_loss_preserves_accepted_duplicate_identities() {
    let script = ScriptedTransport::new([
        SendOutcome::FailBeforeAcceptance,
        SendOutcome::FailAfterAcceptance,
        SendOutcome::Accept,
    ])
    .unwrap();
    let envelope = Envelope::encode(&Probe).unwrap();
    for _ in 0..2 {
        assert_eq!(
            script.publish("out", &envelope).await,
            Err(TransportError::Transient)
        );
    }
    assert_eq!(script.publish("out", &envelope).await, Ok(()));
    let receipts = script.receipts().unwrap();
    assert_eq!(receipts.len(), 2);
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.message_id == envelope.id())
    );
    assert_eq!(
        script.publish("out", &envelope).await,
        Err(TransportError::Rejected)
    );
    assert_eq!(script.receipts().unwrap().len(), 2);
}
