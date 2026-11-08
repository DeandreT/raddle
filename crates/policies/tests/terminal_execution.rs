//! Real terminal fault publication and independent missing-handler observers.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, EnvelopeLimits, WireMessage};
use raddle_policies::{
    Direction, DriverError, ExceptionNode, FailureDisposition, FailureOrigin, Fault, FaultOptions,
    FaultPublication, InMemoryEndpoint, MemoryDelivery, MemoryStatus, PolicyErrorCode,
    PolicyExecutor, PolicyExecutorDriver, PolicySet, TerminalFaultPublisher, Transition,
    UnknownMessageHandler, UnknownMessageOptions,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Serialize, Deserialize)]
struct Work(u32);
impl WireMessage for Work {
    const NAME: &'static str = "work";
    const VERSION: u32 = 1;
}

struct FaultTransport {
    seen: Arc<Mutex<Vec<Envelope>>>,
    fail: bool,
}
impl Transport for FaultTransport {
    async fn publish(&self, _: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.seen.lock().unwrap().push(envelope.clone());
        if self.fail {
            Err(TransportError::Transient)
        } else {
            Ok(())
        }
    }
}
#[tokio::test]
async fn failed_fault_publication_never_loses_dead_letter_or_replays_original() {
    let executor = PolicyExecutor::new(PolicySet::default());
    let mut endpoint = InMemoryEndpoint::new("source", Direction::Handling, 1).unwrap();
    endpoint
        .enqueue(MemoryDelivery::new(Envelope::encode(&Work(1)).unwrap()))
        .unwrap();
    let (_sender, mut stop) = watch::channel(false);
    endpoint
        .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
            Err(Box::new(std::io::Error::other("redacted")) as raddle_policies::PolicyError)
        })
        .await
        .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let publisher = TerminalFaultPublisher::new(
        FaultTransport {
            seen: seen.clone(),
            fail: true,
        },
        FaultOptions {
            enabled: true,
            include_discard: false,
        },
        "faults".into(),
    )
    .unwrap();
    let result = publisher
        .publish::<Work>(
            &endpoint.deliveries()[0],
            FailureOrigin::Handler,
            false,
            0,
            "source".into(),
            None,
            ExceptionNode {
                summary: "safe".into(),
                cause: None,
            },
        )
        .await;
    assert_eq!(result, FaultPublication::Failed);
    assert_eq!(
        endpoint.deliveries()[0].status,
        MemoryStatus::DeadLetter {
            replay_denied: false
        }
    );
    assert_eq!(endpoint.deliveries()[0].attempts.handling, 1);
    assert!(
        !endpoint
            .run_once(&executor, Duration::ZERO, &mut stop, |_| async {
                panic!("original must not replay");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
            .unwrap()
    );
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let fault: Fault<Work> = seen[0].decode().unwrap();
    assert_eq!(fault.message.0, 1);
    assert_eq!(fault.message_id, endpoint.deliveries()[0].envelope.id());
}

#[derive(Default)]
struct Driver {
    transitioned: Vec<Transition>,
}
impl PolicyExecutorDriver for Driver {
    async fn transition(&mut self, transition: Transition) -> Result<(), DriverError> {
        self.transitioned.push(transition);
        Ok(())
    }
    async fn pause_until(&mut self, _: Duration) -> Result<(), DriverError> {
        Ok(())
    }
}
#[tokio::test]
async fn unknown_observers_are_additive_failure_isolated_and_keep_exact_original_bytes() {
    for disposition in [FailureDisposition::Discard, FailureDisposition::DeadLetter] {
        let mut handler = UnknownMessageHandler::new(
            UnknownMessageOptions {
                disposition,
                tracking_capacity: 1,
            },
            EnvelopeLimits::default(),
        )
        .unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        for i in 0..3 {
            let calls = calls.clone();
            handler
                .observe(move |_| {
                    let calls = calls.clone();
                    Box::pin(async move {
                        calls.lock().unwrap().push(i);
                        if i == 1 { Err(PolicyErrorCode) } else { Ok(()) }
                    })
                })
                .unwrap();
        }
        let envelope = Envelope::encode(&Work(99)).unwrap();
        let mut original = envelope.to_vec().unwrap();
        original.push(b'\n');
        let mut driver = Driver::default();
        let report = handler.handle(&mut driver, &original).await.unwrap();
        assert_eq!(report.observer_failures, 1);
        assert_eq!(*calls.lock().unwrap(), [0, 1, 2]);
        assert_eq!(handler.records()[0].original_bytes(), original);
        assert_eq!(
            driver.transitioned[0],
            if disposition == FailureDisposition::Discard {
                Transition::Discard
            } else {
                Transition::DeadLetter {
                    replay_denied: false,
                }
            }
        );
        let next = Envelope::encode(&Work(100)).unwrap();
        handler
            .handle(&mut driver, &next.to_vec().unwrap())
            .await
            .unwrap();
        assert_eq!(handler.records().len(), 1);
        assert_eq!(handler.records()[0].envelope().id(), next.id());
        // The allowed random UUID may itself contain the payload digits. Check
        // the full identity-only representation so no payload field can leak.
        assert_eq!(
            format!("{:?}", handler.records()[0]),
            format!("UnknownRecord {{ message_id: {}, .. }}", next.id())
        );
    }
}

#[tokio::test]
async fn unknown_messages_settle_through_actual_memory_driver_without_handler_attempts() {
    let mut endpoint = InMemoryEndpoint::new("incoming", Direction::Handling, 1).unwrap();
    let envelope = Envelope::encode(&Work(10)).unwrap();
    let mut original = envelope.to_vec().unwrap();
    original.extend_from_slice(b"\n ");
    endpoint.enqueue(MemoryDelivery::new(envelope)).unwrap();
    let mut handler =
        UnknownMessageHandler::new(UnknownMessageOptions::default(), EnvelopeLimits::default())
            .unwrap();
    let report = endpoint
        .handle_unknown(&mut handler, &original, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.disposition, FailureDisposition::Discard);
    assert_eq!(endpoint.deliveries()[0].status, MemoryStatus::Discarded);
    assert_eq!(endpoint.deliveries()[0].attempts.handling, 0);
    assert_eq!(handler.records()[0].original_bytes(), original);
}
