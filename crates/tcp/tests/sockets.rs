//! Real-socket framing, custody, failure isolation and lifecycle contracts.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, HashMap};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_tcp::{Consumer, Delivery, Endpoint, Error, Limits, Peer, Publisher, Receipt, Receiver};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize, Serialize)]
struct Command {
    value: u32,
}

impl WireMessage for Command {
    const NAME: &'static str = "tcp.command";
    const VERSION: u32 = 1;
}

#[derive(Debug, Deserialize, Serialize)]
struct Answer {
    value: u32,
}

impl WireMessage for Answer {
    const NAME: &'static str = "tcp.answer";
    const VERSION: u32 = 1;
}

fn envelope(value: u32) -> Envelope {
    Envelope::encode(&Command { value }).unwrap()
}

fn address() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

fn publisher(address: SocketAddr, limits: Limits) -> Publisher {
    Publisher::try_new(
        BTreeMap::from([(
            "commands".into(),
            Endpoint::try_new(address.ip().to_string(), address.port()).unwrap(),
        )]),
        limits,
    )
    .unwrap()
}

struct Running {
    address: SocketAddr,
    cancellation: CancellationToken,
    task: JoinHandle<Result<(), Error>>,
}

impl Running {
    async fn start<C: Consumer>(consumer: Arc<C>, limits: Limits) -> Self {
        let receiver = Receiver::bind(address(), limits).await.unwrap();
        let address = receiver.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(receiver.run(consumer, cancellation.clone()));
        Self {
            address,
            cancellation,
            task,
        }
    }

    async fn stop(self) {
        self.cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[derive(Default)]
struct Custody {
    failing: Mutex<Option<u32>>,
    attempts: Mutex<Vec<u32>>,
    accepted: Mutex<HashMap<String, u32>>,
    received: Mutex<Vec<Vec<u8>>>,
    reply: AtomicBool,
}

impl Consumer for Custody {
    async fn accept(&self, _peer: Peer, envelope: Envelope) -> Delivery {
        self.received
            .lock()
            .unwrap()
            .push(envelope.to_vec().unwrap());
        let Ok(command) = envelope.decode::<Command>() else {
            return Delivery::Rejected;
        };
        self.attempts.lock().unwrap().push(command.value);
        if *self.failing.lock().unwrap() == Some(command.value) {
            return Delivery::Retry;
        }
        self.accepted
            .lock()
            .unwrap()
            .entry(envelope.id().to_string())
            .or_insert(command.value);
        if self.reply.load(Ordering::SeqCst) {
            Delivery::Reply(
                Envelope::encode(&Answer {
                    value: command.value,
                })
                .unwrap()
                .correlated(envelope.id(), envelope.id())
                .unwrap(),
            )
        } else {
            Delivery::Accepted
        }
    }
}

#[tokio::test]
async fn first_middle_last_failures_do_not_block_valid_work_or_replay_successes() {
    for failing in [1, 2, 3] {
        let custody = Arc::new(Custody::default());
        *custody.failing.lock().unwrap() = Some(failing);
        let running = Running::start(Arc::clone(&custody), Limits::default()).await;
        let publisher = publisher(running.address, Limits::default());
        let envelopes = [envelope(1), envelope(2), envelope(3)];
        let results = publisher
            .publish_batch("commands", &envelopes)
            .await
            .unwrap();
        for (index, result) in results.iter().enumerate() {
            if index + 1 == failing as usize {
                assert!(matches!(result, Err(Error::Uncertain)));
            } else {
                assert!(matches!(result, Ok(Receipt::Accepted)));
            }
        }
        assert_eq!(custody.accepted.lock().unwrap().len(), 2);
        *custody.failing.lock().unwrap() = None;
        publisher
            .publish("commands", &envelopes[failing as usize - 1])
            .await
            .unwrap();
        assert_eq!(custody.accepted.lock().unwrap().len(), 3);
        let mut expected = vec![1, 2, 3];
        expected.push(failing);
        assert_eq!(*custody.attempts.lock().unwrap(), expected);
        running.stop().await;
    }
}

#[tokio::test]
async fn native_request_reply_checks_contract_and_preserves_request_identity() {
    let custody = Arc::new(Custody::default());
    custody.reply.store(true, Ordering::SeqCst);
    let running = Running::start(Arc::clone(&custody), Limits::default()).await;
    let publisher = publisher(running.address, Limits::default());
    let request = envelope(42)
        .with_header("application-tag".into(), "test".into())
        .unwrap();
    let response = publisher
        .request("commands", &request, Answer::NAME)
        .await
        .unwrap();
    assert_eq!(
        custody.received.lock().unwrap()[0],
        request.to_vec().unwrap()
    );
    assert_eq!(response.decode::<Answer>().unwrap().value, 42);
    assert_eq!(response.correlation_id(), Some(request.id()));
    assert_eq!(response.causation_id(), Some(request.id()));
    assert!(
        custody
            .accepted
            .lock()
            .unwrap()
            .contains_key(&request.id().to_string())
    );
    assert!(matches!(
        publisher
            .request("commands", &envelope(4), "wrong.contract")
            .await,
        Err(Error::Reply)
    ));
    running.stop().await;
}

async fn start_raw_batch(stream: &mut TcpStream, count: u16) {
    stream.write_all(b"RDTC\x01").await.unwrap();
    stream.write_u16(count).await.unwrap();
}

async fn write_raw(stream: &mut TcpStream, bytes: &[u8]) {
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(bytes).await.unwrap();
}

#[tokio::test]
async fn malformed_framed_items_are_refused_at_every_position_without_losing_alignment() {
    for invalid in 0..3 {
        let custody = Arc::new(Custody::default());
        let running = Running::start(Arc::clone(&custody), Limits::default()).await;
        let mut stream = TcpStream::connect(running.address).await.unwrap();
        start_raw_batch(&mut stream, 3).await;
        for position in 0..3 {
            let bytes = if position == invalid {
                b"not-an-envelope".to_vec()
            } else {
                envelope(position).to_vec().unwrap()
            };
            write_raw(&mut stream, &bytes).await;
            assert_eq!(
                stream.read_u8().await.unwrap(),
                if position == invalid { 2 } else { 0 }
            );
        }
        assert_eq!(custody.accepted.lock().unwrap().len(), 2);
        running.stop().await;
    }
}

#[tokio::test]
async fn partial_reads_oversized_lengths_and_truncated_connections_remain_bounded() {
    let custody = Arc::new(Custody::default());
    let running = Running::start(Arc::clone(&custody), Limits::default()).await;
    let bytes = envelope(7).to_vec().unwrap();
    let mut framed = b"RDTC\x01\x00\x01".to_vec();
    framed.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    framed.extend_from_slice(&bytes);
    let mut stream = TcpStream::connect(running.address).await.unwrap();
    for byte in framed {
        stream.write_u8(byte).await.unwrap();
    }
    assert_eq!(stream.read_u8().await.unwrap(), 0);

    let mut oversized = TcpStream::connect(running.address).await.unwrap();
    start_raw_batch(&mut oversized, 1).await;
    oversized.write_u32(u32::MAX).await.unwrap();
    assert_eq!(oversized.read(&mut [0]).await.unwrap(), 0);

    let mut truncated = TcpStream::connect(running.address).await.unwrap();
    start_raw_batch(&mut truncated, 1).await;
    truncated.write_u32(1024).await.unwrap();
    truncated.write_all(b"partial").await.unwrap();
    truncated.shutdown().await.unwrap();
    assert_eq!(truncated.read(&mut [0]).await.unwrap(), 0);
    publisher(running.address, Limits::default())
        .publish("commands", &envelope(8))
        .await
        .unwrap();
    assert_eq!(custody.accepted.lock().unwrap().len(), 2);
    running.stop().await;
}

#[tokio::test]
async fn preflight_invalid_messages_do_not_prevent_valid_siblings() {
    let limits = Limits {
        envelope: raddle::wire::EnvelopeLimits {
            max_bytes: 512,
            max_headers: 0,
        },
        ..Limits::default()
    };
    let custody = Arc::new(Custody::default());
    let running = Running::start(Arc::clone(&custody), limits).await;
    let invalid = envelope(2)
        .with_header("test".into(), "too-many".into())
        .unwrap();
    let results = publisher(running.address, limits)
        .publish_batch("commands", &[envelope(1), invalid, envelope(3)])
        .await
        .unwrap();
    assert!(matches!(results[0], Ok(Receipt::Accepted)));
    assert!(matches!(results[1], Err(Error::Rejected)));
    assert!(matches!(results[2], Ok(Receipt::Accepted)));
    assert_eq!(*custody.attempts.lock().unwrap(), [1, 3]);
    running.stop().await;
}

#[tokio::test]
async fn lost_ack_retains_identity_and_does_not_automatically_resend() {
    let listener = TcpListener::bind(address()).await.unwrap();
    let publisher = publisher(listener.local_addr().unwrap(), Limits::default());
    let accepted = Arc::new(Mutex::new(HashMap::<String, u32>::new()));
    let state = Arc::clone(&accepted);
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0; 7];
            stream.read_exact(&mut header).await.unwrap();
            assert_eq!(&header, b"RDTC\x01\x00\x01");
            let length = stream.read_u32().await.unwrap();
            let mut bytes = vec![0; length as usize];
            stream.read_exact(&mut bytes).await.unwrap();
            let envelope = Envelope::from_slice(&bytes, Default::default()).unwrap();
            state
                .lock()
                .unwrap()
                .entry(envelope.id().to_string())
                .or_insert(1);
            if attempt == 1 {
                stream.write_u8(0).await.unwrap();
            }
        }
    });
    let envelope = envelope(1);
    assert!(matches!(
        publisher.publish("commands", &envelope).await,
        Err(TransportError::Transient)
    ));
    assert_eq!(accepted.lock().unwrap().len(), 1);
    publisher.publish("commands", &envelope).await.unwrap();
    server.await.unwrap();
    assert_eq!(accepted.lock().unwrap().len(), 1);
}

struct Hanging {
    entered: Notify,
    dropped: Arc<AtomicBool>,
}

struct DropProof(Arc<AtomicBool>);
impl Drop for DropProof {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Consumer for Hanging {
    async fn accept(&self, _: Peer, _: Envelope) -> Delivery {
        let _guard = DropProof(Arc::clone(&self.dropped));
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn capacity_is_finite_and_shutdown_joins_inflight_custody() {
    let custody = Arc::new(Hanging {
        entered: Notify::new(),
        dropped: Arc::new(AtomicBool::new(false)),
    });
    let limits = Limits {
        connections: 1,
        ..Limits::default()
    };
    let running = Running::start(Arc::clone(&custody), limits).await;
    let publisher = Arc::new(publisher(running.address, limits));
    let first = Arc::clone(&publisher);
    let sending = tokio::spawn(async move { first.publish("commands", &envelope(1)).await });
    custody.entered.notified().await;
    assert!(matches!(
        publisher.publish("commands", &envelope(2)).await,
        Err(TransportError::Transient)
    ));
    running.stop().await;
    assert!(custody.dropped.load(Ordering::SeqCst));
    assert!(matches!(
        sending.await.unwrap(),
        Err(TransportError::Transient)
    ));
}

#[tokio::test]
async fn total_deadline_drops_stalled_consumer_and_releases_capacity() {
    let custody = Arc::new(Hanging {
        entered: Notify::new(),
        dropped: Arc::new(AtomicBool::new(false)),
    });
    let limits = Limits {
        connections: 1,
        ..Limits::default()
    };
    let running = Running::start(Arc::clone(&custody), limits).await;
    let publisher = Arc::new(publisher(running.address, limits));
    let sending = Arc::clone(&publisher);
    let sent = tokio::spawn(async move { sending.publish("commands", &envelope(1)).await });
    tokio::time::timeout(Duration::from_secs(5), custody.entered.notified())
        .await
        .unwrap();
    // Establish the actual socket and custody future before advancing a virtual
    // deadline, avoiding a scheduler-speed assumption on a busy test host.
    tokio::time::pause();
    tokio::time::advance(limits.deadline + Duration::from_secs(1)).await;
    assert!(matches!(
        sent.await.unwrap(),
        Err(TransportError::Transient)
    ));
    // Stop joins all remaining workers rather than using timing as cleanup proof.
    running.stop().await;
    assert!(custody.dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn invalid_configuration_and_untrusted_destinations_are_rejected_locally() {
    assert!(Endpoint::try_new("https://example.test", 80).is_err());
    assert!(Endpoint::try_new("localhost", 0).is_err());
    assert!(Publisher::try_new(BTreeMap::new(), Limits::default()).is_err());
    assert!(
        Receiver::bind(
            address(),
            Limits {
                batch_messages: 257,
                ..Limits::default()
            }
        )
        .await
        .is_err()
    );
    let custody = Arc::new(Custody::default());
    let running = Running::start(Arc::clone(&custody), Limits::default()).await;
    let publisher = publisher(running.address, Limits::default());
    assert!(matches!(
        publisher.publish("127.0.0.1:80", &envelope(1)).await,
        Err(TransportError::Rejected)
    ));
    assert!(publisher.publish_batch("commands", &[]).await.is_err());
    assert!(custody.attempts.lock().unwrap().is_empty());
    running.stop().await;
}

#[tokio::test]
async fn positive_acknowledgements_survive_a_later_batch_disconnect() {
    let listener = TcpListener::bind(address()).await.unwrap();
    let publisher = publisher(listener.local_addr().unwrap(), Limits::default());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = [0; 7];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header, b"RDTC\x01\x00\x03");
        for index in 0..2 {
            let length = stream.read_u32().await.unwrap();
            let mut bytes = vec![0; length as usize];
            stream.read_exact(&mut bytes).await.unwrap();
            if index == 0 {
                stream.write_u8(0).await.unwrap();
            }
        }
    });
    let results = publisher
        .publish_batch("commands", &[envelope(1), envelope(2), envelope(3)])
        .await
        .unwrap();
    assert!(matches!(results[0], Ok(Receipt::Accepted)));
    assert!(matches!(results[1], Err(Error::Uncertain)));
    assert!(matches!(results[2], Err(Error::Uncertain)));
    server.await.unwrap();
}

#[tokio::test]
async fn unrelated_reply_or_unknown_ack_never_confirms_custody() {
    for status in [3, 99] {
        let listener = TcpListener::bind(address()).await.unwrap();
        let publisher = publisher(listener.local_addr().unwrap(), Limits::default());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0; 7];
            stream.read_exact(&mut header).await.unwrap();
            let length = stream.read_u32().await.unwrap();
            let mut bytes = vec![0; length as usize];
            stream.read_exact(&mut bytes).await.unwrap();
            stream.write_u8(status).await.unwrap();
            if status == 3 {
                // An otherwise valid but uncorrelated envelope is not this reply.
                write_raw(
                    &mut stream,
                    &Envelope::encode(&Answer { value: 99 })
                        .unwrap()
                        .to_vec()
                        .unwrap(),
                )
                .await;
            }
        });
        assert!(matches!(
            publisher.publish("commands", &envelope(1)).await,
            Err(TransportError::Transient)
        ));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn aggregate_batch_budget_prevents_unbounded_requests_and_replies() {
    let custody = Arc::new(Custody::default());
    custody.reply.store(true, Ordering::SeqCst);
    let request = envelope(1);
    let limits = Limits {
        batch_bytes: request.to_vec().unwrap().len(),
        ..Limits::default()
    };
    let running = Running::start(Arc::clone(&custody), limits).await;
    let publisher = publisher(running.address, limits);
    assert!(matches!(
        publisher
            .publish_batch("commands", &[envelope(2), envelope(3)])
            .await,
        Err(Error::Configuration)
    ));
    assert!(custody.attempts.lock().unwrap().is_empty());
    // The request fits, but no reply can fit. It remains uncertain, not a false
    // completed reply; the accepted business identity is retained by the peer.
    assert!(matches!(
        publisher.request("commands", &request, Answer::NAME).await,
        Err(Error::Uncertain)
    ));
    assert_eq!(custody.accepted.lock().unwrap().len(), 1);
    running.stop().await;
}

#[tokio::test]
async fn caller_cancellation_closes_socket_and_releases_publish_admission() {
    let listener = TcpListener::bind(address()).await.unwrap();
    let limits = Limits {
        connections: 1,
        ..Limits::default()
    };
    let publisher = Arc::new(publisher(listener.local_addr().unwrap(), limits));
    let entered = Arc::new(Notify::new());
    let ready = Arc::clone(&entered);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut header = [0; 7];
        first.read_exact(&mut header).await.unwrap();
        let length = first.read_u32().await.unwrap();
        let mut bytes = vec![0; length as usize];
        first.read_exact(&mut bytes).await.unwrap();
        ready.notify_one();
        assert_eq!(first.read(&mut [0]).await.unwrap(), 0);
        let (mut second, _) = listener.accept().await.unwrap();
        second.read_exact(&mut header).await.unwrap();
        let length = second.read_u32().await.unwrap();
        let mut bytes = vec![0; length as usize];
        second.read_exact(&mut bytes).await.unwrap();
        second.write_u8(0).await.unwrap();
    });
    let sending = Arc::clone(&publisher);
    let first = tokio::spawn(async move { sending.publish("commands", &envelope(1)).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    publisher.publish("commands", &envelope(2)).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn valid_but_unsupported_contract_is_isolated_and_correctable() {
    let custody = Arc::new(Custody::default());
    let running = Running::start(Arc::clone(&custody), Limits::default()).await;
    let publisher = publisher(running.address, Limits::default());
    let unsupported = Envelope::encode(&Answer { value: 2 }).unwrap();
    let envelopes = [envelope(1), unsupported, envelope(3)];
    let results = publisher
        .publish_batch("commands", &envelopes)
        .await
        .unwrap();
    assert!(matches!(results[0], Ok(Receipt::Accepted)));
    assert!(matches!(results[1], Err(Error::Rejected)));
    assert!(matches!(results[2], Ok(Receipt::Accepted)));
    let corrected = Envelope::with_identity(
        envelopes[1].id(),
        envelopes[1].created_at(),
        &Command { value: 2 },
    )
    .unwrap();
    publisher.publish("commands", &corrected).await.unwrap();
    assert_eq!(*custody.attempts.lock().unwrap(), [1, 3, 2]);
    assert_eq!(custody.accepted.lock().unwrap().len(), 3);
    running.stop().await;
}

struct UncorrelatedReply;
impl Consumer for UncorrelatedReply {
    async fn accept(&self, _: Peer, _: Envelope) -> Delivery {
        Delivery::Reply(Envelope::encode(&Answer { value: 42 }).unwrap())
    }
}

#[tokio::test]
async fn receiver_never_confirms_a_consumers_invalid_reply() {
    let running = Running::start(Arc::new(UncorrelatedReply), Limits::default()).await;
    let publisher = publisher(running.address, Limits::default());
    assert!(matches!(
        publisher.publish("commands", &envelope(1)).await,
        Err(TransportError::Transient)
    ));
    running.stop().await;
}
