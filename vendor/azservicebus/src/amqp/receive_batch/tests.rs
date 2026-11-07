//! Real fe2o3 links over a bounded in-memory AMQP transport. The peer deliberately
//! stops reading to fill the SDK's actual outgoing queues; no mock drop closes them.

use std::time::Duration;

use fe2o3_amqp::{link::receiver::CreditMode, Connection, Receiver, Session};
use fe2o3_amqp_types::{
    definitions::{ReceiverSettleMode, Role},
    messaging::{message::__private::Serializable, AmqpValue, Body, Message},
    performatives::{Performative, Transfer},
    primitives::Uuid,
};
use futures_util::FutureExt;
use serde_amqp::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    time::timeout,
};

use super::{CreditLease, OwnedOperation, Recovery};
use crate::{
    core::BasicRetryPolicy,
    primitives::{
        service_bus_retry_mode::ServiceBusRetryMode,
        service_bus_retry_options::ServiceBusRetryOptions,
    },
    ServiceBusReceiveMode,
};

const SHORT: Duration = Duration::from_millis(5);

struct Peer {
    stream: DuplexStream,
}

impl Peer {
    async fn read(&mut self) -> (u16, Performative) {
        let mut header = [0; 8];
        self.stream.read_exact(&mut header).await.unwrap();
        let size = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        assert_eq!(header[4], 2);
        assert_eq!(header[5], 0);
        let mut body = vec![0; size - 8];
        self.stream.read_exact(&mut body).await.unwrap();
        (
            u16::from_be_bytes([header[6], header[7]]),
            serde_amqp::from_slice(&body).unwrap(),
        )
    }

    async fn write(&mut self, channel: u16, frame: Performative, payload: &[u8]) {
        let mut body = serde_amqp::to_vec(&frame).unwrap();
        body.extend_from_slice(payload);
        let size = u32::try_from(body.len() + 8).unwrap();
        let mut bytes = Vec::from(size.to_be_bytes());
        bytes.extend_from_slice(&[2, 0]);
        bytes.extend_from_slice(&channel.to_be_bytes());
        bytes.extend_from_slice(&body);
        self.stream.write_all(&bytes).await.unwrap();
    }

    async fn open(&mut self) {
        let mut header = [0; 8];
        self.stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header, b"AMQP\0\x01\0\0");
        self.stream.write_all(&header).await.unwrap();
        let (channel, Performative::Open(mut open)) = self.read().await else {
            panic!("expected open")
        };
        open.container_id = "bounded-test-peer".into();
        self.write(channel, Performative::Open(open), &[]).await;
    }

    async fn begin(&mut self) {
        let (channel, Performative::Begin(mut begin)) = self.read().await else {
            panic!("expected begin")
        };
        begin.remote_channel = Some(channel);
        self.write(channel, Performative::Begin(begin), &[]).await;
    }

    async fn attach(&mut self) -> (u16, u32) {
        loop {
            let (channel, frame) = self.read().await;
            match frame {
                Performative::Attach(mut attach) => {
                    let handle = attach.handle.0;
                    attach.role = Role::Sender;
                    attach.initial_delivery_count = Some(0);
                    self.write(channel, Performative::Attach(attach), &[]).await;
                    return (channel, handle);
                }
                Performative::Flow(_) => {}
                other => panic!("unexpected frame: {other:?}"),
            }
        }
    }

    async fn flow(&mut self, handle: u32, credit: u32) {
        loop {
            let (_, frame) = self.read().await;
            match frame {
                Performative::Flow(flow)
                    if flow.handle.as_ref().map(|h| h.0) == Some(handle)
                        && flow.link_credit == Some(credit) =>
                {
                    return
                }
                Performative::Flow(_) | Performative::Disposition(_) => {}
                // In particular, a failed best-effort destructor detach or a shared
                // session end must not be mistaken for successful zero-credit cleanup.
                other => panic!("unexpected frame during credit cleanup: {other:?}"),
            }
        }
    }

    async fn deliver(&mut self, channel: u16, handle: u32, index: u8) {
        let transfer = Transfer {
            handle: handle.into(),
            delivery_id: Some(u32::from(index)),
            delivery_tag: Some(Vec::from([index; 16]).into()),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        };
        let message = Message::builder()
            .body(Body::Value(AmqpValue(Value::Ubyte(index))))
            .build();
        let payload = serde_amqp::to_vec(&Serializable(message)).unwrap();
        self.write(channel, Performative::Transfer(transfer), &payload)
            .await;
    }

    async fn detach(&mut self) {
        loop {
            let (channel, frame) = self.read().await;
            match frame {
                Performative::Detach(detach) => {
                    self.write(channel, Performative::Detach(detach), &[]).await;
                    return;
                }
                Performative::Flow(_) | Performative::Disposition(_) => {}
                other => panic!("unexpected teardown frame: {other:?}"),
            }
        }
    }
}

async fn pair() -> (
    fe2o3_amqp::connection::ConnectionHandle<()>,
    fe2o3_amqp::session::SessionHandle<()>,
    Receiver,
    Peer,
    u16,
    u32,
) {
    let (stream, peer) = tokio::io::duplex(512);
    let mut peer = Peer { stream: peer };
    let (connection, ()) = tokio::join!(
        Connection::builder()
            .container_id("bounded-client")
            .buffer_size(1)
            .open_with_stream(stream),
        peer.open(),
    );
    let mut connection = connection.unwrap();
    let (session, ()) = tokio::join!(
        Session::builder().buffer_size(1).begin(&mut connection),
        peer.begin()
    );
    let mut session = session.unwrap();
    let (receiver, (channel, handle)) = tokio::join!(
        Receiver::builder()
            .name("retained")
            .source("queue")
            .credit_mode(CreditMode::Manual)
            .receiver_settle_mode(ReceiverSettleMode::Second)
            .attach(&mut session),
        peer.attach(),
    );
    (
        connection,
        session,
        receiver.unwrap(),
        peer,
        channel,
        handle,
    )
}

#[tokio::test]
async fn actual_full_sdk_queue_retains_cleanup_across_cancelled_waits() {
    timeout(Duration::from_secs(10), async {
        let (mut connection, mut session, mut receiver, mut peer, _, handle) = pair().await;
        let (other, (_, other_handle)) = tokio::join!(
            Receiver::builder()
                .name("unrelated")
                .source("other")
                .credit_mode(CreditMode::Manual)
                .attach(&mut session),
            peer.attach(),
        );
        let mut other = other.unwrap();
        // Do not read the peer: the socket plus both SDK queues become full.
        let mut full = false;
        for _ in 0..1024 {
            if timeout(SHORT, receiver.set_credit(1)).await.is_err() {
                full = true;
                break;
            }
        }
        assert!(full, "test never reached actual AMQP backpressure");
        let mut slot = Some(receiver);
        let mut cleanup = None;
        let mut error = None;
        drop(CreditLease {
            slot: &mut slot,
            cleanup: &mut cleanup,
            cleanup_error: &mut error,
            manual: true,
        });
        assert!(slot.is_none());
        assert!(
            cleanup.is_some(),
            "blocked cleanup must retain its receiver"
        );
        for _ in 0..3 {
            assert!(timeout(SHORT, cleanup.as_mut().unwrap()).await.is_err());
            assert!(slot.is_none());
        }
        let ((receiver, result), ()) =
            tokio::join!(cleanup.as_mut().unwrap(), peer.flow(handle, 0));
        result.unwrap();
        cleanup = None;
        assert!(cleanup.is_none());
        assert!(error.is_none());
        assert!(
            !session.is_ended(),
            "cancellation ended the shared SDK session"
        );
        let (result, ()) = tokio::join!(other.set_credit(1), peer.flow(other_handle, 1));
        result.unwrap();
        let (result, ()) = tokio::join!(receiver.close(), peer.detach());
        result.unwrap();
        let (result, ()) = tokio::join!(other.close(), peer.detach());
        result.unwrap();
        let (result, ()) = tokio::join!(session.end(), async {
            let (channel, Performative::End(end)) = peer.read().await else {
                panic!("expected end")
            };
            peer.write(channel, Performative::End(end), &[]).await;
        });
        result.unwrap();
        let (result, ()) = tokio::join!(connection.close(), async {
            let (channel, Performative::Close(close)) = peer.read().await else {
                panic!("expected close")
            };
            peer.write(channel, Performative::Close(close), &[]).await;
        });
        result.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_resume_handshake_is_not_restarted_by_short_polls() {
    timeout(Duration::from_secs(10), async {
        let (_connection, session, mut receiver, mut peer, _, _) = pair().await;
        let session = std::sync::Arc::new(session);
        let recovery_session = session.clone();
        let mut operation = OwnedOperation::new(async move {
            let result = receiver
                .detach_then_resume_on_session(&recovery_session)
                .await
                .map(|_| ());
            (receiver, result)
        });
        for _ in 0..3 {
            assert!(timeout(SHORT, &mut operation).await.is_err());
        }
        let ((receiver, result), ()) = tokio::join!(&mut operation, async {
            // Exactly the original detach followed by one attach. A restarted
            // recovery would send another detach or orphan its allocated handle.
            peer.detach().await;
            peer.attach().await;
        });
        result.unwrap();
        let (result, ()) = tokio::join!(receiver.close(), peer.detach());
        result.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn external_cancellation_keeps_real_partial_deliveries_and_smaller_next_batch() {
    timeout(Duration::from_secs(10), async {
        let (_connection, _session, receiver, mut peer, channel, handle) = pair().await;
        let mut slot = Some(receiver);
        let mut state = Recovery::default();
        let epoch = uuid::Uuid::new_v4();
        let (result, ()) = tokio::join!(
            timeout(Duration::from_millis(30), async {
                let lease = CreditLease {
                    slot: &mut slot,
                    cleanup: &mut state.cleanup,
                    cleanup_error: &mut state.cleanup_error,
                    manual: true,
                };
                super::super::amqp_receiver::receive_messages(
                    lease.slot.as_mut().unwrap(),
                    0,
                    &ServiceBusReceiveMode::PeekLock,
                    epoch,
                    &mut state.buffer,
                    5,
                )
                .await
            }),
            async {
                peer.flow(handle, 5).await;
                for index in 1..=3 {
                    peer.deliver(channel, handle, index).await;
                }
            }
        );
        assert!(
            result.is_err(),
            "caller cancellation did not interrupt receive"
        );
        assert_eq!(state.buffer.len(), 3);
        let first = state.take_batch(1);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].lock_token(), &Uuid::from([1; 16]));
        let next = state.take_batch(2);
        assert_eq!(next.len(), 2);
        assert_eq!(next[0].lock_token(), &Uuid::from([2; 16]));
        assert_eq!(next[1].lock_token(), &Uuid::from([3; 16]));
        assert!(state.buffer.is_empty());
        if let Some(operation) = state.cleanup.as_mut() {
            let (receiver, result) = operation.await;
            result.unwrap();
            slot = Some(receiver);
        }
        peer.flow(handle, 0).await;
        let (result, ()) = tokio::join!(slot.take().unwrap().close(), peer.detach());
        result.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn recovery_failures_keep_their_backoff_and_attempt_count() {
    let policy = BasicRetryPolicy::from(ServiceBusRetryOptions {
        delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(1),
        max_retries: 2,
        mode: ServiceBusRetryMode::Fixed,
        ..Default::default()
    });
    let error = super::super::error::RecoverReceiverError::ConnectionScopeDisposed;
    let mut recovery = Recovery::default();
    assert!(recovery.failed(&policy, &error));
    let first_deadline = recovery.retry_at;
    for _ in 0..3 {
        assert!(
            timeout(SHORT, crate::util::time::sleep(recovery.remaining()))
                .await
                .is_err()
        );
        assert_eq!(recovery.retry_at, first_deadline);
    }
    // A failed recovery, not a receive error, advances the same retry sequence.
    assert!(recovery.failed(&policy, &error));
    assert!(recovery.retry_at > first_deadline);
    assert!(!recovery.failed(&policy, &error));
    assert_eq!(recovery.failed_attempts, 3);
    recovery.recovered();
    assert!(!recovery.pending());
    assert_eq!(
        recovery.failed_attempts, 3,
        "a resume alone is not a healthy receive"
    );
    recovery.healthy();
    assert_eq!(recovery.failed_attempts, 0);
}

#[test]
fn successful_recovery_does_not_reset_a_repeated_receive_failure_budget() {
    let policy = BasicRetryPolicy::from(ServiceBusRetryOptions {
        max_retries: 1,
        ..Default::default()
    });
    let error = super::super::error::AmqpRecvError::LockTokenNotFound;
    let mut recovery = Recovery::default();
    assert!(recovery.failed(&policy, &error));
    recovery.recovered();
    assert!(
        !recovery.failed(&policy, &error),
        "a resumed but still failing link must exhaust its retry budget"
    );
}

#[tokio::test]
async fn owned_work_retains_an_allocated_response_waiter_after_caller_timeout() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut operation = OwnedOperation::new(async move {
        let (reply, response) = tokio::sync::oneshot::channel();
        tx.send(reply).await.unwrap();
        let result = response.await;
        (tx, result)
    });
    assert!(timeout(SHORT, &mut operation).await.is_err());
    let reply = rx.recv().await.unwrap();
    // fe2o3 ends the session if this send fails. A retained future keeps the
    // actual bounded-queue/oneshot exchange alive, unlike a persistent Boolean.
    reply.send(()).unwrap();
    let (_, result) = (&mut operation).now_or_never().unwrap();
    result.unwrap();
}
