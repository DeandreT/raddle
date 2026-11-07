//! Real AMQP transfers and dispositions, with the production retry policy.

use std::{io::Cursor, time::Duration};

use fe2o3_amqp::{Connection, Sender, Session};
use fe2o3_amqp_types::{
    definitions::Role,
    messaging::{Accepted, DeliveryState, Modified, Rejected, Released},
    performatives::{Disposition, Flow, Performative},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::oneshot,
    time::timeout,
};

use super::*;
use crate::{core::BasicRetryPolicy, ServiceBusRetryOptions};

#[derive(Clone, Copy, Debug)]
enum Refusal {
    Rejected,
    Released,
    Modified,
    MissingState,
}

impl Refusal {
    fn state(self) -> Option<DeliveryState> {
        Some(match self {
            Self::Rejected => Rejected { error: None }.into(),
            Self::Released => Released {}.into(),
            Self::Modified => Modified {
                delivery_failed: Some(true),
                undeliverable_here: Some(false),
                message_annotations: None,
            }
            .into(),
            Self::MissingState => return None,
        })
    }

    fn matches(self, error: &RetryError<AmqpSendError>) -> bool {
        matches!(
            (self, error),
            (
                Self::Rejected,
                RetryError::Operation(AmqpSendError::NotAccepted(NotAcceptedError::Rejected(_)))
            ) | (
                Self::Released,
                RetryError::Operation(AmqpSendError::NotAccepted(NotAcceptedError::Released(_)))
            ) | (
                Self::Modified,
                RetryError::Operation(AmqpSendError::NotAccepted(NotAcceptedError::Modified(_)))
            ) | (
                Self::MissingState,
                RetryError::Operation(AmqpSendError::Send(
                    fe2o3_amqp::link::SendError::IllegalDeliveryState
                ))
            )
        )
    }
}

struct Peer {
    stream: DuplexStream,
}

impl Peer {
    async fn read(&mut self) -> (u16, Performative, Vec<u8>) {
        let mut header = [0; 8];
        self.stream.read_exact(&mut header).await.unwrap();
        let size = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        assert!((8..=512 * 1024).contains(&size));
        assert_eq!(&header[4..6], &[2, 0]);
        let mut body = vec![0; size - 8];
        self.stream.read_exact(&mut body).await.unwrap();
        let mut cursor = Cursor::new(&body);
        let frame = serde_amqp::from_reader(&mut cursor).unwrap();
        let offset = cursor.position() as usize;
        (
            u16::from_be_bytes([header[6], header[7]]),
            frame,
            body[offset..].to_vec(),
        )
    }

    async fn write(&mut self, channel: u16, frame: Performative) {
        let body = serde_amqp::to_vec(&frame).unwrap();
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
        let (channel, Performative::Open(mut open), _) = self.read().await else {
            panic!("open");
        };
        open.container_id = "refusing-peer".into();
        self.write(channel, Performative::Open(open)).await;
    }

    async fn begin(&mut self) {
        let (channel, Performative::Begin(mut begin), _) = self.read().await else {
            panic!("begin");
        };
        begin.remote_channel = Some(channel);
        self.write(channel, Performative::Begin(begin)).await;
    }

    async fn attach(&mut self) {
        let (channel, Performative::Attach(mut attach), _) = self.read().await else {
            panic!("attach");
        };
        assert_eq!(attach.role, Role::Sender);
        attach.role = Role::Receiver;
        let handle = attach.handle.clone();
        self.write(channel, Performative::Attach(attach)).await;
        self.write(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 100,
                next_outgoing_id: 0,
                outgoing_window: 100,
                handle: Some(handle),
                delivery_count: Some(0),
                link_credit: Some(20),
                available: None,
                drain: false,
                echo: false,
                properties: None,
            }),
        )
        .await;
    }

    async fn respond(
        &mut self,
        refusal: Refusal,
        accept_after: Option<usize>,
        mut done: oneshot::Receiver<()>,
    ) -> Vec<Vec<u8>> {
        let mut payloads = Vec::new();
        let mut ids = std::collections::HashSet::new();
        loop {
            tokio::select! {
                _ = &mut done => return payloads,
                (channel, frame, payload) = self.read() => match frame {
                    Performative::Transfer(transfer) => {
                        assert!(!transfer.more);
                        assert!(!transfer.settled.unwrap_or(false));
                        let id = transfer.delivery_id.unwrap();
                        assert!(ids.insert(id), "retry must be a new delivery, not a repeated completed future");
                        payloads.push(payload);
                        assert!(payloads.len() <= 5, "retry budget was exceeded");
                        let state = if accept_after.is_some_and(|count| payloads.len() > count) {
                            Some(Accepted {}.into())
                        } else { refusal.state() };
                        self.write(channel, Performative::Disposition(Disposition {
                            role: Role::Receiver, first: id, last: None, settled: true,
                            state, batchable: false,
                        })).await;
                    }
                    Performative::Flow(_) | Performative::Disposition(_) => {}
                    other => panic!("unexpected retry traffic: {other:?}"),
                }
            }
        }
    }

    async fn echo_teardown(&mut self) {
        loop {
            let (channel, frame, _) = self.read().await;
            match frame {
                Performative::Detach(_) | Performative::End(_) | Performative::Close(_) => {
                    self.write(channel, frame).await;
                    return;
                }
                Performative::Flow(_) | Performative::Disposition(_) => {}
                other => panic!("unexpected teardown: {other:?}"),
            }
        }
    }
}

async fn pair() -> (
    fe2o3_amqp::connection::ConnectionHandle<()>,
    fe2o3_amqp::session::SessionHandle<()>,
    Sender,
    Peer,
) {
    let (stream, peer) = tokio::io::duplex(512);
    let mut peer = Peer { stream: peer };
    let (connection, ()) = tokio::join!(
        Connection::builder()
            .container_id("retry-client")
            .open_with_stream(stream),
        peer.open()
    );
    let mut connection = connection.unwrap();
    let (session, ()) = tokio::join!(Session::begin(&mut connection), peer.begin());
    let mut session = session.unwrap();
    let (sender, ()) = tokio::join!(
        Sender::attach(&mut session, "retry-sender", "orders"),
        peer.attach()
    );
    (connection, session, sender.unwrap(), peer)
}

async fn run_case(refusal: Refusal, messages: usize, accept_after: Option<usize>) {
    timeout(Duration::from_secs(15), async {
        let (mut connection, mut session, mut sender, mut peer) = pair().await;
        let mut batch = batch_service_bus_messages_as_amqp_message(
            (0..messages).map(|number| {
                let mut message = ServiceBusMessage::new(format!("payload-{number}"));
                message.set_message_id(format!("stable-{number}")).unwrap();
                message
            }),
            false,
        )
        .unwrap();
        // Keep the real policy, default retry count and exponential mode. Only
        // shorten its wall-clock delays so the matrix is deterministic and fast.
        let policy = BasicRetryPolicy::from(ServiceBusRetryOptions {
            delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            try_timeout: Duration::from_secs(2),
            ..Default::default()
        });
        assert_eq!(policy.options().max_retries, 3);
        let mut try_timeout = policy.calculate_try_timeout(0);
        let mut recoveries = 0;
        let (finished, done) = oneshot::channel();
        let operation = async {
            let result = run_operation! {
                {&policy}, AmqpSendError, try_timeout,
                send_batch_envelope(&mut sender, &mut batch),
                async { recoveries += 1; Ok::<(), AmqpSendError>(()) }
            };
            finished.send(()).unwrap();
            result
        };
        let (result, payloads) = tokio::join!(operation, peer.respond(refusal, accept_after, done));
        let (closed, ()) = tokio::join!(sender.close(), peer.echo_teardown());
        closed.unwrap();
        let (closed, ()) = tokio::join!(session.end(), peer.echo_teardown());
        closed.unwrap();
        let (closed, ()) = tokio::join!(connection.close(), peer.echo_teardown());
        closed.unwrap();
        assert_eq!(
            recoveries, 0,
            "terminal refusal does not require link recovery"
        );
        match accept_after {
            Some(refusals) => {
                result.unwrap();
                assert_eq!(
                    payloads.len(),
                    refusals + 1,
                    "success requires a transfer that was actually accepted"
                );
            }
            None => {
                let error = result
                    .expect_err("repeated broker refusal must not become publication success");
                assert!(refusal.matches(&error), "wrong final outcome: {error:?}");
                assert_eq!(
                    payloads.len(),
                    4,
                    "one original transfer plus the three configured retries"
                );
            }
        }
        assert!(!payloads[0].is_empty());
        assert!(
            payloads.iter().all(|payload| payload == &payloads[0]),
            "retries must preserve the message IDs and payload"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn refused_single_publication_exhausts_default_retries_without_success() {
    for refusal in [Refusal::Rejected, Refusal::Released, Refusal::Modified] {
        run_case(refusal, 1, None).await;
    }
}

#[tokio::test]
async fn refused_batch_publication_exhausts_default_retries_without_success() {
    for refusal in [Refusal::Rejected, Refusal::Released, Refusal::Modified] {
        run_case(refusal, 2, None).await;
    }
}

#[tokio::test]
async fn refused_single_publication_succeeds_only_after_acceptance() {
    for refusal in [Refusal::Rejected, Refusal::Released, Refusal::Modified] {
        for refusals in [1, 3] {
            run_case(refusal, 1, Some(refusals)).await;
        }
    }
}

#[tokio::test]
async fn refused_batch_publication_succeeds_only_after_acceptance() {
    for refusal in [Refusal::Rejected, Refusal::Released, Refusal::Modified] {
        for refusals in [1, 3] {
            run_case(refusal, 2, Some(refusals)).await;
        }
    }
}

#[tokio::test]
async fn refused_completed_delivery_error_is_not_polled_again() {
    run_case(Refusal::MissingState, 1, None).await;
}

#[tokio::test]
async fn refused_publication_pending_wait_retains_original_and_accepted_state_is_terminal() {
    timeout(Duration::from_secs(15), async {
        let (mut connection, mut session, mut sender, mut peer) = pair().await;
        let mut batch = batch_service_bus_messages_as_amqp_message(
            std::iter::once(ServiceBusMessage::new(vec![7])),
            false,
        )
        .unwrap();
        assert!(timeout(
            Duration::from_millis(10),
            send_batch_envelope(&mut sender, &mut batch)
        )
        .await
        .is_err());
        assert!(matches!(batch.state, BatchEnvelopeState::Sent(_)));
        let (channel, Performative::Transfer(transfer), _) = peer.read().await else {
            panic!("original transfer");
        };
        let (result, ()) = tokio::join!(
            send_batch_envelope(&mut sender, &mut batch),
            peer.write(
                channel,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: transfer.delivery_id.unwrap(),
                    last: None,
                    settled: true,
                    state: Some(Accepted {}.into()),
                    batchable: false,
                })
            ),
        );
        result.unwrap();
        assert!(matches!(batch.state, BatchEnvelopeState::Accepted));
        send_batch_envelope(&mut sender, &mut batch).await.unwrap();
        assert!(
            timeout(Duration::from_millis(10), peer.read())
                .await
                .is_err(),
            "pending or accepted delivery was resent"
        );
        let (closed, ()) = tokio::join!(sender.close(), peer.echo_teardown());
        closed.unwrap();
        let (closed, ()) = tokio::join!(session.end(), peer.echo_teardown());
        closed.unwrap();
        let (closed, ()) = tokio::join!(connection.close(), peer.echo_teardown());
        closed.unwrap();
    })
    .await
    .unwrap();
}
