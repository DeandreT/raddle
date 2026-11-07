use super::*;
use crate::{
    endpoint::{SenderLink as _, Session as _},
    session::Session,
    util::Consumer,
};
use fe2o3_amqp_types::{
    definitions::Role,
    messaging::{Accepted, Outcome},
    performatives::Disposition,
};
use futures_util::{poll, FutureExt};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

tokio::task_local! { static ENQUEUE_GATE: Arc<Gate>; }

struct Gate {
    frame: usize,
    seen: AtomicUsize,
    release: Notify,
}

impl Gate {
    fn new(frame: usize) -> Arc<Self> {
        Arc::new(Self {
            frame,
            seen: AtomicUsize::new(0),
            release: Notify::new(),
        })
    }
}

pub(super) async fn after_enqueue() {
    if let Ok(gate) = ENQUEUE_GATE.try_with(Arc::clone) {
        if gate.seen.fetch_add(1, Ordering::SeqCst) == gate.frame {
            gate.release.notified().await;
        }
    }
}

fn link() -> SenderLink<Target> {
    let flow = Arc::new(LinkFlowState::sender(state::LinkFlowStateInner {
        initial_delivery_count: 0,
        delivery_count: 0,
        link_credit: 20,
        available: 0,
        drain: false,
        properties: None,
    }));
    SenderLink {
        role: PhantomData,
        local_state: LinkState::Attached,
        name: "race".into(),
        output_handle: Some(OutputHandle(0)),
        input_handle: Some(InputHandle(0)),
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: None,
        target: Some(Target::default()),
        max_message_size: 0,
        offered_capabilities: None,
        desired_capabilities: None,
        flow_state: Consumer::new(Arc::new(Notify::new()), flow),
        unsettled: Arc::new(RwLock::new(None)),
        verify_incoming_source: true,
        verify_incoming_target: true,
    }
}

fn session(link: &SenderLink<Target>) -> Session {
    let mut session = Session::builder().into_session(
        crate::endpoint::OutgoingChannel(0),
        fe2o3_amqp_types::states::SessionState::Mapped,
    );
    session.remote_incoming_window = 20;
    let (tx, _) = mpsc::channel(2);
    session.link_by_input_handle.insert(
        InputHandle(0),
        LinkRelay::Sender {
            tx,
            output_handle: OutputHandle(0),
            flow_state: link.flow_state.producer(),
            unsettled: link.unsettled.clone(),
            receiver_settle_mode: ReceiverSettleMode::First,
        },
    );
    session
}

fn transfer(link: &SenderLink<Target>) -> Transfer {
    link.generate_non_resuming_transfer_performative(vec![1].into(), 0, Some(false), None, true)
        .unwrap()
}

fn accept_frame(session: &mut Session, frame: LinkFrame) -> u32 {
    let LinkFrame::Transfer {
        input_handle,
        performative,
        payload,
    } = frame
    else {
        panic!("expected transfer");
    };
    let outgoing = session
        .on_outgoing_transfer(input_handle, performative, payload)
        .unwrap()
        .unwrap();
    let crate::session::frame::SessionOutgoingItem::SingleFrame(frame) = outgoing else {
        panic!("single frame");
    };
    let crate::session::frame::SessionFrameBody::Transfer { performative, .. } = frame.body else {
        panic!("transfer frame");
    };
    performative.delivery_id.unwrap_or(0)
}

fn accepted(session: &mut Session, id: u32) {
    session
        .on_incoming_disposition(Disposition {
            role: Role::Receiver,
            first: id,
            last: None,
            settled: true,
            state: Some(Accepted {}.into()),
            batchable: false,
        })
        .unwrap();
}

fn pending_count(map: &ArcSenderUnsettledMap) -> usize {
    map.read().as_ref().map_or(0, |map| map.len())
}

fn assert_accepted(settlement: Settlement) {
    let outcome: delivery::DeliveryFut<Result<Outcome, SendError>> = settlement.into();
    assert!(matches!(
        outcome.now_or_never(),
        Some(Ok(Outcome::Accepted(_)))
    ));
}

#[tokio::test]
async fn registration_race_early_ack_survives_session_dispatch() {
    let mut link = link();
    let mut session = session(&link);
    let transfer = transfer(&link);
    let (tx, mut rx) = mpsc::channel(1);
    let gate = Gate::new(0);
    let send = ENQUEUE_GATE.scope(
        gate.clone(),
        link.send_payload_with_transfer(&tx, 0, transfer, bytes::Bytes::from_static(b"payload")),
    );
    tokio::pin!(send);
    assert!(poll!(&mut send).is_pending());
    let id = accept_frame(&mut session, rx.recv().await.unwrap());
    accepted(&mut session, id);
    assert!(session.delivery_tag_by_id.is_empty());
    gate.release.notify_one();
    let settlement = send.await.unwrap();
    let outcome: delivery::DeliveryFut<Result<Outcome, SendError>> = settlement.into();
    assert!(
        matches!(outcome.now_or_never(), Some(Ok(Outcome::Accepted(_)))),
        "accepted acknowledgment was lost before waiter registration"
    );
}

#[tokio::test]
async fn registration_race_public_batchable_ref_waiter_survives_early_ack() {
    let link = link();
    let mut session = session(&link);
    let (tx, mut rx) = mpsc::channel(1);
    let (control_tx, _control_rx) = mpsc::channel(1);
    let (_incoming_tx, incoming_rx) = mpsc::channel(1);
    let mut sender = crate::Sender {
        inner: crate::link::sender::SenderInner {
            link,
            buffer_size: 1,
            session: control_tx,
            outgoing: tx,
            incoming: incoming_rx,
        },
    };
    let sendable: delivery::Sendable<fe2o3_amqp_types::messaging::AmqpValue<&str>> =
        "public API".into();
    let gate = Gate::new(0);
    let send = ENQUEUE_GATE.scope(gate.clone(), sender.send_batchable_ref(&sendable));
    tokio::pin!(send);
    assert!(poll!(&mut send).is_pending());
    let id = accept_frame(&mut session, rx.try_recv().unwrap());
    accepted(&mut session, id);
    gate.release.notify_one();
    let outcome = send.await.unwrap();
    assert!(matches!(
        outcome.now_or_never(),
        Some(Ok(Outcome::Accepted(_)))
    ));
}

#[tokio::test]
async fn registration_race_multi_frame_ack_before_send_returns() {
    let mut link = link();
    link.max_message_size = 3;
    let mut session = session(&link);
    let map = link.unsettled.clone();
    let transfer = transfer(&link);
    let (tx, mut rx) = mpsc::channel(1);
    let gate = Gate::new(3);
    let send = ENQUEUE_GATE.scope(
        gate.clone(),
        link.send_payload_with_transfer(&tx, 0, transfer, bytes::Bytes::from_static(b"0123456789")),
    );
    tokio::pin!(send);
    let mut payload = Vec::new();
    for index in 0..4 {
        assert!(poll!(&mut send).is_pending());
        let frame = rx.try_recv().unwrap();
        let LinkFrame::Transfer {
            performative,
            payload: chunk,
            ..
        } = &frame
        else {
            panic!("transfer");
        };
        assert_eq!(performative.more, index < 3);
        payload.extend_from_slice(chunk);
        accept_frame(&mut session, frame);
    }
    assert_eq!(payload, b"0123456789");
    accepted(&mut session, 0);
    gate.release.notify_one();
    assert_accepted(send.await.unwrap());
    assert_eq!(pending_count(&map), 0);
    assert_eq!(session.next_outgoing_id, 4);
}

#[tokio::test]
async fn registration_race_send_failure_before_enqueue_rolls_back() {
    let mut link = link();
    let map = link.unsettled.clone();
    let transfer = transfer(&link);
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let result = link
        .send_payload_with_transfer(&tx, 0, transfer, bytes::Bytes::from_static(b"payload"))
        .await;
    assert!(matches!(result, Err(LinkStateError::IllegalSessionState)));
    assert_eq!(pending_count(&map), 0);
}

#[tokio::test]
async fn registration_race_failed_send_does_not_remove_another_delivery() {
    let mut link = link();
    let mut session = session(&link);
    let map = link.unsettled.clone();
    let (tx, mut rx) = mpsc::channel(1);
    let first = link
        .send_payload_with_transfer(&tx, 0, transfer(&link), bytes::Bytes::from_static(b"first"))
        .await
        .unwrap();
    let id = accept_frame(&mut session, rx.try_recv().unwrap());
    drop(rx);
    let mut second = transfer(&link);
    second.delivery_tag = Some(vec![2].into());
    assert!(link
        .send_payload_with_transfer(&tx, 0, second, bytes::Bytes::from_static(b"second"))
        .await
        .is_err());
    assert_eq!(pending_count(&map), 1);
    accepted(&mut session, id);
    assert_accepted(first);
    assert_eq!(pending_count(&map), 0);
}

#[tokio::test]
async fn registration_race_dropped_delivery_future_is_cleaned_by_ack() {
    let mut link = link();
    let mut session = session(&link);
    let map = link.unsettled.clone();
    let (tx, mut rx) = mpsc::channel(1);
    let settlement = link
        .send_payload_with_transfer(&tx, 0, transfer(&link), bytes::Bytes::from_static(b"first"))
        .await
        .unwrap();
    let outcome: delivery::DeliveryFut<Result<Outcome, SendError>> = settlement.into();
    let id = accept_frame(&mut session, rx.try_recv().unwrap());
    drop(outcome);
    assert_eq!(pending_count(&map), 1);
    accepted(&mut session, id);
    assert_eq!(pending_count(&map), 0);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn registration_race_missing_handle_rolls_back() {
    let mut link = link();
    let map = link.unsettled.clone();
    let transfer = transfer(&link);
    link.input_handle = None;
    let (tx, _rx) = mpsc::channel(1);
    assert!(link
        .send_payload_with_transfer(&tx, 0, transfer, bytes::Bytes::from_static(b"payload"))
        .await
        .is_err());
    assert_eq!(pending_count(&map), 0);
}

#[tokio::test]
async fn registration_race_cancel_before_enqueue_rolls_back() {
    let mut link = link();
    let map = link.unsettled.clone();
    let transfer = transfer(&link);
    let (tx, mut rx) = mpsc::channel(1);
    tx.send(LinkFrame::Flow(LinkFlow::default())).await.unwrap();
    let mut send = Box::pin(link.send_payload_with_transfer(
        &tx,
        0,
        transfer,
        bytes::Bytes::from_static(b"payload"),
    ));
    assert!(poll!(&mut send).is_pending());
    assert_eq!(pending_count(&map), 1);
    drop(send);
    assert_eq!(pending_count(&map), 0);
    assert!(matches!(rx.try_recv(), Ok(LinkFrame::Flow(_))));
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn registration_race_cancel_while_waiting_for_credit_registers_nothing() {
    let mut link = link();
    link.flow_state.state().lock.write().link_credit = 0;
    let map = link.unsettled.clone();
    let (tx, mut rx) = mpsc::channel(1);
    let mut send = Box::pin(link.send_payload(
        &tx,
        std::future::pending::<Option<LinkFrame>>(),
        bytes::Bytes::from_static(b"payload"),
        0,
        None,
        None,
        true,
    ));
    assert!(poll!(&mut send).is_pending());
    drop(send);
    assert_eq!(pending_count(&map), 0);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn registration_race_cancel_after_enqueue_retains_until_ack() {
    let mut link = link();
    let mut session = session(&link);
    let map = link.unsettled.clone();
    let transfer = transfer(&link);
    let (tx, mut rx) = mpsc::channel(1);
    let gate = Gate::new(0);
    let mut send = Box::pin(ENQUEUE_GATE.scope(
        gate,
        link.send_payload_with_transfer(&tx, 0, transfer, bytes::Bytes::from_static(b"payload")),
    ));
    assert!(poll!(&mut send).is_pending());
    let id = accept_frame(&mut session, rx.try_recv().unwrap());
    drop(send);
    assert_eq!(pending_count(&map), 1);
    accepted(&mut session, id);
    assert_eq!(pending_count(&map), 0);
    accepted(&mut session, id);
    assert_eq!(pending_count(&map), 0);
}

#[tokio::test]
async fn registration_race_cancel_between_frames_preserves_original_payload() {
    let mut link = link();
    link.max_message_size = 3;
    let mut session = session(&link);
    let map = link.unsettled.clone();
    let transfer = transfer(&link);
    let (tx, mut rx) = mpsc::channel(1);
    let mut send = Box::pin(link.send_payload_with_transfer(
        &tx,
        42,
        transfer,
        bytes::Bytes::from_static(b"0123456789"),
    ));
    assert!(poll!(&mut send).is_pending());
    let id = accept_frame(&mut session, rx.try_recv().unwrap());
    drop(send);
    {
        let guard = map.read();
        let message = guard
            .as_ref()
            .unwrap()
            .get(&DeliveryTag::from(vec![1]))
            .unwrap();
        assert_eq!(message.payload.as_ref(), b"0123456789");
        assert_eq!(message.message_format, 42);
    }
    accepted(&mut session, id);
    assert_eq!(pending_count(&map), 0);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn registration_race_send_failure_after_enqueue_retains_recovery_state() {
    let mut link = link();
    link.max_message_size = 3;
    let mut session = session(&link);
    let map = link.unsettled.clone();
    let transfer = transfer(&link);
    let (tx, mut rx) = mpsc::channel(1);
    let mut send = Box::pin(link.send_payload_with_transfer(
        &tx,
        0,
        transfer,
        bytes::Bytes::from_static(b"0123456789"),
    ));
    assert!(poll!(&mut send).is_pending());
    let id = accept_frame(&mut session, rx.try_recv().unwrap());
    drop(rx);
    assert!(matches!(
        send.await,
        Err(LinkStateError::IllegalSessionState)
    ));
    assert_eq!(pending_count(&map), 1);
    accepted(&mut session, id);
    assert_eq!(pending_count(&map), 0);
}

#[tokio::test]
async fn registration_race_pre_settled_send_has_no_waiter() {
    let mut link = link();
    link.snd_settle_mode = SenderSettleMode::Settled;
    let map = link.unsettled.clone();
    let mut transfer = transfer(&link);
    transfer.settled = None;
    let (tx, mut rx) = mpsc::channel(1);
    assert!(matches!(
        link.send_payload_with_transfer(&tx, 0, transfer, bytes::Bytes::from_static(b"payload"))
            .await
            .unwrap(),
        Settlement::Settled(_)
    ));
    assert_eq!(pending_count(&map), 0);
    assert!(matches!(rx.try_recv(), Ok(LinkFrame::Transfer { .. })));
}

#[tokio::test]
async fn registration_race_duplicate_disposition_does_not_affect_later_send() {
    let mut link = link();
    let mut session = session(&link);
    let map = link.unsettled.clone();
    let (tx, mut rx) = mpsc::channel(1);
    let first = link
        .send_payload_with_transfer(&tx, 0, transfer(&link), bytes::Bytes::from_static(b"first"))
        .await
        .unwrap();
    let first_id = accept_frame(&mut session, rx.try_recv().unwrap());
    accepted(&mut session, first_id);
    assert_accepted(first);
    let second = link
        .send_payload_with_transfer(
            &tx,
            0,
            transfer(&link),
            bytes::Bytes::from_static(b"second"),
        )
        .await
        .unwrap();
    let second_id = accept_frame(&mut session, rx.try_recv().unwrap());
    assert_ne!(first_id, second_id);
    accepted(&mut session, first_id);
    assert_eq!(pending_count(&map), 1);
    accepted(&mut session, second_id);
    assert_accepted(second);
    accepted(&mut session, second_id);
    assert_eq!(pending_count(&map), 0);
}

#[tokio::test]
async fn registration_race_opposite_direction_id_does_not_steal_ack() {
    let mut link = link();
    let mut session = session(&link);
    let (incoming_tx, mut incoming_rx) = mpsc::channel(2);
    let incoming_unsettled = Arc::new(RwLock::new(None));
    session.link_by_input_handle.insert(
        InputHandle(1),
        LinkRelay::Receiver {
            tx: incoming_tx,
            output_handle: OutputHandle(1),
            flow_state: Arc::new(LinkFlowState::receiver(state::LinkFlowStateInner {
                initial_delivery_count: 0,
                delivery_count: 0,
                link_credit: 2,
                available: 0,
                drain: false,
                properties: None,
            })),
            unsettled: incoming_unsettled,
            receiver_settle_mode: ReceiverSettleMode::Second,
            more: false,
        },
    );
    let (tx, mut rx) = mpsc::channel(1);
    let publisher_transfer = transfer(&link);
    let mut prefetched_transfer = publisher_transfer.clone();
    prefetched_transfer.handle = 1.into();
    prefetched_transfer.delivery_id = Some(0);
    prefetched_transfer.delivery_tag = Some(vec![9].into());
    let gate = Gate::new(0);
    let send = ENQUEUE_GATE.scope(
        gate.clone(),
        link.send_payload_with_transfer(
            &tx,
            0,
            publisher_transfer,
            bytes::Bytes::from_static(b"publication"),
        ),
    );
    tokio::pin!(send);
    assert!(poll!(&mut send).is_pending());
    let id = accept_frame(&mut session, rx.try_recv().unwrap());
    assert_eq!(id, 0);
    session
        .on_incoming_transfer(prefetched_transfer, bytes::Bytes::from_static(b"prefetch"))
        .await
        .unwrap();
    assert_eq!(session.delivery_tag_by_id.len(), 2);
    accepted(&mut session, id);
    assert!(session.delivery_tag_by_id.contains_key(&(Role::Sender, 0)));
    assert!(!session
        .delivery_tag_by_id
        .contains_key(&(Role::Receiver, 0)));
    gate.release.notify_one();
    assert_accepted(send.await.unwrap());
    assert!(matches!(
        incoming_rx.try_recv(),
        Ok(LinkFrame::Transfer { .. })
    ));
    session
        .on_incoming_disposition(Disposition {
            role: Role::Sender,
            first: 0,
            last: None,
            settled: true,
            state: Some(Accepted {}.into()),
            batchable: false,
        })
        .unwrap();
    assert!(session.delivery_tag_by_id.is_empty());
}

#[tokio::test]
async fn registration_race_second_mode_unsettled_outcome_is_delivered() {
    let mut link = link();
    let mut session = session(&link);
    let Some(LinkRelay::Sender {
        receiver_settle_mode,
        ..
    }) = session.link_by_input_handle.get_mut(&InputHandle(0))
    else {
        panic!("sender");
    };
    *receiver_settle_mode = ReceiverSettleMode::Second;
    let map = link.unsettled.clone();
    let (tx, mut rx) = mpsc::channel(1);
    let gate = Gate::new(0);
    let send = ENQUEUE_GATE.scope(
        gate.clone(),
        link.send_payload_with_transfer(
            &tx,
            0,
            transfer(&link),
            bytes::Bytes::from_static(b"publication"),
        ),
    );
    tokio::pin!(send);
    assert!(poll!(&mut send).is_pending());
    let id = accept_frame(&mut session, rx.try_recv().unwrap());
    let _echo = session
        .on_incoming_disposition(Disposition {
            role: Role::Receiver,
            first: id,
            last: None,
            settled: false,
            state: Some(Accepted {}.into()),
            batchable: false,
        })
        .unwrap()
        .unwrap();
    gate.release.notify_one();
    assert_accepted(send.await.unwrap());
    assert_eq!(pending_count(&map), 0);
}
