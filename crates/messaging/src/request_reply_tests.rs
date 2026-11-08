#![allow(clippy::unwrap_used)]

use std::future::{self, Future};
use std::pin::Pin;
use std::task::Poll;

use chrono::Utc;
use futures_util::FutureExt;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{ReplyFrame, ReplyHubInvoker, TOKEN};
use crate::{BusError, MessageContext, RemoteInvoker, TrustedOrigin};

#[derive(Serialize, Deserialize)]
struct Reply(u32);
impl WireMessage for Reply {
    const NAME: &'static str = "test.expected-reply";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct WrongReply;
impl WireMessage for WrongReply {
    const NAME: &'static str = "test.wrong-reply";
    const VERSION: u32 = 1;
}

struct Capture(mpsc::UnboundedSender<Envelope>);
impl Transport for Capture {
    async fn publish(&self, _: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.0
            .send(envelope.clone())
            .map_err(|_| TransportError::Transient)
    }
}

async fn is_pending<F: Future + Unpin>(future: &mut F) -> bool {
    future::poll_fn(|context| Poll::Ready(Pin::new(&mut *future).poll(context).is_pending())).await
}

fn setup() -> (
    ReplyHubInvoker<Capture>,
    mpsc::UnboundedReceiver<Envelope>,
    MessageContext,
    TrustedOrigin,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let invoker = ReplyHubInvoker::new(
        Capture(sender),
        1,
        "replies",
        [("peer".into(), "peer-auth".into())],
    )
    .unwrap();
    let context = MessageContext::new(
        TrustedOrigin::authenticated("tenant", "caller", None).unwrap(),
        Envelope::encode(&Reply(0)).unwrap(),
        Utc::now(),
    );
    let responder = TrustedOrigin::authenticated("tenant", "peer-auth", None).unwrap();
    (invoker, receiver, context, responder)
}

fn frame(request: &Envelope, body: Option<Vec<u8>>) -> Envelope {
    let token = request.headers()[TOKEN].parse::<Uuid>().unwrap();
    Envelope::encode(&ReplyFrame { body })
        .unwrap()
        .correlated(token, request.id())
        .unwrap()
}

fn reject_invalid_frames(
    invoker: &ReplyHubInvoker<Capture>,
    request: &Envelope,
    origin: &TrustedOrigin,
) {
    for body in [
        Envelope::encode(&WrongReply).unwrap().to_vec().unwrap(),
        b"not an envelope".to_vec(),
    ] {
        assert!(
            !invoker
                .accept_reply(origin, frame(request, Some(body)))
                .unwrap()
        );
        assert_eq!(invoker.pending().unwrap(), 1);
    }
}

#[tokio::test]
async fn invalid_inner_reply_preserves_the_active_waiter_and_capacity_until_a_valid_reply() {
    let (invoker, mut receiver, context, responder) = setup();
    let mut request = Box::pin(invoker.request("peer", &context, Reply::NAME));
    let outgoing = tokio::select! {
        outgoing = receiver.recv() => outgoing.unwrap(),
        result = &mut request => panic!("request unexpectedly completed: {result:?}"),
    };
    reject_invalid_frames(&invoker, &outgoing, &responder);
    let second_context = MessageContext::new(
        context.origin().clone(),
        Envelope::encode(&Reply(1)).unwrap(),
        Utc::now(),
    );
    assert!(matches!(
        invoker
            .request("peer", &second_context, Reply::NAME)
            .now_or_never(),
        Some(Err(BusError::Invalid(_)))
    ));
    assert!(receiver.try_recv().is_err());

    let reply = frame(
        &outgoing,
        Some(Envelope::encode(&Reply(42)).unwrap().to_vec().unwrap()),
    );
    assert!(invoker.accept_reply(&responder, reply.clone()).unwrap());
    assert_eq!(request.await.unwrap().decode::<Reply>().unwrap().0, 42);
    assert_eq!(invoker.pending().unwrap(), 0);
    assert!(!invoker.accept_reply(&responder, reply).unwrap());

    // The completed request released the single capacity slot. A valid negative
    // acknowledgement does not need an inner envelope and still reaches its caller.
    let mut next = Box::pin(invoker.request("peer", &second_context, Reply::NAME));
    let outgoing = tokio::select! {
        outgoing = receiver.recv() => outgoing.unwrap(),
        result = &mut next => panic!("request unexpectedly completed: {result:?}"),
    };
    assert!(
        invoker
            .accept_reply(&responder, frame(&outgoing, None))
            .unwrap()
    );
    assert!(matches!(next.await, Err(BusError::RemoteFailure)));
    assert_eq!(invoker.pending().unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn invalid_inner_reply_does_not_short_circuit_timeout_and_timeout_releases_capacity() {
    let (invoker, mut receiver, context, responder) = setup();
    let mut request = Box::pin(tokio::time::timeout(
        std::time::Duration::from_secs(5),
        invoker.request("peer", &context, Reply::NAME),
    ));
    let outgoing = tokio::select! {
        outgoing = receiver.recv() => outgoing.unwrap(),
        result = &mut request => panic!("request unexpectedly completed: {result:?}"),
    };
    reject_invalid_frames(&invoker, &outgoing, &responder);
    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    assert!(is_pending(&mut request).await);
    assert_eq!(invoker.pending().unwrap(), 1);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert!(request.as_mut().await.is_err());
    drop(request);
    assert_eq!(invoker.pending().unwrap(), 0);
    assert!(
        !invoker
            .accept_reply(
                &responder,
                frame(
                    &outgoing,
                    Some(Envelope::encode(&Reply(42)).unwrap().to_vec().unwrap())
                )
            )
            .unwrap()
    );
}
