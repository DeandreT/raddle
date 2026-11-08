//! Trusted metadata boundaries and explicit typed return wrappers.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;

use chrono::{TimeZone, Utc};
use futures_util::stream;
use raddle::wire::{Envelope, WireMessage};
use raddle_messaging::effects::{Effect, EffectError, EffectIter, Effects};
use raddle_messaging::{Cancellation, HeaderError, MessageContext, TrustedOrigin};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Input;
impl WireMessage for Input {
    const NAME: &'static str = "test.context";
    const VERSION: u32 = 1;
}

fn context() -> MessageContext {
    let envelope = Envelope::encode(&Input)
        .unwrap()
        .with_header("tenant".into(), "attacker".into())
        .unwrap()
        .with_header("x-count".into(), "42".into())
        .unwrap()
        .with_header("x-bad".into(), "not-a-number".into())
        .unwrap()
        .with_header("authorization".into(), "not-trusted".into())
        .unwrap();
    MessageContext::new(
        TrustedOrigin::authenticated("tenant-a", "source-a", Some("alice".into())).unwrap(),
        envelope,
        Utc.timestamp_opt(1234, 0).unwrap(),
    )
}

#[test]
fn trusted_context_header_bindings_and_child_metadata_do_not_upgrade_untrusted_identity() {
    let context = context();
    assert_eq!(context.origin().tenant(), "tenant-a");
    assert_eq!(context.origin().source(), "source-a");
    assert_eq!(context.origin().username(), Some("alice"));
    assert_eq!(context.now(), Utc.timestamp_opt(1234, 0).unwrap());
    assert_eq!(context.header::<usize>("x-count"), Ok(42));
    assert_eq!(context.header::<usize>("x-bad"), Err(HeaderError::Invalid));
    assert_eq!(context.header::<usize>("absent"), Err(HeaderError::Missing));
    let outgoing = Envelope::encode(&Input)
        .unwrap()
        .with_header("x-count".into(), "99".into())
        .unwrap();
    let outgoing = context
        .outgoing(outgoing, &BTreeSet::from(["x-count".into()]))
        .unwrap();
    assert_eq!(
        outgoing.headers().get("x-count").map(String::as_str),
        Some("99")
    );
    assert!(!outgoing.headers().contains_key("tenant"));
    assert!(!outgoing.headers().contains_key("authorization"));
    assert_eq!(outgoing.correlation_id(), Some(context.correlation_id()));
    assert_eq!(outgoing.causation_id(), Some(context.envelope().id()));
}

#[tokio::test]
async fn message_cancellation_is_independent_but_host_shutdown_reaches_every_child() {
    let host = Cancellation::new();
    let first = host.child();
    let second = host.child();
    first.cancel();
    first.cancelled().await;
    assert!(!host.is_cancelled());
    assert!(!second.is_cancelled());
    host.cancel();
    second.cancelled().await;
    assert!(second.is_cancelled());
}

#[tokio::test]
async fn explicit_optional_tuple_iterable_and_stream_return_shapes_are_bounded_and_cancellable() {
    let _: Effects<()> = Effects::returned((
        Some(Effect::Nothing),
        None::<Effect<()>>,
        vec![Effect::Nothing],
    ))
    .unwrap();
    let _: Effects<()> = Effects::returned(EffectIter((0..256).map(|_| Effect::Nothing))).unwrap();
    assert!(matches!(
        Effects::<()>::returned(EffectIter((0..257).map(|_| Effect::Nothing))),
        Err(EffectError::Capacity)
    ));
    let context = context();
    let _: Effects<()> = Effects::from_stream(stream::iter([Ok(Effect::Nothing)]), &context)
        .await
        .unwrap();
    let _: Effects<()> = Effects::from_stream(stream::empty(), &context)
        .await
        .unwrap();
    assert!(matches!(
        Effects::<()>::from_stream(stream::iter([Err(EffectError::Failed("source"))]), &context)
            .await,
        Err(EffectError::Failed("source"))
    ));
    context.cancellation().cancel();
    assert!(matches!(
        Effects::<()>::from_stream(stream::pending(), &context).await,
        Err(EffectError::Cancelled)
    ));
}
