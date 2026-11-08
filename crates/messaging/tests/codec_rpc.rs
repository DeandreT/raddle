//! Selected native RPC admission, hostile neighbors and caller-owned lifecycle.
#![cfg(feature = "serialization-rpc")]
#![allow(clippy::unwrap_used)]

mod codec_rpc_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use futures_util::FutureExt;
use raddle::RuntimeBuilder;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_messaging::{
    BusBuilder, BusError, InvokeError, InvokeOptions, MessageContext, RemoteInvoker,
    RemoteResponder, ReplyHubInvoker, SelectedRpc, TrustedOrigin,
};
use raddle_serialization::{EndpointCodecs, Limits};
use tokio::sync::mpsc;
use uuid::Uuid;

use codec_rpc_support::{Answer, Double, Frame, Response};
use codec_rpc_support::{body_frame, codecs, origin, registry, reply_frame, selected};

#[derive(Clone)]
struct Capture(mpsc::UnboundedSender<Envelope>);
impl Transport for Capture {
    async fn publish(&self, _: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.0
            .send(envelope.clone())
            .map_err(|_| TransportError::Transient)
    }
}

fn setup(
    codecs: Arc<EndpointCodecs>,
) -> (
    raddle_messaging::MessageBus,
    Arc<ReplyHubInvoker<Capture>>,
    mpsc::UnboundedReceiver<Envelope>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let invoker = Arc::new(
        ReplyHubInvoker::new(
            Capture(sender.clone()),
            1,
            "replies",
            [("requests".into(), "server".into())],
        )
        .unwrap()
        .with_selected_rpc(selected(codecs))
        .unwrap(),
    );
    let mut bus = BusBuilder::new(RuntimeBuilder::default().build());
    bus.remote("requests", Capture(sender)).unwrap();
    bus.route::<Double>("requests").unwrap();
    bus.remote_invoker(invoker.clone(), true);
    (bus.build(), invoker, receiver)
}

async fn outgoing<F: std::future::Future>(
    future: &mut std::pin::Pin<Box<F>>,
    receiver: &mut mpsc::UnboundedReceiver<Envelope>,
) -> Envelope {
    tokio::select! {
        outgoing = receiver.recv() => outgoing.unwrap(),
        _ = future => panic!("invocation completed before response"),
    }
}

fn remodel(envelope: &Envelope, change: impl FnOnce(&mut serde_json::Value)) -> Envelope {
    let mut value = serde_json::to_value(envelope).unwrap();
    change(&mut value);
    Envelope::from_slice(&serde_json::to_vec(&value).unwrap(), Default::default()).unwrap()
}

#[tokio::test]
async fn malformed_first_middle_last_cannot_consume_waiter_and_later_invocations_work() {
    for media in ["application/json", "binary/messagepack", "binary/protobuf"] {
        let codecs = codecs(media);
        let (bus, invoker, mut receiver) = setup(Arc::clone(&codecs));
        let inner = Envelope::encode(&Answer { number: 42 }).unwrap();
        let good = codecs.encode(&inner).unwrap();
        for bad_position in 0..3 {
            for position in 0..3 {
                let mut call = Box::pin(bus.invoke(Double { number: 21 }, origin("caller")));
                let request = outgoing(&mut call, &mut receiver).await;
                if position == bad_position {
                    let mut wrong_schema = inner.metadata();
                    wrong_schema.version += 1;
                    let mut wrong_contract = inner.metadata();
                    wrong_contract.contract = Double::NAME.into();
                    let mut padded = good.clone();
                    padded.push(0);
                    let mut truncated = good.clone();
                    truncated.pop();
                    let mut version = good.clone();
                    version[4] = 2;
                    let bad_bodies = [
                        b"not a frame".to_vec(),
                        vec![],
                        version,
                        padded,
                        truncated,
                        body_frame(inner.metadata(), "binary/no-codec", b"x"),
                        body_frame(wrong_schema, media, b"x"),
                        body_frame(wrong_contract, media, b"x"),
                        codecs
                            .encode(&Envelope::encode(&Double { number: 42 }).unwrap())
                            .unwrap(),
                    ];
                    for bytes in bad_bodies {
                        assert!(
                            !invoker
                                .accept_reply(
                                    &origin("server"),
                                    reply_frame(&request, media, Some(bytes))
                                )
                                .unwrap()
                        );
                        assert_eq!(invoker.pending().unwrap(), 1);
                    }
                    if media != "application/json" {
                        // JSON is registered too, but explicit selected mode may
                        // not silently accept that different incoming media.
                        let json =
                            body_frame(inner.metadata(), "application/json", br#"{"number":42}"#);
                        assert!(
                            !invoker
                                .accept_reply(
                                    &origin("server"),
                                    reply_frame(&request, media, Some(json))
                                )
                                .unwrap()
                        );
                    }
                    let valid = reply_frame(&request, media, Some(good.clone()));
                    let claiming_identity = codecs
                        .encode(
                            &inner
                                .clone()
                                .with_header("tenant".into(), "configured-tenant".into())
                                .unwrap()
                                .with_header("source".into(), "server".into())
                                .unwrap(),
                        )
                        .unwrap();
                    assert!(
                        !invoker
                            .accept_reply(
                                &origin("impostor"),
                                reply_frame(&request, media, Some(claiming_identity))
                            )
                            .unwrap()
                    );
                    for bad in [
                        remodel(&valid, |v| v["version"] = 1.into()),
                        remodel(&valid, |v| v["version"] = 3.into()),
                        remodel(&valid, |v| {
                            v["correlation_id"] = Uuid::new_v4().to_string().into()
                        }),
                        remodel(&valid, |v| {
                            v["causation_id"] = Uuid::new_v4().to_string().into()
                        }),
                        remodel(&valid, |v| {
                            v["payload"]["mode"] = "selected-v2:unknown".into()
                        }),
                        remodel(&valid, |v| {
                            v["payload"].as_object_mut().unwrap().remove("response");
                        }),
                        remodel(&valid, |v| {
                            v["payload"]["response"] = serde_json::json!({"status":"success"})
                        }),
                        remodel(&valid, |v| {
                            v["payload"]["response"] =
                                serde_json::json!({"status":"failure","body":[]})
                        }),
                    ] {
                        assert!(!invoker.accept_reply(&origin("server"), bad).unwrap());
                    }
                    assert!(
                        !invoker
                            .accept_reply(&origin("impostor"), valid.clone())
                            .unwrap()
                    );
                    assert!(
                        !invoker
                            .accept_reply(
                                &TrustedOrigin::authenticated("other-tenant", "server", None)
                                    .unwrap(),
                                valid.clone()
                            )
                            .unwrap()
                    );
                    assert_eq!(invoker.pending().unwrap(), 1);
                    // Capacity refusal must not publish another business request.
                    assert!(matches!(
                        bus.invoke(Double { number: 1 }, origin("caller"))
                            .now_or_never(),
                        Some(Err(InvokeError::Bus(BusError::Invalid(_))))
                    ));
                    assert!(receiver.try_recv().is_err());
                }
                let valid = reply_frame(&request, media, Some(good.clone()));
                assert!(
                    invoker
                        .accept_reply(&origin("server"), valid.clone())
                        .unwrap()
                );
                assert_eq!(call.await.unwrap().number, 42);
                assert_eq!(invoker.pending().unwrap(), 0);
                assert!(!invoker.accept_reply(&origin("server"), valid).unwrap());
            }
        }
    }
}

#[tokio::test]
async fn protected_raw_roots_fail_before_defaulted_typed_reply_acceptance() {
    for media in ["application/json", "binary/messagepack"] {
        let codecs = codecs(media);
        let (bus, invoker, mut receiver) = setup(Arc::clone(&codecs));
        let mut call = Box::pin(bus.invoke(Double { number: 21 }, origin("caller")));
        let request = outgoing(&mut call, &mut receiver).await;
        let protected =
            serde_json::json!({"format":"raddle-aes256gcm-v2", "ciphertext":"not-a-domain-model"});
        let bytes = if media == "application/json" {
            serde_json::to_vec(&protected).unwrap()
        } else {
            rmp_serde::to_vec_named(&protected).unwrap()
        };
        // A permissive/defaulted model alone would erase the reserved root marker.
        let bytes = body_frame(
            Envelope::encode(&Answer { number: 0 }).unwrap().metadata(),
            media,
            &bytes,
        );
        assert!(
            !invoker
                .accept_reply(&origin("server"), reply_frame(&request, media, Some(bytes)))
                .unwrap()
        );
        assert_eq!(invoker.pending().unwrap(), 1);
        let valid = codecs
            .encode(&Envelope::encode(&Answer { number: 42 }).unwrap())
            .unwrap();
        assert!(
            invoker
                .accept_reply(&origin("server"), reply_frame(&request, media, Some(valid)))
                .unwrap()
        );
        assert_eq!(call.await.unwrap().number, 42);
    }
}

#[cfg(feature = "crypto-ingress")]
#[tokio::test]
async fn actual_encryption_codec_v2_frames_cannot_default_into_typed_rpc_replies() {
    use raddle_crypto::{
        EncryptionCodec, EncryptionPolicy, KeyId, RotatingKeys, SecretKey, Selection,
    };
    let keys = RotatingKeys::new(1).unwrap();
    keys.insert(
        KeyId::new("synthetic-local-test").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    let crypto = EncryptionCodec::new(
        keys,
        EncryptionPolicy {
            send: Selection::All,
            ..Default::default()
        },
        Default::default(),
    );
    let encrypted = crypto
        .encode(&Envelope::encode(&Answer { number: 42 }).unwrap())
        .await
        .unwrap();
    let value = serde_json::to_value(&encrypted).unwrap();
    let protected = &value["payload"];
    assert_eq!(protected["format"], "raddle-aes256gcm-v2");
    for media in ["application/json", "binary/messagepack"] {
        let codecs = codecs(media);
        let (bus, invoker, mut receiver) = setup(Arc::clone(&codecs));
        let mut call = Box::pin(bus.invoke(Double { number: 21 }, origin("caller")));
        let request = outgoing(&mut call, &mut receiver).await;
        let bytes = if media == "application/json" {
            serde_json::to_vec(protected).unwrap()
        } else {
            rmp_serde::to_vec_named(protected).unwrap()
        };
        let body = body_frame(encrypted.metadata(), media, &bytes);
        assert!(
            !invoker
                .accept_reply(&origin("server"), reply_frame(&request, media, Some(body)))
                .unwrap()
        );
        assert_eq!(invoker.pending().unwrap(), 1);
        let body = codecs
            .encode(&Envelope::encode(&Answer { number: 42 }).unwrap())
            .unwrap();
        assert!(
            invoker
                .accept_reply(&origin("server"), reply_frame(&request, media, Some(body)))
                .unwrap()
        );
        assert_eq!(call.await.unwrap().number, 42);
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_timeout_drop_and_stale_token_release_both_registries() {
    let codecs = codecs("binary/messagepack");
    let (bus, invoker, mut receiver) = setup(Arc::clone(&codecs));
    let mut stale = None;
    for reason in ["cancel", "timeout", "drop"] {
        let context = MessageContext::new(
            origin("caller"),
            Envelope::encode(&Double { number: 1 }).unwrap(),
            Utc::now(),
        );
        let options = InvokeOptions {
            remote_timeout: Duration::from_secs(1),
            ..Default::default()
        };
        let mut call = Box::pin(bus.invoke_context(Double { number: 1 }, context.clone(), options));
        let request = outgoing(&mut call, &mut receiver).await;
        stale = Some(reply_frame(&request, "binary/messagepack", None));
        assert_eq!(invoker.pending().unwrap(), 1);
        match reason {
            "cancel" => {
                context.cancellation().cancel();
                assert!(matches!(call.as_mut().await, Err(InvokeError::Cancelled)));
            }
            "timeout" => {
                tokio::time::advance(Duration::from_secs(2)).await;
                assert!(matches!(call.as_mut().await, Err(InvokeError::TimedOut)));
            }
            _ => {}
        }
        drop(call);
        assert_eq!(invoker.pending().unwrap(), 0);
    }
    let mut call = Box::pin(bus.invoke(Double { number: 21 }, origin("caller")));
    let request = outgoing(&mut call, &mut receiver).await;
    assert!(
        !invoker
            .accept_reply(&origin("server"), stale.unwrap())
            .unwrap()
    );
    assert_eq!(invoker.pending().unwrap(), 1);
    let good = codecs
        .encode(&Envelope::encode(&Answer { number: 42 }).unwrap())
        .unwrap();
    assert!(
        invoker
            .accept_reply(
                &origin("server"),
                reply_frame(&request, "binary/messagepack", Some(good))
            )
            .unwrap()
    );
    assert_eq!(call.await.unwrap().number, 42);
    assert_eq!(invoker.pending().unwrap(), 0);
}

#[tokio::test]
async fn real_typed_configuration_and_peer_modes_refuse_before_handler() {
    for media in ["application/json", "binary/messagepack", "binary/protobuf"] {
        for (request, response) in [(true, false), (false, true)] {
            let endpoint = Arc::new(
                EndpointCodecs::new(
                    Arc::new(registry(media, request, response)),
                    Limits::default(),
                )
                .unwrap(),
            );
            let mut missing = SelectedRpc::new(Arc::clone(&endpoint));
            assert!(missing.register::<Double>().is_err());
            let (sender, _) = mpsc::unbounded_channel();
            assert!(
                ReplyHubInvoker::new(
                    Capture(sender.clone()),
                    1,
                    "replies",
                    [("requests".into(), "server".into())]
                )
                .unwrap()
                .with_selected_rpc(missing)
                .is_err()
            );
            assert!(
                RemoteResponder::new(
                    Capture(sender),
                    RuntimeBuilder::default().build(),
                    ["replies".into()]
                )
                .unwrap()
                .with_selected_rpc(SelectedRpc::new(endpoint))
                .is_err()
            );
        }
        let codecs = codecs(media);
        let count = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&count);
        let mut runtime = RuntimeBuilder::default();
        runtime
            .handle(move |_: Double| {
                let count = Arc::clone(&counted);
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(Answer { number: 42 })
                }
            })
            .unwrap();
        let runtime = runtime.build();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut responder =
            RemoteResponder::new(Capture(sender.clone()), runtime.clone(), ["replies".into()])
                .unwrap()
                .with_selected_rpc(selected(Arc::clone(&codecs)))
                .unwrap();
        responder.handle::<Double>().unwrap();
        let request = Envelope::encode(&Double { number: 21 })
            .unwrap()
            .with_header("raddle-reply-to".into(), "replies".into())
            .unwrap()
            .with_header("raddle-request-token".into(), Uuid::new_v4().to_string())
            .unwrap();
        for mode in [
            None,
            Some("legacy"),
            Some("selected-v3:application/json"),
            Some("selected-v2:binary/unknown"),
        ] {
            let request = match mode {
                Some(mode) => request
                    .clone()
                    .with_header("raddle-reply-mode".into(), mode.into())
                    .unwrap(),
                None => request.clone(),
            };
            assert!(
                responder
                    .respond(&MessageContext::new(origin("caller"), request, Utc::now()))
                    .await
                    .is_err()
            );
            assert_eq!(count.load(Ordering::SeqCst), 0);
            assert!(receiver.try_recv().is_err());
        }
        let request = request
            .with_header("raddle-reply-mode".into(), format!("selected-v2:{media}"))
            .unwrap();
        let protected_request = remodel(&request, |value| {
            value["payload"] =
                serde_json::json!({"format":"raddle-aes256gcm-v2", "ciphertext":"synthetic"})
        });
        assert!(
            responder
                .respond(&MessageContext::new(
                    origin("caller"),
                    protected_request,
                    Utc::now()
                ))
                .await
                .is_err()
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        for bad in [
            request
                .clone()
                .with_header("raddle-reply-to".into(), "unconfigured".into())
                .unwrap(),
            request
                .clone()
                .with_header("raddle-request-token".into(), Uuid::nil().to_string())
                .unwrap(),
            request
                .clone()
                .with_header("raddle-request-token".into(), "not-a-token".into())
                .unwrap(),
        ] {
            assert!(
                responder
                    .respond(&MessageContext::new(origin("caller"), bad, Utc::now()))
                    .await
                    .is_err()
            );
            assert_eq!(count.load(Ordering::SeqCst), 0);
        }
        let mut legacy =
            RemoteResponder::new(Capture(sender), runtime, ["replies".into()]).unwrap();
        legacy.handle::<Double>().unwrap();
        assert!(
            legacy
                .respond(&MessageContext::new(
                    origin("caller"),
                    request.clone(),
                    Utc::now()
                ))
                .await
                .is_err()
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        responder
            .respond(&MessageContext::new(origin("caller"), request, Utc::now()))
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let frame = receiver.recv().await.unwrap().decode::<Frame>().unwrap();
        let Response::Success { body } = frame.response else {
            panic!("expected success")
        };
        codec_rpc_support::assert_representation(&body, media, 42);

        let (_, invoker, mut outgoing) = setup(Arc::clone(&codecs));
        let context = MessageContext::new(
            origin("caller"),
            Envelope::encode(&Double { number: 1 }).unwrap(),
            Utc::now(),
        );
        assert!(
            invoker
                .request("requests", &context, Double::NAME)
                .await
                .is_err()
        );
        assert_eq!(invoker.pending().unwrap(), 0);
        assert!(outgoing.try_recv().is_err());
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Expanded {
    number: u32,
    padding: String,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct InvalidAnswer {
    number: String,
}
impl WireMessage for InvalidAnswer {
    const NAME: &'static str = Answer::NAME;
    const VERSION: u32 = Answer::VERSION;
}
impl WireMessage for Expanded {
    const NAME: &'static str = Answer::NAME;
    const VERSION: u32 = Answer::VERSION;
}

#[tokio::test]
async fn encoded_outer_and_post_upcast_bounds_precede_waiter_consumption_and_decode_runs_once() {
    let expand = Arc::new(AtomicUsize::new(1));
    let calls = Arc::new(AtomicUsize::new(0));
    let should_expand = Arc::clone(&expand);
    let counted = Arc::clone(&calls);
    let limits = Limits {
        max_encoded_bytes: 1024,
        canonical: raddle::wire::EnvelopeLimits {
            max_bytes: 800,
            ..Default::default()
        },
        ..Default::default()
    };
    let codecs = Arc::new(
        EndpointCodecs::new(Arc::new(registry("application/json", true, true)), limits)
            .unwrap()
            .with_canonicalizer(move |envelope| {
                counted.fetch_add(1, Ordering::SeqCst);
                if should_expand.load(Ordering::SeqCst) == 1 {
                    envelope
                        .rebody(
                            &Expanded {
                                number: 42,
                                padding: "x".repeat(4000),
                            },
                            Default::default(),
                        )
                        .map_err(|_| raddle_serialization::Error::Projection)
                } else if should_expand.load(Ordering::SeqCst) == 2 {
                    envelope
                        .rebody(
                            &InvalidAnswer {
                                number: "not-an-integer".into(),
                            },
                            Default::default(),
                        )
                        .map_err(|_| raddle_serialization::Error::Projection)
                } else {
                    Ok(envelope)
                }
            }),
    );
    let (bus, invoker, mut receiver) = setup(Arc::clone(&codecs));
    let mut call = Box::pin(bus.invoke(Double { number: 21 }, origin("caller")));
    let request = outgoing(&mut call, &mut receiver).await;
    let inner = Envelope::encode(&Answer { number: 42 }).unwrap();
    let body = codecs.encode(&inner).unwrap();
    let good = reply_frame(&request, "application/json", Some(body));
    assert!(
        !invoker
            .accept_reply(&origin("server"), good.clone())
            .unwrap()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let too_big = body_frame(inner.metadata(), "application/json", &vec![b' '; 2000]);
    assert!(
        !invoker
            .accept_reply(
                &origin("server"),
                reply_frame(&request, "application/json", Some(too_big))
            )
            .unwrap()
    );
    // An Envelope can be explicitly constructed with looser limits; accept_reply
    // must still enforce its own outer protocol boundary before typed decoding.
    let large = serde_json::json!({"mode":"selected-v2:application/json", "response":{"status":"success", "body": vec![255u8; 300_000]}});
    let large = Envelope::from_parts(
        good.metadata(),
        large,
        raddle::wire::EnvelopeLimits {
            max_bytes: 2_000_000,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!invoker.accept_reply(&origin("server"), large).unwrap());
    assert_eq!(invoker.pending().unwrap(), 1);
    expand.store(2, Ordering::SeqCst);
    assert!(
        !invoker
            .accept_reply(&origin("server"), good.clone())
            .unwrap()
    );
    assert_eq!(invoker.pending().unwrap(), 1);
    expand.store(0, Ordering::SeqCst);
    assert!(invoker.accept_reply(&origin("server"), good).unwrap());
    assert_eq!(call.await.unwrap().number, 42);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(invoker.pending().unwrap(), 0);
}

struct EncodeGate(Arc<AtomicUsize>);
impl raddle_serialization::BodyCodec<Answer> for EncodeGate {
    fn content_type(&self) -> &'static str {
        "application/json"
    }
    fn encode(
        &self,
        message: &Answer,
        limits: Limits,
    ) -> Result<Vec<u8>, raddle_serialization::Error> {
        if self.0.load(Ordering::SeqCst) == 0 {
            return Err(raddle_serialization::Error::Projection);
        }
        raddle_serialization::BodyCodec::encode(&raddle_serialization::Json, message, limits)
    }
    fn decode(&self, bytes: &[u8], limits: Limits) -> Result<Answer, raddle_serialization::Error> {
        raddle_serialization::BodyCodec::decode(&raddle_serialization::Json, bytes, limits)
    }
}

struct Uncertain(Arc<AtomicUsize>);
impl Transport for Uncertain {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::Transient)
    }
}

#[tokio::test]
async fn reply_encoding_and_uncertain_publication_never_report_success_or_replay_handler() {
    let gate = Arc::new(AtomicUsize::new(0));
    let mut registry = registry("application/json", true, false);
    registry
        .register::<Answer, Answer, _, _, _>(EncodeGate(Arc::clone(&gate)), Ok, Ok)
        .unwrap();
    let codecs = Arc::new(EndpointCodecs::new(Arc::new(registry), Limits::default()).unwrap());
    let business = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&business);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(move |_: Double| {
            let count = Arc::clone(&counted);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(Answer { number: 42 })
            }
        })
        .unwrap();
    let sent = Arc::new(AtomicUsize::new(0));
    let mut responder = RemoteResponder::new(
        Uncertain(Arc::clone(&sent)),
        runtime.build(),
        ["replies".into()],
    )
    .unwrap()
    .with_selected_rpc(selected(codecs))
    .unwrap();
    responder.handle::<Double>().unwrap();
    let context = MessageContext::new(
        origin("caller"),
        Envelope::encode(&Double { number: 21 })
            .unwrap()
            .with_header(
                "raddle-reply-mode".into(),
                "selected-v2:application/json".into(),
            )
            .unwrap()
            .with_header("raddle-reply-to".into(), "replies".into())
            .unwrap()
            .with_header("raddle-request-token".into(), Uuid::new_v4().to_string())
            .unwrap(),
        Utc::now(),
    );
    assert!(responder.respond(&context).await.is_err());
    assert_eq!(business.load(Ordering::SeqCst), 1);
    assert_eq!(sent.load(Ordering::SeqCst), 0);
    gate.store(1, Ordering::SeqCst);
    assert!(matches!(
        responder.respond(&context).await,
        Err(BusError::Transport(TransportError::Transient))
    ));
    assert_eq!(business.load(Ordering::SeqCst), 2);
    assert_eq!(sent.load(Ordering::SeqCst), 1);
    // No spawned retry exists; subsequent independent requests remain callable.
}

#[tokio::test]
async fn publication_failure_releases_registration_and_keeps_transport_owned() {
    let sent = Arc::new(AtomicUsize::new(0));
    let invoker = ReplyHubInvoker::new(
        Uncertain(Arc::clone(&sent)),
        1,
        "replies",
        [("requests".into(), "server".into())],
    )
    .unwrap()
    .with_selected_rpc(selected(codecs("binary/protobuf")))
    .unwrap();
    for _ in 0..3 {
        let context = MessageContext::new(
            origin("caller"),
            Envelope::encode(&Double { number: 21 }).unwrap(),
            Utc::now(),
        );
        assert!(matches!(
            invoker.request("requests", &context, Answer::NAME).await,
            Err(BusError::Transport(TransportError::Transient))
        ));
        assert_eq!(invoker.pending().unwrap(), 0);
    }
    assert_eq!(sent.load(Ordering::SeqCst), 3);
}
