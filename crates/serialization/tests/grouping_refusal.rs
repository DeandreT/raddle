//! Metadata-losing selected codecs refuse before application conversion.
#![allow(clippy::unwrap_used)]
use raddle::grouping::{Dispatch, GroupId};
use raddle::wire::{Envelope, WireMessage};
use raddle_serialization::{EndpointCodecs, Json, Registry};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Serialize, Deserialize)]
struct Work;
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}

#[test]
fn selected_and_body_only_encoding_refuse_before_custom_projection() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut registry = Registry::default();
    registry
        .register::<Work, Work, _, _, _>(Json, Ok, move |work| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(work)
        })
        .unwrap();
    let codecs = EndpointCodecs::new(Arc::new(registry), Default::default()).unwrap();
    let envelope = Envelope::encode(&Work)
        .unwrap()
        .with_dispatch(Dispatch::resolved(Some(GroupId::new("")), None))
        .unwrap();
    assert!(codecs.encode(&envelope).is_err());
    assert!(codecs.encode_body(&envelope, "application/json").is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    use raddle_serialization::cloudevents::{
        Aliases, Binding, EventMetadata, Mapper, NativeIdentity,
    };
    let mut aliases = Aliases::default();
    aliases.register::<Work>("urn:group.work").unwrap();
    let identity_calls = Arc::new(AtomicUsize::new(0));
    let observed = identity_calls.clone();
    let id = envelope.id();
    let created_at = envelope.created_at();
    let mapper = Mapper::new(Arc::new(codecs), aliases, Default::default(), move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(NativeIdentity { id, created_at })
    })
    .unwrap();
    let external = EventMetadata {
        id: "example".into(),
        source: "/synthetic".into(),
        event_type: "urn:group.work".into(),
        time: None,
        subject: None,
        dataschema: None,
        extensions: Default::default(),
    };
    assert!(mapper.encode_structured(&envelope, &external).is_err());
    assert!(
        mapper
            .encode_binary(Binding::Http, &envelope, &external)
            .is_err()
    );
    assert_eq!(identity_calls.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn selected_decode_refuses_dispatch_injected_by_a_canonicalizer() {
    let mut registry = Registry::default();
    registry.json::<Work>().unwrap();
    let codecs = EndpointCodecs::new(Arc::new(registry), Default::default()).unwrap();
    let envelope = Envelope::encode(&Work).unwrap();
    let frame = codecs.encode(&envelope).unwrap();
    let body = codecs.encode_body(&envelope, "application/json").unwrap();
    let codecs = codecs.with_canonicalizer(|envelope| {
        envelope
            .with_dispatch(Dispatch::resolved(None, None))
            .map_err(|_| raddle_serialization::Error::Projection)
    });
    assert!(codecs.decode(&frame).is_err());
    assert!(
        codecs
            .decode_body(envelope.metadata(), "application/json", &body)
            .is_err()
    );
}
