//! Exact codec availability, inheritance and callback-free startup lookup.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use raddle::wire::{Envelope, WireMessage};
use raddle_serialization::{EndpointCodecs, Json, Limits, Registry};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Canonical(u32);
impl WireMessage for Canonical {
    const NAME: &'static str = "availability.canonical";
    const VERSION: u32 = 2;
}

#[test]
fn exact_canonical_keys_inherit_and_local_override_without_callbacks_or_fallback() {
    let mut application = Registry::default();
    application
        .register::<String, Canonical, _, _, _>(
            Json,
            |text| {
                text.parse()
                    .map(Canonical)
                    .map_err(|_| raddle_serialization::Error::Projection)
            },
            |canonical| Ok(canonical.0.to_string()),
        )
        .unwrap();
    let application = Arc::new(application);
    let mut endpoint = EndpointCodecs::new(Arc::clone(&application), Limits::default()).unwrap();
    let sibling = EndpointCodecs::new(application, Limits::default()).unwrap();
    assert!(endpoint.supports("application/json", Canonical::NAME, 2));
    for (media, contract, version) in [
        ("binary/messagepack", Canonical::NAME, 2),
        ("application/json", "availability.missing", 2),
        ("application/json", Canonical::NAME, 1),
        ("application/json", Canonical::NAME, 0),
        ("application/json; charset=utf-8", Canonical::NAME, 2),
        ("application/json", "Invalid Contract", 2),
    ] {
        assert!(!endpoint.supports(media, contract, version));
    }
    endpoint.local_mut().json::<Canonical>().unwrap();
    assert!(endpoint.supports("application/json", Canonical::NAME, 2));
    let model = Envelope::encode(&Canonical(7)).unwrap();
    assert_eq!(
        endpoint.encode_body(&model, "application/json").unwrap(),
        b"7"
    );
    assert_eq!(
        sibling.encode_body(&model, "application/json").unwrap(),
        b"\"7\""
    );

    // Lookup itself must not fabricate models or execute either projection.
    let mut exploding = Registry::default();
    exploding
        .register::<Canonical, Canonical, _, _, _>(Json, |_| panic!("decode"), |_| panic!("encode"))
        .unwrap();
    let endpoint = EndpointCodecs::new(Arc::new(exploding), Limits::default()).unwrap();
    assert!(endpoint.supports("application/json", Canonical::NAME, 2));
}
