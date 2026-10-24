//! Wire validation, redaction and schema evolution contracts.
#![allow(clippy::unwrap_used)]

use raddle::wire::{Codecs, Envelope, EnvelopeLimits, WireError, WireMessage};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Old {
    amount: u32,
}
impl WireMessage for Old {
    const NAME: &'static str = "orders.place";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct New {
    quantity: u32,
}
impl WireMessage for New {
    const NAME: &'static str = "orders.place";
    const VERSION: u32 = 2;
}

#[test]
fn envelope_round_trip_preserves_identity_and_contract() {
    let envelope = Envelope::encode(&Old { amount: 42 }).unwrap();
    let decoded = Envelope::from_slice(&envelope.to_vec().unwrap(), Default::default()).unwrap();
    assert_eq!(decoded.id(), envelope.id());
    assert_eq!(decoded.decode::<Old>().unwrap(), Old { amount: 42 });
    assert!(matches!(
        decoded.decode::<New>(),
        Err(WireError::Unsupported)
    ));
}

#[test]
fn framing_limits_and_unknown_metadata_are_rejected() {
    let bytes = Envelope::encode(&Old { amount: 1 })
        .unwrap()
        .to_vec()
        .unwrap();
    assert!(
        Envelope::from_slice(
            &bytes,
            EnvelopeLimits {
                max_bytes: 5,
                ..Default::default()
            }
        )
        .is_err()
    );
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["tenant"] = serde_json::json!("untrusted");
    assert!(
        Envelope::from_slice(&serde_json::to_vec(&value).unwrap(), Default::default()).is_err()
    );
    value.as_object_mut().unwrap().remove("tenant");
    value["version"] = serde_json::json!(0);
    assert!(
        Envelope::from_slice(&serde_json::to_vec(&value).unwrap(), Default::default()).is_err()
    );
}

#[test]
fn upcasting_is_explicit_and_does_not_mutate_the_original() {
    let envelope = Envelope::encode(&Old { amount: 7 }).unwrap();
    let mut codecs = Codecs::default();
    assert!(codecs.decode::<New>(&envelope).is_err());
    codecs
        .upcast(Old::NAME, 1, |value| {
            Ok(serde_json::json!({ "quantity": value["amount"] }))
        })
        .unwrap();
    assert_eq!(
        codecs.decode::<New>(&envelope).unwrap(),
        New { quantity: 7 }
    );
    assert_eq!(envelope.decode::<Old>().unwrap(), Old { amount: 7 });
    assert!(matches!(
        codecs.upcast(Old::NAME, 1, Ok),
        Err(WireError::DuplicateUpcaster)
    ));
    assert!(
        codecs
            .decode::<Old>(&Envelope::encode(&New { quantity: 2 }).unwrap())
            .is_err()
    );
}

#[test]
fn debug_does_not_disclose_payload_or_header_values() {
    let envelope = Envelope::encode(&Old { amount: 735713 })
        .unwrap()
        .with_header("secret".into(), "private-token".into())
        .unwrap();
    let output = format!("{envelope:?}");
    assert!(!output.contains("735713"));
    assert!(!output.contains("private-token"));
    assert!(
        envelope
            .clone()
            .with_header("Bad Header".into(), "value".into())
            .is_err()
    );
}
