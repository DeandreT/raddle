//! Canonical representability and independent admission bounds.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use prost::Message;
use raddle::wire::{Envelope, WireMessage};
use raddle_serialization::{
    BodyCodec, EndpointCodecs, Error, Json, Limits, MessagePack, MessagePackLayout, Protobuf,
    Registry,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Canonical {
    values: Vec<Option<f64>>,
}
impl WireMessage for Canonical {
    const NAME: &'static str = "canonical.finite";
    const VERSION: u32 = 1;
}
#[derive(Clone, PartialEq, Message)]
struct ProtoFloat {
    #[prost(double, tag = "1")]
    value: f64,
}

#[test]
fn nonfinite_native_values_are_diagnosed_before_nullable_canonical_projection_not_changed_to_null()
{
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            Json.encode(&Some(value), Limits::default()),
            Err(Error::Projection)
        );
        assert_eq!(
            Json.encode(
                &Canonical {
                    values: vec![Some(value)]
                },
                Limits::default()
            ),
            Err(Error::Projection)
        );
        assert_eq!(
            Json.encode(
                &BTreeMap::from([("a".to_owned(), Some(value))]),
                Limits::default()
            ),
            Err(Error::Projection)
        );
        assert_eq!(
            Json.encode(&(1, Some(value)), Limits::default()),
            Err(Error::Projection)
        );
    }
    let mut registry = Registry::default();
    registry
        .register::<ProtoFloat, Canonical, _, _, _>(
            Protobuf,
            |m| {
                Ok(Canonical {
                    values: vec![Some(m.value)],
                })
            },
            |m| {
                Ok(ProtoFloat {
                    value: m.values[0].unwrap(),
                })
            },
        )
        .unwrap();
    let codecs = EndpointCodecs::new(Arc::new(registry), Limits::default()).unwrap();
    let metadata = Envelope::encode(&Canonical {
        values: vec![Some(1.0)],
    })
    .unwrap()
    .metadata();
    // Independent proto3 field1 fixed64 quiet NaN: valid protobuf, not JSON.
    assert!(matches!(
        codecs.decode_body(
            metadata,
            "binary/protobuf",
            b"\x09\x00\x00\x00\x00\x00\x00\xf8\x7f"
        ),
        Err(Error::Projection)
    ));
}

#[test]
fn preflight_depth_nodes_encoded_and_canonical_output_bounds_are_independent() {
    assert_eq!(
        <Json as BodyCodec<u32>>::decode(
            &Json,
            b"\xef\xbb\xbf1",
            Limits {
                max_encoded_bytes: 3,
                ..Limits::default()
            }
        ),
        Err(Error::Limits)
    );
    let codec = MessagePack(MessagePackLayout::Positional);
    let deep = b"\x91\x91\x01";
    assert!(
        <MessagePack as BodyCodec<Vec<Vec<u32>>>>::decode(
            &codec,
            deep,
            Limits {
                max_depth: 2,
                ..Limits::default()
            }
        )
        .is_err()
    );
    assert!(
        <MessagePack as BodyCodec<Vec<Vec<u32>>>>::decode(
            &codec,
            deep,
            Limits {
                max_nodes: 2,
                ..Limits::default()
            }
        )
        .is_err()
    );
    assert!(
        <MessagePack as BodyCodec<Vec<Vec<u32>>>>::decode(
            &codec,
            deep,
            Limits {
                max_encoded_bytes: 2,
                ..Limits::default()
            }
        )
        .is_err()
    );
    assert!(
        <Protobuf as BodyCodec<ProtoFloat>>::decode(
            &Protobuf,
            b"\x10\x01\x18\x01",
            Limits {
                max_nodes: 1,
                ..Limits::default()
            }
        )
        .is_err()
    );
    #[derive(Serialize, Deserialize)]
    struct Expanded {
        text: String,
    }
    impl WireMessage for Expanded {
        const NAME: &'static str = "canonical.expanded";
        const VERSION: u32 = 1;
    }
    let mut registry = Registry::default();
    registry
        .register::<u8, Expanded, _, _, _>(
            Json,
            |_| {
                Ok(Expanded {
                    text: "x".repeat(2000),
                })
            },
            |_| Ok(1),
        )
        .unwrap();
    let metadata = Envelope::encode(&Expanded { text: "x".into() })
        .unwrap()
        .metadata();
    let limits = Limits {
        canonical: raddle::wire::EnvelopeLimits {
            max_bytes: 500,
            max_headers: 0,
        },
        ..Limits::default()
    };
    let codecs = EndpointCodecs::new(Arc::new(registry), limits).unwrap();
    assert!(matches!(
        codecs.decode_body(metadata, "application/json", b"1"),
        Err(Error::Limits)
    ));
    let short_declarations = Limits {
        max_declared_length: 1,
        ..Limits::default()
    };
    let map = BTreeMap::from([("a".to_owned(), u64::MAX)]);
    let bytes = codec.encode(&map, short_declarations).unwrap();
    assert_eq!(
        <MessagePack as BodyCodec<BTreeMap<String, u64>>>::decode(
            &codec,
            &bytes,
            short_declarations
        )
        .unwrap(),
        map
    );
}
