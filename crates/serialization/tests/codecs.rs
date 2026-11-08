//! Independently authored typed body vectors, selection and projection bounds.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use prost::Message;
use raddle::wire::{Codecs, Envelope, EnvelopeMetadata, WireMessage};
use raddle_serialization::{
    BodyCodec, EndpointCodecs, Error, Json, Limits, MessagePack, MessagePackLayout, Protobuf,
    Registry,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Text {
    text: String,
}
impl WireMessage for Text {
    const NAME: &'static str = "serial.text";
    const VERSION: u32 = 1;
}

#[derive(Clone, PartialEq, Message)]
struct ProtoText {
    #[prost(string, tag = "1")]
    text: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Collection {
    text: String,
    number: u32,
    maybe: Option<String>,
    items: Vec<i32>,
}
impl WireMessage for Collection {
    const NAME: &'static str = "serial.collection";
    const VERSION: u32 = 1;
}

fn metadata() -> EnvelopeMetadata {
    EnvelopeMetadata {
        id: Uuid::from_u128(1),
        contract: Text::NAME.into(),
        version: 1,
        created_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        correlation_id: Some(Uuid::from_u128(2)),
        causation_id: None,
        headers: BTreeMap::from([("schema".into(), "independent".into())]),
    }
}
fn envelope() -> Envelope {
    Envelope::from_parts(metadata(), json!({"text":"雪"}), Default::default()).unwrap()
}
fn app() -> Registry {
    let mut app = Registry::default();
    app.json::<Text>().unwrap();
    app.register::<Text, Text, _, _, _>(MessagePack(MessagePackLayout::Map), Ok, Ok)
        .unwrap();
    app.register::<ProtoText, Text, _, _, _>(
        Protobuf,
        |m| Ok(Text { text: m.text }),
        |m| Ok(ProtoText { text: m.text }),
    )
    .unwrap();
    app
}

#[test]
fn hand_authored_messagepack_vectors_have_real_array_map_integer_key_bytes() {
    // MessagePack format specification: array4, UTF-8 str3, positive fixint,
    // nil, array2, positive fixint, negative fixint. No local encoder oracle.
    let array = [0x94, 0xa3, 0xe9, 0x9b, 0xaa, 7, 0xc0, 0x92, 1, 0xfe];
    let expected = Collection {
        text: "雪".into(),
        number: 7,
        maybe: None,
        items: vec![1, -2],
    };
    let positional = MessagePack(MessagePackLayout::Positional);
    assert_eq!(
        <MessagePack as BodyCodec<Collection>>::decode(&positional, &array, Limits::default())
            .unwrap(),
        expected
    );
    assert_eq!(
        positional.encode(&expected, Limits::default()).unwrap(),
        array
    );
    let named = MessagePack(MessagePackLayout::Map);
    let map = b"\x81\xa4text\xa3\xe9\x9b\xaa";
    assert_eq!(
        named
            .encode(&Text { text: "雪".into() }, Limits::default())
            .unwrap(),
        map
    );
    assert_eq!(
        <MessagePack as BodyCodec<Text>>::decode(&named, map, Limits::default())
            .unwrap()
            .text,
        "雪"
    );
    let integers = BTreeMap::from([(0u32, "雪".to_owned())]);
    assert_eq!(
        named.encode(&integers, Limits::default()).unwrap(),
        b"\x81\x00\xa3\xe9\x9b\xaa"
    );
    assert_eq!(
        <MessagePack as BodyCodec<BTreeMap<u32, String>>>::decode(
            &named,
            b"\x81\x00\xa3\xe9\x9b\xaa",
            Limits::default()
        )
        .unwrap(),
        integers
    );
    // Wrong target and invalid UTF-8, nil message, truncation, trailing value,
    // extension and hostile declared array/str lengths are refused independently.
    for bad in [
        &b"\x01"[..],
        b"\xc0",
        b"\x81\xa4text\xa1\xff",
        &map[..map.len() - 1],
        b"\x81\xa4text\xa1a\x00",
    ] {
        assert!(<MessagePack as BodyCodec<Text>>::decode(&named, bad, Limits::default()).is_err());
    }
    for bad in [&b"\xdd\xff\xff\xff\xff"[..], b"\xdb\xff\xff\xff\xff"] {
        assert_eq!(
            <MessagePack as BodyCodec<Text>>::decode(&named, bad, Limits::default()),
            Err(Error::Limits)
        );
    }
    assert_eq!(
        <MessagePack as BodyCodec<Option<Text>>>::encode(&named, &None, Limits::default()),
        Err(Error::Projection)
    );
    assert!(
        <MessagePack as BodyCodec<Text>>::decode(&named, b"\xd4\x01\x00", Limits::default())
            .is_err()
    );
}

#[test]
fn messagepack_reserved_crypto_root_is_checked_before_any_target_model() {
    #[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
    struct Defaulted {
        optional: Option<String>,
        #[serde(default)]
        count: u32,
    }
    let codec = MessagePack(MessagePackLayout::Map);
    // Independent map/fixstr, map16/str8, map32/str16/str32 and bin vectors;
    // also inspect every duplicate root key, not only serde's retained value.
    for bytes in [
        &b"\x81\xa6format\xadraddle-aes-v2"[..],
        b"\xde\x00\x01\xd9\x06format\xd9\x0draddle-aes-v2",
        b"\xdf\x00\x00\x00\x01\xda\x00\x06format\xdb\x00\x00\x00\x0draddle-aes-v2",
        b"\x81\xc4\x06format\xc4\x0draddle-aes-v2",
        b"\x82\xa6format\xa5plain\xa6format\xadraddle-aes-v2",
        b"\x82\xa6format\xadraddle-aes-v2\xa6format\xa5plain",
        b"\x82\xa6nested\x81\xa6format\xa5plain\xa6format\xadraddle-aes-v2",
    ] {
        assert_eq!(
            <MessagePack as BodyCodec<Defaulted>>::decode(&codec, bytes, Limits::default()),
            Err(Error::UnsupportedProtection)
        );
        assert!(matches!(
            <MessagePack as BodyCodec<serde_json::Value>>::decode(&codec, bytes, Limits::default()),
            Err(Error::UnsupportedProtection)
        ));
    }
    for value in [
        json!({"nested":{"format":"raddle-aes-v2"}}),
        json!({"format":{"nested":"raddle-aes-v2"}}),
        json!({"format":"plain"}),
    ] {
        let bytes = codec.encode(&value, Limits::default()).unwrap();
        assert_eq!(
            <MessagePack as BodyCodec<serde_json::Value>>::decode(
                &codec,
                &bytes,
                Limits::default()
            )
            .unwrap(),
            value
        );
    }
    assert_eq!(
        codec.encode(&json!({"format":"raddle-aes-v2"}), Limits::default()),
        Err(Error::UnsupportedProtection)
    );
    assert_eq!(
        <MessagePack as BodyCodec<Vec<String>>>::decode(
            &codec,
            b"\x92\xa6format\xadraddle-aes-v2",
            Limits::default()
        )
        .unwrap(),
        ["format", "raddle-aes-v2"]
    );
    assert_eq!(
        <MessagePack as BodyCodec<BTreeMap<u32, String>>>::decode(
            &codec,
            b"\x81\x00\xadraddle-aes-v2",
            Limits::default()
        )
        .unwrap(),
        BTreeMap::from([(0, "raddle-aes-v2".into())])
    );
}

#[test]
fn protobuf_spec_vector_zero_byte_asymmetry_and_declared_length_bounds() {
    // Independently authored proto3: message Text { string text = 1; }
    let wire = [0x0a, 3, 0xe9, 0x9b, 0xaa];
    let message = ProtoText { text: "雪".into() };
    assert_eq!(Protobuf.encode(&message, Limits::default()).unwrap(), wire);
    assert_eq!(
        <Protobuf as BodyCodec<ProtoText>>::decode(&Protobuf, &wire, Limits::default()).unwrap(),
        message
    );
    assert!(
        Protobuf
            .encode(&ProtoText::default(), Limits::default())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        <Protobuf as BodyCodec<ProtoText>>::decode(&Protobuf, b"", Limits::default()),
        Err(Error::Malformed)
    );
    for bad in [
        &wire[..4],
        &b"\x0a\x01\xff"[..],
        b"\x0a\x01a\xff",
        b"\x00",
        b"\x0b",
        b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x00",
    ] {
        assert!(
            <Protobuf as BodyCodec<ProtoText>>::decode(&Protobuf, bad, Limits::default()).is_err()
        );
    }
    assert_eq!(
        <Protobuf as BodyCodec<ProtoText>>::decode(
            &Protobuf,
            b"\x0a\xff\xff\xff\xff\x0f",
            Limits::default()
        ),
        Err(Error::Limits)
    );
    // Protobuf unknown-field evolution follows the declared prost schema. It is
    // not automatic preservation of unknown fields in the canonical projection.
    assert_eq!(
        <Protobuf as BodyCodec<ProtoText>>::decode(
            &Protobuf,
            b"\x0a\x01a\x10\x01",
            Limits::default()
        )
        .unwrap()
        .text,
        "a"
    );
}

#[test]
fn selection_local_first_json_alternative_metadata_and_content_identity() {
    let mut registry = app();
    registry.set_default("binary/protobuf").unwrap();
    // Registering JSON before OR after binary selection never changes default.
    registry.json::<Collection>().unwrap();
    let application = Arc::new(registry);
    let sibling = EndpointCodecs::new(Arc::clone(&application), Limits::default()).unwrap();
    let mut local = EndpointCodecs::new(application, Limits::default()).unwrap();
    local
        .local_mut()
        .register::<Text, Text, _, _, _>(MessagePack(MessagePackLayout::Positional), Ok, Ok)
        .unwrap();
    local.local_mut().set_default("binary/messagepack").unwrap();
    let original = envelope();
    for codec in [&sibling, &local] {
        let bytes = codec.encode(&original).unwrap();
        assert!(bytes.starts_with(b"RDSB\x01"));
        let len = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let header: serde_json::Value = serde_json::from_slice(&bytes[9..9 + len]).unwrap();
        assert_eq!(header["content_type"], codec.default_media());
        assert_eq!(
            &bytes[9 + len..],
            if codec.default_media() == "binary/protobuf" {
                &b"\x0a\x03\xe9\x9b\xaa"[..]
            } else {
                &b"\x91\xa3\xe9\x9b\xaa"[..]
            }
        );
        let decoded = codec.decode(&bytes).unwrap();
        assert_eq!(decoded.id(), original.id());
        assert_eq!(decoded.created_at(), original.created_at());
        assert_eq!(decoded.correlation_id(), original.correlation_id());
        assert_eq!(decoded.causation_id(), None);
        assert_eq!(decoded.headers(), original.headers());
        assert_eq!(
            decoded.content_bytes().unwrap(),
            original.content_bytes().unwrap()
        );
        // JSON remains an incoming alternative even with a binary default.
        let json = codec.encode_body(&original, "application/json").unwrap();
        assert_eq!(
            codec
                .decode_body(original.metadata(), "application/json", &json)
                .unwrap()
                .content_bytes()
                .unwrap(),
            original.content_bytes().unwrap()
        );
    }
    assert_eq!(sibling.default_media(), "binary/protobuf");
    assert!(matches!(
        sibling.decode_body(original.metadata(), "text/unknown", b"{}"),
        Err(Error::Unsupported)
    ));
    let mut wrong = original.metadata();
    wrong.contract = "unknown".into();
    assert!(matches!(
        sibling.decode_body(wrong, "application/json", b"{}"),
        Err(Error::Unsupported)
    ));
    let mut missing = original.metadata();
    missing.version = 0;
    assert!(
        sibling
            .decode_body(missing, "application/json", b"{}")
            .is_err()
    );
    let mut bytes = local.encode(&original).unwrap();
    bytes.extend_from_slice(b"garbage");
    assert!(local.decode(&bytes).is_err());
    assert!(local.decode(b"RDSB\x01\xff\xff\xff\xff").is_err());
}

#[test]
fn native_framing_refuses_duplicate_missing_conflicting_or_unknown_selection_metadata() {
    let codecs = EndpointCodecs::new(Arc::new(app()), Limits::default()).unwrap();
    let metadata = serde_json::to_string(&metadata()).unwrap();
    let valid = format!(r#"{{"metadata":{metadata},"content_type":"application/json"}}"#);
    let duplicate = format!(
        r#"{{"metadata":{metadata},"content_type":"application/json","content_type":"binary/protobuf"}}"#
    );
    let missing = format!(r#"{{"metadata":{metadata}}}"#);
    let unknown = format!(r#"{{"metadata":{metadata},"content_type":"text/unknown"}}"#);
    let conflict =
        format!(r#"{{"metadata":{metadata},"content_type":"application/json","extra":"refused"}}"#);
    for header in [&duplicate, &missing, &unknown, &conflict] {
        let mut bytes = b"RDSB\x01".to_vec();
        bytes.extend_from_slice(&(header.len() as u32).to_be_bytes());
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(b"{\"text\":\"x\"}");
        assert!(codecs.decode(&bytes).is_err());
    }
    let mut bytes = b"RDSB\x01".to_vec();
    bytes.extend_from_slice(&(valid.len() as u32).to_be_bytes());
    bytes.extend_from_slice(valid.as_bytes());
    bytes.extend_from_slice(b"{\"text\":\"x\"}");
    assert_eq!(
        codecs
            .decode(&bytes)
            .unwrap()
            .decode::<Text>()
            .unwrap()
            .text,
        "x"
    );
    let changed = EndpointCodecs::new(Arc::new(app()), Limits::default())
        .unwrap()
        .with_canonicalizer(|original| {
            Envelope::with_identity(
                Uuid::from_u128(99),
                original.created_at(),
                &original.decode::<Text>().map_err(|_| Error::Projection)?,
            )
            .map_err(|_| Error::Projection)
        });
    assert!(matches!(changed.decode(&bytes), Err(Error::Projection)));
}

struct TextCodec;
impl BodyCodec<String> for TextCodec {
    fn content_type(&self) -> &'static str {
        "text/plain"
    }
    fn encode(&self, text: &String, limits: Limits) -> Result<Vec<u8>, Error> {
        if text.len() > limits.max_encoded_bytes {
            return Err(Error::Limits);
        }
        Ok(format!("text:{text}").into_bytes())
    }
    fn decode(&self, bytes: &[u8], _: Limits) -> Result<String, Error> {
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.strip_prefix("text:"))
            .map(str::to_owned)
            .ok_or(Error::Malformed)
    }
}
#[test]
fn different_wire_and_canonical_models_for_json_and_custom_text() {
    #[derive(Serialize, Deserialize)]
    struct Foreign {
        renamed: String,
    }
    for use_text in [false, true] {
        let mut registry = Registry::default();
        if use_text {
            registry
                .register::<String, Text, _, _, _>(
                    TextCodec,
                    |s| Ok(Text { text: s }),
                    |m| Ok(m.text),
                )
                .unwrap();
            registry.set_default("text/plain").unwrap();
        } else {
            registry
                .register::<Foreign, Text, _, _, _>(
                    Json,
                    |m| Ok(Text { text: m.renamed }),
                    |m| Ok(Foreign { renamed: m.text }),
                )
                .unwrap();
        }
        let codecs = EndpointCodecs::new(Arc::new(registry), Limits::default()).unwrap();
        let bytes = codecs
            .encode_body(&envelope(), codecs.default_media())
            .unwrap();
        assert_eq!(
            bytes,
            if use_text {
                "text:雪".as_bytes().to_vec()
            } else {
                "{\"renamed\":\"雪\"}".as_bytes().to_vec()
            }
        );
        assert_eq!(
            codecs
                .decode_body(metadata(), codecs.default_media(), &bytes)
                .unwrap()
                .decode::<Text>()
                .unwrap()
                .text,
            "雪"
        );
    }
}

#[test]
fn json_null_unicode_bom_repeated_writes_and_strict_failure_bounds() {
    assert_eq!(
        <Json as BodyCodec<Option<Text>>>::decode(&Json, b"null", Limits::default()).unwrap(),
        None
    );
    assert_eq!(
        Json.encode(&None::<Text>, Limits::default()).unwrap(),
        b"null"
    );
    assert_eq!(
        <Json as BodyCodec<Text>>::decode(
            &Json,
            "\u{feff}{\"text\":\"雪\"}".as_bytes(),
            Limits::default()
        )
        .unwrap()
        .text,
        "雪"
    );
    let large = Text {
        text: "雪".repeat(10_000),
    };
    for value in [&large, &Text { text: "a".into() }] {
        let bytes = Json.encode(value, Limits::default()).unwrap();
        assert_eq!(
            &<Json as BodyCodec<Text>>::decode(&Json, &bytes, Limits::default()).unwrap(),
            value
        );
    }
    for bytes in [
        &b"{\"text\":\"a\",\"text\":\"b\"}"[..],
        b"{\"text\":\"a\"}null",
        b"{\"text\":\"\xff\"}",
        b"null",
    ] {
        assert!(<Json as BodyCodec<Text>>::decode(&Json, bytes, Limits::default()).is_err());
    }
    let small = Limits {
        max_depth: 2,
        ..Limits::default()
    };
    assert!(<Json as BodyCodec<serde_json::Value>>::decode(&Json, b"[[[0]]]", small).is_err());
    let small = Limits {
        max_nodes: 2,
        ..Limits::default()
    };
    assert!(<Json as BodyCodec<serde_json::Value>>::decode(&Json, b"[1,2]", small).is_err());
    let small = Limits {
        max_declared_length: 2,
        ..Limits::default()
    };
    assert!(<Json as BodyCodec<serde_json::Value>>::decode(&Json, b"\"abc\"", small).is_err());
    let codecs = EndpointCodecs::new(
        Arc::new(app()),
        Limits {
            canonical: raddle::wire::EnvelopeLimits {
                max_bytes: 20,
                max_headers: 0,
            },
            ..Limits::default()
        },
    )
    .unwrap();
    assert!(
        codecs
            .decode_body(metadata(), "application/json", b"{\"text\":\"x\"}")
            .is_err()
    );
}

#[test]
fn explicit_projection_refusal_and_pre_custody_upcast_preserve_identity() {
    let mut registry = Registry::default();
    registry
        .register::<ProtoText, Text, _, _, _>(
            Protobuf,
            |m| {
                if m.text == "not-representable" {
                    Err(Error::Projection)
                } else {
                    Ok(Text { text: m.text })
                }
            },
            |m| Ok(ProtoText { text: m.text }),
        )
        .unwrap();
    let codecs = EndpointCodecs::new(Arc::new(registry), Limits::default()).unwrap();
    assert!(matches!(
        codecs.decode_body(
            metadata(),
            "binary/protobuf",
            &ProtoText {
                text: "not-representable".into()
            }
            .encode_to_vec()
        ),
        Err(Error::Projection)
    ));
    #[derive(Serialize, Deserialize)]
    struct V2 {
        text: String,
        added: bool,
    }
    impl WireMessage for V2 {
        const NAME: &'static str = Text::NAME;
        const VERSION: u32 = 2;
    }
    let mut upcasters = Codecs::default();
    upcasters
        .upcast(Text::NAME, 1, |mut v| {
            v["added"] = true.into();
            Ok(v)
        })
        .unwrap();
    let codecs = EndpointCodecs::new(Arc::new(app()), Limits::default())
        .unwrap()
        .with_canonicalizer(move |envelope| {
            let current = upcasters
                .decode::<V2>(&envelope)
                .map_err(|_| Error::Projection)?;
            envelope
                .rebody(&current, Limits::default().canonical)
                .map_err(|_| Error::Projection)
        });
    let original = envelope();
    let result = codecs.decode(&codecs.encode(&original).unwrap()).unwrap();
    assert_eq!(result.version(), 2);
    assert_eq!(result.id(), original.id());
    assert_eq!(result.headers(), original.headers());
    assert_eq!(result.correlation_id(), original.correlation_id());
    assert_eq!(result.causation_id(), None);
    assert!(result.decode::<V2>().unwrap().added);
}
