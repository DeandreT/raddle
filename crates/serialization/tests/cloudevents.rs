//! Independent structured and binding-specific binary CloudEvents vectors.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_serialization::cloudevents::{Aliases, Binding, EventMetadata, Mapper, NativeIdentity};
use raddle_serialization::{
    EndpointCodecs, Error, Limits, MessagePack, MessagePackLayout, Registry,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Text {
    text: String,
}
impl WireMessage for Text {
    const NAME: &'static str = "ce.text";
    const VERSION: u32 = 1;
}

fn identity(external: &EventMetadata) -> Result<NativeIdentity, Error> {
    // An intentionally finite application-owned identity map. External time is
    // absent in the independent fixture, never substituted with receipt time.
    if external.id != "external/not-a-uuid" || external.source != "/synthetic/source" {
        return Err(Error::Projection);
    }
    Ok(NativeIdentity {
        id: Uuid::from_u128(0x123),
        created_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
    })
}
fn mapper(binary: bool) -> Mapper {
    mapper_with_limits(binary, Limits::default(), Limits::default(), false)
}
fn mapper_with_limits(
    binary: bool,
    codec_limits: Limits,
    mapper_limits: Limits,
    expand: bool,
) -> Mapper {
    let mut registry = Registry::default();
    registry.json::<Text>().unwrap();
    registry
        .register::<Text, Text, _, _, _>(MessagePack(MessagePackLayout::Map), Ok, Ok)
        .unwrap();
    if binary {
        registry.set_default("binary/messagepack").unwrap();
    }
    let mut codecs = EndpointCodecs::new(Arc::new(registry), codec_limits).unwrap();
    if expand {
        let mut upcasters = Codecs::default();
        upcasters
            .upcast(Text::NAME, 1, |mut value| {
                value["padding"] = if value["text"] == "expand" {
                    "x".repeat(1024)
                } else {
                    String::new()
                }
                .into();
                Ok(value)
            })
            .unwrap();
        codecs = codecs.with_canonicalizer(move |envelope| {
            let current = upcasters
                .decode::<ExpandedText>(&envelope)
                .map_err(|_| Error::Projection)?;
            envelope
                .rebody(&current, Limits::default().canonical)
                .map_err(|_| Error::Projection)
        });
    }
    let mut aliases = Aliases::default();
    aliases
        .register::<Text>("urn:foreign.event:Text/v7")
        .unwrap();
    Mapper::new(Arc::new(codecs), aliases, mapper_limits, identity).unwrap()
}

#[derive(Serialize, Deserialize)]
struct ExpandedText {
    text: String,
    padding: String,
}
impl WireMessage for ExpandedText {
    const NAME: &'static str = Text::NAME;
    const VERSION: u32 = 2;
}

fn small_canonical_limit() -> Limits {
    Limits {
        canonical: raddle::wire::EnvelopeLimits {
            max_bytes: 512,
            ..Limits::default().canonical
        },
        ..Limits::default()
    }
}

fn text_envelope(text: String) -> Envelope {
    let identity = identity(&external()).unwrap();
    Envelope::with_identity(identity.id, identity.created_at, &Text { text }).unwrap()
}

#[test]
fn mapper_and_codec_canonical_limits_are_independent_inbound_and_outbound() {
    for binary in [false, true] {
        let producer = mapper(binary);
        for (codec_limits, mapper_limits) in [
            (Limits::default(), small_canonical_limit()),
            (small_canonical_limit(), Limits::default()),
        ] {
            let limited = mapper_with_limits(binary, codec_limits, mapper_limits, false);
            for (text, oversized) in [
                ("healthy-before".into(), false),
                ("x".repeat(1024), true),
                ("healthy-after".into(), false),
            ] {
                let envelope = text_envelope(text);
                assert_eq!(envelope.to_vec().unwrap().len() > 512, oversized);
                let bytes = producer.encode_structured(&envelope, &external()).unwrap();
                let inbound = limited.decode_structured("application/cloudevents+json", &bytes);
                let outbound = limited.encode_structured(&envelope, &external());
                if oversized {
                    assert!(matches!(inbound, Err(Error::Limits)));
                    assert_eq!(outbound, Err(Error::Limits));
                } else {
                    assert_eq!(
                        inbound.unwrap().envelope.content_bytes().unwrap(),
                        envelope.content_bytes().unwrap()
                    );
                    assert!(outbound.is_ok());
                }
                for binding in [Binding::Http, Binding::Kafka] {
                    let event = producer
                        .encode_binary(binding, &envelope, &external())
                        .unwrap();
                    let inbound = limited.decode_binary(binding, &event.headers, &event.body);
                    let outbound = limited.encode_binary(binding, &envelope, &external());
                    if oversized {
                        assert!(matches!(inbound, Err(Error::Limits)));
                        assert!(matches!(outbound, Err(Error::Limits)));
                    } else {
                        assert_eq!(
                            inbound.unwrap().envelope.content_bytes().unwrap(),
                            envelope.content_bytes().unwrap()
                        );
                        assert!(outbound.is_ok());
                    }
                }
            }
        }
    }
}

#[test]
fn mapper_checks_final_upcast_expansion_for_structured_http_and_kafka() {
    for binary in [false, true] {
        let producer = mapper(binary);
        let receiving =
            mapper_with_limits(binary, Limits::default(), small_canonical_limit(), true);
        let unconstrained = mapper_with_limits(binary, Limits::default(), Limits::default(), true);
        for (text, oversized) in [
            ("healthy-before", false),
            ("expand", true),
            ("healthy-after", false),
        ] {
            let envelope = text_envelope(text.into());
            assert!(envelope.to_vec().unwrap().len() < 512);
            let bytes = producer.encode_structured(&envelope, &external()).unwrap();
            assert!(bytes.len() < 512);
            let expanded = unconstrained
                .decode_structured("application/cloudevents+json", &bytes)
                .unwrap()
                .envelope;
            assert_eq!(expanded.version(), 2);
            assert_eq!(expanded.to_vec().unwrap().len() > 512, oversized);
            let result = receiving.decode_structured("application/cloudevents+json", &bytes);
            if oversized {
                assert!(matches!(result, Err(Error::Limits)));
            } else {
                assert_eq!(
                    result.unwrap().envelope.content_bytes().unwrap(),
                    expanded.content_bytes().unwrap()
                );
            }
            for binding in [Binding::Http, Binding::Kafka] {
                let event = producer
                    .encode_binary(binding, &envelope, &external())
                    .unwrap();
                assert!(
                    event.body.len()
                        + event
                            .headers
                            .iter()
                            .map(|(k, v)| k.len() + v.len())
                            .sum::<usize>()
                        < 512
                );
                let result = receiving.decode_binary(binding, &event.headers, &event.body);
                if oversized {
                    assert!(matches!(result, Err(Error::Limits)));
                } else {
                    assert_eq!(
                        result.unwrap().envelope.content_bytes().unwrap(),
                        expanded.content_bytes().unwrap()
                    );
                }
            }
        }
    }
}
fn external() -> EventMetadata {
    EventMetadata {
        id: "external/not-a-uuid".into(),
        source: "/synthetic/source".into(),
        event_type: "urn:foreign.event:Text/v7".into(),
        time: None,
        subject: Some("雪".into()),
        dataschema: None,
        extensions: BTreeMap::from([("traceparent".into(), json!("untrusted-trace"))]),
    }
}
fn native() -> Envelope {
    let identity = identity(&external()).unwrap();
    Envelope::with_identity(
        identity.id,
        identity.created_at,
        &Text { text: "雪".into() },
    )
    .unwrap()
}

#[test]
fn independent_structured_json_and_base64_vectors_retain_external_identity_and_extensions() {
    let mapper = mapper(false);
    let fixture=br#"{"specversion":"1.0","id":"external/not-a-uuid","source":"/synthetic/source","type":"urn:foreign.event:Text/v7","subject":"\u96ea","traceparent":"untrusted-trace","datacontenttype":"application/json","data":{"text":"\u96ea"}}"#;
    let first = mapper
        .decode_structured("application/cloudevents+json", fixture)
        .unwrap();
    let second = mapper
        .decode_structured("application/cloudevents+json", fixture)
        .unwrap();
    assert_eq!(first.external, external());
    assert_eq!(first.envelope.id(), Uuid::from_u128(0x123));
    assert_eq!(first.envelope.id(), second.envelope.id());
    assert_eq!(first.envelope.created_at(), second.envelope.created_at());
    assert_eq!(first.envelope.contract(), Text::NAME);
    assert_eq!(first.envelope.decode::<Text>().unwrap().text, "雪");
    assert!(first.envelope.headers().is_empty());
    assert_eq!(
        first.envelope.content_bytes().unwrap(),
        native().content_bytes().unwrap()
    );
    // Same external event in genuine MessagePack: map1, str4 "text", str3 雪.
    // Base64 below is a fixed independently authored byte vector.
    let fixture=br#"{"specversion":"1.0","id":"external/not-a-uuid","source":"/synthetic/source","type":"urn:foreign.event:Text/v7","subject":"\u96ea","traceparent":"untrusted-trace","datacontenttype":"binary/messagepack","data_base64":"gaR0ZXh0o+mbqg=="}"#;
    let binary = mapper
        .decode_structured("application/cloudevents+json", fixture)
        .unwrap();
    assert_eq!(
        binary.envelope.content_bytes().unwrap(),
        first.envelope.content_bytes().unwrap()
    );
    assert_eq!(binary.external, first.external);
}

#[test]
fn outgoing_structured_data_is_not_base64_json_and_binary_data_is_actual_body() {
    let native = native();
    for binary in [false, true] {
        let mapper = mapper(binary);
        let bytes = mapper.encode_structured(&native, &external()).unwrap();
        let object: Value = serde_json::from_slice(&bytes).unwrap();
        if binary {
            assert_eq!(object["data_base64"], "gaR0ZXh0o+mbqg==");
            assert!(object.get("data").is_none());
        } else {
            assert_eq!(object["data"], json!({"text":"雪"}));
            assert!(object.get("data_base64").is_none());
        }
        assert_eq!(
            mapper
                .decode_structured("application/cloudevents+json", &bytes)
                .unwrap()
                .envelope
                .content_bytes()
                .unwrap(),
            native.content_bytes().unwrap()
        );
        for binding in [Binding::Http, Binding::Kafka] {
            let event = mapper.encode_binary(binding, &native, &external()).unwrap();
            let prefix = match binding {
                Binding::Http => "ce-",
                Binding::Kafka => "ce_",
                _ => panic!("unexpected binding in fixed fixture"),
            };
            assert!(
                event
                    .headers
                    .iter()
                    .any(|(key, value)| key == &format!("{prefix}type")
                        && value == b"urn:foreign.event:Text/v7")
            );
            assert_eq!(
                event.body,
                if binary {
                    &b"\x81\xa4text\xa3\xe9\x9b\xaa"[..]
                } else {
                    "{\"text\":\"雪\"}".as_bytes()
                }
            );
            let decoded = mapper
                .decode_binary(binding, &event.headers, &event.body)
                .unwrap();
            assert_eq!(
                decoded.envelope.content_bytes().unwrap(),
                native.content_bytes().unwrap()
            );
            assert_eq!(decoded.external, external());
        }
    }
}

#[test]
fn independent_kafka_ce_versus_http_ce_headers_duplicates_utf8_and_reserved_metadata() {
    let mapper = mapper(false);
    let headers = vec![
        ("ce_specversion".into(), b"1.0".to_vec()),
        ("ce_id".into(), b"external/not-a-uuid".to_vec()),
        ("ce_source".into(), b"/synthetic/source".to_vec()),
        ("ce_type".into(), b"urn:foreign.event:Text/v7".to_vec()),
        ("content-type".into(), b"application/json".to_vec()),
    ];
    let body = b"{\"text\":\"hello\"}";
    assert_eq!(
        mapper
            .decode_binary(Binding::Kafka, &headers, body)
            .unwrap()
            .envelope
            .decode::<Text>()
            .unwrap()
            .text,
        "hello"
    );
    assert!(mapper.decode_binary(Binding::Http, &headers, body).is_err());
    let mut duplicate = headers.clone();
    duplicate.push(("ce_type".into(), b"unknown".to_vec()));
    assert!(
        mapper
            .decode_binary(Binding::Kafka, &duplicate, body)
            .is_err()
    );
    let mut conflict = headers.clone();
    conflict.push(("ce_datacontenttype".into(), b"text/plain".to_vec()));
    assert!(
        mapper
            .decode_binary(Binding::Kafka, &conflict, body)
            .is_err()
    );
    for key in [
        "ce_tenant",
        "ce_raddletenant",
        "raddle-tenant",
        "authorization",
        "ce_trustedsource",
    ] {
        let mut spoof = headers.clone();
        spoof.push((key.into(), b"untrusted".to_vec()));
        assert!(mapper.decode_binary(Binding::Kafka, &spoof, body).is_err());
    }
    let mut invalid = headers.clone();
    invalid[1].1 = vec![0xff];
    assert!(
        mapper
            .decode_binary(Binding::Kafka, &invalid, body)
            .is_err()
    );
    let mut missing = headers.clone();
    missing.remove(3);
    assert!(
        mapper
            .decode_binary(Binding::Kafka, &missing, body)
            .is_err()
    );
    let mut http: Vec<_> = headers
        .iter()
        .map(|(key, value)| (key.replace("ce_", "CE-"), value.clone()))
        .collect();
    assert!(mapper.decode_binary(Binding::Http, &http, body).is_ok());
    http.push(("ce-type".into(), b"unknown".to_vec()));
    assert!(mapper.decode_binary(Binding::Http, &http, body).is_err());
}

#[test]
fn http_percent_quoted_and_utf8_attribute_vectors_are_not_applied_to_kafka() {
    let mapper = mapper(false);
    let mut headers = vec![
        ("ce-specversion".into(), b"1.0".to_vec()),
        ("ce-id".into(), b"external/not-a-uuid".to_vec()),
        ("ce-source".into(), b"/synthetic/source".to_vec()),
        ("ce-type".into(), b"urn:foreign.event:Text/v7".to_vec()),
        ("content-type".into(), b"application/json".to_vec()),
        (
            "ce-subject".into(),
            b"Euro%20%E2%82%AC%20%F0%9F%98%80".to_vec(),
        ),
        ("ce-literal".into(), b"%2520".to_vec()),
        ("ce-quoted".into(), br#""a \"quoted\" value""#.to_vec()),
    ];
    let body = b"{\"text\":\"x\"}";
    let decoded = mapper.decode_binary(Binding::Http, &headers, body).unwrap();
    assert_eq!(decoded.external.subject.as_deref(), Some("Euro € 😀"));
    assert_eq!(decoded.external.extensions["literal"], "%20");
    assert_eq!(decoded.external.extensions["quoted"], "a \"quoted\" value");
    for invalid in [
        &b"%C0%A0"[..],
        b"%ED%A0%80",
        b"%FF",
        b"bad%",
        b"%XX",
        b"%0A",
        b"raw\r\n",
        b"\"unfinished",
    ] {
        headers[5].1 = invalid.to_vec();
        assert!(mapper.decode_binary(Binding::Http, &headers, body).is_err());
    }
    let mut attributes = external();
    attributes.subject = Some("雪".repeat(682));
    let http = mapper
        .encode_binary(Binding::Http, &native(), &attributes)
        .unwrap();
    let subject = http
        .headers
        .iter()
        .find(|(name, _)| name == "ce-subject")
        .unwrap();
    assert_eq!(subject.1.len(), 6138);
    assert_eq!(
        mapper
            .decode_binary(Binding::Http, &http.headers, &http.body)
            .unwrap()
            .external,
        attributes
    );
    let kafka = mapper
        .encode_binary(Binding::Kafka, &native(), &attributes)
        .unwrap();
    assert_eq!(
        kafka
            .headers
            .iter()
            .find(|(name, _)| name == "ce_subject")
            .unwrap()
            .1,
        "雪".repeat(682).as_bytes()
    );
    attributes.subject = Some("%20".into());
    let kafka = mapper
        .encode_binary(Binding::Kafka, &native(), &attributes)
        .unwrap();
    assert_eq!(
        mapper
            .decode_binary(Binding::Kafka, &kafka.headers, &kafka.body)
            .unwrap()
            .external
            .subject
            .as_deref(),
        Some("%20")
    );
    attributes.subject = Some("injected\nvalue".into());
    assert!(mapper.encode_structured(&native(), &attributes).is_err());
}

#[test]
fn malformed_conflicting_missing_unknown_and_oversized_structured_events_are_refused() {
    let mapper = mapper(false);
    let good = json!({"specversion":"1.0","id":"external/not-a-uuid","source":"/synthetic/source","type":"urn:foreign.event:Text/v7","data":{"text":"x"}});
    let mut cases = Vec::new();
    for key in ["specversion", "id", "source", "type"] {
        let mut bad = good.clone();
        bad.as_object_mut().unwrap().remove(key);
        cases.push(bad);
    }
    for (key, value) in [
        ("specversion", json!("0.3")),
        ("type", json!("unknown")),
        ("source", json!("bad%XX")),
        ("tenant", json!("spoof")),
        ("datacontenttype", json!("text/plain")),
        ("data_base64", json!("eA==")),
        ("time", json!("bad")),
    ] {
        let mut bad = good.clone();
        bad[key] = value;
        cases.push(bad);
    }
    for base64 in ["!", "eA", "eA===", "eB=="] {
        let mut bad = good.clone();
        bad.as_object_mut().unwrap().remove("data");
        bad["data_base64"] = base64.into();
        cases.push(bad);
    }
    for case in cases {
        assert!(
            mapper
                .decode_structured(
                    "application/cloudevents+json",
                    &serde_json::to_vec(&case).unwrap()
                )
                .is_err(),
            "{case}"
        );
    }
    for bytes in [
        &b"{\"specversion\":\"1.0\",\"specversion\":\"1.0\"}"[..],
        b"{\"type\":\"\xff\"}",
        b"{}trailing",
    ] {
        assert!(
            mapper
                .decode_structured("application/cloudevents+json", bytes)
                .is_err()
        );
    }
    assert!(
        mapper
            .decode_structured("application/json", &serde_json::to_vec(&good).unwrap())
            .is_err()
    );
    let mut huge = good.clone();
    huge["data"]["text"] = "x".repeat(1_048_576).into();
    assert!(
        mapper
            .decode_structured(
                "application/cloudevents+json",
                &serde_json::to_vec(&huge).unwrap()
            )
            .is_err()
    );
    let mut spoof = external();
    spoof.extensions.insert("tenant".into(), json!("untrusted"));
    assert!(mapper.encode_structured(&native(), &spoof).is_err());
    let correlated = native()
        .correlated(Uuid::from_u128(2), Uuid::from_u128(3))
        .unwrap();
    assert_eq!(
        mapper.encode_structured(&correlated, &external()),
        Err(Error::Projection)
    );
    let mut both = good.clone();
    both["data"] = Value::Null;
    both["data_base64"] = "bnVsbA==".into();
    assert!(
        mapper
            .decode_structured(
                "application/cloudevents+json",
                &serde_json::to_vec(&both).unwrap()
            )
            .is_err()
    );
    let mut typed = external();
    typed.extensions.insert("attempt".into(), json!(7));
    typed.extensions.insert("active".into(), json!(true));
    let wire = mapper.encode_structured(&native(), &typed).unwrap();
    assert_eq!(
        mapper
            .decode_structured("application/cloudevents+json", &wire)
            .unwrap()
            .external,
        typed
    );
    typed.extensions.insert("attempt".into(), json!(i64::MAX));
    assert!(mapper.encode_structured(&native(), &typed).is_err());
    let too_many = vec![("ignored".to_owned(), b"value".to_vec()); 33];
    assert!(
        mapper
            .decode_binary(Binding::Kafka, &too_many, b"null")
            .is_err()
    );
    let mut at_limit = external();
    at_limit.subject = None;
    at_limit.extensions = (0..27).map(|n| (format!("x{n}"), json!("1"))).collect();
    let event = mapper
        .encode_binary(Binding::Kafka, &native(), &at_limit)
        .unwrap();
    assert_eq!(event.headers.len(), 32);
    assert_eq!(
        mapper
            .decode_binary(Binding::Kafka, &event.headers, &event.body)
            .unwrap()
            .external,
        at_limit
    );
    at_limit.extensions.insert("extra".into(), json!("1"));
    assert!(
        mapper
            .encode_binary(Binding::Kafka, &native(), &at_limit)
            .is_err()
    );
}
