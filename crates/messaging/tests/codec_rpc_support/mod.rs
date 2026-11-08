#![allow(clippy::unwrap_used)]
#![allow(dead_code)] // Shared by independently compiled socket and lifecycle suites.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use raddle::Request;
use raddle::wire::WireMessage;
use raddle_messaging::{SelectedRpc, TrustedOrigin};
use raddle_serialization::{
    EndpointCodecs, Limits, MessagePack, MessagePackLayout, Protobuf, Registry,
};
use raddle_tcp::{Endpoint, Publisher};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Double {
    #[serde(default)]
    pub number: u32,
}
impl WireMessage for Double {
    const NAME: &'static str = "codec-rpc.double";
    const VERSION: u32 = 1;
}
impl Request for Double {
    type Reply = Answer;
    type Error = Refused;
}
#[derive(Deserialize, Serialize)]
pub struct Answer {
    #[serde(default)]
    pub number: u32,
}
impl WireMessage for Answer {
    const NAME: &'static str = "codec-rpc.answer";
    const VERSION: u32 = 1;
}
#[derive(Debug, thiserror::Error)]
#[error("synthetic request refused")]
pub struct Refused;

#[derive(Serialize, Deserialize)]
pub struct Frame {
    pub mode: String,
    pub response: Response,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum Response {
    Success { body: Vec<u8> },
    Failure,
}
impl WireMessage for Frame {
    const NAME: &'static str = "raddle.native-reply";
    const VERSION: u32 = 2;
}

pub fn reply_frame(
    request: &raddle::wire::Envelope,
    media: &str,
    body: Option<Vec<u8>>,
) -> raddle::wire::Envelope {
    raddle::wire::Envelope::encode(&Frame {
        mode: format!("selected-v2:{media}"),
        response: body.map_or(Response::Failure, |body| Response::Success { body }),
    })
    .unwrap()
    .correlated(
        request.headers()["raddle-request-token"].parse().unwrap(),
        request.id(),
    )
    .unwrap()
}

#[derive(prost::Message)]
pub struct Number {
    #[prost(uint32, tag = "1")]
    pub number: u32,
}

pub fn registry(media: &str, request: bool, response: bool) -> Registry {
    let mut registry = Registry::default();
    if request {
        registry.json::<Double>().unwrap();
    }
    if response {
        registry.json::<Answer>().unwrap();
    }
    match media {
        "application/json" => {}
        "binary/messagepack" => {
            if request {
                registry
                    .register::<Double, Double, _, _, _>(
                        MessagePack(MessagePackLayout::Map),
                        Ok,
                        Ok,
                    )
                    .unwrap();
            }
            if response {
                registry
                    .register::<Answer, Answer, _, _, _>(
                        MessagePack(MessagePackLayout::Map),
                        Ok,
                        Ok,
                    )
                    .unwrap();
            }
        }
        "binary/protobuf" => {
            if request {
                registry
                    .register::<Number, Double, _, _, _>(
                        Protobuf,
                        |n| Ok(Double { number: n.number }),
                        |n| Ok(Number { number: n.number }),
                    )
                    .unwrap();
            }
            if response {
                registry
                    .register::<Number, Answer, _, _, _>(
                        Protobuf,
                        |n| Ok(Answer { number: n.number }),
                        |n| Ok(Number { number: n.number }),
                    )
                    .unwrap();
            }
        }
        _ => panic!("unexpected fixture media"),
    }
    registry.set_default(media).unwrap();
    registry
}

pub fn codecs(media: &str) -> Arc<EndpointCodecs> {
    Arc::new(EndpointCodecs::new(Arc::new(registry(media, true, true)), Limits::default()).unwrap())
}

pub fn selected(codecs: Arc<EndpointCodecs>) -> SelectedRpc {
    let mut selected = SelectedRpc::new(codecs);
    selected.register::<Double>().unwrap();
    selected
}

pub fn origin(source: &str) -> TrustedOrigin {
    TrustedOrigin::authenticated("configured-tenant", source, None).unwrap()
}

pub fn publisher(alias: &str, address: SocketAddr) -> Publisher {
    Publisher::try_new(
        BTreeMap::from([(
            alias.into(),
            Endpoint::try_new(address.ip().to_string(), address.port()).unwrap(),
        )]),
        raddle_tcp::Limits::default(),
    )
    .unwrap()
}

pub fn body_frame(metadata: raddle::wire::EnvelopeMetadata, media: &str, body: &[u8]) -> Vec<u8> {
    let header =
        serde_json::to_vec(&serde_json::json!({"metadata": metadata, "content_type": media}))
            .unwrap();
    let mut frame = b"RDSB\x01".to_vec();
    frame.extend_from_slice(&(header.len() as u32).to_be_bytes());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(body);
    frame
}

pub fn assert_representation(bytes: &[u8], media: &str, number: u32) {
    assert!(bytes.starts_with(b"RDSB\x01"));
    let length = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let header: serde_json::Value = serde_json::from_slice(&bytes[9..9 + length]).unwrap();
    assert_eq!(header["content_type"], media);
    let body = &bytes[9 + length..];
    match media {
        "application/json" => assert_eq!(
            serde_json::from_slice::<Answer>(body).unwrap().number,
            number
        ),
        "binary/messagepack" => {
            assert_eq!(body, rmp_serde::to_vec_named(&Answer { number }).unwrap());
            assert!(serde_json::from_slice::<serde_json::Value>(body).is_err());
            assert_eq!(
                rmp_serde::from_slice::<Answer>(body).unwrap().number,
                number
            );
        }
        "binary/protobuf" => {
            use prost::Message;
            assert_eq!(body, Number { number }.encode_to_vec());
            assert!(serde_json::from_slice::<serde_json::Value>(body).is_err());
            assert_eq!(Number::decode(body).unwrap().number, number);
        }
        _ => panic!("unexpected media"),
    }
}
