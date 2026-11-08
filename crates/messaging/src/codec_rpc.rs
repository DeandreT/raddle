//! Explicit selected application bodies inside the native JSON RPC protocol.

use std::collections::BTreeMap;
use std::sync::Arc;

use raddle::Request;
use raddle::wire::{Envelope, WireMessage};
use raddle_serialization::EndpointCodecs;
use serde::{Deserialize, Serialize};

use crate::BusError;

/// Frozen typed pairs for selected native RPC v2. Configure the request publisher
/// and receiver with the same selected codecs, and keep the reply transport on
/// JSON outer envelopes. A generic `Transport` cannot verify those settings.
/// This plaintext-only profile does not compose with encrypted transports.
pub struct SelectedRpc {
    codecs: Arc<EndpointCodecs>,
    pairs: BTreeMap<(String, u32), ReplyContract>,
}

#[derive(Clone, Copy)]
pub(crate) struct ReplyContract {
    pub(crate) name: &'static str,
    pub(crate) version: u32,
    pub(crate) validate: fn(&Envelope) -> bool,
}

impl SelectedRpc {
    /// Starts an explicit selected profile. Register every accepted request pair
    /// before attaching it to an invoker or responder; no tasks are started.
    pub fn new(codecs: Arc<EndpointCodecs>) -> Self {
        Self {
            codecs,
            pairs: BTreeMap::new(),
        }
    }

    /// Validates real request and reply registrations under the selected media.
    /// No default model is fabricated or application converter invoked at startup.
    pub fn register<M>(&mut self) -> Result<(), BusError>
    where
        M: Request + WireMessage,
        M::Reply: WireMessage,
    {
        let media = self.codecs.default_media();
        if !self.codecs.supports(media, M::NAME, M::VERSION)
            || !self
                .codecs
                .supports(media, M::Reply::NAME, M::Reply::VERSION)
            || self.pairs.len() >= 1024
            || self.pairs.contains_key(&(M::NAME.into(), M::VERSION))
        {
            return Err(BusError::Invalid(
                "missing/duplicate selected RPC codec pair",
            ));
        }
        self.pairs.insert(
            (M::NAME.into(), M::VERSION),
            ReplyContract {
                name: M::Reply::NAME,
                version: M::Reply::VERSION,
                validate: |envelope| envelope.decode::<M::Reply>().is_ok(),
            },
        );
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), BusError> {
        if self.pairs.is_empty() {
            return Err(BusError::Invalid("empty selected RPC configuration"));
        }
        Ok(())
    }

    pub(crate) fn pair(&self, request: &Envelope) -> Result<ReplyContract, BusError> {
        self.pairs
            .get(&(request.contract().into(), request.version()))
            .copied()
            .ok_or(BusError::Invalid("unconfigured selected RPC pair"))
    }

    pub(crate) fn has_pair<M>(&self) -> bool
    where
        M: Request + WireMessage,
        M::Reply: WireMessage,
    {
        self.pairs
            .get(&(M::NAME.into(), M::VERSION))
            .is_some_and(|reply| reply.name == M::Reply::NAME && reply.version == M::Reply::VERSION)
    }

    pub(crate) fn declaration(&self) -> String {
        format!("selected-v2:{}", self.codecs.default_media())
    }

    pub(crate) fn encode(&self, envelope: &Envelope) -> Result<Vec<u8>, BusError> {
        self.codecs
            .encode(envelope)
            .map_err(|_| BusError::Invalid("selected RPC encoding refused"))
    }

    pub(crate) fn decode(&self, bytes: &[u8]) -> Result<Envelope, BusError> {
        // Require the configured media, not merely any registered alternative.
        // Prefilter the explicit declaration before invoking any typed converter.
        // EndpointCodecs subsequently validates all framing, duplicate members,
        // body/protection and post-upcast canonical bounds. These bytes have
        // already passed the outer native envelope's byte bound.
        if !bytes.starts_with(b"RDSB\x01") {
            return Err(BusError::Invalid("selected RPC frame"));
        }
        let length = bytes
            .get(5..9)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(u32::from_be_bytes)
            .ok_or(BusError::Invalid("selected RPC frame"))? as usize;
        let header = bytes
            .get(9..9usize.saturating_add(length))
            .ok_or(BusError::Invalid("selected RPC frame"))?;
        let header: Media =
            serde_json::from_slice(header).map_err(|_| BusError::Invalid("selected RPC frame"))?;
        if header.content_type != self.codecs.default_media() {
            return Err(BusError::Invalid("selected RPC media mismatch"));
        }
        self.codecs
            .decode(bytes)
            .map_err(|_| BusError::Invalid("selected RPC decoding refused"))
    }
}

#[derive(Deserialize)]
struct Media {
    content_type: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SelectedReplyFrame {
    pub(crate) mode: String,
    pub(crate) response: SelectedResponse,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum SelectedResponse {
    Success { body: Vec<u8> },
    // An empty struct variant is intentional: serde's internally tagged unit
    // variants can discard extra fields despite deny_unknown_fields.
    Failure {},
}
impl WireMessage for SelectedReplyFrame {
    const NAME: &'static str = "raddle.native-reply";
    const VERSION: u32 = 2;
}
