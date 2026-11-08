//! CloudEvents 1.0 context mapping, not a broker implementation or Wolverine's
//! private headers. Event source and extensions never become trusted origin.

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Utc};
use raddle::wire::{Envelope, EnvelopeMetadata, WireMessage};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::http_attributes;
use crate::{EndpointCodecs, Error, Limits, codecs};

const ATTRIBUTE_BYTES: usize = 2048;
const HTTP_WIRE_BYTES: usize = ATTRIBUTE_BYTES * 3 + 2;

const STANDARD_NAMES: &[&str] = &[
    "specversion",
    "id",
    "source",
    "type",
    "time",
    "subject",
    "dataschema",
    "datacontenttype",
    "data",
    "data_base64",
];

/// Supported context-attribute prefixes. Kafka uses `ce_`, never HTTP's `ce-`.
/// These generic mappings do not configure a Kafka/HTTP client or acknowledgement.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Binding {
    /// HTTP protocol binding, case-insensitive header names.
    Http,
    /// Kafka protocol binding, case-sensitive UTF-8 header keys/values.
    Kafka,
}
impl Binding {
    fn prefix(self) -> &'static str {
        match self {
            Self::Http => "ce-",
            Self::Kafka => "ce_",
        }
    }
}

/// Retained untrusted context. Persist/forward it separately if the application
/// needs external identity or extensions after custody; it is not authorization.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventMetadata {
    /// Arbitrary nonempty CloudEvents identifier, not necessarily a UUID.
    pub id: String,
    /// CloudEvents URI-reference source, not `TrustedOrigin::source`.
    pub source: String,
    /// External type resolved only by an explicit application alias.
    pub event_type: String,
    /// Optional external occurrence time; absence never implies "now".
    pub time: Option<DateTime<Utc>>,
    /// Optional event subject.
    pub subject: Option<String>,
    /// Optional absolute schema URI.
    pub dataschema: Option<String>,
    /// Non-reserved extension attributes (JSON scalar strings/bools/integers).
    pub extensions: BTreeMap<String, Value>,
}

/// Application-chosen stable native identity/time for an external event. The
/// projection MUST be deterministic over the same source/id on retries. There
/// is deliberately no random UUID or receipt-time default.
#[derive(Clone, Copy, Debug)]
pub struct NativeIdentity {
    /// Non-nil stable native message ID.
    pub id: Uuid,
    /// Stable canonical creation time, including when external time is absent.
    pub created_at: DateTime<Utc>,
}

/// Validated canonical body and separately retained external context.
#[derive(Debug)]
pub struct MappedEvent {
    /// Canonical native message, ready for independent trusted-origin admission.
    pub envelope: Envelope,
    /// Untrusted external event identity and context.
    pub external: EventMetadata,
}

/// Explicit external type aliases. Arbitrary URNs/URLs/foreign type strings map
/// to validated native contracts, never to runtime reflection or type activation.
#[derive(Default)]
pub struct Aliases(BTreeMap<String, (String, u32)>);
impl Aliases {
    /// Adds one known native contract/version. Duplicate aliases are refused.
    pub fn register<M: WireMessage>(&mut self, event_type: &str) -> Result<(), Error> {
        if event_type.is_empty()
            || event_type.len() > 2048
            || event_type.chars().any(char::is_control)
            || M::VERSION == 0
            || raddle::SubscriptionName::try_new(M::NAME).is_err()
            || self.0.contains_key(event_type)
        {
            return Err(Error::Configuration);
        }
        self.0
            .insert(event_type.to_owned(), (M::NAME.to_owned(), M::VERSION));
        Ok(())
    }
}

type IdentityProjection = dyn Fn(&EventMetadata) -> Result<NativeIdentity, Error> + Send + Sync;

/// Application-owned structured/binary mapping with explicit type and identity
/// projection. Codec and projection failures occur before dispatch or custody.
pub struct Mapper {
    codecs: Arc<EndpointCodecs>,
    aliases: Aliases,
    identity: Box<IdentityProjection>,
    limits: Limits,
}
impl Mapper {
    /// Configures trusted deterministic identity projection. Do not use receipt
    /// time or random IDs; persist an identity mapping if hashing is unsuitable.
    pub fn new<F>(
        codecs: Arc<EndpointCodecs>,
        aliases: Aliases,
        limits: Limits,
        identity: F,
    ) -> Result<Self, Error>
    where
        F: Fn(&EventMetadata) -> Result<NativeIdentity, Error> + Send + Sync + 'static,
    {
        limits.validate()?;
        Ok(Self {
            codecs,
            aliases,
            identity: Box::new(identity),
            limits,
        })
    }

    /// Decodes structured JSON `data` or strict `data_base64`, never both.
    /// The binding-level media type must be `application/cloudevents+json`.
    pub fn decode_structured(
        &self,
        content_type: &str,
        bytes: &[u8],
    ) -> Result<MappedEvent, Error> {
        if content_type != "application/cloudevents+json" {
            return Err(Error::Unsupported);
        }
        let value = codecs::json_value(bytes, self.limits)?;
        let mut object = value.as_object().cloned().ok_or(Error::Malformed)?;
        if object.contains_key("data") && object.contains_key("data_base64") {
            return Err(Error::Malformed);
        }
        let data = object.remove("data");
        let encoded = object.remove("data_base64");
        let (external, media) = self.context(object)?;
        let body = match (data, encoded) {
            (Some(value), None) => {
                if media != "application/json" {
                    return Err(Error::Unsupported);
                }
                serde_json::to_vec(&value).map_err(|_| Error::Malformed)?
            }
            (None, Some(Value::String(value))) => {
                STANDARD.decode(value).map_err(|_| Error::Malformed)?
            }
            (None, None) if media == "application/json" => b"null".to_vec(),
            _ => return Err(Error::Malformed),
        };
        self.finish(external, &media, &body)
    }

    /// Encodes selected real body bytes. JSON is represented by `data`; other
    /// supported media use `data_base64`. No JSON string wrapping of binary data.
    pub fn encode_structured(
        &self,
        envelope: &Envelope,
        external: &EventMetadata,
    ) -> Result<Vec<u8>, Error> {
        self.check_outbound(envelope, external)?;
        let media = self.codecs.default_media();
        let body = self.codecs.encode_body(envelope, media)?;
        let mut object = to_context(external, media)?;
        if media == "application/json" {
            object.insert("data".into(), codecs::json_value(&body, self.limits)?);
        } else {
            object.insert("data_base64".into(), Value::String(STANDARD.encode(body)));
        }
        let bytes = serde_json::to_vec(&object).map_err(|_| Error::Projection)?;
        self.limits.encoded(&bytes)?;
        Ok(bytes)
    }

    /// Decodes binding-specific attributes plus the actual raw body. Duplicate
    /// attributes/content type, wrong binding prefixes and native reserved headers
    /// fail; unrelated headers are not copied into the canonical envelope.
    pub fn decode_binary(
        &self,
        binding: Binding,
        headers: &[(String, Vec<u8>)],
        body: &[u8],
    ) -> Result<MappedEvent, Error> {
        self.limits.encoded(body)?;
        if headers.len() > self.limits.canonical.max_headers {
            return Err(Error::Limits);
        }
        let mut total = body.len();
        let mut object = Map::new();
        for (name, value) in headers {
            let value_limit = match binding {
                Binding::Http => HTTP_WIRE_BYTES,
                Binding::Kafka => ATTRIBUTE_BYTES,
            };
            if name.len() > 67 || value.len() > value_limit {
                return Err(Error::Limits);
            }
            total = total
                .checked_add(name.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or(Error::Limits)?;
            if total > self.limits.max_encoded_bytes {
                return Err(Error::Limits);
            }
            let name = match binding {
                Binding::Http => name.to_ascii_lowercase(),
                Binding::Kafka => name.clone(),
            };
            if reserved(&name) {
                return Err(Error::Reserved);
            }
            if name.starts_with(match binding {
                Binding::Http => "ce_",
                Binding::Kafka => "ce-",
            }) {
                return Err(Error::Malformed);
            }
            let attribute = if name == "content-type" {
                Some("datacontenttype")
            } else {
                name.strip_prefix(binding.prefix())
            };
            if let Some(attribute) = attribute {
                if matches!(attribute, "data" | "data_base64" | "datacontenttype")
                    && name != "content-type"
                {
                    return Err(Error::Malformed);
                }
                let text = if matches!(binding, Binding::Http) && name != "content-type" {
                    http_attributes::decode(value)?
                } else {
                    std::str::from_utf8(value)
                        .map_err(|_| Error::Malformed)?
                        .to_owned()
                };
                if text.len() > ATTRIBUTE_BYTES {
                    return Err(Error::Limits);
                }
                if object
                    .insert(attribute.to_owned(), Value::String(text))
                    .is_some()
                {
                    return Err(Error::Malformed);
                }
            }
        }
        let (external, media) = self.context(object)?;
        self.finish(external, &media, body)
    }

    /// Returns binding-specific context headers and the real selected body bytes.
    pub fn encode_binary(
        &self,
        binding: Binding,
        envelope: &Envelope,
        external: &EventMetadata,
    ) -> Result<BinaryEvent, Error> {
        self.check_outbound(envelope, external)?;
        let media = self.codecs.default_media();
        let body = self.codecs.encode_body(envelope, media)?;
        let mut headers = Vec::new();
        let mut total = body.len();
        for (name, value) in to_context(external, media)? {
            let name = if name == "datacontenttype" {
                "content-type".to_owned()
            } else {
                format!("{}{name}", binding.prefix())
            };
            let text = match value {
                Value::String(s) => s,
                other => other.to_string(),
            };
            let value = if matches!(binding, Binding::Http) && name != "content-type" {
                http_attributes::encode(&text)
            } else {
                text.into_bytes()
            };
            total = total
                .checked_add(name.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or(Error::Limits)?;
            headers.push((name, value));
        }
        if headers.len() > self.limits.canonical.max_headers
            || total > self.limits.max_encoded_bytes
        {
            return Err(Error::Limits);
        }
        Ok(BinaryEvent { headers, body })
    }

    fn context(&self, mut object: Map<String, Value>) -> Result<(EventMetadata, String), Error> {
        if object.len() > self.limits.canonical.max_headers {
            return Err(Error::Limits);
        }
        if string(&mut object, "specversion")? != "1.0" {
            return Err(Error::Unsupported);
        }
        let id = string(&mut object, "id")?;
        let source = string(&mut object, "source")?;
        let event_type = string(&mut object, "type")?;
        let media = optional_string(&mut object, "datacontenttype")?
            .unwrap_or_else(|| "application/json".into());
        let time = optional_string(&mut object, "time")?
            .map(|s| {
                DateTime::parse_from_rfc3339(&s)
                    .map(|t| t.with_timezone(&Utc))
                    .map_err(|_| Error::Malformed)
            })
            .transpose()?;
        let subject = optional_string(&mut object, "subject")?;
        let dataschema = optional_string(&mut object, "dataschema")?;
        let external = EventMetadata {
            id,
            source,
            event_type,
            time,
            subject,
            dataschema,
            extensions: object.into_iter().collect(),
        };
        validate_external(&external, self.limits)?;
        Ok((external, media))
    }

    fn finish(
        &self,
        external: EventMetadata,
        media: &str,
        bytes: &[u8],
    ) -> Result<MappedEvent, Error> {
        let (contract, version) = self
            .aliases
            .0
            .get(&external.event_type)
            .ok_or(Error::Unsupported)?;
        let identity = (self.identity)(&external)?;
        let metadata = EnvelopeMetadata {
            id: identity.id,
            created_at: identity.created_at,
            contract: contract.clone(),
            version: *version,
            correlation_id: None,
            causation_id: None,
            headers: BTreeMap::new(),
        };
        let envelope = self.codecs.decode_body(metadata, media, bytes)?;
        // Endpoint decoding/canonicalization can expand a small input under
        // different limits. Enforce this mapper's own final output boundary.
        self.limits.envelope(&envelope)?;
        Ok(MappedEvent { envelope, external })
    }

    fn check_outbound(&self, envelope: &Envelope, external: &EventMetadata) -> Result<(), Error> {
        validate_external(external, self.limits)?;
        let (contract, version) = self
            .aliases
            .0
            .get(&external.event_type)
            .ok_or(Error::Unsupported)?;
        let identity = (self.identity)(external)?;
        if contract != envelope.contract()
            || *version != envelope.version()
            || identity.id != envelope.id()
            || identity.created_at != envelope.created_at()
        {
            return Err(Error::Projection);
        }
        // Standard CE has no native header/correlation/causation representation.
        // Refuse silent metadata loss instead of inventing private CE attributes.
        if !envelope.headers().is_empty()
            || envelope.correlation_id().is_some()
            || envelope.causation_id().is_some()
        {
            return Err(Error::Projection);
        }
        self.limits.envelope(envelope)
    }
}

/// Binary protocol-binding output, deliberately independent of any broker SDK.
#[derive(Clone, Debug)]
pub struct BinaryEvent {
    /// Binding-specific CE headers plus content-type (not trusted origin).
    pub headers: Vec<(String, Vec<u8>)>,
    /// Actual selected body bytes, not a structured CE envelope or base64 text.
    pub body: Vec<u8>,
}

fn optional_string(object: &mut Map<String, Value>, key: &str) -> Result<Option<String>, Error> {
    match object.remove(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        _ => Err(Error::Malformed),
    }
}
fn string(object: &mut Map<String, Value>, key: &str) -> Result<String, Error> {
    optional_string(object, key)?
        .filter(|s| !s.is_empty())
        .ok_or(Error::Malformed)
}
fn reserved(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("raddle")
        || name.starts_with("wolverine")
        || matches!(
            name.as_str(),
            "tenant"
                | "tenantid"
                | "tenant-id"
                | "authorization"
                | "trustedsource"
                | "trusted-source"
                | "correlation-id"
                | "correlationid"
                | "causation-id"
                | "causationid"
        )
}
fn validate_external(external: &EventMetadata, limits: Limits) -> Result<(), Error> {
    if external.id.is_empty() || external.event_type.is_empty() || external.source.is_empty() {
        return Err(Error::Malformed);
    }
    for value in [&external.id, &external.event_type, &external.source] {
        if value.len() > 2048 || value.chars().any(char::is_control) {
            return Err(Error::Malformed);
        }
    }
    uri(&external.source, false)?;
    if external.subject.as_ref().is_some_and(|s| s.len() > 2048) {
        return Err(Error::Limits);
    }
    if external
        .subject
        .as_ref()
        .is_some_and(|s| s.chars().any(char::is_control))
    {
        return Err(Error::Malformed);
    }
    if external
        .time
        .is_some_and(|t| DateTime::parse_from_rfc3339(&t.to_rfc3339()).is_err())
    {
        return Err(Error::Malformed);
    }
    if let Some(schema) = &external.dataschema {
        uri(schema, true)?;
    }
    let standard_attributes = 5
        + usize::from(external.time.is_some())
        + usize::from(external.subject.is_some())
        + usize::from(external.dataschema.is_some());
    if external
        .extensions
        .len()
        .saturating_add(standard_attributes)
        > limits.canonical.max_headers
    {
        return Err(Error::Limits);
    }
    for (name, value) in &external.extensions {
        if reserved(name) {
            return Err(Error::Reserved);
        }
        if name.is_empty()
            || name.len() > 64
            || STANDARD_NAMES.contains(&name.as_str())
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err(Error::Malformed);
        }
        if !(value.is_string()
            || value.is_boolean()
            || value.as_i64().is_some_and(|n| i32::try_from(n).is_ok()))
        {
            return Err(Error::Malformed);
        }
        if value.as_str().is_some_and(|s| s.len() > 2048) {
            return Err(Error::Limits);
        }
        if value
            .as_str()
            .is_some_and(|s| s.chars().any(char::is_control))
        {
            return Err(Error::Malformed);
        }
    }
    limits.value(&serde_json::to_value(external).map_err(|_| Error::Malformed)?)
}
fn uri(value: &str, absolute: bool) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > 2048
        || value
            .bytes()
            .any(|b| !b.is_ascii_alphanumeric() && !b"-._~:/?#[]@!$&'()*+,;=%".contains(&b))
    {
        return Err(Error::Malformed);
    }
    let bytes = value.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && !bytes
                .get(index + 1..index + 3)
                .is_some_and(|v| v.iter().all(u8::is_ascii_hexdigit))
        {
            return Err(Error::Malformed);
        }
    }
    if absolute {
        url::Url::parse(value).map_err(|_| Error::Malformed)?;
    } else {
        let base =
            url::Url::parse("https://cloudevents.invalid/").map_err(|_| Error::Configuration)?;
        base.join(value).map_err(|_| Error::Malformed)?;
    }
    Ok(())
}
fn to_context(external: &EventMetadata, media: &str) -> Result<Map<String, Value>, Error> {
    let mut object: Map<String, Value> = external.extensions.clone().into_iter().collect();
    for (name, value) in [
        ("specversion", "1.0"),
        ("id", &external.id),
        ("source", &external.source),
        ("type", &external.event_type),
        ("datacontenttype", media),
    ] {
        object.insert(name.to_owned(), Value::String(value.to_owned()));
    }
    if let Some(time) = external.time {
        object.insert("time".into(), Value::String(time.to_rfc3339()));
    }
    if let Some(subject) = &external.subject {
        object.insert("subject".into(), Value::String(subject.clone()));
    }
    if let Some(schema) = &external.dataschema {
        object.insert("dataschema".into(), Value::String(schema.clone()));
    }
    Ok(object)
}
