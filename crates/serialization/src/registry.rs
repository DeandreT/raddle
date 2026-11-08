use std::collections::BTreeMap;
use std::sync::Arc;

use raddle::wire::{Envelope, EnvelopeMetadata, WireMessage};
use serde::{Deserialize, Serialize};

use crate::canonical::Checked;
use crate::codecs;
use crate::{BodyCodec, Error, Json, Limits};

const MAGIC: &[u8; 5] = b"RDSB\x01";
type Key = (String, String, u32);
type Encode = dyn Fn(&Envelope, Limits) -> Result<Vec<u8>, Error> + Send + Sync;
type Decode = dyn Fn(&[u8], EnvelopeMetadata, Limits) -> Result<Envelope, Error> + Send + Sync;
type Canonicalize = dyn Fn(Envelope) -> Result<Envelope, Error> + Send + Sync;

struct Registration {
    encode: Box<Encode>,
    decode: Box<Decode>,
}

/// Explicit application registrations. Duplicate keys fail; no registration
/// changes the selected default. JSON can be retained under a binary default.
#[derive(Default)]
pub struct Registry {
    entries: BTreeMap<Key, Arc<Registration>>,
    // None inherits at an endpoint; the application fallback is always JSON.
    default_media: Option<String>,
}

impl Registry {
    /// Registers JSON for a canonical native contract. Unknown contracts are
    /// never guessed from object keys or activated from a foreign type name.
    pub fn json<R: WireMessage>(&mut self) -> Result<(), Error> {
        self.register::<R, R, _, _, _>(Json, Ok, Ok)
    }

    /// Registers a typed wire model with explicit trusted projections to/from a
    /// canonical native contract. These callbacks must reject any unsupported or
    /// lossy representation (graphs, non-finite numbers, native-only map keys,
    /// unknown protobuf fields when preservation matters, etc.). They can allocate
    /// application memory; the registry validates results, not their allocator.
    pub fn register<M, R, C, IntoCanonical, FromCanonical>(
        &mut self,
        codec: C,
        into_canonical: IntoCanonical,
        from_canonical: FromCanonical,
    ) -> Result<(), Error>
    where
        M: 'static,
        R: WireMessage,
        C: BodyCodec<M>,
        IntoCanonical: Fn(M) -> Result<R, Error> + Send + Sync + 'static,
        FromCanonical: Fn(R) -> Result<M, Error> + Send + Sync + 'static,
    {
        let media = codec.content_type();
        validate_media(media)?;
        raddle::SubscriptionName::try_new(R::NAME).map_err(|_| Error::Configuration)?;
        if R::VERSION == 0 {
            return Err(Error::Configuration);
        }
        let key = (media.to_owned(), R::NAME.to_owned(), R::VERSION);
        if self.entries.contains_key(&key) {
            return Err(Error::Configuration);
        }
        let codec = Arc::new(codec);
        let encoder = Arc::clone(&codec);
        self.entries.insert(
            key,
            Arc::new(Registration {
                encode: Box::new(move |envelope, limits| {
                    limits.envelope(envelope)?;
                    let canonical = envelope.decode::<R>().map_err(|_| Error::Projection)?;
                    // Refuse serde targets that silently ignore canonical members or
                    // change their representation before conversion to the wire model.
                    let projected = envelope
                        .rebody(&canonical, limits.canonical)
                        .map_err(|_| Error::Projection)?;
                    if projected.content_bytes().map_err(|_| Error::Projection)?
                        != envelope.content_bytes().map_err(|_| Error::Projection)?
                    {
                        return Err(Error::Projection);
                    }
                    let bytes = encoder.encode(&from_canonical(canonical)?, limits)?;
                    limits.encoded(&bytes)?;
                    Ok(bytes)
                }),
                decode: Box::new(move |bytes, metadata, limits| {
                    limits.encoded(bytes)?;
                    let model = codec.decode(bytes, limits)?;
                    let canonical = into_canonical(model)?;
                    let value =
                        serde_json::to_value(Checked(&canonical)).map_err(|_| Error::Projection)?;
                    crate::protection::plaintext(&value)?;
                    limits.value(&value)?;
                    // The target is fixed by registration, not by decoded model data.
                    // Metadata was validated before the callback. With unchanged
                    // metadata and a JSON value, failure here is an output bound.
                    let envelope = Envelope::from_parts(metadata, value, limits.canonical)
                        .map_err(|_| Error::Limits)?;
                    limits.envelope(&envelope)?;
                    Ok(envelope)
                }),
            }),
        );
        Ok(())
    }

    /// Chooses an already registered media type as the application default.
    /// The corresponding contract must still have a registration at send time.
    pub fn set_default(&mut self, media: &str) -> Result<(), Error> {
        validate_media(media)?;
        if !self.entries.keys().any(|key| key.0 == media) {
            return Err(Error::Unsupported);
        }
        self.default_media = Some(media.to_owned());
        Ok(())
    }
}

/// Endpoint-local registration/selection, falling back to immutable application
/// registrations. A local codec wins for its exact media/contract/version key;
/// untouched siblings retain the application default and registrations.
pub struct EndpointCodecs {
    application: Arc<Registry>,
    local: Registry,
    limits: Limits,
    canonicalize: Option<Arc<Canonicalize>>,
}

impl EndpointCodecs {
    /// Builds an endpoint view. Limits are validated before any codec is called.
    pub fn new(application: Arc<Registry>, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self {
            application,
            local: Registry::default(),
            limits,
            canonicalize: None,
        })
    }

    /// Endpoint-only registrations/selection. This never mutates application
    /// state. Use `EndpointCodecs::set_default` to select an inherited codec.
    pub fn local_mut(&mut self) -> &mut Registry {
        &mut self.local
    }

    /// Selects an endpoint default present locally or at application level.
    pub fn set_default(&mut self, media: &str) -> Result<(), Error> {
        validate_media(media)?;
        if !self
            .local
            .entries
            .keys()
            .chain(self.application.entries.keys())
            .any(|key| key.0 == media)
        {
            return Err(Error::Unsupported);
        }
        self.local.default_media = Some(media.to_owned());
        Ok(())
    }

    /// Adds a trusted final canonicalizer, e.g. an explicit bounded `wire::Codecs`
    /// upcast followed by `Envelope::rebody`. It runs before dispatch/custody and
    /// must retain identity, time, correlation, causation and headers. Changing
    /// those fields is rejected; schema/body may change through the projection.
    pub fn with_canonicalizer<F>(mut self, canonicalize: F) -> Self
    where
        F: Fn(Envelope) -> Result<Envelope, Error> + Send + Sync + 'static,
    {
        self.canonicalize = Some(Arc::new(canonicalize));
        self
    }

    /// Actual selected outgoing media type, not a reflection-based type hint.
    pub fn default_media(&self) -> &str {
        self.local
            .default_media
            .as_deref()
            .or(self.application.default_media.as_deref())
            .unwrap_or("application/json")
    }

    /// Validates an already-canonical plaintext envelope, e.g. a legacy JSON
    /// transport bridge. Protected raddle crypto envelopes are an unsupported
    /// composition, not typed application models. This does not decode a body,
    /// authenticate/decrypt anything or apply the selected-body canonicalizer.
    pub fn validate_plaintext(&self, envelope: &Envelope) -> Result<(), Error> {
        self.limits.envelope(envelope)
    }

    /// Encodes the selected typed body plus explicit native metadata framing.
    /// RDSB1 is opt-in raddle framing, not a Wolverine/private broker envelope.
    pub fn encode(&self, envelope: &Envelope) -> Result<Vec<u8>, Error> {
        let media = self.default_media();
        let body = self.encode_body(envelope, media)?;
        let header = serde_json::to_vec(&Header {
            metadata: envelope.metadata(),
            content_type: media.to_owned(),
        })
        .map_err(|_| Error::Projection)?;
        let length = u32::try_from(header.len()).map_err(|_| Error::Limits)?;
        let total = 9usize
            .checked_add(header.len())
            .and_then(|n| n.checked_add(body.len()))
            .ok_or(Error::Limits)?;
        if total > self.limits.max_encoded_bytes {
            return Err(Error::Limits);
        }
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&body);
        Ok(bytes)
    }

    /// Resolves incoming media locally first, then globally. Missing, unknown or
    /// conflicting media/schema is refused; this transport boundary deliberately
    /// does not apply a permissive serializer-helper fallback.
    pub fn decode(&self, bytes: &[u8]) -> Result<Envelope, Error> {
        self.limits.encoded(bytes)?;
        if !bytes.starts_with(MAGIC) {
            return Err(Error::Malformed);
        }
        let length_bytes: [u8; 4] = bytes
            .get(5..9)
            .ok_or(Error::Malformed)?
            .try_into()
            .map_err(|_| Error::Malformed)?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        self.limits.length(length)?;
        let end = 9usize.checked_add(length).ok_or(Error::Limits)?;
        let header = bytes.get(9..end).ok_or(Error::Malformed)?;
        // Refuse duplicate JSON members before serde target resolution.
        codecs::json_value(header, self.limits)?;
        let header: Header = serde_json::from_slice(header).map_err(|_| Error::Malformed)?;
        self.decode_body(header.metadata, &header.content_type, &bytes[end..])
    }

    /// Produces real body bytes for a binding that carries metadata separately.
    pub fn encode_body(&self, envelope: &Envelope, media: &str) -> Result<Vec<u8>, Error> {
        let registration = self.lookup(media, envelope.contract(), envelope.version())?;
        (registration.encode)(envelope, self.limits)
    }

    /// Binding-independent receive boundary with explicit native metadata. This
    /// returns a validated canonical envelope, never an erased runtime object.
    pub fn decode_body(
        &self,
        metadata: EnvelopeMetadata,
        media: &str,
        bytes: &[u8],
    ) -> Result<Envelope, Error> {
        self.limits.encoded(bytes)?;
        validate_media(media).map_err(|_| Error::Unsupported)?;
        // Validate metadata BEFORE invoking application callbacks, even for a
        // malformed body. The temporary null is never dispatched or persisted.
        Envelope::from_parts(
            metadata.clone(),
            serde_json::Value::Null,
            self.limits.canonical,
        )
        .map_err(|_| Error::Malformed)?;
        let registration = self.lookup(media, &metadata.contract, metadata.version)?;
        let envelope = (registration.decode)(bytes, metadata, self.limits)?;
        if let Some(canonicalize) = &self.canonicalize {
            let before = envelope.metadata();
            let envelope = canonicalize(envelope)?;
            let after = envelope.metadata();
            if before.id != after.id
                || before.created_at != after.created_at
                || before.correlation_id != after.correlation_id
                || before.causation_id != after.causation_id
                || before.headers != after.headers
            {
                return Err(Error::Projection);
            }
            self.limits.envelope(&envelope)?;
            return Ok(envelope);
        }
        Ok(envelope)
    }

    fn lookup(&self, media: &str, contract: &str, version: u32) -> Result<&Registration, Error> {
        let key = (media.to_owned(), contract.to_owned(), version);
        self.local
            .entries
            .get(&key)
            .or_else(|| self.application.entries.get(&key))
            .map(AsRef::as_ref)
            .ok_or(Error::Unsupported)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    metadata: EnvelopeMetadata,
    content_type: String,
}

fn validate_media(media: &str) -> Result<(), Error> {
    if media.len() > 128
        || media.bytes().filter(|b| *b == b'/').count() != 1
        || media.split('/').any(str::is_empty)
        || !media
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"/+.-".contains(&b))
    {
        return Err(Error::Configuration);
    }
    Ok(())
}
