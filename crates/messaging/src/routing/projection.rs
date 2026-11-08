use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use raddle::wire::{Envelope, WireMessage};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{Key, RoutingError};
use crate::TrustedOrigin;

type Convert = dyn Fn(&Envelope, Uuid) -> Result<Envelope, RoutingError> + Send + Sync;

pub(crate) struct Projection {
    pub id: String,
    pub version: u32,
    pub from: Key,
    pub to: Key,
    convert: Box<Convert>,
}
impl Projection {
    pub fn new<From: WireMessage, To: WireMessage>(
        id: &str,
        version: u32,
        from: Key,
        to: Key,
        map: impl Fn(From) -> Result<To, RoutingError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            version,
            from,
            to,
            convert: Box::new(move |input, id| {
                // Inspect actual framing BEFORE permissive serde models can discard it.
                plaintext(input)?;
                let source = input
                    .decode::<From>()
                    .map_err(|_| RoutingError::Projection)?;
                let target = map(source)?;
                let output = Envelope::with_identity(id, input.created_at(), &target)
                    .map_err(|_| RoutingError::Projection)?;
                plaintext(&output)?;
                Ok(output)
            }),
        }
    }
    pub fn apply(
        &self,
        input: &Envelope,
        root: &Envelope,
        origin: &TrustedOrigin,
        path: &[std::sync::Arc<Self>],
        headers: &BTreeSet<String>,
    ) -> Result<Envelope, RoutingError> {
        let id = identity(root, origin, path);
        let mut output = catch_unwind(AssertUnwindSafe(|| (self.convert)(input, id)))
            .map_err(|_| RoutingError::Projection)??;
        for name in headers {
            // Even an overly broad propagation allowlist cannot forward RPC authority.
            if !matches!(
                name.as_str(),
                "raddle-reply-to" | "raddle-request-token" | "raddle-reply-mode"
            ) && let Some(value) = root.headers().get(name)
            {
                output = output
                    .with_header(name.clone(), value.clone())
                    .map_err(|_| RoutingError::Projection)?;
            }
        }
        output
            .correlated(
                root.correlation_id().unwrap_or(root.id()),
                root.causation_id().unwrap_or(root.id()),
            )
            .map_err(|_| RoutingError::Projection)
    }
}

// Versioned, length-delimited native derivation. UUIDv8 denotes application-defined
// identity, not a foreign wire identity. Trusted origin separates independent roots
// before they converge on an egress namespace; headers never participate as authority.
fn identity(root: &Envelope, origin: &TrustedOrigin, path: &[std::sync::Arc<Projection>]) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"raddle.publication-projection.v1\0");
    for value in [
        origin.source().as_bytes(),
        origin.tenant().as_bytes(),
        root.id().as_bytes(),
        root.contract().as_bytes(),
        &root.version().to_be_bytes(),
    ] {
        digest.update((value.len() as u32).to_be_bytes());
        digest.update(value);
    }
    for projection in path {
        for value in [
            projection.id.as_bytes(),
            &projection.version.to_be_bytes(),
            projection.to.0.as_bytes(),
            &projection.to.1.to_be_bytes(),
        ] {
            digest.update((value.len() as u32).to_be_bytes());
            digest.update(value);
        }
    }
    let hash = digest.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn plaintext(envelope: &Envelope) -> Result<(), RoutingError> {
    let value = serde_json::to_value(envelope).map_err(|_| RoutingError::Projection)?;
    if value
        .get("payload")
        .and_then(|payload| payload.get("format"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|format| format.starts_with("raddle-aes"))
    {
        return Err(RoutingError::Protected);
    }
    Ok(())
}
