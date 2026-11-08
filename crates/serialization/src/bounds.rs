use raddle::wire::{Envelope, EnvelopeLimits};
use serde_json::Value;

use crate::Error;

/// Finite admission bounds. Callback/serde allocations are trusted application
/// work, not an allocator sandbox. Canonical bounds apply after projection;
/// MessagePack declared lengths are scanned before typed deserialization.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Entire encoded frame or external CloudEvent bytes.
    pub max_encoded_bytes: usize,
    /// Entire canonical envelope, including metadata.
    pub canonical: EnvelopeLimits,
    /// Maximum canonical/JSON/MessagePack value nesting, root at depth one.
    pub max_depth: usize,
    /// Maximum canonical/JSON/MessagePack value nodes or protobuf fields.
    pub max_nodes: usize,
    /// Maximum declared string, byte, collection or protobuf field length.
    pub max_declared_length: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 1_048_576,
            canonical: EnvelopeLimits::default(),
            max_depth: 64,
            max_nodes: 65_536,
            max_declared_length: 1_048_576,
        }
    }
}

impl Limits {
    pub(crate) fn validate(self) -> Result<(), Error> {
        if self.max_encoded_bytes == 0
            || self.canonical.max_bytes == 0
            || self.max_depth == 0
            || self.max_depth > 128
            || self.max_nodes == 0
            || self.max_declared_length == 0
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }

    pub(crate) fn encoded(self, bytes: &[u8]) -> Result<(), Error> {
        self.validate()?;
        if bytes.len() > self.max_encoded_bytes {
            return Err(Error::Limits);
        }
        Ok(())
    }

    pub(crate) fn value(self, value: &Value) -> Result<(), Error> {
        self.validate()?;
        let mut stack = vec![(value, 1)];
        let mut nodes = 0usize;
        while let Some((value, depth)) = stack.pop() {
            nodes = nodes.checked_add(1).ok_or(Error::Limits)?;
            if nodes > self.max_nodes || depth > self.max_depth {
                return Err(Error::Limits);
            }
            match value {
                Value::Array(items) => {
                    self.length(items.len())?;
                    if stack.len().saturating_add(items.len()) > self.max_nodes {
                        return Err(Error::Limits);
                    }
                    stack.extend(items.iter().map(|v| (v, depth + 1)));
                }
                Value::Object(items) => {
                    self.length(items.len())?;
                    if stack.len().saturating_add(items.len()) > self.max_nodes {
                        return Err(Error::Limits);
                    }
                    for (key, value) in items {
                        self.length(key.len())?;
                        stack.push((value, depth + 1));
                    }
                }
                Value::String(s) => self.length(s.len())?,
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn length(self, length: usize) -> Result<(), Error> {
        if length > self.max_declared_length {
            return Err(Error::Limits);
        }
        Ok(())
    }

    pub(crate) fn envelope(self, envelope: &Envelope) -> Result<(), Error> {
        envelope
            .validate(self.canonical)
            .map_err(|_| Error::Limits)?;
        // Traverse the full canonical object, including header nodes and strings.
        let value = serde_json::to_value(envelope).map_err(|_| Error::Projection)?;
        if let Some(payload) = value.get("payload") {
            crate::protection::plaintext(payload)?;
        }
        self.value(&value)
    }
}
