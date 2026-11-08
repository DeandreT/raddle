use crate::Error;

const PREFIX: &[u8] = b"raddle-mqtt-quarantine-v1\0";

/// Borrowed original bytes from an explicitly versioned quarantine snapshot.
/// Framing keeps even an empty original payload nonempty on the retained topic:
/// a zero-byte retained MQTT publication would delete the evidence instead.
/// Neither the frame nor its topic establishes tenant authority.
pub struct QuarantineSnapshot<'a> {
    payload: &'a [u8],
}
impl<'a> QuarantineSnapshot<'a> {
    /// Decodes a snapshot without allocating, bounding the original payload size.
    pub fn from_slice(bytes: &'a [u8], max_payload_bytes: usize) -> Result<Self, Error> {
        let payload = bytes
            .strip_prefix(PREFIX)
            .ok_or(Error::Invalid("quarantine snapshot format"))?;
        if payload.len() > max_payload_bytes {
            return Err(Error::Invalid("quarantine snapshot limit"));
        }
        Ok(Self { payload })
    }
    /// Exact original bytes, including an empty payload, for inspection or replay.
    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }
    pub(crate) fn encode(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(PREFIX.len() + payload.len());
        bytes.extend_from_slice(PREFIX);
        bytes.extend_from_slice(payload);
        bytes
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::QuarantineSnapshot;
    #[test]
    fn frame_preserves_empty_arbitrary_and_nested_payloads_with_a_bound() {
        for payload in [&[][..], &[0, 255, 0][..], b"raddle-mqtt-quarantine-v1\0"] {
            let encoded = QuarantineSnapshot::encode(payload);
            assert!(!encoded.is_empty());
            let decoded = QuarantineSnapshot::from_slice(&encoded, payload.len()).unwrap();
            assert_eq!(decoded.payload(), payload);
        }
        assert!(QuarantineSnapshot::from_slice(b"", 10).is_err());
        assert!(QuarantineSnapshot::from_slice(&QuarantineSnapshot::encode(b"a"), 0).is_err());
    }
}
