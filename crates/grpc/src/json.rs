use std::io::{self, Write};

use serde::Serialize;

use crate::MAX_JSON_BYTES;

pub(crate) enum EncodeError {
    TooLarge,
    Serialization,
}

struct BoundedJson {
    bytes: Vec<u8>,
    exceeded: bool,
}

impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_JSON_BYTES - self.bytes.len() {
            self.exceeded = true;
            return Err(io::Error::other("JSON size limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn encode(value: &impl Serialize) -> Result<Vec<u8>, EncodeError> {
    let mut writer = BoundedJson {
        bytes: Vec::new(),
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.bytes),
        Err(_) if writer.exceeded => Err(EncodeError::TooLarge),
        Err(_) => Err(EncodeError::Serialization),
    }
}
