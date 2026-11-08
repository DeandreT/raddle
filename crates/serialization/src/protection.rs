use serde_json::Value;

use crate::Error;

/// Existing raddle crypto reserves this root format namespace. This is a
/// fail-closed composition guard, not decryption or format interoperability.
pub(crate) fn plaintext(value: &Value) -> Result<(), Error> {
    if value
        .get("format")
        .and_then(Value::as_str)
        .is_some_and(|s| reserved_format(s.as_bytes()))
    {
        return Err(Error::UnsupportedProtection);
    }
    Ok(())
}

pub(crate) fn reserved_format(value: &[u8]) -> bool {
    value.starts_with(b"raddle-aes")
}
