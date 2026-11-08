use serde_json::Value;

use crate::Error;

/// Existing raddle crypto reserves this root format namespace. This is a
/// fail-closed composition guard, not decryption or format interoperability.
pub(crate) fn plaintext(value: &Value) -> Result<(), Error> {
    if value
        .get("format")
        .and_then(Value::as_str)
        .is_some_and(|s| s.starts_with("raddle-aes"))
    {
        return Err(Error::UnsupportedProtection);
    }
    Ok(())
}
