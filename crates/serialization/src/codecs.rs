use std::io::Cursor;

use prost::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::canonical::Checked;
use crate::{Error, Limits};

/// An application-known Rust body model, independent of the canonical contract.
/// Implementations must consume the whole body, honor bounds and diagnose lossy
/// conversion. Custom codecs and converters are trusted allocation boundaries.
pub trait BodyCodec<M>: Send + Sync + 'static {
    /// Explicit media type. It does not establish a foreign envelope protocol.
    fn content_type(&self) -> &'static str;
    /// Encodes the typed model, never a substituted generic JSON body.
    fn encode(&self, message: &M, limits: Limits) -> Result<Vec<u8>, Error>;
    /// Decodes one complete body of the registered type.
    fn decode(&self, bytes: &[u8], limits: Limits) -> Result<M, Error>;
}

/// Typed UTF-8 JSON. A UTF-8 BOM is accepted on reads; trailing data is refused.
/// No implicit `$type`, reference graph handling or CLR type activation.
#[derive(Clone, Copy, Debug, Default)]
pub struct Json;
impl<M: Serialize + DeserializeOwned> BodyCodec<M> for Json {
    fn content_type(&self) -> &'static str {
        "application/json"
    }
    fn encode(&self, message: &M, limits: Limits) -> Result<Vec<u8>, Error> {
        limits.validate()?;
        let bytes = serde_json::to_vec(&Checked(message)).map_err(|_| Error::Projection)?;
        limits.encoded(&bytes)?;
        // Validate the emitted representation too (e.g. custom serde output).
        crate::protection::plaintext(&json_value(&bytes, limits)?)?;
        Ok(bytes)
    }
    fn decode(&self, bytes: &[u8], limits: Limits) -> Result<M, Error> {
        limits.encoded(bytes)?;
        let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
        crate::protection::plaintext(&json_value(bytes, limits)?)?;
        serde_json::from_slice(bytes).map_err(|_| Error::Malformed)
    }
}

pub(crate) fn json_value(bytes: &[u8], limits: Limits) -> Result<serde_json::Value, Error> {
    limits.encoded(bytes)?;
    let value = crate::strict_json::parse(bytes)?;
    limits.value(&value)?;
    Ok(value)
}

/// Explicit encoding shape for structs. Tuple/record models retain their own
/// serde shape; integer-key maps require an explicitly declared typed map model.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum MessagePackLayout {
    /// Struct fields encoded as positional arrays (stable declared field order).
    Positional,
    /// Struct fields encoded as maps with their serde property names.
    Map,
}

/// Genuine MessagePack bodies with a safe bounded profile. Extension types,
/// compression and runtime formatters are not part of this native profile.
#[derive(Clone, Copy, Debug)]
pub struct MessagePack(pub MessagePackLayout);
impl<M: Serialize + DeserializeOwned> BodyCodec<M> for MessagePack {
    fn content_type(&self) -> &'static str {
        "binary/messagepack"
    }
    fn encode(&self, message: &M, limits: Limits) -> Result<Vec<u8>, Error> {
        limits.validate()?;
        let bytes = match self.0 {
            MessagePackLayout::Positional => rmp_serde::to_vec(message),
            MessagePackLayout::Map => rmp_serde::to_vec_named(message),
        }
        .map_err(|_| Error::Projection)?;
        if bytes == [0xc0] {
            return Err(Error::Projection);
        }
        scan_messagepack(&bytes, limits)?;
        Ok(bytes)
    }
    fn decode(&self, bytes: &[u8], limits: Limits) -> Result<M, Error> {
        if bytes == [0xc0] {
            return Err(Error::Malformed);
        }
        scan_messagepack(bytes, limits)?;
        let mut decoder = rmp_serde::Deserializer::new(Cursor::new(bytes));
        M::deserialize(&mut decoder).map_err(|_| Error::Malformed)
    }
}

fn scan_messagepack(bytes: &[u8], limits: Limits) -> Result<(), Error> {
    limits.encoded(bytes)?;
    // Counts of still-unread values at each depth. No length-sized allocation.
    let mut stack = vec![1usize];
    let mut cursor = 0usize;
    let mut nodes = 0usize;
    while let Some(left) = stack.last_mut() {
        if *left == 0 {
            stack.pop();
            continue;
        }
        *left -= 1;
        nodes += 1;
        if nodes > limits.max_nodes || stack.len() > limits.max_depth {
            return Err(Error::Limits);
        }
        let tag = take(bytes, &mut cursor, 1)?[0];
        let (scalar, children) = match tag {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => (0, 0),
            0x80..=0x8f => (0, usize::from(tag & 15) * 2),
            0x90..=0x9f => (0, usize::from(tag & 15)),
            0xa0..=0xbf => (usize::from(tag & 31), 0),
            0xc4 | 0xd9 => (number(bytes, &mut cursor, 1)?, 0),
            0xc5 | 0xda => (number(bytes, &mut cursor, 2)?, 0),
            0xc6 | 0xdb => (number(bytes, &mut cursor, 4)?, 0),
            0xca | 0xce | 0xd2 => (4, 0),
            0xcb | 0xcf | 0xd3 => (8, 0),
            0xcc | 0xd0 => (1, 0),
            0xcd | 0xd1 => (2, 0),
            0xdc => (0, number(bytes, &mut cursor, 2)?),
            0xdd => (0, number(bytes, &mut cursor, 4)?),
            0xde => (
                0,
                number(bytes, &mut cursor, 2)?
                    .checked_mul(2)
                    .ok_or(Error::Limits)?,
            ),
            0xdf => (
                0,
                number(bytes, &mut cursor, 4)?
                    .checked_mul(2)
                    .ok_or(Error::Limits)?,
            ),
            // Includes reserved and extension types: never guess a formatter.
            _ => return Err(Error::Unsupported),
        };
        if matches!(tag, 0xa0..=0xbf | 0xc4..=0xc6 | 0xd9..=0xdb) {
            limits.length(scalar)?;
        }
        // A map declares entries, not twice that many values. Node accounting
        // still includes both keys and values; fixed-width numbers declare no
        // variable length and are bounded by encoded bytes instead.
        let declared = if matches!(tag, 0x80..=0x8f | 0xde | 0xdf) {
            children / 2
        } else {
            children
        };
        limits.length(declared)?;
        if children > limits.max_nodes.saturating_sub(nodes) {
            return Err(Error::Limits);
        }
        take(bytes, &mut cursor, scalar)?;
        if children != 0 {
            stack.push(children);
        }
    }
    if cursor != bytes.len() {
        return Err(Error::Malformed);
    }
    Ok(())
}

fn number(bytes: &[u8], cursor: &mut usize, width: usize) -> Result<usize, Error> {
    Ok(take(bytes, cursor, width)?
        .iter()
        .fold(0usize, |n, byte| (n << 8) | usize::from(*byte)))
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8], Error> {
    let end = cursor.checked_add(length).ok_or(Error::Limits)?;
    let value = bytes.get(*cursor..end).ok_or(Error::Malformed)?;
    *cursor = end;
    Ok(value)
}

/// Genuine protobuf bodies for an explicit `prost::Message` schema. This profile
/// deliberately rejects empty reads, matching the released body-wrapper boundary,
/// although an all-default proto3 value can successfully encode to zero bytes.
/// Unknown fields follow prost semantics; groups are not supported here.
#[derive(Clone, Copy, Debug, Default)]
pub struct Protobuf;
impl<M: Message + Default> BodyCodec<M> for Protobuf {
    fn content_type(&self) -> &'static str {
        "binary/protobuf"
    }
    fn encode(&self, message: &M, limits: Limits) -> Result<Vec<u8>, Error> {
        limits.validate()?;
        if message.encoded_len() > limits.max_encoded_bytes {
            return Err(Error::Limits);
        }
        let bytes = message.encode_to_vec();
        if !bytes.is_empty() {
            scan_protobuf(&bytes, limits)?;
        }
        Ok(bytes)
    }
    fn decode(&self, bytes: &[u8], limits: Limits) -> Result<M, Error> {
        scan_protobuf(bytes, limits)?;
        M::decode(bytes).map_err(|_| Error::Malformed)
    }
}

fn scan_protobuf(bytes: &[u8], limits: Limits) -> Result<(), Error> {
    limits.encoded(bytes)?;
    if bytes.is_empty() {
        return Err(Error::Malformed);
    }
    let mut cursor = 0;
    let mut nodes = 0;
    while cursor < bytes.len() {
        nodes += 1;
        if nodes > limits.max_nodes {
            return Err(Error::Limits);
        }
        let key = varint(bytes, &mut cursor)?;
        if key >> 3 == 0 || key >> 3 > 0x1fff_ffff {
            return Err(Error::Malformed);
        }
        let length = match key & 7 {
            0 => {
                varint(bytes, &mut cursor)?;
                0
            }
            1 => 8,
            2 => {
                let length =
                    usize::try_from(varint(bytes, &mut cursor)?).map_err(|_| Error::Limits)?;
                limits.length(length)?;
                length
            }
            5 => 4,
            _ => return Err(Error::Malformed),
        };
        take(bytes, &mut cursor, length)?;
    }
    Ok(())
}

fn varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, Error> {
    let mut result = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = take(bytes, cursor, 1)?[0];
        if shift == 63 && byte > 1 {
            return Err(Error::Malformed);
        }
        result |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(result);
        }
    }
    Err(Error::Malformed)
}
