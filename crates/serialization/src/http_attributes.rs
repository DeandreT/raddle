use crate::Error;

// CloudEvents HTTP 1.0.2 attribute values are printable ASCII on the wire.
// Kafka deliberately does not use this transformation. Work over UTF-8 bytes
// so each non-ASCII character is encoded as its complete byte sequence.
pub(crate) fn encode(value: &str) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = Vec::new();
    for byte in value.bytes() {
        if (b'!'..=b'~').contains(&byte) && !matches!(byte, b'"' | b'%') {
            output.push(byte);
        } else {
            output.extend_from_slice(&[
                b'%',
                HEX[usize::from(byte >> 4)],
                HEX[usize::from(byte & 15)],
            ]);
        }
    }
    output
}

pub(crate) fn decode(value: &[u8]) -> Result<String, Error> {
    if !value
        .iter()
        .all(|b| (b' '..=b'~').contains(b) || *b == b'\t')
    {
        return Err(Error::Malformed);
    }
    let value = value.trim_ascii();
    let unescaped = if value.first() == Some(&b'"') {
        if value.len() < 2 || value.last() != Some(&b'"') {
            return Err(Error::Malformed);
        }
        let mut output = Vec::new();
        let mut cursor = 1;
        let end = value.len() - 1;
        while cursor < end {
            let byte = value[cursor];
            if byte == b'"' {
                return Err(Error::Malformed);
            }
            if byte == b'\\' {
                cursor += 1;
                if cursor >= end {
                    return Err(Error::Malformed);
                }
            }
            output.push(value[cursor]);
            cursor += 1;
        }
        output
    } else {
        if value.contains(&b'"') {
            return Err(Error::Malformed);
        }
        value.to_vec()
    };
    let mut output = Vec::new();
    let mut cursor = 0;
    while cursor < unescaped.len() {
        if unescaped[cursor] == b'%' {
            let pair = unescaped
                .get(cursor + 1..cursor + 3)
                .ok_or(Error::Malformed)?;
            output.push((hex(pair[0])? << 4) | hex(pair[1])?);
            cursor += 3;
        } else {
            output.push(unescaped[cursor]);
            cursor += 1;
        }
    }
    let text = String::from_utf8(output).map_err(|_| Error::Malformed)?;
    if text.chars().any(char::is_control) {
        return Err(Error::Malformed);
    }
    Ok(text)
}

fn hex(byte: u8) -> Result<u8, Error> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::Malformed),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{decode, encode};

    #[test]
    fn specification_vector_utf8_percent_and_legacy_quoted_values() {
        assert_eq!(encode("Euro € 😀"), b"Euro%20%E2%82%AC%20%F0%9F%98%80");
        assert_eq!(
            decode(b"Euro%20%e2%82%ac%20%f0%9f%98%80").unwrap(),
            "Euro € 😀"
        );
        assert_eq!(encode("a \"100%\""), b"a%20%22100%25%22");
        assert_eq!(decode(br#"  "a \"100%25\""  "#).unwrap(), "a \"100%\"");
        assert_eq!(decode(b"%2520").unwrap(), "%20");
        assert_eq!(decode(b"a\\b").unwrap(), "a\\b");
    }

    #[test]
    fn rejects_bad_quotes_escapes_percent_sequences_controls_and_invalid_utf8() {
        for value in [
            &b"\"unfinished"[..],
            b"\"a\"trailing",
            b"\"a\\\"",
            b"%",
            b"%0",
            b"%XX",
            b"%C0%A0",
            b"%ED%A0%80",
            b"%E2%82",
            b"%0a",
            b"%7F",
            b"raw\r\n",
            "雪".as_bytes(),
        ] {
            assert!(decode(value).is_err(), "{value:?}");
        }
    }
}
