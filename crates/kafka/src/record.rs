use crate::{Error, Limits};

/// A native Kafka header. Duplicate names and null/empty values are preserved.
#[derive(Clone)]
pub struct Header {
    name: String,
    value: Option<Vec<u8>>,
}

impl Header {
    /// Validates allocation and native C-string boundaries without interpreting
    /// a header as authentication, tenant, saga or correlation authority.
    pub fn try_new(name: impl Into<String>, value: Option<Vec<u8>>) -> Result<Self, Error> {
        let name = name.into();
        if name.len() > 8192
            || name.contains('\0')
            || value.as_ref().is_some_and(|v| v.len() > 1_048_576)
        {
            return Err(Error::Rejected);
        }
        Ok(Self { name, value })
    }
    /// Exact UTF-8 name.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// None is a native null value, not an empty byte slice.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

impl std::fmt::Debug for Header {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Header")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Raw Kafka record input. A tombstone has a non-null key and null value; a
/// zero-length value is represented separately and is never turned into null.
pub struct Record<'a> {
    pub(crate) key: Option<&'a [u8]>,
    pub(crate) value: Option<&'a [u8]>,
    pub(crate) headers: &'a [Header],
}

impl<'a> Record<'a> {
    /// Constructs a non-null value with an optional native key.
    pub fn value(key: Option<&'a [u8]>, value: &'a [u8], headers: &'a [Header]) -> Self {
        Self {
            key,
            value: Some(value),
            headers,
        }
    }
    /// Constructs a true tombstone, retaining even a valid zero-length key.
    pub fn tombstone(key: &'a [u8], headers: &'a [Header]) -> Self {
        Self {
            key: Some(key),
            value: None,
            headers,
        }
    }
    pub(crate) fn validate(&self, limits: Limits) -> Result<(), Error> {
        if self.headers.len() > limits.max_headers {
            return Err(Error::Rejected);
        }
        let mut bytes = self.value.map_or(0, <[u8]>::len) + self.key.map_or(0, <[u8]>::len);
        for header in self.headers {
            bytes += header.name.len() + header.value.as_ref().map_or(0, Vec::len);
        }
        if bytes > limits.max_record_bytes {
            return Err(Error::Rejected);
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{Header, Record};
    use crate::Limits;

    #[test]
    fn null_and_empty_are_distinct_and_native_nuls_are_refused() {
        let null = Header::try_new("name", None).unwrap();
        let empty = Header::try_new("name", Some(Vec::new())).unwrap();
        assert_eq!(null.value(), None);
        assert_eq!(empty.value(), Some(&[][..]));
        assert!(Header::try_new("name\0", None).is_err());
        let tombstone = Record::tombstone(b"key", &[]);
        let empty = Record::value(Some(b"key"), b"", &[]);
        assert!(tombstone.value.is_none());
        assert!(empty.value.is_some());
        tombstone.validate(Limits::default()).unwrap();
        empty.validate(Limits::default()).unwrap();
    }
}
