use std::collections::BTreeMap;

use axum::body::{Body, Bytes, HttpBody};
use axum::http::{HeaderMap, StatusCode, Uri, Version, header};
use serde::de::DeserializeOwned;

use super::{BodyMode, EndpointContract, MultipartLimits, Parameters, Problem};

/// A buffered named file. Streaming endpoints should use `RequestInput::multipart`.
#[derive(Clone)]
pub struct UploadedFile {
    /// Multipart field name.
    pub name: String,
    /// Untrusted file name; never use directly as a filesystem path.
    pub filename: Option<String>,
    /// Untrusted media type.
    pub content_type: Option<String>,
    /// Bounded file bytes.
    pub bytes: Bytes,
}
impl std::fmt::Debug for UploadedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadedFile")
            .field("name_bytes", &self.name.len())
            .field("has_filename", &self.filename.is_some())
            .field("body_bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Buffered URL-encoded or multipart form with repeated fields/files preserved.
#[derive(Clone, Default)]
pub struct FormData {
    /// Decoded external field aliases.
    pub values: BTreeMap<String, Vec<String>>,
    /// Named files, including repeated file names.
    pub files: Vec<UploadedFile>,
}
impl std::fmt::Debug for FormData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FormData")
            .field("field_count", &self.values.len())
            .field("file_count", &self.files.len())
            .finish_non_exhaustive()
    }
}

impl FormData {
    /// Parses one field (repeated scalar values are joined by commas).
    pub fn value<T: std::str::FromStr>(&self, name: &str, default: T) -> Result<T, Problem> {
        self.values
            .get(name)
            .map(|values| values.join(",").parse().map_err(|_| Problem::invalid(name)))
            .unwrap_or(Ok(default))
    }
    /// Returns files for an exact field name, preserving order.
    pub fn files<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a UploadedFile> {
        self.files.iter().filter(move |file| file.name == name)
    }
}

/// Request inputs owned by the executor, including the one shared body reader.
pub struct RequestInput {
    /// Explicit typed scalar/collection bindings.
    pub parameters: Parameters,
    /// Original URI, including exact raw query bytes.
    pub uri: Uri,
    /// Transport version used for body-presence semantics.
    pub version: Version,
    pub(crate) body: Option<Body>,
    pub(crate) raw: Option<Bytes>,
    mode: BodyMode,
    can_have_body: bool,
    form: Option<FormData>,
    multipart: Option<MultipartReader>,
}

impl RequestInput {
    pub(crate) fn new(
        request: axum::extract::Request,
        captures: BTreeMap<String, String>,
        contract: &EndpointContract,
    ) -> Self {
        let (parts, body) = request.into_parts();
        let query = url::form_urlencoded::parse(parts.uri.query().unwrap_or("").as_bytes()).fold(
            BTreeMap::<String, Vec<String>>::new(),
            |mut map, (key, value)| {
                map.entry(key.into_owned())
                    .or_default()
                    .push(value.into_owned());
                map
            },
        );
        // Hyper exposes an exact zero size for END_STREAM HTTP/2 and ordinary
        // missing HTTP/1 bodies. Chunked bodies have an unknown size even at EOF.
        let declared_zero = parts
            .headers
            .get(header::CONTENT_LENGTH)
            .is_some_and(|value| value == "0");
        let can_have_body = !declared_zero
            && (parts.headers.contains_key(header::TRANSFER_ENCODING)
                || body.size_hint().exact() != Some(0));
        Self {
            parameters: Parameters {
                path: captures,
                query,
                headers: parts.headers,
                query_policy: contract.query_policy,
                header_policy: contract.header_policy,
                route_failure: contract.route_failure,
            },
            uri: parts.uri,
            version: parts.version,
            body: Some(body),
            raw: None,
            mode: contract.body.clone(),
            can_have_body,
            form: None,
            multipart: None,
        }
    }

    /// Reads required/optional JSON using transport body presence, not length alone.
    /// A declared body whose actual stream is empty is malformed, not missing.
    pub async fn json<T: DeserializeOwned>(&mut self) -> Result<Option<T>, Problem> {
        let BodyMode::Json { optional, .. } = self.mode else {
            return Err(Problem::invalid("body reader"));
        };
        if !self.can_have_body {
            return if optional {
                Ok(None)
            } else {
                Err(Problem::invalid("body"))
            };
        }
        let media = media_type(self.parameters.headers());
        if !media.is_some_and(|media| {
            media == "application/json"
                || (media.starts_with("application/") && media.ends_with("+json"))
        }) {
            return Err(Problem::new(StatusCode::UNSUPPORTED_MEDIA_TYPE));
        }
        let bytes = self.capture().await?;
        if optional {
            serde_json::from_slice::<Option<T>>(bytes).map_err(|_| Problem::invalid("body"))
        } else {
            serde_json::from_slice(bytes)
                .map(Some)
                .map_err(|_| Problem::invalid("body"))
        }
    }

    /// Borrows the one shared incremental multipart cursor. Calling this again
    /// continues from the current section; it never rewinds or buffers the body.
    pub fn multipart(&mut self) -> Result<&mut MultipartReader, Problem> {
        let limits = match self.mode {
            BodyMode::Multipart(limits) | BodyMode::Form(limits) => limits,
            _ => return Err(Problem::invalid("body reader")),
        };
        self.check_length(limits.total_bytes)?;
        if self.multipart.is_none() {
            if self.raw.is_some() || self.form.is_some() {
                return Err(Problem::invalid("body already consumed"));
            }
            let boundary = boundary(self.parameters.headers())?;
            let body = self
                .body
                .take()
                .ok_or_else(|| Problem::invalid("body already consumed"))?;
            let constraints = multer::Constraints::new().size_limit(
                multer::SizeLimit::new()
                    .whole_stream(limits.total_bytes)
                    .per_field(limits.section_bytes),
            );
            self.multipart = Some(MultipartReader {
                inner: multer::Multipart::with_constraints(
                    body.into_data_stream(),
                    boundary,
                    constraints,
                ),
                limits,
                sections: 0,
            });
        }
        self.multipart.as_mut().ok_or_else(Problem::internal)
    }

    /// Buffers a bounded form once; subsequent middleware/binder reads share it.
    pub async fn form(&mut self) -> Result<&FormData, Problem> {
        let BodyMode::Form(limits) = self.mode else {
            return Err(Problem::invalid("body reader"));
        };
        self.check_length(limits.total_bytes)?;
        if self.form.is_none() {
            let mut form = FormData::default();
            if media_type(self.parameters.headers()) == Some("application/x-www-form-urlencoded") {
                let body = self
                    .body
                    .take()
                    .ok_or_else(|| Problem::invalid("body already consumed"))?;
                let bytes = axum::body::to_bytes(body, limits.total_bytes as usize)
                    .await
                    .map_err(|_| Problem::new(StatusCode::PAYLOAD_TOO_LARGE))?;
                for (name, value) in url::form_urlencoded::parse(&bytes) {
                    if value.len() as u64 > limits.section_bytes {
                        return Err(Problem::new(StatusCode::PAYLOAD_TOO_LARGE));
                    }
                    form.values
                        .entry(name.into_owned())
                        .or_default()
                        .push(value.into_owned());
                    if form.values.values().map(Vec::len).sum::<usize>() > limits.sections {
                        return Err(Problem::new(StatusCode::PAYLOAD_TOO_LARGE));
                    }
                }
            } else {
                let reader = self.multipart()?;
                while let Some(field) = reader.next_field().await? {
                    let name = field.name().unwrap_or("").to_owned();
                    let filename = field.file_name().map(str::to_owned);
                    let content_type = field.content_type().map(ToString::to_string);
                    let bytes = field.bytes().await.map_err(multipart_error)?;
                    if filename.is_some() {
                        form.files.push(UploadedFile {
                            name,
                            filename,
                            content_type,
                            bytes,
                        });
                    } else {
                        let text = String::from_utf8(bytes.to_vec())
                            .map_err(|_| Problem::invalid("form encoding"))?;
                        form.values.entry(name).or_default().push(text);
                    }
                }
            }
            self.form = Some(form);
        }
        self.form.as_ref().ok_or_else(Problem::internal)
    }

    pub(crate) async fn capture(&mut self) -> Result<&Bytes, Problem> {
        if self.raw.is_none() {
            let limit = match self.mode {
                BodyMode::Json { max_bytes, .. } => max_bytes,
                BodyMode::None => 1_048_576,
                _ => return Err(Problem::invalid("body reader")),
            };
            self.check_length(limit as u64)?;
            let body = self
                .body
                .take()
                .ok_or_else(|| Problem::invalid("body already consumed"))?;
            self.raw = Some(
                axum::body::to_bytes(body, limit)
                    .await
                    .map_err(|_| Problem::new(StatusCode::PAYLOAD_TOO_LARGE))?,
            );
        }
        self.raw.as_ref().ok_or_else(Problem::internal)
    }

    fn check_length(&self, limit: u64) -> Result<(), Problem> {
        if let Some(value) = self.parameters.headers().get(header::CONTENT_LENGTH) {
            let length = value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| Problem::invalid("Content-Length"))?;
            if length > limit {
                return Err(Problem::new(StatusCode::PAYLOAD_TOO_LARGE));
            }
        }
        Ok(())
    }
}

/// A single consumable multipart reader with section and total streaming bounds.
pub struct MultipartReader {
    inner: multer::Multipart<'static>,
    limits: MultipartLimits,
    sections: usize,
}

impl MultipartReader {
    /// Advances one section. Consume its chunks before requesting the next field.
    pub async fn next_field(&mut self) -> Result<Option<multer::Field<'static>>, Problem> {
        let field = self.inner.next_field().await.map_err(multipart_error)?;
        if field.is_some() {
            self.sections += 1;
            if self.sections > self.limits.sections {
                return Err(Problem::new(StatusCode::PAYLOAD_TOO_LARGE));
            }
        }
        Ok(field)
    }
}

pub(crate) fn media_type(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::CONTENT_TYPE)?
        .to_str()
        .ok()?
        .split(';')
        .next()
        .map(str::trim)
}

fn boundary(headers: &HeaderMap) -> Result<String, Problem> {
    if media_type(headers) != Some("multipart/form-data") {
        return Err(Problem::new(StatusCode::UNSUPPORTED_MEDIA_TYPE));
    }
    let content = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Problem::invalid("boundary"))?;
    let boundary = multer::parse_boundary(content).map_err(|_| Problem::invalid("boundary"))?;
    if boundary.is_empty()
        || boundary.len() > 70
        || !boundary.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
    {
        return Err(Problem::invalid("boundary"));
    }
    Ok(boundary)
}

pub(crate) fn multipart_error(error: multer::Error) -> Problem {
    match error {
        multer::Error::FieldSizeExceeded { .. } | multer::Error::StreamSizeExceeded { .. } => {
            Problem::new(StatusCode::PAYLOAD_TOO_LARGE)
        }
        _ => Problem::invalid("multipart"),
    }
}
