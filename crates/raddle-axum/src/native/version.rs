use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;

use super::{Problem, RequestInput};

/// Version metadata, expressed natively rather than pretending to be Asp.Versioning.
#[derive(Clone, Debug)]
pub struct ApiVersion {
    /// Exact public selector (for example `1.0`).
    pub value: String,
    /// Optional HTTP-date sunset header, supplied by the application.
    pub sunset: Option<String>,
    /// Optional Deprecation structured value.
    pub deprecation: Option<String>,
    /// Optional Link header with migration documentation.
    pub link: Option<String>,
}

impl ApiVersion {
    /// Creates a supported nondeprecated version.
    pub fn new(value: &str) -> Self {
        Self {
            value: value.into(),
            sunset: None,
            deprecation: None,
            link: None,
        }
    }
}

/// Native version selector; multiple readers must agree when simultaneously present.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum VersionReader {
    /// Expand `{version}` in the route to a literal supported version.
    UrlSegment,
    /// Exact query alias.
    Query(String),
    /// Case-insensitive header name.
    Header(String),
    /// Parameter name in Content-Type (for example `v`).
    MediaType(String),
}

/// Explicit version subset/default/neutral and per-version response headers.
#[derive(Clone, Debug)]
pub struct Versioning {
    /// Versions declared by this endpoint scope (method overrides are explicit).
    pub supported: Vec<ApiVersion>,
    /// Optional subset exposed by this registration; must be declared above.
    pub mapped: Vec<String>,
    /// Readers in precedence order. Conflicting supplied versions are rejected.
    pub readers: Vec<VersionReader>,
    /// Default when none is supplied; None requires an explicit selector.
    pub default: Option<String>,
    /// Neutral endpoint ignores selectors and version expansion.
    pub neutral: bool,
}

impl Versioning {
    pub(crate) fn validate(&self) -> Result<(), Problem> {
        for reader in &self.readers {
            match reader {
                VersionReader::Header(name) if axum::http::HeaderName::try_from(name).is_err() => {
                    return Err(Problem::invalid("version header"));
                }
                VersionReader::Query(name) | VersionReader::MediaType(name)
                    if name.is_empty() || name.len() > 128 =>
                {
                    return Err(Problem::invalid("version reader"));
                }
                _ => {}
            }
        }
        if !self.neutral && (self.supported.is_empty() || self.readers.is_empty()) {
            return Err(Problem::invalid("versioning"));
        }
        let mut versions = std::collections::HashSet::new();
        for version in &self.supported {
            if version.value.is_empty()
                || version.value.len() > 32
                || !version
                    .value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
                || !versions.insert(&version.value)
                || version
                    .sunset
                    .iter()
                    .chain(&version.deprecation)
                    .chain(&version.link)
                    .any(|value| HeaderValue::try_from(value).is_err())
            {
                return Err(Problem::invalid("version metadata"));
            }
        }
        if self
            .mapped
            .iter()
            .any(|version| !versions.contains(version))
            || self
                .default
                .as_ref()
                .is_some_and(|version| !versions.contains(version))
        {
            return Err(Problem::invalid("version subset/default"));
        }
        if self
            .mapped
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != self.mapped.len()
            || self
                .default
                .as_ref()
                .is_some_and(|version| !self.mapped.is_empty() && !self.mapped.contains(version))
        {
            return Err(Problem::invalid("version subset/default"));
        }
        Ok(())
    }

    pub(crate) fn values(&self) -> Vec<String> {
        self.supported
            .iter()
            .filter(|version| self.mapped.is_empty() || self.mapped.contains(&version.value))
            .map(|version| version.value.clone())
            .collect()
    }

    pub(crate) fn select(
        &self,
        input: &RequestInput,
        url_version: Option<&str>,
    ) -> Result<Option<String>, Problem> {
        if self.neutral {
            return Ok(None);
        }
        select(self, &input.uri, input.parameters.headers(), url_version)
    }

    pub(crate) fn apply(
        &self,
        selected: Option<&str>,
        response: &mut Response,
    ) -> Result<(), Problem> {
        response.headers_mut().insert(
            "api-supported-versions",
            HeaderValue::try_from(self.values().join(", ")).map_err(|_| Problem::internal())?,
        );
        if let Some(version) = self
            .supported
            .iter()
            .find(|version| Some(version.value.as_str()) == selected)
        {
            for (name, value) in [
                ("sunset", &version.sunset),
                ("deprecation", &version.deprecation),
                ("link", &version.link),
            ] {
                if let Some(value) = value {
                    response.headers_mut().insert(
                        name,
                        HeaderValue::try_from(value).map_err(|_| Problem::internal())?,
                    );
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn select(
    config: &Versioning,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
    url: Option<&str>,
) -> Result<Option<String>, Problem> {
    if config.neutral {
        return Ok(None);
    }
    let mut selected: Option<String> = None;
    for reader in &config.readers {
        let values: Vec<String> = match reader {
            VersionReader::UrlSegment => url.into_iter().map(str::to_owned).collect(),
            VersionReader::Query(name) => {
                url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
                    .filter(|(key, _)| key == name)
                    .map(|(_, value)| value.into_owned())
                    .collect()
            }
            VersionReader::Header(name) => headers
                .get_all(name)
                .iter()
                .filter_map(|value| value.to_str().ok().map(str::to_owned))
                .collect(),
            VersionReader::MediaType(name) => headers
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .into_iter()
                .flat_map(|value| value.split(';').skip(1))
                .filter_map(|part| part.trim().split_once('='))
                .filter(|(key, _)| key == name)
                .map(|(_, value)| value.trim_matches('"').into())
                .collect(),
        };
        for value in values {
            if selected.as_ref().is_some_and(|selected| selected != &value) {
                return Err(Problem::invalid("api version"));
            }
            selected = Some(value);
        }
    }
    let selected = selected
        .or_else(|| config.default.clone())
        .ok_or_else(|| Problem::invalid("api version"))?;
    if !config.values().contains(&selected) {
        return Err(Problem::new(StatusCode::BAD_REQUEST).detail("Unsupported API version"));
    }
    Ok(Some(selected))
}
