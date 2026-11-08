use std::collections::HashSet;

use axum::http::{Method, StatusCode};

use super::{
    BindingPolicy, CsrfPolicy, MissingResponse, Negotiation, OutputCachePolicy, Parameter,
    ParameterSource, Problem, RatePolicy, ResponseCache, ResponseKind, RouteParseFailure,
    Versioning,
};

/// Supported native endpoint methods, including body-carrying QUERY.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum HttpMethod {
    /// GET.
    Get,
    /// POST.
    Post,
    /// PUT.
    Put,
    /// DELETE.
    Delete,
    /// PATCH.
    Patch,
    /// OPTIONS.
    Options,
    /// HEAD (response body suppressed).
    Head,
    /// Safe body-carrying QUERY. Published as an OpenAPI extension, not an invalid operation.
    Query,
}

impl HttpMethod {
    /// Uppercase wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Patch => "PATCH",
            Self::Options => "OPTIONS",
            Self::Head => "HEAD",
            Self::Query => "QUERY",
        }
    }
    pub(crate) fn matches(self, method: &Method) -> bool {
        self.as_str() == method.as_str()
    }
}

/// Multipart bounds enforced incrementally by the shared reader.
#[derive(Clone, Copy, Debug)]
pub struct MultipartLimits {
    /// Maximum total wire bytes, including boundaries.
    pub total_bytes: u64,
    /// Maximum bytes in one file or form field.
    pub section_bytes: u64,
    /// Maximum accepted sections (checked when the next field is requested).
    pub sections: usize,
}

impl Default for MultipartLimits {
    fn default() -> Self {
        Self {
            total_bytes: 1_048_576,
            section_bytes: 262_144,
            sections: 128,
        }
    }
}

/// One body reader per endpoint. Incompatible readers cannot be composed.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum BodyMode {
    /// No body binding.
    #[default]
    None,
    /// Required or optional JSON with an explicit byte bound.
    Json {
        /// Whether a transport-declared missing body is accepted.
        optional: bool,
        /// Maximum bytes consumed.
        max_bytes: usize,
    },
    /// Buffered URL-encoded or multipart form, including named files.
    Form(MultipartLimits),
    /// Unbuffered incremental multipart. Middleware and endpoint share one cursor.
    Multipart(MultipartLimits),
}

/// Prefix selection is replacement, never stacking.
#[derive(Clone, Debug, Default)]
pub struct RoutePrefixes {
    /// Lowest-precedence global prefix.
    pub global: Option<String>,
    /// Namespace prefixes, keyed by specificity (higher wins).
    pub namespace: Vec<(usize, String)>,
    /// Highest-precedence endpoint prefix.
    pub endpoint: Option<String>,
}

impl RoutePrefixes {
    pub(crate) fn apply(&self, path: &str) -> String {
        let prefix = self
            .endpoint
            .as_ref()
            .or_else(|| {
                self.namespace
                    .iter()
                    .max_by_key(|(depth, _)| depth)
                    .map(|(_, prefix)| prefix)
            })
            .or(self.global.as_ref());
        match prefix {
            Some(prefix) => format!("{}{path}", prefix.trim_end_matches('/')),
            None => path.into(),
        }
    }
}

/// Explicit startup contract shared by routing, execution and OpenAPI generation.
#[derive(Clone, Debug)]
pub struct EndpointContract {
    /// Wire method.
    pub method: HttpMethod,
    /// Absolute route, optionally ending in `{id?}` or `{*rest}`.
    pub path: String,
    /// Stable route name and operation identity. Must be unique within an API.
    pub name: String,
    /// Prefix precedence settings.
    pub prefixes: RoutePrefixes,
    /// Exactly one selected body reader.
    pub body: BodyMode,
    /// Binding metadata, using external aliases.
    pub parameters: Vec<Parameter>,
    /// Query parsing mode; defaults to released 6.x leniency.
    pub query_policy: BindingPolicy,
    /// Header parsing mode; defaults to released 6.x leniency.
    pub header_policy: BindingPolicy,
    /// Native strong route IDs default to 404 without changing legacy APIs.
    pub route_failure: RouteParseFailure,
    /// Allowed response categories; undeclared handler results fail safely.
    pub responses: Vec<ResponseKind>,
    /// Additional/overridden OpenAPI responses, required for arbitrary started responses.
    pub response_metadata: std::collections::BTreeMap<String, serde_json::Value>,
    /// Missing resource policy, legal only on GET/QUERY when 204 is selected.
    pub missing: MissingResponse,
    /// Configured resource writer negotiation behavior.
    pub negotiation: Negotiation,
    /// Exact input media type for same-route dispatch; parameters are ignored.
    pub accepts: Option<String>,
    /// Explicit anonymous override. Authentication is required by default.
    pub anonymous: bool,
    /// Opt-in session-bound antiforgery enforcement.
    pub csrf: CsrfPolicy,
    /// Optional output caching, distinct from conditional GET and response headers.
    pub output_cache: Option<OutputCachePolicy>,
    /// Optional response-cache headers.
    pub response_cache: Option<ResponseCache>,
    /// Optional named partitioned rate policy; None is an explicit exemption.
    pub rate: Option<RatePolicy>,
    /// Version matching and metadata (native semantic equivalent only).
    pub versioning: Option<Versioning>,
    /// PostgreSQL persisted response replay, separate from durable 202 acceptance.
    pub replay: Option<super::endpoint::ReplayPolicy>,
}

impl EndpointContract {
    /// Generates a named route link with percent-encoded captures. Missing optional
    /// terminal captures are removed; wildcard captures preserve path separators.
    pub fn link(&self, values: &std::collections::BTreeMap<&str, &str>) -> Result<String, Problem> {
        self.paths()?;
        let path = self.prefixes.apply(&self.path);
        let mut output = Vec::new();
        for segment in path.split('/') {
            if let Some(name) = segment
                .strip_prefix('{')
                .and_then(|value| value.strip_suffix('}'))
            {
                let wildcard = name.starts_with('*');
                let optional = name.ends_with('?');
                let name = name.trim_start_matches('*').trim_end_matches('?');
                let Some(value) = values.get(name) else {
                    if optional {
                        continue;
                    }
                    return Err(Problem::invalid(name));
                };
                let encode = |value: &str| {
                    // Form/query encoding uses '+' for spaces; route segments do not.
                    url::form_urlencoded::byte_serialize(value.as_bytes())
                        .collect::<String>()
                        .replace('+', "%20")
                };
                output.push(if wildcard {
                    value.split('/').map(encode).collect::<Vec<_>>().join("/")
                } else {
                    encode(value)
                });
            } else {
                output.push(segment.to_owned());
            }
        }
        let output = output.join("/");
        Ok(if output.is_empty() {
            "/".into()
        } else {
            output
        })
    }
    /// Creates a secure JSON-resource contract; body binding is off until selected.
    pub fn new(method: HttpMethod, path: &str, name: &str) -> Self {
        Self {
            method,
            path: path.into(),
            name: name.into(),
            prefixes: RoutePrefixes::default(),
            body: BodyMode::None,
            parameters: Vec::new(),
            query_policy: BindingPolicy::Lenient,
            header_policy: BindingPolicy::Lenient,
            route_failure: RouteParseFailure::NotFound,
            responses: vec![ResponseKind::Json],
            response_metadata: std::collections::BTreeMap::new(),
            missing: MissingResponse::NotFound,
            negotiation: Negotiation::Loose,
            accepts: None,
            anonymous: false,
            csrf: CsrfPolicy::Disabled,
            output_cache: None,
            response_cache: None,
            rate: None,
            versioning: None,
            replay: None,
        }
    }

    pub(crate) fn paths(&self) -> Result<Vec<String>, Problem> {
        let path = self.prefixes.apply(&self.path);
        if !path.starts_with('/')
            || path.len() > 240
            || path.contains("//")
            || path == "/openapi.json"
        {
            return Err(Problem::invalid("route"));
        }
        let mut names = HashSet::new();
        let segments: Vec<_> = path.split('/').skip(1).collect();
        for (index, segment) in segments.iter().enumerate() {
            if let Some(name) = segment
                .strip_prefix('{')
                .and_then(|value| value.strip_suffix('}'))
            {
                let terminal = name.starts_with('*') || name.ends_with('?');
                let name = name.trim_start_matches('*').trim_end_matches('?');
                if name.is_empty()
                    || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    || !names.insert(name)
                    || (terminal && index + 1 != segments.len())
                {
                    return Err(Problem::invalid("route"));
                }
            } else if !segment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            {
                return Err(Problem::invalid("route"));
            }
        }
        let url_version = self.versioning.as_ref().is_some_and(|config| {
            !config.neutral
                && config
                    .readers
                    .iter()
                    .any(|reader| matches!(reader, super::VersionReader::UrlSegment))
        });
        if url_version && !names.remove("version") {
            return Err(Problem::invalid("version route"));
        }
        let declared: HashSet<_> = self
            .parameters
            .iter()
            .filter(|parameter| parameter.source == ParameterSource::Path)
            .map(|parameter| parameter.name.as_str())
            .collect();
        if names != declared {
            return Err(Problem::invalid("route parameters"));
        }
        if let Some(last) = segments.last().filter(|segment| segment.ends_with("?}")) {
            let base = path[..path.len() - last.len() - 1].to_owned();
            return Ok(vec![
                if base.is_empty() { "/".into() } else { base },
                path.replace("?}", "}"),
            ]);
        }
        Ok(vec![path])
    }

    pub(crate) fn validate(&self) -> Result<(), Problem> {
        self.paths()?;
        if self.name.is_empty()
            || self.name.len() > 128
            || !self
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            || self.responses.is_empty()
        {
            return Err(Problem::invalid("endpoint name or responses"));
        }
        if self.responses.contains(&ResponseKind::Started) && self.response_metadata.is_empty() {
            return Err(Problem::invalid("started response metadata"));
        }
        for (status, response) in &self.response_metadata {
            if (status != "default"
                && !status
                    .parse::<u16>()
                    .is_ok_and(|status| (100..600).contains(&status)))
                || !response.is_object()
                || !response["description"].is_string()
                || response
                    .get("content")
                    .is_some_and(|content| !content.is_object())
                || response
                    .get("headers")
                    .is_some_and(|headers| !headers.is_object())
            {
                return Err(Problem::invalid("response metadata"));
            }
        }
        if matches!(self.method, HttpMethod::Get | HttpMethod::Head)
            && !matches!(self.body, BodyMode::None)
        {
            return Err(Problem::invalid("GET/HEAD body reader"));
        }
        if self.missing == MissingResponse::NoContent
            && !matches!(self.method, HttpMethod::Get | HttpMethod::Query)
        {
            return Err(Problem::invalid("missing response policy"));
        }
        match self.body {
            BodyMode::Json { max_bytes, .. } if max_bytes == 0 || max_bytes > 64 * 1024 * 1024 => {
                return Err(Problem::invalid("body limit"));
            }
            BodyMode::Form(limits) | BodyMode::Multipart(limits)
                if limits.total_bytes == 0
                    || limits.section_bytes == 0
                    || limits.sections == 0
                    || limits.sections > 4096
                    || limits.section_bytes > limits.total_bytes =>
            {
                return Err(Problem::invalid("multipart limits"));
            }
            _ => {}
        }
        let mut parameters = HashSet::new();
        for parameter in &self.parameters {
            if parameter.name.is_empty()
                || parameter.name.len() > 128
                || !parameters.insert((
                    format!("{:?}", parameter.source),
                    if parameter.source == ParameterSource::Header {
                        parameter.name.to_ascii_lowercase()
                    } else {
                        parameter.name.clone()
                    },
                ))
            {
                return Err(Problem::invalid("parameter"));
            }
            if parameter.source == ParameterSource::Header
                && (parameter.name.eq_ignore_ascii_case("authorization")
                    || axum::http::HeaderName::try_from(&parameter.name).is_err())
            {
                return Err(Problem::invalid("header"));
            }
        }
        if let Some(media) = &self.accepts
            && (!media.contains('/') || media.contains(';') || media.len() > 128)
        {
            return Err(Problem::invalid("media type"));
        }
        if let Some(versioning) = &self.versioning {
            versioning.validate()?;
        }
        if let Some(cache) = &self.output_cache {
            cache.validate()?;
            if !matches!(self.method, HttpMethod::Get | HttpMethod::Head) {
                return Err(Problem::invalid("output cache method"));
            }
        }
        if let Some(cache) = &self.response_cache
            && (cache.vary.len() > 64
                || cache
                    .vary
                    .iter()
                    .any(|name| axum::http::HeaderName::try_from(name).is_err()))
        {
            return Err(Problem::invalid("response cache headers"));
        }
        if let Some(replay) = &self.replay {
            replay.validate()?;
            if self.output_cache.is_some() {
                return Err(Problem::invalid(
                    "output cache and response replay cannot share an endpoint",
                ));
            }
            if matches!(self.body, BodyMode::Form(_) | BodyMode::Multipart(_)) {
                return Err(Problem::invalid("replay body reader"));
            }
        }
        if matches!(self.body, BodyMode::Form(_))
            && self.parameters.iter().any(|p| {
                p.source == ParameterSource::Form && p.schema == serde_json::Value::Bool(false)
            })
        {
            return Err(Problem::new(StatusCode::BAD_REQUEST));
        }
        Ok(())
    }
}
