use std::collections::BTreeMap;
use std::str::FromStr;

use axum::http::{HeaderMap, StatusCode};
use schemars::JsonSchema;
use serde_json::{Value, json};

use super::Problem;

/// Policy for malformed present query/header values. Missing values keep defaults.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum BindingPolicy {
    /// Preserve scalar defaults; drop malformed collection elements (6.x semantics).
    #[default]
    Lenient,
    /// Reject the entire value or collection with a named 400 problem.
    Strict,
}

/// Route parse failure status. Legacy APIs continue to use their original 400.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub enum RouteParseFailure {
    /// Native strong identifier compatibility mode.
    #[default]
    NotFound,
    /// Explicit malformed-input mode.
    BadRequest,
}

/// Declarative binding source for OpenAPI metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ParameterSource {
    /// Case-sensitive route argument.
    Path,
    /// Decoded repeated query keys.
    Query,
    /// Case-insensitive HTTP header.
    Header,
    /// Form value (included in the request-body schema).
    Form,
}

/// A typed parameter's external name, schema, default and requiredness.
#[derive(Clone, Debug, PartialEq)]
pub struct Parameter {
    /// Public alias, not necessarily a Rust identifier.
    pub name: String,
    /// Binding location.
    pub source: ParameterSource,
    /// Whether absence is rejected by the application's binder.
    pub required: bool,
    /// Inline JSON schema, including defaults and annotations when supplied.
    pub schema: Value,
}

impl Parameter {
    /// Describes the actual parsed type, including arrays and newtype inner types.
    pub fn typed<T: JsonSchema>(name: &str, source: ParameterSource, required: bool) -> Self {
        Self {
            name: name.into(),
            source,
            required,
            schema: serde_json::to_value(schemars::schema_for!(T)).unwrap_or(Value::Bool(false)),
        }
    }

    /// Adds a declared default to the published schema.
    pub fn default(mut self, value: impl Into<Value>) -> Self {
        self.schema["default"] = value.into();
        self
    }

    pub(crate) fn metadata(&self) -> Value {
        json!({"name":self.name,"in":match self.source {ParameterSource::Path=>"path",ParameterSource::Query=>"query",ParameterSource::Header=>"header",ParameterSource::Form=>"form"},"required":self.required || self.source==ParameterSource::Path,"schema":self.schema})
    }
}

/// Explicit scalar/collection binding using Rust `FromStr`, including strong IDs.
/// Application constructors combine these values into mixed-source models.
#[derive(Debug, Default)]
pub struct Parameters {
    pub(crate) path: BTreeMap<String, String>,
    pub(crate) query: BTreeMap<String, Vec<String>>,
    pub(crate) headers: HeaderMap,
    pub(crate) query_policy: BindingPolicy,
    pub(crate) header_policy: BindingPolicy,
    pub(crate) route_failure: RouteParseFailure,
}

impl Parameters {
    /// Parses an optional case-sensitive route value; malformed is 404 by default.
    pub fn path<T: FromStr>(&self, name: &str) -> Result<Option<T>, Problem> {
        self.path
            .get(name)
            .map(|value| {
                value.parse().map_err(|_| match self.route_failure {
                    RouteParseFailure::NotFound => Problem::new(StatusCode::NOT_FOUND),
                    RouteParseFailure::BadRequest => Problem::invalid(name),
                })
            })
            .transpose()
    }

    /// Requires a route member. A name/case mismatch never falls back to query.
    pub fn required_path<T: FromStr>(&self, name: &str) -> Result<T, Problem> {
        self.path(name)?
            .ok_or_else(|| Problem::new(StatusCode::NOT_FOUND))
    }

    /// Binds a scalar query alias, preserving the explicit default on absence.
    pub fn query<T: FromStr>(&self, alias: &str, default: T) -> Result<T, Problem> {
        scalar(
            self.query.get(alias).map(|values| values.join(",")),
            alias,
            default,
            self.query_policy,
        )
    }

    /// Binds repeated query keys. Strict parsing is atomic; lenient drops bad items.
    pub fn query_many<T: FromStr>(&self, alias: &str) -> Result<Vec<T>, Problem> {
        self.query
            .get(alias)
            .into_iter()
            .flatten()
            .filter_map(|value| match value.parse() {
                Ok(value) => Some(Ok(value)),
                Err(_) if self.query_policy == BindingPolicy::Lenient => None,
                Err(_) => Some(Err(Problem::invalid(alias))),
            })
            .collect()
    }

    /// Preserves an explicit collection initializer when the query key is absent.
    /// Present malformed elements still follow the configured collection policy.
    pub fn query_many_or<T: FromStr>(
        &self,
        alias: &str,
        default: Vec<T>,
    ) -> Result<Vec<T>, Problem> {
        if self.query.contains_key(alias) {
            self.query_many(alias)
        } else {
            Ok(default)
        }
    }

    /// Nullable query value with an explicit optional default.
    pub fn query_optional<T: FromStr>(
        &self,
        alias: &str,
        default: Option<T>,
    ) -> Result<Option<T>, Problem> {
        optional(
            self.query.get(alias).map(|values| values.join(",")),
            alias,
            default,
            self.query_policy,
        )
    }

    /// Binds typed headers; repeated scalar headers are joined by commas.
    /// Credential headers remain the authenticator's responsibility.
    pub fn header<T: FromStr>(&self, alias: &str, default: T) -> Result<T, Problem> {
        let values: Result<Vec<_>, _> = self
            .headers
            .get_all(alias)
            .iter()
            .map(|value| value.to_str())
            .collect();
        let values = values.map_err(|_| Problem::invalid(alias))?;
        scalar(
            (!values.is_empty()).then(|| values.join(",")),
            alias,
            default,
            self.header_policy,
        )
    }

    /// Borrows raw headers without treating caller-selected values as identity.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Nullable typed header with the same repeated-value and strictness rules.
    pub fn header_optional<T: FromStr>(
        &self,
        alias: &str,
        default: Option<T>,
    ) -> Result<Option<T>, Problem> {
        let values: Result<Vec<_>, _> = self
            .headers
            .get_all(alias)
            .iter()
            .map(|value| value.to_str())
            .collect();
        let values = values.map_err(|_| Problem::invalid(alias))?;
        optional(
            (!values.is_empty()).then(|| values.join(",")),
            alias,
            default,
            self.header_policy,
        )
    }
}

fn optional<T: FromStr>(
    value: Option<String>,
    alias: &str,
    default: Option<T>,
    policy: BindingPolicy,
) -> Result<Option<T>, Problem> {
    match value {
        None => Ok(default),
        Some(value) => match value.parse() {
            Ok(value) => Ok(Some(value)),
            Err(_) if policy == BindingPolicy::Lenient => Ok(default),
            Err(_) => Err(Problem::invalid(alias)),
        },
    }
}

fn scalar<T: FromStr>(
    value: Option<String>,
    alias: &str,
    default: T,
    policy: BindingPolicy,
) -> Result<T, Problem> {
    match value {
        None => Ok(default),
        Some(value) => match value.parse() {
            Ok(value) => Ok(value),
            Err(_) if policy == BindingPolicy::Lenient => Ok(default),
            Err(_) => Err(Problem::invalid(alias)),
        },
    }
}
