//! Typed resource/body schemas describe actual socket responses, not a JSON catch-all.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;
use std::str::FromStr;

use axum::http::{HeaderMap, StatusCode};
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    BindingPolicy, BodyMode, Endpoint, EndpointContext, EndpointContract, EndpointOutput,
    EndpointRegistration, EndpointResponse, HttpMethod, NativeApiBuilder, Parameter,
    ParameterSource, Problem, RequestInput,
};
use raddle_axum::{ApiError, Permission, Principal};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(JsonSchema)]
struct ItemId(u32);
impl FromStr for ItemId {
    type Err = std::num::ParseIntError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}
#[derive(Deserialize, JsonSchema)]
struct BodyDto {
    #[serde(rename = "display-name")]
    name: String,
    #[serde(default)]
    metadata: Metadata,
}
#[derive(Default, Deserialize, Serialize, JsonSchema)]
struct Metadata {
    active: bool,
}
#[derive(Serialize, JsonSchema)]
struct Resource {
    id: u32,
    #[serde(rename = "display-name")]
    name: String,
    optional_count: Option<u32>,
    metadata: Metadata,
}
struct Typed;
impl Endpoint for Typed {
    type Input = (ItemId, BodyDto, Option<u32>);
    type Resource = Resource;
    type Error = Infallible;
    async fn bind(&self, input: &mut RequestInput) -> Result<Self::Input, Problem> {
        let id = input.parameters.required_path::<ItemId>("id")?;
        let optional = input
            .parameters
            .query_optional::<u32>("optional-count", None)?;
        let body = input
            .json::<BodyDto>()
            .await?
            .ok_or_else(|| Problem::invalid("body"))?;
        Ok((id, body, optional))
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        (id, body, optional_count): Self::Input,
    ) -> Result<EndpointOutput<Resource>, Infallible> {
        Ok(EndpointResponse::Json(Resource {
            id: id.0,
            name: body.name,
            optional_count,
            metadata: body.metadata,
        })
        .into())
    }
    fn body_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(BodyDto)).unwrap()
    }
}

#[tokio::test]
async fn strong_newtype_optional_query_serde_alias_and_precise_resource_schema_match_socket() {
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Put, "/typed/{id}", "typed");
    contract.body = BodyMode::Json {
        optional: false,
        max_bytes: 4096,
    };
    contract.query_policy = BindingPolicy::Strict;
    contract.parameters = vec![
        Parameter::typed::<ItemId>("id", ParameterSource::Path, true),
        Parameter::typed::<Option<u32>>("optional-count", ParameterSource::Query, false),
    ];
    let reg = EndpointRegistration::prepare(&mut runtime, contract, Typed).unwrap();
    let mut builder = NativeApiBuilder::new(
        runtime.build(),
        |_: &HeaderMap| -> Result<Principal, ApiError> {
            Principal::try_new("tenant", "subject", [Permission::Invoke])
        },
    );
    builder.endpoint(reg).unwrap();
    let api = builder.build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = api.router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::new();
    let response = client
        .put(format!("{url}/typed/42?optional-count=3"))
        .json(&json!({"display-name":"native"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(
        body,
        json!({"id":42,"display-name":"native","optional_count":3,"metadata":{"active":false}})
    );
    let mut schema = api.specification()["paths"]["/typed/{id}"]["put"]["responses"]["200"]["content"]
        ["application/json"]["schema"].clone();
    schema["components"] = api.specification()["components"].clone();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&body));
    assert!(
        !jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&json!({"id":"wrong","display-name":3}))
    );
    let request_schema = &api.specification()["paths"]["/typed/{id}"]["put"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert!(
        api.specification()
            .pointer(
                request_schema["$ref"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches('#')
            )
            .unwrap()["properties"]["display-name"]
            .is_object()
    );
    assert_refs_resolve(api.specification(), api.specification());
    assert_eq!(
        client
            .put(format!("{url}/typed/bad"))
            .json(&json!({"display-name":"native"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        client
            .put(format!("{url}/typed/42?optional-count=bad"))
            .json(&json!({"display-name":"native"}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let response = client
        .put(format!("{url}/typed/42"))
        .json(&json!({"display-name":"native"}))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(response["optional_count"], Value::Null);
    task.abort();
    let _ = task.await;
}

fn assert_refs_resolve(value: &Value, document: &Value) {
    match value {
        Value::Object(values) => {
            if let Some(reference) = values.get("$ref").and_then(Value::as_str)
                && reference.starts_with('#')
            {
                assert!(
                    document
                        .pointer(reference.trim_start_matches('#'))
                        .is_some(),
                    "unresolved document reference {reference}"
                );
            }
            for value in values.values() {
                assert_refs_resolve(value, document);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_refs_resolve(value, document);
            }
        }
        _ => {}
    }
}

struct Nullable<const N: usize>;
impl<const N: usize> Endpoint for Nullable<N> {
    type Input = Option<BodyDto>;
    type Resource = Resource;
    type Error = Infallible;
    async fn bind(&self, input: &mut RequestInput) -> Result<Self::Input, Problem> {
        input.json().await
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        input: Self::Input,
    ) -> Result<EndpointOutput<Resource>, Infallible> {
        Ok(match input {
            None => EndpointResponse::Missing,
            Some(body) => EndpointResponse::Json(Resource {
                id: 0,
                name: body.name,
                optional_count: None,
                metadata: body.metadata,
            }),
        }
        .into())
    }
    fn body_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(BodyDto)).unwrap()
    }
}

#[tokio::test]
async fn nullable_json_input_and_missing_resource_emit_no_json_null_body() {
    use raddle_axum::native::{MissingResponse, ResponseKind};
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Query, "/nullable", "nullable");
    contract.body = BodyMode::Json {
        optional: true,
        max_bytes: 1024,
    };
    contract.responses = vec![ResponseKind::Json, ResponseKind::Missing];
    let first =
        EndpointRegistration::prepare(&mut runtime, contract.clone(), Nullable::<0>).unwrap();
    contract.path = "/nullable204".into();
    contract.name = "nullable204".into();
    contract.missing = MissingResponse::NoContent;
    let second = EndpointRegistration::prepare(&mut runtime, contract, Nullable::<1>).unwrap();
    let mut builder = NativeApiBuilder::new(
        runtime.build(),
        |_: &HeaderMap| -> Result<Principal, ApiError> {
            Principal::try_new("tenant", "subject", [Permission::Invoke])
        },
    );
    builder.endpoint(first).unwrap().endpoint(second).unwrap();
    let api = builder.build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = api.router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::new();
    for (path, status) in [("nullable", 404), ("nullable204", 204)] {
        for body in ["null", ""] {
            let mut request = client
                .request("QUERY".parse().unwrap(), format!("{url}/{path}"))
                .header("content-type", "application/json");
            if body.is_empty() {
                request = request.header("content-length", "0");
            } else {
                request = request.body(body);
            }
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), status);
            assert!(!response.headers().contains_key("content-type"));
            assert!(response.bytes().await.unwrap().is_empty());
        }
    }
    assert_eq!(
        api.specification()["paths"]["/nullable"]["x-http-query-operation"]["requestBody"]["content"]
            ["application/json"]["schema"]["anyOf"][1]["type"],
        "null"
    );
    task.abort();
    let _ = task.await;
}
