//! Shared schema export preserves subschema pointers, escaping and recursive references.
#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::convert::Infallible;

use axum::body::Body;
use axum::http::{HeaderMap, Request};
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    BodyMode, Endpoint, EndpointContext, EndpointContract, EndpointOutput, EndpointRegistration,
    EndpointResponse, HttpMethod, NativeApiBuilder, Problem, RequestInput,
};
use raddle_axum::{Api, ApiBuilder, ApiError, Authenticated, Permission, Principal};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Deserialize, Serialize)]
struct PointerDto<const LEGACY_DEFINITIONS: bool> {
    name: String,
    escaped: u32,
    literal: bool,
    nested: Value,
    child: Option<Box<Self>>,
}
impl<const LEGACY_DEFINITIONS: bool> JsonSchema for PointerDto<LEGACY_DEFINITIONS> {
    fn schema_name() -> Cow<'static, str> {
        format!("PointerDto_{LEGACY_DEFINITIONS}").into()
    }
    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        pointer_schema(LEGACY_DEFINITIONS).try_into().unwrap()
    }
}
impl<const LEGACY_DEFINITIONS: bool> raddle::Request for PointerDto<LEGACY_DEFINITIONS> {
    type Reply = Self;
    type Error = Infallible;
}

fn pointer_schema(legacy: bool) -> Value {
    let key = if legacy { "definitions" } else { "$defs" };
    let reference = |tail: &str| json!({"$ref":format!("#/{key}/{tail}")});
    let mut schema = json!({
        "type":"object", "required":["name","escaped","literal","nested","child"],
        "properties":{
            "name":reference("Meta/properties/name"),
            "escaped":reference("Meta~1~0/properties/name~1~0"),
            "literal":reference("Meta~01/properties/literal~01"),
            "nested":reference("Container"),
            "child":{"anyOf":[{"$ref":"#"},{"type":"null"}]}
        }
    });
    schema[key] = json!({
        "Meta":{"type":"object","properties":{"name":{"type":"string","minLength":1}}},
        "Meta/~":{"type":"object","properties":{"name/~":{"type":"integer","minimum":1}}},
        "Meta~1":{"type":"object","properties":{"literal~1":{"type":"boolean"}}},
        "Container":{"type":"object","required":["choice"],"properties":{
            "choice":{"oneOf":[reference("Meta/properties/name"),reference("Meta~1~0/properties/name~1~0")]}
        }}
    });
    schema
}

fn auth(_: &HeaderMap) -> Result<Principal, ApiError> {
    Principal::try_new("tenant", "subject", [Permission::Invoke])
}
struct Echo<const LEGACY_DEFINITIONS: bool>;
impl<const LEGACY_DEFINITIONS: bool> Endpoint for Echo<LEGACY_DEFINITIONS> {
    type Input = PointerDto<LEGACY_DEFINITIONS>;
    type Resource = Self::Input;
    type Error = Infallible;
    async fn bind(&self, input: &mut RequestInput) -> Result<Self::Input, Problem> {
        input.json().await?.ok_or_else(|| Problem::invalid("body"))
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        input: Self::Input,
    ) -> Result<EndpointOutput<Self::Resource>, Infallible> {
        Ok(EndpointResponse::Json(input).into())
    }
    fn body_schema(&self) -> Value {
        // Exercise the native custom-body entry point separately from the
        // JsonSchema trait used by legacy requests and both resource exports.
        pointer_schema(LEGACY_DEFINITIONS)
    }
}

fn assert_references(value: &Value, document: &Value, references: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                assert!(
                    reference.starts_with("#/components/schemas/"),
                    "unrewritten {reference}"
                );
                assert!(
                    document
                        .pointer(reference.trim_start_matches('#'))
                        .is_some(),
                    "unresolved {reference}"
                );
                references.push(reference.to_owned());
            }
            for value in object.values() {
                assert_references(value, document, references);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_references(value, document, references);
            }
        }
        _ => {}
    }
}

async fn exercise<const LEGACY_DEFINITIONS: bool>() {
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(
            |request: Authenticated<PointerDto<LEGACY_DEFINITIONS>>| async move {
                Ok::<_, Infallible>(request.into_parts().1)
            },
        )
        .unwrap();
    let mut legacy = ApiBuilder::new(runtime.build(), auth);
    legacy
        .request::<PointerDto<LEGACY_DEFINITIONS>>("/echo")
        .unwrap();
    let legacy = legacy.build();
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Post, "/echo", "pointer");
    contract.body = BodyMode::Json {
        optional: false,
        max_bytes: 4096,
    };
    let registration =
        EndpointRegistration::prepare(&mut runtime, contract, Echo::<LEGACY_DEFINITIONS>).unwrap();
    let mut native = NativeApiBuilder::new(runtime.build(), auth);
    native.endpoint(registration).unwrap();
    for api in [legacy, native.build()] {
        verify(api).await;
    }
}

async fn verify(api: Api) {
    let valid = json!({"name":"top","escaped":2,"literal":true,"nested":{"choice":"nested"},"child":{"name":"child","escaped":3,"literal":false,"nested":{"choice":4},"child":null}});
    let spec = api.specification();
    let mut references = Vec::new();
    assert_references(spec, spec, &mut references);
    assert!(
        references
            .iter()
            .any(|reference| reference.ends_with("/properties/name~1~0"))
    );
    assert!(
        references
            .iter()
            .any(|reference| reference.ends_with("Meta~01/properties/literal~01"))
    );
    for schema in [
        &spec["paths"]["/echo"]["post"]["requestBody"]["content"]["application/json"]["schema"],
        &spec["paths"]["/echo"]["post"]["responses"]["200"]["content"]["application/json"]["schema"],
    ] {
        let mut schema = schema.clone();
        schema["components"] = spec["components"].clone();
        let validator = jsonschema::validator_for(&schema).unwrap();
        assert!(validator.is_valid(&valid));
        for pointer in [
            "/name",
            "/escaped",
            "/literal",
            "/nested/choice",
            "/child/name",
        ] {
            let mut invalid = valid.clone();
            *invalid.pointer_mut(pointer).unwrap() = Value::Null;
            assert!(
                !validator.is_valid(&invalid),
                "subschema at {pointer} must still be enforced"
            );
        }
    }
    let response = api
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", "Bearer allowed")
                .header("content-type", "application/json")
                .body(Body::from(valid.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let actual: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(actual, valid);
}

#[tokio::test]
async fn legacy_and_native_custom_schemas_preserve_definition_pointer_tails_and_recursion() {
    exercise::<false>().await;
    exercise::<true>().await;
}
