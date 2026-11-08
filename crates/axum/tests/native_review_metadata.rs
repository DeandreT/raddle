//! Collision-safe schema bundles and unique concrete-route operation identities.
#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::convert::Infallible;
use std::marker::PhantomData;

use axum::body::Body;
use axum::http::{HeaderMap, Request};
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    ApiVersion, Endpoint, EndpointContext, EndpointContract, EndpointOutput, EndpointRegistration,
    EndpointResponse, HttpMethod, NativeApiBuilder, Parameter, ParameterSource, Problem,
    RequestInput, VersionReader, Versioning,
};
use raddle_axum::{Api, ApiError, Permission, Principal};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Default, Serialize, JsonSchema)]
struct Meta {
    active: bool,
}
#[derive(Default, Serialize, JsonSchema)]
struct Nested {
    meta: Meta,
}
#[derive(Default, Serialize, JsonSchema)]
struct Flat {
    label: String,
}
struct Typed<R>(PhantomData<R>);
impl<R: Default + Serialize + JsonSchema + Send + Sync + 'static> Endpoint for Typed<R> {
    type Input = ();
    type Resource = R;
    type Error = Infallible;
    async fn bind(&self, _input: &mut RequestInput) -> Result<(), Problem> {
        Ok(())
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        (): (),
    ) -> Result<EndpointOutput<R>, Infallible> {
        Ok(EndpointResponse::Json(R::default()).into())
    }
}
fn auth(_: &HeaderMap) -> Result<Principal, ApiError> {
    Principal::try_new("tenant", "subject", [Permission::Invoke])
}
fn response_validator(api: &Api, path: &str) -> jsonschema::Validator {
    let mut schema = api.specification()["paths"][path]["get"]["responses"]["200"]["content"]["application/json"]["schema"].clone();
    schema["components"] = api.specification()["components"].clone();
    jsonschema::validator_for(&schema).unwrap()
}
async fn get(api: &Api, path: &str) -> Value {
    let response = api
        .router()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn endpoint_root_and_another_endpoints_nested_definition_never_overwrite_in_either_order() {
    for reverse in [false, true] {
        let mut runtime = RuntimeBuilder::default();
        let nested = EndpointRegistration::prepare(
            &mut runtime,
            EndpointContract::new(HttpMethod::Get, "/items", "items"),
            Typed::<Nested>(PhantomData),
        )
        .unwrap();
        let flat = EndpointRegistration::prepare(
            &mut runtime,
            EndpointContract::new(HttpMethod::Get, "/items-meta", "items_Meta"),
            Typed::<Flat>(PhantomData),
        )
        .unwrap();
        let mut builder = NativeApiBuilder::new(runtime.build(), auth);
        if reverse {
            builder.endpoint(flat).unwrap().endpoint(nested).unwrap();
        } else {
            builder.endpoint(nested).unwrap().endpoint(flat).unwrap();
        }
        let api = builder.build();
        let nested = response_validator(&api, "/items");
        let flat = response_validator(&api, "/items-meta");
        assert!(nested.is_valid(&get(&api, "/items").await));
        assert!(flat.is_valid(&get(&api, "/items-meta").await));
        assert!(!nested.is_valid(&json!({"meta":{"label":"wrong"}})));
        assert!(!flat.is_valid(&json!({"active":false})));
    }
}

fn versioned_api(reader: Option<VersionReader>) -> Api {
    let mut runtime = RuntimeBuilder::default();
    let path = if matches!(reader, Some(VersionReader::UrlSegment)) {
        "/v{version}/items/{id?}"
    } else {
        "/items/{id?}"
    };
    // URL versions must occupy a whole segment in the native route contract.
    let path = path.replace("v{version}", "{version}");
    let mut contract = EndpointContract::new(HttpMethod::Get, &path, "optional");
    contract.parameters = vec![Parameter::typed::<u32>("id", ParameterSource::Path, false)];
    if let Some(reader) = reader {
        contract.versioning = Some(Versioning {
            supported: vec![ApiVersion::new("1.0"), ApiVersion::new("2.0")],
            mapped: vec![],
            readers: vec![reader],
            default: Some("1.0".into()),
            neutral: false,
        });
    }
    let registration =
        EndpointRegistration::prepare(&mut runtime, contract, Typed::<Flat>(PhantomData)).unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(registration).unwrap();
    builder.build()
}

#[tokio::test]
async fn optional_paths_have_stable_unique_operation_ids_with_url_and_non_url_versions() {
    for reader in [
        None,
        Some(VersionReader::UrlSegment),
        Some(VersionReader::Header("x-version".into())),
    ] {
        let api = versioned_api(reader.clone());
        let repeated = versioned_api(reader.clone());
        assert_eq!(api.specification(), repeated.specification());
        let paths = api.specification()["paths"].as_object().unwrap();
        let mut ids = HashSet::new();
        let operation_schema = json!({"type":"object","required":["operationId","responses"],"properties":{"operationId":{"type":"string","minLength":1},"responses":{"type":"object"},"parameters":{"type":"array","items":{"type":"object","required":["name","in","schema"]}}}});
        let validator = jsonschema::validator_for(&operation_schema).unwrap();
        for (path, item) in paths {
            if path == "/openapi.json" {
                continue;
            }
            let operation = &item["get"];
            assert!(validator.is_valid(operation));
            assert!(
                ids.insert(operation["operationId"].as_str().unwrap()),
                "duplicate operationId for {path}"
            );
            for parameter in operation["parameters"].as_array().unwrap() {
                if parameter["in"] == "path" {
                    assert!(path.contains(&format!("{{{}}}", parameter["name"].as_str().unwrap())));
                    assert_eq!(parameter["required"], true);
                }
            }
        }
        assert_eq!(
            ids.len(),
            if matches!(reader, Some(VersionReader::UrlSegment)) {
                4
            } else {
                2
            }
        );
        let prefix = if matches!(reader, Some(VersionReader::UrlSegment)) {
            "/1.0"
        } else {
            ""
        };
        assert_eq!(
            get(&api, &format!("{prefix}/items")).await,
            json!({"label":""})
        );
        assert_eq!(
            get(&api, &format!("{prefix}/items/42")).await,
            json!({"label":""})
        );
        for path in paths.keys().filter(|path| path.as_str() != "/openapi.json") {
            assert!(response_validator(&api, path).is_valid(&json!({"label":""})));
        }
    }
}
