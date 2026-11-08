//! Minimal explicit native endpoint registration (not a production identity issuer).

use std::convert::Infallible;

use axum::http::HeaderMap;
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    BodyMode, Endpoint, EndpointContext, EndpointContract, EndpointOutput, EndpointRegistration,
    EndpointResponse, HttpMethod, NativeApiBuilder, Problem, RequestInput, ResponseKind,
};
use raddle_axum::{ApiError, Permission, Principal};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Deserialize, JsonSchema)]
struct CreateItem {
    name: String,
}

#[derive(Serialize, JsonSchema)]
struct Item {
    id: Uuid,
    name: String,
}

struct CreateItemEndpoint;

impl Endpoint for CreateItemEndpoint {
    type Input = CreateItem;
    type Resource = Item;
    type Error = Infallible;

    async fn bind(&self, input: &mut RequestInput) -> Result<CreateItem, Problem> {
        input.json().await?.ok_or_else(|| Problem::invalid("body"))
    }

    async fn handle(
        &self,
        _context: &mut EndpointContext,
        input: CreateItem,
    ) -> Result<EndpointOutput<Item>, Infallible> {
        // This example is intentionally nonpersistent. A real repository/session
        // is an explicit endpoint dependency or the enrolled context.scope().
        let resource = Item {
            id: Uuid::new_v4(),
            name: input.name,
        };
        let location = format!("/items/{}", resource.id);
        Ok(EndpointResponse::Created {
            resource: Some(resource),
            location,
        }
        .into())
    }

    fn body_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(CreateItem)).unwrap_or(Value::Bool(false))
    }
}

fn development_authentication(headers: &HeaderMap) -> Result<Principal, ApiError> {
    // Replace this loopback-only example with signature/issuer/expiry verification.
    if headers
        .get("authorization")
        .is_some_and(|value| value == "Bearer development-only")
    {
        Principal::try_new("example-tenant", "example-subject", [Permission::Invoke])
    } else {
        Err(ApiError::unauthorized())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = RuntimeBuilder::try_new(16)?;
    let mut contract = EndpointContract::new(HttpMethod::Post, "/items", "create-item");
    contract.body = BodyMode::Json {
        optional: false,
        max_bytes: 16_384,
    };
    contract.responses = vec![ResponseKind::Created];
    let registration = EndpointRegistration::prepare(&mut runtime, contract, CreateItemEndpoint)?;
    let mut api = NativeApiBuilder::new(runtime.build(), development_authentication);
    api.endpoint(registration)?;
    let api = api.build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, api.router()).await?;
    // No retries, hidden executors, durable acceptance or store are implied.
    Ok(())
}
