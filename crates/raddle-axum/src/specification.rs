use schemars::JsonSchema;
use serde_json::{Map, Value, json};

pub(crate) fn schema<T: JsonSchema>(name: &str, components: &mut Map<String, Value>) -> Value {
    let mut root = schemars::schema_for!(T).to_value();
    let definitions = root.as_object_mut().and_then(|object| {
        object.remove("$schema");
        object
            .remove("$defs")
            .or_else(|| object.remove("definitions"))
    });
    rewrite(&mut root, name);
    if let Some(Value::Object(definitions)) = definitions {
        for (key, mut value) in definitions {
            rewrite(&mut value, name);
            components.insert(format!("{name}_{key}"), value);
        }
    }
    components.insert(name.into(), root);
    json!({ "$ref": format!("#/components/schemas/{name}") })
}

fn rewrite(value: &mut Value, prefix: &str) {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get_mut("$ref") {
                if reference == "#" {
                    *reference = format!("#/components/schemas/{prefix}");
                } else if let Some(name) = reference
                    .strip_prefix("#/$defs/")
                    .or_else(|| reference.strip_prefix("#/definitions/"))
                {
                    *reference = format!("#/components/schemas/{prefix}_{name}");
                }
            }
            for value in object.values_mut() {
                rewrite(value, prefix);
            }
        }
        Value::Array(values) => {
            for value in values {
                rewrite(value, prefix);
            }
        }
        _ => {}
    }
}

pub(crate) fn operation(
    method: &str,
    index: usize,
    request: Option<Value>,
    response: Value,
    success: u16,
) -> Value {
    let mut responses = Map::new();
    responses.insert(success.to_string(), json!({ "description": "Successful operation", "content": { "application/json": { "schema": response } } }));
    for status in [400, 401, 403, 404, 405, 409, 413, 415, 422, 500, 503] {
        let mut error = json!({ "description": "Operation rejected", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } });
        if status == 401 {
            error["headers"] = json!({ "WWW-Authenticate": { "description": "Bearer authentication challenge", "schema": { "type": "string" } } });
        }
        if status == 405 {
            error["headers"] = json!({ "Allow": { "description": "Registered resource methods", "schema": { "type": "string" } } });
        }
        responses.insert(status.to_string(), error);
    }
    let mut operation = json!({ "operationId": format!("raddle_{method}_{index}"), "security": [{ "bearerAuth": [] }], "responses": responses });
    if let Some(request) = request {
        operation["requestBody"] =
            json!({ "required": true, "content": { "application/json": { "schema": request } } });
    }
    operation
}
