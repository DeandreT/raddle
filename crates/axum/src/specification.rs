use schemars::JsonSchema;
use schemars::generate::{Contract, SchemaSettings};
use serde_json::{Map, Value, json};

pub(crate) fn schema<T: JsonSchema>(
    name: &str,
    contract: Contract,
    components: &mut Map<String, Value>,
) -> Value {
    let root = SchemaSettings::draft2020_12()
        .with(|settings| settings.contract = contract)
        .into_generator()
        .into_root_schema_for::<T>()
        .to_value();
    value_schema(root, name, components)
}

pub(crate) fn value_schema(
    mut root: Value,
    name: &str,
    components: &mut Map<String, Value>,
) -> Value {
    let definitions = root.as_object_mut().and_then(|object| {
        object.remove("$schema");
        object
            .remove("$defs")
            .or_else(|| object.remove("definitions"))
    });
    // Allocate the whole bundle before rewriting references. A definition from
    // one endpoint may have the same preferred name as another endpoint's root.
    // Never overwrite a previously published component, in either ordering.
    let root_name = reserve_component(name, components);
    let definitions = definitions
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let names: std::collections::BTreeMap<_, _> = definitions
        .keys()
        .map(|key| {
            (
                key.clone(),
                reserve_component(&format!("{name}_{key}"), components),
            )
        })
        .collect();
    rewrite(&mut root, &root_name, &names);
    for (key, mut value) in definitions {
        rewrite(&mut value, &root_name, &names);
        components.insert(names[&key].clone(), value);
    }
    components.insert(root_name.clone(), root);
    json!({ "$ref": component_reference(&root_name) })
}

fn reserve_component(preferred: &str, components: &mut Map<String, Value>) -> String {
    let mut name = preferred.to_owned();
    let mut suffix = 2;
    while components.contains_key(&name) {
        name = format!("{preferred}_{suffix}");
        suffix += 1;
    }
    components.insert(name.clone(), Value::Null);
    name
}

fn component_reference(name: &str) -> String {
    format!(
        "#/components/schemas/{}",
        name.replace('~', "~0").replace('/', "~1")
    )
}

fn rewrite(
    value: &mut Value,
    root: &str,
    definitions: &std::collections::BTreeMap<String, String>,
) {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get_mut("$ref") {
                if reference == "#" {
                    *reference = component_reference(root);
                } else if let Some(name) = reference
                    .strip_prefix("#/$defs/")
                    .or_else(|| reference.strip_prefix("#/definitions/"))
                {
                    // Decode only the definition-name token. The remaining JSON
                    // Pointer is already escaped and must survive byte-for-byte.
                    let (name, suffix) = name.split_at(name.find('/').unwrap_or(name.len()));
                    let name = name.replace("~1", "/").replace("~0", "~");
                    if let Some(component) = definitions.get(&name) {
                        *reference = format!("{}{suffix}", component_reference(component));
                    }
                }
            }
            for value in object.values_mut() {
                rewrite(value, root, definitions);
            }
        }
        Value::Array(values) => {
            for value in values {
                rewrite(value, root, definitions);
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
    for status in [400, 401, 403, 404, 405, 409, 413, 415, 422, 429, 500, 503] {
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::schema;
    use schemars::JsonSchema;
    use schemars::generate::Contract;
    use serde_json::{Map, Value, json};

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct Meta {
        active: bool,
    }
    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct Nested {
        meta: Meta,
    }
    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct Flat {
        label: String,
    }

    #[test]
    fn legacy_typed_schema_entry_point_preserves_colliding_bundles_in_both_orders() {
        for reverse in [false, true] {
            let mut components = Map::new();
            let (nested, flat) = if reverse {
                let flat =
                    schema::<Flat>("Resource_items_Meta", Contract::Serialize, &mut components);
                (
                    schema::<Nested>("Resource_items", Contract::Serialize, &mut components),
                    flat,
                )
            } else {
                let nested =
                    schema::<Nested>("Resource_items", Contract::Serialize, &mut components);
                (
                    nested,
                    schema::<Flat>("Resource_items_Meta", Contract::Serialize, &mut components),
                )
            };
            let document = json!({"components":{"schemas":components}});
            for (mut root, valid, invalid) in [
                (
                    nested,
                    json!({"meta":{"active":true}}),
                    json!({"meta":{"label":"wrong"}}),
                ),
                (flat, json!({"label":"right"}), json!({"active":true})),
            ] {
                root["components"] = document["components"].clone();
                let validator = jsonschema::validator_for(&root).unwrap();
                assert!(validator.is_valid(&valid));
                assert!(!validator.is_valid(&invalid));
            }
            assert!(
                document["components"]["schemas"]
                    .as_object()
                    .unwrap()
                    .values()
                    .all(|value| !matches!(value, Value::Null))
            );
        }
    }
}
