use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json};
use raddle_postgres::Store;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::Serialize;
use serde_json::json;

use crate::{ApiBuilder, ApiError, Permission, Principal, specification};

#[derive(Serialize, JsonSchema)]
struct Live {
    live: bool,
}

#[derive(Serialize, JsonSchema)]
struct Ready {
    ready: bool,
    database: bool,
    workers: bool,
}

#[derive(Serialize, JsonSchema)]
struct Queues {
    ready: i64,
    active: i64,
    dead: i64,
    completed: i64,
    due: i64,
}

impl ApiBuilder {
    /// Registers private liveness, database/worker readiness and tenant queue views.
    ///
    /// All three routes require Inspect permission. `workers_ready` must reflect
    /// the application's actual service readiness (for example a host snapshot),
    /// not merely process startup. A database round trip never proves broker health.
    /// No listener, migration or external dependency is started by registration.
    pub fn health(
        &mut self,
        prefix: &str,
        store: Store,
        workers_ready: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Result<&mut Self, ApiError> {
        if prefix.ends_with('/') || prefix.contains('{') {
            return Err(ApiError::invalid());
        }
        let live_path = format!("{prefix}/live");
        let ready_path = format!("{prefix}/ready");
        let queues_path = format!("{prefix}/queues");
        // Reserve atomically: invalid or duplicate routes must not consume slots.
        let registered = self.registered.clone();
        for path in [&live_path, &ready_path, &queues_path] {
            if let Err(error) = self.reserve_template(path, "get") {
                self.registered = registered;
                return Err(error);
            }
        }
        let live =
            specification::schema::<Live>("Liveness", Contract::Serialize, &mut self.schemas);
        let ready =
            specification::schema::<Ready>("Readiness", Contract::Serialize, &mut self.schemas);
        let queues = specification::schema::<Queues>(
            "QueueStatistics",
            Contract::Serialize,
            &mut self.schemas,
        );
        let index = self.registered.len() - 2;
        let mut ready_op = specification::operation("get", index + 1, None, ready.clone(), 200);
        ready_op["responses"]["503"] = json!({"description":"A configured readiness dependency is unavailable","content":{"application/json":{"schema":ready}}});
        for (path, operation) in [
            (
                &live_path,
                specification::operation("get", index, None, live, 200),
            ),
            (&ready_path, ready_op),
            (
                &queues_path,
                specification::operation("get", index + 2, None, queues, 200),
            ),
        ] {
            self.paths
                .entry(path.to_owned())
                .or_insert_with(|| json!({}))["get"] = operation;
        }
        let ready_store = store.clone();
        let workers_ready = Arc::new(workers_ready);
        self.router = self
            .router
            .clone()
            .route(
                &live_path,
                get(|Extension(principal): Extension<Principal>| async move {
                    principal.require(Permission::Inspect)?;
                    Ok::<_, ApiError>(Json(Live { live: true }))
                }),
            )
            .route(
                &ready_path,
                get(move |Extension(principal): Extension<Principal>| {
                    let store = ready_store.clone();
                    let workers_ready = Arc::clone(&workers_ready);
                    async move {
                        principal.require(Permission::Inspect)?;
                        let database = store.is_available().await;
                        let workers = workers_ready();
                        let ready = database && workers;
                        Ok::<_, ApiError>((
                            if ready {
                                StatusCode::OK
                            } else {
                                StatusCode::SERVICE_UNAVAILABLE
                            },
                            Json(Ready {
                                ready,
                                database,
                                workers,
                            }),
                        ))
                    }
                }),
            )
            .route(
                &queues_path,
                get(move |Extension(principal): Extension<Principal>| {
                    let store = store.clone();
                    async move {
                        principal.require(Permission::Inspect)?;
                        let view = store.statistics(principal.tenant()).await?;
                        Ok::<_, ApiError>(Json(Queues {
                            ready: view.ready,
                            active: view.active,
                            dead: view.dead,
                            completed: view.completed,
                            due: view.due,
                        }))
                    }
                }),
            );
        Ok(self)
    }
}
