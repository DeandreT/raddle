//! Native PostgreSQL external-table clauses and independent row failure recovery.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "external_tables/binding.rs"]
mod binding;
#[path = "external_tables/configuration.rs"]
mod configuration;
#[path = "external_tables/failures.rs"]
mod failures;
#[path = "external_tables/projection.rs"]
mod projection;
#[path = "external_tables/recovery.rs"]
mod recovery;
#[path = "external_tables/recovery_bounds.rs"]
mod recovery_bounds;
#[path = "external_tables/redelivery.rs"]
mod redelivery;
#[path = "external_tables/registry.rs"]
mod registry;
#[path = "external_tables/runtime.rs"]
mod runtime;
#[path = "external_tables/support.rs"]
mod support;

#[tokio::test]
async fn external_native_child() {
    use chrono::Utc;
    use sqlx::{
        ConnectOptions,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    let Ok(url) = std::env::var("RADDLE_EXTERNAL_CHILD_URL") else {
        return;
    };
    let phase = std::env::var("RADDLE_EXTERNAL_CHILD_PHASE").unwrap();
    let options: PgConnectOptions = url.parse().unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            options
                .application_name(&format!("external-native-{phase}"))
                .disable_statement_logging(),
        )
        .await
        .unwrap();
    let listener = support::listener(&pool, support::config(), Default::default());
    let report = listener.poll_once(Utc::now()).await.unwrap();
    std::process::exit(if report.busy { 25 } else { 24 });
}
