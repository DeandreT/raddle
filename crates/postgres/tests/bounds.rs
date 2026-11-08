//! Invalid time bounds are rejected before accessing PostgreSQL.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle_postgres::{DeliveryKind, Error, Store};
use sqlx::postgres::PgPoolOptions;

fn disconnected_store() -> Store {
    Store::new(
        PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(50))
            .connect_lazy("postgresql://127.0.0.1:1/unused")
            .unwrap(),
    )
}

#[tokio::test]
async fn lease_timestamp_overflow_is_a_typed_error() {
    assert!(matches!(
        disconnected_store()
            .claim(
                DeliveryKind::Local,
                1,
                Duration::from_secs(1),
                DateTime::<Utc>::MAX_UTC
            )
            .await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn sub_millisecond_leases_are_refused_before_database_access() {
    assert!(matches!(
        disconnected_store()
            .claim(DeliveryKind::Local, 1, Duration::from_nanos(1), Utc::now())
            .await,
        Err(Error::Invalid(_))
    ));
}
