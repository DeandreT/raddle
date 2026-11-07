//! Full opt-in application composition against actual disposable local services.
#![allow(clippy::unwrap_used)]

#[path = "../examples/support/application.rs"]
mod application;

use sqlx::PgPool;

#[test]
fn application_rejects_nonloopback_or_ambiguous_service_addresses() {
    for (database, rabbit) in [
        ("postgres://127.0.0.1/example", "amqp://localhost/"),
        ("postgres://127.0.0.1/example", "amqps://192.0.2.1/"),
        ("postgres://db.example/example", "amqp://127.0.0.1/"),
        ("postgres://127.0.0.1/example", "https://127.0.0.1/"),
    ] {
        assert!(application::validate_endpoints(database, rabbit).is_err());
    }
    application::validate_endpoints("postgres://127.0.0.1/example", "amqp://127.0.0.1/").unwrap();
    application::validate_endpoints("postgres://[::1]/example", "amqp://[::1]/").unwrap();
    application::validate_endpoints(
        "postgresql:///example?host=/absolute/local/socket",
        "amqp://127.0.0.1/",
    )
    .unwrap();
    assert!(
        application::validate_endpoints(
            "postgresql:///example?host=192.0.2.1",
            "amqp://127.0.0.1/"
        )
        .is_err()
    );
}

#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires isolated local DATABASE_URL and numeric-loopback RADDLE_RABBITMQ_TEST_URI"]
async fn pipeline_schedule_saga_events_projection_and_confirmed_broker_compose(
    pool: PgPool,
) -> application::Result<()> {
    let uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")?;
    let summary = application::run(&pool, &uri).await?;
    assert_eq!(
        (summary.accepted, summary.rejected, summary.scheduled),
        (2, 1, 2)
    );
    assert_eq!(
        (summary.completed_jobs, summary.projected, summary.settled),
        (4, 2, 2)
    );
    assert_eq!(summary.event_ids.len(), 2);
    assert_eq!(summary.quarantined, 1);
    assert_ne!(summary.event_ids[0], summary.event_ids[1]);
    let totals: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
        (SELECT sum(units) FROM raddle_example_orders WHERE tenant=$1),
        (SELECT sum(units) FROM raddle_example_order_view WHERE tenant=$1),
        (SELECT sum(apply_count) FROM raddle_example_order_view WHERE tenant=$1),
        (SELECT count(*) FROM raddle_recurring WHERE tenant=$1 AND cancelled)",
    )
    .bind(&summary.tenant)
    .fetch_one(&pool)
    .await?;
    assert_eq!(totals, (10, 10, 2, 2));
    let rejected: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raddle_example_orders WHERE order_id=$1")
            .bind(summary.rejected_order)
            .fetch_one(&pool)
            .await?;
    assert_eq!(rejected, 0);
    let quarantined: Vec<u8> =
        sqlx::query_scalar("SELECT wire FROM raddle_example_quarantine WHERE tenant=$1")
            .bind(&summary.tenant)
            .fetch_one(&pool)
            .await?;
    assert_eq!(quarantined, b"application fixture: malformed envelope");
    Ok(())
}
