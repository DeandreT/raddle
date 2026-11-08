//! A real PostgreSQL/RabbitMQ application, restricted to disposable loopback services.

#[path = "support/application.rs"]
mod application;

#[tokio::main(flavor = "current_thread")]
async fn main() -> application::Result<()> {
    if std::env::var("RADDLE_APPLICATION_ALLOW_DISPOSABLE_SERVICES").as_deref() != Ok("yes") {
        return Err("set RADDLE_APPLICATION_ALLOW_DISPOSABLE_SERVICES=yes for isolated local PostgreSQL and RabbitMQ".into());
    }
    let database_url = std::env::var("DATABASE_URL")?;
    let rabbit_uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")?;
    // Validate before opening either connection or applying migrations.
    application::validate_endpoints(&database_url, &rabbit_uri)?;
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let result: application::Result<()> = async {
        raddle_postgres::Store::new(pool.clone()).migrate().await?;
        let summary = application::run(&pool, &rabbit_uri).await?;
        println!("{}: {} accepted, {} rejected, {} scheduled, {} jobs completed, {} projected, {} broker settlements",
            summary.tenant, summary.accepted, summary.rejected, summary.scheduled,
            summary.completed_jobs, summary.projected, summary.settled);
        println!("{} malformed fixture delivery durably quarantined", summary.quarantined);
        println!("stable event identities: {:?}; rejected order: {}", summary.event_ids, summary.rejected_order);
        Ok(())
    }.await;
    let closed = tokio::time::timeout(std::time::Duration::from_secs(2), pool.close()).await;
    result?;
    closed.map_err(|_| "example PostgreSQL pool close deadline expired")?;
    Ok(())
}
