//! Complete a purchase saga and its durable receipt using an isolated PostgreSQL database.

use std::error::Error;

use chrono::{DateTime, Utc};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_postgres::{
    Delivery, DeliveryKind, HandlerFailure, JobStatus, RetryPolicy, SagaState, Store, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const TENANT: &str = "example";
const ORDERS: &str = "example.orders";
const RECEIPTS: &str = "example.receipts";

#[derive(Serialize, Deserialize)]
struct PlaceOrder {
    order_id: Uuid,
    total: i64,
}
impl WireMessage for PlaceOrder {
    const NAME: &'static str = "example.place-order";
    const VERSION: u32 = 1;
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    order_id: Uuid,
}
impl WireMessage for Receipt {
    const NAME: &'static str = "example.receipt";
    const VERSION: u32 = 1;
}

#[derive(Serialize, Deserialize)]
struct Purchase {
    total: i64,
}
impl SagaState for Purchase {
    const NAME: &'static str = "example.purchase";
    const VERSION: u32 = 1;
}

struct Report {
    job_id: Uuid,
    total: i64,
    receipt_recorded: bool,
    processed: usize,
}

fn require_isolated(confirmation: Option<&str>) -> Result<(), Box<dyn Error>> {
    if confirmation != Some("isolated") {
        return Err(
            "set RADDLE_EXAMPLE_CONFIRM=isolated only for a dedicated example database".into(),
        );
    }
    Ok(())
}

async fn run(pool: PgPool) -> Result<Report, Box<dyn Error>> {
    let store = Store::new(pool);
    store.migrate().await?;
    let order_id = Uuid::from_u128(1001);
    let order = Envelope::with_identity(
        Uuid::from_u128(1),
        DateTime::<Utc>::UNIX_EPOCH,
        &PlaceOrder {
            order_id,
            total: 25,
        },
    )?;
    let mut scope = store.begin().await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS raddle_example_orders(id UUID PRIMARY KEY,total BIGINT NOT NULL,receipt_recorded BOOLEAN NOT NULL DEFAULT false)")
        .execute(scope.connection()).await?;
    let ids = scope
        .stage(
            "example.postgres",
            TENANT,
            &order,
            &[Delivery::new(
                DeliveryKind::Local,
                ORDERS,
                order.created_at(),
            )],
        )
        .await?;
    let job_id = ids.first().copied().ok_or("no order was staged")?;
    scope.commit(Utc::now()).await?;

    let mut worker = Worker::new(store.clone(), RetryPolicy::default());
    worker.handle::<PlaceOrder, _>(ORDERS, Codecs::default(), |order, scope| {
        Box::pin(async move {
            if scope.tenant() != Some(TENANT) || order.total <= 0 {
                return Err(HandlerFailure::Reject);
            }
            let mut purchase = scope
                .saga::<Purchase>(TENANT, &order.order_id.to_string(), || Purchase {
                    total: 0,
                })
                .await
                .map_err(|_| HandlerFailure::Retry)?;
            if purchase.is_completed() {
                return Ok(());
            }
            sqlx::query("INSERT INTO raddle_example_orders(id,total) VALUES($1,$2)")
                .bind(order.order_id)
                .bind(order.total)
                .execute(scope.connection())
                .await
                .map_err(|_| HandlerFailure::Retry)?;
            purchase.state_mut().total = order.total;
            purchase.finish();
            scope
                .save_saga(&mut purchase)
                .await
                .map_err(|_| HandlerFailure::Retry)?;
            let incoming = scope
                .envelope()
                .map_err(|_| HandlerFailure::Reject)?
                .ok_or(HandlerFailure::Reject)?;
            let receipt = Envelope::with_identity(
                Uuid::from_u128(2),
                DateTime::<Utc>::UNIX_EPOCH,
                &Receipt {
                    order_id: order.order_id,
                },
            )
            .and_then(|envelope| envelope.correlated(incoming.id(), incoming.id()))
            .map_err(|_| HandlerFailure::Reject)?;
            scope
                .stage(
                    "example.postgres",
                    TENANT,
                    &receipt,
                    &[Delivery::new(
                        DeliveryKind::Local,
                        RECEIPTS,
                        receipt.created_at(),
                    )],
                )
                .await
                .map_err(|_| HandlerFailure::Retry)?;
            Ok(())
        })
    })?;
    worker.handle::<Receipt, _>(RECEIPTS, Codecs::default(), |receipt, scope| {
        Box::pin(async move {
            if scope.tenant() != Some(TENANT) {
                return Err(HandlerFailure::Reject);
            }
            let changed =
                sqlx::query("UPDATE raddle_example_orders SET receipt_recorded=true WHERE id=$1")
                    .bind(receipt.order_id)
                    .execute(scope.connection())
                    .await
                    .map_err(|_| HandlerFailure::Retry)?;
            if changed.rows_affected() != 1 {
                return Err(HandlerFailure::Reject);
            }
            Ok(())
        })
    })?;
    // This dedicated database contains exactly the order and its follow-up.
    let processed = worker.run_once(1).await? + worker.run_once(1).await?;
    if store
        .inspect(TENANT, job_id)
        .await?
        .ok_or("missing order job")?
        .status
        != JobStatus::Completed
    {
        return Err("order did not complete; inspect the retained job before retrying".into());
    }
    let mut scope = store.begin().await?;
    let (total, receipt_recorded): (i64, bool) =
        sqlx::query_as("SELECT total,receipt_recorded FROM raddle_example_orders WHERE id=$1")
            .bind(order_id)
            .fetch_one(scope.connection())
            .await?;
    let purchase = scope
        .saga::<Purchase>(TENANT, &order_id.to_string(), || Purchase { total: 0 })
        .await?;
    if !purchase.is_completed() || purchase.state().total != total || !receipt_recorded {
        return Err("purchase or receipt is incomplete".into());
    }
    scope.rollback().await?;
    Ok(Report {
        job_id,
        total,
        receipt_recorded,
        processed,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    require_isolated(std::env::var("RADDLE_EXAMPLE_CONFIRM").ok().as_deref())?;
    let url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await?;
    let result = run(pool.clone()).await;
    pool.close().await;
    let report = result?;
    println!(
        "job={} total={} receipt={} processed={}",
        report.job_id, report.total, report.receipt_recorded, report.processed
    );
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use sqlx::PgPool;

    use super::{require_isolated, run};

    #[test]
    fn command_requires_explicit_isolation_confirmation() {
        assert!(require_isolated(None).is_err());
        assert!(require_isolated(Some("production")).is_err());
        assert!(require_isolated(Some("isolated")).is_ok());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
    async fn repeated_submission_retains_one_purchase_and_receipt(pool: PgPool) {
        let first = run(pool.clone()).await.unwrap();
        assert_eq!(first.processed, 2);
        assert_eq!(first.total, 25);
        assert!(first.receipt_recorded);
        let second = run(pool).await.unwrap();
        assert_eq!(second.processed, 0);
        assert_eq!(second.total, 25);
        assert!(second.receipt_recorded);
        assert_eq!(second.job_id, first.job_id);
    }
}
