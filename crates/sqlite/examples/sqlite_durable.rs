//! Persist one credit, reopen the database, and apply it once across repeated runs.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_sqlite::{Delivery, DeliveryKind, FailureDisposition, Store, Worker};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const TENANT: &str = "example";
const SUBSCRIPTION: &str = "example.credit";

#[derive(Serialize, Deserialize)]
struct Credit {
    amount: i64,
}
impl WireMessage for Credit {
    const NAME: &'static str = "example.credit";
    const VERSION: u32 = 1;
}

struct Report {
    job_id: Uuid,
    balance: i64,
    processed: bool,
}

async fn seed(store: &Store) -> Result<Uuid, Box<dyn Error>> {
    store.migrate().await?;
    let mut scope = store.begin().await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS raddle_example_balances(name TEXT PRIMARY KEY, balance INTEGER NOT NULL)")
        .execute(scope.connection()).await?;
    sqlx::query("INSERT INTO raddle_example_balances(name,balance) VALUES('account',0) ON CONFLICT(name) DO NOTHING")
        .execute(scope.connection()).await?;
    // A redelivery retains identity, creation time, and the resolved delivery snapshot.
    let envelope = Envelope::with_identity(
        Uuid::from_u128(1),
        DateTime::<Utc>::UNIX_EPOCH,
        &Credit { amount: 25 },
    )?;
    let ids = scope
        .stage(
            "example.sqlite",
            TENANT,
            &envelope,
            &[Delivery::new(
                DeliveryKind::Local,
                SUBSCRIPTION,
                envelope.created_at(),
            )],
        )
        .await?;
    let id = ids.first().copied().ok_or("no delivery was staged")?;
    scope.commit(Utc::now()).await?;
    Ok(id)
}

async fn consume(store: &Store, job_id: Uuid) -> Result<Report, Box<dyn Error>> {
    let mut worker = Worker::try_new(store.clone(), Duration::from_secs(10))?;
    worker.handle::<Credit, _>(SUBSCRIPTION, Codecs::default(), |credit, scope| {
        Box::pin(async move {
            if scope.tenant() != Some(TENANT) || credit.amount <= 0 {
                return Err(FailureDisposition::Reject);
            }
            let changed = sqlx::query(
                "UPDATE raddle_example_balances SET balance=balance+? WHERE name='account'",
            )
            .bind(credit.amount)
            .execute(scope.connection())
            .await
            .map_err(|_| FailureDisposition::Retry)?;
            if changed.rows_affected() != 1 {
                return Err(FailureDisposition::Reject);
            }
            Ok(())
        })
    })?;
    let processed = worker.run_once().await?;
    let job = store.inspect(TENANT, job_id).await?.ok_or("missing job")?;
    if job.status != "completed" {
        return Err("credit did not complete; inspect the retained job before retrying".into());
    }
    let mut scope = store.begin().await?;
    let balance =
        sqlx::query_scalar("SELECT balance FROM raddle_example_balances WHERE name='account'")
            .fetch_one(scope.connection())
            .await?;
    scope.rollback().await?;
    Ok(Report {
        job_id,
        balance,
        processed,
    })
}

async fn run(path: &Path) -> Result<Report, Box<dyn Error>> {
    let store = Store::open(path).await?;
    let staged = seed(&store).await;
    store.close().await;
    let job_id = staged?;

    // No in-memory queue or connection survives this boundary.
    let reopened = Store::open(path).await?;
    let result = consume(&reopened, job_id).await;
    reopened.close().await;
    result
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let path = PathBuf::from(
        arguments
            .next()
            .ok_or("usage: sqlite_durable <isolated-example.sqlite>")?,
    );
    if arguments.next().is_some() {
        return Err("usage: sqlite_durable <isolated-example.sqlite>".into());
    }
    let report = run(&path).await?;
    println!(
        "job={} balance={} processed={}",
        report.job_id, report.balance, report.processed
    );
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::run;

    #[tokio::test]
    async fn reopening_and_resubmitting_do_not_repeat_the_credit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("example.sqlite");
        let first = run(&path).await.unwrap();
        assert!(first.processed);
        assert_eq!(first.balance, 25);
        let second = run(&path).await.unwrap();
        assert!(!second.processed);
        assert_eq!(second.balance, 25);
        assert_eq!(second.job_id, first.job_id);
    }
}
