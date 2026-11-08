//! Migration compatibility with already persisted legacy job states.
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{JobStatus, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
struct Failed {
    private: String,
}

impl WireMessage for Failed {
    const NAME: &'static str = "migration.legacy";
    const VERSION: u32 = 1;
}

#[sqlx::test(migrations = false)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn migration_upgrades_populated_four_state_schema_without_reclassifying_success(
    pool: PgPool,
) {
    let mut initial = sqlx::migrate!();
    initial.migrations = std::borrow::Cow::Owned(initial.iter().take(1).cloned().collect());
    initial.run(&pool).await.unwrap();
    let mut ids = Vec::new();
    for status in ["ready", "active", "completed", "dead"] {
        let id = Uuid::new_v4();
        let envelope = Envelope::encode(&Failed {
            private: format!("legacy-{status}"),
        })
        .unwrap();
        sqlx::query("INSERT INTO raddle_jobs(id,source,tenant,destination,kind,message_id,envelope,fingerprint,status,available_at,max_attempts) VALUES($1,'legacy','tenant','legacy','local',$2,$3,$4,$5,clock_timestamp(),5)")
            .bind(id).bind(envelope.id()).bind(envelope.to_vec().unwrap()).bind(vec![0u8;32]).bind(status)
            .execute(&pool).await.unwrap();
        ids.push(id);
    }
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    for (id, expected) in ids.iter().zip([
        JobStatus::Ready,
        JobStatus::Active,
        JobStatus::Completed,
        JobStatus::Dead,
    ]) {
        assert_eq!(
            store.inspect("tenant", *id).await.unwrap().unwrap().status,
            expected
        );
    }
    assert!(
        store
            .discard_dead_letter(
                "tenant",
                ids[3],
                "operator",
                "retire legacy dead delivery",
                Utc::now()
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .inspect("tenant", ids[3])
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Discarded
    );
    assert_eq!(store.statistics("tenant").await.unwrap().completed, 1);
}
