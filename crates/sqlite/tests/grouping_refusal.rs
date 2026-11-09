//! Real SQLite lower-level dispatch refusal and rollback-only custody.
#![allow(clippy::unwrap_used)]
use chrono::Utc;
use raddle::grouping::{Dispatch, GroupId};
use raddle::wire::{Envelope, WireMessage};
use raddle_sqlite::{Delivery, DeliveryKind, Error, Store};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Work;
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}

#[tokio::test]
async fn direct_staging_refuses_dispatch_before_job_write_and_poisons_scope() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path().join("grouping.sqlite"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let legacy = Envelope::encode(&Work).unwrap();
    for group in [None, Some(GroupId::new("")), Some(GroupId::new("g"))] {
        let envelope = legacy
            .clone()
            .with_dispatch(Dispatch::resolved(group, None))
            .unwrap();
        let mut scope = store.begin().await.unwrap();
        assert!(matches!(
            scope
                .stage(
                    "source",
                    "tenant",
                    &envelope,
                    &[Delivery::new(DeliveryKind::Local, "orders", Utc::now())]
                )
                .await,
            Err(Error::Wire(_))
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
                .fetch_one(scope.connection())
                .await
                .unwrap(),
            0
        );
        assert!(matches!(
            scope.commit(Utc::now()).await,
            Err(Error::RollbackOnly)
        ));
    }
    let mut scope = store.begin().await.unwrap();
    let now = Utc::now();
    assert!(
        scope
            .stage(
                "source",
                "tenant",
                &legacy,
                &[Delivery::new(DeliveryKind::Local, "orders", now)]
            )
            .await
            .is_ok()
    );
    scope.commit(now).await.unwrap();
    store.close().await;
}
