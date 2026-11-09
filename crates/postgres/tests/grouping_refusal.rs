//! Opt-in direct PostgreSQL staging/conversation/event admission refusal.
#![allow(clippy::unwrap_used)]

use chrono::{TimeDelta, Utc};
use raddle::grouping::{Dispatch, GroupId};
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::RequestOptions;
use raddle_postgres::events::{EventStream, ExpectedRevision};
use raddle_postgres::{Delivery, DeliveryKind, Error, Scope};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct Work;
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn direct_admission_refuses_before_any_custody_write_and_poisons_scope(pool: PgPool) {
    let original = Envelope::encode(&Work).unwrap();
    let request = original
        .clone()
        .with_header("reply-endpoint".into(), "replies".into())
        .unwrap();
    let allowed = [raddle::SubscriptionName::try_new("replies").unwrap()];
    let now = Utc::now();
    for group in [None, Some(GroupId::new("")), Some(GroupId::new("g"))] {
        let envelope = original
            .clone()
            .with_dispatch(Dispatch::resolved(group, None))
            .unwrap();
        for operation in 0..7 {
            let mut scope = Scope::enroll(pool.begin().await.unwrap());
            let delivery = Delivery::new(DeliveryKind::Local, "orders", now);
            let result = match operation {
                0 => scope
                    .stage("source", "tenant", &envelope, &[delivery])
                    .await
                    .map(|_| ()),
                1 => scope
                    .stage_received("source", "tenant", &envelope, &[delivery])
                    .await
                    .map(|_| ()),
                2 => scope
                    .stage_idempotent("source", "tenant", "key", &envelope, &[delivery])
                    .await
                    .map(|_| ()),
                3 => {
                    scope
                        .request(
                            "tenant",
                            &envelope,
                            &RequestOptions {
                                destination: "orders".into(),
                                reply_endpoint: "replies".into(),
                                responder: "peer".into(),
                                reply_contract: Work::NAME.into(),
                                deadline: now + TimeDelta::seconds(60),
                            },
                        )
                        .await
                }
                4 => {
                    scope
                        .reply("tenant", &request, envelope.clone(), &allowed, now)
                        .await
                }
                5 => scope
                    .accept_reply("tenant", "source", &envelope, now)
                    .await
                    .map(|_| ()),
                _ => scope
                    .append_events(
                        &EventStream::new("tenant", uuid::Uuid::new_v4(), "aggregate", 1).unwrap(),
                        ExpectedRevision::NoStream,
                        std::slice::from_ref(&envelope),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|_| Error::Invalid("event refusal")),
            };
            assert!(result.is_err());
            if operation == 4 {
                assert!(matches!(
                    result,
                    Err(Error::Wire(raddle::wire::WireError::Invalid(
                        "native dispatch is unsupported by this profile"
                    )))
                ));
            }
            for table in [
                "raddle_jobs",
                "raddle_idempotency",
                "raddle_conversations",
                "raddle_events",
            ] {
                assert_eq!(
                    sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                        .fetch_one(scope.connection())
                        .await
                        .unwrap(),
                    0
                );
            }
            assert!(matches!(scope.commit(now).await, Err(Error::RollbackOnly)));
        }
    }
    // The same authorized request and allowlist admit a legacy response.
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    scope
        .reply("tenant", &request, original, &allowed, now)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(scope.connection())
            .await
            .unwrap(),
        1
    );
    scope.commit(now).await.unwrap();
}
