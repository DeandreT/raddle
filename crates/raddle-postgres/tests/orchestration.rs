//! Versioned saga and conversation behavior on a disposable database.
#![allow(clippy::unwrap_used)]

use chrono::{TimeDelta, Utc};
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{
    ConversationStatus, ReplyDisposition, RequestOptions, SagaState, SagaTimeout, Store,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct State {
    count: u32,
}
impl SagaState for State {
    const NAME: &'static str = "orders.saga";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct NextState {
    count: u32,
    ready: bool,
}
impl SagaState for NextState {
    const NAME: &'static str = "orders.saga";
    const VERSION: u32 = 2;
}
#[derive(Serialize, Deserialize)]
struct Request(u32);
impl WireMessage for Request {
    const NAME: &'static str = "orders.request";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Reply(u32);
impl WireMessage for Reply {
    const NAME: &'static str = "orders.reply";
    const VERSION: u32 = 1;
}

fn routing() -> RequestOptions {
    RequestOptions {
        destination: "orders".into(),
        reply_endpoint: "replies".into(),
        responder: "orders".into(),
        reply_contract: Reply::NAME.into(),
        deadline: Utc::now() + TimeDelta::minutes(1),
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn saga_state_and_timeout_generation_are_atomic(pool: PgPool) {
    let store = Store::new(pool);
    let now = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let mut saga = scope
        .saga("tenant", "order-1", || State { count: 0 })
        .await
        .unwrap();
    saga.state_mut().count = 1;
    scope
        .schedule_saga_timeout(&mut saga, "timeouts", now + TimeDelta::seconds(5))
        .await
        .unwrap();
    let first = SagaTimeout {
        kind: State::NAME.into(),
        correlation: "order-1".into(),
        generation: 1,
    };
    assert!(!saga.accepts_timeout(&first, now));
    assert!(saga.accepts_timeout(&first, now + TimeDelta::seconds(6)));
    scope
        .schedule_saga_timeout(&mut saga, "timeouts", now + TimeDelta::seconds(10))
        .await
        .unwrap();
    assert!(!saga.accepts_timeout(&first, now + TimeDelta::seconds(20)));
    scope.commit(now).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    let mut loaded = scope
        .saga("tenant", "order-1", || State { count: 99 })
        .await
        .unwrap();
    assert_eq!(loaded.state().count, 1);
    loaded.finish();
    scope.save_saga(&mut loaded).await.unwrap();
    scope.commit(now).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    let loaded = scope
        .saga("tenant", "order-1", || State { count: 99 })
        .await
        .unwrap();
    assert!(loaded.is_completed());
    assert!(!loaded.accepts_timeout(&first, now + TimeDelta::seconds(20)));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn rollback_discards_saga_changes_and_its_timer(pool: PgPool) {
    let store = Store::new(pool.clone());
    let mut scope = store.begin().await.unwrap();
    let mut saga = scope
        .saga("tenant", "order", || State { count: 42 })
        .await
        .unwrap();
    scope
        .schedule_saga_timeout(&mut saga, "timeouts", Utc::now())
        .await
        .unwrap();
    scope.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_sagas")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn concurrent_saga_updates_do_not_lose_changes(pool: PgPool) {
    let store = Store::new(pool);
    async fn increment(store: &Store) {
        let mut scope = store.begin().await.unwrap();
        let mut saga = scope
            .saga("tenant", "order", || State { count: 0 })
            .await
            .unwrap();
        saga.state_mut().count += 1;
        scope.save_saga(&mut saga).await.unwrap();
        scope.commit(Utc::now()).await.unwrap();
    }
    tokio::join!(increment(&store), increment(&store));
    let mut scope = store.begin().await.unwrap();
    assert_eq!(
        scope
            .saga("tenant", "order", || State { count: 0 })
            .await
            .unwrap()
            .state()
            .count,
        2
    );
    assert_eq!(
        scope
            .saga("other", "order", || State { count: 0 })
            .await
            .unwrap()
            .state()
            .count,
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn schema_evolution_requires_an_explicit_migration(pool: PgPool) {
    let store = Store::new(pool);
    let mut scope = store.begin().await.unwrap();
    scope
        .saga("tenant", "order", || State { count: 3 })
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    assert!(
        scope
            .saga("tenant", "order", || NextState {
                count: 0,
                ready: false
            })
            .await
            .is_err()
    );
    scope.rollback().await.unwrap();
    let mut scope = store.begin().await.unwrap();
    scope
        .migrate_saga::<NextState>("tenant", "order", 1, |value| {
            Ok(NextState {
                count: value["count"].as_u64().unwrap() as u32,
                ready: true,
            })
        })
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    assert!(
        scope
            .saga("tenant", "order", || NextState {
                count: 0,
                ready: false
            })
            .await
            .unwrap()
            .state()
            .ready
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn replies_are_tenant_source_and_contract_scoped(pool: PgPool) {
    let store = Store::new(pool);
    let request = Envelope::encode(&Request(1)).unwrap();
    let mut scope = store.begin().await.unwrap();
    scope.request("tenant", &request, &routing()).await.unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let reply = Envelope::encode(&Reply(42))
        .unwrap()
        .correlated(request.id(), request.id());
    let mut scope = store.begin().await.unwrap();
    assert_eq!(
        scope
            .accept_reply("other", "orders", &reply, Utc::now())
            .await
            .unwrap(),
        ReplyDisposition::Ignored
    );
    assert_eq!(
        scope
            .accept_reply("tenant", "forged", &reply, Utc::now())
            .await
            .unwrap(),
        ReplyDisposition::Rejected
    );
    assert_eq!(
        scope
            .accept_reply("tenant", "orders", &reply, Utc::now())
            .await
            .unwrap(),
        ReplyDisposition::Accepted
    );
    assert_eq!(
        scope
            .accept_reply("tenant", "orders", &reply, Utc::now())
            .await
            .unwrap(),
        ReplyDisposition::Ignored
    );
    scope.commit(Utc::now()).await.unwrap();
    let loaded = store
        .conversation("tenant", request.id(), Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status, ConversationStatus::Completed);
    assert_eq!(loaded.reply.unwrap().decode::<Reply>().unwrap().0, 42);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelled_and_expired_conversations_ignore_late_replies(pool: PgPool) {
    let store = Store::new(pool);
    let request = Envelope::encode(&Request(1)).unwrap();
    let options = routing();
    let mut scope = store.begin().await.unwrap();
    scope.request("tenant", &request, &options).await.unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    assert!(
        scope
            .cancel_request("tenant", request.id(), Utc::now())
            .await
            .unwrap()
    );
    let reply = Envelope::encode(&Reply(42))
        .unwrap()
        .correlated(request.id(), request.id());
    assert_eq!(
        scope
            .accept_reply("tenant", "orders", &reply, Utc::now())
            .await
            .unwrap(),
        ReplyDisposition::Ignored
    );
    scope.commit(Utc::now()).await.unwrap();
    assert_eq!(
        store
            .conversation("tenant", request.id(), Utc::now())
            .await
            .unwrap()
            .unwrap()
            .status,
        ConversationStatus::Cancelled
    );
    let other = Envelope::encode(&Request(2)).unwrap();
    let claims = store
        .claim(
            raddle_postgres::DeliveryKind::Remote,
            8,
            std::time::Duration::from_secs(30),
            Utc::now(),
        )
        .await
        .unwrap();
    let cancellation = claims
        .iter()
        .map(|claim| claim.envelope().unwrap())
        .find(|envelope| envelope.contract() == "raddle.cancel")
        .unwrap();
    assert_eq!(
        cancellation
            .decode::<raddle_postgres::Cancellation>()
            .unwrap()
            .request_id,
        request.id()
    );
    let mut scope = store.begin().await.unwrap();
    scope.request("tenant", &other, &options).await.unwrap();
    scope.commit(Utc::now()).await.unwrap();
    assert_eq!(
        store
            .conversation(
                "tenant",
                other.id(),
                options.deadline + TimeDelta::seconds(1)
            )
            .await
            .unwrap()
            .unwrap()
            .status,
        ConversationStatus::TimedOut
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn untrusted_reply_destinations_cannot_stage_messages(pool: PgPool) {
    let store = Store::new(pool.clone());
    let request = Envelope::encode(&Request(1))
        .unwrap()
        .with_header("reply-endpoint".into(), "other".into())
        .unwrap();
    let mut scope = store.begin().await.unwrap();
    assert!(
        scope
            .reply(
                "tenant",
                &request,
                Envelope::encode(&Reply(1)).unwrap(),
                &["replies".parse().unwrap()],
                Utc::now()
            )
            .await
            .is_err()
    );
    assert!(scope.commit(Utc::now()).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn completed_sagas_cannot_be_mutated_or_restarted(pool: PgPool) {
    let store = Store::new(pool);
    let mut scope = store.begin().await.unwrap();
    let mut saga = scope
        .saga("tenant", "finished", || State { count: 1 })
        .await
        .unwrap();
    saga.finish();
    scope.save_saga(&mut saga).await.unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    let mut saga = scope
        .saga("tenant", "finished", || State { count: 99 })
        .await
        .unwrap();
    saga.state_mut().count = 500;
    assert!(scope.save_saga(&mut saga).await.is_err());
    assert!(scope.commit(Utc::now()).await.is_err());
    let mut scope = store.begin().await.unwrap();
    assert_eq!(
        scope
            .saga("tenant", "finished", || State { count: 99 })
            .await
            .unwrap()
            .state()
            .count,
        1
    );
}
