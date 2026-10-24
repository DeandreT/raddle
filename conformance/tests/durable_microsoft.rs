//! Real Service Bus sockets and PostgreSQL transactions, including post-commit redelivery.

#[path = "support/microsoft.rs"]
mod fixture;

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use raddle::SubscriptionName;
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_postgres::{
    ConversationStatus, Delivery, DeliveryKind, HandlerFailure, JobStatus, OutboxWorker,
    ReplyDisposition, RequestOptions, RetryPolicy, Store, Worker,
};
use raddle_servicebus::{Connection, HeldMessage, Receiver, Source};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use fixture::TestResult;

const TENANT: &str = "test-tenant";
const SOURCE: &str = "trusted.servicebus";
const REQUESTS: &str = "durable-requests";
const REPLIES: &str = "durable-replies";

#[derive(Serialize, Deserialize)]
struct Request(u32);
impl WireMessage for Request {
    const NAME: &'static str = "test.durable-request";
    const VERSION: u32 = 1;
}

#[derive(Serialize, Deserialize)]
struct Reply(u32);
impl WireMessage for Reply {
    const NAME: &'static str = "test.durable-reply";
    const VERSION: u32 = 1;
}

async fn commit_custody(
    receiver: &mut Receiver,
    store: &Store,
    subscription: &str,
) -> TestResult<(HeldMessage, Uuid)> {
    let mut received = receiver.receive(1, Duration::from_secs(20)).await?;
    let held = received.pop().ok_or("missing broker delivery")?;
    let envelope = held.decode()?;
    let mut scope = store.begin().await?;
    let ids = scope
        .stage(
            SOURCE,
            TENANT,
            &envelope,
            &[Delivery::new(
                DeliveryKind::Local,
                subscription,
                envelope.created_at(),
            )],
        )
        .await?;
    let id = ids.first().copied().ok_or("missing inbox identity")?;
    scope.commit(Utc::now()).await?;
    // Returning the held delivery makes the commit-before-settlement order explicit.
    Ok((held, id))
}

async fn redeliver_after_commit(
    receiver: &mut Receiver,
    store: &Store,
    subscription: &str,
) -> TestResult<Uuid> {
    let (mut first, original_id) = commit_custody(receiver, store, subscription).await?;
    // Models durable commit followed by a lost completion: the broker redelivers.
    receiver.abandon(&mut first).await?;
    let (mut repeated, repeated_id) = commit_custody(receiver, store, subscription).await?;
    assert_eq!(repeated_id, original_id);
    receiver.complete(&mut repeated).await?;
    Ok(original_id)
}

fn worker(store: Store) -> TestResult<Worker> {
    let mut worker = Worker::new(store, RetryPolicy::default());
    worker.handle::<Request, _>(REQUESTS, Codecs::default(), |request, scope| {
        Box::pin(async move {
            if scope.tenant() != Some(TENANT) {
                return Err(HandlerFailure::Reject);
            }
            let original = scope
                .envelope()
                .map_err(|_| HandlerFailure::Reject)?
                .ok_or(HandlerFailure::Reject)?;
            sqlx::query("INSERT INTO processed_requests(id,value) VALUES($1,$2)")
                .bind(original.id())
                .bind(i64::from(request.0))
                .execute(scope.connection())
                .await
                .map_err(|_| HandlerFailure::Retry)?;
            let value = request.0.checked_mul(2).ok_or(HandlerFailure::Reject)?;
            let reply = Envelope::encode(&Reply(value)).map_err(|_| HandlerFailure::Reject)?;
            let destination =
                SubscriptionName::try_new(REPLIES).map_err(|_| HandlerFailure::Reject)?;
            scope
                .reply(TENANT, &original, reply, &[destination], Utc::now())
                .await
                .map_err(|_| HandlerFailure::Retry)
        })
    })?;
    worker.handle::<Reply, _>(REPLIES, Codecs::default(), |_, scope| {
        Box::pin(async move {
            if scope.tenant() != Some(TENANT) {
                return Err(HandlerFailure::Reject);
            }
            let reply = scope
                .envelope()
                .map_err(|_| HandlerFailure::Reject)?
                .ok_or(HandlerFailure::Reject)?;
            match scope.accept_reply(TENANT, SOURCE, &reply, Utc::now()).await {
                Ok(ReplyDisposition::Accepted) => Ok(()),
                _ => Err(HandlerFailure::Reject),
            }
        })
    })?;
    Ok(worker)
}

async fn exchange(connection: &Connection, pool: PgPool) -> TestResult<Uuid> {
    let store = Store::new(pool);
    let publisher = connection
        .publisher([
            (REQUESTS.into(), REQUESTS.into()),
            (REPLIES.into(), REPLIES.into()),
        ])
        .await?;
    let outbox = OutboxWorker::new(store.clone(), publisher, RetryPolicy::default());
    let result = async {
        let request = Envelope::encode(&Request(21))?;
        let mut scope = store.begin().await?;
        scope
            .request(
                TENANT,
                &request,
                &RequestOptions {
                    destination: REQUESTS.into(),
                    reply_endpoint: REPLIES.into(),
                    responder: SOURCE.into(),
                    reply_contract: Reply::NAME.into(),
                    deadline: Utc::now() + TimeDelta::minutes(10),
                },
            )
            .await?;
        scope.commit(Utc::now()).await?;
        assert_eq!(outbox.run_once(4).await?, 1);

        let mut requests = connection
            .receiver(Source::Queue(REQUESTS.into()), None, 0)
            .await?;
        let request_job = redeliver_after_commit(&mut requests, &store, REQUESTS).await?;
        requests.close().await?;
        let execution = worker(store.clone())?;
        assert_eq!(execution.run_once(4).await?, 1);
        assert_eq!(
            store
                .inspect(TENANT, request_job)
                .await?
                .ok_or("missing request job")?
                .status,
            JobStatus::Completed
        );
        assert_eq!(outbox.run_once(4).await?, 1);

        let mut replies = connection
            .receiver(Source::Queue(REPLIES.into()), None, 0)
            .await?;
        let reply_job = redeliver_after_commit(&mut replies, &store, REPLIES).await?;
        replies.close().await?;
        assert_eq!(execution.run_once(4).await?, 1);
        assert_eq!(execution.run_once(4).await?, 0);
        assert_eq!(outbox.run_once(4).await?, 0);
        assert_eq!(
            store
                .inspect(TENANT, reply_job)
                .await?
                .ok_or("missing reply job")?
                .status,
            JobStatus::Completed
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(request.id())
    }
    .await;
    let closed = outbox.into_transport().close().await;
    let request_id = result?;
    closed?;
    Ok(request_id)
}

#[sqlx::test(migrations = "../crates/raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL and the official Microsoft emulator fixture"]
async fn durable_request_reply_survives_post_commit_redelivery(pool: PgPool) -> TestResult {
    sqlx::query("CREATE TABLE processed_requests(id UUID PRIMARY KEY,value BIGINT NOT NULL)")
        .execute(&pool)
        .await?;
    let endpoint = fixture::endpoint()?;
    let connection = Connection::emulator(&fixture::connection_string(endpoint)?, endpoint).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(240),
        exchange(&connection, pool.clone()),
    )
    .await;
    let closed = connection.close().await;
    let request_id = result??;
    closed?;

    // No worker or transport state survives: inspect the result through a fresh store handle.
    let recovered = Store::new(pool.clone());
    let conversation = recovered
        .conversation(TENANT, request_id, Utc::now())
        .await?
        .ok_or("missing durable conversation")?;
    assert_eq!(conversation.status, ConversationStatus::Completed);
    assert_eq!(
        conversation
            .reply
            .ok_or("missing durable reply")?
            .decode::<Reply>()?
            .0,
        42
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM processed_requests")
            .fetch_one(&pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT value FROM processed_requests")
            .fetch_one(&pool)
            .await?,
        21
    );
    Ok(())
}
