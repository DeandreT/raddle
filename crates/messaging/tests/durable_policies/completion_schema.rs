//! A shared completion-query defect must not masquerade as isolated uncertainty.
use super::{TypedFailure, Work, schema, stage};
use chrono::Utc;
use raddle::wire::Envelope;
use raddle_host::Host;
use raddle_messaging::effects::HandlerOutput;
use raddle_messaging::scoped::{Failure, LocalOnlyFlush, ScopedHandler};
use raddle_messaging::{
    BoxFuture, DurableEndpoint, EndpointPolicies, MessageContext, PolicyQueueBuilder, TrustedOrigin,
};
use raddle_policies::{MemoryStatus, PolicySet, ReplaySafety};
use raddle_postgres::endpoint_policies::PolicyRecovery;
use raddle_postgres::{JobStatus, RetryPolicy, Scope, Store};
use sqlx::PgPool;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Default)]
struct Handler {
    calls: AtomicUsize,
    disposed: AtomicUsize,
    errors: Mutex<Vec<(bool, String)>>,
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn local_shared_commit_schema_failure_is_not_suppressed_as_uncertainty(pool: PgPool) {
    schema(&pool).await;
    // The standalone local scope has no inbox completion UPDATE. Its actual
    // deferred COMMIT hook exercises the same shared schema/query classification.
    sqlx::raw_sql("CREATE FUNCTION check_completion_schema() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM completed_at FROM raddle_jobs LIMIT 1; RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER check_completion_schema AFTER INSERT ON business DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_completion_schema();").execute(&pool).await.unwrap();
    let handler = Arc::new(Handler::default());
    let mut builder = PolicyQueueBuilder::new(
        "local-schema",
        8,
        Duration::from_secs(30),
        EndpointPolicies::new(PolicySet::default()),
    )
    .unwrap();
    builder
        .scoped::<Work, _>(
            Arc::clone(&handler),
            Store::new(pool.clone()),
            Arc::new(LocalOnlyFlush),
            ReplaySafety::Transactional,
        )
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    let input = |i| {
        MessageContext::new(
            TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
            Envelope::encode(&Work(i)).unwrap(),
            Utc::now(),
        )
    };
    queue.submit(input(0)).await.unwrap();
    let mut host = Host::new(Duration::from_secs(10)).unwrap();
    runner.attach(&mut host, "local-schema").unwrap();
    let task = tokio::spawn(host.run());
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if queue.deliveries().unwrap().len() == 1
                && queue.deliveries().unwrap()[0].status == MemoryStatus::Completed
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    sqlx::query("ALTER TABLE raddle_jobs DROP COLUMN completed_at")
        .execute(&pool)
        .await
        .unwrap();
    queue.submit(input(1)).await.unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(
        stopped.is_ok(),
        "shared local COMMIT schema failure was hidden as isolated uncertainty"
    );
    assert!(stopped.unwrap().unwrap().is_err());
    assert_eq!(*handler.errors.lock().await, [(false, "42703".to_owned())]);
    assert_eq!(handler.disposed.load(Ordering::SeqCst), 2);
    let states = queue.deliveries().unwrap();
    assert_eq!(states[0].status, MemoryStatus::Completed);
    assert_eq!(states[1].status, MemoryStatus::Interrupted);
    sqlx::query("ALTER TABLE raddle_jobs ADD COLUMN completed_at timestamptz")
        .execute(&pool)
        .await
        .unwrap();
    let rows: Vec<i32> = sqlx::query_scalar("SELECT id FROM business ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows, [0]);
}
impl ScopedHandler<Work> for Handler {
    type Resources = ();
    type Reply = ();
    type Error = TypedFailure;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        work: &'a Work,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<(), ()>, TypedFailure>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            sqlx::query("INSERT INTO business(id) VALUES($1)")
                .bind(work.0)
                .execute(scope.connection())
                .await
                .map_err(|_| TypedFailure)?;
            Ok(HandlerOutput::reply(()))
        })
    }
    fn on_exception<'a>(
        &'a self,
        error: &'a Failure<TypedFailure>,
        _: &'a mut (),
        _: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let (uncertain, error) = match error {
                Failure::Database(error) => (false, error),
                Failure::UncertainDatabase(error) => (true, error),
                _ => panic!("expected a typed completion database error"),
            };
            let raddle_postgres::Error::Database(error) = error else {
                panic!("expected PostgreSQL error");
            };
            let code = error
                .as_database_error()
                .and_then(|error| error.code())
                .unwrap()
                .into_owned();
            self.errors.lock().await.push((uncertain, code));
        })
    }
    fn dispose<'a>(
        &'a self,
        _: (),
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), TypedFailure>> {
        Box::pin(async move {
            self.disposed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}
fn make_endpoint(store: Store, handler: Arc<Handler>) -> DurableEndpoint {
    let mut endpoint =
        DurableEndpoint::new(store, "completion-schema", 1, RetryPolicy::default()).unwrap();
    endpoint
        .policies(EndpointPolicies::new(PolicySet::default()))
        .unwrap();
    endpoint
        .handle_policy::<Work, _>(
            handler,
            Arc::new(LocalOnlyFlush),
            ReplaySafety::Transactional,
        )
        .unwrap();
    endpoint
}
async fn wait_completed(store: &Store, id: uuid::Uuid, handler: &Handler, disposed: usize) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if store.inspect("tenant", id).await.unwrap().unwrap().status == JobStatus::Completed
                && handler.disposed.load(Ordering::SeqCst) == disposed
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn missing_completion_column_stops_never_host_and_recovery_requires_restoration_and_audit(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let handler = Arc::new(Handler::default());
    let completed = stage(&store, "completion-schema", 0).await;
    let endpoint = make_endpoint(store.clone(), Arc::clone(&handler));
    let control = endpoint.control();
    let mut host = Host::new(Duration::from_secs(10)).unwrap();
    endpoint
        .attach(&mut host, Duration::from_millis(5))
        .unwrap();
    let stop = host.handle();
    let task = tokio::spawn(host.run());
    wait_completed(&store, completed, &handler, 1).await;
    let prior_completion: chrono::DateTime<Utc> =
        sqlx::query_scalar("SELECT completed_at FROM raddle_jobs WHERE id=$1")
            .bind(completed)
            .fetch_one(&pool)
            .await
            .unwrap();
    control.pause();
    let before = stage(&store, "completion-schema", 1).await;
    // DDL is confined to this SQLx-created disposable database, after the
    // previous root and its transaction/cleanup have finished. Never shared DDL.
    sqlx::query("ALTER TABLE raddle_jobs DROP COLUMN completed_at")
        .execute(&pool)
        .await
        .unwrap();
    let after = stage(&store, "completion-schema", 2).await; // valid admission still works
    control.resume();
    let stopped = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(
        stopped.is_ok(),
        "shared completion schema failure was hidden as per-item uncertainty; NEVER Host stayed healthy"
    );
    assert!(stopped.unwrap().unwrap().is_err());
    assert_eq!(*handler.errors.lock().await, [(false, "42703".to_owned())]);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
    assert_eq!(handler.disposed.load(Ordering::SeqCst), 2);
    let state: (String,bool,i64)=sqlx::query_as("SELECT j.status,p.uncertain,p.handling FROM raddle_jobs j JOIN raddle_policy_attempts p ON p.job_id=j.id WHERE j.id=$1").bind(before).fetch_one(&pool).await.unwrap();
    assert_eq!(state, ("active".to_owned(), true, 1));
    let pending: String = sqlx::query_scalar("SELECT status FROM raddle_jobs WHERE id=$1")
        .bind(after)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(pending, "ready");
    let business: Vec<i32> = sqlx::query_scalar("SELECT id FROM business ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(business, [0]);
    // Restoring the schema does not itself authorize replay or claim rollback.
    sqlx::query("ALTER TABLE raddle_jobs ADD COLUMN completed_at timestamptz")
        .execute(&pool)
        .await
        .unwrap();
    // Restore the captured successful tombstone metadata removed by this
    // deliberate fixture DDL, without re-executing that successful root.
    sqlx::query("UPDATE raddle_jobs SET completed_at=$2 WHERE id=$1")
        .bind(completed)
        .bind(prior_completion)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        !store
            .recover_policy_uncertain(
                "tenant",
                before,
                PolicyRecovery::RolledBack,
                "operator",
                "lease still live",
                Utc::now()
            )
            .await
            .unwrap()
    );
    sqlx::query(
        "UPDATE raddle_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(before)
    .execute(&pool)
    .await
    .unwrap();
    let mut restored = Host::new(Duration::from_secs(10)).unwrap();
    make_endpoint(store.clone(), Arc::clone(&handler))
        .attach(&mut restored, Duration::from_millis(5))
        .unwrap();
    let finish = restored.handle();
    let restored_task = tokio::spawn(restored.run());
    wait_completed(&store, after, &handler, 3).await;
    assert_eq!(
        store
            .inspect("tenant", before)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Active
    );
    assert!(
        store
            .recover_policy_uncertain(
                "tenant",
                before,
                PolicyRecovery::RolledBack,
                "operator",
                "schema restored and business rollback verified",
                Utc::now()
            )
            .await
            .unwrap()
    );
    wait_completed(&store, before, &handler, 4).await;
    finish.shutdown();
    restored_task.await.unwrap().unwrap();
    drop(stop);
    assert_eq!(
        store
            .inspect("tenant", completed)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Completed
    );
    let preserved: chrono::DateTime<Utc> =
        sqlx::query_scalar("SELECT completed_at FROM raddle_jobs WHERE id=$1")
            .bind(completed)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(preserved, prior_completion);
    let rows: Vec<(i32, i64)> =
        sqlx::query_as("SELECT id,count(*) FROM business GROUP BY id ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows, [(0, 1), (1, 1), (2, 1)]);
    let audit: (String, String) =
        sqlx::query_as("SELECT actor,reason FROM raddle_replay_audit WHERE job_id=$1")
            .bind(before)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        audit,
        (
            "operator".to_owned(),
            "schema restored and business rollback verified".to_owned()
        )
    );
}
