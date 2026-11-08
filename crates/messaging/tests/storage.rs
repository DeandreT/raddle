//! Real PostgreSQL execution of returned typed storage decisions.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::effects::{
    Effect, EffectError, Effects, HandlerOutput, Storage, StorageAction,
};
use raddle_messaging::scoped::{Failure, LocalOnlyFlush, ScopedHandler};
use raddle_messaging::{
    BoxFuture, BusBuilder, LocalQueue, MessageContext, QueueMode, QueueSettings, TrustedOrigin,
};
use raddle_postgres::{Scope, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct Save;
impl WireMessage for Save {
    const NAME: &'static str = "test.storage-actions";
    const VERSION: u32 = 1;
}
#[derive(Clone)]
struct Item {
    id: i32,
    value: i32,
}
struct Repository(AtomicUsize);
impl Storage<Item, ()> for Repository {
    fn apply<'a>(
        &'a self,
        action: StorageAction<Item>,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), EffectError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            let (sql, item, require_existing) = match action {
                StorageAction::Insert(item) => {
                    ("INSERT INTO items(id,value) VALUES($1,$2)", item, false)
                }
                StorageAction::Update(item) | StorageAction::Store(item) => {
                    ("UPDATE items SET value=$2 WHERE id=$1", item, true)
                }
                StorageAction::Upsert(item) => (
                    "INSERT INTO items(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
                    item,
                    false,
                ),
                StorageAction::Delete(item) => (
                    "DELETE FROM items WHERE id=$1 AND $2::int IS NOT NULL",
                    item,
                    false,
                ),
                StorageAction::Nothing => {
                    return Err(EffectError::Failed("nothing-must-not-call-repository"));
                }
                _ => return Err(EffectError::Failed("unsupported-storage-action")),
            };
            let rows = sqlx::query(sql)
                .bind(item.id)
                .bind(item.value)
                .execute(scope.connection())
                .await
                .map_err(raddle_postgres::Error::from)?;
            if require_existing && rows.rows_affected() != 1 {
                return Err(EffectError::Failed("missing-record"));
            }
            Ok(())
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("storage decision failed")]
struct DecisionError;
struct Handler {
    actions: Vec<StorageAction<Item>>,
    repository: Arc<Repository>,
}
impl ScopedHandler<Save> for Handler {
    type Resources = ();
    type Reply = ();
    type Error = DecisionError;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), DecisionError>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        _: &'a Save,
        _: &'a mut (),
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<(), ()>, DecisionError>> {
        Box::pin(async move {
            let effects = self
                .actions
                .iter()
                .cloned()
                .map(|action| Effect::storage(action, Arc::clone(&self.repository)))
                .collect::<Vec<_>>();
            Ok(HandlerOutput {
                reply: (),
                effects: Effects::returned(effects).map_err(|_| DecisionError)?,
            })
        })
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn returned_storage_unit_of_work_distinguishes_store_update_upsert_delete_and_noop(
    pool: PgPool,
) {
    sqlx::query("CREATE TABLE items(id integer PRIMARY KEY,value integer NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let (queue, runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 8,
        parallelism: 1,
    })
    .unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "storage").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.execution_queue(queue).unwrap();
    let bus = builder.build();
    let repository = Arc::new(Repository(AtomicUsize::new(0)));
    let phases = [
        vec![
            StorageAction::Nothing,
            StorageAction::Insert(Item { id: 1, value: 10 }),
            StorageAction::Update(Item { id: 1, value: 20 }),
            StorageAction::Store(Item { id: 1, value: 30 }),
            StorageAction::Upsert(Item { id: 2, value: 40 }),
        ],
        vec![
            StorageAction::Insert(Item { id: 3, value: 50 }),
            StorageAction::Store(Item { id: 999, value: 60 }),
        ],
        vec![
            StorageAction::Delete(Item { id: 2, value: 0 }),
            StorageAction::Upsert(Item { id: 1, value: 70 }),
        ],
    ];
    for (index, actions) in phases.into_iter().enumerate() {
        let context = MessageContext::new(
            TrustedOrigin::authenticated("tenant", "storage", None).unwrap(),
            Envelope::encode(&Save).unwrap(),
            Utc::now(),
        );
        let result = bus
            .execute::<Save, _>(
                Arc::new(Handler {
                    actions,
                    repository: Arc::clone(&repository),
                }),
                context,
                Store::new(pool.clone()),
                Arc::new(LocalOnlyFlush),
            )
            .await;
        if index == 1 {
            assert!(matches!(
                result,
                Err(Failure::Effect(EffectError::Failed("missing-record")))
            ));
        } else {
            assert!(result.unwrap().post_commit.is_ok());
        }
    }
    assert_eq!(
        sqlx::query_as::<_, (i32, i32)>("SELECT id,value FROM items ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap(),
        [(1, 70)]
    );
    assert_eq!(repository.0.load(Ordering::SeqCst), 8); // Nothing never calls the storage adapter
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}
