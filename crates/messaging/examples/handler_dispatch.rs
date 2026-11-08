//! Run only against a dedicated, disposable PostgreSQL example database.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use raddle::RuntimeBuilder;
use raddle::wire::WireMessage;
use raddle_host::Host;
use raddle_messaging::composition::{Group, Then};
use raddle_messaging::effects::HandlerOutput;
use raddle_messaging::handler_routing::HandlerSet;
use raddle_messaging::scoped::{LocalOnlyFlush, ScopedHandler};
use raddle_messaging::{BoxFuture, BusBuilder, MessageContext, TrustedOrigin};
use raddle_postgres::{Scope, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Order(Uuid);
impl WireMessage for Order {
    const NAME: &'static str = "example.handler-order";
    const VERSION: u32 = 1;
}
struct Record<const N: i32>;
impl<const N: i32> ScopedHandler<Order> for Record<N> {
    type Resources = ();
    type Reply = ();
    type Error = raddle_postgres::Error;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        order: &'a Order,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<(), ()>, Self::Error>> {
        Box::pin(async move {
            sqlx::query("INSERT INTO raddle_example_handler_steps(order_id,step,tx) VALUES($1,$2,txid_current())")
                .bind(order.0).bind(N).execute(scope.connection()).await?;
            Ok(HandlerOutput::reply(()))
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if std::env::var("RADDLE_EXAMPLE_CONFIRM").as_deref() != Ok("isolated") {
        return Err("set RADDLE_EXAMPLE_CONFIRM=isolated for a dedicated example database".into());
    }
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    run(pool).await
}

async fn run(pool: PgPool) -> Result<(), Box<dyn Error>> {
    let store = Store::new(pool.clone());
    store.migrate().await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS raddle_example_handler_steps(order_id uuid,step integer,tx bigint,PRIMARY KEY(order_id,step))").execute(&pool).await?;
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let registration = HandlerSet::<Order, _>::new(Then::new(
        Group::new("inventory", Arc::new(Record::<1>)),
        Group::new("accounting", Arc::new(Record::<2>)),
    ))
    .register(&mut builder, "orders", store, Arc::new(LocalOnlyFlush))?;
    // Applications may log this structured startup warning or instead call
    // builder.assert_no_combined_handlers() to prohibit multi-group combination.
    assert_eq!(builder.combined_handlers().len(), 1);
    let bus = builder.build();
    let mut host = Host::new(Duration::from_secs(5))?;
    for (name, _, runner) in registration.owners.local {
        runner.attach(&mut host, &name)?;
    }
    let stop = host.handle();
    let running = tokio::spawn(host.run());
    tokio::time::timeout(Duration::from_secs(5), async {
        while !stop.snapshot().ready {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let order = Order(Uuid::new_v4());
    bus.send(
        &order,
        TrustedOrigin::authenticated("example", "example.handler-dispatch", None)?,
    )
    .await?;
    stop.shutdown();
    running.await??;
    let result: (i64, i64) = sqlx::query_as(
        "SELECT count(*),count(DISTINCT tx) FROM raddle_example_handler_steps WHERE order_id=$1",
    )
    .bind(order.0)
    .fetch_one(&pool)
    .await?;
    assert_eq!(result, (2, 1));
    println!("Both native handler groups committed in one transaction.");
    Ok(())
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn example_executes_the_actual_bus_and_host(pool: PgPool) -> Result<(), Box<dyn Error>> {
    run(pool).await
}
