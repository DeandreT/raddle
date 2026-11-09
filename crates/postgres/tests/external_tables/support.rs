use chrono::Utc;
use raddle::wire::{Codecs, WireMessage};
use raddle_postgres::external_tables::{Aliases, Config, Listener, Options};
use raddle_postgres::{HandlerFailure, RetryPolicy, Store, Worker};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct Order {
    #[serde(default)]
    pub value: i32,
}
impl WireMessage for Order {
    const NAME: &'static str = "external.order";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
pub struct Other {
    pub text: String,
}
impl WireMessage for Other {
    const NAME: &'static str = "external.other";
    const VERSION: u32 = 1;
}

pub fn config() -> Config {
    let mut config = Config::new(
        "tenant",
        "external-native",
        "ext",
        "items",
        "external-inbox",
    );
    config.default_alias = Some("order".into());
    config
}
pub fn listener(pool: &PgPool, config: Config, options: Options) -> Listener {
    let mut aliases = Aliases::default();
    aliases.register::<Order>("order").unwrap();
    aliases.register::<Other>("other").unwrap();
    Listener::new(Store::new(pool.clone()), config, aliases, options).unwrap()
}
pub fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
pub fn table(c: &Config) -> String {
    format!("{}.{}", quote(&c.schema), quote(&c.table))
}
pub async fn insert(pool: &PgPool, c: &Config, id: u128, value: Value, alias: Option<&str>) {
    let alias_name = c
        .alias_column
        .as_ref()
        .map(|v| format!(",{}", quote(v)))
        .unwrap_or_default();
    let alias_value = if c.alias_column.is_some() { ",$3" } else { "" };
    let sql = format!(
        "INSERT INTO {} ({},{},{}{}) VALUES($1,$2,clock_timestamp(){})",
        table(c),
        quote(&c.id_column),
        quote(&c.body_column),
        quote(&c.timestamp_column),
        alias_name,
        alias_value
    );
    let query = sqlx::query(&sql).bind(Uuid::from_u128(id)).bind(value);
    if c.alias_column.is_some() {
        query.bind(alias).execute(pool).await.unwrap();
    } else {
        query.execute(pool).await.unwrap();
    }
}
pub async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}
pub async fn effects(pool: &PgPool) {
    sqlx::query("CREATE TABLE external_effects(id uuid PRIMARY KEY,value text NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}
pub fn worker(pool: &PgPool) -> Worker {
    let mut worker = Worker::new(Store::new(pool.clone()), RetryPolicy::default());
    worker
        .handle::<Order, _>("external-inbox", Codecs::default(), |message, scope| {
            Box::pin(async move {
                let id = scope
                    .envelope()
                    .map_err(|_| HandlerFailure::Reject)?
                    .unwrap()
                    .id();
                sqlx::query("INSERT INTO external_effects VALUES($1,$2)")
                    .bind(id)
                    .bind(message.value.to_string())
                    .execute(scope.connection())
                    .await
                    .map_err(|_| HandlerFailure::Retry)?;
                Ok(())
            })
        })
        .unwrap();
    worker
        .handle::<Other, _>("external-inbox", Codecs::default(), |message, scope| {
            Box::pin(async move {
                let id = scope
                    .envelope()
                    .map_err(|_| HandlerFailure::Reject)?
                    .unwrap()
                    .id();
                sqlx::query("INSERT INTO external_effects VALUES($1,$2)")
                    .bind(id)
                    .bind(message.text)
                    .execute(scope.connection())
                    .await
                    .map_err(|_| HandlerFailure::Retry)?;
                Ok(())
            })
        })
        .unwrap();
    worker
}
pub async fn setup(pool: &PgPool, options: Options) -> Listener {
    let listener = listener(pool, config(), options);
    listener.provision_source().await.unwrap();
    effects(pool).await;
    listener
}
pub async fn healthy(pool: &PgPool, listener: &Listener, c: &Config, id: u128) {
    insert(pool, c, id, json!({"value":id as i32}), None).await;
    for _ in 0..8 {
        listener.poll_once(Utc::now()).await.unwrap();
    }
    worker(pool).run_once(256).await.unwrap();
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM external_effects WHERE id=$1)")
            .bind(Uuid::from_u128(id))
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(exists, "new healthy work was starved");
}
