use chrono::Utc;
use raddle_postgres::{
    Store,
    external_tables::{Aliases, Config, Listener, Options},
};
use serde_json::json;
use sqlx::PgPool;

use super::support::{self, Order};

#[tokio::test]
async fn external_native_defaults_and_invalid_bindings_are_explicit() {
    let options = Options::default();
    assert_eq!(options.interval.as_secs(), 10);
    assert_eq!(options.batch_size, 100);
    assert!(options.control_source);
    assert_eq!(options.advisory_lock, 12000);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgresql:///unused")
        .unwrap();
    let mut aliases = Aliases::default();
    aliases.register::<Order>("order").unwrap();
    assert!(aliases.register::<Order>("order").is_err());
    for schema in [String::new(), "invalid\0name".into(), "λ".repeat(32)] {
        let mut invalid = support::config();
        invalid.schema = schema;
        let mut registered = Aliases::default();
        registered.register::<Order>("order").unwrap();
        assert!(
            Listener::new(
                Store::new(pool.clone()),
                invalid,
                registered,
                options.clone()
            )
            .is_err()
        );
    }
    let mut remote = support::config();
    remote.delivery.kind = raddle_postgres::DeliveryKind::Remote;
    let mut remote_aliases = Aliases::default();
    remote_aliases.register::<Order>("order").unwrap();
    assert!(
        Listener::new(
            Store::new(pool.clone()),
            remote,
            remote_aliases,
            options.clone()
        )
        .is_err()
    );
    let mut c = support::config();
    c.body_column = c.id_column.clone();
    assert!(Listener::new(Store::new(pool), c, aliases, options).is_err());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_c01_c02_provisioning_fixed_types_all_quoted_names(pool: PgPool) {
    for alias in [false, true] {
        let mut c = support::config();
        c.schema = format!("Mixed schema λ {alias}");
        c.table = "items\"; SELECT 1; --".into();
        c.id_column = format!("{}x", "λ".repeat(31));
        c.body_column = "JSON\nBody".into();
        c.timestamp_column = "Added\tAt".into();
        if alias {
            c.alias_column = Some("Message Kind".into());
        }
        c.source = format!("external-{alias}");
        let listener = support::listener(&pool, c.clone(), Options::default());
        listener.provision_source().await.unwrap();
        listener.validate_source().await.unwrap();
        listener.provision_source().await.unwrap();
        let columns:Vec<(String,String)>=sqlx::query_as("SELECT a.attname::text,t.typname::text FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum")
            .bind(support::table(&c)).fetch_all(&pool).await.unwrap();
        let mut expected = vec![
            (c.id_column.clone(), "uuid".into()),
            (c.body_column.clone(), "jsonb".into()),
            (c.timestamp_column.clone(), "timestamptz".into()),
        ];
        if alias {
            expected.push(("Message Kind".into(), "varchar".into()));
        }
        assert_eq!(columns, expected);
        support::insert(&pool, &c, 1, json!({"value":7}), alias.then_some("order")).await;
        assert_eq!(listener.poll_once(Utc::now()).await.unwrap().accepted, 1);
        assert_eq!(support::count(&pool, &support::table(&c)).await, 0);
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_ownership_disabled_has_no_source_ddl_and_unbounded_is_supported(
    pool: PgPool,
) {
    let mut options = Options {
        control_source: false,
        ..Default::default()
    };
    let missing = support::listener(&pool, support::config(), options.clone());
    assert!(missing.provision_source().await.is_err());
    assert!(missing.poll_once(Utc::now()).await.is_err());
    let absent: bool = sqlx::query_scalar("SELECT to_regnamespace('ext') IS NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(absent);
    sqlx::raw_sql("CREATE SCHEMA ext; CREATE TABLE ext.items(id uuid PRIMARY KEY,json jsonb,timestamp timestamptz,kind varchar)")
        .execute(&pool).await.unwrap();
    let mut c = support::config();
    c.alias_column = Some("kind".into());
    let before:String=sqlx::query_scalar("SELECT string_agg(pg_get_constraintdef(oid),';' ORDER BY oid) FROM pg_constraint WHERE conrelid='ext.items'::regclass")
        .fetch_one(&pool).await.unwrap();
    options.body_bytes = 128;
    let listener = support::listener(&pool, c.clone(), options);
    listener.validate_source().await.unwrap();
    assert!(listener.provision_source().await.is_err());
    support::insert(&pool, &c, 1, json!({"value":1}), Some("order")).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().accepted, 1);
    let after:String=sqlx::query_scalar("SELECT string_agg(pg_get_constraintdef(oid),';' ORDER BY oid) FROM pg_constraint WHERE conrelid='ext.items'::regclass")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(before, after);
    // Diagnostic infinity is not a business timestamp or an admission prerequisite.
    sqlx::query("INSERT INTO ext.items VALUES($1,'{\"value\":2}','infinity','order')")
        .bind(uuid::Uuid::from_u128(2))
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().accepted, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_wrong_shape_and_cross_tenant_binding_are_shared_failures(pool: PgPool) {
    let c = support::config();
    let listener = support::listener(&pool, c.clone(), Default::default());
    listener.provision_source().await.unwrap();
    listener.poll_once(Utc::now()).await.unwrap();
    let mut other = c.clone();
    other.tenant = "other".into();
    assert!(
        support::listener(&pool, other, Default::default())
            .poll_once(Utc::now())
            .await
            .is_err()
    );
    sqlx::query("ALTER TABLE ext.items ALTER COLUMN json TYPE text USING json::text")
        .execute(&pool)
        .await
        .unwrap();
    assert!(listener.validate_source().await.is_err());
    assert!(listener.provision_source().await.is_err());
    let _unused = Config::new("t", "s", "ext", "items", "dest");
}
