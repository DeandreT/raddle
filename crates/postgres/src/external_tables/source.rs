use chrono::{DateTime, Utc};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

use super::config::quote;
use super::{Error, Listener, connection};

pub(super) struct Input {
    pub id: Uuid,
    pub fingerprint: Vec<u8>,
    pub content_hash: Vec<u8>,
    pub body: Option<String>,
    pub alias: Option<String>,
    pub body_bytes: Option<i64>,
    pub alias_bytes: Option<i64>,
    pub diagnostic_time: Option<String>,
}

impl Listener {
    /// Checks the configured UUID primary key, JSONB body, timestamptz diagnostic
    /// column and optional VARCHAR alias. No source DDL or length constraints.
    pub async fn validate_source(&self) -> Result<(), Error> {
        tokio::time::timeout(self.options.operation_timeout, async {
            let mut connection = connection::acquire(&self.store.pool, &self.options).await?;
            self.validate_on(&mut connection).await
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// Explicit controlled setup. Disabled ownership refuses before opening a
    /// connection. Existing drift is an error, never an implicit ALTER or DROP.
    /// Framework tables must first be migrated through `Store::migrate`.
    pub async fn provision_source(&self) -> Result<(), Error> {
        if !self.options.control_source {
            return Err(Error::Invalid("source control disabled"));
        }
        tokio::time::timeout(self.options.operation_timeout, async {
            let mut connection = connection::acquire(&self.store.pool, &self.options).await?;
            let mut tx = connection.begin().await?;
            sqlx::query(&format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                quote(&self.config.schema)?
            ))
            .execute(&mut *tx)
            .await?;
            let alias = match &self.config.alias_column {
                Some(name) => format!(",{} varchar", quote(name)?),
                None => String::new(),
            };
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {} ({} uuid PRIMARY KEY,{} jsonb,{} timestamptz{})",
                self.config.qualified()?,
                quote(&self.config.id_column)?,
                quote(&self.config.body_column)?,
                quote(&self.config.timestamp_column)?,
                alias
            ))
            .execute(&mut *tx)
            .await?;
            self.validate_on(&mut tx).await?;
            tx.commit().await?;
            Ok(())
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    pub(super) async fn validate_on(&self, connection: &mut PgConnection) -> Result<(), Error> {
        let oid: Option<i64> = sqlx::query_scalar("SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind IN ('r','p')")
            .bind(&self.config.schema).bind(&self.config.table).fetch_optional(&mut *connection).await?;
        let Some(oid) = oid else {
            return Err(Error::Schema);
        };
        let mut columns = vec![
            (&self.config.id_column, "uuid"),
            (&self.config.body_column, "jsonb"),
            (&self.config.timestamp_column, "timestamptz"),
        ];
        if let Some(alias) = &self.config.alias_column {
            columns.push((alias, "varchar"));
        }
        for (name, expected) in columns {
            let row = sqlx::query("SELECT t.typname,a.attnum,a.attnotnull FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid WHERE a.attrelid=$1::bigint::oid AND a.attname=$2 AND NOT a.attisdropped AND a.attnum>0")
                .bind(oid).bind(name).fetch_optional(&mut *connection).await?;
            let Some(row) = row else {
                return Err(Error::Schema);
            };
            if row.try_get::<String, _>("typname")? != expected {
                return Err(Error::Schema);
            }
            if name == &self.config.id_column {
                let primary: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=$1::bigint::oid AND indisprimary AND indnkeyatts=1 AND indkey[0]=$2)")
                    .bind(oid).bind(row.try_get::<i16,_>("attnum")?).fetch_one(&mut *connection).await?;
                if !primary || !row.try_get::<bool, _>("attnotnull")? {
                    return Err(Error::Schema);
                }
            }
        }
        Ok(())
    }

    pub(super) async fn bind_source(&self, connection: &mut PgConnection) -> Result<(), Error> {
        let definition = self.config.definition()?;
        let binding = self.config.qualified()?;
        sqlx::query("INSERT INTO raddle_external_sources(tenant,source,binding,definition) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
            .bind(&self.config.tenant).bind(&self.config.source).bind(&binding).bind(&definition)
            .execute(&mut *connection).await?;
        self.verify_binding(connection).await
    }

    pub(super) async fn verify_binding(&self, connection: &mut PgConnection) -> Result<(), Error> {
        let definition = self.config.definition()?;
        let binding = self.config.qualified()?;
        let actual: Option<Vec<u8>> = sqlx::query_scalar("SELECT definition FROM raddle_external_sources WHERE tenant=$1 AND source=$2 AND binding=$3")
            .bind(&self.config.tenant).bind(&self.config.source).bind(binding)
            .fetch_optional(connection).await?;
        if actual.as_ref() != Some(&definition) {
            return Err(Error::Schema);
        }
        Ok(())
    }

    // Every producer-controlled variable-width field is guarded on the server.
    // jsonb_build_array gives unambiguous full stored-value/null framing. This
    // deliberately makes no claim about PostgreSQL detoast/render/hash memory.
    pub(super) async fn select(
        &self,
        connection: &mut PgConnection,
        id: Uuid,
    ) -> Result<Option<Input>, Error> {
        self.select_inner(connection, id, true).await
    }

    pub(super) async fn select_metadata(
        &self,
        connection: &mut PgConnection,
        id: Uuid,
    ) -> Result<Option<Input>, Error> {
        self.select_inner(connection, id, false).await
    }

    async fn select_inner(
        &self,
        connection: &mut PgConnection,
        id: Uuid,
        include_payload: bool,
    ) -> Result<Option<Input>, Error> {
        let c = &self.config;
        let body = quote(&c.body_column)?;
        let alias = match &c.alias_column {
            Some(a) => quote(a)?,
            None => "NULL::varchar".into(),
        };
        let time = quote(&c.timestamp_column)?;
        let sql = format!(
            "SELECT {key} AS id,sha256(convert_to(jsonb_build_array({key},{body} IS NULL,{body},{alias},{time}::text)::text,'UTF8')) AS fingerprint,sha256(convert_to(jsonb_build_array({body} IS NULL,{body},{alias})::text,'UTF8')) AS content_hash, CASE WHEN $4::boolean AND octet_length(convert_to({body}::text,'UTF8'))<=$2 THEN {body}::text END AS body, CASE WHEN $4::boolean AND octet_length(convert_to({alias},'UTF8'))<=$3 THEN {alias} END AS alias,octet_length(convert_to({body}::text,'UTF8'))::bigint AS body_bytes,octet_length(convert_to({alias},'UTF8'))::bigint AS alias_bytes,left({time}::text,64) AS diagnostic_time FROM {table} WHERE {key}=$1 FOR UPDATE SKIP LOCKED",
            key = quote(&c.id_column)?,
            table = c.qualified()?
        );
        let row = sqlx::query(&sql)
            .bind(id)
            .bind(i64::from(self.options.body_bytes))
            .bind(i64::from(self.options.alias_bytes))
            .bind(include_payload)
            .fetch_optional(connection)
            .await?;
        row.map(|r| {
            Ok(Input {
                id: r.try_get("id")?,
                fingerprint: r.try_get("fingerprint")?,
                content_hash: r.try_get("content_hash")?,
                body: r.try_get("body")?,
                alias: r.try_get("alias")?,
                body_bytes: r.try_get("body_bytes")?,
                alias_bytes: r.try_get("alias_bytes")?,
                diagnostic_time: r.try_get("diagnostic_time")?,
            })
        })
        .transpose()
    }

    pub(super) async fn delete(
        &self,
        connection: &mut PgConnection,
        id: Uuid,
    ) -> Result<(), Error> {
        let deleted = sqlx::query(&format!(
            "DELETE FROM {} WHERE {}=$1",
            self.config.qualified()?,
            quote(&self.config.id_column)?
        ))
        .bind(id)
        .execute(connection)
        .await?;
        if deleted.rows_affected() != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    pub(super) async fn observe(
        &self,
        connection: &mut PgConnection,
        input: &Input,
        now: DateTime<Utc>,
    ) -> Result<(), Error> {
        sqlx::query("INSERT INTO raddle_external_inputs(tenant,source,input_id,fingerprint,first_seen,diagnostic_time,status,body_bytes,alias_bytes,content_hash) VALUES($1,$2,$3,$4,coalesce((SELECT min(first_seen) FROM raddle_external_inputs WHERE tenant=$1 AND source=$2 AND input_id=$3),$5),$6,'observed',$7,$8,$9) ON CONFLICT DO NOTHING")
            .bind(&self.config.tenant).bind(&self.config.source).bind(input.id).bind(&input.fingerprint)
            .bind(now).bind(&input.diagnostic_time).bind(input.body_bytes).bind(input.alias_bytes).bind(&input.content_hash)
            .execute(connection).await?;
        Ok(())
    }
}
