use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

use super::{Error, Options};

// This lane deliberately closes its checked-out connection even after success.
// A cancelled advisory-lock acquisition/unlock has an unknown server outcome;
// returning that connection to a shared pool would leak a session-owned lock.
pub(super) async fn acquire(
    pool: &PgPool,
    options: &Options,
) -> Result<PoolConnection<Postgres>, Error> {
    let mut connection = tokio::time::timeout(options.statement_timeout, pool.acquire())
        .await
        .map_err(|_| Error::Timeout)??;
    connection.close_on_drop();
    sqlx::query("SELECT set_config('statement_timeout',$1,false),set_config('lock_timeout',$1,false),set_config('TimeZone','UTC',false),set_config('DateStyle','ISO, YMD',false)")
        .bind(format!("{}ms", options.statement_timeout.as_millis()))
        .execute(&mut *connection).await?;
    Ok(connection)
}

pub(super) async fn lock(connection: &mut PgConnection, key: i64) -> Result<bool, Error> {
    Ok(sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(connection)
        .await?)
}

pub(super) async fn unlock(connection: &mut PgConnection, key: i64) -> Result<(), Error> {
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .fetch_one(connection)
        .await?;
    if !released {
        return Err(Error::Invalid("advisory lock ownership lost"));
    }
    Ok(())
}

pub(super) fn shared(error: &Error) -> bool {
    match error {
        Error::Schema | Error::Invalid(_) => true,
        Error::Database(sqlx::Error::Database(error))
        | Error::Custody(crate::Error::Database(sqlx::Error::Database(error))) => {
            error.code().is_none_or(|code| {
                code.starts_with("08")
                    || code.starts_with("42")
                    || code.starts_with("28")
                    || code.starts_with("53")
                    || code.starts_with("58")
                    || code.starts_with("57P")
            })
        }
        Error::Database(_) | Error::Timeout => true,
        Error::Custody(crate::Error::Database(_)) => true,
        _ => false,
    }
}
