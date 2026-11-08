//! Explicit operational commands. Connection credentials are accepted only through the environment.

use std::str::FromStr;
use std::time::Duration;

use raddle_postgres::Store;
use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

const USAGE: &str = "usage: raddle-admin check | stats TENANT | inspect TENANT JOB | replay TENANT JOB ACTOR REASON --confirm-replay";

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Check,
    Stats(String),
    Inspect(String, uuid::Uuid),
    Replay {
        tenant: String,
        job: uuid::Uuid,
        actor: String,
        reason: String,
    },
}

fn bounded(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

fn parse(args: &[String]) -> Result<Command, &'static str> {
    match args {
        [command] if command == "check" => Ok(Command::Check),
        [command, tenant] if command == "stats" && bounded(tenant, 128) => {
            Ok(Command::Stats(tenant.clone()))
        }
        [command, tenant, job] if command == "inspect" && bounded(tenant, 128) => Ok(
            Command::Inspect(tenant.clone(), job.parse().map_err(|_| USAGE)?),
        ),
        [command, tenant, job, actor, reason, confirm]
            if command == "replay"
                && confirm == "--confirm-replay"
                && bounded(tenant, 128)
                && bounded(actor, 128)
                && bounded(reason, 512) =>
        {
            Ok(Command::Replay {
                tenant: tenant.clone(),
                job: job.parse().map_err(|_| USAGE)?,
                actor: actor.clone(),
                reason: reason.clone(),
            })
        }
        _ => Err(USAGE),
    }
}

async fn execute(command: Command) -> Result<serde_json::Value, &'static str> {
    let url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is required")?;
    let mut options = PgConnectOptions::from_str(&url)
        .map_err(|_| "invalid database configuration")?
        .disable_statement_logging();
    if !matches!(command, Command::Replay { .. }) {
        options = options.options([("default_transaction_read_only", "on")]);
    }
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(2))
        .connect_with(options)
        .await
        .map_err(|_| "database unavailable")?;
    let store = Store::new(pool.clone());
    let result = match command {
        Command::Check => {
            if !store.is_available().await {
                return Err("database unavailable");
            }
            // Check the versioned schema without applying migrations.
            let applied: i64 =
                sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE success=true")
                    .fetch_one(&pool)
                    .await
                    .map_err(|_| "schema unavailable")?;
            let read_only: bool =
                sqlx::query_scalar("SELECT current_setting('transaction_read_only')='on'")
                    .fetch_one(&pool)
                    .await
                    .map_err(|_| "database unavailable")?;
            Ok(
                serde_json::json!({"database":"available","applied_migrations":applied,"read_only":read_only}),
            )
        }
        Command::Stats(tenant) => serde_json::to_value(
            store
                .statistics(&tenant)
                .await
                .map_err(|_| "statistics unavailable")?,
        )
        .map_err(|_| "response unavailable"),
        Command::Inspect(tenant, job) => {
            let view = store
                .inspect(&tenant, job)
                .await
                .map_err(|_| "inspection unavailable")?
                .ok_or("job not found")?;
            Ok(
                serde_json::json!({"id":view.id,"message_id":view.message_id,"status":format!("{:?}",view.status).to_lowercase(),"attempts":view.attempts}),
            )
        }
        Command::Replay {
            tenant,
            job,
            actor,
            reason,
        } => {
            if !store
                .replay(&tenant, job, &actor, &reason, chrono::Utc::now())
                .await
                .map_err(|_| "replay unavailable")?
            {
                return Err("job is not replayable");
            }
            Ok(serde_json::json!({"replayed":true,"id":job}))
        }
    };
    pool.close().await;
    result
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--help"] || args == ["-h"] {
        println!("{USAGE}");
        return std::process::ExitCode::SUCCESS;
    }
    let command = match parse(&args) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            return std::process::ExitCode::from(2);
        }
    };
    match tokio::time::timeout(Duration::from_secs(20), execute(command)).await {
        Ok(Ok(value)) => {
            println!("{value}");
            std::process::ExitCode::SUCCESS
        }
        Ok(Err(error)) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
        Err(_) => {
            eprintln!("operation timed out; a replay outcome may be uncertain");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, parse};

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn writes_require_explicit_confirmation_and_bounded_audit() {
        let id = "00000000-0000-0000-0000-000000000001";
        assert!(parse(&args(&["replay", "tenant", id, "actor", "reason"])).is_err());
        assert!(
            parse(&args(&[
                "replay",
                "tenant",
                id,
                "",
                "reason",
                "--confirm-replay"
            ]))
            .is_err()
        );
        assert!(
            parse(&args(&[
                "replay",
                "tenant",
                id,
                "actor",
                "reason",
                "--confirm-replay"
            ]))
            .is_ok()
        );
        assert!(parse(&args(&["inspect", "tenant", "invalid-id"])).is_err());
        assert_eq!(parse(&args(&["check"])), Ok(Command::Check));
        assert!(parse(&args(&["stats", "tenant", "extra"])).is_err());
    }
}
