use raddle_postgres::external_tables::{Error, Listener};

use crate::{ServiceContext, ServiceFailure};

/// Hosts explicitly configured external-table polling; performs no provisioning
/// or migration. Readiness follows a real successful source poll, not registration
/// or advisory-lock contention. Row-local refusals remain in the poll report and
/// do not fail the Host. Cooperative shutdown drains the current bounded round;
/// forced abort closes its owned database connection and leaves source recovery.
pub async fn postgres_external_table(
    listener: &Listener,
    mut context: ServiceContext,
) -> Result<(), ServiceFailure> {
    loop {
        if *context.shutdown().borrow() {
            return Ok(());
        }
        let report = listener
            .poll_once(std::time::SystemTime::now().into())
            .await
            .map_err(classify)?;
        if !report.busy {
            context.set_ready(true);
        }
        tokio::select! {
            biased;
            _=context.cancelled()=>return Ok(()),
            _=tokio::time::sleep(listener.options().interval)=>{}
        }
    }
}

fn classify(error: Error) -> ServiceFailure {
    match error {
        Error::Database(_)
        | Error::Custody(raddle_postgres::Error::Database(_))
        | Error::Timeout => ServiceFailure::Transient,
        _ => ServiceFailure::Permanent,
    }
}
