//! Explicit policy configuration shared by managed application endpoints.

use std::sync::Arc;
use std::time::Duration;

use raddle_policies::{
    CircuitAdmission, CircuitBreaker, DriverError, PolicyError, PolicyExecutor, PolicySet,
    QuotaAdmission, QuotaPermit, ScopedQuotas,
};
use tokio::sync::Mutex;

use crate::scoped::Failure;

/// Opt-in endpoint policy ownership. Quotas/circuits are process-local, never a
/// distributed admission claim. Retry authorization is supplied per handler.
pub struct EndpointPolicies {
    pub(crate) executor: PolicyExecutor,
    pub(crate) admission: Mutex<Admission>,
    pub(crate) quotas: Option<Arc<ScopedQuotas>>,
}

pub(crate) struct Admission {
    pub circuit: Option<CircuitBreaker>,
    pub paused_until: Duration,
}

pub(crate) enum Permit {
    Granted(Option<QuotaPermit>),
    Limited(Duration),
}

impl EndpointPolicies {
    /// Freezes ordered message, endpoint and global rules; starts no worker.
    pub fn new(policies: PolicySet) -> Self {
        Self {
            executor: PolicyExecutor::new(policies),
            admission: Mutex::new(Admission {
                circuit: None,
                paused_until: Duration::ZERO,
            }),
            quotas: None,
        }
    }
    /// Installs one endpoint-local receive circuit.
    pub fn with_circuit(mut self, circuit: CircuitBreaker) -> Self {
        self.admission.get_mut().circuit = Some(circuit);
        self
    }
    /// Checks scoped quotas anew before every actual attempt.
    pub fn with_quotas(mut self, quotas: Arc<ScopedQuotas>) -> Self {
        self.quotas = Some(quotas);
        self
    }

    pub(crate) async fn acquire(
        &self,
        endpoint: &str,
        contract: &str,
        now: Duration,
    ) -> Result<Permit, DriverError> {
        let entered = tokio::time::Instant::now();
        let base = now;
        let mut admission = self.admission.lock().await;
        let now = base
            .checked_add(entered.elapsed())
            .ok_or(DriverError::Invalid)?;
        if now < admission.paused_until {
            return Ok(Permit::Limited(admission.paused_until));
        }
        if let Some(circuit) = &mut admission.circuit
            && let CircuitAdmission::Paused(until) = circuit.admission(now)
        {
            // A probe already in flight can have an elapsed deadline. Yield
            // rather than spin or incrementing a fictitious attempt.
            return Ok(Permit::Limited(
                until.max(now.saturating_add(Duration::from_millis(1))),
            ));
        }
        let quota = self
            .quotas
            .as_ref()
            .and_then(|q| q.select(endpoint, contract));
        match quota
            .map(|q| q.acquire_current())
            .transpose()
            .map_err(|_| DriverError::Invalid)?
        {
            Some((QuotaAdmission::Limited { retry_at }, quota_now)) => {
                if let Some(circuit) = &mut admission.circuit {
                    circuit.cancel_probe();
                }
                let local_now = base
                    .checked_add(entered.elapsed())
                    .ok_or(DriverError::Invalid)?;
                let delay = retry_at
                    .map(|due| due.saturating_sub(quota_now))
                    .unwrap_or(Duration::from_millis(1));
                let until = local_now.checked_add(delay).ok_or(DriverError::Invalid)?;
                if retry_at.is_some() {
                    admission.paused_until = admission.paused_until.max(until);
                }
                Ok(Permit::Limited(until))
            }
            Some((QuotaAdmission::Granted(permit), _)) => Ok(Permit::Granted(Some(permit))),
            None => Ok(Permit::Granted(None)),
        }
    }

    pub(crate) async fn cancel_probe(&self) {
        if let Some(circuit) = &mut self.admission.lock().await.circuit {
            circuit.cancel_probe();
        }
    }

    pub(crate) async fn record(
        &self,
        now: Duration,
        error: Option<&(dyn std::error::Error + Send + Sync + 'static)>,
    ) -> Result<Option<Duration>, DriverError> {
        let mut admission = self.admission.lock().await;
        let until = match &mut admission.circuit {
            Some(circuit) => circuit
                .record(
                    now,
                    error.map(|error| error as &(dyn std::error::Error + 'static)),
                    false,
                )
                .map_err(|_| DriverError::Invalid)?,
            None => None,
        };
        if let Some(until) = until {
            admission.paused_until = admission.paused_until.max(until);
        }
        Ok(until)
    }
}

pub(crate) enum AttemptFailure {
    Application(PolicyError),
    Cancelled,
    Infrastructure(raddle_postgres::Error),
    // The already-admitted attempt cannot prove its transaction outcome. This
    // retains only that delivery; it is not evidence of a shared admission outage.
    Uncertain(raddle_postgres::Error),
    InvalidInput,
}

impl AttemptFailure {
    pub(crate) fn scoped<E: std::error::Error + Send + Sync + 'static>(
        failure: Failure<E>,
    ) -> Self {
        match failure {
            Failure::Handler(error) => Self::Application(Box::new(error)),
            Failure::Cancelled => Self::Cancelled,
            Failure::UncertainDatabase(error) => Self::Uncertain(error),
            Failure::Database(error) if !order_failure(&error) => Self::Infrastructure(error),
            Failure::Effect(crate::effects::EffectError::Storage(error))
                if !order_failure(&error) =>
            {
                Self::Infrastructure(error)
            }
            Failure::Wire(_) => Self::InvalidInput,
            other => Self::Application(Box::new(other)),
        }
    }
}

// These errors belong to this message only after run_strict joined a successful
// rollback, or a PostgreSQL transaction rejection with a known aborted outcome.
// Uncertain commit/rollback completion never reaches this classifier.
fn order_failure(error: &raddle_postgres::Error) -> bool {
    match error {
        raddle_postgres::Error::Wire(_)
        | raddle_postgres::Error::Invalid(_)
        | raddle_postgres::Error::IdentityConflict
        | raddle_postgres::Error::Capacity => true,
        raddle_postgres::Error::Database(error) => error.as_database_error().is_some_and(|error| {
            error.code().is_some_and(|code| {
                code.starts_with("22")
                    || code.starts_with("23")
                    || code.starts_with("40")
                    || code == "57014"
            })
        }),
        _ => false,
    }
}
