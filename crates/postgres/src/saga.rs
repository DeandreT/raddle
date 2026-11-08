use chrono::{DateTime, Utc};
use raddle::wire::{Envelope, WireMessage};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

use crate::{Delivery, DeliveryKind, Error, Scope};

/// Versioned persisted state. Migrations are explicit application operations.
pub trait SagaState: Serialize + DeserializeOwned + Send + Sync {
    /// Stable saga identity, independent of Rust type paths.
    const NAME: &'static str;
    /// Positive persisted schema version.
    const VERSION: u32;
}

/// State and concurrency token loaded within a transaction.
pub struct Saga<S> {
    tenant: String,
    correlation: String,
    revision: i64,
    state: S,
    completed: bool,
    timeout_generation: i64,
    timeout_due: Option<DateTime<Utc>>,
}

impl<S: SagaState> Saga<S> {
    /// Borrows the current application state.
    pub fn state(&self) -> &S {
        &self.state
    }
    /// Mutates state locally; `Scope::save_saga` persists it.
    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }
    /// Marks completion and invalidates pending timeouts.
    pub fn finish(&mut self) {
        self.completed = true;
        self.timeout_due = None;
    }
    /// Whether this saga already completed.
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Invalidates the current timeout without removing already queued messages.
    pub fn cancel_timeout(&mut self) {
        self.timeout_due = None;
    }
    /// Accepts only the current timeout after its persisted due time.
    pub fn accepts_timeout(&self, timeout: &SagaTimeout, now: DateTime<Utc>) -> bool {
        !self.completed
            && timeout.kind == S::NAME
            && timeout.correlation == self.correlation
            && timeout.generation == self.timeout_generation
            && self.timeout_due.is_some_and(|due| due <= now)
    }
}

/// A durable wakeup; handlers check generation and persisted due time before acting.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SagaTimeout {
    /// Stable saga type.
    pub kind: String,
    /// Business correlation key within the authenticated tenant.
    pub correlation: String,
    /// Superseded generations are harmless no-ops.
    pub generation: i64,
}

impl WireMessage for SagaTimeout {
    const NAME: &'static str = "raddle.saga-timeout";
    const VERSION: u32 = 1;
}

impl Scope<'_> {
    /// Creates or locks a saga. Concurrent starts converge on one persisted row.
    ///
    /// Completed rows remain tombstones. Loading them never restarts the saga.
    pub async fn saga<S: SagaState>(
        &mut self,
        tenant: &str,
        correlation: &str,
        initial: impl FnOnce() -> S,
    ) -> Result<Saga<S>, Error> {
        let result = self.load_saga(tenant, correlation, initial).await;
        if result.is_err() {
            self.mark_rollback_only();
        }
        result
    }

    async fn load_saga<S: SagaState>(
        &mut self,
        tenant: &str,
        correlation: &str,
        initial: impl FnOnce() -> S,
    ) -> Result<Saga<S>, Error> {
        if tenant.is_empty()
            || tenant.len() > 128
            || correlation.is_empty()
            || correlation.len() > 256
            || S::VERSION == 0
            || S::VERSION > i32::MAX as u32
        {
            return Err(Error::Invalid("invalid saga identity or version"));
        }
        raddle::SubscriptionName::try_new(S::NAME)
            .map_err(|_| Error::Invalid("invalid saga name"))?;
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raddle_sagas WHERE tenant=$1 AND kind=$2 AND correlation=$3)")
            .bind(tenant).bind(S::NAME).bind(correlation).fetch_one(self.connection()).await?;
        if !exists {
            let state = serde_json::to_value(initial()).map_err(raddle::wire::WireError::from)?;
            sqlx::query("INSERT INTO raddle_sagas(tenant,kind,correlation,schema_version,state) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
                .bind(tenant).bind(S::NAME).bind(correlation).bind(S::VERSION as i32).bind(state).execute(self.connection()).await?;
        }
        let row = sqlx::query("SELECT schema_version,revision,state,completed,timeout_generation,timeout_due FROM raddle_sagas WHERE tenant=$1 AND kind=$2 AND correlation=$3 FOR UPDATE")
            .bind(tenant).bind(S::NAME).bind(correlation).fetch_one(self.connection()).await?;
        if row.try_get::<i32, _>("schema_version")? != S::VERSION as i32 {
            return Err(Error::Invalid("saga state needs explicit migration"));
        }
        Ok(Saga {
            tenant: tenant.into(),
            correlation: correlation.into(),
            revision: row.try_get("revision")?,
            state: serde_json::from_value(row.try_get("state")?)
                .map_err(raddle::wire::WireError::from)?,
            completed: row.try_get("completed")?,
            timeout_generation: row.try_get("timeout_generation")?,
            timeout_due: row.try_get("timeout_due")?,
        })
    }

    /// Saves state with a revision check, in the same transaction as all outgoing effects.
    pub async fn save_saga<S: SagaState>(&mut self, saga: &mut Saga<S>) -> Result<(), Error> {
        let state = match serde_json::to_value(&saga.state) {
            Ok(state) => state,
            Err(error) => {
                self.mark_rollback_only();
                return Err(Error::Wire(error.into()));
            }
        };
        let result = sqlx::query("UPDATE raddle_sagas SET state=$4,completed=$5,timeout_generation=$6,timeout_due=$7,revision=revision+1 WHERE tenant=$1 AND kind=$2 AND correlation=$3 AND revision=$8 AND schema_version=$9 AND completed=false")
            .bind(&saga.tenant).bind(S::NAME).bind(&saga.correlation).bind(state).bind(saga.completed).bind(saga.timeout_generation).bind(saga.timeout_due).bind(saga.revision).bind(S::VERSION as i32).execute(self.connection()).await;
        match result {
            Ok(result) if result.rows_affected() == 1 => {
                saga.revision += 1;
                Ok(())
            }
            Ok(_) => {
                self.mark_rollback_only();
                Err(Error::StaleClaim)
            }
            Err(error) => {
                self.mark_rollback_only();
                Err(error.into())
            }
        }
    }

    /// Replaces the saga's current timer and saves its generation atomically.
    pub async fn schedule_saga_timeout<S: SagaState>(
        &mut self,
        saga: &mut Saga<S>,
        subscription: &str,
        due: DateTime<Utc>,
    ) -> Result<(), Error> {
        if saga.completed || saga.timeout_generation == i64::MAX {
            self.mark_rollback_only();
            return Err(Error::Invalid(
                "completed saga or exhausted timer generation",
            ));
        }
        saga.timeout_generation += 1;
        saga.timeout_due = Some(due);
        let timeout = SagaTimeout {
            kind: S::NAME.into(),
            correlation: saga.correlation.clone(),
            generation: saga.timeout_generation,
        };
        let envelope = Envelope::encode(&timeout)?;
        self.stage(
            "raddle.sagas",
            &saga.tenant,
            &envelope,
            &[Delivery::new(DeliveryKind::Local, subscription, due)],
        )
        .await?;
        self.save_saga(saga).await
    }

    /// Explicitly migrates one locked state schema. Does not execute business transitions.
    pub async fn migrate_saga<S: SagaState>(
        &mut self,
        tenant: &str,
        correlation: &str,
        from_version: u32,
        migrate: impl FnOnce(Value) -> Result<S, Error>,
    ) -> Result<(), Error> {
        let result = async {
            if from_version == 0 || from_version >= S::VERSION || S::VERSION > i32::MAX as u32 { return Err(Error::Invalid("invalid saga migration versions")); }
            let row = sqlx::query("SELECT schema_version,state FROM raddle_sagas WHERE tenant=$1 AND kind=$2 AND correlation=$3 FOR UPDATE")
                .bind(tenant).bind(S::NAME).bind(correlation).fetch_one(self.connection()).await?;
            if row.try_get::<i32,_>("schema_version")? != from_version as i32 { return Err(Error::Invalid("unexpected saga schema version")); }
            let state = serde_json::to_value(migrate(row.try_get("state")?)?).map_err(raddle::wire::WireError::from)?;
            sqlx::query("UPDATE raddle_sagas SET state=$4,schema_version=$5,revision=revision+1 WHERE tenant=$1 AND kind=$2 AND correlation=$3")
                .bind(tenant).bind(S::NAME).bind(correlation).bind(state).bind(S::VERSION as i32).execute(self.connection()).await?;
            Ok(())
        }.await;
        if result.is_err() {
            self.mark_rollback_only();
        }
        result
    }
}
