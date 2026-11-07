//! Typed saga transitions inside the worker's authenticated execution scope.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use raddle::wire::{Codecs, WireMessage};
use sqlx::Row;

use crate::{Error, HandlerFailure, Saga, SagaState, Scope, Worker};

/// Validated business correlation, deliberately separate from tenant identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelationKey(String);

impl CorrelationKey {
    /// Rejects empty, oversized, or control-character-containing keys.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(Error::Invalid("invalid saga correlation"));
        }
        Ok(Self(value))
    }

    /// Borrows the validated key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Explicit policy for a message that has no existing saga.
pub enum SagaInitialization<S> {
    /// Creates initial state only if the row does not exist; never restarts a tombstone.
    Start(fn() -> S),
    /// Unknown correlations are acknowledged as no-ops without creating state.
    ExistingOnly,
}

/// A transition borrows both locked state and the worker's existing transaction.
pub type SagaTransitionFuture<'scope> =
    Pin<Box<dyn Future<Output = Result<(), HandlerFailure>> + Send + 'scope>>;

impl Worker {
    /// Registers a typed saga transition using only the authenticated scope tenant.
    ///
    /// The helper locks/initializes state, skips completed tombstones, executes the
    /// transition, then saves it. Outgoing staging and timeout changes use the same
    /// scope. It never begins or commits a transaction. A transition may save early
    /// (including via `schedule_saga_timeout`); its final state is saved again.
    pub fn handle_saga<M, S, C, F>(
        &mut self,
        subscription: &str,
        codecs: Codecs,
        correlation: C,
        initialization: SagaInitialization<S>,
        transition: F,
    ) -> Result<(), Error>
    where
        M: WireMessage,
        S: SagaState + 'static,
        C: Fn(&M) -> Result<CorrelationKey, HandlerFailure> + Send + Sync + 'static,
        F: for<'scope> Fn(
                M,
                &'scope mut Saga<S>,
                &'scope mut Scope<'static>,
            ) -> SagaTransitionFuture<'scope>
            + Send
            + Sync
            + 'static,
    {
        raddle::SubscriptionName::try_new(S::NAME)
            .map_err(|_| Error::Invalid("invalid saga name"))?;
        if S::VERSION == 0 || S::VERSION > i32::MAX as u32 {
            return Err(Error::Invalid("invalid saga version"));
        }
        let correlation = Arc::new(correlation);
        let transition = Arc::new(transition);
        let initialization = Arc::new(initialization);
        self.handle::<M, _>(subscription, codecs, move |message, scope| {
            let correlation = Arc::clone(&correlation);
            let transition = Arc::clone(&transition);
            let initialization = Arc::clone(&initialization);
            Box::pin(async move {
                let result = async {
                    let key = correlation(&message)?;
                    let tenant = scope.tenant().ok_or(HandlerFailure::Reject)?.to_owned();
                    let existing = sqlx::query("SELECT completed,state FROM raddle_sagas WHERE tenant=$1 AND kind=$2 AND correlation=$3 FOR UPDATE")
                        .bind(&tenant).bind(S::NAME).bind(key.as_str())
                        .fetch_optional(scope.connection()).await.map_err(|_| HandlerFailure::Retry)?;
                    if existing.as_ref().map(|row| row.try_get::<bool, _>("completed"))
                        .transpose().map_err(|_| HandlerFailure::Retry)? == Some(true) {
                        return Ok(());
                    }
                    let initial_state = match existing {
                        Some(row) => serde_json::from_value(row.try_get("state").map_err(|_| HandlerFailure::Retry)?)
                            .map_err(|_| HandlerFailure::Retry)?,
                        None => match initialization.as_ref() {
                            SagaInitialization::Start(initial) => initial(),
                            SagaInitialization::ExistingOnly => return Ok(()),
                        },
                    };
                    let mut saga = scope.saga(&tenant, key.as_str(), || initial_state)
                        .await.map_err(|_| HandlerFailure::Retry)?;
                    if saga.is_completed() {
                        return Ok(());
                    }
                    transition(message, &mut saga, scope).await?;
                    scope.save_saga(&mut saga).await.map_err(|_| HandlerFailure::Retry)
                }.await;
                if result.is_err() {
                    scope.mark_rollback_only();
                }
                result
            })
        })
    }
}
