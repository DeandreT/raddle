//! Explicit same-database PostgreSQL external-table ingress and raw recovery.
//!
//! Constructors start no tasks and perform no SQL. Source DDL is restricted to
//! [`Listener::provision_source`]; framework migrations remain owned by `Store`.
//! See the repository's external-tables guide for allocation and trust boundaries.

mod config;
mod connection;
mod poll;
mod quarantine;
mod source;

pub use config::{Aliases, Config, Options};
pub use poll::PollReport;
pub use quarantine::{Correction, QuarantinePage, QuarantinedInput, ReplayOutcome};

use crate::Store;

/// Payload-free listener/configuration/recovery error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid application configuration or correction.
    #[error("invalid external table configuration: {0}")]
    Invalid(&'static str),
    /// An underlying operation failed. Do not expose its private source publicly.
    #[error("external table database operation failed")]
    Database(#[from] sqlx::Error),
    /// Canonical custody refused this input.
    #[error("external table custody failed")]
    Custody(#[from] crate::Error),
    /// Source schema or its persisted trusted binding changed.
    #[error("external table source definition differs")]
    Schema,
    /// An original in-place input disappeared/changed, or an accepted identity conflicts.
    #[error("external table input conflicts with retained identity")]
    Conflict,
    /// A finite operation deadline elapsed; reconcile uncertain commits by identity.
    #[error("external table operation deadline elapsed")]
    Timeout,
}

/// Caller-polled listener. Clones are unnecessary: share it through an `Arc` when
/// registering a restartable Host service. It owns no tasks or private pool.
pub struct Listener {
    store: Store,
    config: Config,
    options: Options,
    aliases: Aliases,
}

impl Listener {
    /// Validates application bindings without opening a connection or issuing DDL.
    pub fn new(
        store: Store,
        config: Config,
        aliases: Aliases,
        options: Options,
    ) -> Result<Self, Error> {
        config.validate()?;
        options.validate()?;
        if aliases.entries.is_empty()
            || config
                .default_alias
                .as_ref()
                .is_some_and(|a| !aliases.entries.contains_key(a))
        {
            return Err(Error::Invalid("missing registered message type"));
        }
        Ok(Self {
            store,
            config,
            options,
            aliases,
        })
    }

    /// Validated caller-owned polling options, including the configured interval.
    pub fn options(&self) -> &Options {
        &self.options
    }
}
