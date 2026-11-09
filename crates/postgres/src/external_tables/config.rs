use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::{Envelope, WireMessage};
use serde_json::Value;
use uuid::Uuid;

use super::Error;
use crate::{Delivery, DeliveryKind};

/// Trusted application binding and separately validated SQL identifiers.
#[derive(Clone)]
pub struct Config {
    /// Authorized tenant, never taken from source content.
    pub tenant: String,
    /// Stable custody namespace, never taken from source content.
    pub source: String,
    /// Explicit PostgreSQL schema.
    pub schema: String,
    /// Source table within that schema.
    pub table: String,
    /// UUID primary key column; defaults to `id`.
    pub id_column: String,
    /// JSONB application body column; defaults to `json`.
    pub body_column: String,
    /// Diagnostic timestamptz column; defaults to `timestamp`.
    pub timestamp_column: String,
    /// Optional VARCHAR alias column. Unknown nonempty aliases never fall back.
    pub alias_column: Option<String>,
    /// Single-message default, also used for null/empty aliases when explicitly configured.
    pub default_alias: Option<String>,
    /// Trusted local-inbox policy. `available_at` is replaced by each receipt time;
    /// expiry/partition/attempt policy remain explicit application choices.
    pub delivery: Delivery,
}

impl Config {
    /// Constructs the conventional native source shape and one local destination.
    pub fn new(tenant: &str, source: &str, schema: &str, table: &str, destination: &str) -> Self {
        Self {
            tenant: tenant.into(),
            source: source.into(),
            schema: schema.into(),
            table: table.into(),
            id_column: "id".into(),
            body_column: "json".into(),
            timestamp_column: "timestamp".into(),
            alias_column: None,
            default_alias: None,
            delivery: Delivery::new(DeliveryKind::Local, destination, Utc::now()),
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        if !bounded(&self.tenant, 128) || !bounded(&self.source, 128) {
            return Err(Error::Invalid("invalid trusted origin"));
        }
        let mut names = vec![&self.id_column, &self.body_column, &self.timestamp_column];
        if let Some(alias) = &self.alias_column {
            names.push(alias);
        }
        for name in [&self.schema, &self.table]
            .into_iter()
            .chain(names.iter().copied())
        {
            quote(name)?;
        }
        names.sort();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Invalid("duplicate column names"));
        }
        if self.alias_column.is_none() && self.default_alias.is_none() {
            return Err(Error::Invalid("default type or alias column required"));
        }
        if self
            .default_alias
            .as_deref()
            .is_some_and(|a| !bounded(a, 4096))
        {
            return Err(Error::Invalid("invalid default alias"));
        }
        if self.delivery.kind != DeliveryKind::Local {
            return Err(Error::Invalid("external listener requires a local inbox"));
        }
        self.delivery.validate().map_err(Error::Custody)
    }

    pub(super) fn qualified(&self) -> Result<String, Error> {
        Ok(format!("{}.{}", quote(&self.schema)?, quote(&self.table)?))
    }

    pub(super) fn definition(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(&(
            &self.schema,
            &self.table,
            &self.id_column,
            &self.body_column,
            &self.timestamp_column,
            &self.alias_column,
        ))
        .map_err(|_| Error::Invalid("invalid source definition"))
    }
}

/// Finite resource controls. No retention or automatic eviction is implied.
#[derive(Clone, Debug)]
pub struct Options {
    /// Documented upstream polling default: ten seconds.
    pub interval: Duration,
    /// Bounded discovery page, 1..=256; default 100.
    pub batch_size: u32,
    /// Enables only explicit provisioning; default true. Polling never issues DDL.
    pub control_source: bool,
    /// PostgreSQL session advisory lock, default 12000. Different values may reduce
    /// unrelated source contention; identity/row locks still protect custody.
    pub advisory_lock: i64,
    /// Maximum body text transferred per selected row, 1..=1 MiB.
    pub body_bytes: u32,
    /// Maximum alias transferred per row, 1..=4096 bytes.
    pub alias_bytes: u32,
    /// Maximum retained raw-quarantine occurrences, including resolved audits.
    pub quarantine_rows: u32,
    /// Maximum retained raw snapshot bytes. In-place references cost zero payload bytes.
    pub quarantine_bytes: u64,
    /// Per-statement/lock/acquire deadline, 1ms..=2s.
    pub statement_timeout: Duration,
    /// Whole caller-polled operation deadline, 1ms..=60s.
    pub operation_timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            batch_size: 100,
            control_source: true,
            advisory_lock: 12000,
            body_bytes: 1_048_576,
            alias_bytes: 4096,
            quarantine_rows: 1000,
            quarantine_bytes: 16 * 1_048_576,
            statement_timeout: Duration::from_secs(2),
            operation_timeout: Duration::from_secs(30),
        }
    }
}

impl Options {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&self.interval)
            || !(1..=256).contains(&self.batch_size)
            || !(1..=1_048_576).contains(&self.body_bytes)
            || !(1..=4096).contains(&self.alias_bytes)
            || self.quarantine_rows == 0
            || self.quarantine_bytes > i64::MAX as u64
            || !(Duration::from_millis(1)..=Duration::from_secs(2))
                .contains(&self.statement_timeout)
            || !(Duration::from_millis(1)..=Duration::from_secs(60))
                .contains(&self.operation_timeout)
        {
            return Err(Error::Invalid("invalid polling or quarantine bounds"));
        }
        Ok(())
    }
}

type Decode =
    Box<dyn Fn(Uuid, DateTime<Utc>, Value) -> Result<Envelope, &'static str> + Send + Sync>;

/// Finite explicit aliases, not runtime reflection or foreign type activation.
#[derive(Default)]
pub struct Aliases {
    pub(super) entries: BTreeMap<String, Decode>,
}

impl Aliases {
    /// Registers a typed body decoder. Known protected/selected-wire root framing
    /// is rejected before permissive/defaulted application deserialization.
    pub fn register<M: WireMessage>(&mut self, alias: &str) -> Result<(), Error> {
        if !bounded(alias, 4096)
            || self.entries.len() >= 256
            || self.entries.contains_key(alias)
            || M::VERSION == 0
            || raddle::SubscriptionName::try_new(M::NAME).is_err()
        {
            return Err(Error::Invalid("invalid or duplicate type alias"));
        }
        self.entries.insert(
            alias.into(),
            Box::new(|id, time, value| {
                let message: M = serde_json::from_value(value).map_err(|_| "invalid-body")?;
                Envelope::with_identity(id, time, &message).map_err(|_| "invalid-envelope")
            }),
        );
        Ok(())
    }
}

pub(super) fn bounded(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

pub(super) fn quote(name: &str) -> Result<String, Error> {
    if name.is_empty() || name.len() > 63 || name.contains('\0') {
        return Err(Error::Invalid("invalid SQL identifier"));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}
