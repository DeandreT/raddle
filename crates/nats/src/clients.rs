use std::collections::BTreeMap;

use crate::{Core, Error, JetStream};

/// Frozen named/tenant connection registry. Lookup keys are application-authorized
/// context, never automatically copied from untrusted message headers.
pub struct Clients {
    core: BTreeMap<(String, String), Core>,
    jetstream: BTreeMap<(String, String), JetStream>,
}
impl Clients {
    /// Registers independent account or prefix-tenanted connections. Core and JetStream
    /// remain distinct APIs and may share the same authorized lookup key.
    pub fn new(
        core: Vec<(String, String, Core)>,
        jetstream: Vec<(String, String, JetStream)>,
    ) -> Result<Self, Error> {
        if core.len() + jetstream.len() > 256 {
            return Err(Error::Invalid("client count"));
        }
        let mut result = Self {
            core: BTreeMap::new(),
            jetstream: BTreeMap::new(),
        };
        for (name, tenant, client) in core {
            crate::config::name(&name)?;
            crate::config::name(&tenant)?;
            if result.core.insert((name, tenant), client).is_some() {
                return Err(Error::Invalid("duplicate client"));
            }
        }
        for (name, tenant, client) in jetstream {
            crate::config::name(&name)?;
            crate::config::name(&tenant)?;
            if client
                .dedup_scope()
                .is_some_and(|scope| scope.client != name || scope.tenant != tenant)
            {
                return Err(Error::Invalid("deduplication source binding differs"));
            }
            if result.jetstream.insert((name, tenant), client).is_some() {
                return Err(Error::Invalid("duplicate client"));
            }
        }
        Ok(result)
    }
    /// Resolves an at-most-once Core client without falling back to another tenant.
    pub fn core(&self, name: &str, tenant: &str) -> Result<&Core, Error> {
        self.core
            .get(&(name.to_owned(), tenant.to_owned()))
            .ok_or(Error::Invalid("unknown tenant/client"))
    }
    /// Resolves a confirmed JetStream client without falling back to another tenant.
    pub fn jetstream(&self, name: &str, tenant: &str) -> Result<&JetStream, Error> {
        self.jetstream
            .get(&(name.to_owned(), tenant.to_owned()))
            .ok_or(Error::Invalid("unknown tenant/client"))
    }
}
