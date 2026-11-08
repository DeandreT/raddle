use std::collections::BTreeMap;

use crate::{Broker, Error};

/// Frozen registry of application-authorized named/tenant multiplexers. Separate
/// instances can target separate databases/servers or intentionally share prefixed keys.
pub struct Clients(BTreeMap<(String, String), Broker>);
impl Clients {
    /// Registers at most 256 independent bindings; duplicate keys are refused.
    pub fn new(clients: Vec<(String, String, Broker)>) -> Result<Self, Error> {
        if clients.len() > 256 {
            return Err(Error::Invalid("client count"));
        }
        let mut result = BTreeMap::new();
        for (name, tenant, client) in clients {
            crate::config::name(&name)?;
            crate::config::name(&tenant)?;
            if result.insert((name, tenant), client).is_some() {
                return Err(Error::Invalid("duplicate client"));
            }
        }
        Ok(Self(result))
    }
    /// Resolves trusted caller context. Missing tenants never fall back to a default broker.
    pub fn get(&self, name: &str, tenant: &str) -> Result<&Broker, Error> {
        self.0
            .get(&(name.to_owned(), tenant.to_owned()))
            .ok_or(Error::Invalid("unknown tenant/client"))
    }
}
