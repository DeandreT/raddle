use std::collections::{BTreeMap, BTreeSet};

use crate::{Error, Session};

/// Frozen named/tenant clients with distinct protocol client IDs. Caller-owned
/// polling avoids an orphan worker per registered tenant.
pub struct Clients(BTreeMap<(String, String), Session>);
impl Clients {
    /// Takes ownership of explicit sessions and refuses duplicate logical keys or client IDs.
    pub fn new(clients: Vec<(String, String, Session)>) -> Result<Self, Error> {
        if clients.len() > 256 {
            return Err(Error::Invalid("client count"));
        }
        let mut result = BTreeMap::new();
        let mut ids = BTreeSet::new();
        for (name, tenant, client) in clients {
            crate::config::name(&name)?;
            crate::config::name(&tenant)?;
            if client.source().client != name || client.source().tenant != tenant {
                return Err(Error::Invalid("client source binding differs"));
            }
            if !ids.insert(client.client_id()) || result.insert((name, tenant), client).is_some() {
                return Err(Error::Invalid("duplicate client or protocol id"));
            }
        }
        Ok(Self(result))
    }
    /// Gets only the authorized tenant's session; received tenant headers never select it.
    pub fn get_mut(&mut self, name: &str, tenant: &str) -> Result<&mut Session, Error> {
        self.0
            .get_mut(&(name.to_owned(), tenant.to_owned()))
            .ok_or(Error::Invalid("unknown tenant/client"))
    }
    /// Explicitly disconnects all clients and reports independent failures without
    /// preventing later clients from draining.
    pub async fn close(self) -> Vec<Result<(), Error>> {
        let mut outcomes = Vec::with_capacity(self.0.len());
        for (_, client) in self.0 {
            outcomes.push(client.close().await);
        }
        outcomes
    }
}
