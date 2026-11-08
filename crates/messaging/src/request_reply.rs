use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use raddle::conversation::ReplyHub;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, Runtime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BoxFuture, BusError, MessageContext, RemoteInvoker, TrustedOrigin};

const REPLY_TO: &str = "raddle-reply-to";
const TOKEN: &str = "raddle-request-token";

/// Native raddle protocol frame, NOT a claim of Wolverine/.NET wire compatibility.
#[derive(Serialize, Deserialize)]
struct ReplyFrame {
    body: Option<Vec<u8>>,
}
impl WireMessage for ReplyFrame {
    const NAME: &'static str = "raddle.native-reply";
    const VERSION: u32 = 1;
}

struct ExpectedReply {
    tenant: String,
    contract: String,
}

struct Pending<'a> {
    token: Uuid,
    expected: &'a Mutex<HashMap<Uuid, ExpectedReply>>,
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Ok(mut expected) = self.expected.lock() {
            expected.remove(&self.token);
        }
    }
}

/// Real native request/reply over any existing confirmed Transport. The application
/// owns the reply listener and feeds authenticated deliveries to `accept_reply`.
/// Request nonces, trusted responder and trusted tenant all have to match; late,
/// forged and duplicate replies cannot complete another invocation.
pub struct ReplyHubInvoker<T> {
    transport: T,
    hub: ReplyHub,
    reply_to: String,
    responders: BTreeMap<String, String>,
    expected: Mutex<HashMap<Uuid, ExpectedReply>>,
}

impl<T: Transport> ReplyHubInvoker<T> {
    /// Configures a bounded waiter registry and explicit alias-to-authenticated-peer
    /// map. `reply_to` is a configured transport alias, not an inbound arbitrary URI.
    pub fn new(
        transport: T,
        capacity: usize,
        reply_to: &str,
        responders: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, BusError> {
        raddle::SubscriptionName::try_new(reply_to)
            .map_err(|_| BusError::Invalid("invalid reply endpoint"))?;
        let mut configured = BTreeMap::new();
        for (alias, source) in responders {
            if configured.len() >= 256
                || configured.contains_key(&alias)
                || raddle::SubscriptionName::try_new(&alias).is_err()
                || raddle::SubscriptionName::try_new(&source).is_err()
            {
                return Err(BusError::Invalid("invalid/duplicate responder"));
            }
            configured.insert(alias, source);
        }
        let hub = ReplyHub::try_new(capacity)
            .map_err(|_| BusError::Invalid("invalid reply waiter capacity"))?;
        Ok(Self {
            transport,
            hub,
            reply_to: reply_to.to_owned(),
            responders: configured,
            expected: Mutex::new(HashMap::new()),
        })
    }

    /// Receives an independently authenticated reply. False means late, wrong
    /// tenant/source/token/contract, duplicate or unrequested; acknowledge/drop
    /// those according to the application's receive policy, never upgrade trust.
    pub fn accept_reply(&self, origin: &TrustedOrigin, reply: Envelope) -> Result<bool, BusError> {
        let Ok(frame) = reply.decode::<ReplyFrame>() else {
            return Ok(false);
        };
        let Some(token) = reply.correlation_id() else {
            return Ok(false);
        };
        let expected = self
            .expected
            .lock()
            .map_err(|_| BusError::Invalid("reply registry poisoned"))?;
        let Some(expected_reply) = expected.get(&token) else {
            return Ok(false);
        };
        if expected_reply.tenant != origin.tenant() {
            return Ok(false);
        }
        // Do not let an authenticated but invalid inner response consume the
        // outer-frame waiter. A later valid response must still be deliverable.
        if let Some(bytes) = frame.body {
            let Ok(inner) = Envelope::from_slice(&bytes, Default::default()) else {
                return Ok(false);
            };
            if inner.contract() != expected_reply.contract {
                return Ok(false);
            }
        }
        drop(expected);
        self.hub
            .deliver(origin.source(), reply)
            .map_err(|_| BusError::Invalid("reply registry closed"))
    }
    /// Payload-free count for capacity, timeout and shutdown diagnostics.
    pub fn pending(&self) -> Result<usize, BusError> {
        self.expected
            .lock()
            .map(|entries| entries.len())
            .map_err(|_| BusError::Invalid("reply registry poisoned"))
    }
}

impl<T: Transport + 'static> RemoteInvoker for ReplyHubInvoker<T> {
    fn request<'a>(
        &'a self,
        destination: &'a str,
        context: &'a MessageContext,
        reply_contract: &'a str,
    ) -> BoxFuture<'a, Result<Envelope, BusError>> {
        Box::pin(async move {
            raddle::SubscriptionName::try_new(reply_contract)
                .map_err(|_| BusError::Invalid("invalid reply contract"))?;
            let responder = self
                .responders
                .get(destination)
                .ok_or(BusError::RemoteDisabled)?;
            // Every attempt has a fresh nonce even when the business envelope is
            // retried. A late reply to the old attempt can never match this one.
            let token = Uuid::new_v4();
            let waiter = self
                .hub
                .register(token, responder, ReplyFrame::NAME)
                .map_err(|_| BusError::Invalid("reply registration refused"))?;
            self.expected
                .lock()
                .map_err(|_| BusError::Invalid("reply registry poisoned"))?
                .insert(
                    token,
                    ExpectedReply {
                        tenant: context.origin().tenant().to_owned(),
                        contract: reply_contract.to_owned(),
                    },
                );
            let _pending = Pending {
                token,
                expected: &self.expected,
            };
            let request = context
                .envelope()
                .clone()
                .with_header(REPLY_TO.into(), self.reply_to.clone())?
                .with_header(TOKEN.into(), token.to_string())?;
            self.transport.publish(destination, &request).await?;
            // MessageBus owns the effective request deadline and drops this future
            // on timeout/cancellation, which removes BOTH registries through Drop.
            let reply = waiter
                .wait(Duration::from_secs(3600))
                .await
                .map_err(|_| TransportError::Transient)?;
            let frame = reply.decode::<ReplyFrame>()?;
            match frame.body {
                Some(bytes) => Ok(Envelope::from_slice(&bytes, Default::default())?),
                None => Err(BusError::RemoteFailure),
            }
        })
    }
}

type Handler =
    Box<dyn Fn(Envelope) -> BoxFuture<'static, Result<Envelope, BusError>> + Send + Sync>;

/// Native peer dispatcher using typed local Runtime handlers and an explicit reply
/// endpoint allowlist. It never opens a route supplied by an untrusted message.
pub struct RemoteResponder<T> {
    transport: T,
    runtime: Runtime,
    reply_endpoints: BTreeSet<String>,
    handlers: BTreeMap<(String, u32), Handler>,
}
impl<T: Transport> RemoteResponder<T> {
    /// Freezes configured reply aliases. Runtime remains explicit application state.
    pub fn new(
        transport: T,
        runtime: Runtime,
        reply_endpoints: impl IntoIterator<Item = String>,
    ) -> Result<Self, BusError> {
        let mut configured = BTreeSet::new();
        for endpoint in reply_endpoints {
            if configured.len() >= 256
                || !configured.insert(endpoint.clone())
                || raddle::SubscriptionName::try_new(endpoint).is_err()
            {
                return Err(BusError::Invalid("invalid/duplicate reply alias"));
            }
        }
        Ok(Self {
            transport,
            runtime,
            reply_endpoints: configured,
            handlers: BTreeMap::new(),
        })
    }
    /// Declares one accepted typed request/reply pair; no wire name is inferred
    /// from a Rust type name and unknown requests get a negative acknowledgement.
    pub fn handle<M>(&mut self) -> Result<(), BusError>
    where
        M: Request + WireMessage,
        M::Reply: WireMessage,
    {
        let key = (M::NAME.to_owned(), M::VERSION);
        raddle::routing::WireMessageType::of::<M>()
            .map_err(|_| BusError::Invalid("invalid request contract"))?;
        if !self.runtime.has_request::<M>()
            || self.handlers.contains_key(&key)
            || self.handlers.len() >= 1024
        {
            return Err(BusError::Invalid("missing/duplicate remote handler"));
        }
        let runtime = self.runtime.clone();
        self.handlers.insert(
            key,
            Box::new(move |envelope| {
                let runtime = runtime.clone();
                Box::pin(async move {
                    let message = envelope.decode::<M>()?;
                    let reply = runtime
                        .invoke(message)
                        .await
                        .map_err(|_| BusError::RemoteFailure)?;
                    Ok(Envelope::encode(&reply)?)
                })
            }),
        );
        Ok(())
    }
    /// Handles one authenticated ingress delivery, publishing a correlated positive
    /// or negative acknowledgement. Settle the request only after this confirms.
    /// Request/reply is volatile; a caller timeout does not undo peer business work.
    pub async fn respond(&self, context: &MessageContext) -> Result<(), BusError> {
        let reply_to = context
            .envelope()
            .headers()
            .get(REPLY_TO)
            .ok_or(BusError::RemoteDisabled)?;
        if !self.reply_endpoints.contains(reply_to) {
            return Err(BusError::RemoteDisabled);
        }
        let token = context
            .header::<Uuid>(TOKEN)
            .map_err(|_| BusError::Invalid("invalid request token"))?;
        if token.is_nil() {
            return Err(BusError::Invalid("nil request token"));
        }
        let key = (
            context.envelope().contract().to_owned(),
            context.envelope().version(),
        );
        let body = match self.handlers.get(&key) {
            Some(handler) => handler(context.envelope().clone())
                .await
                .ok()
                .map(|reply| reply.to_vec())
                .transpose()?,
            None => None,
        };
        let reply =
            Envelope::encode(&ReplyFrame { body })?.correlated(token, context.envelope().id())?;
        self.transport.publish(reply_to, &reply).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "request_reply_tests.rs"]
mod tests;
