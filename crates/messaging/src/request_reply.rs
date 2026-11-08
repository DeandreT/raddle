use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use raddle::conversation::ReplyHub;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, Runtime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "serialization-rpc")]
use crate::codec_rpc::{ReplyContract, SelectedReplyFrame, SelectedResponse, SelectedRpc};
use crate::{BoxFuture, BusError, MessageContext, RemoteInvoker, TrustedOrigin};

const REPLY_TO: &str = "raddle-reply-to";
const TOKEN: &str = "raddle-request-token";
const MODE: &str = "raddle-reply-mode";

/// Native raddle protocol frame, NOT a claim of Wolverine/.NET wire compatibility.
#[derive(Serialize, Deserialize)]
struct ReplyFrame {
    body: Option<Vec<u8>>,
}
impl WireMessage for ReplyFrame {
    const NAME: &'static str = "raddle.native-reply";
    const VERSION: u32 = 1;
}

#[derive(Clone)]
struct ExpectedReply {
    tenant: String,
    contract: String,
    #[cfg(feature = "serialization-rpc")]
    selected: Option<SelectedExpected>,
}

#[cfg(feature = "serialization-rpc")]
#[derive(Clone)]
struct SelectedExpected {
    source: String,
    request_id: Uuid,
    reply: ReplyContract,
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
    #[cfg(feature = "serialization-rpc")]
    selected: Option<SelectedRpc>,
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
            #[cfg(feature = "serialization-rpc")]
            selected: None,
        })
    }

    /// Explicitly opts into selected native RPC v2; both peers must agree.
    /// Configure selected request transport and JSON outer reply transport
    /// separately. Generic transports cannot be inspected for codec/encryption
    /// configuration. Existing `new` remains legacy JSON v1.
    #[cfg(feature = "serialization-rpc")]
    pub fn with_selected_rpc(mut self, selected: SelectedRpc) -> Result<Self, BusError> {
        selected.validate()?;
        if self.selected.is_some() || self.pending()? != 0 {
            return Err(BusError::Invalid("selected RPC already configured/active"));
        }
        self.selected = Some(selected);
        Ok(self)
    }

    /// Receives an independently authenticated reply. False means late, wrong
    /// tenant/source/token/contract, duplicate or unrequested; acknowledge/drop
    /// those according to the application's receive policy, never upgrade trust.
    pub fn accept_reply(&self, origin: &TrustedOrigin, reply: Envelope) -> Result<bool, BusError> {
        #[cfg(feature = "serialization-rpc")]
        if let Some(selected) = &self.selected {
            return self.accept_selected(origin, reply, selected);
        }
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

    #[cfg(feature = "serialization-rpc")]
    fn accept_selected(
        &self,
        origin: &TrustedOrigin,
        reply: Envelope,
        selected: &SelectedRpc,
    ) -> Result<bool, BusError> {
        // All validation and canonical conversion precedes ReplyHub::deliver.
        // Never decode a second time after consuming the waiter: converters can
        // be stateful application code, and a bad reply must leave it alive.
        let Some(token) = reply.correlation_id() else {
            return Ok(false);
        };
        let expected = self
            .expected
            .lock()
            .map_err(|_| BusError::Invalid("reply registry poisoned"))?
            .get(&token)
            .cloned();
        let Some(expected) = expected else {
            return Ok(false);
        };
        let Some(details) = &expected.selected else {
            return Ok(false);
        };
        if expected.tenant != origin.tenant()
            || details.source != origin.source()
            || reply.causation_id() != Some(details.request_id)
            || reply.validate(Default::default()).is_err()
        {
            return Ok(false);
        }
        let Ok(frame) = reply.decode::<SelectedReplyFrame>() else {
            return Ok(false);
        };
        if frame.mode != selected.declaration() {
            return Ok(false);
        }
        let body = match frame.response {
            SelectedResponse::Success { body: bytes } => {
                let Ok(inner) = selected.decode(&bytes) else {
                    return Ok(false);
                };
                if inner.contract() != expected.contract || inner.version() != details.reply.version
                {
                    return Ok(false);
                }
                let Ok(bytes) = inner.to_vec() else {
                    return Ok(false);
                };
                // The downstream RemoteInvoker/MessageBus handoff reparses a
                // canonical envelope and decodes the expected typed R. Validate
                // both now, including a canonicalizer's final model/schema.
                let Ok(canonical) = Envelope::from_slice(&bytes, Default::default()) else {
                    return Ok(false);
                };
                if !(details.reply.validate)(&canonical) {
                    return Ok(false);
                }
                Some(bytes)
            }
            SelectedResponse::Failure {} => None,
        };
        // Internal v1-shaped carrier contains ONLY validated canonical content;
        // it is not published and cannot trigger a selected decode after custody.
        let Ok(canonical) = reply.rebody(&ReplyFrame { body }, Default::default()) else {
            return Ok(false);
        };
        self.hub
            .deliver(origin.source(), canonical)
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
            #[cfg(feature = "serialization-rpc")]
            let selected_reply = if let Some(selected) = &self.selected {
                let pair = selected.pair(context.envelope())?;
                if pair.name != reply_contract {
                    return Err(BusError::Invalid("selected RPC reply contract mismatch"));
                }
                Some(pair)
            } else {
                None
            };
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
                        #[cfg(feature = "serialization-rpc")]
                        selected: selected_reply.map(|reply| SelectedExpected {
                            source: responder.clone(),
                            request_id: context.envelope().id(),
                            reply,
                        }),
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
            #[cfg(feature = "serialization-rpc")]
            let request = if let Some(selected) = &self.selected {
                let request = request.with_header(MODE.into(), selected.declaration())?;
                // Admit before serializing; exhaustion cannot create unbounded
                // concurrent preflight bodies. The guard releases both slots on
                // projection failure. Physical encoding remains the transport's job.
                selected.encode(&request)?;
                request
            } else {
                request
            };
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
    #[cfg(feature = "serialization-rpc")]
    selected: Option<SelectedRpc>,
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
            #[cfg(feature = "serialization-rpc")]
            selected: None,
        })
    }
    /// Opts into selected application replies, carried inside JSON native v2
    /// protocol envelopes. Must be called before registering handlers. Configure
    /// the reply publisher as JSON, not as a selected application-body publisher.
    #[cfg(feature = "serialization-rpc")]
    pub fn with_selected_rpc(mut self, selected: SelectedRpc) -> Result<Self, BusError> {
        selected.validate()?;
        if self.selected.is_some() || !self.handlers.is_empty() {
            return Err(BusError::Invalid(
                "selected RPC already configured/handlers registered",
            ));
        }
        self.selected = Some(selected);
        Ok(self)
    }
    /// Declares one accepted typed request/reply pair; no wire name is inferred
    /// from a Rust type name and unknown requests get a negative acknowledgement.
    pub fn handle<M>(&mut self) -> Result<(), BusError>
    where
        M: Request + WireMessage,
        M::Reply: WireMessage,
    {
        #[cfg(feature = "serialization-rpc")]
        if self
            .selected
            .as_ref()
            .is_some_and(|selected| !selected.has_pair::<M>())
        {
            return Err(BusError::Invalid("unconfigured selected RPC handler pair"));
        }
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
        self.validate_mode(context)?;
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
            Some(handler) => handler(context.envelope().clone()).await.ok(),
            None => None,
        };
        #[cfg(feature = "serialization-rpc")]
        if let Some(selected) = &self.selected {
            let response = match body.as_ref() {
                Some(reply) => {
                    // The caller's internal canonical carrier is bounded too.
                    Envelope::encode(&ReplyFrame {
                        body: Some(reply.to_vec()?),
                    })?;
                    SelectedResponse::Success {
                        body: selected.encode(reply)?,
                    }
                }
                None => SelectedResponse::Failure {},
            };
            let reply = Envelope::encode(&SelectedReplyFrame {
                mode: selected.declaration(),
                response,
            })?
            .correlated(token, context.envelope().id())?;
            self.transport.publish(reply_to, &reply).await?;
            return Ok(());
        }
        let body = body.as_ref().map(Envelope::to_vec).transpose()?;
        let reply =
            Envelope::encode(&ReplyFrame { body })?.correlated(token, context.envelope().id())?;
        self.transport.publish(reply_to, &reply).await?;
        Ok(())
    }

    fn validate_mode(&self, context: &MessageContext) -> Result<(), BusError> {
        let mode = context.envelope().headers().get(MODE);
        #[cfg(feature = "serialization-rpc")]
        if let Some(selected) = &self.selected {
            if mode != Some(&selected.declaration()) {
                return Err(BusError::Invalid("native RPC peer mode mismatch"));
            }
            selected.pair(context.envelope())?;
            selected.encode(context.envelope())?;
            return Ok(());
        }
        if mode.is_some() {
            return Err(BusError::Invalid("unsupported native RPC peer mode"));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "request_reply_tests.rs"]
mod tests;
