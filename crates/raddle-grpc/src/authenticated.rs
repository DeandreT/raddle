use raddle::Request;

use crate::{Principal, RpcContext};

/// RPC-specific request wrapper carrying verified identity and call lifetime.
/// Register this request type in the ordinary `RuntimeBuilder`.
pub struct Authenticated<M> {
    principal: Principal,
    context: RpcContext,
    message: M,
}

impl<M> Authenticated<M> {
    pub(crate) fn new(principal: Principal, context: RpcContext, message: M) -> Self {
        Self {
            principal,
            context,
            message,
        }
    }
    /// Verified identity; never built from x-tenant or other caller ownership fields.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }
    /// Monotonic call deadline and cancellation signal.
    pub fn context(&self) -> &RpcContext {
        &self.context
    }
    /// Typed decoded request.
    pub fn message(&self) -> &M {
        &self.message
    }
    /// Consumes the wrapper while retaining trusted identity and call lifetime.
    pub fn into_parts(self) -> (Principal, RpcContext, M) {
        (self.principal, self.context, self.message)
    }
}

impl<M: Request> Request for Authenticated<M> {
    type Reply = M::Reply;
    type Error = M::Error;
}
