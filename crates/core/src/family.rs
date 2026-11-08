use std::any::{self, Any, TypeId};
use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::handler::{ErasedEvent, ErasedRequest, HandlerFuture, Subscriber, TypedRequest};
use crate::{Event, ExecutionContext, RegistrationError, Request, SubscriptionName};

/// A rejected family declaration or installation. No existing handler is replaced.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum FamilyRegistrationError {
    /// A family must declare at least one concrete member before installation.
    #[error("a message family has no members")]
    Empty,
    /// A concrete type already has a converter in this family.
    #[error("family member already declared for {message}")]
    DuplicateMember {
        /// Rust type name for diagnostics, not a wire identity.
        message: &'static str,
    },
    /// An explicit subscriber name or runtime request registration conflicts.
    #[error(transparent)]
    Registration(#[from] RegistrationError),
}

pub(crate) struct FamilyRequest {
    pub message: TypeId,
    pub diagnostic: &'static str,
    pub handler: Box<dyn ErasedRequest>,
}

/// Stages concrete requests for one shared typed behavior.
///
/// `T` is an application enum or typed adapter, not a discovered base class.
/// Each member moves into its converter and must have exactly the family's
/// reply and error types. Neither requests, replies, nor `T` need `Clone` or
/// `Sync`. The reusable behavior is stored once and deliberately shared by Arc.
/// Installation with [`crate::RuntimeBuilder::handle_family`] is all-or-nothing.
/// Declaring members alone does not register or execute anything.
///
/// Reply and error mismatches are rejected at compile time:
///
/// ```compile_fail
/// use std::convert::Infallible;
/// use raddle::{Request, RequestFamily};
/// struct Read;
/// impl Request for Read { type Reply = String; type Error = Infallible; }
/// let mut family = RequestFamily::new(|_: (), _| async { Ok::<u32, Infallible>(1) });
/// family.member::<Read>(|_| ()).unwrap(); // String is not u32
/// ```
///
/// ```compile_fail
/// use std::convert::Infallible;
/// use raddle::{Request, RequestFamily};
/// struct Read;
/// impl Request for Read { type Reply = u32; type Error = std::io::Error; }
/// let mut family = RequestFamily::new(|_: (), _| async { Ok::<u32, Infallible>(1) });
/// family.member::<Read>(|_| ()).unwrap(); // io::Error is not Infallible
/// ```
///
/// The behavior must be reusable, not consume captured state on its first call:
///
/// ```compile_fail
/// use std::convert::Infallible;
/// use raddle::RequestFamily;
/// let value = String::from("owned");
/// let _family = RequestFamily::new(move |_: (), _| async move {
///     Ok::<_, Infallible>(value)
/// });
/// ```
pub struct RequestFamily<T, R, E, F> {
    function: Arc<F>,
    pub(crate) members: Vec<FamilyRequest>,
    marker: PhantomData<fn(T) -> Result<R, E>>,
}

impl<T, R, E, F, Fut> RequestFamily<T, R, E, F>
where
    T: Send + 'static,
    R: Send + 'static,
    E: Error + Send + Sync + 'static,
    F: Fn(T, ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, E>> + Send + 'static,
{
    /// Binds a reusable behavior once. Context is local diagnostic identity.
    pub fn new(handler: F) -> Self {
        Self {
            function: Arc::new(handler),
            members: Vec::new(),
            marker: PhantomData,
        }
    }

    /// Declares one exact request and its owned conversion, without executing it.
    ///
    /// A duplicate type is rejected even with a different converter. A failed
    /// declaration leaves earlier members intact; they can still be installed.
    /// Conversion runs only after ordinary runtime admission, once per invocation.
    pub fn member<M>(
        &mut self,
        convert: impl Fn(M) -> T + Send + Sync + 'static,
    ) -> Result<&mut Self, FamilyRegistrationError>
    where
        M: Request<Reply = R, Error = E>,
    {
        let message = TypeId::of::<M>();
        if self.members.iter().any(|member| member.message == message) {
            return Err(FamilyRegistrationError::DuplicateMember {
                message: any::type_name::<M>(),
            });
        }
        let function = Arc::clone(&self.function);
        self.members.push(FamilyRequest {
            message,
            diagnostic: any::type_name::<M>(),
            handler: Box::new(TypedRequest::<M, _> {
                function: move |message, context| function(convert(message), context),
                marker: PhantomData,
            }),
        });
        Ok(self)
    }
}

/// Stages named concrete event subscriptions for one shared typed behavior.
///
/// Each member converts the runtime's existing `Arc<M>`, so enums can retain
/// shared input ownership without cloning event bodies. `T` need only be Send;
/// concrete events remain Send + Sync. Errors must match every member exactly.
/// Names are explicit and runtime-wide unique, including across family variants.
/// Independent behaviors use separate families with separate names.
///
/// ```compile_fail
/// use std::convert::Infallible;
/// use raddle::{Event, EventFamily};
/// struct Changed;
/// impl Event for Changed { type Error = std::io::Error; }
/// let mut family = EventFamily::new(|_: (), _| async { Ok::<(), Infallible>(()) });
/// family.member::<Changed>("audit".parse().unwrap(), |_| ()).unwrap();
/// ```
pub struct EventFamily<T, E, F> {
    function: Arc<F>,
    pub(crate) members: Vec<(TypeId, Subscriber)>,
    marker: PhantomData<fn(T) -> E>,
}

impl<T, E, F, Fut> EventFamily<T, E, F>
where
    T: Send + 'static,
    E: Error + Send + Sync + 'static,
    F: Fn(T, ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
{
    /// Binds one reusable subscriber behavior, shared across its concrete members.
    pub fn new(handler: F) -> Self {
        Self {
            function: Arc::new(handler),
            members: Vec::new(),
            marker: PhantomData,
        }
    }

    /// Declares a concrete event, unique name and shared-ownership converter.
    ///
    /// Duplicate types (ambiguous converters) and names are rejected without
    /// changing this declaration. Conversion occurs under each subscriber's
    /// ordinary admission permit. Registration order remains report order.
    pub fn member<M>(
        &mut self,
        name: SubscriptionName,
        convert: impl Fn(Arc<M>) -> T + Send + Sync + 'static,
    ) -> Result<&mut Self, FamilyRegistrationError>
    where
        M: Event<Error = E>,
    {
        let message = TypeId::of::<M>();
        if self.members.iter().any(|(member, _)| *member == message) {
            return Err(FamilyRegistrationError::DuplicateMember {
                message: any::type_name::<M>(),
            });
        }
        if self.members.iter().any(|(_, member)| member.name == name) {
            return Err(RegistrationError::DuplicateSubscription { name }.into());
        }
        self.members.push((
            message,
            Subscriber {
                name,
                handler: Box::new(FamilyEvent::<M, _, _> {
                    function: Arc::clone(&self.function),
                    convert,
                    marker: PhantomData,
                }),
            },
        ));
        Ok(self)
    }
}

struct FamilyEvent<M, F, C> {
    function: Arc<F>,
    convert: C,
    marker: PhantomData<fn(M)>,
}

#[derive(Debug, thiserror::Error)]
#[error("internal family event input type mismatch")]
struct InputTypeMismatch;

impl<M, T, F, C, Fut> ErasedEvent for FamilyEvent<M, F, C>
where
    M: Event,
    F: Fn(T, ExecutionContext) -> Fut + Send + Sync,
    C: Fn(Arc<M>) -> T + Send + Sync,
    Fut: Future<Output = Result<(), M::Error>> + Send,
{
    fn call(
        &self,
        message: Arc<dyn Any + Send + Sync>,
        context: ExecutionContext,
    ) -> HandlerFuture<'_> {
        Box::pin(async move {
            let message = message
                .downcast::<M>()
                .map_err(|_| Box::new(InputTypeMismatch) as Box<dyn Error + Send + Sync>)?;
            (self.function)((self.convert)(message), context)
                .await
                .map(|()| Box::new(()) as Box<dyn Any + Send>)
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        })
    }
}
