//! Explicit native HTTP contracts over the application's existing bounded runtime.
//!
//! Prepare registrations before building the runtime, then mount them in a
//! [`NativeApiBuilder`]. This is not ASP.NET, CLR discovery or Asp.Versioning.
//! Legacy [`crate::ApiBuilder`] contracts and defaults remain unchanged.

mod binding;
mod cache;
mod contract;
mod endpoint;
mod input;
mod problem;
mod rate;
mod response;
mod router;
mod session;
mod version;

pub use binding::{BindingPolicy, Parameter, ParameterSource, Parameters, RouteParseFailure};
pub use cache::{CacheEligibility, OutputCache, OutputCachePolicy, ResponseCache};
pub use contract::{BodyMode, EndpointContract, HttpMethod, MultipartLimits, RoutePrefixes};
pub use endpoint::{
    Endpoint, EndpointContext, EndpointEffect, EndpointOutput, EndpointRegistration, ReplayMode,
    ReplayPolicy, ReplayScope,
};
pub use input::{FormData, MultipartReader, RequestInput, UploadedFile};
pub use problem::{Problem, Validation};
pub use rate::{RateAlgorithm, RateLimiter, RatePolicy};
pub use response::{EndpointResponse, MissingResponse, Negotiation, ResourceWriter, ResponseKind};
pub use router::NativeApiBuilder;
pub use session::{CookieSession, CookieSessions, CsrfPolicy};
pub use version::{ApiVersion, VersionReader, Versioning};
