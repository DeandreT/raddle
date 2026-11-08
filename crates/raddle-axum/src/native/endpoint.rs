use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{FromRequestParts, Path};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use futures_util::future::BoxFuture;
use raddle::{Request, RuntimeBuilder};
use raddle_postgres::http_responses::{
    self, ClaimResult, ResponseClaim, ResponseKey, StoredResponse,
};
use raddle_postgres::{Scope, Store};
use schemars::JsonSchema;
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    CsrfPolicy, EndpointContract, EndpointResponse, Problem, RequestInput, ResourceWriter,
    Validation,
};
use crate::{Authenticator, Permission, Principal};

/// Explicit native endpoint with typed input, resource and domain error.
/// All runtime hooks, including binding and resource authorization, execute while
/// holding the *same* application's core admission permit. Do not recursively
/// invoke that runtime from a hook when its last permit may be held.
pub trait Endpoint: Send + Sync + 'static {
    /// Mixed-source application input, built explicitly by `bind`.
    type Input: Send + Sync + 'static;
    /// Primary JSON representation, also used for generated schemas.
    type Resource: Serialize + JsonSchema + Send + 'static;
    /// Typed domain failure, mapped safely by `map_error`.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Middleware may consume fields from the shared multipart cursor before binding.
    fn before_bind(
        &self,
        _input: &mut RequestInput,
    ) -> impl Future<Output = Result<(), Problem>> + Send {
        async { Ok(()) }
    }
    /// Constructs an immutable mixed-source request using the declared body reader.
    fn bind(
        &self,
        input: &mut RequestInput,
    ) -> impl Future<Output = Result<Self::Input, Problem>> + Send;
    /// Loads/checks resource permissions on *every* request, before cache lookup.
    /// The default still requires the authenticated Invoke permission globally.
    fn authorize(
        &self,
        _context: &mut EndpointContext,
        _input: &Self::Input,
    ) -> impl Future<Output = Result<(), Problem>> + Send {
        async { Ok(()) }
    }
    /// Explicit NoProblems sentinel continues; Reject stops before work/effects.
    fn validate(
        &self,
        _context: &mut EndpointContext,
        _input: &Self::Input,
    ) -> impl Future<Output = Validation> + Send {
        async { Validation::NoProblems }
    }
    /// Executes application behavior. Any replay scope is already the same scope
    /// used by claim/response recording; use `context.scope()` for business writes.
    fn handle(
        &self,
        context: &mut EndpointContext,
        input: Self::Input,
    ) -> impl Future<Output = Result<EndpointOutput<Self::Resource>, Self::Error>> + Send;
    /// Postprocesses the primary response before effects and transaction commit.
    fn after(
        &self,
        _context: &mut EndpointContext,
        _output: &mut EndpointOutput<Self::Resource>,
    ) -> impl Future<Output = Result<(), Problem>> + Send {
        async { Ok(()) }
    }
    /// Runs after admitted processing succeeds or fails. Authentication/rate/CSRF
    /// denials do not enter the endpoint lifecycle. Cancellation drops owned resources;
    /// Rust does not promise an async finally callback when its future is dropped.
    fn finally(
        &self,
        _context: &mut EndpointContext,
        _status: StatusCode,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// Typed/filtered error mapping; unhandled errors are redacted 500s.
    /// Match native enum variants here to keep mixed aggregate failures distinct.
    fn map_error(&self, _error: Self::Error) -> Problem {
        Problem::internal()
    }
    /// Ordered custom resource writers. First matching media substring wins.
    /// Evaluated once during preparation; metadata and requests share that frozen list.
    fn writers(&self) -> Vec<ResourceWriter<Self::Resource>> {
        Vec::new()
    }
    /// Runs only after a confirmed enrolled business commit. An error here cannot
    /// roll back committed work and must retain the response claim. In separate
    /// claim mode this precedes recording; in same-transaction mode it follows
    /// atomic response recording. Do not use this hook for transactional writes.
    fn after_commit(
        &self,
        _context: &mut EndpointContext,
    ) -> impl Future<Output = Result<(), Problem>> + Send {
        async { Ok(()) }
    }
    /// JSON/form request schema, separate from route/header/query inputs.
    /// Override for the exact body DTO; Serde controls converters and aliases.
    fn body_schema(&self) -> serde_json::Value {
        serde_json::json!({})
    }
}

/// One inline effect, executed after the primary result but before commit.
/// It receives the same transaction/context. External effects are not rollbackable.
pub type EndpointEffect =
    Box<dyn for<'a> FnOnce(&'a mut EndpointContext) -> BoxFuture<'a, Result<(), Problem>> + Send>;

/// Separates the primary HTTP response from secondary effects.
pub struct EndpointOutput<R> {
    /// First/primary return value.
    pub response: EndpointResponse<R>,
    /// Secondary returns. Never serialized as tuple elements.
    pub effects: Vec<EndpointEffect>,
}
impl<R> From<EndpointResponse<R>> for EndpointOutput<R> {
    fn from(response: EndpointResponse<R>) -> Self {
        Self {
            response,
            effects: Vec::new(),
        }
    }
}
impl<R> From<(EndpointResponse<R>, Vec<EndpointEffect>)> for EndpointOutput<R> {
    fn from((response, effects): (EndpointResponse<R>, Vec<EndpointEffect>)) -> Self {
        Self { response, effects }
    }
}

/// Per-call context. Trusted identity is separate from untrusted binding values.
pub struct EndpointContext {
    principal: Option<Principal>,
    /// Validated selected native API version, separate from raw caller headers.
    pub api_version: Option<String>,
    /// Original URI including raw query.
    pub uri: Uri,
    /// Original headers; these do not themselves grant permissions.
    pub headers: HeaderMap,
    /// Headers added by endpoint middleware to the first live response. Response
    /// replay intentionally does not restore these; version/cache policy headers
    /// are recomputed separately for every response.
    pub response_headers: HeaderMap,
    scope: Option<Scope<'static>>,
    // Context ownership keeps concurrency admission through Finally and header
    // postprocessing, not only through the handler/cache phase.
    _rate: Option<super::rate::RateGuard>,
    lifecycle_started: bool,
}
impl EndpointContext {
    /// Identity already verified by the application-supplied authenticator.
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }
    /// The exact enrolled replay/business transaction, never a hidden second scope.
    pub fn scope(&mut self) -> Result<&mut Scope<'static>, Problem> {
        self.scope
            .as_mut()
            .ok_or_else(|| Problem::internal().detail("Endpoint has no enrolled transaction"))
    }
}

/// Selected nonzero replay identity dimensions. Values always come from the
/// verified principal and stable endpoint name, never caller-selected tenant hints.
#[derive(Clone, Copy, Debug)]
pub struct ReplayScope {
    /// Include verified tenant.
    pub tenant: bool,
    /// Include verified subject.
    pub user: bool,
    /// Include stable registration name.
    pub endpoint: bool,
}

/// The crash boundary is an explicit API choice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReplayMode {
    /// Claim commits first; business commit precedes separate response recording.
    /// Crashes can leave an unanswered 409 until explicit expiry cleanup.
    SeparateClaim,
    /// Claim, business writes and response commit on the *same* supplied scope.
    /// A crash before commit rolls back all; concurrent duplicates return 409.
    SameTransaction,
}

/// Persisted response replay, not the legacy durable acceptance API.
#[derive(Clone)]
pub struct ReplayPolicy {
    /// Explicit PostgreSQL store used for claim and business scope.
    pub store: Store,
    /// Selected scope flags; all-false is rejected at startup.
    pub scope: ReplayScope,
    /// Claim transaction ownership.
    pub mode: ReplayMode,
    /// Whether a missing Idempotency-Key is a 400 (default recommended true).
    pub required: bool,
    /// Retention; rows remain honored until explicit store cleanup.
    pub ttl: Duration,
}
impl std::fmt::Debug for ReplayPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayPolicy")
            .field("scope", &self.scope)
            .field("mode", &self.mode)
            .field("required", &self.required)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}
impl ReplayPolicy {
    pub(crate) fn validate(&self) -> Result<(), Problem> {
        if (!self.scope.tenant && !self.scope.user && !self.scope.endpoint)
            || self.ttl.is_zero()
            || self.ttl > Duration::from_secs(86400 * 30)
        {
            return Err(Problem::invalid("response replay policy"));
        }
        Ok(())
    }
}

/// A prepared endpoint. Preparation registers its entire pipeline before runtime
/// freeze; mounting only installs routes over that same runtime.
pub struct EndpointRegistration<E: Endpoint> {
    pub(crate) endpoint: Arc<E>,
    pub(crate) contract: Arc<EndpointContract>,
    pub(crate) writers: Arc<Vec<ResourceWriter<E::Resource>>>,
}
impl<E: Endpoint> EndpointRegistration<E> {
    /// Validates before registration. One endpoint type owns one runtime pipeline;
    /// use distinct newtypes for independent definitions of the same business code.
    pub fn prepare(
        runtime: &mut RuntimeBuilder,
        contract: EndpointContract,
        endpoint: E,
    ) -> Result<Self, Problem> {
        contract.validate()?;
        let writers = endpoint.writers();
        if !writers.is_empty() && !contract.responses.contains(&super::ResponseKind::Json) {
            return Err(Problem::invalid(
                "resource writers require a JSON resource alternative",
            ));
        }
        if writers
            .iter()
            .any(|writer| HeaderValue::try_from(writer.media_type).is_err())
        {
            return Err(Problem::invalid("resource writer"));
        }
        if contract.replay.is_some()
            && (!writers.is_empty()
                || contract.responses.iter().all(|kind| {
                    matches!(
                        kind,
                        super::ResponseKind::Text | super::ResponseKind::Started
                    )
                }))
        {
            return Err(Problem::invalid("unsupported replay response"));
        }
        runtime
            .handle(|call: NativeCall<E>| async move { execute(call).await })
            .map_err(|_| Problem::invalid("duplicate endpoint type"))?;
        if contract.query_policy == super::BindingPolicy::Lenient {
            tracing::warn!(
                endpoint = contract.name,
                "native HTTP query parsing uses explicit 6.x leniency"
            );
        }
        Ok(Self {
            endpoint: Arc::new(endpoint),
            contract: Arc::new(contract),
            writers: Arc::new(writers),
        })
    }
}

pub(crate) struct NativeCall<E: Endpoint> {
    pub endpoint: Arc<E>,
    pub contract: Arc<EndpointContract>,
    pub request: axum::extract::Request,
    pub auth: Arc<dyn Authenticator>,
    pub url_version: Option<String>,
    pub writers: Arc<Vec<ResourceWriter<E::Resource>>>,
}
impl<E: Endpoint> Request for NativeCall<E> {
    type Reply = Response;
    type Error = Problem;
}

async fn execute<E: Endpoint>(call: NativeCall<E>) -> Result<Response, Problem> {
    let NativeCall {
        endpoint,
        contract,
        request,
        auth,
        url_version,
        writers,
    } = call;
    let (mut parts, body) = request.into_parts();
    let captures = Path::<BTreeMap<String, String>>::from_request_parts(&mut parts, &())
        .await
        .map(|Path(path)| path)
        .map_err(|_| Problem::new(StatusCode::NOT_FOUND))?;
    let request = axum::extract::Request::from_parts(parts, body);
    let mut input = RequestInput::new(request, captures, &contract);
    let selected = contract
        .versioning
        .as_ref()
        .map(|config| config.select(&input, url_version.as_deref()))
        .transpose()?
        .flatten();
    let mut context = EndpointContext {
        principal: None,
        api_version: selected.clone(),
        uri: input.uri.clone(),
        headers: input.parameters.headers().clone(),
        response_headers: HeaderMap::new(),
        scope: None,
        _rate: None,
        lifecycle_started: false,
    };
    let result = execute_inner(
        endpoint.as_ref(),
        &contract,
        &auth,
        &mut input,
        &mut context,
        writers.as_slice(),
    )
    .await;
    let mut response = match result {
        Ok(response) => response,
        Err(problem) => problem.into_response(),
    };
    if context.lifecycle_started {
        endpoint.finally(&mut context, response.status()).await;
    }
    if let Some(config) = &contract.versioning {
        config.apply(selected.as_deref(), &mut response)?;
    }
    if let Some(cache) = &contract.response_cache {
        cache.apply(&mut response)?;
    }
    Ok(response)
}

async fn execute_inner<E: Endpoint>(
    endpoint: &E,
    contract: &EndpointContract,
    auth: &Arc<dyn Authenticator>,
    input: &mut RequestInput,
    context: &mut EndpointContext,
    writers: &[ResourceWriter<E::Resource>],
) -> Result<Response, Problem> {
    if contract.anonymous {
        context.principal = auth.authenticate(input.parameters.headers()).ok();
    } else {
        let principal = auth
            .authenticate(input.parameters.headers())
            .map_err(|_| Problem::new(StatusCode::UNAUTHORIZED))?;
        principal
            .require(Permission::Invoke)
            .map_err(|_| Problem::new(StatusCode::FORBIDDEN))?;
        context.principal = Some(principal);
    }
    context._rate = contract
        .rate
        .as_ref()
        .map(|policy| policy.limiter.admit(&policy.name, context.principal()))
        .transpose()?;
    if let CsrfPolicy::Session(sessions) = &contract.csrf {
        sessions.check_csrf(input.parameters.headers(), context.principal())?;
    }
    context.lifecycle_started = true;
    for parameter in &contract.parameters {
        if !parameter.required {
            continue;
        }
        let present = match parameter.source {
            super::ParameterSource::Query => input.parameters.query.contains_key(&parameter.name),
            super::ParameterSource::Header => input
                .parameters
                .headers()
                .contains_key(parameter.name.as_str()),
            _ => true,
        };
        if !present {
            return Err(Problem::invalid(&parameter.name));
        }
    }
    // Capture exact bytes before application binding can normalize the request.
    let fingerprint = if contract.replay.is_some() {
        let method = contract.method.as_str().as_bytes();
        let uri = input.uri.clone();
        let raw = input.capture().await?;
        let mut hash = Sha256::new();
        for bytes in [
            method,
            uri.path().as_bytes(),
            uri.query().unwrap_or("").as_bytes(),
            raw.as_ref(),
        ] {
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        Some(<[u8; 32]>::from(hash.finalize()))
    } else {
        None
    };
    endpoint.before_bind(input).await?;
    let bound = endpoint.bind(input).await?;
    // This check cannot be bypassed by output-cache hits or response replay.
    endpoint.authorize(context, &bound).await?;
    if let Validation::Reject(problem) = endpoint.validate(context, &bound).await {
        return Err(problem);
    }
    let cache_slot = contract
        .output_cache
        .as_ref()
        .and_then(|policy| {
            policy
                .key(
                    &format!(
                        "{}@{}",
                        contract.name,
                        context.api_version.as_deref().unwrap_or("neutral")
                    ),
                    input,
                    context.principal(),
                )
                .map(|key| (policy, key))
        })
        .map(|(policy, key)| policy.store.slot(key, policy))
        .transpose()?
        .flatten();
    let mut cached = match cache_slot.as_ref() {
        Some(slot) => Some(slot.value.lock().await),
        None => None,
    };
    if let (Some(slot), Some(cached)) = (&cache_slot, &cached)
        && slot.fresh()
        && let Some(cached) = cached.as_ref()
    {
        return Ok(cached.response());
    }
    let mut response =
        execute_work(endpoint, contract, bound, context, fingerprint, writers).await?;
    if let Some(policy) = &contract.response_cache {
        policy.apply(&mut response)?;
    }
    if let (Some(policy), Some(slot), Some(cached)) =
        (&contract.output_cache, cache_slot.as_ref(), cached.as_mut())
        && slot.valid()
    {
        let (response, stored) = policy.store.capture(response).await?;
        **cached = stored;
        slot.filled(policy.ttl)?;
        return Ok(response);
    }
    Ok(response)
}

struct ReplayState {
    policy: ReplayPolicy,
    claim: ResponseClaim,
    committed: bool,
}
enum ReplayStart {
    Run(Option<ReplayState>),
    Return(Response),
}

async fn start_replay(
    contract: &EndpointContract,
    context: &mut EndpointContext,
    fingerprint: Option<[u8; 32]>,
) -> Result<ReplayStart, Problem> {
    let Some(policy) = &contract.replay else {
        return Ok(ReplayStart::Run(None));
    };
    let key = context
        .headers
        .get("idempotency-key")
        .map(|value| {
            value
                .to_str()
                .map_err(|_| Problem::invalid("Idempotency-Key"))
        })
        .transpose()?;
    let Some(key) = key else {
        return if policy.required {
            Err(Problem::invalid("Idempotency-Key"))
        } else {
            context.scope = Some(policy.store.begin().await.map_err(store_error)?);
            Ok(ReplayStart::Run(None))
        };
    };
    let tenant = if policy.scope.tenant {
        Some(
            context
                .principal()
                .ok_or_else(|| Problem::new(StatusCode::UNAUTHORIZED))?
                .tenant(),
        )
    } else {
        None
    };
    let user = if policy.scope.user {
        Some(
            context
                .principal()
                .ok_or_else(|| Problem::new(StatusCode::UNAUTHORIZED))?
                .subject(),
        )
    } else {
        None
    };
    let endpoint_identity = match context.api_version.as_deref() {
        Some(version) => format!(
            "{:x}",
            Sha256::digest(format!("{}\0{version}", contract.name).as_bytes())
        ),
        None => contract.name.clone(),
    };
    let key = ResponseKey::new(
        tenant,
        user,
        policy.scope.endpoint.then_some(endpoint_identity.as_str()),
        key,
    )
    .map_err(|_| Problem::invalid("Idempotency-Key"))?;
    let mut scope = policy.store.begin().await.map_err(store_error)?;
    let expires =
        Utc::now() + chrono::Duration::from_std(policy.ttl).map_err(|_| Problem::internal())?;
    match http_responses::claim(
        &mut scope,
        &key,
        &fingerprint.ok_or_else(Problem::internal)?,
        expires,
    )
    .await
    .map_err(store_error)?
    {
        ClaimResult::InFlight => Err(Problem::new(StatusCode::CONFLICT)),
        ClaimResult::Mismatch => Err(Problem::new(StatusCode::UNPROCESSABLE_ENTITY)),
        ClaimResult::Replay(response) => Ok(ReplayStart::Return(replayed(response)?)),
        ClaimResult::Claimed(claim) => {
            if policy.mode == ReplayMode::SeparateClaim {
                scope.commit(Utc::now()).await.map_err(store_error)?;
                scope = policy.store.begin().await.map_err(store_error)?;
            }
            context.scope = Some(scope);
            Ok(ReplayStart::Run(Some(ReplayState {
                policy: policy.clone(),
                claim,
                committed: false,
            })))
        }
        _ => Err(Problem::internal()),
    }
}

async fn execute_work<E: Endpoint>(
    endpoint: &E,
    contract: &EndpointContract,
    bound: E::Input,
    context: &mut EndpointContext,
    fingerprint: Option<[u8; 32]>,
    writers: &[ResourceWriter<E::Resource>],
) -> Result<Response, Problem> {
    let mut replay = match start_replay(contract, context, fingerprint).await? {
        ReplayStart::Return(response) => return Ok(response),
        ReplayStart::Run(replay) => replay,
    };
    let result = async {
        let mut output = endpoint
            .handle(context, bound)
            .await
            .map_err(|error| endpoint.map_error(error))?;
        endpoint.after(context, &mut output).await?;
        if !contract.responses.contains(&output.response.kind()) {
            return Err(Problem::internal().detail("Undeclared endpoint response"));
        }
        for effect in output.effects {
            effect(context).await?;
        }
        let mut rendered = output.response.render(
            &context.headers,
            contract.missing,
            contract.negotiation,
            writers,
        )?;
        rendered
            .response
            .headers_mut()
            .extend(context.response_headers.clone());
        let Some(replay) = replay.as_mut() else {
            if let Some(scope) = context.scope.take()
                && rendered.response.status().is_success()
            {
                scope.commit(Utc::now()).await.map_err(store_error)?;
                endpoint.after_commit(context).await?;
            }
            return Ok(rendered.response);
        };
        if rendered.response.status().is_client_error()
            || rendered.response.status().is_server_error()
        {
            // A rejected response is a precommit refusal, never a committed write.
            context.scope.take();
            if replay.policy.mode == ReplayMode::SeparateClaim {
                replay
                    .policy
                    .store
                    .release_http_response(&replay.claim)
                    .await
                    .map_err(store_error)?;
            }
            return Ok(rendered.response);
        }
        let (response, stored) = if rendered.replayable {
            capture_response(rendered.response).await?
        } else {
            (rendered.response, None)
        };
        let mut scope = context.scope.take().ok_or_else(Problem::internal)?;
        if replay.policy.mode == ReplayMode::SameTransaction
            && let Some(stored) = &stored
        {
            http_responses::complete(&mut scope, &replay.claim, stored)
                .await
                .map_err(store_error)?;
        }
        // Once commit is attempted its outcome can be uncertain (connection loss).
        // Never compensate that claim: retain 409 rather than repeat committed work.
        replay.committed = true;
        if let Err(error) = scope.commit(Utc::now()).await {
            // Scope reports this only after its rollback was confirmed. Unlike
            // connection loss during COMMIT, it is safe to release a separate key.
            if matches!(
                error,
                raddle_postgres::Error::RollbackOnly | raddle_postgres::Error::StaleClaim
            ) {
                replay.committed = false;
            }
            return Err(store_error(error));
        }
        endpoint.after_commit(context).await?;
        if replay.policy.mode == ReplayMode::SeparateClaim
            && let Some(stored) = &stored
        {
            let mut record = replay.policy.store.begin().await.map_err(store_error)?;
            http_responses::complete(&mut record, &replay.claim, stored)
                .await
                .map_err(store_error)?;
            record.commit(Utc::now()).await.map_err(store_error)?;
        }
        if stored.is_none() {
            tracing::warn!(
                endpoint = contract.name,
                "unsupported response retained as unanswered replay claim"
            );
        }
        Ok(response)
    }
    .await;
    if result.is_err() {
        context.scope.take();
        if let Some(replay) = replay
            && !replay.committed
            && replay.policy.mode == ReplayMode::SeparateClaim
        {
            replay
                .policy
                .store
                .release_http_response(&replay.claim)
                .await
                .map_err(store_error)?;
        }
    }
    result
}

async fn capture_response(
    response: Response,
) -> Result<(Response, Option<StoredResponse>), Problem> {
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, 1_048_576)
        .await
        .map_err(|_| Problem::internal())?;
    let stored = StoredResponse {
        status: parts.status.as_u16(),
        body: bytes.to_vec(),
        location: parts
            .headers
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    };
    Ok((Response::from_parts(parts, Body::from(bytes)), Some(stored)))
}

fn replayed(stored: StoredResponse) -> Result<Response, Problem> {
    let mut response = Response::new(Body::from(stored.body.clone()));
    *response.status_mut() =
        StatusCode::from_u16(stored.status).map_err(|_| Problem::internal())?;
    if !stored.body.is_empty() {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    if let Some(location) = stored.location {
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::try_from(location).map_err(|_| Problem::internal())?,
        );
    }
    Ok(response)
}

fn store_error(_error: raddle_postgres::Error) -> Problem {
    Problem::new(StatusCode::SERVICE_UNAVAILABLE)
}
