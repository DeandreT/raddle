//! Opt-in, application-authorized dead-letter management over PostgreSQL custody.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::extract::rejection::JsonRejection;
use axum::routing::post;
use axum::{Extension, Json};
use chrono::Utc;
use raddle_postgres::{DeadLetterQuery, Store};
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::api::json_error;
use crate::{ApiBuilder, ApiError, Principal, specification};

/// Separately authorized management operations. Search includes private message data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeadLetterAction {
    /// Read canonical failed messages, including their bounded headers and payloads.
    Search,
    /// Requeue eligible failed deliveries under their original identities.
    Replay,
    /// Retire failed deliveries while retaining deduplication tombstones and audits.
    Discard,
}

/// Required application policy; there is deliberately no permissive implementation.
///
/// Check an explicit operation-specific role in `authorize`. `confirm` must verify
/// application-owned approval (for example, a signed action/tenant/ID-bound challenge),
/// not merely the presence of the caller's confirmation string. The principal's
/// tenant remains the only storage scope; neither callback can broaden it.
pub trait DeadLetterAuthorizer: Send + Sync + 'static {
    /// Authorizes the current verified identity for this particular action.
    fn authorize(&self, principal: &Principal, action: DeadLetterAction) -> Result<(), ApiError>;
    /// Confirms this precise bounded mutation after operation authorization.
    fn confirm(
        &self,
        principal: &Principal,
        action: DeadLetterAction,
        request: &DeadLetterMutation,
    ) -> Result<(), ApiError>;
}

/// Bounded cursor query. Unknown fields, including caller-selected tenants, are rejected.
#[derive(Default, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DeadLetterSearch {
    /// At most ten canonical envelopes per page; omitted means ten.
    #[schemars(range(min = 1, max = 10))]
    pub limit: Option<u32>,
    /// Exclusive durable sequence cursor; omitted means the beginning.
    #[schemars(range(min = 0))]
    pub after: i64,
    /// Exact bounded destination alias.
    #[schemars(length(min = 1, max = 128))]
    pub destination: Option<String>,
    /// Exact stable wire contract identifier.
    #[schemars(length(min = 1, max = 128))]
    pub contract: Option<String>,
    /// Exact classified failure code, not an exception-message search.
    #[schemars(length(min = 1, max = 64))]
    pub error_code: Option<String>,
}

/// One failed delivery and its validated canonical wire data, not a .NET wire DTO.
#[derive(Serialize, JsonSchema)]
pub struct DeadLetterView {
    /// Delivery identity used by mutations; distinct from message identity.
    pub id: Uuid,
    /// Original message identity, unchanged by replay and discard.
    pub message_id: Uuid,
    /// Durable sequence used for cursor pagination.
    pub sequence: i64,
    /// Configured delivery destination.
    pub destination: String,
    /// Validated raddle envelope including bounded metadata and actual failed payload.
    pub envelope: Value,
}

/// Fixed read outcome, without exposing the kind of protected or corrupt content.
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeadLetterReadOutcome {
    /// The candidate's payload cannot safely be returned by this management view.
    PayloadUnavailable,
}

/// An unreadable candidate represented only by trusted persisted custody metadata.
#[derive(Serialize, JsonSchema)]
pub struct DeadLetterUnavailableView {
    /// Persisted delivery ID within the authenticated tenant, not an envelope claim.
    pub id: Uuid,
    /// Persisted sequence; pagination advances past this candidate.
    pub sequence: i64,
    /// Fixed classification, with no payload bytes, claimed identity or parser text.
    pub outcome: DeadLetterReadOutcome,
}

/// A bounded scan page. An empty page may still have a continuation cursor.
#[derive(Serialize, JsonSchema)]
pub struct DeadLetterPage {
    /// Matching canonical messages, at most the requested limit.
    pub items: Vec<DeadLetterView>,
    /// Payload-free unreadable candidates. Combined with `items`, bounded by the
    /// requested candidate limit. Their contract cannot be verified by this view.
    pub unavailable: Vec<DeadLetterUnavailableView>,
    /// Continue here, or stop when null. This is not a snapshot across requests.
    pub next_after: Option<i64>,
}

/// Explicit mutation input. Tenant selection and arbitrary payload editing are not supported.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeadLetterMutation {
    /// One to 32 delivery IDs. Malformed individual IDs fail independently.
    #[schemars(length(min = 1, max = 32))]
    pub ids: Vec<String>,
    /// Bounded operator reason retained with each committed mutation's audit.
    #[schemars(length(min = 1, max = 512))]
    pub reason: String,
    /// Application-defined approval, verified by the required confirmation callback.
    #[schemars(length(min = 1, max = 128))]
    pub confirmation: String,
}

/// Fixed payload-free per-ID outcome. Only committed writes report success.
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeadLetterOutcome {
    /// The transaction committed and the original delivery is ready again.
    Replayed,
    /// The transaction committed and the delivery is a retained discarded tombstone.
    Discarded,
    /// Malformed, nil, oversized, or duplicate ID; other IDs were still considered.
    Invalid,
    /// Missing, another tenant, not dead, expired replay, or concurrently locked.
    NotEligible,
    /// Backend failure or deadline: reconcile state/audit before retrying this ID.
    Unknown,
}

/// A result identifies the input position without echoing untrusted strings.
#[derive(Serialize, JsonSchema)]
pub struct DeadLetterResult {
    /// Zero-based index in the submitted list.
    pub index: usize,
    /// Fixed classification with no backend or message error text.
    pub outcome: DeadLetterOutcome,
}

/// Independent mutation results; 200 does not imply that every ID succeeded.
#[derive(Serialize, JsonSchema)]
pub struct DeadLetterResults {
    /// Exactly one result for each submitted position.
    pub results: Vec<DeadLetterResult>,
}

fn bounded(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

impl ApiBuilder {
    /// Registers POST query, POST replay and DELETE discard at `/dead-letters/`.
    pub fn dead_letters(
        &mut self,
        store: Store,
        authorizer: impl DeadLetterAuthorizer,
    ) -> Result<&mut Self, ApiError> {
        self.dead_letters_at("/dead-letters", store, authorizer)
    }

    /// Registers an explicitly secured management slice at a custom prefix.
    ///
    /// Uses the API's bearer-only authentication (never cookie authentication).
    /// Each ID has an independent transaction and two-second execution deadline.
    /// Cancellation drops unfinished transactions; a lost connection or a deadline
    /// during commit can leave an unknown outcome, never fabricated acceptance.
    pub fn dead_letters_at(
        &mut self,
        prefix: &str,
        store: Store,
        authorizer: impl DeadLetterAuthorizer,
    ) -> Result<&mut Self, ApiError> {
        if prefix.ends_with('/')
            || prefix.is_empty()
            || prefix
                .split('/')
                .any(|segment| matches!(segment, "." | ".."))
        {
            return Err(ApiError::invalid());
        }
        let root = format!("{prefix}/");
        let replay = format!("{prefix}/replay");
        let original = self.registered.clone();
        let index = self.registered.len() + 1;
        for (path, method) in [(&root, "post"), (&replay, "post"), (&root, "delete")] {
            if let Err(error) = self.reserve(path, method) {
                self.registered = original;
                return Err(error);
            }
        }
        let query = specification::schema::<DeadLetterSearch>(
            "DeadLetterSearch",
            Contract::Deserialize,
            &mut self.schemas,
        );
        let page = specification::schema::<DeadLetterPage>(
            "DeadLetterPage",
            Contract::Serialize,
            &mut self.schemas,
        );
        let mutation = specification::schema::<DeadLetterMutation>(
            "DeadLetterMutation",
            Contract::Deserialize,
            &mut self.schemas,
        );
        let results = specification::schema::<DeadLetterResults>(
            "DeadLetterResults",
            Contract::Serialize,
            &mut self.schemas,
        );
        for (path, method, offset, request, response) in [
            (&root, "post", 0, query, page),
            (&replay, "post", 1, mutation.clone(), results.clone()),
            (&root, "delete", 2, mutation, results),
        ] {
            self.paths.entry(path.clone()).or_insert_with(|| json!({}))[method] =
                specification::operation(method, index + offset, Some(request), response, 200);
        }
        let authorizer: Arc<dyn DeadLetterAuthorizer> = Arc::new(authorizer);
        let search_store = store.clone();
        let search_authorizer = Arc::clone(&authorizer);
        let discard_store = store.clone();
        let discard_authorizer = Arc::clone(&authorizer);
        self.router = self.router.clone().route(&root, post(
            move |Extension(principal): Extension<Principal>, body: Result<Json<DeadLetterSearch>, JsonRejection>| {
                let store = search_store.clone();
                let authorizer = Arc::clone(&search_authorizer);
                async move {
                    authorizer.authorize(&principal, DeadLetterAction::Search)?;
                    let Json(query) = body.map_err(json_error)?;
                    let query = DeadLetterQuery {
                        limit: query.limit.unwrap_or(10), after: query.after,
                        destination: query.destination, contract: query.contract,
                        error_code: query.error_code,
                    };
                    let page = tokio::time::timeout(Duration::from_secs(2), store.search_dead_letters(principal.tenant(), &query))
                        .await.map_err(|_| ApiError::unavailable())??;
                    let items = page.items.into_iter().map(|item| {
                        Ok(DeadLetterView {
                            id: item.id, message_id: item.envelope.id(), sequence: item.sequence,
                            destination: item.destination,
                            envelope: serde_json::to_value(item.envelope).map_err(|_| ApiError::unavailable())?,
                        })
                    }).collect::<Result<_, ApiError>>()?;
                    let unavailable = page.unavailable.into_iter().map(|item| DeadLetterUnavailableView {
                        id: item.id, sequence: item.sequence, outcome: DeadLetterReadOutcome::PayloadUnavailable,
                    }).collect();
                    Ok::<_, ApiError>(Json(DeadLetterPage { items, unavailable, next_after: page.next_after }))
                }
            }
        ).delete(move |Extension(principal): Extension<Principal>, body: Result<Json<DeadLetterMutation>, JsonRejection>| {
            mutate(discard_store.clone(), Arc::clone(&discard_authorizer), principal, DeadLetterAction::Discard, body)
        }).layer(DefaultBodyLimit::max(16_384)))
        .route(&replay, post(move |Extension(principal): Extension<Principal>, body: Result<Json<DeadLetterMutation>, JsonRejection>| {
            mutate(store.clone(), Arc::clone(&authorizer), principal, DeadLetterAction::Replay, body)
        }).layer(DefaultBodyLimit::max(16_384)));
        Ok(self)
    }
}

async fn mutate(
    store: Store,
    authorizer: Arc<dyn DeadLetterAuthorizer>,
    principal: Principal,
    action: DeadLetterAction,
    body: Result<Json<DeadLetterMutation>, JsonRejection>,
) -> Result<Json<DeadLetterResults>, ApiError> {
    authorizer.authorize(&principal, action)?;
    let Json(request) = body.map_err(json_error)?;
    if !(1..=32).contains(&request.ids.len())
        || !bounded(&request.reason, 512)
        || !bounded(&request.confirmation, 128)
    {
        return Err(ApiError::invalid());
    }
    authorizer.confirm(&principal, action, &request)?;
    let mut seen = std::collections::HashSet::new();
    let mut results = Vec::with_capacity(request.ids.len());
    for (index, input) in request.ids.iter().enumerate() {
        let id = if input.len() == 36 {
            Uuid::parse_str(input).ok()
        } else {
            None
        };
        let outcome = match id.filter(|id| !id.is_nil() && seen.insert(*id)) {
            None => DeadLetterOutcome::Invalid,
            Some(id) => {
                let operation = async {
                    match action {
                        DeadLetterAction::Replay => {
                            store
                                .replay_dead_letter_bounded(
                                    principal.tenant(),
                                    id,
                                    principal.subject(),
                                    &request.reason,
                                    Utc::now(),
                                )
                                .await
                        }
                        DeadLetterAction::Discard => {
                            store
                                .discard_dead_letter(
                                    principal.tenant(),
                                    id,
                                    principal.subject(),
                                    &request.reason,
                                    Utc::now(),
                                )
                                .await
                        }
                        DeadLetterAction::Search => {
                            Err(raddle_postgres::Error::Invalid("invalid mutation action"))
                        }
                    }
                };
                match tokio::time::timeout(Duration::from_secs(2), operation).await {
                    Ok(Ok(true)) if action == DeadLetterAction::Replay => {
                        DeadLetterOutcome::Replayed
                    }
                    Ok(Ok(true)) => DeadLetterOutcome::Discarded,
                    Ok(Ok(false)) => DeadLetterOutcome::NotEligible,
                    _ => DeadLetterOutcome::Unknown,
                }
            }
        };
        results.push(DeadLetterResult { index, outcome });
    }
    Ok(Json(DeadLetterResults { results }))
}
