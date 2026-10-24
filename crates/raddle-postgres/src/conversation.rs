use chrono::{DateTime, Utc};
use raddle::wire::{Envelope, WireMessage};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::{Delivery, DeliveryKind, Error, Scope, Store};

/// Durable request state; a timeout does not imply remote failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConversationStatus {
    /// No terminal outcome has been observed.
    Pending,
    /// A trusted matching reply was committed.
    Completed,
    /// Cancellation was requested; remote effects may already have occurred.
    Cancelled,
    /// The deadline elapsed before a reply was observed.
    TimedOut,
}

/// A tenant-scoped persisted conversation that survives caller restarts.
pub struct Conversation {
    /// Request identity used for correlation.
    pub id: Uuid,
    /// Durable terminal or pending state.
    pub status: ConversationStatus,
    /// Original reply envelope when completed.
    pub reply: Option<Envelope>,
}

/// Whether an incoming reply changed the durable conversation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReplyDisposition {
    /// First trusted reply before the deadline.
    Accepted,
    /// Duplicate, late, unknown, or terminal conversation; no mutation occurred.
    Ignored,
    /// Source or expected reply contract did not match.
    Rejected,
}

#[derive(Serialize, Deserialize)]
struct Cancellation {
    request_id: Uuid,
}
impl WireMessage for Cancellation {
    const NAME: &'static str = "raddle.cancel";
    const VERSION: u32 = 1;
}

/// Explicit trusted routing and deadline for a distributed conversation.
pub struct RequestOptions {
    /// Configured remote destination alias.
    pub destination: String,
    /// Configured, allowlisted reply endpoint alias.
    pub reply_endpoint: String,
    /// Authenticated source expected for the response.
    pub responder: String,
    /// Stable reply contract name.
    pub reply_contract: String,
    /// Latest acceptable response time.
    pub deadline: DateTime<Utc>,
}

impl Scope<'_> {
    /// Persists a conversation and its request outbox record atomically.
    ///
    /// Reply addresses are configured aliases, never arbitrary inbound URLs.
    pub async fn request(
        &mut self,
        tenant: &str,
        request: &Envelope,
        options: &RequestOptions,
    ) -> Result<(), Error> {
        let RequestOptions {
            destination,
            reply_endpoint,
            responder,
            reply_contract,
            deadline,
        } = options;
        let result = async {
            for name in [destination, reply_endpoint, responder, reply_contract] { raddle::SubscriptionName::try_new(name).map_err(|_| Error::Invalid("invalid conversation routing"))?; }
            if *deadline <= request.created_at() { return Err(Error::Invalid("request deadline must follow creation")); }
            let request = request.clone().with_header("reply-endpoint".into(), reply_endpoint.into())?.with_header("reply-deadline".into(), deadline.to_rfc3339())?;
            let inserted = sqlx::query("INSERT INTO raddle_conversations(tenant,id,destination,responder,reply_contract,deadline) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING")
                .bind(tenant).bind(request.id()).bind(destination).bind(responder).bind(reply_contract).bind(deadline).execute(self.connection()).await?;
            if inserted.rows_affected() != 1 { return Err(Error::IdentityConflict); }
            let mut delivery = Delivery::new(DeliveryKind::Remote, destination, request.created_at());
            delivery.expires_at = Some(*deadline);
            self.stage("raddle.requests", tenant, &request, &[delivery]).await?;
            Ok(())
        }.await;
        if result.is_err() {
            self.mark_rollback_only();
        }
        result
    }

    /// Stages a responder's reply atomically, restricted to configured reply aliases.
    pub async fn reply(
        &mut self,
        tenant: &str,
        request: &Envelope,
        response: Envelope,
        allowed: &[raddle::SubscriptionName],
        now: DateTime<Utc>,
    ) -> Result<(), Error> {
        let result = async {
            let endpoint = request
                .headers()
                .get("reply-endpoint")
                .ok_or(Error::Invalid("request has no reply endpoint"))?;
            if !allowed.iter().any(|allowed| allowed.as_str() == endpoint) {
                return Err(Error::Invalid("reply endpoint is not authorized"));
            }
            let response = response.correlated(request.id(), request.id());
            self.stage(
                "raddle.replies",
                tenant,
                &response,
                &[Delivery::new(DeliveryKind::Remote, endpoint, now)],
            )
            .await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.mark_rollback_only();
        }
        result
    }

    /// Accepts a reply only for the configured tenant, responder, contract, and live deadline.
    pub async fn accept_reply(
        &mut self,
        tenant: &str,
        trusted_source: &str,
        reply: &Envelope,
        now: DateTime<Utc>,
    ) -> Result<ReplyDisposition, Error> {
        let Some(id) = reply.correlation_id() else {
            return Ok(ReplyDisposition::Rejected);
        };
        let result = async {
            let row = sqlx::query("SELECT responder,reply_contract,status,deadline FROM raddle_conversations WHERE tenant=$1 AND id=$2 FOR UPDATE")
                .bind(tenant).bind(id).fetch_optional(self.connection()).await?;
            let Some(row) = row else { return Ok(ReplyDisposition::Ignored); };
            if row.try_get::<&str,_>("responder")? != trusted_source || row.try_get::<&str,_>("reply_contract")? != reply.contract() { return Ok(ReplyDisposition::Rejected); }
            if row.try_get::<&str,_>("status")? != "pending" { return Ok(ReplyDisposition::Ignored); }
            if row.try_get::<DateTime<Utc>,_>("deadline")? <= now {
                sqlx::query("UPDATE raddle_conversations SET status='timed-out' WHERE tenant=$1 AND id=$2").bind(tenant).bind(id).execute(self.connection()).await?;
                return Ok(ReplyDisposition::Ignored);
            }
            sqlx::query("UPDATE raddle_conversations SET status='completed',response=$3 WHERE tenant=$1 AND id=$2")
                .bind(tenant).bind(id).bind(reply.to_vec()?).execute(self.connection()).await?;
            Ok(ReplyDisposition::Accepted)
        }.await;
        if result.is_err() {
            self.mark_rollback_only();
        }
        result
    }

    /// Commits local cancellation and a best-effort remote cancellation command together.
    pub async fn cancel_request(
        &mut self,
        tenant: &str,
        id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        let result = async {
            let destination: Option<String> = sqlx::query_scalar("UPDATE raddle_conversations SET status='cancelled' WHERE tenant=$1 AND id=$2 AND status='pending' RETURNING destination")
                .bind(tenant).bind(id).fetch_optional(self.connection()).await?;
            let Some(destination) = destination else { return Ok(false); };
            let cancel = Envelope::encode(&Cancellation { request_id: id })?.correlated(id, id);
            self.stage("raddle.requests", tenant, &cancel, &[Delivery::new(DeliveryKind::Remote, destination, now)]).await?;
            Ok(true)
        }.await;
        if result.is_err() {
            self.mark_rollback_only();
        }
        result
    }
}

impl Store {
    /// Reads durable conversation state, distinguishing deadline expiry from remote failure.
    pub async fn conversation(
        &self,
        tenant: &str,
        id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Option<Conversation>, Error> {
        let mut scope = self.begin().await?;
        sqlx::query("UPDATE raddle_conversations SET status='timed-out' WHERE tenant=$1 AND id=$2 AND status='pending' AND deadline <= $3")
            .bind(tenant).bind(id).bind(now).execute(scope.connection()).await?;
        let row = sqlx::query(
            "SELECT status,response FROM raddle_conversations WHERE tenant=$1 AND id=$2",
        )
        .bind(tenant)
        .bind(id)
        .fetch_optional(scope.connection())
        .await?;
        let result = row
            .map(|row| {
                let status = match row.try_get::<&str, _>("status")? {
                    "pending" => ConversationStatus::Pending,
                    "completed" => ConversationStatus::Completed,
                    "cancelled" => ConversationStatus::Cancelled,
                    "timed-out" => ConversationStatus::TimedOut,
                    _ => return Err(Error::Invalid("invalid conversation state")),
                };
                let reply = row
                    .try_get::<Option<Vec<u8>>, _>("response")?
                    .map(|bytes| Envelope::from_slice(&bytes, Default::default()))
                    .transpose()?;
                Ok(Conversation { id, status, reply })
            })
            .transpose()?;
        scope.commit(now).await?;
        Ok(result)
    }
}
