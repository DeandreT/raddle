use std::sync::Arc;
use std::time::Duration;

use redis::streams::{StreamAutoClaimReply, StreamPendingCountReply, StreamReadReply};
use tokio::sync::Semaphore;

use crate::config;
use crate::{Broker, Error, HeldMessage, Ownership, Source};

/// Initial position applies only when creating a group, never when resuming one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GroupStart {
    /// Read retained history.
    Beginning,
    /// Read only entries created after group creation.
    New,
}
/// Native pending-entry view, not a claim of message processing completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pending {
    /// Stable stream ID.
    pub id: String,
    /// Current Redis consumer.
    pub owner: String,
    /// Milliseconds since last delivery/claim.
    pub idle_ms: usize,
    /// Delivery generation used by settlement fencing.
    pub deliveries: usize,
}
/// Receiver with a unique physical consumer ID and finite outstanding window.
/// Dropped unsettled entries stay in the PEL for explicit reclamation.
pub struct Receiver {
    pub(crate) broker: Broker,
    pub(crate) stream: String,
    pub(crate) group: String,
    pub(crate) consumer: String,
    source: Source,
    permits: Arc<Semaphore>,
    reclaim_cursor: String,
    prefer_reclaim: bool,
}
impl Broker {
    /// Creates or verifies a stream group. Existing group offsets are never reset.
    pub async fn receiver(
        &self,
        alias: &str,
        group: &str,
        source: Source,
        ownership: Ownership,
        start: GroupStart,
    ) -> Result<Receiver, Error> {
        let stream = self
            .routes
            .get(alias)
            .ok_or(Error::Invalid("unknown alias"))?
            .clone();
        config::name(group)?;
        source.validate()?;
        if ownership == Ownership::Owned {
            let mut cmd = redis::cmd("XGROUP");
            cmd.arg("CREATE")
                .arg(&stream)
                .arg(group)
                .arg(match start {
                    GroupStart::Beginning => "0",
                    GroupStart::New => "$",
                })
                .arg("MKSTREAM");
            // Only BUSYGROUP is a benign create race; network/permission errors are not.
            let mut connection = self.connection.clone();
            let result: Result<String, redis::RedisError> =
                tokio::time::timeout(self.config.timeout, cmd.query_async(&mut connection))
                    .await
                    .map_err(|_| Error::Uncertain)?;
            if let Err(error) = result
                && error.code() != Some("BUSYGROUP")
            {
                return Err(Error::Uncertain);
            }
        }
        // A bounded group-specific command verifies existence without enumerating
        // every group or consumer in an externally managed stream.
        let _: StreamPendingCountReply = self
            .query(
                redis::cmd("XPENDING")
                    .arg(&stream)
                    .arg(group)
                    .arg("-")
                    .arg("+")
                    .arg(1),
            )
            .await?;
        Ok(Receiver {
            broker: self.clone(),
            stream,
            group: group.to_owned(),
            consumer: format!("raddle-{}", uuid::Uuid::new_v4()),
            source,
            permits: Arc::new(Semaphore::new(usize::from(self.config.prefetch))),
            reclaim_cursor: "0-0".to_owned(),
            prefer_reclaim: true,
        })
    }
}
impl Receiver {
    /// Physical consumer ID; this receiver exclusively owns it.
    pub fn consumer_id(&self) -> &str {
        &self.consumer
    }
    /// Bounded native pending diagnostics; never reads another tenant's configured stream.
    pub async fn pending(&self, limit: usize) -> Result<Vec<Pending>, Error> {
        self.limit(limit)?;
        let entries: StreamPendingCountReply = self
            .broker
            .query(
                redis::cmd("XPENDING")
                    .arg(&self.stream)
                    .arg(&self.group)
                    .arg("-")
                    .arg("+")
                    .arg(limit),
            )
            .await?;
        Ok(entries
            .ids
            .into_iter()
            .map(|p| Pending {
                id: p.id,
                owner: p.consumer,
                idle_ms: p.last_delivered_ms,
                deliveries: p.times_delivered,
            })
            .collect())
    }
    /// Alternates pending/new priority so a poison pending entry cannot starve new
    /// work in one-member pages. All commands, pending lookups
    /// and waits share one total deadline and one page/window bound.
    /// Use a dedicated connection for blocking receive; a shared multiplexer can head-of-line block.
    pub async fn receive(
        &mut self,
        limit: usize,
        wait: Duration,
        reclaim_after: Duration,
    ) -> Result<Vec<HeldMessage>, Error> {
        self.limit(limit)?;
        config::wait(wait)?;
        if reclaim_after < Duration::from_millis(1) || reclaim_after > Duration::from_secs(86_400) {
            return Err(Error::Invalid("reclaim idle"));
        }
        let count = limit.min(self.permits.available_permits());
        if count == 0 {
            return Ok(Vec::new());
        }
        let deadline = tokio::time::Instant::now() + wait;
        let mut result = Vec::with_capacity(count);
        let mut entries = Vec::with_capacity(count);
        if !self.prefer_reclaim {
            let reply: StreamReadReply = match tokio::time::timeout_at(
                deadline,
                self.broker.query(
                    redis::cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(&self.group)
                        .arg(&self.consumer)
                        .arg("COUNT")
                        .arg(count)
                        .arg("STREAMS")
                        .arg(&self.stream)
                        .arg(">"),
                ),
            )
            .await
            {
                Ok(reply) => reply?,
                Err(_) => return Ok(result),
            };
            for key in reply.keys {
                entries.extend(key.ids);
            }
        }
        self.prefer_reclaim = !self.prefer_reclaim;
        if entries.len() < count {
            let claimed: StreamAutoClaimReply = match tokio::time::timeout_at(
                deadline,
                self.broker.query(
                    redis::cmd("XAUTOCLAIM")
                        .arg(&self.stream)
                        .arg(&self.group)
                        .arg(&self.consumer)
                        .arg(reclaim_after.as_millis() as u64)
                        .arg(&self.reclaim_cursor)
                        .arg("COUNT")
                        .arg(count - entries.len()),
                ),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => return Ok(result),
            };
            self.reclaim_cursor = claimed.next_stream_id;
            entries.extend(claimed.claimed);
        }
        if entries.len() < count && tokio::time::Instant::now() < deadline {
            // Leave time for fencing lookups, and never block behind reclaimed work.
            let remaining = deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis()
                .saturating_sub(20)
                .max(1) as u64;
            let mut command = redis::cmd("XREADGROUP");
            command
                .arg("GROUP")
                .arg(&self.group)
                .arg(&self.consumer)
                .arg("COUNT")
                .arg(count - entries.len());
            if entries.is_empty() && remaining > 1 {
                command.arg("BLOCK").arg(remaining);
            }
            command.arg("STREAMS").arg(&self.stream).arg(">");
            let read: Result<Result<StreamReadReply, Error>, _> =
                tokio::time::timeout_at(deadline, self.broker.query(&command)).await;
            match read {
                Ok(Ok(reply)) => {
                    for key in reply.keys {
                        entries.extend(key.ids);
                    }
                }
                Ok(Err(error)) if entries.is_empty() => return Err(error),
                _ => {}
            }
        }
        for entry in entries.into_iter().take(count) {
            let pending: StreamPendingCountReply = match tokio::time::timeout_at(
                deadline,
                self.broker.query(
                    redis::cmd("XPENDING")
                        .arg(&self.stream)
                        .arg(&self.group)
                        .arg(&entry.id)
                        .arg(&entry.id)
                        .arg(1),
                ),
            )
            .await
            {
                Ok(Ok(p)) => p,
                _ => break,
            };
            let Some(pending) = pending.ids.into_iter().next() else {
                continue;
            };
            if pending.consumer != self.consumer {
                continue;
            }
            let permit = Arc::clone(&self.permits)
                .try_acquire_owned()
                .map_err(|_| Error::Lost)?;
            result.push(HeldMessage::new(
                self.broker.clone(),
                self.stream.clone(),
                self.group.clone(),
                self.consumer.clone(),
                self.source.clone(),
                entry,
                pending.times_delivered,
                permit,
            ));
        }
        Ok(result)
    }
    fn limit(&self, limit: usize) -> Result<(), Error> {
        if limit == 0 || limit > usize::from(self.broker.config.prefetch) {
            return Err(Error::Invalid("page limit"));
        }
        Ok(())
    }
}
