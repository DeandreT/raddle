use raddle::wire::{Envelope, WireError};
use redis::streams::StreamId;
use tokio::sync::OwnedSemaphorePermit;

use crate::{AckMode, Broker, Error, Source};

const SETTLE: &str = r#"
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 or p[1][2] ~= ARGV[3] or p[1][4] ~= tonumber(ARGV[4]) then return 0 end
if ARGV[5] == 'delete' then
  local r = redis.call('XACKDEL', KEYS[1], ARGV[1], 'ACKED', 'IDS', 1, ARGV[2])
  if r[1] == 1 or r[1] == 2 then return 1 else return 0 end
end
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
"#;

const DEAD_LETTER: &str = r#"
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 or p[1][2] ~= ARGV[3] or p[1][4] ~= tonumber(ARGV[4]) then return false end
local id = redis.call('XADD', KEYS[2], '*', 'body', ARGV[6], 'source-stream', KEYS[1], 'source-id', ARGV[2], 'attempt', ARGV[4], 'reason', ARGV[7])
if ARGV[5] == 'delete' then
  redis.call('XACKDEL', KEYS[1], ARGV[1], 'ACKED', 'IDS', 1, ARGV[2])
else
  redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
end
return id
"#;

const PROGRESS: &str = r#"
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 or p[1][2] ~= ARGV[3] or p[1][4] ~= tonumber(ARGV[4]) then return 0 end
local r = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], 'IDLE', 0, 'JUSTID')
return #r
"#;

/// An unsettled native entry. Private consumer/generation fencing prevents an old
/// owner from acknowledging an entry reclaimed by another receiver.
pub struct HeldMessage {
    broker: Broker,
    stream: String,
    group: String,
    consumer: String,
    source: Source,
    entry: StreamId,
    generation: usize,
    permit: Option<OwnedSemaphorePermit>,
    settled: bool,
    received_at: std::time::Instant,
}
impl HeldMessage {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        broker: Broker,
        stream: String,
        group: String,
        consumer: String,
        source: Source,
        entry: StreamId,
        generation: usize,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            broker,
            stream,
            group,
            consumer,
            source,
            entry,
            generation,
            permit: Some(permit),
            settled: false,
            received_at: std::time::Instant::now(),
        }
    }
    /// Stable native identity, even for malformed bodies.
    pub fn id(&self) -> &str {
        &self.entry.id
    }
    /// Redis's delivery/claim generation.
    pub fn attempt(&self) -> usize {
        self.generation
    }
    /// Caller-trusted source and tenant.
    pub fn source(&self) -> &Source {
        &self.source
    }
    /// Fenced idle reset for queued/running entries. JUSTID avoids changing the claim
    /// generation; a reclaimed receipt cannot be renewed. Extension is capped at one hour.
    pub async fn progress(&mut self) -> Result<(), Error> {
        self.check()?;
        if self.received_at.elapsed() > std::time::Duration::from_secs(3600) {
            return Err(Error::Lost);
        }
        let script = redis::Script::new(PROGRESS);
        let result: i64 = self
            .broker
            .script(
                script
                    .key(&self.stream)
                    .arg(&self.group)
                    .arg(&self.entry.id)
                    .arg(&self.consumer)
                    .arg(self.generation),
            )
            .await?;
        if result != 1 {
            return Err(Error::Lost);
        }
        Ok(())
    }
    /// Bounded validated envelope, without losing settlement ownership on decode failure.
    pub fn decode(&self) -> Result<Envelope, WireError> {
        Envelope::from_slice(self.body()?, self.broker.config.envelope)
    }
    /// Bounded raw native body. Other native fields have separate small metadata limits.
    pub fn body(&self) -> Result<&[u8], WireError> {
        self.check()
            .map_err(|_| WireError::Invalid("settled Redis receipt"))?;
        if self.entry.map.len() > 32 {
            return Err(WireError::Invalid("stream fields"));
        }
        for (key, value) in &self.entry.map {
            let redis::Value::BulkString(bytes) = value else {
                return Err(WireError::Invalid("stream field type"));
            };
            let limit = if key == "body" {
                self.broker.config.envelope.max_bytes
            } else {
                2048
            };
            if key.is_empty() || key.len() > 64 || bytes.len() > limit {
                return Err(WireError::Invalid("stream metadata limit"));
            }
        }
        match self.entry.map.get("body") {
            Some(redis::Value::BulkString(bytes)) => Ok(bytes),
            _ => Err(WireError::Invalid("missing stream body")),
        }
    }
    /// Physical stream identity for caller-owned native/foreign mapper scoping.
    pub fn stream(&self) -> &str {
        &self.stream
    }
    /// Consumer group identity; physical receiver IDs are deliberately not stable IDs.
    pub fn group(&self) -> &str {
        &self.group
    }
    /// Runs a custom raw mapper without surrendering the receipt on failure.
    /// Scope any derived identity by trusted source, stream, group and native entry ID.
    pub fn decode_with<F>(&self, mapper: F) -> Result<Envelope, WireError>
    where
        F: FnOnce(&[u8]) -> Result<Envelope, WireError>,
    {
        let envelope = mapper(self.body()?)?;
        envelope.validate(self.broker.config.envelope)?;
        Ok(envelope)
    }
    /// Atomically fences ownership and acknowledges after effects/inbox custody committed.
    /// Redis acceptance is not proof that caller-side database custody actually committed.
    pub async fn complete(&mut self) -> Result<(), Error> {
        self.check()?;
        self.settled = true;
        let script = redis::Script::new(SETTLE);
        let result: i64 = self
            .broker
            .script(
                script
                    .key(&self.stream)
                    .arg(&self.group)
                    .arg(&self.entry.id)
                    .arg(&self.consumer)
                    .arg(self.generation)
                    .arg(self.mode()),
            )
            .await?;
        self.permit.take();
        if result != 1 {
            return Err(Error::Lost);
        }
        Ok(())
    }
    /// Confirms the raw DLQ copy before acknowledging, inside one atomic Redis script.
    /// Keys must share a hash slot on Redis Cluster. A failed copy never acknowledges.
    pub async fn dead_letter(&mut self, alias: &str, reason: &str) -> Result<String, Error> {
        self.check()?;
        let target = self
            .broker
            .routes
            .get(alias)
            .ok_or(Error::Invalid("DLQ alias"))?;
        if target == &self.stream {
            return Err(Error::Invalid("DLQ must differ from source"));
        }
        crate::config::name(reason)?;
        let body: Vec<u8> = self
            .entry
            .get("body")
            .ok_or(Error::Invalid("missing body"))?;
        if body.len() > self.broker.config.envelope.max_bytes {
            return Err(Error::Invalid("DLQ body limit"));
        }
        self.settled = true;
        let script = redis::Script::new(DEAD_LETTER);
        let result: Option<String> = self
            .broker
            .script(
                script
                    .key(&self.stream)
                    .key(target)
                    .arg(&self.group)
                    .arg(&self.entry.id)
                    .arg(&self.consumer)
                    .arg(self.generation)
                    .arg(self.mode())
                    .arg(body)
                    .arg(reason),
            )
            .await?;
        self.permit.take();
        result.ok_or(Error::Lost)
    }
    /// Leaves the entry pending for idle reclamation without acknowledging or republishing.
    pub fn retry(mut self) {
        self.permit.take();
    }
    fn check(&self) -> Result<(), Error> {
        if self.settled {
            Err(Error::Lost)
        } else {
            Ok(())
        }
    }
    fn mode(&self) -> &'static str {
        match self.broker.config.acknowledgement {
            AckMode::Retain => "retain",
            AckMode::DeleteWhenAcknowledged => "delete",
        }
    }
}
