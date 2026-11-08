use raddle::wire::Envelope;

use crate::{Broker, Error};

const SCHEDULE: &str = r#"
local old = redis.call('HGET', KEYS[2], ARGV[1])
if old then
  if old ~= ARGV[2] or redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[3] or redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[4] then return -1 end
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[4], ARGV[1], ARGV[4])
redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
return 1
"#;
const RELEASE: &str = r#"
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local count = 0
local failed = 0
for _,id in ipairs(ids) do
  local body = redis.call('HGET', KEYS[2], id)
  local stream = redis.call('HGET', KEYS[3], id)
  if body and stream then
    local result = redis.pcall('XADD', stream, '*', 'body', body, 'message-id', id)
    if type(result) == 'table' and result.err then
      failed = failed + 1
      redis.call('ZADD', KEYS[1], ARGV[3], id)
    else
      redis.call('ZREM', KEYS[1], id)
      count = count + 1
    end
  else
    failed = failed + 1
    redis.call('ZADD', KEYS[1], ARGV[3], id)
  end
end
return {count, failed}
"#;

impl Broker {
    /// Atomically stores an envelope in native sorted-set scheduling state. The namespace
    /// is application-trusted; its four keys and target streams must share a cluster slot.
    /// Identity/content is retained after dispatch so uncertain scheduling retries do not
    /// republish completed work. The application must manage long-term index retention.
    pub async fn schedule(
        &self,
        namespace: &str,
        alias: &str,
        envelope: &Envelope,
        unix_millis: u64,
    ) -> Result<(), Error> {
        crate::config::key(namespace)?;
        let target = self
            .routes
            .get(alias)
            .ok_or(Error::Invalid("schedule alias"))?;
        if unix_millis == 0 || unix_millis > 9_007_199_254_740_991 {
            return Err(Error::Invalid("schedule timestamp"));
        }
        envelope
            .validate(self.config.envelope)
            .map_err(|_| Error::Invalid("envelope"))?;
        let bytes = envelope.to_vec().map_err(|_| Error::Invalid("envelope"))?;
        let script = redis::Script::new(SCHEDULE);
        let result: i64 = self
            .script(
                script
                    .key(format!("{namespace}:due"))
                    .key(format!("{namespace}:body"))
                    .key(format!("{namespace}:target"))
                    .key(format!("{namespace}:time"))
                    .arg(envelope.id().to_string())
                    .arg(bytes)
                    .arg(target)
                    .arg(unix_millis),
            )
            .await?;
        if result < 0 {
            return Err(Error::Invalid("schedule identity conflict"));
        }
        Ok(())
    }
    /// Caller/host-owned bounded scheduler pass. No hidden timer or orphan worker.
    /// Atomic script removes each due entry only after its XADD succeeds. An invalid
    /// target remains scheduled with the configured retry delay, preventing poison-head
    /// starvation even when the caller requests a one-member page.
    pub async fn release_due(
        &self,
        namespace: &str,
        unix_millis: u64,
        limit: usize,
    ) -> Result<SchedulePass, Error> {
        crate::config::key(namespace)?;
        if limit == 0 || limit > usize::from(self.config.prefetch) {
            return Err(Error::Invalid("schedule page"));
        }
        let retry_delay = self.config.schedule_retry_delay.as_millis() as u64;
        if unix_millis > 9_007_199_254_740_991 - retry_delay {
            return Err(Error::Invalid("schedule timestamp"));
        }
        let script = redis::Script::new(RELEASE);
        let (released, failed): (usize, usize) = self
            .script(
                script
                    .key(format!("{namespace}:due"))
                    .key(format!("{namespace}:body"))
                    .key(format!("{namespace}:target"))
                    .arg(unix_millis)
                    .arg(limit)
                    .arg(unix_millis + retry_delay),
            )
            .await?;
        Ok(SchedulePass { released, failed })
    }
}

/// Bounded scheduler outcome; failures retain their sorted-set membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulePass {
    /// Entries confirmed by XADD and removed from scheduling.
    pub released: usize,
    /// Entries left pending because their target/body is invalid or unavailable.
    pub failed: usize,
}
