//! Lua scripts. CLAIM and RENEW are upstream llm-d-async's, verbatim; the
//! others have the same effect on upstream's keys and take the store's
//! logical time as an argument, so expiry is judged by the caller's clock.
//!
//! Expiry is always an absolute time (`PEXPIREAT`, read back with
//! `PEXPIRETIME`), computed from a deadline or from that logical time. Where
//! the logical time is the wall clock this is exactly upstream's
//! `EX <deadline - now>` and `EXPIRE <ttl>`.
//!
//! PROMOTE moves members into queues named inside them, which Redis Cluster
//! cannot route; the store needs a single Redis.

use ::redis::Script;

/// KEYS: pending, claimed, owners, idx. ARGV: claim key, member, owner,
/// lease expiry (s).
const CLAIM: &str = r"
if redis.call('ZREM', KEYS[1], ARGV[2]) == 0 then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[4], ARGV[4], ARGV[1])
return 1
";

/// KEYS: claimed, idx, owners. ARGV: claim key, lease expiry (s), owner.
/// 1 renewed, -1 held by another owner, 0 gone.
const RENEW: &str = r"
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[3] then
  if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
    return -1
  end
  return 0
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
return 1
";

/// KEYS: pending, claimed, owners, idx. ARGV: now (s), limit. Returns every
/// claim whose lease lapsed to the queue at its deadline.
const RECLAIM: &str = r"
local lapsed = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local moved = 0
for _, field in ipairs(lapsed) do
  local member = redis.call('HGET', KEYS[2], field)
  if member then
    local deadline = 0
    local ok, env = pcall(cjson.decode, member)
    if ok and type(env) == 'table' and type(env['data']) == 'table' then
      deadline = tonumber(env['data']['deadline']) or 0
    end
    redis.call('ZADD', KEYS[1], deadline, member)
    redis.call('HDEL', KEYS[2], field)
    redis.call('HDEL', KEYS[3], field)
    moved = moved + 1
  end
  redis.call('ZREM', KEYS[4], field)
end
return moved
";

/// KEYS: pending, claimed, owners, idx. ARGV: claim key, owner, deadline.
const RELEASE: &str = r"
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] or ARGV[2] == '' then
  return 0
end
local member = redis.call('HGET', KEYS[2], ARGV[1])
if member then
  redis.call('ZADD', KEYS[1], tonumber(ARGV[3]), member)
end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
";

/// KEYS: claimed, owners, idx, retry, progress. ARGV: claim key, owner,
/// due (s), member, progress JSON ('' deletes it), progress expiry (ms).
const RETRY: &str = r"
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] or ARGV[2] == '' then
  return 0
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[4], ARGV[3], ARGV[4])
if ARGV[5] == '' then
  redis.call('DEL', KEYS[5])
else
  redis.call('SET', KEYS[5], ARGV[5], 'PXAT', ARGV[6])
end
return 1
";

/// KEYS: retry. ARGV: now (s), limit. Moves due retries to the queue their
/// envelope names, at their deadline, verbatim.
const PROMOTE: &str = r"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local moved = 0
for _, member in ipairs(due) do
  if redis.call('ZREM', KEYS[1], member) == 1 then
    local ok, env = pcall(cjson.decode, member)
    if ok and type(env) == 'table' and type(env['data']) == 'table' then
      local internal = env['internal']
      local queue = nil
      if type(internal) == 'table' and type(internal['request_queue_name']) == 'string' and internal['request_queue_name'] ~= '' then
        queue = internal['request_queue_name']
      elseif type(env['data']['request_queue_name']) == 'string' and env['data']['request_queue_name'] ~= '' then
        queue = env['data']['request_queue_name']
      end
      local deadline = tonumber(env['data']['deadline'])
      if queue and deadline then
        redis.call('ZADD', queue, deadline, member)
        moved = moved + 1
      end
    end
  end
end
return moved
";

/// The shared end of a finished request. KEYS from 4: result list, active,
/// cancel, progress, payload key, routes, blob types, blob expiry. ARGV from
/// 3: result JSON, result TTL (ms), now (ms), token, request blob, result
/// blob, result blob content type, route, channel.
const FINISH_TAIL: &str = r"
redis.call('LPUSH', KEYS[4], ARGV[3])
local ttl = tonumber(ARGV[4])
if ttl > 0 then
  redis.call('PEXPIREAT', KEYS[4], tonumber(ARGV[5]) + ttl)
end
redis.call('SADD', KEYS[9], ARGV[10])
local token = ARGV[6]
if token ~= '' then
  if redis.call('GET', KEYS[5]) == token then
    redis.call('DEL', KEYS[5])
  end
  if redis.call('GET', KEYS[6]) == token then
    redis.call('DEL', KEYS[6])
  end
end
redis.call('DEL', KEYS[7])
if KEYS[8] ~= '' then
  redis.call('DEL', KEYS[8])
end
if ARGV[7] ~= '' then
  redis.call('HDEL', KEYS[10], ARGV[7])
  redis.call('ZREM', KEYS[11], ARGV[7])
end
if ARGV[8] ~= '' then
  redis.call('HSET', KEYS[10], ARGV[8], ARGV[9])
  if ttl > 0 then
    redis.call('ZADD', KEYS[11], tonumber(ARGV[5]) + ttl, ARGV[8])
  end
end
redis.call('PUBLISH', ARGV[11], ARGV[10])
return 1
";

/// Ends a claim with a result. KEYS: claimed, owners, idx, then the tail's.
/// ARGV: claim key, owner, then the tail's. Only the claim's owner writes.
fn finish_claimed() -> String {
    format!(
        r"
local owner = redis.call('HGET', KEYS[2], ARGV[1])
if not owner or owner ~= ARGV[2] or ARGV[2] == '' then
  return 0
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
{FINISH_TAIL}"
    )
}

/// Finishes a request still in its queue. KEYS: pending, unused, unused,
/// then the tail's. ARGV: member, unused, then the tail's.
fn finish_pending() -> String {
    format!(
        r"
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
  return 0
end
{FINISH_TAIL}"
    )
}

/// KEYS: pending, blob types, blob expiry. ARGV: member, request blob.
const DISCARD: &str = r"
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
  return 0
end
if ARGV[2] ~= '' then
  redis.call('HDEL', KEYS[2], ARGV[2])
  redis.call('ZREM', KEYS[3], ARGV[2])
end
return 1
";

/// KEYS: active, cancel. ARGV: now (ms), marker TTL (ms). Marks the live
/// generation cancelled unless its deadline passed.
const CANCEL: &str = r"
local active = redis.call('GET', KEYS[1])
if not active then
  return 0
end
local expires = redis.call('PEXPIRETIME', KEYS[1])
if expires >= 0 and expires <= tonumber(ARGV[1]) then
  return 0
end
redis.call('SET', KEYS[2], active, 'PXAT', tonumber(ARGV[1]) + tonumber(ARGV[2]))
return 1
";

/// Drops a result list whose expiry passed. KEYS: list. ARGV: now (ms).
const EXPIRE_CHECK: &str = r"
local expires = redis.call('PEXPIRETIME', KEYS[1])
if expires >= 0 and expires <= tonumber(ARGV[1]) then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
";

/// Starts a popped result's blob retention. KEYS: blob types, blob expiry.
/// ARGV: blob, retention expiry (ms).
const RETAIN_BLOB: &str = r"
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
  return 0
end
local retained = tonumber(ARGV[2])
local current = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not current or retained < tonumber(current) then
  redis.call('ZADD', KEYS[2], retained, ARGV[1])
end
return 1
";

/// Leases the oldest result of a route: upstream's RECEIVE with the
/// caller's time. KEYS: route, claimed, owners, idx, tombstones. ARGV:
/// owner, lease (ms), now (ms), batch. Returns {1, payload, claim field},
/// {0} after skipping duplicates, or {} when idle.
const RECEIVE: &str = r"
local now = tonumber(ARGV[3])
local expires = redis.call('PEXPIRETIME', KEYS[1])
if expires >= 0 and expires <= now then
  redis.call('DEL', KEYS[1])
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, ARGV[4])
for i = #expired, 1, -1 do
  local claimID = expired[i]
  local payload = redis.call('HGET', KEYS[2], claimID)
  if payload then
    redis.call('RPUSH', KEYS[1], payload)
  end
  redis.call('HDEL', KEYS[2], claimID)
  redis.call('HDEL', KEYS[3], claimID)
  redis.call('ZREM', KEYS[4], claimID)
end
local lapsed = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', now, 'LIMIT', 0, ARGV[4])
if #lapsed > 0 then
  redis.call('ZREM', KEYS[5], unpack(lapsed))
end
local pending = math.min(redis.call('LLEN', KEYS[1]), tonumber(ARGV[4]))
if pending == 0 then
  return {}
end
local function claim(payload, claimID)
  redis.call('RPOP', KEYS[1])
  redis.call('HSET', KEYS[2], claimID, payload)
  redis.call('HSET', KEYS[3], claimID, ARGV[1])
  redis.call('ZADD', KEYS[4], now + tonumber(ARGV[2]), claimID)
end
for _ = 1, pending do
  local payload = redis.call('LINDEX', KEYS[1], -1)
  if not payload then
    return {}
  end
  local ok, result = pcall(cjson.decode, payload)
  if not ok or type(result) ~= 'table' or type(result['id']) ~= 'string' or result['id'] == '' then
    local claimID = string.char(0) .. 'unparsable-result' .. string.char(0) .. ARGV[1]
    claim(payload, claimID)
    return {1, payload, claimID}
  end
  local token = result['request_token']
  if token == nil then
    token = ''
  elseif type(token) ~= 'string' then
    local claimID = string.char(0) .. 'unparsable-result' .. string.char(0) .. ARGV[1]
    claim(payload, claimID)
    return {1, payload, claimID}
  end
  local claimID = result['id']
  if token ~= '' then
    claimID = claimID .. string.char(0) .. token
  end
  local tombstone = redis.call('ZSCORE', KEYS[5], claimID)
  if tombstone and tonumber(tombstone) <= now then
    redis.call('ZREM', KEYS[5], claimID)
    tombstone = false
  end
  if tombstone or redis.call('HEXISTS', KEYS[3], claimID) == 1 then
    redis.call('RPOP', KEYS[1])
  else
    claim(payload, claimID)
    return {1, payload, claimID}
  end
end
return {0}
";

/// KEYS: owners, idx. ARGV: claim field, owner, lease (ms), now (ms).
const RENEW_RESULT: &str = r"
local now = tonumber(ARGV[4])
local owner = redis.call('HGET', KEYS[1], ARGV[1])
local expiry = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not owner or owner ~= ARGV[2] or ARGV[2] == '' or not expiry or tonumber(expiry) <= now then
  return 0
end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
return 1
";

/// KEYS: claimed, owners, idx, tombstones, blob types, blob expiry. ARGV: claim field, owner,
/// tombstone TTL (ms), now (ms). Returns {1, blob} acked (blob is the
/// result's body blob or ''), {2} already acked, {0} ownership lost.
const ACK_RESULT: &str = r"
local now = tonumber(ARGV[4])
local owner = redis.call('HGET', KEYS[2], ARGV[1])
local expiry = redis.call('ZSCORE', KEYS[3], ARGV[1])
if owner and owner == ARGV[2] and ARGV[2] ~= '' and expiry and tonumber(expiry) > now then
  local blob = ''
  local payload = redis.call('HGET', KEYS[1], ARGV[1])
  if payload then
    local ok, result = pcall(cjson.decode, payload)
    if ok and type(result) == 'table' and type(result['payload_ref']) == 'string' then
      blob = string.match(result['payload_ref'], '^blob://(.+)$') or ''
    end
  end
  if blob ~= '' then
    redis.call('HDEL', KEYS[5], blob)
    redis.call('ZREM', KEYS[6], blob)
  end
  redis.call('HDEL', KEYS[1], ARGV[1])
  redis.call('HDEL', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[3], ARGV[1])
  redis.call('ZADD', KEYS[4], now + tonumber(ARGV[3]), ARGV[1])
  return {1, blob}
end
local tombstone = redis.call('ZSCORE', KEYS[4], ARGV[1])
if tombstone and tonumber(tombstone) > now then
  return {2}
end
if tombstone then
  redis.call('ZREM', KEYS[4], ARGV[1])
end
return {0}
";

/// Upstream redis-quota concurrency acquire. KEYS: counter. ARGV: limit, TTL (s).
pub const QUOTA_ACQUIRE: &str = r#"
local current = redis.call("GET", KEYS[1])
if current and tonumber(current) >= tonumber(ARGV[1]) then
	return 0
end
redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], ARGV[2])
return 1
"#;

/// Upstream redis-quota concurrency release. KEYS: counter. ARGV: TTL (s).
pub const QUOTA_RELEASE: &str = r#"
local current = redis.call("GET", KEYS[1])
if current and tonumber(current) > 0 then
	local remaining = redis.call("DECR", KEYS[1])
	if remaining > 0 then
		redis.call("EXPIRE", KEYS[1], ARGV[1])
	end
end
"#;

/// Upstream redis-quota sliding window. KEYS: log. ARGV: window start (ns),
/// limit, now (ns), TTL (s).
pub const QUOTA_WINDOW: &str = r#"
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, ARGV[1])
local count = redis.call("ZCARD", KEYS[1])
if count >= tonumber(ARGV[2]) then
	return 0
end
redis.call("ZADD", KEYS[1], ARGV[3], ARGV[3])
redis.call("EXPIRE", KEYS[1], ARGV[4])
return 1
"#;

/// A token bucket in a hash. KEYS: bucket. ARGV: rate (/s), capacity,
/// expiry (ms), now (ms). A bucket whose stored expiry passed starts full;
/// each take stores the caller's expiry for the next.
pub const TAKE_TOKEN: &str = r"
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local expires = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
if not rate or not capacity or not expires or not now
  or rate ~= rate or rate == math.huge or rate == -math.huge
  or capacity ~= capacity or capacity == math.huge or capacity == -math.huge then
  return 0
end
capacity = math.max(1, capacity)
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
local last = tonumber(redis.call('HGET', KEYS[1], 'last_ms'))
local stored = tonumber(redis.call('HGET', KEYS[1], 'expires_ms')) or 0
if not tokens or not last then
  tokens = capacity
  last = now
  stored = 0
end
if now >= stored then
  tokens = capacity
  last = now
end
if now < last then
  now = last
end
tokens = math.min(capacity, tokens + ((now - last) / 1000.0) * rate)
last = now
local taken = 0
if tokens >= 1 then
  tokens = tokens - 1
  taken = 1
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'last_ms', tostring(last), 'expires_ms', tostring(expires))
redis.call('PEXPIREAT', KEYS[1], math.max(expires, now) + 60000)
return taken
";

pub struct Scripts {
    pub claim: Script,
    pub renew: Script,
    pub reclaim: Script,
    pub release: Script,
    pub retry: Script,
    pub promote: Script,
    pub finish_claimed: Script,
    pub finish_pending: Script,
    pub discard: Script,
    pub cancel: Script,
    pub expire_check: Script,
    pub retain_blob: Script,
    pub receive: Script,
    pub renew_result: Script,
    pub ack_result: Script,
}

impl Scripts {
    pub fn new() -> Self {
        Self {
            claim: Script::new(CLAIM),
            renew: Script::new(RENEW),
            reclaim: Script::new(RECLAIM),
            release: Script::new(RELEASE),
            retry: Script::new(RETRY),
            promote: Script::new(PROMOTE),
            finish_claimed: Script::new(&finish_claimed()),
            finish_pending: Script::new(&finish_pending()),
            discard: Script::new(DISCARD),
            cancel: Script::new(CANCEL),
            expire_check: Script::new(EXPIRE_CHECK),
            retain_blob: Script::new(RETAIN_BLOB),
            receive: Script::new(RECEIVE),
            renew_result: Script::new(RENEW_RESULT),
            ack_result: Script::new(ACK_RESULT),
        }
    }
}
