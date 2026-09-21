-- token_bucket.lua
--
-- Atomic token-bucket rate limiter backed by a Redis HASH.
--
-- Hash fields:
--   tokens        - current number of tokens available (float, as string)
--   last_refill_ms - unix ms timestamp of the last refill/consume operation
--
-- KEYS[1] = bucket key, e.g. "rl:tb:{tenant}:{route}"
-- ARGV[1] = capacity            (max tokens the bucket can hold)
-- ARGV[2] = refill_rate_per_sec (tokens added per second, float)
-- ARGV[3] = requested           (tokens this request wants to consume, usually 1)
-- ARGV[4] = now_ms              (current unix timestamp in ms, from caller)
-- ARGV[5] = ttl_seconds         (idle-key expiry safety net)
--
-- Returns an array:
--   [1] allowed        (1 = allowed, 0 = rejected)
--   [2] remaining      (tokens left AFTER this decision, floor'd to int for header use)
--   [3] reset_ms       (estimated unix ms timestamp when the bucket will be full again)
--   [4] retry_after_ms (0 if allowed; otherwise ms until `requested` tokens will exist)

local key             = KEYS[1]
local capacity        = tonumber(ARGV[1])
local refill_rate     = tonumber(ARGV[2])   -- tokens / second
local requested       = tonumber(ARGV[3])
local now_ms          = tonumber(ARGV[4])
local ttl_seconds     = tonumber(ARGV[5])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill_ms')

local tokens
local last_refill_ms

if bucket[1] and bucket[2] then
    tokens = tonumber(bucket[1])
    last_refill_ms = tonumber(bucket[2])
else
    -- First time we see this key: start full, as if it had been idle forever.
    tokens = capacity
    last_refill_ms = now_ms
end

-- Refill proportionally to elapsed time. Guard against clock skew producing
-- a negative elapsed value (e.g. requests racing with slightly different
-- "now" values across app servers) by clamping elapsed to >= 0.
local elapsed_ms = math.max(now_ms - last_refill_ms, 0)
local refill = (elapsed_ms / 1000.0) * refill_rate

tokens = math.min(capacity, tokens + refill)

local allowed = 0
local retry_after_ms = 0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    -- Not enough tokens: compute how long until we'd have `requested`.
    local deficit = requested - tokens
    if refill_rate > 0 then
        retry_after_ms = math.ceil((deficit / refill_rate) * 1000.0)
    else
        -- No refill configured: bucket will never allow this, signal a
        -- long-but-finite retry so clients don't wait forever silently.
        retry_after_ms = 1000 * 3600
    end
end

redis.call('HMSET', key, 'tokens', tostring(tokens), 'last_refill_ms', tostring(now_ms))
redis.call('EXPIRE', key, ttl_seconds)

-- Estimate when the bucket will be back to full, for the reset header.
local reset_ms
if refill_rate > 0 then
    local missing = capacity - tokens
    reset_ms = now_ms + math.ceil((missing / refill_rate) * 1000.0)
else
    reset_ms = now_ms
end

return { allowed, math.floor(tokens), reset_ms, retry_after_ms }
