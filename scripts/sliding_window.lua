-- sliding_window.lua
--
-- Atomic sliding-window-log rate limiter backed by a Redis sorted set (ZSET).
--
-- KEYS[1] = rate limit key, e.g. "rl:sw:{tenant}:{route}"
-- ARGV[1] = window size in milliseconds
-- ARGV[2] = max requests allowed within the window (limit)
-- ARGV[3] = current unix timestamp in milliseconds (passed in, not computed
--           in Lua, so all nodes agree on "now" via the caller's clock and
--           the script stays deterministic/replication-safe)
-- ARGV[4] = unique member id for this request (e.g. uuid4), guarantees
--           ZADD never collides two requests arriving in the same millisecond
--
-- Returns an array:
--   [1] allowed          (1 = allowed, 0 = rejected)
--   [2] remaining        (tokens left in the window AFTER this decision)
--   [3] reset_ms         (unix ms timestamp when the window fully resets,
--                          i.e. when the oldest entry currently counted
--                          will fall out of the window)
--   [4] retry_after_ms   (0 if allowed; otherwise ms until the caller
--                          should retry - derived from the oldest entry)

local key            = KEYS[1]
local window_ms       = tonumber(ARGV[1])
local limit           = tonumber(ARGV[2])
local now_ms          = tonumber(ARGV[3])
local member          = ARGV[4]

local window_start = now_ms - window_ms

-- 1. Prune anything older than the current window.
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- 2. Count what's left in the window.
local current = redis.call('ZCARD', key)

local allowed = 0
local remaining = 0
local retry_after_ms = 0

if current < limit then
    -- 3. Admit the request: record it and refresh TTL so idle keys expire.
    redis.call('ZADD', key, now_ms, member)
    -- Defensive TTL: window length rounded up to whole seconds + 1s buffer,
    -- so keys evaporate on their own even if a node crashes mid-flow.
    local ttl_seconds = math.ceil(window_ms / 1000) + 1
    redis.call('EXPIRE', key, ttl_seconds)

    allowed = 1
    remaining = limit - (current + 1)
else
    remaining = 0
end

-- 4. Compute reset_ms from the oldest surviving entry (if any). If the set
--    is empty (shouldn't happen right after ZADD, but guards the reject
--    path where we didn't insert), fall back to now + window.
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local reset_ms
if oldest and oldest[2] then
    reset_ms = tonumber(oldest[2]) + window_ms
else
    reset_ms = now_ms + window_ms
end

if allowed == 0 then
    retry_after_ms = math.max(reset_ms - now_ms, 0)
end

return { allowed, remaining, reset_ms, retry_after_ms }
