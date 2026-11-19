-- rate_limit.lua
-- Sliding window rate limiter using Redis Sorted Sets.
--
-- KEYS[1]: ratelimit:<key> (Sorted Set)
-- ARGV[1]: limit (max operations in window)
-- ARGV[2]: windowMs (window size in ms)
-- ARGV[3]: nowMs
-- ARGV[4]: memberId

local rateKey = KEYS[1]
local limit = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local nowMs = tonumber(ARGV[3])
local memberId = ARGV[4]

-- 1. Evict timestamps outside sliding window
local clearBefore = nowMs - windowMs
redis.call('ZREMRANGEBYSCORE', rateKey, 0, clearBefore)

-- 2. Count current entries in window
local currentCount = redis.call('ZCARD', rateKey)

if currentCount < limit then
  -- Add new record
  redis.call('ZADD', rateKey, nowMs, memberId)
  redis.call('EXPIRE', rateKey, math.ceil(windowMs / 1000) * 2)
  return { 1, tostring(limit - currentCount - 1) }
else
  return { 0, '0' }
end
