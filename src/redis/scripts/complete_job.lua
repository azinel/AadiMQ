-- complete_job.lua
-- Atomically completes a job after verifying the caller's fencing lock token.
--
-- KEYS[1]: queue:<name>:active (Sorted Set)
-- KEYS[2]: job:<jobId> (Hash)
--
-- ARGV[1]: jobId
-- ARGV[2]: lockToken (fencing token)
-- ARGV[3]: nowMs
-- ARGV[4]: retentionTtlSeconds (TTL for completed job in Redis, e.g. 86400)

local activeKey = KEYS[1]
local jobKey = KEYS[2]

local jobId = ARGV[1]
local lockToken = ARGV[2]
local nowMs = ARGV[3]
local retentionTtlSeconds = tonumber(ARGV[4]) or 86400

-- 1. Check if job exists
if redis.call('EXISTS', jobKey) == 0 then
  return {'ERR_JOB_NOT_FOUND', ''}
end

-- 2. Verify fencing token
local currentLockToken = redis.call('HGET', jobKey, 'lock_token')
if currentLockToken ~= lockToken then
  return {'ERR_FENCE_MISMATCH', currentLockToken or ''}
end

-- 3. Remove from active sorted set
redis.call('ZREM', activeKey, jobId)

-- 4. Mark job as COMPLETED
redis.call('HSET', jobKey,
  'status', 'COMPLETED',
  'completedAt', nowMs,
  'updatedAt', nowMs
)

-- 5. Set retention TTL on completed job key
if retentionTtlSeconds > 0 then
  redis.call('EXPIRE', jobKey, retentionTtlSeconds)
end

return {'OK', ''}
