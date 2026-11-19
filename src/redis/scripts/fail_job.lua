-- fail_job.lua
-- Atomically handles job failure, checking fencing token and deciding retry vs terminal failure.
--
-- KEYS[1]: queue:<name>:active (Sorted Set)
-- KEYS[2]: queue:<name>:waiting (List)
-- KEYS[3]: job:<jobId> (Hash)
--
-- ARGV[1]: jobId
-- ARGV[2]: lockToken
-- ARGV[3]: errorReason
-- ARGV[4]: nowMs

local activeKey = KEYS[1]
local waitingKey = KEYS[2]
local jobKey = KEYS[3]

local jobId = ARGV[1]
local lockToken = ARGV[2]
local errorReason = ARGV[3]
local nowMs = ARGV[4]

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

-- 4. Check attempts vs maxAttempts
local attempts = tonumber(redis.call('HGET', jobKey, 'attempts') or '1')
local maxAttempts = tonumber(redis.call('HGET', jobKey, 'maxAttempts') or '3')

if attempts < maxAttempts then
  -- Re-queue for retry (FIFO tail)
  redis.call('HSET', jobKey,
    'status', 'WAITING',
    'failureReason', errorReason,
    'updatedAt', nowMs
  )
  redis.call('LPUSH', waitingKey, jobId)
  return {'RETRYING', tostring(attempts)}
else
  -- Terminal failure
  redis.call('HSET', jobKey,
    'status', 'FAILED',
    'failureReason', errorReason,
    'failedAt', nowMs,
    'updatedAt', nowMs
  )
  return {'FAILED', tostring(attempts)}
end
