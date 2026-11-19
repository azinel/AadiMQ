-- fail_job_with_backoff.lua
-- Atomically handles job failure, applying exponential backoff into delayed ZSET,
-- or routing to Dead Letter Queue if maximum attempts are exhausted.
--
-- KEYS[1]: queue:<name>:active (Sorted Set)
-- KEYS[2]: queue:<name>:delayed (Sorted Set)
-- KEYS[3]: queue:<name>:dlq (List)
-- KEYS[4]: job:<jobId> (Hash)
--
-- ARGV[1]: jobId
-- ARGV[2]: lockToken (fencing token)
-- ARGV[3]: errorReason
-- ARGV[4]: backoffDelayMs
-- ARGV[5]: nowMs

local activeKey = KEYS[1]
local delayedKey = KEYS[2]
local dlqKey = KEYS[3]
local jobKey = KEYS[4]

local jobId = ARGV[1]
local lockToken = ARGV[2]
local errorReason = ARGV[3]
local backoffDelayMs = tonumber(ARGV[4]) or 0
local nowMs = tonumber(ARGV[5])

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
  -- Re-queue into delayed queue with exponential backoff score
  local runAt = nowMs + backoffDelayMs
  redis.call('ZADD', delayedKey, runAt, jobId)
  redis.call('HSET', jobKey,
    'status', 'DELAYED',
    'failureReason', errorReason,
    'scheduledFor', runAt,
    'lock_token', '',
    'worker_id', '',
    'updatedAt', nowMs
  )
  return {'DELAYED', tostring(attempts), tostring(runAt)}
else
  -- Move to Dead Letter Queue (DLQ)
  redis.call('LPUSH', dlqKey, jobId)
  redis.call('HSET', jobKey,
    'status', 'DEAD_LETTER',
    'failureReason', errorReason,
    'lock_token', '',
    'worker_id', '',
    'failedAt', nowMs,
    'updatedAt', nowMs
  )
  return {'DEAD_LETTER', tostring(attempts), ''}
end
