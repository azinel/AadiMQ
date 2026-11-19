-- recover_stalled_jobs.lua
-- Atomically detects expired leases in the active set and recovers them to waiting (or DLQ if exhausted).
--
-- KEYS[1]: queue:<name>:active (Sorted Set)
-- KEYS[2]: queue:<name>:waiting (List)
-- KEYS[3]: queue:<name>:dlq (List)
--
-- ARGV[1]: nowMs
-- ARGV[2]: maxBatchSize

local activeKey = KEYS[1]
local waitingKey = KEYS[2]
local dlqKey = KEYS[3]

local nowMs = tonumber(ARGV[1])
local maxBatchSize = tonumber(ARGV[2]) or 50

-- 1. Query active jobs whose lease expiration score <= nowMs
local expiredJobIds = redis.call('ZRANGEBYSCORE', activeKey, 0, nowMs, 'LIMIT', 0, maxBatchSize)
local recovered = {}
local deadLettered = {}

for _, jobId in ipairs(expiredJobIds) do
  local jobKey = 'job:' .. jobId

  -- Verify job hash exists
  if redis.call('EXISTS', jobKey) == 1 then
    local attempts = tonumber(redis.call('HGET', jobKey, 'attempts') or '1')
    local maxAttempts = tonumber(redis.call('HGET', jobKey, 'maxAttempts') or '3')

    -- Remove from active
    redis.call('ZREM', activeKey, jobId)

    if attempts < maxAttempts then
      -- Re-queue to waiting FIFO list
      redis.call('LPUSH', waitingKey, jobId)
      redis.call('HSET', jobKey,
        'status', 'WAITING',
        'failureReason', 'Worker lease expired (stalled job recovery)',
        'lock_token', '',
        'worker_id', '',
        'updatedAt', nowMs
      )
      table.insert(recovered, jobId)
    else
      -- Move to Dead Letter Queue
      redis.call('LPUSH', dlqKey, jobId)
      redis.call('HSET', jobKey,
        'status', 'DEAD_LETTER',
        'failureReason', 'Max retry attempts exceeded after lease timeout',
        'failedAt', nowMs,
        'updatedAt', nowMs
      )
      table.insert(deadLettered, jobId)
    end
  else
    -- Job hash does not exist (orphaned ZSET entry); clean up
    redis.call('ZREM', activeKey, jobId)
  end
end

return { recovered, deadLettered }
