-- promote_delayed_jobs.lua
-- Atomically finds delayed jobs whose execution timestamp has arrived,
-- moves them to the appropriate waiting queue (priority ZSET or FIFO list),
-- and updates job metadata.
--
-- KEYS[1]: queue:<name>:delayed (Sorted Set)
-- KEYS[2]: queue:<name>:waiting (List)
-- KEYS[3]: queue:<name>:waiting:priority (Sorted Set)
--
-- ARGV[1]: nowMs
-- ARGV[2]: maxBatchSize

local delayedKey = KEYS[1]
local waitingKey = KEYS[2]
local priorityKey = KEYS[3]

local nowMs = tonumber(ARGV[1])
local maxBatchSize = tonumber(ARGV[2]) or 50

-- Find all matured delayed jobs
local maturedJobIds = redis.call('ZRANGEBYSCORE', delayedKey, 0, nowMs, 'LIMIT', 0, maxBatchSize)
local promoted = {}

for _, jobId in ipairs(maturedJobIds) do
  local jobKey = 'job:' .. jobId

  if redis.call('EXISTS', jobKey) == 1 then
    -- Remove from delayed
    redis.call('ZREM', delayedKey, jobId)

    local priority = tonumber(redis.call('HGET', jobKey, 'priority') or '0')
    local createdAt = tonumber(redis.call('HGET', jobKey, 'createdAt') or nowMs)

    if priority > 0 then
      -- Score = priority * 10^13 + (10^13 - createdAt) for tiebreaking
      local score = (priority * 10000000000000) + (10000000000000 - createdAt)
      redis.call('ZADD', priorityKey, score, jobId)
    else
      redis.call('LPUSH', waitingKey, jobId)
    end

    redis.call('HSET', jobKey,
      'status', 'WAITING',
      'lock_token', '',
      'worker_id', '',
      'updatedAt', nowMs
    )

    table.insert(promoted, jobId)
  else
    -- Orphaned entry in delayed set
    redis.call('ZREM', delayedKey, jobId)
  end
end

return promoted
