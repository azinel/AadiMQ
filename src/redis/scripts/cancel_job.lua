-- cancel_job.lua
-- Atomically cancels a job by removing it from waiting, priority, or delayed queues.
--
-- KEYS[1]: queue:<name>:waiting (List)
-- KEYS[2]: queue:<name>:waiting:priority (Sorted Set)
-- KEYS[3]: queue:<name>:delayed (Sorted Set)
-- KEYS[4]: job:<jobId> (Hash)
--
-- ARGV[1]: jobId
-- ARGV[2]: nowMs

local waitingKey = KEYS[1]
local priorityKey = KEYS[2]
local delayedKey = KEYS[3]
local jobKey = KEYS[4]

local jobId = ARGV[1]
local nowMs = ARGV[2]

if redis.call('EXISTS', jobKey) == 0 then
  return {'ERR_NOT_FOUND', ''}
end

local currentStatus = redis.call('HGET', jobKey, 'status')
if currentStatus == 'COMPLETED' or currentStatus == 'CANCELLED' or currentStatus == 'DEAD_LETTER' then
  return {'ERR_TERMINAL', currentStatus or ''}
end

-- Remove from all potential pre-execution queues
redis.call('LREM', waitingKey, 0, jobId)
redis.call('ZREM', priorityKey, jobId)
redis.call('ZREM', delayedKey, jobId)

-- Update status
redis.call('HSET', jobKey,
  'status', 'CANCELLED',
  'updatedAt', nowMs
)

return {'OK', currentStatus}
