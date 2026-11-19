-- replay_dlq_job.lua
-- Atomically moves a dead-lettered job back into the waiting FIFO queue.
--
-- KEYS[1]: queue:<name>:dlq (List)
-- KEYS[2]: queue:<name>:waiting (List)
-- KEYS[3]: job:<jobId> (Hash)
--
-- ARGV[1]: jobId
-- ARGV[2]: nowMs

local dlqKey = KEYS[1]
local waitingKey = KEYS[2]
local jobKey = KEYS[3]

local jobId = ARGV[1]
local nowMs = ARGV[2]

if redis.call('EXISTS', jobKey) == 0 then
  return {'ERR_NOT_FOUND', ''}
end

local currentStatus = redis.call('HGET', jobKey, 'status')
if currentStatus ~= 'DEAD_LETTER' and currentStatus ~= 'FAILED' then
  return {'ERR_INVALID_STATUS', currentStatus or ''}
end

-- Remove from DLQ
redis.call('LREM', dlqKey, 0, jobId)

-- Reset attempts, clear failures, and push to waiting
redis.call('HSET', jobKey,
  'status', 'WAITING',
  'attempts', '0',
  'failureReason', '',
  'lock_token', '',
  'worker_id', '',
  'updatedAt', nowMs
)
redis.call('LPUSH', waitingKey, jobId)

return {'OK', ''}
