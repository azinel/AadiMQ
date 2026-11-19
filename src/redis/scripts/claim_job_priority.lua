-- claim_job_priority.lua
-- Atomically claims the next job, giving strict precedence to priority queue jobs
-- before falling back to standard FIFO waiting list.
--
-- KEYS[1]: queue:<name>:waiting:priority (Sorted Set, score = priority * 10^13 + tiebreaker)
-- KEYS[2]: queue:<name>:waiting (List, FIFO)
-- KEYS[3]: queue:<name>:active (Sorted Set, score = lease_expires_at)
--
-- ARGV[1]: workerId
-- ARGV[2]: lockToken (fencing token)
-- ARGV[3]: leaseDurationMs
-- ARGV[4]: nowMs

local priorityKey = KEYS[1]
local waitingKey = KEYS[2]
local activeKey = KEYS[3]

local workerId = ARGV[1]
local lockToken = ARGV[2]
local leaseDurationMs = tonumber(ARGV[3])
local nowMs = tonumber(ARGV[4])

local jobId = nil

-- 1. Check priority Sorted Set first (pop highest score)
local priorityPop = redis.call('ZPOPMAX', priorityKey, 1)
if priorityPop and #priorityPop > 0 then
  jobId = priorityPop[1]
end

-- 2. Fallback to standard FIFO list
if not jobId then
  jobId = redis.call('RPOP', waitingKey)
end

if not jobId then
  return nil -- Both queues empty
end

local jobKey = 'job:' .. jobId

if redis.call('EXISTS', jobKey) == 0 then
  return nil
end

-- 3. Move to active with lease
local leaseExpiresAt = nowMs + leaseDurationMs
redis.call('ZADD', activeKey, leaseExpiresAt, jobId)

-- 4. Increment attempts and update metadata
local attempts = redis.call('HINCRBY', jobKey, 'attempts', 1)
redis.call('HSET', jobKey,
  'status', 'ACTIVE',
  'worker_id', workerId,
  'lock_token', lockToken,
  'startedAt', nowMs,
  'updatedAt', nowMs
)

local jobData = redis.call('HGETALL', jobKey)

return { jobId, lockToken, tostring(attempts), tostring(leaseExpiresAt), jobData }
