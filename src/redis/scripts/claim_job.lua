-- claim_job.lua
-- Atomically claims the next FIFO job from waiting, moves it to active with a lease,
-- and assigns a fencing lock token to the worker.
--
-- KEYS[1]: queue:<name>:waiting (List)
-- KEYS[2]: queue:<name>:active (Sorted Set with score = lease_expires_at)
--
-- ARGV[1]: workerId
-- ARGV[2]: lockToken (fencing token)
-- ARGV[3]: leaseDurationMs
-- ARGV[4]: nowMs

local waitingKey = KEYS[1]
local activeKey = KEYS[2]

local workerId = ARGV[1]
local lockToken = ARGV[2]
local leaseDurationMs = tonumber(ARGV[3])
local nowMs = tonumber(ARGV[4])

-- 1. Pop the oldest job ID from waiting list (FIFO)
local jobId = redis.call('RPOP', waitingKey)
if not jobId then
  return nil
end

local jobKey = 'job:' .. jobId

-- Check if job metadata exists (in case of manual deletion or eviction)
if redis.call('EXISTS', jobKey) == 0 then
  return nil
end

-- 2. Calculate lease expiration timestamp
local leaseExpiresAt = nowMs + leaseDurationMs

-- 3. Add to active sorted set with lease expiration as score
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

-- 5. Fetch full job hash to return to worker
local jobData = redis.call('HGETALL', jobKey)

-- Return table: [jobId, lockToken, attempts, leaseExpiresAt, jobData]
return { jobId, lockToken, tostring(attempts), tostring(leaseExpiresAt), jobData }
