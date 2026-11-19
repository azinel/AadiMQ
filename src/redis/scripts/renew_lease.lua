-- renew_lease.lua
-- Atomically renews the lease expiration timestamp for an active job if the caller owns the lock token.
--
-- KEYS[1]: queue:<name>:active (Sorted Set)
-- KEYS[2]: job:<jobId> (Hash)
--
-- ARGV[1]: jobId
-- ARGV[2]: lockToken (fencing token)
-- ARGV[3]: leaseDurationMs
-- ARGV[4]: nowMs

local activeKey = KEYS[1]
local jobKey = KEYS[2]

local jobId = ARGV[1]
local lockToken = ARGV[2]
local leaseDurationMs = tonumber(ARGV[3])
local nowMs = tonumber(ARGV[4])

-- 1. Check if job exists
if redis.call('EXISTS', jobKey) == 0 then
  return {'ERR_JOB_NOT_FOUND', ''}
end

-- 2. Verify fencing token
local currentLockToken = redis.call('HGET', jobKey, 'lock_token')
if currentLockToken ~= lockToken then
  return {'ERR_FENCE_MISMATCH', currentLockToken or ''}
end

-- 3. Calculate new expiration and update Sorted Set score
local newExpiresAt = nowMs + leaseDurationMs
redis.call('ZADD', activeKey, newExpiresAt, jobId)
redis.call('HSET', jobKey, 'updatedAt', nowMs)

return {'OK', tostring(newExpiresAt)}
