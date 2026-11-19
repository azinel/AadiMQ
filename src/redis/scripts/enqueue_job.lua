-- enqueue_job.lua
-- Atomically creates the job metadata hash and adds the job ID to the waiting FIFO list.
--
-- KEYS[1]: queue:<name>:waiting (List)
-- KEYS[2]: job:<jobId> (Hash)
--
-- ARGV[1]: jobId
-- ARGV[2]: queueName
-- ARGV[3]: jobType
-- ARGV[4]: payload JSON string
-- ARGV[5]: maxAttempts
-- ARGV[6]: priority
-- ARGV[7]: timestamp (ms)

local waitingKey = KEYS[1]
local jobKey = KEYS[2]

local jobId = ARGV[1]
local queueName = ARGV[2]
local jobType = ARGV[3]
local payload = ARGV[4]
local maxAttempts = ARGV[5]
local priority = ARGV[6]
local now = ARGV[7]

-- 1. Initialize job metadata hash
redis.call('HSET', jobKey,
  'id', jobId,
  'queue', queueName,
  'type', jobType,
  'payload', payload,
  'status', 'WAITING',
  'attempts', '0',
  'maxAttempts', maxAttempts,
  'priority', priority,
  'createdAt', now,
  'updatedAt', now
)

-- 2. Push job ID to the tail of the waiting FIFO list
-- Using LPUSH; workers consume from the other end via RPOP or Lua claim for strict FIFO ordering
redis.call('LPUSH', waitingKey, jobId)

return 'OK'
