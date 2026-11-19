import os from "os";
import { v4 as uuidv4 } from "uuid";
import { appConfig } from "../config/app.config.js";
import { getRedisClient } from "../redis/client.js";
import { runScript } from "../redis/scriptLoader.js";
import { getPrismaClient } from "../config/db.config.js";
import { Job } from "../queue/job.js";
import { HeartbeatManager } from "./heartbeat.js";
import { logger } from "../utils/logger.js";

/**
 * Worker engine that processes jobs from a named queue.
 * Claims jobs atomically using Redis Lua scripts, fences execution with lock tokens,
 * continuously renews active leases via background heartbeats, enforces execution timeouts,
 * applies exponential backoff on retries, and routes exhausted jobs to DLQ.
 */
export class Worker {
  /**
   * @param {Object} options
   * @param {string} [options.queueName] Name of the queue to process
   * @param {string} [options.workerId] Unique identifier for this worker
   * @param {Function} options.processor Async function (job: Job) => Promise<any>
   * @param {number} [options.concurrency=1] Concurrency level
   * @param {number} [options.leaseDurationMs] Lease timeout in ms (default 30000)
   * @param {number} [options.pollIntervalMs] Polling backoff when queue is empty (default 500)
   * @param {number} [options.jobTimeoutMs=30000] Maximum allowed processing time before timeout
   * @param {number} [options.initialBackoffMs=500] Initial exponential backoff delay
   * @param {number} [options.backoffFactor=2] Exponential backoff multiplier
   * @param {import("ioredis").Redis} [options.redis]
   * @param {import("@prisma/client").PrismaClient} [options.prisma]
   */
  constructor({
    queueName = appConfig.defaultQueue,
    workerId = null,
    processor,
    concurrency = 1,
    leaseDurationMs = appConfig.jobLeaseMs,
    pollIntervalMs = 500,
    jobTimeoutMs = 30000,
    initialBackoffMs = 500,
    backoffFactor = 2,
    redis = null,
    prisma = null,
  }) {
    if (typeof processor !== "function") {
      throw new Error("Worker requires a valid processor function");
    }

    this.queueName = queueName;
    this.workerId = workerId || `worker-${os.hostname()}-${process.pid}-${uuidv4().slice(0, 8)}`;
    this.processor = processor;
    this.concurrency = Math.max(1, concurrency);
    this.leaseDurationMs = leaseDurationMs;
    this.pollIntervalMs = pollIntervalMs;
    this.jobTimeoutMs = jobTimeoutMs;
    this.initialBackoffMs = initialBackoffMs;
    this.backoffFactor = backoffFactor;

    this.redis = redis || getRedisClient();
    this.prisma = prisma || getPrismaClient();

    this.heartbeat = new HeartbeatManager({
      workerId: this.workerId,
      queueName: this.queueName,
      redis: this.redis,
      prisma: this.prisma,
      intervalMs: Math.max(500, Math.floor(this.leaseDurationMs / 3)),
      leaseDurationMs: this.leaseDurationMs,
    });

    this.isRunning = false;
    this.activeJobCount = 0;
    this._loopPromises = [];
  }

  /**
   * Calculates exponential backoff with jitter.
   * @param {number} attempt Current attempt number (1-indexed)
   * @returns {number} Delay in milliseconds
   */
  calculateBackoff(attempt) {
    const base = this.initialBackoffMs * Math.pow(this.backoffFactor, Math.max(0, attempt - 1));
    // Add 10% jitter to prevent retry storms
    const jitter = base * (0.9 + Math.random() * 0.2);
    return Math.floor(jitter);
  }

  /**
   * Starts the worker, registers with DB/Redis, and starts concurrency loops.
   */
  async start() {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info(`Worker starting`, {
      workerId: this.workerId,
      queue: this.queueName,
      concurrency: this.concurrency,
      leaseMs: this.leaseDurationMs,
      timeoutMs: this.jobTimeoutMs,
    });

    // 1. Register Worker in PostgreSQL
    try {
      await this.prisma.worker.upsert({
        where: { id: this.workerId },
        update: {
          status: "ONLINE",
          concurrency: this.concurrency,
          lastHeartbeat: new Date(),
        },
        create: {
          id: this.workerId,
          hostname: os.hostname(),
          pid: process.pid,
          status: "ONLINE",
          concurrency: this.concurrency,
          startedAt: new Date(),
          lastHeartbeat: new Date(),
        },
      });
    } catch (err) {
      logger.warn(`Could not register worker in database`, { error: err.message });
    }

    // 2. Start Heartbeat & Lease Renewal daemon
    this.heartbeat.start();

    // 3. Start processing slots
    for (let i = 0; i < this.concurrency; i++) {
      this._loopPromises.push(this._runLoop(i));
    }
  }

  /**
   * Internal polling loop for an individual execution slot.
   * @param {number} slotId
   * @private
   */
  async _runLoop(slotId) {
    while (this.isRunning) {
      try {
        const processed = await this._claimAndProcess();
        if (!processed && this.isRunning) {
          // Queue was empty; backoff before polling again
          await new Promise((resolve) => setTimeout(resolve, this.pollIntervalMs));
        }
      } catch (err) {
        logger.error(`Error in worker slot ${slotId}`, {
          workerId: this.workerId,
          error: err.message,
        });
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  /**
   * Atomically claims one job from Redis and executes it with timeout protection.
   * @returns {Promise<boolean>} True if a job was claimed and processed, false if queue was empty
   * @private
   */
  async _claimAndProcess() {
    const lockToken = uuidv4();
    const now = Date.now();

    const priorityKey = `queue:${this.queueName}:waiting:priority`;
    const waitingKey = `queue:${this.queueName}:waiting`;
    const activeKey = `queue:${this.queueName}:active`;
    const delayedKey = `queue:${this.queueName}:delayed`;
    const dlqKey = `queue:${this.queueName}:dlq`;
    const jobKey = (id) => `job:${id}`;

    // 1. Atomic claim via Lua script (priority-aware: checks priority ZSET before FIFO list)
    const claimResult = await runScript(
      this.redis,
      "claim_job_priority",
      [priorityKey, waitingKey, activeKey],
      [this.workerId, lockToken, this.leaseDurationMs, now]
    );

    if (!claimResult) {
      return false; // Queue empty
    }

    const [jobId, returnedLockToken, attemptsStr, leaseExpiresAtStr, rawJobHash] = claimResult;
    const attempts = parseInt(attemptsStr, 10);

    // Parse Redis HGETALL array
    const hash = {};
    for (let i = 0; i < rawJobHash.length; i += 2) {
      hash[rawJobHash[i]] = rawJobHash[i + 1];
    }
    hash.lock_token = returnedLockToken;

    const job = Job.fromRedisHash(hash);
    this.activeJobCount++;

    // Register job with HeartbeatManager for automatic lease renewals during execution
    this.heartbeat.registerJob(jobId, lockToken, this.leaseDurationMs);

    const startTime = Date.now();
    logger.info(`Job claimed`, {
      jobId,
      workerId: this.workerId,
      attempt: attempts,
      lockToken,
    });

    let executionRecord = null;

    // 2. Persist state to PostgreSQL: status=ACTIVE and create JobExecution audit record
    try {
      await this.prisma.job.update({
        where: { id: jobId },
        data: {
          status: "ACTIVE",
          attempts: attempts,
          startedAt: new Date(startTime),
        },
      });

      executionRecord = await this.prisma.jobExecution.create({
        data: {
          jobId,
          workerId: this.workerId,
          attempt: attempts,
          status: "ACTIVE",
          startedAt: new Date(startTime),
        },
      });
    } catch (dbErr) {
      logger.warn(`Failed to update PostgreSQL on job claim`, {
        jobId,
        error: dbErr.message,
      });
    }

    // 3. Execute the user processor with timeout protection
    try {
      let timeoutHandle;
      const timeoutPromise = new Promise((_, reject) => {
        timeoutHandle = setTimeout(() => {
          reject(new Error(`JobExecutionTimeout: Exceeded configured timeout of ${this.jobTimeoutMs}ms`));
        }, this.jobTimeoutMs);
      });

      const result = await Promise.race([
        this.processor(job),
        timeoutPromise,
      ]).finally(() => {
        clearTimeout(timeoutHandle);
      });

      const finishedAt = Date.now();
      const durationMs = finishedAt - startTime;

      // 4. Atomic completion in Redis with lock token fencing
      const completeResult = await runScript(
        this.redis,
        "complete_job",
        [activeKey, jobKey(jobId)],
        [jobId, lockToken, finishedAt, 86400]
      );

      const [status, detail] = completeResult;
      if (status === "ERR_FENCE_MISMATCH") {
        logger.error(`Fencing violation on completion! Lease was lost`, {
          jobId,
          workerId: this.workerId,
          currentLockToken: detail,
        });
        return true;
      }

      // 5. Update PostgreSQL status to COMPLETED
      try {
        await this.prisma.job.update({
          where: { id: jobId },
          data: {
            status: "COMPLETED",
            completedAt: new Date(finishedAt),
          },
        });

        if (executionRecord) {
          await this.prisma.jobExecution.update({
            where: { id: executionRecord.id },
            data: {
              status: "COMPLETED",
              finishedAt: new Date(finishedAt),
              durationMs,
              result: result !== undefined ? result : null,
            },
          });
        }
      } catch (dbErr) {
        logger.warn(`Failed to update PostgreSQL on job completion`, {
          jobId,
          error: dbErr.message,
        });
      }

      logger.info(`Job completed successfully`, {
        jobId,
        workerId: this.workerId,
        durationMs,
      });

      return true;
    } catch (procErr) {
      const failedAt = Date.now();
      const durationMs = failedAt - startTime;
      const backoffDelay = this.calculateBackoff(attempts);

      logger.error(`Job execution failed: ${procErr.message}`, {
        jobId,
        workerId: this.workerId,
        attempt: attempts,
        maxAttempts: job.maxAttempts,
        backoffDelay,
      });

      // 4b. Report failure in Redis via fail_job_with_backoff.lua
      const failResult = await runScript(
        this.redis,
        "fail_job_with_backoff",
        [activeKey, delayedKey, dlqKey, jobKey(jobId)],
        [jobId, lockToken, procErr.message, backoffDelay, failedAt]
      );

      const [failOutcome, , runAtStr] = failResult || [];

      // 5b. Update PostgreSQL with failure details
      try {
        const isDeadLetter = failOutcome === "DEAD_LETTER";
        await this.prisma.job.update({
          where: { id: jobId },
          data: {
            status: isDeadLetter ? "DEAD_LETTER" : "DELAYED",
            failureReason: procErr.message,
            scheduledFor: isDeadLetter ? null : new Date(Number(runAtStr)),
            failedAt: new Date(failedAt),
          },
        });

        if (executionRecord) {
          await this.prisma.jobExecution.update({
            where: { id: executionRecord.id },
            data: {
              status: isDeadLetter ? "DEAD_LETTER" : "FAILED",
              finishedAt: new Date(failedAt),
              durationMs,
              error: procErr.message,
            },
          });
        }
      } catch (dbErr) {
        logger.warn(`Failed to update PostgreSQL on job failure`, {
          jobId,
          error: dbErr.message,
        });
      }

      return true;
    } finally {
      this.heartbeat.unregisterJob(jobId);
      this.activeJobCount--;
    }
  }

  /**
   * Gracefully shuts down the worker, waiting for in-flight jobs to conclude.
   * @returns {Promise<void>}
   */
  async stop() {
    this.isRunning = false;
    logger.info(`Stopping worker ${this.workerId}...`);
    this.heartbeat.stop();

    await Promise.all(this._loopPromises);

    // Update status to OFFLINE in PostgreSQL
    try {
      await this.prisma.worker.update({
        where: { id: this.workerId },
        data: { status: "OFFLINE", lastHeartbeat: new Date() },
      });
      await this.redis.del(`worker:${this.workerId}`);
    } catch (err) {
      // Ignore teardown errors
    }

    logger.info(`Worker ${this.workerId} stopped cleanly`);
  }
}
