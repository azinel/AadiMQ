import { v4 as uuidv4 } from "uuid";
import { z } from "zod";
import { appConfig } from "../config/app.config.js";
import { getRedisClient } from "../redis/client.js";
import { runScript } from "../redis/scriptLoader.js";
import { getPrismaClient } from "../config/db.config.js";
import { Job } from "./job.js";
import { logger } from "../utils/logger.js";

/**
 * Zod validation schema for job enqueue input.
 */
export const EnqueueJobSchema = z.object({
  queueName: z.string().min(1).default(appConfig.defaultQueue),
  jobType: z.string().min(1, "jobType is required"),
  payload: z.any().default({}),
  priority: z.number().int().default(0),
  maxAttempts: z.number().int().min(1).max(50).default(3),
  idempotencyKey: z.string().nullish(),
  delayMs: z.number().int().min(0).default(0),
});

/**
 * Producer responsible for validating, persisting, and enqueuing jobs.
 */
export class Producer {
  /**
   * @param {Object} [options]
   * @param {import("ioredis").Redis} [options.redis]
   * @param {import("@prisma/client").PrismaClient} [options.prisma]
   */
  constructor(options = {}) {
    this.redis = options.redis || getRedisClient();
    this.prisma = options.prisma || getPrismaClient();
  }

  /**
   * Enqueues a new background job into PostgreSQL and Redis.
   * Supports immediate FIFO, priority scheduling, and delayed scheduling.
   * 
   * @param {Object} input
   * @param {string} [input.queueName] Target queue name (defaults to 'default')
   * @param {string} input.jobType Semantic name of the job
   * @param {any} [input.payload] JSON payload data
   * @param {number} [input.priority] Integer priority (higher numbers executed first)
   * @param {number} [input.maxAttempts] Maximum retries on failure (default 3)
   * @param {string} [input.idempotencyKey] Unique key to prevent duplicate processing
   * @param {number} [input.delayMs] Milliseconds to delay before job execution
   * @returns {Promise<Job>}
   */
  async enqueue(input) {
    const validated = EnqueueJobSchema.parse(input);
    const now = Date.now();
    const isDelayed = validated.delayMs > 0;
    const initialStatus = isDelayed ? "DELAYED" : "WAITING";
    const scheduledFor = isDelayed ? new Date(now + validated.delayMs) : null;

    // 1. Idempotency Check: prevent duplicate job generation
    if (validated.idempotencyKey) {
      const existing = await this.prisma.job.findUnique({
        where: { idempotencyKey: validated.idempotencyKey },
      });

      if (existing) {
        logger.info(`Idempotent submission detected for key '${validated.idempotencyKey}'`, {
          jobId: existing.id,
          status: existing.status,
        });
        return Job.fromDatabase(existing);
      }
    }

    const jobId = uuidv4();

    // 2. Persist to PostgreSQL (System of Record)
    const dbRecord = await this.prisma.job.create({
      data: {
        id: jobId,
        queueName: validated.queueName,
        jobType: validated.jobType,
        payload: validated.payload,
        status: initialStatus,
        priority: validated.priority,
        attempts: 0,
        maxAttempts: validated.maxAttempts,
        idempotencyKey: validated.idempotencyKey || null,
        scheduledFor,
        createdAt: new Date(now),
      },
    });

    // 3. Persist to Redis
    try {
      const jobKey = `job:${jobId}`;

      // Initialize Redis metadata hash
      await this.redis.hset(jobKey, {
        id: jobId,
        queue: validated.queueName,
        type: validated.jobType,
        payload: JSON.stringify(validated.payload),
        status: initialStatus,
        attempts: "0",
        maxAttempts: validated.maxAttempts,
        priority: validated.priority,
        scheduledFor: scheduledFor ? scheduledFor.getTime() : "",
        createdAt: now,
        updatedAt: now,
      });

      if (isDelayed) {
        // Delayed scheduling: insert into delayed ZSET with score = scheduled timestamp
        const delayedKey = `queue:${validated.queueName}:delayed`;
        await this.redis.zadd(delayedKey, now + validated.delayMs, jobId);
      } else if (validated.priority > 0) {
        // Priority scheduling: insert into waiting:priority ZSET
        const priorityKey = `queue:${validated.queueName}:waiting:priority`;
        const score = validated.priority * 10000000000000 + (10000000000000 - now);
        await this.redis.zadd(priorityKey, score, jobId);
      } else {
        // Standard FIFO scheduling: LPUSH into waiting list
        const waitingKey = `queue:${validated.queueName}:waiting`;
        await this.redis.lpush(waitingKey, jobId);
      }

      logger.info(`Job enqueued successfully`, {
        jobId,
        queue: validated.queueName,
        type: validated.jobType,
        status: initialStatus,
        priority: validated.priority,
      });

      return Job.fromDatabase(dbRecord);
    } catch (err) {
      logger.error(`Failed to push job ${jobId} to Redis after DB persistence`, {
        error: err.message,
        jobId,
      });

      await this.prisma.job.update({
        where: { id: jobId },
        data: {
          status: "FAILED",
          failureReason: `Redis enqueue failure: ${err.message}`,
        },
      });

      throw new Error(`Queue submission failed: ${err.message}`);
    }
  }
}
