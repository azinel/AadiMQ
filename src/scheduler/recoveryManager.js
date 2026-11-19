import { getRedisClient } from "../redis/client.js";
import { runScript } from "../redis/scriptLoader.js";
import { getPrismaClient } from "../config/db.config.js";
import { logger } from "../utils/logger.js";

/**
 * RecoveryManager monitors active worker leases and automatically recovers
 * stalled or abandoned jobs caused by worker crashes or network partitions.
 */
export class RecoveryManager {
  /**
   * @param {Object} [options]
   * @param {string[]} [options.queues] Queues to monitor (defaults to ['default'])
   * @param {number} [options.checkIntervalMs=5000] Polling frequency
   * @param {number} [options.workerTimeoutMs=30000] Threshold to flag workers OFFLINE
   * @param {import("ioredis").Redis} [options.redis]
   * @param {import("@prisma/client").PrismaClient} [options.prisma]
   */
  constructor(options = {}) {
    this.queues = options.queues || ["default"];
    this.checkIntervalMs = options.checkIntervalMs || 5000;
    this.workerTimeoutMs = options.workerTimeoutMs || 30000;
    this.redis = options.redis || getRedisClient();
    this.prisma = options.prisma || getPrismaClient();

    this.timer = null;
    this.isRunning = false;
  }

  /**
   * Starts the recovery monitor background loop.
   */
  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info(`RecoveryManager started`, {
      queues: this.queues,
      intervalMs: this.checkIntervalMs,
    });

    this.timer = setInterval(async () => {
      try {
        await this.checkAll();
      } catch (err) {
        logger.error(`RecoveryManager encountered error during check`, {
          error: err.message,
        });
      }
    }, this.checkIntervalMs);
  }

  /**
   * Runs an immediate check and recovery across all registered queues and workers.
   */
  async checkAll() {
    for (const queue of this.queues) {
      await this.recoverQueue(queue);
    }
    await this.cleanupDeadWorkers();
  }

  /**
   * Recovers stalled jobs for a specific queue.
   * @param {string} queueName
   * @returns {Promise<{ recovered: string[], deadLettered: string[] }>}
   */
  async recoverQueue(queueName) {
    const activeKey = `queue:${queueName}:active`;
    const waitingKey = `queue:${queueName}:waiting`;
    const dlqKey = `queue:${queueName}:dlq`;
    const now = Date.now();

    const result = await runScript(
      this.redis,
      "recover_stalled_jobs",
      [activeKey, waitingKey, dlqKey],
      [now, 50]
    );

    if (!result) return { recovered: [], deadLettered: [] };

    const [recoveredIds, dlqIds] = result;

    if (recoveredIds && recoveredIds.length > 0) {
      logger.warn(`Recovered stalled jobs in queue '${queueName}'`, {
        count: recoveredIds.length,
        jobIds: recoveredIds,
      });

      // Synchronize recovered jobs in PostgreSQL
      try {
        await this.prisma.job.updateMany({
          where: { id: { in: recoveredIds } },
          data: {
            status: "WAITING",
            failureReason: "Worker lease expired (automatically recovered)",
          },
        });
      } catch (dbErr) {
        logger.warn(`Failed to update DB for recovered jobs`, { error: dbErr.message });
      }
    }

    if (dlqIds && dlqIds.length > 0) {
      logger.warn(`Moved stalled jobs with exceeded retries to DLQ in queue '${queueName}'`, {
        count: dlqIds.length,
        jobIds: dlqIds,
      });

      try {
        await this.prisma.job.updateMany({
          where: { id: { in: dlqIds } },
          data: {
            status: "DEAD_LETTER",
            failureReason: "Max attempts exceeded after lease expiration",
            failedAt: new Date(now),
          },
        });
      } catch (dbErr) {
        logger.warn(`Failed to update DB for DLQ jobs`, { error: dbErr.message });
      }
    }

    return { recovered: recoveredIds || [], deadLettered: dlqIds || [] };
  }

  /**
   * Finds workers in PostgreSQL whose heartbeats have ceased and updates them to OFFLINE.
   */
  async cleanupDeadWorkers() {
    const cutoff = new Date(Date.now() - this.workerTimeoutMs);

    try {
      const deadWorkers = await this.prisma.worker.updateMany({
        where: {
          status: "ONLINE",
          lastHeartbeat: { lt: cutoff },
        },
        data: {
          status: "OFFLINE",
        },
      });

      if (deadWorkers.count > 0) {
        logger.info(`Marked ${deadWorkers.count} inactive worker(s) as OFFLINE`);
      }
    } catch (err) {
      logger.warn(`Failed to update offline workers`, { error: err.message });
    }
  }

  /**
   * Stops the recovery manager daemon.
   */
  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    logger.info(`RecoveryManager stopped`);
  }
}
