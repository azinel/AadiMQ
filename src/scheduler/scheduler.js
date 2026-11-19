import { getRedisClient } from "../redis/client.js";
import { runScript } from "../redis/scriptLoader.js";
import { getPrismaClient } from "../config/db.config.js";
import { logger } from "../utils/logger.js";

/**
 * Scheduler daemon that periodically inspects delayed queues and promotes
 * matured jobs to the active waiting queues for worker consumption.
 */
export class Scheduler {
  /**
   * @param {Object} [options]
   * @param {string[]} [options.queues] Array of queue names to manage
   * @param {number} [options.pollIntervalMs=1000] Interval for checking delayed queues
   * @param {import("ioredis").Redis} [options.redis]
   * @param {import("@prisma/client").PrismaClient} [options.prisma]
   */
  constructor(options = {}) {
    this.queues = options.queues || ["default"];
    this.pollIntervalMs = options.pollIntervalMs || 1000;
    this.redis = options.redis || getRedisClient();
    this.prisma = options.prisma || getPrismaClient();

    this.timer = null;
    this.isRunning = false;
  }

  /**
   * Starts the background scheduler loop.
   */
  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info(`Scheduler started`, {
      queues: this.queues,
      intervalMs: this.pollIntervalMs,
    });

    this.timer = setInterval(async () => {
      try {
        await this.promoteAll();
      } catch (err) {
        logger.error(`Scheduler promotion cycle encountered error`, {
          error: err.message,
        });
      }
    }, this.pollIntervalMs);
  }

  /**
   * Immediately checks all queues and promotes all matured delayed jobs.
   */
  async promoteAll() {
    for (const queue of this.queues) {
      await this.promoteQueue(queue);
    }
  }

  /**
   * Promotes matured jobs for a specific queue.
   * @param {string} queueName
   * @returns {Promise<string[]>} Array of promoted job IDs
   */
  async promoteQueue(queueName) {
    const delayedKey = `queue:${queueName}:delayed`;
    const waitingKey = `queue:${queueName}:waiting`;
    const priorityKey = `queue:${queueName}:waiting:priority`;
    const now = Date.now();

    const promotedIds = await runScript(
      this.redis,
      "promote_delayed_jobs",
      [delayedKey, waitingKey, priorityKey],
      [now, 50]
    );

    if (promotedIds && promotedIds.length > 0) {
      logger.info(`Promoted ${promotedIds.length} delayed job(s) in queue '${queueName}'`, {
        jobIds: promotedIds,
      });

      // Synchronize in PostgreSQL
      try {
        await this.prisma.job.updateMany({
          where: { id: { in: promotedIds } },
          data: { status: "WAITING" },
        });
      } catch (dbErr) {
        logger.warn(`Failed to update DB on delayed job promotion`, { error: dbErr.message });
      }
    }

    return promotedIds || [];
  }

  /**
   * Stops the scheduler daemon.
   */
  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    logger.info("Scheduler stopped");
  }
}
