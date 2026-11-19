import { getRedisClient } from "../redis/client.js";
import { getPrismaClient } from "../config/db.config.js";

/**
 * Service to aggregate real-time Redis queue lengths and historical database metrics.
 */
export class QueueStatsService {
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
   * Retrieves comprehensive metrics for a specific queue.
   * @param {string} queueName
   * @returns {Promise<Object>}
   */
  async getQueueStats(queueName) {
    const waitingKey = `queue:${queueName}:waiting`;
    const priorityKey = `queue:${queueName}:waiting:priority`;
    const activeKey = `queue:${queueName}:active`;
    const delayedKey = `queue:${queueName}:delayed`;
    const dlqKey = `queue:${queueName}:dlq`;

    // Fetch real-time Redis counts concurrently
    const [
      waitingCount,
      priorityCount,
      activeCount,
      delayedCount,
      dlqCount,
    ] = await Promise.all([
      this.redis.llen(waitingKey).catch(() => 0),
      this.redis.zcard(priorityKey).catch(() => 0),
      this.redis.zcard(activeKey).catch(() => 0),
      this.redis.zcard(delayedKey).catch(() => 0),
      this.redis.llen(dlqKey).catch(() => 0),
    ]);

    // Fetch durable historical totals from PostgreSQL
    const [completedCount, failedCount, totalCount, latencyAgg] = await Promise.all([
      this.prisma.job.count({ where: { queueName, status: "COMPLETED" } }),
      this.prisma.job.count({ where: { queueName, status: { in: ["FAILED", "DEAD_LETTER"] } } }),
      this.prisma.job.count({ where: { queueName } }),
      this.prisma.jobExecution.aggregate({
        where: { job: { queueName }, status: "COMPLETED" },
        _avg: { durationMs: true },
        _min: { durationMs: true },
        _max: { durationMs: true },
      }),
    ]);

    return {
      queueName,
      counts: {
        waiting: waitingCount + priorityCount,
        waitingFifo: waitingCount,
        waitingPriority: priorityCount,
        active: activeCount,
        delayed: delayedCount,
        deadLetter: dlqCount,
        completed: completedCount,
        failed: failedCount,
        totalSubmitted: totalCount,
      },
      latency: {
        avgMs: Math.round(latencyAgg._avg?.durationMs || 0),
        minMs: latencyAgg._min?.durationMs || 0,
        maxMs: latencyAgg._max?.durationMs || 0,
      },
    };
  }

  /**
   * Discovers all queues and returns aggregate statistics.
   * @returns {Promise<Object[]>}
   */
  async getAllQueuesStats() {
    // 1. Discover queues from DB distinct queueNames
    const dbQueues = await this.prisma.job.findMany({
      select: { queueName: true },
      distinct: ["queueName"],
    });

    const queueNames = new Set(dbQueues.map((q) => q.queueName));
    queueNames.add("default");

    // 2. Discover from Redis keys
    try {
      const redisKeys = await this.redis.keys("queue:*:waiting");
      for (const k of redisKeys) {
        const parts = k.split(":");
        if (parts.length >= 2) queueNames.add(parts[1]);
      }
    } catch (err) {
      // Ignore keys scanning errors
    }

    const results = [];
    for (const name of queueNames) {
      const stats = await this.getQueueStats(name);
      results.push(stats);
    }

    return results;
  }

  /**
   * Retrieves all registered workers with real-time status.
   * @returns {Promise<Object[]>}
   */
  async getWorkers() {
    const dbWorkers = await this.prisma.worker.findMany({
      orderBy: { lastHeartbeat: "desc" },
    });

    // Check live Redis state for active jobs
    const enriched = await Promise.all(
      dbWorkers.map(async (w) => {
        const redisKey = `worker:${w.id}`;
        const redisData = await this.redis.hgetall(redisKey).catch(() => ({}));
        const isLive = Boolean(redisData && redisData.id);

        return {
          id: w.id,
          hostname: w.hostname,
          pid: w.pid,
          status: isLive ? "ONLINE" : w.status,
          concurrency: w.concurrency,
          activeJobs: parseInt(redisData.activeJobs || "0", 10),
          lastHeartbeat: w.lastHeartbeat,
          startedAt: w.startedAt,
        };
      })
    );

    return enriched;
  }
}
