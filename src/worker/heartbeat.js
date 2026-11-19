import { runScript } from "../redis/scriptLoader.js";
import { logger } from "../utils/logger.js";

/**
 * Manages heartbeat pinging and background lease renewals for a Worker process.
 */
export class HeartbeatManager {
  /**
   * @param {Object} options
   * @param {string} options.workerId
   * @param {string} options.queueName
   * @param {import("ioredis").Redis} options.redis
   * @param {import("@prisma/client").PrismaClient} options.prisma
   * @param {number} [options.intervalMs=2000]
   * @param {number} [options.leaseDurationMs=10000]
   */
  constructor({
    workerId,
    queueName,
    redis,
    prisma,
    intervalMs = 2000,
    leaseDurationMs = 10000,
  }) {
    this.workerId = workerId;
    this.queueName = queueName;
    this.redis = redis;
    this.prisma = prisma;
    this.intervalMs = intervalMs;
    this.leaseDurationMs = leaseDurationMs;

    this.activeLeases = new Map(); // jobId -> { lockToken, leaseDurationMs }
    this.timer = null;
    this.isRunning = false;
  }

  /**
   * Registers a newly claimed job for automatic background lease renewals.
   * @param {string} jobId
   * @param {string} lockToken
   * @param {number} [leaseDurationMs]
   */
  registerJob(jobId, lockToken, leaseDurationMs = null) {
    this.activeLeases.set(jobId, {
      lockToken,
      leaseDurationMs: leaseDurationMs || this.leaseDurationMs,
    });
  }

  /**
   * Unregisters a job when execution concludes.
   * @param {string} jobId
   */
  unregisterJob(jobId) {
    this.activeLeases.delete(jobId);
  }

  /**
   * Starts the periodic heartbeat & lease renewal loop.
   */
  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    this.timer = setInterval(async () => {
      await this.ping();
    }, this.intervalMs);

    // Initial ping
    this.ping().catch((err) => {
      logger.warn(`Initial heartbeat ping failed`, { error: err.message });
    });
  }

  /**
   * Performs an atomic renewal of worker registration and all active job leases.
   */
  async ping() {
    if (!this.isRunning) return;
    const now = Date.now();

    // 1. Renew Worker registration in Redis with TTL
    try {
      const workerKey = `worker:${this.workerId}`;
      await this.redis
        .pipeline()
        .hset(workerKey, {
          id: this.workerId,
          queue: this.queueName,
          activeJobs: this.activeLeases.size,
          lastHeartbeat: now,
        })
        .expire(workerKey, Math.ceil((this.intervalMs * 3) / 1000))
        .exec();
    } catch (err) {
      logger.warn(`Worker Redis heartbeat failed`, {
        workerId: this.workerId,
        error: err.message,
      });
    }

    // 2. Renew active job leases via renew_lease.lua
    const activeKey = `queue:${this.queueName}:active`;
    for (const [jobId, { lockToken, leaseDurationMs }] of this.activeLeases.entries()) {
      try {
        const result = await runScript(
          this.redis,
          "renew_lease",
          [activeKey, `job:${jobId}`],
          [jobId, lockToken, leaseDurationMs, now]
        );

        const [status, detail] = result || [];
        if (status === "ERR_FENCE_MISMATCH") {
          logger.error(`Heartbeat lease renewal failed: fencing token mismatch`, {
            jobId,
            workerId: this.workerId,
          });
          this.activeLeases.delete(jobId);
        }
      } catch (err) {
        logger.warn(`Failed to renew lease for job ${jobId}`, { error: err.message });
      }
    }

    // 3. Update PostgreSQL Worker heartbeat periodically (best-effort)
    try {
      await this.prisma.worker.updateMany({
        where: { id: this.workerId },
        data: { lastHeartbeat: new Date(now) },
      });
    } catch (dbErr) {
      // Ignore transient DB heartbeat failures
    }
  }

  /**
   * Stops the heartbeat loop.
   */
  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.activeLeases.clear();
  }
}
