import { Worker } from "./worker.js";
import { logger } from "../utils/logger.js";

/**
 * Coordinates a pool of independent Worker instances.
 * In a distributed architecture, workers can run across multiple processes or containers.
 * WorkerPool simulates multi-worker architectures in-process or coordinates local worker groups.
 */
export class WorkerPool {
  /**
   * @param {Object} options
   * @param {number} [options.workerCount=2] Number of independent worker instances
   * @param {number} [options.concurrency=1] Concurrency per worker instance
   * @param {string} [options.queueName] Target queue name
   * @param {Function} options.processor Async processing function
   * @param {import("ioredis").Redis} [options.redis]
   * @param {import("@prisma/client").PrismaClient} [options.prisma]
   * @param {number} [options.pollIntervalMs=100]
   */
  constructor({
    workerCount = 2,
    concurrency = 1,
    queueName = "default",
    processor,
    redis,
    prisma,
    pollIntervalMs = 100,
  }) {
    this.workerCount = Math.max(1, workerCount);
    this.concurrency = Math.max(1, concurrency);
    this.queueName = queueName;
    this.processor = processor;
    this.redis = redis;
    this.prisma = prisma;
    this.pollIntervalMs = pollIntervalMs;

    this.workers = [];
    this.isRunning = false;
  }

  /**
   * Initializes and starts all worker instances in the pool.
   */
  async start() {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info(`Starting WorkerPool`, {
      workerCount: this.workerCount,
      concurrencyPerWorker: this.concurrency,
      totalSlots: this.workerCount * this.concurrency,
      queue: this.queueName,
    });

    for (let i = 0; i < this.workerCount; i++) {
      const worker = new Worker({
        queueName: this.queueName,
        workerId: `pool-worker-${i + 1}-${Date.now().toString(36)}`,
        processor: this.processor,
        concurrency: this.concurrency,
        redis: this.redis,
        prisma: this.prisma,
        pollIntervalMs: this.pollIntervalMs,
      });

      this.workers.push(worker);
      await worker.start();
    }
  }

  /**
   * Gracefully drains and stops all workers in the pool.
   */
  async stop() {
    this.isRunning = false;
    logger.info(`Stopping WorkerPool (${this.workers.length} workers)...`);
    await Promise.all(this.workers.map((w) => w.stop()));
    this.workers = [];
    logger.info(`WorkerPool stopped cleanly`);
  }

  /**
   * Returns current statistics across the worker pool.
   */
  getStats() {
    return {
      workerCount: this.workers.length,
      activeJobs: this.workers.reduce((acc, w) => acc + w.activeJobCount, 0),
      totalCapacity: this.workerCount * this.concurrency,
    };
  }
}
