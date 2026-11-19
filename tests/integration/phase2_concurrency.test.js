import { RedisMemoryServer } from "redis-memory-server";
import Redis from "ioredis";
import { getPrismaClient } from "../../src/config/db.config.js";
import { Producer } from "../../src/queue/producer.js";
import { Worker } from "../../src/worker/worker.js";
import { WorkerPool } from "../../src/worker/workerPool.js";
import { registerScriptsWithRedis } from "../../src/redis/scriptLoader.js";

describe("Phase 2: Distributed Processing & High-Concurrency Race Condition Tests", () => {
  let redisServer;
  let redis;
  let prisma;
  let producer;

  beforeAll(async () => {
    redisServer = new RedisMemoryServer();
    const host = await redisServer.getHost();
    const port = await redisServer.getPort();
    redis = new Redis({ host, port });
    await registerScriptsWithRedis(redis);

    prisma = getPrismaClient();
    await prisma.$connect();

    await prisma.jobExecution.deleteMany();
    await prisma.job.deleteMany();

    producer = new Producer({ redis, prisma });
  }, 30000);

  afterAll(async () => {
    if (prisma) {
      await prisma.jobExecution.deleteMany();
      await prisma.job.deleteMany();
      await prisma.$disconnect();
    }
    if (redis) {
      await redis.quit();
    }
    if (redisServer) {
      await redisServer.stop();
    }
  });

  test("1. High concurrency in a single worker (concurrency=5) processes 25 jobs without data races", async () => {
    const queueName = "single-worker-concurrency";
    const jobCount = 25;
    const processedJobIds = new Set();
    const claimLogs = [];

    // Enqueue 25 jobs in parallel
    const enqueuePromises = [];
    for (let i = 0; i < jobCount; i++) {
      enqueuePromises.push(
        producer.enqueue({
          queueName,
          jobType: "batch:item",
          payload: { index: i },
        })
      );
    }
    const createdJobs = await Promise.all(enqueuePromises);
    expect(createdJobs.length).toBe(jobCount);

    const worker = new Worker({
      queueName,
      concurrency: 5,
      redis,
      prisma,
      pollIntervalMs: 20,
      processor: async (job) => {
        // Record claim to detect any duplicate processing
        if (processedJobIds.has(job.id)) {
          throw new Error(`DUPLICATE CLAIM DETECTED: Job ${job.id} was claimed twice!`);
        }
        processedJobIds.add(job.id);
        claimLogs.push({ jobId: job.id, timestamp: Date.now() });

        // Simulate varying async processing latency
        await new Promise((r) => setTimeout(r, 15 + Math.random() * 20));
        return { success: true };
      },
    });

    await worker.start();

    // Wait until all jobs are processed
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const completedCount = await prisma.job.count({
          where: { queueName, status: "COMPLETED" },
        });
        if (completedCount === jobCount) {
          clearInterval(interval);
          resolve();
        }
      }, 50);
    });

    await worker.stop();

    // Verification
    expect(processedJobIds.size).toBe(jobCount);
    const waitingLen = await redis.llen(`queue:${queueName}:waiting`);
    expect(waitingLen).toBe(0);
    const activeCount = await redis.zcard(`queue:${queueName}:active`);
    expect(activeCount).toBe(0);
  }, 20000);

  test("2. Multiple competing workers (4 workers, 50 jobs) - Zero duplicate claims guarantee", async () => {
    const queueName = "multi-worker-race";
    const jobCount = 50;
    const workerCount = 4;
    const concurrencyPerWorker = 2; // Total 8 concurrent slots

    // Enqueue 50 jobs
    const enqueued = [];
    for (let i = 0; i < jobCount; i++) {
      enqueued.push(
        producer.enqueue({
          queueName,
          jobType: "heavy:calc",
          payload: { data: `data-${i}` },
        })
      );
    }
    const jobs = await Promise.all(enqueued);
    expect(jobs.length).toBe(jobCount);

    const claimedByWorker = new Map(); // workerId -> Set of jobIds
    const globallyProcessedJobIds = new Set();
    let duplicateClaimsDetected = 0;

    const pool = new WorkerPool({
      queueName,
      workerCount,
      concurrency: concurrencyPerWorker,
      redis,
      prisma,
      pollIntervalMs: 20,
      processor: async (job) => {
        // Strict invariant check: exactly one claim
        if (globallyProcessedJobIds.has(job.id)) {
          duplicateClaimsDetected++;
          throw new Error(`CRITICAL: Job ${job.id} was claimed by multiple workers!`);
        }
        globallyProcessedJobIds.add(job.id);

        // Track which worker processed the job
        const workerId = job.lockToken ? job.lockToken.slice(0, 8) : "unknown";
        if (!claimedByWorker.has(workerId)) {
          claimedByWorker.set(workerId, new Set());
        }
        claimedByWorker.get(workerId).add(job.id);

        // Simulate async I/O
        await new Promise((r) => setTimeout(r, 20));
        return { done: true };
      },
    });

    await pool.start();

    // Poll until all jobs are marked COMPLETED in PostgreSQL
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("Timeout waiting for 50 jobs to complete across worker pool"));
      }, 25000);

      const interval = setInterval(async () => {
        const completed = await prisma.job.count({
          where: { queueName, status: "COMPLETED" },
        });

        if (completed === jobCount) {
          clearInterval(interval);
          clearTimeout(timeout);
          resolve();
        }
      }, 50);
    });

    await pool.stop();

    // Assertions
    expect(duplicateClaimsDetected).toBe(0);
    expect(globallyProcessedJobIds.size).toBe(jobCount);

    // Verify all 50 jobs in PostgreSQL have status COMPLETED and exactly 1 execution
    const allDbJobs = await prisma.job.findMany({
      where: { queueName },
      include: { executions: true },
    });

    expect(allDbJobs.length).toBe(jobCount);
    for (const dbJob of allDbJobs) {
      expect(dbJob.status).toBe("COMPLETED");
      expect(dbJob.attempts).toBe(1);
      expect(dbJob.executions.length).toBe(1);
      expect(dbJob.executions[0].status).toBe("COMPLETED");
    }

    // Verify Redis state is clean
    const waitingLen = await redis.llen(`queue:${queueName}:waiting`);
    expect(waitingLen).toBe(0);
    const activeLen = await redis.zcard(`queue:${queueName}:active`);
    expect(activeLen).toBe(0);
  }, 35000);
});
