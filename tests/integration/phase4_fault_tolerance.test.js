import { RedisMemoryServer } from "redis-memory-server";
import Redis from "ioredis";
import request from "supertest";
import { getPrismaClient } from "../../src/config/db.config.js";
import { Producer } from "../../src/queue/producer.js";
import { Worker } from "../../src/worker/worker.js";
import { registerScriptsWithRedis } from "../../src/redis/scriptLoader.js";
import { createServer } from "../../src/api/server.js";

describe("Phase 4: Fault Tolerance, Exponential Backoff, DLQ & Timeouts", () => {
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

  test("1. Failed job with remaining attempts moves to DELAYED with exponential backoff", async () => {
    const queueName = "backoff-test";
    const job = await producer.enqueue({
      queueName,
      jobType: "api:sync",
      payload: { endpoint: "/external" },
      maxAttempts: 3,
    });

    const worker = new Worker({
      queueName,
      workerId: "backoff-worker",
      redis,
      prisma,
      initialBackoffMs: 800,
      pollIntervalMs: 20,
      processor: async () => {
        throw new Error("HTTP 503 Service Unavailable");
      },
    });

    await worker.start();

    // Wait until job is processed and marked DELAYED
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const check = await prisma.job.findUnique({ where: { id: job.id } });
        if (check && check.status === "DELAYED") {
          clearInterval(interval);
          resolve();
        }
      }, 50);
    });

    await worker.stop();

    // Verify in Redis: in delayed ZSET with future timestamp
    const delayedScore = await redis.zscore(`queue:${queueName}:delayed`, job.id);
    expect(delayedScore).not.toBeNull();
    expect(Number(delayedScore)).toBeGreaterThan(Date.now());

    // Verify in PostgreSQL
    const dbJob = await prisma.job.findUnique({ where: { id: job.id } });
    expect(dbJob.status).toBe("DELAYED");
    expect(dbJob.attempts).toBe(1);
    expect(dbJob.failureReason).toContain("503");
  });

  test("2. Job exceeding maxAttempts transitions to Dead Letter Queue (DLQ)", async () => {
    const queueName = "dlq-test";
    const job = await producer.enqueue({
      queueName,
      jobType: "charge:card",
      payload: { card: "expired" },
      maxAttempts: 1, // Will fail terminally on first attempt
    });

    const worker = new Worker({
      queueName,
      workerId: "dlq-worker",
      redis,
      prisma,
      pollIntervalMs: 20,
      processor: async () => {
        throw new Error("Card expired permanently");
      },
    });

    await worker.start();

    // Wait until job reaches DEAD_LETTER
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const check = await prisma.job.findUnique({ where: { id: job.id } });
        if (check && check.status === "DEAD_LETTER") {
          clearInterval(interval);
          resolve();
        }
      }, 50);
    });

    await worker.stop();

    // Verify in Redis DLQ list
    const dlqList = await redis.lrange(`queue:${queueName}:dlq`, 0, -1);
    expect(dlqList).toContain(job.id);

    // Verify in PostgreSQL
    const dbJob = await prisma.job.findUnique({ where: { id: job.id } });
    expect(dbJob.status).toBe("DEAD_LETTER");
    expect(dbJob.attempts).toBe(1);
    expect(dbJob.failedAt).not.toBeNull();
  });

  test("3. Job execution timeout terminates runaway tasks", async () => {
    const queueName = "timeout-test";
    const job = await producer.enqueue({
      queueName,
      jobType: "runaway:loop",
      payload: {},
      maxAttempts: 2,
    });

    const worker = new Worker({
      queueName,
      workerId: "timeout-worker",
      redis,
      prisma,
      jobTimeoutMs: 200, // 200ms timeout
      pollIntervalMs: 20,
      processor: async () => {
        // Runaway processor that hangs for 2000ms
        await new Promise((r) => setTimeout(r, 2000));
        return { completed: true };
      },
    });

    await worker.start();

    // Wait until timeout failure is recorded
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const check = await prisma.job.findUnique({ where: { id: job.id } });
        if (check && check.attempts >= 1) {
          clearInterval(interval);
          resolve();
        }
      }, 50);
    });

    await worker.stop();

    const dbJob = await prisma.job.findUnique({ where: { id: job.id } });
    expect(dbJob.failureReason).toContain("JobExecutionTimeout");
  });

  test("4. Dead Letter Queue replay API re-enqueues job into waiting list", async () => {
    const app = createServer({ redis, prisma, producer });

    // Enqueue a job that will fail directly into DLQ
    const job = await producer.enqueue({
      queueName: "replay-test",
      jobType: "manual:fix",
      payload: { data: "corrupt" },
      maxAttempts: 1,
    });

    // Run worker to fail it
    const worker = new Worker({
      queueName: "replay-test",
      redis,
      prisma,
      pollIntervalMs: 20,
      processor: async () => {
        throw new Error("Corrupt payload");
      },
    });

    await worker.start();
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const check = await prisma.job.findUnique({ where: { id: job.id } });
        if (check && check.status === "DEAD_LETTER") {
          clearInterval(interval);
          resolve();
        }
      }, 50);
    });
    await worker.stop();

    // Now replay via API: POST /api/jobs/:id/retry
    const replayRes = await request(app).post(`/api/jobs/${job.id}/retry`);
    expect(replayRes.status).toBe(200);
    expect(replayRes.body.success).toBe(true);

    // Verify state in PostgreSQL: WAITING, attempts = 0
    const replayedDbJob = await prisma.job.findUnique({ where: { id: job.id } });
    expect(replayedDbJob.status).toBe("WAITING");
    expect(replayedDbJob.attempts).toBe(0);

    // Verify state in Redis: present in waiting FIFO list, absent from DLQ
    const waitingList = await redis.lrange("queue:replay-test:waiting", 0, -1);
    expect(waitingList).toContain(job.id);
    const dlqList = await redis.lrange("queue:replay-test:dlq", 0, -1);
    expect(dlqList).not.toContain(job.id);
  });
});
