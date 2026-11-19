import { RedisMemoryServer } from "redis-memory-server";
import Redis from "ioredis";
import request from "supertest";
import { getPrismaClient } from "../../src/config/db.config.js";
import { Producer } from "../../src/queue/producer.js";
import { Worker } from "../../src/worker/worker.js";
import { Scheduler } from "../../src/scheduler/scheduler.js";
import { registerScriptsWithRedis, runScript } from "../../src/redis/scriptLoader.js";
import { createServer } from "../../src/api/server.js";

describe("Phase 5: Advanced Scheduling (Priority, Delayed Jobs, Cancellation & Rate Limiting)", () => {
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

  test("1. Priority scheduling: High-priority jobs are claimed before lower-priority jobs", async () => {
    const queueName = "priority-test";

    // Enqueue in reverse priority order
    const jobLow = await producer.enqueue({
      queueName,
      jobType: "low:prio",
      priority: 0,
      payload: { rank: "low" },
    });

    const jobMed = await producer.enqueue({
      queueName,
      jobType: "med:prio",
      priority: 10,
      payload: { rank: "med" },
    });

    const jobHigh = await producer.enqueue({
      queueName,
      jobType: "high:prio",
      priority: 50,
      payload: { rank: "high" },
    });

    const executionOrder = [];

    const worker = new Worker({
      queueName,
      redis,
      prisma,
      pollIntervalMs: 20,
      processor: async (j) => {
        executionOrder.push(j.id);
        return { rank: j.payload.rank };
      },
    });

    await worker.start();

    // Wait until all 3 jobs are completed
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        if (executionOrder.length === 3) {
          clearInterval(interval);
          resolve();
        }
      }, 50);
    });

    await worker.stop();

    // Verify strict priority execution order: High (50) -> Med (10) -> Low (0)
    expect(executionOrder[0]).toBe(jobHigh.id);
    expect(executionOrder[1]).toBe(jobMed.id);
    expect(executionOrder[2]).toBe(jobLow.id);
  });

  test("2. Delayed jobs remain dormant until scheduled timestamp and scheduler promotion", async () => {
    const queueName = "delayed-test";
    const delayMs = 400; // 400ms delay

    const job = await producer.enqueue({
      queueName,
      jobType: "reminder:notify",
      payload: { text: "Time to wake up" },
      delayMs,
    });

    expect(job.status).toBe("DELAYED");

    // Before delay expires: job is in delayed ZSET and not in waiting list
    const delayedScore = await redis.zscore(`queue:${queueName}:delayed`, job.id);
    expect(delayedScore).not.toBeNull();
    const waitingLen = await redis.llen(`queue:${queueName}:waiting`);
    expect(waitingLen).toBe(0);

    let executed = false;
    const worker = new Worker({
      queueName,
      redis,
      prisma,
      pollIntervalMs: 20,
      processor: async () => {
        executed = true;
        return { done: true };
      },
    });
    await worker.start();

    // Check that worker did NOT process it immediately
    await new Promise((r) => setTimeout(r, 100));
    expect(executed).toBe(false);

    // Wait past the delay threshold and trigger Scheduler promotion
    await new Promise((r) => setTimeout(r, 350));

    const scheduler = new Scheduler({
      queues: [queueName],
      redis,
      prisma,
    });

    const promoted = await scheduler.promoteQueue(queueName);
    expect(promoted).toContain(job.id);

    // Worker now processes the promoted job
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const check = await prisma.job.findUnique({ where: { id: job.id } });
        if (check && check.status === "COMPLETED") {
          clearInterval(interval);
          resolve();
        }
      }, 50);
    });

    await worker.stop();
    expect(executed).toBe(true);
  });

  test("3. Job cancellation: DELETE /api/jobs/:id removes job from queue and marks CANCELLED", async () => {
    const app = createServer({ redis, prisma, producer });

    // Enqueue a delayed job that we will cancel before it runs
    const job = await producer.enqueue({
      queueName: "cancel-test",
      jobType: "will:cancel",
      payload: {},
      delayMs: 10000,
    });

    const deleteRes = await request(app).delete(`/api/jobs/${job.id}`);
    expect(deleteRes.status).toBe(200);
    expect(deleteRes.body.success).toBe(true);

    // Verify in PostgreSQL: status is CANCELLED
    const dbJob = await prisma.job.findUnique({ where: { id: job.id } });
    expect(dbJob.status).toBe("CANCELLED");

    // Verify in Redis: removed from delayed ZSET
    const delayedScore = await redis.zscore("queue:cancel-test:delayed", job.id);
    expect(delayedScore).toBeNull();
  });

  test("4. Rate limiting: Enforces sliding window throughput threshold", async () => {
    const rateLimitKey = "ratelimit:api:tenant-123";
    const limit = 3;
    const windowMs = 1000;
    const now = Date.now();

    // 1st request -> allowed
    const res1 = await runScript(redis, "rate_limit", [rateLimitKey], [limit, windowMs, now, "req-1"]);
    expect(res1[0]).toBe(1);

    // 2nd request -> allowed
    const res2 = await runScript(redis, "rate_limit", [rateLimitKey], [limit, windowMs, now + 10, "req-2"]);
    expect(res2[0]).toBe(1);

    // 3rd request -> allowed
    const res3 = await runScript(redis, "rate_limit", [rateLimitKey], [limit, windowMs, now + 20, "req-3"]);
    expect(res3[0]).toBe(1);

    // 4th request -> blocked (rate limited!)
    const res4 = await runScript(redis, "rate_limit", [rateLimitKey], [limit, windowMs, now + 30, "req-4"]);
    expect(res4[0]).toBe(0);
  });
});
