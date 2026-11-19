import { RedisMemoryServer } from "redis-memory-server";
import Redis from "ioredis";
import request from "supertest";
import { getPrismaClient } from "../../src/config/db.config.js";
import { Producer } from "../../src/queue/producer.js";
import { Worker } from "../../src/worker/worker.js";
import { registerScriptsWithRedis } from "../../src/redis/scriptLoader.js";
import { createServer } from "../../src/api/server.js";

describe("Phase 1: End-to-End Distributed Job Queue Integration", () => {
  let redisServer;
  let redis;
  let prisma;
  let producer;

  beforeAll(async () => {
    // 1. Spin up an in-memory Redis instance for clean, isolated testing
    redisServer = new RedisMemoryServer();
    const host = await redisServer.getHost();
    const port = await redisServer.getPort();
    redis = new Redis({ host, port });
    await registerScriptsWithRedis(redis);

    // 2. Connect to local PostgreSQL test database
    prisma = getPrismaClient();
    await prisma.$connect();

    // 3. Clean up any previous test data in DB
    await prisma.jobExecution.deleteMany();
    await prisma.job.deleteMany();

    // 4. Initialize Producer with test clients
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

  test("1. Producer enqueues a job into PostgreSQL and Redis waiting FIFO", async () => {
    const job = await producer.enqueue({
      queueName: "integration-test",
      jobType: "email:welcome",
      payload: { userId: "user-101", email: "user@example.com" },
    });

    expect(job.id).toBeDefined();
    expect(job.status).toBe("WAITING");

    // Verify in PostgreSQL
    const dbJob = await prisma.job.findUnique({ where: { id: job.id } });
    expect(dbJob).not.toBeNull();
    expect(dbJob.status).toBe("WAITING");
    expect(dbJob.jobType).toBe("email:welcome");
    expect(dbJob.attempts).toBe(0);

    // Verify in Redis
    const waitingJobs = await redis.lrange("queue:integration-test:waiting", 0, -1);
    expect(waitingJobs).toContain(job.id);

    const redisHash = await redis.hgetall(`job:${job.id}`);
    expect(redisHash.status).toBe("WAITING");
    expect(redisHash.type).toBe("email:welcome");
  });

  test("2. Enqueue enforces idempotency and does not duplicate jobs", async () => {
    const idempotencyKey = "order-checkout-ref-9876";

    const job1 = await producer.enqueue({
      queueName: "orders",
      jobType: "order:charge",
      payload: { orderId: 9876, amount: 150 },
      idempotencyKey,
    });

    const job2 = await producer.enqueue({
      queueName: "orders",
      jobType: "order:charge",
      payload: { orderId: 9876, amount: 150 },
      idempotencyKey,
    });

    // Both calls must return the same job ID
    expect(job1.id).toBe(job2.id);

    // PostgreSQL must have exactly 1 record
    const count = await prisma.job.count({ where: { idempotencyKey } });
    expect(count).toBe(1);
  });

  test("3. Worker claims job atomically, executes, and marks COMPLETED in both stores", async () => {
    const testQueue = "worker-success-test";
    const job = await producer.enqueue({
      queueName: testQueue,
      jobType: "report:pdf",
      payload: { documentId: "doc-55" },
    });

    let executedJob = null;
    let executionCount = 0;

    const worker = new Worker({
      queueName: testQueue,
      redis,
      prisma,
      pollIntervalMs: 50,
      processor: async (j) => {
        executedJob = j;
        executionCount++;
        return { pdfUrl: "https://storage.example.com/doc-55.pdf" };
      },
    });

    await worker.start();

    // Wait until job is completed
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

    // Verify execution
    expect(executionCount).toBe(1);
    expect(executedJob.id).toBe(job.id);

    // Verify Redis state: removed from active, status is COMPLETED
    const activeCount = await redis.zcard(`queue:${testQueue}:active`);
    expect(activeCount).toBe(0);

    const redisHash = await redis.hgetall(`job:${job.id}`);
    expect(redisHash.status).toBe("COMPLETED");

    // Verify PostgreSQL state: COMPLETED and JobExecution audit trail
    const updatedDbJob = await prisma.job.findUnique({
      where: { id: job.id },
      include: { executions: true },
    });

    expect(updatedDbJob.status).toBe("COMPLETED");
    expect(updatedDbJob.attempts).toBe(1);
    expect(updatedDbJob.completedAt).not.toBeNull();
    expect(updatedDbJob.executions.length).toBe(1);
    expect(updatedDbJob.executions[0].status).toBe("COMPLETED");
    expect(updatedDbJob.executions[0].result).toEqual({
      pdfUrl: "https://storage.example.com/doc-55.pdf",
    });
  });

  test("4. Worker handles processor failure and re-enqueues job if attempts < maxAttempts", async () => {
    const testQueue = "worker-failure-test";
    const job = await producer.enqueue({
      queueName: testQueue,
      jobType: "payment:process",
      payload: { card: "invalid" },
      maxAttempts: 3,
    });

    let attemptsSeen = 0;

    const worker = new Worker({
      queueName: testQueue,
      redis,
      prisma,
      pollIntervalMs: 50,
      processor: async () => {
        attemptsSeen++;
        throw new Error("Payment gateway declined transaction");
      },
    });

    await worker.start();

    // Wait for the first attempt to fail and re-queue
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

    expect(attemptsSeen).toBeGreaterThanOrEqual(1);

    // Verify in PostgreSQL: status=WAITING (re-queued), failure reason recorded
    const dbJob = await prisma.job.findUnique({
      where: { id: job.id },
      include: { executions: true },
    });

    expect(dbJob.attempts).toBeGreaterThanOrEqual(1);
    expect(dbJob.failureReason).toContain("Payment gateway declined");
    expect(dbJob.executions.length).toBeGreaterThanOrEqual(1);
    expect(dbJob.executions[0].status).toBe("FAILED");
  });

  test("5. REST API: POST /api/jobs, GET /api/jobs/:id, GET /api/jobs, GET /api/health", async () => {
    const app = createServer({ redis, prisma, producer });

    // Test POST /api/jobs
    const postRes = await request(app)
      .post("/api/jobs")
      .send({
        queueName: "api-test",
        jobType: "notification:push",
        payload: { title: "New Message", body: "You have 1 unread message" },
      });

    expect(postRes.status).toBe(201);
    expect(postRes.body.success).toBe(true);
    const createdId = postRes.body.data.id;
    expect(createdId).toBeDefined();

    // Test GET /api/jobs/:id
    const getRes = await request(app).get(`/api/jobs/${createdId}`);
    expect(getRes.status).toBe(200);
    expect(getRes.body.success).toBe(true);
    expect(getRes.body.data.id).toBe(createdId);
    expect(getRes.body.data.jobType).toBe("notification:push");

    // Test GET /api/jobs (list)
    const listRes = await request(app).get("/api/jobs?queue=api-test");
    expect(listRes.status).toBe(200);
    expect(listRes.body.success).toBe(true);
    expect(Array.isArray(listRes.body.data)).toBe(true);
    expect(listRes.body.pagination.total).toBeGreaterThanOrEqual(1);

    // Test GET /api/health
    const healthRes = await request(app).get("/api/health");
    expect(healthRes.status).toBe(200);
    expect(healthRes.body.status).toBe("ok");
    expect(healthRes.body.services.database).toBe("healthy");
  });
});
