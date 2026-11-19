import { RedisMemoryServer } from "redis-memory-server";
import Redis from "ioredis";
import request from "supertest";
import { getPrismaClient } from "../../src/config/db.config.js";
import { Producer } from "../../src/queue/producer.js";
import { Worker } from "../../src/worker/worker.js";
import { registerScriptsWithRedis } from "../../src/redis/scriptLoader.js";
import { createServer } from "../../src/api/server.js";

describe("Phase 6: Monitoring Dashboard & Statistics API", () => {
  let redisServer;
  let redis;
  let prisma;
  let producer;
  let app;

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
    await prisma.worker.deleteMany();

    producer = new Producer({ redis, prisma });
    app = createServer({ redis, prisma, producer });
  }, 30000);

  afterAll(async () => {
    if (prisma) {
      await prisma.jobExecution.deleteMany();
      await prisma.job.deleteMany();
      await prisma.worker.deleteMany();
      await prisma.$disconnect();
    }
    if (redis) {
      await redis.quit();
    }
    if (redisServer) {
      await redisServer.stop();
    }
  });

  test("1. GET /api/queues lists all queues and summary metrics", async () => {
    const queueName = "metrics-test";

    // Enqueue 2 jobs
    await producer.enqueue({
      queueName,
      jobType: "stat:item",
      payload: {},
    });
    await producer.enqueue({
      queueName,
      jobType: "stat:item",
      payload: {},
    });

    const res = await request(app).get("/api/queues");
    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(Array.isArray(res.body.data)).toBe(true);

    const queueStats = res.body.data.find((q) => q.queueName === queueName);
    expect(queueStats).toBeDefined();
    expect(queueStats.counts.waiting).toBe(2);
  });

  test("2. GET /api/queues/:name/stats returns deep breakdown and execution latency", async () => {
    const queueName = "latency-test";

    const job = await producer.enqueue({
      queueName,
      jobType: "calc:pi",
      payload: {},
    });

    // Run worker to process and record latency
    const worker = new Worker({
      queueName,
      redis,
      prisma,
      pollIntervalMs: 20,
      processor: async () => {
        await new Promise((r) => setTimeout(r, 40));
        return { pi: 3.14159 };
      },
    });

    await worker.start();
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const check = await prisma.job.findUnique({ where: { id: job.id } });
        if (check && check.status === "COMPLETED") {
          clearInterval(interval);
          resolve();
        }
      }, 30);
    });
    await worker.stop();

    const res = await request(app).get(`/api/queues/${queueName}/stats`);
    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.data.counts.completed).toBe(1);
    expect(res.body.data.latency.avgMs).toBeGreaterThanOrEqual(30);
  });

  test("3. GET /api/workers returns active registered workers with health status", async () => {
    const worker = new Worker({
      queueName: "worker-stats-queue",
      workerId: "stats-worker-99",
      concurrency: 3,
      redis,
      prisma,
      processor: async () => ({}),
    });

    await worker.start();

    const res = await request(app).get("/api/workers");
    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);

    const target = res.body.data.find((w) => w.id === "stats-worker-99");
    expect(target).toBeDefined();
    expect(target.status).toBe("ONLINE");
    expect(target.concurrency).toBe(3);

    await worker.stop();
  });

  test("4. GET /dashboard serves the interactive web dashboard HTML", async () => {
    const res = await request(app).get("/dashboard");
    expect(res.status).toBe(200);
    expect(res.headers["content-type"]).toContain("text/html");
    expect(res.text).toContain("AadiMQ");
  });
});
