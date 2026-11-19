import { RedisMemoryServer } from "redis-memory-server";
import Redis from "ioredis";
import { getPrismaClient } from "../../src/config/db.config.js";
import { Producer } from "../../src/queue/producer.js";
import { Worker } from "../../src/worker/worker.js";
import { RecoveryManager } from "../../src/scheduler/recoveryManager.js";
import { registerScriptsWithRedis } from "../../src/redis/scriptLoader.js";

describe("Phase 3: Worker Coordination, Heartbeats & Crash Recovery Tests", () => {
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
    await prisma.worker.deleteMany();

    producer = new Producer({ redis, prisma });
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

  test("1. Worker registration creates active records in PostgreSQL and Redis", async () => {
    const queueName = "reg-test";
    const worker = new Worker({
      queueName,
      workerId: "worker-reg-test-1",
      redis,
      prisma,
      processor: async () => ({ ok: true }),
    });

    await worker.start();

    // Check PostgreSQL registration
    const dbWorker = await prisma.worker.findUnique({
      where: { id: "worker-reg-test-1" },
    });
    expect(dbWorker).not.toBeNull();
    expect(dbWorker.status).toBe("ONLINE");

    // Check Redis registration
    const redisWorker = await redis.hgetall("worker:worker-reg-test-1");
    expect(redisWorker.id).toBe("worker-reg-test-1");
    expect(redisWorker.queue).toBe(queueName);

    await worker.stop();

    // After graceful stop, status must be OFFLINE
    const stoppedWorker = await prisma.worker.findUnique({
      where: { id: "worker-reg-test-1" },
    });
    expect(stoppedWorker.status).toBe("OFFLINE");
  });

  test("2. Heartbeat automatically renews lease during long-running execution", async () => {
    const queueName = "heartbeat-test";
    const initialLeaseMs = 600; // 600ms lease

    const job = await producer.enqueue({
      queueName,
      jobType: "long:task",
      payload: { steps: 5 },
    });

    let completed = false;

    const worker = new Worker({
      queueName,
      workerId: "worker-heartbeat-runner",
      redis,
      prisma,
      leaseDurationMs: initialLeaseMs,
      pollIntervalMs: 50,
      processor: async () => {
        // Task runs for 1200ms (> initial 600ms lease)
        // HeartbeatManager must extend the lease during this time
        await new Promise((r) => setTimeout(r, 1200));
        completed = true;
        return { success: true };
      },
    });

    await worker.start();

    // Wait for completion
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

    expect(completed).toBe(true);

    // Job completed successfully without fence mismatch or timeout
    const finalJob = await prisma.job.findUnique({ where: { id: job.id } });
    expect(finalJob.status).toBe("COMPLETED");
  }, 15000);

  test("3. Worker crash simulation: RecoveryManager detects expired lease and re-queues job for second worker", async () => {
    const queueName = "recovery-test";
    const leaseDurationMs = 300; // Very short lease

    const job = await producer.enqueue({
      queueName,
      jobType: "crash:simulate",
      payload: { itemId: "item-777" },
      maxAttempts: 3,
    });

    // Worker 1: Claims the job and immediately "crashes" (stops heartbeating without completing)
    const worker1 = new Worker({
      queueName,
      workerId: "crashed-worker-1",
      redis,
      prisma,
      leaseDurationMs,
      pollIntervalMs: 20,
      processor: async () => {
        // Stop heartbeat immediately to simulate an abrupt crash
        worker1.heartbeat.stop();
        // Hang indefinitely so the lease expires
        await new Promise((r) => setTimeout(r, 5000));
        return { shouldNotReach: true };
      },
    });

    await worker1.start();

    // Wait until job is in active state
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const activeCount = await redis.zcard(`queue:${queueName}:active`);
        if (activeCount > 0) {
          clearInterval(interval);
          resolve();
        }
      }, 30);
    });

    // Wait for the lease to expire (300ms lease + 150ms buffer)
    await new Promise((r) => setTimeout(r, 500));

    // Stop worker1 loop to clean up the hung promise
    worker1.isRunning = false;

    // Run RecoveryManager to reclaim the stalled job
    const recoveryManager = new RecoveryManager({
      queues: [queueName],
      redis,
      prisma,
    });

    const recoveryResult = await recoveryManager.recoverQueue(queueName);
    expect(recoveryResult.recovered).toContain(job.id);

    // The job must now be back in the waiting list in Redis
    const waitingList = await redis.lrange(`queue:${queueName}:waiting`, 0, -1);
    expect(waitingList).toContain(job.id);

    // Worker 2: Starts up and successfully picks up the recovered job!
    let worker2Processed = false;
    const worker2 = new Worker({
      queueName,
      workerId: "healthy-worker-2",
      redis,
      prisma,
      pollIntervalMs: 30,
      processor: async (j) => {
        expect(j.id).toBe(job.id);
        worker2Processed = true;
        return { recoveredAndDone: true };
      },
    });

    await worker2.start();

    // Wait until Worker 2 completes the job
    await new Promise((resolve) => {
      const interval = setInterval(async () => {
        const check = await prisma.job.findUnique({ where: { id: job.id } });
        if (check && check.status === "COMPLETED") {
          clearInterval(interval);
          resolve();
        }
      }, 50);
    });

    await worker2.stop();

    expect(worker2Processed).toBe(true);
    const finalDbJob = await prisma.job.findUnique({ where: { id: job.id } });
    expect(finalDbJob.status).toBe("COMPLETED");
  }, 20000);
});
