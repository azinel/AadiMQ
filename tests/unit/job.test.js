import { Job } from "../../src/queue/job.js";
import { EnqueueJobSchema } from "../../src/queue/producer.js";

describe("Job Entity & Validation Unit Tests", () => {
  test("creates a Job instance with valid defaults", () => {
    const job = new Job({
      id: "test-uuid-1",
      queueName: "default",
      jobType: "email:send",
      payload: { to: "user@example.com" },
    });

    expect(job.id).toBe("test-uuid-1");
    expect(job.queueName).toBe("default");
    expect(job.jobType).toBe("email:send");
    expect(job.payload).toEqual({ to: "user@example.com" });
    expect(job.status).toBe("WAITING");
    expect(job.attempts).toBe(0);
    expect(job.maxAttempts).toBe(3);
    expect(job.priority).toBe(0);
    expect(job.isTerminal()).toBe(false);
  });

  test("serializes and deserializes from JSON accurately", () => {
    const now = Date.now();
    const job = new Job({
      id: "test-uuid-2",
      queueName: "notifications",
      jobType: "sms:send",
      payload: { phone: "+1234567890", message: "Hello" },
      status: "COMPLETED",
      attempts: 1,
      createdAt: now,
      completedAt: now + 500,
    });

    const json = job.toJSON();
    expect(json.id).toBe("test-uuid-2");
    expect(json.status).toBe("COMPLETED");
    expect(json.payload.phone).toBe("+1234567890");
    expect(json.completedAt).toBe(new Date(now + 500).toISOString());
    expect(job.isTerminal()).toBe(true);
  });

  test("reconstructs Job correctly from Redis hash structure", () => {
    const redisHash = {
      id: "test-uuid-3",
      queue: "payments",
      type: "charge:card",
      payload: JSON.stringify({ amount: 5000, currency: "USD" }),
      status: "ACTIVE",
      attempts: "2",
      maxAttempts: "5",
      priority: "10",
      createdAt: "1700000000000",
      startedAt: "1700000001000",
      lock_token: "fence-token-xyz",
    };

    const job = Job.fromRedisHash(redisHash);
    expect(job.id).toBe("test-uuid-3");
    expect(job.queueName).toBe("payments");
    expect(job.jobType).toBe("charge:card");
    expect(job.payload.amount).toBe(5000);
    expect(job.status).toBe("ACTIVE");
    expect(job.attempts).toBe(2);
    expect(job.maxAttempts).toBe(5);
    expect(job.priority).toBe(10);
    expect(job.lockToken).toBe("fence-token-xyz");
    expect(job.isTerminal()).toBe(false);
  });

  test("reconstructs Job correctly from Database record", () => {
    const dbRecord = {
      id: "test-uuid-4",
      queueName: "reports",
      jobType: "report:generate",
      payload: { reportId: "rep-99" },
      status: "COMPLETED",
      priority: 1,
      attempts: 1,
      maxAttempts: 3,
      idempotencyKey: "unique-rep-99",
      createdAt: new Date("2026-01-01T00:00:00.000Z"),
      startedAt: new Date("2026-01-01T00:00:01.000Z"),
      completedAt: new Date("2026-01-01T00:00:05.000Z"),
      failedAt: null,
      failureReason: null,
    };

    const job = Job.fromDatabase(dbRecord);
    expect(job.id).toBe("test-uuid-4");
    expect(job.idempotencyKey).toBe("unique-rep-99");
    expect(job.status).toBe("COMPLETED");
    expect(job.isTerminal()).toBe(true);
  });

  test("EnqueueJobSchema validates inputs correctly", () => {
    // Valid input
    const valid = EnqueueJobSchema.parse({
      jobType: "email:send",
      payload: { email: "test@example.com" },
    });
    expect(valid.queueName).toBe("default");
    expect(valid.maxAttempts).toBe(3);

    // Invalid input: missing jobType
    expect(() => EnqueueJobSchema.parse({})).toThrow();

    // Invalid input: maxAttempts exceeds allowed range
    expect(() =>
      EnqueueJobSchema.parse({
        jobType: "email:send",
        maxAttempts: 100,
      })
    ).toThrow();
  });
});
