import dotenv from "dotenv";

dotenv.config();

export const appConfig = {
  env: process.env.NODE_ENV || "development",
  port: parseInt(process.env.PORT || "3000", 10),
  defaultQueue: process.env.DEFAULT_QUEUE || "default",
  workerConcurrency: parseInt(process.env.WORKER_CONCURRENCY || "1", 10),
  jobLeaseMs: parseInt(process.env.JOB_LEASE_MS || "30000", 10), // 30s default lease
  redisUrl: process.env.REDIS_URL || "redis://127.0.0.1:6379",
  databaseUrl: process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/aadimq?schema=public",
};
