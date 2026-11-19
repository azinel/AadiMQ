import { Worker } from "./worker.js";
import { appConfig } from "../config/app.config.js";
import { connectRedis, disconnectRedis } from "../redis/client.js";
import { prisma } from "../config/db.config.js";
import { logger } from "../utils/logger.js";

async function main() {
  logger.info("Initializing AadiMQ Worker Process...");

  await connectRedis();
  await prisma.$connect();

  // Sample processor function that simulates work
  const worker = new Worker({
    queueName: appConfig.defaultQueue,
    concurrency: appConfig.workerConcurrency,
    processor: async (job) => {
      logger.info(`Processing job ${job.id} (type: ${job.jobType})`, {
        payload: job.payload,
      });

      // Simulate async task work
      await new Promise((resolve) => setTimeout(resolve, 500));

      return { processed: true, processedAt: new Date().toISOString() };
    },
  });

  await worker.start();

  const shutdown = async (signal) => {
    logger.info(`Received ${signal}. Draining worker...`);
    await worker.stop();
    await disconnectRedis();
    await prisma.$disconnect();
    logger.info("Worker process exited cleanly");
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  logger.error("Fatal error starting worker", { error: err.message });
  process.exit(1);
});
