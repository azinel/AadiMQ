import { createServer } from "./api/server.js";
import { appConfig } from "./config/app.config.js";
import { connectRedis, disconnectRedis } from "./redis/client.js";
import { prisma } from "./config/db.config.js";
import { logger } from "./utils/logger.js";

async function main() {
  logger.info("Initializing AadiMQ API Server...", { env: appConfig.env });

  // Connect to Redis
  try {
    await connectRedis();
    logger.info("Connected to Redis");
  } catch (err) {
    logger.error("Could not connect to Redis", { error: err.message });
    process.exit(1);
  }

  // Connect to Database
  try {
    await prisma.$connect();
    logger.info("Connected to PostgreSQL");
  } catch (err) {
    logger.error("Could not connect to PostgreSQL", { error: err.message });
    process.exit(1);
  }

  const app = createServer();
  const server = app.listen(appConfig.port, () => {
    logger.info(`AadiMQ API Server listening on port ${appConfig.port}`);
  });

  // Graceful shutdown handling
  const shutdown = async (signal) => {
    logger.info(`Received ${signal}. Shutting down gracefully...`);
    server.close(async () => {
      await disconnectRedis();
      await prisma.$disconnect();
      logger.info("Clean shutdown complete");
      process.exit(0);
    });
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  logger.error("Fatal error during startup", { error: err.message });
  process.exit(1);
});
