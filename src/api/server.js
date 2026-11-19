import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import jobsRouter from "./routes/jobs.routes.js";
import healthRouter from "./routes/health.routes.js";
import queuesRouter from "./routes/queues.routes.js";
import workersRouter from "./routes/workers.routes.js";
import { errorHandler } from "./middlewares/errorHandler.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PUBLIC_DIR = path.join(__dirname, "../../public");

/**
 * Creates and configures the Express application.
 * Accepts optional dependency overrides (useful for testing and modular composition).
 * 
 * @param {Object} [options]
 * @param {import("ioredis").Redis} [options.redis]
 * @param {import("@prisma/client").PrismaClient} [options.prisma]
 * @param {import("../queue/producer.js").Producer} [options.producer]
 * @returns {express.Application}
 */
export function createServer(options = {}) {
  const app = express();

  if (options.redis) app.locals.redis = options.redis;
  if (options.prisma) app.locals.prisma = options.prisma;
  if (options.producer) app.locals.producer = options.producer;

  app.use(express.json());

  // Static files for monitoring dashboard
  app.use(express.static(PUBLIC_DIR));

  // Mount API routes
  app.use("/api/health", healthRouter);
  app.use("/api/jobs", jobsRouter);
  app.use("/api/queues", queuesRouter);
  app.use("/api/workers", workersRouter);

  // Serve Dashboard on /dashboard and root
  app.get(["/", "/dashboard"], (req, res) => {
    res.sendFile(path.join(PUBLIC_DIR, "index.html"));
  });

  // Fallback 404 handler
  app.use((req, res) => {
    res.status(404).json({
      error: "NotFound",
      message: `Cannot ${req.method} ${req.originalUrl}`,
    });
  });

  // Centralized error handler
  app.use(errorHandler);

  return app;
}
