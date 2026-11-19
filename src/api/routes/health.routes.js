import { Router } from "express";
import { isRedisHealthy, getRedisClient } from "../../redis/client.js";
import { getPrismaClient } from "../../config/db.config.js";

const router = Router();

router.get("/", async (req, res) => {
  const redis = req.app?.locals?.redis || getRedisClient();
  const prisma = req.app?.locals?.prisma || getPrismaClient();

  let dbStatus = "unhealthy";
  let redisStatus = "unhealthy";

  try {
    const pong = await redis.ping();
    if (pong === "PONG") redisStatus = "healthy";
  } catch (err) {
    redisStatus = "unhealthy";
  }

  try {
    await prisma.$queryRaw`SELECT 1`;
    dbStatus = "healthy";
  } catch (err) {
    dbStatus = "unhealthy";
  }

  const isHealthy = dbStatus === "healthy" && redisStatus === "healthy";
  res.status(isHealthy ? 200 : 503).json({
    status: isHealthy ? "ok" : "degraded",
    services: {
      redis: redisStatus,
      database: dbStatus,
    },
    timestamp: new Date().toISOString(),
  });
});

export default router;
