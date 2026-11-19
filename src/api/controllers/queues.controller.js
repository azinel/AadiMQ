import { QueueStatsService } from "../../queue/stats.js";
import { getRedisClient } from "../../redis/client.js";
import { getPrismaClient } from "../../config/db.config.js";

function getStatsService(req) {
  const redis = req.app?.locals?.redis || getRedisClient();
  const prisma = req.app?.locals?.prisma || getPrismaClient();
  return new QueueStatsService({ redis, prisma });
}

/**
 * Lists all queues with summary counts.
 * GET /api/queues
 */
export async function listQueues(req, res, next) {
  try {
    const service = getStatsService(req);
    const queues = await service.getAllQueuesStats();
    res.status(200).json({
      success: true,
      data: queues,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Retrieves detailed stats for a specific queue.
 * GET /api/queues/:name/stats
 */
export async function getQueueStats(req, res, next) {
  try {
    const { name } = req.params;
    const service = getStatsService(req);
    const stats = await service.getQueueStats(name);
    res.status(200).json({
      success: true,
      data: stats,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Lists all registered workers with health status.
 * GET /api/workers
 */
export async function listWorkers(req, res, next) {
  try {
    const service = getStatsService(req);
    const workers = await service.getWorkers();
    res.status(200).json({
      success: true,
      data: workers,
    });
  } catch (err) {
    next(err);
  }
}
