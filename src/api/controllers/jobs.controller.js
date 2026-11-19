import { Producer } from "../../queue/producer.js";
import { Job } from "../../queue/job.js";
import { getRedisClient } from "../../redis/client.js";
import { runScript } from "../../redis/scriptLoader.js";
import { getPrismaClient } from "../../config/db.config.js";

function getDependencies(req) {
  const redis = req.app?.locals?.redis || getRedisClient();
  const prisma = req.app?.locals?.prisma || getPrismaClient();
  const producer = req.app?.locals?.producer || new Producer({ redis, prisma });
  return { redis, prisma, producer };
}

/**
 * Submits a new job to the queue.
 * POST /api/jobs
 */
export async function createJob(req, res, next) {
  try {
    const { producer } = getDependencies(req);
    const job = await producer.enqueue(req.body);
    res.status(201).json({
      success: true,
      data: job.toJSON(),
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Retrieves a job by its unique identifier.
 * Checks Redis for real-time status first, then falls back to PostgreSQL.
 * GET /api/jobs/:id
 */
export async function getJobById(req, res, next) {
  try {
    const { id } = req.params;
    const { redis, prisma } = getDependencies(req);

    const jobKey = `job:${id}`;
    const hash = await redis.hgetall(jobKey);

    if (hash && hash.id) {
      const job = Job.fromRedisHash(hash);
      return res.status(200).json({
        success: true,
        source: "redis",
        data: job.toJSON(),
      });
    }

    const dbRecord = await prisma.job.findUnique({
      where: { id },
      include: {
        executions: {
          orderBy: { startedAt: "desc" },
        },
      },
    });

    if (!dbRecord) {
      return res.status(404).json({
        success: false,
        error: "JobNotFound",
        message: `Job with ID '${id}' was not found`,
      });
    }

    const job = Job.fromDatabase(dbRecord);
    res.status(200).json({
      success: true,
      source: "database",
      data: {
        ...job.toJSON(),
        executions: dbRecord.executions,
      },
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Lists jobs with filtering and pagination.
 * GET /api/jobs
 */
export async function listJobs(req, res, next) {
  try {
    const {
      queue,
      status,
      page = "1",
      limit = "20",
    } = req.query;
    const { prisma } = getDependencies(req);

    const pageNum = Math.max(1, parseInt(page, 10));
    const take = Math.min(100, Math.max(1, parseInt(limit, 10)));
    const skip = (pageNum - 1) * take;

    const where = {};
    if (queue) where.queueName = queue;
    if (status) where.status = status;

    const [total, records] = await Promise.all([
      prisma.job.count({ where }),
      prisma.job.findMany({
        where,
        skip,
        take,
        orderBy: { createdAt: "desc" },
      }),
    ]);

    res.status(200).json({
      success: true,
      data: records.map((r) => Job.fromDatabase(r).toJSON()),
      pagination: {
        total,
        page: pageNum,
        limit: take,
        totalPages: Math.ceil(total / take),
      },
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Cancels a pending or delayed job.
 * DELETE /api/jobs/:id
 */
export async function cancelJob(req, res, next) {
  try {
    const { id } = req.params;
    const { redis, prisma } = getDependencies(req);

    const dbJob = await prisma.job.findUnique({ where: { id } });
    if (!dbJob) {
      return res.status(404).json({
        error: "JobNotFound",
        message: `Job '${id}' not found`,
      });
    }

    const queueName = dbJob.queueName;
    const waitingKey = `queue:${queueName}:waiting`;
    const priorityKey = `queue:${queueName}:waiting:priority`;
    const delayedKey = `queue:${queueName}:delayed`;
    const jobKey = `job:${id}`;
    const now = Date.now();

    const result = await runScript(
      redis,
      "cancel_job",
      [waitingKey, priorityKey, delayedKey, jobKey],
      [id, now]
    );

    const [status, detail] = result || [];
    if (status === "ERR_TERMINAL") {
      return res.status(400).json({
        error: "InvalidState",
        message: `Job cannot be cancelled because it is in terminal state '${detail}'`,
      });
    }

    await prisma.job.update({
      where: { id },
      data: { status: "CANCELLED" },
    });

    res.status(200).json({
      success: true,
      message: `Job '${id}' was successfully cancelled`,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Retries / replays a dead-lettered job by moving it back to the waiting queue.
 * POST /api/jobs/:id/retry
 */
export async function retryJob(req, res, next) {
  try {
    const { id } = req.params;
    const { redis, prisma } = getDependencies(req);

    const dbJob = await prisma.job.findUnique({ where: { id } });
    if (!dbJob) {
      return res.status(404).json({
        error: "JobNotFound",
        message: `Job '${id}' not found`,
      });
    }

    const queueName = dbJob.queueName;
    const dlqKey = `queue:${queueName}:dlq`;
    const waitingKey = `queue:${queueName}:waiting`;
    const jobKey = `job:${id}`;
    const now = Date.now();

    const result = await runScript(
      redis,
      "replay_dlq_job",
      [dlqKey, waitingKey, jobKey],
      [id, now]
    );

    const [status] = result || [];
    if (status !== "OK") {
      return res.status(400).json({
        error: "InvalidState",
        message: `Job cannot be retried: status is ${status}`,
      });
    }

    await prisma.job.update({
      where: { id },
      data: {
        status: "WAITING",
        attempts: 0,
        failureReason: null,
      },
    });

    res.status(200).json({
      success: true,
      message: `Job '${id}' successfully re-enqueued for retry`,
    });
  } catch (err) {
    next(err);
  }
}
