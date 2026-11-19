/**
 * @typedef {"WAITING" | "DELAYED" | "ACTIVE" | "COMPLETED" | "FAILED" | "DEAD_LETTER" | "CANCELLED"} JobStatus
 */

/**
 * Domain entity representing a distributed Job in AadiMQ.
 * Unifies the in-memory, Redis hash, and PostgreSQL database representations.
 */
export class Job {
  /**
   * @param {Object} params
   * @param {string} params.id
   * @param {string} params.queueName
   * @param {string} params.jobType
   * @param {any} params.payload
   * @param {JobStatus} [params.status="WAITING"]
   * @param {number} [params.priority=0]
   * @param {number} [params.attempts=0]
   * @param {number} [params.maxAttempts=3]
   * @param {string|null} [params.idempotencyKey=null]
   * @param {Date|number|null} [params.createdAt=null]
   * @param {Date|number|null} [params.startedAt=null]
   * @param {Date|number|null} [params.completedAt=null]
   * @param {Date|number|null} [params.failedAt=null]
   * @param {string|null} [params.failureReason=null]
   * @param {string|null} [params.lockToken=null]
   */
  constructor({
    id,
    queueName,
    jobType,
    payload,
    status = "WAITING",
    priority = 0,
    attempts = 0,
    maxAttempts = 3,
    idempotencyKey = null,
    createdAt = Date.now(),
    startedAt = null,
    completedAt = null,
    failedAt = null,
    failureReason = null,
    lockToken = null,
  }) {
    this.id = id;
    this.queueName = queueName;
    this.jobType = jobType;
    this.payload = typeof payload === "string" ? JSON.parse(payload) : payload;
    this.status = status;
    this.priority = Number(priority) || 0;
    this.attempts = Number(attempts) || 0;
    this.maxAttempts = Number(maxAttempts) || 3;
    this.idempotencyKey = idempotencyKey;
    this.createdAt = createdAt instanceof Date ? createdAt.getTime() : Number(createdAt) || Date.now();
    this.startedAt = startedAt ? (startedAt instanceof Date ? startedAt.getTime() : Number(startedAt)) : null;
    this.completedAt = completedAt ? (completedAt instanceof Date ? completedAt.getTime() : Number(completedAt)) : null;
    this.failedAt = failedAt ? (failedAt instanceof Date ? failedAt.getTime() : Number(failedAt)) : null;
    this.failureReason = failureReason;
    this.lockToken = lockToken;
  }

  /**
   * Serializes the Job to a standard JSON-friendly format.
   * @returns {Object}
   */
  toJSON() {
    return {
      id: this.id,
      queueName: this.queueName,
      jobType: this.jobType,
      payload: this.payload,
      status: this.status,
      priority: this.priority,
      attempts: this.attempts,
      maxAttempts: this.maxAttempts,
      idempotencyKey: this.idempotencyKey,
      createdAt: new Date(this.createdAt).toISOString(),
      startedAt: this.startedAt ? new Date(this.startedAt).toISOString() : null,
      completedAt: this.completedAt ? new Date(this.completedAt).toISOString() : null,
      failedAt: this.failedAt ? new Date(this.failedAt).toISOString() : null,
      failureReason: this.failureReason,
    };
  }

  /**
   * Reconstructs a Job entity from a Prisma database record.
   * @param {Object} record
   * @returns {Job}
   */
  static fromDatabase(record) {
    if (!record) return null;
    return new Job({
      id: record.id,
      queueName: record.queueName,
      jobType: record.jobType,
      payload: record.payload,
      status: record.status,
      priority: record.priority,
      attempts: record.attempts,
      maxAttempts: record.maxAttempts,
      idempotencyKey: record.idempotencyKey,
      createdAt: record.createdAt,
      startedAt: record.startedAt,
      completedAt: record.completedAt,
      failedAt: record.failedAt,
      failureReason: record.failureReason,
    });
  }

  /**
   * Reconstructs a Job entity from a Redis HGETALL dictionary.
   * @param {Record<string, string>} hash
   * @returns {Job|null}
   */
  static fromRedisHash(hash) {
    if (!hash || !hash.id) return null;
    return new Job({
      id: hash.id,
      queueName: hash.queue,
      jobType: hash.type,
      payload: hash.payload ? JSON.parse(hash.payload) : {},
      status: hash.status,
      priority: parseInt(hash.priority || "0", 10),
      attempts: parseInt(hash.attempts || "0", 10),
      maxAttempts: parseInt(hash.maxAttempts || "3", 10),
      idempotencyKey: hash.idempotencyKey || null,
      createdAt: parseInt(hash.createdAt || "0", 10),
      startedAt: hash.startedAt ? parseInt(hash.startedAt, 10) : null,
      completedAt: hash.completedAt ? parseInt(hash.completedAt, 10) : null,
      failedAt: hash.failedAt ? parseInt(hash.failedAt, 10) : null,
      failureReason: hash.failureReason || null,
      lockToken: hash.lock_token || null,
    });
  }

  /**
   * Checks if the job is in a terminal state.
   * @returns {boolean}
   */
  isTerminal() {
    return ["COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED"].includes(this.status);
  }
}
