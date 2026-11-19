import Redis from "ioredis";
import { redisConfig } from "../config/redis.config.js";
import { logger } from "../utils/logger.js";
import { registerScriptsWithRedis } from "./scriptLoader.js";

let redisClient = null;
let isConnected = false;

/**
 * Sets or overrides the singleton Redis client (useful for testing and dependency injection).
 * @param {Redis} client
 */
export function setRedisClient(client) {
  redisClient = client;
  isConnected = true;
}

/**
 * Creates and returns the singleton Redis client.
 * 
 * @param {string} [customUrl] Optional custom connection string (useful for testing)
 * @returns {Redis}
 */
export function getRedisClient(customUrl = null) {
  if (redisClient) {
    return redisClient;
  }

  const url = customUrl || redisConfig.url;
  redisClient = new Redis(url, redisConfig.options);

  redisClient.on("connect", () => {
    logger.info("Connecting to Redis...");
  });

  redisClient.on("ready", async () => {
    isConnected = true;
    logger.info("Redis connection established and ready");
    try {
      await registerScriptsWithRedis(redisClient);
    } catch (err) {
      logger.warn("Failed to register Lua scripts on ready", { error: err.message });
    }
  });

  redisClient.on("error", (err) => {
    isConnected = false;
    logger.error("Redis client encountered an error", { error: err.message });
  });

  redisClient.on("close", () => {
    isConnected = false;
    logger.warn("Redis connection closed");
  });

  redisClient.on("reconnecting", (time) => {
    logger.info(`Redis reconnecting in ${time}ms...`);
  });

  return redisClient;
}

/**
 * Ensures Redis is actively connected.
 * @returns {Promise<Redis>}
 */
export async function connectRedis() {
  const client = getRedisClient();
  if (client.status === "ready") {
    return client;
  }
  if (client.status === "wait") {
    await client.connect();
  }
  return client;
}

/**
 * Closes the Redis connection gracefully.
 */
export async function disconnectRedis() {
  if (redisClient) {
    try {
      await redisClient.quit();
    } catch (err) {
      redisClient.disconnect();
    }
    redisClient = null;
    isConnected = false;
    logger.info("Redis disconnected cleanly");
  }
}

/**
 * Checks if Redis is currently healthy.
 * @returns {Promise<boolean>}
 */
export async function isRedisHealthy() {
  try {
    if (!redisClient) return false;
    const response = await redisClient.ping();
    return response === "PONG";
  } catch (err) {
    return false;
  }
}
