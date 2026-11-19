import { appConfig } from "./app.config.js";

/**
 * Production Redis configuration.
 * Configured with exponential backoff retry strategy, health check ping timeouts,
 * and lazy connect for resilient bootstrapping.
 */
export const redisConfig = {
  url: appConfig.redisUrl,
  options: {
    lazyConnect: true,
    maxRetriesPerRequest: 3,
    connectTimeout: 10000,
    enableAutoPipelining: true,
    retryStrategy: (times) => {
      // Exponential backoff with a cap at 3000ms
      const delay = Math.min(times * 150, 3000);
      return delay;
    },
    reconnectOnError: (err) => {
      const targetErrors = ["READONLY", "ETIMEDOUT", "ECONNRESET"];
      return targetErrors.some((target) => err.message.includes(target));
    },
  },
};
