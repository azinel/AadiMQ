import { logger } from "../../utils/logger.js";

/**
 * Centralized Express error handler.
 */
export function errorHandler(err, req, res, next) {
  logger.error("Unhandled error in API request", {
    method: req.method,
    path: req.originalUrl,
    error: err.message,
    stack: process.env.NODE_ENV === "development" ? err.stack : undefined,
  });

  const statusCode = err.status || err.statusCode || 500;
  res.status(statusCode).json({
    error: err.name || "InternalServerError",
    message: err.message || "An unexpected error occurred",
  });
}
