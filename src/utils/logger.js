/**
 * Lightweight structured JSON logger.
 * In production environments, structured logs can be ingested by Datadog, ELK, or CloudWatch.
 */

const LOG_LEVELS = {
  DEBUG: 10,
  INFO: 20,
  WARN: 30,
  ERROR: 40,
};

const currentLevel = process.env.NODE_ENV === "test" ? LOG_LEVELS.ERROR : LOG_LEVELS.INFO;

function formatLog(level, message, meta = {}) {
  return JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    message,
    ...meta,
  });
}

export const logger = {
  debug(message, meta) {
    if (currentLevel <= LOG_LEVELS.DEBUG) {
      console.log(formatLog("DEBUG", message, meta));
    }
  },
  info(message, meta) {
    if (currentLevel <= LOG_LEVELS.INFO) {
      console.log(formatLog("INFO", message, meta));
    }
  },
  warn(message, meta) {
    if (currentLevel <= LOG_LEVELS.WARN) {
      console.warn(formatLog("WARN", message, meta));
    }
  },
  error(message, meta) {
    if (currentLevel <= LOG_LEVELS.ERROR) {
      console.error(formatLog("ERROR", message, meta));
    }
  },
};
