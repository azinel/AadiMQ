import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { logger } from "../utils/logger.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SCRIPTS_DIR = path.join(__dirname, "scripts");

/**
 * Cache of loaded Lua scripts:
 * scriptName -> { content: string, sha: string }
 */
const scriptCache = new Map();

/**
 * Computes SHA1 hash for a script content.
 * @param {string} content
 * @returns {string}
 */
function computeSha1(content) {
  return crypto.createHash("sha1").update(content).digest("hex");
}

/**
 * Loads all Lua scripts from the scripts directory into local cache.
 */
export function loadLocalScripts() {
  if (!fs.existsSync(SCRIPTS_DIR)) {
    throw new Error(`Lua scripts directory not found at: ${SCRIPTS_DIR}`);
  }

  const files = fs.readdirSync(SCRIPTS_DIR).filter((f) => f.endsWith(".lua"));
  for (const file of files) {
    const scriptName = path.basename(file, ".lua");
    const fullPath = path.join(SCRIPTS_DIR, file);
    const content = fs.readFileSync(fullPath, "utf-8");
    const sha = computeSha1(content);

    scriptCache.set(scriptName, {
      name: scriptName,
      content,
      sha,
    });
  }

  logger.debug(`Loaded ${scriptCache.size} Lua scripts into memory cache`, {
    scripts: Array.from(scriptCache.keys()),
  });
}

/**
 * Registers all loaded Lua scripts with Redis using SCRIPT LOAD.
 * This primes Redis so EVALSHA can be used with zero network transfer overhead.
 * 
 * @param {import("ioredis").Redis} redisClient
 */
export async function registerScriptsWithRedis(redisClient) {
  if (scriptCache.size === 0) {
    loadLocalScripts();
  }

  for (const [name, script] of scriptCache.entries()) {
    try {
      if (typeof redisClient.script === "function") {
        const loadedSha = await redisClient.script("LOAD", script.content);
        script.sha = loadedSha;
      }
    } catch (err) {
      logger.warn(`Could not preload script '${name}' via SCRIPT LOAD, will use EVAL fallback`, {
        error: err.message,
      });
    }
  }
}

/**
 * Executes a Lua script atomically using EVALSHA with automatic fallback.
 * 
 * @param {import("ioredis").Redis} redisClient
 * @param {string} scriptName
 * @param {string[]} keys
 * @param {(string|number)[]} args
 * @returns {Promise<any>}
 */
export async function runScript(redisClient, scriptName, keys = [], args = []) {
  if (scriptCache.size === 0) {
    loadLocalScripts();
  }

  const script = scriptCache.get(scriptName);
  if (!script) {
    throw new Error(`Lua script '${scriptName}' not found in registry`);
  }

  // Attempt EVALSHA first
  try {
    if (script.sha && typeof redisClient.evalsha === "function") {
      return await redisClient.evalsha(script.sha, keys.length, ...keys, ...args);
    }
  } catch (err) {
    // NOSCRIPT error happens when Redis restarted or script wasn't loaded yet
    if (err.message && err.message.includes("NOSCRIPT")) {
      logger.debug(`NOSCRIPT received for ${scriptName}, reloading...`);
      if (typeof redisClient.script === "function") {
        const newSha = await redisClient.script("LOAD", script.content);
        script.sha = newSha;
        return await redisClient.evalsha(newSha, keys.length, ...keys, ...args);
      }
    }
    // If not a NOSCRIPT error or SCRIPT LOAD failed, continue to EVAL fallback
  }

  // Fallback to EVAL with raw content
  return await redisClient.eval(script.content, keys.length, ...keys, ...args);
}
