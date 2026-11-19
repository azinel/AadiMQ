import { PrismaClient } from "@prisma/client";

let prismaInstance = null;

/**
 * Returns a singleton instance of PrismaClient.
 * In development, ensures hot reloads do not exhaust database connections.
 * 
 * @returns {PrismaClient}
 */
export function getPrismaClient() {
  if (!prismaInstance) {
    prismaInstance = new PrismaClient({
      log: process.env.NODE_ENV === "development" ? ["warn", "error"] : ["error"],
    });
  }
  return prismaInstance;
}

export const prisma = getPrismaClient();
