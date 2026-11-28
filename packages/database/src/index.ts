import { PrismaPg } from "@prisma/adapter-pg";
import pg from "pg";
import { config } from "dotenv";
import { fileURLToPath } from "node:url";
import { PrismaClient } from "./generated/prisma/client.js";

export { Prisma } from "./generated/prisma/client.js";
export type {
  DeploymentHealthStatus,
  HealthFailureReason,
  HealthProbeStatus,
  RecoveryAction,
  RecoveryStatus,
  RecoveryTrigger,
} from "./generated/prisma/enums.js";
export * from "./health-ownership.js";
export * from "./recovery-store.js";
export * from "./auto-recovery-circuit.js";

config({ path: fileURLToPath(new URL("../../../.env", import.meta.url)) });

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required");
}

const pool = new pg.Pool({
  connectionString: databaseUrl,
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

const adapter = new PrismaPg(pool);

export const prisma = new PrismaClient({ adapter });

export interface DatabaseHealth {
  healthy: boolean;
  latencyMs: number;
}

export async function checkDatabaseHealth(timeoutMs = 1500): Promise<DatabaseHealth> {
  const startedAt = performance.now();

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error("Database health check timed out"));
    }, timeoutMs);
  });

  try {
    await Promise.race([
      prisma.$queryRaw`SELECT 1`,
      timeout,
    ]);
    return {
      healthy: true,
      latencyMs: Math.round(performance.now() - startedAt),
    };
  } catch {
    return {
      healthy: false,
      latencyMs: Math.round(performance.now() - startedAt),
    };
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

export async function disconnectDatabase(): Promise<void> {
  await prisma.$disconnect();
  await pool.end();
}
