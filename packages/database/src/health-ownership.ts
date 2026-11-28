import { randomUUID } from "node:crypto";
import { Prisma, type PrismaClient } from "./generated/prisma/client.js";

export interface HealthScope {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  runtimeInstanceId: string;
  generation: number;
}

export interface HealthLease {
  ownerId: string;
  token: string;
  expiresAt: Date;
}

type HealthDatabase = Pick<PrismaClient, "$queryRaw">;
type LeaseIdentity = Pick<HealthLease, "ownerId" | "token">;

function checkLeaseDuration(durationMs: number): void {
  if (!Number.isInteger(durationMs) || durationMs < 1_000 || durationMs > 120_000) {
    throw new RangeError("Health lease duration must be between 1000 and 120000 milliseconds");
  }
}

function scoped(scope: HealthScope) {
  if (!Number.isInteger(scope.generation) || scope.generation < 0 || scope.generation > 2_147_483_647) {
    throw new RangeError("Invalid health generation");
  }
  return Prisma.sql`
    "workspaceId" = ${scope.workspaceId}::uuid
    AND "projectId" = ${scope.projectId}::uuid
    AND "deploymentId" = ${scope.deploymentId}::uuid
    AND "runtimeInstanceId" = ${scope.runtimeInstanceId}::uuid
    AND "generation" = ${scope.generation}::integer
  `;
}

// Sample the database clock only after locking the exact generation row.
function lockedClock(scope: HealthScope) {
  return Prisma.sql`
    WITH locked AS MATERIALIZED (
      SELECT 1 FROM "deployment_health_states" WHERE ${scoped(scope)} FOR UPDATE
    ), lease_clock AS MATERIALIZED (
      SELECT clock_timestamp() AT TIME ZONE 'UTC' AS db_now FROM locked
    )
  `;
}

function owned(lease: LeaseIdentity) {
  return Prisma.sql`
    "leaseOwnerId" = ${lease.ownerId}::uuid AND "leaseToken" = ${lease.token}::uuid
    AND "leaseExpiresAt" > lease_clock.db_now
  `;
}

export async function claimHealthCheck(
  db: HealthDatabase, scope: HealthScope, ownerId: string, durationMs: number,
): Promise<HealthLease | null> {
  checkLeaseDuration(durationMs);
  const token = randomUUID();
  const [lease] = await db.$queryRaw<HealthLease[]>`
    ${lockedClock(scope)}
    UPDATE "deployment_health_states"
    SET "leaseOwnerId" = ${ownerId}::uuid, "leaseToken" = ${token}::uuid,
        "leaseExpiresAt" = lease_clock.db_now + ${durationMs}::integer * INTERVAL '1 millisecond',
        "updatedAt" = lease_clock.db_now
    FROM lease_clock
    WHERE ${scoped(scope)} AND "nextCheckAt" <= lease_clock.db_now AND "status" <> 'FAILED'
      AND ("leaseExpiresAt" IS NULL OR "leaseExpiresAt" <= lease_clock.db_now)
    RETURNING "leaseOwnerId" AS "ownerId", "leaseToken" AS token, "leaseExpiresAt" AS "expiresAt"
  `;
  return lease ?? null;
}

export async function renewHealthCheck(
  db: HealthDatabase, scope: HealthScope, lease: LeaseIdentity, durationMs: number,
): Promise<HealthLease | null> {
  checkLeaseDuration(durationMs);
  const [renewed] = await db.$queryRaw<HealthLease[]>`
    ${lockedClock(scope)}
    UPDATE "deployment_health_states"
    SET "leaseExpiresAt" = lease_clock.db_now + ${durationMs}::integer * INTERVAL '1 millisecond',
        "updatedAt" = lease_clock.db_now
    FROM lease_clock
    WHERE ${scoped(scope)} AND ${owned(lease)}
    RETURNING "leaseOwnerId" AS "ownerId", "leaseToken" AS token, "leaseExpiresAt" AS "expiresAt"
  `;
  return renewed ?? null;
}

export async function releaseHealthCheck(
  db: HealthDatabase, scope: HealthScope, lease: LeaseIdentity,
): Promise<boolean> {
  const rows = await db.$queryRaw<Array<{ released: boolean }>>`
    ${lockedClock(scope)}
    UPDATE "deployment_health_states"
    SET "leaseOwnerId" = NULL, "leaseToken" = NULL, "leaseExpiresAt" = NULL,
        "updatedAt" = lease_clock.db_now
    FROM lease_clock
    WHERE ${scoped(scope)} AND ${owned(lease)}
    RETURNING true AS released
  `;
  return rows.length === 1;
}
