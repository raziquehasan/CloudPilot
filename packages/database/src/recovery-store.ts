import { Prisma, type PrismaClient } from "./generated/prisma/client.js";
import type { HealthScope } from "./health-ownership.js";

export const recoveryPolicy = Object.freeze({ windowMs: 15 * 60_000, limit: 3, cooldownMs: 15_000, leaseMs: 120_000 });
export type RecoveryScope = Pick<HealthScope, "workspaceId" | "projectId" | "deploymentId">;
export type RecoveryRuntimeScope = RecoveryScope & { runtimeInstanceId: string };
type Transaction = Prisma.TransactionClient;

export function recoveryRuntimeWhere(scope: RecoveryRuntimeScope) {
  const { workspaceId, projectId, deploymentId, runtimeInstanceId } = scope;
  return { workspaceId, projectId, deploymentId, id: runtimeInstanceId };
}

export async function lockRecoveryRuntime(db: Transaction, scope: RecoveryRuntimeScope) {
  const rows = await db.$queryRaw<Array<{ id: string }>>`
    SELECT "id" FROM "deployment_runtime_instances" WHERE "workspaceId" = ${scope.workspaceId}::uuid
      AND "projectId" = ${scope.projectId}::uuid AND "deploymentId" = ${scope.deploymentId}::uuid
      AND "id" = ${scope.runtimeInstanceId}::uuid FOR UPDATE
  `;
  return rows.length === 1;
}

export async function recoveryNow(db: Transaction): Promise<Date> {
  const [row] = await db.$queryRaw<Array<{ now: Date }>>`SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now`;
  if (!row) throw new Error("Database clock unavailable");
  return row.now;
}

export function currentRecoveryRuntime(db: Transaction, scope: RecoveryScope) {
  const { workspaceId, projectId, deploymentId } = scope;
  return db.deploymentRuntimeInstance.findFirst({
    where: { workspaceId, projectId, deploymentId }, orderBy: { runtimeAttempt: "desc" },
    include: { deployment: { select: { status: true, project: { select: { repository: { select: { fullName: true } } } } } } },
  });
}

type Runtime = NonNullable<Awaited<ReturnType<typeof currentRecoveryRuntime>>>;

export function isRecoveryRuntimeEligible(runtime: Runtime | null): runtime is Runtime & {
  containerId: string; imageId: string; hostPort: number; hostBindingAddress: string; readyAt: Date;
} {
  return !!runtime && runtime.status === "RUNNING" && runtime.deployment.status === "RUNNING" &&
    runtime.readyAt !== null && runtime.containerId !== null && /^[a-f0-9]{64}$/.test(runtime.containerId) &&
    runtime.imageId !== null && /^sha256:[a-f0-9]{64}$/.test(runtime.imageId) &&
    runtime.hostPort !== null && runtime.hostPort >= 1 && runtime.hostPort <= 65535 &&
    (runtime.hostBindingAddress === "127.0.0.1" || runtime.hostBindingAddress === "::1") &&
    !!runtime.deployment.project.repository?.fullName;
}

export const publicRecoverySelect = {
  id: true, generation: true, runtimeInstanceId: true, status: true, action: true, trigger: true,
  attemptCount: true, failureReason: true, startedAt: true, finishedAt: true, createdAt: true, updatedAt: true,
} as const satisfies Prisma.DeploymentRecoveryAttemptSelect;

const dispatchSelect = { ...publicRecoverySelect, healthState: { select: { runtime: { select: { buildId: true } } } } } as const;
type Reservation = Prisma.DeploymentRecoveryAttemptGetPayload<{ select: typeof dispatchSelect }>;
type ReserveResult =
  | { kind: "reserved" | "replay"; attempt: Reservation }
  | { kind: "not_found" | "ineligible" | "busy" | "exhausted" | "cooldown" };

const active = ["REQUESTED", "RUNNING"] as const;

export async function recoverySchemaReady(db: Pick<PrismaClient, "$queryRaw">): Promise<boolean> {
  const [table] = await db.$queryRaw<Array<{ ready: boolean }>>`
    SELECT to_regclass('public._prisma_migrations') IS NOT NULL
      AND to_regclass('public.deployment_recovery_attempts') IS NOT NULL AS ready
  `;
  if (!table?.ready) return false;
  const [migration] = await db.$queryRaw<Array<{ ready: boolean }>>`
    SELECT EXISTS (SELECT 1 FROM "_prisma_migrations" WHERE migration_name = '20261127150000_recovery_restart_intent'
      AND finished_at IS NOT NULL AND rolled_back_at IS NULL) AS ready
  `;
  return migration?.ready === true;
}

export async function reserveManualRecovery(
  db: PrismaClient, scope: RecoveryScope, actorId: string, idempotencyKey: string,
): Promise<ReserveResult> {
  const replay = (tx: Transaction) => tx.deploymentRecoveryAttempt.findFirst({
    where: { ...scope, idempotencyKey }, select: dispatchSelect,
  });
  try {
    return await db.$transaction(async (tx): Promise<ReserveResult> => {
      const existing = await replay(tx);
      if (existing) return { kind: "replay", attempt: existing };
      const candidate = await currentRecoveryRuntime(tx, scope);
      if (!candidate) {
        const deployment = await tx.deployment.findFirst({
          where: { workspaceId: scope.workspaceId, projectId: scope.projectId, id: scope.deploymentId }, select: { id: true },
        });
        return { kind: deployment ? "ineligible" : "not_found" };
      }
      const runtimeScope = { ...scope, runtimeInstanceId: candidate.id };
      if (!await lockRecoveryRuntime(tx, runtimeScope)) return { kind: "ineligible" };
      const repeated = await replay(tx);
      if (repeated) return { kind: "replay", attempt: repeated };
      const runtime = await currentRecoveryRuntime(tx, scope);
      if (!isRecoveryRuntimeEligible(runtime) || runtime.id !== candidate.id) return { kind: "ineligible" };

      const now = await recoveryNow(tx);
      if (runtime.executionLeaseExpiresAt && runtime.executionLeaseExpiresAt > now) return { kind: "busy" };
      if (await tx.deploymentRecoveryAttempt.findFirst({
        where: { ...scope, status: { in: [...active] } }, select: { id: true },
      })) return { kind: "busy" };
      const health = await tx.deploymentHealthState.findFirst({
        where: runtimeScope, orderBy: { generation: "desc" },
      });
      if (!health || health.status !== "UNHEALTHY" || !health.nextCheckAt) return { kind: "ineligible" };
      const healthKey = { ...runtimeScope, generation: health.generation, version: health.version };
      const recent = await tx.deploymentRecoveryAttempt.findMany({
        where: { ...scope, createdAt: { gt: new Date(now.getTime() - recoveryPolicy.windowMs) } },
        orderBy: { createdAt: "desc" }, take: recoveryPolicy.limit,
        select: { finishedAt: true, status: true },
      });
      const unleased = { nextCheckAt: null, leaseOwnerId: null, leaseToken: null, leaseExpiresAt: null, version: { increment: 1 } };
      if (recent.length >= recoveryPolicy.limit) {
        await tx.deploymentHealthState.updateMany({
          where: healthKey, data: { ...unleased, status: "FAILED", lastFailureReason: "RECOVERY_EXHAUSTED" },
        });
        return { kind: "exhausted" };
      }
      const last = recent[0];
      if (last?.status === "FAILED" && last.finishedAt &&
        now.getTime() < last.finishedAt.getTime() + recoveryPolicy.cooldownMs * 2 ** (recent.length - 1)) {
        return { kind: "cooldown" };
      }
      const reserved = await tx.deploymentHealthState.updateMany({
        where: healthKey, data: { ...unleased, status: "RECOVERING" },
      });
      if (reserved.count !== 1) return { kind: "ineligible" };
      const attempt = await tx.deploymentRecoveryAttempt.create({
        data: {
          ...runtimeScope, generation: health.generation, healthVersion: health.version + 1,
          action: "RESTART", trigger: "MANUAL", idempotencyKey, createdAt: now,
        },
        select: dispatchSelect,
      });
      await tx.auditLog.create({ data: {
        workspaceId: scope.workspaceId, actorId, entityType: "Deployment", entityId: scope.deploymentId,
        action: "DEPLOYMENT_RECOVERY_REQUESTED", metadata: { recoveryId: attempt.id, runtimeInstanceId: runtime.id },
      } });
      return { kind: "reserved", attempt };
    });
  } catch (error) {
    if (!(error instanceof Prisma.PrismaClientKnownRequestError) || error.code !== "P2002") throw error;
    const attempt = await replay(db);
    return attempt ? { kind: "replay", attempt } : { kind: "busy" };
  }
}
