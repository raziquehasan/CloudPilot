import { createHash } from "node:crypto";
import { Prisma, type PrismaClient } from "./generated/prisma/client.js";
import type { HealthScope } from "./health-ownership.js";
import { evaluateCircuit, type CircuitState } from "./auto-recovery-circuit.js";

export const recoveryPolicy = Object.freeze({ windowMs: 15 * 60_000, limit: 3, cooldownMs: 15_000, leaseMs: 120_000 });
export type RecoveryScope = Pick<HealthScope, "workspaceId" | "projectId" | "deploymentId">;
export type RecoveryRuntimeScope = RecoveryScope & { runtimeInstanceId: string };
type Transaction = Prisma.TransactionClient;

/**
 * Deterministic, idempotent recovery key for automatic recovery.
 *
 * Derived from the runtime identity, health generation and health
 * version (the eligibility epoch). The same unhealthy generation
 * always maps to the same key, so duplicate scheduler evaluations
 * and concurrent workers cannot create duplicate reservations. A
 * successful or failed recovery advances the health version (and a
 * success advances the generation), so a legitimate future recovery
 * is never permanently blocked.
 */
export function automaticRecoveryKey(runtimeInstanceId: string, generation: number, healthVersion: number): string {
  const digest = createHash("sha256")
    .update(`cloudpilot:auto-recovery:${runtimeInstanceId}:${generation}:${healthVersion}`)
    .digest("hex");
  // Format the first 128 bits as a canonical UUID (version 4, RFC 4122).
  const hex = digest.slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

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
const circuitFailureThreshold = 3;

async function readCircuitHistory(tx: Transaction, scope: RecoveryScope) {
  const [history] = await tx.$queryRaw<Array<{
    consecutiveFailures: number;
    restartUncertain: boolean;
    failedHalfOpenTrial: boolean;
    legacyTrialUnclassified: boolean;
  }>>`
    WITH verified AS (
      SELECT max("finishedAt") AS recovered_at
      FROM "deployment_recovery_attempts"
      WHERE "workspaceId" = ${scope.workspaceId}::uuid AND "projectId" = ${scope.projectId}::uuid
        AND "deploymentId" = ${scope.deploymentId}::uuid AND status = 'SUCCEEDED'
    ), failures AS MATERIALIZED (
      SELECT attempt.*
      FROM "deployment_recovery_attempts" attempt CROSS JOIN verified
      WHERE "workspaceId" = ${scope.workspaceId}::uuid AND "projectId" = ${scope.projectId}::uuid
        AND "deploymentId" = ${scope.deploymentId}::uuid AND status = 'FAILED'
        AND (verified.recovered_at IS NULL OR "finishedAt" >= verified.recovered_at)
    ), classified AS (
      SELECT f.*, (
        SELECT audit.metadata->>'automaticCircuitState'
        FROM "audit_logs" audit
        WHERE audit."workspaceId" = f."workspaceId" AND audit."entityId" = f."deploymentId"
          AND audit."entityType" = 'Deployment' AND audit.action = 'DEPLOYMENT_RECOVERY_REQUESTED'
          AND audit.metadata->>'recoveryId' = f.id::text
          AND audit.metadata->>'automaticCircuitState' IN ('CLOSED', 'HALF_OPEN')
        LIMIT 1
      ) AS circuit_state
      FROM failures f WHERE f.trigger = 'AUTOMATIC'
    )
    SELECT
      (SELECT count(*)::integer FROM failures) AS "consecutiveFailures",
      EXISTS (
        SELECT 1 FROM failures WHERE "failureReason" = 'RECOVERY_RESTART_UNCERTAIN'
          OR ("restartIssuedAt" IS NOT NULL AND "failureReason" IN
            ('RECOVERY_TIMEOUT', 'RECOVERY_STALE', 'RECOVERY_DOCKER_UNAVAILABLE'))
      ) AS "restartUncertain",
      EXISTS (SELECT 1 FROM classified WHERE circuit_state = 'HALF_OPEN') AS "failedHalfOpenTrial",
      EXISTS (
        SELECT 1 FROM classified legacy
        WHERE legacy.circuit_state IS NULL AND (
          SELECT count(*) FROM failures earlier
          WHERE earlier.id <> legacy.id AND earlier."finishedAt" <= legacy."createdAt"
        ) >= ${circuitFailureThreshold}::integer
      ) AS "legacyTrialUnclassified"
  `;
  if (!history) throw new Error("Recovery circuit history unavailable");
  return history;
}

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
  return reserveRecovery(db, scope, { trigger: "MANUAL", actorId, idempotencyKey });
}

/**
 * Atomically reserve an AUTOMATIC recovery for a discovered candidate.
 *
 * Revalidates every safety condition inside the reservation
 * transaction (PostgreSQL time, runtime + health locks, ownership,
 * generation, fresh unhealthy observation, shared budget, cooldown,
 * active/conflicting operations). The idempotency key is derived
 * deterministically from the eligibility epoch so duplicate
 * evaluation or concurrent workers cannot double-schedule, while a
 * later generation/version still permits a legitimate retry.
 *
 * This only persists a REQUESTED attempt; it never enqueues a job or
 * touches Docker. Publication is handled separately and must be
 * reconciled from this durable record.
 */
export async function reserveAutomaticRecovery(
  db: PrismaClient, scope: RecoveryScope, runtimeInstanceId: string,
): Promise<ReserveResult> {
  return reserveRecovery(db, scope, { trigger: "AUTOMATIC", actorId: null, idempotencyKey: "", expectedRuntimeId: runtimeInstanceId });
}

type ReserveOptions = {
  trigger: "MANUAL" | "AUTOMATIC";
  actorId: string | null;
  idempotencyKey: string;
  expectedRuntimeId?: string;
};

async function reserveRecovery(
  db: PrismaClient, scope: RecoveryScope, options: ReserveOptions,
): Promise<ReserveResult> {
  const { trigger } = options;
  // Manual uses a client-provided key for replay; automatic derives the
  // key from the eligibility epoch inside the transaction.
  const replayByKey = (tx: Transaction, key: string) => tx.deploymentRecoveryAttempt.findFirst({
    where: { ...scope, idempotencyKey: key }, select: dispatchSelect,
  });
  let derivedKey: string | undefined;
  try {
    return await db.$transaction(async (tx): Promise<ReserveResult> => {
      if (trigger === "MANUAL") {
        const existing = await replayByKey(tx, options.idempotencyKey);
        if (existing) return { kind: "replay", attempt: existing };
      }
      const candidate = await currentRecoveryRuntime(tx, scope);
      if (!candidate) {
        const deployment = await tx.deployment.findFirst({
          where: { workspaceId: scope.workspaceId, projectId: scope.projectId, id: scope.deploymentId }, select: { id: true },
        });
        return { kind: deployment ? "ineligible" : "not_found" };
      }
      if (options.expectedRuntimeId && candidate.id !== options.expectedRuntimeId) return { kind: "ineligible" };
      const runtimeScope = { ...scope, runtimeInstanceId: candidate.id };
      if (!await lockRecoveryRuntime(tx, runtimeScope)) return { kind: "ineligible" };
      if (trigger === "MANUAL") {
        const repeated = await replayByKey(tx, options.idempotencyKey);
        if (repeated) return { kind: "replay", attempt: repeated };
      }
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
      // Automatic recovery requires a fresh unhealthy observation so a
      // stale, lagging health state can never authorize a restart.
      if (trigger === "AUTOMATIC" &&
        (!health.lastCheckedAt || now.getTime() - health.lastCheckedAt.getTime() > 30_000)) {
        return { kind: "ineligible" };
      }
      const idempotencyKey = trigger === "AUTOMATIC"
        ? automaticRecoveryKey(runtime.id, health.generation, health.version)
        : options.idempotencyKey;
      if (trigger === "AUTOMATIC") {
        derivedKey = idempotencyKey;
        const derived = await replayByKey(tx, idempotencyKey);
        if (derived) return { kind: "replay", attempt: derived };
      }
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
      let automaticCircuitState: CircuitState | undefined;
      if (trigger === "AUTOMATIC") {
        const history = await readCircuitHistory(tx, scope);
        const circuit = evaluateCircuit({
          ...history,
          failureThreshold: circuitFailureThreshold,
          failedAttemptsInWindow: recent.filter((attempt) => attempt.status === "FAILED").length,
          attemptsInWindow: recent.length,
          recoveryLimit: recoveryPolicy.limit,
          cooldownRemainingMs: 0,
          now: now.getTime(),
          // The history starts at the last verified success. The deployment-wide
          // active-attempt guard above already excludes an in-progress trial.
          verifiedRecoverySinceOpen: false,
          halfOpenInProgress: false,
        });
        // Older attempts have no admission marker. When they could have
        // consumed a trial, require a verified recovery instead of guessing.
        if (history.legacyTrialUnclassified || circuit.reason !== null || circuit.state === "OPEN") {
          return { kind: "ineligible" };
        }
        automaticCircuitState = circuit.state;
      }
      const reserved = await tx.deploymentHealthState.updateMany({
        where: healthKey, data: { ...unleased, status: "RECOVERING" },
      });
      if (reserved.count !== 1) return { kind: "ineligible" };
      const attempt = await tx.deploymentRecoveryAttempt.create({
        data: {
          ...runtimeScope, generation: health.generation, healthVersion: health.version + 1,
          action: "RESTART", trigger, idempotencyKey, createdAt: now,
        },
        select: dispatchSelect,
      });
      await tx.auditLog.create({ data: {
        workspaceId: scope.workspaceId, actorId: options.actorId, entityType: "Deployment", entityId: scope.deploymentId,
        action: "DEPLOYMENT_RECOVERY_REQUESTED", metadata: {
          recoveryId: attempt.id, runtimeInstanceId: runtime.id, trigger,
          ...(automaticCircuitState === undefined ? {} : { automaticCircuitState }),
        },
      } });
      return { kind: "reserved", attempt };
    });
  } catch (error) {
    if (!(error instanceof Prisma.PrismaClientKnownRequestError) || error.code !== "P2002") throw error;
    const key = trigger === "AUTOMATIC" ? derivedKey : options.idempotencyKey;
    const attempt = key ? await replayByKey(db, key) : null;
    return attempt ? { kind: "replay", attempt } : { kind: "busy" };
  }
}
