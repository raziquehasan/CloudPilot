import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import {
  prisma, currentRecoveryRuntime, isRecoveryRuntimeEligible, lockRecoveryRuntime,
  inspectAutomaticRecoveryCircuit,
  recoveryNow, recoveryPolicy, recoveryRuntimeWhere, recoverySchemaReady,
  type HealthFailureReason, type HealthScope, type Prisma,
} from "@cloudpilot/database";
import { TransientJobError, type RuntimeJob } from "@cloudpilot/queue";
import { workerEnv } from "../config/env.js";
import { workerLogger } from "../config/logger.js";
import type { RuntimeExecutor } from "../runtime/runtime-executor.js";
import { healthDefaults, transitionHealth } from "./health-state.js";
import type { HealthRuntime } from "./probe-runtime.js";
import { verifyRecoveryRestart, type RecoveryOperationResult } from "./recovery-operations.js";

type Transaction = Prisma.TransactionClient;
type Claim = {
  scope: HealthScope; attemptId: string; token: string; ownerId: string; version: number;
  runtime: HealthRuntime; readyAt: Date; sourceEpoch: string | null;
  trigger: RuntimeJob["recoveryTrigger"];
};
type ClaimResult = { kind: "claimed"; claim: Claim } | { kind: "busy" | "done" };
const cleared = { leaseOwnerId: null, leaseToken: null, leaseExpiresAt: null };
const runtimeCleared = { executionLeaseToken: null, executionLeaseExpiresAt: null };

function scopeOf(job: RuntimeJob) {
  return {
    workspaceId: job.workspaceId, projectId: job.projectId,
    deploymentId: job.deploymentId, runtimeInstanceId: job.runtimeInstanceId,
  };
}

// Test-only crash hook (Phase 5.7.9). Strictly triple-gated so it can
// never fire outside an explicit test run: NODE_ENV must be "test",
// the test flag must be set, and the stage must match a known boundary.
// It terminates the process abruptly (no finally) to emulate a worker
// crash at a specific recovery boundary.
function maybeCrash(stage: "after-claim" | "after-intent" | "after-restart" | "after-finish") {
  if (process.env.NODE_ENV !== "test") return;
  if (process.env.CLOUDPILOT_RECOVERY_CRASH_TEST !== "1") return;
  if (process.env.CLOUDPILOT_RECOVERY_CRASH_AFTER === stage) {
    workerLogger.warn({ stage }, "Simulated worker crash (test only)");
    process.exit(1);
  }
}

// Test-only fault-injection gate (Phase 5.8.6.B). Strictly gated so it
// can never fire outside an explicit test run: NODE_ENV must be "test",
// the arm flag must be set, and the boundary must match. When armed it
// pauses the worker at a named recovery boundary until
// releaseRecoveryFault() is called (or a bounded timeout elapses). It
// never bypasses runtime identity, ownership, trust, or restart fencing;
// it only holds execution at an existing boundary.
let releaseFault: (() => void) | null = null;
export function releaseRecoveryFault() {
  releaseFault?.();
  releaseFault = null;
}
export function recoveryFaultArmed(stage: string): boolean {
  return process.env.NODE_ENV === "test" && process.env.CLOUDPILOT_RECOVERY_FAULT_TEST === "1" &&
    process.env.CLOUDPILOT_RECOVERY_FAULT_BOUNDARY === stage;
}
async function maybeFault(stage: "after-intent" | "after-restart") {
  if (!recoveryFaultArmed(stage)) return;
  workerLogger.warn({ stage }, "Recovery fault pause (test only)");
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, 30_000);
    releaseFault = () => {
      clearTimeout(timer);
      resolve();
    };
  });
  releaseFault = null;
}

function runtimeTarget(runtime: Awaited<ReturnType<typeof currentRecoveryRuntime>>, buildId?: string): HealthRuntime | null {
  if (!workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY || !isRecoveryRuntimeEligible(runtime) ||
      (buildId && runtime.buildId !== buildId)) return null;
  const repo = runtime.deployment.project.repository?.fullName?.toLowerCase();
  if (!workerEnv.BUILD_TRUSTED_REPOSITORIES.some((item) => item.trim().toLowerCase() === repo)) return null;
  return {
    runtimeAttempt: runtime.runtimeAttempt, containerId: runtime.containerId, imageId: runtime.imageId,
    containerPort: runtime.containerPort ?? workerEnv.RUNTIME_CONTAINER_PORT,
    host: runtime.hostBindingAddress, port: runtime.hostPort,
  };
}

async function latestHealth(tx: Transaction, scope: Omit<HealthScope, "generation">) {
  const { workspaceId, projectId, deploymentId, runtimeInstanceId } = scope;
  return tx.deploymentHealthState.findFirst({
    where: { workspaceId, projectId, deploymentId, runtimeInstanceId }, orderBy: { generation: "desc" },
  });
}

async function audit(tx: Transaction, scope: Omit<HealthScope, "generation">, id: string, status: "FAILED" | "SUCCEEDED" | "CANCELLED",
  reason: HealthFailureReason | null) {
  await tx.auditLog.create({ data: {
    workspaceId: scope.workspaceId, entityType: "Deployment", entityId: scope.deploymentId,
    action: `DEPLOYMENT_RECOVERY_${status}`,
    metadata: { recoveryId: id, runtimeInstanceId: scope.runtimeInstanceId, reason },
  } });
}

async function claimRecovery(job: RuntimeJob, ownerId: string): Promise<ClaimResult> {
  const scope = scopeOf(job);
  return prisma.$transaction(async (tx): Promise<ClaimResult> => {
    if (!await lockRecoveryRuntime(tx, scope)) return { kind: "done" };
    const attempt = await tx.deploymentRecoveryAttempt.findFirst({
      where: { ...scope, id: job.correlationId, trigger: job.recoveryTrigger, action: "RESTART" },
    });
    if (!attempt || (attempt.status !== "REQUESTED" && attempt.status !== "RUNNING")) return { kind: "done" };
    const now = await recoveryNow(tx);
    if (attempt.leaseExpiresAt && attempt.leaseExpiresAt > now) return { kind: "busy" };
    const runtime = await currentRecoveryRuntime(tx, scope);
    const target = runtimeTarget(runtime, job.buildId);
    const health = await latestHealth(tx, scope);
    const generation = { ...scope, generation: attempt.generation };
    if (!runtime?.readyAt || runtime.id !== scope.runtimeInstanceId || !target || !health ||
      health.generation !== attempt.generation || health.version !== attempt.healthVersion || health.status !== "RECOVERING") {
      await tx.deploymentRecoveryAttempt.update({ where: { id: attempt.id }, data: {
        ...cleared, status: "FAILED", failureReason: "RECOVERY_STALE",
        attemptCount: 1, startedAt: attempt.startedAt ?? now, finishedAt: now,
      } });
      if (attempt.healthVersion !== null) await tx.deploymentHealthState.updateMany({
        where: { ...generation, version: attempt.healthVersion, status: "RECOVERING" },
        data: { status: "FAILED", lastFailureReason: "RECOVERY_STALE", nextCheckAt: null, version: { increment: 1 } },
      });
      await audit(tx, scope, attempt.id, "FAILED", "RECOVERY_STALE");
      return { kind: "done" };
    }
    if (runtime.executionLeaseExpiresAt && runtime.executionLeaseExpiresAt > now) return { kind: "busy" };
    const token = randomUUID();
    const expiry = new Date(now.getTime() + recoveryPolicy.leaseMs);
    await tx.deploymentRuntimeInstance.updateMany({
      where: recoveryRuntimeWhere(scope), data: { executionLeaseToken: token, executionLeaseExpiresAt: expiry },
    });
    await tx.deploymentRecoveryAttempt.update({ where: { id: attempt.id }, data: {
      status: "RUNNING", startedAt: attempt.startedAt ?? now, attemptCount: 1,
      leaseOwnerId: ownerId, leaseToken: token, leaseExpiresAt: expiry,
    } });
    return { kind: "claimed", claim: {
      scope: generation, attemptId: attempt.id, version: health.version, token, ownerId, runtime: target,
      readyAt: runtime.readyAt, sourceEpoch: attempt.restartSourceStartedAt, trigger: attempt.trigger,
    } };
  });
}

function ownedAttempt(claim: Claim, now: Date) {
  return {
    ...claim.scope, id: claim.attemptId, status: "RUNNING" as const,
    leaseOwnerId: claim.ownerId, leaseToken: claim.token, leaseExpiresAt: { gt: now },
  };
}

async function revalidate(tx: Transaction, claim: Claim, now: Date) {
  const row = await currentRecoveryRuntime(tx, claim.scope);
  const target = runtimeTarget(row);
  const health = await latestHealth(tx, claim.scope);
  if (!row || row.id !== claim.scope.runtimeInstanceId || row.executionLeaseToken !== claim.token ||
    !row.executionLeaseExpiresAt || row.executionLeaseExpiresAt <= now ||
    row.readyAt?.getTime() !== claim.readyAt.getTime() || !target ||
    target.containerId !== claim.runtime.containerId || target.imageId !== claim.runtime.imageId ||
    target.host !== claim.runtime.host || target.port !== claim.runtime.port ||
    target.containerPort !== claim.runtime.containerPort || target.runtimeAttempt !== claim.runtime.runtimeAttempt ||
    health?.generation !== claim.scope.generation || health.version !== claim.version || health.status !== "RECOVERING") return null;
  return health;
}

async function markRestart(claim: Claim, epoch: string, signal: AbortSignal): Promise<boolean> {
  return prisma.$transaction(async (tx) => {
    if (!await lockRecoveryRuntime(tx, claim.scope) || signal.aborted) return false;
    const now = await recoveryNow(tx);
    if (!await revalidate(tx, claim, now) || signal.aborted) return false;
    if (claim.trigger === "AUTOMATIC") {
      if (!workerEnv.AUTO_RECOVERY_ENABLED) return false;
      const authorization = await tx.auditLog.findFirst({
        where: { workspaceId: claim.scope.workspaceId, entityId: claim.scope.deploymentId, entityType: "Deployment",
          action: "DEPLOYMENT_RECOVERY_REQUESTED", metadata: { path: ["recoveryId"], equals: claim.attemptId } },
        select: { metadata: true },
      });
      const circuit = await inspectAutomaticRecoveryCircuit(tx, claim.scope, now, { excludeAttemptId: claim.attemptId });
      const marker = authorization?.metadata;
      if (circuit.reason !== null || !marker || typeof marker !== "object" || Array.isArray(marker) ||
        marker.automaticCircuitState !== circuit.state || signal.aborted) return false;
    }
    const authorizedAt = await recoveryNow(tx);
    if (signal.aborted) return false;
    const updated = await tx.deploymentRecoveryAttempt.updateMany({
      where: { ...ownedAttempt(claim, authorizedAt), restartIssuedAt: null },
      data: { restartIssuedAt: authorizedAt, restartSourceStartedAt: epoch },
    });
    return updated.count === 1;
  });
}

async function finishRecovery(claim: Claim, observed: RecoveryOperationResult) {
  return prisma.$transaction(async (tx) => {
    if (!await lockRecoveryRuntime(tx, claim.scope)) return null;
    const now = await recoveryNow(tx);
    const owned = ownedAttempt(claim, now);
    if (!await tx.deploymentRecoveryAttempt.findFirst({ where: owned, select: { id: true } })) {
      const terminal = await tx.deploymentRecoveryAttempt.findFirst({
        where: { ...claim.scope, id: claim.attemptId, status: { in: ["CANCELLED", "FAILED", "SUCCEEDED"] } }, select: { id: true },
      });
      if (terminal) return null;
      throw new Error("Recovery lease expired");
    }
    const health = await revalidate(tx, claim, now);
    const result = health ? observed : { ok: false as const, reason: "RECOVERY_STALE" as const };
    const failureReason = result.ok ? null : result.reason;
    const status = result.ok ? "SUCCEEDED" : "FAILED";
    if (health) {
      const { generation: _generation, runtimeInstanceId: _runtimeId, ...deployment } = claim.scope;
      const attemptsInWindow = await tx.deploymentRecoveryAttempt.count({
        where: { ...deployment, createdAt: { gt: new Date(now.getTime() - recoveryPolicy.windowMs) } },
      });
      const state = { ...health, scope: claim.scope };
      const transition = result.ok
        ? transitionHealth(state, { scope: claim.scope, kind: "RECOVERY_VERIFIED" })
        : transitionHealth(state, { scope: claim.scope, kind: "RECOVERY_FAILED", attemptsInWindow: Math.max(1, attemptsInWindow) });
      if (!transition.applied) throw new Error("Invalid recovery settlement");
      const next = transition.state;
      const nextCheckAt = new Date(now.getTime() + healthDefaults.checkIntervalMs);
      const unsafeBinding = failureReason === "RECOVERY_PORT_CHANGED" ||
        failureReason === "RECOVERY_IDENTITY_MISMATCH" || failureReason === "RECOVERY_RESTART_UNCERTAIN";
      await tx.deploymentHealthState.updateMany({
        where: { ...claim.scope, version: claim.version, status: "RECOVERING" },
        data: {
          ...cleared, version: { increment: 1 }, status: result.ok ? "HEALTHY" : (unsafeBinding ? "FAILED" : next.status),
          consecutiveFailures: next.consecutiveFailures, consecutiveSuccesses: next.consecutiveSuccesses,
          ...(result.ok ? { lastCheckedAt: now, lastHealthyAt: now } : {}),
          lastFailureReason: next.status === "FAILED" ? "RECOVERY_EXHAUSTED" : failureReason,
          nextCheckAt: result.ok || unsafeBinding || next.status === "FAILED" ? null : nextCheckAt,
        },
      });
      if (result.ok) {
        const newScope = { ...claim.scope, generation: claim.scope.generation + 1 };
        await tx.deploymentHealthState.create({ data: {
          ...newScope, status: "HEALTHY", consecutiveSuccesses: 1, lastCheckedAt: now, lastHealthyAt: now, nextCheckAt,
        } });
        const observation = result.observation;
        await tx.deploymentHealthCheck.create({ data: {
          ...newScope, checkedAt: now, dockerResult: observation.dockerResult,
          dockerState: observation.docker?.state ?? null, httpResult: observation.httpResult,
          httpStatus: observation.httpStatus, responseLatencyMs: observation.responseLatencyMs,
        } });
        await tx.deploymentRuntimeInstance.updateMany({
          where: { ...recoveryRuntimeWhere(claim.scope), executionLeaseToken: claim.token },
          data: { readyAt: result.startedAt, restartCount: { increment: 1 } },
        });
      }
    }
    await tx.deploymentRecoveryAttempt.updateMany({
      where: owned, data: { ...cleared, status, failureReason, finishedAt: now },
    });
    await tx.deploymentRuntimeInstance.updateMany({
      where: { ...recoveryRuntimeWhere(claim.scope), executionLeaseToken: claim.token }, data: runtimeCleared,
    });
    await audit(tx, claim.scope, claim.attemptId, status, failureReason);
    return { status, failureReason };
  });
}

async function releaseRecoveryForRetry(claim: Claim) {
  await prisma.$transaction(async (tx) => {
    if (!await lockRecoveryRuntime(tx, claim.scope)) return;
    const now = await recoveryNow(tx);
    await tx.deploymentRecoveryAttempt.updateMany({
      where: { id: claim.attemptId, ...claim.scope, status: "RUNNING", leaseOwnerId: claim.ownerId, leaseToken: claim.token },
      data: { leaseExpiresAt: now },
    });
    await tx.deploymentRuntimeInstance.updateMany({
      where: { ...recoveryRuntimeWhere(claim.scope), executionLeaseToken: claim.token }, data: runtimeCleared,
    });
  });
}

/**
 * Conservatively close a not-yet-started AUTOMATIC reservation when
 * automatic recovery has been disabled before execution. A restart
 * that was already issued (RUNNING or intent persisted) is never
 * cancelled here; the caller reconciles its outcome instead.
 * Returns true when the attempt was cancelled (caller skips).
 */
export async function cancelUnstartedAutomatic(job: RuntimeJob, mode: "disabled" | "stale" = "disabled"): Promise<boolean> {
  const scope = scopeOf(job);
  return prisma.$transaction(async (tx) => {
    if (!await lockRecoveryRuntime(tx, scope)) return false;
    const attempt = await tx.deploymentRecoveryAttempt.findFirst({
      where: { ...scope, id: job.correlationId, trigger: "AUTOMATIC", action: "RESTART" },
    });
    if (!attempt) return true;
    if (attempt.restartIssuedAt) return false;
    if (attempt.status !== "REQUESTED" && attempt.status !== "RUNNING") return true;
    const now = await recoveryNow(tx);
    if (mode === "stale") {
      const runtime = await currentRecoveryRuntime(tx, scope);
      const health = await latestHealth(tx, scope);
      const compatible = runtime?.id === scope.runtimeInstanceId && runtimeTarget(runtime, job.buildId) &&
        health?.generation === attempt.generation && health.version === attempt.healthVersion && health.status === "RECOVERING";
      const expired = (!attempt.leaseExpiresAt || attempt.leaseExpiresAt <= now) &&
        attempt.createdAt.getTime() + recoveryPolicy.windowMs <= now.getTime();
      if (compatible && !expired) return false;
    }
    const changed = await tx.deploymentRecoveryAttempt.updateMany({
      where: { id: attempt.id, ...scope, status: attempt.status, restartIssuedAt: null },
      data: { ...cleared, status: "CANCELLED", failureReason: "RECOVERY_STALE", finishedAt: now },
    });
    if (changed.count !== 1) return false;
    if (attempt.leaseToken) await tx.deploymentRuntimeInstance.updateMany({
      where: { ...recoveryRuntimeWhere(scope), executionLeaseToken: attempt.leaseToken }, data: runtimeCleared,
    });
    if (attempt.healthVersion !== null) {
      await tx.deploymentHealthState.updateMany({
        where: { ...scope, runtimeInstanceId: attempt.runtimeInstanceId, generation: attempt.generation, version: attempt.healthVersion, status: "RECOVERING" },
        data: { ...cleared, version: { increment: 1 }, status: "UNHEALTHY", lastFailureReason: "RECOVERY_STALE", nextCheckAt: new Date(now.getTime() + healthDefaults.checkIntervalMs) },
      });
    }
    await audit(tx, scope, attempt.id, "CANCELLED", "RECOVERY_STALE");
    return true;
  });
}

export async function processRecoveryJob(jobId: string | undefined, job: RuntimeJob, executor: RuntimeExecutor, workerSignal: AbortSignal) {
  if (jobId !== `recovery-${job.correlationId}` ||
    !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(job.correlationId)) return { status: "skipped" as const };
  const trigger = job.recoveryTrigger;
  const schemaReady = await recoverySchemaReady(prisma);
  if (trigger === "AUTOMATIC") {
    if (!workerEnv.RUNTIME_ENABLED || !schemaReady) throw new TransientJobError("Automatic recovery is unavailable");
    if (!workerEnv.AUTO_RECOVERY_ENABLED) {
      // Disabled before execution: cancel unstarted reservations, but
      // still reconcile a restart that was already issued.
      if (await cancelUnstartedAutomatic(job)) return { status: "skipped" as const };
    }
  } else if (!workerEnv.MANUAL_RECOVERY_ENABLED || !workerEnv.RUNTIME_ENABLED || !schemaReady) {
    throw new TransientJobError("Manual recovery is unavailable");
  }
  const ownerId = randomUUID();
  while (!workerSignal.aborted) {
    const deadline = AbortSignal.timeout(90_000);
    const signal = AbortSignal.any([workerSignal, deadline]);
    const claimed = await claimRecovery(job, ownerId);
    if (claimed.kind === "done") return { status: "skipped" as const };
    if (claimed.kind === "busy") {
      await delay(2_000, undefined, { signal: workerSignal }).catch(() => {});
      continue;
    }
    if (claimed.kind !== "claimed") break;
    const { claim } = claimed;
    maybeCrash("after-claim");
    try {
      const result = await verifyRecoveryRestart({
        scope: claim.scope, runtime: claim.runtime, executor, signal, sourceEpoch: claim.sourceEpoch,
        timeoutMs: Math.min(15_000, workerEnv.RUNTIME_STOP_TIMEOUT_MS),
        markIntent: async (epoch) => { const ok = await markRestart(claim, epoch, signal); if (ok) { maybeCrash("after-intent"); await maybeFault("after-intent"); } return ok; },
      });
      maybeCrash("after-restart");
      await maybeFault("after-restart");
      if (workerSignal.aborted) throw new Error("Recovery interrupted");
      const settled = await finishRecovery(claim, result);
      maybeCrash("after-finish");
      if (!settled) return { status: "skipped" as const };
      workerLogger.info({ recoveryId: claim.attemptId, deploymentId: job.deploymentId, trigger, failureReason: settled.failureReason },
        `${trigger === "AUTOMATIC" ? "Automatic" : "Manual"} recovery settled`);
      return { status: settled.status === "SUCCEEDED" ? "succeeded" as const : "failed" as const };
    } catch {
      await releaseRecoveryForRetry(claim).catch(() => {});
      throw new TransientJobError("Recovery interrupted or settlement unavailable");
    }
  }
  throw new TransientJobError("Recovery interrupted");
}

export async function withRuntimeLifecycleLease<T>(job: RuntimeJob, workerSignal: AbortSignal, run: (signal: AbortSignal) => Promise<T>) {
  const scope = scopeOf(job);
  const token = randomUUID();
  const signal = AbortSignal.any([workerSignal, AbortSignal.timeout(100_000)]);
  const claimed = await prisma.$transaction(async (tx) => {
    if (signal.aborted) return "busy";
    if (!await lockRecoveryRuntime(tx, scope)) return "missing";
    const [table] = await tx.$queryRaw<Array<{ ready: boolean }>>`SELECT to_regclass('public.deployment_recovery_attempts') IS NOT NULL AS ready`;
    if (table?.ready && await tx.deploymentRecoveryAttempt.findFirst({
      where: {
        workspaceId: scope.workspaceId, projectId: scope.projectId, deploymentId: scope.deploymentId,
        status: { in: ["REQUESTED", "RUNNING"] },
      },
      select: { id: true },
    })) return "busy";
    const now = await recoveryNow(tx);
    const updated = await tx.deploymentRuntimeInstance.updateMany({
      where: { ...recoveryRuntimeWhere(scope), OR: [
        { executionLeaseToken: null }, { executionLeaseExpiresAt: { lte: now } },
      ] },
      data: { executionLeaseToken: token, executionLeaseExpiresAt: new Date(now.getTime() + recoveryPolicy.leaseMs) },
    });
    return updated.count === 1 ? "claimed" : "busy";
  });
  if (claimed === "missing") return { status: "skipped" as const };
  if (claimed !== "claimed") throw new TransientJobError("A runtime operation is already in progress");
  try {
    return await run(signal);
  } finally {
    await prisma.deploymentRuntimeInstance.updateMany({
      where: { ...recoveryRuntimeWhere(scope), executionLeaseToken: token }, data: runtimeCleared,
    }).catch(() => {});
  }
}
