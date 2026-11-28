// ==================================================
// Automatic recovery publication & reconciliation (Phase 5.8.3)
// ==================================================
//
// Bridges durable AUTOMATIC recovery reservations to the
// existing runtime BullMQ queue using deterministic job ids.
// Publication is idempotent (add-by-job-id) and is always
// preceded by a durable reservation, so a Redis outage or
// worker crash can never silently lose a reserved recovery:
// the reconciliation pass re-publishes any REQUESTED
// automatic attempt whose queue job is missing.
//
// This module never restarts containers. Execution is performed
// only by the existing recovery processor via the runtime queue.
// ==================================================

import { QueueManager, type RecoveryJobTrigger, type RuntimeJob } from "@cloudpilot/queue";
import { setTimeout as delay } from "node:timers/promises";
import { lockRecoveryRuntime, prisma, recoveryNow, recoverySchemaReady, type RecoveryScope } from "@cloudpilot/database";
import { workerEnv } from "../config/env.js";
import { workerLogger } from "../config/logger.js";
import { cancelUnstartedAutomatic } from "./recovery-processor.js";

/** Minimal queue surface required to publish recovery jobs. */
export interface RecoveryJobQueue {
  getJob(jobId: string): Promise<{ getState(): Promise<string>; retry?(state: "failed" | "completed", options: { resetAttemptsMade: boolean; resetAttemptsStarted: boolean }): Promise<void> } | null>;
  add(name: string, payload: RuntimeJob, opts: { jobId: string }): Promise<unknown>;
}

export interface RecoveryPublicationInput extends RecoveryScope {
  runtimeInstanceId: string;
  buildId: string;
}

export interface ReconcileResult {
  checked: number;
  published: number;
  failed: number;
  cancelled: number;
  repaired: number;
  nextCursor: string | null;
}

function recoveryJobId(recoveryId: string): string {
  return QueueManager.validateJobId(`recovery-${recoveryId}`);
}

const pendingQueueOperations = new WeakSet<RecoveryJobQueue>();

/**
 * Idempotently publish a recovery job for a durable reservation.
 * Returns true when a job is present (added or already existing),
 * false when publication failed (the reservation remains durable
 * and will be reconciled later).
 */
export async function publishRecoveryJob(
  queue: RecoveryJobQueue,
  input: RecoveryPublicationInput,
  recoveryId: string,
  trigger: RecoveryJobTrigger,
): Promise<boolean> {
  if (pendingQueueOperations.has(queue)) return false;
  const jobId = recoveryJobId(recoveryId);
  const payload: RuntimeJob = {
    kind: "runtime",
    workspaceId: input.workspaceId,
    projectId: input.projectId,
    deploymentId: input.deploymentId,
    runtimeInstanceId: input.runtimeInstanceId,
    buildId: input.buildId,
    action: "recover",
    recoveryTrigger: trigger,
    correlationId: recoveryId,
  };
  pendingQueueOperations.add(queue);
  const work = (async () => {
    const existing = await queue.getJob(jobId);
    const state = await existing?.getState();
    if (!existing || state === "unknown") {
      await queue.add("runtime", payload, { jobId });
    } else if (state === "failed" || state === "completed") {
      if (!existing.retry) return false;
      try {
        await existing.retry(state, { resetAttemptsMade: true, resetAttemptsStarted: true });
      } catch (error) {
        if (!["waiting", "active", "delayed", "prioritized"].includes(await existing.getState())) throw error;
      }
    }
    return true;
  })().finally(() => pendingQueueOperations.delete(queue));
  const timeout = new AbortController();
  try {
    return await Promise.race([
      work,
      delay(2_000, undefined, { signal: timeout.signal }).then(() => {
        throw new Error("Recovery queue deadline exceeded");
      }),
    ]);
  } catch (error) {
    workerLogger.warn(
      { recoveryId, deploymentId: input.deploymentId, errorName: error instanceof Error ? error.name : "UnknownError" },
      "Automatic recovery queue publication failed; reservation retained for reconciliation",
    );
    return false;
  } finally {
    timeout.abort();
  }
}

/**
 * Re-publish REQUESTED automatic recoveries whose queue job is
 * missing (e.g. a crash or Redis outage between reservation and
 * publication). Bounded and idempotent; never touches Docker.
 */
export async function reconcileUnpublishedAutoRecoveries(
  queue: RecoveryJobQueue | null,
  limit: number,
  options: { deploymentId?: string; afterId?: string; signal?: AbortSignal; autoEnabled?: boolean } = {},
): Promise<ReconcileResult> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 128) throw new RangeError("Invalid reconciliation batch size");
  const result: ReconcileResult = { checked: 0, published: 0, failed: 0, cancelled: 0, repaired: 0, nextCursor: null };
  if (options.signal?.aborted || !await recoverySchemaReady(prisma)) return result;
  const enabled = options.autoEnabled ?? workerEnv.AUTO_RECOVERY_ENABLED;
  const now = await recoveryNow(prisma);
  const deploymentScope = options.deploymentId ? { deploymentId: options.deploymentId } : {};
  const attempts = await prisma.deploymentRecoveryAttempt.findMany({
    where: {
      ...deploymentScope,
      ...(options.afterId ? { id: { gt: options.afterId } } : {}),
      trigger: "AUTOMATIC", OR: [
        { status: "REQUESTED" },
        { status: "RUNNING", leaseExpiresAt: { lte: now } },
        ...(!enabled ? [{ status: "RUNNING" as const, restartIssuedAt: null }] : []),
      ],
    },
    select: {
      id: true, workspaceId: true, projectId: true, deploymentId: true, runtimeInstanceId: true,
      restartIssuedAt: true,
      healthState: { select: { runtime: { select: { buildId: true } } } },
    },
    orderBy: { id: "asc" },
    take: limit,
  });
  result.nextCursor = attempts.length === limit ? attempts.at(-1)?.id ?? null : null;
  for (const attempt of attempts) {
    if (options.signal?.aborted) break;
    result.checked++;
    const { workspaceId, projectId, deploymentId, runtimeInstanceId } = attempt;
    const input = { workspaceId, projectId, deploymentId, runtimeInstanceId, buildId: attempt.healthState.runtime.buildId };
    try {
      if (!attempt.restartIssuedAt && await cancelUnstartedAutomatic({
        ...input, kind: "runtime", action: "recover", correlationId: attempt.id, recoveryTrigger: "AUTOMATIC",
      }, enabled ? "stale" : "disabled")) {
        result.cancelled++;
        continue;
      }
      // Disablement can race with intent persistence. Only durable intent permits
      // dispatch when disabled. The processor independently fences execution.
      if (!enabled && !attempt.restartIssuedAt && !await prisma.deploymentRecoveryAttempt.findFirst({
        where: { id: attempt.id, status: "RUNNING", restartIssuedAt: { not: null } }, select: { id: true },
      })) continue;
      if (queue && await publishRecoveryJob(queue, input, attempt.id, "AUTOMATIC")) result.published++;
      else result.failed++;
    } catch {
      result.failed++;
      workerLogger.warn({ recoveryId: attempt.id, deploymentId }, "Automatic recovery reconciliation failed");
    }
  }
  if (!options.signal?.aborted) {
    const orphans = await prisma.deploymentHealthState.findMany({
      where: { ...deploymentScope, status: "RECOVERING", recoveryAttempts: { none: { status: { in: ["REQUESTED", "RUNNING"] } } } },
      select: { workspaceId: true, projectId: true, deploymentId: true, runtimeInstanceId: true, generation: true, version: true },
      orderBy: [{ updatedAt: "asc" }, { runtimeInstanceId: "asc" }, { generation: "asc" }], take: limit,
    });
    for (const orphan of orphans) {
      if (options.signal?.aborted) break;
      result.repaired += await prisma.$transaction(async (tx) => {
        if (!await lockRecoveryRuntime(tx, orphan)) return 0;
        const updated = await tx.deploymentHealthState.updateMany({
          where: { ...orphan, status: "RECOVERING", recoveryAttempts: { none: { status: { in: ["REQUESTED", "RUNNING"] } } } },
          data: { status: "FAILED", lastFailureReason: "RECOVERY_STALE", nextCheckAt: null,
            leaseOwnerId: null, leaseToken: null, leaseExpiresAt: null, version: { increment: 1 } },
        });
        return updated.count;
      });
    }
  }
  return result;
}
