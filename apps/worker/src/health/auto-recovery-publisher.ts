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
import { prisma, recoverySchemaReady, type RecoveryScope } from "@cloudpilot/database";
import { workerLogger } from "../config/logger.js";

/** Minimal queue surface required to publish recovery jobs. */
export interface RecoveryJobQueue {
  getJob(jobId: string): Promise<{ getState(): Promise<string> } | null>;
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
}

function recoveryJobId(recoveryId: string): string {
  return QueueManager.validateJobId(`recovery-${recoveryId}`);
}

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
  try {
    const existing = await queue.getJob(jobId);
    if (!existing) await queue.add("runtime", payload, { jobId });
    return true;
  } catch (error) {
    workerLogger.warn(
      { recoveryId, deploymentId: input.deploymentId, errorName: error instanceof Error ? error.name : "UnknownError" },
      "Automatic recovery queue publication failed; reservation retained for reconciliation",
    );
    return false;
  }
}

/**
 * Re-publish REQUESTED automatic recoveries whose queue job is
 * missing (e.g. a crash or Redis outage between reservation and
 * publication). Bounded and idempotent; never touches Docker.
 */
export async function reconcileUnpublishedAutoRecoveries(
  queue: RecoveryJobQueue,
  limit: number,
): Promise<ReconcileResult> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 128) throw new RangeError("Invalid reconciliation batch size");
  if (!await recoverySchemaReady(prisma)) return { checked: 0, published: 0, failed: 0 };
  const attempts = await prisma.deploymentRecoveryAttempt.findMany({
    where: { status: "REQUESTED", trigger: "AUTOMATIC" },
    select: {
      id: true, workspaceId: true, projectId: true, deploymentId: true, runtimeInstanceId: true,
      healthState: { select: { runtime: { select: { buildId: true } } } },
    },
    orderBy: { createdAt: "asc" },
    take: limit,
  });
  let published = 0;
  let failed = 0;
  for (const attempt of attempts) {
    const buildId = attempt.healthState?.runtime?.buildId;
    if (!buildId) continue;
    const ok = await publishRecoveryJob(
      queue,
      { workspaceId: attempt.workspaceId, projectId: attempt.projectId, deploymentId: attempt.deploymentId, runtimeInstanceId: attempt.runtimeInstanceId, buildId },
      attempt.id,
      "AUTOMATIC",
    );
    if (ok) published += 1;
    else failed += 1;
  }
  return { checked: attempts.length, published, failed };
}