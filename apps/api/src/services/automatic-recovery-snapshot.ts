import {
  currentRecoveryRuntime, inspectAutomaticRecoveryCircuit, isRecoveryRuntimeEligible,
  recoveryNow, recoveryPolicy, recoverySchemaReady, type prisma, type RecoveryScope,
} from "@cloudpilot/database";
import { deriveAutomaticRecoveryAvailability } from "./automatic-recovery-availability.js";
import type { DeriveMonitoringInput } from "./monitoring-report.js";

export async function readAutomaticRecoverySnapshot(
  db: Pick<typeof prisma, "$transaction">, scope: RecoveryScope, configured: boolean, workers: DeriveMonitoringInput,
  repository: string | null, healthSchemaAvailable: boolean,
) {
  const workerAvailability = deriveAutomaticRecoveryAvailability(workers, repository);
  const base = { configured, workerAvailability, eligible: false };
  const unavailable = (reason: string, now: Date | null = null) => ({
    ...base, rejectionReason: reason, snapshotAt: now,
    circuitState: null, remainingBudget: null, maxBudget: null, consecutiveFailures: null,
    cooldownRemainingMs: null, nextEligibleAt: null, manualInterventionRequired: null, latestOutcome: null,
  });

  try {
    return await db.$transaction(async (tx) => {
      await tx.$executeRaw`SET TRANSACTION READ ONLY`;
      const now = await recoveryNow(tx);
      if (!healthSchemaAvailable || !await recoverySchemaReady(tx)) return unavailable("SCHEMA_UNAVAILABLE", now);
      const runtime = await currentRecoveryRuntime(tx, scope);
      if (!runtime) return unavailable("RUNTIME_MISSING", now);
      const state = await tx.deploymentHealthState.findFirst({
        where: { ...scope, runtimeInstanceId: runtime.id }, orderBy: { generation: "desc" },
        select: { status: true, nextCheckAt: true, lastCheckedAt: true, consecutiveFailures: true },
      });
      if (!state) return unavailable("HEALTH_STATE_MISSING", now);

      const active = await tx.deploymentRecoveryAttempt.findFirst({
        where: { ...scope, status: { in: ["REQUESTED", "RUNNING"] } },
        select: { id: true, trigger: true },
      });
      const trial = active?.trigger === "AUTOMATIC" && await tx.auditLog.findFirst({
        where: {
          workspaceId: scope.workspaceId, entityId: scope.deploymentId, entityType: "Deployment",
          action: "DEPLOYMENT_RECOVERY_REQUESTED",
          AND: [
            { metadata: { path: ["recoveryId"], equals: active.id } },
            { metadata: { path: ["automaticCircuitState"], equals: "HALF_OPEN" } },
          ],
        },
        select: { id: true },
      });
      const circuit = await inspectAutomaticRecoveryCircuit(tx, scope, now, { halfOpenInProgress: Boolean(trial) });
      const rejectionReason =
        !configured || workerAvailability === "DISABLED" ? "AUTO_DISABLED" :
          workerAvailability !== "AVAILABLE" ? `WORKER_${workerAvailability}` :
          circuit.reason ? circuit.reason :
          !isRecoveryRuntimeEligible(runtime) ? "RUNTIME_INELIGIBLE" :
          active ? "ACTIVE_RECOVERY" :
          runtime.executionLeaseExpiresAt && runtime.executionLeaseExpiresAt > now ? "CONFLICTING_OPERATION" :
          state.status !== "UNHEALTHY" || !state.nextCheckAt ? "HEALTH_NOT_UNHEALTHY" :
          state.consecutiveFailures < recoveryPolicy.automaticFailureThreshold ? "INSUFFICIENT_FAILURES" :
          !state.lastCheckedAt || state.lastCheckedAt > now ||
            now.getTime() - state.lastCheckedAt.getTime() > recoveryPolicy.automaticFreshnessMs ? "STALE_HEALTH" : null;
      const nextEligibleAt = circuit.nextEligibleAt;

      return {
        ...base, eligible: rejectionReason === null, rejectionReason, snapshotAt: now,
        circuitState: circuit.state, remainingBudget: circuit.remainingBudget, maxBudget: recoveryPolicy.limit,
        consecutiveFailures: circuit.consecutiveFailures, cooldownRemainingMs: circuit.cooldownRemainingMs,
        nextEligibleAt: nextEligibleAt === null ? null : new Date(nextEligibleAt),
        manualInterventionRequired: circuit.manualInterventionRequired, latestOutcome: circuit.latestOutcome,
      };
    }, { isolationLevel: "RepeatableRead", maxWait: 1000, timeout: 3000 });
  } catch {
    return unavailable("CIRCUIT_UNAVAILABLE");
  }
}
