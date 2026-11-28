// ==================================================
// Automatic recovery candidate discovery (Phase 5.8.2)
// ==================================================
//
// Bounded, READ-ONLY discovery and evaluation of
// potentially eligible unhealthy runtimes. This module
// never creates recovery attempts, enqueues jobs, or
// invokes Docker. Results are advisory snapshots only;
// any future reservation stage must revalidate all
// safety conditions atomically.
// ==================================================

import { inspectAutomaticRecoveryCircuit, Prisma, prisma, recoveryPolicy } from "@cloudpilot/database";
import { workerEnv } from "../config/env.js";
import { healthDefaults } from "./health-state.js";
import { evaluateAutoRecovery, type AutoRecoveryDecision } from "./auto-recovery-policy.js";

type CircuitSnapshot = Awaited<ReturnType<typeof inspectAutomaticRecoveryCircuit>>;

export interface AutoRecoveryCandidateRow {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  runtimeInstanceId: string;
  generation: number;
  version: number;
  deploymentStatus: string;
  runtimeStatus: string;
  runtimeEligible: boolean;
  healthStatus: string;
  consecutiveFailures: number;
  lastCheckedAt: Date | null;
  generationMatches: boolean;
  restartUncertain: boolean;
  activeRecovery: boolean;
  conflictingOperation: boolean;
  attemptsInWindow: number;
  lastFailedFinishedAt: Date | null;
  recentFailureCount: number;
  now: Date;
  circuit?: CircuitSnapshot;
}

export interface CandidateEvaluation {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  runtimeInstanceId: string;
  generation: number;
  version: number;
  decision: AutoRecoveryDecision | { eligible: false; reason: NonNullable<CircuitSnapshot["reason"]> };
  circuit: CircuitSnapshot | null;
  automaticRecoveryAvailable: boolean;
}

export interface EvaluationBatch {
  evaluated: number;
  eligible: number;
  rejected: number;
  reasons: Record<string, number>;
  durationMs: number;
}

/**
 * Pure evaluation of discovered rows. Uses the row's
 * database `now` for freshness and cooldown so all
 * time-based decisions share the PostgreSQL clock.
 */
export interface EvaluationOptions {
  recoveryLimit?: number;
  autoRecoveryEnabled?: boolean;
  persistenceEnabled?: boolean;
  monitoringAvailable?: boolean;
  infrastructureReady?: boolean;
}

export function evaluateCandidateRows(
  rows: AutoRecoveryCandidateRow[],
  options: EvaluationOptions = {},
): { results: CandidateEvaluation[]; batch: Omit<EvaluationBatch, "durationMs"> } {
  const recoveryLimit = options.recoveryLimit ?? recoveryPolicy.limit;
  const autoRecoveryEnabled = options.autoRecoveryEnabled ?? workerEnv.AUTO_RECOVERY_ENABLED;
  const persistenceEnabled = options.persistenceEnabled ?? workerEnv.HEALTH_PERSISTENCE_ENABLED;
  const monitoringAvailable = options.monitoringAvailable ?? true;
  const infrastructureReady = options.infrastructureReady ?? true;

  const results: CandidateEvaluation[] = [];
  const reasons: Record<string, number> = {};
  let eligible = 0;

  for (const row of rows) {
    const nowMs = row.now.getTime();
    const cooldownRemainingMs = row.circuit?.cooldownRemainingMs ?? (row.lastFailedFinishedAt
      ? Math.max(0, row.lastFailedFinishedAt.getTime() + recoveryPolicy.cooldownMs * 2 ** Math.max(0, row.recentFailureCount - 1) - nowMs)
      : 0);

    const baseDecision = evaluateAutoRecovery({
      autoRecoveryEnabled,
      persistenceEnabled,
      monitoringAvailable,
      infrastructureReady,
      deploymentStatus: row.deploymentStatus,
      runtimeStatus: row.runtimeStatus,
      runtimeEligible: row.runtimeEligible,
      healthStatus: row.healthStatus as never,
      consecutiveFailures: row.consecutiveFailures,
      lastCheckedAt: row.lastCheckedAt ? row.lastCheckedAt.getTime() : null,
      now: nowMs,
      freshnessMs: healthDefaults.checkIntervalMs * 2,
      generationMatches: row.generationMatches,
      restartUncertain: row.restartUncertain,
      activeRecovery: row.activeRecovery,
      conflictingOperation: row.conflictingOperation,
      attemptsInWindow: row.attemptsInWindow,
      recoveryLimit,
      cooldownRemainingMs,
    });

    const circuit = row.circuit ?? null;
    const decision = baseDecision.eligible && circuit?.reason
      ? { eligible: false as const, reason: circuit.reason }
      : baseDecision;
    if (decision.eligible) eligible += 1;
    else reasons[decision.reason] = (reasons[decision.reason] ?? 0) + 1;

    results.push({
      workspaceId: row.workspaceId,
      projectId: row.projectId,
      deploymentId: row.deploymentId,
      runtimeInstanceId: row.runtimeInstanceId,
      generation: row.generation,
      version: row.version,
      decision,
      circuit,
      automaticRecoveryAvailable: decision.eligible,
    });
  }

  return { results, batch: { evaluated: rows.length, eligible, rejected: rows.length - eligible, reasons } };
}

/**
 * Bounded, read-only discovery of candidate runtimes using a
 * bounded query plus shared circuit snapshots. Pre-filters to current, trusted,
 * eligible, unhealthy runtimes with sufficient, fresh failures
 * and no active/conflicting operation; the evaluator makes the
 * final advisory decision (budget, cooldown, uncertainty).
 */
export async function discoverAutoRecoveryCandidates(limit: number, deploymentId?: string): Promise<AutoRecoveryCandidateRow[]> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 128) {
    throw new RangeError("Invalid candidate batch size");
  }
  const repositories = workerEnv.BUILD_TRUSTED_REPOSITORIES.map((name) => name.trim().toLowerCase());
  if (!workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY || repositories.length === 0) return [];

  const deploymentFilter = deploymentId ? Prisma.sql`AND h."deploymentId" = ${deploymentId}::uuid` : Prisma.empty;
  const rows = await prisma.$queryRaw<Array<Record<string, unknown>>>(Prisma.sql`
    WITH db_clock AS MATERIALIZED (SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now)
    SELECT
      h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId", h."generation", h."version",
      d."status" AS "deploymentStatus", r."status" AS "runtimeStatus",
      h."status" AS "healthStatus", h."consecutiveFailures", h."lastCheckedAt",
      db_clock.now AS "now"
    FROM "deployment_health_states" h
    JOIN "deployment_runtime_instances" r ON (r."workspaceId", r."projectId", r."deploymentId", r.id) =
      (h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId")
    JOIN "deployments" d ON (d."workspaceId", d."projectId", d.id) = (h."workspaceId", h."projectId", h."deploymentId")
    JOIN "repositories" repo ON (repo."workspaceId", repo."projectId") = (h."workspaceId", h."projectId")
    CROSS JOIN db_clock
    WHERE h."status" = 'UNHEALTHY' AND h."nextCheckAt" IS NOT NULL
      ${deploymentFilter}
      AND NOT EXISTS (
        SELECT 1 FROM "deployment_health_states" newer
        WHERE (newer."workspaceId", newer."projectId", newer."deploymentId", newer."runtimeInstanceId") =
          (h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId") AND newer."generation" > h."generation"
      )
      AND NOT EXISTS (
        SELECT 1 FROM "deployment_runtime_instances" newer
        WHERE (newer."workspaceId", newer."projectId", newer."deploymentId") =
          (h."workspaceId", h."projectId", h."deploymentId") AND newer."runtimeAttempt" > r."runtimeAttempt"
      )
      AND h."consecutiveFailures" >= ${healthDefaults.consecutiveFailureLimit}
      AND h."lastCheckedAt" IS NOT NULL
      AND h."lastCheckedAt" > db_clock.now - (${healthDefaults.checkIntervalMs * 2}::bigint * interval '1 millisecond')
      AND d."status" = 'RUNNING' AND r."status" = 'RUNNING'
      AND r."readyAt" IS NOT NULL AND r."containerId" ~ '^[0-9a-f]{64}$' AND r."imageId" ~ '^sha256:[0-9a-f]{64}$'
      AND r."hostBindingAddress" IN ('127.0.0.1','::1') AND r."hostPort" BETWEEN 1 AND 65535
      AND lower(repo."fullName") IN (${Prisma.join(repositories)})
      AND NOT EXISTS (
        SELECT 1 FROM "deployment_recovery_attempts" a
        WHERE (a."workspaceId", a."projectId", a."deploymentId") = (h."workspaceId", h."projectId", h."deploymentId")
          AND a."status" IN ('REQUESTED','RUNNING')
      )
      AND (r."executionLeaseExpiresAt" IS NULL OR r."executionLeaseExpiresAt" <= db_clock.now)
    ORDER BY h."lastCheckedAt", h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId"
    LIMIT ${limit}::integer
  `);

  const candidates: AutoRecoveryCandidateRow[] = [];
  for (const row of rows) {
    const scope = {
      workspaceId: String(row.workspaceId), projectId: String(row.projectId), deploymentId: String(row.deploymentId),
    };
    const now = new Date(row.now as string);
    const circuit = await inspectAutomaticRecoveryCircuit(prisma, scope, now);
    candidates.push({
      ...scope, runtimeInstanceId: String(row.runtimeInstanceId),
      generation: Number(row.generation), version: Number(row.version),
      deploymentStatus: String(row.deploymentStatus), runtimeStatus: String(row.runtimeStatus),
      runtimeEligible: true, healthStatus: String(row.healthStatus),
      consecutiveFailures: Number(row.consecutiveFailures),
      lastCheckedAt: row.lastCheckedAt ? new Date(row.lastCheckedAt as string) : null,
      generationMatches: true, restartUncertain: false, activeRecovery: false, conflictingOperation: false,
      attemptsInWindow: recoveryPolicy.limit - circuit.remainingBudget,
      lastFailedFinishedAt: null, recentFailureCount: 0,
      now, circuit,
    });
  }
  return candidates;
}
