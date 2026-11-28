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

import { Prisma, prisma } from "@cloudpilot/database";
import { recoveryPolicy } from "@cloudpilot/database";
import { workerEnv } from "../config/env.js";
import { healthDefaults } from "./health-state.js";
import { evaluateAutoRecovery, type AutoRecoveryDecision } from "./auto-recovery-policy.js";

export interface AutoRecoveryCandidateRow {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  runtimeInstanceId: string;
  generation: number;
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
}

export interface CandidateEvaluation {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  runtimeInstanceId: string;
  generation: number;
  decision: AutoRecoveryDecision;
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
    const cooldownRemainingMs = row.lastFailedFinishedAt
      ? Math.max(0, row.lastFailedFinishedAt.getTime() + recoveryPolicy.cooldownMs * 2 ** Math.max(0, row.recentFailureCount - 1) - nowMs)
      : 0;

    const decision = evaluateAutoRecovery({
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

    if (decision.eligible) eligible += 1;
    else reasons[decision.reason] = (reasons[decision.reason] ?? 0) + 1;

    results.push({
      workspaceId: row.workspaceId,
      projectId: row.projectId,
      deploymentId: row.deploymentId,
      runtimeInstanceId: row.runtimeInstanceId,
      generation: row.generation,
      decision,
    });
  }

  return { results, batch: { evaluated: rows.length, eligible, rejected: rows.length - eligible, reasons } };
}

/**
 * Bounded, read-only discovery of candidate runtimes using a
 * single query (no N+1). Pre-filters to current, trusted,
 * eligible, unhealthy runtimes with sufficient, fresh failures
 * and no active/conflicting operation; the evaluator makes the
 * final advisory decision (budget, cooldown, uncertainty).
 */
export async function discoverAutoRecoveryCandidates(limit: number): Promise<AutoRecoveryCandidateRow[]> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 128) {
    throw new RangeError("Invalid candidate batch size");
  }
  const repositories = workerEnv.BUILD_TRUSTED_REPOSITORIES.map((name) => name.trim().toLowerCase());
  if (!workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY || repositories.length === 0) return [];

  const rows = await prisma.$queryRaw<Array<Record<string, unknown>>>(Prisma.sql`
    WITH db_clock AS MATERIALIZED (SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now)
    SELECT
      h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId", h."generation",
      d."status" AS "deploymentStatus", r."status" AS "runtimeStatus",
      h."status" AS "healthStatus", h."consecutiveFailures", h."lastCheckedAt",
      (h."generation" = r."runtimeAttempt") AS "generationMatches",
      (h."status" = 'FAILED' AND h."lastFailureReason" = 'RECOVERY_RESTART_UNCERTAIN') AS "restartUncertain",
      EXISTS (
        SELECT 1 FROM "deployment_recovery_attempts" a
        WHERE (a."workspaceId", a."projectId", a."deploymentId") = (h."workspaceId", h."projectId", h."deploymentId")
          AND a."status" IN ('REQUESTED','RUNNING')
      ) AS "activeRecovery",
      (r."executionLeaseExpiresAt" IS NOT NULL AND r."executionLeaseExpiresAt" > db_clock.now) AS "conflictingOperation",
      (
        SELECT count(*) FROM "deployment_recovery_attempts" a
        WHERE (a."workspaceId", a."projectId", a."deploymentId") = (h."workspaceId", h."projectId", h."deploymentId")
          AND a."createdAt" > db_clock.now - (${recoveryPolicy.windowMs}::bigint * interval '1 millisecond')
      ) AS "attemptsInWindow",
      (
        SELECT max(a."finishedAt") FROM "deployment_recovery_attempts" a
        WHERE (a."workspaceId", a."projectId", a."deploymentId") = (h."workspaceId", h."projectId", h."deploymentId")
          AND a."status" = 'FAILED'
      ) AS "lastFailedFinishedAt",
      (
        SELECT count(*) FROM "deployment_recovery_attempts" a
        WHERE (a."workspaceId", a."projectId", a."deploymentId") = (h."workspaceId", h."projectId", h."deploymentId")
          AND a."createdAt" > db_clock.now - (${recoveryPolicy.windowMs}::bigint * interval '1 millisecond')
      ) AS "recentFailureCount",
      db_clock.now AS "now"
    FROM "deployment_health_states" h
    JOIN "deployment_runtime_instances" r ON (r."workspaceId", r."projectId", r."deploymentId", r.id) =
      (h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId")
    JOIN "deployments" d ON (d."workspaceId", d."projectId", d.id) = (h."workspaceId", h."projectId", h."deploymentId")
    JOIN "repositories" repo ON (repo."workspaceId", repo."projectId") = (h."workspaceId", h."projectId")
    CROSS JOIN db_clock
    WHERE h."status" = 'UNHEALTHY'
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

  return rows.map((row) => ({
    workspaceId: String(row.workspaceId),
    projectId: String(row.projectId),
    deploymentId: String(row.deploymentId),
    runtimeInstanceId: String(row.runtimeInstanceId),
    generation: Number(row.generation),
    deploymentStatus: String(row.deploymentStatus),
    runtimeStatus: String(row.runtimeStatus),
    runtimeEligible: true,
    healthStatus: String(row.healthStatus),
    consecutiveFailures: Number(row.consecutiveFailures),
    lastCheckedAt: row.lastCheckedAt ? new Date(row.lastCheckedAt as string) : null,
    generationMatches: row.generationMatches === true,
    restartUncertain: row.restartUncertain === true,
    activeRecovery: row.activeRecovery === true,
    conflictingOperation: row.conflictingOperation === true,
    attemptsInWindow: Number(row.attemptsInWindow),
    lastFailedFinishedAt: row.lastFailedFinishedAt ? new Date(row.lastFailedFinishedAt as string) : null,
    recentFailureCount: Number(row.recentFailureCount),
    now: new Date(row.now as string),
  }));
}