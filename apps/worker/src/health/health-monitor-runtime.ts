import { workerMetrics } from "../metrics.js";
import { randomUUID } from "node:crypto";
import { Prisma, prisma, reserveAutomaticRecovery, type HealthScope } from "@cloudpilot/database";
import { workerEnv } from "../config/env.js";
import { workerLogger } from "../config/logger.js";
import { startHealthMonitor } from "./health-monitor.js";
import type { HealthInspector } from "./probe-runtime.js";
import { healthPersistenceReady, runRuntimeHealthCheck } from "./runtime-health.js";
import { discoverAutoRecoveryCandidates, evaluateCandidateRows } from "./auto-recovery-candidates.js";
import { publishRecoveryJob, reconcileUnpublishedAutoRecoveries, type RecoveryJobQueue } from "./auto-recovery-publisher.js";

export async function discoverDueHealthChecks(limit: number, deploymentId?: string): Promise<HealthScope[]> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 128) throw new RangeError("Invalid health batch size");
  const repositories = workerEnv.BUILD_TRUSTED_REPOSITORIES.map((name) => name.trim().toLowerCase());
  if (!workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY || repositories.length === 0) return [];
  const deploymentFilter = deploymentId ? Prisma.sql`AND h."deploymentId" = ${deploymentId}::uuid` : Prisma.empty;
  return prisma.$queryRaw<HealthScope[]>`
    WITH db_clock AS MATERIALIZED (SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now)
    SELECT h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId", h."generation"
    FROM "deployment_health_states" h
    JOIN "deployment_runtime_instances" r ON (r."workspaceId", r."projectId", r."deploymentId", r.id) =
      (h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId")
    JOIN "deployments" d ON (d."workspaceId", d."projectId", d.id) =
      (h."workspaceId", h."projectId", h."deploymentId")
    JOIN "repositories" repo ON (repo."workspaceId", repo."projectId") = (h."workspaceId", h."projectId")
    CROSS JOIN db_clock
    WHERE h."nextCheckAt" <= db_clock.now
      AND (h."leaseExpiresAt" IS NULL OR h."leaseExpiresAt" <= db_clock.now)
      AND h.status NOT IN ('FAILED', 'RECOVERING') AND d.status = 'RUNNING' AND r.status = 'RUNNING'
      AND r."readyAt" IS NOT NULL AND r."containerId" ~ '^[0-9a-f]{64}$' AND r."imageId" ~ '^sha256:[0-9a-f]{64}$'
      AND r."hostBindingAddress" IN ('127.0.0.1', '::1') AND r."hostPort" BETWEEN 1 AND 65535
      AND lower(repo."fullName") IN (${Prisma.join(repositories)})
      ${deploymentFilter}
      AND NOT EXISTS (
        SELECT 1 FROM "deployment_health_states" newer
        WHERE (newer."workspaceId", newer."projectId", newer."deploymentId", newer."runtimeInstanceId") =
          (h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId")
          AND newer."generation" > h."generation"
      )
      AND NOT EXISTS (
        SELECT 1 FROM "deployment_runtime_instances" replacement
        WHERE (replacement."workspaceId", replacement."projectId", replacement."deploymentId") =
          (h."workspaceId", h."projectId", h."deploymentId")
          AND replacement."runtimeAttempt" > r."runtimeAttempt" AND replacement.status = 'RUNNING'
          AND replacement."readyAt" IS NOT NULL
      )
    ORDER BY h."nextCheckAt", h."workspaceId", h."projectId", h."deploymentId", h."runtimeInstanceId", h."generation"
    LIMIT ${limit}::integer
  `;
}

export async function pruneHealthHistory(limit = 500): Promise<number> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 500) throw new RangeError("Invalid health retention batch size");
  return prisma.$executeRaw`
    WITH expired AS (
      SELECT id FROM "deployment_health_checks"
      WHERE "checkedAt" < (statement_timestamp() AT TIME ZONE 'UTC') - INTERVAL '7 days'
      ORDER BY "checkedAt", id LIMIT ${limit}::integer FOR UPDATE SKIP LOCKED
    )
    DELETE FROM "deployment_health_checks" h USING expired WHERE h.id = expired.id
  `;
}

export function startDeploymentHealthMonitor(inspect: HealthInspector, signal: AbortSignal, deploymentId?: string, recoveryQueue?: RecoveryJobQueue | null) {
  const ownerId = randomUUID();
  let reconciliationCursor: string | null = null;
  let nextReconciliationAt = 0;
  return startHealthMonitor({
    enabled: workerEnv.HEALTH_MONITORING_ENABLED && workerEnv.HEALTH_PERSISTENCE_ENABLED &&
      workerEnv.RUNTIME_ENABLED && workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY,
    concurrency: workerEnv.HEALTH_MONITORING_CONCURRENCY,
    batchSize: workerEnv.HEALTH_MONITORING_BATCH_SIZE,
    signal,
    ready: healthPersistenceReady,
    discover: (limit) => discoverDueHealthChecks(limit, deploymentId),
    ...(deploymentId ? {} : { retain: pruneHealthHistory }),
    async maintenance() {
      if (performance.now() < nextReconciliationAt) return;
      nextReconciliationAt = performance.now() + 5_000;
      const result = await reconcileUnpublishedAutoRecoveries(recoveryQueue ?? null, workerEnv.HEALTH_MONITORING_BATCH_SIZE, {
        ...(deploymentId ? { deploymentId } : {}),
        ...(reconciliationCursor ? { afterId: reconciliationCursor } : {}),
        signal,
      });
      reconciliationCursor = result.nextCursor;
      if (result.checked || result.repaired) workerLogger.info({ ownerId, ...result }, "Automatic recovery reconciliation");
    },
    async afterBatch() {
      // Automatic recovery is disabled by default; skip discovery and
      // reservation entirely (no DB query) when the flag is off.
      if (!workerEnv.AUTO_RECOVERY_ENABLED) return;
      const started = performance.now();
      const rows = await discoverAutoRecoveryCandidates(workerEnv.HEALTH_MONITORING_BATCH_SIZE, deploymentId);
      const { results, batch } = evaluateCandidateRows(rows);
      for (const candidate of results) workerLogger.debug(candidate, "Automatic recovery eligibility");
      const eligible = results.filter((result) => result.decision.eligible);
      let reserved = 0;
      let published = 0;
      let publicationFailed = 0;
      // Reserve + publish only when a queue is available; otherwise a
      // reservation would be orphaned with no way to execute it.
      if (recoveryQueue) {
        for (const candidate of eligible) {
          if (signal.aborted) break;
          const scope = { workspaceId: candidate.workspaceId, projectId: candidate.projectId, deploymentId: candidate.deploymentId };
          const result = await reserveAutomaticRecovery(prisma, scope, candidate.runtimeInstanceId, {
            generation: candidate.generation, version: candidate.version,
          });
          if (result.kind !== "reserved" && result.kind !== "replay") continue;
          reserved += 1;
          const buildId = result.attempt.healthState?.runtime?.buildId;
          if (!buildId) continue;
          const ok = await publishRecoveryJob(
            recoveryQueue,
            { workspaceId: candidate.workspaceId, projectId: candidate.projectId, deploymentId: candidate.deploymentId, runtimeInstanceId: candidate.runtimeInstanceId, buildId },
            result.attempt.id,
            "AUTOMATIC",
          );
          if (ok) published += 1;
          else publicationFailed += 1;
        }
        workerLogger.info({
          ownerId,
          event: "auto_recovery",
          evaluated: batch.evaluated,
          eligible: eligible.length,
          rejected: batch.rejected,
          reasons: Object.entries(batch.reasons).map(([k, v]) => `${k}:${v}`).join(","),
          reserved,
          published,
          publicationFailed,
          durationMs: Math.round(performance.now() - started),
        }, "Automatic recovery candidate evaluation");
      } else {
        workerLogger.info({
          ownerId,
          event: "auto_recovery",
          evaluated: batch.evaluated,
          eligible: eligible.length,
          rejected: batch.rejected,
          reasons: Object.entries(batch.reasons).map(([k, v]) => `${k}:${v}`).join(","),
          reserved: 0,
          published: 0,
          publicationFailed: 0,
          skipped: "no-queue",
          durationMs: Math.round(performance.now() - started),
        }, "Automatic recovery candidate evaluation");
      }
    },
    async check(scope, checkSignal) {
      const started = performance.now();
      let dockerError = false;
      await runRuntimeHealthCheck(scope, ownerId, inspect, {
        signal: checkSignal,
        onEvent(event) {
          const detail = { ...scope, ...event };
          if (event.kind === "docker_error") {
            dockerError = true;
            workerLogger.debug(detail, "Health check Docker inspection failed");
            return;
          }
          if (event.kind !== "recorded") {
            workerLogger.debug(detail, "Health check skipped");
            return;
          }
          workerMetrics?.healthCheck(event.failureReason, (performance.now() - started) / 1000);
          if (event.previousStatus !== event.status) workerLogger.info(detail, "Deployment health state changed");
          workerLogger.debug(detail, event.failureReason ? "Deployment health check failed" : "Deployment health check passed");
        },
      });
      if (dockerError && !checkSignal.aborted) throw new Error("Docker health inspection failed");
    },
    emit(event, detail) {
      const data = { ownerId, deploymentId, event, ...detail };
      if (event === "batch" && typeof detail?.batchDurationMs === "number") {
        workerMetrics?.healthBatch(detail.consecutiveFailures ? "infrastructure_error" : "completed", detail.batchDurationMs / 1000);
      }
      if (event === "infrastructure_error") workerLogger.warn(data, "Deployment health monitor infrastructure error");
      else if (event === "batch") workerLogger.debug(data, "Deployment health monitor batch");
      else workerLogger.info(data, "Deployment health monitor");
    },
  });
}
