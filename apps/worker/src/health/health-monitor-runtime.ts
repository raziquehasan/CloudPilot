import { randomUUID } from "node:crypto";
import { Prisma, prisma, type HealthScope } from "@cloudpilot/database";
import { workerEnv } from "../config/env.js";
import { workerLogger } from "../config/logger.js";
import { startHealthMonitor } from "./health-monitor.js";
import type { HealthInspector } from "./probe-runtime.js";
import { healthPersistenceReady, runRuntimeHealthCheck } from "./runtime-health.js";

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

export function startDeploymentHealthMonitor(inspect: HealthInspector, signal: AbortSignal, deploymentId?: string) {
  const ownerId = randomUUID();
  return startHealthMonitor({
    enabled: workerEnv.HEALTH_MONITORING_ENABLED && workerEnv.HEALTH_PERSISTENCE_ENABLED &&
      workerEnv.RUNTIME_ENABLED && workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY,
    concurrency: workerEnv.HEALTH_MONITORING_CONCURRENCY,
    batchSize: workerEnv.HEALTH_MONITORING_BATCH_SIZE,
    signal,
    ready: healthPersistenceReady,
    discover: (limit) => discoverDueHealthChecks(limit, deploymentId),
    ...(deploymentId ? {} : { retain: pruneHealthHistory }),
    async check(scope, checkSignal) {
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
          if (event.previousStatus !== event.status) workerLogger.info(detail, "Deployment health state changed");
          workerLogger.debug(detail, event.failureReason ? "Deployment health check failed" : "Deployment health check passed");
        },
      });
      if (dockerError && !checkSignal.aborted) throw new Error("Docker health inspection failed");
    },
    emit(event, detail) {
      const data = { ownerId, deploymentId, event, ...detail };
      if (event === "infrastructure_error") workerLogger.warn(data, "Deployment health monitor infrastructure error");
      else workerLogger.info(data, "Deployment health monitor");
    },
  });
}
