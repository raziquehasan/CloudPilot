import {
  Prisma, claimHealthCheck, prisma, releaseHealthCheck,
  type DeploymentHealthStatus, type HealthFailureReason, type HealthLease, type HealthScope,
} from "@cloudpilot/database";
import { workerEnv } from "../config/env.js";
import { isHealthBinding, isHealthPath } from "./http-health.js";
import { healthDefaults, transitionHealth, type HealthEvent } from "./health-state.js";
import {
  inspectHealthRuntime, matchesHealthRuntime, probeRuntime,
  type HealthInspector, type HealthRuntime, type RuntimeHealthObservation,
} from "./probe-runtime.js";

type RuntimeIdentity = Omit<HealthScope, "generation">;
type Database = Prisma.TransactionClient;
const healthMigration = "20261127140000_health_probe_failure_reasons";

export async function healthPersistenceReady(): Promise<boolean> {
  if (!workerEnv.RUNTIME_ENABLED || !workerEnv.HEALTH_PERSISTENCE_ENABLED) return false;
  const [tables] = await prisma.$queryRaw<Array<{ ready: boolean }>>`
    SELECT to_regclass('public._prisma_migrations') IS NOT NULL
      AND to_regclass('public.deployment_health_states') IS NOT NULL
      AND to_regclass('public.deployment_health_checks') IS NOT NULL AS ready
  `;
  if (!tables?.ready) return false;
  const [migration] = await prisma.$queryRaw<Array<{ ready: boolean }>>`
    SELECT count(DISTINCT migration_name) = 2 AS ready FROM "_prisma_migrations"
    WHERE migration_name IN ('20261127130000_deployment_health_domain', ${healthMigration})
      AND finished_at IS NOT NULL AND rolled_back_at IS NULL
  `;
  return migration?.ready === true;
}

function runtimeWhere(scope: RuntimeIdentity) {
  const { workspaceId, projectId, deploymentId, runtimeInstanceId } = scope;
  return { workspaceId, projectId, deploymentId, id: runtimeInstanceId };
}

function healthWhere(scope: RuntimeIdentity) {
  const { workspaceId, projectId, deploymentId, runtimeInstanceId } = scope;
  return { workspaceId, projectId, deploymentId, runtimeInstanceId };
}

function stateKey(scope: HealthScope) {
  return { workspaceId_projectId_deploymentId_runtimeInstanceId_generation: scope };
}

async function readRuntime(db: Database, scope: RuntimeIdentity) {
  return db.deploymentRuntimeInstance.findUnique({
    where: { workspaceId_projectId_deploymentId_id: runtimeWhere(scope) },
    select: {
      status: true, containerId: true, imageId: true, hostBindingAddress: true,
      hostPort: true, containerPort: true, runtimeAttempt: true, readyAt: true,
      deployment: { select: { status: true, project: { select: { repository: { select: { fullName: true } } } } } },
    },
  });
}

type RuntimeRecord = NonNullable<Awaited<ReturnType<typeof readRuntime>>>;

async function hasReplacement(db: Database, scope: RuntimeIdentity, runtimeAttempt: number): Promise<boolean> {
  const replacement = await db.deploymentRuntimeInstance.findFirst({
    where: {
      workspaceId: scope.workspaceId, projectId: scope.projectId, deploymentId: scope.deploymentId,
      runtimeAttempt: { gt: runtimeAttempt }, status: "RUNNING", readyAt: { not: null },
    },
    select: { id: true },
  });
  return replacement !== null;
}

function verifiedRuntime(runtime: RuntimeRecord | null): HealthRuntime | null {
  if (!runtime || !workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY || runtime.status !== "RUNNING" || runtime.deployment.status !== "RUNNING" ||
    !runtime.readyAt || !runtime.containerId || !runtime.imageId ||
    !/^[a-f0-9]{64}$/.test(runtime.containerId) || !/^sha256:[a-f0-9]{64}$/.test(runtime.imageId) ||
    !runtime.hostBindingAddress || runtime.hostPort === null ||
    !isHealthBinding(runtime.hostBindingAddress, runtime.hostPort)) return null;
  const repository = runtime.deployment.project.repository?.fullName?.toLowerCase();
  if (!repository || !workerEnv.BUILD_TRUSTED_REPOSITORIES.some((item) => item.toLowerCase() === repository)) return null;
  return {
    runtimeAttempt: runtime.runtimeAttempt, containerId: runtime.containerId, imageId: runtime.imageId,
    host: runtime.hostBindingAddress, port: runtime.hostPort,
    containerPort: runtime.containerPort ?? workerEnv.RUNTIME_CONTAINER_PORT,
  };
}

async function lockRuntime(db: Database, scope: RuntimeIdentity, wholeDeployment = false) {
  await db.$queryRaw`
    SELECT "id" FROM "deployment_runtime_instances"
    WHERE "workspaceId" = ${scope.workspaceId}::uuid AND "projectId" = ${scope.projectId}::uuid
      AND "deploymentId" = ${scope.deploymentId}::uuid
      AND (${wholeDeployment} OR "id" = ${scope.runtimeInstanceId}::uuid)
    ORDER BY "id" FOR UPDATE
  `;
}

async function databaseNow(db: Database): Promise<Date> {
  const [row] = await db.$queryRaw<Array<{ now: Date }>>`
    SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now
  `;
  if (!row) throw new Error("Database clock unavailable");
  return row.now;
}

const retired = { nextCheckAt: null, leaseOwnerId: null, leaseToken: null, leaseExpiresAt: null };

export async function retireRuntimeHealth(scope: RuntimeIdentity): Promise<void> {
  if (!await healthPersistenceReady()) return;
  await prisma.$transaction(async (tx) => {
    await lockRuntime(tx, scope);
    await tx.deploymentHealthState.updateMany({
      where: { ...healthWhere(scope), nextCheckAt: { not: null } }, data: retired,
    });
  });
}

async function initializeRuntimeHealth(
  scope: RuntimeIdentity, readyAt: Date, containerId: string, hostPort: number,
): Promise<HealthScope | null> {
  return prisma.$transaction(async (tx) => {
    await lockRuntime(tx, scope, true);
    const row = await readRuntime(tx, scope);
    const runtime = verifiedRuntime(row);
    if (!runtime || row?.readyAt?.getTime() !== readyAt.getTime() ||
        runtime.containerId !== containerId || runtime.port !== hostPort) return null;
    if (await hasReplacement(tx, scope, runtime.runtimeAttempt)) return null;

    const latest = await tx.deploymentHealthState.findFirst({
      where: healthWhere(scope), orderBy: { generation: "desc" },
    });
    if (latest?.nextCheckAt) {
      return { ...healthWhere(scope), generation: latest.generation };
    }

    const { runtimeInstanceId: _runtimeInstanceId, ...deployment } = healthWhere(scope);
    await tx.deploymentHealthState.updateMany({
      where: { ...deployment, nextCheckAt: { not: null } }, data: retired,
    });
    const next = { ...healthWhere(scope), generation: (latest?.generation ?? -1) + 1 };
    const now = await databaseNow(tx);
    await tx.deploymentHealthState.create({
      data: { ...next, status: "STARTING", nextCheckAt: now, createdAt: now },
    });
    return next;
  });
}

export async function syncRuntimeHealthAfterStart(
  scope: RuntimeIdentity, readyAt: Date, inspect: HealthInspector, signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted || !await healthPersistenceReady()) return;
  const row = await readRuntime(prisma, scope);
  const runtime = verifiedRuntime(row);
  if (!runtime || row?.readyAt?.getTime() !== readyAt.getTime()) return;
  const snapshot = await inspectHealthRuntime(scope, runtime, inspect, signal);
  if (!signal?.aborted && matchesHealthRuntime(snapshot, runtime)) {
    await initializeRuntimeHealth(scope, readyAt, runtime.containerId, runtime.port);
  }
}

async function persistObservation(
  scope: HealthScope, runtime: HealthRuntime, readyAt: Date, version: number,
  lease: HealthLease, observation: RuntimeHealthObservation, signal?: AbortSignal,
): Promise<DeploymentHealthStatus | null> {
  return prisma.$transaction(async (tx) => {
    await lockRuntime(tx, scope);
    const row = await readRuntime(tx, scope);
    const current = verifiedRuntime(row);
    if (!current || current.containerId !== runtime.containerId || current.imageId !== runtime.imageId ||
      current.port !== runtime.port || current.host !== runtime.host || current.containerPort !== runtime.containerPort ||
      current.runtimeAttempt !== runtime.runtimeAttempt || row?.readyAt?.getTime() !== readyAt.getTime()) return null;
    if (await hasReplacement(tx, scope, runtime.runtimeAttempt)) return null;

    const key = stateKey(scope);
    const state = await tx.deploymentHealthState.findUnique({ where: key });
    if (!state || !state.nextCheckAt || state.version !== version || signal?.aborted) return null;
    const event: HealthEvent = observation.failureReason
      ? { scope, kind: "CHECK_FAILED", reason: observation.failureReason }
      : { scope, kind: "CHECK_PASSED" };
    const outcome = transitionHealth({ ...state, scope }, event);
    if (!outcome.applied) return null;

    // Lock the health row after its runtime. Compare time after acquiring both locks.
    await tx.$queryRaw`
      SELECT 1 FROM "deployment_health_states"
      WHERE "workspaceId" = ${scope.workspaceId}::uuid AND "projectId" = ${scope.projectId}::uuid
        AND "deploymentId" = ${scope.deploymentId}::uuid AND "runtimeInstanceId" = ${scope.runtimeInstanceId}::uuid
        AND "generation" = ${scope.generation}::integer FOR UPDATE
    `;
    const now = await databaseNow(tx);
    if (signal?.aborted) return null;
    const { status, consecutiveFailures, consecutiveSuccesses, lastFailureReason } = outcome.state;
    const updated = await tx.deploymentHealthState.updateMany({
      where: {
        ...scope, version, nextCheckAt: { not: null },
        leaseOwnerId: lease.ownerId, leaseToken: lease.token, leaseExpiresAt: { gt: now },
      },
      data: {
        ...retired, status, consecutiveFailures, consecutiveSuccesses, lastFailureReason,
        version: { increment: 1 }, lastCheckedAt: now,
        lastHealthyAt: status === "HEALTHY" ? now : state.lastHealthyAt,
        nextCheckAt: new Date(now.getTime() + healthDefaults.checkIntervalMs),
      },
    });
    if (updated.count !== 1) return null;
    const { dockerResult, httpResult, responseLatencyMs, httpStatus, failureReason } = observation;
    await tx.deploymentHealthCheck.create({
      data: {
        ...scope, dockerResult, dockerState: observation.docker?.state ?? null,
        httpResult, responseLatencyMs, httpStatus, failureReason, checkedAt: now,
      },
    });
    return status;
  });
}

export type RuntimeHealthCheckEvent =
  | { kind: "lease_conflict" | "stale" }
  | { kind: "docker_error"; phase: "initial_inspection" | "revalidation" }
  | { kind: "recorded"; status: DeploymentHealthStatus; previousStatus: DeploymentHealthStatus;
      failureReason: HealthFailureReason | null; httpStatus: number | null };

export async function runRuntimeHealthCheck(
  scope: HealthScope, ownerId: string, inspect: HealthInspector,
  options: { path?: string; signal?: AbortSignal; onEvent?: (event: RuntimeHealthCheckEvent) => void } = {},
): Promise<boolean> {
  if (options.path !== undefined && !isHealthPath(options.path)) throw new RangeError("Invalid health path");
  if (options.signal?.aborted || !await healthPersistenceReady()) return false;
  const row = await readRuntime(prisma, scope);
  const runtime = verifiedRuntime(row);
  const stale = () => {
    if (!options.signal?.aborted) options.onEvent?.({ kind: "stale" });
    return false;
  };
  if (!runtime || !row?.readyAt) return stale();
  if (await hasReplacement(prisma, scope, runtime.runtimeAttempt)) return stale();
  // Start the local probe deadline before the claim. Even if the database round trip
  // stalls, Docker/HTTP work cannot outlive this 90-second lease.
  const deadline = AbortSignal.timeout(80_000);
  const signal = options.signal ? AbortSignal.any([options.signal, deadline]) : deadline;
  const lease = await claimHealthCheck(prisma, scope, ownerId, 90_000);
  if (!lease) {
    if (!options.signal?.aborted) options.onEvent?.({ kind: "lease_conflict" });
    return false;
  }
  let persisted = false;
  try {
    const state = await prisma.deploymentHealthState.findUnique({
      where: stateKey(scope), select: { version: true, status: true },
    });
    if (!state || signal.aborted) return stale();
    const observation = await probeRuntime(scope, runtime, inspect, options.path, signal,
      (phase) => options.onEvent?.({ kind: "docker_error", phase }));
    if (!observation) return stale();
    const status = await persistObservation(scope, runtime, row.readyAt, state.version, lease, observation, signal);
    if (!status) return stale();
    persisted = true;
    options.onEvent?.({
      kind: "recorded", status, previousStatus: state.status,
      failureReason: observation.failureReason, httpStatus: observation.httpStatus,
    });
    return true;
  } finally {
    if (!persisted) await releaseHealthCheck(prisma, scope, lease);
  }
}
