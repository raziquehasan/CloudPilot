import { checkDatabaseHealth } from "@cloudpilot/database";
import {
  checkRedisHealth,
  listWorkerHeartbeats,
  type WorkerHeartbeat,
} from "@cloudpilot/queue";
import { prisma } from "@cloudpilot/database";
import { access, constants } from "node:fs/promises";
import { resolve } from "node:path";
import { env } from "../config/env.js";
import { logger } from "../config/logger.js";
import { deploymentOutboxRuntime } from "./deployment-outbox-runtime.js";
import { queueEnv, queueManager, redisProducerConnection } from "./queue-producer.js";

function workerSummary(workers: WorkerHeartbeat[]) {
  const availableWorkers = workers.filter((worker) => worker.available && !worker.draining);
  return {
    connected: workers.length > 0,
    available: availableWorkers.length > 0,
    count: workers.length,
    availableCount: availableWorkers.length,
    capabilities: {
      deploymentQueue: availableWorkers.some((worker) => worker.capabilities.deploymentQueue),
      sourceRetrieval: availableWorkers.some((worker) => worker.capabilities.sourceRetrieval),
      dockerBuild: availableWorkers.some((worker) => worker.capabilities.dockerBuild),
      containerExecution: availableWorkers.some((worker) => worker.capabilities.containerExecution),
    },
    lastHeartbeatAt: workers
      .map((worker) => worker.lastHeartbeatAt)
      .sort()
      .at(-1) ?? null,
  };
}

export async function hasAvailableDeploymentWorker(): Promise<boolean> {
  try {
    const redis = await checkRedisHealth(redisProducerConnection);
    if (!redis.healthy) return false;
    const workers = await listWorkerHeartbeats(redisProducerConnection, {
      prefix: queueEnv.QUEUE_PREFIX,
      ttlSeconds: queueEnv.WORKER_HEARTBEAT_TTL_SECONDS,
    }, "deployment");
    return workers.some((worker) => worker.available && !worker.draining && worker.capabilities.deploymentQueue);
  } catch {
    return false;
  }
}

export async function checkWorkspaceStorage(): Promise<boolean> {
  try {
    await access(resolve(env.SOURCE_WORKSPACE_ROOT), constants.W_OK);
    return true;
  } catch {
    return false;
  }
}

export async function getDeploymentReadiness(workspaceId: string, projectId: string) {
  let [database, redis] = await Promise.all([
    checkDatabaseHealth(),
    checkRedisHealth(redisProducerConnection),
  ]);
  let queueAvailable = false;
  let deploymentWorkers: WorkerHeartbeat[] = [];
  let diagnosticWorkers: WorkerHeartbeat[] = [];

  if (redis.healthy) {
    const queueResult = await Promise.allSettled([
      queueManager.getQueue("deployment").getJobCounts(),
      listWorkerHeartbeats(redisProducerConnection, {
        prefix: queueEnv.QUEUE_PREFIX,
        ttlSeconds: queueEnv.WORKER_HEARTBEAT_TTL_SECONDS,
      }, "deployment"),
      listWorkerHeartbeats(redisProducerConnection, {
        prefix: queueEnv.QUEUE_PREFIX,
        ttlSeconds: queueEnv.WORKER_HEARTBEAT_TTL_SECONDS,
      }, "diagnostic"),
    ]);
    queueAvailable = queueResult[0]?.status === "fulfilled";
    if (queueResult[0]?.status === "rejected") {
      logger.warn({ errorName: queueResult[0].reason instanceof Error ? queueResult[0].reason.name : "UnknownError" }, "Deployment queue readiness check failed");
    }
    if (queueResult[1]?.status === "fulfilled") deploymentWorkers = queueResult[1].value;
    else if (queueResult[1]?.status === "rejected") {
      logger.warn({ errorName: queueResult[1].reason instanceof Error ? queueResult[1].reason.name : "UnknownError" }, "Deployment worker heartbeat check failed");
    }
    if (queueResult[2]?.status === "fulfilled") diagnosticWorkers = queueResult[2].value;
    else if (queueResult[2]?.status === "rejected") {
      logger.warn({ errorName: queueResult[2].reason instanceof Error ? queueResult[2].reason.name : "UnknownError" }, "Diagnostic worker heartbeat check failed");
    }
  }

  let project: {
    repository: { fullName: string | null; selectedBranch: string | null; installation: { status: string } | null } | null;
    buildConfiguration: { framework: string | null; runtime: string | null; buildCommand: string | null } | null;
  } | null = null;
  let outboxStats: Array<{ status: string; _count: { _all: number } }> = [];
  if (database.healthy) {
    const [projectResult, outboxResult] = await Promise.allSettled([
      prisma.project.findFirst({
        where: { id: projectId, workspaceId },
        select: {
          repository: {
            select: {
              fullName: true,
              selectedBranch: true,
              installation: { select: { status: true } },
            },
          },
          buildConfiguration: {
            select: { framework: true, runtime: true, buildCommand: true },
          },
        },
      }),
      prisma.deploymentOutboxEvent.groupBy({
        by: ["status"],
        where: { workspaceId, projectId },
        _count: { _all: true },
      }),
    ]);
    if (projectResult.status === "fulfilled") project = projectResult.value;
    else {
      database = { healthy: false, latencyMs: database.latencyMs };
      logger.warn({ errorName: projectResult.reason instanceof Error ? projectResult.reason.name : "UnknownError" }, "Deployment readiness project query failed");
    }
    if (outboxResult.status === "fulfilled") outboxStats = outboxResult.value;
    else {
      database = { healthy: false, latencyMs: database.latencyMs };
      logger.warn({ errorName: outboxResult.reason instanceof Error ? outboxResult.reason.name : "UnknownError" }, "Deployment readiness outbox query failed");
    }
  }

  const repositoryConnected = Boolean(
    project?.repository?.fullName
      && project.repository.selectedBranch
      && project.repository.installation?.status === "ACTIVE",
  );
  const buildConfigured = Boolean(
    project?.buildConfiguration?.framework
      && project.buildConfiguration.runtime
      && project.buildConfiguration.buildCommand,
  );
  const deploymentWorker = workerSummary(deploymentWorkers);
  const diagnosticWorker = workerSummary(diagnosticWorkers);
  const outboxRuntime = deploymentOutboxRuntime.getStatus();
  const outboxPublisherAvailable = outboxRuntime.running
    && Boolean(outboxRuntime.lastSuccessfulRunAt)
    && Date.now() - new Date(outboxRuntime.lastSuccessfulRunAt!).getTime() < queueEnv.DEPLOYMENT_OUTBOX_PUBLISH_INTERVAL_MS * 3;
  const executionCapabilitiesAvailable = deploymentWorker.capabilities.sourceRetrieval
    && deploymentWorker.capabilities.dockerBuild
    && deploymentWorker.capabilities.containerExecution;
  const pendingOutboxEvents = outboxStats
    .filter((entry) => entry.status === "PENDING" || entry.status === "PUBLISHING")
    .reduce((count, entry) => count + entry._count._all, 0);
  const workspaceStorageAvailable = await checkWorkspaceStorage();
  const sourceRetrievalAvailable = deploymentWorker.capabilities.sourceRetrieval;
  const dockerBuildAvailable = deploymentWorker.capabilities.dockerBuild;
  const containerExecutionAvailable = deploymentWorker.capabilities.containerExecution;

  return {
    postgres: { available: database.healthy },
    redis: { available: redis.healthy },
    deploymentQueue: { available: queueAvailable },
    outboxPublisher: {
      available: outboxPublisherAvailable,
      running: outboxRuntime.running,
      pendingEvents: pendingOutboxEvents,
      lastSuccessfulRunAt: outboxRuntime.lastSuccessfulRunAt,
      lastErrorCode: outboxRuntime.lastErrorCode,
    },
    workers: {
      diagnostic: diagnosticWorker,
      deployment: deploymentWorker,
    },
    repository: { connected: repositoryConnected },
    commitResolution: { available: repositoryConnected && sourceRetrievalAvailable },
    sourceRetrieval: {
      available: sourceRetrievalAvailable && workspaceStorageAvailable,
      workerCapable: sourceRetrievalAvailable,
      workspaceStorage: workspaceStorageAvailable,
    },
    workspaceStorage: { available: workspaceStorageAvailable },
    buildConfiguration: { configured: buildConfigured },
    build: {
      // Image build capability is distinct from application
      // hosting readiness. A successful build does not imply
      // the application is deployed or live.
      workerCapable: dockerBuildAvailable,
      available: dockerBuildAvailable && workspaceStorageAvailable,
      producesImageArtifact: dockerBuildAvailable,
      containerDeployment: {
        available: containerExecutionAvailable,
        enabled: false,
      },
    },
    execution: {
      capabilitiesAvailable: executionCapabilitiesAvailable,
      available: database.healthy
        && redis.healthy
        && queueAvailable
        && outboxPublisherAvailable
        && deploymentWorker.available
        && executionCapabilitiesAvailable
        && repositoryConnected
        && buildConfigured,
    },
  };
}
