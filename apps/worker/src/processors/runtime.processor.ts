/**
 * CloudPilot — Runtime Deployment Processor
 *
 * Processes a single runtime deployment job:
 *
 *   verify outbox -> verify build artifact -> claim runtime
 *   (fenced) -> resolve secrets -> verify runtime -> ensure
 *   network -> create -> persist identity -> start -> inspect
 *   -> readiness -> settle RUNNING
 *
 * Execution is tenant-scoped, idempotent, fenced by an
 * execution lease, and fail-closed. Untrusted workloads are
 * rejected. Container execution is disabled by default.
 */

import { randomUUID } from "node:crypto";

import { prisma } from "@cloudpilot/database";

import {
  RuntimeDeploymentError,
  TransientJobError,
  buildOwnershipLabels,
  generateContainerName,
  runtimeFailureMessage,
  getRuntimeFailureCode,
  type RuntimeFailureCode,
  type RuntimeJob,
  type QueueJob,
} from "@cloudpilot/queue";

import {
  UnrecoverableError,
  type Job,
} from "bullmq";

import { workerEnv } from "../config/env.js";
import { workerLogger } from "../config/logger.js";
import { retireRuntimeHealth, syncRuntimeHealthAfterStart } from "../health/runtime-health.js";
import { processRecoveryJob, withRuntimeLifecycleLease } from "../health/recovery-processor.js";
import { DeploymentLogWriter } from "../logging/log-writer.js";
import { emitSystemLog } from "../logging/system-logs.js";

import type { RuntimeExecutor } from "../runtime/runtime-executor.js";

import {
  assertLoopbackBind,
  assertValidContainerPort,
  assertValidImageId,
  assertValidRequestedHostPort,
  assertValidResourceLimits,
  generateNetworkName,
} from "../runtime/runtime-config.js";

import { resolveRuntimeEnvironment } from "../runtime/runtime-env.js";

import {
  buildReadinessUrl,
  probeTcp,
} from "../runtime/runtime-readiness.js";

// ==================================================
// Constants
// ==================================================

const RUNTIME_TERMINAL_DEPLOYMENT_STATUSES = new Set<string>([
  "FAILED",
  "CANCELLED",
  "SUCCEEDED",
  "STOPPED",
  "ROLLED_BACK",
  "RUNNING",
]);

// ==================================================
// Public contracts
// ==================================================

export interface RuntimeProcessorContext {
  signal: AbortSignal;
}

export interface ProcessRuntimeOptions {
  executor: RuntimeExecutor;
}

async function initializeHealthAfterStart(payload: RuntimeJob, readyAt: Date, executor: RuntimeExecutor, signal: AbortSignal) {
  if (!executor.inspectOwnedContainer) return;
  try {
    await syncRuntimeHealthAfterStart({
      ...runtimeScope(payload), runtimeInstanceId: payload.runtimeInstanceId,
    }, readyAt, executor.inspectOwnedContainer.bind(executor), signal);
  } catch {
    workerLogger.warn({ deploymentId: payload.deploymentId, runtimeInstanceId: payload.runtimeInstanceId },
      "Runtime health generation could not be initialized");
  }
}

export type ProcessRuntimeResultStatus =
  | "skipped"
  | "succeeded"
  | "failed"
  | "cancelled";

export interface ProcessRuntimeResult {
  status: ProcessRuntimeResultStatus;
}

// ==================================================
// Internal contracts
// ==================================================

interface RuntimeScope {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
}

interface ExecutionClaim {
  payload: RuntimeJob;
  runtimeInstanceId: string;
  leaseToken: string;
  runtimeAttempt: number;
  startedAt: Date;
}

interface BuildArtifact {
  buildId: string;
  imageId: string;
  imageTag: string | null;
}

type ExecutionState =
  | "active"
  | "cancelling"
  | "lost"
  | "unavailable";

type SettlementResult =
  | "succeeded"
  | "failed"
  | "cancelled"
  | "retry"
  | "stale";

// ==================================================
// Scopes
// ==================================================

function runtimeScope(payload: RuntimeJob): RuntimeScope {
  return {
    workspaceId: payload.workspaceId,
    projectId: payload.projectId,
    deploymentId: payload.deploymentId,
  };
}

function deploymentScope({ workspaceId, projectId, deploymentId }: RuntimeScope) {
  return { workspaceId, projectId, id: deploymentId };
}

function fencedRuntimeScope(claim: ExecutionClaim, now: Date) {
  return {
    ...runtimeScope(claim.payload),
    id: claim.runtimeInstanceId,
    executionLeaseToken: claim.leaseToken,
    runtimeAttempt: claim.runtimeAttempt,
    executionLeaseExpiresAt: { gt: now },
  };
}

// ==================================================
// Outbox validation
// ==================================================

async function validateRuntimeOutbox(
  job: Job<QueueJob>,
  payload: RuntimeJob,
): Promise<boolean> {
  if (payload.runtimeInstanceId !== payload.correlationId) return false;

  const event =
    await prisma.deploymentOutboxEvent.findFirst({
      where: {
        id: payload.correlationId,
        ...runtimeScope(payload),
        eventType: "RUNTIME_REQUESTED",
      },

      select: { id: true, jobId: true },
    });

  if (!event || event.jobId !== job.id) {
    return false;
  }

  return true;
}

// ==================================================
// Build artifact resolution
// ==================================================

async function resolveBuildArtifact(
  payload: RuntimeJob,
): Promise<BuildArtifact | null> {
  const build = await prisma.deploymentBuild.findFirst({
    where: {
      ...runtimeScope(payload),
      id: payload.buildId,
      status: "SUCCEEDED",
    },

    select: {
      id: true,
      imageId: true,
      imageTag: true,
    },
  });

  if (!build || !build.imageId) {
    return null;
  }

  return {
    buildId: build.id,
    imageId: build.imageId,
    imageTag: build.imageTag,
  };
}

// ==================================================
// Atomic runtime claim
// ==================================================

class RuntimeClaimConflict extends Error {}

export async function claimRuntime(
  payload: RuntimeJob,
): Promise<ExecutionClaim | null> {
  const now = new Date();
  const leaseToken = randomUUID();
  const leaseExpiresAt = new Date(now.getTime() + workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS);
  const queuedInstance = {
    ...runtimeScope(payload),
    id: payload.runtimeInstanceId,
    buildId: payload.buildId,
    status: "QUEUED",
    executionLeaseToken: null,
    executionLeaseExpiresAt: null,
    containerId: null,
    containerName: null,
  } as const;

  try {
    return await prisma.$transaction(async (tx) => {
      // Lock the deployment first, in the same order as runtime settlement.
      const transitioned = await tx.deployment.updateMany({
        where: {
          ...deploymentScope(runtimeScope(payload)),
          status: { in: ["BUILD_SUCCEEDED", "DEPLOYING"] },
          runtimeInstances: { some: queuedInstance },
        },
        data: { status: "DEPLOYING", updatedAt: now },
      });

      if (transitioned.count !== 1) return null;

      const [instance] = await tx.deploymentRuntimeInstance.updateManyAndReturn({
        where: queuedInstance,
        data: {
          status: "CREATING",
          executionLeaseToken: leaseToken,
          executionLeaseExpiresAt: leaseExpiresAt,
          startedAt: now,
          readyAt: null,
          stoppedAt: null,
          failureCode: null,
          failureMessage: null,
        },
        select: { id: true, runtimeAttempt: true },
      });

      // Recheck the instance under the write lock and roll back a lost claim.
      if (!instance) throw new RuntimeClaimConflict();

      await tx.auditLog.create({
        data: {
          workspaceId: payload.workspaceId,
          action: "DEPLOYMENT_RUNTIME_CLAIMED",
          entityType: "Deployment",
          entityId: payload.deploymentId,
          metadata: {
            runtimeInstanceId: instance.id,
            runtimeAttempt: instance.runtimeAttempt,
          },
        },
      });

      return {
        payload,
        runtimeInstanceId: instance.id,
        leaseToken,
        runtimeAttempt: instance.runtimeAttempt,
        startedAt: now,
      };
    });
  } catch (error) {
    if (error instanceof RuntimeClaimConflict) return null;
    throw error;
  }
}

// ==================================================
// Lease renewal
// ==================================================

async function renewRuntimeLease(
  claim: ExecutionClaim,
): Promise<ExecutionState> {
  const now = new Date();

  const expiresAt = new Date(
    now.getTime() +
      workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS,
  );

  const renewed =
    await prisma.deploymentRuntimeInstance.updateMany({
      where: fencedRuntimeScope(claim, now),

      data: { executionLeaseExpiresAt: expiresAt },
    });

  if (renewed.count === 1) {
    return "active";
  }

  const current =
    await prisma.deploymentRuntimeInstance.findFirst({
      where: {
        ...runtimeScope(claim.payload),
        id: claim.runtimeInstanceId,
      },

      select: {
        executionLeaseToken: true,
        runtimeAttempt: true,
        executionLeaseExpiresAt: true,
      },
    });

  if (
    !current ||
    current.executionLeaseToken !== claim.leaseToken ||
    current.runtimeAttempt !== claim.runtimeAttempt ||
    !current.executionLeaseExpiresAt ||
    current.executionLeaseExpiresAt.getTime() <= Date.now()
  ) {
    return "lost";
  }

  const deployment = await prisma.deployment.findFirst({
    where: deploymentScope(runtimeScope(claim.payload)),
    select: { status: true },
  });

  if (deployment?.status === "CANCELLING") {
    return "cancelling";
  }

  return "lost";
}

// ==================================================
// Execution monitor
// ==================================================

function createRuntimeMonitor(
  claim: ExecutionClaim,
  externalSignal: AbortSignal,
) {
  const controller = new AbortController();

  const signal = AbortSignal.any([
    externalSignal,
    controller.signal,
  ]);

  const intervalMs = Math.max(
    100,
    Math.min(
      2_000,
      Math.floor(
        workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS / 3,
      ),
    ),
  );

  let state: ExecutionState = "active";

  let stopped = false;

  let timer: ReturnType<typeof setTimeout> | undefined;

  let pending: Promise<void> | undefined;

  const abort = (next: ExecutionState): void => {
    state = next;

    if (!controller.signal.aborted) {
      controller.abort();
    }
  };

  const poll = async (): Promise<void> => {
    if (stopped || signal.aborted) {
      return;
    }

    try {
      const next = await renewRuntimeLease(claim);

      if (next !== "active") {
        abort(next);
      }
    } catch {
      abort("unavailable");
    }
  };

  const schedule = (): void => {
    if (stopped || signal.aborted) {
      return;
    }

    timer = setTimeout(() => {
      pending = poll().finally(() => {
        pending = undefined;
        schedule();
      });
    }, intervalMs);

    timer.unref();
  };

  schedule();

  return {
    signal,

    get state(): ExecutionState {
      return state;
    },

    shouldCancel(): boolean {
      return signal.aborted;
    },

    async stop(): Promise<void> {
      stopped = true;

      if (timer) {
        clearTimeout(timer);
        timer = undefined;
      }

      if (pending) {
        await pending;
      }
    },
  };
}

// ==================================================
// Container identity persistence
// ==================================================

async function persistContainerIdentity(
  claim: ExecutionClaim,
  container: { containerId: string; containerName: string; hostPort: number; networkName: string },
): Promise<boolean> {
  const now = new Date();

  const updated =
    await prisma.deploymentRuntimeInstance.updateMany({
      where: fencedRuntimeScope(claim, now),

      data: {
        containerId: container.containerId,
        containerName: container.containerName,
        hostPort: container.hostPort === 0 ? null : container.hostPort,
        networkName: container.networkName,
        status: "STARTING",
      },
    });

  return updated.count === 1;
}

// ==================================================
// Settlement
// ==================================================

async function settleSuccess(
  claim: ExecutionClaim,
  metadata: {
    containerId: string;
    containerName: string;
    hostPort: number;
    networkName: string;
    imageId: string;
    imageTag: string | null;
    readyAt: Date;
  },
): Promise<SettlementResult> {
  return prisma.$transaction(async (tx) => {
    const now = new Date();

    const fence = fencedRuntimeScope(claim, now);

    const scope = runtimeScope(claim.payload);

    const cancelled = await tx.deployment.updateMany({
      where: { ...deploymentScope(scope), status: "CANCELLING" },

      data: {
        status: "CANCELLED",
        failureCode: "CANCELLED",
        failureMessage:
          "Deployment cancellation was acknowledged by the runtime worker.",
        finishedAt: now,
      },
    });

    if (cancelled.count === 1) {
      await tx.deploymentRuntimeInstance.updateMany({
        where: fence,
        data: {
          status: "CANCELLED",
          executionLeaseToken: null,
          executionLeaseExpiresAt: null,
          stoppedAt: now,
        },
      });

      return "cancelled";
    }

    const updated =
      await tx.deploymentRuntimeInstance.updateMany({
        where: fence,

        data: {
          status: "RUNNING",
          imageId: metadata.imageId,
          imageTag: metadata.imageTag,
          containerId: metadata.containerId,
          containerName: metadata.containerName,
          hostPort: metadata.hostPort,
          networkName: metadata.networkName,
          hostBindingAddress: workerEnv.RUNTIME_BIND_HOST,
          cpuLimit: workerEnv.RUNTIME_CPU_LIMIT,
          memoryLimitMb: workerEnv.RUNTIME_MEMORY_LIMIT_MB,
          pidsLimit: workerEnv.RUNTIME_PIDS_LIMIT,
          readyAt: metadata.readyAt,
          containerPort: workerEnv.RUNTIME_CONTAINER_PORT,
          executionLeaseToken: null,
          executionLeaseExpiresAt: null,
          failureCode: null,
          failureMessage: null,
        },
      });

    if (updated.count !== 1) {
      return "stale";
    }

    // DEPLOYING -> HEALTH_CHECKING -> RUNNING
    const checking = await tx.deployment.updateMany({
      where: { ...deploymentScope(scope), status: "DEPLOYING" },
      data: { status: "HEALTH_CHECKING", updatedAt: now },
    });

    if (checking.count !== 1) {
      return "stale";
    }

    const running = await tx.deployment.updateMany({
      where: { ...deploymentScope(scope), status: "HEALTH_CHECKING" },
      data: {
        status: "RUNNING",
        failureCode: null,
        failureMessage: null,
        updatedAt: now,
      },
    });

    if (running.count !== 1) {
      return "stale";
    }

    await tx.auditLog.create({
      data: {
        workspaceId: claim.payload.workspaceId,

        action: "DEPLOYMENT_RUNTIME_SUCCEEDED",

        entityType: "Deployment",

        entityId: claim.payload.deploymentId,

        metadata: {
          runtimeInstanceId: claim.runtimeInstanceId,
          containerName: metadata.containerName,
          hostPort: metadata.hostPort,
        },
      },
    });

    return "succeeded";
  });
}

async function settleFailure(
  claim: ExecutionClaim,
  code: RuntimeFailureCode,
  retryable: boolean,
  containerName: string | null,
): Promise<SettlementResult> {
  return prisma.$transaction(async (tx) => {
    const now = new Date();

    const scope = runtimeScope(claim.payload);

    const fence = fencedRuntimeScope(claim, now);

    const cancelled = await tx.deployment.updateMany({
      where: { ...deploymentScope(scope), status: "CANCELLING" },

      data: {
        status: "CANCELLED",
        failureCode: "CANCELLED",
        failureMessage:
          "Deployment cancellation was acknowledged by the runtime worker.",
        finishedAt: now,
      },
    });

    if (cancelled.count === 1) {
      await tx.deploymentRuntimeInstance.updateMany({
        where: fence,
        data: {
          status: "CANCELLED",
          failureCode: "RUNTIME_CANCELLED",
          failureMessage: runtimeFailureMessage("RUNTIME_CANCELLED"),
          executionLeaseToken: null,
          executionLeaseExpiresAt: null,
          stoppedAt: now,
        },
      });

      return "cancelled";
    }

    const updated =
      await tx.deploymentRuntimeInstance.updateMany({
        where: fence,

        data: {
          status: retryable ? "QUEUED" : "FAILED",
          failureCode: retryable ? null : code,
          failureMessage: retryable
            ? null
            : runtimeFailureMessage(code),
          executionLeaseToken: null,
          executionLeaseExpiresAt: null,
          stoppedAt: retryable ? null : now,
        },
      });

    if (updated.count !== 1) {
      return "stale";
    }

    if (!retryable) {
      await tx.deployment.updateMany({
        where: { ...deploymentScope(scope), status: "DEPLOYING" },
        data: {
          status: "FAILED",
          failureCode: "RUNTIME_FAILED",
          failureMessage: runtimeFailureMessage(code),
          finishedAt: now,
        },
      });
    }

    await tx.auditLog.create({
      data: {
        workspaceId: claim.payload.workspaceId,

        action: retryable
          ? "DEPLOYMENT_RUNTIME_RETRY_SCHEDULED"
          : "DEPLOYMENT_RUNTIME_FAILED",

        entityType: "Deployment",

        entityId: claim.payload.deploymentId,

        metadata: {
          runtimeInstanceId: claim.runtimeInstanceId,
          failureCode: code,
          retryable,
          containerName,
        },
      },
    });

    return retryable ? "retry" : "failed";
  });
}

// ==================================================
// Retry budget
// ==================================================

function hasRuntimeRetryBudget(
  job: Job<QueueJob>,
): boolean {
  const queueAttemptsRemaining =
    job.attemptsMade + 1 <
    (job.opts.attempts ?? workerEnv.QUEUE_JOB_ATTEMPTS);

  const configuredRetries = workerEnv.RUNTIME_MAX_RETRIES;

  return queueAttemptsRemaining && configuredRetries >= 1;
}

// ==================================================
// Readiness wait
// ==================================================

async function waitForReadiness(
  host: string,
  port: number,
  timeoutMs: number,
  intervalMs: number,
  signal: AbortSignal,
): Promise<boolean> {
  const url = buildReadinessUrl({ host, port });

  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (signal.aborted) {
      return false;
    }

    const reachable = await probeTcp(
      host,
      port,
      Math.min(intervalMs, 2_000),
    );

    if (reachable) {
      return true;
    }

    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, intervalMs);
      timer.unref?.();
    });
  }

  void url;

  return false;
}

// ==================================================
// Main runtime processor
// ==================================================

export async function processRuntimeJob(
  job: Job<QueueJob>,
  payload: RuntimeJob,
  signal: AbortSignal,
  options: ProcessRuntimeOptions,
): Promise<ProcessRuntimeResult> {
  // ================================================
  // Lifecycle actions operate on an existing instance
  // ================================================

  if (payload.action === "recover") return processRecoveryJob(job.id, payload, options.executor, signal);
  if (payload.action !== "deploy") {
    return withRuntimeLifecycleLease(payload, signal, (operationSignal) =>
      processRuntimeLifecycle(payload, operationSignal, options));
  }

  // ================================================
  // Validate outbox event
  // ================================================

  const valid = await validateRuntimeOutbox(job, payload);

  if (!valid) {
    throw new UnrecoverableError(
      "Runtime queue event is invalid.",
    );
  }

  // ================================================
  // Fail closed when runtime is disabled
  // ================================================

  if (!workerEnv.RUNTIME_ENABLED) {
    throw new UnrecoverableError(
      "Container execution is disabled.",
    );
  }

  // ================================================
  // Skip terminal deployments
  // ================================================

  const existing = await prisma.deployment.findFirst({
    where: deploymentScope(runtimeScope(payload)),
    select: { status: true },
  });

  if (
    !existing ||
    RUNTIME_TERMINAL_DEPLOYMENT_STATUSES.has(existing.status)
  ) {
    return { status: "skipped" };
  }

  if (signal.aborted) {
    throw new TransientJobError(
      "Runtime worker is shutting down.",
    );
  }

  // ================================================
  // Resolve and verify the build artifact
  // ================================================

  const artifact = await resolveBuildArtifact(payload);

  if (!artifact) {
    throw new UnrecoverableError(
      "No verified image artifact is available.",
    );
  }

  assertValidImageId(artifact.imageId);

  const image = await options.executor.inspectImage(
    artifact.imageId,
  );

  if (!image.exists || image.imageId !== artifact.imageId) {
    throw new UnrecoverableError(
      "The verified image artifact is unavailable.",
    );
  }

  // ================================================
  // Claim the runtime instance (fenced)
  // ================================================

  const claimed = await claimRuntime(payload);

  if (!claimed) {
    const current = await prisma.deployment.findFirst({
      where: deploymentScope(runtimeScope(payload)),
      select: { status: true },
    });

    if (
      !current ||
      RUNTIME_TERMINAL_DEPLOYMENT_STATUSES.has(current.status)
    ) {
      return { status: "skipped" };
    }

    throw new TransientJobError(
      "Runtime execution ownership is unavailable.",
    );
  }

  const claim: ExecutionClaim = {
    payload,
    runtimeInstanceId: claimed.runtimeInstanceId,
    leaseToken: claimed.leaseToken,
    runtimeAttempt: claimed.runtimeAttempt,
    startedAt: claimed.startedAt,
  };

  const monitor = createRuntimeMonitor(claim, signal);

  let containerName: string | null = null;
  let containerCreationStarted = false;
  let stage = "runtime configuration";

  try {
    // ==============================================
    // Validate runtime configuration
    // ==============================================

    assertLoopbackBind(workerEnv.RUNTIME_BIND_HOST);

    assertValidContainerPort(workerEnv.RUNTIME_CONTAINER_PORT);

    assertValidResourceLimits({
      cpuLimit: workerEnv.RUNTIME_CPU_LIMIT,
      memoryLimitMb: workerEnv.RUNTIME_MEMORY_LIMIT_MB,
      pidsLimit: workerEnv.RUNTIME_PIDS_LIMIT,
    });

    const containerPort = workerEnv.RUNTIME_CONTAINER_PORT;

    containerName = generateContainerName({
      projectId: payload.projectId,
      deploymentId: payload.deploymentId,
      attempt: claim.runtimeAttempt,
    });

    // ==============================================
    // Resolve authorized runtime environment
    // ==============================================

    const deployment = await prisma.deployment.findFirst({
      where: deploymentScope(runtimeScope(payload)),
      select: { environment: true, project: { select: { repository: { select: { fullName: true } } } } },
    });

    if (!deployment) {
      throw new RuntimeDeploymentError(
        "INVALID_RUNTIME_CONFIGURATION",
        "Deployment is unavailable.",
      );
    }

    // Explicit host ports are honored only for authorized trusted
    // local deployments. Dynamic allocation (0) remains the default
    // and preserves existing behavior for every other deployment.
    assertValidRequestedHostPort(payload.requestedHostPort ?? 0);
    const trustedRepositories = workerEnv.BUILD_TRUSTED_REPOSITORIES.map(
      (name) => name.trim().toLowerCase(),
    );
    const repositoryName = deployment.project.repository?.fullName?.toLowerCase();
    const trustedLocal =
      workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY &&
      !!repositoryName &&
      trustedRepositories.includes(repositoryName);
    const requestedHostPort = trustedLocal
      ? payload.requestedHostPort ?? 0
      : 0;

    stage = "runtime environment resolution";
    let env: Record<string, string> = {};

    if (workerEnv.ENVIRONMENT_VARIABLE_ENCRYPTION_KEY) {
      const variables =
        await prisma.environmentVariable.findMany({
          where: {
            workspaceId: payload.workspaceId,
            projectId: payload.projectId,
            environment: deployment.environment,
          },

          select: { key: true, encryptedValue: true },
        });

      env = resolveRuntimeEnvironment(
        {
          workspaceId: payload.workspaceId,
          projectId: payload.projectId,
          environment: deployment.environment,
          encryptionKey:
            workerEnv.ENVIRONMENT_VARIABLE_ENCRYPTION_KEY,
        },
        variables,
      );
    }

    if (monitor.signal.aborted) {
      throw new RuntimeDeploymentError("RUNTIME_CANCELLED");
    }

    // ==============================================
    // Verify runtime capabilities
    // ==============================================

    stage = "Docker capability probe";
    const capabilities = await options.executor.probe();

    if (!capabilities.available) {
      throw new RuntimeDeploymentError(
        "RUNTIME_UNAVAILABLE",
        "No compatible container runtime is available.",
      );
    }

    // ==============================================
    // Prepare the managed network
    // ==============================================

    const networkName = generateNetworkName(
      workerEnv.RUNTIME_NETWORK_PREFIX,
      payload.projectId,
    );

    stage = "managed network verification";
    await options.executor.ensureNetwork(networkName, {
      "cloudpilot.managed": "true",
      "cloudpilot.projectId": payload.projectId,
      "cloudpilot.workspaceId": payload.workspaceId,
    });

    if (monitor.signal.aborted) {
      throw new RuntimeDeploymentError("RUNTIME_CANCELLED");
    }

    // ==============================================
    // Create the container
    // ==============================================

    containerCreationStarted = true;
    stage = "container creation";
    const createdContainer =
      await options.executor.createContainer({
        imageId: artifact.imageId,
        imageTag: artifact.imageTag ?? artifact.imageId,
        name: containerName,
        labels: buildOwnershipLabels({
          workspaceId: payload.workspaceId,
          projectId: payload.projectId,
          deploymentId: payload.deploymentId,
          runtimeInstanceId: claim.runtimeInstanceId,
          attempt: claim.runtimeAttempt,
        }),
        networkName,
        containerPort,
        hostPort: requestedHostPort,
        bindHost: workerEnv.RUNTIME_BIND_HOST,
        env,
        cpuLimit: workerEnv.RUNTIME_CPU_LIMIT,
        memoryLimitMb: workerEnv.RUNTIME_MEMORY_LIMIT_MB,
        pidsLimit: workerEnv.RUNTIME_PIDS_LIMIT,
        signal: monitor.signal,
      });

    if (monitor.signal.aborted) {
      throw new RuntimeDeploymentError("RUNTIME_CANCELLED");
    }

    stage = "container identity persistence";
    const persisted = await persistContainerIdentity(
      claim,
      createdContainer,
    );

    if (!persisted) {
      throw new RuntimeDeploymentError("RUNTIME_LEASE_LOST");
    }

    // ==============================================
    // Start the container
    // ==============================================

    stage = "container startup";
    await options.executor.startContainer(
      createdContainer.containerName,
      monitor.signal,
    );

    if (monitor.signal.aborted) {
      throw new RuntimeDeploymentError("RUNTIME_CANCELLED");
    }

    // ==============================================
    // Verify startup
    // ==============================================

    stage = "started container inspection";
    const inspection =
      await options.executor.inspectContainer(
        createdContainer.containerName,
      );

    if (
      !inspection.running ||
      inspection.containerId !== createdContainer.containerId ||
      inspection.imageId !== artifact.imageId ||
      inspection.hostPort === null
    ) {
      throw new RuntimeDeploymentError(
        "CONTAINER_START_FAILED",
        "The running container identity or loopback port could not be verified.",
      );
    }
    assertValidContainerPort(inspection.hostPort);

    // ==============================================
    // Startup readiness (loopback liveness)
    // ==============================================

    stage = "loopback readiness check";
    const ready = await waitForReadiness(
      workerEnv.RUNTIME_BIND_HOST,
      inspection.hostPort,
      workerEnv.RUNTIME_STARTUP_TIMEOUT_MS,
      workerEnv.RUNTIME_HEALTH_CHECK_INTERVAL_MS,
      monitor.signal,
    );

    if (monitor.signal.aborted) {
      throw new RuntimeDeploymentError("RUNTIME_CANCELLED");
    }

    if (!ready) {
      throw new RuntimeDeploymentError("READINESS_FAILED");
    }

    await monitor.stop();

    if (monitor.signal.aborted) {
      throw new RuntimeDeploymentError("RUNTIME_CANCELLED");
    }

    // ==============================================
    // Publish verified success
    // ==============================================

    stage = "runtime settlement";
    const readyAt = new Date();
    const settled = await settleSuccess(claim, {
      containerId: createdContainer.containerId,
      containerName: createdContainer.containerName,
      hostPort: inspection.hostPort,
      networkName: createdContainer.networkName,
      imageId: artifact.imageId,
      imageTag: artifact.imageTag,
      readyAt,
    });

    if (settled === "succeeded") {
      await initializeHealthAfterStart(payload, readyAt, options.executor, signal);
      workerLogger.info(
        {
          deploymentId: payload.deploymentId,
          runtimeInstanceId: claim.runtimeInstanceId,
          containerName: createdContainer.containerName,
          hostPort: inspection.hostPort,
        },
        "Container deployment succeeded",
      );

      return { status: "succeeded" };
    }

    if (settled === "cancelled") {
      return { status: "cancelled" };
    }

    throw new TransientJobError(
      "Runtime execution ownership was lost.",
    );
  } catch (error) {
    await monitor.stop();

    const runtimeError =
      error instanceof RuntimeDeploymentError
        ? error
        : new RuntimeDeploymentError(
            getRuntimeFailureCode(error),
            runtimeFailureMessage(
              getRuntimeFailureCode(error),
            ),
          );

    const diagnostic = {
      deploymentId: payload.deploymentId,
      runtimeInstanceId: claim.runtimeInstanceId,
      stage,
      failureCode: runtimeError.code,
    };
    if (runtimeError.code !== "RUNTIME_CANCELLED") {
      workerLogger.warn(diagnostic, "Container deployment operation failed");
    }

    const interrupted =
      signal.aborted ||
      monitor.state === "unavailable";

    const retryable =
      !containerCreationStarted &&
      (runtimeError.retryable || interrupted) &&
      hasRuntimeRetryBudget(job);

    let settled: SettlementResult;

    try {
      settled = await settleFailure(
        claim,
        runtimeError.code,
        retryable,
        containerName,
      );
    } catch {
      throw new TransientJobError(
        "Runtime settlement is unavailable.",
      );
    }

    if (settled === "cancelled") {
      return { status: "cancelled" };
    }

    if (settled === "failed" && workerEnv.DEPLOYMENT_LOGS_ENABLED) {
      try {
        const writer = new DeploymentLogWriter({
          ...runtimeScope(payload),
          runtimeInstanceId: claim.runtimeInstanceId,
          maxEntryBytes: workerEnv.LOG_MAX_ENTRY_BYTES,
        });
        // Only fixed stage names and normalized failure codes enter the log.
        emitSystemLog(writer, `Runtime failed during ${stage} (${runtimeError.code}).`, "ERROR");
        await writer.stop();
      } catch {
        workerLogger.warn(diagnostic, "Runtime failure log could not be persisted");
      }
    }

    if (settled === "retry") {
      throw new TransientJobError(
        `Container deployment failed: ${runtimeError.code}`,
      );
    }

    if (settled === "stale") {
      return { status: "skipped" };
    }

    throw new UnrecoverableError(
      `Container deployment failed: ${runtimeError.code}`,
    );
  } finally {
    await monitor.stop();
  }
}

// ==================================================
// Lifecycle operations (start/stop/restart)
// ==================================================

async function processRuntimeLifecycle(
  payload: RuntimeJob,
  signal: AbortSignal,
  options: ProcessRuntimeOptions,
): Promise<ProcessRuntimeResult> {
  if (!workerEnv.RUNTIME_ENABLED) {
    throw new UnrecoverableError(
      "Container execution is disabled.",
    );
  }

  const scope = runtimeScope(payload);

  const instance = await prisma.deploymentRuntimeInstance.findFirst({
    where: { ...scope, id: payload.runtimeInstanceId },

    select: {
      id: true,
      status: true,
      containerName: true,
    },
  });

  if (!instance) {
    return { status: "skipped" };
  }

  if (!instance.containerName) {
    throw new UnrecoverableError(
      "The runtime instance has no managed container.",
    );
  }

  const containerName = instance.containerName;

  if (signal.aborted) {
    throw new TransientJobError(
      "Runtime worker is shutting down.",
    );
  }

  const now = new Date();

  try {
    await retireRuntimeHealth({ ...scope, runtimeInstanceId: instance.id });
    if (payload.action === "stop") {
      await options.executor.stopContainer(
        containerName,
        workerEnv.RUNTIME_STOP_TIMEOUT_MS,
        signal,
      );

      await prisma.$transaction(async (tx) => {
        await tx.deploymentRuntimeInstance.updateMany({
          where: { ...scope, id: instance.id },
          data: {
            status: "STOPPED",
            stoppedAt: now,
            readyAt: null,
          },
        });

        await tx.deployment.updateMany({
          where: { ...deploymentScope(scope), status: "RUNNING" },
          data: { status: "STOPPED", updatedAt: now },
        });

        await tx.auditLog.create({
          data: {
            workspaceId: payload.workspaceId,
            action: "DEPLOYMENT_RUNTIME_STOPPED",
            entityType: "Deployment",
            entityId: payload.deploymentId,
            metadata: {
              runtimeInstanceId: instance.id,
              containerName,
            },
          },
        });
      });

      return { status: "succeeded" };
    }

    if (payload.action === "restart") {
      await options.executor.restartContainer(
        containerName,
        workerEnv.RUNTIME_STOP_TIMEOUT_MS,
        signal,
      );

      await prisma.$transaction(async (tx) => {
        await tx.deploymentRuntimeInstance.updateMany({
          where: { ...scope, id: instance.id },
          data: {
            status: "RUNNING",
            stoppedAt: null,
            readyAt: now,
            restartCount: { increment: 1 },
          },
        });

        await tx.auditLog.create({
          data: {
            workspaceId: payload.workspaceId,
            action: "DEPLOYMENT_RUNTIME_RESTARTED",
            entityType: "Deployment",
            entityId: payload.deploymentId,
            metadata: {
              runtimeInstanceId: instance.id,
              containerName,
            },
          },
        });
      });

      await initializeHealthAfterStart(payload, now, options.executor, signal);
      return { status: "succeeded" };
    }

    // start
    await options.executor.startContainer(
      containerName,
      signal,
    );

    await prisma.$transaction(async (tx) => {
      await tx.deploymentRuntimeInstance.updateMany({
        where: { ...scope, id: instance.id },
        data: {
          status: "RUNNING",
          stoppedAt: null,
          readyAt: now,
        },
      });

      await tx.deployment.updateMany({
        where: { ...deploymentScope(scope), status: "STOPPED" },
        data: { status: "RUNNING", updatedAt: now },
      });

      await tx.auditLog.create({
        data: {
          workspaceId: payload.workspaceId,
          action: "DEPLOYMENT_RUNTIME_STARTED",
          entityType: "Deployment",
          entityId: payload.deploymentId,
          metadata: {
            runtimeInstanceId: instance.id,
            containerName,
          },
        },
      });
    });

    await initializeHealthAfterStart(payload, now, options.executor, signal);
    return { status: "succeeded" };
  } catch (error) {
    const code = getRuntimeFailureCode(error);

    workerLogger.warn(
      {
        deploymentId: payload.deploymentId,
        runtimeInstanceId: instance.id,
        action: payload.action,
        code,
      },

      "Runtime lifecycle operation failed",
    );

    throw new UnrecoverableError(
      `Runtime lifecycle ${payload.action} failed: ${code}`,
    );
  }
}

// ==================================================
// Reconciliation
// ==================================================

/**
 * Conservatively reconcile abandoned runtime instances whose
 * execution lease has expired. Marks them FAILED so a new
 * attempt can be scheduled, and never deletes active or
 * referenced containers.
 */
export async function reconcileRuntimeInstances(): Promise<{
  recovered: number;
}> {
  const now = new Date();

  const expired =
    await prisma.deploymentRuntimeInstance.findMany({
      where: {
        status: { in: ["CREATING", "STARTING"] },
        executionLeaseExpiresAt: { lte: now },
      },

      select: {
        id: true,
        workspaceId: true,
        projectId: true,
        deploymentId: true,
        runtimeAttempt: true,
      },
    });

  let recovered = 0;

  for (const instance of expired) {
    const result = await prisma.$transaction(
      async (tx) => {
        const updated =
          await tx.deploymentRuntimeInstance.updateMany({
            where: {
              id: instance.id,
              status: { in: ["CREATING", "STARTING"] },
              executionLeaseExpiresAt: { lte: now },
            },

            data: {
              status: "FAILED",
              failureCode: "RUNTIME_LEASE_LOST",
              failureMessage: runtimeFailureMessage(
                "RUNTIME_LEASE_LOST",
              ),
              executionLeaseToken: null,
              executionLeaseExpiresAt: null,
              stoppedAt: now,
            },
          });

        if (updated.count !== 1) {
          return false;
        }

        await tx.deployment.updateMany({
          where: {
            workspaceId: instance.workspaceId,
            projectId: instance.projectId,
            id: instance.deploymentId,
            status: "DEPLOYING",
          },

          data: {
            status: "FAILED",
            failureCode: "RUNTIME_FAILED",
            failureMessage: runtimeFailureMessage(
              "RUNTIME_LEASE_LOST",
            ),
            finishedAt: now,
          },
        });

        return true;
      },
    );

    if (result) {
      recovered += 1;
    }
  }

  return { recovered };
}
