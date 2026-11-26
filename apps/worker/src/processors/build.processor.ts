/**
 * CloudPilot — Build Processor
 *
 * Processes a single deployment build job:
 *
 *   verify outbox + source -> claim build (fenced) -> execute
 *   -> verify artifact -> publish BUILD_SUCCEEDED
 *
 * Build execution is tenant-scoped, idempotent, fenced by an
 * execution lease, and fail-closed. Untrusted repository builds
 * are rejected unless an isolated builder is available.
 */

import { randomUUID } from "node:crypto";

import { prisma } from "@cloudpilot/database";

import {
  BuildError,
  TransientJobError,
  buildFailureMessage,
  getBuildFailureCode,
  isRetryableBuildFailure,
  type BuildFailureCode,
  type BuildJob,
  type QueueJob,
} from "@cloudpilot/queue";

import {
  UnrecoverableError,
  type Job,
} from "bullmq";

import { workerEnv } from "../config/env.js";
import { workerLogger } from "../config/logger.js";

import { WorkspaceManager } from "../source/workspace.js";

import { runBuild } from "../build/build-engine.js";

import type { BuildExecutor } from "../build/build-executor.js";



import type { BuildLogEntry } from "../build/build-logs.js";

// ==================================================
// Constants
// ==================================================

const SHA_PATTERN = /^[0-9a-f]{40}$/u;

const BUILD_TERMINAL_DEPLOYMENT_STATUSES = new Set<string>([
  "FAILED",
  "CANCELLED",
  "SUCCEEDED",
  "STOPPED",
  "ROLLED_BACK",
  "BUILD_SUCCEEDED",
]);

// ==================================================
// Public contracts
// ==================================================

export interface BuildProcessorContext {
  signal: AbortSignal;
}

export interface ProcessBuildOptions {
  executor: BuildExecutor;
  workspace: WorkspaceManager;
}

export type ProcessBuildResultStatus =
  | "skipped"
  | "succeeded"
  | "failed"
  | "cancelled";

export interface ProcessBuildResult {
  status: ProcessBuildResultStatus;
}

// ==================================================
// Internal contracts
// ==================================================

interface BuildScope {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
}

interface ExecutionClaim {
  payload: BuildJob;
  buildId: string;
  leaseToken: string;
  buildAttempt: number;
  startedAt: Date;
}

interface BuildSourceContext {
  sourceDir: string;
  dockerfilePath: string | null;
  buildContextPath: string | null;
  repositoryFullName: string;
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

function buildScope(payload: BuildJob): BuildScope {
  return {
    workspaceId: payload.workspaceId,
    projectId: payload.projectId,
    deploymentId: payload.deploymentId,
  };
}

function fencedBuildScope(claim: ExecutionClaim, now: Date) {
  return {
    ...buildScope(claim.payload),
    id: claim.buildId,
    executionLeaseToken: claim.leaseToken,
    buildAttempt: claim.buildAttempt,
    executionLeaseExpiresAt: { gt: now },
  };
}

// ==================================================
// Outbox validation
// ==================================================

async function validateBuildOutbox(
  job: Job<QueueJob>,
  payload: BuildJob,
): Promise<boolean> {
  const event =
    await prisma.deploymentOutboxEvent.findFirst({
      where: {
        id: payload.correlationId,
        ...buildScope(payload),
        eventType: "BUILD_REQUESTED",
      },

      select: { id: true, jobId: true },
    });

  if (!event || event.jobId !== job.id) {
    return false;
  }

  return true;
}

// ==================================================
// Atomic build claim
// ==================================================

async function claimBuild(
  payload: BuildJob,
  maxAttempts: number,
): Promise<ExecutionClaim | null> {
  const now = new Date();

  const leaseToken = randomUUID();

  const leaseExpiresAt = new Date(
    now.getTime() +
      workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS,
  );

  return prisma.$transaction(async (tx) => {
    // Transition the deployment into BUILDING first,
    // fenced by the absence of an active build lease.
    const transitioned =
      await tx.deployment.updateMany({
        where: {
          ...buildScope(payload),

          status: "SOURCE_PREPARED",
        },

        data: {
          status: "BUILDING",

          updatedAt: now,
        },
      });

    if (transitioned.count !== 1) {
      return null;
    }

    // Upsert the build record and claim it atomically.
    const build = await tx.deploymentBuild.upsert({
      where: {
        workspaceId_projectId_deploymentId_buildAttempt: {
          ...buildScope(payload),
          buildAttempt: 1,
        },
      },

      create: {
        ...buildScope(payload),

        sourcePreparationId:
          payload.sourcePreparationId,

        resolvedCommitSha: payload.resolvedCommitSha,

        buildAttempt: 1,

        status: "RUNNING",

        executionLeaseToken: leaseToken,

        executionLeaseExpiresAt: leaseExpiresAt,

        startedAt: now,
      },

      update: {
        status: "RUNNING",

        executionLeaseToken: leaseToken,

        executionLeaseExpiresAt: leaseExpiresAt,

        startedAt: now,

        completedAt: null,

        durationMs: null,

        failureCode: null,

        failureMessage: null,
      },
    });

    await tx.auditLog.create({
      data: {
        workspaceId: payload.workspaceId,

        action: "DEPLOYMENT_BUILD_CLAIMED",

        entityType: "Deployment",

        entityId: payload.deploymentId,

        metadata: {
          buildId: build.id,

          buildAttempt: build.buildAttempt,
        },
      },
    });

    return {
      payload,

      buildId: build.id,

      leaseToken,

      buildAttempt: build.buildAttempt,

      startedAt: now,
    };
  });
}

// ==================================================
// Lease renewal
// ==================================================

async function renewBuildLease(
  claim: ExecutionClaim,
): Promise<ExecutionState> {
  const now = new Date();

  const expiresAt = new Date(
    now.getTime() +
      workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS,
  );

  const renewed =
    await prisma.deploymentBuild.updateMany({
      where: fencedBuildScope(claim, now),

      data: { executionLeaseExpiresAt: expiresAt },
    });

  if (renewed.count === 1) {
    return "active";
  }

  const current = await prisma.deploymentBuild.findFirst({
    where: {
      ...buildScope(claim.payload),
      id: claim.buildId,
    },

    select: {
      executionLeaseToken: true,
      buildAttempt: true,
      executionLeaseExpiresAt: true,
    },
  });

  if (
    !current ||
    current.executionLeaseToken !== claim.leaseToken ||
    current.buildAttempt !== claim.buildAttempt ||
    !current.executionLeaseExpiresAt ||
    current.executionLeaseExpiresAt.getTime() <= Date.now()
  ) {
    return "lost";
  }

  const deployment = await prisma.deployment.findFirst({
    where: buildScope(claim.payload),
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

function createBuildMonitor(
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
      const next = await renewBuildLease(claim);

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
// Source resolution and authorization
// ==================================================

async function resolveBuildSource(
  payload: BuildJob,
  workspace: WorkspaceManager,
): Promise<BuildSourceContext> {
  const deployment = await prisma.deployment.findFirst({
    where: buildScope(payload),

    select: {
      commitSha: true,

      project: {
        select: {
          repository: {
            select: { fullName: true },
          },

          buildConfiguration: {
            select: {
              dockerfilePath: true,
              rootDirectory: true,
            },
          },
        },
      },
    },
  });

  if (
    !deployment ||
    deployment.commitSha !== payload.resolvedCommitSha
  ) {
    throw new BuildError(
      "SOURCE_INTEGRITY_FAILED",
      "Deployment source identity is invalid.",
    );
  }

  const repositoryFullName =
    deployment.project.repository?.fullName;

  if (!repositoryFullName) {
    throw new BuildError(
      "SOURCE_WORKSPACE_UNAVAILABLE",
      "Deployment repository is unavailable.",
    );
  }

  // Enforce trusted-repository policy for local builds.
  if (workerEnv.BUILD_TRUSTED_LOCAL_ONLY) {
    const trusted =
      workerEnv.BUILD_TRUSTED_REPOSITORIES.some(
        (entry) =>
          entry.toLowerCase() ===
          repositoryFullName.toLowerCase(),
      );

    if (!trusted) {
      throw new BuildError(
        "BUILD_POLICY_VIOLATION",
        "Repository is not authorized for local builds.",
      );
    }
  }

  const preparation = await prisma.deploymentSourcePreparation.findFirst({
    where: {
      ...buildScope(payload),
      status: "READY",
    },

    select: { sourceWorkspaceKey: true },
  });

  if (!preparation?.sourceWorkspaceKey) {
    throw new BuildError(
      "SOURCE_WORKSPACE_UNAVAILABLE",
      "Prepared source workspace is unavailable.",
    );
  }

  let sourceDir: string;

  try {
    sourceDir = await workspace.resolvePreparedSourceDir(
      payload.deploymentId,
      preparation.sourceWorkspaceKey,
    );
  } catch {
    throw new BuildError(
      "SOURCE_WORKSPACE_UNAVAILABLE",
      "Prepared source workspace could not be resolved.",
    );
  }

  return {
    sourceDir,
    dockerfilePath:
      deployment.project.buildConfiguration?.dockerfilePath ??
      null,
    buildContextPath:
      deployment.project.buildConfiguration?.rootDirectory ??
      null,
    repositoryFullName,
  };
}

// ==================================================
// Settlement
// ==================================================

async function settleSuccess(
  claim: ExecutionClaim,
  artifact: {
    imageTag: string;
    imageId: string | null;
    imageDigest: string | null;
    imageSizeBytes: number | null;
    platform: string;
    durationMs: number;
  },
  logs: BuildLogEntry[],
): Promise<SettlementResult> {
  return prisma.$transaction(async (tx) => {
    const now = new Date();

    const fence = fencedBuildScope(claim, now);

    // Cancellation takes precedence over success.
    const cancelled = await tx.deployment.updateMany({
      where: {
        ...buildScope(claim.payload),
        status: "CANCELLING",
      },

      data: {
        status: "CANCELLED",
        failureCode: "CANCELLED",
        failureMessage:
          "Deployment cancellation was acknowledged by the build worker.",
        finishedAt: now,
      },
    });

    if (cancelled.count === 1) {
      await tx.deploymentBuild.updateMany({
        where: fence,
        data: {
          status: "CANCELLED",
          executionLeaseToken: null,
          executionLeaseExpiresAt: null,
          completedAt: now,
        },
      });

      return "cancelled";
    }

    const updated = await tx.deploymentBuild.updateMany({
      where: fence,

      data: {
        status: "SUCCEEDED",
        imageTag: artifact.imageTag,
        imageId: artifact.imageId,
        imageDigest: artifact.imageDigest,
        artifactType: "DOCKER_IMAGE_STORE",
        artifactLocation: artifact.imageTag,
        platform: artifact.platform,
        imageSizeBytes:
          artifact.imageSizeBytes === null
            ? null
            : BigInt(artifact.imageSizeBytes),
        durationMs: artifact.durationMs,
        executionLeaseToken: null,
        executionLeaseExpiresAt: null,
        completedAt: now,
        failureCode: null,
        failureMessage: null,
      },
    });

    if (updated.count !== 1) {
      return "stale";
    }

    const transitioned = await tx.deployment.updateMany({
      where: {
        ...buildScope(claim.payload),
        status: "BUILDING",
      },

      data: {
        status: "BUILD_SUCCEEDED",
        updatedAt: now,
      },
    });

    if (transitioned.count !== 1) {
      return "stale";
    }

    if (logs.length > 0) {
      await tx.deploymentBuildLog.createMany({
        data: logs.map((entry) => ({
          ...buildScope(claim.payload),
          buildId: claim.buildId,
          sequence: BigInt(entry.sequence),
          stream: entry.stream,
          message: entry.message,
        })),
      });
    }

    await tx.auditLog.create({
      data: {
        workspaceId: claim.payload.workspaceId,

        action: "DEPLOYMENT_BUILD_SUCCEEDED",

        entityType: "Deployment",

        entityId: claim.payload.deploymentId,

        metadata: {
          buildId: claim.buildId,
          imageTag: artifact.imageTag,
          durationMs: artifact.durationMs,
        },
      },
    });

    return "succeeded";
  });
}

async function settleFailure(
  claim: ExecutionClaim,
  code: BuildFailureCode,
  retryable: boolean,
  logs: BuildLogEntry[],
): Promise<SettlementResult> {
  return prisma.$transaction(async (tx) => {
    const now = new Date();

    const scope = buildScope(claim.payload);

    const fence = fencedBuildScope(claim, now);

    const cancelled = await tx.deployment.updateMany({
      where: { ...scope, status: "CANCELLING" },

      data: {
        status: "CANCELLED",
        failureCode: "CANCELLED",
        failureMessage:
          "Deployment cancellation was acknowledged by the build worker.",
        finishedAt: now,
      },
    });

    if (cancelled.count === 1) {
      await tx.deploymentBuild.updateMany({
        where: fence,
        data: {
          status: "CANCELLED",
          failureCode: "BUILD_CANCELLED",
          failureMessage: buildFailureMessage("BUILD_CANCELLED"),
          executionLeaseToken: null,
          executionLeaseExpiresAt: null,
          completedAt: now,
        },
      });

      return "cancelled";
    }

    const updated = await tx.deploymentBuild.updateMany({
      where: fence,

      data: {
        status: retryable ? "QUEUED" : "FAILED",

        failureCode: retryable ? null : code,

        failureMessage: retryable
          ? null
          : buildFailureMessage(code),

        executionLeaseToken: null,

        executionLeaseExpiresAt: null,

        completedAt: retryable ? null : now,
      },
    });

    if (updated.count !== 1) {
      return "stale";
    }

    if (!retryable) {
      await tx.deployment.updateMany({
        where: { ...scope, status: "BUILDING" },

        data: {
          status: "FAILED",
          failureCode: "BUILD_FAILED",
          failureMessage: buildFailureMessage(code),
          finishedAt: now,
        },
      });
    }

    if (logs.length > 0) {
      await tx.deploymentBuildLog.createMany({
        data: logs.map((entry) => ({
          ...scope,
          buildId: claim.buildId,
          sequence: BigInt(entry.sequence),
          stream: entry.stream,
          message: entry.message,
        })),
      });
    }

    await tx.auditLog.create({
      data: {
        workspaceId: claim.payload.workspaceId,

        action: retryable
          ? "DEPLOYMENT_BUILD_RETRY_SCHEDULED"
          : "DEPLOYMENT_BUILD_FAILED",

        entityType: "Deployment",

        entityId: claim.payload.deploymentId,

        metadata: {
          buildId: claim.buildId,
          failureCode: code,
          retryable,
        },
      },
    });

    return retryable ? "retry" : "failed";
  });
}

// ==================================================
// Retry budget
// ==================================================

function hasBuildRetryBudget(
  job: Job<QueueJob>,
  buildAttempt: number,
): boolean {
  const queueAttemptsRemaining =
    job.attemptsMade + 1 <
    (job.opts.attempts ?? workerEnv.QUEUE_JOB_ATTEMPTS);

  const configuredRetries = workerEnv.BUILD_MAX_RETRIES;

  const retryBudgetRemaining =
    buildAttempt <= configuredRetries + 1;

  return queueAttemptsRemaining && retryBudgetRemaining;
}

// ==================================================
// Main build processor
// ==================================================

export async function processBuildJob(
  job: Job<QueueJob>,
  payload: BuildJob,
  signal: AbortSignal,
  options: ProcessBuildOptions,
): Promise<ProcessBuildResult> {
  // ================================================
  // Validate outbox event
  // ================================================

  const valid = await validateBuildOutbox(job, payload);

  if (!valid) {
    throw new UnrecoverableError(
      "Build queue event is invalid.",
    );
  }

  // ================================================
  // Fail closed when builds are disabled
  // ================================================

  if (!workerEnv.BUILD_ENABLED) {
    throw new UnrecoverableError(
      "Build execution is disabled.",
    );
  }

  if (!SHA_PATTERN.test(payload.resolvedCommitSha)) {
    throw new UnrecoverableError(
      "Build commit identity is invalid.",
    );
  }

  // ================================================
  // Skip terminal deployments
  // ================================================

  const existing = await prisma.deployment.findFirst({
    where: buildScope(payload),
    select: { status: true },
  });

  if (
    !existing ||
    BUILD_TERMINAL_DEPLOYMENT_STATUSES.has(
      existing.status,
    )
  ) {
    return { status: "skipped" };
  }

  if (signal.aborted) {
    throw new TransientJobError(
      "Build worker is shutting down.",
    );
  }

  // ================================================
  // Claim the build (fenced)
  // ================================================

  const claimed = await claimBuild(
    payload,
    workerEnv.QUEUE_JOB_ATTEMPTS,
  );

  if (!claimed) {
    const current = await prisma.deployment.findFirst({
      where: buildScope(payload),
      select: { status: true },
    });

    if (
      !current ||
      BUILD_TERMINAL_DEPLOYMENT_STATUSES.has(
        current.status,
      )
    ) {
      return { status: "skipped" };
    }

    throw new TransientJobError(
      "Build execution ownership is unavailable.",
    );
  }

  const claim: ExecutionClaim = {
    payload,
    buildId: claimed.buildId,
    leaseToken: claimed.leaseToken,
    buildAttempt: claimed.buildAttempt,
    startedAt: claimed.startedAt,
  };

  const monitor = createBuildMonitor(claim, signal);

  let imageTag: string | null = null;

  let logs: BuildLogEntry[] = [];

  try {
    // ==============================================
    // Resolve authorized source
    // ==============================================

    const source = await resolveBuildSource(
      payload,
      options.workspace,
    );

    if (monitor.signal.aborted) {
      throw new BuildError("BUILD_CANCELLED");
    }

    // ==============================================
    // Execute the isolated build
    // ==============================================

    const result = await runBuild({
      sourceDir: source.sourceDir,
      projectId: payload.projectId,
      deploymentId: payload.deploymentId,
      resolvedCommitSha: payload.resolvedCommitSha,
      buildAttempt: claim.buildAttempt,
      dockerfilePath: source.dockerfilePath,
      buildContextPath: source.buildContextPath,
      buildArgs: {},
      target: null,
      platform: workerEnv.BUILD_PLATFORM,
      limits: {
        timeoutMs: workerEnv.BUILD_TIMEOUT_MS,
        maxContextBytes:
          workerEnv.BUILD_MAX_CONTEXT_BYTES,
        maxFileCount: workerEnv.SOURCE_MAX_FILE_COUNT,
        maxLogBytes: workerEnv.BUILD_MAX_LOG_BYTES,
      },
      executor: options.executor,
      signal: monitor.signal,
    });

    imageTag = result.imageTag;
    logs = result.logEntries;

    if (monitor.signal.aborted) {
      throw new BuildError("BUILD_CANCELLED");
    }

    await monitor.stop();

    if (monitor.signal.aborted) {
      throw new BuildError("BUILD_CANCELLED");
    }

    // ==============================================
    // Publish verified success
    // ==============================================

    const settled = await settleSuccess(
      claim,
      {
        imageTag: result.imageTag,
        imageId: result.imageId,
        imageDigest: result.imageDigest,
        imageSizeBytes: result.imageSizeBytes,
        platform: result.platform,
        durationMs: result.durationMs,
      },
      result.logEntries,
    );

    if (settled === "succeeded") {
      workerLogger.info(
        {
          deploymentId: payload.deploymentId,
          buildId: claim.buildId,
          imageTag: result.imageTag,
          durationMs: result.durationMs,
        },
        "Deployment build succeeded",
      );

      return { status: "succeeded" };
    }

    if (settled === "cancelled") {
      workerLogger.warn({ deploymentId: payload.deploymentId, buildId: claim.buildId }, "Artifact cleanup deferred pending ownership verification");

      return { status: "cancelled" };
    }

    // stale: a newer execution owns the build.
    throw new TransientJobError(
      "Build execution ownership was lost.",
    );
  } catch (error) {
    await monitor.stop();

    const buildError =
      error instanceof BuildError
        ? error
        : new BuildError(
            getBuildFailureCode(error),
            buildFailureMessage(
              getBuildFailureCode(error),
            ),
          );

    if (monitor.state === "lost") {
      logs = [];
    }

    const interrupted =
      signal.aborted ||
      monitor.state === "unavailable";

    const retryable =
      (buildError.retryable || interrupted) &&
      hasBuildRetryBudget(job, claim.buildAttempt);

    let settled: SettlementResult;

    try {
      settled = await settleFailure(
        claim,
        buildError.code,
        retryable,
        logs,
      );
    } catch {
      throw new TransientJobError(
        "Build settlement is unavailable.",
      );
    }

    if (settled === "cancelled") {
      if (imageTag) {
        workerLogger.warn({ deploymentId: payload.deploymentId, buildId: claim.buildId }, "Artifact cleanup deferred pending ownership verification");
      }

      return { status: "cancelled" };
    }

    if (settled === "retry") {
      if (imageTag) {
        workerLogger.warn({ deploymentId: payload.deploymentId, buildId: claim.buildId }, "Artifact cleanup deferred pending ownership verification");
      }

      throw new TransientJobError(
        `Build failed: ${buildError.code}`,
      );
    }

    if (settled === "stale") {
      return { status: "skipped" };
    }

    if (imageTag) {
      workerLogger.warn({ deploymentId: payload.deploymentId, buildId: claim.buildId }, "Artifact cleanup deferred pending ownership verification");
    }

    throw new UnrecoverableError(
      `Build failed: ${buildError.code}`,
    );
  } finally {
    await monitor.stop();
  }
}