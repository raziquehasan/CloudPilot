import { randomUUID } from "node:crypto";

import { prisma } from "@cloudpilot/database";

import type {
  DeploymentJob,
  QueueJob,
  SourceFailureCode,
} from "@cloudpilot/queue";

import {
  SourcePreparationError,
  TransientJobError,
  QueueManager,
} from "@cloudpilot/queue";

import {
  UnrecoverableError,
  type Job,
} from "bullmq";

import { workerEnv } from "../config/env.js";
import { workerLogger } from "../config/logger.js";

import { WorkerGitHubAuth } from "../source/github-auth.js";
import { WorkspaceManager } from "../source/workspace.js";
import { prepareSource } from "../source/prepare.js";

// ==================================================
// Constants
// ==================================================

const SHA_PATTERN = /^[0-9a-f]{40}$/u;

const TERMINAL_STATUSES = new Set<string>([
  "CANCELLED",
  "FAILED",
  "SUCCEEDED",
  "STOPPED",
  "ROLLED_BACK",
  "SOURCE_PREPARED",
]);

// ==================================================
// Public contracts
// ==================================================

export interface DeploymentProcessorContext {
  signal: AbortSignal;
  leaseToken: string;
  executionAttempt: number;
  isCancellationRequested(): Promise<boolean>;
}

export interface PreparedSourceResult {
  attemptId: string;
  downloadedBytes: number;
  extractedBytes: number;
  fileCount: number;
}

export interface ProcessDeploymentOptions {
  auth: WorkerGitHubAuth | null;
  workspace: WorkspaceManager;
  fetchImpl?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
}

export interface ProcessDeploymentResult {
  status:
    | "skipped"
    | "failed"
    | "cancelled"
    | "prepared";
}

// ==================================================
// Internal contracts
// ==================================================

interface ClaimedDeployment {
  id: string;
  workspaceId: string;
  executionAttempt: number;
  executionLeaseToken: string;
}

interface SourceIdentity {
  repositoryFullName: string;
  requestedBranch: string | null;
  resolvedCommitSha: string;
}

interface ExecutionClaim {
  payload: DeploymentJob;
  leaseToken: string;
  executionAttempt: number;
  startedAt: Date;
}

interface FailureSettlement {
  claim: ExecutionClaim;
  identity: SourceIdentity | null;
  code: SourceFailureCode;
  retryable: boolean;
}

type SettlementResult =
  | "failed"
  | "cancelled"
  | "retry"
  | "stale";

type ExecutionState =
  | "active"
  | "cancelling"
  | "lost"
  | "unavailable";

// ==================================================
// Worker dependencies
// ==================================================

export function createWorkerGitHubAuth():
  WorkerGitHubAuth | null {
  if (
    !workerEnv.GITHUB_APP_ID ||
    !workerEnv.GITHUB_APP_PRIVATE_KEY_PATH
  ) {
    return null;
  }

  return new WorkerGitHubAuth({
    appId: workerEnv.GITHUB_APP_ID,
    privateKeyPath:
      workerEnv.GITHUB_APP_PRIVATE_KEY_PATH,
  });
}

export function createWorkspaceManager():
  WorkspaceManager {
  return new WorkspaceManager({
    root: workerEnv.SOURCE_WORKSPACE_ROOT,

    ttlSeconds:
      workerEnv.SOURCE_WORKSPACE_TTL_SECONDS,

    minFreeBytes:
      workerEnv.SOURCE_MIN_FREE_BYTES,
  });
}

// ==================================================
// Shared database filters
// ==================================================

function deploymentScope(
  payload: DeploymentJob,
) {
  return {
    id: payload.deploymentId,
    workspaceId: payload.workspaceId,
    projectId: payload.projectId,
  };
}

function sourceScope(
  payload: DeploymentJob,
) {
  return {
    workspaceId: payload.workspaceId,
    projectId: payload.projectId,
    deploymentId: payload.deploymentId,
  };
}

function fencedScope(
  claim: ExecutionClaim,
  now: Date,
) {
  return {
    ...deploymentScope(claim.payload),

    executionLeaseToken: claim.leaseToken,

    executionAttempt: claim.executionAttempt,

    executionLeaseExpiresAt: {
      gt: now,
    },
  };
}

// ==================================================
// Sanitized failure messages
// ==================================================

function failureMessage(
  code: SourceFailureCode,
): string {
  switch (code) {
    case "REPOSITORY_ACCESS_DENIED":
      return "Repository access is unavailable.";

    case "INSTALLATION_UNAVAILABLE":
      return "GitHub installation is unavailable.";

    case "INSTALLATION_SUSPENDED":
      return "GitHub installation is suspended.";

    case "COMMIT_NOT_FOUND":
      return "The requested immutable commit is unavailable.";

    case "GITHUB_RATE_LIMITED":
      return "GitHub API rate limit was reached.";

    case "GITHUB_UNAVAILABLE":
      return "GitHub is temporarily unavailable.";

    case "DOWNLOAD_TIMEOUT":
      return "Source archive download timed out.";

    case "DOWNLOAD_INTERRUPTED":
      return "Source archive download was interrupted.";

    case "ARCHIVE_SIZE_EXCEEDED":
      return "Source archive exceeds its download size limit.";

    case "EXTRACTION_SIZE_EXCEEDED":
      return "Extracted source exceeds its size limit.";

    case "FILE_COUNT_EXCEEDED":
      return "Source archive exceeds its entry count limit.";

    case "FILE_SIZE_EXCEEDED":
      return "A source file exceeds its size limit.";

    case "UNSAFE_ARCHIVE":
      return "Source archive contains unsafe content.";

    case "MALFORMED_ARCHIVE":
      return "Source archive failed integrity validation.";

    case "INSUFFICIENT_DISK_SPACE":
      return "Insufficient storage for source preparation.";

    case "WORKSPACE_UNAVAILABLE":
      return "Source workspace is unavailable.";

    case "SOURCE_PREPARATION_CANCELLED":
      return "Source preparation was interrupted.";

    case "EXECUTION_LEASE_LOST":
      return "Deployment execution ownership was lost.";

    default:
      return "Source preparation failed unexpectedly.";
  }
}

// ==================================================
// Atomic deployment claim
// ==================================================

async function claimDeployment(
  payload: DeploymentJob,
  maxAttempts: number,
): Promise<ClaimedDeployment | null> {
  const now = new Date();

  const leaseToken = randomUUID();

  const leaseExpiresAt = new Date(
    now.getTime() +
      workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS,
  );

  return prisma.$transaction(async (tx) => {
    const rows =
      await tx.$queryRaw<ClaimedDeployment[]>`
        UPDATE "deployments"

        SET
          "status" =
            'PREPARING'::"DeploymentStatus",

          "executionLeaseToken" =
            ${leaseToken}::uuid,

          "executionLeaseExpiresAt" =
            ${leaseExpiresAt},

          "executionAttempt" =
            "executionAttempt" + 1,

          "startedAt" =
            COALESCE("startedAt", ${now}),

          "updatedAt" = ${now}

        WHERE
          "id" = ${payload.deploymentId}::uuid

          AND "workspaceId" =
            ${payload.workspaceId}::uuid

          AND "projectId" =
            ${payload.projectId}::uuid

          AND "executionAttempt" < ${maxAttempts}

          AND (
            "status" IN (
              'QUEUED'::"DeploymentStatus",
              'PENDING'::"DeploymentStatus",
              'PREPARING'::"DeploymentStatus"
            )
          )

          AND (
            "executionLeaseToken" IS NULL

            OR "executionLeaseExpiresAt" <= ${now}
          )

        RETURNING
          "id",
          "workspaceId",
          "executionAttempt",
          "executionLeaseToken"
      `;

    const claimed = rows[0] ?? null;

    if (!claimed) {
      return null;
    }

    await tx.auditLog.create({
      data: {
        workspaceId: payload.workspaceId,

        action: "DEPLOYMENT_EXECUTION_CLAIMED",

        entityType: "Deployment",

        entityId: payload.deploymentId,

        metadata: {
          attempt: claimed.executionAttempt,
        },
      },
    });

    return claimed;
  });
}

// ==================================================
// Expired execution recovery
// ==================================================

async function failExpiredAttempt(
  payload: DeploymentJob,
  maxAttempts: number,
): Promise<boolean> {
  return prisma.$transaction(async (tx) => {
    const now = new Date();

    const updated =
      await tx.deployment.updateMany({
        where: {
          ...deploymentScope(payload),

          status: {
            in: [
              "PREPARING",
              "QUEUED",
              "PENDING",
            ],
          },

          executionAttempt: {
            gte: maxAttempts,
          },

          OR: [
            {
              executionLeaseToken: null,
            },
            {
              executionLeaseExpiresAt: {
                lte: now,
              },
            },
          ],
        },

        data: {
          status: "FAILED",

          failureCode: "UNKNOWN",

          failureMessage:
            "Deployment source preparation exhausted its execution attempts.",

          executionLeaseToken: null,

          executionLeaseExpiresAt: null,

          finishedAt: now,
        },
      });

    if (updated.count !== 1) {
      return false;
    }

    await tx.deploymentSourcePreparation.updateMany({
      where: {
        ...sourceScope(payload),

        status: {
          in: [
            "PENDING",
            "DOWNLOADING",
            "EXTRACTING",
            "FAILED",
          ],
        },
      },

      data: {
        status: "FAILED",

        failureCode: "EXECUTION_LEASE_LOST",

        sourceWorkspaceKey: null,

        expiresAt: null,

        completedAt: now,
      },
    });

    await tx.auditLog.create({
      data: {
        workspaceId: payload.workspaceId,

        action:
          "DEPLOYMENT_EXECUTION_LEASE_EXPIRED",

        entityType: "Deployment",

        entityId: payload.deploymentId,

        metadata: {
          attemptLimitReached: true,
        },
      },
    });

    return true;
  });
}

// ==================================================
// Recover cancellation without an active lease
// ==================================================

async function recoverCancellation(
  payload: DeploymentJob,
): Promise<boolean> {
  return prisma.$transaction(async (tx) => {
    const now = new Date();

    const updated =
      await tx.deployment.updateMany({
        where: {
          ...deploymentScope(payload),

          status: "CANCELLING",

          OR: [
            {
              executionLeaseToken: null,
            },
            {
              executionLeaseExpiresAt: {
                lte: now,
              },
            },
          ],
        },

        data: {
          status: "CANCELLED",

          failureCode: "CANCELLED",

          failureMessage:
            "Deployment cancellation was acknowledged.",

          executionLeaseToken: null,

          executionLeaseExpiresAt: null,

          finishedAt: now,
        },
      });

    if (updated.count !== 1) {
      return false;
    }

    await tx.deploymentSourcePreparation.updateMany({
      where: {
        ...sourceScope(payload),

        status: {
          in: [
            "PENDING",
            "DOWNLOADING",
            "EXTRACTING",
            "FAILED",
          ],
        },
      },

      data: {
        status: "CANCELLED",

        failureCode:
          "SOURCE_PREPARATION_CANCELLED",

        sourceWorkspaceKey: null,

        expiresAt: null,

        completedAt: now,
      },
    });

    await tx.auditLog.create({
      data: {
        workspaceId: payload.workspaceId,

        action:
          "DEPLOYMENT_CANCELLATION_ACKNOWLEDGED",

        entityType: "Deployment",

        entityId: payload.deploymentId,

        metadata: {
          source: "worker-recovery",
        },
      },
    });

    return true;
  });
}

// ==================================================
// Execution lease renewal
// ==================================================

async function renewExecutionLease(
  claim: ExecutionClaim,
): Promise<ExecutionState> {
  const now = new Date();

  const expiresAt = new Date(
    now.getTime() +
      workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS,
  );

  const renewed =
    await prisma.deployment.updateMany({
      where: {
        ...fencedScope(claim, now),

        status: "PREPARING",
      },

      data: {
        executionLeaseExpiresAt: expiresAt,
      },
    });

  if (renewed.count === 1) {
    return "active";
  }

  const current =
    await prisma.deployment.findFirst({
      where: deploymentScope(claim.payload),

      select: {
        status: true,

        executionLeaseToken: true,

        executionLeaseExpiresAt: true,

        executionAttempt: true,
      },
    });

  if (
    !current ||
    current.executionLeaseToken !==
      claim.leaseToken ||
    current.executionAttempt !==
      claim.executionAttempt ||
    !current.executionLeaseExpiresAt ||
    current.executionLeaseExpiresAt.getTime() <=
      Date.now()
  ) {
    return "lost";
  }

  if (current.status === "CANCELLING") {
    return "cancelling";
  }

  return "lost";
}

// ==================================================
// Cancellation and lease heartbeat
// ==================================================

function createExecutionMonitor(
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
        workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS /
          3,
      ),
    ),
  );

  let state: ExecutionState = "active";

  let stopped = false;

  let timer:
    | ReturnType<typeof setTimeout>
    | undefined;

  let pending:
    | Promise<void>
    | undefined;

  const abort = (
    nextState: ExecutionState,
  ): void => {
    state = nextState;

    if (!controller.signal.aborted) {
      controller.abort();
    }
  };

  const poll = async (): Promise<void> => {
    if (stopped || signal.aborted) {
      return;
    }

    try {
      const nextState =
        await renewExecutionLease(claim);

      if (nextState !== "active") {
        abort(nextState);
      }
    } catch {
      // Fail closed if execution ownership
      // cannot be confirmed.

      abort("unavailable");

      workerLogger.warn(
        {
          deploymentId:
            claim.payload.deploymentId,

          attempt:
            claim.executionAttempt,
        },

        "Deployment execution lease could not be renewed",
      );
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
// Persist initial source preparation metadata
// ==================================================

async function markSourcePreparing(
  claim: ExecutionClaim,
  identity: SourceIdentity,
): Promise<boolean> {
  return prisma.$transaction(async (tx) => {
    const now = new Date();

    // Acquire the deployment row lock using a
    // fenced update before modifying source metadata.

    const owned =
      await tx.deployment.updateMany({
        where: {
          ...fencedScope(claim, now),

          status: "PREPARING",
        },

        data: {
          updatedAt: now,
        },
      });

    if (owned.count !== 1) {
      return false;
    }

    const scope = sourceScope(claim.payload);

    await tx.deploymentSourcePreparation.upsert({
      where: {
        workspaceId_projectId_deploymentId:
          scope,
      },

      create: {
        ...scope,

        ...identity,

        status: "DOWNLOADING",

        attempt: claim.executionAttempt,

        downloadedBytes: BigInt(0),

        extractedBytes: BigInt(0),

        fileCount: 0,

        startedAt: claim.startedAt,
      },

      update: {
        ...identity,

        status: "DOWNLOADING",

        attempt: claim.executionAttempt,

        sourceWorkspaceKey: null,

        downloadedBytes: BigInt(0),

        extractedBytes: BigInt(0),

        fileCount: 0,

        failureCode: null,

        expiresAt: null,

        startedAt: claim.startedAt,

        completedAt: null,
      },
    });

    return true;
  });
}

// ==================================================
// Atomic successful source preparation
// ==================================================

async function markSourcePrepared(
  claim: ExecutionClaim,
  identity: SourceIdentity,
  result: PreparedSourceResult,
): Promise<boolean> {
  return prisma.$transaction(async (tx) => {
    const now = new Date();

    const expiresAt = new Date(
      now.getTime() +
        workerEnv.SOURCE_WORKSPACE_TTL_SECONDS *
          1000,
    );

    const transitioned =
      await tx.deployment.updateMany({
        where: {
          ...fencedScope(claim, now),

          status: "PREPARING",
        },

        data: {
          status: "SOURCE_PREPARED",

          executionLeaseToken: null,

          executionLeaseExpiresAt: null,

          failureCode: null,

          failureMessage: null,

          updatedAt: now,
        },
      });

    if (transitioned.count !== 1) {
      return false;
    }

    const scope = sourceScope(claim.payload);

    const metadata = {
      ...identity,

      status: "READY" as const,

      attempt: claim.executionAttempt,

      sourceWorkspaceKey: result.attemptId,

      downloadedBytes:
        BigInt(result.downloadedBytes),

      extractedBytes:
        BigInt(result.extractedBytes),

      fileCount: result.fileCount,

      failureCode: null,

      expiresAt,

      startedAt: claim.startedAt,

      completedAt: now,
    };

    await tx.deploymentSourcePreparation.upsert({
      where: {
        workspaceId_projectId_deploymentId:
          scope,
      },

      create: {
        ...scope,

        ...metadata,
      },

      update: metadata,
    });

    await tx.auditLog.create({
      data: {
        workspaceId:
          claim.payload.workspaceId,

        action: "DEPLOYMENT_SOURCE_PREPARED",

        entityType: "Deployment",

        entityId:
          claim.payload.deploymentId,

        metadata: {
          from: "PREPARING",

          to: "SOURCE_PREPARED",

          commitSha:
            identity.resolvedCommitSha,

          attempt:
            claim.executionAttempt,

          fileCount: result.fileCount,

          downloadedBytes:
            result.downloadedBytes,

          extractedBytes:
            result.extractedBytes,
        },
      },
    });

    // ==============================================
    // Schedule the build in the same transaction
    // ==============================================
    //
    // The build outbox event is committed atomically
    // with SOURCE_PREPARED so there is no gap between
    // the database transition and queue publication.
    //
    // When builds are disabled the deployment remains
    // SOURCE_PREPARED and no build is scheduled.
    // ==============================================

    if (workerEnv.BUILD_ENABLED) {
      await tx.deploymentOutboxEvent.create({
        data: {
          workspaceId:
            claim.payload.workspaceId,

          projectId:
            claim.payload.projectId,

          deploymentId:
            claim.payload.deploymentId,

          eventType: "BUILD_REQUESTED",

          jobId:
            QueueManager.deterministicJobId(
              "build",
              [claim.payload.deploymentId],
            ),
        },
      });
    }

    return true;
  });
}

// ==================================================
// Atomic failure, cancellation and retry settlement
// ==================================================

async function settleExecution(
  input: FailureSettlement,
): Promise<SettlementResult> {
  const {
    claim,
    identity,
    code,
    retryable,
  } = input;

  return prisma.$transaction(async (tx) => {
    const now = new Date();

    const scope = sourceScope(claim.payload);

    const fence = fencedScope(claim, now);

    // Cancellation takes precedence over failure.

    const cancelled =
      await tx.deployment.updateMany({
        where: {
          ...fence,

          status: "CANCELLING",
        },

        data: {
          status: "CANCELLED",

          failureCode: "CANCELLED",

          failureMessage:
            "Deployment cancellation was acknowledged by the worker.",

          executionLeaseToken: null,

          executionLeaseExpiresAt: null,

          finishedAt: now,
        },
      });

    if (cancelled.count === 1) {
      await tx.deploymentSourcePreparation.updateMany({
        where: scope,

        data: {
          status: "CANCELLED",

          failureCode:
            "SOURCE_PREPARATION_CANCELLED",

          sourceWorkspaceKey: null,

          expiresAt: null,

          completedAt: now,
        },
      });

      await tx.auditLog.create({
        data: {
          workspaceId:
            claim.payload.workspaceId,

          action:
            "DEPLOYMENT_CANCELLATION_ACKNOWLEDGED",

          entityType: "Deployment",

          entityId:
            claim.payload.deploymentId,

          metadata: {
            source: "worker",

            attempt:
              claim.executionAttempt,
          },
        },
      });

      return "cancelled";
    }

    // Keep PREPARING for retryable failures.
    // A released lease can be reclaimed by the
    // next BullMQ execution attempt.

    const updated =
      await tx.deployment.updateMany({
        where: {
          ...fence,

          status: "PREPARING",
        },

        data: {
          status: retryable
            ? "PREPARING"
            : "FAILED",

          failureCode: retryable
            ? null
            : "UNKNOWN",

          failureMessage: retryable
            ? null
            : failureMessage(code),

          executionLeaseToken: null,

          executionLeaseExpiresAt: null,

          finishedAt: retryable
            ? null
            : now,
        },
      });

    if (updated.count !== 1) {
      return "stale";
    }

    // Never fabricate repository names or SHAs.

    if (identity) {
      const metadata = {
        ...identity,

        status: "FAILED" as const,

        attempt: claim.executionAttempt,

        sourceWorkspaceKey: null,

        downloadedBytes: BigInt(0),

        extractedBytes: BigInt(0),

        fileCount: 0,

        failureCode: code,

        expiresAt: null,

        startedAt: claim.startedAt,

        completedAt: now,
      };

      await tx.deploymentSourcePreparation.upsert({
        where: {
          workspaceId_projectId_deploymentId:
            scope,
        },

        create: {
          ...scope,

          ...metadata,
        },

        update: metadata,
      });
    } else {
      // Update an existing record only.
      // Do not create invalid source identity.

      await tx.deploymentSourcePreparation.updateMany({
        where: scope,

        data: {
          status: "FAILED",

          failureCode: code,

          sourceWorkspaceKey: null,

          expiresAt: null,

          completedAt: now,
        },
      });
    }

    await tx.auditLog.create({
      data: {
        workspaceId:
          claim.payload.workspaceId,

        action: retryable
          ? "DEPLOYMENT_SOURCE_RETRY_SCHEDULED"
          : "DEPLOYMENT_STATUS_CHANGED",

        entityType: "Deployment",

        entityId:
          claim.payload.deploymentId,

        metadata: {
          from: "PREPARING",

          to: retryable
            ? "PREPARING"
            : "FAILED",

          attempt:
            claim.executionAttempt,

          failureCode: code,

          retryable,
        },
      },
    });

    return retryable
      ? "retry"
      : "failed";
  });
}

// ==================================================
// Workspace lifecycle management
// ==================================================

async function discardPreparedWorkspace(
  workspace: WorkspaceManager,
  deploymentId: string,
  attemptId: string,
): Promise<void> {
  workspace.release(attemptId);

  try {
    await workspace.cleanupAttempt(
      deploymentId,
      attemptId,
    );
  } catch {
    workerLogger.warn(
      {
        deploymentId,

        attemptId,
      },

      "Source workspace cleanup deferred",
    );
  }
}

// ==================================================
// Retry budget
// ==================================================

function hasRetryBudget(
  job: Job<QueueJob>,
  executionAttempt: number,
): boolean {
  const configuredAttempts =
    job.opts.attempts ??
    workerEnv.QUEUE_JOB_ATTEMPTS;

  const queueAttemptsRemaining =
    job.attemptsMade + 1 <
    configuredAttempts;

  const executionAttemptsRemaining =
    executionAttempt <
    workerEnv.QUEUE_JOB_ATTEMPTS;

  return (
    queueAttemptsRemaining &&
    executionAttemptsRemaining
  );
}

// ==================================================
// Error normalization
// ==================================================

function normalizeSourceError(
  error: unknown,
): SourcePreparationError {
  if (error instanceof SourcePreparationError) {
    return error;
  }

  // Do not expose raw errors, request URLs,
  // tokens, private keys or filesystem paths.

  return new SourcePreparationError(
    "UNEXPECTED_FAILURE",

    "Source preparation failed unexpectedly.",
  );
}

// ==================================================
// Main deployment processor
// ==================================================

export async function processDeploymentJob(
  job: Job<QueueJob>,
  payload: DeploymentJob,
  signal: AbortSignal,
  options: ProcessDeploymentOptions,
): Promise<ProcessDeploymentResult> {
  // ================================================
  // Validate transactional outbox event
  // ================================================

  const event =
    await prisma.deploymentOutboxEvent.findFirst({
      where: {
        id: payload.eventId,

        workspaceId: payload.workspaceId,

        projectId: payload.projectId,

        deploymentId: payload.deploymentId,

        eventType: "DEPLOYMENT_REQUESTED",
      },

      select: {
        id: true,

        jobId: true,
      },
    });

  if (
    !event ||
    event.jobId !== job.id
  ) {
    throw new UnrecoverableError(
      "Deployment queue event is invalid.",
    );
  }

  // ================================================
  // Validate deployment lifecycle
  // ================================================

  const deployment =
    await prisma.deployment.findFirst({
      where: deploymentScope(payload),

      select: {
        status: true,
      },
    });

  if (
    !deployment ||
    TERMINAL_STATUSES.has(deployment.status)
  ) {
    return {
      status: "skipped",
    };
  }

  if (deployment.status === "CANCELLING") {
    const cancelled =
      await recoverCancellation(payload);

    if (cancelled) {
      return {
        status: "cancelled",
      };
    }

    throw new TransientJobError(
      "Deployment cancellation is awaiting execution settlement.",
    );
  }

  if (signal.aborted) {
    throw new TransientJobError(
      "Deployment worker is shutting down.",
    );
  }

  // ================================================
  // Acquire execution ownership
  // ================================================

  const claimed = await claimDeployment(
    payload,

    workerEnv.QUEUE_JOB_ATTEMPTS,
  );

  if (!claimed) {
    const exhausted =
      await failExpiredAttempt(
        payload,

        workerEnv.QUEUE_JOB_ATTEMPTS,
      );

    if (exhausted) {
      return {
        status: "failed",
      };
    }

    const current =
      await prisma.deployment.findFirst({
        where: deploymentScope(payload),

        select: {
          status: true,
        },
      });

    if (
      !current ||
      TERMINAL_STATUSES.has(current.status)
    ) {
      return {
        status: "skipped",
      };
    }

    throw new TransientJobError(
      "Deployment execution ownership is unavailable.",
    );
  }

  const claim: ExecutionClaim = {
    payload,

    leaseToken:
      claimed.executionLeaseToken,

    executionAttempt:
      claimed.executionAttempt,

    startedAt: new Date(),
  };

  const monitor =
    createExecutionMonitor(
      claim,
      signal,
    );

  let identity: SourceIdentity | null = null;

  let preparedResult:
    | PreparedSourceResult
    | undefined;

  let sourceCommitted = false;

  let commitOutcomeUnknown = false;

  try {
    // ==============================================
    // Resolve authorized source identity
    // ==============================================

    const deploymentRecord =
      await prisma.deployment.findFirst({
        where: deploymentScope(payload),

        select: {
          requestedBranch: true,

          commitSha: true,

          project: {
            select: {
              repository: {
                select: {
                  fullName: true,

                  installation: {
                    select: {
                      installationId: true,

                      status: true,
                    },
                  },
                },
              },
            },
          },
        },
      });

    const repository =
      deploymentRecord?.project.repository;

    const commitSha =
      deploymentRecord?.commitSha;

    if (
      !commitSha ||
      !SHA_PATTERN.test(commitSha)
    ) {
      throw new SourcePreparationError(
        "COMMIT_NOT_FOUND",

        "Deployment has no valid immutable commit.",
      );
    }

    if (!repository?.fullName) {
      throw new SourcePreparationError(
        "REPOSITORY_ACCESS_DENIED",

        "Deployment has no authorized repository.",
      );
    }

    identity = {
      repositoryFullName:
        repository.fullName,

      requestedBranch:
        deploymentRecord.requestedBranch,

      resolvedCommitSha: commitSha,
    };

    if (!repository.installation) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",

        "Repository installation is unavailable.",
      );
    }

    if (
      repository.installation.status !== "ACTIVE"
    ) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",

        "Repository installation is not active.",
      );
    }

    if (!options.auth) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",

        "Source retrieval is not configured.",
      );
    }

    if (monitor.signal.aborted) {
      throw new SourcePreparationError(
        "SOURCE_PREPARATION_CANCELLED",
      );
    }

    // ==============================================
    // Persist source preparation start
    // ==============================================

    const started =
      await markSourcePreparing(
        claim,
        identity,
      );

    if (!started) {
      throw new SourcePreparationError(
        "EXECUTION_LEASE_LOST",
      );
    }

    // ==============================================
    // Secure source retrieval
    // ==============================================

    preparedResult = await prepareSource({
      deploymentId:
        payload.deploymentId,

      repositoryFullName:
        identity.repositoryFullName,

      requestedBranch:
        identity.requestedBranch,

      resolvedCommitSha:
        identity.resolvedCommitSha,

      installationId:
        repository.installation.installationId,

      auth: options.auth,

      workspace: options.workspace,

      limits: {
        maxArchiveBytes:
          workerEnv.SOURCE_MAX_ARCHIVE_BYTES,

        maxExtractedBytes:
          workerEnv.SOURCE_MAX_EXTRACTED_BYTES,

        maxFileBytes:
          workerEnv.SOURCE_MAX_FILE_BYTES,

        maxFileCount:
          workerEnv.SOURCE_MAX_FILE_COUNT,

        timeoutMs:
          workerEnv.SOURCE_DOWNLOAD_TIMEOUT_MS,

        maxRetries:
          workerEnv.SOURCE_MAX_RETRIES,
      },

      signal: monitor.signal,

      shouldCancel: () =>
        monitor.shouldCancel(),

      ...(options.fetchImpl
        ? {
            fetchImpl: options.fetchImpl,
          }
        : {}),

      ...(options.sleep
        ? {
            sleep: options.sleep,
          }
        : {}),
    });

    if (monitor.signal.aborted) {
      throw new SourcePreparationError(
        "SOURCE_PREPARATION_CANCELLED",
      );
    }

    // ==============================================
    // Stop heartbeat before final transaction
    // ==============================================

    await monitor.stop();

    if (monitor.signal.aborted) {
      throw new SourcePreparationError(
        "SOURCE_PREPARATION_CANCELLED",
      );
    }

    // ==============================================
    // Atomically commit prepared source
    // ==============================================

    commitOutcomeUnknown = true;

    const committed =
      await markSourcePrepared(
        claim,

        identity,

        preparedResult,
      );

    commitOutcomeUnknown = false;

    if (!committed) {
      throw new SourcePreparationError(
        "EXECUTION_LEASE_LOST",
      );
    }

    sourceCommitted = true;

    workerLogger.info(
      {
        deploymentId:
          payload.deploymentId,

        attempt:
          claim.executionAttempt,

        fileCount:
          preparedResult.fileCount,

        downloadedBytes:
          preparedResult.downloadedBytes,

        extractedBytes:
          preparedResult.extractedBytes,
      },

      "Deployment source preparation completed",
    );

    return {
      status: "prepared",
    };
  } catch (error) {
    await monitor.stop();

    // ==============================================
    // Handle ambiguous database commit outcome
    // ==============================================

    if (
      commitOutcomeUnknown &&
      preparedResult
    ) {
      try {
        const persisted =
          await prisma.deploymentSourcePreparation.findFirst({
            where: {
              ...sourceScope(payload),

              sourceWorkspaceKey:
                preparedResult.attemptId,

              status: "READY",

              attempt:
                claim.executionAttempt,
            },

            select: {
              id: true,
            },
          });

        if (persisted) {
          sourceCommitted = true;

          commitOutcomeUnknown = false;

          return {
            status: "prepared",
          };
        }

        commitOutcomeUnknown = false;
      } catch {
        // Preserve the workspace when the database
        // commit outcome cannot be determined.

        throw new TransientJobError(
          "Source preparation commit outcome is unavailable.",
        );
      }
    }

    // ==============================================
    // Classify execution interruption
    // ==============================================

    let sourceError =
      normalizeSourceError(error);

    if (monitor.state === "lost") {
      sourceError =
        new SourcePreparationError(
          "EXECUTION_LEASE_LOST",
        );
    } else if (
      monitor.state === "unavailable"
    ) {
      sourceError =
        new SourcePreparationError(
          "WORKSPACE_UNAVAILABLE",

          "Execution ownership could not be verified.",
        );
    }

    const interrupted =
      signal.aborted ||
      monitor.state === "unavailable";

    const retryable =
      (
        sourceError.retryable ||
        interrupted
      ) &&
      hasRetryBudget(
        job,
        claim.executionAttempt,
      );

    // ==============================================
    // Atomically settle execution
    // ==============================================

    let settled: SettlementResult;

    try {
      settled = await settleExecution({
        claim,

        identity,

        code: sourceError.code,

        retryable,
      });
    } catch {
      workerLogger.error(
        {
          deploymentId:
            payload.deploymentId,

          attempt:
            claim.executionAttempt,
        },

        "Deployment execution settlement failed",
      );

      throw new TransientJobError(
        "Deployment execution settlement is unavailable.",
      );
    }

    if (settled === "cancelled") {
      workerLogger.info(
        {
          deploymentId:
            payload.deploymentId,

          attempt:
            claim.executionAttempt,
        },

        "Deployment cancellation acknowledged",
      );

      return {
        status: "cancelled",
      };
    }

    if (settled === "stale") {
      workerLogger.warn(
        {
          deploymentId:
            payload.deploymentId,

          attempt:
            claim.executionAttempt,
        },

        "Stale deployment execution was fenced",
      );

      const current =
        await prisma.deployment.findFirst({
          where: deploymentScope(payload),

          select: {
            status: true,
          },
        });

      if (
        !current ||
        TERMINAL_STATUSES.has(current.status)
      ) {
        return {
          status: "skipped",
        };
      }

      throw new TransientJobError(
        "Deployment execution lease was lost.",
      );
    }

    if (settled === "retry") {
      workerLogger.warn(
        {
          deploymentId:
            payload.deploymentId,

          code: sourceError.code,

          attempt:
            claim.executionAttempt,
        },

        "Source preparation will be retried",
      );

      throw new TransientJobError(
        `Source preparation failed: ${sourceError.code}`,
      );
    }

    workerLogger.error(
      {
        deploymentId:
          payload.deploymentId,

        code: sourceError.code,

        attempt:
          claim.executionAttempt,
      },

      "Deployment source preparation failed",
    );

    throw new UnrecoverableError(
      `Source preparation failed: ${sourceError.code}`,
    );
  } finally {
    // ==============================================
    // Deterministic execution cleanup
    // ==============================================

    await monitor.stop();

    if (preparedResult) {
      if (
        sourceCommitted ||
        commitOutcomeUnknown
      ) {
        // Preserve committed source for Phase 5.4.
        // Also preserve source if database commit
        // outcome remains ambiguous.

        options.workspace.release(
          preparedResult.attemptId,
        );
      } else {
        await discardPreparedWorkspace(
          options.workspace,

          payload.deploymentId,

          preparedResult.attemptId,
        );
      }
    }
  }
}