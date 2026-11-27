
/**
 * CloudPilot — Build Processor
 *
 * Processes a single deployment build job:
 *
 *   verify outbox + source -> claim build (fenced) -> execute
 *   -> verify artifact -> publish BUILD_SUCCEEDED
 *
 * Build execution is tenant-scoped, idempotent and fenced
 * by an execution lease.
 *
 * Local builds are restricted to explicitly trusted repositories.
 * Uncertain execution outcomes must not trigger automatic rebuilds.
 */

import { randomUUID } from "node:crypto";

import { prisma } from "@cloudpilot/database";

import {
  BuildError,
  TransientJobError,
  QueueManager,
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

import {
  BuildEngineError,
  runBuild,
} from "../build/build-engine.js";

import type { BuildExecutor } from "../build/build-executor.js";

import type { BuildLogEntry } from "../build/build-logs.js";

import type { CollectedLogRecord } from "@cloudpilot/queue";

import { DeploymentLogWriter } from "../logging/log-writer.js";

import { inferLogLevel } from "../logging/normalize.js";

import { emitSystemLog } from "../logging/system-logs.js";

// ==================================================
// Constants
// ==================================================

const SHA_PATTERN = /^[0-9a-f]{40}$/u;

const IMAGE_ID_PATTERN = /^sha256:[0-9a-f]{64}$/u;

const BUILD_TERMINAL_DEPLOYMENT_STATUSES = new Set<string>([
  "FAILED",
  "CANCELLED",
  "SUCCEEDED",
  "STOPPED",
  "ROLLED_BACK",
  "BUILD_SUCCEEDED",
  "DEPLOYING",
  "HEALTH_CHECKING",
  "RUNTIME_FAILED",
  "RUNNING",
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
  trustedRepository: boolean;
}

interface VerifiedBuildArtifact {
  imageTag: string;
  imageId: string | null;
  imageDigest: string | null;
  imageSizeBytes: number | null;
  platform: string;
  durationMs: number;
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

type ClaimResult =
  | {
      status: "claimed";
      claim: ExecutionClaim;
    }
  | {
      status: "skipped" | "busy" | "uncertain";
    };

// ==================================================
// Internal errors
// ==================================================

/**
 * Throwing this error inside a transaction guarantees
 * rollback instead of committing a partial settlement.
 */
class StaleBuildOwnershipError extends Error {
  constructor() {
    super("Build execution ownership is stale.");
    this.name = "StaleBuildOwnershipError";
  }
}

class InvalidBuildIdentityError extends Error {
  constructor() {
    super("Build execution identity is invalid.");
    this.name = "InvalidBuildIdentityError";
  }
}

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

function deploymentScope({ workspaceId, projectId, deploymentId }: BuildScope) {
  return { workspaceId, projectId, id: deploymentId };
}

function fencedBuildScope(
  claim: ExecutionClaim,
  now: Date,
) {
  return {
    ...buildScope(claim.payload),

    id: claim.buildId,

    status: "RUNNING" as const,

    executionLeaseToken: claim.leaseToken,

    buildAttempt: claim.buildAttempt,

    executionLeaseExpiresAt: {
      gt: now,
    },
  };
}

function assertSingleUpdate(count: number): void {
  if (count !== 1) {
    throw new StaleBuildOwnershipError();
  }
}

// ==================================================
// Retry policy
// ==================================================

function getMaximumBuildAttempts(
  job: Job<QueueJob>,
): number {
  const queueAttempts =
    job.opts.attempts ??
    workerEnv.QUEUE_JOB_ATTEMPTS;

  const configuredRetries =
    workerEnv.BUILD_MAX_RETRIES;

  if (
    !Number.isSafeInteger(queueAttempts) ||
    queueAttempts < 1 ||
    !Number.isSafeInteger(configuredRetries) ||
    configuredRetries < 0
  ) {
    throw new UnrecoverableError(
      "Build retry configuration is invalid.",
    );
  }

  return Math.min(
    queueAttempts,
    configuredRetries + 1,
  );
}

function hasBuildRetryBudget(
  job: Job<QueueJob>,
  buildAttempt: number,
): boolean {
  const maxAttempts = getMaximumBuildAttempts(job);

  const queueAttempts =
    job.opts.attempts ??
    workerEnv.QUEUE_JOB_ATTEMPTS;

  return (
    job.attemptsMade + 1 < queueAttempts &&
    buildAttempt < maxAttempts
  );
}

// ==================================================
// Outbox validation
// ==================================================

async function validateBuildOutbox(
  job: Job<QueueJob>,
  payload: BuildJob,
): Promise<boolean> {
  if (
    !job.id ||
    payload.buildId !== payload.correlationId
  ) {
    return false;
  }

  const event =
    await prisma.deploymentOutboxEvent.findFirst({
      where: {
        id: payload.correlationId,

        ...buildScope(payload),

        eventType: "BUILD_REQUESTED",
      },

      select: {
        id: true,
        jobId: true,
        status: true,
      },
    });

  if (
    !event ||
    event.id !== payload.buildId ||
    event.jobId !== job.id
  ) {
    return false;
  }

  /**
   * A worker can receive a job before the publisher
   * finishes marking the outbox event as PUBLISHED.
   */
  return (
    event.status === "PUBLISHING" ||
    event.status === "PUBLISHED"
  );
}

// ==================================================
// Atomic build claim
// ==================================================

async function claimBuild(
  payload: BuildJob,
  maxAttempts: number,
): Promise<ClaimResult> {
  const scope = buildScope(payload);

  try {
    return await prisma.$transaction(
      async (tx): Promise<ClaimResult> => {
        /**
         * Serialize claim and settlement operations by
         * locking the authoritative deployment row first.
         *
         * Parameterized SQL prevents query injection.
         */
        const locked = await tx.$queryRaw<
          Array<{ id: string }>
        >`
          SELECT "id"
          FROM "deployments"
          WHERE "id" = ${payload.deploymentId}::uuid
            AND "workspaceId" = ${payload.workspaceId}::uuid
            AND "projectId" = ${payload.projectId}::uuid
          FOR UPDATE
        `;

        if (locked.length !== 1) {
          return { status: "skipped" };
        }

        const now = new Date();

        const leaseToken = randomUUID();

        const leaseExpiresAt = new Date(
          now.getTime() +
            workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS,
        );

        const deployment =
          await tx.deployment.findFirst({
            where: deploymentScope(scope),

            select: {
              status: true,
              commitSha: true,
            },
          });

        if (!deployment) {
          return { status: "skipped" };
        }

        if (
          BUILD_TERMINAL_DEPLOYMENT_STATUSES.has(
            deployment.status,
          )
        ) {
          return { status: "skipped" };
        }

        if (
          deployment.status === "CANCELLING"
        ) {
          return { status: "skipped" };
        }

        if (
          deployment.commitSha !==
          payload.resolvedCommitSha
        ) {
          throw new InvalidBuildIdentityError();
        }

        if (
          deployment.status !== "SOURCE_PREPARED" &&
          deployment.status !== "BUILDING"
        ) {
          return { status: "busy" };
        }

        const source =
          await tx.deploymentSourcePreparation.findFirst({
            where: {
              ...scope,

              id: payload.sourcePreparationId,

              resolvedCommitSha:
                payload.resolvedCommitSha,

              status: "READY",
            },

            select: {
              id: true,
            },
          });

        if (!source) {
          throw new InvalidBuildIdentityError();
        }

        const existing =
          await tx.deploymentBuild.findFirst({
            where: {
              ...scope,

              id: payload.buildId,
            },

            select: {
              id: true,
              status: true,
              buildAttempt: true,
              sourcePreparationId: true,
              resolvedCommitSha: true,
              executionLeaseToken: true,
              executionLeaseExpiresAt: true,
            },
          });

        const conflictingBuild =
          await tx.deploymentBuild.findFirst({
            where: {
              ...scope,

              id: {
                not: payload.buildId,
              },

              status: {
                in: [
                  "QUEUED",
                  "RUNNING",
                  "SUCCEEDED",
                ],
              },
            },

            select: {
              id: true,
            },
          });

        if (conflictingBuild) {
          return { status: "uncertain" };
        }

        let buildAttempt = 1;

        if (existing) {
          if (
            existing.sourcePreparationId !==
              payload.sourcePreparationId ||
            existing.resolvedCommitSha !==
              payload.resolvedCommitSha
          ) {
            throw new InvalidBuildIdentityError();
          }

          if (
            existing.status === "SUCCEEDED" ||
            existing.status === "CANCELLED" ||
            existing.status === "FAILED"
          ) {
            return { status: "skipped" };
          }

          if (existing.status === "RUNNING") {
            const leaseActive =
              existing.executionLeaseToken !== null &&
              existing.executionLeaseExpiresAt !== null &&
              existing.executionLeaseExpiresAt.getTime() >
                now.getTime();

            /**
             * An expired lease does not prove that
             * Docker has stopped executing the build.
             *
             * Never automatically take over an
             * uncertain RUNNING build.
             */
            return {
              status: leaseActive
                ? "busy"
                : "uncertain",
            };
          }

          if (
            existing.status !== "QUEUED" ||
            existing.executionLeaseToken !== null ||
            existing.executionLeaseExpiresAt !== null
          ) {
            return { status: "uncertain" };
          }

          buildAttempt =
            existing.buildAttempt + 1;
        } else {
          /**
           * A BUILDING deployment without the expected
           * build record indicates an inconsistent or
           * legacy execution state.
           */
          if (
            deployment.status !== "SOURCE_PREPARED"
          ) {
            return { status: "uncertain" };
          }

          const previousBuild =
            await tx.deploymentBuild.findFirst({
              where: scope,

              orderBy: {
                buildAttempt: "desc",
              },

              select: {
                buildAttempt: true,
              },
            });

          if (previousBuild) {
            return { status: "uncertain" };
          }
        }

        if (
          !Number.isSafeInteger(buildAttempt) ||
          buildAttempt < 1 ||
          buildAttempt > maxAttempts
        ) {
          if (existing) {
            const exhausted =
              await tx.deploymentBuild.updateMany({
                where: {
                  ...scope,

                  id: payload.buildId,

                  status: "QUEUED",

                  executionLeaseToken: null,

                  executionLeaseExpiresAt: null,
                },

                data: {
                  status: "FAILED",

                  failureCode:
                    "BUILDER_UNAVAILABLE",

                  failureMessage:
                    buildFailureMessage(
                      "BUILDER_UNAVAILABLE",
                    ),

                  completedAt: now,
                },
              });

            assertSingleUpdate(exhausted.count);

            const failed =
              await tx.deployment.updateMany({
                where: {
                  ...deploymentScope(scope),

                  status: deployment.status,
                },

                data: {
                  status: "FAILED",

                  failureCode: "BUILD_FAILED",

                  failureMessage:
                    "Build retry budget exhausted.",

                  finishedAt: now,
                },
              });

            assertSingleUpdate(failed.count);
          }

          return { status: "skipped" };
        }

        const transitioned =
          await tx.deployment.updateMany({
            where: {
              ...deploymentScope(scope),

              status: deployment.status,

              commitSha:
                payload.resolvedCommitSha,
            },

            data: {
              status: "BUILDING",

              updatedAt: now,
            },
          });

        assertSingleUpdate(transitioned.count);

        if (existing) {
          const updated =
            await tx.deploymentBuild.updateMany({
              where: {
                ...scope,

                id: payload.buildId,

                status: "QUEUED",

                buildAttempt:
                  existing.buildAttempt,

                executionLeaseToken: null,

                executionLeaseExpiresAt: null,
              },

              data: {
                status: "RUNNING",

                buildAttempt,

                executionLeaseToken:
                  leaseToken,

                executionLeaseExpiresAt:
                  leaseExpiresAt,

                startedAt: now,

                completedAt: null,

                durationMs: null,

                failureCode: null,

                failureMessage: null,
              },
            });

          assertSingleUpdate(updated.count);
        } else {
          /**
           * The API outbox publisher uses event.id as
           * payload.buildId. Preserve that exact identity.
           */
          await tx.deploymentBuild.create({
            data: {
              ...scope,

              id: payload.buildId,

              sourcePreparationId:
                payload.sourcePreparationId,

              resolvedCommitSha:
                payload.resolvedCommitSha,

              buildAttempt,

              status: "RUNNING",

              executionLeaseToken:
                leaseToken,

              executionLeaseExpiresAt:
                leaseExpiresAt,

              startedAt: now,
            },
          });
        }

        await tx.auditLog.create({
          data: {
            workspaceId:
              payload.workspaceId,

            action:
              "DEPLOYMENT_BUILD_CLAIMED",

            entityType: "Deployment",

            entityId:
              payload.deploymentId,

            metadata: {
              buildId: payload.buildId,

              buildAttempt,
            },
          },
        });

        return {
          status: "claimed",

          claim: {
            payload,

            buildId: payload.buildId,

            leaseToken,

            buildAttempt,

            startedAt: now,
          },
        };
      },
    );
  } catch (error) {
    if (
      error instanceof
      StaleBuildOwnershipError
    ) {
      return { status: "busy" };
    }

    if (
      error instanceof
      InvalidBuildIdentityError
    ) {
      throw new UnrecoverableError(
        "Build source or execution identity is invalid.",
      );
    }

    throw error;
  }
}

// ==================================================
// Lease renewal
// ==================================================

async function renewBuildLease(
  claim: ExecutionClaim,
): Promise<ExecutionState> {
  const scope = buildScope(claim.payload);

  return prisma.$transaction(async (tx) => {
    const locked = await tx.$queryRaw<
      Array<{ id: string }>
    >`
      SELECT "id"
      FROM "deployments"
      WHERE "id" = ${scope.deploymentId}::uuid
        AND "workspaceId" = ${scope.workspaceId}::uuid
        AND "projectId" = ${scope.projectId}::uuid
      FOR UPDATE
    `;

    if (locked.length !== 1) {
      return "lost";
    }

    const now = new Date();

    const deployment =
      await tx.deployment.findFirst({
        where: deploymentScope(scope),

        select: {
          status: true,
        },
      });

    if (!deployment) {
      return "lost";
    }

    if (
      deployment.status !== "BUILDING" &&
      deployment.status !== "CANCELLING"
    ) {
      return "lost";
    }

    const expiresAt = new Date(
      now.getTime() +
        workerEnv.DEPLOYMENT_EXECUTION_LEASE_MS,
    );

    const renewed =
      await tx.deploymentBuild.updateMany({
        where: fencedBuildScope(
          claim,
          now,
        ),

        data: {
          executionLeaseExpiresAt:
            expiresAt,
        },
      });

    if (renewed.count !== 1) {
      return "lost";
    }

    /**
     * Cancellation is checked even when lease
     * renewal succeeds.
     */
    if (
      deployment.status === "CANCELLING"
    ) {
      return "cancelling";
    }

    return "active";
  });
}

// ==================================================
// Execution monitor
// ==================================================

function createBuildMonitor(
  claim: ExecutionClaim,
  externalSignal: AbortSignal,
) {
  const controller =
    new AbortController();

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
    next: ExecutionState,
  ): void => {
    state = next;

    if (!controller.signal.aborted) {
      controller.abort(
        new BuildError(
          next === "cancelling"
            ? "BUILD_CANCELLED"
            : "BUILD_LEASE_LOST",
        ),
      );
    }
  };

  const poll = async (): Promise<void> => {
    if (
      stopped ||
      signal.aborted
    ) {
      return;
    }

    try {
      const next =
        await renewBuildLease(claim);

      if (next !== "active") {
        abort(next);
      }
    } catch {
      abort("unavailable");
    }
  };

  const schedule = (): void => {
    if (
      stopped ||
      signal.aborted
    ) {
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
  const deployment =
    await prisma.deployment.findFirst({
      where: deploymentScope(buildScope(payload)),

      select: {
        status: true,

        commitSha: true,

        project: {
          select: {
            repository: {
              select: {
                fullName: true,
              },
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
    deployment.commitSha !==
      payload.resolvedCommitSha
  ) {
    throw new BuildError(
      "SOURCE_INTEGRITY_FAILED",
      "Deployment source identity is invalid.",
    );
  }

  if (
    deployment.status === "CANCELLING"
  ) {
    throw new BuildError(
      "BUILD_CANCELLED",
    );
  }

  if (
    deployment.status !== "BUILDING"
  ) {
    throw new BuildError(
      "BUILD_LEASE_LOST",
    );
  }

  const repositoryFullName =
    deployment.project.repository?.fullName;

  if (
    !repositoryFullName ||
    repositoryFullName.trim().length === 0
  ) {
    throw new BuildError(
      "SOURCE_WORKSPACE_UNAVAILABLE",
      "Deployment repository is unavailable.",
    );
  }

  /**
   * Local Docker is not a strong multi-tenant
   * isolation boundary.
   *
   * Disabling the trusted-only flag must not
   * silently authorize untrusted builds.
   */
  if (
    !workerEnv.BUILD_TRUSTED_LOCAL_ONLY
  ) {
    throw new BuildError(
      "BUILD_POLICY_VIOLATION",
      "Unrestricted local builds are not supported.",
    );
  }

  const trustedRepository =
    workerEnv.BUILD_TRUSTED_REPOSITORIES.some(
      (entry) =>
        entry.trim().toLowerCase() ===
        repositoryFullName.toLowerCase(),
    );

  if (!trustedRepository) {
    throw new BuildError(
      "BUILD_POLICY_VIOLATION",
      "Repository is not authorized for local builds.",
    );
  }

  const preparation =
    await prisma.deploymentSourcePreparation.findFirst({
      where: {
        ...buildScope(payload),

        id: payload.sourcePreparationId,

        resolvedCommitSha:
          payload.resolvedCommitSha,

        status: "READY",
      },

      select: {
        id: true,

        sourceWorkspaceKey: true,

        resolvedCommitSha: true,
      },
    });

  if (
    !preparation ||
    !preparation.sourceWorkspaceKey
  ) {
    throw new BuildError(
      "SOURCE_WORKSPACE_UNAVAILABLE",
      "Prepared source workspace is unavailable.",
    );
  }

  if (
    preparation.id !==
      payload.sourcePreparationId ||
    preparation.resolvedCommitSha !==
      payload.resolvedCommitSha
  ) {
    throw new BuildError(
      "SOURCE_INTEGRITY_FAILED",
    );
  }

  let sourceDir: string;

  try {
    sourceDir =
      await workspace.resolvePreparedSourceDir(
        payload.deploymentId,

        preparation.sourceWorkspaceKey,
        repositoryFullName,
        payload.resolvedCommitSha,
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
      deployment.project.buildConfiguration
        ?.dockerfilePath ?? null,

    buildContextPath:
      deployment.project.buildConfiguration
        ?.rootDirectory ?? null,

    repositoryFullName,

    trustedRepository,
  };
}

// ==================================================
// Artifact verification
// ==================================================

function assertVerifiedArtifact(
  artifact: VerifiedBuildArtifact,
): void {
  if (
    typeof artifact.imageTag !== "string" ||
    artifact.imageTag.length === 0 ||
    artifact.imageTag.length > 255 ||
    /[\u0000-\u001f\u007f-\u009f]/u.test(
      artifact.imageTag,
    )
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
    );
  }

  if (
    !artifact.imageId ||
    artifact.imageId.length !== 71 ||
    !IMAGE_ID_PATTERN.test(
      artifact.imageId,
    )
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
    );
  }

  if (
    artifact.imageSizeBytes !== null &&
    (
      !Number.isSafeInteger(
        artifact.imageSizeBytes,
      ) ||
      artifact.imageSizeBytes < 0
    )
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
    );
  }

  if (
    artifact.platform !==
      workerEnv.BUILD_PLATFORM ||
    !Number.isSafeInteger(
      artifact.durationMs,
    ) ||
    artifact.durationMs < 0
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
    );
  }
}

// ==================================================
// Settlement
// ==================================================

async function settleSuccess(
  claim: ExecutionClaim,
  artifact: VerifiedBuildArtifact,
  logs: BuildLogEntry[],
): Promise<SettlementResult> {
  assertVerifiedArtifact(artifact);

  try {
    return await prisma.$transaction(
      async (tx): Promise<SettlementResult> => {
        const scope =
          buildScope(claim.payload);

        const locked =
          await tx.$queryRaw<
            Array<{ id: string }>
          >`
            SELECT "id"
            FROM "deployments"
            WHERE "id" = ${scope.deploymentId}::uuid
              AND "workspaceId" = ${scope.workspaceId}::uuid
              AND "projectId" = ${scope.projectId}::uuid
            FOR UPDATE
          `;

        if (locked.length !== 1) {
          throw new StaleBuildOwnershipError();
        }

        const now = new Date();

        const fence =
          fencedBuildScope(claim, now);

        const deployment =
          await tx.deployment.findFirst({
            where: deploymentScope(scope),

            select: {
              status: true,

              commitSha: true,
            },
          });

        if (
          !deployment ||
          deployment.commitSha !==
            claim.payload.resolvedCommitSha
        ) {
          throw new StaleBuildOwnershipError();
        }

        const cancelling =
          deployment.status === "CANCELLING";

        if (
          !cancelling &&
          deployment.status !== "BUILDING"
        ) {
          throw new StaleBuildOwnershipError();
        }

        const updated =
          await tx.deploymentBuild.updateMany({
            where: fence,

            data: {
              status: cancelling
                ? "CANCELLED"
                : "SUCCEEDED",

              imageTag:
                artifact.imageTag,

              imageId:
                artifact.imageId,

              imageDigest:
                artifact.imageDigest,

              artifactType:
                "DOCKER_IMAGE_STORE",

              artifactLocation:
                artifact.imageTag,

              platform:
                artifact.platform,

              imageSizeBytes:
                artifact.imageSizeBytes === null
                  ? null
                  : BigInt(
                      artifact.imageSizeBytes,
                    ),

              durationMs:
                artifact.durationMs,

              executionLeaseToken: null,

              executionLeaseExpiresAt: null,

              completedAt: now,

              failureCode: cancelling
                ? "BUILD_CANCELLED"
                : null,

              failureMessage: cancelling
                ? buildFailureMessage(
                    "BUILD_CANCELLED",
                  )
                : null,
            },
          });

        assertSingleUpdate(updated.count);

        const transitioned =
          await tx.deployment.updateMany({
            where: {
              ...deploymentScope(scope),

              status:
                deployment.status,
            },

            data: cancelling
              ? {
                  status: "CANCELLED",

                  failureCode: "CANCELLED",

                  failureMessage:
                    "Deployment cancellation was acknowledged after build completion.",

                  finishedAt: now,
                }
              : {
                  status: "BUILD_SUCCEEDED",

                  updatedAt: now,
                },
          });

        assertSingleUpdate(
          transitioned.count,
        );

        // ==============================================
        // Schedule the runtime deployment atomically
        // ==============================================
        //
        // The runtime outbox event is committed in the
        // same transaction as BUILD_SUCCEEDED.
        //
        // Preserve the existing API publisher contract.
        // The runtime publisher must subsequently be
        // updated to consume the exact reserved record.
        // ==============================================

        if (
          !cancelling &&
          workerEnv.RUNTIME_ENABLED
        ) {
          const runtimeInstanceId =
            randomUUID();

          await tx.deploymentRuntimeInstance.create({
            data: {
              ...scope,

              id: runtimeInstanceId,

              buildId: claim.buildId,

              runtimeAttempt: 1,

              status: "QUEUED",

              imageId: artifact.imageId,

              imageTag: artifact.imageTag,
            },
          });

          await tx.deploymentOutboxEvent.create({
            data: {
              ...scope,

              id: runtimeInstanceId,

              eventType:
                "RUNTIME_REQUESTED",

              jobId:
                QueueManager.deterministicJobId(
                  "runtime",
                  [
                    claim.payload.deploymentId,
                  ],
                ),
            },
          });
        }

        if (logs.length > 0) {
          await tx.deploymentBuildLog.createMany({
            data: logs.map((entry) => ({
              ...scope,

              buildId: claim.buildId,

              sequence:
                BigInt(entry.sequence),

              stream: entry.stream,

              message: entry.message,
            })),
          });
        }

        await tx.auditLog.create({
          data: {
            workspaceId:
              claim.payload.workspaceId,

            action: cancelling
              ? "DEPLOYMENT_BUILD_CANCELLED"
              : "DEPLOYMENT_BUILD_SUCCEEDED",

            entityType: "Deployment",

            entityId:
              claim.payload.deploymentId,

            metadata: {
              buildId: claim.buildId,

              buildAttempt:
                claim.buildAttempt,

              imageTag:
                artifact.imageTag,

              durationMs:
                artifact.durationMs,

              artifactCleanupRequired:
                cancelling,
            },
          },
        });

        return cancelling
          ? "cancelled"
          : "succeeded";
      },
    );
  } catch (error) {
    if (
      error instanceof
      StaleBuildOwnershipError
    ) {
      return "stale";
    }

    throw error;
  }
}

async function settleFailure(
  claim: ExecutionClaim,
  code: BuildFailureCode,
  retryable: boolean,
  logs: BuildLogEntry[],
  executionMayHaveStarted: boolean,
): Promise<SettlementResult> {
  try {
    return await prisma.$transaction(
      async (tx): Promise<SettlementResult> => {
        const scope =
          buildScope(claim.payload);

        const locked =
          await tx.$queryRaw<
            Array<{ id: string }>
          >`
            SELECT "id"
            FROM "deployments"
            WHERE "id" = ${scope.deploymentId}::uuid
              AND "workspaceId" = ${scope.workspaceId}::uuid
              AND "projectId" = ${scope.projectId}::uuid
            FOR UPDATE
          `;

        if (locked.length !== 1) {
          throw new StaleBuildOwnershipError();
        }

        const now = new Date();

        const fence =
          fencedBuildScope(claim, now);

        const deployment =
          await tx.deployment.findFirst({
            where: deploymentScope(scope),

            select: {
              status: true,
            },
          });

        if (
          !deployment ||
          (
            deployment.status !== "BUILDING" &&
            deployment.status !== "CANCELLING"
          )
        ) {
          throw new StaleBuildOwnershipError();
        }

        /**
         * A cancelled CLI command is not proof that
         * the underlying Docker build has terminated.
         */
        const cancelling =
          deployment.status === "CANCELLING";

        if (
          cancelling &&
          executionMayHaveStarted
        ) {
          const fenced =
            await tx.deploymentBuild.updateMany({
              where: fence,

              data: {
                failureCode:
                  "BUILD_CANCELLED",

                failureMessage:
                  "Build termination requires reconciliation.",
              },
            });

          assertSingleUpdate(fenced.count);

          await tx.auditLog.create({
            data: {
              workspaceId:
                claim.payload.workspaceId,

              action:
                "DEPLOYMENT_BUILD_RECONCILIATION_REQUIRED",

              entityType: "Deployment",

              entityId:
                claim.payload.deploymentId,

              metadata: {
                buildId: claim.buildId,

                reason:
                  "CANCELLATION_OUTCOME_UNCERTAIN",
              },
            },
          });

          return "stale";
        }

        const shouldRetry =
          !cancelling &&
          retryable &&
          !executionMayHaveStarted;

        const finalCode =
          cancelling
            ? "BUILD_CANCELLED"
            : code;

        const updated =
          await tx.deploymentBuild.updateMany({
            where: fence,

            data: {
              status: cancelling
                ? "CANCELLED"
                : shouldRetry
                  ? "QUEUED"
                  : "FAILED",

              failureCode:
                finalCode,

              failureMessage:
                buildFailureMessage(
                  finalCode,
                ),

              executionLeaseToken: null,

              executionLeaseExpiresAt: null,

              completedAt: shouldRetry
                ? null
                : now,
            },
          });

        assertSingleUpdate(updated.count);

        if (!shouldRetry) {
          const transitioned =
            await tx.deployment.updateMany({
              where: {
                ...deploymentScope(scope),

                status:
                  deployment.status,
              },

              data: {
                status: cancelling
                  ? "CANCELLED"
                  : "FAILED",

                failureCode: cancelling
                  ? "CANCELLED"
                  : "BUILD_FAILED",

                failureMessage:
                  buildFailureMessage(
                    finalCode,
                  ),

                finishedAt: now,
              },
            });

          assertSingleUpdate(
            transitioned.count,
          );
        }

        /**
         * Retryable pre-execution failures do not
         * write attempt logs into the shared sequence.
         *
         * The same build identity is reused, so log
         * sequence collisions must be avoided.
         */
        if (
          !shouldRetry &&
          logs.length > 0
        ) {
          await tx.deploymentBuildLog.createMany({
            data: logs.map((entry) => ({
              ...scope,

              buildId: claim.buildId,

              sequence:
                BigInt(entry.sequence),

              stream: entry.stream,

              message: entry.message,
            })),
          });
        }

        await tx.auditLog.create({
          data: {
            workspaceId:
              claim.payload.workspaceId,

            action: cancelling
              ? "DEPLOYMENT_BUILD_CANCELLED"
              : shouldRetry
                ? "DEPLOYMENT_BUILD_RETRY_SCHEDULED"
                : "DEPLOYMENT_BUILD_FAILED",

            entityType: "Deployment",

            entityId:
              claim.payload.deploymentId,

            metadata: {
              buildId: claim.buildId,

              buildAttempt:
                claim.buildAttempt,

              failureCode:
                finalCode,

              retryable:
                shouldRetry,

              reconciliationRequired:
                executionMayHaveStarted,
            },
          },
        });

        if (cancelling) {
          return "cancelled";
        }

        return shouldRetry
          ? "retry"
          : "failed";
      },
    );
  } catch (error) {
    if (
      error instanceof
      StaleBuildOwnershipError
    ) {
      return "stale";
    }

    throw error;
  }
}

// ==================================================
// Deferred artifact cleanup
// ==================================================

function reportDeferredCleanup(
  claim: ExecutionClaim,
  reason: string,
): void {
  /**
   * Never delete an image merely because its tag
   * resembles a CloudPilot-managed artifact.
   *
   * Cleanup requires verified artifact identity,
   * tenant ownership and reconciliation eligibility.
   */
  workerLogger.warn(
    {
      workspaceId:
        claim.payload.workspaceId,

      projectId:
        claim.payload.projectId,

      deploymentId:
        claim.payload.deploymentId,

      buildId:
        claim.buildId,

      buildAttempt:
        claim.buildAttempt,

      reason,
    },

    "Build artifact reconciliation required; automatic cleanup deferred",
  );
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

  const valid =
    await validateBuildOutbox(
      job,
      payload,
    );

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

  if (
    payload.resolvedCommitSha.length !== 40 ||
    !SHA_PATTERN.test(
      payload.resolvedCommitSha,
    )
  ) {
    throw new UnrecoverableError(
      "Build commit identity is invalid.",
    );
  }

  if (signal.aborted) {
    throw new TransientJobError(
      "Build worker execution was interrupted.",
    );
  }

  // ================================================
  // Skip terminal deployments
  // ================================================

  const existing =
    await prisma.deployment.findFirst({
      where: deploymentScope(buildScope(payload)),

      select: {
        status: true,
      },
    });

  if (
    !existing ||
    BUILD_TERMINAL_DEPLOYMENT_STATUSES.has(
      existing.status,
    )
  ) {
    return {
      status: "skipped",
    };
  }

  // ================================================
  // Claim the build (fenced)
  // ================================================

  const claimed = await claimBuild(
    payload,

    getMaximumBuildAttempts(job),
  );

  if (claimed.status === "skipped") {
    return {
      status: "skipped",
    };
  }

  if (claimed.status === "uncertain") {
    workerLogger.warn(
      {
        deploymentId:
          payload.deploymentId,

        buildId:
          payload.buildId,
      },

      "Build ownership is uncertain; automatic execution refused",
    );

    throw new UnrecoverableError(
      "Build execution requires reconciliation.",
    );
  }

  if (claimed.status === "busy") {
    throw new TransientJobError(
      "Build execution ownership is unavailable.",
    );
  }

  if (claimed.status !== "claimed") {
    throw new UnrecoverableError(
      "Unexpected build claim result.",
    );
  }

  const claim = claimed.claim;

  const monitor =
    createBuildMonitor(
      claim,
      signal,
    );

  let executionMayHaveStarted = false;

  let logs: BuildLogEntry[] = [];

  let logWriter: DeploymentLogWriter | null = null;

  let logWriterOwnershipLost = false;

  /**
   * Stop ingestion and drain accepted records before
   * releasing the build execution lease.
   *
   * Logging failures must not trigger another Docker
   * build or override an authoritative build result.
   */
  async function closeBuildLogWriter(): Promise<void> {
    const writer = logWriter;

    logWriter = null;

    if (!writer) {
      return;
    }

    try {
      await writer.stop();
    } catch {
      workerLogger.warn(
        {
          deploymentId: payload.deploymentId,

          buildId: claim.buildId,

          buildAttempt: claim.buildAttempt,
        },

        "Build log writer shutdown failed",
      );
    } finally {
      if (writer.hasLostOwnership) {
        logWriterOwnershipLost = true;
      }
    }
  }

  let artifact:
    | VerifiedBuildArtifact
    | undefined;

  let executionFailure:
    | BuildError
    | undefined;

  try {
    // ==============================================
    // Resolve authorized source
    // ==============================================

    try {
      const source =
        await resolveBuildSource(
          payload,

          options.workspace,
        );

      if (monitor.signal.aborted) {
        throw new BuildError(
          monitor.state === "cancelling"
            ? "BUILD_CANCELLED"
            : "BUILD_LEASE_LOST",
        );
      }

      /**
       * Revalidate ownership immediately before
       * crossing the external execution boundary.
       */
      const ownership =
        await renewBuildLease(claim);

      if (ownership !== "active") {
        throw new BuildError(
          ownership === "cancelling"
            ? "BUILD_CANCELLED"
            : "BUILD_LEASE_LOST",
        );
      }

      if (monitor.signal.aborted) {
        throw new BuildError(
          "BUILD_LEASE_LOST",
        );
      }

      // ==============================================
      // Execute the trusted local build
      // ==============================================

      /**
       * Once runBuild is invoked, conservatively assume
       * Docker may have received a build request.
       *
       * Do not retry uncertain external side effects.
       */
      // External execution is marked immediately
      // before invoking runBuild.

      // ==============================================
      // Live build log streaming (Phase 5.6)
      // ==============================================
      //
      // Persist sanitized build output to the unified log
      // store as it is produced. This does not block the
      // build and does not retry Docker on log failures.
      // ==============================================

      if (workerEnv.DEPLOYMENT_LOGS_ENABLED) {
        try {
          logWriter = new DeploymentLogWriter({
            workspaceId: payload.workspaceId,

            projectId: payload.projectId,

            deploymentId: payload.deploymentId,

            buildId: claim.buildId,

            buildExecutionFence: {
              buildAttempt: claim.buildAttempt,

              executionLeaseToken: claim.leaseToken,
            },

            maxEntryBytes:
              workerEnv.LOG_MAX_ENTRY_BYTES,

            batchSize: workerEnv.LOG_BATCH_SIZE,

            flushIntervalMs:
              workerEnv.LOG_FLUSH_INTERVAL_MS,

            maxBufferedBytes: Math.min(
              workerEnv.LOG_MAX_DEPLOYMENT_BYTES,
              8 * 1024 * 1024,
            ),

            maxBufferedEntries: 20_000,
          });

          emitSystemLog(
            logWriter,
            "Build execution starting.",
          );
        } catch {
          await closeBuildLogWriter();

          workerLogger.warn(
            {
              deploymentId: payload.deploymentId,

              buildId: claim.buildId,

              buildAttempt: claim.buildAttempt,
            },

            "Live build logging unavailable; continuing with bounded build log collection",
          );
        }
      }

      if (
        monitor.signal.aborted ||
        logWriterOwnershipLost
      ) {
        throw new BuildError(
          monitor.state === "cancelling"
            ? "BUILD_CANCELLED"
            : "BUILD_LEASE_LOST",
        );
      }

      /**
       * External execution boundary.
       *
       * Never retry automatically once Docker may
       * have received an execution request.
       */
      executionMayHaveStarted = true;

      const result = await runBuild({
        sourceDir:
          source.sourceDir,

        projectId:
          payload.projectId,

        deploymentId:
          payload.deploymentId,

        resolvedCommitSha:
          payload.resolvedCommitSha,

        buildAttempt:
          claim.buildAttempt,

        dockerfilePath:
          source.dockerfilePath,

        buildContextPath:
          source.buildContextPath,

        trustedRepository:
          source.trustedRepository,

        buildArgs: {},

        target: null,

        platform:
          workerEnv.BUILD_PLATFORM,

        limits: {
          timeoutMs:
            workerEnv.BUILD_TIMEOUT_MS,

          maxContextBytes:
            workerEnv.BUILD_MAX_CONTEXT_BYTES,

          maxFileCount:
            workerEnv.SOURCE_MAX_FILE_COUNT,

          maxLogBytes:
            workerEnv.BUILD_MAX_LOG_BYTES,
        },

        executor:
          options.executor,

        signal:
          monitor.signal,

        ...(logWriter
          ? {
              onLogEntry: (
                entry: BuildLogEntry,
              ) => {
                const record: CollectedLogRecord = {
                  source: "BUILD",

                  stream:
                    entry.stream === "stderr"
                      ? "STDERR"
                      : entry.stream === "system"
                        ? "SYSTEM"
                        : "STDOUT",

                  level: inferLogLevel(
                    entry.message,
                    entry.stream,
                  ),

                  message: entry.message,

                  eventTimestamp: new Date(),
                };

                const writer = logWriter;

                if (
                  !writer ||
                  writer.hasLostOwnership ||
                  monitor.signal.aborted
                ) {
                  return;
                }

                writer.enqueue(record);
              },
            }
          : {}),
      });

      logs = result.logEntries;

      artifact = {
        imageTag:
          result.imageTag,

        imageId:
          result.imageId,

        imageDigest:
          result.imageDigest,

        imageSizeBytes:
          result.imageSizeBytes,

        platform:
          result.platform,

        durationMs:
          result.durationMs,
      };

      assertVerifiedArtifact(artifact);
    } catch (error) {
      if (error instanceof BuildEngineError) {
        logs = error.details.logEntries;
      }

      const code =
        getBuildFailureCode(error);

      executionFailure =
        error instanceof BuildError
          ? error
          : new BuildError(
              code,

              buildFailureMessage(code),
            );
    }

    // ==============================================
    // Drain live logs before releasing execution
    // ownership through authoritative settlement
    // ==============================================

    if (logWriter && !monitor.signal.aborted) {
      emitSystemLog(
        logWriter,
        "Build execution ended; verifying final deployment state.",
      );
    }

    await closeBuildLogWriter();

    if (logWriterOwnershipLost) {
      workerLogger.warn(
        {
          deploymentId: payload.deploymentId,

          buildId: claim.buildId,

          buildAttempt: claim.buildAttempt,
        },

        "Build log writer rejected stale execution ownership",
      );
    }

    // ==============================================
    // Verify ownership before settlement
    // ==============================================

    if (
      monitor.state === "lost" ||
      monitor.state === "unavailable"
    ) {
      if (executionMayHaveStarted) {
        reportDeferredCleanup(
          claim,
          "EXECUTION_OWNERSHIP_UNCERTAIN",
        );
      }

      throw new UnrecoverableError(
        "Build ownership could not be verified; reconciliation required.",
      );
    }

    let ownership: ExecutionState;

    try {
      ownership =
        await renewBuildLease(claim);
    } catch {
      if (executionMayHaveStarted) {
        reportDeferredCleanup(
          claim,
          "LEASE_RENEWAL_FAILED",
        );
      }

      throw new UnrecoverableError(
        "Build lease verification failed; reconciliation required.",
      );
    }

    if (
      ownership === "lost" ||
      ownership === "unavailable"
    ) {
      if (executionMayHaveStarted) {
        reportDeferredCleanup(
          claim,
          "SETTLEMENT_OWNERSHIP_LOST",
        );
      }

      throw new UnrecoverableError(
        "Build execution ownership was lost.",
      );
    }

    // ==============================================
    // Publish verified success
    // ==============================================

    if (
      artifact &&
      !executionFailure
    ) {
      if (
        signal.aborted &&
        ownership !== "cancelling"
      ) {
        reportDeferredCleanup(
          claim,
          "WORKER_INTERRUPTED_AFTER_BUILD",
        );

        throw new UnrecoverableError(
          "Build completed during worker interruption; reconciliation required.",
        );
      }

      let settled: SettlementResult;

      try {
        settled =
          await settleSuccess(
            claim,

            artifact,

            logs,
          );
      } catch {
        /**
         * A failed transaction response can represent
         * an uncertain commit outcome.
         *
         * Never settle the same operation as failed
         * or rebuild automatically from this catch.
         */
        reportDeferredCleanup(
          claim,
          "SUCCESS_SETTLEMENT_UNCERTAIN",
        );

        throw new UnrecoverableError(
          "Build success settlement is uncertain.",
        );
      }

      if (settled === "succeeded") {
        // Authoritative success has been committed.
        // The execution-scoped writer is already closed.

        workerLogger.info(
          {
            deploymentId:
              payload.deploymentId,

            buildId:
              claim.buildId,

            buildAttempt:
              claim.buildAttempt,

            imageTag:
              artifact.imageTag,

            durationMs:
              artifact.durationMs,
          },

          "Deployment build succeeded",
        );

        return {
          status: "succeeded",
        };
      }

      if (settled === "cancelled") {
        // Cancellation has been committed.
        // Do not write using the released execution lease.

        reportDeferredCleanup(
          claim,
          "CANCELLED_AFTER_BUILD_COMPLETION",
        );

        return {
          status: "cancelled",
        };
      }

      reportDeferredCleanup(
        claim,
        "STALE_SUCCESS_SETTLEMENT",
      );

      throw new UnrecoverableError(
        "Build success ownership was lost.",
      );
    }

    // ==============================================
    // Publish verified failure
    // ==============================================

    const buildError =
      executionFailure ??
      new BuildError(
        "UNEXPECTED_BUILD_FAILURE",
      );

    const retryable =
      !executionMayHaveStarted &&
      !signal.aborted &&
      ownership === "active" &&
      buildError.retryable &&
      isRetryableBuildFailure(
        buildError.code,
      ) &&
      hasBuildRetryBudget(
        job,

        claim.buildAttempt,
      );

    // Failure, cancellation and retry outcomes are
    // determined by the fenced settlement transaction.
    // Do not emit an uncommitted terminal log event.

    let settled: SettlementResult;

    try {
      settled = await settleFailure(
        claim,

        buildError.code,

        retryable,

        logs,

        executionMayHaveStarted,
      );
    } catch {
      if (executionMayHaveStarted) {
        reportDeferredCleanup(
          claim,
          "FAILURE_SETTLEMENT_UNCERTAIN",
        );
      }

      throw new UnrecoverableError(
        "Build failure settlement is uncertain.",
      );
    }

    if (settled === "cancelled") {
      return {
        status: "cancelled",
      };
    }

    if (settled === "retry") {
      throw new TransientJobError(
        `Build failed: ${buildError.code}`,
      );
    }

    if (executionMayHaveStarted) {
      reportDeferredCleanup(
        claim,
        "BUILD_EXECUTION_FAILED",
      );
    }

    if (settled === "stale") {
      throw new UnrecoverableError(
        "Build settlement requires reconciliation.",
      );
    }

    throw new UnrecoverableError(
      `Build failed: ${buildError.code}`,
    );
  } finally {
    try {
      await closeBuildLogWriter();
    } finally {
      try {
        await monitor.stop();
      } catch {
        workerLogger.warn(
          {
            deploymentId: payload.deploymentId,

            buildId: claim.buildId,

            buildAttempt: claim.buildAttempt,
          },

          "Build execution monitor shutdown failed",
        );
      }
    }
  }
}
