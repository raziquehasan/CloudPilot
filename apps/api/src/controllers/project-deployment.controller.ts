import { prisma } from "@cloudpilot/database";

import { Router } from "express";
import type { Request, Response } from "express";

import {
  requireAuth,
  requireCsrf,
} from "../middlewares/auth.middleware.js";

import {
  canManage,
  resolveProject,
  sendError,
} from "./project-build-configuration.controller.js";

import {
  createDeploymentSchema,
  deploymentIdSchema,
  deploymentListQuerySchema,
  idempotencyKeySchema,
} from "../validators/deployment.validator.js";

import {
  getDeploymentReadiness,
  hasAvailableDeploymentWorker,
} from "../services/deployment-readiness.service.js";

import {
  createDeploymentWithOutbox,
} from "../services/deployment-outbox.service.js";

import {
  resolveImmutableCommit,
} from "../services/commit-resolution.service.js";

import {
  GitHubServiceError,
} from "../services/github-app.service.js";

import {
  removeQueuedDeploymentJob,
} from "../services/queue-producer.js";

import { logger } from "../config/logger.js";

// ==================================================
// Constants
// ==================================================

const COMMIT_SHA_PATTERN = /^[0-9a-f]{40}$/u;

const REPOSITORY_OWNER_PATTERN =
  /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$/u;

const REPOSITORY_NAME_PATTERN =
  /^[A-Za-z0-9._-]{1,100}$/u;

// ==================================================
// Public deployment selection
// ==================================================

const deploymentSelect = {
  id: true,
  projectId: true,
  environment: true,
  status: true,
  triggerSource: true,
  requestedBranch: true,
  gitRef: true,
  commitSha: true,
  commitMessage: true,
  queueJobId: true,
  buildConfigurationSnapshot: true,
  failureCode: true,
  failureMessage: true,
  previousDeploymentId: true,
  rollbackOfId: true,
  createdAt: true,
  startedAt: true,
  finishedAt: true,
  updatedAt: true,
  deploymentBuilds: {
    select: {
      id: true,
      buildAttempt: true,
      status: true,
      builderType: true,
      dockerfilePath: true,
      imageTag: true,
      imageId: true,
      imageDigest: true,
      artifactType: true,
      platform: true,
      imageSizeBytes: true,
      failureCode: true,
      failureMessage: true,
      startedAt: true,
      completedAt: true,
      durationMs: true,
    },
    orderBy: { buildAttempt: "desc" },
    take: 1,
  },
} as const;

// ==================================================
// BigInt-safe serialization
// ==================================================

type DeploymentRow = {
  deploymentBuilds: Array<{
    id: string;
    buildAttempt: number;
    status: string;
    builderType: string | null;
    dockerfilePath: string | null;
    imageTag: string | null;
    imageId: string | null;
    imageDigest: string | null;
    artifactType: string | null;
    platform: string | null;
    imageSizeBytes: bigint | null;
    failureCode: string | null;
    failureMessage: string | null;
    startedAt: Date | null;
    completedAt: Date | null;
    durationMs: number | null;
  }>;
};

function serializeDeployment(row: DeploymentRow) {
  const { deploymentBuilds, ...rest } = row;

  const latest = deploymentBuilds[0] ?? null;

  return {
    ...rest,

    build: latest
      ? {
          id: latest.id,
          buildAttempt: latest.buildAttempt,
          status: latest.status,
          builderType: latest.builderType,
          dockerfilePath: latest.dockerfilePath,
          imageTag: latest.imageTag,
          imageId: latest.imageId,
          imageDigest: latest.imageDigest,
          artifactType: latest.artifactType,
          platform: latest.platform,
          imageSizeBytes:
            latest.imageSizeBytes === null
              ? null
              : latest.imageSizeBytes.toString(),
          failureCode: latest.failureCode,
          failureMessage: latest.failureMessage,
          startedAt: latest.startedAt,
          completedAt: latest.completedAt,
          durationMs: latest.durationMs,
        }
      : null,
  };
}

// ==================================================
// Internal contracts
// ==================================================

interface RepositoryIdentity {
  owner: string;
  repo: string;
}

interface DeploymentRequestIdentity {
  branch?: string | null | undefined;
  commit?: string | null | undefined;
}

interface ExistingDeploymentIdentity {
  requestedBranch: string | null;
  commitSha: string | null;
}

// ==================================================
// Sanitized error utilities
// ==================================================

function getErrorName(error: unknown): string {
  if (
    error instanceof Error &&
    /^[A-Za-z0-9_-]{1,80}$/u.test(error.name)
  ) {
    return error.name;
  }

  return "UnknownError";
}

function getErrorCode(
  error: unknown,
): string | undefined {
  if (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    typeof error.code === "string" &&
    /^[A-Za-z0-9_-]{1,80}$/u.test(error.code)
  ) {
    return error.code;
  }

  return undefined;
}

function sanitizedError(error: unknown) {
  return {
    errorName: getErrorName(error),
    errorCode: getErrorCode(error),
  };
}

// ==================================================
// Response security
// ==================================================

function disableResponseCaching(
  res: Response,
): void {
  res.set("Cache-Control", "no-store");
}

// ==================================================
// Repository identity validation
// ==================================================

function parseRepositoryFullName(
  fullName: string,
): RepositoryIdentity | null {
  const parts = fullName.split("/");

  if (parts.length !== 2) {
    return null;
  }

  const [owner, repo] = parts;

  if (
    !owner ||
    !repo ||
    !REPOSITORY_OWNER_PATTERN.test(owner) ||
    !REPOSITORY_NAME_PATTERN.test(repo) ||
    repo === "." ||
    repo === ".."
  ) {
    return null;
  }

  return {
    owner,
    repo,
  };
}

// ==================================================
// Idempotency validation
// ==================================================

function matchesDeploymentRequest(
  existing: ExistingDeploymentIdentity,
  request: DeploymentRequestIdentity,
): boolean {
  if (
    request.branch &&
    existing.requestedBranch !== request.branch
  ) {
    return false;
  }

  if (request.commit) {
    const requestedCommit =
      request.commit.toLowerCase();

    const existingCommit =
      existing.commitSha?.toLowerCase();

    if (
      !existingCommit ||
      requestedCommit !== existingCommit
    ) {
      return false;
    }

    // A commit-only request must not silently
    // reuse a branch-based deployment.

    if (
      !request.branch &&
      existing.requestedBranch !== null
    ) {
      return false;
    }
  }

  return true;
}

function sendIdempotencyConflict(
  res: Response,
): void {
  sendError(
    res,
    409,
    "IDEMPOTENCY_KEY_REUSED",
    "This Idempotency-Key was already used for a different deployment request.",
  );
}

// ==================================================
// Safe GitHub error responses
// ==================================================

function sendCommitResolutionError(
  res: Response,
  error: GitHubServiceError,
): void {
  if (error.status === 429) {
    sendError(
      res,
      429,
      "COMMIT_RESOLUTION_FAILED",
      "GitHub rate limit was reached. Please try again later.",
    );

    return;
  }

  if (
    error.status === 401 ||
    error.status === 403
  ) {
    sendError(
      res,
      422,
      "COMMIT_RESOLUTION_FAILED",
      "GitHub repository access could not be verified.",
    );

    return;
  }

  if (error.status === 404) {
    sendError(
      res,
      422,
      "COMMIT_RESOLUTION_FAILED",
      "The requested repository, branch or commit is unavailable.",
    );

    return;
  }

  if (
    error.status === 408 ||
    error.status >= 500
  ) {
    sendError(
      res,
      503,
      "COMMIT_RESOLUTION_FAILED",
      "GitHub is temporarily unavailable. Please try again later.",
    );

    return;
  }

  sendError(
    res,
    422,
    "COMMIT_RESOLUTION_FAILED",
    "The requested immutable commit could not be resolved.",
  );
}

// ==================================================
// Create deployment
// ==================================================

async function createDeployment(
  req: Request,
  res: Response,
): Promise<void> {
  disableResponseCaching(res);

  const scope = await resolveProject(req, res);

  if (!scope) {
    return;
  }

  // ================================================
  // Authentication and authorization
  // ================================================

  const actorId = req.userId;

  if (!actorId) {
    sendError(
      res,
      401,
      "UNAUTHORIZED",
      "Authentication is required.",
    );

    return;
  }

  if (!canManage(scope.role)) {
    sendError(
      res,
      403,
      "FORBIDDEN",
      "Workspace administrator access required to deploy this project.",
    );

    return;
  }

  // ================================================
  // Validate deployment request
  // ================================================

  const request =
    createDeploymentSchema.safeParse(req.body);

  if (!request.success) {
    sendError(
      res,
      400,
      "INVALID_DEPLOYMENT_REQUEST",
      "Choose a valid branch or commit and deployment environment.",
    );

    return;
  }

  const key = idempotencyKeySchema.safeParse(
    req.get("Idempotency-Key"),
  );

  if (!key.success) {
    sendError(
      res,
      400,
      "IDEMPOTENCY_KEY_REQUIRED",
      "Provide a valid Idempotency-Key header.",
    );

    return;
  }

  const requestData = request.data;

  const requestedCommit =
    requestData.commit?.toLowerCase() ?? null;

  // Phase 5.3 uses complete immutable Git SHAs.

  if (
    requestedCommit &&
    !COMMIT_SHA_PATTERN.test(requestedCommit)
  ) {
    sendError(
      res,
      400,
      "INVALID_DEPLOYMENT_REQUEST",
      "Provide a complete 40-character commit SHA.",
    );

    return;
  }

  const idempotencyScope = {
    workspaceId: scope.workspaceId,
    projectId: scope.projectId,
    environment: requestData.environment,
    idempotencyKey: key.data,
  };

  // ================================================
  // Existing idempotent deployment
  // ================================================

  const existing =
    await prisma.deployment.findFirst({
      where: idempotencyScope,
      select: deploymentSelect,
    });

  if (existing) {
    if (
      !matchesDeploymentRequest(
        existing,
        requestData,
      )
    ) {
      sendIdempotencyConflict(res);
      return;
    }

    res.status(200).json({
      success: true,
      data: serializeDeployment(existing),
      idempotent: true,
    });

    return;
  }

  // ================================================
  // Resolve tenant-scoped repository
  // ================================================

  const repositoryScope = {
    workspaceId: scope.workspaceId,
    projectId: scope.projectId,
  };

  const repository =
    await prisma.repository.findFirst({
      where: repositoryScope,

      select: {
        fullName: true,
        selectedBranch: true,

        installation: {
          select: {
            installationId: true,
            status: true,
          },
        },
      },
    });

  if (
    !repository?.fullName ||
    !repository.installation?.installationId
  ) {
    sendError(
      res,
      409,
      "REPOSITORY_NOT_CONNECTED",
      "Connect an authorized GitHub repository before deploying.",
    );

    return;
  }

  if (
    repository.installation.status !== "ACTIVE"
  ) {
    sendError(
      res,
      409,
      "INSTALLATION_UNAVAILABLE",
      "The GitHub App installation is not active.",
    );

    return;
  }

  const repositoryIdentity =
    parseRepositoryFullName(repository.fullName);

  if (!repositoryIdentity) {
    sendError(
      res,
      409,
      "REPOSITORY_NOT_CONNECTED",
      "The connected repository configuration is invalid.",
    );

    return;
  }

  const installationId =
    repository.installation.installationId;

  // ================================================
  // Resolve requested branch or commit
  // ================================================
  //
  // Explicit commit-only requests do not inherit
  // the project's selected branch.
  //
  // Branch fallback is used only when the request
  // does not specify an immutable commit.
  // ================================================

  const branch =
    requestData.branch ??
    (
      requestedCommit
        ? null
        : repository.selectedBranch
    );

  if (!branch && !requestedCommit) {
    sendError(
      res,
      400,
      "BRANCH_REQUIRED",
      "Select a branch or commit to deploy.",
    );

    return;
  }

  // ================================================
  // Check deployment worker availability
  // ================================================

  let workerAvailable: boolean;

  try {
    workerAvailable =
      await hasAvailableDeploymentWorker();
  } catch (error) {
    logger.warn(
      {
        projectId: scope.projectId,
        ...sanitizedError(error),
      },
      "Deployment worker availability check failed",
    );

    workerAvailable = false;
  }

  if (!workerAvailable) {
    sendError(
      res,
      503,
      "DEPLOYMENT_WORKER_UNAVAILABLE",
      "Deployment creation is disabled because no compatible deployment worker is registered.",
    );

    return;
  }

  // ================================================
  // Resolve immutable GitHub commit
  // ================================================

  let resolved: Awaited<
    ReturnType<typeof resolveImmutableCommit>
  >;

  try {
    resolved = await resolveImmutableCommit({
      installationId,

      owner: repositoryIdentity.owner,

      repo: repositoryIdentity.repo,

      expectedFullName: repository.fullName,

      requestedBranch: branch,

      requestedCommit,
    });
  } catch (error) {
    if (error instanceof GitHubServiceError) {
      logger.warn(
        {
          projectId: scope.projectId,
          githubStatus: error.status,
        },
        "GitHub commit resolution rejected",
      );

      sendCommitResolutionError(res, error);

      return;
    }

    logger.error(
      {
        projectId: scope.projectId,
        ...sanitizedError(error),
      },
      "Commit resolution failed",
    );

    sendError(
      res,
      503,
      "COMMIT_RESOLUTION_FAILED",
      "The commit could not be resolved right now.",
    );

    return;
  }

  // ================================================
  // Validate resolved immutable identity
  // ================================================

  const resolvedSha =
    resolved.sha.toLowerCase();

  if (
    !COMMIT_SHA_PATTERN.test(resolvedSha)
  ) {
    logger.error(
      {
        projectId: scope.projectId,
      },
      "Commit resolution returned an invalid SHA",
    );

    sendError(
      res,
      503,
      "COMMIT_RESOLUTION_FAILED",
      "GitHub returned an invalid commit identity.",
    );

    return;
  }

  if (
    requestedCommit &&
    resolvedSha !== requestedCommit
  ) {
    sendError(
      res,
      422,
      "COMMIT_RESOLUTION_FAILED",
      "The resolved commit does not match the requested immutable commit.",
    );

    return;
  }

  // ================================================
  // Atomic deployment and outbox creation
  // ================================================

  try {
    const result = await prisma.$transaction(
      async (tx) => {
        // Recheck idempotency after the external
        // GitHub request has completed.

        const concurrentDeployment =
          await tx.deployment.findFirst({
            where: idempotencyScope,
            select: deploymentSelect,
          });

        if (concurrentDeployment) {
          if (
            !matchesDeploymentRequest(
              concurrentDeployment,
              requestData,
            )
          ) {
            return {
              kind: "idempotency_conflict" as const,
            };
          }

          return {
            kind: "existing" as const,
            deployment: concurrentDeployment,
          };
        }

        // Recheck repository configuration before
        // persisting the resolved commit.
        //
        // This reduces the configuration-change
        // window during external GitHub requests.

        const currentRepository =
          await tx.repository.findFirst({
            where: repositoryScope,

            select: {
              fullName: true,
              selectedBranch: true,

              installation: {
                select: {
                  installationId: true,
                  status: true,
                },
              },
            },
          });

        if (
          !currentRepository ||
          currentRepository.fullName !==
            repository.fullName ||
          currentRepository.installation
            ?.installationId !== installationId ||
          currentRepository.installation
            ?.status !== "ACTIVE"
        ) {
          return {
            kind: "repository_changed" as const,
          };
        }

        // If the request relied on the project's
        // selected branch, ensure it has not changed.

        if (
          !requestData.branch &&
          !requestedCommit &&
          currentRepository.selectedBranch !== branch
        ) {
          return {
            kind: "repository_changed" as const,
          };
        }

        const deployment =
          await createDeploymentWithOutbox(
            tx,
            {
              workspaceId: scope.workspaceId,

              projectId: scope.projectId,

              environment:
                requestData.environment,

              triggerSource: "MANUAL",

              idempotencyKey: key.data,

              ...(resolved.requestedBranch
                ? {
                    requestedBranch:
                      resolved.requestedBranch,
                  }
                : {}),

              commitSha: resolvedSha,

              commitMessage:
                resolved.commitMessage,

              triggeredByUserId: actorId,
            },
          );

        return {
          kind: "created" as const,
          deployment,
        };
      },
    );

    // ==============================================
    // Handle transactional outcomes
    // ==============================================

    if (
      result.kind === "idempotency_conflict"
    ) {
      sendIdempotencyConflict(res);
      return;
    }

    if (result.kind === "repository_changed") {
      sendError(
        res,
        409,
        "REPOSITORY_CONFIGURATION_CHANGED",
        "Repository configuration changed during commit resolution. Please retry.",
      );

      return;
    }

    if (result.kind === "existing") {
      res.status(200).json({
        success: true,
        data: serializeDeployment(result.deployment),
        idempotent: true,
      });

      return;
    }

    const deployment = result.deployment;

    logger.info(
      {
        workspaceId: scope.workspaceId,

        projectId: scope.projectId,

        deploymentId: deployment.id,

        commitSha: resolvedSha,
      },
      "Deployment and transactional outbox created",
    );

    res.status(201).json({
      success: true,

      data: {
        id: deployment.id,

        projectId: deployment.projectId,

        environment: deployment.environment,

        status: deployment.status,

        requestedBranch:
          deployment.requestedBranch,

        commitSha: deployment.commitSha,

        commitMessage:
          deployment.commitMessage,

        queueJobId: deployment.queueJobId,

        createdAt: deployment.createdAt,
      },
    });
  } catch (error) {
    // ==============================================
    // Recover concurrent unique-key conflicts
    // ==============================================
    //
    // A database uniqueness constraint remains
    // the final authority for idempotency.
    //
    // Prisma P2002 represents a unique constraint
    // violation.
    // ==============================================

    if (getErrorCode(error) === "P2002") {
      try {
        const concurrentDeployment =
          await prisma.deployment.findFirst({
            where: idempotencyScope,
            select: deploymentSelect,
          });

        if (concurrentDeployment) {
          if (
            !matchesDeploymentRequest(
              concurrentDeployment,
              requestData,
            )
          ) {
            sendIdempotencyConflict(res);
            return;
          }

          res.status(200).json({
            success: true,

            data: serializeDeployment(concurrentDeployment),

            idempotent: true,
          });

          return;
        }
      } catch (recoveryError) {
        logger.error(
          {
            projectId: scope.projectId,

            ...sanitizedError(recoveryError),
          },
          "Deployment idempotency recovery failed",
        );
      }
    }

    logger.error(
      {
        workspaceId: scope.workspaceId,

        projectId: scope.projectId,

        ...sanitizedError(error),
      },
      "Deployment creation failed",
    );

    sendError(
      res,
      503,
      "DEPLOYMENT_CREATE_FAILED",
      "The deployment could not be created right now. Retry using the same Idempotency-Key.",
    );
  }
}

// ==================================================
// Deployment readiness
// ==================================================

async function getReadiness(
  req: Request,
  res: Response,
): Promise<void> {
  disableResponseCaching(res);

  const scope = await resolveProject(req, res);

  if (!scope) {
    return;
  }

  try {
    const readiness =
      await getDeploymentReadiness(
        scope.workspaceId,
        scope.projectId,
      );

    res.json({
      success: true,
      data: readiness,
    });
  } catch (error) {
    logger.error(
      {
        projectId: scope.projectId,

        ...sanitizedError(error),
      },
      "Deployment readiness check failed",
    );

    sendError(
      res,
      503,
      "DEPLOYMENT_READINESS_UNAVAILABLE",
      "Deployment readiness could not be determined.",
    );
  }
}

// ==================================================
// List deployments
// ==================================================

async function listDeployments(
  req: Request,
  res: Response,
): Promise<void> {
  disableResponseCaching(res);

  const scope = await resolveProject(req, res);

  if (!scope) {
    return;
  }

  const query =
    deploymentListQuerySchema.safeParse(
      req.query,
    );

  if (!query.success) {
    sendError(
      res,
      400,
      "INVALID_DEPLOYMENT_QUERY",
      "Use a valid status, page, and page size.",
    );

    return;
  }

  const {
    page,
    perPage,
    status,
  } = query.data;

  const offset = (page - 1) * perPage;

  if (
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    offset > 2_147_483_647
  ) {
    sendError(
      res,
      400,
      "INVALID_DEPLOYMENT_QUERY",
      "The requested pagination range is invalid.",
    );

    return;
  }

  const where = {
    workspaceId: scope.workspaceId,

    projectId: scope.projectId,

    ...(status ? { status } : {}),
  };

  try {
    const [total, items] =
      await prisma.$transaction(
        [
          prisma.deployment.count({
            where,
          }),

          prisma.deployment.findMany({
            where,

            orderBy: [
              { createdAt: "desc" },
              { id: "desc" },
            ],

            skip: offset,

            take: perPage,

            select: deploymentSelect,
          }),
        ],
        {
          isolationLevel: "RepeatableRead",
        },
      );

    res.json({
      success: true,

      data: {
        items: items.map(serializeDeployment),

        page,

        perPage,

        total,

        totalPages:
          Math.ceil(total / perPage),
      },
    });
  } catch (error) {
    logger.error(
      {
        projectId: scope.projectId,

        ...sanitizedError(error),
      },
      "Deployment list retrieval failed",
    );

    sendError(
      res,
      503,
      "DEPLOYMENT_LIST_UNAVAILABLE",
      "Deployments could not be retrieved right now.",
    );
  }
}

// ==================================================
// Get individual deployment
// ==================================================

async function getDeployment(
  req: Request,
  res: Response,
): Promise<void> {
  disableResponseCaching(res);

  const scope = await resolveProject(req, res);

  if (!scope) {
    return;
  }

  const deploymentId =
    deploymentIdSchema.safeParse(
      req.params.deploymentId,
    );

  if (!deploymentId.success) {
    sendError(
      res,
      404,
      "DEPLOYMENT_NOT_FOUND",
      "Deployment not found in this project.",
    );

    return;
  }

  try {
    const deployment =
      await prisma.deployment.findFirst({
        where: {
          id: deploymentId.data,

          workspaceId: scope.workspaceId,

          projectId: scope.projectId,
        },

        select: deploymentSelect,
      });

    if (!deployment) {
      sendError(
        res,
        404,
        "DEPLOYMENT_NOT_FOUND",
        "Deployment not found in this project.",
      );

      return;
    }

    res.json({
      success: true,
      data: serializeDeployment(deployment),
    });
  } catch (error) {
    logger.error(
      {
        projectId: scope.projectId,

        deploymentId: deploymentId.data,

        ...sanitizedError(error),
      },

      "Deployment retrieval failed",
    );

    sendError(
      res,
      503,
      "DEPLOYMENT_UNAVAILABLE",
      "Deployment details could not be retrieved.",
    );
  }
}

// ==================================================
// Cancel deployment
// ==================================================

async function cancelDeployment(
  req: Request,
  res: Response,
): Promise<void> {
  disableResponseCaching(res);

  const scope = await resolveProject(req, res);

  if (!scope) {
    return;
  }

  const actorId = req.userId;

  if (!actorId) {
    sendError(
      res,
      401,
      "UNAUTHORIZED",
      "Authentication is required.",
    );

    return;
  }

  if (!canManage(scope.role)) {
    sendError(
      res,
      403,
      "FORBIDDEN",
      "Workspace administrator access required to cancel this deployment.",
    );

    return;
  }

  const deploymentId =
    deploymentIdSchema.safeParse(
      req.params.deploymentId,
    );

  if (!deploymentId.success) {
    sendError(
      res,
      404,
      "DEPLOYMENT_NOT_FOUND",
      "Deployment not found in this project.",
    );

    return;
  }

  const deploymentScope = {
    id: deploymentId.data,

    workspaceId: scope.workspaceId,

    projectId: scope.projectId,
  };

  const sourceScope = {
    deploymentId: deploymentId.data,

    workspaceId: scope.workspaceId,

    projectId: scope.projectId,
  };

  // ================================================
  // Atomic cancellation transaction
  // ================================================

  try {
    const result = await prisma.$transaction(
      async (tx) => {
        const current =
          await tx.deployment.findFirst({
            where: deploymentScope,

            select: {
              status: true,

              queueJobId: true,
            },
          });

        if (!current) {
          return {
            kind: "not_found" as const,
          };
        }

        const queued =
          current.status === "QUEUED" ||
          current.status === "PENDING";

        const preparing =
          current.status === "PREPARING";

        // Preserve the existing Phase 5.3
        // cancellation lifecycle.

        if (!queued && !preparing) {
          return {
            kind: "conflict" as const,
          };
        }

        const nextStatus = queued
          ? "CANCELLED"
          : "CANCELLING";

        const now = new Date();

        // ==========================================
        // Compare-and-set deployment transition
        // ==========================================

        const updated =
          await tx.deployment.updateMany({
            where: {
              ...deploymentScope,

              status: current.status,
            },

            data: {
              status: nextStatus,

              failureCode: "CANCELLED",

              failureMessage: queued
                ? "Deployment was cancelled by a workspace administrator."
                : "Cancellation was requested; the worker will stop cooperatively.",

              ...(queued
                ? {
                    finishedAt: now,

                    executionLeaseToken: null,

                    executionLeaseExpiresAt: null,
                  }
                : {}),
            },
          });

        if (updated.count !== 1) {
          return {
            kind: "conflict" as const,
          };
        }

        // ==========================================
        // Cancel queued deployment infrastructure
        // ==========================================

        if (queued) {
          await tx.deploymentOutboxEvent.updateMany({
            where: {
              ...sourceScope,

              status: {
                in: [
                  "PENDING",
                  "PUBLISHING",
                ],
              },
            },

            data: {
              status: "DEAD",

              leaseToken: null,

              leaseExpiresAt: null,

              lastErrorCode:
                "DEPLOYMENT_CANCELLED",
            },
          });

          // Synchronize any existing source
          // preparation record.
          //
          // Do not create fabricated metadata.
          // Do not delete READY source here.

          await tx.deploymentSourcePreparation.updateMany({
            where: {
              ...sourceScope,

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
        }

        // Active source preparation is settled
        // by the worker using execution fencing.
        //
        // Never delete an active workspace or
        // invalidate its lease from this endpoint.

        // ==========================================
        // Audit cancellation request
        // ==========================================

        await tx.auditLog.create({
          data: {
            workspaceId: scope.workspaceId,

            actorId,

            action:
              "DEPLOYMENT_CANCELLATION_REQUESTED",

            entityType: "Deployment",

            entityId: deploymentId.data,

            metadata: {
              from: current.status,

              to: nextStatus,

              cancellationMode: queued
                ? "immediate"
                : "cooperative",
            },
          },
        });

        const deployment =
          await tx.deployment.findFirst({
            where: deploymentScope,

            select: deploymentSelect,
          });

        return {
          kind: "updated" as const,

          deployment,

          queued,

          queueJobId: current.queueJobId,
        };
      },
    );

    // ==============================================
    // Cancellation response
    // ==============================================

    if (result.kind === "not_found") {
      sendError(
        res,
        404,
        "DEPLOYMENT_NOT_FOUND",
        "Deployment not found in this project.",
      );

      return;
    }

    if (
      result.kind === "conflict" ||
      !result.deployment
    ) {
      sendError(
        res,
        409,
        "DEPLOYMENT_STATE_CONFLICT",
        "Deployment state changed or is not cancellable.",
      );

      return;
    }

    // ==============================================
    // Best-effort BullMQ job removal
    // ==============================================
    //
    // PostgreSQL is the source of truth.
    //
    // Queue removal must happen after the database
    // transaction commits.
    // ==============================================

    if (
      result.queued &&
      result.queueJobId
    ) {
      try {
        await removeQueuedDeploymentJob(
          result.queueJobId,
        );
      } catch (error) {
        logger.warn(
          {
            deploymentId: deploymentId.data,

            ...sanitizedError(error),
          },
          "Queued deployment job removal deferred to reconciliation",
        );
      }
    }

    logger.info(
      {
        workspaceId: scope.workspaceId,

        projectId: scope.projectId,

        deploymentId: deploymentId.data,

        status: result.deployment.status,
      },
      "Deployment cancellation request accepted",
    );

    res.json({
      success: true,

      data: serializeDeployment(result.deployment),
    });
  } catch (error) {
    logger.error(
      {
        projectId: scope.projectId,

        deploymentId: deploymentId.data,

        ...sanitizedError(error),
      },

      "Deployment cancellation failed",
    );

    sendError(
      res,
      503,
      "DEPLOYMENT_CANCELLATION_FAILED",
      "Deployment cancellation could not be confirmed. Refresh the deployment status before retrying.",
    );
  }
}

// ==================================================
// Build logs
// ==================================================

async function getBuildLogs(
  req: Request,
  res: Response,
): Promise<void> {
  disableResponseCaching(res);

  const scope = await resolveProject(req, res);

  if (!scope) {
    return;
  }

  const deploymentId =
    deploymentIdSchema.safeParse(
      req.params.deploymentId,
    );

  if (!deploymentId.success) {
    sendError(
      res,
      404,
      "DEPLOYMENT_NOT_FOUND",
      "Deployment not found in this project.",
    );

    return;
  }

  const deploymentScope = {
    workspaceId: scope.workspaceId,
    projectId: scope.projectId,
    deploymentId: deploymentId.data,
  };

  try {
    const build =
      await prisma.deploymentBuild.findFirst({
        where: deploymentScope,

        orderBy: { buildAttempt: "desc" },

        select: {
          id: true,
          buildAttempt: true,
          status: true,
        },
      });

    if (!build) {
      res.json({
        success: true,
        data: { build: null, logs: [] },
      });

      return;
    }

    const logs =
      await prisma.deploymentBuildLog.findMany({
        where: {
          ...deploymentScope,
          buildId: build.id,
        },

        orderBy: { sequence: "asc" },

        take: 2_000,

        select: {
          sequence: true,
          stream: true,
          message: true,
        },
      });

    res.json({
      success: true,
      data: {
        build: {
          id: build.id,
          buildAttempt: build.buildAttempt,
          status: build.status,
        },

        logs: logs.map((entry) => ({
          sequence: entry.sequence.toString(),
          stream: entry.stream,
          message: entry.message,
        })),
      },
    });
  } catch (error) {
    logger.error(
      {
        projectId: scope.projectId,

        deploymentId: deploymentId.data,

        ...sanitizedError(error),
      },

      "Build log retrieval failed",
    );

    sendError(
      res,
      503,
      "BUILD_LOG_UNAVAILABLE",
      "Build logs could not be retrieved right now.",
    );
  }
}

// ==================================================
// Project deployment router
// ==================================================

export const projectDeploymentRouter =
  Router();

// All deployment routes require authentication.

projectDeploymentRouter.use(requireAuth);

// Create deployment.

projectDeploymentRouter.post(
  "/:projectId/deployments",
  requireCsrf,
  createDeployment,
);

// Deployment readiness.

projectDeploymentRouter.get(
  "/:projectId/deployment-readiness",
  getReadiness,
);

// List deployments.

projectDeploymentRouter.get(
  "/:projectId/deployments",
  listDeployments,
);

// Get deployment details.

projectDeploymentRouter.get(
  "/:projectId/deployments/:deploymentId",
  getDeployment,
);

// Get build logs for the latest build attempt.

projectDeploymentRouter.get(
  "/:projectId/deployments/:deploymentId/build-logs",
  getBuildLogs,
);

// Cancel deployment.

projectDeploymentRouter.post(
  "/:projectId/deployments/:deploymentId/cancel",
  requireCsrf,
  cancelDeployment,
);