/**
 * CloudPilot — Deployment Logs REST Controller
 *
 * Phase 5.6 — Realtime Deployment Logs
 *
 * Authoritative historical log retrieval and export.
 *
 * Mounted under the existing /projects router.
 *
 * Security:
 *
 * - Existing session authentication.
 * - Existing workspace and project authorization.
 * - Exact deployment scope verification.
 * - Strict query parameter validation.
 * - Bounded download response size.
 * - Safe attachment filenames.
 * - Sensitive response cache prevention.
 * - Restricted internal error logging.
 *
 * Endpoints:
 *
 * GET /:projectId/deployments/:deploymentId/logs
 *
 * GET /:projectId/deployments/:deploymentId/logs?after=<cursor>
 *
 * GET /:projectId/deployments/:deploymentId/logs?before=<cursor>
 *
 * GET /:projectId/deployments/:deploymentId/logs?source=BUILD
 *
 * GET /:projectId/deployments/:deploymentId/logs?level=ERROR
 *
 * GET /:projectId/deployments/:deploymentId/logs?download=true
 *
 * GET /:projectId/deployments/:deploymentId/logs/download
 *
 * PostgreSQL remains the authoritative log store.
 */

import { Router } from "express";

import type {
  Request,
  Response,
  NextFunction,
} from "express";

import { prisma } from "@cloudpilot/database";

import {
  logHistoryQuerySchema,
} from "@cloudpilot/queue";

import { env } from "../config/env.js";

import { logger } from "../config/logger.js";

import {
  requireAuth,
} from "../middlewares/auth.middleware.js";

import {
  resolveProject,
  sendError,
} from "./project-build-configuration.controller.js";

import {
  deploymentIdSchema,
} from "../validators/deployment.validator.js";

import {
  downloadLogs,
  getLogHistory,
} from "../services/deployment-logs.service.js";

// ==================================================
// Constants
// ==================================================

const MAX_DOWNLOAD_BYTES =
  16 * 1024 * 1024;

const MAX_CONCURRENT_DOWNLOADS = 4;

const MAX_CONCURRENT_HISTORY_REQUESTS = 32;

const MAX_FILENAME_LENGTH = 160;

const SAFE_FILENAME_PATTERN =
  /^[a-zA-Z0-9][a-zA-Z0-9._-]*\.log$/;

const DOWNLOAD_QUERY_FIELDS = [
  "after",
  "before",
  "source",
  "stream",
  "level",
  "limit",
] as const;

// ==================================================
// Process-local request accounting
// ==================================================

let activeDownloads = 0;

let activeHistoryRequests = 0;

// ==================================================
// Internal contracts
// ==================================================

interface DeploymentLogScope {
  workspaceId: string;

  projectId: string;

  deploymentId: string;
}

type RequestMode =
  | "HISTORY"
  | "DOWNLOAD";

// ==================================================
// Router
// ==================================================

export const projectDeploymentLogsRouter =
  Router();

// ==================================================
// Response security
// ==================================================

function noStore(
  res: Response,
): void {
  res.setHeader(
    "Cache-Control",
    "private, no-store, max-age=0",
  );

  res.setHeader(
    "Pragma",
    "no-cache",
  );

  res.setHeader(
    "Expires",
    "0",
  );

  res.setHeader(
    "X-Content-Type-Options",
    "nosniff",
  );

  res.setHeader(
    "Referrer-Policy",
    "no-referrer",
  );
}

/**
 * Apply security headers before authentication,
 * including unauthorized responses.
 *
 * Restrict this middleware to deployment log
 * endpoints because the router is mounted
 * under the existing projects router.
 */
function secureLogResponse(
  _req: Request,
  res: Response,
  next: NextFunction,
): void {
  noStore(res);

  next();
}

// ==================================================
// Response lifecycle
// ==================================================

function canRespond(
  req: Request,
  res: Response,
): boolean {
  return (
    !req.aborted &&
    !res.destroyed &&
    !res.writableEnded &&
    !res.headersSent
  );
}

// ==================================================
// Safe error responses
// ==================================================

function respondError(
  req: Request,
  res: Response,
  status: number,
  code: string,
  message: string,
): void {
  if (!canRespond(req, res)) {
    return;
  }

  sendError(
    res,
    status,
    code,
    message,
  );
}

function deploymentNotFound(
  req: Request,
  res: Response,
): void {
  respondError(
    req,
    res,
    404,
    "DEPLOYMENT_NOT_FOUND",
    "Deployment not found in this project.",
  );
}

// ==================================================
// Deployment scope verification
// ==================================================

/**
 * Never trust a deployment identifier independently
 * of its authorized workspace and project.
 */
async function deploymentExists(
  scope: DeploymentLogScope,
): Promise<boolean> {
  const deployment =
    await prisma.deployment.findFirst({
      where: {
        id: scope.deploymentId,

        workspaceId: scope.workspaceId,

        projectId: scope.projectId,
      },

      select: {
        id: true,
      },
    });

  return deployment !== null;
}

// ==================================================
// Download filename validation
// ==================================================

function safeDownloadFilename(
  value: unknown,
  deploymentId: string,
): string {
  if (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MAX_FILENAME_LENGTH &&
    SAFE_FILENAME_PATTERN.test(value) &&
    !value.includes("..")
  ) {
    return value;
  }

  // deploymentId has already passed the existing
  // deployment identifier schema.

  return `deployment-${deploymentId}.log`;
}

// ==================================================
// Download body validation
// ==================================================

function downloadSize(
  body: unknown,
): number | null {
  if (typeof body === "string") {
    return Buffer.byteLength(
      body,
      "utf8",
    );
  }

  if (Buffer.isBuffer(body)) {
    return body.byteLength;
  }

  return null;
}

// ==================================================
// Download query policy
// ==================================================

/**
 * Downloads currently export the bounded,
 * unfiltered deployment log snapshot.
 *
 * Reject unsupported filter combinations
 * instead of silently ignoring them.
 */
function hasDownloadFilters(
  query: Request["query"],
): boolean {
  return DOWNLOAD_QUERY_FIELDS.some(
    (field) =>
      Object.hasOwn(query, field),
  );
}

// ==================================================
// Historical log retrieval
// ==================================================

async function sendHistory(
  req: Request,
  res: Response,
  scope: DeploymentLogScope,
  query: ReturnType<
    typeof logHistoryQuerySchema.parse
  >,
): Promise<void> {
  if (!canRespond(req, res)) {
    return;
  }

  if (
    activeHistoryRequests >=
    MAX_CONCURRENT_HISTORY_REQUESTS
  ) {
    res.setHeader(
      "Retry-After",
      "2",
    );

    respondError(
      req,
      res,
      503,
      "LOG_HISTORY_BUSY",
      "Log retrieval is temporarily busy. Please retry.",
    );

    return;
  }

  activeHistoryRequests += 1;

  try {
    const result = await getLogHistory(
      scope,
      {
        after:
          query.after ?? null,

        before:
          query.before ?? null,

        ...(query.source
          ? { source: query.source }
          : {}),

        ...(query.stream
          ? { stream: query.stream }
          : {}),

        ...(query.level
          ? { level: query.level }
          : {}),

        limit: query.limit,
      },
    );

    if (!canRespond(req, res)) {
      return;
    }

    res.status(200).json({
      success: true,

      data: result,
    });
  } finally {
    activeHistoryRequests = Math.max(
      0,
      activeHistoryRequests - 1,
    );
  }
}

// ==================================================
// Deployment log download
// ==================================================

async function sendDownload(
  req: Request,
  res: Response,
  scope: DeploymentLogScope,
): Promise<void> {
  if (!canRespond(req, res)) {
    return;
  }

  if (
    activeDownloads >=
    MAX_CONCURRENT_DOWNLOADS
  ) {
    res.setHeader(
      "Retry-After",
      "5",
    );

    respondError(
      req,
      res,
      503,
      "LOG_DOWNLOAD_BUSY",
      "Too many log downloads are in progress. Please retry.",
    );

    return;
  }

  activeDownloads += 1;

  try {
    const download =
      await downloadLogs(scope);

    if (!canRespond(req, res)) {
      return;
    }

    if (!download) {
      deploymentNotFound(req, res);

      return;
    }

    const bytes = downloadSize(
      download.body,
    );

    if (bytes === null) {
      logger.warn(
        {
          code: "LOG_DOWNLOAD_INVALID_BODY",
        },
        "Invalid deployment log download response",
      );

      respondError(
        req,
        res,
        503,
        "LOG_UNAVAILABLE",
        "Logs could not be downloaded right now.",
      );

      return;
    }

    if (bytes > MAX_DOWNLOAD_BYTES) {
      respondError(
        req,
        res,
        413,
        "LOG_DOWNLOAD_TOO_LARGE",
        "The log export exceeds the maximum download size.",
      );

      return;
    }

    const filename =
      safeDownloadFilename(
        download.filename,
        scope.deploymentId,
      );

    // Force plain-text attachment delivery.
    // Do not trust a dynamic service-provided
    // MIME type for log content.

    res.setHeader(
      "Content-Type",
      "text/plain; charset=utf-8",
    );

    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filename}"`,
    );

    res.setHeader(
      "Content-Security-Policy",
      "default-src 'none'; sandbox",
    );

    res.setHeader(
      "Content-Length",
      String(bytes),
    );

    res.status(200).send(
      download.body,
    );
  } finally {
    activeDownloads = Math.max(
      0,
      activeDownloads - 1,
    );
  }
}

// ==================================================
// Shared request handler
// ==================================================

async function handleLogRequest(
  req: Request,
  res: Response,
  mode: RequestMode,
): Promise<void> {
  noStore(res);

  try {
    if (!canRespond(req, res)) {
      return;
    }

    // Fail closed when realtime deployment
    // logging is disabled.

    if (!env.DEPLOYMENT_LOGS_ENABLED) {
      respondError(
        req,
        res,
        503,
        "DEPLOYMENT_LOGS_DISABLED",
        "Deployment logs are currently disabled.",
      );

      return;
    }

    // Preserve existing project authorization.

    const scope = await resolveProject(
      req,
      res,
    );

    if (
      !scope ||
      !canRespond(req, res)
    ) {
      return;
    }

    // Validate deployment identity.

    const deploymentId =
      deploymentIdSchema.safeParse(
        req.params.deploymentId,
      );

    if (!deploymentId.success) {
      deploymentNotFound(req, res);

      return;
    }

    // Validate all supported query parameters.

    const query =
      logHistoryQuerySchema.safeParse(
        req.query,
      );

    if (!query.success) {
      respondError(
        req,
        res,
        400,
        "VALIDATION_ERROR",
        "Invalid log query parameters.",
      );

      return;
    }

    const wantsDownload =
      mode === "DOWNLOAD" ||
      query.data.download === "true";

    if (
      mode === "DOWNLOAD" &&
      query.data.download === "false"
    ) {
      respondError(
        req,
        res,
        400,
        "VALIDATION_ERROR",
        "Conflicting download parameters.",
      );

      return;
    }

    if (
      wantsDownload &&
      hasDownloadFilters(req.query)
    ) {
      respondError(
        req,
        res,
        400,
        "VALIDATION_ERROR",
        "Log downloads do not support pagination or filters.",
      );

      return;
    }

    // Construct the scope only from
    // authorized project information and
    // the validated deployment identifier.

    const logScope: DeploymentLogScope = {
      workspaceId:
        scope.workspaceId,

      projectId:
        scope.projectId,

      deploymentId:
        deploymentId.data,
    };

    const exists =
      await deploymentExists(
        logScope,
      );

    if (!canRespond(req, res)) {
      return;
    }

    if (!exists) {
      deploymentNotFound(req, res);

      return;
    }

    if (wantsDownload) {
      await sendDownload(
        req,
        res,
        logScope,
      );

      return;
    }

    await sendHistory(
      req,
      res,
      logScope,
      query.data,
    );
  } catch {
    // Never log raw request queries, session
    // cookies, log content or database errors.
    //
    // Internal error details may contain
    // sensitive deployment information.

    logger.error(
      {
        code: "LOG_RETRIEVAL_FAILED",

        mode,
      },
      "Deployment log retrieval failed",
    );

    respondError(
      req,
      res,
      503,
      "LOG_UNAVAILABLE",
      "Logs could not be retrieved right now.",
    );
  }
}

// ==================================================
// Public route handlers
// ==================================================

async function getLogs(
  req: Request,
  res: Response,
): Promise<void> {
  await handleLogRequest(
    req,
    res,
    "HISTORY",
  );
}

async function getLogsDownload(
  req: Request,
  res: Response,
): Promise<void> {
  await handleLogRequest(
    req,
    res,
    "DOWNLOAD",
  );
}

// ==================================================
// Route registration
// ==================================================

/**
 * Authentication is applied explicitly to
 * both log routes.
 *
 * Avoid router-wide authentication middleware
 * affecting unrelated project endpoints.
 */

projectDeploymentLogsRouter.get(
  "/:projectId/deployments/:deploymentId/logs",

  secureLogResponse,

  requireAuth,

  getLogs,
);

projectDeploymentLogsRouter.get(
  "/:projectId/deployments/:deploymentId/logs/download",

  secureLogResponse,

  requireAuth,

  getLogsDownload,
);