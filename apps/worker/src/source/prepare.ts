/**
 * CloudPilot — Secure Source Preparation Orchestrator
 *
 * Coordinates authorized GitHub source retrieval,
 * isolated workspace creation, archive extraction,
 * metadata persistence, cancellation, and cleanup.
 *
 * Repository-controlled code must never be executed
 * by this module.
 */

import { dirname, join } from "node:path";

import {
  SourcePreparationError,
} from "@cloudpilot/queue";

import {
  downloadGitHubArchive,
  removeDownloadArtifact,
} from "./download.js";

import {
  extractTarGzSafely,
} from "./extract.js";

import type {
  WorkerGitHubAuth,
} from "./github-auth.js";

import type {
  WorkspaceManager,
  PreparedWorkspace,
} from "./workspace.js";

// ==================================================
// Constants
// ==================================================

const COMMIT_SHA_PATTERN =
  /^[0-9a-f]{40}$/u;

const REPOSITORY_OWNER_PATTERN =
  /^[A-Za-z0-9-]{1,39}$/u;

const REPOSITORY_NAME_PATTERN =
  /^[A-Za-z0-9_.-]{1,100}$/u;

const MAX_REPOSITORY_FULL_NAME_LENGTH = 255;

const MAX_BRANCH_LENGTH = 255;

const MAX_ALLOWED_RETRIES = 10;

const MAX_TIMEOUT_MS = 2_147_483_647;

// ==================================================
// Interfaces
// ==================================================

export interface SourcePreparationLimits {
  maxArchiveBytes: number;

  maxExtractedBytes: number;

  maxFileBytes: number;

  maxFileCount: number;

  timeoutMs: number;

  maxRetries: number;
}

export interface PrepareSourceInput {
  deploymentId: string;

  repositoryFullName: string;

  requestedBranch: string | null;

  resolvedCommitSha: string;

  installationId: bigint;

  auth: WorkerGitHubAuth;

  workspace: WorkspaceManager;

  limits: SourcePreparationLimits;

  signal: AbortSignal;

  shouldCancel: () => boolean;

  fetchImpl?: typeof fetch;

  sleep?: (ms: number) => Promise<void>;

  /**
   * Optional notification for cleanup failures.
   *
   * Receives sanitized identifiers only.
   * Must not expose credentials or filesystem paths.
   */
  onCleanupFailure?: (details: {
    deploymentId: string;
    attemptId: string;
    code: "WORKSPACE_UNAVAILABLE";
  }) => void;
}

export interface PrepareSourceResult {
  attemptId: string;

  sourceDir: string;

  downloadedBytes: number;

  extractedBytes: number;

  fileCount: number;
}

// ==================================================
// Repository identity validation
// ==================================================

function parseFullName(
  fullName: string,
): {
  owner: string;
  repo: string;
} {
  if (
    typeof fullName !== "string" ||
    fullName.length === 0 ||
    fullName.length >
      MAX_REPOSITORY_FULL_NAME_LENGTH ||
    fullName.trim() !== fullName
  ) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Repository identity is invalid.",
    );
  }

  const parts = fullName.split("/");

  if (parts.length !== 2) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Repository identity is invalid.",
    );
  }

  const [owner, repo] = parts;

  if (
    !owner ||
    !repo ||
    !REPOSITORY_OWNER_PATTERN.test(owner) ||
    !REPOSITORY_NAME_PATTERN.test(repo) ||
    owner.startsWith("-") ||
    owner.endsWith("-") ||
    repo === "." ||
    repo === ".."
  ) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Repository identity is invalid.",
    );
  }

  return {
    owner,
    repo,
  };
}

// ==================================================
// Immutable source validation
// ==================================================

function validateCommitSha(
  sha: string,
): void {
  if (
    typeof sha !== "string" ||
    !COMMIT_SHA_PATTERN.test(sha)
  ) {
    throw new SourcePreparationError(
      "COMMIT_NOT_FOUND",
      "A valid immutable commit SHA is required.",
    );
  }
}

function validateRequestedBranch(
  branch: string | null,
): void {
  if (branch === null) {
    return;
  }

  if (
    typeof branch !== "string" ||
    branch.length === 0 ||
    branch.length > MAX_BRANCH_LENGTH ||
    branch.trim() !== branch ||
    /[\u0000-\u001f\u007f]/u.test(branch)
  ) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Requested branch is invalid.",
    );
  }
}

function validateInstallationId(
  installationId: bigint,
): void {
  if (
    typeof installationId !== "bigint" ||
    installationId <= 0n
  ) {
    throw new SourcePreparationError(
      "INSTALLATION_UNAVAILABLE",
      "GitHub installation identity is invalid.",
    );
  }
}

// ==================================================
// Resource limit validation
// ==================================================

function isPositiveSafeInteger(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value > 0
  );
}

function validateLimits(
  limits: SourcePreparationLimits,
): void {
  if (
    !isPositiveSafeInteger(
      limits.maxArchiveBytes,
    ) ||
    !isPositiveSafeInteger(
      limits.maxExtractedBytes,
    ) ||
    !isPositiveSafeInteger(
      limits.maxFileBytes,
    ) ||
    !isPositiveSafeInteger(
      limits.maxFileCount,
    ) ||
    !isPositiveSafeInteger(
      limits.timeoutMs,
    )
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Source preparation limits are invalid.",
    );
  }

  if (
    limits.maxFileBytes >
    limits.maxExtractedBytes
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Individual file limit exceeds extraction limit.",
    );
  }

  if (limits.timeoutMs > MAX_TIMEOUT_MS) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Source preparation timeout is invalid.",
    );
  }

  if (
    !Number.isSafeInteger(limits.maxRetries) ||
    limits.maxRetries < 0 ||
    limits.maxRetries > MAX_ALLOWED_RETRIES
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Source preparation retry limit is invalid.",
    );
  }
}

// ==================================================
// Cancellation handling
// ==================================================

function assertActive(
  signal: AbortSignal,
  shouldCancel: () => boolean,
): void {
  if (
    signal.aborted ||
    shouldCancel()
  ) {
    throw new SourcePreparationError(
      "SOURCE_PREPARATION_CANCELLED",
      "Source preparation was cancelled.",
    );
  }
}

// ==================================================
// Result validation
// ==================================================

function validateDownloadResult(
  downloadedBytes: number,
  limits: SourcePreparationLimits,
): void {
  if (
    !isPositiveSafeInteger(downloadedBytes)
  ) {
    throw new SourcePreparationError(
      "MALFORMED_ARCHIVE",
      "Downloaded archive is empty or invalid.",
    );
  }

  if (
    downloadedBytes >
    limits.maxArchiveBytes
  ) {
    throw new SourcePreparationError(
      "ARCHIVE_SIZE_EXCEEDED",
      "Downloaded archive exceeded its size limit.",
    );
  }
}

function validateExtractionResult(
  extractedBytes: number,
  fileCount: number,
  limits: SourcePreparationLimits,
): void {
  if (
    !Number.isSafeInteger(extractedBytes) ||
    extractedBytes < 0 ||
    !Number.isSafeInteger(fileCount) ||
    fileCount < 0
  ) {
    throw new SourcePreparationError(
      "MALFORMED_ARCHIVE",
      "Archive extraction metrics are invalid.",
    );
  }

  if (
    extractedBytes >
    limits.maxExtractedBytes
  ) {
    throw new SourcePreparationError(
      "EXTRACTION_SIZE_EXCEEDED",
      "Extracted source exceeded its size limit.",
    );
  }

  if (
    fileCount >
    limits.maxFileCount
  ) {
    throw new SourcePreparationError(
      "FILE_COUNT_EXCEEDED",
      "Extracted source exceeded its file count limit.",
    );
  }
}

// ==================================================
// Sanitized error normalization
// ==================================================

function normalizePreparationError(
  error: unknown,
  signal: AbortSignal,
): SourcePreparationError {
  // Preserve existing classified errors, including
  // lease-loss and retryable infrastructure errors.

  if (error instanceof SourcePreparationError) {
    return error;
  }

  if (signal.aborted) {
    return new SourcePreparationError(
      "SOURCE_PREPARATION_CANCELLED",
      "Source preparation was cancelled.",
    );
  }

  // Never include raw external errors, request
  // headers, credentials, or local paths.

  return new SourcePreparationError(
    "UNEXPECTED_FAILURE",
    "Source preparation failed unexpectedly.",
  );
}

// ==================================================
// Failed-attempt cleanup
// ==================================================

async function cleanupFailedAttempt(
  input: PrepareSourceInput,
  prepared: PreparedWorkspace,
): Promise<void> {
  // Release local ownership before requesting removal.
  // This attempt will not be returned to the caller.

  input.workspace.release(
    prepared.attemptId,
  );

  try {
    await input.workspace.cleanupAttempt(
      prepared.deploymentId,
      prepared.attemptId,
    );
  } catch {
    // Cleanup failures must not replace the original
    // preparation failure.

    try {
      input.onCleanupFailure?.({
        deploymentId: prepared.deploymentId,

        attemptId: prepared.attemptId,

        code: "WORKSPACE_UNAVAILABLE",
      });
    } catch {
      // Observability callbacks must not interfere
      // with the original failure.
    }
  }
}

// ==================================================
// Source preparation orchestration
// ==================================================

export async function prepareSource(
  input: PrepareSourceInput,
): Promise<PrepareSourceResult> {
  const {
    signal,
    shouldCancel,
    limits,
    auth,
    workspace,
  } = input;

  let prepared:
    | PreparedWorkspace
    | undefined;

  let archivePath:
    | string
    | undefined;

  try {
    // ==============================================
    // 1. Validate trusted input
    // ==============================================

    assertActive(
      signal,
      shouldCancel,
    );

    const { owner, repo } =
      parseFullName(
        input.repositoryFullName,
      );

    validateCommitSha(
      input.resolvedCommitSha,
    );

    validateRequestedBranch(
      input.requestedBranch,
    );

    validateInstallationId(
      input.installationId,
    );

    validateLimits(limits);

    // ==============================================
    // 2. Revalidate GitHub installation
    // ==============================================

    await auth.verifyInstallation(
      input.installationId,
    );

    assertActive(
      signal,
      shouldCancel,
    );

    // ==============================================
    // 3. Create isolated source workspace
    // ==============================================

    prepared = await workspace.prepareAttempt(
      input.deploymentId,
    );

    const attemptDir = dirname(
      prepared.sourceDir,
    );

    archivePath = join(
      attemptDir,
      "archive.tar.gz",
    );

    assertActive(
      signal,
      shouldCancel,
    );

    // ==============================================
    // 4. Mint short-lived installation token
    // ==============================================

    const minted =
      await auth.mintInstallationToken(
        input.installationId,
      );

    if (
      typeof minted.token !== "string" ||
      minted.token.length === 0
    ) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",
        "GitHub installation token is unavailable.",
      );
    }

    assertActive(
      signal,
      shouldCancel,
    );

    // ==============================================
    // 5. Download immutable commit archive
    // ==============================================

    const download =
      await downloadGitHubArchive({
        token: minted.token,

        owner,

        repo,

        sha: input.resolvedCommitSha,

        destinationPath: archivePath,

        maxArchiveBytes:
          limits.maxArchiveBytes,

        timeoutMs:
          limits.timeoutMs,

        maxRetries:
          limits.maxRetries,

        signal,

        ...(input.fetchImpl
          ? { fetchImpl: input.fetchImpl }
          : {}),

        ...(input.sleep
          ? { sleep: input.sleep }
          : {}),
      });

    validateDownloadResult(
      download.downloadedBytes,
      limits,
    );

    assertActive(
      signal,
      shouldCancel,
    );

    // ==============================================
    // 6. Safely extract repository source
    // ==============================================

    const extraction =
      await extractTarGzSafely({
        archivePath,

        destinationDir:
          prepared.sourceDir,

        limits: {
          maxExtractedBytes:
            limits.maxExtractedBytes,

          maxFileBytes:
            limits.maxFileBytes,

          maxFileCount:
            limits.maxFileCount,
        },

        shouldCancel: () =>
          signal.aborted ||
          shouldCancel(),
      });

    validateExtractionResult(
      extraction.extractedBytes,
      extraction.fileCount,
      limits,
    );

    assertActive(
      signal,
      shouldCancel,
    );

    // ==============================================
    // 7. Remove compressed archive
    // ==============================================

    // The archive is no longer needed after successful
    // extraction. Remove it before publishing READY.

    try {
      await removeDownloadArtifact(
        archivePath,
      );
    } catch {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Temporary source archive cleanup failed.",
      );
    }

    archivePath = undefined;

    assertActive(
      signal,
      shouldCancel,
    );

    // ==============================================
    // 8. Persist completed workspace metadata
    // ==============================================

    await workspace.writeMetadata({
      deploymentId:
        prepared.deploymentId,

      attemptId:
        prepared.attemptId,

      repositoryFullName:
        input.repositoryFullName,

      requestedBranch:
        input.requestedBranch,

      resolvedCommitSha:
        input.resolvedCommitSha,

      status: "READY",

      downloadedBytes:
        download.downloadedBytes,

      extractedBytes:
        extraction.extractedBytes,

      fileCount:
        extraction.fileCount,

      failureCode: null,

      createdAt:
        prepared.createdAt,

      completedAt:
        new Date().toISOString(),
    });

    // Cancellation may have arrived while metadata
    // was being persisted.

    assertActive(
      signal,
      shouldCancel,
    );

    // ==============================================
    // 9. Transfer workspace ownership to caller
    // ==============================================

    // Do not release a successful attempt here.
    //
    // The deployment processor must retain ownership
    // until its fenced database transaction completes.
    //
    // The caller is responsible for releasing the
    // attempt after successful publication or cleaning
    // it after a rejected/stale publication.

    return {
      attemptId:
        prepared.attemptId,

      sourceDir:
        prepared.sourceDir,

      downloadedBytes:
        download.downloadedBytes,

      extractedBytes:
        extraction.extractedBytes,

      fileCount:
        extraction.fileCount,
    };
  } catch (error) {
    // ==============================================
    // 10. Failure and cancellation handling
    // ==============================================

    const failure =
      normalizePreparationError(
        error,
        signal,
      );

    if (archivePath) {
      await removeDownloadArtifact(
        archivePath,
      ).catch(() => undefined);
    }

    if (prepared) {
      await cleanupFailedAttempt(
        input,
        prepared,
      );
    }

    throw failure;
  }
}
