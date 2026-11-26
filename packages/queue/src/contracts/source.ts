
/**
 * CloudPilot — Source Preparation Contracts
 *
 * Shared contracts used by the API, deployment worker,
 * and source retrieval pipeline.
 */

// ==================================================
// Source preparation statuses
// ==================================================

export const sourcePreparationStatuses = [
  "PENDING",
  "DOWNLOADING",
  "EXTRACTING",
  "READY",
  "FAILED",
  "CANCELLED",
] as const;

export type SourcePreparationStatus =
  (typeof sourcePreparationStatuses)[number];

const sourcePreparationStatusSet: ReadonlySet<string> =
  new Set(sourcePreparationStatuses);

// ==================================================
// Source preparation status groups
// ==================================================

export const activeSourcePreparationStatuses = [
  "PENDING",
  "DOWNLOADING",
  "EXTRACTING",
] as const satisfies readonly SourcePreparationStatus[];

export const terminalSourcePreparationStatuses = [
  "READY",
  "FAILED",
  "CANCELLED",
] as const satisfies readonly SourcePreparationStatus[];

export type ActiveSourcePreparationStatus =
  (typeof activeSourcePreparationStatuses)[number];

export type TerminalSourcePreparationStatus =
  (typeof terminalSourcePreparationStatuses)[number];

const activeStatusSet: ReadonlySet<string> =
  new Set(activeSourcePreparationStatuses);

const terminalStatusSet: ReadonlySet<string> =
  new Set(terminalSourcePreparationStatuses);

// ==================================================
// Status validation utilities
// ==================================================

export function isSourcePreparationStatus(
  value: unknown,
): value is SourcePreparationStatus {
  return (
    typeof value === "string" &&
    sourcePreparationStatusSet.has(value)
  );
}

export function isActiveSourcePreparationStatus(
  value: unknown,
): value is ActiveSourcePreparationStatus {
  return (
    typeof value === "string" &&
    activeStatusSet.has(value)
  );
}

export function isTerminalSourcePreparationStatus(
  value: unknown,
): value is TerminalSourcePreparationStatus {
  return (
    typeof value === "string" &&
    terminalStatusSet.has(value)
  );
}

// ==================================================
// Source preparation failure codes
// ==================================================

export const sourceFailureCodes = [
  "REPOSITORY_ACCESS_DENIED",
  "INSTALLATION_UNAVAILABLE",
  "INSTALLATION_SUSPENDED",
  "COMMIT_NOT_FOUND",
  "GITHUB_RATE_LIMITED",
  "GITHUB_UNAVAILABLE",
  "DOWNLOAD_TIMEOUT",
  "DOWNLOAD_INTERRUPTED",
  "ARCHIVE_SIZE_EXCEEDED",
  "EXTRACTION_SIZE_EXCEEDED",
  "FILE_COUNT_EXCEEDED",
  "FILE_SIZE_EXCEEDED",
  "UNSAFE_ARCHIVE",
  "MALFORMED_ARCHIVE",
  "INSUFFICIENT_DISK_SPACE",
  "WORKSPACE_UNAVAILABLE",
  "SOURCE_PREPARATION_CANCELLED",
  "EXECUTION_LEASE_LOST",
  "UNEXPECTED_FAILURE",
] as const;

export type SourceFailureCode =
  (typeof sourceFailureCodes)[number];

const sourceFailureCodeSet: ReadonlySet<string> =
  new Set(sourceFailureCodes);

// ==================================================
// Retryable failure classification
// ==================================================

export const retryableSourceFailureCodes = [
  "GITHUB_RATE_LIMITED",
  "GITHUB_UNAVAILABLE",
  "DOWNLOAD_TIMEOUT",
  "DOWNLOAD_INTERRUPTED",
  "INSUFFICIENT_DISK_SPACE",
  "WORKSPACE_UNAVAILABLE",
] as const satisfies readonly SourceFailureCode[];

export type RetryableSourceFailureCode =
  (typeof retryableSourceFailureCodes)[number];

const retryableFailureCodeSet: ReadonlySet<string> =
  new Set(retryableSourceFailureCodes);

/**
 * Validate an external or persisted failure code.
 */
export function isSourceFailureCode(
  value: unknown,
): value is SourceFailureCode {
  return (
    typeof value === "string" &&
    sourceFailureCodeSet.has(value)
  );
}

/**
 * Indicates whether a failure is eligible for retry.
 *
 * Actual retry execution must still respect:
 * - Maximum job attempts
 * - Backoff policies
 * - Cancellation state
 * - Execution lease ownership
 */
export function isRetryableSourceFailure(
  code: SourceFailureCode,
): code is RetryableSourceFailureCode {
  return retryableFailureCodeSet.has(code);
}

// ==================================================
// Source preparation error
// ==================================================

export class SourcePreparationError extends Error {
  readonly code: SourceFailureCode;
  readonly retryable: boolean;

  constructor(
    code: SourceFailureCode,
    message?: string,
    options?: ErrorOptions,
  ) {
    super(message ?? code, options);

    this.name = "SourcePreparationError";
    this.code = code;
    this.retryable = isRetryableSourceFailure(code);

    Error.captureStackTrace?.(
      this,
      SourcePreparationError,
    );
  }
}

/**
 * Identify source preparation errors originating
 * from this shared contract.
 */
export function isSourcePreparationError(
  error: unknown,
): error is SourcePreparationError {
  return error instanceof SourcePreparationError;
}

/**
 * Resolve an unknown error into a known failure code.
 *
 * Unknown exceptions are deliberately classified as
 * unexpected failures rather than automatically retried.
 */
export function getSourceFailureCode(
  error: unknown,
): SourceFailureCode {
  if (isSourcePreparationError(error)) {
    return error.code;
  }

  return "UNEXPECTED_FAILURE";
}

// ==================================================
// Source preparation metadata
// ==================================================

export interface SourcePreparationMetadata {
  repositoryFullName: string;

  requestedBranch: string | null;

  resolvedCommitSha: string;

  status: SourcePreparationStatus;

  attempt: number;

  downloadedBytes: number;

  extractedBytes: number;

  fileCount: number;

  failureCode: SourceFailureCode | null;
}

/**
 * Read-only metadata snapshot for consumers that
 * should not mutate source preparation state.
 */
export type ReadonlySourcePreparationMetadata =
  Readonly<SourcePreparationMetadata>;
