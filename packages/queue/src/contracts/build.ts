
/**
 * CloudPilot — Build Engine Contracts
 *
 * Shared build contracts used by the API, queue,
 * deployment worker, and isolated build subsystem.
 *
 * Failure classification is intentionally conservative.
 * Only explicitly classified transient failures are
 * eligible for automatic retry.
 */

// ==================================================
// Build failure codes
// ==================================================

export const buildFailureCodes = Object.freeze([
  "BUILDER_UNAVAILABLE",
  "BUILD_CONTEXT_INVALID",
  "DOCKERFILE_NOT_FOUND",
  "DOCKERFILE_INVALID",
  "BUILD_POLICY_VIOLATION",
  "BUILD_TIMEOUT",
  "BUILD_CANCELLED",
  "BUILD_FAILED",
  "BUILD_RESOURCE_LIMIT_EXCEEDED",
  "BUILD_NETWORK_DENIED",
  "SOURCE_WORKSPACE_UNAVAILABLE",
  "SOURCE_INTEGRITY_FAILED",
  "IMAGE_ARTIFACT_UNAVAILABLE",
  "IMAGE_VERIFICATION_FAILED",
  "BUILD_LEASE_LOST",
  "UNEXPECTED_BUILD_FAILURE",
] as const);

export type BuildFailureCode =
  (typeof buildFailureCodes)[number];

const buildFailureCodeSet: ReadonlySet<string> =
  new Set(buildFailureCodes);

// ==================================================
// Retryable failure classification
// ==================================================

/**
 * Only explicitly transient infrastructure failures
 * should be retried automatically.
 *
 * Timeouts and resource limit violations are permanent
 * by default because retrying the same build may cause
 * repeated resource exhaustion.
 *
 * A retryable classification does not itself authorize
 * a retry. The orchestrator must also verify:
 *
 * - Remaining retry budget
 * - Current execution ownership
 * - Deployment cancellation status
 * - Previous execution termination
 * - Source workspace availability
 */
export const retryableBuildFailureCodes = Object.freeze([
  "BUILDER_UNAVAILABLE",
] as const satisfies readonly BuildFailureCode[]);

export type RetryableBuildFailureCode =
  (typeof retryableBuildFailureCodes)[number];

const retryableBuildFailureCodeSet: ReadonlySet<string> =
  new Set(retryableBuildFailureCodes);

// ==================================================
// Runtime validation utilities
// ==================================================

export function isBuildFailureCode(
  value: unknown,
): value is BuildFailureCode {
  return (
    typeof value === "string" &&
    buildFailureCodeSet.has(value)
  );
}

/**
 * Normalize an invalid or unknown failure code.
 */
export function normalizeBuildFailureCode(
  value: unknown,
): BuildFailureCode {
  return isBuildFailureCode(value)
    ? value
    : "UNEXPECTED_BUILD_FAILURE";
}

/**
 * Check whether a failure is eligible for automatic retry.
 *
 * Unknown values are never retryable.
 */
export function isRetryableBuildFailure(
  code: unknown,
): code is RetryableBuildFailureCode {
  return (
    typeof code === "string" &&
    retryableBuildFailureCodeSet.has(code)
  );
}

// ==================================================
// Build error
// ==================================================

export class BuildError extends Error {
  readonly code: BuildFailureCode;
  readonly retryable: boolean;

  constructor(
    code: BuildFailureCode,
    message?: string,
    options?: ErrorOptions,
  ) {
    const normalizedCode =
      normalizeBuildFailureCode(code);

    super(
      message ?? buildFailureMessage(normalizedCode),
      options,
    );

    this.name = "BuildError";
    this.code = normalizedCode;
    this.retryable =
      isRetryableBuildFailure(normalizedCode);

    Error.captureStackTrace?.(this, BuildError);
  }
}

/**
 * Recognize local BuildError instances with valid
 * failure classification.
 *
 * Arbitrary objects received from external systems
 * are not trusted as BuildError instances.
 */
export function isBuildError(
  error: unknown,
): error is BuildError {
  return (
    error instanceof BuildError &&
    isBuildFailureCode(error.code) &&
    error.retryable ===
      isRetryableBuildFailure(error.code)
  );
}

/**
 * Resolve an unknown exception into a known failure code.
 *
 * Unknown exceptions are never automatically retried.
 */
export function getBuildFailureCode(
  error: unknown,
): BuildFailureCode {
  return isBuildError(error)
    ? error.code
    : "UNEXPECTED_BUILD_FAILURE";
}

/**
 * Determine whether an exception is eligible for retry.
 *
 * Actual retry authorization remains the responsibility
 * of the deployment orchestrator.
 */
export function isRetryableBuildError(
  error: unknown,
): boolean {
  return (
    isBuildError(error) &&
    isRetryableBuildFailure(error.code)
  );
}

// ==================================================
// Sanitized user-facing messages
// ==================================================

/**
 * These messages are safe for API responses and
 * deployment dashboard rendering.
 *
 * Never expose raw Error.message, Error.cause,
 * stack traces, process environments, host paths,
 * credentials, or internal command arguments.
 */
const failureMessages: Readonly<
  Record<BuildFailureCode, string>
> = Object.freeze({
  BUILDER_UNAVAILABLE:
    "The build service is temporarily unavailable.",

  BUILD_CONTEXT_INVALID:
    "The build context is invalid or exceeds policy limits.",

  DOCKERFILE_NOT_FOUND:
    "No Dockerfile was found for this deployment.",

  DOCKERFILE_INVALID:
    "The Dockerfile could not be validated.",

  BUILD_POLICY_VIOLATION:
    "The build configuration violates the build security policy.",

  BUILD_TIMEOUT:
    "The build exceeded its allowed execution time.",

  BUILD_CANCELLED:
    "The build was cancelled.",

  BUILD_FAILED:
    "The build process failed.",

  BUILD_RESOURCE_LIMIT_EXCEEDED:
    "The build exceeded its resource limits.",

  BUILD_NETWORK_DENIED:
    "The build attempted a denied network operation.",

  SOURCE_WORKSPACE_UNAVAILABLE:
    "The prepared source workspace is unavailable.",

  SOURCE_INTEGRITY_FAILED:
    "The prepared source could not be verified.",

  IMAGE_ARTIFACT_UNAVAILABLE:
    "The build did not produce a verifiable image artifact.",

  IMAGE_VERIFICATION_FAILED:
    "The produced image artifact could not be verified.",

  BUILD_LEASE_LOST:
    "Build execution ownership was lost.",

  UNEXPECTED_BUILD_FAILURE:
    "The build failed unexpectedly.",
});

/**
 * Return a sanitized message for any supplied code.
 *
 * Unknown codes always receive a safe fallback.
 */
export function buildFailureMessage(
  code: unknown,
): string {
  return failureMessages[
    normalizeBuildFailureCode(code)
  ];
}

/**
 * Resolve an exception directly into a safe,
 * user-facing failure message.
 */
export function getBuildFailureMessage(
  error: unknown,
): string {
  return buildFailureMessage(
    getBuildFailureCode(error),
  );
}

// ==================================================
// Public build failure metadata
// ==================================================

export interface BuildFailureMetadata {
  readonly code: BuildFailureCode;
  readonly message: string;
  readonly retryable: boolean;
}

/**
 * Convert an exception into sanitized metadata.
 *
 * This does not serialize internal messages, stack
 * traces, causes, or infrastructure information.
 */
export function toBuildFailureMetadata(
  error: unknown,
): BuildFailureMetadata {
  const code = getBuildFailureCode(error);

  return {
    code,
    message: buildFailureMessage(code),
    retryable: isRetryableBuildFailure(code),
  };
}

// ==================================================
// Build artifact types
// ==================================================

export const buildArtifactTypes = Object.freeze([
  "DOCKER_IMAGE_STORE",
  "OCI_ARCHIVE",
] as const);

export type BuildArtifactType =
  (typeof buildArtifactTypes)[number];

const buildArtifactTypeSet: ReadonlySet<string> =
  new Set(buildArtifactTypes);

/**
 * Validate an artifact type received at runtime.
 */
export function isBuildArtifactType(
  value: unknown,
): value is BuildArtifactType {
  return (
    typeof value === "string" &&
    buildArtifactTypeSet.has(value)
  );
}

// ==================================================
// Build artifact metadata
// ==================================================

/**
 * Metadata describing a produced image artifact.
 *
 * imageId:
 *   Local image identifier, normally a configuration
 *   digest in Docker's image store.
 *
 * imageDigest:
 *   Verified manifest or index digest, when available.
 *
 * imageSizeBytes:
 *   Non-negative safe integer, or null when the
 *   selected executor cannot determine the size.
 *
 * An image ID must never be substituted for a
 * manifest digest.
 *
 * This contract describes metadata only. Actual
 * artifact existence, integrity, tenant ownership,
 * and execution fencing must be verified separately.
 */
export interface BuildArtifactMetadata {
  artifactType: BuildArtifactType;
  imageTag: string;
  imageId: string | null;
  imageDigest: string | null;
  platform: string;
  imageSizeBytes: number | null;
}

// ==================================================
// Artifact validation utilities
// ==================================================

const sha256DigestPattern =
  /^sha256:[a-f0-9]{64}$/u;

/**
 * Validate a canonical SHA-256 digest.
 *
 * Syntax validation alone does not establish
 * artifact integrity or ownership.
 */
export function isBuildSha256Digest(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    sha256DigestPattern.test(value)
  );
}

/**
 * Validate image size before serialization or
 * conversion into a database BigInt field.
 */
export function isValidBuildImageSize(
  value: unknown,
): value is number | null {
  return (
    value === null ||
    (
      typeof value === "number" &&
      Number.isSafeInteger(value) &&
      value >= 0
    )
  );
}
