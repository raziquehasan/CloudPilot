/**
 * CloudPilot — Container Runtime Contracts
 *
 * Shared runtime deployment contracts used by the API, queue,
 * deployment worker, and container runtime subsystem.
 *
 * Failure classification is intentionally conservative. Only
 * explicitly classified transient failures are eligible for
 * automatic retry.
 *
 * Queue payloads must contain identifiers and trusted metadata
 * only. Credentials, decrypted environment variables, private
 * keys, and unrestricted filesystem paths must never be placed
 * in runtime job payloads.
 */

import { z } from "zod";

// ==================================================
// Runtime failure codes
// ==================================================

export const runtimeFailureCodes = Object.freeze([
  "RUNTIME_UNAVAILABLE",
  "RUNTIME_DISABLED",
  "INVALID_RUNTIME_CONFIGURATION",
  "IMAGE_UNAVAILABLE",
  "IMAGE_OWNERSHIP_FAILED",
  "CONTAINER_CREATION_FAILED",
  "CONTAINER_START_FAILED",
  "PORT_ALLOCATION_FAILED",
  "STARTUP_TIMEOUT",
  "READINESS_FAILED",
  "RESOURCE_LIMIT_FAILED",
  "CONTAINER_EXECUTION_FAILED",
  "RUNTIME_CANCELLED",
  "RUNTIME_LEASE_LOST",
  "UNEXPECTED_RUNTIME_FAILURE",
] as const);

export type RuntimeFailureCode =
  (typeof runtimeFailureCodes)[number];

const runtimeFailureCodeSet: ReadonlySet<string> =
  new Set(runtimeFailureCodes);

// ==================================================
// Retryable failure classification
// ==================================================

/**
 * Only explicitly transient infrastructure failures should be
 * retried automatically.
 *
 * Timeouts, readiness failures, and resource-limit violations
 * are permanent by default because retrying the same workload
 * may cause repeated resource exhaustion or never become ready.
 *
 * A retryable classification does not itself authorize a retry.
 * The orchestrator must also verify remaining retry budget,
 * current execution ownership, cancellation status, and
 * previous execution termination.
 *
 * Uncertain container creation or startup outcomes must be
 * reconciled before another execution attempt is authorized.
 */

export const retryableRuntimeFailureCodes = Object.freeze([
  "RUNTIME_UNAVAILABLE",
] as const satisfies readonly RuntimeFailureCode[]);

export type RetryableRuntimeFailureCode =
  (typeof retryableRuntimeFailureCodes)[number];

const retryableRuntimeFailureCodeSet: ReadonlySet<string> =
  new Set(retryableRuntimeFailureCodes);

// ==================================================
// Runtime validation utilities
// ==================================================

export function isRuntimeFailureCode(
  value: unknown,
): value is RuntimeFailureCode {
  return (
    typeof value === "string" &&
    runtimeFailureCodeSet.has(value)
  );
}

export function normalizeRuntimeFailureCode(
  value: unknown,
): RuntimeFailureCode {
  return isRuntimeFailureCode(value)
    ? value
    : "UNEXPECTED_RUNTIME_FAILURE";
}

export function isRetryableRuntimeFailure(
  code: unknown,
): code is RetryableRuntimeFailureCode {
  return (
    typeof code === "string" &&
    retryableRuntimeFailureCodeSet.has(code)
  );
}

// ==================================================
// Runtime error
// ==================================================

export class RuntimeDeploymentError extends Error {
  readonly code: RuntimeFailureCode;
  readonly retryable: boolean;

  constructor(
    code: RuntimeFailureCode,
    message?: string,
    options?: ErrorOptions,
  ) {
    const normalizedCode =
      normalizeRuntimeFailureCode(code);

    super(
      message ?? runtimeFailureMessage(normalizedCode),
      options,
    );

    this.name = "RuntimeDeploymentError";

    this.code = normalizedCode;

    this.retryable =
      isRetryableRuntimeFailure(normalizedCode);

    Error.captureStackTrace?.(
      this,
      RuntimeDeploymentError,
    );
  }
}

export function isRuntimeDeploymentError(
  error: unknown,
): error is RuntimeDeploymentError {
  return (
    error instanceof RuntimeDeploymentError &&
    isRuntimeFailureCode(error.code) &&
    error.retryable ===
      isRetryableRuntimeFailure(error.code)
  );
}

export function getRuntimeFailureCode(
  error: unknown,
): RuntimeFailureCode {
  return isRuntimeDeploymentError(error)
    ? error.code
    : "UNEXPECTED_RUNTIME_FAILURE";
}

export function isRetryableRuntimeError(
  error: unknown,
): boolean {
  return (
    isRuntimeDeploymentError(error) &&
    isRetryableRuntimeFailure(error.code)
  );
}

// ==================================================
// Sanitized user-facing messages
// ==================================================

/**
 * Safe for API responses and dashboard rendering.
 *
 * Never expose raw Error.message, Error.cause, stack traces,
 * process environments, host paths, container environment
 * variables, credentials, or internal command arguments.
 */

const failureMessages: Readonly<
  Record<RuntimeFailureCode, string>
> = Object.freeze({
  RUNTIME_UNAVAILABLE:
    "The container runtime is temporarily unavailable.",

  RUNTIME_DISABLED:
    "Container deployment is disabled.",

  INVALID_RUNTIME_CONFIGURATION:
    "The runtime configuration is invalid.",

  IMAGE_UNAVAILABLE:
    "The verified image artifact is unavailable.",

  IMAGE_OWNERSHIP_FAILED:
    "The image artifact could not be verified for this deployment.",

  CONTAINER_CREATION_FAILED:
    "The container could not be created.",

  CONTAINER_START_FAILED:
    "The container could not be started.",

  PORT_ALLOCATION_FAILED:
    "A runtime port could not be allocated.",

  STARTUP_TIMEOUT:
    "The container did not start within its allowed time.",

  READINESS_FAILED:
    "The application did not become ready.",

  RESOURCE_LIMIT_FAILED:
    "The container exceeded its resource limits.",

  CONTAINER_EXECUTION_FAILED:
    "The container exited unexpectedly.",

  RUNTIME_CANCELLED:
    "The deployment was cancelled.",

  RUNTIME_LEASE_LOST:
    "Deployment execution ownership was lost.",

  UNEXPECTED_RUNTIME_FAILURE:
    "The deployment failed unexpectedly.",
});

export function runtimeFailureMessage(
  code: unknown,
): string {
  return failureMessages[
    normalizeRuntimeFailureCode(code)
  ];
}

export function getRuntimeFailureMessage(
  error: unknown,
): string {
  return runtimeFailureMessage(
    getRuntimeFailureCode(error),
  );
}

// ==================================================
// Public runtime failure metadata
// ==================================================

export interface RuntimeFailureMetadata {
  readonly code: RuntimeFailureCode;
  readonly message: string;
  readonly retryable: boolean;
}

export function toRuntimeFailureMetadata(
  error: unknown,
): RuntimeFailureMetadata {
  const code = getRuntimeFailureCode(error);

  return {
    code,
    message: runtimeFailureMessage(code),
    retryable: isRetryableRuntimeFailure(code),
  };
}

// ==================================================
// Container lifecycle status
// ==================================================

export const runtimeInstanceStatuses = Object.freeze([
  "QUEUED",
  "CREATING",
  "STARTING",
  "RUNNING",
  "STOPPED",
  "FAILED",
  "CANCELLED",
] as const);

export type RuntimeInstanceStatus =
  (typeof runtimeInstanceStatuses)[number];

const runtimeInstanceStatusSet: ReadonlySet<string> =
  new Set(runtimeInstanceStatuses);

export function isRuntimeInstanceStatus(
  value: unknown,
): value is RuntimeInstanceStatus {
  return (
    typeof value === "string" &&
    runtimeInstanceStatusSet.has(value)
  );
}

// ==================================================
// Runtime job payload
// ==================================================

const uuidSchema = z.uuid();

const identifierSchema = z
  .string()
  .trim()
  .min(1)
  .max(128)
  .refine(
    (value) =>
      !/[\u0000-\u001f\u007f-\u009f]/u.test(value),
    {
      message:
        "Identifier contains invalid control characters.",
    },
  );

/**
 * Phase 5.5 runtime deployment job.
 *
 * The processor must verify:
 *
 * - Workspace and project ownership
 * - Deployment identity and lifecycle state
 * - Successful build artifact identity and ownership
 * - Runtime instance identity
 * - Execution lease and fencing token
 * - Runtime execution policy
 *
 * Queue payload validation alone does not authorize execution,
 * and never carries secrets or environment variable values.
 */

export const runtimeJobActions = Object.freeze([
  "deploy",
  "start",
  "stop",
  "restart",
  "recover",
] as const);

export type RuntimeJobAction =
  (typeof runtimeJobActions)[number];

export const runtimeJobSchema = z
  .object({
    kind: z.literal("runtime"),

    workspaceId: uuidSchema,

    projectId: uuidSchema,

    deploymentId: uuidSchema,

    runtimeInstanceId: uuidSchema,

    buildId: uuidSchema,

    /**
     * Runtime lifecycle action. Defaults to a full
     * deployment. Lifecycle actions operate on an
     * existing managed runtime instance.
     */
    action: z
      .enum(runtimeJobActions)
      .default("deploy"),

    correlationId: identifierSchema,
  })
  .strict();

export type RuntimeJob = z.infer<
  typeof runtimeJobSchema
>;

// ==================================================
// Runtime lifecycle commands
// ==================================================

export const runtimeLifecycleActions = Object.freeze([
  "start",
  "stop",
  "restart",
] as const);

export type RuntimeLifecycleAction =
  (typeof runtimeLifecycleActions)[number];

const runtimeLifecycleActionSet: ReadonlySet<string> =
  new Set(runtimeLifecycleActions);

export function isRuntimeLifecycleAction(
  value: unknown,
): value is RuntimeLifecycleAction {
  return (
    typeof value === "string" &&
    runtimeLifecycleActionSet.has(value)
  );
}

// ==================================================
// Managed container identity
// ==================================================

/**
 * Deterministic container name derived from trusted
 * identifiers only.
 *
 * Format:
 * cloudpilot-<projectId>-<deploymentId>-<attempt>
 *
 * The attempt is the runtime instance attempt number.
 *
 * UUIDs are normalized to lowercase to prevent multiple
 * container names representing the same logical identity.
 */

export interface ManagedContainerIdentity {
  projectId: string;
  deploymentId: string;
  attempt: number;
}

export const CONTAINER_NAME_PREFIX = "cloudpilot";

// ==================================================
// Runtime identity validation
// ==================================================

/**
 * Maximum supported runtime attempt.
 *
 * This limit is compatible with PostgreSQL signed INTEGER
 * fields and prevents unsafe numeric values from becoming
 * part of container names or ownership labels.
 */

const MAX_RUNTIME_ATTEMPT = 2_147_483_647;

const MAX_CONTAINER_NAME_LENGTH = 128;

/**
 * Validate runtime attempts before constructing managed
 * resource identities.
 *
 * Rejects:
 *
 * - Zero and negative numbers
 * - Fractional numbers
 * - NaN and Infinity
 * - Unsafe JavaScript integers
 * - Values exceeding the supported database integer range
 */

function isValidRuntimeAttempt(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value > 0 &&
    value <= MAX_RUNTIME_ATTEMPT
  );
}

/**
 * Validate and normalize UUIDs used by managed resources.
 *
 * Do not silently trim or repair malformed identifiers.
 * Only valid UUIDs may participate in container identities
 * and ownership labels.
 */

function normalizeRuntimeUuid(
  value: unknown,
): string {
  const result = uuidSchema.safeParse(value);

  if (!result.success) {
    throw new RuntimeDeploymentError(
      "INVALID_RUNTIME_CONFIGURATION",
      "The runtime identity is invalid.",
    );
  }

  return result.data.toLowerCase();
}

// ==================================================
// Managed container name validation
// ==================================================

/**
 * Canonical managed container name pattern.
 *
 * Captures:
 *
 * 1. Project UUID
 * 2. Deployment UUID
 * 3. Runtime attempt
 *
 * The expression intentionally accepts lowercase UUIDs
 * only. Numeric bounds and UUID validity are checked
 * separately after matching.
 */

const MANAGED_CONTAINER_NAME_PATTERN =
  /^cloudpilot-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-([1-9][0-9]{0,9})$/u;

/**
 * Generate a canonical CloudPilot-managed container name.
 *
 * The same project, deployment, and runtime attempt must
 * always produce the same container name.
 *
 * This deterministic identity supports duplicate job
 * detection and recovery after uncertain Docker operations.
 *
 * A valid generated name does not independently authorize
 * access to an existing container.
 */

export function generateContainerName(
  identity: ManagedContainerIdentity,
): string {
  const projectId = normalizeRuntimeUuid(
    identity.projectId,
  );

  const deploymentId = normalizeRuntimeUuid(
    identity.deploymentId,
  );

  if (!isValidRuntimeAttempt(identity.attempt)) {
    throw new RuntimeDeploymentError(
      "INVALID_RUNTIME_CONFIGURATION",
      "The runtime attempt is invalid.",
    );
  }

  const name =
    `${CONTAINER_NAME_PREFIX}-` +
    `${projectId}-` +
    `${deploymentId}-` +
    `${identity.attempt}`;

  if (!isManagedContainerName(name)) {
    throw new RuntimeDeploymentError(
      "INVALID_RUNTIME_CONFIGURATION",
      "The managed container name is invalid.",
    );
  }

  return name;
}

/**
 * Determine whether a value represents a canonical
 * CloudPilot-managed container name.
 *
 * Name validation alone does not prove container ownership.
 * Callers must verify the corresponding database record,
 * actual container identity, and expected ownership labels.
 */

export function isManagedContainerName(
  value: unknown,
): value is string {
  if (
    typeof value !== "string" ||
    value.length > MAX_CONTAINER_NAME_LENGTH
  ) {
    return false;
  }

  const match =
    MANAGED_CONTAINER_NAME_PATTERN.exec(value);

  /**
   * Exact comparison prevents accepting trailing characters
   * such as a final newline that some regular-expression
   * end anchors may otherwise permit.
   */

  if (!match || match[0] !== value) {
    return false;
  }

  const projectId = match[1];

  const deploymentId = match[2];

  const attempt = Number(match[3]);

  return (
    uuidSchema.safeParse(projectId).success &&
    uuidSchema.safeParse(deploymentId).success &&
    isValidRuntimeAttempt(attempt)
  );
}

// ==================================================
// Ownership labels
// ==================================================

/**
 * Labels attached to every CloudPilot-managed container.
 *
 * These labels provide resource identity metadata.
 *
 * Lifecycle and cleanup operations must additionally verify:
 *
 * - Database ownership
 * - Workspace and project authorization
 * - Deployment identity
 * - Runtime instance identity
 * - Actual Docker container identity
 * - Expected ownership labels
 * - Runtime execution attempt
 * - Applicable execution lease and fencing token
 *
 * Docker labels alone do not establish authorization.
 *
 * Resources with missing, inconsistent, or unexpected
 * ownership metadata must not be modified or removed
 * automatically.
 */

export const runtimeOwnershipLabels = Object.freeze({
  managed: "cloudpilot.managed",

  workspace: "cloudpilot.workspaceId",

  project: "cloudpilot.projectId",

  deployment: "cloudpilot.deploymentId",

  runtimeInstance: "cloudpilot.runtimeInstanceId",

  attempt: "cloudpilot.attempt",
} as const);

/**
 * Construct validated ownership labels for a managed
 * runtime container.
 *
 * All identifiers must be valid UUIDs and the runtime
 * attempt must be a positive bounded integer.
 *
 * Labels must never contain credentials, decrypted
 * environment variables, access tokens, or private keys.
 */

export function buildOwnershipLabels(input: {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  runtimeInstanceId: string;
  attempt: number;
}): Record<string, string> {
  if (!isValidRuntimeAttempt(input.attempt)) {
    throw new RuntimeDeploymentError(
      "INVALID_RUNTIME_CONFIGURATION",
      "The runtime attempt is invalid.",
    );
  }

  const workspaceId = normalizeRuntimeUuid(
    input.workspaceId,
  );

  const projectId = normalizeRuntimeUuid(
    input.projectId,
  );

  const deploymentId = normalizeRuntimeUuid(
    input.deploymentId,
  );

  const runtimeInstanceId = normalizeRuntimeUuid(
    input.runtimeInstanceId,
  );

  return {
    [runtimeOwnershipLabels.managed]: "true",

    [runtimeOwnershipLabels.workspace]:
      workspaceId,

    [runtimeOwnershipLabels.project]:
      projectId,

    [runtimeOwnershipLabels.deployment]:
      deploymentId,

    [runtimeOwnershipLabels.runtimeInstance]:
      runtimeInstanceId,

    [runtimeOwnershipLabels.attempt]:
      String(input.attempt),
  };
}
