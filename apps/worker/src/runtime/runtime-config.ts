/**
 * CloudPilot — Runtime Configuration and Validation
 *
 * Centralizes managed runtime identity, network naming, and
 * strict validation of runtime configuration derived from
 * trusted identifiers and validated environment configuration.
 *
 * Configuration validation is not an authorization boundary.
 * Runtime resource ownership must additionally be verified
 * against tenant-scoped database records and Docker metadata.
 */

import { RuntimeDeploymentError } from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const UUID_SOURCE =
  "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

const UUID_PATTERN = new RegExp(
  `^${UUID_SOURCE}$`,
  "iu",
);

const UUID_LENGTH = 36;

const IMAGE_ID_PATTERN =
  /^sha256:[a-f0-9]{64}$/u;

const IMAGE_ID_LENGTH = 71;

const MIN_CONTAINER_PORT = 1;

const MAX_CONTAINER_PORT = 65_535;

const MIN_CPU_LIMIT = 1;

const MAX_CPU_LIMIT = 64;

const MIN_MEMORY_LIMIT_MB = 128;

const MAX_MEMORY_LIMIT_MB = 65_536;

const MIN_PIDS_LIMIT = 16;

const MAX_PIDS_LIMIT = 4_096;

const MAX_NETWORK_PREFIX_LENGTH = 32;

const MAX_NETWORK_NAME_LENGTH = 128;

const NETWORK_PREFIX_PATTERN =
  /^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$/u;

const INVALID_NETWORK_CHARACTER_PATTERN =
  /[^A-Za-z0-9_.-]/u;

const CONTROL_CHARACTER_PATTERN =
  /[\u0000-\u001f\u007f-\u009f]/u;

// ==================================================
// Internal validation helpers
// ==================================================

function invalidConfiguration(
  message: string,
): RuntimeDeploymentError {
  return new RuntimeDeploymentError(
    "INVALID_RUNTIME_CONFIGURATION",
    message,
  );
}

/**
 * Validate a bounded safe integer without coercing
 * strings, floating-point values, NaN, or Infinity.
 */
function isIntegerInRange(
  value: unknown,
  minimum: number,
  maximum: number,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= minimum &&
    value <= maximum
  );
}

/**
 * Prevent invalid or unbounded labels from appearing
 * in runtime validation error messages.
 */
function safeIdentityLabel(
  label: unknown,
): string {
  if (
    typeof label !== "string" ||
    label.length === 0 ||
    label.length > 64 ||
    CONTROL_CHARACTER_PATTERN.test(label)
  ) {
    return "Runtime";
  }

  return label;
}

// ==================================================
// Identity validation
// ==================================================

/**
 * Validate the canonical textual UUID structure.
 *
 * This helper intentionally preserves compatibility
 * with existing UUID-shaped identifiers.
 *
 * UUID validation alone does not establish resource
 * ownership or authorization.
 */
export function assertValidUuid(
  value: unknown,
  label: string,
): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length !== UUID_LENGTH ||
    CONTROL_CHARACTER_PATTERN.test(value) ||
    !UUID_PATTERN.test(value)
  ) {
    throw invalidConfiguration(
      `${safeIdentityLabel(label)} identity is invalid.`,
    );
  }
}

/**
 * Containers must be created from verified immutable
 * image IDs, never mutable user-provided tags.
 *
 * The image ID must also be compared with the expected
 * build artifact and the actual Docker image metadata.
 */
export function assertValidImageId(
  value: unknown,
): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length !== IMAGE_ID_LENGTH ||
    CONTROL_CHARACTER_PATTERN.test(value) ||
    !IMAGE_ID_PATTERN.test(value)
  ) {
    throw new RuntimeDeploymentError(
      "IMAGE_OWNERSHIP_FAILED",
      "The image identifier is invalid.",
    );
  }
}

// ==================================================
// Container port validation
// ==================================================

/**
 * Validate an application container port.
 *
 * Port zero is intentionally rejected because it is
 * not a valid application listening port.
 *
 * Docker-managed host port allocation must be handled
 * separately by the runtime client.
 */
export function assertValidContainerPort(
  value: unknown,
): asserts value is number {
  if (
    !isIntegerInRange(
      value,
      MIN_CONTAINER_PORT,
      MAX_CONTAINER_PORT,
    )
  ) {
    throw invalidConfiguration(
      "The container port is invalid.",
    );
  }
}

// ==================================================
// Runtime resource limits
// ==================================================

/**
 * Validate explicit runtime resource limits.
 *
 * Unlimited, zero, negative, fractional, and unsafe
 * integer values are rejected.
 *
 * The runtime client must apply these validated
 * limits without silently weakening them.
 */
export function assertValidResourceLimits(
  input: {
    cpuLimit: number;
    memoryLimitMb: number;
    pidsLimit: number;
  },
): void {
  if (
    !input ||
    typeof input !== "object" ||
    Array.isArray(input)
  ) {
    throw invalidConfiguration(
      "Runtime resource limits are invalid.",
    );
  }

  const {
    cpuLimit,
    memoryLimitMb,
    pidsLimit,
  } = input;

  // --------------------------------------------------
  // CPU limit
  // --------------------------------------------------

  if (
    !isIntegerInRange(
      cpuLimit,
      MIN_CPU_LIMIT,
      MAX_CPU_LIMIT,
    )
  ) {
    throw invalidConfiguration(
      "The CPU limit is invalid.",
    );
  }

  // --------------------------------------------------
  // Memory limit
  // --------------------------------------------------

  if (
    !isIntegerInRange(
      memoryLimitMb,
      MIN_MEMORY_LIMIT_MB,
      MAX_MEMORY_LIMIT_MB,
    )
  ) {
    throw invalidConfiguration(
      "The memory limit is invalid.",
    );
  }

  // --------------------------------------------------
  // Process limit
  // --------------------------------------------------

  if (
    !isIntegerInRange(
      pidsLimit,
      MIN_PIDS_LIMIT,
      MAX_PIDS_LIMIT,
    )
  ) {
    throw invalidConfiguration(
      "The process limit is invalid.",
    );
  }
}

// ==================================================
// Managed network naming
// ==================================================

/**
 * Generate a deterministic per-project managed
 * bridge network name.
 *
 * Network names are derived from validated identifiers.
 *
 * A valid network name does not prove ownership.
 * Existing networks must be inspected and their
 * driver, scope, configuration, and ownership labels
 * verified before reuse or deletion.
 */
export function generateNetworkName(
  prefix: string,
  projectId: string,
): string {
  assertValidUuid(
    projectId,
    "Project",
  );

  // --------------------------------------------------
  // Validate the configured network prefix
  // --------------------------------------------------

  if (
    typeof prefix !== "string" ||
    prefix.length === 0 ||
    prefix.length >
      MAX_NETWORK_PREFIX_LENGTH ||
    CONTROL_CHARACTER_PATTERN.test(prefix) ||
    INVALID_NETWORK_CHARACTER_PATTERN.test(prefix) ||
    !NETWORK_PREFIX_PATTERN.test(prefix)
  ) {
    throw invalidConfiguration(
      "The network name prefix is invalid.",
    );
  }

  // --------------------------------------------------
  // Normalize the project UUID
  // --------------------------------------------------

  const normalizedProjectId =
    projectId.toLowerCase();

  // Preserve the configured prefix for compatibility
  // with existing managed network names.

  const name =
    `${prefix}-net-${normalizedProjectId}`;

  // --------------------------------------------------
  // Validate the final network name
  // --------------------------------------------------

  if (
    name.length === 0 ||
    name.length > MAX_NETWORK_NAME_LENGTH ||
    CONTROL_CHARACTER_PATTERN.test(name) ||
    INVALID_NETWORK_CHARACTER_PATTERN.test(name)
  ) {
    throw invalidConfiguration(
      "The managed network name is invalid.",
    );
  }

  return name;
}

// ==================================================
// Loopback bind validation
// ==================================================

/**
 * Runtime ports must be restricted to explicit
 * numeric loopback addresses.
 *
 * Reject wildcard addresses, public addresses,
 * private LAN addresses, hostnames, and addresses
 * containing unexpected whitespace.
 *
 * IPv6 loopback publishing requires correct Docker
 * CLI address formatting and runtime support.
 */
export function assertLoopbackBind(
  host: unknown,
): asserts host is string {
  if (
    typeof host !== "string" ||
    (
      host !== "127.0.0.1" &&
      host !== "::1"
    )
  ) {
    throw invalidConfiguration(
      "Runtime ports must bind to a numeric loopback address.",
    );
  }
}