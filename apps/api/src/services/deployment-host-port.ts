// ==================================================
// Fixed host port authorization (Phase 5.7.7)
// ==================================================
//
// Pure, side-effect-free helpers shared by the
// deployment API and the deployment outbox so the
// same normalization and authorization rules are
// applied consistently and can be unit tested
// without a database.
// ==================================================

export const REQUESTED_HOST_PORT_MIN = 1_024;
export const REQUESTED_HOST_PORT_MAX = 65_535;

export interface FixedPortAuthorizationInput {
  requestedHostPort: number;
  role: string;
  trustedLocalMode: boolean;
  trustedRepositories: readonly string[];
  repositoryFullName: string;
}

/**
 * Normalize a persisted build configuration snapshot into
 * an effective requested host port.
 *
 * `0` (or any absent/invalid value) means dynamic
 * allocation, preserving existing behavior. A valid
 * fixed port is returned unchanged so an occupied port
 * fails authoritatively downstream instead of silently
 * falling back to dynamic allocation.
 */
export function normalizeRequestedHostPort(snapshot: unknown): number {
  if (typeof snapshot !== "object" || snapshot === null) {
    return 0;
  }

  const value = (snapshot as { requestedHostPort?: unknown })
    .requestedHostPort;

  if (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 0 &&
    value <= REQUESTED_HOST_PORT_MAX
  ) {
    return value;
  }

  return 0;
}

/**
 * A non-zero fixed host port is only authorized for an
 * OWNER/ADMIN actor when trusted local runtime mode is
 * enabled and the target repository is explicitly
 * trusted. Dynamic allocation (port 0) is always
 * permitted and subject only to the existing
 * deployment authorization.
 */
export function isFixedPortAuthorized(
  input: FixedPortAuthorizationInput,
): boolean {
  if (input.requestedHostPort === 0) {
    return true;
  }

  if (input.role !== "OWNER" && input.role !== "ADMIN") {
    return false;
  }

  if (!input.trustedLocalMode) {
    return false;
  }

  return input.trustedRepositories.includes(input.repositoryFullName);
}