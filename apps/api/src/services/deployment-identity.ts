// ==================================================
// Deployment request identity (Phase 5.7.7)
// ==================================================
//
// Pure idempotency comparison shared by the
// deployment API. Extracted so it can be unit
// tested without a database or environment
// validation side effects.
// ==================================================

import { normalizeRequestedHostPort } from "./deployment-host-port.js";

export interface DeploymentRequestIdentity {
  branch?: string | null | undefined;
  commit?: string | null | undefined;
  requestedHostPort?: number | undefined;
}

export interface ExistingDeploymentIdentity {
  requestedBranch: string | null;
  commitSha: string | null;
  buildConfigurationSnapshot?: unknown;
}

export function matchesDeploymentRequest(
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

  // A fixed host port is part of the request identity.
  // The same Idempotency-Key must not silently
  // represent a different port request.

  const existingPort =
    normalizeRequestedHostPort(
      existing.buildConfigurationSnapshot,
    );

  const requestPort =
    request.requestedHostPort ?? 0;

  if (existingPort !== requestPort) {
    return false;
  }

  return true;
}