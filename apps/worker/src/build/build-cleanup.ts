
/**
 * CloudPilot — Build Cleanup
 *
 * Removes attempt-owned image artifacts.
 *
 * Cleanup is scoped to a single validated image tag
 * belonging to the expected build attempt.
 *
 * CloudPilot never runs global Docker cleanup commands.
 */

import type {
  BuildExecutor,
} from "./build-executor.js";

import {
  assertImageOwnership,
  type ImageTagInput,
} from "./image-artifact.js";

// ==================================================
// Cleanup result
// ==================================================

export type BuildCleanupResult =
  | { status: "skipped" }
  | { status: "removed" }
  | {
      status: "failed";
      reason:
        | "INVALID_IMAGE_OWNERSHIP"
        | "IMAGE_REMOVAL_FAILED";
    };

// ==================================================
// Scoped artifact cleanup
// ==================================================

/**
 * Remove a single attempt-owned image artifact.
 *
 * Cleanup failures are returned instead of thrown,
 * preserving the original build outcome.
 *
 * The caller must establish database ownership,
 * execution fencing and safe cleanup eligibility
 * before invoking this function.
 */
export async function removeBuildArtifact(
  executor: BuildExecutor,
  imageTag: string | null,
  expected: ImageTagInput,
): Promise<BuildCleanupResult> {
  if (!imageTag) {
    return {
      status: "skipped",
    };
  }

  // Verify project, deployment, commit and attempt.
  try {
    assertImageOwnership(
      imageTag,
      expected,
    );
  } catch {
    return {
      status: "failed",
      reason: "INVALID_IMAGE_OWNERSHIP",
    };
  }

  // Remove only the verified managed image reference.
  try {
    await executor.removeImage(imageTag);

    return {
      status: "removed",
    };
  } catch {
    return {
      status: "failed",
      reason: "IMAGE_REMOVAL_FAILED",
    };
  }
}
