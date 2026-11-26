
/**
 * CloudPilot — Image Artifact Naming and Verification
 *
 * Generates deterministic, deployment-scoped image
 * references from trusted internal identifiers.
 *
 * Security properties:
 *
 * - Project-scoped image namespace
 * - Deployment-scoped image tags
 * - Immutable commit SHA association
 * - Build attempt isolation
 * - Strict managed image reference validation
 * - SHA-256 image metadata validation
 *
 * CloudPilot does not accept unrestricted user-defined
 * image references or publish images to external registries.
 */

import { BuildError } from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const UUID_SOURCE =
  "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

const UUID_PATTERN = new RegExp(
  `^${UUID_SOURCE}$`,
  "iu",
);

const COMMIT_SHA_PATTERN =
  /^[0-9a-f]{40}$/u;

const SHA256_DIGEST_PATTERN =
  /^sha256:[0-9a-f]{64}$/u;

const IMAGE_TAG_PATTERN =
  /^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/u;

const MANAGED_IMAGE_REFERENCE_PATTERN =
  new RegExp(
    `^cloudpilot/project-(${UUID_SOURCE}):` +
      `([0-9a-f]{40})-` +
      `(${UUID_SOURCE})-` +
      `([1-9][0-9]{0,15})$`,
    "u",
  );

const MAX_REPOSITORY_LENGTH = 255;

const MAX_TAG_LENGTH = 128;

const MAX_REFERENCE_LENGTH =
  MAX_REPOSITORY_LENGTH +
  1 +
  MAX_TAG_LENGTH;

// ==================================================
// Public contracts
// ==================================================

export interface ImageTagInput {
  projectId: string;

  deploymentId: string;

  resolvedCommitSha: string;

  buildAttempt: number;
}

export interface ManagedImageIdentity {
  projectId: string;

  deploymentId: string;

  resolvedCommitSha: string;

  buildAttempt: number;
}

export interface ImageArtifactVerificationInput {
  imageId: string | null;

  imageDigest: string | null;

  imageSizeBytes: number | null;
}

export interface VerifiedImageArtifact {
  imageId: string;

  imageDigest: string | null;

  imageSizeBytes: number;
}

// ==================================================
// Error helpers
// ==================================================

function invalidReference(
  message: string,
): BuildError {
  return new BuildError(
    "UNEXPECTED_BUILD_FAILURE",
    message,
  );
}

function verificationFailure(
  message: string,
): BuildError {
  return new BuildError(
    "IMAGE_VERIFICATION_FAILED",
    message,
  );
}

// ==================================================
// Identity validation
// ==================================================

function normalizeUuid(
  value: string,
  identity: "Project" | "Deployment",
): string {
  if (
    typeof value !== "string" ||
    !UUID_PATTERN.test(value)
  ) {
    throw invalidReference(
      `${identity} identity for the image tag is invalid.`,
    );
  }

  return value.toLowerCase();
}

function assertValidCommitSha(
  value: string,
): void {
  if (
    typeof value !== "string" ||
    !COMMIT_SHA_PATTERN.test(value)
  ) {
    throw invalidReference(
      "Commit identity for the image tag is invalid.",
    );
  }
}

function assertValidBuildAttempt(
  value: number,
): void {
  if (
    !Number.isSafeInteger(value) ||
    value < 1
  ) {
    throw invalidReference(
      "Build attempt for the image tag is invalid.",
    );
  }
}

// ==================================================
// Image tag validation
// ==================================================

export function assertValidImageTag(
  tag: string,
): void {
  if (
    typeof tag !== "string" ||
    tag.length === 0 ||
    tag.length > MAX_TAG_LENGTH ||
    !IMAGE_TAG_PATTERN.test(tag)
  ) {
    throw invalidReference(
      "The image tag is invalid.",
    );
  }
}

// ==================================================
// Managed image reference parsing
// ==================================================

/**
 * Parse a CloudPilot-managed image reference.
 *
 * Arbitrary repositories, external registry references,
 * digest-only references and implicit latest tags are
 * not accepted.
 */
export function parseManagedImageReference(
  reference: string,
): ManagedImageIdentity {
  if (
    typeof reference !== "string" ||
    reference.length === 0 ||
    reference.length > MAX_REFERENCE_LENGTH
  ) {
    throw invalidReference(
      "The image reference is invalid.",
    );
  }

  const match =
    MANAGED_IMAGE_REFERENCE_PATTERN.exec(
      reference,
    );

  if (!match) {
    throw invalidReference(
      "The image reference is not a valid CloudPilot-managed reference.",
    );
  }

  const projectId = match[1];

  const resolvedCommitSha = match[2];

  const deploymentId = match[3];

  const attempt = match[4];

  if (
    !projectId ||
    !resolvedCommitSha ||
    !deploymentId ||
    !attempt
  ) {
    throw invalidReference(
      "The image reference identity is incomplete.",
    );
  }

  const buildAttempt = Number(attempt);

  assertValidBuildAttempt(buildAttempt);

  return {
    projectId,

    deploymentId,

    resolvedCommitSha,

    buildAttempt,
  };
}

// ==================================================
// Image reference validation
// ==================================================

/**
 * Validate only CloudPilot-managed image references.
 */
export function assertValidImageReference(
  reference: string,
): void {
  parseManagedImageReference(reference);
}

// ==================================================
// Image tag generation
// ==================================================

/**
 * Generate a deterministic deployment-scoped image tag.
 *
 * Format:
 *
 * cloudpilot/project-<projectId>:
 * <commitSha>-<deploymentId>-<buildAttempt>
 *
 * Different deployments of the same commit receive
 * different image tags.
 */
export function generateImageTag(
  input: ImageTagInput,
): string {
  const projectId = normalizeUuid(
    input.projectId,
    "Project",
  );

  const deploymentId = normalizeUuid(
    input.deploymentId,
    "Deployment",
  );

  assertValidCommitSha(
    input.resolvedCommitSha,
  );

  assertValidBuildAttempt(
    input.buildAttempt,
  );

  const repository =
    `cloudpilot/project-${projectId}`;

  const tag =
    `${input.resolvedCommitSha}-` +
    `${deploymentId}-` +
    `${input.buildAttempt}`;

  assertValidImageTag(tag);

  const reference =
    `${repository}:${tag}`;

  assertValidImageReference(reference);

  return reference;
}

// ==================================================
// Image ownership verification
// ==================================================

/**
 * Verify that an image reference matches the expected
 * project, deployment, commit and build attempt.
 *
 * This checks naming identity only. Database ownership
 * and execution fencing must be established separately.
 */
export function assertImageOwnership(
  reference: string,
  expected: ImageTagInput,
): void {
  const expectedReference =
    generateImageTag(expected);

  assertValidImageReference(reference);

  if (reference !== expectedReference) {
    throw verificationFailure(
      "The image reference does not belong to the expected build attempt.",
    );
  }
}

// ==================================================
// SHA-256 metadata validation
// ==================================================

export function isValidImageDigest(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    SHA256_DIGEST_PATTERN.test(value)
  );
}

export function assertValidImageId(
  imageId: unknown,
): asserts imageId is string {
  if (!isValidImageDigest(imageId)) {
    throw verificationFailure(
      "The image identifier is invalid.",
    );
  }
}

export function assertValidImageDigest(
  digest: unknown,
): asserts digest is string {
  if (!isValidImageDigest(digest)) {
    throw verificationFailure(
      "The image digest is invalid.",
    );
  }
}

export function assertValidImageSize(
  sizeBytes: unknown,
): asserts sizeBytes is number {
  if (
    typeof sizeBytes !== "number" ||
    !Number.isSafeInteger(sizeBytes) ||
    sizeBytes < 0
  ) {
    throw verificationFailure(
      "The image size is invalid.",
    );
  }
}

// ==================================================
// Artifact metadata verification
// ==================================================

/**
 * Validate metadata for a local Docker image artifact.
 *
 * A local image ID is mandatory.
 *
 * A registry manifest digest is optional because
 * locally built images may not have a RepoDigest.
 *
 * Never substitute an image ID for a manifest digest.
 */
export function verifyImageArtifact(
  input: ImageArtifactVerificationInput,
): VerifiedImageArtifact {
  const {
    imageId,
    imageDigest,
    imageSizeBytes,
  } = input;

  assertValidImageId(imageId);

  assertValidImageSize(imageSizeBytes);

  if (imageDigest !== null) {
    assertValidImageDigest(imageDigest);
  }

  return {
    imageId,

    imageDigest,

    imageSizeBytes,
  };
}
