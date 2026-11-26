
/**
 * CloudPilot — Dockerfile Validation
 *
 * Performs bounded Dockerfile reading, structural
 * validation, stage extraction, and build policy
 * enforcement.
 *
 * This module never executes repository-controlled code.
 *
 * Filesystem containment and workspace ownership must
 * already be verified by the build context preparation
 * layer.
 */

import {
  lstat,
  open,
} from "node:fs/promises";

import {
  constants,
} from "node:fs";

import {
  isAbsolute,
} from "node:path";

import {
  TextDecoder,
} from "node:util";

import {
  BuildError,
} from "@cloudpilot/queue";

import {
  parseDockerfileInstructions,
  validateBuildPolicy,
  validateBuildTarget,
  defaultBuildPolicy,
  type BuildPolicy,
  type DockerfileInstruction,
} from "./build-policy.js";

// ==================================================
// Constants
// ==================================================

const MAX_DOCKERFILE_BYTES = 1024 * 1024;

const MAX_BUILD_STAGES = 256;

const READ_CHUNK_BYTES = 64 * 1024;

const STAGE_NAME_PATTERN =
  /^[A-Za-z][A-Za-z0-9_.-]{0,127}$/u;

const RESERVED_STAGE_NAMES =
  new Set(["scratch", "context"]);

const FROM_PATTERN =
  /^(?:--platform=[^\s]+\s+)?([^\s]+)(?:\s+AS\s+([^\s]+))?$/iu;

// ==================================================
// Public contracts
// ==================================================

export interface DockerfileValidationResult {
  content: string;

  stages: string[];

  instructionCount: number;
}

export interface DockerfileValidationOptions {
  policy?: BuildPolicy;

  target?: string | null;

  signal?: AbortSignal;
}

// ==================================================
// Error helpers
// ==================================================

function invalidDockerfile(
  reason: string,
  cause?: unknown,
): BuildError {
  return new BuildError(
    "DOCKERFILE_INVALID",
    `Invalid Dockerfile: ${reason}`,
    cause === undefined
      ? undefined
      : { cause },
  );
}

function dockerfileNotFound(
  cause?: unknown,
): BuildError {
  return new BuildError(
    "DOCKERFILE_NOT_FOUND",
    "The Dockerfile could not be found.",
    cause === undefined
      ? undefined
      : { cause },
  );
}

function isMissingPathError(
  error: unknown,
): boolean {
  if (
    typeof error !== "object" ||
    error === null ||
    !("code" in error)
  ) {
    return false;
  }

  return (
    error.code === "ENOENT" ||
    error.code === "ENOTDIR"
  );
}

// ==================================================
// Cancellation
// ==================================================

function assertNotCancelled(
  signal?: AbortSignal,
): void {
  if (!signal?.aborted) {
    return;
  }

  if (signal.reason instanceof BuildError) {
    throw signal.reason;
  }

  throw new BuildError(
    "BUILD_CANCELLED",
    "Dockerfile validation was cancelled.",
  );
}

// ==================================================
// Bounded Dockerfile reading
// ==================================================

/**
 * Read at most MAX_DOCKERFILE_BYTES + 1.
 *
 * Checking the file size before reading is not enough:
 * the file could grow between inspection and reading.
 */
async function readDockerfile(
  absolutePath: string,
  signal?: AbortSignal,
): Promise<string> {
  assertNotCancelled(signal);

  if (
    typeof absolutePath !== "string" ||
    !isAbsolute(absolutePath) ||
    absolutePath.includes("\0")
  ) {
    throw invalidDockerfile(
      "an absolute validated path is required",
    );
  }

  let handle:
    | Awaited<ReturnType<typeof open>>
    | undefined;

  try {
    const initialInfo =
      await lstat(absolutePath);

    if (
      initialInfo.isSymbolicLink() ||
      !initialInfo.isFile()
    ) {
      throw invalidDockerfile(
        "expected a regular file",
      );
    }

    if (initialInfo.nlink !== 1) {
      throw invalidDockerfile(
        "hard-linked Dockerfiles are not permitted",
      );
    }

    if (
      initialInfo.size >
      MAX_DOCKERFILE_BYTES
    ) {
      throw invalidDockerfile(
        "file exceeds its size limit",
      );
    }

    assertNotCancelled(signal);

    /**
     * O_NOFOLLOW provides additional final-component
     * symlink protection on supported POSIX systems.
     *
     * Windows relies on filesystem inspection and
     * exclusive workspace ownership.
     */
    const flags =
      constants.O_RDONLY |
      (
        process.platform !== "win32"
          ? constants.O_NOFOLLOW ?? 0
          : 0
      ) |
      (
        process.platform !== "win32"
          ? constants.O_NONBLOCK ?? 0
          : 0
      );

    handle = await open(
      absolutePath,
      flags,
    );

    const openedInfo = await handle.stat();

    if (
      !openedInfo.isFile() ||
      openedInfo.nlink !== 1
    ) {
      throw invalidDockerfile(
        "unexpected filesystem entry type",
      );
    }

    if (
      (
        // Windows lstat may report an unavailable device ID.
        !(
          process.platform === "win32" &&
          initialInfo.dev === 0
        ) &&
        openedInfo.dev !== initialInfo.dev
      ) ||
      openedInfo.ino !== initialInfo.ino ||
      openedInfo.size !== initialInfo.size ||
      openedInfo.mtimeMs !== initialInfo.mtimeMs
    ) {
      throw invalidDockerfile(
        "file changed before validation",
      );
    }

    if (
      openedInfo.size >
      MAX_DOCKERFILE_BYTES
    ) {
      throw invalidDockerfile(
        "file exceeds its size limit",
      );
    }

    const chunks: Buffer[] = [];

    const buffer = Buffer.allocUnsafe(
      READ_CHUNK_BYTES,
    );

    let totalBytes = 0;

    while (true) {
      assertNotCancelled(signal);

      const remaining =
        MAX_DOCKERFILE_BYTES +
        1 -
        totalBytes;

      if (remaining <= 0) {
        throw invalidDockerfile(
          "file exceeds its size limit",
        );
      }

      const readLength = Math.min(
        buffer.length,
        remaining,
      );

      const { bytesRead } =
        await handle.read(
          buffer,
          0,
          readLength,
          null,
        );

      if (bytesRead === 0) {
        break;
      }

      totalBytes += bytesRead;

      if (
        totalBytes >
        MAX_DOCKERFILE_BYTES
      ) {
        throw invalidDockerfile(
          "file exceeds its size limit",
        );
      }

      chunks.push(
        Buffer.from(
          buffer.subarray(0, bytesRead),
        ),
      );
    }

    if (totalBytes === 0) {
      throw invalidDockerfile(
        "file is empty",
      );
    }

    const finalInfo = await handle.stat();

    if (
      finalInfo.size !== openedInfo.size ||
      finalInfo.mtimeMs !== openedInfo.mtimeMs ||
      finalInfo.ctimeMs !== openedInfo.ctimeMs ||
      finalInfo.nlink !== 1 ||
      totalBytes !== finalInfo.size
    ) {
      throw invalidDockerfile(
        "file changed during validation",
      );
    }

    assertNotCancelled(signal);

    try {
      /**
       * Fatal UTF-8 decoding rejects malformed bytes
       * instead of silently replacing them.
       */
      const decoder = new TextDecoder(
        "utf-8",
        {
          fatal: true,
          ignoreBOM: true,
        },
      );

      return decoder.decode(
        Buffer.concat(
          chunks,
          totalBytes,
        ),
      );
    } catch (error) {
      throw invalidDockerfile(
        "file is not valid UTF-8",
        error,
      );
    }
  } catch (error) {
    if (error instanceof BuildError) {
      throw error;
    }

    assertNotCancelled(signal);

    if (isMissingPathError(error)) {
      throw dockerfileNotFound(error);
    }

    throw invalidDockerfile(
      "file could not be read safely",
      error,
    );
  } finally {
    if (handle) {
      await handle.close();
    }
  }
}

// ==================================================
// Stage validation
// ==================================================

function validateStageName(
  name: string,
): void {
  if (!STAGE_NAME_PATTERN.test(name)) {
    throw invalidDockerfile(
      "invalid build stage name",
    );
  }

  if (
    RESERVED_STAGE_NAMES.has(
      name.toLowerCase(),
    )
  ) {
    throw invalidDockerfile(
      "reserved build stage name",
    );
  }
}

// ==================================================
// Structural validation
// ==================================================

/**
 * Extract named stages from logical instructions,
 * including instructions spanning multiple lines.
 *
 * This is structural validation, not a replacement
 * for the actual Dockerfile frontend parser.
 */
function extractStages(
  instructions: readonly DockerfileInstruction[],
  signal?: AbortSignal,
): string[] {
  const stages: string[] = [];

  const seenStages = new Set<string>();

  let stageCount = 0;

  for (const instruction of instructions) {
    assertNotCancelled(signal);

    if (instruction.keyword !== "FROM") {
      /**
       * Global ARG instructions may appear before
       * the first FROM instruction.
       */
      if (
        stageCount === 0 &&
        instruction.keyword !== "ARG"
      ) {
        throw invalidDockerfile(
          "only ARG instructions may precede the first FROM stage",
        );
      }

      continue;
    }

    stageCount += 1;

    if (stageCount > MAX_BUILD_STAGES) {
      throw invalidDockerfile(
        "too many build stages",
      );
    }

    const match = FROM_PATTERN.exec(
      instruction.rest.trim(),
    );

    if (!match) {
      throw invalidDockerfile(
        "malformed FROM instruction",
      );
    }

    const imageReference = match[1];

    const stageName = match[2];

    if (
      !imageReference ||
      imageReference.startsWith("--")
    ) {
      throw invalidDockerfile(
        "FROM requires a base image",
      );
    }

    // Unnamed stages are valid.
    if (!stageName) {
      continue;
    }

    validateStageName(stageName);

    const normalizedName =
      stageName.toLowerCase();

    if (seenStages.has(normalizedName)) {
      throw invalidDockerfile(
        "duplicate build stage name",
      );
    }

    seenStages.add(normalizedName);

    stages.push(stageName);
  }

  if (stageCount === 0) {
    throw invalidDockerfile(
      "no FROM stage was found",
    );
  }

  return stages;
}

// ==================================================
// Requested target validation
// ==================================================

function validateRequestedTarget(
  target: string | null | undefined,
  stages: readonly string[],
  policy: BuildPolicy,
): void {
  /**
   * Enforce configured policy authorization first.
   */
  validateBuildTarget(
    target,
    policy,
  );

  if (target == null) {
    return;
  }

  validateStageName(target);

  if (!stages.includes(target)) {
    throw invalidDockerfile(
      "requested build target stage does not exist",
    );
  }
}

// ==================================================
// Public API
// ==================================================

/**
 * Read and validate a Dockerfile located inside
 * an already authorized build context.
 */
export async function validateDockerfile(
  absolutePath: string,
  options?: DockerfileValidationOptions,
): Promise<DockerfileValidationResult> {
  const policy =
    options?.policy ??
    defaultBuildPolicy;

  const signal = options?.signal;

  assertNotCancelled(signal);

  // ------------------------------------------
  // Read bounded, valid UTF-8 content
  // ------------------------------------------

  const content = await readDockerfile(
    absolutePath,
    signal,
  );

  assertNotCancelled(signal);

  // ------------------------------------------
  // Apply security policy
  // ------------------------------------------

  validateBuildPolicy(
    content,
    policy,
  );

  // ------------------------------------------
  // Parse logical instructions
  // ------------------------------------------

  const instructions =
    parseDockerfileInstructions(content);

  if (instructions.length === 0) {
    throw invalidDockerfile(
      "file contains no instructions",
    );
  }

  // ------------------------------------------
  // Validate Dockerfile stages
  // ------------------------------------------

  const stages = extractStages(
    instructions,
    signal,
  );

  // ------------------------------------------
  // Authorize and validate build target
  // ------------------------------------------

  validateRequestedTarget(
    options?.target,
    stages,
    policy,
  );

  assertNotCancelled(signal);

  return {
    content,

    stages,

    instructionCount:
      instructions.length,
  };
}
