
/**
 * CloudPilot — Build Context Preparation
 *
 * Validates local build contexts inside an authorized
 * Phase 5.3 source workspace.
 *
 * Security controls:
 *
 * - Cross-platform relative path validation
 * - Canonical path containment
 * - Intermediate symlink and junction rejection
 * - Hard link and special file rejection
 * - Bounded directory traversal
 * - Context size and file count limits
 * - Cooperative cancellation
 *
 * The source workspace must remain immutable and
 * exclusively controlled throughout build execution.
 */

import {
  lstat,
  opendir,
  realpath,
} from "node:fs/promises";

import type { Stats } from "node:fs";

import {
  relative,
  isAbsolute,
  resolve,
  parse,
  sep,
  join,
  win32,
  posix,
} from "node:path";

import { BuildError } from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const DEFAULT_DOCKERFILE = "Dockerfile";

const MAX_DOCKERFILE_PATH_LENGTH = 500;

const MAX_CONTEXT_PATH_LENGTH = 500;

const MAX_DIRECTORY_DEPTH = 64;

const DEFAULT_MAX_DIRECTORY_COUNT = 50_000;

const MAX_DIRECTORY_COUNT = 1_000_000;

const WINDOWS_RESERVED_NAME =
  /^(?:CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³])(?:\.|$)/iu;

const UNSAFE_PATH_CHARACTERS =
  /[\u0000-\u001f\u007f-\u009f<>:"|?*]/u;

// ==================================================
// Public contracts
// ==================================================

export interface BuildContextInput {
  sourceDir: string;

  dockerfilePath: string | null;

  buildContextPath: string | null;

  maxContextBytes: number;

  maxFileCount: number;

  /**
   * Optional independent directory count limit.
   */
  maxDirectoryCount?: number;

  /**
   * Optional cooperative cancellation signal.
   */
  signal?: AbortSignal;
}

export interface BuildContextResult {
  contextDir: string;

  dockerfileAbsolutePath: string;

  dockerfileRelativePath: string;

  contextSizeBytes: number;

  fileCount: number;
}

interface ContextWalkLimits {
  maxContextBytes: number;

  maxFileCount: number;

  maxDirectoryCount: number;

  signal?: AbortSignal;
}

interface ContextWalkResult {
  sizeBytes: number;

  fileCount: number;
}

// ==================================================
// Error helpers
// ==================================================

function invalidContext(
  reason: string,
  cause?: unknown,
): BuildError {
  return new BuildError(
    "BUILD_CONTEXT_INVALID",
    `Build context is invalid: ${reason}`,
    cause === undefined
      ? undefined
      : { cause },
  );
}

function resourceLimit(
  reason: string,
): BuildError {
  return new BuildError(
    "BUILD_RESOURCE_LIMIT_EXCEEDED",
    reason,
  );
}

function workspaceUnavailable(
  cause?: unknown,
): BuildError {
  return new BuildError(
    "SOURCE_WORKSPACE_UNAVAILABLE",
    "The prepared source workspace is unavailable or unsafe.",
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
    "No regular Dockerfile was found for this deployment.",
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
  if (signal?.aborted) {
    if (signal.reason instanceof BuildError) {
      throw signal.reason;
    }

    throw new BuildError(
      "BUILD_CANCELLED",
      "Build context preparation was cancelled.",
    );
  }
}

// ==================================================
// Limit validation
// ==================================================

function validatePositiveInteger(
  value: number,
  name: string,
  maximum = Number.MAX_SAFE_INTEGER,
): void {
  if (
    !Number.isSafeInteger(value) ||
    value <= 0 ||
    value > maximum
  ) {
    throw invalidContext(
      `${name} is invalid`,
    );
  }
}

// ==================================================
// Path containment
// ==================================================

function assertContained(
  root: string,
  candidate: string,
): void {
  const difference = relative(
    root,
    candidate,
  );

  if (
    difference === ".." ||
    difference.startsWith(`..${sep}`) ||
    isAbsolute(difference)
  ) {
    throw invalidContext(
      "path escapes its authorized directory",
    );
  }
}

function pathsEqual(
  first: string,
  second: string,
): boolean {
  return relative(first, second) === "";
}

// ==================================================
// Portable path validation
// ==================================================

function validatePathSegment(
  segment: string,
): void {
  if (
    segment.length === 0 ||
    segment === "." ||
    segment === ".."
  ) {
    throw invalidContext(
      "unsafe path segment",
    );
  }

  if (
    UNSAFE_PATH_CHARACTERS.test(segment) ||
    segment.includes("/") ||
    segment.includes("\\")
  ) {
    throw invalidContext(
      "unsupported path characters",
    );
  }

  if (
    segment.endsWith(".") ||
    segment.endsWith(" ")
  ) {
    throw invalidContext(
      "ambiguous path segment",
    );
  }

  if (WINDOWS_RESERVED_NAME.test(segment)) {
    throw invalidContext(
      "reserved filesystem name",
    );
  }

  if (
    Buffer.byteLength(segment, "utf8") > 255
  ) {
    throw invalidContext(
      "path segment is too long",
    );
  }
}

function validateRelativeConfigPath(
  value: string,
  maxLength: number,
  allowRoot = false,
): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maxLength
  ) {
    throw invalidContext(
      "configured path length is invalid",
    );
  }

  if (
    UNSAFE_PATH_CHARACTERS.test(value)
  ) {
    throw invalidContext(
      "configured path contains unsafe characters",
    );
  }

  /**
   * Validate both POSIX and Windows paths,
   * regardless of the worker's operating system.
   */
  if (
    isAbsolute(value) ||
    posix.isAbsolute(value) ||
    win32.isAbsolute(value) ||
    /^[A-Za-z]:/u.test(value)
  ) {
    throw invalidContext(
      "absolute paths are not permitted",
    );
  }

  const normalized = value.replace(
    /\\/gu,
    "/",
  );

  const segments: string[] = [];

  for (const segment of normalized.split("/")) {
    if (
      segment === "" ||
      segment === "."
    ) {
      continue;
    }

    if (segment === "..") {
      throw invalidContext(
        "parent directory traversal is not permitted",
      );
    }

    validatePathSegment(segment);

    segments.push(segment);
  }

  if (segments.length > MAX_DIRECTORY_DEPTH) {
    throw invalidContext(
      "configured path is too deeply nested",
    );
  }

  if (segments.length === 0) {
    if (allowRoot) {
      return ".";
    }

    throw invalidContext(
      "configured file path is empty",
    );
  }

  return segments.join("/");
}

// ==================================================
// Filesystem inspection
// ==================================================

async function inspectPath(
  path: string,
  missingFileIsDockerfile = false,
): Promise<Stats> {
  try {
    return await lstat(path);
  } catch (error) {
    if (
      missingFileIsDockerfile &&
      isMissingPathError(error)
    ) {
      throw dockerfileNotFound(error);
    }

    throw invalidContext(
      "filesystem entry could not be inspected",
      error,
    );
  }
}

async function resolveCanonicalPath(
  path: string,
): Promise<string> {
  try {
    return await realpath(path);
  } catch (error) {
    throw invalidContext(
      "filesystem entry could not be resolved",
      error,
    );
  }
}

function assertRegularFile(
  info: Stats,
): void {
  if (
    info.isSymbolicLink() ||
    !info.isFile()
  ) {
    throw invalidContext(
      "expected a regular file",
    );
  }

  /**
   * Prepared GitHub source archives should not
   * contain hard-linked files.
   */
  if (info.nlink !== 1) {
    throw invalidContext(
      "hard-linked files are not permitted",
    );
  }
}

function assertRegularDirectory(
  info: Stats,
): void {
  if (
    info.isSymbolicLink() ||
    !info.isDirectory()
  ) {
    throw invalidContext(
      "expected a regular directory",
    );
  }
}

// ==================================================
// Safe path resolution
// ==================================================

/**
 * Inspect every intermediate path component.
 *
 * Checking only the final path with lstat() is
 * insufficient because an intermediate directory
 * may be a symbolic link or junction.
 */
async function resolveContainedPath(
  root: string,
  configuredPath: string,
  expectedType: "directory" | "file",
  signal?: AbortSignal,
): Promise<string> {
  assertNotCancelled(signal);

  const segments =
    configuredPath === "."
      ? []
      : configuredPath.split("/");

  let current = root;

  if (segments.length === 0) {
    if (expectedType !== "directory") {
      throw invalidContext(
        "expected a file path",
      );
    }

    const info = await inspectPath(current);

    assertRegularDirectory(info);

    return current;
  }

  for (
    let index = 0;
    index < segments.length;
    index += 1
  ) {
    assertNotCancelled(signal);

    const segment = segments[index];

    if (segment === undefined) {
      throw invalidContext(
        "invalid path component",
      );
    }

    validatePathSegment(segment);

    const candidate = join(
      current,
      segment,
    );

    assertContained(root, candidate);

    const isLast =
      index === segments.length - 1;

    const expectsFile =
      isLast && expectedType === "file";

    const info = await inspectPath(
      candidate,
      expectsFile,
    );

    if (info.isSymbolicLink()) {
      throw invalidContext(
        "symbolic links and junctions are not permitted",
      );
    }

    if (expectsFile) {
      if (!info.isFile()) {
        throw dockerfileNotFound();
      }

      assertRegularFile(info);
    } else {
      assertRegularDirectory(info);
    }

    const canonical =
      await resolveCanonicalPath(candidate);

    assertContained(root, canonical);

    if (!pathsEqual(candidate, canonical)) {
      throw invalidContext(
        "unexpected filesystem path redirection",
      );
    }

    current = canonical;
  }

  return current;
}

// ==================================================
// Source workspace validation
// ==================================================

async function resolveSourceRoot(
  sourceDir: string,
  signal?: AbortSignal,
): Promise<string> {
  assertNotCancelled(signal);

  if (
    typeof sourceDir !== "string" ||
    sourceDir.trim().length === 0 ||
    sourceDir.includes("\0") ||
    !isAbsolute(sourceDir)
  ) {
    throw workspaceUnavailable();
  }

  const sourceRoot = resolve(sourceDir);

  // Never permit a filesystem root as a workspace.
  if (
    pathsEqual(
      sourceRoot,
      parse(sourceRoot).root,
    )
  ) {
    throw workspaceUnavailable();
  }

  try {
    const info = await lstat(sourceRoot);

    if (
      !info.isDirectory() ||
      info.isSymbolicLink()
    ) {
      throw workspaceUnavailable();
    }

    const canonicalSource =
      await realpath(sourceRoot);

    if (
      pathsEqual(
        canonicalSource,
        parse(canonicalSource).root,
      )
    ) {
      throw workspaceUnavailable();
    }

    return canonicalSource;
  } catch (error) {
    if (error instanceof BuildError) {
      throw error;
    }

    throw workspaceUnavailable(error);
  }
}

// ==================================================
// Bounded context traversal
// ==================================================

async function walkContext(
  contextDir: string,
  limits: ContextWalkLimits,
): Promise<ContextWalkResult> {
  let sizeBytes = 0;

  let fileCount = 0;

  let directoryCount = 1;

  const visitedDirectories = new Set<string>([
    contextDir,
  ]);

  const stack: Array<{
    path: string;
    depth: number;
  }> = [
    {
      path: contextDir,
      depth: 0,
    },
  ];

  while (stack.length > 0) {
    assertNotCancelled(limits.signal);

    const current = stack.pop();

    if (!current) {
      break;
    }

    if (current.depth > MAX_DIRECTORY_DEPTH) {
      throw resourceLimit(
        "Build context exceeds its directory depth limit.",
      );
    }

    const currentInfo =
      await inspectPath(current.path);

    assertRegularDirectory(currentInfo);

    const canonicalCurrent =
      await resolveCanonicalPath(current.path);

    assertContained(
      contextDir,
      canonicalCurrent,
    );

    if (
      !pathsEqual(
        current.path,
        canonicalCurrent,
      )
    ) {
      throw invalidContext(
        "context directory was redirected",
      );
    }

    /**
     * Stream directory entries instead of loading
     * an entire directory into memory with readdir().
     */
    try {
      const directory = await opendir(
        canonicalCurrent,
        {
          bufferSize: 64,
        },
      );

      for await (const entry of directory) {
        assertNotCancelled(limits.signal);

        validatePathSegment(entry.name);

        const entryPath = join(
          canonicalCurrent,
          entry.name,
        );

        assertContained(
          contextDir,
          entryPath,
        );

        const info =
          await inspectPath(entryPath);

        if (info.isSymbolicLink()) {
          throw invalidContext(
            "symbolic links are not permitted in the build context",
          );
        }

        const canonicalEntry =
          await resolveCanonicalPath(entryPath);

        assertContained(
          contextDir,
          canonicalEntry,
        );

        if (
          !pathsEqual(
            entryPath,
            canonicalEntry,
          )
        ) {
          throw invalidContext(
            "unexpected filesystem path redirection",
          );
        }

        // ------------------------------------------
        // Directory handling
        // ------------------------------------------

        if (info.isDirectory()) {
          const nextDepth =
            current.depth + 1;

          if (
            nextDepth > MAX_DIRECTORY_DEPTH
          ) {
            throw resourceLimit(
              "Build context exceeds its directory depth limit.",
            );
          }

          if (
            directoryCount >=
            limits.maxDirectoryCount
          ) {
            throw resourceLimit(
              "Build context exceeds its directory count limit.",
            );
          }

          if (
            visitedDirectories.has(
              canonicalEntry,
            )
          ) {
            throw invalidContext(
              "repeated directory reference",
            );
          }

          directoryCount += 1;

          visitedDirectories.add(
            canonicalEntry,
          );

          stack.push({
            path: canonicalEntry,
            depth: nextDepth,
          });

          continue;
        }

        // ------------------------------------------
        // Regular file handling
        // ------------------------------------------

        assertRegularFile(info);

        if (
          !Number.isSafeInteger(info.size) ||
          info.size < 0
        ) {
          throw invalidContext(
            "invalid filesystem entry size",
          );
        }

        if (
          fileCount >= limits.maxFileCount
        ) {
          throw resourceLimit(
            "Build context exceeds its file count limit.",
          );
        }

        /**
         * Check before addition to avoid unsafe
         * integer arithmetic and limit overflow.
         */
        if (
          info.size >
          limits.maxContextBytes - sizeBytes
        ) {
          throw resourceLimit(
            "Build context exceeds its size limit.",
          );
        }

        fileCount += 1;

        sizeBytes += info.size;
      }
    } catch (error) {
      if (error instanceof BuildError) {
        throw error;
      }

      assertNotCancelled(limits.signal);

      throw invalidContext(
        "context directory could not be read",
        error,
      );
    }
  }

  assertNotCancelled(limits.signal);

  return {
    sizeBytes,
    fileCount,
  };
}

// ==================================================
// Public API
// ==================================================

/**
 * Prepare a validated local build context.
 *
 * Both the context and Dockerfile must remain
 * inside the authorized source workspace.
 *
 * The Dockerfile must additionally remain inside
 * the selected build context.
 */
export async function prepareBuildContext(
  input: BuildContextInput,
): Promise<BuildContextResult> {
  assertNotCancelled(input.signal);

  validatePositiveInteger(
    input.maxContextBytes,
    "context size limit",
  );

  validatePositiveInteger(
    input.maxFileCount,
    "context file count limit",
  );

  const maxDirectoryCount =
    input.maxDirectoryCount ??
    DEFAULT_MAX_DIRECTORY_COUNT;

  validatePositiveInteger(
    maxDirectoryCount,
    "context directory count limit",
    MAX_DIRECTORY_COUNT,
  );

  // ------------------------------------------
  // Resolve the authorized source workspace
  // ------------------------------------------

  const canonicalSource =
    await resolveSourceRoot(
      input.sourceDir,
      input.signal,
    );

  // ------------------------------------------
  // Resolve the build context
  // ------------------------------------------

  const contextConfig =
    input.buildContextPath?.trim()
      ? input.buildContextPath
      : ".";

  const normalizedContext =
    validateRelativeConfigPath(
      contextConfig,
      MAX_CONTEXT_PATH_LENGTH,
      true,
    );

  const contextDir =
    await resolveContainedPath(
      canonicalSource,
      normalizedContext,
      "directory",
      input.signal,
    );

  assertContained(
    canonicalSource,
    contextDir,
  );

  // ------------------------------------------
  // Resolve the Dockerfile
  // ------------------------------------------

  const dockerfileConfig =
    input.dockerfilePath?.trim()
      ? input.dockerfilePath
      : DEFAULT_DOCKERFILE;

  const normalizedDockerfile =
    validateRelativeConfigPath(
      dockerfileConfig,
      MAX_DOCKERFILE_PATH_LENGTH,
    );

  const dockerfileAbsolutePath =
    await resolveContainedPath(
      contextDir,
      normalizedDockerfile,
      "file",
      input.signal,
    );

  assertContained(
    canonicalSource,
    dockerfileAbsolutePath,
  );

  assertContained(
    contextDir,
    dockerfileAbsolutePath,
  );

  const dockerfileRelativePath = relative(
    contextDir,
    dockerfileAbsolutePath,
  ).split(sep).join("/");

  // ------------------------------------------
  // Validate context resources
  // ------------------------------------------

  const { sizeBytes, fileCount } =
    await walkContext(
      contextDir,
      {
        maxContextBytes:
          input.maxContextBytes,

        maxFileCount:
          input.maxFileCount,

        maxDirectoryCount,

        ...(input.signal
          ? { signal: input.signal }
          : {}),
      },
    );

  // ------------------------------------------
  // Recheck the Dockerfile after traversal
  // ------------------------------------------

  const verifiedDockerfile =
    await resolveContainedPath(
      contextDir,
      normalizedDockerfile,
      "file",
      input.signal,
    );

  if (
    !pathsEqual(
      dockerfileAbsolutePath,
      verifiedDockerfile,
    )
  ) {
    throw invalidContext(
      "Dockerfile path changed during validation",
    );
  }

  assertNotCancelled(input.signal);

  return {
    contextDir,

    dockerfileAbsolutePath:
      verifiedDockerfile,

    dockerfileRelativePath,

    contextSizeBytes: sizeBytes,

    fileCount,
  };
}
