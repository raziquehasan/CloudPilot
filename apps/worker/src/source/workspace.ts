/**
 * CloudPilot — Secure Source Workspace Manager
 *
 * Manages isolated source preparation workspaces,
 * metadata persistence, disk capacity checks,
 * attempt ownership, and conservative cleanup.
 */

import { randomUUID } from "node:crypto";

import {
  mkdir,
  readdir,
  readFile,
  rename,
  rm,
  lstat,
  realpath,
  statfs,
  open,
} from "node:fs/promises";

import {
  join,
  resolve,
  relative,
  isAbsolute,
  sep,
  parse,
} from "node:path";

import {
  SourcePreparationError,
  isSourceFailureCode,
} from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const SAFE_IDENTIFIER = /^[A-Za-z0-9-]{1,64}$/u;

const COMMIT_SHA_PATTERN = /^[0-9a-f]{40}$/u;

const REPOSITORY_PATTERN =
  /^[A-Za-z0-9-]+\/[A-Za-z0-9_.-]+$/u;

const WINDOWS_RESERVED_NAME =
  /^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$/iu;

const WORKSPACE_DIR_MODE = 0o700;

const METADATA_FILE_MODE = 0o600;

const MAX_METADATA_BYTES = 64 * 1024;

const MIN_TTL_SECONDS = 60;

const MAX_TTL_SECONDS = 86_400;

const ORPHAN_GRACE_PERIOD_MS = 60_000;

// ==================================================
// Interfaces
// ==================================================

export interface WorkspaceManagerOptions {
  root: string;

  ttlSeconds: number;

  minFreeBytes: number;

  clock?: () => Date;

  random?: () => string;

  /**
   * Optional authoritative cleanup authorization.
   *
   * Production callers should consult PostgreSQL
   * execution leases and source preparation records.
   *
   * Missing authorization fails closed.
   */
  canRemoveAttempt?: (
    deploymentId: string,
    attemptId: string,
  ) => Promise<boolean>;
}

export interface PreparedWorkspace {
  attemptId: string;

  deploymentId: string;

  sourceDir: string;

  metadataPath: string;

  createdAt: string;
}

export interface WorkspaceMetadata {
  deploymentId: string;

  attemptId: string;

  repositoryFullName: string;

  requestedBranch: string | null;

  resolvedCommitSha: string;

  status: "READY" | "FAILED" | "CANCELLED";

  downloadedBytes: number;

  extractedBytes: number;

  fileCount: number;

  failureCode: string | null;

  createdAt: string;

  completedAt: string;
}

interface CleanupResult {
  removed: number;

  skippedActive: number;
}

// ==================================================
// Validation utilities
// ==================================================

function isSafeIdentifier(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    SAFE_IDENTIFIER.test(value) &&
    !WINDOWS_RESERVED_NAME.test(value)
  );
}

function assertSafeIdentifier(
  value: string,
  label: string,
): void {
  if (!isSafeIdentifier(value)) {
    throw new SourcePreparationError(
      "WORKSPACE_UNAVAILABLE",
      `${label} is invalid.`,
    );
  }
}

function isNonNegativeInteger(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 0
  );
}

function isValidTimestamp(
  value: unknown,
): value is string {
  if (typeof value !== "string") {
    return false;
  }

  const timestamp = new Date(value);

  return (
    Number.isFinite(timestamp.getTime()) &&
    timestamp.toISOString() === value
  );
}

function isMissingPath(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "ENOENT"
  );
}

function isAlreadyExists(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "EEXIST"
  );
}

// ==================================================
// Metadata runtime validation
// ==================================================

function isWorkspaceMetadata(
  value: unknown,
): value is WorkspaceMetadata {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value)
  ) {
    return false;
  }

  const metadata =
    value as Record<string, unknown>;

  if (
    !isSafeIdentifier(metadata.deploymentId) ||
    !isSafeIdentifier(metadata.attemptId)
  ) {
    return false;
  }

  if (
    typeof metadata.repositoryFullName !== "string" ||
    metadata.repositoryFullName.length > 255 ||
    !REPOSITORY_PATTERN.test(
      metadata.repositoryFullName,
    )
  ) {
    return false;
  }

  if (
    metadata.requestedBranch !== null &&
    (
      typeof metadata.requestedBranch !== "string" ||
      metadata.requestedBranch.length === 0 ||
      metadata.requestedBranch.length > 255
    )
  ) {
    return false;
  }

  if (
    typeof metadata.resolvedCommitSha !== "string" ||
    !COMMIT_SHA_PATTERN.test(
      metadata.resolvedCommitSha,
    )
  ) {
    return false;
  }

  if (
    metadata.status !== "READY" &&
    metadata.status !== "FAILED" &&
    metadata.status !== "CANCELLED"
  ) {
    return false;
  }

  if (
    !isNonNegativeInteger(metadata.downloadedBytes) ||
    !isNonNegativeInteger(metadata.extractedBytes) ||
    !isNonNegativeInteger(metadata.fileCount)
  ) {
    return false;
  }

  if (
    metadata.failureCode !== null &&
    !isSourceFailureCode(metadata.failureCode)
  ) {
    return false;
  }

  if (
    metadata.status === "READY" &&
    metadata.failureCode !== null
  ) {
    return false;
  }

  if (
    !isValidTimestamp(metadata.createdAt) ||
    !isValidTimestamp(metadata.completedAt)
  ) {
    return false;
  }

  if (
    Date.parse(metadata.completedAt) <
    Date.parse(metadata.createdAt)
  ) {
    return false;
  }

  return true;
}

// ==================================================
// Workspace manager
// ==================================================

export class WorkspaceManager {
  private readonly root: string;

  private canonicalRoot: string | null = null;

  /**
   * Local process protection only.
   *
   * Cross-process ownership must be coordinated
   * through PostgreSQL execution leases.
   */
  private readonly active = new Set<string>();

  private readonly cleaning = new Set<string>();

  constructor(
    private readonly options: WorkspaceManagerOptions,
  ) {
    if (
      typeof options.root !== "string" ||
      options.root.trim().length === 0
    ) {
      throw new Error(
        "Source workspace root is required",
      );
    }

    if (
      !Number.isSafeInteger(options.ttlSeconds) ||
      options.ttlSeconds < MIN_TTL_SECONDS ||
      options.ttlSeconds > MAX_TTL_SECONDS
    ) {
      throw new Error(
        "Source workspace TTL must be between 60 and 86400 seconds",
      );
    }

    if (
      !Number.isSafeInteger(options.minFreeBytes) ||
      options.minFreeBytes < 0
    ) {
      throw new Error(
        "minFreeBytes must be a non-negative safe integer",
      );
    }

    this.root = resolve(options.root);

    if (this.root === parse(this.root).root) {
      throw new Error(
        "Filesystem root cannot be used as a source workspace",
      );
    }
  }

  // ================================================
  // Public root accessor
  // ================================================

  get rootPath(): string {
    return this.root;
  }

  // ================================================
  // Internal utilities
  // ================================================

  private now(): Date {
    return this.options.clock
      ? this.options.clock()
      : new Date();
  }

  private randomId(): string {
    const identifier = this.options.random
      ? this.options.random()
      : randomUUID();

    assertSafeIdentifier(
      identifier,
      "Generated identifier",
    );

    return identifier;
  }

  private safeJoin(...segments: string[]): string {
    const target = resolve(
      this.root,
      ...segments,
    );

    const difference = relative(
      this.root,
      target,
    );

    if (
      difference === ".." ||
      difference.startsWith(`..${sep}`) ||
      isAbsolute(difference)
    ) {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Workspace path escapes the configured root.",
      );
    }

    return target;
  }

  private attemptPath(
    deploymentId: string,
    attemptId: string,
  ): string {
    assertSafeIdentifier(
      deploymentId,
      "Deployment identifier",
    );

    assertSafeIdentifier(
      attemptId,
      "Attempt identifier",
    );

    return this.safeJoin(
      "deployments",
      deploymentId,
      attemptId,
    );
  }

  // ================================================
  // Filesystem containment
  // ================================================

  private async assertDirectory(
    path: string,
  ): Promise<void> {
    const info = await lstat(path);

    if (
      !info.isDirectory() ||
      info.isSymbolicLink()
    ) {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Unsafe workspace directory.",
      );
    }
  }

  private async assertContainedDirectory(
    path: string,
  ): Promise<void> {
    const difference = relative(
      this.root,
      path,
    );

    if (
      difference === ".." ||
      difference.startsWith(`..${sep}`) ||
      isAbsolute(difference)
    ) {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Workspace directory escapes the root.",
      );
    }

    await this.assertDirectory(this.root);

    let current = this.root;

    for (
      const segment of difference.split(sep)
    ) {
      if (!segment) continue;

      current = join(current, segment);

      await this.assertDirectory(current);
    }

    const canonicalPath = await realpath(path);

    const canonicalRoot =
      this.canonicalRoot ??
      await realpath(this.root);

    const canonicalDifference = relative(
      canonicalRoot,
      canonicalPath,
    );

    if (
      canonicalDifference === ".." ||
      canonicalDifference.startsWith(`..${sep}`) ||
      isAbsolute(canonicalDifference)
    ) {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Resolved workspace directory escapes the root.",
      );
    }
  }

  private async ensureChildDirectory(
    path: string,
  ): Promise<void> {
    try {
      await mkdir(path, {
        mode: WORKSPACE_DIR_MODE,
      });
    } catch (error) {
      if (!isAlreadyExists(error)) {
        throw error;
      }
    }

    await this.assertContainedDirectory(path);
  }

  // ================================================
  // Root initialization
  // ================================================

  async ensureRoot(): Promise<void> {
    try {
      await mkdir(this.root, {
        recursive: true,
        mode: WORKSPACE_DIR_MODE,
      });

      await this.assertDirectory(this.root);

      const canonical = await realpath(this.root);

      if (
        this.canonicalRoot !== null &&
        this.canonicalRoot !== canonical
      ) {
        throw new SourcePreparationError(
          "WORKSPACE_UNAVAILABLE",
          "Workspace root identity changed.",
        );
      }

      this.canonicalRoot = canonical;
    } catch (error) {
      if (error instanceof SourcePreparationError) {
        throw error;
      }

      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Source workspace root is unavailable.",
      );
    }
  }

  // ================================================
  // Disk capacity verification
  // ================================================

  async checkDiskSpace(): Promise<void> {
    try {
      await this.ensureRoot();

      const stats = await statfs(this.root, {
        bigint: true,
      });

      const availableBytes =
        stats.bavail * stats.bsize;

      if (
        availableBytes <
        BigInt(this.options.minFreeBytes)
      ) {
        throw new SourcePreparationError(
          "INSUFFICIENT_DISK_SPACE",
          "Insufficient disk space for source retrieval.",
        );
      }
    } catch (error) {
      if (error instanceof SourcePreparationError) {
        throw error;
      }

      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Disk space could not be verified.",
      );
    }
  }

  // ================================================
  // Isolated attempt creation
  // ================================================

  async prepareAttempt(
    deploymentId: string,
  ): Promise<PreparedWorkspace> {
    assertSafeIdentifier(
      deploymentId,
      "Deployment identifier",
    );

    await this.ensureRoot();

    await this.checkDiskSpace();

    const attemptId = this.randomId();

    if (
      this.active.has(attemptId) ||
      this.cleaning.has(attemptId)
    ) {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Workspace attempt identifier is already in use.",
      );
    }

    const deploymentsRoot =
      this.safeJoin("deployments");

    const deploymentDir =
      this.safeJoin(
        "deployments",
        deploymentId,
      );

    const attemptDir = this.attemptPath(
      deploymentId,
      attemptId,
    );

    const sourceDir = join(
      attemptDir,
      "source",
    );

    const metadataPath = join(
      attemptDir,
      "metadata.json",
    );

    let createdAttempt = false;

    this.active.add(attemptId);

    try {
      await this.ensureChildDirectory(
        deploymentsRoot,
      );

      await this.ensureChildDirectory(
        deploymentDir,
      );

      // Exclusive creation prevents accidental reuse
      // of an existing source preparation attempt.

      await mkdir(attemptDir, {
        recursive: false,
        mode: WORKSPACE_DIR_MODE,
      });

      createdAttempt = true;

      await this.assertContainedDirectory(
        attemptDir,
      );

      await mkdir(sourceDir, {
        recursive: false,
        mode: WORKSPACE_DIR_MODE,
      });

      await this.assertContainedDirectory(
        sourceDir,
      );

      return {
        attemptId,

        deploymentId,

        sourceDir,

        metadataPath,

        createdAt:
          this.now().toISOString(),
      };
    } catch (error) {
      this.active.delete(attemptId);

      if (createdAttempt) {
        try {
          await this.assertContainedDirectory(
            attemptDir,
          );

          await rm(attemptDir, {
            recursive: true,
            force: true,
          });
        } catch {
          // Preserve unsafe or inaccessible paths
          // for controlled recovery.
        }
      }

      if (error instanceof SourcePreparationError) {
        throw error;
      }

      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Could not create an isolated source workspace.",
      );
    }
  }

  // ================================================
  // Local attempt ownership
  // ================================================

  isActive(attemptId: string): boolean {
    return this.active.has(attemptId);
  }

  release(attemptId: string): void {
    this.active.delete(attemptId);
  }

  // ================================================
  // Atomic metadata persistence
  // ================================================

  async writeMetadata(
    metadata: WorkspaceMetadata,
  ): Promise<void> {
    if (!isWorkspaceMetadata(metadata)) {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Invalid source workspace metadata.",
      );
    }

    const attemptDir = this.attemptPath(
      metadata.deploymentId,
      metadata.attemptId,
    );

    await this.assertContainedDirectory(
      attemptDir,
    );

    const metadataPath = join(
      attemptDir,
      "metadata.json",
    );

    const tempPath = join(
      attemptDir,
      `metadata.${this.randomId()}.tmp`,
    );

    const json =
      `${JSON.stringify(metadata, null, 2)}\n`;

    if (
      Buffer.byteLength(json, "utf8") >
      MAX_METADATA_BYTES
    ) {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Source metadata exceeds its size limit.",
      );
    }

    let createdTemp = false;

    try {
      const handle = await open(
        tempPath,
        "wx",
        METADATA_FILE_MODE,
      );

      createdTemp = true;

      try {
        await handle.writeFile(
          json,
          "utf8",
        );

        await handle.sync();
      } finally {
        await handle.close();
      }

      await this.assertContainedDirectory(
        attemptDir,
      );

      await rename(
        tempPath,
        metadataPath,
      );

      createdTemp = false;
    } catch {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Could not persist source workspace metadata.",
      );
    } finally {
      if (createdTemp) {
        try {
          await this.assertContainedDirectory(
            attemptDir,
          );

          await rm(tempPath, {
            force: true,
          });
        } catch {
          // Controlled recovery can remove
          // abandoned temporary metadata.
        }
      }
    }
  }

  // ================================================
  // Metadata retrieval and validation
  // ================================================

  async readMetadata(
    deploymentId: string,
    attemptId: string,
  ): Promise<WorkspaceMetadata | null> {
    const attemptDir = this.attemptPath(
      deploymentId,
      attemptId,
    );

    const metadataPath = join(
      attemptDir,
      "metadata.json",
    );

    try {
      await this.assertContainedDirectory(
        attemptDir,
      );

      const info = await lstat(
        metadataPath,
      );

      if (
        !info.isFile() ||
        info.isSymbolicLink() ||
        info.size > MAX_METADATA_BYTES
      ) {
        throw new SourcePreparationError(
          "WORKSPACE_UNAVAILABLE",
          "Unsafe source metadata file.",
        );
      }

      const raw = await readFile(
        metadataPath,
        "utf8",
      );

      if (
        Buffer.byteLength(raw, "utf8") >
        MAX_METADATA_BYTES
      ) {
        throw new SourcePreparationError(
          "WORKSPACE_UNAVAILABLE",
          "Source metadata exceeds its size limit.",
        );
      }

      const metadata: unknown =
        JSON.parse(raw);

      if (
        !isWorkspaceMetadata(metadata) ||
        metadata.deploymentId !== deploymentId ||
        metadata.attemptId !== attemptId
      ) {
        throw new SourcePreparationError(
          "WORKSPACE_UNAVAILABLE",
          "Source metadata identity is invalid.",
        );
      }

      return metadata;
    } catch (error) {
      if (isMissingPath(error)) {
        return null;
      }

      if (error instanceof SourcePreparationError) {
        throw error;
      }

      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Source workspace metadata could not be read.",
      );
    }
  }

  // ================================================
  // Prepared source resolution (build phase)
  // ================================================

  /**
   * Resolve and validate the source directory of a prepared
   * attempt for the build phase. Verifies containment and
   * that the source directory exists and is a real directory.
   *
   * Does not take local ownership; distributed ownership is
   * coordinated through PostgreSQL execution leases.
   */
  async resolvePreparedSourceDir(
    deploymentId: string,
    attemptId: string,
  ): Promise<string> {
    const attemptDir = this.attemptPath(
      deploymentId,
      attemptId,
    );

    const sourceDir = join(attemptDir, "source");

    try {
      await this.assertContainedDirectory(sourceDir);
    } catch (error) {
      if (error instanceof SourcePreparationError) {
        throw error;
      }

      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Prepared source workspace is unavailable.",
      );
    }

    return sourceDir;
  }

  // ================================================
  // Explicit attempt cleanup
  // ================================================

  async cleanupAttempt(
    deploymentId: string,
    attemptId: string,
  ): Promise<void> {
    const attemptDir = this.attemptPath(
      deploymentId,
      attemptId,
    );

    if (
      this.active.has(attemptId) ||
      this.cleaning.has(attemptId)
    ) {
      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Refusing to remove an active workspace.",
      );
    }

    this.cleaning.add(attemptId);

    try {
      await this.assertContainedDirectory(
        attemptDir,
      );

      await rm(attemptDir, {
        recursive: true,
        force: true,
        maxRetries: 3,
        retryDelay: 100,
      });
    } catch (error) {
      if (isMissingPath(error)) {
        return;
      }

      if (error instanceof SourcePreparationError) {
        throw error;
      }

      throw new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Source workspace cleanup failed.",
      );
    } finally {
      this.cleaning.delete(attemptId);
    }
  }

  // ================================================
  // Authoritative cleanup eligibility
  // ================================================

  private async canRemove(
    deploymentId: string,
    attemptId: string,
  ): Promise<boolean> {
    if (
      this.active.has(attemptId) ||
      this.cleaning.has(attemptId)
    ) {
      return false;
    }

    // Never infer distributed execution ownership
    // from directory timestamps alone.

    if (!this.options.canRemoveAttempt) {
      return false;
    }

    try {
      return (
        await this.options.canRemoveAttempt(
          deploymentId,
          attemptId,
        )
      ) === true;
    } catch {
      // Database or ownership verification failure
      // must prevent automatic deletion.

      return false;
    }
  }

  // ================================================
  // Shared workspace cleanup scanner
  // ================================================

  private async scanForCleanup(
    cutoff: number,
    orphanRecovery: boolean,
  ): Promise<CleanupResult> {
    await this.ensureRoot();

    const deploymentsRoot =
      this.safeJoin("deployments");

    let removed = 0;

    let skippedActive = 0;

    try {
      await this.assertContainedDirectory(
        deploymentsRoot,
      );
    } catch (error) {
      if (isMissingPath(error)) {
        return {
          removed,
          skippedActive,
        };
      }

      throw error;
    }

    const deployments = await readdir(
      deploymentsRoot,
      {
        withFileTypes: true,
      },
    );

    for (const deployment of deployments) {
      if (
        !isSafeIdentifier(deployment.name) ||
        !deployment.isDirectory() ||
        deployment.isSymbolicLink()
      ) {
        continue;
      }

      const deploymentId =
        deployment.name;

      const deploymentDir =
        this.safeJoin(
          "deployments",
          deploymentId,
        );

      try {
        await this.assertContainedDirectory(
          deploymentDir,
        );

        const attempts = await readdir(
          deploymentDir,
          {
            withFileTypes: true,
          },
        );

        for (const attempt of attempts) {
          if (
            !isSafeIdentifier(attempt.name) ||
            !attempt.isDirectory() ||
            attempt.isSymbolicLink()
          ) {
            continue;
          }

          const attemptId = attempt.name;

          if (
            this.active.has(attemptId) ||
            this.cleaning.has(attemptId)
          ) {
            skippedActive += 1;

            continue;
          }

          const attemptDir =
            this.attemptPath(
              deploymentId,
              attemptId,
            );

          try {
            await this.assertContainedDirectory(
              attemptDir,
            );

            const info = await lstat(
              attemptDir,
            );

            if (info.mtimeMs >= cutoff) {
              continue;
            }

            const metadata =
              await this.readMetadata(
                deploymentId,
                attemptId,
              );

            // Restart recovery must never remove
            // a successfully prepared source.

            if (
              orphanRecovery &&
              metadata?.status === "READY"
            ) {
              continue;
            }

            if (metadata) {
              const completedAt =
                Date.parse(
                  metadata.completedAt,
                );

              if (completedAt >= cutoff) {
                continue;
              }
            }

            if (
              !await this.canRemove(
                deploymentId,
                attemptId,
              )
            ) {
              continue;
            }

            await this.cleanupAttempt(
              deploymentId,
              attemptId,
            );

            removed += 1;
          } catch {
            // Skip unsafe, malformed, inaccessible,
            // or concurrently modified workspaces.
          }
        }

        // Intentionally do not recursively delete
        // the deployment parent directory.
        //
        // Another worker may be creating a new
        // attempt inside it concurrently.
      } catch {
        // A single invalid deployment directory
        // must not interrupt the complete scan.
      }
    }

    return {
      removed,
      skippedActive,
    };
  }

  // ================================================
  // Expired workspace cleanup
  // ================================================

  async cleanupExpired(): Promise<{
    removed: number;
    skippedActive: number;
  }> {
    const cutoff =
      this.now().getTime() -
      this.options.ttlSeconds * 1000;

    return this.scanForCleanup(
      cutoff,
      false,
    );
  }

  // ================================================
  // Conservative restart recovery
  // ================================================

  async recoverOrphaned(): Promise<{
    removed: number;
  }> {
    const cutoff =
      this.now().getTime() -
      ORPHAN_GRACE_PERIOD_MS;

    const result =
      await this.scanForCleanup(
        cutoff,
        true,
      );

    return {
      removed: result.removed,
    };
  }
}