/**
 * CloudPilot — Build Engine
 *
 * Orchestrates a single authorized image build:
 *
 *   authorize execution
 *   -> validate context
 *   -> validate Dockerfile and policy
 *   -> verify builder capabilities
 *   -> execute build
 *   -> verify artifact identity
 *   -> finalize logs
 *   -> return verified metadata
 *
 * Phase 5.6 integration:
 *
 * - Bounded real-time build log collection.
 * - Registered secret redaction.
 * - Explicit live subscriber lifecycle.
 * - Late executor callback protection.
 * - Subscriber failure diagnostics.
 * - Cancellation-aware log collection.
 * - Final deadline verification.
 * - Legacy build log snapshot compatibility.
 *
 * The engine never publishes images externally.
 *
 * Database ownership, execution leases, trusted repository
 * authorization and artifact lifecycle management remain
 * responsibilities of the orchestration layer.
 *
 * Log streaming is best effort. Successful collection
 * does not establish successful database persistence.
 */

import { performance } from "node:perf_hooks";

import {
  BuildError,
  type BuildArtifactType,
} from "@cloudpilot/queue";

import {
  prepareBuildContext,
} from "./build-context.js";

import {
  validateDockerfile,
} from "./dockerfile-validator.js";

import {
  generateImageTag,
  assertImageOwnership,
  verifyImageArtifact,
  type ImageTagInput,
} from "./image-artifact.js";

import {
  BuildLogCollector,
  type BuildLogEntry,
} from "./build-logs.js";

import type {
  BuildExecutor,
  BuildExecutorResult,
  BuilderCapabilities,
  ImageInspection,
} from "./build-executor.js";

import {
  defaultBuildPolicy,
  type BuildPolicy,
} from "./build-policy.js";

// ==================================================
// Constants
// ==================================================

const MAX_BUILD_TIMEOUT_MS = 3_600_000;

const MAX_BUILD_LOG_BYTES =
  100 * 1024 * 1024;

const MAX_BUILD_LOG_ENTRIES = 100_000;

const MAX_REGISTERED_SECRETS = 64;

const MIN_REGISTERED_SECRET_BYTES = 4;

const MAX_REGISTERED_SECRET_BYTES = 4_096;

const MAX_REGISTERED_SECRET_TOTAL_BYTES =
  256 * 1024;

// ==================================================
// Public contracts
// ==================================================

export interface BuildEngineLimits {
  timeoutMs: number;

  maxContextBytes: number;

  maxFileCount: number;

  maxLogBytes: number;

  maxDirectoryCount?: number;

  maxLogEntries?: number;
}

export interface BuildEngineInput {
  sourceDir: string;

  projectId: string;

  deploymentId: string;

  resolvedCommitSha: string;

  buildAttempt: number;

  dockerfilePath: string | null;

  buildContextPath: string | null;

  buildArgs: Readonly<Record<string, string>>;

  target: string | null;

  platform: string;

  limits: BuildEngineLimits;

  executor: BuildExecutor;

  signal: AbortSignal;

  policy?: BuildPolicy;

  /**
   * Must be established by the trusted orchestration
   * layer, never directly from user input.
   *
   * Defaults to false.
   */
  trustedRepository?: boolean;

  /**
   * Optional synchronous live streaming sink invoked
   * for sanitized build log entries.
   *
   * The sink must perform bounded synchronous work.
   *
   * Database persistence belongs in the bounded
   * unified log writer, not directly in this callback.
   *
   * Local BuildLogEntry sequence numbers are not
   * deployment-wide database sequence numbers.
   */
  onLogEntry?: (entry: BuildLogEntry) => void;

  /**
   * Optional sensitive values supplied by trusted
   * orchestration for defensive log redaction.
   *
   * These values are not forwarded to the executor.
   *
   * Infrastructure credentials must never be passed
   * to repository builds.
   */
  registeredLogSecrets?: readonly string[];
}

export interface BuildEngineResult {
  imageTag: string;

  artifactType: BuildArtifactType;

  imageId: string | null;

  imageDigest: string | null;

  imageSizeBytes: number | null;

  platform: string;

  durationMs: number;

  contextSizeBytes: number;

  fileCount: number;

  logEntries: BuildLogEntry[];

  logsTruncated: boolean;

  /**
   * Optional diagnostic count for synchronous
   * live subscriber failures.
   *
   * Does not represent asynchronous persistence
   * failures inside the unified log writer.
   */
  logSubscriberFailures?: number;
}

// ==================================================
// Failure diagnostics
// ==================================================

export interface BuildEngineFailureDetails {
  imageTag: string | null;

  durationMs: number;

  contextSizeBytes: number | null;

  fileCount: number | null;

  logEntries: BuildLogEntry[];

  logsTruncated: boolean;

  /**
   * True once the executor has been invoked.
   *
   * This does not establish that an image exists.
   */
  artifactMayExist: boolean;

  /**
   * True only when the executor's build promise
   * returned successfully.
   *
   * False does not prove the backend is still running,
   * nor does it prove that the backend has stopped.
   */
  executorReturnedSuccessfully: boolean;

  /**
   * Synchronous live subscriber failures.
   */
  logSubscriberFailures?: number;
}

/**
 * Preserves the typed BuildError contract while
 * attaching bounded build diagnostics.
 */
export class BuildEngineError extends BuildError {
  readonly details: BuildEngineFailureDetails;

  constructor(
    error: BuildError,
    details: BuildEngineFailureDetails,
  ) {
    super(
      error.code,
      error.message,
      { cause: error },
    );

    this.name = "BuildEngineError";

    this.details = details;
  }
}

// ==================================================
// Validation helpers
// ==================================================

function assertPositiveInteger(
  value: number,
  name: string,
  maximum = Number.MAX_SAFE_INTEGER,
): void {
  if (
    !Number.isSafeInteger(value) ||
    value <= 0 ||
    value > maximum
  ) {
    throw new BuildError(
      "BUILD_CONTEXT_INVALID",
      `Invalid ${name}.`,
    );
  }
}

function validateLimits(
  limits: BuildEngineLimits,
): void {
  assertPositiveInteger(
    limits.timeoutMs,
    "build timeout",
    MAX_BUILD_TIMEOUT_MS,
  );

  assertPositiveInteger(
    limits.maxContextBytes,
    "build context byte limit",
  );

  assertPositiveInteger(
    limits.maxFileCount,
    "build context file limit",
  );

  assertPositiveInteger(
    limits.maxLogBytes,
    "build log byte limit",
    MAX_BUILD_LOG_BYTES,
  );

  if (
    limits.maxDirectoryCount !== undefined
  ) {
    assertPositiveInteger(
      limits.maxDirectoryCount,
      "build context directory limit",
      1_000_000,
    );
  }

  if (
    limits.maxLogEntries !== undefined
  ) {
    assertPositiveInteger(
      limits.maxLogEntries,
      "build log entry limit",
      MAX_BUILD_LOG_ENTRIES,
    );
  }
}

function validateLiveLogSink(
  listener:
    | BuildEngineInput["onLogEntry"]
    | undefined,
): void {
  if (
    listener !== undefined &&
    typeof listener !== "function"
  ) {
    throw new BuildError(
      "BUILD_CONTEXT_INVALID",
      "Invalid build log streaming configuration.",
    );
  }
}

/**
 * Validate explicitly registered log secrets.
 *
 * Never include rejected secret values in errors.
 *
 * Short or multiline secrets cannot safely be
 * supported by the current line-oriented redactor.
 */
function validateRegisteredLogSecrets(
  secrets: readonly string[],
): string[] {
  if (
    !Array.isArray(secrets) ||
    secrets.length >
      MAX_REGISTERED_SECRETS
  ) {
    throw new BuildError(
      "BUILD_CONTEXT_INVALID",
      "Invalid build log redaction configuration.",
    );
  }

  const uniqueSecrets =
    new Set<string>();

  let totalBytes = 0;

  for (const secret of secrets) {
    if (
      typeof secret !== "string" ||
      !secret.trim() ||
      /[\u0000\r\n]/u.test(secret)
    ) {
      throw new BuildError(
        "BUILD_CONTEXT_INVALID",
        "Invalid registered build log secret.",
      );
    }

    const bytes = Buffer.byteLength(
      secret,
      "utf8",
    );

    if (
      bytes <
        MIN_REGISTERED_SECRET_BYTES ||
      bytes >
        MAX_REGISTERED_SECRET_BYTES
    ) {
      throw new BuildError(
        "BUILD_CONTEXT_INVALID",
        "Invalid registered build log secret size.",
      );
    }

    if (uniqueSecrets.has(secret)) {
      continue;
    }

    totalBytes += bytes;

    if (
      totalBytes >
      MAX_REGISTERED_SECRET_TOTAL_BYTES
    ) {
      throw new BuildError(
        "BUILD_CONTEXT_INVALID",
        "Build log redaction capacity exceeded.",
      );
    }

    /**
     * Preserve the exact original value.
     */
    uniqueSecrets.add(secret);
  }

  return [...uniqueSecrets];
}

// ==================================================
// Cancellation and deadline helpers
// ==================================================

function cancellationError(
  signal: AbortSignal,
): BuildError {
  if (signal.reason instanceof BuildError) {
    return signal.reason;
  }

  return new BuildError(
    "BUILD_CANCELLED",
    "The build was cancelled.",
  );
}

function assertActive(
  signal: AbortSignal,
): void {
  if (signal.aborted) {
    throw cancellationError(signal);
  }
}

function createTimeoutError(): BuildError {
  return new BuildError(
    "BUILD_TIMEOUT",
    "The build exceeded its execution deadline.",
  );
}

function remainingTime(
  deadline: number,
  signal: AbortSignal,
): number {
  assertActive(signal);

  const remaining = Math.floor(
    deadline - performance.now(),
  );

  if (remaining <= 0) {
    throw createTimeoutError();
  }

  return remaining;
}

/**
 * Wait for a non-mutating backend operation while
 * observing cancellation.
 *
 * The underlying operation may continue if its
 * implementation does not support AbortSignal.
 *
 * Never use this helper to detach an active image
 * build from its executor.
 */
function awaitReadOperation<T>(
  operation: () => Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  assertActive(signal);

  return new Promise<T>((resolve, reject) => {
    let settled = false;

    const cleanup = (): void => {
      signal.removeEventListener(
        "abort",
        abort,
      );
    };

    const abort = (): void => {
      if (settled) {
        return;
      }

      settled = true;

      cleanup();

      reject(cancellationError(signal));
    };

    signal.addEventListener(
      "abort",
      abort,
      { once: true },
    );

    if (signal.aborted) {
      abort();

      return;
    }

    Promise.resolve()
      .then(() => {
        assertActive(signal);

        return operation();
      })
      .then(
        (result) => {
          if (settled) {
            return;
          }

          settled = true;

          cleanup();

          resolve(result);
        },
        (error: unknown) => {
          if (settled) {
            return;
          }

          settled = true;

          cleanup();

          reject(error);
        },
      );
  });
}

// ==================================================
// Builder authorization
// ==================================================

function assertBuilderAllowed(
  capabilities: BuilderCapabilities,
  trustedRepository: boolean,
): void {
  if (!capabilities.available) {
    throw new BuildError(
      "BUILDER_UNAVAILABLE",
      "No compatible build backend is available.",
    );
  }

  /**
   * Phase 5.4 intentionally supports trusted
   * repositories only.
   *
   * Capability flags alone do not establish
   * sufficient isolation for untrusted builds.
   */
  if (!trustedRepository) {
    throw new BuildError(
      "BUILD_POLICY_VIOLATION",
      "Untrusted repository builds are disabled.",
    );
  }
}

// ==================================================
// Artifact verification
// ==================================================

function verifyProducedArtifact(
  result: BuildExecutorResult,
  inspection: ImageInspection,
  expectedPlatform: string,
): {
  imageId: string;
  imageDigest: string | null;
  imageSizeBytes: number;
} {
  /**
   * Phase 5.4 currently supports local Docker
   * image-store artifacts.
   */
  if (
    result.artifactType !==
    "DOCKER_IMAGE_STORE"
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The build produced an unsupported artifact type.",
    );
  }

  if (!inspection.exists) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The produced image artifact could not be verified.",
    );
  }

  if (result.platform !== expectedPlatform) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The reported image platform does not match the requested platform.",
    );
  }

  const produced = verifyImageArtifact({
    imageId: result.imageId,

    imageDigest: result.imageDigest,

    imageSizeBytes: result.imageSizeBytes,
  });

  const inspected = verifyImageArtifact({
    imageId: inspection.imageId,

    imageDigest: inspection.digest,

    imageSizeBytes:
      inspection.imageSizeBytes,
  });

  /**
   * Never silently substitute metadata when
   * the produced and inspected images disagree.
   */
  if (
    produced.imageId !== inspected.imageId
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The inspected image identifier does not match the produced image.",
    );
  }

  if (
    produced.imageSizeBytes !==
    inspected.imageSizeBytes
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The inspected image size does not match the produced image.",
    );
  }

  if (
    produced.imageDigest !==
    inspected.imageDigest
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The inspected image digest does not match the produced image.",
    );
  }

  return inspected;
}

// ==================================================
// Failure normalization
// ==================================================

function normalizeFailure(
  error: unknown,
  signal: AbortSignal,
): BuildError {
  /**
   * Preserve explicit typed failures, including
   * timeout, cancellation and lease-loss errors.
   */
  if (error instanceof BuildError) {
    return error;
  }

  if (signal.aborted) {
    return cancellationError(signal);
  }

  return new BuildError(
    "UNEXPECTED_BUILD_FAILURE",
    "An unexpected build failure occurred.",
    { cause: error },
  );
}

// ==================================================
// Build orchestration
// ==================================================

export async function runBuild(
  input: BuildEngineInput,
): Promise<BuildEngineResult> {
  const startedAt = performance.now();

  validateLimits(input.limits);

  validateLiveLogSink(input.onLogEntry);

  const deadline =
    startedAt + input.limits.timeoutMs;

  const timeoutController =
    new AbortController();

  const signal = AbortSignal.any([
    input.signal,

    timeoutController.signal,
  ]);

  const registeredSecrets =
    validateRegisteredLogSecrets(
      input.registeredLogSecrets ?? [],
    );

  const collector = new BuildLogCollector({
    maxLogBytes:
      input.limits.maxLogBytes,

    registeredSecrets,

    ...(input.limits.maxLogEntries !==
    undefined
      ? {
          maxLogEntries:
            input.limits.maxLogEntries,
        }
      : {}),
  });

  /**
   * The collector now owns its registered redaction
   * state. Release this temporary array's references.
   *
   * JavaScript strings cannot be reliably zeroized.
   */
  registeredSecrets.length = 0;

  let unsubscribeLogEntry:
    | (() => void)
    | undefined;

  /**
   * Executor callbacks are accepted only while
   * the active executor invocation is running.
   */
  let acceptingExecutorLogs = false;

  let timer:
    | ReturnType<typeof setTimeout>
    | undefined;

  const policy =
    input.policy ?? defaultBuildPolicy;

  const identity: ImageTagInput = {
    projectId: input.projectId,

    deploymentId: input.deploymentId,

    resolvedCommitSha:
      input.resolvedCommitSha,

    buildAttempt: input.buildAttempt,
  };

  let imageTag: string | null = null;

  let contextSizeBytes: number | null =
    null;

  let fileCount: number | null = null;

  let artifactMayExist = false;

  let executorReturnedSuccessfully = false;

  const durationMs = (): number =>
    Math.max(
      0,
      Math.round(
        performance.now() - startedAt,
      ),
    );

  /**
   * Only trusted internal messages are passed
   * to this helper.
   *
   * Never pass raw executor errors, environment
   * variables, build arguments or credentials.
   */
  const logSystem = (
    message: string,
  ): void => {
    collector.append(
      "system",
      `${message}\n`,
    );
  };

  /**
   * Stop accepting executor logs as soon as
   * cancellation is observed.
   *
   * This does not establish that the underlying
   * executor or BuildKit solver has stopped.
   */
  const stopExecutorLogs = (): void => {
    acceptingExecutorLogs = false;
  };

  try {
    /**
     * Register before emitting the first system
     * log so the live sink can observe the full
     * authorized build lifecycle.
     */
    if (input.onLogEntry !== undefined) {
      const listener = input.onLogEntry;

      unsubscribeLogEntry =
        collector.subscribe((entry) => {
          /**
           * Cancellation may represent lease loss.
           *
           * Do not forward additional live records
           * after cancellation has been observed.
           *
           * The processor must independently fence
           * persistence of already accepted records.
           */
          if (signal.aborted) {
            return;
          }

          listener(entry);
        });
    }

    signal.addEventListener(
      "abort",
      stopExecutorLogs,
      { once: true },
    );

    timer = setTimeout(
      () => {
        timeoutController.abort(
          createTimeoutError(),
        );
      },
      Math.max(
        0,
        deadline - performance.now(),
      ),
    );

    /**
     * Keep the deadline timer referenced while
     * the engine is responsible for active work.
     *
     * The finally block always clears it.
     */

    remainingTime(deadline, signal);

    // ==========================================
    // 1. Authorize repository execution
    // ==========================================

    if (input.trustedRepository !== true) {
      throw new BuildError(
        "BUILD_POLICY_VIOLATION",
        "Untrusted repository builds are disabled.",
      );
    }

    logSystem(
      "Build execution authorized.",
    );

    remainingTime(deadline, signal);

    // ==========================================
    // 2. Validate the build context
    // ==========================================

    logSystem(
      "Validating build context.",
    );

    remainingTime(deadline, signal);

    const context =
      await prepareBuildContext({
        sourceDir: input.sourceDir,

        dockerfilePath:
          input.dockerfilePath,

        buildContextPath:
          input.buildContextPath,

        maxContextBytes:
          input.limits.maxContextBytes,

        maxFileCount:
          input.limits.maxFileCount,

        ...(input.limits.maxDirectoryCount !==
        undefined
          ? {
              maxDirectoryCount:
                input.limits.maxDirectoryCount,
            }
          : {}),

        signal,
      });

    remainingTime(deadline, signal);

    contextSizeBytes =
      context.contextSizeBytes;

    fileCount = context.fileCount;

    logSystem(
      "Build context validation completed.",
    );

    // ==========================================
    // 3. Validate Dockerfile and policy
    // ==========================================

    logSystem(
      "Validating Dockerfile and build policy.",
    );

    remainingTime(deadline, signal);

    await validateDockerfile(
      context.dockerfileAbsolutePath,
      {
        policy,

        target: input.target,

        signal,
      },
    );

    remainingTime(deadline, signal);

    logSystem(
      "Dockerfile validation completed.",
    );

    // ==========================================
    // 4. Generate deployment-scoped image tag
    // ==========================================

    remainingTime(deadline, signal);

    imageTag = generateImageTag(identity);

    assertImageOwnership(
      imageTag,
      identity,
    );

    // ==========================================
    // 5. Verify builder capabilities
    // ==========================================

    logSystem(
      "Checking build backend availability.",
    );

    remainingTime(deadline, signal);

    const capabilities =
      await awaitReadOperation(
        () => input.executor.probe(),
        signal,
      );

    remainingTime(deadline, signal);

    assertBuilderAllowed(
      capabilities,
      input.trustedRepository === true,
    );

    logSystem(
      "Build backend is available.",
    );

    // ==========================================
    // 6. Reject pre-existing image references
    // ==========================================

    remainingTime(deadline, signal);

    const existingImage =
      await awaitReadOperation(
        () =>
          input.executor.inspectImage(
            imageTag!,
          ),
        signal,
      );

    remainingTime(deadline, signal);

    if (existingImage.exists) {
      throw new BuildError(
        "IMAGE_VERIFICATION_FAILED",
        "An image already exists for this build attempt.",
      );
    }

    // ==========================================
    // 7. Execute the image build
    // ==========================================

    logSystem(
      "Starting image build.",
    );

    const executionTimeoutMs =
      remainingTime(
        deadline,
        signal,
      );

    /**
     * Once invoked, the executor may create an
     * image even if the operation subsequently
     * fails or is cancelled.
     */
    artifactMayExist = true;

    acceptingExecutorLogs = true;

    let result: BuildExecutorResult;

    try {
      /**
       * Never race the mutating build promise
       * against a timeout promise.
       *
       * The executor must observe cancellation
       * and settle its own operation.
       */
      result = await input.executor.build({
        contextDir: context.contextDir,

        dockerfileRelativePath:
          context.dockerfileRelativePath,

        imageTag,

        platform: input.platform,

        buildArgs: input.buildArgs,

        target: input.target,

        timeoutMs: executionTimeoutMs,

        signal,

        onLog: (stream, chunk) => {
          /**
           * Ignore callbacks from cancelled,
           * completed or otherwise closed
           * executor invocations.
           */
          if (
            !acceptingExecutorLogs ||
            signal.aborted ||
            collector.isFinalized ||
            collector.isTruncated
          ) {
            return;
          }

          /**
           * The timer callback may be delayed
           * by synchronous work.
           *
           * Observe the monotonic deadline
           * independently before accepting
           * another executor chunk.
           */
          if (
            performance.now() >= deadline
          ) {
            acceptingExecutorLogs = false;

            timeoutController.abort(
              createTimeoutError(),
            );

            return;
          }

          collector.append(
            stream,
            chunk,
          );
        },
      });

      executorReturnedSuccessfully = true;
    } finally {
      /**
       * Reject callbacks occurring after the
       * executor's build promise settles.
       */
      acceptingExecutorLogs = false;
    }

    remainingTime(deadline, signal);

    // ==========================================
    // 8. Verify produced image
    // ==========================================

    logSystem(
      "Verifying produced image artifact.",
    );

    remainingTime(deadline, signal);

    const inspection =
      await awaitReadOperation(
        () =>
          input.executor.inspectImage(
            imageTag!,
          ),
        signal,
      );

    remainingTime(deadline, signal);

    const verified =
      verifyProducedArtifact(
        result,
        inspection,
        input.platform,
      );

    assertImageOwnership(
      imageTag,
      identity,
    );

    remainingTime(deadline, signal);

    logSystem(
      "Image artifact verification completed.",
    );

    remainingTime(deadline, signal);

    logSystem(
      "Image build completed successfully.",
    );

    // ==========================================
    // 9. Finalize bounded logs
    // ==========================================

    collector.finalize();

    /**
     * Subscriber callbacks and final pending-line
     * normalization may perform synchronous work.
     *
     * Do not return successful metadata when the
     * deadline or cancellation has been observed.
     */
    remainingTime(deadline, signal);

    const logEntries =
      collector.snapshot();

    remainingTime(deadline, signal);

    // ==========================================
    // 10. Return verified build metadata
    // ==========================================

    return {
      imageTag,

      artifactType: result.artifactType,

      imageId: verified.imageId,

      imageDigest: verified.imageDigest,

      imageSizeBytes:
        verified.imageSizeBytes,

      platform: result.platform,

      durationMs: durationMs(),

      contextSizeBytes:
        context.contextSizeBytes,

      fileCount: context.fileCount,

      logEntries,

      logsTruncated:
        collector.isTruncated,

      logSubscriberFailures:
        collector.subscriberFailureCount,
    };
  } catch (error) {
    acceptingExecutorLogs = false;

    const failure = normalizeFailure(
      error,
      signal,
    );

    /**
     * Persist only the known failure code in
     * system logs, not arbitrary backend errors.
     *
     * When the collector has already finalized,
     * this append is intentionally ignored.
     */
    logSystem(
      `Build failed: ${failure.code}`,
    );

    collector.finalize();

    /**
     * Do not automatically remove image tags here.
     *
     * The engine cannot determine whether its
     * database lease remains valid or whether a
     * cancelled BuildKit solver is still running.
     *
     * The processor must establish cleanup
     * eligibility before removing an artifact.
     */
    throw new BuildEngineError(
      failure,
      {
        imageTag,

        durationMs: durationMs(),

        contextSizeBytes,

        fileCount,

        logEntries: collector.snapshot(),

        logsTruncated:
          collector.isTruncated,

        artifactMayExist,

        executorReturnedSuccessfully,

        logSubscriberFailures:
          collector.subscriberFailureCount,
      },
    );
  } finally {
    acceptingExecutorLogs = false;

    if (timer !== undefined) {
      clearTimeout(timer);

      timer = undefined;
    }

    signal.removeEventListener(
      "abort",
      stopExecutorLogs,
    );

    try {
      /**
       * Flush final pending records before removing
       * the live subscriber.
       *
       * Finalization is idempotent.
       */
      collector.finalize();
    } finally {
      unsubscribeLogEntry?.();

      unsubscribeLogEntry = undefined;
    }
  }
}