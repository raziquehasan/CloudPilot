
/**
 * CloudPilot — BuildKit Executor
 *
 * Executes trusted image builds through Docker Buildx.
 *
 * Security properties:
 * - Shell-free process execution
 * - Restricted child process environment
 * - Bounded process output
 * - Cooperative cancellation
 * - Bounded process termination
 * - Strict image metadata validation
 * - Attempt-scoped image cleanup
 *
 * This executor does NOT provide strong tenant isolation.
 * Untrusted repository builds must remain disabled.
 */

import { spawn } from "node:child_process";

import {
  isAbsolute,
  posix,
  win32,
} from "node:path";

import { StringDecoder } from "node:string_decoder";

import { BuildError } from "@cloudpilot/queue";
import { assertValidImageReference } from "./image-artifact.js";

import type {
  BuilderCapabilities,
  BuildExecutor,
  BuildExecutorRequest,
  BuildExecutorResult,
  ImageInspection,
} from "./build-executor.js";

import type {
  BuildLogStream,
} from "./build-logs.js";

// ==================================================
// Constants
// ==================================================

const DOCKER_BINARY = "docker";

const BUILDX_PROBE_TIMEOUT_MS = 15_000;

const INSPECT_TIMEOUT_MS = 30_000;

const REMOVE_TIMEOUT_MS = 30_000;

const TERMINATION_GRACE_MS = 5_000;

const MAX_CAPTURE_BYTES = 8 * 1024 * 1024;

const MAX_ARG_LENGTH = 32_768;

const MAX_BUILD_ARGUMENTS = 128;

const MAX_ARGUMENT_BYTES = 24 * 1024;

const MAX_TIMEOUT_MS = 3_600_000;

const IMAGE_ID_PATTERN =
  /^sha256:[a-f0-9]{64}$/u;

const BUILD_ARG_NAME_PATTERN =
  /^[A-Za-z_][A-Za-z0-9_]{0,127}$/u;

const TARGET_PATTERN =
  /^[A-Za-z][A-Za-z0-9_.-]{0,127}$/u;

const PLATFORM_PATTERN =
  /^linux\/(?:amd64|arm64|arm\/v7)$/u;

const UNSAFE_CONTROL_PATTERN =
  /[\u0000-\u001f\u007f-\u009f]/u;

// ==================================================
// Process contracts
// ==================================================

interface ProcessResult {
  code: number | null;

  stdout: string;

  stderr: string;

  signal: NodeJS.Signals | null;

  outputTruncated: boolean;
}

interface RunOptions {
  args: string[];

  timeoutMs: number;

  signal?: AbortSignal;

  cwd?: string;

  onData?: (
    stream: BuildLogStream,
    chunk: string,
  ) => void;
}

// ==================================================
// Error helpers
// ==================================================

function policyViolation(
  message: string,
): BuildError {
  return new BuildError(
    "BUILD_POLICY_VIOLATION",
    message,
  );
}

function invalidContext(
  message: string,
): BuildError {
  return new BuildError(
    "BUILD_CONTEXT_INVALID",
    message,
  );
}

function cancellationError(
  signal?: AbortSignal,
): BuildError {
  if (signal?.reason instanceof BuildError) {
    return signal.reason;
  }

  return new BuildError(
    "BUILD_CANCELLED",
    "The build was cancelled.",
  );
}

function timeoutError(): BuildError {
  return new BuildError(
    "BUILD_TIMEOUT",
    "The build exceeded its execution time.",
  );
}

function assertNotCancelled(
  signal?: AbortSignal,
): void {
  if (signal?.aborted) {
    throw cancellationError(signal);
  }
}

// ==================================================
// Restricted process environment
// ==================================================

/**
 * Do not forward the complete worker environment.
 *
 * Database URLs, Redis credentials, JWT secrets,
 * GitHub App credentials and other application
 * secrets must not be inherited by the Docker CLI.
 *
 * Docker configuration remains a trusted
 * infrastructure responsibility.
 */
function createDockerEnvironment():
  NodeJS.ProcessEnv {
  const allowedKeys = new Set([
    "PATH",
    "PATHEXT",
    "HOME",
    "USERPROFILE",
    "SYSTEMROOT",
    "WINDIR",
    "SYSTEMDRIVE",
    "APPDATA",
    "LOCALAPPDATA",
    "TEMP",
    "TMP",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "XDG_RUNTIME_DIR",
    "DOCKER_CONFIG",
    "DOCKER_HOST",
    "DOCKER_CONTEXT",
    "DOCKER_TLS_VERIFY",
    "DOCKER_CERT_PATH",
  ]);

  if (process.platform === "win32") {
    // Docker Desktop uses ProgramFiles to find the installed Buildx plugin.
    allowedKeys.add("PROGRAMFILES");
  }

  const environment: NodeJS.ProcessEnv = {};

  for (const [key, value] of Object.entries(
    process.env,
  )) {
    if (
      value !== undefined &&
      allowedKeys.has(key.toUpperCase())
    ) {
      environment[key] = value;
    }
  }

  environment.DOCKER_BUILDKIT = "1";

  environment.NO_COLOR = "1";

  return environment;
}

// ==================================================
// Process argument validation
// ==================================================

function validateProcessOptions(
  options: RunOptions,
): void {
  if (
    !Number.isSafeInteger(options.timeoutMs) ||
    options.timeoutMs <= 0 ||
    options.timeoutMs > MAX_TIMEOUT_MS
  ) {
    throw invalidContext(
      "Invalid build process timeout.",
    );
  }

  if (
    options.cwd !== undefined &&
    (
      !isAbsolute(options.cwd) ||
      options.cwd.includes("\0")
    )
  ) {
    throw invalidContext(
      "Invalid build working directory.",
    );
  }

  let argumentBytes = 0;

  for (const argument of options.args) {
    if (
      typeof argument !== "string" ||
      argument.includes("\0") ||
      argument.length > MAX_ARG_LENGTH
    ) {
      throw invalidContext(
        "Invalid build process argument.",
      );
    }

    argumentBytes +=
      Buffer.byteLength(argument, "utf8") + 1;

    if (argumentBytes > MAX_ARGUMENT_BYTES) {
      throw invalidContext(
        "Build arguments exceed their total size limit.",
      );
    }
  }
}

// ==================================================
// Process termination
// ==================================================

/**
 * Best-effort local CLI process-tree termination.
 *
 * Terminating the CLI does not guarantee that
 * the remote BuildKit solver has stopped.
 */
function terminateProcessTree(
  child: ReturnType<typeof spawn>,
): void {
  const pid = child.pid;

  if (
    pid === undefined ||
    child.exitCode !== null ||
    child.signalCode !== null
  ) {
    return;
  }

  try {
    if (process.platform === "win32") {
      const killer = spawn(
        "taskkill",
        [
          "/PID",
          String(pid),
          "/T",
          "/F",
        ],
        {
          shell: false,
          windowsHide: true,
          stdio: "ignore",
          env: createDockerEnvironment(),
        },
      );

      killer.once("error", () => {
        try {
          child.kill("SIGKILL");
        } catch {
          // Best-effort fallback.
        }
      });

      killer.once("exit", (code) => {
        if (code !== 0) {
          try {
            child.kill("SIGKILL");
          } catch {
            // Best-effort fallback.
          }
        }
      });

      killer.unref();

      return;
    }

    /**
     * POSIX children are started in a separate
     * process group. A negative PID addresses
     * the complete local process group.
     */
    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      child.kill("SIGKILL");
    }
  } catch {
    // The bounded termination deadline handles
    // processes that cannot be terminated.
  }
}

// ==================================================
// Safe process execution
// ==================================================

async function runProcess(
  options: RunOptions,
): Promise<ProcessResult> {
  assertNotCancelled(options.signal);

  validateProcessOptions(options);

  return new Promise<ProcessResult>(
    (resolve, reject) => {
      let child: ReturnType<typeof spawn>;

      try {
        child = spawn(
          DOCKER_BINARY,
          options.args,
          {
            shell: false,

            windowsHide: true,

            detached:
              process.platform !== "win32",

            stdio: [
              "ignore",
              "pipe",
              "pipe",
            ],

            env: createDockerEnvironment(),

            ...(options.cwd
              ? { cwd: options.cwd }
              : {}),
          },
        );
      } catch (error) {
        reject(
          new BuildError(
            "BUILDER_UNAVAILABLE",
            "The build backend could not be started.",
            { cause: error },
          ),
        );

        return;
      }

      const stdoutDecoder =
        new StringDecoder("utf8");

      const stderrDecoder =
        new StringDecoder("utf8");

      const stdoutChunks: Buffer[] = [];

      const stderrChunks: Buffer[] = [];

      let capturedBytes = 0;

      let outputTruncated = false;

      let settled = false;

      let terminationError:
        | BuildError
        | undefined;

      let deadline:
        | ReturnType<typeof setTimeout>
        | undefined;

      let terminationDeadline:
        | ReturnType<typeof setTimeout>
        | undefined;

      const cleanup = (): void => {
        if (deadline !== undefined) {
          clearTimeout(deadline);
        }

        if (
          terminationDeadline !== undefined
        ) {
          clearTimeout(terminationDeadline);
        }

        options.signal?.removeEventListener(
          "abort",
          abort,
        );
      };

      const fail = (
        error: BuildError,
      ): void => {
        if (settled) {
          return;
        }

        settled = true;

        cleanup();

        reject(error);
      };

      const stop = (
        error: BuildError,
      ): void => {
        if (
          settled ||
          terminationError !== undefined
        ) {
          return;
        }

        terminationError = error;

        if (deadline !== undefined) {
          clearTimeout(deadline);
        }

        terminateProcessTree(child);

        terminationDeadline = setTimeout(
          () => {
            terminateProcessTree(child);

            child.stdout?.destroy();

            child.stderr?.destroy();

            child.unref();

            /**
             * Do not wait indefinitely for a
             * process that cannot be terminated.
             *
             * The caller must treat the build
             * outcome as uncertain.
             */
            fail(
              new BuildError(
                terminationError?.code ??
                  "BUILD_FAILED",
                "Build process termination could not be confirmed.",
              ),
            );
          },
          TERMINATION_GRACE_MS,
        );
      };

      const abort = (): void => {
        stop(
          cancellationError(options.signal),
        );
      };

      const emitLog = (
        stream: BuildLogStream,
        chunk: string,
      ): void => {
        if (
          settled ||
          !chunk ||
          !options.onData
        ) {
          return;
        }

        try {
          options.onData(stream, chunk);
        } catch {
          stop(
            new BuildError(
              "UNEXPECTED_BUILD_FAILURE",
              "Build log collection failed.",
            ),
          );
        }
      };

      const append = (
        stream: BuildLogStream,
        data: Buffer,
      ): void => {
        if (settled) {
          return;
        }

        /**
         * Bound captured process output separately
         * from streamed build logs.
         */
        const remaining =
          MAX_CAPTURE_BYTES - capturedBytes;

        if (data.length > remaining) {
          outputTruncated = true;
        }

        if (remaining > 0) {
          const captured = Buffer.from(
            data.subarray(0, remaining),
          );

          capturedBytes += captured.length;

          if (stream === "stdout") {
            stdoutChunks.push(captured);
          } else {
            stderrChunks.push(captured);
          }
        }

        const decoder =
          stream === "stdout"
            ? stdoutDecoder
            : stderrDecoder;

        emitLog(
          stream,
          decoder.write(data),
        );
      };

      child.stdout?.on(
        "data",
        (data: Buffer) => {
          append("stdout", data);
        },
      );

      child.stderr?.on(
        "data",
        (data: Buffer) => {
          append("stderr", data);
        },
      );

      child.stdout?.on("error", () => {
        stop(
          new BuildError(
            "BUILD_FAILED",
            "Build output stream failed.",
          ),
        );
      });

      child.stderr?.on("error", () => {
        stop(
          new BuildError(
            "BUILD_FAILED",
            "Build error stream failed.",
          ),
        );
      });

      child.once("error", (error) => {
        if (terminationError) {
          fail(terminationError);

          return;
        }

        fail(
          new BuildError(
            "BUILDER_UNAVAILABLE",
            "The build backend could not be started.",
            { cause: error },
          ),
        );
      });

      child.once(
        "close",
        (code, signal) => {
          if (settled) {
            return;
          }

          emitLog(
            "stdout",
            stdoutDecoder.end(),
          );

          emitLog(
            "stderr",
            stderrDecoder.end(),
          );

          if (terminationError) {
            fail(terminationError);

            return;
          }

          settled = true;

          cleanup();

          resolve({
            code,

            signal,

            stdout: Buffer.concat(
              stdoutChunks,
            ).toString("utf8"),

            stderr: Buffer.concat(
              stderrChunks,
            ).toString("utf8"),

            outputTruncated,
          });
        },
      );

      deadline = setTimeout(
        () => {
          stop(timeoutError());
        },
        options.timeoutMs,
      );

      options.signal?.addEventListener(
        "abort",
        abort,
        { once: true },
      );

      /**
       * Close the race between the initial
       * cancellation check and listener setup.
       */
      if (options.signal?.aborted) {
        abort();
      }
    },
  );
}

// ==================================================
// Image reference validation
// ==================================================

export function assertManagedImageTag(
  imageTag: string,
): void {
  try {
    assertValidImageReference(imageTag);
  } catch (error) {
    if (!(error instanceof BuildError)) throw error;
    throw policyViolation(
      "Invalid CloudPilot-managed image reference.",
    );
  }
}

function assertDockerfilePath(
  path: string,
): void {
  if (
    typeof path !== "string" ||
    path.length === 0 ||
    path.length > 500 ||
    UNSAFE_CONTROL_PATTERN.test(path) ||
    path.includes("\\") ||
    path.includes(":") ||
    path.startsWith("-") ||
    posix.isAbsolute(path) ||
    win32.isAbsolute(path)
  ) {
    throw invalidContext(
      "Invalid Dockerfile path.",
    );
  }

  const segments = path.split("/");

  if (
    segments.some(
      (segment) =>
        segment === "" ||
        segment === "." ||
        segment === "..",
    )
  ) {
    throw invalidContext(
      "Invalid Dockerfile path component.",
    );
  }
}

// ==================================================
// Image inspection helpers
// ==================================================

function missingImage(): ImageInspection {
  return {
    exists: false,

    imageId: null,

    imageSizeBytes: null,

    digest: null,
  };
}

function isMissingImage(
  result: ProcessResult,
): boolean {
  return (
    !result.outputTruncated &&
    /no such image|no such object/iu.test(
      result.stderr,
    )
  );
}

function extractRepoDigest(
  repoDigests: unknown,
  imageTag: string,
): string | null {
  if (!Array.isArray(repoDigests)) {
    return null;
  }

  const repository = imageTag.slice(
    0,
    imageTag.lastIndexOf(":"),
  );

  const digests = new Set<string>();

  for (const entry of repoDigests) {
    if (typeof entry !== "string") {
      continue;
    }

    const separator = entry.lastIndexOf("@");

    if (separator <= 0) {
      continue;
    }

    const name = entry.slice(0, separator);

    const digest = entry.slice(separator + 1);

    if (
      (
        name === repository ||
        name === `docker.io/${repository}`
      ) &&
      IMAGE_ID_PATTERN.test(digest)
    ) {
      digests.add(digest);
    }
  }

  // Do not invent a manifest digest when the
  // local image has no unambiguous RepoDigest.
  if (digests.size !== 1) {
    return null;
  }

  return [...digests][0] ?? null;
}

function parseImageInspection(
  stdout: string,
  imageTag: string,
): ImageInspection {
  let parsed: unknown;

  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "Invalid image inspection response.",
    );
  }

  if (
    typeof parsed !== "object" ||
    parsed === null ||
    Array.isArray(parsed)
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "Unexpected image inspection response.",
    );
  }

  const data = parsed as Record<
    string,
    unknown
  >;

  if (
    typeof data.Id !== "string" ||
    !IMAGE_ID_PATTERN.test(data.Id)
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The image identifier is invalid.",
    );
  }

  if (
    typeof data.Size !== "number" ||
    !Number.isSafeInteger(data.Size) ||
    data.Size < 0
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The image size is invalid.",
    );
  }

  if (
    !Array.isArray(data.RepoTags) ||
    !data.RepoTags.includes(imageTag)
  ) {
    throw new BuildError(
      "IMAGE_VERIFICATION_FAILED",
      "The expected image tag was not found.",
    );
  }

  return {
    exists: true,

    imageId: data.Id,

    imageSizeBytes: data.Size,

    digest: extractRepoDigest(
      data.RepoDigests,
      imageTag,
    ),
  };
}

// ==================================================
// Builder capabilities
// ==================================================

function unavailable(
  driver = "unknown",
): BuilderCapabilities {
  return {
    available: false,

    builderType: "buildkit",

    driver,

    supportsResourceLimits: false,

    supportsNetworkIsolation: false,

    supportsStrongIsolation: false,
  };
}

function detectDriver(
  output: string,
): string {
  const match =
    /^\s*Driver:\s*([A-Za-z0-9_-]+)\s*$/imu
      .exec(output);

  return match?.[1] ?? "unknown";
}

// ==================================================
// BuildKit executor
// ==================================================

export class BuildKitExecutor
  implements BuildExecutor
{
  probe(): Promise<BuilderCapabilities> {
    return this.detectCapabilities();
  }

  // ================================================
  // Image build
  // ================================================

  async build(
    request: BuildExecutorRequest,
  ): Promise<BuildExecutorResult> {
    assertNotCancelled(request.signal);

    const args = this.buildArgs(request);

    const result = await runProcess({
      args,

      timeoutMs: request.timeoutMs,

      signal: request.signal,

      cwd: request.contextDir,

      onData: request.onLog,
    });

    assertNotCancelled(request.signal);

    if (result.code !== 0) {
      throw new BuildError(
        "BUILD_FAILED",
        "The image build process failed.",
      );
    }

    const inspection =
      await this.inspectImageInternal(
        request.imageTag,
        request.signal,
      );

    assertNotCancelled(request.signal);

    if (
      !inspection.exists ||
      !inspection.imageId
    ) {
      throw new BuildError(
        "IMAGE_ARTIFACT_UNAVAILABLE",
        "The build did not produce a verifiable image.",
      );
    }

    return {
      artifactType: "DOCKER_IMAGE_STORE",

      imageId: inspection.imageId,

      imageDigest: inspection.digest,

      imageSizeBytes:
        inspection.imageSizeBytes,

      platform: request.platform,
    };
  }

  // ================================================
  // Image inspection
  // ================================================

  inspectImage(
    imageTag: string,
  ): Promise<ImageInspection> {
    return this.inspectImageInternal(
      imageTag,
    );
  }

  private async inspectImageInternal(
    imageTag: string,
    signal?: AbortSignal,
  ): Promise<ImageInspection> {
    assertManagedImageTag(imageTag);

    assertNotCancelled(signal);

    const result = await runProcess({
      args: [
        "image",
        "inspect",
        "--format",
        "{{json .}}",
        "--",
        imageTag,
      ],

      timeoutMs: INSPECT_TIMEOUT_MS,

      ...(signal ? { signal } : {}),
    });

    if (result.code !== 0) {
      if (isMissingImage(result)) {
        return missingImage();
      }

      throw new BuildError(
        "IMAGE_VERIFICATION_FAILED",
        "The image could not be inspected.",
      );
    }

    if (result.outputTruncated) {
      throw new BuildError(
        "IMAGE_VERIFICATION_FAILED",
        "Image inspection output exceeded its limit.",
      );
    }

    return parseImageInspection(
      result.stdout,
      imageTag,
    );
  }

  // ================================================
  // Scoped image cleanup
  // ================================================

  async removeImage(
    imageTag: string,
  ): Promise<void> {
    assertManagedImageTag(imageTag);

    /**
     * Remove only the requested managed tag.
     *
     * Never use global prune operations or force
     * removal of images that may still be in use.
     *
     * The caller must establish attempt ownership.
     */
    const result = await runProcess({
      args: [
        "image",
        "rm",
        "--",
        imageTag,
      ],

      timeoutMs: REMOVE_TIMEOUT_MS,
    });

    if (
      result.code !== 0 &&
      !isMissingImage(result)
    ) {
      throw new BuildError(
        "IMAGE_ARTIFACT_UNAVAILABLE",
        "The build image could not be removed safely.",
      );
    }
  }

  // ================================================
  // Build argument construction
  // ================================================

  private buildArgs(
    request: BuildExecutorRequest,
  ): string[] {
    assertManagedImageTag(
      request.imageTag,
    );

    assertDockerfilePath(
      request.dockerfileRelativePath,
    );

    if (
      !isAbsolute(request.contextDir)
    ) {
      throw invalidContext(
        "An absolute build context is required.",
      );
    }

    if (
      !PLATFORM_PATTERN.test(
        request.platform,
      )
    ) {
      throw policyViolation(
        "The requested build platform is not supported.",
      );
    }

    const args: string[] = [
      "buildx",
      "build",
      "--load",
      "--progress=plain",
      "--platform",
      request.platform,
      "--tag",
      request.imageTag,
      "--file",
      request.dockerfileRelativePath,
    ];

    const buildArguments = Object.entries(
      request.buildArgs,
    );

    if (
      buildArguments.length >
      MAX_BUILD_ARGUMENTS
    ) {
      throw policyViolation(
        "Too many build arguments.",
      );
    }

    for (const [key, value] of buildArguments) {
      if (
        !BUILD_ARG_NAME_PATTERN.test(key)
      ) {
        throw policyViolation(
          "A build argument name is invalid.",
        );
      }

      if (
        typeof value !== "string" ||
        UNSAFE_CONTROL_PATTERN.test(value) ||
        Buffer.byteLength(value, "utf8") >
          MAX_ARG_LENGTH
      ) {
        throw policyViolation(
          "A build argument value is invalid.",
        );
      }

      /**
       * Explicit key=value prevents implicit
       * inheritance from the CLI environment.
       *
       * Build arguments must not contain secrets.
       */
      args.push(
        "--build-arg",
        `${key}=${value}`,
      );
    }

    if (request.target != null) {
      if (
        !TARGET_PATTERN.test(request.target)
      ) {
        throw policyViolation(
          "The requested build target is invalid.",
        );
      }

      args.push(
        "--target",
        request.target,
      );
    }

    // Use the validated context as the working
    // directory and "." as the context argument.
    args.push("--", ".");

    return args;
  }

  // ================================================
  // Capability detection
  // ================================================

  private async detectCapabilities():
    Promise<BuilderCapabilities> {
    try {
      const versionResult =
        await runProcess({
          args: [
            "version",
            "--format",
            "{{.Server.Version}}",
          ],

          timeoutMs:
            BUILDX_PROBE_TIMEOUT_MS,
        });

      if (
        versionResult.code !== 0 ||
        versionResult.outputTruncated ||
        !versionResult.stdout.trim()
      ) {
        return unavailable();
      }

      /**
       * Inspect the selected builder instead of
       * guessing its driver from all builders.
       */
      const builderResult =
        await runProcess({
          args: [
            "buildx",
            "inspect",
          ],

          timeoutMs:
            BUILDX_PROBE_TIMEOUT_MS,
        });

      if (
        builderResult.code !== 0 ||
        builderResult.outputTruncated
      ) {
        return unavailable();
      }

      const driver = detectDriver(
        builderResult.stdout,
      );

      /**
       * Phase 5.4 supports the trusted local
       * Docker driver only.
       *
       * Other drivers require a separate,
       * explicitly hardened executor.
       */
      if (driver !== "docker") {
        return unavailable(driver);
      }

      if (
        !/^\s*Status:\s*running\s*$/imu.test(
          builderResult.stdout,
        )
      ) {
        return unavailable(driver);
      }

      return {
        available: true,

        builderType: "buildkit",

        driver,

        supportsResourceLimits: false,

        supportsNetworkIsolation: false,

        supportsStrongIsolation: false,
      };
    } catch {
      return unavailable();
    }
  }
}
