/**
 * CloudPilot — Docker Runtime Executor
 *
 * Executes CloudPilot-managed containers through the Docker CLI.
 *
 * Security properties:
 *
 * - Shell-free process execution.
 * - Restricted child process environment.
 * - Bounded command output and execution deadlines.
 * - Cooperative cancellation and process cleanup.
 * - Immutable container identity for log collection.
 * - Ownership-aware container lifecycle operations.
 * - Managed network metadata verification.
 * - Bounded historical log recovery.
 * - Incremental stdout/stderr streaming.
 * - Restrictive temporary environment files.
 * - Container resource limits and hardening.
 * - Scoped cleanup only.
 *
 * This executor does NOT provide strong tenant isolation.
 * Untrusted workloads must remain disabled.
 */

import { spawn } from "node:child_process";

import {
  mkdtemp,
  rm,
  writeFile,
} from "node:fs/promises";

import { tmpdir } from "node:os";

import { join } from "node:path";

import { RuntimeDeploymentError } from "@cloudpilot/queue";

import type {
  ContainerInspection,
  ContainerState,
  CreateContainerRequest,
  CreatedContainer,
  ImageVerification,
  RuntimeCapabilities,
  RuntimeExecutor,
  StreamLogsRequest,
} from "./runtime-executor.js";

// ==================================================
// Constants
// ==================================================

const DOCKER_BINARY = "docker";

const PROBE_TIMEOUT_MS = 15_000;

const INSPECT_TIMEOUT_MS = 30_000;

const CREATE_TIMEOUT_MS = 60_000;

const START_TIMEOUT_MS = 60_000;

const REMOVE_TIMEOUT_MS = 30_000;

const LOGS_TIMEOUT_MS = 15_000;

const TERMINATION_GRACE_MS = 5_000;

const MAX_PROCESS_TIMEOUT_MS = 3_600_000;

const MAX_CAPTURE_BYTES = 4 * 1024 * 1024;

const MAX_ARG_LENGTH = 32_768;

const MAX_ARGUMENT_BYTES = 256 * 1024;

const MAX_ARGUMENT_COUNT = 1_024;

const MAX_LOG_CHUNK_BYTES = 16 * 1024;

const MAX_HISTORICAL_LOG_LINES = 5_000;

const MAX_HISTORICAL_LOG_MS = 60 * 60 * 1_000;

const MAX_HISTORICAL_LOG_BYTES = 16 * 1024 * 1024;

const MAX_ENV_VARIABLES = 256;

const MAX_ENV_VALUE_BYTES = 16 * 1024;

const MAX_ENV_TOTAL_BYTES = 256 * 1024;

const MAX_LABELS = 128;

const MAX_LABEL_VALUE_BYTES = 4_096;

const MAX_LIFECYCLE_TIMEOUT_MS = 300_000;

const IMAGE_ID_PATTERN = /^sha256:[a-f0-9]{64}$/u;

const CONTAINER_ID_PATTERN = /^[a-f0-9]{64}$/u;

const UUID_PATTERN =
  "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}";

const CONTAINER_NAME_PATTERN = new RegExp(
  `^cloudpilot-(${UUID_PATTERN})-(${UUID_PATTERN})-([1-9][0-9]{0,9})$`,
  "u",
);

const NETWORK_NAME_PATTERN =
  /^cloudpilot-[a-zA-Z0-9][a-zA-Z0-9_.-]{0,116}$/u;

const LABEL_KEY_PATTERN =
  /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/u;

const ENV_KEY_PATTERN =
  /^[A-Za-z_][A-Za-z0-9_]{0,255}$/u;

const RFC3339_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/u;

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

  signal?: AbortSignal | undefined;
}

interface ManagedContainerSnapshot {
  inspection: ContainerInspection;

  labels: Record<string, string>;

  name: string;
}

interface ManagedNetworkSnapshot {
  id: string;

  name: string;

  driver: string;

  scope: string;

  labels: Record<string, string>;
}

// ==================================================
// Error helpers
// ==================================================

function unavailable(
  message: string,
): RuntimeDeploymentError {
  return new RuntimeDeploymentError(
    "RUNTIME_UNAVAILABLE",
    message,
  );
}

function invalidConfiguration(
  message: string,
): RuntimeDeploymentError {
  return new RuntimeDeploymentError(
    "INVALID_RUNTIME_CONFIGURATION",
    message,
  );
}

function executionFailed(
  message: string,
): RuntimeDeploymentError {
  return new RuntimeDeploymentError(
    "CONTAINER_EXECUTION_FAILED",
    message,
  );
}

function cancellationError(
  signal?: AbortSignal,
): RuntimeDeploymentError {
  if (
    signal?.reason instanceof RuntimeDeploymentError
  ) {
    return signal.reason;
  }

  return new RuntimeDeploymentError(
    "RUNTIME_CANCELLED",
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
// Generic validation
// ==================================================

function isRecord(
  value: unknown,
): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

function isValidContainerId(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    CONTAINER_ID_PATTERN.test(value)
  );
}

function assertContainerId(
  value: unknown,
): asserts value is string {
  if (!isValidContainerId(value)) {
    throw invalidConfiguration(
      "Invalid immutable container identifier.",
    );
  }
}

function assertImageId(
  imageId: string,
): void {
  if (
    typeof imageId !== "string" ||
    !IMAGE_ID_PATTERN.test(imageId)
  ) {
    throw new RuntimeDeploymentError(
      "IMAGE_OWNERSHIP_FAILED",
      "The image identifier is invalid.",
    );
  }
}

function assertManagedContainerName(
  name: string,
): void {
  if (
    typeof name !== "string" ||
    name.length > 128 ||
    !CONTAINER_NAME_PATTERN.test(name)
  ) {
    throw invalidConfiguration(
      "The managed container name is invalid.",
    );
  }

  const match = CONTAINER_NAME_PATTERN.exec(name);

  const attempt = Number(match?.[3]);

  if (
    !Number.isSafeInteger(attempt) ||
    attempt < 1 ||
    attempt > 2_147_483_647
  ) {
    throw invalidConfiguration(
      "The container attempt is invalid.",
    );
  }
}

function assertManagedNetworkName(
  name: string,
): void {
  if (
    typeof name !== "string" ||
    !NETWORK_NAME_PATTERN.test(name)
  ) {
    throw invalidConfiguration(
      "The managed network name is invalid.",
    );
  }
}

function assertLifecycleTimeout(
  timeoutMs: number,
): void {
  if (
    !Number.isSafeInteger(timeoutMs) ||
    timeoutMs < 1 ||
    timeoutMs > MAX_LIFECYCLE_TIMEOUT_MS
  ) {
    throw invalidConfiguration(
      "The runtime lifecycle timeout is invalid.",
    );
  }
}

function isValidPort(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 1 &&
    value <= 65_535
  );
}

function validateLabels(
  labels: Record<string, string>,
): void {
  if (!isRecord(labels)) {
    throw invalidConfiguration(
      "Invalid runtime ownership labels.",
    );
  }

  const entries = Object.entries(labels);

  if (entries.length > MAX_LABELS) {
    throw invalidConfiguration(
      "Runtime ownership labels exceed their limit.",
    );
  }

  for (const [key, value] of entries) {
    if (
      !LABEL_KEY_PATTERN.test(key) ||
      typeof value !== "string" ||
      /[\u0000-\u001f\u007f]/u.test(value) ||
      Buffer.byteLength(value, "utf8") >
        MAX_LABEL_VALUE_BYTES
    ) {
      throw invalidConfiguration(
        "Invalid runtime ownership label.",
      );
    }
  }
}

function parseLabels(
  value: unknown,
): Record<string, string> | null {
  if (!isRecord(value)) {
    return null;
  }

  const entries = Object.entries(value);

  if (entries.length > MAX_LABELS) {
    return null;
  }

  const labels = Object.create(null) as Record<
    string,
    string
  >;

  for (const [key, labelValue] of entries) {
    if (
      !LABEL_KEY_PATTERN.test(key) ||
      typeof labelValue !== "string" ||
      /[\u0000-\u001f\u007f]/u.test(labelValue) ||
      Buffer.byteLength(labelValue, "utf8") >
        MAX_LABEL_VALUE_BYTES
    ) {
      return null;
    }

    labels[key] = labelValue;
  }

  return labels;
}

function labelsMatch(
  actual: Record<string, string>,
  expected: Record<string, string>,
): boolean {
  return Object.entries(expected).every(
    ([key, value]) =>
      Object.hasOwn(actual, key) &&
      actual[key] === value,
  );
}

// ==================================================
// Restricted process environment
// ==================================================

/**
 * Never forward the complete worker environment.
 *
 * Database credentials, Redis credentials, GitHub App
 * credentials, and runtime encryption keys must not
 * be inherited by Docker CLI processes.
 *
 * Docker connection settings are trusted operator
 * configuration, not deployment-controlled input.
 */
function createDockerEnvironment(): NodeJS.ProcessEnv {
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
    "DOCKER_CONFIG",
    "DOCKER_HOST",
    "DOCKER_CONTEXT",
    "DOCKER_TLS_VERIFY",
    "DOCKER_CERT_PATH",
  ]);

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

  environment.NO_COLOR = "1";

  return environment;
}

// ==================================================
// Argument validation
// ==================================================

function validateArgs(
  options: RunOptions,
): void {
  if (
    !Number.isSafeInteger(options.timeoutMs) ||
    options.timeoutMs <= 0 ||
    options.timeoutMs > MAX_PROCESS_TIMEOUT_MS
  ) {
    throw invalidConfiguration(
      "Invalid runtime process timeout.",
    );
  }

  if (
    !Array.isArray(options.args) ||
    options.args.length === 0 ||
    options.args.length > MAX_ARGUMENT_COUNT
  ) {
    throw invalidConfiguration(
      "Invalid runtime process arguments.",
    );
  }

  let totalBytes = 0;

  for (const argument of options.args) {
    if (
      typeof argument !== "string" ||
      argument.includes("\0") ||
      argument.length > MAX_ARG_LENGTH
    ) {
      throw invalidConfiguration(
        "Invalid runtime process argument.",
      );
    }

    totalBytes +=
      Buffer.byteLength(argument, "utf8") + 1;

    if (totalBytes > MAX_ARGUMENT_BYTES) {
      throw invalidConfiguration(
        "Runtime arguments exceed their size limit.",
      );
    }
  }
}

// ==================================================
// Process termination
// ==================================================

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

  const fallback = (): void => {
    try {
      child.kill("SIGKILL");
    } catch {
      // Best effort. Termination deadline handles
      // unconfirmed process shutdown.
    }
  };

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

      killer.once("error", fallback);

      killer.once("close", (code) => {
        if (code !== 0) {
          fallback();
        }
      });

      killer.unref();

      return;
    }

    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      fallback();
    }
  } catch {
    fallback();
  }
}

// ==================================================
// Safe process execution
// ==================================================

function runProcess(
  options: RunOptions,
): Promise<ProcessResult> {
  assertNotCancelled(options.signal);

  validateArgs(options);

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
            detached: process.platform !== "win32",
            stdio: ["ignore", "pipe", "pipe"],
            env: createDockerEnvironment(),
          },
        );
      } catch {
        reject(
          unavailable(
            "The container runtime could not be started.",
          ),
        );

        return;
      }

      const stdoutChunks: Buffer[] = [];

      const stderrChunks: Buffer[] = [];

      let capturedBytes = 0;

      let outputTruncated = false;

      let settled = false;

      let terminationError:
        | RuntimeDeploymentError
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

        if (terminationDeadline !== undefined) {
          clearTimeout(terminationDeadline);
        }

        options.signal?.removeEventListener(
          "abort",
          abort,
        );
      };

      const fail = (
        error: RuntimeDeploymentError,
      ): void => {
        if (settled) {
          return;
        }

        settled = true;

        cleanup();

        reject(error);
      };

      const stop = (
        error: RuntimeDeploymentError,
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

        terminationDeadline = setTimeout(() => {
          terminateProcessTree(child);

          child.stdout?.destroy();

          child.stderr?.destroy();

          child.unref();

          fail(
            new RuntimeDeploymentError(
              terminationError?.code ??
                "CONTAINER_EXECUTION_FAILED",
              "Runtime process termination could not be confirmed.",
            ),
          );
        }, TERMINATION_GRACE_MS);
      };

      const abort = (): void => {
        stop(cancellationError(options.signal));
      };

      const append = (
        stream: "stdout" | "stderr",
        data: Buffer,
      ): void => {
        if (
          settled ||
          terminationError !== undefined
        ) {
          return;
        }

        const remaining =
          MAX_CAPTURE_BYTES - capturedBytes;

        if (data.length > remaining) {
          outputTruncated = true;
        }

        if (remaining <= 0) {
          return;
        }

        const captured = Buffer.from(
          data.subarray(0, remaining),
        );

        capturedBytes += captured.length;

        if (stream === "stdout") {
          stdoutChunks.push(captured);
        } else {
          stderrChunks.push(captured);
        }
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
          executionFailed(
            "Runtime output stream failed.",
          ),
        );
      });

      child.stderr?.on("error", () => {
        stop(
          executionFailed(
            "Runtime error stream failed.",
          ),
        );
      });

      child.once("error", () => {
        if (child.pid === undefined) {
          fail(
            terminationError ??
              unavailable(
                "The container runtime could not be started.",
              ),
          );

          return;
        }

        stop(
          unavailable(
            "The runtime process failed.",
          ),
        );
      });

      child.once(
        "close",
        (code, signal) => {
          if (settled) {
            return;
          }

          if (terminationError) {
            fail(terminationError);

            return;
          }

          if (options.signal?.aborted) {
            fail(
              cancellationError(options.signal),
            );

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

      deadline = setTimeout(() => {
        stop(
          executionFailed(
            "The runtime operation exceeded its deadline.",
          ),
        );
      }, options.timeoutMs);

      options.signal?.addEventListener(
        "abort",
        abort,
        {
          once: true,
        },
      );

      if (options.signal?.aborted) {
        abort();
      }
    },
  );
}

// ==================================================
// Environment file
// ==================================================

/**
 * Validate all environment values before creating
 * any temporary filesystem resources.
 */
function serializeEnvironment(
  env: Readonly<Record<string, string>>,
): string {
  if (!isRecord(env)) {
    throw invalidConfiguration(
      "Invalid runtime environment.",
    );
  }

  const entries = Object.entries(env);

  if (entries.length > MAX_ENV_VARIABLES) {
    throw invalidConfiguration(
      "Runtime environment variable count exceeded.",
    );
  }

  const lines: string[] = [];

  let totalBytes = 0;

  for (const [key, value] of entries) {
    if (
      !ENV_KEY_PATTERN.test(key) ||
      typeof value !== "string" ||
      /[\r\n\u0000]/u.test(value)
    ) {
      throw invalidConfiguration(
        "A runtime environment variable is invalid.",
      );
    }

    if (
      Buffer.byteLength(value, "utf8") >
      MAX_ENV_VALUE_BYTES
    ) {
      throw invalidConfiguration(
        "A runtime environment variable exceeds its size limit.",
      );
    }

    const line = `${key}=${value}`;

    totalBytes +=
      Buffer.byteLength(line, "utf8") + 1;

    if (totalBytes > MAX_ENV_TOTAL_BYTES) {
      throw invalidConfiguration(
        "Runtime environment exceeds its total size limit.",
      );
    }

    lines.push(line);
  }

  return lines.join("\n");
}

async function writeEnvFile(
  env: Readonly<Record<string, string>>,
): Promise<{
  path: string;
  cleanup: () => Promise<void>;
}> {
  const content = serializeEnvironment(env);

  let directory: string | undefined;

  try {
    directory = await mkdtemp(
      join(
        tmpdir(),
        "cloudpilot-runtime-",
      ),
    );

    const path = join(
      directory,
      "env.list",
    );

    await writeFile(
      path,
      content,
      {
        mode: 0o600,
        flag: "wx",
        encoding: "utf8",
      },
    );

    const cleanupDirectory = directory;

    return {
      path,

      cleanup: async (): Promise<void> => {
        await rm(cleanupDirectory, {
          recursive: true,
          force: true,
          maxRetries: 3,
          retryDelay: 100,
        });
      },
    };
  } catch {
    if (directory !== undefined) {
      try {
        await rm(directory, {
          recursive: true,
          force: true,
          maxRetries: 3,
          retryDelay: 100,
        });
      } catch {
        // Cleanup is best effort when filesystem
        // permissions or external processes interfere.
      }
    }

    throw executionFailed(
      "The runtime environment file could not be prepared.",
    );
  }
}

// ==================================================
// Container request validation
// ==================================================

function validateCreateRequest(
  request: CreateContainerRequest,
): void {
  assertNotCancelled(request.signal);

  assertImageId(request.imageId);

  assertManagedContainerName(request.name);

  assertManagedNetworkName(request.networkName);

  validateLabels(request.labels);

  if (
    request.labels["cloudpilot.managed"] !==
    "true"
  ) {
    throw invalidConfiguration(
      "Managed container ownership is required.",
    );
  }

  if (
    typeof request.imageTag !== "string" ||
    request.imageTag.length === 0 ||
    request.imageTag.length > 255 ||
    /[\s\u0000-\u001f\u007f]/u.test(
      request.imageTag,
    )
  ) {
    throw invalidConfiguration(
      "Invalid managed image tag.",
    );
  }

  if (!isValidPort(request.containerPort)) {
    throw invalidConfiguration(
      "Invalid application container port.",
    );
  }

  if (
    request.hostPort !== 0 &&
    !isValidPort(request.hostPort)
  ) {
    throw invalidConfiguration(
      "Invalid requested host port.",
    );
  }

  if (
    request.bindHost !== "127.0.0.1" &&
    request.bindHost !== "::1"
  ) {
    throw invalidConfiguration(
      "Runtime ports must use a loopback binding.",
    );
  }

  if (
    typeof request.cpuLimit !== "number" ||
    !Number.isFinite(request.cpuLimit) ||
    request.cpuLimit <= 0 ||
    request.cpuLimit > 128
  ) {
    throw invalidConfiguration(
      "Invalid container CPU limit.",
    );
  }

  if (
    !Number.isSafeInteger(
      request.memoryLimitMb,
    ) ||
    request.memoryLimitMb < 16 ||
    request.memoryLimitMb > 131_072
  ) {
    throw invalidConfiguration(
      "Invalid container memory limit.",
    );
  }

  if (
    !Number.isSafeInteger(request.pidsLimit) ||
    request.pidsLimit < 1 ||
    request.pidsLimit > 32_768
  ) {
    throw invalidConfiguration(
      "Invalid container process limit.",
    );
  }
}

// ==================================================
// Inspection parsing
// ==================================================

function parseState(
  value: unknown,
): ContainerState {
  switch (value) {
    case "created":
    case "running":
    case "exited":
    case "restarting":
    case "paused":
    case "dead":
      return value;

    default:
      return "unknown";
  }
}

/**
 * Resolve a single valid TCP host-port mapping.
 *
 * When containerPort is zero, inspect all published
 * TCP mappings and reject ambiguous results.
 */
function extractHostPort(
  ports: unknown,
  containerPort: number,
): number | null {
  if (!isRecord(ports)) {
    return null;
  }

  const candidates =
    containerPort > 0
      ? [`${containerPort}/tcp`]
      : Object.keys(ports).filter(
          (key) =>
            /^[1-9][0-9]{0,4}\/tcp$/u.test(
              key,
            ),
        );

  const resolvedPorts = new Set<number>();

  let matchedContainerPorts = 0;

  for (const candidate of candidates) {
    const mappings = ports[candidate];

    if (
      !Array.isArray(mappings) ||
      mappings.length === 0
    ) {
      continue;
    }

    matchedContainerPorts += 1;

    for (const mapping of mappings) {
      if (!isRecord(mapping)) {
        return null;
      }

      const hostIp = mapping.HostIp;

      const hostPort = mapping.HostPort;

      if (
        hostIp !== "127.0.0.1" &&
        hostIp !== "::1"
      ) {
        return null;
      }

      if (
        typeof hostPort !== "string" ||
        !/^[1-9][0-9]{0,4}$/u.test(
          hostPort,
        )
      ) {
        return null;
      }

      const port = Number(hostPort);

      if (!isValidPort(port)) {
        return null;
      }

      resolvedPorts.add(port);
    }
  }

  if (
    matchedContainerPorts !== 1 ||
    resolvedPorts.size !== 1
  ) {
    return null;
  }

  return resolvedPorts.values().next().value ??
    null;
}

function missingContainer(): ContainerInspection {
  return {
    exists: false,

    containerId: null,

    state: "unknown",

    running: false,

    exitCode: null,

    startedAt: null,

    hostPort: null,

    imageId: null,
  };
}

function isMissingContainer(
  result: ProcessResult,
): boolean {
  return (
    result.code !== 0 &&
    !result.outputTruncated &&
    /no such container|no such object/iu.test(
      result.stderr,
    )
  );
}

function isMissingNetwork(
  result: ProcessResult,
): boolean {
  return (
    result.code !== 0 &&
    !result.outputTruncated &&
    /no such network|network .+ not found/iu.test(
      result.stderr,
    )
  );
}

/**
 * Inspect only required fields instead of the full
 * Docker container configuration.
 *
 * This avoids collecting Config.Env and unrelated
 * sensitive container configuration.
 */
const CONTAINER_INSPECT_FORMAT =
  '{"Id":{{json .Id}},' +
  '"Name":{{json .Name}},' +
  '"Image":{{json .Image}},' +
  '"State":{{json .State}},' +
  '"Ports":{{json .NetworkSettings.Ports}},' +
  '"Labels":{{json .Config.Labels}}}';

function parseInspection(
  stdout: string,
  target: string,
  containerPort: number,
): ManagedContainerSnapshot {
  let parsed: unknown;

  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw executionFailed(
      "Invalid container inspection response.",
    );
  }

  if (!isRecord(parsed)) {
    throw executionFailed(
      "Unexpected container inspection response.",
    );
  }

  const containerId = parsed.Id;

  const name = parsed.Name;

  const imageId = parsed.Image;

  if (
    !isValidContainerId(containerId) ||
    typeof name !== "string" ||
    !name.startsWith("/") ||
    typeof imageId !== "string" ||
    !IMAGE_ID_PATTERN.test(imageId)
  ) {
    throw executionFailed(
      "Container inspection identity is invalid.",
    );
  }

  const normalizedName = name.slice(1);

  assertManagedContainerName(normalizedName);

  if (isValidContainerId(target)) {
    if (containerId !== target) {
      throw executionFailed(
        "Container inspection identity mismatch.",
      );
    }
  } else if (normalizedName !== target) {
    throw executionFailed(
      "Container inspection name mismatch.",
    );
  }

  const labels = parseLabels(parsed.Labels);

  if (
    labels === null ||
    labels["cloudpilot.managed"] !== "true"
  ) {
    throw executionFailed(
      "Container ownership could not be verified.",
    );
  }

  const state = isRecord(parsed.State)
    ? parsed.State
    : {};

  const exitCode =
    typeof state.ExitCode === "number" &&
    Number.isSafeInteger(state.ExitCode)
      ? state.ExitCode
      : null;

  const startedAt =
    typeof state.StartedAt === "string" &&
    Number.isFinite(
      Date.parse(state.StartedAt),
    )
      ? state.StartedAt
      : null;

  return {
    name: normalizedName,

    labels,

    inspection: {
      exists: true,

      containerId,

      state: parseState(state.Status),

      running: state.Running === true,

      exitCode,

      startedAt,

      hostPort: extractHostPort(
        parsed.Ports,
        containerPort,
      ),

      imageId,
    },
  };
}

// ==================================================
// Network inspection
// ==================================================

const NETWORK_INSPECT_FORMAT =
  '{"Id":{{json .Id}},' +
  '"Name":{{json .Name}},' +
  '"Driver":{{json .Driver}},' +
  '"Scope":{{json .Scope}},' +
  '"Labels":{{json .Labels}}}';

function parseNetworkInspection(
  stdout: string,
  expectedName: string,
): ManagedNetworkSnapshot {
  let parsed: unknown;

  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw executionFailed(
      "Invalid runtime network inspection response.",
    );
  }

  if (
    !isRecord(parsed) ||
    !isValidContainerId(parsed.Id) ||
    parsed.Name !== expectedName ||
    parsed.Driver !== "bridge" ||
    parsed.Scope !== "local"
  ) {
    throw executionFailed(
      "Runtime network identity could not be verified.",
    );
  }

  const labels = parseLabels(parsed.Labels);

  if (labels === null) {
    throw executionFailed(
      "Runtime network labels are invalid.",
    );
  }

  return {
    id: parsed.Id,

    name: expectedName,

    driver: "bridge",

    scope: "local",

    labels,
  };
}

// ==================================================
// UTF-8 byte-safe truncation
// ==================================================

function truncateUtf8(
  value: string,
  maxBytes: number,
): string {
  if (
    Buffer.byteLength(value, "utf8") <=
    maxBytes
  ) {
    return value;
  }

  const output: string[] = [];

  let bytes = 0;

  for (const character of value) {
    const characterBytes =
      Buffer.byteLength(character, "utf8");

    if (bytes + characterBytes > maxBytes) {
      break;
    }

    output.push(character);

    bytes += characterBytes;
  }

  return output.join("");
}

// ==================================================
// Historical log validation
// ==================================================

function resolveHistoricalSince(
  since: string | null | undefined,
): string | null {
  if (
    since === undefined ||
    since === null
  ) {
    return null;
  }

  if (
    typeof since !== "string" ||
    since.length > 64 ||
    !RFC3339_PATTERN.test(since)
  ) {
    throw invalidConfiguration(
      "Invalid historical log timestamp.",
    );
  }

  const timestamp = Date.parse(since);

  const now = Date.now();

  if (
    !Number.isFinite(timestamp) ||
    timestamp > now + 60_000
  ) {
    throw invalidConfiguration(
      "Historical log timestamp is outside its allowed range.",
    );
  }

  const earliest =
    now - MAX_HISTORICAL_LOG_MS;

  return new Date(
    Math.max(timestamp, earliest),
  ).toISOString();
}

// ==================================================
// Docker runtime executor
// ==================================================

export class DockerRuntimeExecutor
  implements RuntimeExecutor
{
  // ==================================================
  // Runtime availability
  // ==================================================

  async probe(): Promise<RuntimeCapabilities> {
    const unavailableCapabilities:
      RuntimeCapabilities = {
        available: false,

        runtimeType: "docker",

        supportsResourceLimits: false,

        supportsNetworkIsolation: false,

        supportsStrongIsolation: false,
      };

    try {
      const version = await runProcess({
        args: [
          "version",
          "--format",
          "{{.Server.Version}}",
        ],

        timeoutMs: PROBE_TIMEOUT_MS,
      });

      if (
        version.code !== 0 ||
        version.outputTruncated ||
        !version.stdout.trim()
      ) {
        return unavailableCapabilities;
      }

      const info = await runProcess({
        args: [
          "info",
          "--format",
          '{"OSType":{{json .OSType}},' +
            '"MemoryLimit":{{json .MemoryLimit}},' +
            '"CpuCfsQuota":{{json .CpuCfsQuota}},' +
            '"PidsLimit":{{json .PidsLimit}},' +
            '"Network":{{json .Plugins.Network}}}',
        ],

        timeoutMs: PROBE_TIMEOUT_MS,
      });

      if (
        info.code !== 0 ||
        info.outputTruncated
      ) {
        return unavailableCapabilities;
      }

      const parsed: unknown = JSON.parse(
        info.stdout,
      );

      if (
        !isRecord(parsed) ||
        parsed.OSType !== "linux"
      ) {
        return unavailableCapabilities;
      }

      const supportsResourceLimits =
        parsed.MemoryLimit === true &&
        parsed.CpuCfsQuota === true &&
        parsed.PidsLimit === true;

      const supportsNetworkIsolation =
        Array.isArray(parsed.Network) &&
        parsed.Network.includes("bridge");

      return {
        available: true,

        runtimeType: "docker",

        supportsResourceLimits,

        supportsNetworkIsolation,

        supportsStrongIsolation: false,
      };
    } catch {
      return unavailableCapabilities;
    }
  }

  // ==================================================
  // Image verification
  // ==================================================

  async inspectImage(
    imageId: string,
  ): Promise<ImageVerification> {
    assertImageId(imageId);

    const result = await runProcess({
      args: [
        "image",
        "inspect",
        "--format",
        "{{json .Id}}",
        "--",
        imageId,
      ],

      timeoutMs: INSPECT_TIMEOUT_MS,
    });

    if (result.code !== 0) {
      if (
        !result.outputTruncated &&
        /no such image|no such object/iu.test(
          result.stderr,
        )
      ) {
        return {
          exists: false,

          imageId: null,
        };
      }

      throw new RuntimeDeploymentError(
        "IMAGE_UNAVAILABLE",
        "The image could not be inspected.",
      );
    }

    if (result.outputTruncated) {
      throw new RuntimeDeploymentError(
        "IMAGE_UNAVAILABLE",
        "Image inspection output exceeded its limit.",
      );
    }

    let parsed: unknown;

    try {
      parsed = JSON.parse(result.stdout);
    } catch {
      throw new RuntimeDeploymentError(
        "IMAGE_UNAVAILABLE",
        "Invalid image inspection response.",
      );
    }

    if (
      typeof parsed !== "string" ||
      !IMAGE_ID_PATTERN.test(parsed) ||
      parsed !== imageId
    ) {
      throw new RuntimeDeploymentError(
        "IMAGE_OWNERSHIP_FAILED",
        "The verified image identity does not match.",
      );
    }

    return {
      exists: true,

      imageId: parsed,
    };
  }

  // ==================================================
  // Managed network inspection
  // ==================================================

  private async inspectNetwork(
    name: string,
    signal?: AbortSignal,
  ): Promise<ManagedNetworkSnapshot | null> {
    assertManagedNetworkName(name);

    const result = await runProcess({
      args: [
        "network",
        "inspect",
        "--format",
        NETWORK_INSPECT_FORMAT,
        "--",
        name,
      ],

      timeoutMs: INSPECT_TIMEOUT_MS,

      signal,
    });

    if (result.code !== 0) {
      if (isMissingNetwork(result)) {
        return null;
      }

      throw unavailable(
        "The managed runtime network could not be inspected.",
      );
    }

    if (result.outputTruncated) {
      throw executionFailed(
        "Runtime network inspection exceeded its limit.",
      );
    }

    return parseNetworkInspection(
      result.stdout,
      name,
    );
  }

  // ==================================================
  // Network creation
  // ==================================================

  async ensureNetwork(
    name: string,
    labels: Record<string, string>,
  ): Promise<boolean> {
    assertManagedNetworkName(name);

    validateLabels(labels);

    const expectedLabels = {
      ...labels,

      "cloudpilot.managed": "true",
    };

    const existing = await this.inspectNetwork(
      name,
    );

    if (existing !== null) {
      if (
        !labelsMatch(
          existing.labels,
          expectedLabels,
        )
      ) {
        throw executionFailed(
          "Existing runtime network ownership does not match.",
        );
      }

      return true;
    }

    const args: string[] = [
      "network",
      "create",
      "--driver",
      "bridge",
    ];

    for (const [key, value] of Object.entries(
      expectedLabels,
    )) {
      args.push(
        "--label",
        `${key}=${value}`,
      );
    }

    args.push("--", name);

    const created = await runProcess({
      args,

      timeoutMs: CREATE_TIMEOUT_MS,
    });

    /**
     * Reinspect even after successful creation.
     *
     * A concurrent creator may have won the race.
     * Name existence alone is not ownership proof.
     */
    const verified = await this.inspectNetwork(
      name,
    );

    if (
      verified === null ||
      !labelsMatch(
        verified.labels,
        expectedLabels,
      )
    ) {
      throw unavailable(
        "The managed runtime network could not be verified.",
      );
    }

    if (
      created.code === 0 &&
      !created.outputTruncated
    ) {
      const createdId = created.stdout.trim();

      if (
        !isValidContainerId(createdId) ||
        createdId !== verified.id
      ) {
        throw executionFailed(
          "Created runtime network identity mismatch.",
        );
      }
    }

    return true;
  }

  // ==================================================
  // Container inspection
  // ==================================================

  private async inspectManagedContainer(
    target: string,
    containerPort = 0,
    signal?: AbortSignal,
  ): Promise<ManagedContainerSnapshot | null> {
    if (!isValidContainerId(target)) {
      assertManagedContainerName(target);
    }

    assertNotCancelled(signal);

    const result = await runProcess({
      args: [
        "container",
        "inspect",
        "--format",
        CONTAINER_INSPECT_FORMAT,
        "--",
        target,
      ],

      timeoutMs: INSPECT_TIMEOUT_MS,

      signal,
    });

    if (result.code !== 0) {
      if (isMissingContainer(result)) {
        return null;
      }

      throw executionFailed(
        "The container could not be inspected.",
      );
    }

    if (result.outputTruncated) {
      throw executionFailed(
        "Container inspection output exceeded its limit.",
      );
    }

    return parseInspection(
      result.stdout,
      target,
      containerPort,
    );
  }

  /**
   * Resolve a legacy managed name into an immutable
   * container identifier before executing a command.
   *
   * The orchestration layer must still compare the
   * resolved identity against its persisted identity.
   */
  private async resolveManagedContainerId(
    name: string,
    signal?: AbortSignal,
  ): Promise<string | null> {
    assertManagedContainerName(name);

    const snapshot =
      await this.inspectManagedContainer(
        name,
        0,
        signal,
      );

    if (snapshot === null) {
      return null;
    }

    const containerId =
      snapshot.inspection.containerId;

    assertContainerId(containerId);

    return containerId;
  }

  // ==================================================
  // Container creation
  // ==================================================

  async createContainer(
    request: CreateContainerRequest,
  ): Promise<CreatedContainer> {
    validateCreateRequest(request);

    const network = await this.inspectNetwork(
      request.networkName,
      request.signal,
    );

    if (
      network === null ||
      network.labels["cloudpilot.managed"] !==
        "true"
    ) {
      throw executionFailed(
        "The managed runtime network is unavailable.",
      );
    }

    const image = await this.inspectImage(
      request.imageId,
    );

    if (
      !image.exists ||
      image.imageId !== request.imageId
    ) {
      throw new RuntimeDeploymentError(
        "IMAGE_UNAVAILABLE",
        "The verified runtime image is unavailable.",
      );
    }

    assertNotCancelled(request.signal);

    const envFile = await writeEnvFile(
      request.env,
    );

    let operationFailed = false;

    try {
      assertNotCancelled(request.signal);

      const args: string[] = [
        "create",

        "--name",
        request.name,

        "--network",
        network.id,

        "--pull",
        "never",

        "--cpus",
        String(request.cpuLimit),

        "--memory",
        `${request.memoryLimitMb}m`,

        "--pids-limit",
        String(request.pidsLimit),

        "--read-only",

        "--tmpfs",
        "/tmp:rw,noexec,nosuid,size=64m",

        "--cap-drop",
        "ALL",

        "--security-opt",
        "no-new-privileges=true",

        "--log-driver",
        "json-file",

        "--log-opt",
        "max-size=10m",

        "--log-opt",
        "max-file=3",

        "--label",
        "cloudpilot.managed=true",
      ];

      for (const [key, value] of Object.entries(
        request.labels,
      )) {
        if (key === "cloudpilot.managed") {
          continue;
        }

        args.push(
          "--label",
          `${key}=${value}`,
        );
      }

      args.push(
        "--env-file",
        envFile.path,
      );

      const bindAddress =
        request.bindHost === "::1"
          ? "[::1]"
          : request.bindHost;

      const requestedPort =
        request.hostPort === 0
          ? ""
          : String(request.hostPort);

      args.push(
        "--publish",
        `${bindAddress}:${requestedPort}:${request.containerPort}/tcp`,
      );

      /**
       * Immutable image identity only.
       */
      args.push(
        "--",
        request.imageId,
      );

      const result = await runProcess({
        args,

        timeoutMs: CREATE_TIMEOUT_MS,

        signal: request.signal,
      });

      assertNotCancelled(request.signal);

      if (
        result.code !== 0 ||
        result.outputTruncated
      ) {
        throw new RuntimeDeploymentError(
          "CONTAINER_CREATION_FAILED",
          "The container could not be created.",
        );
      }

      const containerId = result.stdout.trim();

      if (!isValidContainerId(containerId)) {
        throw new RuntimeDeploymentError(
          "CONTAINER_CREATION_FAILED",
          "The container identifier is invalid.",
        );
      }

      /**
       * Inspect the immutable ID, not the mutable name.
       */
      const snapshot =
        await this.inspectManagedContainer(
          containerId,
          request.containerPort,
          request.signal,
        );

      if (
        snapshot === null ||
        snapshot.name !== request.name ||
        snapshot.inspection.containerId !==
          containerId ||
        snapshot.inspection.imageId !==
          request.imageId ||
        !labelsMatch(
          snapshot.labels,
          request.labels,
        )
      ) {
        throw new RuntimeDeploymentError(
          "CONTAINER_CREATION_FAILED",
          "Created container ownership could not be verified.",
        );
      }

      assertNotCancelled(request.signal);

      /**
       * Dynamic host ports may not be available
       * until after the container has started.
       */
      const hostPort =
        snapshot.inspection.hostPort ??
        request.hostPort;

      return {
        containerId,

        containerName: request.name,

        hostPort,

        networkName: request.networkName,
      };
    } catch (error) {
      operationFailed = true;

      /**
       * Do not automatically remove a container after
       * an uncertain create result.
       *
       * The orchestration layer must reconcile the
       * actual runtime state and execution ownership.
       */
      throw error;
    } finally {
      try {
        await envFile.cleanup();
      } catch {
        /**
         * Preserve the original execution error when
         * the operation already failed.
         */
        if (!operationFailed) {
          throw executionFailed(
            "Runtime environment file cleanup failed.",
          );
        }
      }
    }
  }

  // ==================================================
  // Container start
  // ==================================================

  async startContainer(
    name: string,
    signal: AbortSignal,
  ): Promise<void> {
    assertNotCancelled(signal);

    const containerId =
      await this.resolveManagedContainerId(
        name,
        signal,
      );

    if (containerId === null) {
      throw new RuntimeDeploymentError(
        "CONTAINER_START_FAILED",
        "The managed container does not exist.",
      );
    }

    const result = await runProcess({
      args: [
        "start",
        "--",
        containerId,
      ],

      timeoutMs: START_TIMEOUT_MS,

      signal,
    });

    assertNotCancelled(signal);

    if (
      result.code !== 0 ||
      result.outputTruncated
    ) {
      throw new RuntimeDeploymentError(
        "CONTAINER_START_FAILED",
        "The container could not be started.",
      );
    }
  }

  // ==================================================
  // Public container inspection
  // ==================================================

  async inspectContainer(
    name: string,
  ): Promise<ContainerInspection> {
    const snapshot =
      await this.inspectManagedContainer(
        name,
      );

    return (
      snapshot?.inspection ??
      missingContainer()
    );
  }

  // ==================================================
  // Container stop
  // ==================================================

  async stopContainer(
    name: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<void> {
    assertLifecycleTimeout(timeoutMs);

    assertNotCancelled(signal);

    const containerId =
      await this.resolveManagedContainerId(
        name,
        signal,
      );

    /**
     * Stopping an already absent container is
     * treated as an idempotent success.
     */
    if (containerId === null) {
      return;
    }

    const graceSeconds = Math.max(
      1,
      Math.ceil(timeoutMs / 1_000),
    );

    const result = await runProcess({
      args: [
        "stop",

        "--time",
        String(graceSeconds),

        "--",
        containerId,
      ],

      timeoutMs: timeoutMs + 10_000,

      signal,
    });

    assertNotCancelled(signal);

    if (
      result.outputTruncated ||
      (
        result.code !== 0 &&
        !isMissingContainer(result)
      )
    ) {
      throw executionFailed(
        "The container could not be stopped.",
      );
    }
  }

  // ==================================================
  // Container restart
  // ==================================================

  async restartContainer(
    name: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<void> {
    assertLifecycleTimeout(timeoutMs);

    assertNotCancelled(signal);

    const containerId =
      await this.resolveManagedContainerId(
        name,
        signal,
      );

    if (containerId === null) {
      throw executionFailed(
        "The managed container does not exist.",
      );
    }

    const graceSeconds = Math.max(
      1,
      Math.ceil(timeoutMs / 1_000),
    );

    const result = await runProcess({
      args: [
        "restart",

        "--time",
        String(graceSeconds),

        "--",
        containerId,
      ],

      timeoutMs: timeoutMs + 10_000,

      signal,
    });

    assertNotCancelled(signal);

    if (
      result.code !== 0 ||
      result.outputTruncated
    ) {
      throw executionFailed(
        "The container could not be restarted.",
      );
    }
  }

  // ==================================================
  // Container removal
  // ==================================================

  async removeContainer(
    name: string,
  ): Promise<void> {
    const containerId =
      await this.resolveManagedContainerId(
        name,
      );

    if (containerId === null) {
      return;
    }

    const result = await runProcess({
      args: [
        "rm",

        "--force",

        "--",
        containerId,
      ],

      timeoutMs: REMOVE_TIMEOUT_MS,
    });

    if (
      result.outputTruncated ||
      (
        result.code !== 0 &&
        !isMissingContainer(result)
      )
    ) {
      throw executionFailed(
        "The container could not be removed safely.",
      );
    }
  }

  // ==================================================
  // Bounded one-shot logs
  // ==================================================

  async logs(
    name: string,
    maxBytes: number,
  ): Promise<string> {
    if (
      !Number.isSafeInteger(maxBytes) ||
      maxBytes <= 0 ||
      maxBytes > MAX_CAPTURE_BYTES
    ) {
      throw invalidConfiguration(
        "The log byte limit is invalid.",
      );
    }

    const containerId =
      await this.resolveManagedContainerId(
        name,
      );

    if (containerId === null) {
      return "";
    }

    const result = await runProcess({
      args: [
        "logs",

        "--tail",
        "200",

        "--",
        containerId,
      ],

      timeoutMs: LOGS_TIMEOUT_MS,
    });

    if (result.code !== 0) {
      return "";
    }

    const combined =
      `${result.stdout}${result.stderr}`;

    /**
     * Raw output remains potentially sensitive.
     *
     * Consumers must redact before persistence
     * or user-facing exposure.
     */
    return truncateUtf8(
      combined,
      maxBytes,
    );
  }

  // ==================================================
  // Real-time container log streaming
  // ==================================================

  async streamLogs(
    request: StreamLogsRequest,
  ): Promise<void> {
    assertContainerId(request.containerId);

    assertNotCancelled(request.signal);

    if (
      typeof request.follow !== "boolean" ||
      typeof request.onData !== "function"
    ) {
      throw invalidConfiguration(
        "Invalid runtime log streaming request.",
      );
    }

    const since = resolveHistoricalSince(
      request.since,
    );

    /**
     * Reverify managed ownership immediately before
     * attaching to the immutable container ID.
     *
     * The collector remains responsible for checking
     * the complete deployment ownership labels.
     */
    const labels = await this.readContainerLabels(
      request.containerId,
    );

    assertNotCancelled(request.signal);

    if (
      labels === null ||
      labels["cloudpilot.managed"] !== "true"
    ) {
      throw executionFailed(
        "Log stream container ownership could not be verified.",
      );
    }

    /**
     * No history and no follow means there is
     * nothing to retrieve.
     */
    if (
      since === null &&
      !request.follow
    ) {
      return;
    }

    /**
     * Historical recovery and live following use
     * separate bounded operations.
     *
     * This avoids an unlimited initial history
     * replay on a long-lived Docker logs process.
     *
     * The boundary is best effort: Docker timestamps
     * and reconnects can produce duplicate records
     * or gaps.
     */
    const liveBoundary =
      new Date().toISOString();

    if (since !== null) {
      const historicalArgs: string[] = [
        "logs",

        "--timestamps",

        "--since",
        since,

        "--until",
        liveBoundary,

        "--tail",
        String(MAX_HISTORICAL_LOG_LINES),

        "--",
        request.containerId,
      ];

      await this.executeLogStream(
        request,
        historicalArgs,
        {
          maxBytes: MAX_HISTORICAL_LOG_BYTES,

          timeoutMs: LOGS_TIMEOUT_MS,
        },
      );
    }

    assertNotCancelled(request.signal);

    if (!request.follow) {
      return;
    }

    const liveArgs: string[] = [
      "logs",

      "--timestamps",

      "--follow",
    ];

    if (since === null) {
      /**
       * Explicitly exclude existing historical logs.
       */
      liveArgs.push(
        "--tail",
        "0",
      );
    } else {
      /**
       * Recover output generated between the
       * historical query and live attachment.
       *
       * The tail limit bounds the initial catch-up.
       */
      liveArgs.push(
        "--since",
        liveBoundary,

        "--tail",
        String(MAX_HISTORICAL_LOG_LINES),
      );
    }

    liveArgs.push(
      "--",
      request.containerId,
    );

    await this.executeLogStream(
      request,
      liveArgs,
      {},
    );
  }

  // ==================================================
  // Streaming process lifecycle
  // ==================================================

  private async executeLogStream(
    request: StreamLogsRequest,
    args: string[],
    limits: {
      maxBytes?: number;

      timeoutMs?: number;
    },
  ): Promise<void> {
    assertNotCancelled(request.signal);

    validateArgs({
      args,

      timeoutMs:
        limits.timeoutMs ??
        MAX_PROCESS_TIMEOUT_MS,
    });

    await new Promise<void>(
      (resolve, reject) => {
        let child: ReturnType<typeof spawn>;

        try {
          child = spawn(
            DOCKER_BINARY,
            args,
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
            },
          );
        } catch {
          reject(
            unavailable(
              "The container log stream could not be started.",
            ),
          );

          return;
        }

        let settled = false;

        let stopping = false;

        let cancelled = false;

        let deliveredBytes = 0;

        let failure:
          | RuntimeDeploymentError
          | undefined;

        let deadline:
          | ReturnType<typeof setTimeout>
          | undefined;

        let terminationDeadline:
          | ReturnType<typeof setTimeout>
          | undefined;

        const cleanup = (): void => {
          request.signal.removeEventListener(
            "abort",
            onAbort,
          );

          if (deadline !== undefined) {
            clearTimeout(deadline);
          }

          if (
            terminationDeadline !== undefined
          ) {
            clearTimeout(
              terminationDeadline,
            );
          }

          child.stdout?.removeListener(
            "data",
            onStdout,
          );

          child.stderr?.removeListener(
            "data",
            onStderr,
          );
        };

        const finish = (
          error?: RuntimeDeploymentError,
        ): void => {
          if (settled) {
            return;
          }

          settled = true;

          stopping = true;

          cleanup();

          if (error !== undefined) {
            reject(error);
          } else {
            resolve();
          }
        };

        const stop = (
          error?: RuntimeDeploymentError,
          isCancellation = false,
        ): void => {
          if (settled || stopping) {
            return;
          }

          /**
           * Disable all subsequent callbacks before
           * initiating process termination.
           */
          stopping = true;

          cancelled = isCancellation;

          failure = error;

          if (deadline !== undefined) {
            clearTimeout(deadline);
          }

          terminateProcessTree(child);

          /**
           * Wait for process close whenever possible.
           *
           * If termination cannot be confirmed,
           * reject explicitly rather than report
           * successful cleanup.
           */
          terminationDeadline = setTimeout(
            () => {
              terminateProcessTree(child);

              child.stdout?.destroy();

              child.stderr?.destroy();

              child.unref();

              finish(
                executionFailed(
                  "Log stream process termination could not be confirmed.",
                ),
              );
            },
            TERMINATION_GRACE_MS,
          );
        };

        const onAbort = (): void => {
          stop(
            undefined,
            true,
          );
        };

        const deliver = (
          stream: "stdout" | "stderr",
          data: Buffer,
        ): void => {
          if (
            settled ||
            stopping ||
            request.signal.aborted
          ) {
            if (
              request.signal.aborted &&
              !settled &&
              !stopping
            ) {
              onAbort();
            }

            return;
          }

          /**
           * Deliver bounded raw chunks.
           *
           * UTF-8 decoding and line reconstruction
           * belong to LogNormalizer.
           */
          for (
            let offset = 0;
            offset < data.length;
            offset += MAX_LOG_CHUNK_BYTES
          ) {
            if (
              settled ||
              stopping ||
              request.signal.aborted
            ) {
              if (
                request.signal.aborted &&
                !settled &&
                !stopping
              ) {
                onAbort();
              }

              return;
            }

            const chunk = data.subarray(
              offset,
              Math.min(
                offset + MAX_LOG_CHUNK_BYTES,
                data.length,
              ),
            );

            if (
              limits.maxBytes !== undefined &&
              deliveredBytes + chunk.length >
                limits.maxBytes
            ) {
              /**
               * Fail closed instead of silently
               * discarding arbitrary partial lines.
               */
              stop(
                executionFailed(
                  "Historical runtime log byte limit exceeded.",
                ),
              );

              return;
            }

            if (limits.maxBytes !== undefined) {
              deliveredBytes += chunk.length;
            }

            try {
              request.onData(
                stream,
                chunk,
              );
            } catch {
              stop(
                unavailable(
                  "Log collection sink failed.",
                ),
              );

              return;
            }
          }
        };

        const onStdout = (
          data: Buffer,
        ): void => {
          deliver("stdout", data);
        };

        const onStderr = (
          data: Buffer,
        ): void => {
          deliver("stderr", data);
        };

        child.stdout?.on(
          "data",
          onStdout,
        );

        child.stderr?.on(
          "data",
          onStderr,
        );

        child.stdout?.on(
          "error",
          () => {
            stop(
              executionFailed(
                "Runtime log output stream failed.",
              ),
            );
          },
        );

        child.stderr?.on(
          "error",
          () => {
            stop(
              executionFailed(
                "Runtime log error stream failed.",
              ),
            );
          },
        );

        child.once("error", () => {
          if (child.pid === undefined) {
            finish(
              failure ??
                unavailable(
                  "The container log stream could not be started.",
                ),
            );

            return;
          }

          stop(
            unavailable(
              "The container log stream failed.",
            ),
          );
        });

        child.once(
          "close",
          (code, signal) => {
            if (settled) {
              return;
            }

            if (failure !== undefined) {
              finish(failure);

              return;
            }

            if (
              cancelled ||
              request.signal.aborted
            ) {
              finish();

              return;
            }

            if (
              code !== 0 ||
              signal !== null
            ) {
              finish(
                unavailable(
                  "The container log stream terminated unexpectedly.",
                ),
              );

              return;
            }

            finish();
          },
        );

        if (
          limits.timeoutMs !== undefined
        ) {
          deadline = setTimeout(
            () => {
              stop(
                executionFailed(
                  "Historical runtime log recovery exceeded its deadline.",
                ),
              );
            },
            limits.timeoutMs,
          );
        }

        request.signal.addEventListener(
          "abort",
          onAbort,
          {
            once: true,
          },
        );

        if (request.signal.aborted) {
          onAbort();
        }
      },
    );
  }

  // ==================================================
  // Immutable ownership label inspection
  // ==================================================

  async readContainerLabels(
    containerId: string,
  ): Promise<Record<string, string> | null> {
    if (!isValidContainerId(containerId)) {
      return null;
    }

    try {
      const result = await runProcess({
        args: [
          "container",

          "inspect",

          "--format",

          '{"Id":{{json .Id}},' +
            '"Labels":{{json .Config.Labels}}}',

          "--",

          containerId,
        ],

        timeoutMs: INSPECT_TIMEOUT_MS,
      });

      if (
        result.code !== 0 ||
        result.outputTruncated
      ) {
        return null;
      }

      const parsed: unknown = JSON.parse(
        result.stdout,
      );

      if (
        !isRecord(parsed) ||
        parsed.Id !== containerId
      ) {
        return null;
      }

      const labels = parseLabels(
        parsed.Labels,
      );

      if (
        labels === null ||
        labels["cloudpilot.managed"] !== "true"
      ) {
        return null;
      }

      return labels;
    } catch {
      /**
       * Fail closed.
       *
       * No fallback to mutable names or partial
       * container identifiers.
       */
      return null;
    }
  }
}