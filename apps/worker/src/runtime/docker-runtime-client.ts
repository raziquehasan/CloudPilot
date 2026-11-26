/**
 * CloudPilot — Docker Runtime Executor
 *
 * Executes CloudPilot-managed containers through the Docker CLI.
 *
 * Security properties:
 * - Shell-free process execution (argument arrays).
 * - Restricted child process environment.
 * - Bounded process output and timeouts.
 * - Cooperative cancellation.
 * - Secrets passed via a restrictive temporary env file,
 *   never on the command line.
 * - Strong container hardening flags.
 * - Scoped, name-bounded cleanup only.
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

import { StringDecoder } from "node:string_decoder";

import { RuntimeDeploymentError } from "@cloudpilot/queue";

import type {
  ContainerInspection,
  ContainerState,
  CreateContainerRequest,
  CreatedContainer,
  ImageVerification,
  RuntimeCapabilities,
  RuntimeExecutor,
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

const MAX_CAPTURE_BYTES = 4 * 1024 * 1024;

const MAX_ARG_LENGTH = 32_768;

const IMAGE_ID_PATTERN = /^sha256:[a-f0-9]{64}$/u;

const CONTAINER_NAME_PATTERN =
  /^cloudpilot-[0-9a-f-]{36}-[0-9a-f-]{36}-[1-9][0-9]{0,15}$/iu;

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
}

// ==================================================
// Error helpers
// ==================================================

function unavailable(message: string): RuntimeDeploymentError {
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

function cancellationError(
  signal?: AbortSignal,
): RuntimeDeploymentError {
  if (signal?.reason instanceof RuntimeDeploymentError) {
    return signal.reason;
  }

  return new RuntimeDeploymentError("RUNTIME_CANCELLED");
}

function assertNotCancelled(signal?: AbortSignal): void {
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
 * Database URLs, Redis credentials, GitHub App credentials,
 * encryption keys and other secrets must not be inherited by
 * the Docker CLI.
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

  for (const [key, value] of Object.entries(process.env)) {
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

function validateArgs(options: RunOptions): void {
  if (
    !Number.isSafeInteger(options.timeoutMs) ||
    options.timeoutMs <= 0 ||
    options.timeoutMs > 3_600_000
  ) {
    throw invalidConfiguration(
      "Invalid runtime process timeout.",
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

    totalBytes += Buffer.byteLength(argument, "utf8") + 1;

    if (totalBytes > 256 * 1024) {
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

  try {
    if (process.platform === "win32") {
      const killer = spawn(
        "taskkill",
        ["/PID", String(pid), "/T", "/F"],
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
          // Best effort.
        }
      });

      killer.unref();

      return;
    }

    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      child.kill("SIGKILL");
    }
  } catch {
    // The termination deadline handles stubborn processes.
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

  return new Promise<ProcessResult>((resolve, reject) => {
    let child: ReturnType<typeof spawn>;

    try {
      child = spawn(DOCKER_BINARY, options.args, {
        shell: false,
        windowsHide: true,
        detached: process.platform !== "win32",
        stdio: ["ignore", "pipe", "pipe"],
        env: createDockerEnvironment(),
      });
    } catch (error) {
      reject(
        unavailable(
          "The container runtime could not be started.",
        ),
      );

      return;
    }

    const stdoutDecoder = new StringDecoder("utf8");
    const stderrDecoder = new StringDecoder("utf8");

    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];

    let capturedBytes = 0;
    let outputTruncated = false;
    let settled = false;
    let terminationError: RuntimeDeploymentError | undefined;
    let deadline: ReturnType<typeof setTimeout> | undefined;
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

      options.signal?.removeEventListener("abort", abort);
    };

    const fail = (error: RuntimeDeploymentError): void => {
      if (settled) {
        return;
      }

      settled = true;

      cleanup();

      reject(error);
    };

    const stop = (error: RuntimeDeploymentError): void => {
      if (settled || terminationError !== undefined) {
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
      }, 5_000);
    };

    const abort = (): void => {
      stop(cancellationError(options.signal));
    };

    const append = (stream: "stdout" | "stderr", data: Buffer): void => {
      if (settled) {
        return;
      }

      const remaining = MAX_CAPTURE_BYTES - capturedBytes;

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
    };

    child.stdout?.on("data", (data: Buffer) => {
      const text = stdoutDecoder.write(data);

      if (text) {
        // Streamed logs are not surfaced for runtime create;
        // output is captured for bounded inspection.
      }

      append("stdout", data);
    });

    child.stderr?.on("data", (data: Buffer) => {
      stderrDecoder.write(data);
      append("stderr", data);
    });

    child.stdout?.on("error", () => {
      stop(
        new RuntimeDeploymentError(
          "CONTAINER_EXECUTION_FAILED",
          "Runtime output stream failed.",
        ),
      );
    });

    child.stderr?.on("error", () => {
      stop(
        new RuntimeDeploymentError(
          "CONTAINER_EXECUTION_FAILED",
          "Runtime error stream failed.",
        ),
      );
    });

    child.once("error", () => {
      if (terminationError) {
        fail(terminationError);

        return;
      }

      fail(
        unavailable(
          "The container runtime could not be started.",
        ),
      );
    });

    child.once("close", (code, signal) => {
      if (settled) {
        return;
      }

      if (terminationError) {
        fail(terminationError);

        return;
      }

      settled = true;

      cleanup();

      resolve({
        code,
        signal,
        stdout: Buffer.concat(stdoutChunks).toString("utf8"),
        stderr: Buffer.concat(stderrChunks).toString("utf8"),
        outputTruncated,
      });
    });

    deadline = setTimeout(() => {
      stop(
        new RuntimeDeploymentError(
          "CONTAINER_EXECUTION_FAILED",
          "The runtime operation exceeded its deadline.",
        ),
      );
    }, options.timeoutMs);

    options.signal?.addEventListener("abort", abort, {
      once: true,
    });

    if (options.signal?.aborted) {
      abort();
    }
  });
}

// ==================================================
// Argument builders
// ==================================================

function assertManagedContainerName(name: string): void {
  if (
    typeof name !== "string" ||
    name.length === 0 ||
    name.length > 128 ||
    !CONTAINER_NAME_PATTERN.test(name)
  ) {
    throw invalidConfiguration(
      "The managed container name is invalid.",
    );
  }
}

function assertImageId(imageId: string): void {
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

// ==================================================
// Env file
// ==================================================

async function writeEnvFile(
  env: Readonly<Record<string, string>>,
): Promise<{ path: string; cleanup: () => Promise<void> }> {
  const directory = await mkdtemp(
    join(tmpdir(), "cloudpilot-runtime-"),
  );

  const path = join(directory, "env.list");

  const lines: string[] = [];

  for (const [key, value] of Object.entries(env)) {
    if (
      !/^[A-Za-z_][A-Za-z0-9_]*$/u.test(key) ||
      /[\r\n\u0000]/u.test(value)
    ) {
      throw invalidConfiguration(
        "A runtime environment variable is invalid.",
      );
    }

    lines.push(`${key}=${value}`);
  }

  await writeFile(path, lines.join("\n"), {
    mode: 0o600,
  });

  return {
    path,
    cleanup: () => rm(directory, { recursive: true, force: true }),
  };
}

// ==================================================
// Inspection parsing
// ==================================================

function parseState(value: unknown): ContainerState {
  if (typeof value !== "string") {
    return "unknown";
  }

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

function extractHostPort(
  ports: unknown,
  containerPort: number,
): number | null {
  if (
    typeof ports !== "object" ||
    ports === null ||
    Array.isArray(ports)
  ) {
    return null;
  }

  const record = ports as Record<string, unknown>;

  const candidates = [
    `${containerPort}/tcp`,
    `${containerPort}/udp`,
  ];

  for (const candidate of candidates) {
    const mappings = record[candidate];

    if (!Array.isArray(mappings)) {
      continue;
    }

    for (const mapping of mappings) {
      if (
        typeof mapping === "object" &&
        mapping !== null &&
        "HostPort" in mapping &&
        typeof (mapping as { HostPort: unknown }).HostPort ===
          "string"
      ) {
        const port = Number(
          (mapping as { HostPort: string }).HostPort,
        );

        if (Number.isInteger(port) && port > 0) {
          return port;
        }
      }
    }
  }

  return null;
}

function parseInspection(
  stdout: string,
  name: string,
  containerPort: number,
): ContainerInspection {
  let parsed: unknown;

  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw new RuntimeDeploymentError(
      "CONTAINER_EXECUTION_FAILED",
      "Invalid container inspection response.",
    );
  }

  if (
    typeof parsed !== "object" ||
    parsed === null ||
    Array.isArray(parsed)
  ) {
    throw new RuntimeDeploymentError(
      "CONTAINER_EXECUTION_FAILED",
      "Unexpected container inspection response.",
    );
  }

  const data = parsed as Record<string, unknown>;

  const state =
    typeof data.State === "object" && data.State !== null
      ? (data.State as Record<string, unknown>)
      : {};

  const network =
    typeof data.NetworkSettings === "object" &&
    data.NetworkSettings !== null
      ? (data.NetworkSettings as Record<string, unknown>)
      : {};

  const running = state.Running === true;

  const exitCode =
    typeof state.ExitCode === "number"
      ? state.ExitCode
      : null;

  const startedAt =
    typeof state.StartedAt === "string"
      ? state.StartedAt
      : null;

  const containerId =
    typeof data.Id === "string" ? data.Id : null;

  const imageId =
    typeof data.Image === "string" ? data.Image : null;

  void name;

  return {
    exists: true,
    containerId,
    state: parseState(state.Status),
    running,
    exitCode,
    startedAt,
    hostPort: extractHostPort(
      network.Ports,
      containerPort,
    ),
    imageId,
  };
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

function isMissingContainer(result: ProcessResult): boolean {
  return (
    !result.outputTruncated &&
    /no such container|no such object/iu.test(
      result.stderr,
    )
  );
}

// ==================================================
// Docker runtime executor
// ==================================================

export class DockerRuntimeExecutor
  implements RuntimeExecutor
{
  async probe(): Promise<RuntimeCapabilities> {
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
        return {
          available: false,
          runtimeType: "docker",
          supportsResourceLimits: false,
          supportsNetworkIsolation: false,
          supportsStrongIsolation: false,
        };
      }

      return {
        available: true,
        runtimeType: "docker",
        supportsResourceLimits: true,
        supportsNetworkIsolation: true,
        supportsStrongIsolation: false,
      };
    } catch {
      return {
        available: false,
        runtimeType: "docker",
        supportsResourceLimits: false,
        supportsNetworkIsolation: false,
        supportsStrongIsolation: false,
      };
    }
  }

  async inspectImage(
    imageId: string,
  ): Promise<ImageVerification> {
    assertImageId(imageId);

    const result = await runProcess({
      args: [
        "image",
        "inspect",
        "--format",
        "{{json .}}",
        "--",
        imageId,
      ],
      timeoutMs: INSPECT_TIMEOUT_MS,
    });

    if (result.code !== 0) {
      if (
        isMissingContainer(result) ||
        /no such image/iu.test(result.stderr)
      ) {
        return { exists: false, imageId: null };
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

    const id =
      typeof parsed === "object" &&
      parsed !== null &&
      "Id" in parsed &&
      typeof (parsed as { Id: unknown }).Id === "string"
        ? (parsed as { Id: string }).Id
        : null;

    if (!id || !IMAGE_ID_PATTERN.test(id)) {
      throw new RuntimeDeploymentError(
        "IMAGE_UNAVAILABLE",
        "The image identifier is invalid.",
      );
    }

    return { exists: true, imageId: id };
  }

  async ensureNetwork(
    name: string,
    labels: Record<string, string>,
  ): Promise<boolean> {
    if (
      typeof name !== "string" ||
      name.length === 0 ||
      name.length > 128 ||
      name.includes("\0")
    ) {
      throw invalidConfiguration(
        "The managed network name is invalid.",
      );
    }

    const inspect = await runProcess({
      args: ["network", "inspect", "--", name],
      timeoutMs: INSPECT_TIMEOUT_MS,
    });

    if (inspect.code === 0) {
      return true;
    }

    const args: string[] = [
      "network",
      "create",
      "--driver",
      "bridge",
    ];

    for (const [key, value] of Object.entries(labels)) {
      args.push("--label", `${key}=${value}`);
    }

    args.push("--", name);

    const created = await runProcess({
      args,
      timeoutMs: CREATE_TIMEOUT_MS,
    });

    if (created.code !== 0) {
      // A concurrent create is acceptable if the network
      // now exists.
      const recheck = await runProcess({
        args: ["network", "inspect", "--", name],
        timeoutMs: INSPECT_TIMEOUT_MS,
      });

      if (recheck.code === 0) {
        return true;
      }

      throw unavailable(
        "The managed runtime network could not be created.",
      );
    }

    return true;
  }

  async createContainer(
    request: CreateContainerRequest,
  ): Promise<CreatedContainer> {
    assertNotCancelled(request.signal);

    assertImageId(request.imageId);

    assertManagedContainerName(request.name);

    const envFile = await writeEnvFile(request.env);

    try {
      const args: string[] = [
        "create",
        "--name",
        request.name,
        "--network",
        request.networkName,
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
        "--label",
        `cloudpilot.managed=true`,
      ];

      for (const [key, value] of Object.entries(
        request.labels,
      )) {
        if (key === "cloudpilot.managed") {
          continue;
        }

        args.push("--label", `${key}=${value}`);
      }

      args.push("--env-file", envFile.path);

      if (request.hostPort > 0) {
        args.push(
          "--publish",
          `${request.bindHost}:${request.hostPort}:${request.containerPort}`,
        );
      } else {
        args.push(
          "--publish",
          `${request.bindHost}::${request.containerPort}`,
        );
      }

      // Immutable image id, never a mutable tag.
      args.push("--", request.imageId);

      const result = await runProcess({
        args,
        timeoutMs: CREATE_TIMEOUT_MS,
        signal: request.signal,
      });

      assertNotCancelled(request.signal);

      if (result.code !== 0) {
        throw new RuntimeDeploymentError(
          "CONTAINER_CREATION_FAILED",
          "The container could not be created.",
        );
      }

      if (result.outputTruncated) {
        throw new RuntimeDeploymentError(
          "CONTAINER_CREATION_FAILED",
          "Container creation output exceeded its limit.",
        );
      }

      const containerId = result.stdout.trim();

      if (!containerId) {
        throw new RuntimeDeploymentError(
          "CONTAINER_CREATION_FAILED",
          "The container identifier was not returned.",
        );
      }

      const inspection = await this.inspectContainerPort(
        request.name,
        request.containerPort,
      );

      const hostPort =
        inspection.hostPort ?? request.hostPort;

      return {
        containerId,
        containerName: request.name,
        hostPort,
        networkName: request.networkName,
      };
    } finally {
      await envFile.cleanup();
    }
  }

  async startContainer(
    name: string,
    signal: AbortSignal,
  ): Promise<void> {
    assertManagedContainerName(name);

    const result = await runProcess({
      args: ["start", "--", name],
      timeoutMs: START_TIMEOUT_MS,
      signal,
    });

    assertNotCancelled(signal);

    if (result.code !== 0) {
      throw new RuntimeDeploymentError(
        "CONTAINER_START_FAILED",
        "The container could not be started.",
      );
    }
  }

  async inspectContainer(
    name: string,
  ): Promise<ContainerInspection> {
    return this.inspectContainerPort(name, 0);
  }

  private async inspectContainerPort(
    name: string,
    containerPort: number,
  ): Promise<ContainerInspection> {
    assertManagedContainerName(name);

    const result = await runProcess({
      args: [
        "inspect",
        "--format",
        "{{json .}}",
        "--",
        name,
      ],
      timeoutMs: INSPECT_TIMEOUT_MS,
    });

    if (result.code !== 0) {
      if (isMissingContainer(result)) {
        return missingContainer();
      }

      throw new RuntimeDeploymentError(
        "CONTAINER_EXECUTION_FAILED",
        "The container could not be inspected.",
      );
    }

    if (result.outputTruncated) {
      throw new RuntimeDeploymentError(
        "CONTAINER_EXECUTION_FAILED",
        "Container inspection output exceeded its limit.",
      );
    }

    return parseInspection(
      result.stdout,
      name,
      containerPort,
    );
  }

  async stopContainer(
    name: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<void> {
    assertManagedContainerName(name);

    const graceSeconds = Math.max(
      1,
      Math.ceil(timeoutMs / 1000),
    );

    const result = await runProcess({
      args: [
        "stop",
        "--time",
        String(graceSeconds),
        "--",
        name,
      ],
      timeoutMs: timeoutMs + 10_000,
      signal,
    });

    assertNotCancelled(signal);

    if (
      result.code !== 0 &&
      !isMissingContainer(result)
    ) {
      throw new RuntimeDeploymentError(
        "CONTAINER_EXECUTION_FAILED",
        "The container could not be stopped.",
      );
    }
  }

  async restartContainer(
    name: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<void> {
    assertManagedContainerName(name);

    const graceSeconds = Math.max(
      1,
      Math.ceil(timeoutMs / 1000),
    );

    const result = await runProcess({
      args: [
        "restart",
        "--time",
        String(graceSeconds),
        "--",
        name,
      ],
      timeoutMs: timeoutMs + 10_000,
      signal,
    });

    assertNotCancelled(signal);

    if (
      result.code !== 0 &&
      !isMissingContainer(result)
    ) {
      throw new RuntimeDeploymentError(
        "CONTAINER_EXECUTION_FAILED",
        "The container could not be restarted.",
      );
    }
  }

  async removeContainer(name: string): Promise<void> {
    assertManagedContainerName(name);

    const result = await runProcess({
      args: ["rm", "--force", "--", name],
      timeoutMs: REMOVE_TIMEOUT_MS,
    });

    if (
      result.code !== 0 &&
      !isMissingContainer(result)
    ) {
      throw new RuntimeDeploymentError(
        "CONTAINER_EXECUTION_FAILED",
        "The container could not be removed safely.",
      );
    }
  }

  async logs(
    name: string,
    maxBytes: number,
  ): Promise<string> {
    assertManagedContainerName(name);

    if (
      !Number.isInteger(maxBytes) ||
      maxBytes <= 0
    ) {
      throw invalidConfiguration(
        "The log byte limit is invalid.",
      );
    }

    const result = await runProcess({
      args: [
        "logs",
        "--tail",
        "200",
        "--",
        name,
      ],
      timeoutMs: LOGS_TIMEOUT_MS,
    });

    if (
      result.code !== 0 &&
      !isMissingContainer(result)
    ) {
      return "";
    }

    const combined = `${result.stdout}${result.stderr}`;

    return Buffer.byteLength(combined, "utf8") >
      maxBytes
      ? combined.slice(0, maxBytes)
      : combined;
  }
}