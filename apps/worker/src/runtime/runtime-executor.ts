/**
 * CloudPilot — Container Runtime Executor Interface
 *
 * Abstracts the container runtime backend so orchestration is
 * not coupled to the Docker CLI or Engine internals.
 * Implementations may target local development, dedicated
 * hosts, or future Kubernetes/cloud runtimes.
 *
 * This abstraction does not itself provide strong tenant
 * isolation. Untrusted workloads must remain disabled.
 */

// ==================================================
// Runtime capabilities
// ==================================================

export interface RuntimeCapabilities {
  available: boolean;

  runtimeType: string;

  /**
   * Whether the runtime can enforce per-container CPU/memory/
   * PID limits.
   */
  supportsResourceLimits: boolean;

  /**
   * Whether the runtime can enforce network isolation between
   * workloads.
   */
  supportsNetworkIsolation: boolean;

  /**
   * Whether the runtime provides strong multi-tenant isolation
   * suitable for untrusted workloads.
   */
  supportsStrongIsolation: boolean;
}

// ==================================================
// Image verification
// ==================================================

export interface ImageVerification {
  exists: boolean;

  imageId: string | null;
}

// ==================================================
// Container creation
// ==================================================

export interface CreateContainerRequest {
  /** Immutable image identifier (sha256:...). */
  imageId: string;

  /** Managed image tag, verified before creation. */
  imageTag: string;

  /** Deterministic managed container name. */
  name: string;

  /** Ownership labels. */
  labels: Record<string, string>;

  /** Managed bridge network name. */
  networkName: string;

  /** Application container port. */
  containerPort: number;

  /**
   * Requested host port. Zero requests dynamic allocation
   * by the runtime.
   */
  hostPort: number;

  /** Loopback bind address. */
  bindHost: string;

  /**
   * Decrypted runtime environment variables.
   *
   * Passed to the runtime out-of-band (never on a command
   * line). The executor must not log these values.
   */
  env: Readonly<Record<string, string>>;

  cpuLimit: number;

  memoryLimitMb: number;

  pidsLimit: number;

  signal: AbortSignal;
}

export interface CreatedContainer {
  containerId: string;

  containerName: string;

  /** Actual host port assigned by the runtime. */
  hostPort: number;

  networkName: string;
}

// ==================================================
// Container inspection
// ==================================================

export type ContainerState =
  | "created"
  | "running"
  | "exited"
  | "restarting"
  | "paused"
  | "dead"
  | "unknown";

export interface ContainerInspection {
  exists: boolean;

  containerId: string | null;

  state: ContainerState;

  running: boolean;

  exitCode: number | null;

  startedAt: string | null;

  hostPort: number | null;

  imageId: string | null;
}

// ==================================================
// Executor contract
// ==================================================

export interface RuntimeExecutor {
  /**
   * Detect runtime availability and isolation capabilities.
   */
  probe(): Promise<RuntimeCapabilities>;

  /**
   * Verify an immutable image exists in the runtime image store.
   */
  inspectImage(
    imageId: string,
  ): Promise<ImageVerification>;

  /**
   * Ensure a managed bridge network exists. Idempotent.
   * Returns true when the network is available.
   */
  ensureNetwork(
    name: string,
    labels: Record<string, string>,
  ): Promise<boolean>;

  /**
   * Create a container without starting it.
   */
  createContainer(
    request: CreateContainerRequest,
  ): Promise<CreatedContainer>;

  /**
   * Start a created container.
   */
  startContainer(
    name: string,
    signal: AbortSignal,
  ): Promise<void>;

  /**
   * Inspect a managed container.
   */
  inspectContainer(
    name: string,
  ): Promise<ContainerInspection>;

  /**
   * Stop a running container within a bounded time.
   */
  stopContainer(
    name: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<void>;

  /**
   * Restart a managed container within a bounded time.
   */
  restartContainer(
    name: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<void>;

  /**
   * Remove a managed container. Scoped to a single name.
   * Never runs global cleanup.
   */
  removeContainer(name: string): Promise<void>;

  /**
   * Retrieve bounded container logs.
   */
  logs(name: string, maxBytes: number): Promise<string>;
}