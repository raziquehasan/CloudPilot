/**
 * CloudPilot — Container Runtime Executor Interface
 *
 * Abstracts the container runtime backend so orchestration is
 * not coupled to the Docker CLI or Engine internals.
 *
 * Implementations may target local development, dedicated
 * hosts, or future Kubernetes/cloud runtimes.
 *
 * This abstraction does not itself provide strong tenant
 * isolation. Untrusted workloads must remain disabled.
 *
 * Security principles:
 *
 * - Runtime operations must remain scoped to managed resources.
 * - Container names are not proof of resource ownership.
 * - Log collection must use immutable container identifiers.
 * - Runtime environment secrets must never be logged.
 * - Cancellation must propagate to underlying operations.
 * - Runtime capabilities must reflect actual enforcement.
 */

// ==================================================
// Runtime capabilities
// ==================================================

export interface RuntimeCapabilities {
  /**
   * Whether the runtime is available and usable.
   */
  available: boolean;

  /**
   * Runtime implementation identifier.
   *
   * Examples: docker, containerd, kubernetes.
   */
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
   *
   * Must remain false unless strong isolation has actually
   * been established and verified.
   */
  supportsStrongIsolation: boolean;
}

// ==================================================
// Image verification
// ==================================================

export interface ImageVerification {
  exists: boolean;

  /**
   * Immutable image identifier returned by the runtime.
   *
   * Null when the image cannot be verified.
   */
  imageId: string | null;
}

// ==================================================
// Container creation
// ==================================================

export interface CreateContainerRequest {
  /**
   * Immutable image identifier (sha256:...).
   *
   * Container creation must use the verified image identity
   * rather than relying solely on a mutable image tag.
   */
  imageId: string;

  /** Managed image tag, verified before creation. */
  imageTag: string;

  /** Deterministic managed container name. */
  name: string;

  /**
   * Ownership labels.
   *
   * Implementations must preserve the supplied ownership
   * metadata on the created resource.
   */
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
   *
   * Implementations must also avoid exposing these values
   * through diagnostics, command errors, or temporary files
   * that are accessible to unrelated users.
   */
  env: Readonly<Record<string, string>>;

  /** Requested CPU limit. */
  cpuLimit: number;

  /** Requested memory limit in megabytes. */
  memoryLimitMb: number;

  /** Requested process limit. */
  pidsLimit: number;

  /**
   * Cancellation signal for container creation.
   *
   * Cancellation does not guarantee that a runtime-side
   * operation has been rolled back. Callers must reconcile
   * uncertain outcomes before retrying.
   */
  signal: AbortSignal;
}

export interface CreatedContainer {
  /**
   * Immutable container identifier.
   */
  containerId: string;

  /**
   * Managed container name.
   */
  containerName: string;

  /**
   * Actual host port assigned by the runtime, when available.
   *
   * Zero may represent a dynamically requested port that
   * has not yet been resolved.
   *
   * Callers must inspect the running container to determine
   * its actual host port before readiness checks or exposing
   * a deployment endpoint.
   */
  hostPort: number;

  /**
   * Managed network associated with the container.
   */
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

  /**
   * Immutable identifier of the inspected container.
   *
   * Callers should compare this value against the expected
   * persisted container identity.
   */
  containerId: string | null;

  state: ContainerState;

  running: boolean;

  exitCode: number | null;

  /**
   * Runtime-reported start timestamp.
   */
  startedAt: string | null;

  /**
   * Actual published host port, when known.
   *
   * Null when no valid host port can be determined.
   */
  hostPort: number | null;

  /**
   * Immutable image identifier of the inspected container.
   */
  imageId: string | null;
}

// ==================================================
// Runtime log streaming contracts
// ==================================================

/**
 * Supported container output streams.
 *
 * Runtime implementations must not intentionally merge
 * stdout and stderr when separate streams are available.
 */
export type RuntimeLogStream =
  | "stdout"
  | "stderr";

/**
 * Synchronous consumer of raw container output.
 *
 * Chunks may contain:
 *
 * - Partial lines.
 * - Multiple lines.
 * - Partial UTF-8 sequences.
 * - Terminal escape sequences.
 * - Sensitive application output.
 *
 * Consumers must normalize and redact output before
 * persistence or publication.
 *
 * Implementations must not assume that one chunk
 * corresponds to one complete log record.
 */
export type RuntimeLogDataHandler = (
  stream: RuntimeLogStream,
  chunk: Buffer,
) => void;

export interface StreamLogsRequest {
  /**
   * Immutable container identifier.
   *
   * Mutable container names, prefixes, and ambiguous
   * identifiers must not be accepted for log attachment.
   */
  containerId: string;

  /**
   * RFC3339 timestamp for bounded historical recovery.
   *
   * When omitted or null, only new output should be
   * streamed.
   *
   * Implementations must explicitly enforce this behavior.
   * Runtime defaults must not be assumed to exclude
   * historical output.
   *
   * Historical recovery is best-effort and may contain
   * duplicate records after reconnection.
   */
  since?: string | null;

  /**
   * Whether to continue streaming after the available
   * historical output has been consumed.
   */
  follow: boolean;

  /**
   * Cancellation signal for the entire streaming operation.
   *
   * Implementations must observe cancellation before
   * attachment and throughout the streaming lifecycle.
   */
  signal: AbortSignal;

  /**
   * Receives raw output chunks.
   *
   * The callback is synchronous and does not provide
   * asynchronous backpressure.
   *
   * Consumers must maintain bounded buffering.
   * Implementations must avoid unbounded internal queues.
   *
   * Callback exceptions must trigger stream cleanup
   * rather than leave an orphaned streaming operation.
   */
  onData: RuntimeLogDataHandler;
}

// ==================================================
// Executor contract
// ==================================================

export interface RuntimeExecutor {
  /**
   * Detect runtime availability and isolation capabilities.
   *
   * Runtime availability alone does not establish
   * strong multi-tenant isolation.
   */
  probe(): Promise<RuntimeCapabilities>;

  /**
   * Verify an immutable image exists in the runtime image store.
   *
   * Implementations should return the actual verified image
   * identity, not merely echo the requested identifier.
   */
  inspectImage(
    imageId: string,
  ): Promise<ImageVerification>;

  /**
   * Ensure a managed bridge network exists. Idempotent.
   *
   * Returns true when the expected network is available.
   *
   * An existing network with the requested name is not
   * automatically proof of ownership. Implementations
   * should verify the expected network metadata.
   */
  ensureNetwork(
    name: string,
    labels: Record<string, string>,
  ): Promise<boolean>;

  /**
   * Create a container without starting it.
   *
   * The operation must remain scoped to the supplied
   * image, network, ownership labels, and resource limits.
   */
  createContainer(
    request: CreateContainerRequest,
  ): Promise<CreatedContainer>;

  /**
   * Start a created container.
   *
   * Legacy name-based contract retained for compatibility.
   *
   * Callers must verify ownership and expected container
   * identity before invoking lifecycle operations.
   */
  startContainer(
    name: string,
    signal: AbortSignal,
  ): Promise<void>;

  /**
   * Inspect a managed container.
   *
   * Callers must verify the returned immutable identity
   * before relying on inspection results.
   */
  inspectContainer(
    name: string,
  ): Promise<ContainerInspection>;

  /**
   * Stop a running container within a bounded time.
   *
   * Legacy name-based contract retained for compatibility.
   *
   * Cancellation may leave the final runtime state
   * uncertain and require subsequent reconciliation.
   */
  stopContainer(
    name: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<void>;

  /**
   * Restart a managed container within a bounded time.
   *
   * Callers must establish ownership before invoking
   * the operation and reconcile the resulting state.
   */
  restartContainer(
    name: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<void>;

  /**
   * Remove a managed container. Scoped to a single name.
   *
   * Never runs global cleanup.
   *
   * Callers must verify ownership before removal.
   */
  removeContainer(
    name: string,
  ): Promise<void>;

  /**
   * Retrieve bounded container logs.
   *
   * maxBytes represents a byte limit, not a JavaScript
   * character count.
   *
   * Returned output is potentially sensitive and must
   * be normalized and redacted before exposure.
   */
  logs(
    name: string,
    maxBytes: number,
  ): Promise<string>;

  /**
   * Stream container logs.
   *
   * Security requirements:
   *
   * - Use the exact immutable container identifier.
   * - Never resolve a mutable container name.
   * - Reject invalid or ambiguous identifiers.
   * - Respect cancellation before and during attachment.
   *
   * The caller is responsible for verifying ownership
   * before attaching.
   *
   * Lifecycle requirements:
   *
   * - Resolve when streaming completes or cancellation
   *   has been handled.
   * - Reject operational failures.
   * - Clean up underlying processes, listeners, and
   *   other streaming resources.
   * - Do not deliver additional callbacks after the
   *   returned promise has settled.
   * - Stop delivering output when cancellation is
   *   observed.
   *
   * Resource requirements:
   *
   * - Avoid unbounded output accumulation.
   * - Deliver output incrementally.
   * - Bound historical recovery according to runtime
   *   capabilities and implementation limits.
   *
   * Delivery guarantees:
   *
   * Streaming is best-effort. Reconnection may produce
   * duplicates or gaps. This contract does not guarantee
   * exactly-once delivery.
   */
  streamLogs(
    request: StreamLogsRequest,
  ): Promise<void>;

  /**
   * Read the ownership labels of a container by immutable ID.
   *
   * Returns null when the container is absent or cannot
   * be inspected.
   *
   * Fail-closed: callers must not attach when ownership
   * cannot be established.
   *
   * Implementations must:
   *
   * - Inspect the exact immutable container identifier.
   * - Avoid fallback to mutable names.
   * - Verify the returned container identity.
   * - Return only container label metadata.
   * - Avoid exposing unrelated inspection data, including
   *   decrypted runtime environment variables.
   *
   * Label retrieval alone does not establish ownership.
   * Callers must compare the required ownership labels
   * against the expected deployment and runtime identity.
   */
  readContainerLabels(
    containerId: string,
  ): Promise<Record<string, string> | null>;
}