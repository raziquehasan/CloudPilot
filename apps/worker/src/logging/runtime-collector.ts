
/**
 * CloudPilot — Runtime Log Collector
 *
 * Phase 5.6 — Real-Time Deployment Logs & Streaming Engine
 *
 * Secure and bounded collection of container logs for a
 * single runtime instance.
 *
 * Security and coordination:
 *
 * - Attaches using an immutable Docker container ID.
 * - Verifies managed ownership labels before streaming.
 * - Uses a dedicated Redis ownership lease.
 * - Tracks conservative local lease expiration.
 * - Serializes lease renewal operations.
 * - Stops ingestion after ownership loss or cancellation.
 * - Uses independent stateful redactors for each stream.
 * - Supports registered deployment-specific secrets.
 * - Uses an explicit bounded historical recovery timestamp.
 * - Coordinates initialization, streaming and shutdown.
 * - Never modifies or removes the container.
 *
 * Important limitations:
 *
 * Docker logs do not expose a reliable unique event ID.
 * Historical replay can therefore produce duplicate
 * logical records.
 *
 * Redis ownership leases do not provide durable database
 * fencing. A failover or suspended process may permit
 * temporary overlapping collectors.
 *
 * The runtime log manager owns reconnection policy,
 * runtime-instance replacement detection and collector
 * recreation.
 *
 * This collector owns its redactors and, by default,
 * its deployment log writer.
 */

import {
  performance,
} from "node:perf_hooks";

import {
  runtimeOwnershipLabels,
} from "@cloudpilot/queue";

import type {
  RuntimeExecutor,
} from "../runtime/runtime-executor.js";

import type {
  DistributedLock,
} from "./collector-lock.js";

import type {
  DeploymentLogWriter,
} from "./log-writer.js";

import {
  LogNormalizer,
} from "./normalize.js";

import {
  SecretRedactor,
} from "./redact.js";

import {
  logMetrics,
} from "./metrics.js";

// ==================================================
// Default configuration
// ==================================================

const DEFAULT_MAX_HISTORICAL_MS =
  5 * 60_000;

const DEFAULT_LOCK_TTL_MS =
  15_000;

const DEFAULT_RENEW_INTERVAL_MS =
  5_000;

const DEFAULT_ACQUIRE_ATTEMPTS =
  3;

const DEFAULT_ACQUIRE_DELAY_MS =
  500;

// ==================================================
// Hard limits
// ==================================================

const MIN_LOCK_TTL_MS =
  1_000;

const MAX_LOCK_TTL_MS =
  10 * 60_000;

const MIN_RENEW_INTERVAL_MS =
  100;

const MAX_HISTORICAL_MS =
  60 * 60_000;

const MAX_ACQUIRE_ATTEMPTS =
  10;

const MAX_ACQUIRE_DELAY_MS =
  10_000;

const MIN_LINE_BYTES =
  64;

const MAX_LINE_BYTES =
  16 * 1024;

const MAX_REGISTERED_SECRETS =
  64;

const MAX_SECRET_BYTES =
  4_096;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

const CONTAINER_ID_PATTERN =
  /^[0-9a-f]{64}$/u;

const OWNERSHIP_LOST_REASON =
  "Runtime log collection ownership was lost.";

// ==================================================
// Public contracts
// ==================================================

export interface RuntimeLogCollectorOptions {
  workspaceId: string;

  projectId: string;

  deploymentId: string;

  runtimeInstanceId: string;

  containerId: string;

  executor: RuntimeExecutor;

  writer: DeploymentLogWriter;

  lock: DistributedLock;

  lockTtlMs?: number;

  renewIntervalMs?: number;

  maxHistoricalMs?: number;

  acquireAttempts?: number;

  acquireDelayMs?: number;

  maxLineBytes?: number;

  signal: AbortSignal;

  now?: () => Date;

  sleep?: (ms: number) => Promise<void>;

  onOwnershipLost?: (
    reason: string,
  ) => void;

  /**
   * Known deployment-specific secret values.
   *
   * These values are registered in each independent
   * stream redactor.
   *
   * Never log this array or include its values in
   * exception messages.
   */

  registeredSecrets?: readonly string[];

  /**
   * Optional immutable runtime attempt identity.
   *
   * When supplied, the corresponding Docker ownership
   * label must also match.
   */

  attempt?: number;

  /**
   * Whether this collector owns the writer lifecycle.
   *
   * Dedicated writer: true (default).
   * Shared writer: false.
   */

  ownsWriter?: boolean;
}

// ==================================================
// Internal contracts
// ==================================================

type CollectorState =
  | "IDLE"
  | "STARTING"
  | "RUNNING"
  | "STOPPING"
  | "STOPPED";

interface ResolvedCollectorOptions {
  workspaceId: string;

  projectId: string;

  deploymentId: string;

  runtimeInstanceId: string;

  containerId: string;

  executor: RuntimeExecutor;

  writer: DeploymentLogWriter;

  lock: DistributedLock;

  lockTtlMs: number;

  renewIntervalMs: number;

  maxHistoricalMs: number;

  acquireAttempts: number;

  acquireDelayMs: number;

  maxLineBytes: number;

  signal: AbortSignal;

  now: () => Date;

  sleep?: (ms: number) => Promise<void>;

  onOwnershipLost?: (
    reason: string,
  ) => void;

  attempt?: number;

  ownsWriter: boolean;
}

interface StreamRedactors {
  stdout: SecretRedactor;

  stderr: SecretRedactor;

  system: SecretRedactor;
}

/**
 * Optional local ownership information exposed by
 * RedisCollectorLock.
 *
 * Existing DistributedLock implementations remain
 * compatible with the original interface.
 */

type ObservableDistributedLock =
  DistributedLock & {
    readonly isHeld?: boolean;

    readonly remainingTtlMs?: number;
  };

// ==================================================
// Configuration validation
// ==================================================

function assertInteger(
  value: number,
  name: string,
  minimum: number,
  maximum: number,
): void {
  if (
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    throw new RangeError(
      `Invalid runtime collector ${name}.`,
    );
  }
}

function normalizeUuid(
  value: string,
  name: string,
): string {
  if (
    typeof value !== "string" ||
    value.length !== 36 ||
    !UUID_PATTERN.test(value)
  ) {
    throw new TypeError(
      `Invalid runtime collector ${name}.`,
    );
  }

  return value.toLowerCase();
}

function normalizeContainerId(
  value: string,
): string {
  if (
    typeof value !== "string" ||
    value.length !== 64 ||
    !CONTAINER_ID_PATTERN.test(value)
  ) {
    throw new TypeError(
      "Invalid immutable container ID.",
    );
  }

  return value;
}

function resolveOptions(
  options: RuntimeLogCollectorOptions,
): ResolvedCollectorOptions {
  const lockTtlMs =
    options.lockTtlMs ??
    DEFAULT_LOCK_TTL_MS;

  assertInteger(
    lockTtlMs,
    "lockTtlMs",
    MIN_LOCK_TTL_MS,
    MAX_LOCK_TTL_MS,
  );

  const renewIntervalMs =
    options.renewIntervalMs ??
    Math.min(
      DEFAULT_RENEW_INTERVAL_MS,
      Math.floor(lockTtlMs / 3),
    );

  const maxHistoricalMs =
    options.maxHistoricalMs ??
    DEFAULT_MAX_HISTORICAL_MS;

  const acquireAttempts =
    options.acquireAttempts ??
    DEFAULT_ACQUIRE_ATTEMPTS;

  const acquireDelayMs =
    options.acquireDelayMs ??
    DEFAULT_ACQUIRE_DELAY_MS;

  const maxLineBytes =
    options.maxLineBytes ??
    MAX_LINE_BYTES;

  assertInteger(
    renewIntervalMs,
    "renewIntervalMs",
    MIN_RENEW_INTERVAL_MS,
    Math.floor(lockTtlMs / 3),
  );

  assertInteger(
    maxHistoricalMs,
    "maxHistoricalMs",
    0,
    MAX_HISTORICAL_MS,
  );

  assertInteger(
    acquireAttempts,
    "acquireAttempts",
    1,
    MAX_ACQUIRE_ATTEMPTS,
  );

  assertInteger(
    acquireDelayMs,
    "acquireDelayMs",
    0,
    MAX_ACQUIRE_DELAY_MS,
  );

  assertInteger(
    maxLineBytes,
    "maxLineBytes",
    MIN_LINE_BYTES,
    MAX_LINE_BYTES,
  );

  if (options.attempt !== undefined) {
    assertInteger(
      options.attempt,
      "attempt",
      1,
      2_147_483_647,
    );
  }

  if (
    !options.executor ||
    typeof options.executor.streamLogs !==
      "function" ||
    typeof options.executor.readContainerLabels !==
      "function"
  ) {
    throw new TypeError(
      "Invalid runtime log executor.",
    );
  }

  if (
    !options.writer ||
    typeof options.writer.enqueue !==
      "function" ||
    typeof options.writer.flush !==
      "function" ||
    typeof options.writer.stop !==
      "function"
  ) {
    throw new TypeError(
      "Invalid deployment log writer.",
    );
  }

  if (
    !options.lock ||
    typeof options.lock.acquire !==
      "function" ||
    typeof options.lock.renew !==
      "function" ||
    typeof options.lock.release !==
      "function"
  ) {
    throw new TypeError(
      "Invalid collector ownership lock.",
    );
  }

  if (
    !(options.signal instanceof AbortSignal)
  ) {
    throw new TypeError(
      "Invalid collector abort signal.",
    );
  }

  if (
    options.now !== undefined &&
    typeof options.now !== "function"
  ) {
    throw new TypeError(
      "Invalid collector clock.",
    );
  }

  if (
    options.sleep !== undefined &&
    typeof options.sleep !== "function"
  ) {
    throw new TypeError(
      "Invalid collector sleep function.",
    );
  }

  if (
    options.onOwnershipLost !== undefined &&
    typeof options.onOwnershipLost !==
      "function"
  ) {
    throw new TypeError(
      "Invalid ownership loss callback.",
    );
  }

  if (
    options.ownsWriter !== undefined &&
    typeof options.ownsWriter !== "boolean"
  ) {
    throw new TypeError(
      "Invalid writer ownership configuration.",
    );
  }

  return {
    workspaceId: normalizeUuid(
      options.workspaceId,
      "workspaceId",
    ),

    projectId: normalizeUuid(
      options.projectId,
      "projectId",
    ),

    deploymentId: normalizeUuid(
      options.deploymentId,
      "deploymentId",
    ),

    runtimeInstanceId: normalizeUuid(
      options.runtimeInstanceId,
      "runtimeInstanceId",
    ),

    containerId: normalizeContainerId(
      options.containerId,
    ),

    executor: options.executor,

    writer: options.writer,

    lock: options.lock,

    lockTtlMs,

    renewIntervalMs,

    maxHistoricalMs,

    acquireAttempts,

    acquireDelayMs,

    maxLineBytes,

    signal: options.signal,

    now:
      options.now ??
      (() => new Date()),

    ...(options.sleep !== undefined
      ? { sleep: options.sleep }
      : {}),

    ...(options.onOwnershipLost !== undefined
      ? {
          onOwnershipLost:
            options.onOwnershipLost,
        }
      : {}),

    ...(options.attempt !== undefined
      ? { attempt: options.attempt }
      : {}),

    ownsWriter:
      options.ownsWriter ?? true,
  };
}

// ==================================================
// Abort-aware retry delay
// ==================================================

function abortableDelay(
  ms: number,
  signal: AbortSignal,
): Promise<boolean> {
  if (signal.aborted) {
    return Promise.resolve(false);
  }

  return new Promise<boolean>(
    (resolve) => {
      let settled = false;

      let timer:
        | ReturnType<typeof setTimeout>
        | undefined;

      const finish = (
        completed: boolean,
      ): void => {
        if (settled) {
          return;
        }

        settled = true;

        if (timer !== undefined) {
          clearTimeout(timer);
        }

        signal.removeEventListener(
          "abort",
          onAbort,
        );

        resolve(completed);
      };

      const onAbort = (): void => {
        finish(false);
      };

      signal.addEventListener(
        "abort",
        onAbort,
        { once: true },
      );

      if (signal.aborted) {
        finish(false);

        return;
      }

      timer = setTimeout(
        () => finish(true),
        ms,
      );
    },
  );
}

/**
 * Supports an injected test sleep implementation
 * while still allowing collector cancellation.
 *
 * Cancellation does not terminate the underlying
 * caller-owned sleep operation.
 */

async function waitForRetry(
  ms: number,
  signal: AbortSignal,
  sleep?: (
    ms: number,
  ) => Promise<void>,
): Promise<boolean> {
  if (!sleep) {
    return abortableDelay(
      ms,
      signal,
    );
  }

  if (signal.aborted) {
    return false;
  }

  return new Promise<boolean>(
    (resolve) => {
      let settled = false;

      const finish = (
        completed: boolean,
      ): void => {
        if (settled) {
          return;
        }

        settled = true;

        signal.removeEventListener(
          "abort",
          onAbort,
        );

        resolve(completed);
      };

      const onAbort = (): void => {
        finish(false);
      };

      signal.addEventListener(
        "abort",
        onAbort,
        { once: true },
      );

      if (signal.aborted) {
        finish(false);

        return;
      }

      void Promise.resolve()
        .then(() => sleep(ms))
        .then(
          () => finish(!signal.aborted),
          () => finish(false),
        );
    },
  );
}

// ==================================================
// Independent stream redactors
// ==================================================

function createStreamRedactors(
  secrets: readonly string[],
): StreamRedactors {
  if (
    !Array.isArray(secrets) ||
    secrets.length >
      MAX_REGISTERED_SECRETS
  ) {
    throw new RangeError(
      "Invalid collector secret configuration.",
    );
  }

  const uniqueSecrets =
    new Set<string>();

  for (const secret of secrets) {
    if (
      typeof secret !== "string" ||
      secret.length < 4 ||
      secret.length >
        MAX_SECRET_BYTES ||
      Buffer.byteLength(
        secret,
        "utf8",
      ) > MAX_SECRET_BYTES ||
      secret.trim().length === 0 ||
      /[\u0000\r\n]/u.test(secret)
    ) {
      throw new TypeError(
        "Unsupported collector secret value.",
      );
    }

    uniqueSecrets.add(secret);
  }

  const redactors: StreamRedactors = {
    stdout: new SecretRedactor(),

    stderr: new SecretRedactor(),

    system: new SecretRedactor(),
  };

  try {
    for (const redactor of Object.values(
      redactors,
    )) {
      for (const secret of uniqueSecrets) {
        const registered =
          redactor.register(secret);

        if (!registered) {
          throw new Error(
            "Collector secret registration failed.",
          );
        }
      }
    }

    return redactors;
  } catch {
    for (const redactor of Object.values(
      redactors,
    )) {
      redactor.dispose();
    }

    throw new Error(
      "Collector redaction initialization failed.",
    );
  } finally {
    uniqueSecrets.clear();
  }
}

// ==================================================
// Runtime log collector
// ==================================================

export class RuntimeLogCollector {
  private readonly o:
    ResolvedCollectorOptions;

  private readonly controller:
    AbortController;

  private readonly signal:
    AbortSignal;

  private readonly redactors:
    StreamRedactors;

  private normalizer:
    LogNormalizer | null = null;

  private renewTimer:
    | ReturnType<typeof setTimeout>
    | undefined;

  private expiryTimer:
    | ReturnType<typeof setTimeout>
    | undefined;

  private renewPromise:
    Promise<void> | null = null;

  private streamPromise:
    Promise<void> | null = null;

  private startPromise:
    Promise<boolean> | null = null;

  private finishPromise:
    Promise<void> | null = null;

  private stopPromise:
    Promise<void> | null = null;

  private state: CollectorState =
    "IDLE";

  private lockAcquired = false;

  private ownershipLost = false;

  private activeMetricRegistered = false;

  private leaseExpiresAt = 0;

  private acceptData = false;

  private finalized = false;

  private redactorsDisposed = false;

  constructor(
    options: RuntimeLogCollectorOptions,
  ) {
    this.o =
      resolveOptions(options);

    this.controller =
      new AbortController();

    this.signal =
      AbortSignal.any([
        this.o.signal,
        this.controller.signal,
      ]);

    this.redactors =
      createStreamRedactors(
        options.registeredSecrets ?? [],
      );
  }

  // ================================================
  // Public state
  // ================================================

  get isRunning(): boolean {
    return (
      this.state === "RUNNING" &&
      this.acceptData &&
      !this.signal.aborted &&
      this.hasLocalOwnership()
    );
  }

  get hasLostOwnership(): boolean {
    return this.ownershipLost;
  }

  // ================================================
  // Local ownership validation
  // ================================================

  private hasLocalOwnership(): boolean {
    if (
      !this.lockAcquired ||
      this.ownershipLost ||
      performance.now() >=
        this.leaseExpiresAt
    ) {
      return false;
    }

    const lock =
      this.o.lock as ObservableDistributedLock;

    try {
      if (lock.isHeld === false) {
        return false;
      }

      if (
        typeof lock.remainingTtlMs ===
          "number" &&
        (
          !Number.isFinite(
            lock.remainingTtlMs,
          ) ||
          lock.remainingTtlMs <= 0
        )
      ) {
        return false;
      }

      return true;
    } catch {
      return false;
    }
  }

  private updateLeaseDeadline(
    startedAt: number,
  ): void {
    let deadline =
      startedAt + this.o.lockTtlMs;

    const lock =
      this.o.lock as ObservableDistributedLock;

    const remaining =
      lock.remainingTtlMs;

    if (
      typeof remaining === "number" &&
      Number.isFinite(remaining)
    ) {
      deadline = Math.min(
        deadline,
        performance.now() +
          Math.max(0, remaining),
      );
    }

    this.leaseExpiresAt =
      deadline;
  }

  // ================================================
  // Public initialization
  // ================================================

  /**
   * Concurrent start calls share initialization.
   *
   * Collector instances are single-use. A stopped
   * instance cannot be restarted.
   */

  async start(): Promise<boolean> {
    if (this.startPromise) {
      if (
        this.state === "STARTING"
      ) {
        return this.startPromise;
      }

      return false;
    }

    if (
      this.state !== "IDLE" ||
      this.signal.aborted
    ) {
      await this.finish();

      return false;
    }

    this.state = "STARTING";

    this.signal.addEventListener(
      "abort",
      this.handleAbort,
      { once: true },
    );

    this.startPromise =
      this.startInternal();

    return this.startPromise;
  }

  private async startInternal():
    Promise<boolean> {
    let startedSuccessfully = false;

    try {
      const acquired =
        await this.acquire();

      if (
        !acquired ||
        this.signal.aborted ||
        this.state !== "STARTING"
      ) {
        return false;
      }

      /**
       * Start ownership monitoring before container
       * verification, which may involve a slow Docker
       * command.
       */

      this.startRenewal();

      this.scheduleExpiryCheck();

      const verified =
        await this.verifyOwnership();

      if (
        !verified ||
        this.signal.aborted ||
        this.state !== "STARTING" ||
        !this.hasLocalOwnership()
      ) {
        return false;
      }

      this.normalizer =
        new LogNormalizer({
          source: "RUNTIME",

          // Compatibility fallback. All supported
          // streams have dedicated redactors below.

          redactor:
            this.redactors.system,

          streamRedactors: {
            stdout:
              this.redactors.stdout,

            stderr:
              this.redactors.stderr,

            system:
              this.redactors.system,
          },

          maxLineBytes:
            this.o.maxLineBytes,

          now: this.o.now,

          onRecord: (record) => {
            /**
             * Recheck ownership at the record
             * boundary, not only the chunk boundary.
             */

            if (
              !this.acceptData ||
              this.signal.aborted ||
              !this.hasLocalOwnership()
            ) {
              if (
                !this.signal.aborted &&
                !this.hasLocalOwnership()
              ) {
                this.loseOwnership();
              }

              return;
            }

            this.o.writer.enqueue(
              record,
            );

            /**
             * Do not increment redactedEntries here.
             *
             * A normalized record does not expose
             * whether secret redaction occurred.
             * Accurate instrumentation belongs at
             * the actual redaction boundary.
             */
          },
        });

      const since =
        this.computeSince();

      if (
        this.signal.aborted ||
        !this.hasLocalOwnership()
      ) {
        return false;
      }

      this.state = "RUNNING";

      this.acceptData = true;

      logMetrics.incrementGauge(
        "activeCollectors",
        1,
      );

      this.activeMetricRegistered =
        true;

      /**
       * Schedule stream startup after the promise
       * reference has been assigned.
       *
       * Synchronous executor failures are converted
       * into rejected promises.
       */

      this.streamPromise =
        Promise.resolve()
          .then(async () => {
            if (
              this.signal.aborted ||
              !this.hasLocalOwnership()
            ) {
              return;
            }

            await this.o.executor.streamLogs({
              containerId:
                this.o.containerId,

              since,

              follow: true,

              signal:
                this.signal,

              onData: (
                stream,
                chunk,
              ) => {
                this.handleData(
                  stream,
                  chunk,
                );
              },
            });
          })
          .catch(() => {
            /**
             * Stream failure ends this collector.
             *
             * The manager decides whether to create
             * another collector with bounded replay.
             *
             * Never expose raw Docker errors here.
             */
          })
          .finally(async () => {
            await this.finish();
          });

      startedSuccessfully = true;

      return true;
    } catch {
      return false;
    } finally {
      if (!startedSuccessfully) {
        await this.finish();
      }
    }
  }

  // ================================================
  // Abort handling
  // ================================================

  private readonly handleAbort =
    (): void => {
      this.acceptData = false;

      this.clearRenewTimer();

      this.clearExpiryTimer();
    };

  // ================================================
  // Stream ingestion
  // ================================================

  private handleData(
    stream:
      | "stdout"
      | "stderr"
      | "system",

    chunk: Buffer | string,
  ): void {
    if (
      this.state !== "RUNNING" ||
      !this.acceptData ||
      this.signal.aborted
    ) {
      return;
    }

    if (!this.hasLocalOwnership()) {
      this.loseOwnership();

      return;
    }

    try {
      this.normalizer?.push(
        stream,
        chunk,
      );
    } catch {
      /**
       * Fail closed on unexpected normalization
       * errors instead of forwarding raw output.
       */

      this.acceptData = false;

      this.controller.abort();
    }
  }

  // ================================================
  // Ownership acquisition
  // ================================================

  private async acquire():
    Promise<boolean> {
    for (
      let attempt = 0;
      attempt < this.o.acquireAttempts;
      attempt += 1
    ) {
      if (
        this.signal.aborted ||
        this.state !== "STARTING"
      ) {
        return false;
      }

      const startedAt =
        performance.now();

      let acquired = false;

      try {
        acquired =
          await this.o.lock.acquire();
      } catch {
        acquired = false;
      }

      if (acquired) {
        this.lockAcquired = true;

        this.updateLeaseDeadline(
          startedAt,
        );

        if (
          this.signal.aborted ||
          this.state !== "STARTING" ||
          !this.hasLocalOwnership()
        ) {
          await this.releaseOwnership();

          return false;
        }

        return true;
      }

      if (
        attempt + 1 <
        this.o.acquireAttempts
      ) {
        const completed =
          await waitForRetry(
            this.o.acquireDelayMs,
            this.signal,
            this.o.sleep,
          );

        if (!completed) {
          return false;
        }
      }
    }

    return false;
  }

  // ================================================
  // Docker identity verification
  // ================================================

  private async verifyOwnership():
    Promise<boolean> {
    if (
      this.signal.aborted ||
      !this.hasLocalOwnership()
    ) {
      return false;
    }

    try {
      const labels =
        await this.o.executor
          .readContainerLabels(
            this.o.containerId,
          );

      if (
        !labels ||
        this.signal.aborted ||
        !this.hasLocalOwnership()
      ) {
        return false;
      }

      const expected =
        runtimeOwnershipLabels;

      const matches =
        Object.hasOwn(
          labels,
          expected.managed,
        ) &&
        Object.hasOwn(
          labels,
          expected.workspace,
        ) &&
        Object.hasOwn(
          labels,
          expected.project,
        ) &&
        Object.hasOwn(
          labels,
          expected.deployment,
        ) &&
        Object.hasOwn(
          labels,
          expected.runtimeInstance,
        ) &&
        labels[expected.managed] ===
          "true" &&
        labels[expected.workspace] ===
          this.o.workspaceId &&
        labels[expected.project] ===
          this.o.projectId &&
        labels[expected.deployment] ===
          this.o.deploymentId &&
        labels[expected.runtimeInstance] ===
          this.o.runtimeInstanceId;

      if (!matches) {
        return false;
      }

      if (
        this.o.attempt !== undefined
      ) {
        if (
          !Object.hasOwn(
            labels,
            expected.attempt,
          ) ||
          labels[expected.attempt] !==
            String(this.o.attempt)
        ) {
          return false;
        }
      }

      return true;
    } catch {
      return false;
    }
  }

  // ================================================
  // Bounded historical recovery
  // ================================================

  private computeSince(): string {
    const now =
      this.o.now();

    if (
      !(now instanceof Date) ||
      !Number.isFinite(
        now.getTime(),
      )
    ) {
      throw new Error(
        "Invalid collector clock.",
      );
    }

    /**
     * Always supply an explicit timestamp.
     *
     * Zero historical recovery uses the current
     * observation time instead of omitting --since.
     */

    const since =
      new Date(
        now.getTime() -
          this.o.maxHistoricalMs,
      );

    if (
      !Number.isFinite(
        since.getTime(),
      )
    ) {
      throw new Error(
        "Invalid historical recovery timestamp.",
      );
    }

    return since.toISOString();
  }

  // ================================================
  // Renewal scheduling
  // ================================================

  private clearRenewTimer(): void {
    if (
      this.renewTimer !== undefined
    ) {
      clearTimeout(
        this.renewTimer,
      );

      this.renewTimer =
        undefined;
    }
  }

  private clearExpiryTimer(): void {
    if (
      this.expiryTimer !== undefined
    ) {
      clearTimeout(
        this.expiryTimer,
      );

      this.expiryTimer =
        undefined;
    }
  }

  private startRenewal(): void {
    this.clearRenewTimer();

    if (
      this.signal.aborted ||
      this.ownershipLost ||
      !this.lockAcquired ||
      this.state === "STOPPING" ||
      this.state === "STOPPED"
    ) {
      return;
    }

    const remaining =
      this.leaseExpiresAt -
      performance.now();

    if (remaining <= 0) {
      this.loseOwnership();

      return;
    }

    const delay =
      Math.max(
        0,
        Math.min(
          this.o.renewIntervalMs,
          Math.floor(remaining / 2),
        ),
      );

    this.renewTimer =
      setTimeout(() => {
        this.renewTimer =
          undefined;

        if (
          this.renewPromise !== null
        ) {
          return;
        }

        const operation =
          this.renew();

        this.renewPromise =
          operation;

        void operation
          .catch(() => {
            this.loseOwnership();
          })
          .finally(() => {
            if (
              this.renewPromise ===
              operation
            ) {
              this.renewPromise =
                null;
            }

            this.startRenewal();
          });
      }, delay);

    this.renewTimer.unref?.();
  }

  // ================================================
  // Independent lease expiry watchdog
  // ================================================

  private scheduleExpiryCheck():
    void {
    this.clearExpiryTimer();

    if (
      this.signal.aborted ||
      this.ownershipLost ||
      !this.lockAcquired
    ) {
      return;
    }

    const remaining =
      this.leaseExpiresAt -
      performance.now();

    if (remaining <= 0) {
      this.loseOwnership();

      return;
    }

    this.expiryTimer =
      setTimeout(() => {
        this.expiryTimer =
          undefined;

        if (
          !this.hasLocalOwnership()
        ) {
          this.loseOwnership();

          return;
        }

        this.scheduleExpiryCheck();
      }, Math.max(
        1,
        Math.ceil(remaining),
      ));

    this.expiryTimer.unref?.();
  }

  // ================================================
  // Serialized ownership renewal
  // ================================================

  private async renew():
    Promise<void> {
    if (
      this.signal.aborted ||
      this.ownershipLost ||
      !this.lockAcquired
    ) {
      return;
    }

    if (!this.hasLocalOwnership()) {
      this.loseOwnership();

      return;
    }

    const startedAt =
      performance.now();

    const previousExpiry =
      this.leaseExpiresAt;

    try {
      const renewed =
        await this.o.lock.renew();

      if (
        this.signal.aborted ||
        this.ownershipLost
      ) {
        return;
      }

      if (
        !renewed ||
        performance.now() >=
          previousExpiry
      ) {
        this.loseOwnership();

        return;
      }

      this.updateLeaseDeadline(
        startedAt,
      );

      if (
        !this.hasLocalOwnership()
      ) {
        this.loseOwnership();

        return;
      }

      this.scheduleExpiryCheck();
    } catch {
      this.loseOwnership();
    }
  }

  // ================================================
  // Ownership loss
  // ================================================

  private loseOwnership(): void {
    if (
      this.ownershipLost ||
      this.state === "STOPPED"
    ) {
      return;
    }

    this.ownershipLost = true;

    this.acceptData = false;

    this.clearRenewTimer();

    this.clearExpiryTimer();

    /**
     * Abort before notifying external callbacks.
     *
     * A callback failure must never prevent
     * cancellation of the Docker stream.
     */

    this.controller.abort();

    try {
      this.o.onOwnershipLost?.(
        OWNERSHIP_LOST_REASON,
      );
    } catch {
      // Callback failures cannot restore ownership.
    }
  }

  // ================================================
  // Wait for collection completion
  // ================================================

  async run(): Promise<void> {
    if (this.startPromise) {
      await this.startPromise;
    }

    if (this.streamPromise) {
      await this.streamPromise;
    }

    if (this.finishPromise) {
      await this.finishPromise;
    }
  }

  // ================================================
  // Public shutdown
  // ================================================

  async stop(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }

    this.acceptData = false;

    this.state = "STOPPING";

    this.clearRenewTimer();

    this.clearExpiryTimer();

    this.controller.abort();

    this.stopPromise =
      this.stopInternal();

    return this.stopPromise;
  }

  private async stopInternal():
    Promise<void> {
    try {
      /**
       * Initialization may still be acquiring
       * ownership or verifying container labels.
       *
       * Wait for it to settle before final cleanup.
       */

      if (this.startPromise) {
        await this.startPromise;
      }

      if (this.streamPromise) {
        await this.streamPromise;
      }
    } finally {
      await this.finish();
    }
  }

  // ================================================
  // Normalizer cleanup
  // ================================================

  private finalizeNormalizer():
    void {
    if (this.finalized) {
      return;
    }

    this.finalized = true;

    try {
      /**
       * A naturally completed stream may contain
       * one final partial line.
       *
       * On cancellation or ownership loss, the
       * record callback rejects pending fragments.
       */

      this.normalizer?.finalize();
    } catch {
      // Never forward unnormalized fallback data.
    } finally {
      this.normalizer = null;
    }
  }

  private disposeRedactors(): void {
    if (this.redactorsDisposed) {
      return;
    }

    this.redactorsDisposed = true;

    for (const redactor of Object.values(
      this.redactors,
    )) {
      try {
        redactor.dispose();
      } catch {
        // Continue disposing other redactors.
      }
    }
  }

  // ================================================
  // Ownership-safe cleanup
  // ================================================

  private async releaseOwnership():
    Promise<void> {
    if (!this.lockAcquired) {
      return;
    }

    this.lockAcquired = false;

    this.leaseExpiresAt = 0;

    try {
      await this.o.lock.release();
    } catch {
      /**
       * RedisCollectorLock uses ownership-safe
       * release and TTL-based eventual expiration.
       */
    }
  }

  // ================================================
  // Idempotent finalization
  // ================================================

  private async finish():
    Promise<void> {
    if (this.finishPromise) {
      return this.finishPromise;
    }

    this.finishPromise =
      Promise.resolve().then(
        () => this.finishInternal(),
      );

    return this.finishPromise;
  }

  private async finishInternal():
    Promise<void> {
    /**
     * Finalize before disabling ingestion so that a
     * naturally completed, still-owned stream can
     * emit its final partial record.
     */

    this.finalizeNormalizer();

    this.acceptData = false;

    this.state = "STOPPING";

    this.clearRenewTimer();

    this.clearExpiryTimer();

    this.controller.abort();

    this.signal.removeEventListener(
      "abort",
      this.handleAbort,
    );

    try {
      /**
       * Let an active renewal settle before release.
       *
       * The concrete RedisCollectorLock bounds
       * command waiting and prevents stale renewal
       * from restoring released ownership.
       */

      if (this.renewPromise) {
        try {
          await this.renewPromise;
        } catch {
          // Continue cleanup.
        }
      }

      /**
       * Release the collection lease before
       * potentially slow database draining.
       *
       * No new records are accepted after this
       * point. Already accepted records may still
       * persist after lease release.
       */

      await this.releaseOwnership();
    } finally {
      this.disposeRedactors();

      if (
        this.activeMetricRegistered
      ) {
        this.activeMetricRegistered =
          false;

        logMetrics.decrementGauge(
          "activeCollectors",
          1,
        );
      }

      try {
        if (this.o.ownsWriter) {
          await this.o.writer.stop();
        } else {
          await this.o.writer.flush();
        }
      } catch {
        /**
         * Writer cleanup failures must not retain
         * collector ownership or active metrics.
         */
      } finally {
        this.state = "STOPPED";
      }
    }
  }
}
