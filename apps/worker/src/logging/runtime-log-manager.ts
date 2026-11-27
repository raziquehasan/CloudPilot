
/**
 * CloudPilot — Runtime Log Collector Manager
 *
 * Phase 5.6 — Real-Time Deployment Logs & Streaming Engine
 *
 * Owns and reconciles the runtime log collectors associated
 * with a single worker.
 *
 * Responsibilities:
 *
 * - Reconcile collectors against authoritative runtime state.
 * - Prevent overlapping local reconciliation operations.
 * - Bound collector count and configured message-buffer capacity.
 * - Recover terminated collectors using bounded retry backoff.
 * - Detect runtime identity changes and container replacement.
 * - Coordinate dedicated writer and collector lifecycles.
 * - Monitor collector completion and clean registry entries.
 * - Coordinate worker shutdown using an owned AbortController.
 *
 * Security:
 *
 * - Uses immutable container identities.
 * - Uses deployment-scoped Redis ownership leases.
 * - Preserves workspace, project, deployment, build and
 *   runtime-instance identity.
 * - Supports secure caller-provided secret registration.
 * - Never modifies or removes Docker containers.
 * - Never holds deployment execution leases.
 *
 * Limitations:
 *
 * Redis ownership leases do not provide durable fencing.
 *
 * Historical replay may produce duplicate logical Docker
 * events after collector recreation.
 *
 * Configured message-buffer capacity is not an exact
 * measurement of total process heap usage.
 *
 * Reconciliation and recovery are driven by the worker's
 * periodic reconciliation scheduler.
 */

import {
  randomUUID,
} from "node:crypto";

import {
  performance,
} from "node:perf_hooks";

import {
  prisma,
} from "@cloudpilot/database";

import type {
  Redis,
} from "ioredis";

import type {
  RuntimeExecutor,
} from "../runtime/runtime-executor.js";

import {
  RedisCollectorLock,
} from "./collector-lock.js";

import {
  DeploymentLogWriter,
} from "./log-writer.js";

import {
  RuntimeLogCollector,
} from "./runtime-collector.js";

import {
  logMetrics,
} from "./metrics.js";

// ==================================================
// Default configuration
// ==================================================

const DEFAULT_MAX_ACTIVE_COLLECTORS =
  16;

const DEFAULT_MAX_TOTAL_BUFFERED_BYTES =
  128 * 1024 * 1024;

const DEFAULT_RECONCILE_PAGE_SIZE =
  100;

const DEFAULT_MAX_SCAN_PER_RECONCILE =
  500;

const DEFAULT_MAX_STARTS_PER_RECONCILE =
  8;

const DEFAULT_MAX_BUFFERED_ENTRIES =
  20_000;

const DEFAULT_RECONNECT_BASE_MS =
  1_000;

const DEFAULT_RECONNECT_MAX_MS =
  60_000;

const DEFAULT_RETRY_STATE_TTL_MS =
  30 * 60_000;

const DEFAULT_REDIS_LOCK_PREFIX =
  "cloudpilot:runtime-log-lock";

// ==================================================
// Hard limits
// ==================================================

const HARD_MAX_ACTIVE_COLLECTORS =
  128;

const HARD_MAX_TOTAL_BUFFERED_BYTES =
  512 * 1024 * 1024;

const HARD_MAX_PAGE_SIZE =
  500;

const HARD_MAX_SCAN_PER_RECONCILE =
  5_000;

const HARD_MAX_STARTS_PER_RECONCILE =
  128;

const HARD_MAX_BUFFERED_ENTRIES =
  50_000;

const HARD_MAX_RETRY_STATES =
  10_000;

const MAX_RECONNECT_FAILURES =
  20;

const STABLE_COLLECTOR_DURATION_MS =
  60_000;

const MAX_ENTRY_BYTES =
  16 * 1024;

const MAX_WRITER_BUFFER_BYTES =
  64 * 1024 * 1024;

const MAX_REGISTERED_SECRETS =
  64;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

const CONTAINER_ID_PATTERN =
  /^[0-9a-f]{64}$/u;

const REDIS_PREFIX_PATTERN =
  /^[a-zA-Z0-9:_-]{1,160}$/u;

// ==================================================
// Public contracts
// ==================================================

export interface RuntimeLogManagerConfig {
  maxEntryBytes: number;

  batchSize: number;

  flushIntervalMs: number;

  maxBufferedBytes: number;

  lockTtlMs: number;

  renewIntervalMs: number;

  maxHistoricalMs: number;

  /**
   * Maximum collector reservations for this manager.
   * Starting and stopping collectors count toward it.
   */

  maxActiveCollectors?: number;

  /**
   * Maximum sum of reserved writer message buffers.
   */

  maxTotalBufferedBytes?: number;

  maxBufferedEntries?: number;

  reconcilePageSize?: number;

  maxScanPerReconcile?: number;

  maxStartsPerReconcile?: number;

  reconnectBaseMs?: number;

  reconnectMaxMs?: number;

  retryStateTtlMs?: number;

  /**
   * All workers collecting from the same database
   * must use the same lock namespace.
   */

  redisLockPrefix?: string;
}

export interface RuntimeLogSecretContext {
  workspaceId: string;

  projectId: string;

  deploymentId: string;

  buildId: string;

  runtimeInstanceId: string;
}

export interface RuntimeLogManagerOptions {
  executor: RuntimeExecutor;

  redis: Redis;

  config: RuntimeLogManagerConfig;

  now?: () => Date;

  sleep?: (
    ms: number,
  ) => Promise<void>;

  /**
   * Optional worker-owned shutdown signal.
   */

  signal?: AbortSignal;

  /**
   * Optional trusted secret resolver.
   *
   * Resolve only secrets belonging to the exact
   * runtime environment and tenant scope.
   *
   * Never return unrelated workspace secrets.
   * Never log returned values.
   */

  resolveRegisteredSecrets?: (
    context: RuntimeLogSecretContext,
    signal: AbortSignal,
  ) => Promise<readonly string[]>;
}

// ==================================================
// Internal contracts
// ==================================================

type ManagerState =
  | "OPEN"
  | "STOPPING"
  | "STOPPED";

type CollectorState =
  | "STARTING"
  | "RUNNING"
  | "STOPPING";

interface ResolvedManagerConfig {
  maxEntryBytes: number;

  batchSize: number;

  flushIntervalMs: number;

  maxBufferedBytes: number;

  maxBufferedEntries: number;

  lockTtlMs: number;

  renewIntervalMs: number;

  maxHistoricalMs: number;

  maxActiveCollectors: number;

  maxTotalBufferedBytes: number;

  reconcilePageSize: number;

  maxScanPerReconcile: number;

  maxStartsPerReconcile: number;

  reconnectBaseMs: number;

  reconnectMaxMs: number;

  retryStateTtlMs: number;

  redisLockPrefix: string;
}

interface RuntimeIdentity {
  id: string;

  workspaceId: string;

  projectId: string;

  deploymentId: string;

  buildId: string;

  containerId: string;

  attempt: number;
}

interface RuntimeDatabaseRow {
  id: string;

  workspaceId: string;

  projectId: string;

  deploymentId: string;

  buildId: string;

  containerId: string | null;

  runtimeAttempt: number;
}

interface ActiveCollector {
  identity: RuntimeIdentity;

  collector: RuntimeLogCollector;

  writer: DeploymentLogWriter;

  controller: AbortController;

  state: CollectorState;

  startedAt: number;

  intentionalStop: boolean;

  reservedBytes: number;

  completion: Promise<void> | null;

  stopPromise: Promise<void> | null;
}

interface RetryState {
  containerId: string;

  failures: number;

  nextAttemptAt: number;

  updatedAt: number;
}

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
      `Invalid runtime log manager ${name}.`,
    );
  }
}

function resolveConfig(
  config: RuntimeLogManagerConfig,
): ResolvedManagerConfig {
  const resolved: ResolvedManagerConfig = {
    maxEntryBytes:
      config.maxEntryBytes,

    batchSize:
      config.batchSize,

    flushIntervalMs:
      config.flushIntervalMs,

    maxBufferedBytes:
      config.maxBufferedBytes,

    maxBufferedEntries:
      config.maxBufferedEntries ??
      DEFAULT_MAX_BUFFERED_ENTRIES,

    lockTtlMs:
      config.lockTtlMs,

    renewIntervalMs:
      config.renewIntervalMs,

    maxHistoricalMs:
      config.maxHistoricalMs,

    maxActiveCollectors:
      config.maxActiveCollectors ??
      DEFAULT_MAX_ACTIVE_COLLECTORS,

    maxTotalBufferedBytes:
      config.maxTotalBufferedBytes ??
      DEFAULT_MAX_TOTAL_BUFFERED_BYTES,

    reconcilePageSize:
      config.reconcilePageSize ??
      DEFAULT_RECONCILE_PAGE_SIZE,

    maxScanPerReconcile:
      config.maxScanPerReconcile ??
      DEFAULT_MAX_SCAN_PER_RECONCILE,

    maxStartsPerReconcile:
      config.maxStartsPerReconcile ??
      DEFAULT_MAX_STARTS_PER_RECONCILE,

    reconnectBaseMs:
      config.reconnectBaseMs ??
      DEFAULT_RECONNECT_BASE_MS,

    reconnectMaxMs:
      config.reconnectMaxMs ??
      DEFAULT_RECONNECT_MAX_MS,

    retryStateTtlMs:
      config.retryStateTtlMs ??
      DEFAULT_RETRY_STATE_TTL_MS,

    redisLockPrefix:
      config.redisLockPrefix ??
      DEFAULT_REDIS_LOCK_PREFIX,
  };

  assertInteger(
    resolved.maxEntryBytes,
    "maxEntryBytes",
    64,
    MAX_ENTRY_BYTES,
  );

  assertInteger(
    resolved.batchSize,
    "batchSize",
    1,
    1_000,
  );

  assertInteger(
    resolved.flushIntervalMs,
    "flushIntervalMs",
    10,
    60_000,
  );

  assertInteger(
    resolved.maxBufferedBytes,
    "maxBufferedBytes",
    resolved.maxEntryBytes,
    MAX_WRITER_BUFFER_BYTES,
  );

  assertInteger(
    resolved.maxBufferedEntries,
    "maxBufferedEntries",
    1,
    HARD_MAX_BUFFERED_ENTRIES,
  );

  assertInteger(
    resolved.lockTtlMs,
    "lockTtlMs",
    1_000,
    10 * 60_000,
  );

  assertInteger(
    resolved.renewIntervalMs,
    "renewIntervalMs",
    100,
    Math.floor(
      resolved.lockTtlMs / 3,
    ),
  );

  assertInteger(
    resolved.maxHistoricalMs,
    "maxHistoricalMs",
    0,
    60 * 60_000,
  );

  assertInteger(
    resolved.maxActiveCollectors,
    "maxActiveCollectors",
    1,
    HARD_MAX_ACTIVE_COLLECTORS,
  );

  assertInteger(
    resolved.maxTotalBufferedBytes,
    "maxTotalBufferedBytes",
    resolved.maxBufferedBytes,
    HARD_MAX_TOTAL_BUFFERED_BYTES,
  );

  assertInteger(
    resolved.reconcilePageSize,
    "reconcilePageSize",
    1,
    HARD_MAX_PAGE_SIZE,
  );

  assertInteger(
    resolved.maxScanPerReconcile,
    "maxScanPerReconcile",
    resolved.reconcilePageSize,
    HARD_MAX_SCAN_PER_RECONCILE,
  );

  assertInteger(
    resolved.maxStartsPerReconcile,
    "maxStartsPerReconcile",
    1,
    HARD_MAX_STARTS_PER_RECONCILE,
  );

  assertInteger(
    resolved.reconnectBaseMs,
    "reconnectBaseMs",
    100,
    60_000,
  );

  assertInteger(
    resolved.reconnectMaxMs,
    "reconnectMaxMs",
    resolved.reconnectBaseMs,
    10 * 60_000,
  );

  assertInteger(
    resolved.retryStateTtlMs,
    "retryStateTtlMs",
    resolved.reconnectMaxMs,
    24 * 60 * 60_000,
  );

  if (
    typeof resolved.redisLockPrefix !==
      "string" ||
    !REDIS_PREFIX_PATTERN.test(
      resolved.redisLockPrefix,
    )
  ) {
    throw new TypeError(
      "Invalid runtime log lock namespace.",
    );
  }

  return resolved;
}

// ==================================================
// Runtime identity helpers
// ==================================================

function isUuid(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    value.length === 36 &&
    UUID_PATTERN.test(value)
  );
}

function resolveIdentity(
  row: RuntimeDatabaseRow,
): RuntimeIdentity | null {
  if (
    !isUuid(row.id) ||
    !isUuid(row.workspaceId) ||
    !isUuid(row.projectId) ||
    !isUuid(row.deploymentId) ||
    !isUuid(row.buildId) ||
    typeof row.containerId !==
      "string" ||
    !CONTAINER_ID_PATTERN.test(
      row.containerId,
    ) ||
    !Number.isSafeInteger(
      row.runtimeAttempt,
    ) ||
    row.runtimeAttempt < 1 ||
    row.runtimeAttempt > 2_147_483_647
  ) {
    return null;
  }

  return {
    id: row.id.toLowerCase(),

    workspaceId:
      row.workspaceId.toLowerCase(),

    projectId:
      row.projectId.toLowerCase(),

    deploymentId:
      row.deploymentId.toLowerCase(),

    buildId:
      row.buildId.toLowerCase(),

    containerId:
      row.containerId,

    attempt:
      row.runtimeAttempt,
  };
}

function sameIdentity(
  left: RuntimeIdentity,
  right: RuntimeIdentity,
): boolean {
  return (
    left.id === right.id &&
    left.workspaceId ===
      right.workspaceId &&
    left.projectId ===
      right.projectId &&
    left.deploymentId ===
      right.deploymentId &&
    left.buildId ===
      right.buildId &&
    left.containerId ===
      right.containerId &&
    left.attempt ===
      right.attempt
  );
}

// ==================================================
// Runtime query selection
// ==================================================

const RUNTIME_SELECTION = {
  id: true,

  workspaceId: true,

  projectId: true,

  deploymentId: true,

  buildId: true,

  containerId: true,

  runtimeAttempt: true,
} as const;

// ==================================================
// Runtime log collector manager
// ==================================================

export class RuntimeLogCollectorManager {
  private readonly options:
    RuntimeLogManagerOptions;

  private readonly config:
    ResolvedManagerConfig;

  private readonly controller:
    AbortController;

  private readonly signal:
    AbortSignal;

  private readonly active =
    new Map<string, ActiveCollector>();

  private readonly retryStates =
    new Map<string, RetryState>();

  private state: ManagerState =
    "OPEN";

  private reconcilePromise:
    Promise<void> | null = null;

  private stopPromise:
    Promise<void> | null = null;

  private scanCursor:
    string | null = null;

  private reservedBufferBytes = 0;

  constructor(
    options: RuntimeLogManagerOptions,
  ) {
    if (
      !options.executor ||
      typeof options.executor.streamLogs !==
        "function" ||
      typeof options.executor.readContainerLabels !==
        "function"
    ) {
      throw new TypeError(
        "Invalid runtime log manager executor.",
      );
    }

    if (
      !options.redis ||
      typeof options.redis.set !==
        "function" ||
      typeof options.redis.eval !==
        "function"
    ) {
      throw new TypeError(
        "Invalid runtime log manager Redis client.",
      );
    }

    if (
      options.now !== undefined &&
      typeof options.now !==
        "function"
    ) {
      throw new TypeError(
        "Invalid runtime log manager clock.",
      );
    }

    if (
      options.sleep !== undefined &&
      typeof options.sleep !==
        "function"
    ) {
      throw new TypeError(
        "Invalid runtime log manager sleep function.",
      );
    }

    if (
      options.signal !== undefined &&
      !(options.signal instanceof AbortSignal)
    ) {
      throw new TypeError(
        "Invalid runtime log manager signal.",
      );
    }

    if (
      options.resolveRegisteredSecrets !==
        undefined &&
      typeof options.resolveRegisteredSecrets !==
        "function"
    ) {
      throw new TypeError(
        "Invalid runtime log secret resolver.",
      );
    }

    this.options = {
      ...options,

      config: {
        ...options.config,
      },
    };

    this.config =
      resolveConfig(options.config);

    this.controller =
      new AbortController();

    this.signal = options.signal
      ? AbortSignal.any([
          options.signal,
          this.controller.signal,
        ])
      : this.controller.signal;
  }

  // ================================================
  // Public state
  // ================================================

  /**
   * Includes starting and stopping reservations.
   */

  get activeCount(): number {
    return this.active.size;
  }

  get runningCount(): number {
    let count = 0;

    for (const entry of this.active.values()) {
      if (
        entry.state === "RUNNING" &&
        entry.collector.isRunning
      ) {
        count += 1;
      }
    }

    return count;
  }

  get reservedMessageBufferBytes():
    number {
    return this.reservedBufferBytes;
  }

  private canReconcile(): boolean {
    return (
      this.state === "OPEN" &&
      !this.signal.aborted
    );
  }

  // ================================================
  // Capacity management
  // ================================================

  private hasCapacity(): boolean {
    return (
      this.active.size <
        this.config.maxActiveCollectors &&
      this.config.maxBufferedBytes <=
        this.config.maxTotalBufferedBytes -
          this.reservedBufferBytes
    );
  }

  private registerCollector(
    entry: ActiveCollector,
  ): void {
    if (
      this.active.has(
        entry.identity.id,
      ) ||
      !this.hasCapacity()
    ) {
      throw new Error(
        "Runtime collector reservation unavailable.",
      );
    }

    this.active.set(
      entry.identity.id,
      entry,
    );

    this.reservedBufferBytes +=
      entry.reservedBytes;
  }

  private removeCollector(
    entry: ActiveCollector,
  ): boolean {
    /**
     * Compare the exact registry entry.
     *
     * A stale collector completion must never
     * remove a newer replacement collector.
     */

    if (
      this.active.get(
        entry.identity.id,
      ) !== entry
    ) {
      return false;
    }

    this.active.delete(
      entry.identity.id,
    );

    this.reservedBufferBytes =
      Math.max(
        0,
        this.reservedBufferBytes -
          entry.reservedBytes,
      );

    return true;
  }

  // ================================================
  // Retry state management
  // ================================================

  private canRetry(
    identity: RuntimeIdentity,
  ): boolean {
    const retry =
      this.retryStates.get(
        identity.id,
      );

    if (!retry) {
      return true;
    }

    if (
      retry.containerId !==
      identity.containerId
    ) {
      this.retryStates.delete(
        identity.id,
      );

      return true;
    }

    return (
      performance.now() >=
      retry.nextAttemptAt
    );
  }

  private recordFailure(
    identity: RuntimeIdentity,
    stable = false,
  ): void {
    if (!this.canReconcile()) {
      return;
    }

    const previous =
      this.retryStates.get(
        identity.id,
      );

    const previousFailures =
      !stable &&
      previous?.containerId ===
        identity.containerId
        ? previous.failures
        : 0;

    const failures =
      Math.min(
        MAX_RECONNECT_FAILURES,
        previousFailures + 1,
      );

    const exponential =
      Math.min(
        this.config.reconnectMaxMs,

        this.config.reconnectBaseMs *
          2 ** (failures - 1),
      );

    const jitter =
      0.75 + Math.random() * 0.5;

    const delay =
      Math.min(
        this.config.reconnectMaxMs,

        Math.max(
          this.config.reconnectBaseMs,
          Math.round(
            exponential * jitter,
          ),
        ),
      );

    const now =
      performance.now();

    this.retryStates.delete(
      identity.id,
    );

    this.retryStates.set(
      identity.id,
      {
        containerId:
          identity.containerId,

        failures,

        nextAttemptAt:
          now + delay,

        updatedAt:
          now,
      },
    );

    this.pruneRetryStates();
  }

  private pruneRetryStates():
    void {
    const now =
      performance.now();

    for (
      const [id, retry]
      of this.retryStates
    ) {
      if (
        now - retry.updatedAt >=
        this.config.retryStateTtlMs
      ) {
        this.retryStates.delete(id);
      }
    }

    while (
      this.retryStates.size >
      HARD_MAX_RETRY_STATES
    ) {
      const oldest =
        this.retryStates.keys()
          .next().value;

      if (oldest === undefined) {
        break;
      }

      this.retryStates.delete(
        oldest,
      );
    }
  }

  // ================================================
  // Serialized reconciliation
  // ================================================

  async reconcile(): Promise<void> {
    if (!this.canReconcile()) {
      return;
    }

    if (this.reconcilePromise) {
      return this.reconcilePromise;
    }

    const operation =
      Promise.resolve().then(
        () =>
          this.reconcileInternal(),
      );

    this.reconcilePromise =
      operation;

    try {
      await operation;
    } finally {
      if (
        this.reconcilePromise ===
        operation
      ) {
        this.reconcilePromise =
          null;
      }
    }
  }

  private async reconcileInternal():
    Promise<void> {
    if (!this.canReconcile()) {
      return;
    }

    this.pruneRetryStates();

    /**
     * Validate existing collectors independently
     * from the paginated discovery scan.
     *
     * An active collector must not be stopped
     * simply because its runtime was absent from
     * the current discovery page.
     */

    await this.reconcileActive();

    if (
      !this.canReconcile() ||
      !this.hasCapacity()
    ) {
      return;
    }

    await this.discoverCollectors();
  }

  // ================================================
  // Existing collector reconciliation
  // ================================================

  private async reconcileActive():
    Promise<void> {
    const snapshot =
      [...this.active.values()];

    if (snapshot.length === 0) {
      return;
    }

    const running =
      await prisma.deploymentRuntimeInstance
        .findMany({
          where: {
            id: {
              in: snapshot.map(
                (entry) =>
                  entry.identity.id,
              ),
            },

            status: "RUNNING",

            containerId: {
              not: null,
            },
          },

          select:
            RUNTIME_SELECTION,
        });

    if (!this.canReconcile()) {
      return;
    }

    const identities =
      new Map<
        string,
        RuntimeIdentity
      >();

    for (const row of running) {
      const identity =
        resolveIdentity(row);

      if (identity) {
        identities.set(
          identity.id,
          identity,
        );
      }
    }

    for (const entry of snapshot) {
      if (!this.canReconcile()) {
        return;
      }

      if (
        this.active.get(
          entry.identity.id,
        ) !== entry
      ) {
        continue;
      }

      const current =
        identities.get(
          entry.identity.id,
        );

      if (
        !current ||
        !sameIdentity(
          entry.identity,
          current,
        )
      ) {
        /**
         * Runtime stopped, disappeared or changed.
         *
         * This is an intentional lifecycle stop,
         * not a reconnect failure.
         */

        await this.stopEntry(
          entry,
          true,
        );

        this.retryStates.delete(
          entry.identity.id,
        );

        continue;
      }

      if (
        entry.state === "RUNNING" &&
        !entry.collector.isRunning
      ) {
        /**
         * The collector lost ownership or its
         * Docker stream terminated.
         */

        await this.stopEntry(
          entry,
          false,
        );
      }
    }
  }

  // ================================================
  // Bounded runtime discovery
  // ================================================

  private async discoverCollectors():
    Promise<void> {
    let scanned = 0;

    let starts = 0;

    while (
      this.canReconcile() &&
      this.hasCapacity() &&
      scanned <
        this.config.maxScanPerReconcile &&
      starts <
        this.config.maxStartsPerReconcile
    ) {
      const take =
        Math.min(
          this.config.reconcilePageSize,

          this.config.maxScanPerReconcile -
            scanned,
        );

      const cursor =
        this.scanCursor;

      const rows:
        RuntimeDatabaseRow[] =
        await prisma.deploymentRuntimeInstance
          .findMany({
            where: {
              status: "RUNNING",

              containerId: {
                not: null,
              },

              ...(cursor !== null
                ? {
                    id: {
                      gt: cursor,
                    },
                  }
                : {}),
            },

            orderBy: {
              id: "asc",
            },

            take,

            select:
              RUNTIME_SELECTION,
          });

      if (!this.canReconcile()) {
        return;
      }

      if (rows.length === 0) {
        this.scanCursor = null;

        return;
      }

      for (const row of rows) {
        if (
          !this.canReconcile() ||
          !this.hasCapacity() ||
          starts >=
            this.config.maxStartsPerReconcile
        ) {
          return;
        }

        /**
         * Advance only past rows actually examined.
         *
         * This prevents permanently skipping the
         * unprocessed remainder of a fetched page.
         */

        this.scanCursor =
          row.id;

        scanned += 1;

        const identity =
          resolveIdentity(row);

        if (!identity) {
          continue;
        }

        if (
          this.active.has(
            identity.id,
          ) ||
          !this.canRetry(identity)
        ) {
          continue;
        }

        starts += 1;

        await this.startInstance(
          identity,
        );
      }

      if (rows.length < take) {
        this.scanCursor = null;

        return;
      }
    }
  }

  // ================================================
  // Authoritative runtime revalidation
  // ================================================

  private async revalidateIdentity(
    identity: RuntimeIdentity,
  ): Promise<boolean> {
    if (!this.canReconcile()) {
      return false;
    }

    const current =
      await prisma.deploymentRuntimeInstance
        .findFirst({
          where: {
            id: identity.id,

            workspaceId:
              identity.workspaceId,

            projectId:
              identity.projectId,

            deploymentId:
              identity.deploymentId,

            buildId:
              identity.buildId,

            containerId:
              identity.containerId,

            runtimeAttempt:
              identity.attempt,

            status: "RUNNING",
          },

          select:
            RUNTIME_SELECTION,
        });

    if (
      !current ||
      !this.canReconcile()
    ) {
      return false;
    }

    const resolved =
      resolveIdentity(current);

    return (
      resolved !== null &&
      sameIdentity(
        identity,
        resolved,
      )
    );
  }

  // ================================================
  // Secure secret resolution
  // ================================================

  private async resolveSecrets(
    identity: RuntimeIdentity,
    signal: AbortSignal,
  ): Promise<readonly string[]> {
    if (signal.aborted) {
      throw new Error(
        "Collector initialization cancelled.",
      );
    }

    if (
      !this.options.resolveRegisteredSecrets
    ) {
      /**
       * Baseline pattern-based redaction remains
       * available when no resolver is configured.
       *
       * This does not guarantee detection of
       * deployment-specific arbitrary secrets.
       */

      return [];
    }

    const secrets =
      await this.options
        .resolveRegisteredSecrets(
          {
            workspaceId:
              identity.workspaceId,

            projectId:
              identity.projectId,

            deploymentId:
              identity.deploymentId,

            buildId:
              identity.buildId,

            runtimeInstanceId:
              identity.id,
          },

          signal,
        );

    if (
      signal.aborted ||
      !Array.isArray(secrets) ||
      secrets.length >
        MAX_REGISTERED_SECRETS
    ) {
      throw new Error(
        "Collector secret resolution failed.",
      );
    }

    return secrets;
  }

  // ================================================
  // Collector initialization
  // ================================================

  private async startInstance(
    identity: RuntimeIdentity,
  ): Promise<void> {
    if (
      !this.canReconcile() ||
      !this.hasCapacity() ||
      this.active.has(identity.id)
    ) {
      return;
    }

    const controller =
      new AbortController();

    const signal =
      AbortSignal.any([
        this.signal,
        controller.signal,
      ]);

    let writer:
      DeploymentLogWriter | null =
        null;

    let collector:
      RuntimeLogCollector | null =
        null;

    let entry:
      ActiveCollector | null =
        null;

    try {
      if (
        !(await this.revalidateIdentity(
          identity,
        ))
      ) {
        return;
      }

      const secrets =
        await this.resolveSecrets(
          identity,
          signal,
        );

      if (
        signal.aborted ||
        !this.canReconcile() ||
        !this.hasCapacity()
      ) {
        return;
      }

      writer =
        new DeploymentLogWriter({
          workspaceId:
            identity.workspaceId,

          projectId:
            identity.projectId,

          deploymentId:
            identity.deploymentId,

          buildId:
            identity.buildId,

          runtimeInstanceId:
            identity.id,

          maxEntryBytes:
            this.config.maxEntryBytes,

          batchSize:
            this.config.batchSize,

          flushIntervalMs:
            this.config.flushIntervalMs,

          maxBufferedBytes:
            this.config.maxBufferedBytes,

          maxBufferedEntries:
            this.config.maxBufferedEntries,

          ...(this.options.now
            ? {
                now: this.options.now,
              }
            : {}),

          ...(this.options.sleep
            ? {
                sleep: this.options.sleep,
              }
            : {}),
        });

      const lock =
        new RedisCollectorLock({
          /**
           * Preserve the existing default namespace.
           *
           * Runtime instance IDs are globally unique.
           * All workers must use an identical prefix.
           */

          key:
            `${this.config.redisLockPrefix}:` +
            identity.id,

          token:
            randomUUID(),

          ttlMs:
            this.config.lockTtlMs,

          redis:
            this.options.redis,
        });

      collector =
        new RuntimeLogCollector({
          workspaceId:
            identity.workspaceId,

          projectId:
            identity.projectId,

          deploymentId:
            identity.deploymentId,

          runtimeInstanceId:
            identity.id,

          containerId:
            identity.containerId,

          attempt:
            identity.attempt,

          executor:
            this.options.executor,

          writer,

          lock,

          lockTtlMs:
            this.config.lockTtlMs,

          renewIntervalMs:
            this.config.renewIntervalMs,

          maxHistoricalMs:
            this.config.maxHistoricalMs,

          maxLineBytes:
            this.config.maxEntryBytes,

          /**
           * One acquisition attempt per manager
           * initialization.
           *
           * The manager owns subsequent retry
           * scheduling and backoff.
           */

          acquireAttempts: 1,

          registeredSecrets:
            secrets,

          /**
           * The collector owns this dedicated
           * writer and drains it during cleanup.
           */

          ownsWriter: true,

          signal,

          ...(this.options.now
            ? {
                now: this.options.now,
              }
            : {}),

          ...(this.options.sleep
            ? {
                sleep: this.options.sleep,
              }
            : {}),
        });

      entry = {
        identity,

        collector,

        writer,

        controller,

        state: "STARTING",

        startedAt:
          performance.now(),

        intentionalStop: false,

        reservedBytes:
          this.config.maxBufferedBytes,

        completion: null,

        stopPromise: null,
      };

      this.registerCollector(entry);

      const started =
        await collector.start();

      if (
        !started ||
        signal.aborted ||
        !this.canReconcile()
      ) {
        await this.stopEntry(
          entry,
          signal.aborted ||
            !this.canReconcile(),
        );

        return;
      }

      entry.state = "RUNNING";

      entry.startedAt =
        performance.now();

      /**
       * Monitor every collector until its
       * stream and writer cleanup complete.
       */

      const registeredEntry =
        entry;

      entry.completion =
        collector.run().then(
          () =>
            this.handleCompletion(
              registeredEntry,
            ),

          () =>
            this.handleCompletion(
              registeredEntry,
            ),
        );

      /**
       * Observe completion failures without
       * exposing raw Docker or database errors.
       */

      void entry.completion.catch(
        () => {},
      );
    } catch {
      controller.abort();

      if (
        entry &&
        this.active.get(
          identity.id,
        ) === entry
      ) {
        await this.stopEntry(
          entry,
          !this.canReconcile(),
        );

        return;
      }

      /**
       * Initialization may fail before a registry
       * reservation has been installed.
       */

      if (collector) {
        try {
          await collector.stop();
        } catch {
          // Continue best-effort cleanup.
        }
      }

      if (writer) {
        try {
          await writer.stop();
        } catch {
          // Never expose raw persistence errors.
        }
      }

      if (this.canReconcile()) {
        this.recordFailure(
          identity,
        );
      }
    }
  }

  // ================================================
  // Collector completion monitoring
  // ================================================

  private async handleCompletion(
    entry: ActiveCollector,
  ): Promise<void> {
    /**
     * RuntimeLogCollector.run() resolves after its
     * own stream and writer cleanup.
     *
     * Do not call stopEntry() here because a stop
     * operation may already be waiting for run().
     */

    if (
      this.active.get(
        entry.identity.id,
      ) !== entry
    ) {
      return;
    }

    if (entry.stopPromise) {
      return;
    }

    const stable =
      performance.now() -
        entry.startedAt >=
      STABLE_COLLECTOR_DURATION_MS;

    if (
      !this.removeCollector(entry)
    ) {
      return;
    }

    if (
      !entry.intentionalStop &&
      this.canReconcile()
    ) {
      this.recordFailure(
        entry.identity,
        stable,
      );

      logMetrics.increment(
        "reconnectAttempts",
        1,
      );
    }
  }

  // ================================================
  // Coordinated collector shutdown
  // ================================================

  private async stopEntry(
    entry: ActiveCollector,
    intentional: boolean,
  ): Promise<void> {
    if (intentional) {
      entry.intentionalStop = true;
    }

    if (entry.stopPromise) {
      return entry.stopPromise;
    }

    entry.state = "STOPPING";

    entry.controller.abort();

    /**
     * Install the stop promise before beginning
     * asynchronous cleanup.
     */

    const operation =
      Promise.resolve().then(
        () =>
          this.stopEntryInternal(
            entry,
          ),
      );

    entry.stopPromise =
      operation;

    return operation;
  }

  private async stopEntryInternal(
    entry: ActiveCollector,
  ): Promise<void> {
    try {
      await entry.collector.stop();
    } catch {
      // Continue writer cleanup.
    }

    try {
      /**
       * Normally the collector has already stopped
       * its dedicated writer.
       *
       * The writer's stop operation is idempotent,
       * so this also covers initialization failures.
       */

      await entry.writer.stop();
    } catch {
      // Continue registry cleanup.
    } finally {
      const removed =
        this.removeCollector(
          entry,
        );

      if (
        removed &&
        !entry.intentionalStop &&
        this.canReconcile()
      ) {
        const stable =
          performance.now() -
            entry.startedAt >=
          STABLE_COLLECTOR_DURATION_MS;

        this.recordFailure(
          entry.identity,
          stable,
        );

        logMetrics.increment(
          "reconnectAttempts",
          1,
        );
      }
    }
  }

  // ================================================
  // Manager shutdown
  // ================================================

  /**
   * Stop accepting reconciliation requests.
   *
   * Abort all collectors immediately, wait for
   * in-flight reconciliation and drain registered
   * collectors.
   *
   * The caller owns the Redis and Prisma connection
   * lifecycle and must close those connections only
   * after manager shutdown has completed.
   */

  async stopAll(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }

    if (
      this.state === "STOPPED"
    ) {
      return;
    }

    this.state = "STOPPING";

    this.controller.abort();

    for (const entry of this.active.values()) {
      entry.intentionalStop = true;

      entry.controller.abort();
    }

    this.stopPromise =
      this.stopAllInternal();

    return this.stopPromise;
  }

  private async stopAllInternal():
    Promise<void> {
    try {
      if (this.reconcilePromise) {
        try {
          await this.reconcilePromise;
        } catch {
          /**
           * A failed reconciliation must not
           * prevent shutdown.
           */
        }
      }

      const snapshot =
        [...this.active.values()];

      /**
       * Bound shutdown concurrency to avoid
       * overwhelming the database connection pool.
       */

      const shutdownConcurrency = 4;

      for (
        let index = 0;
        index < snapshot.length;
        index += shutdownConcurrency
      ) {
        const group =
          snapshot.slice(
            index,

            index +
              shutdownConcurrency,
          );

        await Promise.allSettled(
          group.map(
            (entry) =>
              this.stopEntry(
                entry,
                true,
              ),
          ),
        );
      }
    } finally {
      this.retryStates.clear();

      this.scanCursor = null;

      this.state = "STOPPED";

      /**
       * Never reset the process-wide active collector
       * metric here.
       *
       * Each collector owns its balanced gauge
       * increment and decrement.
       */
    }
  }
}
