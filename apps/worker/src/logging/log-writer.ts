
/**
 * CloudPilot — Durable Log Writer
 *
 * Phase 5.6 — Real-Time Deployment Logs & Streaming Engine
 *
 * Bounded buffering and transactional persistence for a
 * single deployment's log stream.
 *
 * Guarantees and limits:
 *
 * - PostgreSQL is the authoritative log store.
 * - Log entries and notification outbox records are written
 *   within the same transaction.
 * - Sequence allocation is serialized by a deployment-
 *   scoped PostgreSQL counter row.
 * - Stable record identities are preserved across retries.
 * - Queued and in-flight entries share the same memory budget.
 * - Batch entry counts and message bytes are bounded.
 * - Database operations use explicit transaction timeouts.
 * - Retries are bounded and limited to potentially
 *   transient or ambiguous database failures.
 * - Metrics are updated only after transaction completion.
 * - Shutdown drains accepted records and stops new ingestion.
 *
 * Limitations:
 *
 * The in-memory buffer is not a durable write-ahead log.
 * A process crash can lose uncommitted records.
 *
 * Retry deduplication depends on stable record IDs remaining
 * available in PostgreSQL. Retention must not remove records
 * during their active retry window.
 *
 * Diagnostic markers are best-effort. Permanent database
 * outages can prevent their persistence.
 *
 * All incoming messages must already be sanitized and
 * redacted by the trusted logging pipeline.
 */

import { randomUUID } from "node:crypto";

import { performance } from "node:perf_hooks";

import { prisma } from "@cloudpilot/database";

import type {
  CollectedLogRecord,
} from "@cloudpilot/queue";

import { logMetrics } from "./metrics.js";

// ==================================================
// Default configuration
// ==================================================

const DEFAULT_MAX_ENTRY_BYTES = 16 * 1024;

const DEFAULT_BATCH_SIZE = 100;

const DEFAULT_FLUSH_INTERVAL_MS = 250;

const DEFAULT_MAX_BUFFERED_BYTES =
  8 * 1024 * 1024;

const DEFAULT_MAX_BUFFERED_ENTRIES = 20_000;

const DEFAULT_MAX_RETRIES = 4;

// ==================================================
// Hard limits
// ==================================================

const HARD_MAX_ENTRY_BYTES = 16 * 1024;

const HARD_MAX_BATCH_SIZE = 1_000;

const HARD_MAX_BATCH_BYTES =
  2 * 1024 * 1024;

const HARD_MAX_BUFFERED_BYTES =
  64 * 1024 * 1024;

const HARD_MAX_BUFFERED_ENTRIES = 50_000;

const HARD_MAX_RETRIES = 10;

const MAX_SEQUENCE =
  9_223_372_036_854_775_807n;

const MIN_ENTRY_BYTES = 64;

const BASE_BACKOFF_MS = 50;

const MAX_BACKOFF_MS = 2_000;

const TRANSACTION_MAX_WAIT_MS = 5_000;

const TRANSACTION_TIMEOUT_MS = 15_000;

const DROP_MARKER =
  "[Log buffer overloaded; some recent entries were dropped]";

const PERSIST_FAILURE_MARKER =
  "[Log persistence failed; some entries were dropped]";

const UNCERTAIN_PERSISTENCE_MARKER =
  "[Log persistence uncertain; some entries may be missing]";

// ==================================================
// Runtime validation
// ==================================================

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

const UNSAFE_CONTROL_PATTERN =
  /[\u0000-\u0008\u000b-\u001f\u007f-\u009f]/u;

const VALID_SOURCES = new Set<string>([
  "BUILD",
  "RUNTIME",
  "SYSTEM",
]);

const VALID_STREAMS = new Set<string>([
  "STDOUT",
  "STDERR",
  "SYSTEM",
]);

const VALID_LEVELS = new Set<string>([
  "DEBUG",
  "INFO",
  "WARN",
  "ERROR",
  "UNKNOWN",
]);

// ==================================================
// Public contracts
// ==================================================

/**
 * Exact execution identity for a claimed build.
 *
 * The token must never be included in log messages,
 * diagnostics or public API responses.
 */
export interface BuildLogExecutionFence {
  buildAttempt: number;

  executionLeaseToken: string;
}

export interface DeploymentLogWriterOptions {
  buildExecutionFence?: BuildLogExecutionFence;

  workspaceId: string;

  projectId: string;

  deploymentId: string;

  buildId?: string | null;

  runtimeInstanceId?: string | null;

  maxEntryBytes?: number;

  batchSize?: number;

  flushIntervalMs?: number;

  maxBufferedBytes?: number;

  maxBufferedEntries?: number;

  maxRetries?: number;

  now?: () => Date;

  sleep?: (ms: number) => Promise<void>;
}

// ==================================================
// Internal contracts
// ==================================================

interface ResolvedWriterOptions {
  buildExecutionFence: Readonly<BuildLogExecutionFence> | null;

  workspaceId: string;

  projectId: string;

  deploymentId: string;

  buildId: string | null;

  runtimeInstanceId: string | null;

  maxEntryBytes: number;

  batchSize: number;

  flushIntervalMs: number;

  maxBufferedBytes: number;

  maxBufferedEntries: number;

  maxRetries: number;

  now: () => Date;

  sleep: (ms: number) => Promise<void>;
}

interface BufferedEntry {
  id: string;

  source: CollectedLogRecord["source"];

  stream: CollectedLogRecord["stream"];

  level: CollectedLogRecord["level"];

  message: string;

  eventTimestamp: Date;

  bytes: number;

  diagnostic: boolean;
}

interface BatchWriteResult {
  insertedCount: number;

  confirmedCount: number;
}

type WriteOutcome =
  | "committed"
  | "failed"
  | "uncertain";

type WriterState =
  | "OPEN"
  | "STOPPING"
  | "STOPPED";

// ==================================================
// Internal errors
// ==================================================

class LogWriterOwnershipError extends Error {
  constructor() {
    super("Build log execution ownership is no longer valid.");

    this.name = "LogWriterOwnershipError";
  }
}

class LogWriterInvariantError extends Error {
  constructor(message: string) {
    super(message);

    this.name = "LogWriterInvariantError";
  }
}

// ==================================================
// Configuration helpers
// ==================================================

function assertBoundedInteger(
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
      `${name} must be an integer between ` +
        `${minimum} and ${maximum}.`,
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
      `Invalid ${name}.`,
    );
  }

  return value.toLowerCase();
}

function normalizeOptionalUuid(
  value: string | null | undefined,
  name: string,
): string | null {
  if (
    value === undefined ||
    value === null
  ) {
    return null;
  }

  return normalizeUuid(value, name);
}

function defaultSleep(
  ms: number,
): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function backoffDelay(
  attempt: number,
): number {
  const exponential = Math.min(
    BASE_BACKOFF_MS * 2 ** attempt,
    MAX_BACKOFF_MS,
  );

  const jitter =
    0.5 + Math.random();

  return Math.min(
    MAX_BACKOFF_MS,
    Math.max(
      1,
      Math.round(exponential * jitter),
    ),
  );
}

function resolveBuildExecutionFence(
  options: DeploymentLogWriterOptions,
): Readonly<BuildLogExecutionFence> | null {
  const fence = options.buildExecutionFence;

  if (fence === undefined) {
    return null;
  }

  if (
    !fence ||
    typeof fence !== "object" ||
    !options.buildId ||
    options.runtimeInstanceId != null
  ) {
    throw new TypeError(
      "Invalid build log execution fence.",
    );
  }

  assertBoundedInteger(
    fence.buildAttempt,
    "buildAttempt",
    1,
    2_147_483_647,
  );

  return Object.freeze({
    buildAttempt: fence.buildAttempt,

    executionLeaseToken: normalizeUuid(
      fence.executionLeaseToken,
      "executionLeaseToken",
    ),
  });
}

function resolveOptions(
  options: DeploymentLogWriterOptions,
): ResolvedWriterOptions {
  const resolved: ResolvedWriterOptions = {
    buildExecutionFence:
      resolveBuildExecutionFence(options),

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

    buildId: normalizeOptionalUuid(
      options.buildId,
      "buildId",
    ),

    runtimeInstanceId:
      normalizeOptionalUuid(
        options.runtimeInstanceId,
        "runtimeInstanceId",
      ),

    maxEntryBytes:
      options.maxEntryBytes ??
      DEFAULT_MAX_ENTRY_BYTES,

    batchSize:
      options.batchSize ??
      DEFAULT_BATCH_SIZE,

    flushIntervalMs:
      options.flushIntervalMs ??
      DEFAULT_FLUSH_INTERVAL_MS,

    maxBufferedBytes:
      options.maxBufferedBytes ??
      DEFAULT_MAX_BUFFERED_BYTES,

    maxBufferedEntries:
      options.maxBufferedEntries ??
      DEFAULT_MAX_BUFFERED_ENTRIES,

    maxRetries:
      options.maxRetries ??
      DEFAULT_MAX_RETRIES,

    now:
      options.now ??
      (() => new Date()),

    sleep:
      options.sleep ??
      defaultSleep,
  };

  assertBoundedInteger(
    resolved.maxEntryBytes,
    "maxEntryBytes",
    MIN_ENTRY_BYTES,
    HARD_MAX_ENTRY_BYTES,
  );

  assertBoundedInteger(
    resolved.batchSize,
    "batchSize",
    1,
    HARD_MAX_BATCH_SIZE,
  );

  assertBoundedInteger(
    resolved.flushIntervalMs,
    "flushIntervalMs",
    10,
    60_000,
  );

  assertBoundedInteger(
    resolved.maxBufferedBytes,
    "maxBufferedBytes",
    resolved.maxEntryBytes,
    HARD_MAX_BUFFERED_BYTES,
  );

  assertBoundedInteger(
    resolved.maxBufferedEntries,
    "maxBufferedEntries",
    1,
    HARD_MAX_BUFFERED_ENTRIES,
  );

  assertBoundedInteger(
    resolved.maxRetries,
    "maxRetries",
    0,
    HARD_MAX_RETRIES,
  );

  if (
    typeof resolved.now !== "function" ||
    typeof resolved.sleep !== "function"
  ) {
    throw new TypeError(
      "Invalid writer clock or sleep function.",
    );
  }

  return resolved;
}

// ==================================================
// Database error classification
// ==================================================

function getErrorCode(
  error: unknown,
): string | undefined {
  if (
    typeof error !== "object" ||
    error === null
  ) {
    return undefined;
  }

  const code = (
    error as { code?: unknown }
  ).code;

  return typeof code === "string"
    ? code
    : undefined;
}

const RETRYABLE_DATABASE_CODES =
  new Set<string>([
    "P1001",
    "P1002",
    "P1008",
    "P1017",
    "P2024",
    "P2028",
    "P2034",

    // PostgreSQL serialization/deadlock failures.
    "40001",
    "40P01",

    // Connection and availability failures.
    "08000",
    "08003",
    "08006",
    "57P01",
    "57P02",
    "57P03",

    // Common transport failures.
    "ECONNRESET",
    "ECONNREFUSED",
    "ETIMEDOUT",
    "EPIPE",
  ]);

function isRetryableDatabaseError(
  error: unknown,
): boolean {
  if (
    error instanceof LogWriterInvariantError ||
    error instanceof LogWriterOwnershipError
  ) {
    return false;
  }

  const code = getErrorCode(error);

  if (code === "P2010") {
    // Prisma raw-query errors may expose the
    // PostgreSQL SQLSTATE in metadata.

    const meta = (
      error as {
        meta?: {
          code?: unknown;
        };
      }
    ).meta;

    return (
      typeof meta?.code === "string" &&
      RETRYABLE_DATABASE_CODES.has(
        meta.code,
      )
    );
  }

  return (
    code !== undefined &&
    RETRYABLE_DATABASE_CODES.has(code)
  );
}

function isDefinitiveFailure(
  error: unknown,
): boolean {
  if (
    error instanceof LogWriterInvariantError ||
    error instanceof LogWriterOwnershipError
  ) {
    return true;
  }

  const code = getErrorCode(error);

  return (
    code === "P2000" ||
    code === "P2002" ||
    code === "P2003" ||
    code === "P2004" ||
    code === "P2011" ||
    code === "P2025"
  );
}

// ==================================================
// Record validation
// ==================================================

function validateRecord(
  record: CollectedLogRecord,
  maxEntryBytes: number,
): number | null {
  if (
    !record ||
    typeof record !== "object"
  ) {
    return null;
  }

  if (
    !VALID_SOURCES.has(record.source) ||
    !VALID_STREAMS.has(record.stream) ||
    !VALID_LEVELS.has(record.level)
  ) {
    return null;
  }

  if (
    typeof record.message !== "string" ||
    record.message.length === 0 ||
    record.message.length > maxEntryBytes ||
    UNSAFE_CONTROL_PATTERN.test(
      record.message,
    )
  ) {
    return null;
  }

  const bytes = Buffer.byteLength(
    record.message,
    "utf8",
  );

  if (
    bytes === 0 ||
    bytes > maxEntryBytes
  ) {
    return null;
  }

  if (
    !(record.eventTimestamp instanceof Date) ||
    !Number.isFinite(
      record.eventTimestamp.getTime(),
    )
  ) {
    return null;
  }

  return bytes;
}

// ==================================================
// Durable deployment log writer
// ==================================================

export class DeploymentLogWriter {
  private readonly options:
    ResolvedWriterOptions;

  private buffer: BufferedEntry[] = [];

  /**
   * These counters include both queued and
   * currently in-flight entries.
   */

  private bufferedBytes = 0;

  private bufferedEntries = 0;

  private queuedBytes = 0;

  private timer:
    | ReturnType<typeof setTimeout>
    | undefined;

  private flushing:
    Promise<void> | null = null;

  private stopPromise:
    Promise<void> | null = null;

  private state: WriterState = "OPEN";

  private ownershipLost = false;

  get hasLostOwnership(): boolean {
    return this.ownershipLost;
  }

  private pendingDropped = 0;

  private pendingPersistenceFailure = false;

  private pendingUncertainFailure = false;

  constructor(
    options: DeploymentLogWriterOptions,
  ) {
    this.options =
      resolveOptions(options);
  }

  // ================================================
  // Public buffer statistics
  // ================================================

  get bufferedEntryCount(): number {
    return this.bufferedEntries;
  }

  get bufferedByteCount(): number {
    return this.bufferedBytes;
  }

  // ================================================
  // Buffer capacity
  // ================================================

  private hasCapacity(
    bytes: number,
  ): boolean {
    return (
      this.bufferedEntries <
        this.options.maxBufferedEntries &&
      bytes <=
        this.options.maxBufferedBytes -
          this.bufferedBytes
    );
  }

  private appendEntry(
    entry: BufferedEntry,
  ): void {
    this.buffer.push(entry);

    this.bufferedEntries += 1;

    this.bufferedBytes += entry.bytes;

    this.queuedBytes += entry.bytes;

    logMetrics.incrementGauge(
      "bufferedBytes",
      entry.bytes,
    );
  }

  private releaseBatch(
    batch: readonly BufferedEntry[],
  ): void {
    let releasedBytes = 0;

    for (const entry of batch) {
      releasedBytes += entry.bytes;
    }

    this.bufferedEntries = Math.max(
      0,
      this.bufferedEntries -
        batch.length,
    );

    this.bufferedBytes = Math.max(
      0,
      this.bufferedBytes -
        releasedBytes,
    );

    logMetrics.decrementGauge(
      "bufferedBytes",
      releasedBytes,
    );
  }

  // ================================================
  // Non-blocking ingestion
  // ================================================

  /**
   * Enqueue an already normalized and redacted record.
   *
   * No database operation is awaited by the producer.
   *
   * Record IDs are generated once and retained across
   * every database retry.
   */

  enqueue(
    record: CollectedLogRecord,
  ): void {
    if (
      this.state !== "OPEN" ||
      this.ownershipLost
    ) {
      return;
    }

    logMetrics.increment(
      "collectedEntries",
    );

    try {
      const bytes = validateRecord(
        record,
        this.options.maxEntryBytes,
      );

      if (
        bytes === null ||
        !this.hasCapacity(bytes)
      ) {
        this.dropOne();

        return;
      }

      this.appendEntry({
        id: randomUUID(),

        source: record.source,

        stream: record.stream,

        level: record.level,

        message: record.message,

        eventTimestamp: new Date(
          record.eventTimestamp.getTime(),
        ),

        bytes,

        diagnostic: false,
      });

      this.scheduleFlush();
    } catch {
      this.dropOne();
    }
  }

  // ================================================
  // Drop accounting
  // ================================================

  private dropOne(): void {
    this.pendingDropped = Math.min(
      Number.MAX_SAFE_INTEGER,
      this.pendingDropped + 1,
    );

    logMetrics.increment(
      "droppedEntries",
    );

    this.scheduleFlush();
  }

  // ================================================
  // Diagnostic marker creation
  // ================================================

  private appendDiagnostic(
    message: string,
    level: "WARN" | "ERROR",
  ): boolean {
    const bytes = Buffer.byteLength(
      message,
      "utf8",
    );

    if (
      bytes >
        this.options.maxEntryBytes ||
      !this.hasCapacity(bytes)
    ) {
      return false;
    }

    try {
      const timestamp =
        this.options.now();

      if (
        !(timestamp instanceof Date) ||
        !Number.isFinite(
          timestamp.getTime(),
        )
      ) {
        return false;
      }

      this.appendEntry({
        id: randomUUID(),

        source: "SYSTEM",

        stream: "SYSTEM",

        level,

        message,

        eventTimestamp: new Date(
          timestamp.getTime(),
        ),

        bytes,

        diagnostic: true,
      });

      return true;
    } catch {
      return false;
    }
  }

  /**
   * Markers are admitted through the same buffer
   * limits as ordinary records.
   *
   * No recursive failure-marker generation occurs
   * when a diagnostic record itself fails.
   */

  private materializeDiagnostics(): void {
    if (this.ownershipLost) {
      return;
    }

    if (
      this.pendingDropped > 0 &&
      this.appendDiagnostic(
        DROP_MARKER,
        "WARN",
      )
    ) {
      this.pendingDropped = 0;
    }

    if (
      this.pendingPersistenceFailure &&
      this.appendDiagnostic(
        PERSIST_FAILURE_MARKER,
        "ERROR",
      )
    ) {
      this.pendingPersistenceFailure =
        false;
    }

    if (
      this.pendingUncertainFailure &&
      this.appendDiagnostic(
        UNCERTAIN_PERSISTENCE_MARKER,
        "WARN",
      )
    ) {
      this.pendingUncertainFailure =
        false;
    }
  }

  // ================================================
  // Flush scheduling
  // ================================================

  private clearTimer(): void {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);

      this.timer = undefined;
    }
  }

  private scheduleFlush(): void {
    if (
      this.state !== "OPEN" ||
      this.ownershipLost ||
      this.flushing !== null ||
      this.timer !== undefined
    ) {
      return;
    }

    const hasWork =
      this.buffer.length > 0 ||
      this.pendingDropped > 0 ||
      this.pendingPersistenceFailure ||
      this.pendingUncertainFailure;

    if (!hasWork) {
      return;
    }

    const immediate =
      this.buffer.length >=
        this.options.batchSize ||
      this.queuedBytes >=
        HARD_MAX_BATCH_BYTES;

    this.timer = setTimeout(() => {
      this.timer = undefined;

      void this.flush().catch(() => {
        // Never expose database errors or raw
        // log messages from a background timer.
        this.scheduleFlush();
      });
    }, immediate
      ? 0
      : this.options.flushIntervalMs,
    );

    this.timer.unref?.();
  }

  // ================================================
  // Bounded batch extraction
  // ================================================

  private takeBatch(): BufferedEntry[] {
    let count = 0;

    let bytes = 0;

    while (
      count < this.buffer.length &&
      count < this.options.batchSize
    ) {
      const entry = this.buffer[count];

      if (!entry) {
        break;
      }

      if (
        entry.bytes >
        HARD_MAX_BATCH_BYTES - bytes
      ) {
        break;
      }

      bytes += entry.bytes;

      count += 1;
    }

    if (count === 0) {
      return [];
    }

    const batch = this.buffer.splice(
      0,
      count,
    );

    this.queuedBytes -= bytes;

    // Total buffered bytes and entries remain
    // accounted for until persistence finishes.

    return batch;
  }

  // ================================================
  // Serialized flush
  // ================================================

  /**
   * Flush the records present when this drain begins.
   *
   * New records arriving during the drain are handled
   * by the next scheduled flush.
   *
   * Concurrent flush callers share the active drain.
   */

  async flush(): Promise<void> {
    if (this.state === "STOPPED") {
      return;
    }

    if (this.flushing) {
      await this.flushing;

      return;
    }

    this.clearTimer();

    const operation = Promise.resolve()
      .then(() => this.drain());

    this.flushing = operation;

    try {
      await operation;
    } finally {
      if (
        this.flushing === operation
      ) {
        this.flushing = null;
      }

      this.scheduleFlush();
    }
  }

  // ================================================
  // Bounded drain cycle
  // ================================================

  private discardStaleBuffer(): void {
    this.clearTimer();

    const abandoned = this.buffer.splice(0);

    const ordinaryCount = abandoned.filter(
      (entry) => !entry.diagnostic,
    ).length;

    logMetrics.increment(
      "droppedEntries",
      ordinaryCount,
    );

    this.releaseBatch(abandoned);

    this.queuedBytes = 0;

    this.pendingDropped = 0;

    this.pendingPersistenceFailure = false;

    this.pendingUncertainFailure = false;
  }

  private async drain(): Promise<void> {
    if (this.ownershipLost) {
      this.discardStaleBuffer();

      return;
    }

    this.materializeDiagnostics();

    let remaining = this.buffer.length;

    while (remaining > 0) {
      const batch = this.takeBatch();

      if (batch.length === 0) {
        break;
      }

      remaining -= batch.length;

      let outcome: WriteOutcome =
        "uncertain";

      try {
        outcome =
          await this.writeBatch(batch);
      } catch {
        outcome = "uncertain";
      } finally {
        this.releaseBatch(batch);
      }

      if (outcome !== "committed") {
        this.handlePermanentFailure(
          batch,
          outcome,
        );
      }

      if (this.ownershipLost) {
        this.discardStaleBuffer();

        break;
      }
    }
  }

  // ================================================
  // Bounded retry policy
  // ================================================

  private async writeBatch(
    batch: BufferedEntry[],
  ): Promise<WriteOutcome> {
    const startedAt =
      performance.now();

    let sawUncertainFailure = false;

    for (
      let attempt = 0;
      attempt <= this.options.maxRetries;
      attempt += 1
    ) {
      try {
        const result =
          await this.persistBatch(batch);

        /**
         * Metrics are updated only after the
         * transaction promise has resolved.
         *
         * confirmedCount includes records recovered
         * through stable-ID retry deduplication.
         */

        const ordinaryCount = batch.filter(
          (entry) => !entry.diagnostic,
        ).length;

        logMetrics.increment(
          "persistedEntries",
          ordinaryCount,
        );

        logMetrics.observeBatchWrite(
          Math.max(
            0,
            performance.now() -
              startedAt,
          ),
        );

        // Validate the internal persistence result.
        // No additional database mutation occurs here.

        if (
          result.confirmedCount !==
          batch.length
        ) {
          return "uncertain";
        }

        return "committed";
      } catch (error) {
        if (error instanceof LogWriterOwnershipError) {
          this.ownershipLost = true;

          return sawUncertainFailure
            ? "uncertain"
            : "failed";
        }

        logMetrics.increment(
          "databaseWriteFailures",
        );

        if (
          !isDefinitiveFailure(error)
        ) {
          sawUncertainFailure = true;
        }

        const exhausted =
          attempt >=
          this.options.maxRetries;

        const retryable =
          isRetryableDatabaseError(error);

        if (
          exhausted ||
          !retryable
        ) {
          return sawUncertainFailure
            ? "uncertain"
            : "failed";
        }

        try {
          await this.options.sleep(
            backoffDelay(attempt),
          );
        } catch {
          return sawUncertainFailure
            ? "uncertain"
            : "failed";
        }
      }
    }

    return sawUncertainFailure
      ? "uncertain"
      : "failed";
  }

  // ================================================
  // Permanent failure accounting
  // ================================================

  private handlePermanentFailure(
    batch: readonly BufferedEntry[],
    outcome: Exclude<
      WriteOutcome,
      "committed"
    >,
  ): void {
    const ordinaryCount = batch.filter(
      (entry) => !entry.diagnostic,
    ).length;

    // A failed diagnostic marker must never
    // create another diagnostic marker.

    if (ordinaryCount === 0) {
      return;
    }

    if (outcome === "uncertain") {
      // An ambiguous transaction may already have
      // committed. Do not report definite loss.

      this.pendingUncertainFailure =
        true;

      return;
    }

    logMetrics.increment(
      "droppedEntries",
      ordinaryCount,
    );

    this.pendingPersistenceFailure =
      true;
  }

  // ================================================
  // Transactional persistence
  // ================================================

  private async persistBatch(
    batch: readonly BufferedEntry[],
  ): Promise<BatchWriteResult> {
    if (batch.length === 0) {
      return {
        insertedCount: 0,
        confirmedCount: 0,
      };
    }

    const ids = batch.map(
      (entry) => entry.id,
    );

    if (
      new Set(ids).size !== ids.length
    ) {
      throw new LogWriterInvariantError(
        "Duplicate buffered record identities.",
      );
    }

    return prisma.$transaction(
      async (tx) => {
        /**
         * Global writer lock order:
         *
         * Deployment -> Build -> Sequence allocator
         *
         * Deployment-first locking matches the build
         * processor claim, renewal and settlement paths.
         */

        const fence = this.options.buildExecutionFence;

        const deploymentLocks = await tx.$queryRaw<
          Array<{ id: string; status: string }>
        >`
          SELECT
            "id",
            "status"::text AS "status"
          FROM "deployments"
          WHERE
            "id" = ${this.options.deploymentId}::uuid
            AND "workspaceId" = ${this.options.workspaceId}::uuid
            AND "projectId" = ${this.options.projectId}::uuid
          FOR UPDATE
        `;

        if (deploymentLocks.length !== 1) {
          if (fence) {
            throw new LogWriterOwnershipError();
          }

          throw new LogWriterInvariantError(
            "Deployment scope is invalid.",
          );
        }

        if (fence) {
          if (
            deploymentLocks[0]?.status !== "BUILDING" ||
            !this.options.buildId
          ) {
            throw new LogWriterOwnershipError();
          }

          const buildLocks = await tx.$queryRaw<
            Array<{ id: string }>
          >`
            SELECT "id"
            FROM "deployment_builds"
            WHERE
              "id" = ${this.options.buildId}::uuid
              AND "workspaceId" = ${this.options.workspaceId}::uuid
              AND "projectId" = ${this.options.projectId}::uuid
              AND "deploymentId" = ${this.options.deploymentId}::uuid
              AND "buildAttempt" = ${fence.buildAttempt}
              AND "status"::text = 'RUNNING'
              AND "executionLeaseToken" =
                ${fence.executionLeaseToken}::uuid
              AND "executionLeaseExpiresAt" >
                (clock_timestamp() AT TIME ZONE 'UTC')
            FOR UPDATE
          `;

          if (buildLocks.length !== 1) {
            throw new LogWriterOwnershipError();
          }
        }

        /**
         * Establish and lock the allocator row before
         * checking record identities.
         *
         * All writers for the same deployment must
         * acquire this lock before allocating sequences.
         */

        await tx.$executeRaw`
          INSERT INTO "deployment_log_sequences" (
            "workspaceId",
            "deploymentId",
            "value"
          )
          VALUES (
            ${this.options.workspaceId}::uuid,
            ${this.options.deploymentId}::uuid,
            0
          )
          ON CONFLICT (
            "workspaceId",
            "deploymentId"
          )
          DO NOTHING
        `;

        const allocatorRows =
          await tx.$queryRaw<
            Array<{ value: bigint }>
          >`
            SELECT "value"
            FROM "deployment_log_sequences"
            WHERE
              "workspaceId" =
                ${this.options.workspaceId}::uuid
              AND "deploymentId" =
                ${this.options.deploymentId}::uuid
            FOR UPDATE
          `;

        const allocator =
          allocatorRows[0];

        if (
          allocatorRows.length !== 1 ||
          !allocator ||
          typeof allocator.value !==
            "bigint" ||
          allocator.value < 0n ||
          allocator.value > MAX_SEQUENCE
        ) {
          throw new LogWriterInvariantError(
            "Invalid sequence allocator state.",
          );
        }

        // ==========================================
        // Verify deployment ownership scope
        // ==========================================

        const deployment =
          await tx.deployment.findFirst({
            where: {
              id: this.options.deploymentId,

              workspaceId:
                this.options.workspaceId,

              projectId:
                this.options.projectId,
            },

            select: {
              id: true,
            },
          });

        if (!deployment) {
          throw new LogWriterInvariantError(
            "Deployment scope is invalid.",
          );
        }

        // ==========================================
        // Stable-ID retry deduplication
        // ==========================================

        const existing =
          await tx.deploymentLogEntry.findMany({
            where: {
              id: {
                in: ids,
              },
            },

            select: {
              id: true,

              workspaceId: true,

              projectId: true,

              deploymentId: true,

              buildId: true,

              runtimeInstanceId: true,

              source: true,

              stream: true,

              level: true,

              message: true,

              eventTimestamp: true,

              sequence: true,
            },
          });

        const existingById = new Map(
          existing.map((entry) => [
            entry.id,
            entry,
          ]),
        );

        const missing: BufferedEntry[] =
          [];

        for (const entry of batch) {
          const persisted =
            existingById.get(entry.id);

          if (!persisted) {
            missing.push(entry);

            continue;
          }

          /**
           * Existing IDs must refer to the exact
           * same logical record and tenant scope.
           *
           * Never silently accept an ID collision.
           */

          if (
            persisted.workspaceId !==
              this.options.workspaceId ||
            persisted.projectId !==
              this.options.projectId ||
            persisted.deploymentId !==
              this.options.deploymentId ||
            persisted.buildId !==
              this.options.buildId ||
            persisted.runtimeInstanceId !==
              this.options.runtimeInstanceId ||
            persisted.source !==
              entry.source ||
            persisted.stream !==
              entry.stream ||
            persisted.level !==
              entry.level ||
            persisted.message !==
              entry.message ||
            persisted.eventTimestamp.getTime() !==
              entry.eventTimestamp.getTime() ||
            persisted.sequence <= 0n ||
            persisted.sequence >
              allocator.value
          ) {
            throw new LogWriterInvariantError(
              "Persisted record identity mismatch.",
            );
          }
        }

        /**
         * A batch committed before an ambiguous
         * connection failure needs no new sequences
         * and no duplicate notification outbox row.
         */

        if (missing.length === 0) {
          return {
            insertedCount: 0,

            confirmedCount:
              batch.length,
          };
        }

        // ==========================================
        // Allocate a contiguous sequence range
        // ==========================================

        const count =
          BigInt(missing.length);

        if (
          count >
          MAX_SEQUENCE - allocator.value
        ) {
          throw new LogWriterInvariantError(
            "Deployment log sequence exhausted.",
          );
        }

        if (fence) {
          const validLease = await tx.$queryRaw<
            Array<{ id: string }>
          >`
            SELECT "id"
            FROM "deployment_builds"
            WHERE
              "id" = ${this.options.buildId}::uuid
              AND "workspaceId" = ${this.options.workspaceId}::uuid
              AND "projectId" = ${this.options.projectId}::uuid
              AND "deploymentId" = ${this.options.deploymentId}::uuid
              AND "buildAttempt" = ${fence.buildAttempt}
              AND "status"::text = 'RUNNING'
              AND "executionLeaseToken" =
                ${fence.executionLeaseToken}::uuid
              AND "executionLeaseExpiresAt" >
                (clock_timestamp() AT TIME ZONE 'UTC')
          `;

          if (validLease.length !== 1) {
            throw new LogWriterOwnershipError();
          }
        }

        const startSequence =
          allocator.value + 1n;

        const maxSequence =
          allocator.value + count;

        const updatedRows =
          await tx.$queryRaw<
            Array<{ value: bigint }>
          >`
            UPDATE "deployment_log_sequences"
            SET
              "value" = ${maxSequence},
              "updatedAt" = now()
            WHERE
              "workspaceId" =
                ${this.options.workspaceId}::uuid
              AND "deploymentId" =
                ${this.options.deploymentId}::uuid
              AND "value" =
                ${allocator.value}
            RETURNING "value"
          `;

        if (
          updatedRows.length !== 1 ||
          updatedRows[0]?.value !==
            maxSequence
        ) {
          throw new LogWriterInvariantError(
            "Sequence allocation failed.",
          );
        }

        // ==========================================
        // Insert authoritative log records
        // ==========================================

        const inserted =
          await tx.deploymentLogEntry.createMany({
            data: missing.map(
              (entry, index) => ({
                id: entry.id,

                workspaceId:
                  this.options.workspaceId,

                projectId:
                  this.options.projectId,

                deploymentId:
                  this.options.deploymentId,

                buildId:
                  this.options.buildId,

                runtimeInstanceId:
                  this.options.runtimeInstanceId,

                source: entry.source,

                stream: entry.stream,

                level: entry.level,

                message: entry.message,

                sequence:
                  startSequence +
                  BigInt(index),

                eventTimestamp:
                  entry.eventTimestamp,
              }),
            ),
          });

        /**
         * Do not use skipDuplicates.
         *
         * Unexpected uniqueness conflicts must
         * roll back the allocator and log insert
         * instead of silently creating cursor gaps.
         */

        if (
          inserted.count !==
          missing.length
        ) {
          throw new LogWriterInvariantError(
            "Unexpected log insert count.",
          );
        }

        // ==========================================
        // Transactional notification outbox
        // ==========================================

        await tx.deploymentLogNotificationOutbox.create({
          data: {
            workspaceId:
              this.options.workspaceId,

            projectId:
              this.options.projectId,

            deploymentId:
              this.options.deploymentId,

            fromSequence:
              startSequence,

            toSequence:
              maxSequence,
          },
        });

        if (fence) {
          const validLease = await tx.$queryRaw<
            Array<{ id: string }>
          >`
            SELECT "id"
            FROM "deployment_builds"
            WHERE
              "id" = ${this.options.buildId}::uuid
              AND "workspaceId" = ${this.options.workspaceId}::uuid
              AND "projectId" = ${this.options.projectId}::uuid
              AND "deploymentId" = ${this.options.deploymentId}::uuid
              AND "buildAttempt" = ${fence.buildAttempt}
              AND "status"::text = 'RUNNING'
              AND "executionLeaseToken" =
                ${fence.executionLeaseToken}::uuid
              AND "executionLeaseExpiresAt" >
                (clock_timestamp() AT TIME ZONE 'UTC')
          `;

          if (validLease.length !== 1) {
            throw new LogWriterOwnershipError();
          }
        }

        return {
          insertedCount:
            inserted.count,

          confirmedCount:
            batch.length,
        };
      },

      {
        maxWait:
          TRANSACTION_MAX_WAIT_MS,

        timeout:
          TRANSACTION_TIMEOUT_MS,

        isolationLevel:
          "ReadCommitted",
      },
    );
  }

  // ================================================
  // Graceful shutdown
  // ================================================

  /**
   * Stop accepting records and drain the finite
   * remaining buffer.
   *
   * Concurrent stop callers share the same promise.
   *
   * Each transaction and retry policy is bounded,
   * but total shutdown time depends on the number
   * of pending batches and database availability.
   */

  async stop(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }

    if (this.state === "STOPPED") {
      return;
    }

    this.state = "STOPPING";

    this.clearTimer();

    this.stopPromise =
      this.finishStop();

    return this.stopPromise;
  }

  private async finishStop(): Promise<void> {
    try {
      if (this.flushing) {
        try {
          await this.flushing;
        } catch {
          // Continue draining the remaining
          // queued records.
        }
      }

      /**
       * No new producer records can enter after
       * STOPPING, so this buffer is finite.
       */

      while (this.buffer.length > 0) {
        await this.flush();
      }

      /**
       * One final best-effort diagnostic pass.
       *
       * Failed diagnostics cannot generate more
       * diagnostics, preventing recursive retries.
       */

      this.materializeDiagnostics();

      if (this.buffer.length > 0) {
        await this.flush();
      }
    } finally {
      this.clearTimer();

      if (this.buffer.length > 0) {
        const abandoned =
          this.buffer.splice(0);

        const ordinaryCount =
          abandoned.filter(
            (entry) =>
              !entry.diagnostic,
          ).length;

        logMetrics.increment(
          "droppedEntries",
          ordinaryCount,
        );

        this.releaseBatch(
          abandoned,
        );
      }

      this.queuedBytes = 0;

      this.pendingDropped = 0;

      this.pendingPersistenceFailure =
        false;

      this.pendingUncertainFailure =
        false;

      this.state = "STOPPED";
    }
  }
}
