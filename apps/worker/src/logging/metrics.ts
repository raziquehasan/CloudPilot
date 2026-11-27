
/**
 * CloudPilot — Metrics-Ready Internal Counters
 *
 * Phase 5.6 — Real-Time Deployment Logs & Streaming Engine
 *
 * Lightweight, dependency-free, in-process metrics prepared
 * for Phase 6 monitoring.
 *
 * No Prometheus or Grafana infrastructure is introduced here.
 *
 * Design properties:
 *
 * - Fixed, low-cardinality metric names.
 * - Explicit counter and gauge semantics.
 * - Runtime metric name validation.
 * - Non-negative metric values.
 * - Safe numeric bounds and overflow protection.
 * - Integer validation for count-based metrics.
 * - Fractional millisecond duration support.
 * - Immutable metric snapshots.
 * - Dependency-free process-wide registry.
 *
 * Metrics must never include deployment IDs, container IDs,
 * workspace IDs, user IDs, secret values or raw log content.
 *
 * Metrics are process-local and reset when the process
 * restarts. They are not durable accounting records.
 */

// ==================================================
// Public metric contracts
// ==================================================

export interface LogEngineCounters {
  collectedEntries: number;

  persistedEntries: number;

  droppedEntries: number;

  redactedEntries: number;

  bufferedBytes: number;

  batchWriteCount: number;

  batchWriteDurationMsTotal: number;

  databaseWriteFailures: number;

  redisPublishFailures: number;

  activeCollectors: number;

  activeSubscriptions: number;

  reconnectAttempts: number;

  retentionDeletions: number;
}

export type LogCounterName = keyof LogEngineCounters;

/**
 * Gauges represent current state and may increase
 * or decrease during normal operation.
 */

export type LogGaugeName =
  | "bufferedBytes"
  | "activeCollectors"
  | "activeSubscriptions";

/**
 * Monotonic counters represent accumulated activity.
 */

export type LogMonotonicCounterName = Exclude<
  LogCounterName,
  LogGaugeName
>;

export type LogMetricKind = "counter" | "gauge";

export interface LogMetricSummary {
  averageBatchWriteDurationMs: number;

  persistenceRatio: number;

  dropRatio: number;

  redactionRatio: number;
}

// ==================================================
// Numeric safety limits
// ==================================================

const MAX_METRIC_VALUE = Number.MAX_SAFE_INTEGER;

const DURATION_METRIC: LogCounterName =
  "batchWriteDurationMsTotal";

// ==================================================
// Metric definitions
// ==================================================

/**
 * Fixed registry of supported metric names.
 *
 * Never derive metric names from external input.
 */

const METRIC_KINDS = Object.freeze({
  collectedEntries: "counter",

  persistedEntries: "counter",

  droppedEntries: "counter",

  redactedEntries: "counter",

  bufferedBytes: "gauge",

  batchWriteCount: "counter",

  batchWriteDurationMsTotal: "counter",

  databaseWriteFailures: "counter",

  redisPublishFailures: "counter",

  activeCollectors: "gauge",

  activeSubscriptions: "gauge",

  reconnectAttempts: "counter",

  retentionDeletions: "counter",
} satisfies Record<LogCounterName, LogMetricKind>);

const METRIC_NAMES: readonly LogCounterName[] =
  Object.freeze(
    Object.keys(
      METRIC_KINDS,
    ) as LogCounterName[],
  );

// ==================================================
// Registry initialization
// ==================================================

function createCounters(): LogEngineCounters {
  return {
    collectedEntries: 0,

    persistedEntries: 0,

    droppedEntries: 0,

    redactedEntries: 0,

    bufferedBytes: 0,

    batchWriteCount: 0,

    batchWriteDurationMsTotal: 0,

    databaseWriteFailures: 0,

    redisPublishFailures: 0,

    activeCollectors: 0,

    activeSubscriptions: 0,

    reconnectAttempts: 0,

    retentionDeletions: 0,
  };
}

// ==================================================
// Validation helpers
// ==================================================

export function isLogMetricName(
  value: unknown,
): value is LogCounterName {
  return (
    typeof value === "string" &&
    Object.prototype.hasOwnProperty.call(
      METRIC_KINDS,
      value,
    )
  );
}

export function isLogGaugeName(
  value: unknown,
): value is LogGaugeName {
  return (
    isLogMetricName(value) &&
    METRIC_KINDS[value] === "gauge"
  );
}

export function isLogMonotonicCounterName(
  value: unknown,
): value is LogMonotonicCounterName {
  return (
    isLogMetricName(value) &&
    METRIC_KINDS[value] === "counter"
  );
}

/**
 * Count-based metrics must use safe integers.
 *
 * Duration metrics may contain fractional milliseconds
 * because high-resolution timers produce decimal values.
 */

function isValidMetricAmount(
  name: LogCounterName,
  value: number,
): boolean {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    Math.abs(value) > MAX_METRIC_VALUE
  ) {
    return false;
  }

  if (name === DURATION_METRIC) {
    return true;
  }

  return Number.isSafeInteger(value);
}

// ==================================================
// Safe arithmetic
// ==================================================

/**
 * Saturating addition for non-negative metrics.
 *
 * Values never exceed the safe numeric boundary or
 * become negative.
 *
 * Saturation protects the logging engine from invalid
 * arithmetic. Once saturated, a counter no longer
 * accurately represents additional activity.
 */

function safeAdd(
  current: number,
  amount: number,
): number {
  if (
    amount > 0 &&
    amount > MAX_METRIC_VALUE - current
  ) {
    return MAX_METRIC_VALUE;
  }

  if (
    amount < 0 &&
    -amount > current
  ) {
    return 0;
  }

  const next = current + amount;

  if (!Number.isFinite(next)) {
    return amount > 0
      ? MAX_METRIC_VALUE
      : 0;
  }

  return Math.min(
    MAX_METRIC_VALUE,
    Math.max(0, next),
  );
}

function safeRatio(
  numerator: number,
  denominator: number,
): number {
  if (
    denominator <= 0 ||
    numerator < 0
  ) {
    return 0;
  }

  const ratio =
    numerator / denominator;

  return Number.isFinite(ratio)
    ? ratio
    : 0;
}

// ==================================================
// Metrics registry
// ==================================================

export class LogMetrics {
  private readonly counters:
    LogEngineCounters = createCounters();

  // ================================================
  // Increment metric
  // ================================================

  /**
   * Increment a metric by a validated amount.
   *
   * Monotonic counters reject negative increments.
   *
   * Gauges accept negative increments for backward
   * compatibility with existing collector and buffer
   * accounting.
   *
   * Invalid inputs are ignored rather than allowing
   * metrics failures to interrupt log collection.
   */

  increment(
    name: LogCounterName,
    amount = 1,
  ): void {
    if (!isLogMetricName(name)) {
      return;
    }

    if (
      !isValidMetricAmount(
        name,
        amount,
      )
    ) {
      return;
    }

    if (
      METRIC_KINDS[name] === "counter" &&
      amount < 0
    ) {
      return;
    }

    this.counters[name] = safeAdd(
      this.counters[name],
      amount,
    );
  }

  // ================================================
  // Set metric value
  // ================================================

  /**
   * Set an absolute metric value.
   *
   * Gauges may be assigned any valid non-negative
   * integer.
   *
   * For backward compatibility, counters may also
   * be assigned an absolute value, but they cannot
   * move backwards through this method.
   *
   * Explicit counter resets must use reset().
   */

  set(
    name: LogCounterName,
    value: number,
  ): void {
    if (!isLogMetricName(name)) {
      return;
    }

    if (
      !isValidMetricAmount(
        name,
        value,
      ) ||
      value < 0
    ) {
      return;
    }

    if (
      METRIC_KINDS[name] === "counter" &&
      value < this.counters[name]
    ) {
      return;
    }

    this.counters[name] =
      Math.min(
        value,
        MAX_METRIC_VALUE,
      );
  }

  // ================================================
  // Explicit gauge management
  // ================================================

  /**
   * Increase a current-state gauge.
   */

  incrementGauge(
    name: LogGaugeName,
    amount = 1,
  ): void {
    if (
      !isLogGaugeName(name) ||
      !Number.isSafeInteger(amount) ||
      amount < 0
    ) {
      return;
    }

    this.increment(
      name,
      amount,
    );
  }

  /**
   * Decrease a current-state gauge.
   *
   * Gauge underflow is clamped to zero.
   */

  decrementGauge(
    name: LogGaugeName,
    amount = 1,
  ): void {
    if (
      !isLogGaugeName(name) ||
      !Number.isSafeInteger(amount) ||
      amount < 0
    ) {
      return;
    }

    this.increment(
      name,
      -amount,
    );
  }

  /**
   * Set an absolute current-state gauge.
   */

  setGauge(
    name: LogGaugeName,
    value: number,
  ): void {
    if (!isLogGaugeName(name)) {
      return;
    }

    this.set(
      name,
      value,
    );
  }

  // ================================================
  // Batch write observations
  // ================================================

  /**
   * Record one completed batch-write observation.
   *
   * Fractional millisecond durations are supported.
   *
   * Callers must consistently define whether this
   * observation represents successful writes only
   * or all completed write attempts.
   */

  observeBatchWrite(
    durationMs: number,
  ): void {
    if (
      !isValidMetricAmount(
        DURATION_METRIC,
        durationMs,
      ) ||
      durationMs < 0
    ) {
      return;
    }

    this.increment(
      "batchWriteCount",
      1,
    );

    this.increment(
      "batchWriteDurationMsTotal",
      durationMs,
    );
  }

  // ================================================
  // Read individual metrics
  // ================================================

  /**
   * Return the current value of a metric.
   *
   * Invalid runtime names return zero.
   */

  get(name: LogCounterName): number {
    if (!isLogMetricName(name)) {
      return 0;
    }

    return this.counters[name];
  }

  /**
   * Return metric semantics for future monitoring
   * adapters.
   */

  getMetricKind(
    name: LogCounterName,
  ): LogMetricKind | undefined {
    if (!isLogMetricName(name)) {
      return undefined;
    }

    return METRIC_KINDS[name];
  }

  // ================================================
  // Immutable metrics snapshot
  // ================================================

  /**
   * Return a detached, runtime-immutable snapshot.
   *
   * Consumers cannot mutate the internal registry
   * through this object.
   */

  snapshot(): Readonly<LogEngineCounters> {
    return Object.freeze({
      ...this.counters,
    });
  }

  // ================================================
  // Derived metrics
  // ================================================

  /**
   * Return the arithmetic mean of observed batch
   * write durations in milliseconds.
   *
   * This is not a percentile or latency histogram.
   */

  getAverageBatchWriteDurationMs(): number {
    return safeRatio(
      this.counters.batchWriteDurationMsTotal,
      this.counters.batchWriteCount,
    );
  }

  /**
   * Return derived process-local metrics.
   *
   * Ratios are intentionally not clamped to one.
   * Values above one can indicate inconsistent
   * accounting, retries or different observation
   * boundaries.
   *
   * These are diagnostic ratios, not guaranteed
   * delivery or security success rates.
   */

  summary(): Readonly<LogMetricSummary> {
    const counters = this.snapshot();

    return Object.freeze({
      averageBatchWriteDurationMs:
        safeRatio(
          counters.batchWriteDurationMsTotal,
          counters.batchWriteCount,
        ),

      persistenceRatio:
        safeRatio(
          counters.persistedEntries,
          counters.collectedEntries,
        ),

      dropRatio:
        safeRatio(
          counters.droppedEntries,
          counters.collectedEntries,
        ),

      redactionRatio:
        safeRatio(
          counters.redactedEntries,
          counters.collectedEntries,
        ),
    });
  }

  // ================================================
  // Registry reset
  // ================================================

  /**
   * Reset all metrics.
   *
   * Intended primarily for isolated tests and
   * controlled registry initialization.
   *
   * Do not reset a live registry during active
   * collection or normal monitoring scrapes.
   */

  reset(): void {
    for (const name of METRIC_NAMES) {
      this.counters[name] = 0;
    }
  }
}

// ==================================================
// Process-wide metrics singleton
// ==================================================

/**
 * Multiple logging components within the same process
 * share this fixed, low-cardinality registry.
 *
 * Separate API and worker processes maintain independent
 * metric registries. Future monitoring infrastructure
 * must collect and aggregate them appropriately.
 */

export const logMetrics = new LogMetrics();
