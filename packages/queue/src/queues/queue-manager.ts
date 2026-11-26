
/**
 * CloudPilot — Queue Manager
 *
 * Central BullMQ infrastructure responsible for:
 *
 * - Queue lifecycle management
 * - Runtime payload validation
 * - Deterministic job identifiers
 * - Queue-specific execution policies
 * - Worker creation
 * - Cooperative execution cancellation
 * - Retry configuration
 * - Graceful shutdown
 *
 * Database-backed idempotency and execution fencing
 * remain authoritative for deployment correctness.
 */

import { createHash } from "node:crypto";

import { Queue, Worker } from "bullmq";

import type {
  Job,
  Processor,
} from "bullmq";

import type {
  Redis as IORedis,
} from "ioredis";

import {
  queuePayloadSchemas,
  type QueueJob,
} from "../contracts/jobs.js";

import {
  QUEUE_NAMES,
  type QueueName,
} from "../config/queues.js";

// ==================================================
// Constants
// ==================================================

const MAX_QUEUE_ATTEMPTS = 20;

const MAX_WORKER_CONCURRENCY = 100;

const MIN_EXECUTION_TIMEOUT_MS = 100;

const MAX_EXECUTION_TIMEOUT_MS = 3_600_000;

const DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;

const MAX_SHUTDOWN_TIMEOUT_MS = 300_000;

const queueNameSet: ReadonlySet<string> =
  new Set(Object.keys(queuePayloadSchemas));

// ==================================================
// Queue policies
// ==================================================

export interface QueuePolicy {
  attempts: number;

  backoffMs: number;

  completedRetention: number;

  failedRetention: number;

  executionTimeoutMs: number;

  backoffJitter?: number;

  /**
   * Optional queue-specific overrides.
   *
   * Allows build jobs to use different execution
   * deadlines and retry budgets.
   */
  queueOverrides?: Partial<
    Record<QueueName, QueuePolicyOverride>
  >;

  shutdownTimeoutMs?: number;
}

export interface QueuePolicyOverride {
  attempts?: number;

  backoffMs?: number;

  completedRetention?: number;

  failedRetention?: number;

  executionTimeoutMs?: number;

  backoffJitter?: number;
}

interface ResolvedQueuePolicy {
  readonly attempts: number;

  readonly backoffMs: number;

  readonly completedRetention: number;

  readonly failedRetention: number;

  readonly executionTimeoutMs: number;

  readonly backoffJitter: number;
}

const defaultQueuePolicy: Readonly<QueuePolicy> =
  Object.freeze({
    attempts: 3,

    backoffMs: 1000,

    completedRetention: 1000,

    failedRetention: 5000,

    executionTimeoutMs: 30_000,

    backoffJitter: 0.25,

    shutdownTimeoutMs:
      DEFAULT_SHUTDOWN_TIMEOUT_MS,
  });

// ==================================================
// Queue errors
// ==================================================

export class TransientJobError extends Error {
  constructor(
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);

    this.name = "TransientJobError";

    Error.captureStackTrace?.(
      this,
      TransientJobError,
    );
  }
}

export class QueueExecutionTimeoutError extends Error {
  constructor() {
    super("Execution deadline exceeded");

    this.name = "QueueExecutionTimeoutError";

    Error.captureStackTrace?.(
      this,
      QueueExecutionTimeoutError,
    );
  }
}

export class QueueManagerClosedError extends Error {
  constructor() {
    super("Queue manager is shutting down or closed");

    this.name = "QueueManagerClosedError";

    Error.captureStackTrace?.(
      this,
      QueueManagerClosedError,
    );
  }
}

// ==================================================
// Validation utilities
// ==================================================

function assertIntegerInRange(
  value: number,
  minimum: number,
  maximum: number,
  name: string,
): void {
  if (
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    throw new Error(
      `${name} must be an integer between ` +
      `${minimum} and ${maximum}`,
    );
  }
}

function assertQueueName(
  name: unknown,
): asserts name is QueueName {
  if (
    typeof name !== "string" ||
    !queueNameSet.has(name)
  ) {
    throw new Error("Unsupported queue name");
  }
}

function validateResolvedPolicy(
  policy: ResolvedQueuePolicy,
): void {
  assertIntegerInRange(
    policy.attempts,
    1,
    MAX_QUEUE_ATTEMPTS,
    "Queue attempts",
  );

  assertIntegerInRange(
    policy.backoffMs,
    100,
    3_600_000,
    "Queue backoff",
  );

  assertIntegerInRange(
    policy.completedRetention,
    1,
    Number.MAX_SAFE_INTEGER,
    "Completed retention",
  );

  assertIntegerInRange(
    policy.failedRetention,
    1,
    Number.MAX_SAFE_INTEGER,
    "Failed retention",
  );

  assertIntegerInRange(
    policy.executionTimeoutMs,
    MIN_EXECUTION_TIMEOUT_MS,
    MAX_EXECUTION_TIMEOUT_MS,
    "Execution timeout",
  );

  if (
    !Number.isFinite(policy.backoffJitter) ||
    policy.backoffJitter < 0 ||
    policy.backoffJitter > 1
  ) {
    throw new Error(
      "Queue backoff jitter must be between 0 and 1",
    );
  }
}

// ==================================================
// Queue manager
// ==================================================

export class QueueManager {
  readonly queues: Partial<
    Record<QueueName, Queue<QueueJob>>
  > = {};

  readonly workers: Worker<QueueJob>[] = [];

  private readonly basePolicy:
    ResolvedQueuePolicy;

  private readonly queuePolicies =
    new Map<QueueName, ResolvedQueuePolicy>();

  private readonly shutdownTimeoutMs: number;

  private readonly activeExecutions =
    new Set<AbortController>();

  private closed = false;

  private closePromise: Promise<void> | null =
    null;

  constructor(
    private readonly producerConnection: IORedis,

    private readonly workerConnection: IORedis,

    policy: QueuePolicy = defaultQueuePolicy,

    private readonly closeConnections = false,

    private readonly prefix = "cloudpilot",
  ) {
    if (
      !/^[A-Za-z0-9_-]{1,128}$/u.test(
        this.prefix,
      )
    ) {
      throw new Error(
        "Queue prefix must contain only letters, " +
        "numbers, '_' or '-'",
      );
    }

    this.basePolicy = Object.freeze({
      attempts: policy.attempts,

      backoffMs: policy.backoffMs,

      completedRetention:
        policy.completedRetention,

      failedRetention:
        policy.failedRetention,

      executionTimeoutMs:
        policy.executionTimeoutMs,

      backoffJitter:
        policy.backoffJitter ?? 0.25,
    });

    validateResolvedPolicy(this.basePolicy);

    this.shutdownTimeoutMs =
      policy.shutdownTimeoutMs ??
      DEFAULT_SHUTDOWN_TIMEOUT_MS;

    assertIntegerInRange(
      this.shutdownTimeoutMs,
      1000,
      MAX_SHUTDOWN_TIMEOUT_MS,
      "Queue shutdown timeout",
    );

    for (const [name, override] of Object.entries(
      policy.queueOverrides ?? {},
    )) {
      assertQueueName(name);

      if (!override) {
        continue;
      }

      const resolved: ResolvedQueuePolicy =
        Object.freeze({
          attempts:
            override.attempts ??
            this.basePolicy.attempts,

          backoffMs:
            override.backoffMs ??
            this.basePolicy.backoffMs,

          completedRetention:
            override.completedRetention ??
            this.basePolicy.completedRetention,

          failedRetention:
            override.failedRetention ??
            this.basePolicy.failedRetention,

          executionTimeoutMs:
            override.executionTimeoutMs ??
            this.basePolicy.executionTimeoutMs,

          backoffJitter:
            override.backoffJitter ??
            this.basePolicy.backoffJitter,
        });

      validateResolvedPolicy(resolved);

      this.queuePolicies.set(name, resolved);
    }
  }

  // ==================================================
  // Lifecycle validation
  // ==================================================

  private assertOpen(): void {
    if (this.closed) {
      throw new QueueManagerClosedError();
    }
  }

  get isClosed(): boolean {
    return this.closed;
  }

  // ==================================================
  // Queue-specific policy
  // ==================================================

  private getPolicy(
    name: QueueName,
  ): ResolvedQueuePolicy {
    assertQueueName(name);

    return (
      this.queuePolicies.get(name) ??
      this.basePolicy
    );
  }

  // ==================================================
  // Job identifier validation
  // ==================================================

  static validateJobId(
    jobId: string,
  ): string {
    if (typeof jobId !== "string") {
      throw new Error(
        "BullMQ job ID must be a string",
      );
    }

    const trimmed = jobId.trim();

    if (
      !/^[A-Za-z0-9_-]{1,128}$/u.test(
        trimmed,
      )
    ) {
      throw new Error(
        "BullMQ job IDs must contain only letters, " +
        "numbers, '_' or '-' and be at most " +
        "128 characters long",
      );
    }

    /**
     * BullMQ reserves integer-only identifiers
     * for its internally generated job IDs.
     */
    if (/^\d+$/u.test(trimmed)) {
      throw new Error(
        "Custom BullMQ job IDs cannot be integer-only",
      );
    }

    return trimmed;
  }

  /**
   * Legacy deterministic ID format.
   *
   * Preserved for compatibility with existing
   * outbox events and previously scheduled jobs.
   *
   * Do not use arbitrary untrusted identifiers
   * with this legacy encoding.
   */
  static deterministicJobId(
    queueName: QueueName,

    parts: readonly (string | number)[],
  ): string {
    assertQueueName(queueName);

    const encoded = parts
      .map((part) =>
        String(part)
          .trim()
          .replace(/[^A-Za-z0-9_-]/gu, "_"),
      )
      .filter((part) => part.length > 0)
      .join("-");

    return QueueManager.validateJobId(
      `${queueName}-${encoded}`,
    );
  }

  /**
   * Collision-resistant deterministic identifier.
   *
   * Structured serialization preserves boundaries
   * between individual identifier components.
   *
   * Existing producers must not migrate to this
   * format without coordinating outbox replay
   * and previously scheduled job identifiers.
   */
  static hashedDeterministicJobId(
    queueName: QueueName,

    parts: readonly (string | number)[],
  ): string {
    assertQueueName(queueName);

    if (
      !Array.isArray(parts) ||
      parts.length === 0 ||
      parts.length > 32
    ) {
      throw new Error(
        "Invalid deterministic job ID components",
      );
    }

    for (const part of parts) {
      if (typeof part === "string") {
        if (
          part.length === 0 ||
          part.length > 1024 ||
          part.trim().length === 0 ||
          /[\u0000-\u001f\u007f-\u009f]/u.test(
            part,
          )
        ) {
          throw new Error(
            "Invalid job ID string component",
          );
        }

        continue;
      }

      if (
        typeof part !== "number" ||
        !Number.isSafeInteger(part)
      ) {
        throw new Error(
          "Invalid job ID numeric component",
        );
      }
    }

    const digest = createHash("sha256")
      .update(
        JSON.stringify([
          "cloudpilot-job-v1",
          queueName,
          parts,
        ]),
        "utf8",
      )
      .digest("hex");

    return QueueManager.validateJobId(
      `${queueName}-v1-${digest}`,
    );
  }

  // ==================================================
  // Payload validation
  // ==================================================

  static validatePayload(
    name: QueueName,

    payload: unknown,
  ): QueueJob {
    assertQueueName(name);

    return queuePayloadSchemas[name].parse(
      payload,
    );
  }

  // ==================================================
  // Queue creation
  // ==================================================

  getQueue(
    name: QueueName,
  ): Queue<QueueJob> {
    this.assertOpen();

    assertQueueName(name);

    const existing = this.queues[name];

    if (existing) {
      return existing;
    }

    const queue = new Queue<QueueJob>(
      name,
      {
        connection:
          this.producerConnection,

        prefix: this.prefix,
      },
    );

    this.queues[name] = queue;

    return queue;
  }

  // ==================================================
  // Job scheduling
  // ==================================================

  addJob(
    name: QueueName,

    payload: QueueJob,

    jobId: string,
  ): Promise<Job<QueueJob>> {
    this.assertOpen();

    const parsed =
      QueueManager.validatePayload(
        name,
        payload,
      );

    const validatedJobId =
      QueueManager.validateJobId(jobId);

    const policy = this.getPolicy(name);

    let attempts = 1;

    switch (parsed.kind) {
      case "diagnostic":
        attempts = parsed.permanentFailure
          ? 1
          : policy.attempts;

        break;

      case "deployment":
      case "build":
        attempts = policy.attempts;

        break;

      case "rollback":
        attempts = 1;

        break;
    }

    return this.getQueue(name).add(
      name,
      parsed,
      {
        jobId: validatedJobId,

        attempts,

        backoff: {
          type: "exponential",

          delay: policy.backoffMs,

          jitter: policy.backoffJitter,
        },

        removeOnComplete: {
          count: policy.completedRetention,
        },

        removeOnFail: {
          count: policy.failedRetention,
        },

        /**
         * Queue payloads contain bounded identifiers
         * and metadata, never repository contents.
         */
        sizeLimit: 16 * 1024,
      },
    );
  }

  // ==================================================
  // Worker creation
  // ==================================================

  createWorker(
    name: QueueName,

    processor: Processor<QueueJob>,

    concurrency: number,
  ): Worker<QueueJob> {
    this.assertOpen();

    assertQueueName(name);

    assertIntegerInRange(
      concurrency,
      1,
      MAX_WORKER_CONCURRENCY,
      "Worker concurrency",
    );

    const policy = this.getPolicy(name);

    const wrappedProcessor: Processor<QueueJob> =
      async (job, token, signal) => {
        const executionController =
          new AbortController();

        this.activeExecutions.add(
          executionController,
        );

        /**
         * Combine BullMQ cancellation with
         * CloudPilot's execution deadline and
         * shutdown cancellation.
         */
        const executionSignal = signal
          ? AbortSignal.any([
              signal,
              executionController.signal,
            ])
          : executionController.signal;

        const deadline = setTimeout(() => {
          executionController.abort(
            new QueueExecutionTimeoutError(),
          );
        }, policy.executionTimeoutMs);

        deadline.unref?.();

        try {
          if (this.closed) {
            executionController.abort(
              new QueueManagerClosedError(),
            );
          }

          executionSignal.throwIfAborted();

          /**
           * The processor must propagate the signal
           * to downloads, build executors, database
           * orchestration, and cleanup operations.
           *
           * Aborting this signal does not itself
           * terminate an external build process.
           */
          return await processor(
            job,
            token,
            executionSignal,
          );
        } finally {
          clearTimeout(deadline);

          this.activeExecutions.delete(
            executionController,
          );
        }
      };

    const worker = new Worker<QueueJob>(
      name,
      wrappedProcessor,
      {
        connection:
          this.workerConnection,

        concurrency,

        /**
         * Preserve the existing lock policy.
         *
         * BullMQ lock renewal and database execution
         * leases remain separate mechanisms.
         */
        lockDuration: Math.max(
          30_000,
          policy.executionTimeoutMs * 2,
        ),

        prefix: this.prefix,

        maxStalledCount: 1,
      },
    );

    this.workers.push(worker);

    return worker;
  }

  // ==================================================
  // Graceful shutdown
  // ==================================================

  /**
   * Stop accepting new operations and close resources.
   *
   * Concurrent callers share the same shutdown promise.
   */
  close(): Promise<void> {
    if (this.closePromise) {
      return this.closePromise;
    }

    this.closed = true;

    this.closePromise =
      this.closeWithDeadline();

    return this.closePromise;
  }

  private async closeWithDeadline(): Promise<void> {
    let timeout:
      | ReturnType<typeof setTimeout>
      | undefined;

    const boundedClose =
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => {
          reject(
            new Error(
              "Queue shutdown timeout exceeded",
            ),
          );
        }, this.shutdownTimeoutMs);
      });

    try {
      await Promise.race([
        this.closeResources(),
        boundedClose,
      ]);
    } finally {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }
    }
  }

  private async closeResources(): Promise<void> {
    const failures: unknown[] = [];

    /**
     * Stop active processors cooperatively.
     *
     * Processors must handle interruption without
     * publishing an invalid success or losing
     * recoverable execution state.
     */
    for (const controller of this.activeExecutions) {
      controller.abort(
        new QueueManagerClosedError(),
      );
    }

    const workerResults =
      await Promise.allSettled(
        this.workers.map((worker) =>
          worker.close(),
        ),
      );

    for (const result of workerResults) {
      if (result.status === "rejected") {
        failures.push(result.reason);
      }
    }

    const queueResults =
      await Promise.allSettled(
        Object.values(this.queues).map(
          (queue) => queue.close(),
        ),
      );

    for (const result of queueResults) {
      if (result.status === "rejected") {
        failures.push(result.reason);
      }
    }

    /**
     * Only close Redis connections when this
     * manager explicitly owns their lifecycle.
     */
    if (this.closeConnections) {
      const connections = new Set([
        this.producerConnection,
        this.workerConnection,
      ]);

      const connectionResults =
        await Promise.allSettled(
          [...connections].map(
            async (connection) => {
              if (
                connection.status === "end" ||
                connection.status === "close"
              ) {
                return;
              }

              try {
                await connection.quit();
              } catch (error) {
                connection.disconnect();

                throw error;
              }
            },
          ),
        );

      for (const result of connectionResults) {
        if (result.status === "rejected") {
          failures.push(result.reason);
        }
      }
    }

    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        "One or more queue resources failed to close",
      );
    }
  }
}
