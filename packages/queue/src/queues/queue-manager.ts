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
 *
 * Queue delivery is not an exactly-once execution
 * guarantee. Processors must independently enforce
 * ownership, execution leases, state transitions,
 * and safe recovery from uncertain external operations.
 */

import { createHash } from "node:crypto";

import {
  Queue,
  Worker,
  UnrecoverableError,
} from "bullmq";

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

const MAX_JOB_ID_LENGTH = 128;

const MAX_QUEUE_PREFIX_LENGTH = 128;

const MAX_JOB_PAYLOAD_BYTES = 16 * 1024;

/**
 * Default runtime execution budget.
 *
 * Runtime jobs require sufficient time for image
 * inspection, container creation, startup, readiness
 * verification, and database settlement.
 *
 * Explicit queue-specific configuration takes priority.
 */
const DEFAULT_RUNTIME_EXECUTION_TIMEOUT_MS = 180_000;

/**
 * Reject control characters before normalization.
 *
 * This prevents identifiers containing newlines, tabs,
 * null bytes, and other control characters from being
 * silently normalized into accepted identifiers.
 */
const CONTROL_CHARACTER_PATTERN =
  /[\u0000-\u001f\u007f-\u009f]/u;

const INVALID_QUEUE_IDENTIFIER_PATTERN =
  /[^A-Za-z0-9_-]/u;

const queueNameSet: ReadonlySet<string> =
  new Set(Object.values(QUEUE_NAMES));

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
   * Allows deployment, build, runtime, and other
   * jobs to use different execution deadlines,
   * retry budgets, and retention policies.
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

/**
 * Represents a failure that may be retried when the
 * processor has independently established retry safety.
 *
 * The presence of this error does not prove that an
 * external operation has stopped or that repeating
 * the operation is safe.
 */
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

/**
 * Cooperative execution deadline exceeded.
 *
 * This error is supplied through an AbortSignal.
 * It does not independently terminate external
 * processes or Docker daemon operations.
 */
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

/**
 * Raised when operations are requested after
 * queue manager shutdown has started.
 */
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

/**
 * Validate the Redis queue namespace.
 *
 * Prefixes must be canonical and must not contain
 * whitespace, separators, control characters,
 * or other unsupported characters.
 */
function validateQueuePrefix(
  prefix: unknown,
): asserts prefix is string {
  if (
    typeof prefix !== "string" ||
    prefix.length === 0 ||
    prefix.length > MAX_QUEUE_PREFIX_LENGTH ||
    INVALID_QUEUE_IDENTIFIER_PATTERN.test(prefix)
  ) {
    throw new Error(
      "Queue prefix must contain only letters, " +
      "numbers, '_' or '-' and be at most " +
      `${MAX_QUEUE_PREFIX_LENGTH} characters long`,
    );
  }
}

/**
 * Validate resolved queue policies before they
 * are used to create queues or workers.
 */
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

/**
 * Resolve a queue-specific policy against the
 * validated base policy.
 *
 * Runtime jobs receive a longer default execution
 * deadline unless an explicit runtime override
 * has been supplied.
 */
function resolveQueuePolicy(
  name: QueueName,
  basePolicy: ResolvedQueuePolicy,
  override?: QueuePolicyOverride,
): ResolvedQueuePolicy {
  const defaultExecutionTimeoutMs =
    name === QUEUE_NAMES.runtime
      ? Math.max(
          basePolicy.executionTimeoutMs,
          DEFAULT_RUNTIME_EXECUTION_TIMEOUT_MS,
        )
      : basePolicy.executionTimeoutMs;

  const resolved: ResolvedQueuePolicy =
    Object.freeze({
      attempts:
        override?.attempts ??
        basePolicy.attempts,

      backoffMs:
        override?.backoffMs ??
        basePolicy.backoffMs,

      completedRetention:
        override?.completedRetention ??
        basePolicy.completedRetention,

      failedRetention:
        override?.failedRetention ??
        basePolicy.failedRetention,

      executionTimeoutMs:
        override?.executionTimeoutMs ??
        defaultExecutionTimeoutMs,

      backoffJitter:
        override?.backoffJitter ??
        basePolicy.backoffJitter,
    });

  validateResolvedPolicy(resolved);

  return resolved;
}

/**
 * Ensure the serialized queue payload remains
 * within the configured metadata size limit.
 *
 * Queue payloads must not contain repository
 * contents, credentials, or large arbitrary data.
 */
function validatePayloadSize(
  payload: QueueJob,
): void {
  const serialized = JSON.stringify(payload);

  const size = Buffer.byteLength(
    serialized,
    "utf8",
  );

  if (size > MAX_JOB_PAYLOAD_BYTES) {
    throw new Error(
      "Queue payload exceeds the allowed size",
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
    validateQueuePrefix(this.prefix);

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

    /**
     * Reject unsupported queue names in the supplied
     * override configuration before resolving policies.
     */
    for (const name of Object.keys(
      policy.queueOverrides ?? {},
    )) {
      assertQueueName(name);
    }

    /**
     * Resolve and freeze every supported queue policy.
     *
     * Producer and worker operations use the same
     * policy resolution rules.
     */
    for (const name of Object.values(
      QUEUE_NAMES,
    )) {
      const override =
        policy.queueOverrides?.[name];

      const resolved = resolveQueuePolicy(
        name,
        this.basePolicy,
        override,
      );

      this.queuePolicies.set(
        name,
        resolved,
      );
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

  /**
   * Indicates that shutdown has started.
   *
   * This does not necessarily mean that every
   * active processor or external operation has
   * already terminated.
   */
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

  /**
   * Validate a custom BullMQ job identifier.
   *
   * Preserve existing whitespace normalization for
   * compatibility, while rejecting control characters
   * before trimming.
   */
  static validateJobId(
    jobId: string,
  ): string {
    if (typeof jobId !== "string") {
      throw new Error(
        "BullMQ job ID must be a string",
      );
    }

    if (
      CONTROL_CHARACTER_PATTERN.test(jobId)
    ) {
      throw new Error(
        "BullMQ job ID contains invalid control characters",
      );
    }

    const trimmed = jobId.trim();

    if (
      trimmed.length === 0 ||
      trimmed.length > MAX_JOB_ID_LENGTH ||
      INVALID_QUEUE_IDENTIFIER_PATTERN.test(
        trimmed,
      )
    ) {
      throw new Error(
        "BullMQ job IDs must contain only letters, " +
        "numbers, '_' or '-' and be at most " +
        `${MAX_JOB_ID_LENGTH} characters long`,
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
   *
   * This encoding is not collision-resistant.
   * Different input components can produce the
   * same normalized identifier.
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
          .replace(
            /[^A-Za-z0-9_-]/gu,
            "_",
          ),
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
          CONTROL_CHARACTER_PATTERN.test(
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

  /**
   * Validate a payload against its expected queue.
   *
   * The corresponding strict Zod schema rejects
   * unknown fields and invalid job kinds.
   *
   * This validation establishes payload structure,
   * not authorization to execute the operation.
   */
  static validatePayload(
    name: QueueName,

    payload: unknown,
  ): QueueJob {
    assertQueueName(name);

    const parsed =
      queuePayloadSchemas[name].parse(
        payload,
      );

    validatePayloadSize(parsed);

    return parsed;
  }

  /**
   * Validate incoming worker jobs independently
   * of producer-side validation.
   *
   * Jobs may originate from older producers,
   * malformed Redis data, or incorrectly routed
   * internal operations.
   *
   * Invalid payloads are permanent failures and
   * must not consume the automatic retry budget.
   *
   * Raw validation errors are intentionally not
   * exposed because they may contain unexpected
   * payload information.
   */
  private validateWorkerJob(
    name: QueueName,
    job: Job<QueueJob>,
  ): QueueJob {
    if (job.name !== name) {
      throw new UnrecoverableError(
        "Queue job name does not match the expected queue",
      );
    }

    try {
      return QueueManager.validatePayload(
        name,
        job.data,
      );
    } catch {
      throw new UnrecoverableError(
        "Invalid queue job payload",
      );
    }
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
  // Retry policy
  // ==================================================

  /**
   * Resolve the maximum automatic attempts for
   * an individual validated job.
   *
   * Runtime lifecycle operations are deliberately
   * limited to a single automatic attempt.
   *
   * A successful Docker restart followed by a
   * database failure must not automatically cause
   * another restart.
   *
   * BullMQ stalled-job recovery remains possible.
   * Runtime processors must still implement durable
   * operation identity and execution fencing.
   */
  private resolveJobAttempts(
    payload: QueueJob,
    policy: ResolvedQueuePolicy,
  ): number {
    switch (payload.kind) {
      case "diagnostic":
        return payload.permanentFailure
          ? 1
          : policy.attempts;

      case "deployment":
        return policy.attempts;

      case "build":
        return policy.attempts;

      case "runtime":
        // Recovery replays reconcile a durable intent and cannot dispatch it twice.
        return payload.action === "deploy" || payload.action === "recover"
          ? policy.attempts
          : 1;

      case "rollback":
        return 1;

      default: {
        const unsupportedJob: never =
          payload;

        void unsupportedJob;

        throw new Error(
          "Unsupported queue job kind",
        );
      }
    }
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

    const attempts =
      this.resolveJobAttempts(
        parsed,
        policy,
      );

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
        sizeLimit: MAX_JOB_PAYLOAD_BYTES,
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
        /**
         * Validate the incoming job before creating
         * an execution controller or invoking the
         * application processor.
         *
         * Use the parsed payload so schema defaults,
         * normalization, and validation results are
         * consistently applied on both boundaries.
         */
        const validatedPayload =
          this.validateWorkerJob(
            name,
            job,
          );

        job.data = validatedPayload;

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
           * to downloads, build executors, runtime
           * executors, database orchestration, and
           * applicable cleanup operations.
           *
           * Aborting this signal does not itself
           * terminate an external build process
           * or Docker daemon operation.
           *
           * Processors must reconcile uncertain
           * external operation outcomes before
           * authorizing another attempt.
           *
           * Do not detach the processor using
           * Promise.race(). The processor may
           * still own an active database lease
           * or external operation.
           *
           * Do not automatically convert a settled
           * successful result into a failure merely
           * because cancellation occurred near
           * the end of execution.
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
         *
         * A valid BullMQ lock does not independently
         * authorize changes to deployment state or
         * external Docker resources.
         */
        lockDuration: Math.max(
          30_000,
          policy.executionTimeoutMs * 2,
        ),

        prefix: this.prefix,

        /**
         * Stalled jobs may be recovered even when
         * their automatic failure attempts are
         * limited to one.
         *
         * Durable processor-level idempotency
         * and fencing remain mandatory.
         */
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
   *
   * Shutdown requests cooperative cancellation of
   * active processors and waits for BullMQ workers
   * to close.
   *
   * The configured deadline bounds the caller's
   * waiting time. It does not guarantee termination
   * of all external operations.
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

  /**
   * Bound the amount of time callers wait for
   * graceful resource shutdown.
   *
   * If the deadline expires, closeResources()
   * may continue running in the background.
   *
   * Do not forcibly disconnect active workers
   * merely to report a successful shutdown.
   *
   * The owning service must handle shutdown
   * failures and preserve recoverable state.
   */
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

  /**
   * Close workers, queues, and owned Redis
   * connections in the appropriate order.
   *
   * Workers must finish or safely settle active
   * processing before producer queues and owned
   * Redis connections are closed.
   */
  private async closeResources(): Promise<void> {
    const failures: unknown[] = [];

    /**
     * Stop active processors cooperatively.
     *
     * Processors must handle interruption without
     * publishing an invalid success or losing
     * recoverable execution state.
     *
     * External Docker operations may continue
     * after cancellation has been requested.
     */
    for (const controller of this.activeExecutions) {
      controller.abort(
        new QueueManagerClosedError(),
      );
    }

    // ==================================================
    // Close workers
    // ==================================================

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

    // ==================================================
    // Close producer queues
    // ==================================================

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

    // ==================================================
    // Close owned Redis connections
    // ==================================================

    /**
     * Only close Redis connections when this
     * manager explicitly owns their lifecycle.
     *
     * Shared connections must remain under the
     * control of their original owner.
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

    // ==================================================
    // Report shutdown failures
    // ==================================================

    /**
     * Preserve all shutdown failures rather than
     * silently reporting successful resource cleanup.
     */
    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        "One or more queue resources failed to close",
      );
    }
  }
}
