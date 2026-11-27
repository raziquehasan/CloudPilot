
/**
 * CloudPilot — Collector Ownership Lock
 *
 * Phase 5.6 — Real-Time Deployment Logs & Streaming Engine
 *
 * A dedicated, bounded Redis ownership lease coordinating
 * which worker collects a given runtime log stream.
 *
 * This lock is independent of deployment execution leases.
 * Runtime log collection must not hold an execution lease
 * for the lifetime of a container.
 *
 * Security and concurrency properties:
 *
 * - Atomic SET NX PX acquisition.
 * - Unique ownership identity for each acquisition attempt.
 * - Atomic compare-and-expire renewal.
 * - Atomic compare-and-delete release.
 * - Fail-closed behavior on uncertain Redis operations.
 * - Conservative local lease expiry tracking.
 * - Serialized ownership operations.
 * - Bounded command waiting.
 * - Validated keys, tokens and lease configuration.
 *
 * Important limitations:
 *
 * This is a Redis ownership lease, not a durable fencing
 * mechanism. Redis failover, eviction, process suspension
 * and network partitions may permit overlapping collectors.
 *
 * Callers must stop collection immediately after ownership
 * loss and must check ownership before accepting new logs.
 *
 * A timed-out Redis command may still execute later.
 * Unique acquisition identities prevent delayed operations
 * from mutating another acquisition's lock.
 *
 * The caller owns the Redis connection lifecycle.
 */

import {
  randomUUID,
} from "node:crypto";

import {
  performance,
} from "node:perf_hooks";

import type {
  Redis,
} from "ioredis";

// ==================================================
// Public contracts
// ==================================================

export interface DistributedLock {
  acquire(): Promise<boolean>;

  renew(): Promise<boolean>;

  release(): Promise<void>;
}

export interface RedisLockOptions {
  key: string;

  token: string;

  ttlMs: number;

  redis: Redis;

  /**
   * Maximum local waiting time for one Redis command.
   *
   * This does not cancel a command already sent to Redis.
   */

  commandTimeoutMs?: number;
}

// ==================================================
// Configuration limits
// ==================================================

const MIN_TTL_MS = 1_000;

const MAX_TTL_MS =
  10 * 60 * 1_000;

const DEFAULT_COMMAND_TIMEOUT_MS =
  2_000;

const MIN_COMMAND_TIMEOUT_MS =
  50;

const MAX_COMMAND_TIMEOUT_MS =
  10_000;

const MAX_KEY_BYTES = 512;

const MAX_TOKEN_BYTES = 256;

const MAX_PENDING_OPERATIONS = 16;

const UNSAFE_IDENTIFIER_PATTERN =
  /[\u0000-\u0020\u007f-\u009f\u061c\u200b-\u200f\u202a-\u202e\u2060-\u206f\uFEFF]/u;

// ==================================================
// Atomic Redis scripts
// ==================================================

/**
 * Renew only when the current Redis value exactly
 * matches this acquisition's ownership identity.
 *
 * Never use SET XX for ownership renewal.
 */

const RENEW_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call(
      "pexpire",
      KEYS[1],
      ARGV[2]
    )
  end

  return 0
`;

/**
 * Delete only the lock belonging to this exact
 * acquisition.
 */

const RELEASE_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  end

  return 0
`;

// ==================================================
// Internal contracts
// ==================================================

type LockState =
  | "IDLE"
  | "HELD"
  | "LOST";

interface ResolvedLockOptions {
  key: string;

  token: string;

  ttlMs: number;

  commandTimeoutMs: number;

  redis: Redis;
}

interface LeaseIdentity {
  value: string;

  expiresAt: number;
}

// ==================================================
// Internal errors
// ==================================================

class RedisLockTimeoutError extends Error {
  constructor() {
    super(
      "Redis collector lock operation timed out.",
    );

    this.name =
      "RedisLockTimeoutError";
  }
}

// ==================================================
// Configuration validation
// ==================================================

function assertIdentifier(
  value: string,
  name: string,
  maxBytes: number,
): void {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maxBytes ||
    UNSAFE_IDENTIFIER_PATTERN.test(
      value,
    ) ||
    Buffer.byteLength(
      value,
      "utf8",
    ) > maxBytes
  ) {
    throw new TypeError(
      `Invalid collector lock ${name}.`,
    );
  }
}

function resolveOptions(
  options: RedisLockOptions,
): ResolvedLockOptions {
  assertIdentifier(
    options.key,
    "key",
    MAX_KEY_BYTES,
  );

  assertIdentifier(
    options.token,
    "token",
    MAX_TOKEN_BYTES,
  );

  if (
    !Number.isSafeInteger(
      options.ttlMs,
    ) ||
    options.ttlMs < MIN_TTL_MS ||
    options.ttlMs > MAX_TTL_MS
  ) {
    throw new RangeError(
      "Invalid collector lock TTL.",
    );
  }

  /**
   * Keep command waiting substantially shorter
   * than the lease duration.
   */

  const maximumTimeout = Math.min(
    MAX_COMMAND_TIMEOUT_MS,
    Math.floor(
      options.ttlMs / 3,
    ),
  );

  const commandTimeoutMs =
    options.commandTimeoutMs ??
    Math.min(
      DEFAULT_COMMAND_TIMEOUT_MS,
      maximumTimeout,
    );

  if (
    !Number.isSafeInteger(
      commandTimeoutMs,
    ) ||
    commandTimeoutMs <
      MIN_COMMAND_TIMEOUT_MS ||
    commandTimeoutMs >
      maximumTimeout
  ) {
    throw new RangeError(
      "Invalid collector lock command timeout.",
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
      "Invalid collector lock Redis client.",
    );
  }

  return {
    key: options.key,

    token: options.token,

    ttlMs: options.ttlMs,

    commandTimeoutMs,

    redis: options.redis,
  };
}

// ==================================================
// Bounded Redis command waiting
// ==================================================

/**
 * Bound local waiting without claiming that the
 * underlying Redis command has been cancelled.
 *
 * Promise.race observes late rejections, preventing
 * an abandoned command from producing an unhandled
 * rejection through this wrapper.
 */

async function withCommandTimeout<T>(
  operation: () => Promise<T>,
  timeoutMs: number,
): Promise<T> {
  let timer:
    | ReturnType<typeof setTimeout>
    | undefined;

  try {
    const timeout =
      new Promise<never>(
        (_, reject) => {
          timer = setTimeout(
            () => {
              reject(
                new RedisLockTimeoutError(),
              );
            },
            timeoutMs,
          );
        },
      );

    const command =
      Promise.resolve().then(
        operation,
      );

    return await Promise.race([
      command,
      timeout,
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

// ==================================================
// Redis collector ownership lease
// ==================================================

export class RedisCollectorLock
  implements DistributedLock
{
  private readonly key: string;

  private readonly token: string;

  private readonly ttlMs: number;

  private readonly commandTimeoutMs:
    number;

  private readonly redis: Redis;

  private state: LockState =
    "IDLE";

  private lease:
    LeaseIdentity | null = null;

  /**
   * Serialize local ownership operations.
   *
   * This does not replace Redis atomicity. It prevents
   * concurrent calls on the same class instance from
   * corrupting local ownership state.
   */

  private operationTail:
    Promise<void> = Promise.resolve();

  private pendingOperations = 0;

  /**
   * Incremented when ownership is explicitly invalidated.
   *
   * Queued or in-flight operations cannot restore local
   * ownership after release or fail-closed invalidation.
   */

  private generation = 0;

  constructor(
    options: RedisLockOptions,
  ) {
    const resolved =
      resolveOptions(options);

    this.key = resolved.key;

    this.token = resolved.token;

    this.ttlMs = resolved.ttlMs;

    this.commandTimeoutMs =
      resolved.commandTimeoutMs;

    this.redis = resolved.redis;
  }

  // ================================================
  // Local lease state
  // ================================================

  /**
   * Conservative local ownership indication.
   *
   * This is not an authoritative Redis ownership
   * check and cannot detect remote deletion or failover.
   */

  get isHeld(): boolean {
    if (
      this.state !== "HELD" ||
      this.lease === null
    ) {
      return false;
    }

    if (
      performance.now() >=
      this.lease.expiresAt
    ) {
      this.invalidateOwnership();

      return false;
    }

    return true;
  }

  /**
   * Remaining conservative local lease duration.
   */

  get remainingTtlMs(): number {
    if (
      !this.isHeld ||
      this.lease === null
    ) {
      return 0;
    }

    return Math.max(
      0,
      Math.floor(
        this.lease.expiresAt -
          performance.now(),
      ),
    );
  }

  private invalidateOwnership(): void {
    this.generation += 1;

    this.state = "LOST";

    /**
     * Preserve the previous identity for a possible
     * ownership-safe cleanup operation.
     */
  }

  // ================================================
  // Redis availability
  // ================================================

  private isRedisReady(): boolean {
    return (
      this.redis.status === "ready"
    );
  }

  // ================================================
  // Serialized operation execution
  // ================================================

  private serialize<T>(
    operation: () => Promise<T>,
    fallback: T,
  ): Promise<T> {
    if (
      this.pendingOperations >=
      MAX_PENDING_OPERATIONS
    ) {
      this.invalidateOwnership();

      return Promise.resolve(
        fallback,
      );
    }

    this.pendingOperations += 1;

    const result =
      this.operationTail.then(
        operation,
      );

    this.operationTail = result.then(
      () => {
        this.pendingOperations -= 1;
      },

      () => {
        this.pendingOperations -= 1;
      },
    );

    return result.catch(() => {
      this.invalidateOwnership();

      return fallback;
    });
  }

  // ================================================
  // Unique acquisition identity
  // ================================================

  /**
   * The caller token identifies the collector owner.
   *
   * A fresh UUID additionally identifies this exact
   * acquisition attempt, preventing token reuse across
   * expired or released leases.
   */

  private createLeaseValue(): string {
    return `${this.token}:${randomUUID()}`;
  }

  // ================================================
  // Ownership-safe cleanup
  // ================================================

  private async releaseIdentity(
    value: string,
  ): Promise<void> {
    if (!this.isRedisReady()) {
      return;
    }

    try {
      await withCommandTimeout(
        () =>
          this.redis.eval(
            RELEASE_SCRIPT,
            1,
            this.key,
            value,
          ),

        this.commandTimeoutMs,
      );
    } catch {
      /**
       * Cleanup is best-effort.
       *
       * The Redis key retains its TTL. Never delete
       * the key without comparing its identity.
       */
    }
  }

  // ================================================
  // Atomic acquisition
  // ================================================

  async acquire(): Promise<boolean> {
    const requestedGeneration =
      this.generation;

    return this.serialize(
      async () => {
        if (
          requestedGeneration !==
          this.generation
        ) {
          return false;
        }

        return this.acquireInternal();
      },

      false,
    );
  }

  private async acquireInternal():
    Promise<boolean> {
    /**
     * Repeated acquire calls on an active lease are
     * treated as ownership-safe renewal attempts.
     */

    if (this.isHeld) {
      return this.renewInternal();
    }

    if (!this.isRedisReady()) {
      this.invalidateOwnership();

      return false;
    }

    const generation =
      this.generation;

    const value =
      this.createLeaseValue();

    /**
     * Measure expiry from command initiation rather
     * than response receipt.
     *
     * Network latency must not extend the local
     * ownership deadline.
     */

    const startedAt =
      performance.now();

    const expiresAt =
      startedAt + this.ttlMs;

    this.lease = {
      value,
      expiresAt,
    };

    this.state = "IDLE";

    try {
      const result =
        await withCommandTimeout(
          () =>
            this.redis.set(
              this.key,
              value,
              "PX",
              this.ttlMs,
              "NX",
            ),

          this.commandTimeoutMs,
        );

      if (result !== "OK") {
        this.invalidateOwnership();

        return false;
      }

      if (
        generation !==
          this.generation ||
        performance.now() >=
          expiresAt ||
        !this.isRedisReady()
      ) {
        this.invalidateOwnership();

        await this.releaseIdentity(
          value,
        );

        return false;
      }

      this.state = "HELD";

      return true;
    } catch {
      /**
       * The SET command may have succeeded before
       * the connection failed.
       *
       * Never claim ownership after an uncertain
       * acquisition.
       */

      this.invalidateOwnership();

      await this.releaseIdentity(
        value,
      );

      return false;
    }
  }

  // ================================================
  // Atomic ownership-safe renewal
  // ================================================

  async renew(): Promise<boolean> {
    const requestedGeneration =
      this.generation;

    return this.serialize(
      async () => {
        if (
          requestedGeneration !==
          this.generation
        ) {
          return false;
        }

        return this.renewInternal();
      },

      false,
    );
  }

  private async renewInternal():
    Promise<boolean> {
    if (
      !this.isHeld ||
      this.lease === null
    ) {
      return false;
    }

    if (!this.isRedisReady()) {
      this.invalidateOwnership();

      return false;
    }

    const generation =
      this.generation;

    const lease = this.lease;

    const previousExpiry =
      lease.expiresAt;

    const startedAt =
      performance.now();

    const nextExpiry =
      startedAt + this.ttlMs;

    /**
     * Do not wait beyond the conservative expiry
     * of the previously confirmed lease.
     */

    const remaining = Math.floor(
      previousExpiry - startedAt,
    );

    if (remaining <= 0) {
      this.invalidateOwnership();

      return false;
    }

    const timeoutMs = Math.min(
      this.commandTimeoutMs,
      remaining,
    );

    try {
      const result =
        await withCommandTimeout(
          () =>
            this.redis.eval(
              RENEW_SCRIPT,
              1,
              this.key,
              lease.value,
              String(this.ttlMs),
            ),

          timeoutMs,
        );

      /**
       * Lua returns 1 only when the exact ownership
       * identity matched and PEXPIRE succeeded.
       */

      if (
        result !== 1 ||
        generation !==
          this.generation ||
        performance.now() >=
          previousExpiry ||
        performance.now() >=
          nextExpiry ||
        !this.isRedisReady()
      ) {
        this.invalidateOwnership();

        await this.releaseIdentity(
          lease.value,
        );

        return false;
      }

      this.lease = {
        value: lease.value,

        expiresAt: nextExpiry,
      };

      this.state = "HELD";

      return true;
    } catch {
      /**
       * Fail closed.
       *
       * A Redis timeout or connection failure is
       * not proof that ownership remains valid.
       */

      this.invalidateOwnership();

      return false;
    }
  }

  // ================================================
  // Ownership-safe release
  // ================================================

  async release(): Promise<void> {
    const identity =
      this.lease?.value;

    /**
     * Invalidate local ownership immediately,
     * before waiting for queued operations.
     *
     * An in-flight acquisition or renewal must not
     * restore ownership after release is requested.
     */

    this.invalidateOwnership();

    this.lease = null;

    if (!identity) {
      return;
    }

    await this.serialize(
      async () => {
        await this.releaseIdentity(
          identity,
        );
      },

      undefined,
    );
  }
}
