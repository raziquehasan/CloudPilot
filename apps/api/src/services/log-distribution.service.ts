/**
 * CloudPilot — Log Notification Distribution
 *
 * Phase 5.6 — Realtime Deployment Logs
 *
 * Drives Redis publication of committed log cursor ranges.
 *
 * Architecture:
 *
 * - Worker persists logs and notification outbox rows
 *   in the same PostgreSQL transaction.
 *
 * - Publisher atomically claims due outbox rows.
 *
 * - Each claim receives a unique execution lease.
 *
 * - Publication and failure settlement are fenced
 *   by the exact lease token.
 *
 * - Expired publishing leases are recoverable.
 *
 * - Redis notifications contain cursor ranges,
 *   never actual deployment log messages.
 *
 * Delivery semantics:
 *
 * Redis Pub/Sub is at-most-once for subscribers.
 * Outbox retries may produce duplicate notifications.
 *
 * PostgreSQL remains the authoritative log store.
 *
 * Redis channels are routing mechanisms,
 * not authorization boundaries.
 */

import { randomUUID } from "node:crypto";

import { prisma } from "@cloudpilot/database";

import {
  createRedisConnection,
  type LogNotification,
} from "@cloudpilot/queue";

import type { Redis } from "ioredis";

import { env } from "../config/env.js";
import { logger } from "../config/logger.js";

import {
  buildLogChannel,
  isValidLogChannelPrefix,
  parseLogNotification,
} from "./log-channel.js";

// ==================================================
// Constants
// ==================================================

const MAX_ATTEMPTS = 8;

const BASE_BACKOFF_MS = 500;

const MAX_BACKOFF_MS = 30_000;

const LEASE_MS = 30_000;

const REDIS_PUBLISH_TIMEOUT_MS = 3_000;

const REDIS_SHUTDOWN_TIMEOUT_MS = 3_000;

const SHUTDOWN_DRAIN_TIMEOUT_MS = 10_000;

const DATABASE_TRANSACTION_TIMEOUT_MS = 10_000;

const DATABASE_TRANSACTION_MAX_WAIT_MS = 5_000;

const MIN_PUBLISH_INTERVAL_MS = 100;

const MAX_PUBLISH_INTERVAL_MS = 60_000;

const MAX_BATCH_ROWS = 50;

const MAX_BATCH_DURATION_MS = 10_000;

const RECOVERY_BATCH_SIZE = 100;

const CLEANUP_BATCH_SIZE = 1_000;

const MAX_CLEANUP_BATCHES = 5;

const CLEANUP_INTERVAL_MS = 60 * 60_000;

const PUBLISHED_RETENTION_MS =
  24 * 60 * 60_000;

// ==================================================
// Internal contracts
// ==================================================

type RuntimeState =
  | "IDLE"
  | "RUNNING"
  | "STOPPING"
  | "STOPPED";

type PublishFailureCode =
  | "LOG_PUBLISH_FAILED"
  | "LOG_PUBLISH_TIMEOUT"
  | "LOG_NOTIFICATION_INVALID"
  | "LOG_PUBLISH_LEASE_EXPIRED"
  | "LOG_PUBLISH_RETRIES_EXHAUSTED";

interface OutboxRow {
  id: string;

  workspaceId: string;

  projectId: string;

  deploymentId: string;

  fromSequence: bigint;

  toSequence: bigint;

  attemptCount: number;

  createdAt: Date;
}

interface ClaimedRow extends OutboxRow {
  leaseToken: string;

  localLeaseDeadline: number;
}

// ==================================================
// Internal errors
// ==================================================

class OperationTimeoutError extends Error {
  constructor() {
    super("Log distribution operation timed out.");

    this.name = "OperationTimeoutError";
  }
}

// ==================================================
// Timeout helper
// ==================================================

/**
 * Bound the time spent waiting for an operation.
 *
 * This does not forcibly cancel an underlying
 * database or Redis command.
 *
 * Callers must treat timed-out external operations
 * as having an uncertain outcome.
 */
async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
): Promise<T> {
  let timer:
    | ReturnType<typeof setTimeout>
    | undefined;

  try {
    return await Promise.race([
      operation,

      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(
            new OperationTimeoutError(),
          );
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

// ==================================================
// Retry policy
// ==================================================

function backoff(
  attempt: number,
): number {
  const exponent = Math.min(
    Math.max(attempt - 1, 0),
    MAX_ATTEMPTS,
  );

  const exponential = Math.min(
    BASE_BACKOFF_MS * 2 ** exponent,
    MAX_BACKOFF_MS,
  );

  const jittered = Math.round(
    exponential *
      (0.8 + Math.random() * 0.4),
  );

  return Math.min(
    MAX_BACKOFF_MS,
    Math.max(
      BASE_BACKOFF_MS,
      jittered,
    ),
  );
}

function validateInterval(
  intervalMs: number,
): void {
  if (
    !Number.isSafeInteger(intervalMs) ||
    intervalMs < MIN_PUBLISH_INTERVAL_MS ||
    intervalMs > MAX_PUBLISH_INTERVAL_MS
  ) {
    throw new Error(
      "Invalid log notification publish interval.",
    );
  }
}

// ==================================================
// Notification construction
// ==================================================

function createNotification(
  row: ClaimedRow,
): LogNotification | null {
  try {
    if (
      !(row.createdAt instanceof Date) ||
      !Number.isFinite(
        row.createdAt.getTime(),
      )
    ) {
      return null;
    }

    const notification: LogNotification = {
      workspaceId: row.workspaceId,

      projectId: row.projectId,

      deploymentId: row.deploymentId,

      fromSequence:
        row.fromSequence.toString(10),

      toSequence:
        row.toSequence.toString(10),

      // Outbox creation time is a stable approximation
      // of the original committed batch time.
      //
      // It is not the exact PostgreSQL commit timestamp.
      committedAt:
        row.createdAt.toISOString(),
    };

    return parseLogNotification(
      JSON.stringify(notification),
    );
  } catch {
    return null;
  }
}

// ==================================================
// Distribution runtime
// ==================================================

export class LogDistributionRuntime {
  private readonly redis: Redis;

  private state: RuntimeState = "IDLE";

  private timer:
    | ReturnType<typeof setTimeout>
    | undefined;

  private cleanupTimer:
    | ReturnType<typeof setTimeout>
    | undefined;

  private publishTask:
    | Promise<void>
    | undefined;

  private cleanupTask:
    | Promise<void>
    | undefined;

  private stopTask:
    | Promise<void>
    | undefined;

  private intervalMs = 1_000;

  constructor() {
    if (
      !isValidLogChannelPrefix(
        env.LOG_REDIS_PREFIX,
      )
    ) {
      throw new Error(
        "Invalid log distribution Redis prefix.",
      );
    }

    this.redis = createRedisConnection({
      url: env.REDIS_URL,

      timeoutMs: 3_000,

      maxRetriesPerRequest: 1,

      name: "cloudpilot-log-publisher",
    });

    // Avoid exposing Redis connection details,
    // credentials or raw command arguments.

    this.redis.on(
      "error",
      this.handleRedisError,
    );
  }

  // ==================================================
  // Redis event handling
  // ==================================================

  private readonly handleRedisError =
    (): void => {
      if (this.state !== "RUNNING") {
        return;
      }

      logger.debug(
        {
          code: "LOG_REDIS_CONNECTION_ERROR",
        },
        "Log notification Redis connection unavailable",
      );
    };

  // ==================================================
  // Runtime startup
  // ==================================================

  start(intervalMs: number): void {
    if (this.state === "RUNNING") {
      return;
    }

    if (
      this.state === "STOPPING" ||
      this.state === "STOPPED"
    ) {
      throw new Error(
        "Stopped log distribution runtime cannot be restarted.",
      );
    }

    validateInterval(intervalMs);

    this.intervalMs = intervalMs;

    this.state = "RUNNING";

    // Support both lazy and automatically
    // connecting Redis client configurations.

    if (
      this.redis.status === "wait" ||
      this.redis.status === "end"
    ) {
      void this.redis.connect().catch(() => {
        // Redis availability is checked before
        // attempting notification publication.
      });
    }

    this.schedulePublish(0);

    this.scheduleCleanup(
      CLEANUP_INTERVAL_MS,
    );
  }

  // ==================================================
  // Serialized publish scheduling
  // ==================================================

  private schedulePublish(
    delayMs: number,
  ): void {
    if (
      this.state !== "RUNNING" ||
      this.timer !== undefined ||
      this.publishTask !== undefined
    ) {
      return;
    }

    this.timer = setTimeout(() => {
      this.timer = undefined;

      this.runPublishTask();
    }, delayMs);

    this.timer.unref?.();
  }

  private runPublishTask(): void {
    if (
      this.state !== "RUNNING" ||
      this.publishTask !== undefined
    ) {
      return;
    }

    const task = this.publishBatch(
      MAX_BATCH_ROWS,
    )
      .catch(() => {
        logger.warn(
          {
            code: "LOG_PUBLISH_BATCH_FAILED",
          },
          "Log notification publish batch failed",
        );
      })
      .finally(() => {
        if (this.publishTask === task) {
          this.publishTask = undefined;
        }

        if (this.state === "RUNNING") {
          this.schedulePublish(
            this.intervalMs,
          );
        }
      });

    this.publishTask = task;
  }

  // ==================================================
  // Expired lease recovery
  // ==================================================

  /**
   * Recover abandoned publishing rows.
   *
   * Database time is used for lease expiry.
   *
   * Rows that exhausted their retry budget
   * transition to DEAD.
   *
   * Recovery is bounded and uses SKIP LOCKED
   * to avoid waiting for rows currently locked
   * by another transaction.
   */
  private async recoverExpiredLeases():
    Promise<void> {
    await prisma.$transaction(
      async (tx) => {
        await tx.$executeRaw`
          WITH expired AS (
            SELECT "id"
            FROM "deployment_log_notification_outbox"
            WHERE "status" = 'PUBLISHING'
              AND (
                "leaseExpiresAt" IS NULL
                OR "leaseExpiresAt" <=
                  (clock_timestamp() AT TIME ZONE 'UTC')
              )
            ORDER BY
              "leaseExpiresAt" ASC NULLS FIRST,
              "id" ASC
            LIMIT ${RECOVERY_BATCH_SIZE}
            FOR UPDATE SKIP LOCKED
          )
          UPDATE
            "deployment_log_notification_outbox" AS outbox
          SET
            "status" = 'DEAD',
            "leaseToken" = NULL,
            "leaseExpiresAt" = NULL,
            "lastErrorCode" =
              'LOG_PUBLISH_RETRIES_EXHAUSTED',
            "updatedAt" =
              (clock_timestamp() AT TIME ZONE 'UTC')
          FROM expired
          WHERE outbox."id" = expired."id"
            AND outbox."status" = 'PUBLISHING'
            AND outbox."attemptCount" >= ${MAX_ATTEMPTS}
        `;

        await tx.$executeRaw`
          WITH expired AS (
            SELECT "id"
            FROM "deployment_log_notification_outbox"
            WHERE "status" = 'PUBLISHING'
              AND "attemptCount" < ${MAX_ATTEMPTS}
              AND (
                "leaseExpiresAt" IS NULL
                OR "leaseExpiresAt" <=
                  (clock_timestamp() AT TIME ZONE 'UTC')
              )
            ORDER BY
              "leaseExpiresAt" ASC NULLS FIRST,
              "id" ASC
            LIMIT ${RECOVERY_BATCH_SIZE}
            FOR UPDATE SKIP LOCKED
          )
          UPDATE
            "deployment_log_notification_outbox" AS outbox
          SET
            "status" = 'PENDING',
            "leaseToken" = NULL,
            "leaseExpiresAt" = NULL,
            "nextAttemptAt" =
              (clock_timestamp() AT TIME ZONE 'UTC')
              + (${BASE_BACKOFF_MS} * interval '1 millisecond'),
            "lastErrorCode" =
              'LOG_PUBLISH_LEASE_EXPIRED',
            "updatedAt" =
              (clock_timestamp() AT TIME ZONE 'UTC')
          FROM expired
          WHERE outbox."id" = expired."id"
            AND outbox."status" = 'PUBLISHING'
        `;

        // Repair exhausted pending rows without
        // allowing another publication attempt.

        await tx.$executeRaw`
          WITH exhausted AS (
            SELECT "id"
            FROM "deployment_log_notification_outbox"
            WHERE "status" = 'PENDING'
              AND "attemptCount" >= ${MAX_ATTEMPTS}
            ORDER BY "createdAt", "id"
            LIMIT ${RECOVERY_BATCH_SIZE}
            FOR UPDATE SKIP LOCKED
          )
          UPDATE
            "deployment_log_notification_outbox" AS outbox
          SET
            "status" = 'DEAD',
            "leaseToken" = NULL,
            "leaseExpiresAt" = NULL,
            "lastErrorCode" =
              'LOG_PUBLISH_RETRIES_EXHAUSTED',
            "updatedAt" =
              (clock_timestamp() AT TIME ZONE 'UTC')
          FROM exhausted
          WHERE outbox."id" = exhausted."id"
            AND outbox."status" = 'PENDING'
        `;
      },
      {
        maxWait:
          DATABASE_TRANSACTION_MAX_WAIT_MS,

        timeout:
          DATABASE_TRANSACTION_TIMEOUT_MS,
      },
    );
  }

  // ==================================================
  // Atomic single-row claim
  // ==================================================

  /**
   * Claim one notification immediately before
   * publication instead of leasing a large batch.
   *
   * This reduces lease expiry while rows wait
   * behind earlier Redis operations.
   */
  private async claim():
    Promise<ClaimedRow | null> {
    if (
      this.state !== "RUNNING" ||
      this.redis.status !== "ready"
    ) {
      return null;
    }

    const leaseToken = randomUUID();

    const requestStartedAt =
      performance.now();

    const rows = await prisma.$queryRaw<
      OutboxRow[]
    >`
      WITH candidate AS (
        SELECT "id"
        FROM "deployment_log_notification_outbox"
        WHERE "status" = 'PENDING'
          AND "attemptCount" < ${MAX_ATTEMPTS}
          AND "nextAttemptAt" <=
            (clock_timestamp() AT TIME ZONE 'UTC')
        ORDER BY
          "nextAttemptAt" ASC,
          "createdAt" ASC,
          "id" ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE
        "deployment_log_notification_outbox" AS outbox
      SET
        "status" = 'PUBLISHING',
        "leaseToken" = ${leaseToken}::uuid,
        "leaseExpiresAt" =
          (clock_timestamp() AT TIME ZONE 'UTC')
          + (${LEASE_MS} * interval '1 millisecond'),
        "attemptCount" =
          outbox."attemptCount" + 1,
        "updatedAt" =
          (clock_timestamp() AT TIME ZONE 'UTC')
      FROM candidate
      WHERE outbox."id" = candidate."id"
        AND outbox."status" = 'PENDING'
      RETURNING
        outbox."id",
        outbox."workspaceId",
        outbox."projectId",
        outbox."deploymentId",
        outbox."fromSequence",
        outbox."toSequence",
        outbox."attemptCount",
        outbox."createdAt"
    `;

    const row = rows[0];

    if (!row) {
      return null;
    }

    return {
      ...row,

      leaseToken,

      // Conservative local lease deadline.
      // The database remains authoritative.
      localLeaseDeadline:
        requestStartedAt + LEASE_MS,
    };
  }

  // ==================================================
  // Ownership verification
  // ==================================================

  private async ownsLease(
    row: ClaimedRow,
  ): Promise<boolean> {
    if (
      performance.now() >=
      row.localLeaseDeadline
    ) {
      return false;
    }

    const rows = await prisma.$queryRaw<
      Array<{ owned: boolean }>
    >`
      SELECT EXISTS (
        SELECT 1
        FROM "deployment_log_notification_outbox"
        WHERE "id" = ${row.id}::uuid
          AND "workspaceId" = ${row.workspaceId}::uuid
          AND "projectId" = ${row.projectId}::uuid
          AND "deploymentId" = ${row.deploymentId}::uuid
          AND "status" = 'PUBLISHING'
          AND "leaseToken" = ${row.leaseToken}::uuid
          AND "attemptCount" = ${row.attemptCount}
          AND "leaseExpiresAt" >
            (clock_timestamp() AT TIME ZONE 'UTC')
      ) AS "owned"
    `;

    return (
      rows[0]?.owned === true &&
      performance.now() <
        row.localLeaseDeadline
    );
  }

  // ==================================================
  // Bounded publication batch
  // ==================================================

  private async publishBatch(
    limit: number,
  ): Promise<void> {
    if (this.state !== "RUNNING") {
      return;
    }

    const startedAt = performance.now();

    // Recovery is independent of Redis readiness.
    await this.recoverExpiredLeases();

    if (
      this.state !== "RUNNING" ||
      this.redis.status !== "ready"
    ) {
      return;
    }

    const safeLimit = Math.min(
      Math.max(1, limit),
      MAX_BATCH_ROWS,
    );

    for (
      let index = 0;
      index < safeLimit;
      index += 1
    ) {
      if (
        this.state !== "RUNNING" ||
        this.redis.status !== "ready" ||
        performance.now() - startedAt >=
          MAX_BATCH_DURATION_MS
      ) {
        break;
      }

      const row = await this.claim();

      if (!row) {
        break;
      }

      if (this.state !== "RUNNING") {
        // No Redis operation was started.
        // Release the unused claim when still owned.

        await this.releaseUnusedClaim(row);

        break;
      }

      await this.publishRow(row);
    }
  }

  // ==================================================
  // Single notification publication
  // ==================================================

  private async publishRow(
    row: ClaimedRow,
  ): Promise<void> {
    const notification =
      createNotification(row);

    if (notification === null) {
      await this.handleFailure(
        row,
        "LOG_NOTIFICATION_INVALID",
        true,
      );

      return;
    }

    const channel = buildLogChannel(
      env.LOG_REDIS_PREFIX,
      {
        workspaceId:
          notification.workspaceId,

        deploymentId:
          notification.deploymentId,
      },
    );

    const payload =
      JSON.stringify(notification);

    if (!(await this.ownsLease(row))) {
      return;
    }

    if (
      this.state !== "RUNNING" ||
      this.redis.status !== "ready"
    ) {
      await this.releaseUnusedClaim(row);

      return;
    }

    const remainingLeaseMs = Math.floor(
      row.localLeaseDeadline -
        performance.now(),
    );

    if (
      remainingLeaseMs <=
      REDIS_PUBLISH_TIMEOUT_MS
    ) {
      await this.releaseUnusedClaim(row);

      return;
    }

    // Redis publication errors and database
    // settlement errors are deliberately separate.
    //
    // A successful Redis publication followed by
    // a failed database update must not be treated
    // as a definite Redis publication failure.

    try {
      await withTimeout(
        this.redis.publish(
          channel,
          payload,
        ),
        REDIS_PUBLISH_TIMEOUT_MS,
      );
    } catch (error) {
      if (
        error instanceof OperationTimeoutError
      ) {
        // Publication outcome is uncertain.
        // Leave the row leased for recovery.
        //
        // A late Redis command may still publish,
        // so subscribers must tolerate duplicates.

        logger.warn(
          {
            code: "LOG_PUBLISH_TIMEOUT",
            attempt: row.attemptCount,
          },
          "Log publication outcome is uncertain",
        );

        return;
      }

      await this.handleFailure(
        row,
        "LOG_PUBLISH_FAILED",
      );

      return;
    }

    // Redis accepting a publication does not
    // guarantee that any subscriber received it.

    try {
      await this.markPublished(row);
    } catch {
      // Preserve the existing lease.
      //
      // Recovery may retry the notification,
      // which is safe only when downstream
      // cursor handling is idempotent.

      logger.warn(
        {
          code: "LOG_PUBLISH_SETTLEMENT_FAILED",
        },
        "Log notification settlement is uncertain",
      );
    }
  }

  // ==================================================
  // Fenced successful settlement
  // ==================================================

  private async markPublished(
    row: ClaimedRow,
  ): Promise<void> {
    const updated = await prisma.$executeRaw`
      UPDATE "deployment_log_notification_outbox"
      SET
        "status" = 'PUBLISHED',
        "publishedAt" =
          (clock_timestamp() AT TIME ZONE 'UTC'),
        "leaseToken" = NULL,
        "leaseExpiresAt" = NULL,
        "lastErrorCode" = NULL,
        "updatedAt" =
          (clock_timestamp() AT TIME ZONE 'UTC')
      WHERE "id" = ${row.id}::uuid
        AND "workspaceId" = ${row.workspaceId}::uuid
        AND "projectId" = ${row.projectId}::uuid
        AND "deploymentId" = ${row.deploymentId}::uuid
        AND "status" = 'PUBLISHING'
        AND "leaseToken" = ${row.leaseToken}::uuid
        AND "attemptCount" = ${row.attemptCount}
        AND "leaseExpiresAt" >
          (clock_timestamp() AT TIME ZONE 'UTC')
    `;

    if (updated !== 1) {
      logger.debug(
        {
          code: "LOG_PUBLISH_OWNERSHIP_LOST",
        },
        "Log notification settlement skipped",
      );
    }
  }

  // ==================================================
  // Fenced failure settlement
  // ==================================================

  private async handleFailure(
    row: ClaimedRow,
    code: PublishFailureCode,
    permanent = false,
  ): Promise<void> {
    const exhausted =
      permanent ||
      row.attemptCount >= MAX_ATTEMPTS;

    if (exhausted) {
      const updated = await prisma.$executeRaw`
        UPDATE "deployment_log_notification_outbox"
        SET
          "status" = 'DEAD',
          "leaseToken" = NULL,
          "leaseExpiresAt" = NULL,
          "lastErrorCode" = ${code},
          "updatedAt" =
            (clock_timestamp() AT TIME ZONE 'UTC')
        WHERE "id" = ${row.id}::uuid
          AND "workspaceId" = ${row.workspaceId}::uuid
          AND "projectId" = ${row.projectId}::uuid
          AND "deploymentId" = ${row.deploymentId}::uuid
          AND "status" = 'PUBLISHING'
          AND "leaseToken" = ${row.leaseToken}::uuid
          AND "attemptCount" = ${row.attemptCount}
          AND "leaseExpiresAt" >
            (clock_timestamp() AT TIME ZONE 'UTC')
      `;

      if (updated === 1) {
        logger.warn(
          {
            code,
            attempt: row.attemptCount,
          },
          "Log notification exhausted publication attempts",
        );
      }

      return;
    }

    const delay = backoff(
      row.attemptCount,
    );

    const updated = await prisma.$executeRaw`
      UPDATE "deployment_log_notification_outbox"
      SET
        "status" = 'PENDING',
        "leaseToken" = NULL,
        "leaseExpiresAt" = NULL,
        "nextAttemptAt" =
          (clock_timestamp() AT TIME ZONE 'UTC')
          + (${delay} * interval '1 millisecond'),
        "lastErrorCode" = ${code},
        "updatedAt" =
          (clock_timestamp() AT TIME ZONE 'UTC')
      WHERE "id" = ${row.id}::uuid
        AND "workspaceId" = ${row.workspaceId}::uuid
        AND "projectId" = ${row.projectId}::uuid
        AND "deploymentId" = ${row.deploymentId}::uuid
        AND "status" = 'PUBLISHING'
        AND "leaseToken" = ${row.leaseToken}::uuid
        AND "attemptCount" = ${row.attemptCount}
        AND "leaseExpiresAt" >
          (clock_timestamp() AT TIME ZONE 'UTC')
    `;

    if (updated === 1) {
      logger.debug(
        {
          code,
          attempt: row.attemptCount,
          retryDelayMs: delay,
        },
        "Log notification scheduled for retry",
      );
    }
  }

  // ==================================================
  // Release unused claim
  // ==================================================

  /**
   * Only call this method when publication
   * has definitely not been invoked.
   *
   * An unused claim does not consume a
   * publication attempt.
   */
  private async releaseUnusedClaim(
    row: ClaimedRow,
  ): Promise<void> {
    await prisma.$executeRaw`
      UPDATE "deployment_log_notification_outbox"
      SET
        "status" = 'PENDING',
        "leaseToken" = NULL,
        "leaseExpiresAt" = NULL,
        "attemptCount" =
          GREATEST("attemptCount" - 1, 0),
        "nextAttemptAt" =
          (clock_timestamp() AT TIME ZONE 'UTC')
          + (${BASE_BACKOFF_MS} * interval '1 millisecond'),
        "updatedAt" =
          (clock_timestamp() AT TIME ZONE 'UTC')
      WHERE "id" = ${row.id}::uuid
        AND "workspaceId" = ${row.workspaceId}::uuid
        AND "projectId" = ${row.projectId}::uuid
        AND "deploymentId" = ${row.deploymentId}::uuid
        AND "status" = 'PUBLISHING'
        AND "leaseToken" = ${row.leaseToken}::uuid
        AND "attemptCount" = ${row.attemptCount}
        AND "leaseExpiresAt" >
          (clock_timestamp() AT TIME ZONE 'UTC')
    `;
  }

  // ==================================================
  // Serialized cleanup scheduling
  // ==================================================

  private scheduleCleanup(
    delayMs: number,
  ): void {
    if (
      this.state !== "RUNNING" ||
      this.cleanupTimer !== undefined ||
      this.cleanupTask !== undefined
    ) {
      return;
    }

    this.cleanupTimer = setTimeout(() => {
      this.cleanupTimer = undefined;

      this.runCleanupTask();
    }, delayMs);

    this.cleanupTimer.unref?.();
  }

  private runCleanupTask(): void {
    if (
      this.state !== "RUNNING" ||
      this.cleanupTask !== undefined
    ) {
      return;
    }

    const task = this.cleanup()
      .catch(() => {
        logger.warn(
          {
            code: "LOG_OUTBOX_CLEANUP_FAILED",
          },
          "Log notification cleanup failed",
        );
      })
      .finally(() => {
        if (this.cleanupTask === task) {
          this.cleanupTask = undefined;
        }

        if (this.state === "RUNNING") {
          this.scheduleCleanup(
            CLEANUP_INTERVAL_MS,
          );
        }
      });

    this.cleanupTask = task;
  }

  // ==================================================
  // Bounded published notification cleanup
  // ==================================================

  /**
   * Remove old published notifications only.
   *
   * Never remove PENDING or PUBLISHING rows.
   *
   * DEAD row retention remains the responsibility
   * of the worker retention subsystem.
   */
  private async cleanup():
    Promise<void> {
    for (
      let index = 0;
      index < MAX_CLEANUP_BATCHES;
      index += 1
    ) {
      if (this.state !== "RUNNING") {
        break;
      }

      const deleted =
        await prisma.$executeRaw`
          WITH expired AS (
            SELECT "id"
            FROM "deployment_log_notification_outbox"
            WHERE "status" = 'PUBLISHED'
              AND "publishedAt" IS NOT NULL
              AND "publishedAt" <
                (clock_timestamp() AT TIME ZONE 'UTC')
                - (
                  ${PUBLISHED_RETENTION_MS}
                  * interval '1 millisecond'
                )
              AND "leaseToken" IS NULL
              AND "leaseExpiresAt" IS NULL
            ORDER BY
              "publishedAt" ASC,
              "id" ASC
            LIMIT ${CLEANUP_BATCH_SIZE}
            FOR UPDATE SKIP LOCKED
          )
          DELETE FROM
            "deployment_log_notification_outbox" AS outbox
          USING expired
          WHERE outbox."id" = expired."id"
            AND outbox."status" = 'PUBLISHED'
            AND outbox."leaseToken" IS NULL
            AND outbox."leaseExpiresAt" IS NULL
        `;

      if (deleted < CLEANUP_BATCH_SIZE) {
        break;
      }
    }
  }

  // ==================================================
  // Graceful shutdown
  // ==================================================

  async stop(): Promise<void> {
    if (this.stopTask !== undefined) {
      return this.stopTask;
    }

    if (this.state === "STOPPED") {
      return;
    }

    this.state = "STOPPING";

    this.stopTask =
      this.performStop();

    return this.stopTask;
  }

  private async performStop():
    Promise<void> {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);

      this.timer = undefined;
    }

    if (this.cleanupTimer !== undefined) {
      clearTimeout(this.cleanupTimer);

      this.cleanupTimer = undefined;
    }

    const activeTasks: Promise<void>[] = [];

    if (this.publishTask !== undefined) {
      activeTasks.push(this.publishTask);
    }

    if (this.cleanupTask !== undefined) {
      activeTasks.push(this.cleanupTask);
    }

    let drained = true;

    try {
      await withTimeout(
        Promise.allSettled(activeTasks),
        SHUTDOWN_DRAIN_TIMEOUT_MS,
      );
    } catch {
      drained = false;

      logger.warn(
        {
          code: "LOG_DISTRIBUTION_DRAIN_TIMEOUT",
        },
        "Log distribution shutdown drain timed out",
      );
    }

    try {
      if (
        drained &&
        this.redis.status === "ready"
      ) {
        await withTimeout(
          this.redis.quit(),
          REDIS_SHUTDOWN_TIMEOUT_MS,
        );
      }
    } catch {
      logger.debug(
        {
          code: "LOG_REDIS_SHUTDOWN_TIMEOUT",
        },
        "Log publisher Redis shutdown incomplete",
      );
    } finally {
      this.redis.disconnect();

      this.redis.off(
        "error",
        this.handleRedisError,
      );

      this.state = "STOPPED";
    }

    if (drained) {
      logger.debug(
        "Log notification distribution stopped",
      );
    } else {
      logger.warn(
        "Log notification distribution stopped with unresolved operations",
      );
    }
  }
}