/**
 * CloudPilot — Log Retention & Cleanup
 *
 * Performs bounded maintenance of unified deployment logs
 * and obsolete log notification outbox records.
 *
 * Security and reliability properties:
 *
 * - Parameterized SQL queries.
 * - Bounded deletion batches.
 * - Bounded work per maintenance execution.
 * - Deterministic oldest-first deletion.
 * - Concurrent worker safety using SKIP LOCKED.
 * - Process-local overlapping execution protection.
 * - Validated retention configuration and timestamps.
 * - Incremental deletion metrics.
 * - No deletion of active notification outbox records.
 * - No modification of deployment sequence allocators.
 *
 * This is an internal, cross-tenant maintenance operation.
 * It must never be exposed directly to untrusted callers.
 *
 * No deployment, build, runtime, container, artifact,
 * environment variable, or audit records are touched.
 *
 * Active deployments are handled by retention policy:
 * expired entries may be removed regardless of deployment
 * state. Reconnecting clients must use history bounds and
 * retention-gap handling to recover safely.
 *
 * Sequence numbers are never reset or reused by retention.
 */

import { performance } from "node:perf_hooks";

import { prisma } from "@cloudpilot/database";

import { workerEnv } from "../config/env.js";

import { logMetrics } from "./metrics.js";

// ==================================================
// Constants
// ==================================================

const DAY_MS = 86_400_000;

/**
 * Maximum rows removed by a single SQL statement.
 */
const DELETE_BATCH = 1_000;

/**
 * Maximum unified log deletion batches per execution.
 */
const MAX_LOG_DELETE_BATCHES = 20;

/**
 * Maximum notification outbox deletion batches
 * per execution.
 */
const MAX_OUTBOX_DELETE_BATCHES = 10;

/**
 * Published notifications are retained for one day.
 *
 * PostgreSQL log entries remain the authoritative
 * history source.
 */
const PUBLISHED_OUTBOX_RETENTION_MS = DAY_MS;

/**
 * Maximum cooperative maintenance execution budget.
 *
 * Checked between database operations. This is not
 * a hard database statement cancellation deadline.
 */
const MAX_EXECUTION_MS = 30_000;

/**
 * Stop cleanup before exhausting the execution budget.
 */
const MIN_REMAINING_BUDGET_MS = 250;

/**
 * Cooperative pause between full deletion batches.
 */
const BATCH_PAUSE_MS = 25;

/**
 * Defensive retention configuration bounds.
 */
const MIN_RETENTION_DAYS = 1;

const MAX_RETENTION_DAYS = 3_650;

// ==================================================
// Public contracts
// ==================================================

export interface RetentionResult {
  deletedEntries: number;
}

// ==================================================
// Internal contracts
// ==================================================

interface RetentionCutoffs {
  logCutoff: Date;

  publishedOutboxCutoff: Date;
}

interface RetentionExecution {
  startedAt: number;

  cutoffs: RetentionCutoffs;
}

// ==================================================
// Process-local execution protection
// ==================================================

/**
 * Concurrent calls in the same worker process share
 * the currently running maintenance execution.
 *
 * Cross-process concurrency is handled by PostgreSQL
 * row locks and SKIP LOCKED.
 */
let activeRetention:
  | Promise<RetentionResult>
  | undefined;

// ==================================================
// Configuration validation
// ==================================================

function validateRetentionDays(
  days: number,
): void {
  if (
    !Number.isSafeInteger(days) ||
    days < MIN_RETENTION_DAYS ||
    days > MAX_RETENTION_DAYS
  ) {
    throw new Error(
      "Invalid deployment log retention configuration.",
    );
  }
}

function validateTimestamp(
  value: Date,
): number {
  if (!(value instanceof Date)) {
    throw new Error(
      "Invalid log retention timestamp.",
    );
  }

  const timestamp = value.getTime();

  if (!Number.isFinite(timestamp)) {
    throw new Error(
      "Invalid log retention timestamp.",
    );
  }

  return timestamp;
}

function createCutoffs(
  now: Date,
): RetentionCutoffs {
  const timestamp = validateTimestamp(now);

  const retentionDays =
    workerEnv.LOG_RETENTION_DAYS;

  validateRetentionDays(retentionDays);

  const retentionMs =
    retentionDays * DAY_MS;

  if (
    !Number.isSafeInteger(retentionMs) ||
    retentionMs <= 0
  ) {
    throw new Error(
      "Invalid deployment log retention duration.",
    );
  }

  const logCutoff = new Date(
    timestamp - retentionMs,
  );

  const publishedOutboxCutoff = new Date(
    timestamp -
      PUBLISHED_OUTBOX_RETENTION_MS,
  );

  if (
    !Number.isFinite(logCutoff.getTime()) ||
    !Number.isFinite(
      publishedOutboxCutoff.getTime(),
    )
  ) {
    throw new Error(
      "Log retention cutoff is outside its valid range.",
    );
  }

  return {
    logCutoff,

    publishedOutboxCutoff,
  };
}

// ==================================================
// Execution budget
// ==================================================

function elapsedMs(
  execution: RetentionExecution,
): number {
  return Math.max(
    0,
    performance.now() -
      execution.startedAt,
  );
}

function remainingBudgetMs(
  execution: RetentionExecution,
): number {
  return Math.max(
    0,
    MAX_EXECUTION_MS -
      elapsedMs(execution),
  );
}

function hasExecutionBudget(
  execution: RetentionExecution,
): boolean {
  return (
    remainingBudgetMs(execution) >=
    MIN_REMAINING_BUDGET_MS
  );
}

async function pauseBetweenBatches(
  execution: RetentionExecution,
): Promise<void> {
  if (!hasExecutionBudget(execution)) {
    return;
  }

  const delay = Math.min(
    BATCH_PAUSE_MS,
    remainingBudgetMs(execution),
  );

  if (delay <= 0) {
    return;
  }

  await new Promise<void>((resolve) => {
    setTimeout(resolve, delay);
  });
}

// ==================================================
// Database result validation
// ==================================================

function validateDeletedCount(
  count: number,
  maximum: number,
): number {
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    count > maximum
  ) {
    throw new Error(
      "Unexpected log retention deletion result.",
    );
  }

  return count;
}

// ==================================================
// Unified log entry cleanup
// ==================================================

/**
 * Delete a bounded set of expired log entries.
 *
 * The selection and deletion occur in one atomic
 * PostgreSQL statement.
 *
 * FOR UPDATE SKIP LOCKED allows concurrent maintenance
 * workers to skip rows already locked by another
 * transaction.
 *
 * The deployment log sequence table is intentionally
 * not modified.
 */
async function deleteExpiredLogBatch(
  cutoff: Date,
): Promise<number> {
  const deleted = await prisma.$executeRaw`
    WITH "expired_logs" AS (
      SELECT
        "id"
      FROM
        "deployment_log_entries"
      WHERE
        "createdAt" < ${cutoff}
      ORDER BY
        "createdAt" ASC,
        "id" ASC
      LIMIT ${DELETE_BATCH}
      FOR UPDATE SKIP LOCKED
    )
    DELETE FROM
      "deployment_log_entries" AS "logs"
    USING
      "expired_logs"
    WHERE
      "logs"."id" = "expired_logs"."id"
      AND "logs"."createdAt" < ${cutoff}
  `;

  return validateDeletedCount(
    deleted,
    DELETE_BATCH,
  );
}

// ==================================================
// Published notification cleanup
// ==================================================

/**
 * Delete obsolete successfully published notifications.
 *
 * PENDING and PUBLISHING notifications are never
 * selected by this operation.
 *
 * Rows carrying an active or unresolved lease token
 * are preserved for the publisher/reconciliation layer.
 */
async function deletePublishedOutboxBatch(
  cutoff: Date,
): Promise<number> {
  const deleted = await prisma.$executeRaw`
    WITH "expired_notifications" AS (
      SELECT
        "id"
      FROM
        "deployment_log_notification_outbox"
      WHERE
        "status" = 'PUBLISHED'
        AND "publishedAt" IS NOT NULL
        AND "publishedAt" < ${cutoff}
        AND "leaseToken" IS NULL
        AND "leaseExpiresAt" IS NULL
      ORDER BY
        "publishedAt" ASC,
        "id" ASC
      LIMIT ${DELETE_BATCH}
      FOR UPDATE SKIP LOCKED
    )
    DELETE FROM
      "deployment_log_notification_outbox"
        AS "notifications"
    USING
      "expired_notifications"
    WHERE
      "notifications"."id" =
        "expired_notifications"."id"
      AND "notifications"."status" =
        'PUBLISHED'
      AND "notifications"."publishedAt" <
        ${cutoff}
      AND "notifications"."leaseToken"
        IS NULL
      AND "notifications"."leaseExpiresAt"
        IS NULL
  `;

  return validateDeletedCount(
    deleted,
    DELETE_BATCH,
  );
}

// ==================================================
// Dead notification cleanup
// ==================================================

/**
 * Delete obsolete terminally failed notifications.
 *
 * DEAD notifications remain available for investigation
 * until the configured retention period expires.
 *
 * Notification deletion does not delete the underlying
 * persisted deployment log entries.
 */
async function deleteDeadOutboxBatch(
  cutoff: Date,
): Promise<number> {
  const deleted = await prisma.$executeRaw`
    WITH "expired_notifications" AS (
      SELECT
        "id"
      FROM
        "deployment_log_notification_outbox"
      WHERE
        "status" = 'DEAD'
        AND "createdAt" < ${cutoff}
        AND "leaseToken" IS NULL
        AND "leaseExpiresAt" IS NULL
      ORDER BY
        "createdAt" ASC,
        "id" ASC
      LIMIT ${DELETE_BATCH}
      FOR UPDATE SKIP LOCKED
    )
    DELETE FROM
      "deployment_log_notification_outbox"
        AS "notifications"
    USING
      "expired_notifications"
    WHERE
      "notifications"."id" =
        "expired_notifications"."id"
      AND "notifications"."status" = 'DEAD'
      AND "notifications"."createdAt" <
        ${cutoff}
      AND "notifications"."leaseToken"
        IS NULL
      AND "notifications"."leaseExpiresAt"
        IS NULL
  `;

  return validateDeletedCount(
    deleted,
    DELETE_BATCH,
  );
}

// ==================================================
// Bounded notification cleanup
// ==================================================

async function cleanupNotificationOutbox(
  execution: RetentionExecution,
): Promise<void> {
  let publishedExhausted = false;

  let deadExhausted = false;

  let batches = 0;

  /**
   * Alternate between published and dead records
   * to reduce starvation of either cleanup category.
   */
  while (
    batches < MAX_OUTBOX_DELETE_BATCHES &&
    hasExecutionBudget(execution) &&
    (!publishedExhausted ||
      !deadExhausted)
  ) {
    if (
      !publishedExhausted &&
      batches < MAX_OUTBOX_DELETE_BATCHES &&
      hasExecutionBudget(execution)
    ) {
      const deleted =
        await deletePublishedOutboxBatch(
          execution.cutoffs
            .publishedOutboxCutoff,
        );

      batches += 1;

      if (deleted < DELETE_BATCH) {
        publishedExhausted = true;
      }
    }

    if (
      !deadExhausted &&
      batches < MAX_OUTBOX_DELETE_BATCHES &&
      hasExecutionBudget(execution)
    ) {
      const deleted =
        await deleteDeadOutboxBatch(
          execution.cutoffs.logCutoff,
        );

      batches += 1;

      if (deleted < DELETE_BATCH) {
        deadExhausted = true;
      }
    }

    if (
      publishedExhausted &&
      deadExhausted
    ) {
      break;
    }

    if (
      batches < MAX_OUTBOX_DELETE_BATCHES
    ) {
      await pauseBetweenBatches(
        execution,
      );
    }
  }
}

// ==================================================
// Retention execution
// ==================================================

async function executeRetention(
  now: Date,
): Promise<RetentionResult> {
  const execution: RetentionExecution = {
    startedAt: performance.now(),

    cutoffs: createCutoffs(now),
  };

  let deletedEntries = 0;

  /**
   * Each successful batch commits independently.
   *
   * Do not wrap the entire maintenance execution
   * in a long-running transaction.
   */
  for (
    let batch = 0;
    batch < MAX_LOG_DELETE_BATCHES;
    batch += 1
  ) {
    if (!hasExecutionBudget(execution)) {
      break;
    }

    const deleted =
      await deleteExpiredLogBatch(
        execution.cutoffs.logCutoff,
      );

    deletedEntries += deleted;

    /**
     * Update metrics immediately after each successful
     * deletion statement.
     *
     * A later batch or notification cleanup failure
     * must not discard already recorded deletions.
     */
    if (deleted > 0) {
      logMetrics.increment(
        "retentionDeletions",
        deleted,
      );
    }

    /**
     * Fewer rows than the batch limit means there
     * is no additional immediately available full
     * batch.
     *
     * Locked rows may still exist and will be
     * reconsidered during a later maintenance run.
     */
    if (deleted < DELETE_BATCH) {
      break;
    }

    if (
      batch + 1 <
      MAX_LOG_DELETE_BATCHES
    ) {
      await pauseBetweenBatches(
        execution,
      );
    }
  }

  /**
   * Clean terminal notification records only when
   * sufficient execution budget remains.
   */
  if (hasExecutionBudget(execution)) {
    await cleanupNotificationOutbox(
      execution,
    );
  }

  return {
    deletedEntries,
  };
}

// ==================================================
// Public retention entry point
// ==================================================

/**
 * Run bounded deployment log retention.
 *
 * Concurrent calls within this process share the
 * current maintenance promise instead of starting
 * overlapping deletion loops.
 *
 * Database errors propagate to the caller so the
 * worker scheduler can report and retry failures.
 *
 * Already committed deletion batches are not rolled
 * back if a later operation fails.
 */
export async function runLogRetention(
  now: Date = new Date(),
): Promise<RetentionResult> {
  if (activeRetention !== undefined) {
    return activeRetention;
  }

  /**
   * Snapshot the caller-provided Date to avoid
   * external mutation during asynchronous execution.
   */
  const timestamp = validateTimestamp(now);

  const execution = executeRetention(
    new Date(timestamp),
  );

  activeRetention = execution;

  try {
    return await execution;
  } finally {
    /**
     * Clear only the execution owned by this call.
     */
    if (activeRetention === execution) {
      activeRetention = undefined;
    }
  }
}