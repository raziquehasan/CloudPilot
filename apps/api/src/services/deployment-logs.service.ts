/**
 * CloudPilot — Deployment Log History Service
 *
 * Phase 5.6 — Realtime Deployment Logs
 *
 * Authoritative, tenant-scoped historical log retrieval
 * from PostgreSQL.
 *
 * Supports:
 * - REST history retrieval.
 * - Forward and backward cursor pagination.
 * - Socket.IO replay and live distribution.
 * - Bounded text downloads.
 * - Retention gap detection.
 *
 * Security:
 * - All database queries use explicit tenant scope.
 * - Callers must authorize access before invoking
 *   this service.
 * - Cursor values are validated before conversion.
 * - Response sizes are bounded.
 * - Database queries never trust arbitrary scope fields.
 */

import { prisma } from "@cloudpilot/database";

import type {
  DeploymentLogEntryDto,
  DeploymentLogLevel,
  DeploymentLogSource,
  DeploymentLogStream,
  LogHistoryResponse,
} from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const DEFAULT_PAGE_SIZE = 200;

const MAX_PAGE_SIZE = 1_000;

const MAX_CURSOR = 9_223_372_036_854_775_807n;

const MAX_ENTRY_BYTES = 16 * 1024;

const MAX_HISTORY_MESSAGE_BYTES =
  8 * 1024 * 1024;

const MAX_RANGE_MESSAGE_BYTES =
  512 * 1024;

const MAX_DOWNLOAD_ENTRIES = 50_000;

const MAX_DOWNLOAD_BYTES =
  50 * 1024 * 1024;

const DOWNLOAD_BATCH_SIZE = 200;

const DOWNLOAD_TIMEOUT_MS = 30_000;

const HISTORY_TRANSACTION_TIMEOUT_MS =
  15_000;

const HISTORY_TRANSACTION_MAX_WAIT_MS =
  5_000;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const CURSOR_PATTERN =
  /^(?:0|[1-9][0-9]{0,18})$/;

const UNSAFE_LOG_CONTROL_PATTERN =
  /[\u0000-\u0008\u000b-\u001f\u007f-\u009f]/u;

const LOG_SOURCES =
  new Set<DeploymentLogSource>([
    "BUILD",
    "RUNTIME",
    "SYSTEM",
  ]);

const LOG_STREAMS =
  new Set<DeploymentLogStream>([
    "STDOUT",
    "STDERR",
    "SYSTEM",
  ]);

const LOG_LEVELS =
  new Set<DeploymentLogLevel>([
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "UNKNOWN",
  ]);

const DOWNLOAD_TRUNCATION_MARKER =
  "[Log export truncated: export limit reached]\n";

const DOWNLOAD_RETENTION_MARKER =
  "[Some historical logs may have been removed by retention]\n";

const DOWNLOAD_MARKER_RESERVE_BYTES =
  Buffer.byteLength(
    DOWNLOAD_TRUNCATION_MARKER,
    "utf8",
  ) +
  Buffer.byteLength(
    DOWNLOAD_RETENTION_MARKER,
    "utf8",
  );

// ==================================================
// Public contracts
// ==================================================

export interface LogScope {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
}

export interface LogHistoryParams {
  after?: string | null;
  before?: string | null;
  source?: DeploymentLogSource;
  stream?: DeploymentLogStream;
  level?: DeploymentLogLevel;
  limit?: number;
}

export interface LogDownload {
  filename: string;
  contentType: string;
  body: string;
}

// ==================================================
// Internal contracts
// ==================================================

interface LogRow {
  id: string;
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  buildId: string | null;
  runtimeInstanceId: string | null;
  source: DeploymentLogSource;
  stream: DeploymentLogStream;
  level: DeploymentLogLevel;
  message: string;
  sequence: bigint;
  eventTimestamp: Date;
  persistedAt: Date;
}

interface SequenceBounds {
  oldest: bigint | null;
  newest: bigint | null;
  allocated: bigint;
}

interface ValidatedHistoryParams {
  after: bigint | null;
  before: bigint | null;
  source: DeploymentLogSource | undefined;
  stream: DeploymentLogStream | undefined;
  level: DeploymentLogLevel | undefined;
  limit: number;
}

interface BoundedRows {
  rows: LogRow[];
  hasMore: boolean;
  messageBytes: number;
}

// ==================================================
// Database field selection
// ==================================================

const LOG_ENTRY_SELECT = {
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
  sequence: true,
  eventTimestamp: true,
  persistedAt: true,
} as const;

// ==================================================
// Service errors
// ==================================================

export class LogHistoryValidationError
  extends Error {
  readonly code =
    "INVALID_LOG_HISTORY_REQUEST";

  constructor(message: string) {
    super(message);

    this.name =
      "LogHistoryValidationError";
  }
}

export class LogHistoryIntegrityError
  extends Error {
  readonly code =
    "LOG_HISTORY_INTEGRITY_ERROR";

  constructor() {
    super(
      "Stored deployment log data failed integrity validation.",
    );

    this.name =
      "LogHistoryIntegrityError";
  }
}

// ==================================================
// Scope validation
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

/**
 * Construct an explicit scope object.
 *
 * Never spread an unvalidated caller object directly
 * into a Prisma query.
 */
function validateScope(
  scope: LogScope,
): LogScope {
  if (
    typeof scope !== "object" ||
    scope === null ||
    !isUuid(scope.workspaceId) ||
    !isUuid(scope.projectId) ||
    !isUuid(scope.deploymentId)
  ) {
    throw new LogHistoryValidationError(
      "Invalid deployment log scope.",
    );
  }

  return {
    workspaceId:
      scope.workspaceId.toLowerCase(),

    projectId:
      scope.projectId.toLowerCase(),

    deploymentId:
      scope.deploymentId.toLowerCase(),
  };
}

// ==================================================
// Cursor validation
// ==================================================

function parseCursor(
  value: unknown,
): bigint | null {
  if (
    value === undefined ||
    value === null
  ) {
    return null;
  }

  if (
    typeof value !== "string" ||
    value.length > 19 ||
    !CURSOR_PATTERN.test(value)
  ) {
    throw new LogHistoryValidationError(
      "Invalid log cursor.",
    );
  }

  const cursor = BigInt(value);

  if (
    cursor < 0n ||
    cursor > MAX_CURSOR
  ) {
    throw new LogHistoryValidationError(
      "Log cursor is outside the supported range.",
    );
  }

  return cursor;
}

function validateSequence(
  value: unknown,
): value is bigint {
  return (
    typeof value === "bigint" &&
    value > 0n &&
    value <= MAX_CURSOR
  );
}

// ==================================================
// Limit validation
// ==================================================

function clampLimit(
  limit?: number,
): number {
  if (limit === undefined) {
    return DEFAULT_PAGE_SIZE;
  }

  if (
    typeof limit !== "number" ||
    !Number.isSafeInteger(limit) ||
    limit < 1
  ) {
    throw new LogHistoryValidationError(
      "Invalid log history page size.",
    );
  }

  return Math.min(
    limit,
    MAX_PAGE_SIZE,
  );
}

// ==================================================
// History parameter validation
// ==================================================

function validateHistoryParams(
  params: LogHistoryParams,
): ValidatedHistoryParams {
  if (
    typeof params !== "object" ||
    params === null ||
    Array.isArray(params)
  ) {
    throw new LogHistoryValidationError(
      "Invalid log history parameters.",
    );
  }

  const after = parseCursor(
    params.after,
  );

  const before = parseCursor(
    params.before,
  );

  if (
    after !== null &&
    before !== null
  ) {
    throw new LogHistoryValidationError(
      "The after and before cursors cannot be used together.",
    );
  }

  if (
    params.source !== undefined &&
    !LOG_SOURCES.has(params.source)
  ) {
    throw new LogHistoryValidationError(
      "Invalid log source.",
    );
  }

  if (
    params.stream !== undefined &&
    !LOG_STREAMS.has(params.stream)
  ) {
    throw new LogHistoryValidationError(
      "Invalid log stream.",
    );
  }

  if (
    params.level !== undefined &&
    !LOG_LEVELS.has(params.level)
  ) {
    throw new LogHistoryValidationError(
      "Invalid log level.",
    );
  }

  return {
    after,
    before,

    source: params.source,

    stream: params.stream,

    level: params.level,

    limit: clampLimit(
      params.limit,
    ),
  };
}

// ==================================================
// Stored row integrity validation
// ==================================================

function validateLogRow(
  row: LogRow,
  scope: LogScope,
): number {
  if (
    !isUuid(row.id) ||
    row.workspaceId !==
      scope.workspaceId ||
    row.projectId !==
      scope.projectId ||
    row.deploymentId !==
      scope.deploymentId ||
    (
      row.buildId !== null &&
      !isUuid(row.buildId)
    ) ||
    (
      row.runtimeInstanceId !== null &&
      !isUuid(row.runtimeInstanceId)
    ) ||
    !validateSequence(row.sequence) ||
    !LOG_SOURCES.has(row.source) ||
    !LOG_STREAMS.has(row.stream) ||
    !LOG_LEVELS.has(row.level) ||
    typeof row.message !== "string" ||
    row.message.length === 0 ||
    row.message.length >
      MAX_ENTRY_BYTES ||
    UNSAFE_LOG_CONTROL_PATTERN.test(
      row.message,
    ) ||
    !(row.eventTimestamp instanceof Date) ||
    !Number.isFinite(
      row.eventTimestamp.getTime(),
    ) ||
    !(row.persistedAt instanceof Date) ||
    !Number.isFinite(
      row.persistedAt.getTime(),
    )
  ) {
    throw new LogHistoryIntegrityError();
  }

  const messageBytes =
    Buffer.byteLength(
      row.message,
      "utf8",
    );

  if (
    messageBytes < 1 ||
    messageBytes > MAX_ENTRY_BYTES
  ) {
    throw new LogHistoryIntegrityError();
  }

  return messageBytes;
}

// ==================================================
// DTO serialization
// ==================================================

function toDto(
  row: LogRow,
): DeploymentLogEntryDto {
  return {
    id: row.id,

    workspaceId: row.workspaceId,

    projectId: row.projectId,

    deploymentId: row.deploymentId,

    buildId: row.buildId,

    runtimeInstanceId:
      row.runtimeInstanceId,

    source: row.source,

    stream: row.stream,

    level: row.level,

    message: row.message,

    sequence:
      row.sequence.toString(10),

    eventTimestamp:
      row.eventTimestamp.toISOString(),

    persistedAt:
      row.persistedAt.toISOString(),
  };
}

// ==================================================
// Bounded result handling
// ==================================================

/**
 * Keep the requested query order while enforcing
 * both entry count and aggregate message byte limits.
 *
 * The continuation cursor must always be based on
 * the last entry actually returned to the caller.
 */
function boundRows(
  rows: readonly LogRow[],
  scope: LogScope,
  limit: number,
  maxMessageBytes: number,
  descending = false,
): BoundedRows {
  const accepted: LogRow[] = [];

  let messageBytes = 0;

  let previousSequence:
    bigint | null = null;

  const ids = new Set<string>();

  for (const row of rows) {
    if (accepted.length >= limit) {
      break;
    }

    const rowBytes =
      validateLogRow(
        row,
        scope,
      );

    if (ids.has(row.id)) {
      throw new LogHistoryIntegrityError();
    }

    if (
      previousSequence !== null &&
      (
        descending
          ? row.sequence >= previousSequence
          : row.sequence <= previousSequence
      )
    ) {
      throw new LogHistoryIntegrityError();
    }

    if (
      messageBytes + rowBytes >
      maxMessageBytes
    ) {
      break;
    }

    accepted.push(row);

    ids.add(row.id);

    messageBytes += rowBytes;

    previousSequence =
      row.sequence;
  }

  return {
    rows: accepted,

    hasMore:
      accepted.length < rows.length,

    messageBytes,
  };
}

// ==================================================
// Sequence bounds
// ==================================================

/**
 * The transaction client is inferred from Prisma's
 * interactive transaction callback.
 *
 * Keeping the allocator and retained bounds in the
 * same snapshot avoids inconsistent metadata during
 * concurrent log insertion and retention deletion.
 */
type TransactionClient =
  Parameters<
    Parameters<
      typeof prisma.$transaction
    >[0]
  >[0];

async function sequenceBounds(
  db: TransactionClient,
  scope: LogScope,
): Promise<SequenceBounds> {
  const oldestRows =
    await db.deploymentLogEntry.findMany({
      where: scope,

      select: {
        sequence: true,
      },

      orderBy: {
        sequence: "asc",
      },

      take: 1,
    });

  const newestRows =
    await db.deploymentLogEntry.findMany({
      where: scope,

      select: {
        sequence: true,
      },

      orderBy: {
        sequence: "desc",
      },

      take: 1,
    });

  const allocator =
    await db.deploymentLogSequence.findFirst({
      where: {
        workspaceId:
          scope.workspaceId,

        deploymentId:
          scope.deploymentId,
      },

      select: {
        value: true,
      },
    });

  const oldest =
    oldestRows[0]?.sequence ?? null;

  const newest =
    newestRows[0]?.sequence ?? null;

  const allocated =
    allocator?.value ?? 0n;

  if (
    allocated < 0n ||
    allocated > MAX_CURSOR ||
    (
      (oldest === null) !==
      (newest === null)
    )
  ) {
    throw new LogHistoryIntegrityError();
  }

  if (
    oldest !== null &&
    newest !== null &&
    (
      !validateSequence(oldest) ||
      !validateSequence(newest) ||
      oldest > newest ||
      newest > allocated
    )
  ) {
    throw new LogHistoryIntegrityError();
  }

  return {
    oldest,
    newest,
    allocated,
  };
}

// ==================================================
// Retention gap detection
// ==================================================

/**
 * Sequence allocation is monotonic and transactional.
 *
 * Missing retained sequence values indicate that
 * history may no longer be complete.
 *
 * Filters must not be used to infer retention gaps.
 */
function detectRetentionGap(
  bounds: SequenceBounds,
  after: bigint | null,
): boolean {
  if (bounds.oldest === null) {
    // The allocator survives log retention.
    // A positive allocator with no retained rows
    // means previously allocated history is missing.

    return bounds.allocated > 0n;
  }

  if (bounds.oldest > 1n) {
    return true;
  }

  if (
    bounds.newest !== null &&
    bounds.allocated > bounds.newest
  ) {
    return true;
  }

  if (
    after !== null &&
    after + 1n < bounds.oldest
  ) {
    return true;
  }

  return false;
}

/**
 * Detect missing sequence values inside an unfiltered
 * page without treating filtered rows as missing.
 */
function detectInternalGap(
  rows: readonly LogRow[],
): boolean {
  for (
    let index = 1;
    index < rows.length;
    index += 1
  ) {
    const previous = rows[index - 1];

    const current = rows[index];

    if (
      previous &&
      current &&
      current.sequence !==
        previous.sequence + 1n
    ) {
      return true;
    }
  }

  return false;
}

// ==================================================
// Historical log retrieval
// ==================================================

/**
 * Cursor semantics:
 *
 * - after: sequence > cursor, ascending.
 * - before: sequence < cursor, descending query,
 *   then reverse into ascending response order.
 * - no cursor: earliest retained entries.
 *
 * Bounds and rows are read using RepeatableRead
 * isolation so retention and concurrent writers
 * cannot produce inconsistent response metadata.
 */
export async function getLogHistory(
  scope: LogScope,
  params: LogHistoryParams,
): Promise<LogHistoryResponse> {
  const safeScope =
    validateScope(scope);

  const validated =
    validateHistoryParams(params);

  return prisma.$transaction(
    async (db) => {
      const bounds =
        await sequenceBounds(
          db,
          safeScope,
        );

      const descending =
        validated.before !== null;

      const sequenceFilter =
        validated.before !== null
          ? {
              lt: validated.before,
            }
          : validated.after !== null
            ? {
                gt: validated.after,
              }
            : undefined;

      const rows =
        await db.deploymentLogEntry.findMany({
          where: {
            workspaceId:
              safeScope.workspaceId,

            projectId:
              safeScope.projectId,

            deploymentId:
              safeScope.deploymentId,

            ...(validated.source !== undefined
              ? {
                  source:
                    validated.source,
                }
              : {}),

            ...(validated.stream !== undefined
              ? {
                  stream:
                    validated.stream,
                }
              : {}),

            ...(validated.level !== undefined
              ? {
                  level:
                    validated.level,
                }
              : {}),

            ...(sequenceFilter !== undefined
              ? {
                  sequence:
                    sequenceFilter,
                }
              : {}),
          },

          orderBy: {
            sequence: descending
              ? "desc"
              : "asc",
          },

          take: validated.limit + 1,

          select: LOG_ENTRY_SELECT,
        });

      const bounded = boundRows(
        rows,
        safeScope,
        validated.limit,
        MAX_HISTORY_MESSAGE_BYTES,
        descending,
      );

      const items = descending
        ? [...bounded.rows].reverse()
        : bounded.rows;

      const first = items[0];

      const last =
        items[items.length - 1];

      if (
        items.length > 0 &&
        (
          bounds.oldest === null ||
          bounds.newest === null ||
          !first ||
          !last ||
          first.sequence < bounds.oldest ||
          last.sequence > bounds.newest
        )
      ) {
        throw new LogHistoryIntegrityError();
      }

      const hasMore =
        bounded.hasMore;

      const nextCursor =
        !descending &&
        hasMore &&
        last
          ? last.sequence.toString(10)
          : null;

      const previousCursor =
        descending &&
        hasMore &&
        first
          ? first.sequence.toString(10)
          : null;

      const hasFilters =
        validated.source !== undefined ||
        validated.stream !== undefined ||
        validated.level !== undefined;

      let retentionTruncated =
        detectRetentionGap(
          bounds,
          validated.after,
        );

      if (!hasFilters) {
        if (detectInternalGap(items)) {
          retentionTruncated = true;
        }

        if (
          !descending &&
          validated.after !== null &&
          first &&
          first.sequence >
            validated.after + 1n
        ) {
          retentionTruncated = true;
        }

        if (
          descending &&
          validated.before !== null &&
          last &&
          bounds.newest !== null &&
          validated.before <=
            bounds.newest + 1n &&
          last.sequence + 1n <
            validated.before
        ) {
          retentionTruncated = true;
        }
      }

      return {
        items: items.map(toDto),

        nextCursor,

        previousCursor,

        hasMore,

        oldestSequence:
          bounds.oldest?.toString(10) ??
          null,

        newestSequence:
          bounds.newest?.toString(10) ??
          null,

        retentionTruncated,
      };
    },
    {
      isolationLevel: "RepeatableRead",

      maxWait:
        HISTORY_TRANSACTION_MAX_WAIT_MS,

      timeout:
        HISTORY_TRANSACTION_TIMEOUT_MS,
    },
  );
}

// ==================================================
// Live distribution range retrieval
// ==================================================

/**
 * Fetch a bounded prefix of an inclusive sequence
 * range for live distribution.
 *
 * IMPORTANT:
 * The caller must continue from the last returned
 * sequence, not the requested upper bound.
 *
 * A short result may be caused by the byte budget
 * rather than exhaustion of the requested range.
 */
export async function getEntriesInRange(
  scope: LogScope,
  fromSequence: bigint,
  toSequence: bigint,
  limit: number,
): Promise<DeploymentLogEntryDto[]> {
  const safeScope =
    validateScope(scope);

  if (
    !validateSequence(fromSequence) ||
    !validateSequence(toSequence)
  ) {
    throw new LogHistoryValidationError(
      "Invalid log sequence range.",
    );
  }

  const safeLimit =
    clampLimit(limit);

  if (
    toSequence < fromSequence
  ) {
    return [];
  }

  const rows =
    await prisma.deploymentLogEntry.findMany({
      where: {
        workspaceId:
          safeScope.workspaceId,

        projectId:
          safeScope.projectId,

        deploymentId:
          safeScope.deploymentId,

        sequence: {
          gte: fromSequence,
          lte: toSequence,
        },
      },

      orderBy: {
        sequence: "asc",
      },

      take: safeLimit,

      select: LOG_ENTRY_SELECT,
    });

  const bounded = boundRows(
    rows,
    safeScope,
    safeLimit,
    MAX_RANGE_MESSAGE_BYTES,
  );

  for (const row of bounded.rows) {
    if (
      row.sequence < fromSequence ||
      row.sequence > toSequence
    ) {
      throw new LogHistoryIntegrityError();
    }
  }

  return bounded.rows.map(toDto);
}

// ==================================================
// Download helpers
// ==================================================

function formatDownloadLine(
  row: LogRow,
): string {
  const timestamp =
    row.eventTimestamp.toISOString();

  const prefix =
    `${timestamp} ` +
    `[${row.source}:${row.stream}:${row.level}] `;

  // Prefix every physical line to prevent multiline
  // messages from appearing as unlabelled records.

  const message =
    row.message.replaceAll(
      "\n",
      `\n${prefix}`,
    );

  return `${prefix}${message}\n`;
}

function createDownloadFilename(
  deploymentId: string,
): string {
  return (
    `deployment-${deploymentId}-logs.txt`
  );
}

// ==================================================
// Bounded log download
// ==================================================

/**
 * Download logs using bounded keyset pagination.
 *
 * The initial upper sequence bound prevents a
 * continuously running deployment from extending
 * the export indefinitely.
 *
 * The public return contract remains compatible
 * with the existing download controller.
 *
 * Retention may run between download batches.
 * This export is not a long-lived database snapshot.
 */
export async function downloadLogs(
  scope: LogScope,
): Promise<LogDownload | null> {
  const safeScope =
    validateScope(scope);

  const startedAt =
    performance.now();

  const bounds =
    await prisma.$transaction(
      async (db) =>
        sequenceBounds(
          db,
          safeScope,
        ),
      {
        isolationLevel:
          "RepeatableRead",

        maxWait:
          HISTORY_TRANSACTION_MAX_WAIT_MS,

        timeout:
          HISTORY_TRANSACTION_TIMEOUT_MS,
      },
    );

  const filename =
    createDownloadFilename(
      safeScope.deploymentId,
    );

  const lines: string[] = [];

  let bytes = 0;

  let entryCount = 0;

  let truncated = false;

  let retentionTruncated =
    detectRetentionGap(
      bounds,
      null,
    );

  let cursor = 0n;

  const upperBound =
    bounds.newest;

  const contentBudget =
    MAX_DOWNLOAD_BYTES -
    DOWNLOAD_MARKER_RESERVE_BYTES;

  if (upperBound !== null) {
    while (
      cursor < upperBound
    ) {
      if (
        performance.now() -
          startedAt >=
          DOWNLOAD_TIMEOUT_MS ||
        entryCount >=
          MAX_DOWNLOAD_ENTRIES
      ) {
        truncated = true;

        break;
      }

      const remainingEntries =
        MAX_DOWNLOAD_ENTRIES -
        entryCount;

      const batchSize = Math.min(
        DOWNLOAD_BATCH_SIZE,
        remainingEntries,
      );

      const rows =
        await prisma.deploymentLogEntry.findMany({
          where: {
            workspaceId:
              safeScope.workspaceId,

            projectId:
              safeScope.projectId,

            deploymentId:
              safeScope.deploymentId,

            sequence: {
              gt: cursor,

              lte: upperBound,
            },
          },

          orderBy: {
            sequence: "asc",
          },

          take: batchSize,

          select: LOG_ENTRY_SELECT,
        });

      if (rows.length === 0) {
        if (cursor < upperBound) {
          retentionTruncated = true;
        }

        break;
      }

      for (const row of rows) {
        if (
          performance.now() -
            startedAt >=
            DOWNLOAD_TIMEOUT_MS
        ) {
          truncated = true;

          break;
        }

        validateLogRow(
          row,
          safeScope,
        );

        if (
          row.sequence <= cursor ||
          row.sequence > upperBound
        ) {
          throw new LogHistoryIntegrityError();
        }

        if (
          row.sequence >
          cursor + 1n
        ) {
          retentionTruncated = true;
        }

        const line =
          formatDownloadLine(row);

        const lineBytes =
          Buffer.byteLength(
            line,
            "utf8",
          );

        if (
          bytes + lineBytes >
          contentBudget
        ) {
          truncated = true;

          break;
        }

        lines.push(line);

        bytes += lineBytes;

        entryCount += 1;

        cursor = row.sequence;
      }

      if (truncated) {
        break;
      }

      if (
        rows.length < batchSize
      ) {
        if (cursor < upperBound) {
          retentionTruncated = true;
        }

        break;
      }
    }
  }

  if (retentionTruncated) {
    lines.push(
      DOWNLOAD_RETENTION_MARKER,
    );
  }

  if (truncated) {
    lines.push(
      DOWNLOAD_TRUNCATION_MARKER,
    );
  }

  const body =
    lines.join("");

  if (
    Buffer.byteLength(
      body,
      "utf8",
    ) > MAX_DOWNLOAD_BYTES
  ) {
    throw new LogHistoryIntegrityError();
  }

  return {
    filename,

    contentType:
      "text/plain; charset=utf-8",

    body,
  };
}