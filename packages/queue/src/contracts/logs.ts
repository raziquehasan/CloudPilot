
/**
 * CloudPilot — Deployment Log Contracts
 *
 * Shared, validated contracts for the real-time deployment
 * logging and streaming engine (Phase 5.6).
 *
 * These contracts are used by the worker (collection and
 * persistence), the API (REST + Socket.IO distribution),
 * and the web client (rendering and live subscription).
 *
 * Design invariants:
 * - The original stream (stdout/stderr/system) is preserved
 *   independently of the inferred log level. A stderr line is
 *   not automatically an error.
 * - BigInt values (sequence, cursors) are serialized as
 *   canonical decimal strings in every API and WebSocket
 *   payload.
 * - Cursor values remain within PostgreSQL signed BIGINT
 *   boundaries.
 * - Identifiers, message sizes, timestamps, enums and cursor
 *   values are validated at every trust boundary.
 * - Log batches are bounded by entry count and message bytes.
 * - Shared contracts remain compatible with Node.js and
 *   modern browser environments.
 * - Internal exceptions, credentials and database errors
 *   must never be exposed to clients.
 *
 * Security boundary:
 * Schema validation does not replace authentication,
 * tenant authorization, execution fencing or secret
 * redaction.
 */

import { z } from "zod";

// ==================================================
// Constants
// ==================================================

const KiB = 1024;
const MiB = 1024 * KiB;

/**
 * Maximum signed PostgreSQL BIGINT value.
 */
export const MAX_LOG_CURSOR =
  9_223_372_036_854_775_807n;

/**
 * Maximum accepted sanitized message size in UTF-8 bytes.
 *
 * Runtime configuration may impose a smaller limit but
 * must not exceed this shared protocol limit.
 */
export const MAX_LOG_MESSAGE_BYTES = 16 * KiB;

/**
 * Maximum number of entries in a historical response.
 */
export const MAX_LOG_HISTORY_ENTRIES = 1_000;

/**
 * Maximum number of entries in a real-time socket batch.
 */
export const MAX_LOG_BATCH_ENTRIES = 1_000;

/**
 * Aggregate UTF-8 message budgets.
 *
 * These limits cover message content only. Transport
 * framing, JSON escaping and metadata require additional
 * limits at the API and Socket.IO boundaries.
 */
export const MAX_LOG_HISTORY_MESSAGE_BYTES =
  8 * MiB;

export const MAX_LOG_BATCH_MESSAGE_BYTES =
  512 * KiB;

const MAX_LOG_ERROR_MESSAGE_LENGTH = 500;

/**
 * Reject unsafe C0/C1 controls while preserving
 * horizontal tabs and line breaks.
 *
 * ESC, NUL, DEL and carriage return are rejected.
 * Normalization must convert CRLF before validation.
 */
const unsafeLogControlPattern =
  /[\u0000-\u0008\u000B-\u001F\u007F-\u009F]/u;

const unsafeMetadataControlPattern =
  /[\u0000-\u001F\u007F-\u009F]/u;

const canonicalCursorPattern =
  /^(?:0|[1-9][0-9]{0,18})$/u;

const positiveIntegerPattern =
  /^[1-9][0-9]{0,3}$/u;

const protocolCodePattern =
  /^[A-Z0-9_]{1,64}$/u;

/**
 * TextEncoder is supported by Node.js and modern browsers.
 *
 * Avoid Buffer in shared browser-facing contracts.
 */
const utf8Encoder = new TextEncoder();

// ==================================================
// Enumerations
// ==================================================

export const deploymentLogSources = Object.freeze([
  "BUILD",
  "RUNTIME",
  "SYSTEM",
] as const);

export type DeploymentLogSource =
  (typeof deploymentLogSources)[number];

export const deploymentLogStreams = Object.freeze([
  "STDOUT",
  "STDERR",
  "SYSTEM",
] as const);

export type DeploymentLogStream =
  (typeof deploymentLogStreams)[number];

export const deploymentLogLevels = Object.freeze([
  "DEBUG",
  "INFO",
  "WARN",
  "ERROR",
  "UNKNOWN",
] as const);

export type DeploymentLogLevel =
  (typeof deploymentLogLevels)[number];

// ==================================================
// Internal validation helpers
// ==================================================

/**
 * Return the UTF-8 byte length of a bounded string.
 */
function utf8ByteLength(value: string): number {
  return utf8Encoder.encode(value).byteLength;
}

/**
 * Compare validated decimal cursor strings without
 * converting them into JavaScript numbers.
 *
 * Returns null for invalid cursor values.
 */
function compareCursors(
  left: string,
  right: string,
): -1 | 0 | 1 | null {
  const leftValue = parseCursor(left);
  const rightValue = parseCursor(right);

  if (
    leftValue === null ||
    rightValue === null
  ) {
    return null;
  }

  if (leftValue < rightValue) {
    return -1;
  }

  if (leftValue > rightValue) {
    return 1;
  }

  return 0;
}

/**
 * UUID hexadecimal characters are case-insensitive.
 */
function sameIdentifier(
  left: string,
  right: string,
): boolean {
  return (
    left.toLowerCase() ===
    right.toLowerCase()
  );
}

/**
 * Validate aggregate message content without allocating
 * a second array or serializing the entire response.
 */
function fitsMessageBudget(
  entries: readonly {
    message: string;
  }[],
  maximumBytes: number,
): boolean {
  let totalBytes = 0;

  for (const entry of entries) {
    if (
      entry.message.length >
      MAX_LOG_MESSAGE_BYTES
    ) {
      return false;
    }

    totalBytes += utf8ByteLength(
      entry.message,
    );

    if (totalBytes > maximumBytes) {
      return false;
    }
  }

  return true;
}

// ==================================================
// Primitive schemas
// ==================================================

const uuidSchema = z
  .string()
  .length(36)
  .uuid();

/**
 * A durable, monotonic log cursor serialized as a
 * canonical decimal string.
 *
 * Clients must echo cursors back verbatim for pagination
 * and resubscription.
 *
 * Zero is reserved as a valid initial cursor.
 */
const sequenceStringSchema = z
  .string()
  .min(1)
  .max(19)
  .refine(
    (value) =>
      !unsafeMetadataControlPattern.test(value) &&
      canonicalCursorPattern.test(value),
    "Cursor must be a canonical decimal string.",
  )
  .refine(
    (value) => {
      if (
        value.length > 19 ||
        unsafeMetadataControlPattern.test(value) ||
        !canonicalCursorPattern.test(value)
      ) {
        return false;
      }

      const numeric = BigInt(value);

      return (
        numeric >= 0n &&
        numeric <= MAX_LOG_CURSOR
      );
    },
    "Cursor is outside the supported BIGINT range.",
  );

/**
 * Persisted log entries use positive sequences.
 */
const positiveSequenceStringSchema =
  sequenceStringSchema.refine(
    (value) => value !== "0",
    "Log sequence must be positive.",
  );

export const logSourceSchema = z.enum(
  deploymentLogSources,
);

export const logStreamSchema = z.enum(
  deploymentLogStreams,
);

export const logLevelSchema = z.enum(
  deploymentLogLevels,
);

/**
 * Wire timestamps are ISO-8601 UTC timestamps.
 *
 * Existing producers using Date.toISOString()
 * remain compatible.
 */
const timestampSchema = z
  .string()
  .max(64)
  .datetime()
  .refine(
    (value) =>
      !unsafeMetadataControlPattern.test(value) &&
      Number.isFinite(Date.parse(value)),
    "Timestamp must be a valid UTC date.",
  );

const protocolCodeSchema = z
  .string()
  .min(1)
  .max(64)
  .refine(
    (value) =>
      !unsafeMetadataControlPattern.test(value) &&
      protocolCodePattern.test(value),
    "Invalid protocol code.",
  );

// ==================================================
// Log entry
// ==================================================

const logMessageSchema = z
  .string()
  .min(1)
  .max(MAX_LOG_MESSAGE_BYTES)
  .refine(
    (value) =>
      !unsafeLogControlPattern.test(value),
    "Log message contains unsafe control characters.",
  )
  .refine(
    (value) =>
      value.length <= MAX_LOG_MESSAGE_BYTES &&
      utf8ByteLength(value) <=
        MAX_LOG_MESSAGE_BYTES,
    "Log message exceeds the maximum UTF-8 size.",
  );

export const deploymentLogEntrySchema = z
  .object({
    id: uuidSchema,
    workspaceId: uuidSchema,
    projectId: uuidSchema,
    deploymentId: uuidSchema,

    buildId: uuidSchema.nullable(),

    runtimeInstanceId:
      uuidSchema.nullable(),

    source: logSourceSchema,
    stream: logStreamSchema,
    level: logLevelSchema,

    message: logMessageSchema,

    sequence:
      positiveSequenceStringSchema,

    eventTimestamp: timestampSchema,

    persistedAt: timestampSchema,
  })
  .strict();

export type DeploymentLogEntryDto = z.infer<
  typeof deploymentLogEntrySchema
>;

// ==================================================
// Collection input (worker internal)
// ==================================================

/**
 * A normalized, sanitized log record produced by the
 * collection layer and handed to the durable writer.
 *
 * `sequence` is assigned by the writer at persistence
 * time and is therefore absent here.
 */
export const collectedLogRecordSchema = z
  .object({
    source: logSourceSchema,
    stream: logStreamSchema,
    level: logLevelSchema,
    message: logMessageSchema,

    eventTimestamp: z
      .date()
      .refine(
        (value) =>
          Number.isFinite(value.getTime()),
        "Invalid event timestamp.",
      ),
  })
  .strict();

export type CollectedLogRecord = z.infer<
  typeof collectedLogRecordSchema
>;

// ==================================================
// Historical logs REST query
// ==================================================

/**
 * Accept a positive integer or its canonical decimal
 * string representation.
 *
 * Reject implicit coercion from booleans, arrays,
 * objects, hexadecimal and scientific notation.
 */
const historyLimitSchema = z.preprocess(
  (value) => {
    if (typeof value !== "string") {
      return value;
    }

    if (
      value.length > 4 ||
      unsafeMetadataControlPattern.test(value) ||
      !positiveIntegerPattern.test(value)
    ) {
      return value;
    }

    return Number(value);
  },

  z
    .number()
    .int()
    .min(1)
    .max(MAX_LOG_HISTORY_ENTRIES)
    .default(200),
);

export const logHistoryQuerySchema = z
  .object({
    after:
      sequenceStringSchema.optional(),

    before:
      sequenceStringSchema.optional(),

    source:
      logSourceSchema.optional(),

    stream:
      logStreamSchema.optional(),

    level:
      logLevelSchema.optional(),

    limit: historyLimitSchema,

    download: z
      .enum(["", "true", "false"])
      .optional(),
  })
  .strict()
  .refine(
    (value) =>
      value.after === undefined ||
      value.before === undefined,
    "Only one of after or before may be supplied.",
  );

export type LogHistoryQuery = z.infer<
  typeof logHistoryQuerySchema
>;

// ==================================================
// Historical logs REST response
// ==================================================

export const logHistoryResponseSchema = z
  .object({
    items: z
      .array(deploymentLogEntrySchema)
      .max(MAX_LOG_HISTORY_ENTRIES),

    nextCursor:
      sequenceStringSchema.nullable(),

    previousCursor:
      sequenceStringSchema.nullable(),

    hasMore: z.boolean(),

    oldestSequence:
      positiveSequenceStringSchema.nullable(),

    newestSequence:
      positiveSequenceStringSchema.nullable(),

    retentionTruncated: z.boolean(),
  })
  .strict()
  .superRefine((value, context) => {
    if (
      value.items.length >
      MAX_LOG_HISTORY_ENTRIES
    ) {
      return;
    }

    if (
      !fitsMessageBudget(
        value.items,
        MAX_LOG_HISTORY_MESSAGE_BYTES,
      )
    ) {
      context.addIssue({
        code: "custom",
        path: ["items"],
        message:
          "Historical log response exceeds its message budget.",
      });
    }

    const oldest = value.oldestSequence;
    const newest = value.newestSequence;

    if (
      (oldest === null) !==
      (newest === null)
    ) {
      context.addIssue({
        code: "custom",
        path: ["oldestSequence"],
        message:
          "Oldest and newest sequences must both be present or absent.",
      });
    }

    if (
      oldest !== null &&
      newest !== null &&
      compareCursors(oldest, newest) === 1
    ) {
      context.addIssue({
        code: "custom",
        path: ["newestSequence"],
        message:
          "Newest sequence cannot precede oldest sequence.",
      });
    }

    const firstEntry = value.items[0];

    const seenIds = new Set<string>();
    const seenSequences = new Set<string>();

    for (
      let index = 0;
      index < value.items.length;
      index += 1
    ) {
      const entry = value.items[index];

      if (!entry) {
        continue;
      }

      if (
        firstEntry &&
        (
          !sameIdentifier(
            entry.workspaceId,
            firstEntry.workspaceId,
          ) ||
          !sameIdentifier(
            entry.projectId,
            firstEntry.projectId,
          ) ||
          !sameIdentifier(
            entry.deploymentId,
            firstEntry.deploymentId,
          )
        )
      ) {
        context.addIssue({
          code: "custom",
          path: ["items", index],
          message:
            "Historical response contains inconsistent deployment identity.",
        });
      }

      const normalizedId =
        entry.id.toLowerCase();

      if (seenIds.has(normalizedId)) {
        context.addIssue({
          code: "custom",
          path: ["items", index, "id"],
          message:
            "Duplicate log entry identifier.",
        });
      }

      seenIds.add(normalizedId);

      if (
        seenSequences.has(entry.sequence)
      ) {
        context.addIssue({
          code: "custom",
          path: ["items", index, "sequence"],
          message:
            "Duplicate log sequence.",
        });
      }

      seenSequences.add(entry.sequence);

      if (
        oldest !== null &&
        compareCursors(
          entry.sequence,
          oldest,
        ) === -1
      ) {
        context.addIssue({
          code: "custom",
          path: ["items", index, "sequence"],
          message:
            "Log sequence precedes retained history.",
        });
      }

      if (
        newest !== null &&
        compareCursors(
          entry.sequence,
          newest,
        ) === 1
      ) {
        context.addIssue({
          code: "custom",
          path: ["items", index, "sequence"],
          message:
            "Log sequence exceeds the newest sequence.",
        });
      }
    }

    if (
      value.items.length > 0 &&
      (oldest === null || newest === null)
    ) {
      context.addIssue({
        code: "custom",
        path: ["items"],
        message:
          "Non-empty history requires sequence boundaries.",
      });
    }
  });

export type LogHistoryResponse = z.infer<
  typeof logHistoryResponseSchema
>;

// ==================================================
// Redis notification payload
// ==================================================

/**
 * Lightweight committed-cursor notification published to
 * Redis after a batch of log entries is durably committed.
 *
 * The payload intentionally omits log message bodies:
 * authoritative history lives in PostgreSQL.
 *
 * Notifications are hints, not proof of authorization
 * or complete delivery.
 */
export const logNotificationSchema = z
  .object({
    workspaceId: uuidSchema,
    projectId: uuidSchema,
    deploymentId: uuidSchema,

    fromSequence:
      positiveSequenceStringSchema,

    toSequence:
      positiveSequenceStringSchema,

    committedAt: timestampSchema,
  })
  .strict()
  .superRefine((value, context) => {
    if (
      compareCursors(
        value.fromSequence,
        value.toSequence,
      ) === 1
    ) {
      context.addIssue({
        code: "custom",
        path: ["toSequence"],
        message:
          "Notification end sequence cannot precede its start sequence.",
      });
    }
  });

export type LogNotification = z.infer<
  typeof logNotificationSchema
>;

// ==================================================
// Socket.IO event contracts
// ==================================================

export const socketEventNames = Object.freeze({
  subscribe:
    "deployment:logs:subscribe",

  unsubscribe:
    "deployment:logs:unsubscribe",

  batch:
    "deployment:logs:batch",

  error:
    "deployment:logs:error",

  resync:
    "deployment:logs:resync",

  statusChanged:
    "deployment:status:changed",
} as const);

// ==================================================
// Socket subscription
// ==================================================

export const logSubscribePayloadSchema = z
  .object({
    deploymentId: uuidSchema,
    projectId: uuidSchema,

    lastCursor:
      sequenceStringSchema.optional(),
  })
  .strict();

export type LogSubscribePayload = z.infer<
  typeof logSubscribePayloadSchema
>;

// ==================================================
// Socket unsubscription
// ==================================================

export const logUnsubscribePayloadSchema = z
  .object({
    deploymentId: uuidSchema,
    projectId: uuidSchema,
  })
  .strict();

export type LogUnsubscribePayload = z.infer<
  typeof logUnsubscribePayloadSchema
>;

// ==================================================
// Socket log batch
// ==================================================

export const logBatchPayloadSchema = z
  .object({
    deploymentId: uuidSchema,

    entries: z
      .array(deploymentLogEntrySchema)
      .max(MAX_LOG_BATCH_ENTRIES),

    highWatermark:
      sequenceStringSchema,

    gapDetected: z.boolean(),
  })
  .strict()
  .superRefine((value, context) => {
    if (
      value.entries.length >
      MAX_LOG_BATCH_ENTRIES
    ) {
      return;
    }

    if (
      !fitsMessageBudget(
        value.entries,
        MAX_LOG_BATCH_MESSAGE_BYTES,
      )
    ) {
      context.addIssue({
        code: "custom",
        path: ["entries"],
        message:
          "Socket log batch exceeds its message budget.",
      });
    }

    const firstEntry = value.entries[0];

    const seenIds = new Set<string>();

    let previousSequence:
      string | null = null;

    for (
      let index = 0;
      index < value.entries.length;
      index += 1
    ) {
      const entry = value.entries[index];

      if (!entry) {
        continue;
      }

      if (
        !sameIdentifier(
          entry.deploymentId,
          value.deploymentId,
        )
      ) {
        context.addIssue({
          code: "custom",
          path: [
            "entries",
            index,
            "deploymentId",
          ],
          message:
            "Log entry belongs to a different deployment.",
        });
      }

      if (
        firstEntry &&
        (
          !sameIdentifier(
            entry.workspaceId,
            firstEntry.workspaceId,
          ) ||
          !sameIdentifier(
            entry.projectId,
            firstEntry.projectId,
          )
        )
      ) {
        context.addIssue({
          code: "custom",
          path: ["entries", index],
          message:
            "Socket batch contains inconsistent tenant identity.",
        });
      }

      const normalizedId =
        entry.id.toLowerCase();

      if (seenIds.has(normalizedId)) {
        context.addIssue({
          code: "custom",
          path: ["entries", index, "id"],
          message:
            "Duplicate log entry identifier.",
        });
      }

      seenIds.add(normalizedId);

      if (previousSequence !== null) {
        const comparison = compareCursors(
          previousSequence,
          entry.sequence,
        );

        if (
          comparison !== null &&
          comparison >= 0
        ) {
          context.addIssue({
            code: "custom",
            path: [
              "entries",
              index,
              "sequence",
            ],
            message:
              "Socket log sequences must be strictly increasing.",
          });
        }
      }

      if (
        compareCursors(
          entry.sequence,
          value.highWatermark,
        ) === 1
      ) {
        context.addIssue({
          code: "custom",
          path: [
            "entries",
            index,
            "sequence",
          ],
          message:
            "Log sequence exceeds the batch high watermark.",
        });
      }

      previousSequence = entry.sequence;
    }
  });

export type LogBatchPayload = z.infer<
  typeof logBatchPayloadSchema
>;

// ==================================================
// Socket error payload
// ==================================================

export const logErrorPayloadSchema = z
  .object({
    deploymentId:
      uuidSchema.nullable(),

    code: protocolCodeSchema,

    message: z
      .string()
      .min(1)
      .max(MAX_LOG_ERROR_MESSAGE_LENGTH)
      .refine(
        (value) =>
          value.trim().length > 0 &&
          !unsafeMetadataControlPattern.test(
            value,
          ),
        "Invalid public error message.",
      ),
  })
  .strict();

export type LogErrorPayload = z.infer<
  typeof logErrorPayloadSchema
>;

// ==================================================
// Socket resynchronization payload
// ==================================================

export const logResyncPayloadSchema = z
  .object({
    deploymentId: uuidSchema,

    reason: protocolCodeSchema,

    highWatermark:
      sequenceStringSchema.nullable(),
  })
  .strict();

export type LogResyncPayload = z.infer<
  typeof logResyncPayloadSchema
>;

// ==================================================
// Helpers
// ==================================================

export function isDeploymentLogSource(
  value: unknown,
): value is DeploymentLogSource {
  return logSourceSchema.safeParse(value)
    .success;
}

export function isDeploymentLogStream(
  value: unknown,
): value is DeploymentLogStream {
  return logStreamSchema.safeParse(value)
    .success;
}

export function isDeploymentLogLevel(
  value: unknown,
): value is DeploymentLogLevel {
  return logLevelSchema.safeParse(value)
    .success;
}

/**
 * Parse a canonical decimal-string cursor into BigInt.
 *
 * Returns null for invalid input.
 *
 * Never converts through JavaScript Number.
 */
export function parseCursor(
  value: unknown,
): bigint | null {
  const parsed =
    sequenceStringSchema.safeParse(value);

  if (!parsed.success) {
    return null;
  }

  return BigInt(parsed.data);
}

/**
 * Serialize a BigInt cursor into a validated canonical
 * decimal string.
 *
 * The serializer and parser enforce the same range.
 */
export function formatCursor(
  value: bigint,
): string {
  if (typeof value !== "bigint") {
    throw new TypeError(
      "Cursor must be a BigInt.",
    );
  }

  if (
    value < 0n ||
    value > MAX_LOG_CURSOR
  ) {
    throw new RangeError(
      "Cursor is outside the supported BIGINT range.",
    );
  }

  return value.toString(10);
}
