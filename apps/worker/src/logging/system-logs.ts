
/**
 * CloudPilot — System / Lifecycle Log Helpers
 *
 * Phase 5.6 — Real-Time Deployment Logs & Streaming Engine
 *
 * Produces bounded and sanitized SYSTEM log records from
 * authoritative deployment lifecycle transitions.
 *
 * These records complement, and do not replace, the
 * security audit log.
 *
 * Responsibilities:
 *
 * - Preserve SYSTEM source and stream classification.
 * - Validate severity and timestamps.
 * - Sanitize terminal and formatting controls.
 * - Apply best-effort secret redaction.
 * - Bound message size and batch processing.
 * - Preserve event ordering within an emitted batch.
 * - Prevent malformed events from crashing callers.
 *
 * Security boundary:
 *
 * Callers must emit lifecycle events only after the
 * corresponding authoritative transition is confirmed.
 *
 * Prefer static, allowlisted lifecycle messages.
 *
 * Never pass raw exceptions, environment dumps, request
 * headers, credentials or arbitrary process output.
 *
 * These helpers do not provide durable audit logging,
 * lifecycle authorization or exactly-once delivery.
 */

import type {
  CollectedLogRecord,
  DeploymentLogLevel,
} from "@cloudpilot/queue";

import type {
  DeploymentLogWriter,
} from "./log-writer.js";

import {
  SecretRedactor,
} from "./redact.js";

// ==================================================
// Constants
// ==================================================

const DEFAULT_MAX_MESSAGE_BYTES =
  16 * 1024;

const HARD_MAX_MESSAGE_BYTES =
  16 * 1024;

const HARD_MAX_INPUT_BYTES =
  64 * 1024;

const MIN_MESSAGE_BYTES = 64;

const MAX_BATCH_EVENTS = 1_000;

const REDACTED_MESSAGE =
  "[REDACTED]";

const INVALID_MESSAGE =
  "[Invalid system log message omitted]";

const OVERSIZED_MESSAGE =
  "[Oversized system log message omitted]";

const UNSAFE_MESSAGE =
  "[Unsafe system log message omitted]";

const BATCH_TRUNCATION_MESSAGE =
  "[System log batch truncated]";

// ==================================================
// Validation patterns
// ==================================================

/**
 * System lifecycle messages should be single-line
 * plain text, not terminal output.
 *
 * Reject messages containing escape sequences or
 * unsafe control characters instead of attempting
 * to recover arbitrary terminal payloads.
 */

const UNSAFE_CONTROL_PATTERN =
  /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/u;

const FORMAT_CONTROL_PATTERN =
  /[\u061c\u200b-\u200f\u202a-\u202e\u2060-\u206f\uFEFF]/gu;

/**
 * System messages are independent records.
 *
 * Multiline messages are omitted because they may
 * contain stack traces, credential blocks or
 * fragmented sensitive values.
 */

const MULTILINE_PATTERN =
  /[\r\n\u2028\u2029]/u;

const PRIVATE_KEY_PATTERN =
  /-----[^\r\n]{0,100}PRIVATE KEY-----/iu;

const VALID_LEVELS =
  new Set<DeploymentLogLevel>([
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "UNKNOWN",
  ]);

// ==================================================
// Public contracts
// ==================================================

export interface SystemLogOptions {
  /**
   * Optional caller-owned redactor containing known
   * sensitive values for this deployment.
   *
   * Use a dedicated redactor for system messages.
   * Do not share a stateful process-output redactor.
   */

  redactor?: SecretRedactor;

  /**
   * Maximum UTF-8 message size.
   *
   * Must not exceed the associated writer's
   * configured maximum entry size.
   */

  maxMessageBytes?: number;
}

export interface SystemLogEvent {
  message: string;

  level?: DeploymentLogLevel;

  timestamp?: Date;
}

// ==================================================
// Severity validation
// ==================================================

function normalizeLevel(
  value: unknown,
): DeploymentLogLevel {
  if (
    typeof value === "string" &&
    VALID_LEVELS.has(
      value as DeploymentLogLevel,
    )
  ) {
    return value as DeploymentLogLevel;
  }

  return "UNKNOWN";
}

// ==================================================
// Timestamp validation
// ==================================================

function normalizeTimestamp(
  timestamp: unknown,
): Date {
  if (timestamp instanceof Date) {
    const milliseconds =
      timestamp.getTime();

    if (
      Number.isFinite(milliseconds)
    ) {
      return new Date(milliseconds);
    }
  }

  // Fall back to the current observation time
  // when the supplied timestamp is invalid.

  return new Date();
}

// ==================================================
// Configuration validation
// ==================================================

function resolveMaxMessageBytes(
  options: SystemLogOptions,
): number {
  const value =
    options.maxMessageBytes ??
    DEFAULT_MAX_MESSAGE_BYTES;

  if (
    !Number.isSafeInteger(value) ||
    value < MIN_MESSAGE_BYTES ||
    value > HARD_MAX_MESSAGE_BYTES
  ) {
    throw new RangeError(
      "Invalid system log message limit.",
    );
  }

  return value;
}

// ==================================================
// UTF-8 helpers
// ==================================================

function byteLength(
  value: string,
): number {
  return Buffer.byteLength(
    value,
    "utf8",
  );
}

/**
 * Truncate after sanitization and redaction.
 *
 * Never split a Unicode code point.
 */

function truncateUtf8(
  value: string,
  maxBytes: number,
): string {
  if (
    byteLength(value) <= maxBytes
  ) {
    return value;
  }

  const suffix = "…";

  const availableBytes =
    maxBytes - byteLength(suffix);

  if (availableBytes < 0) {
    return "";
  }

  const parts: string[] = [];

  let usedBytes = 0;

  for (const character of value) {
    const characterBytes =
      byteLength(character);

    if (
      characterBytes >
      availableBytes - usedBytes
    ) {
      break;
    }

    parts.push(character);

    usedBytes += characterBytes;
  }

  return `${parts.join("")}${suffix}`;
}

// ==================================================
// Message sanitization
// ==================================================

/**
 * System lifecycle messages are deliberately handled
 * more strictly than stdout and stderr.
 *
 * Unsafe or oversized messages are replaced entirely.
 * This avoids exposing partially sanitized terminal
 * payloads or partially truncated credentials.
 */

function sanitizeMessage(
  message: unknown,
): string {
  if (
    typeof message !== "string" ||
    message.length === 0
  ) {
    return INVALID_MESSAGE;
  }

  // Cheap length check before UTF-8 byte counting.

  if (
    message.length >
      HARD_MAX_INPUT_BYTES ||
    byteLength(message) >
      HARD_MAX_INPUT_BYTES
  ) {
    return OVERSIZED_MESSAGE;
  }

  if (
    MULTILINE_PATTERN.test(message) ||
    UNSAFE_CONTROL_PATTERN.test(message)
  ) {
    return UNSAFE_MESSAGE;
  }

  const sanitized = message
    .replace(
      FORMAT_CONTROL_PATTERN,
      "",
    )
    .replace(/\t/gu, " ")
    .trim();

  if (!sanitized) {
    return INVALID_MESSAGE;
  }

  if (
    PRIVATE_KEY_PATTERN.test(
      sanitized,
    )
  ) {
    return REDACTED_MESSAGE;
  }

  return sanitized;
}

// ==================================================
// Secret redaction
// ==================================================

/**
 * Use a fresh baseline redactor for each independent
 * system message.
 *
 * This avoids sharing multiline private-key state
 * between unrelated deployments.
 *
 * A caller-owned redactor may additionally redact
 * registered deployment-specific secret values.
 */

function redactMessage(
  message: string,
  options: SystemLogOptions,
): string {
  const baselineRedactor =
    new SecretRedactor();

  try {
    let result =
      baselineRedactor.redact(
        message,
      );

    if (
      !result ||
      typeof result.message !==
        "string"
    ) {
      return REDACTED_MESSAGE;
    }

    if (options.redactor) {
      if (
        typeof options.redactor.redact !==
        "function"
      ) {
        return REDACTED_MESSAGE;
      }

      result =
        options.redactor.redact(
          result.message,
        );

      if (
        !result ||
        typeof result.message !==
          "string"
      ) {
        return REDACTED_MESSAGE;
      }
    }

    return result.message;
  } catch {
    // Never return raw input after a redaction failure.

    return REDACTED_MESSAGE;
  } finally {
    baselineRedactor.dispose();
  }
}

// ==================================================
// Message normalization pipeline
// ==================================================

function normalizeMessage(
  message: unknown,
  options: SystemLogOptions,
): string {
  const maxBytes =
    resolveMaxMessageBytes(options);

  const sanitized =
    sanitizeMessage(message);

  const redacted =
    redactMessage(
      sanitized,
      options,
    );

  // Validate redactor output as a defensive boundary.

  if (
    redacted.length === 0 ||
    redacted.length >
      HARD_MAX_INPUT_BYTES ||
    byteLength(redacted) >
      HARD_MAX_INPUT_BYTES ||
    UNSAFE_CONTROL_PATTERN.test(
      redacted,
    ) ||
    MULTILINE_PATTERN.test(
      redacted,
    )
  ) {
    return REDACTED_MESSAGE;
  }

  const normalized = redacted
    .replace(
      FORMAT_CONTROL_PATTERN,
      "",
    )
    .trim();

  if (!normalized) {
    return INVALID_MESSAGE;
  }

  return truncateUtf8(
    normalized,
    maxBytes,
  );
}

// ==================================================
// Public record factory
// ==================================================

/**
 * Construct a normalized SYSTEM log record.
 *
 * The original three-argument signature remains
 * supported. Options are an additive extension.
 */

export function systemRecord(
  message: string,
  level: DeploymentLogLevel = "INFO",
  timestamp: Date = new Date(),
  options: SystemLogOptions = {},
): CollectedLogRecord {
  let normalizedMessage: string;

  let normalizedLevel =
    normalizeLevel(level);

  try {
    normalizedMessage =
      normalizeMessage(
        message,
        options,
      );

    if (
      normalizedMessage ===
        INVALID_MESSAGE ||
      normalizedMessage ===
        OVERSIZED_MESSAGE ||
      normalizedMessage ===
        UNSAFE_MESSAGE
    ) {
      normalizedLevel = "WARN";
    }
  } catch {
    normalizedMessage =
      REDACTED_MESSAGE;

    normalizedLevel = "WARN";
  }

  return {
    source: "SYSTEM",

    stream: "SYSTEM",

    level: normalizedLevel,

    message: normalizedMessage,

    eventTimestamp:
      normalizeTimestamp(timestamp),
  };
}

// ==================================================
// Single lifecycle event
// ==================================================

/**
 * Emit one sanitized system lifecycle message.
 *
 * This method does not wait for database persistence.
 * The writer owns buffering, retries and durability.
 */

export function emitSystemLog(
  writer: DeploymentLogWriter,
  message: string,
  level: DeploymentLogLevel = "INFO",
  timestamp: Date = new Date(),
  options: SystemLogOptions = {},
): void {
  if (
    !writer ||
    typeof writer.enqueue !==
      "function"
  ) {
    return;
  }

  try {
    const record =
      systemRecord(
        message,
        level,
        timestamp,
        options,
      );

    writer.enqueue(record);
  } catch {
    /**
     * Logging failures must not replace or obscure
     * the authoritative lifecycle transition.
     *
     * Never print the raw message or exception
     * details from this failure path.
     */
  }
}

// ==================================================
// Ordered lifecycle event batch
// ==================================================

/**
 * Emit a bounded batch of system lifecycle messages.
 *
 * Event ordering is preserved within this call.
 *
 * The database writer determines final persisted
 * sequence order across concurrent producers.
 */

export function emitSystemLogs(
  writer: DeploymentLogWriter,
  events: ReadonlyArray<SystemLogEvent>,
  options: SystemLogOptions = {},
): void {
  if (
    !writer ||
    typeof writer.enqueue !==
      "function" ||
    !Array.isArray(events)
  ) {
    return;
  }

  const eventCount = Math.min(
    events.length,
    MAX_BATCH_EVENTS,
  );

  for (
    let index = 0;
    index < eventCount;
    index += 1
  ) {
    try {
      const event =
        events[index];

      if (
        !event ||
        typeof event !== "object"
      ) {
        emitSystemLog(
          writer,
          INVALID_MESSAGE,
          "WARN",
          new Date(),
          options,
        );

        continue;
      }

      emitSystemLog(
        writer,
        event.message,
        event.level ?? "INFO",
        event.timestamp ??
          new Date(),
        options,
      );
    } catch {
      // Continue processing independent events.
      // Never expose malformed event contents.
    }
  }

  if (
    events.length >
    MAX_BATCH_EVENTS
  ) {
    emitSystemLog(
      writer,
      BATCH_TRUNCATION_MESSAGE,
      "WARN",
      new Date(),
      options,
    );
  }
}
