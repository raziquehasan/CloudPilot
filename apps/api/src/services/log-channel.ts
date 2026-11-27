/**
 * CloudPilot — Log Notification Channel Helpers
 *
 * Phase 5.6 — Realtime Deployment Logs
 *
 * Redis channel naming and notification validation
 * for committed-cursor deployment log notifications.
 *
 * Security guarantees:
 *
 * - Explicit UUID validation.
 * - Bounded notification payload size.
 * - Canonical PostgreSQL BIGINT cursors.
 * - Validated sequence range ordering.
 * - Restricted Redis channel prefixes.
 * - Strict notification object validation.
 *
 * IMPORTANT:
 *
 * Channel names and Socket.IO rooms are routing
 * mechanisms, not authorization boundaries.
 *
 * Every subscription and delivery must be authorized
 * independently against the relevant database scope.
 *
 * Redis Pub/Sub provides at-most-once delivery.
 * PostgreSQL remains the authoritative log store.
 */

import type {
  LogNotification,
} from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const MAX_PREFIX_LENGTH = 100;

const MAX_NOTIFICATION_BYTES = 4096;

const MAX_TIMESTAMP_LENGTH = 64;

const MAX_CURSOR =
  9_223_372_036_854_775_807n;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const PREFIX_PATTERN =
  /^[a-zA-Z0-9_-]+(?::[a-zA-Z0-9_-]+)*$/;

const POSITIVE_CURSOR_PATTERN =
  /^[1-9][0-9]{0,18}$/;

const UTC_TIMESTAMP_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/;

const NOTIFICATION_FIELDS = new Set([
  "workspaceId",
  "projectId",
  "deploymentId",
  "fromSequence",
  "toSequence",
  "committedAt",
]);

// ==================================================
// Public contracts
// ==================================================

export interface LogChannelScope {
  workspaceId: string;
  deploymentId: string;
}

// ==================================================
// Validation errors
// ==================================================

export class LogChannelValidationError
  extends Error {
  readonly code =
    "INVALID_LOG_CHANNEL";

  constructor(message: string) {
    super(message);

    this.name =
      "LogChannelValidationError";
  }
}

// ==================================================
// Object validation
// ==================================================

function isObject(
  value: unknown,
): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

// ==================================================
// UUID validation
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

function normalizeUuid(
  value: unknown,
): string | null {
  if (!isUuid(value)) {
    return null;
  }

  return value.toLowerCase();
}

// ==================================================
// Redis prefix validation
// ==================================================

/**
 * Accept colon-separated namespace segments.
 *
 * Examples:
 *
 * cloudpilot
 * cloudpilot:logs
 * cloudpilot:production:logs
 *
 * Reject whitespace, control characters,
 * wildcard characters and empty segments.
 */
export function isValidLogChannelPrefix(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    value.length >= 1 &&
    value.length <= MAX_PREFIX_LENGTH &&
    PREFIX_PATTERN.test(value)
  );
}

function validatePrefix(
  prefix: unknown,
): string {
  if (
    !isValidLogChannelPrefix(prefix)
  ) {
    throw new LogChannelValidationError(
      "Invalid log notification channel prefix.",
    );
  }

  return prefix;
}

// ==================================================
// Channel scope validation
// ==================================================

function validateScope(
  scope: LogChannelScope,
): LogChannelScope {
  if (!isObject(scope)) {
    throw new LogChannelValidationError(
      "Invalid log channel scope.",
    );
  }

  const workspaceId =
    normalizeUuid(
      scope.workspaceId,
    );

  const deploymentId =
    normalizeUuid(
      scope.deploymentId,
    );

  if (
    workspaceId === null ||
    deploymentId === null
  ) {
    throw new LogChannelValidationError(
      "Invalid workspace or deployment identity.",
    );
  }

  return {
    workspaceId,
    deploymentId,
  };
}

// ==================================================
// Sequence validation
// ==================================================

/**
 * Notification ranges reference committed log entries.
 *
 * Sequence zero is valid for some replay cursors,
 * but never for a persisted log entry notification.
 */
function parsePositiveCursor(
  value: unknown,
): bigint | null {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > 19 ||
    !POSITIVE_CURSOR_PATTERN.test(value)
  ) {
    return null;
  }

  try {
    const cursor = BigInt(value);

    if (
      cursor <= 0n ||
      cursor > MAX_CURSOR
    ) {
      return null;
    }

    return cursor;
  } catch {
    return null;
  }
}

// ==================================================
// Timestamp validation
// ==================================================

function normalizeTimestamp(
  value: unknown,
): string | null {
  if (
    typeof value !== "string" ||
    value.length > MAX_TIMESTAMP_LENGTH ||
    !UTC_TIMESTAMP_PATTERN.test(value)
  ) {
    return null;
  }

  const timestamp = Date.parse(value);

  if (!Number.isFinite(timestamp)) {
    return null;
  }

  try {
    const normalized =
      new Date(timestamp).toISOString();

    // Reject invalid calendar dates that JavaScript
    // might otherwise normalize into another date.

    const canonicalInput =
      value.includes(".")
        ? value.replace(
            /\.(\d{1,3})Z$/,
            (
              _match: string,
              fraction: string,
            ) =>
              `.${fraction.padEnd(3, "0")}Z`,
          )
        : value.replace(
            /Z$/,
            ".000Z",
          );

    if (
      normalized !== canonicalInput
    ) {
      return null;
    }

    return normalized;
  } catch {
    return null;
  }
}

// ==================================================
// Redis channel construction
// ==================================================

/**
 * Construct a deterministic Redis Pub/Sub channel.
 *
 * Channel format:
 *
 * <prefix>:deployment:<workspaceId>:<deploymentId>
 *
 * The original channel format is preserved for
 * publisher/subscriber compatibility.
 */
export function buildLogChannel(
  prefix: string,
  scope: LogChannelScope,
): string {
  const safePrefix =
    validatePrefix(prefix);

  const safeScope =
    validateScope(scope);

  return (
    `${safePrefix}:deployment:` +
    `${safeScope.workspaceId}:` +
    safeScope.deploymentId
  );
}

// ==================================================
// Socket.IO room construction
// ==================================================

/**
 * Construct a deterministic workspace-scoped room.
 *
 * Joining a room must never grant deployment access
 * without independent authorization.
 */
export function buildLogRoom(
  scope: LogChannelScope,
): string {
  const safeScope =
    validateScope(scope);

  return (
    `deployment:` +
    `${safeScope.workspaceId}:` +
    safeScope.deploymentId
  );
}

// ==================================================
// Notification object validation
// ==================================================

function hasExactNotificationFields(
  data: Record<string, unknown>,
): boolean {
  const keys = Object.keys(data);

  if (
    keys.length !==
    NOTIFICATION_FIELDS.size
  ) {
    return false;
  }

  for (const key of keys) {
    if (
      !NOTIFICATION_FIELDS.has(key)
    ) {
      return false;
    }
  }

  for (const field of
    NOTIFICATION_FIELDS) {
    if (
      !Object.hasOwn(data, field)
    ) {
      return false;
    }
  }

  return true;
}

// ==================================================
// Redis notification parsing
// ==================================================

/**
 * Parse an untrusted Redis notification payload.
 *
 * Invalid notifications are rejected without
 * throwing or exposing the raw payload.
 *
 * Notifications contain committed sequence ranges,
 * never deployment log message bodies.
 */
export function parseLogNotification(
  value: string,
): LogNotification | null {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length >
      MAX_NOTIFICATION_BYTES
  ) {
    return null;
  }

  if (
    Buffer.byteLength(
      value,
      "utf8",
    ) > MAX_NOTIFICATION_BYTES
  ) {
    return null;
  }

  try {
    const parsed: unknown =
      JSON.parse(value);

    if (
      !isObject(parsed) ||
      !hasExactNotificationFields(
        parsed,
      )
    ) {
      return null;
    }

    const workspaceId =
      normalizeUuid(
        parsed.workspaceId,
      );

    const projectId =
      normalizeUuid(
        parsed.projectId,
      );

    const deploymentId =
      normalizeUuid(
        parsed.deploymentId,
      );

    if (
      workspaceId === null ||
      projectId === null ||
      deploymentId === null
    ) {
      return null;
    }

    const fromSequence =
      parsePositiveCursor(
        parsed.fromSequence,
      );

    const toSequence =
      parsePositiveCursor(
        parsed.toSequence,
      );

    if (
      fromSequence === null ||
      toSequence === null ||
      fromSequence > toSequence
    ) {
      return null;
    }

    const committedAt =
      normalizeTimestamp(
        parsed.committedAt,
      );

    if (committedAt === null) {
      return null;
    }

    return {
      workspaceId,

      projectId,

      deploymentId,

      fromSequence:
        fromSequence.toString(10),

      toSequence:
        toSequence.toString(10),

      committedAt,
    };
  } catch {
    // Never log untrusted raw Redis payloads.
    return null;
  }
}

// ==================================================
// Notification channel verification
// ==================================================

/**
 * Verify that a notification belongs to the exact
 * Redis channel on which it was received.
 *
 * This protects against accidental or malicious
 * cross-channel routing.
 *
 * It does not replace database authorization.
 */
export function isLogNotificationForChannel(
  prefix: string,
  channel: string,
  notification: LogNotification,
): boolean {
  if (
    typeof channel !== "string" ||
    channel.length >
      MAX_PREFIX_LENGTH + 85
  ) {
    return false;
  }

  try {
    const expectedChannel =
      buildLogChannel(
        prefix,
        {
          workspaceId:
            notification.workspaceId,

          deploymentId:
            notification.deploymentId,
        },
      );

    return channel === expectedChannel;
  } catch {
    return false;
  }
}

// ==================================================
// Notification scope verification
// ==================================================

/**
 * Compare notification identity with an expected
 * workspace, project and deployment scope.
 *
 * Callers must still verify membership and
 * deployment access using authoritative data.
 */
export function matchesLogNotificationScope(
  notification: LogNotification,
  scope: {
    workspaceId: string;
    projectId: string;
    deploymentId: string;
  },
): boolean {
  if (
    !isObject(notification) ||
    !isObject(scope)
  ) {
    return false;
  }

  const workspaceId =
    normalizeUuid(
      scope.workspaceId,
    );

  const projectId =
    normalizeUuid(
      scope.projectId,
    );

  const deploymentId =
    normalizeUuid(
      scope.deploymentId,
    );

  if (
    workspaceId === null ||
    projectId === null ||
    deploymentId === null
  ) {
    return false;
  }

  return (
    normalizeUuid(
      notification.workspaceId,
    ) === workspaceId &&

    normalizeUuid(
      notification.projectId,
    ) === projectId &&

    normalizeUuid(
      notification.deploymentId,
    ) === deploymentId
  );
}