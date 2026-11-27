/**
 * CloudPilot — Build Log Collection
 *
 * Collects bounded, sanitized build logs for legacy
 * build snapshots and real-time unified log persistence.
 *
 * Security and reliability controls:
 *
 * - Independent per-stream normalization.
 * - Incremental UTF-8 decoding for Buffer inputs.
 * - Shared credential redaction implementation.
 * - Registered secret redaction.
 * - Stateful terminal escape sanitization.
 * - Multiline private key protection.
 * - Bounded log message sizes.
 * - Bounded aggregate log bytes.
 * - Bounded log entry counts.
 * - Bounded live subscriber counts.
 * - Oversized line protection.
 * - Explicit truncation notifications.
 * - Reentrant callback protection.
 * - Idempotent stream finalization.
 * - Subscriber cleanup after finalization.
 *
 * Redaction is defense in depth, not a guarantee.
 * Infrastructure secrets must never be passed to builds.
 *
 * Local sequence numbers are for legacy build snapshots.
 * Unified deployment log sequence numbers must be
 * allocated transactionally by the persistence layer.
 */

import { SecretRedactor } from "../logging/redact.js";

import { LogNormalizer } from "../logging/normalize.js";

// ==================================================
// Constants
// ==================================================

const DEFAULT_MAX_LOG_BYTES = 10 * 1024 * 1024;

const MAX_ALLOWED_LOG_BYTES = 100 * 1024 * 1024;

/**
 * Must remain compatible with the unified log contract
 * and PostgreSQL message byte constraint.
 */
const MAX_LINE_BYTES = 16 * 1024;

const MAX_PENDING_LINE_BYTES = 64 * 1024;

const DEFAULT_MAX_LOG_ENTRIES = 20_000;

const MAX_ALLOWED_LOG_ENTRIES = 100_000;

const DEFAULT_MAX_SUBSCRIBERS = 8;

const MAX_ALLOWED_SUBSCRIBERS = 32;

const MAX_REGISTERED_SECRETS = 64;

const MAX_SECRET_BYTES = 4_096;

const MIN_SECRET_BYTES = 4;

const REDACTED = "[REDACTED]";

const TRUNCATION_MESSAGE =
  "[Build log limit reached]";

const NORMALIZATION_FAILURE_MESSAGE =
  "[Build log collection stopped safely]";

const BUILD_LOG_STREAMS = [
  "stdout",
  "stderr",
  "system",
] as const;

// ==================================================
// Terminal sanitization
// ==================================================

/**
 * Compatibility sanitizer for standalone complete lines.
 *
 * The collector itself uses the shared stateful
 * LogNormalizer for incremental stream sanitization.
 */

const OSC_PATTERN =
  /\u001b\][^\u0007\u001b]*(?:\u0007|\u001b\\|$)/gu;

const TERMINAL_STRING_PATTERN =
  /\u001b[PX^_][^\u001b]*(?:\u001b\\|$)/gu;

const ANSI_PATTERN =
  /(?:\u001b\[|\u009b)[0-?]*[ -/]*[@-~]/gu;

const ESCAPE_PATTERN =
  /\u001b[ -/]*[@-_]/gu;

const CONTROL_PATTERN =
  /[\u0000-\u0008\u000a-\u001f\u007f-\u009f]/gu;

const FORMAT_CONTROL_PATTERN =
  /[\u061c\u200b-\u200f\u202a-\u202e\u2060-\u206f\uFEFF]/gu;

function stripTerminalControls(
  value: string,
): string {
  return value
    .replace(OSC_PATTERN, "")
    .replace(TERMINAL_STRING_PATTERN, "")
    .replace(ANSI_PATTERN, "")
    .replace(ESCAPE_PATTERN, "")
    .replace(CONTROL_PATTERN, "")
    .replace(FORMAT_CONTROL_PATTERN, "");
}

// ==================================================
// UTF-8 byte helpers
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
 * Truncate without splitting a Unicode code point.
 *
 * The caller must redact the complete logical line
 * before applying this function.
 */
function truncateUtf8(
  value: string,
  maxBytes: number,
): string {
  if (
    !Number.isSafeInteger(maxBytes) ||
    maxBytes <= 0
  ) {
    return "";
  }

  if (byteLength(value) <= maxBytes) {
    return value;
  }

  const suffix = "…";

  const suffixBytes = byteLength(suffix);

  if (maxBytes < suffixBytes) {
    return "";
  }

  const availableBytes =
    maxBytes - suffixBytes;

  const characters: string[] = [];

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

    characters.push(character);

    usedBytes += characterBytes;
  }

  return `${characters.join("")}${suffix}`;
}

// ==================================================
// Standalone compatibility sanitizer
// ==================================================

/**
 * Sanitize a standalone complete logical log line.
 *
 * This compatibility function is intentionally
 * stateless across calls.
 *
 * Multiline and chunked logs must use BuildLogCollector
 * instead, which maintains independent stream state.
 */
export function sanitizeBuildLogLine(
  line: string,
): string {
  if (
    typeof line !== "string" ||
    byteLength(line) >
      MAX_PENDING_LINE_BYTES
  ) {
    return REDACTED;
  }

  /**
   * A standalone line sanitizer cannot safely track
   * multiline private key state across separate calls.
   */
  if (
    /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----/u
      .test(line) ||
    /-----END [A-Z0-9 ]*PRIVATE KEY-----/u
      .test(line)
  ) {
    return REDACTED;
  }

  const redactor = new SecretRedactor();

  try {
    const sanitized =
      stripTerminalControls(line);

    return redactor.redact(sanitized).message;
  } catch {
    /**
     * Never return unsanitized input after a
     * normalization or redaction failure.
     */
    return REDACTED;
  } finally {
    redactor.dispose();
  }
}

// ==================================================
// Public contracts
// ==================================================

export type BuildLogStream =
  | "stdout"
  | "stderr"
  | "system";

export interface BuildLogEntry {
  sequence: number;

  stream: BuildLogStream;

  message: string;
}

export type BuildLogListener = (
  entry: BuildLogEntry,
) => void;

export interface BuildLogCollectorOptions {
  maxLogBytes?: number;

  maxLogEntries?: number;

  /**
   * Retained for compatibility with existing callers.
   *
   * The normalizer may use this clock when creating
   * normalized records.
   */
  now?: () => Date;

  /**
   * Optional known sensitive values.
   *
   * These values are registered before collection
   * begins and must never be emitted in logs.
   */
  registeredSecrets?: readonly string[];

  /**
   * Maximum simultaneous live log subscribers.
   */
  maxSubscribers?: number;
}

// ==================================================
// Internal contracts
// ==================================================

type CollectorState =
  | "OPEN"
  | "FINALIZING"
  | "FINALIZED";

interface ListenerRegistration {
  listener: BuildLogListener;

  active: boolean;
}

type StreamRedactors = Record<
  BuildLogStream,
  SecretRedactor
>;

// ==================================================
// Validation
// ==================================================

function validateLimit(
  value: number,
  maximum: number,
  name: string,
): void {
  if (
    !Number.isSafeInteger(value) ||
    value <= 0 ||
    value > maximum
  ) {
    throw new RangeError(
      `${name} must be a positive integer not exceeding ${maximum}`,
    );
  }
}

function isBuildLogStream(
  value: unknown,
): value is BuildLogStream {
  return (
    value === "stdout" ||
    value === "stderr" ||
    value === "system"
  );
}

function toBuildLogStream(
  stream: unknown,
): BuildLogStream | null {
  switch (stream) {
    case "STDOUT":
    case "stdout":
      return "stdout";

    case "STDERR":
    case "stderr":
      return "stderr";

    case "SYSTEM":
    case "system":
      return "system";

    default:
      return null;
  }
}

function validateRegisteredSecrets(
  secrets: readonly string[],
): string[] {
  if (
    !Array.isArray(secrets) ||
    secrets.length >
      MAX_REGISTERED_SECRETS
  ) {
    throw new RangeError(
      "Invalid build log registered secret count.",
    );
  }

  const uniqueSecrets = new Set<string>();

  for (const secret of secrets) {
    if (
      typeof secret !== "string" ||
      !secret.trim() ||
      /[\u0000\r\n]/u.test(secret)
    ) {
      throw new Error(
        "Invalid build log registered secret.",
      );
    }

    const bytes = byteLength(secret);

    if (
      bytes < MIN_SECRET_BYTES ||
      bytes > MAX_SECRET_BYTES
    ) {
      throw new RangeError(
        "Invalid build log registered secret size.",
      );
    }

    /**
     * Preserve the original secret value, including
     * leading or trailing whitespace.
     */
    uniqueSecrets.add(secret);
  }

  return [...uniqueSecrets];
}

// ==================================================
// Shared stream redactors
// ==================================================

function disposeStreamRedactors(
  redactors: StreamRedactors,
): void {
  for (const stream of BUILD_LOG_STREAMS) {
    redactors[stream].dispose();
  }
}

function createStreamRedactors(
  secrets: readonly string[],
): StreamRedactors {
  const redactors: StreamRedactors = {
    stdout: new SecretRedactor(),

    stderr: new SecretRedactor(),

    system: new SecretRedactor(),
  };

  try {
    for (
      const stream of BUILD_LOG_STREAMS
    ) {
      const redactor = redactors[stream];

      for (const secret of secrets) {
        const registered =
          redactor.registerSecret(secret);

        if (!registered) {
          throw new Error(
            "Build log secret registration failed.",
          );
        }
      }
    }

    return redactors;
  } catch (error) {
    disposeStreamRedactors(redactors);

    throw error;
  }
}

// ==================================================
// Build log collector
// ==================================================

export class BuildLogCollector {
  private readonly maxLogBytes: number;

  private readonly maxLogEntries: number;

  private readonly maxSubscribers: number;

  private readonly redactors: StreamRedactors;

  private readonly normalizer: LogNormalizer;

  private sequence = 0;

  private totalBytes = 0;

  private truncated = false;

  private state: CollectorState = "OPEN";

  /**
   * Protect mutable collection state against
   * reentrant subscriber callbacks.
   */
  private processing = false;

  private notifying = false;

  private finalizationRequested = false;

  private subscriberFailures = 0;

  private readonly entries: BuildLogEntry[] = [];

  private readonly listeners =
    new Set<ListenerRegistration>();

  constructor(
    options?: BuildLogCollectorOptions,
  ) {
    this.maxLogBytes =
      options?.maxLogBytes ??
      DEFAULT_MAX_LOG_BYTES;

    this.maxLogEntries =
      options?.maxLogEntries ??
      DEFAULT_MAX_LOG_ENTRIES;

    this.maxSubscribers =
      options?.maxSubscribers ??
      DEFAULT_MAX_SUBSCRIBERS;

    validateLimit(
      this.maxLogBytes,
      MAX_ALLOWED_LOG_BYTES,
      "Build log byte limit",
    );

    validateLimit(
      this.maxLogEntries,
      MAX_ALLOWED_LOG_ENTRIES,
      "Build log entry limit",
    );

    validateLimit(
      this.maxSubscribers,
      MAX_ALLOWED_SUBSCRIBERS,
      "Build log subscriber limit",
    );

    if (
      options?.now !== undefined &&
      typeof options.now !== "function"
    ) {
      throw new TypeError(
        "Invalid build log clock.",
      );
    }

    const secrets =
      validateRegisteredSecrets(
        options?.registeredSecrets ?? [],
      );

    this.redactors =
      createStreamRedactors(secrets);

    try {
      /**
       * Independent stream redactors prevent
       * stdout and stderr private-key state from
       * interfering with each other.
       */
      this.normalizer = new LogNormalizer({
        source: "BUILD",

        redactor: this.redactors.stdout,

        streamRedactors: this.redactors,

        maxLineBytes: MAX_LINE_BYTES,

        maxPendingLineBytes:
          MAX_PENDING_LINE_BYTES,

        ...(options?.now !== undefined
          ? { now: options.now }
          : {}),

        onRecord: (record) => {
          const stream =
            toBuildLogStream(record.stream);

          if (stream === null) {
            this.markTruncated(
              NORMALIZATION_FAILURE_MESSAGE,
            );

            return;
          }

          this.pushLine(
            stream,
            record.message,
          );
        },
      });
    } catch (error) {
      disposeStreamRedactors(
        this.redactors,
      );

      throw error;
    }
  }

  // ================================================
  // Public state
  // ================================================

  get sizeBytes(): number {
    return this.totalBytes;
  }

  get isTruncated(): boolean {
    return this.truncated;
  }

  get entryCount(): number {
    return this.entries.length;
  }

  get isFinalized(): boolean {
    return this.state === "FINALIZED";
  }

  get subscriberCount(): number {
    return this.listeners.size;
  }

  get subscriberFailureCount(): number {
    return this.subscriberFailures;
  }

  // ================================================
  // Raw chunk collection
  // ================================================

  /**
   * Append a raw string or Buffer chunk.
   *
   * Existing string-based BuildKit callers remain
   * supported.
   *
   * Buffer input allows the shared normalizer to
   * perform incremental UTF-8 decoding.
   *
   * A single stream must not switch between string
   * and Buffer input modes during collection.
   */
  append(
    stream: BuildLogStream,
    chunk: string | Buffer,
  ): void {
    if (
      this.state !== "OPEN" ||
      this.truncated ||
      this.processing ||
      this.notifying
    ) {
      return;
    }

    if (!isBuildLogStream(stream)) {
      this.markTruncated(
        NORMALIZATION_FAILURE_MESSAGE,
      );

      return;
    }

    if (
      typeof chunk !== "string" &&
      !Buffer.isBuffer(chunk)
    ) {
      this.markTruncated(
        NORMALIZATION_FAILURE_MESSAGE,
      );

      return;
    }

    if (chunk.length === 0) {
      return;
    }

    this.processing = true;

    try {
      this.normalizer.push(
        stream,
        chunk,
      );
    } catch {
      /**
       * Do not expose raw chunks or exception details.
       *
       * A log collection failure should not expose
       * credentials or automatically fail the build.
       */
      this.markTruncated(
        NORMALIZATION_FAILURE_MESSAGE,
      );
    } finally {
      this.processing = false;

      if (this.finalizationRequested) {
        this.finalize();
      }
    }
  }

  // ================================================
  // Bounded log storage
  // ================================================

  private pushLine(
    stream: BuildLogStream,
    line: string,
  ): void {
    if (
      this.truncated ||
      this.state === "FINALIZED"
    ) {
      return;
    }

    if (
      typeof line !== "string" ||
      !line.trim()
    ) {
      return;
    }

    /**
     * The shared normalizer has already sanitized
     * and redacted the complete logical line.
     *
     * Apply a defensive final message byte limit
     * before storing or notifying subscribers.
     */
    const message = truncateUtf8(
      line,
      MAX_LINE_BYTES,
    );

    if (!message) {
      return;
    }

    const messageBytes =
      byteLength(message);

    const markerBytes =
      byteLength(TRUNCATION_MESSAGE);

    /**
     * Reserve space for a truncation marker whenever
     * the configured limits are large enough.
     *
     * Extremely small legacy configurations remain
     * supported, but may not fit a marker.
     */
    const reserveMarker =
      this.maxLogEntries > 1 &&
      this.maxLogBytes > markerBytes;

    const usableEntryLimit =
      this.maxLogEntries -
      (reserveMarker ? 1 : 0);

    const usableByteLimit =
      this.maxLogBytes -
      (reserveMarker ? markerBytes : 0);

    if (
      this.entries.length >=
        usableEntryLimit ||
      messageBytes >
        usableByteLimit - this.totalBytes
    ) {
      this.markTruncated();

      return;
    }

    this.storeEntry(
      stream,
      message,
    );
  }

  /**
   * Store a completed sanitized entry.
   *
   * This method also handles trusted internal
   * truncation markers.
   */
  private storeEntry(
    stream: BuildLogStream,
    message: string,
  ): boolean {
    if (
      this.state === "FINALIZED" ||
      !message
    ) {
      return false;
    }

    const messageBytes =
      byteLength(message);

    if (
      messageBytes <= 0 ||
      messageBytes > MAX_LINE_BYTES ||
      this.entries.length >=
        this.maxLogEntries ||
      messageBytes >
        this.maxLogBytes - this.totalBytes
    ) {
      return false;
    }

    this.sequence += 1;

    this.totalBytes += messageBytes;

    const entry: BuildLogEntry = {
      sequence: this.sequence,

      stream,

      message,
    };

    this.entries.push(entry);

    this.notifySubscribers(entry);

    return true;
  }

  // ================================================
  // Live streaming subscriptions
  // ================================================

  /**
   * Subscribe to future sanitized entries.
   *
   * Existing snapshot entries are not replayed.
   * Callers requiring historical entries must use
   * snapshot() or the unified history API.
   *
   * Listeners must perform bounded synchronous work.
   * Asynchronous persistence belongs in the bounded
   * unified log writer.
   */
  subscribe(
    listener: BuildLogListener,
  ): () => void {
    if (typeof listener !== "function") {
      throw new TypeError(
        "Invalid build log listener.",
      );
    }

    if (
      this.state !== "OPEN" ||
      this.truncated
    ) {
      return () => {
        // Collection is no longer accepting listeners.
      };
    }

    if (this.notifying) {
      throw new Error(
        "Reentrant build log subscription is not supported.",
      );
    }

    if (
      this.listeners.size >=
      this.maxSubscribers
    ) {
      throw new RangeError(
        "Build log subscriber limit reached.",
      );
    }

    const registration:
      ListenerRegistration = {
        listener,

        active: true,
      };

    this.listeners.add(registration);

    /**
     * Unsubscribe is idempotent and removes only
     * the specific registration owned by this call.
     */
    return () => {
      if (!registration.active) {
        return;
      }

      registration.active = false;

      this.listeners.delete(
        registration,
      );
    };
  }

  private notifySubscribers(
    entry: BuildLogEntry,
  ): void {
    if (
      this.notifying ||
      this.listeners.size === 0
    ) {
      return;
    }

    this.notifying = true;

    try {
      /**
       * Snapshot registrations so listener removal
       * does not corrupt the iteration order.
       */
      const registrations = [
        ...this.listeners,
      ];

      for (
        const registration of registrations
      ) {
        if (!registration.active) {
          continue;
        }

        try {
          /**
           * Each listener receives an independent
           * copy, preventing mutation of the stored
           * entry or another listener's payload.
           */
          registration.listener({
            ...entry,
          });
        } catch {
          /**
           * A failing streaming sink must not abort
           * build execution or other subscribers.
           *
           * Remove the failed listener to avoid
           * repeatedly invoking a broken sink.
           */
          registration.active = false;

          this.listeners.delete(
            registration,
          );

          if (
            this.subscriberFailures <
            Number.MAX_SAFE_INTEGER
          ) {
            this.subscriberFailures += 1;
          }
        }
      }
    } finally {
      this.notifying = false;
    }
  }

  private clearSubscribers(): void {
    for (
      const registration of this.listeners
    ) {
      registration.active = false;
    }

    this.listeners.clear();
  }

  // ================================================
  // Truncation
  // ================================================

  private markTruncated(
    reason: string = TRUNCATION_MESSAGE,
  ): void {
    if (
      this.truncated ||
      this.state === "FINALIZED"
    ) {
      return;
    }

    this.truncated = true;

    /**
     * Only trusted internal marker constants are
     * permitted here. Raw executor messages must
     * never bypass the normalizer.
     */
    const marker =
      reason === NORMALIZATION_FAILURE_MESSAGE
        ? NORMALIZATION_FAILURE_MESSAGE
        : TRUNCATION_MESSAGE;

    const stored = this.storeEntry(
      "system",
      marker,
    );

    /**
     * The normal truncation marker is shorter and
     * may fit when the failure marker does not.
     */
    if (
      !stored &&
      marker !== TRUNCATION_MESSAGE
    ) {
      this.storeEntry(
        "system",
        TRUNCATION_MESSAGE,
      );
    }

    /**
     * Truncation is terminal for this collector.
     * Release subscriber references immediately.
     */
    this.clearSubscribers();

    /**
     * Finalize after the current normalizer operation
     * completes. Do not reenter the normalizer from
     * inside its record callback.
     */
    this.finalizationRequested = true;

    if (
      !this.processing &&
      !this.notifying
    ) {
      this.finalize();
    }
  }

  // ================================================
  // Stream finalization
  // ================================================

  /**
   * Flush final unterminated lines after the executor
   * has finished emitting logs.
   *
   * Safe to call more than once.
   */
  finalize(): void {
    if (
      this.state === "FINALIZED" ||
      this.state === "FINALIZING"
    ) {
      return;
    }

    if (
      this.processing ||
      this.notifying
    ) {
      this.finalizationRequested = true;

      return;
    }

    this.state = "FINALIZING";

    this.finalizationRequested = false;

    this.processing = true;

    try {
      /**
       * When truncated, pushLine rejects additional
       * records while the normalizer releases its
       * pending stream state.
       */
      this.normalizer.finalize();
    } catch {
      this.markTruncated(
        NORMALIZATION_FAILURE_MESSAGE,
      );
    } finally {
      this.processing = false;

      this.finalizationRequested = false;

      this.state = "FINALIZED";

      this.clearSubscribers();

      disposeStreamRedactors(
        this.redactors,
      );
    }
  }

  // ================================================
  // Snapshot
  // ================================================

  /**
   * Return an independent copy of completed,
   * sanitized log entries.
   *
   * Pending raw chunks and registered secret values
   * are never exposed.
   */
  snapshot(): BuildLogEntry[] {
    return this.entries.map(
      (entry) => ({
        ...entry,
      }),
    );
  }
}