
/**
 * CloudPilot — Build Log Collection
 *
 * Collects bounded, sanitized build logs.
 *
 * Security controls:
 * - Per-stream chunk buffering
 * - Best-effort credential redaction
 * - Terminal escape sanitization
 * - UTF-8 aware byte limits
 * - Bounded log entry count
 * - Oversized line rejection
 * - Explicit stream finalization
 *
 * Redaction is defense in depth, not a guarantee.
 * Infrastructure secrets must never be passed to builds.
 */

// ==================================================
// Constants
// ==================================================

const DEFAULT_MAX_LOG_BYTES = 10 * 1024 * 1024;

const MAX_ALLOWED_LOG_BYTES = 100 * 1024 * 1024;

const MAX_LINE_BYTES = 64 * 1024;

const MAX_PENDING_LINE_BYTES = 128 * 1024;

const DEFAULT_MAX_LOG_ENTRIES = 20_000;

const MAX_ALLOWED_LOG_ENTRIES = 100_000;

const REDACTED = "[REDACTED]";

const OVERSIZED_LINE_MESSAGE =
  "[Oversized log line omitted]";

const TRUNCATION_MESSAGE =
  "[Build log limit reached]";

// ==================================================
// Terminal sanitization patterns
// ==================================================

// OSC sequences, including terminal hyperlinks.
const OSC_PATTERN =
  /\u001b\][^\u0007\u001b]*(?:\u0007|\u001b\\|$)/gu;

// DCS, SOS, PM and APC sequences.
const TERMINAL_STRING_PATTERN =
  /\u001b[P X^_][^\u001b]*(?:\u001b\\|$)/gu;

// ANSI CSI sequences.
const ANSI_PATTERN =
  /(?:\u001b\[|\u009b)[0-?]*[ -/]*[@-~]/gu;

// Remaining short escape sequences.
const ESCAPE_PATTERN =
  /\u001b[ -/]*[@-_]/gu;

// Preserve tabs and line separators.
const CONTROL_PATTERN =
  /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/gu;

// Bidirectional and invisible formatting controls.
const FORMAT_CONTROL_PATTERN =
  /[\u061c\u200b-\u200f\u202a-\u202e\u2060-\u206f\uFEFF]/gu;

// ==================================================
// Credential redaction
// ==================================================

const TOKEN_PATTERNS: readonly RegExp[] = [
  // GitHub classic and installation tokens.
  /\bgh[pousr]_[A-Za-z0-9_]{20,}\b/gu,

  // GitHub fine-grained personal access tokens.
  /\bgithub_pat_[A-Za-z0-9_]{20,}\b/gu,

  // AWS access key identifiers.
  /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/gu,

  // Common OpenAI-style API keys.
  /\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{16,}\b/gu,

  // Google API keys.
  /\bAIza[A-Za-z0-9_-]{30,}\b/gu,

  // Slack tokens.
  /\bxox[baprs]-[A-Za-z0-9-]{10,}\b/gu,

  // JWT-like compact tokens.
  /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/gu,
];

const AUTHORIZATION_PATTERN =
  /\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{4,}/giu;

const URL_CREDENTIAL_PATTERN =
  /([A-Za-z][A-Za-z0-9+.-]*:\/\/)[^\s/@]+@[^\s/]+/gu;

const SENSITIVE_ASSIGNMENT_PATTERN =
  /(["']?\b(?:[A-Za-z0-9_-]*(?:token|password|passwd|secret|api[_-]?key|access[_-]?key|private[_-]?key|credential)[A-Za-z0-9_-]*|database_url|redis_url|authorization|cookie|set-cookie)["']?\s*[:=]\s*)(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;]+)/giu;

const PRIVATE_KEY_START_PATTERN =
  /-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----/u;

const PRIVATE_KEY_END_PATTERN =
  /-----END (?:[A-Z0-9 ]+ )?PRIVATE KEY-----/u;

// ==================================================
// Sanitization
// ==================================================

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

/**
 * Sanitize a complete logical log line.
 *
 * The collector buffers incomplete lines before
 * calling this function to reduce chunk-boundary
 * credential leakage.
 */
export function sanitizeBuildLogLine(
  line: string,
): string {
  let sanitized = stripTerminalControls(line);

  if (
    PRIVATE_KEY_START_PATTERN.test(sanitized) ||
    PRIVATE_KEY_END_PATTERN.test(sanitized)
  ) {
    return REDACTED;
  }

  for (const pattern of TOKEN_PATTERNS) {
    sanitized = sanitized.replace(
      pattern,
      REDACTED,
    );
  }

  sanitized = sanitized.replace(
    AUTHORIZATION_PATTERN,
    (_match: string, scheme: string) =>
      `${scheme} ${REDACTED}`,
  );

  sanitized = sanitized.replace(
    URL_CREDENTIAL_PATTERN,
    (_match: string, scheme: string) =>
      `${scheme}${REDACTED}`,
  );

  sanitized = sanitized.replace(
    SENSITIVE_ASSIGNMENT_PATTERN,
    (_match: string, prefix: string) =>
      `${prefix}${REDACTED}`,
  );

  return sanitized;
}

// ==================================================
// UTF-8 byte helpers
// ==================================================

function byteLength(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

/**
 * Truncate without splitting a Unicode code point.
 */
function truncateUtf8(
  value: string,
  maxBytes: number,
): string {
  if (maxBytes <= 0) {
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

  let result = "";

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

    result += character;

    usedBytes += characterBytes;
  }

  return `${result}${suffix}`;
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

export interface BuildLogCollectorOptions {
  maxLogBytes?: number;

  maxLogEntries?: number;

  /**
   * Retained for compatibility with existing callers.
   * Log timestamps are managed by the persistence layer.
   */
  now?: () => Date;
}

// ==================================================
// Internal stream state
// ==================================================

interface StreamState {
  pending: string;

  pendingBytes: number;

  discardingOversizedLine: boolean;

  insidePrivateKey: boolean;
}

function createStreamState(): StreamState {
  return {
    pending: "",

    pendingBytes: 0,

    discardingOversizedLine: false,

    insidePrivateKey: false,
  };
}

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

// ==================================================
// Build log collector
// ==================================================

export class BuildLogCollector {
  private readonly maxLogBytes: number;

  private readonly maxLogEntries: number;

  private sequence = 0;

  private totalBytes = 0;

  private truncated = false;

  private finalized = false;

  private readonly entries: BuildLogEntry[] = [];

  private readonly streams: Record<
    BuildLogStream,
    StreamState
  > = {
    stdout: createStreamState(),

    stderr: createStreamState(),

    system: createStreamState(),
  };

  constructor(
    options?: BuildLogCollectorOptions,
  ) {
    this.maxLogBytes =
      options?.maxLogBytes ??
      DEFAULT_MAX_LOG_BYTES;

    this.maxLogEntries =
      options?.maxLogEntries ??
      DEFAULT_MAX_LOG_ENTRIES;

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
    return this.finalized;
  }

  // ================================================
  // Raw chunk collection
  // ================================================

  /**
   * Append a raw chunk.
   *
   * Incomplete lines remain buffered separately
   * for stdout, stderr and system streams.
   */
  append(
    stream: BuildLogStream,
    chunk: string,
  ): void {
    if (
      this.finalized ||
      this.truncated ||
      !chunk
    ) {
      return;
    }

    const state = this.streams[stream];

    let offset = 0;

    while (offset < chunk.length) {
      if (this.truncated) {
        return;
      }

      const newlineIndex =
        chunk.indexOf("\n", offset);

      const hasNewline =
        newlineIndex !== -1;

      const end = hasNewline
        ? newlineIndex
        : chunk.length;

      const segment = chunk.slice(
        offset,
        end,
      );

      this.appendSegment(
        stream,
        state,
        segment,
      );

      if (hasNewline) {
        this.completeLine(
          stream,
          state,
        );

        offset = newlineIndex + 1;
      } else {
        break;
      }
    }
  }

  // ================================================
  // Pending line buffering
  // ================================================

  private appendSegment(
    stream: BuildLogStream,
    state: StreamState,
    segment: string,
  ): void {
    if (
      this.truncated ||
      state.discardingOversizedLine ||
      !segment
    ) {
      return;
    }

    const segmentBytes =
      byteLength(segment);

    if (
      segmentBytes >
      MAX_PENDING_LINE_BYTES -
        state.pendingBytes
    ) {
      /**
       * Do not store a partial oversized line.
       * A partial credential could otherwise
       * escape complete-line redaction.
       */
      state.pending = "";

      state.pendingBytes = 0;

      state.discardingOversizedLine = true;

      this.pushLine(
        stream,
        OVERSIZED_LINE_MESSAGE,
      );

      // Stop collection conservatively.
      this.markTruncated();

      return;
    }

    state.pending += segment;

    state.pendingBytes += segmentBytes;

    /**
     * Reject pending lines that cannot fit within
     * the configured log budget.
     */
    if (
      state.pendingBytes >
      this.maxLogBytes
    ) {
      this.markTruncated();
    }
  }

  // ================================================
  // Complete logical lines
  // ================================================

  private completeLine(
    stream: BuildLogStream,
    state: StreamState,
  ): void {
    if (this.truncated) {
      return;
    }

    if (state.discardingOversizedLine) {
      state.discardingOversizedLine = false;

      state.pending = "";

      state.pendingBytes = 0;

      return;
    }

    const rawLine = state.pending;

    state.pending = "";

    state.pendingBytes = 0;

    const line = stripTerminalControls(
      rawLine.replace(/\r$/u, ""),
    );

    // Suppress multiline private key blocks.
    if (state.insidePrivateKey) {
      if (
        PRIVATE_KEY_END_PATTERN.test(line)
      ) {
        state.insidePrivateKey = false;
      }

      return;
    }

    if (
      PRIVATE_KEY_START_PATTERN.test(line)
    ) {
      state.insidePrivateKey =
        !PRIVATE_KEY_END_PATTERN.test(line);

      this.pushLine(
        stream,
        "[REDACTED PRIVATE KEY]",
      );

      return;
    }

    const sanitized =
      sanitizeBuildLogLine(line);

    this.pushLine(
      stream,
      sanitized,
    );
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
      !line.trim()
    ) {
      return;
    }

    const message = truncateUtf8(
      line,
      MAX_LINE_BYTES,
    );

    if (!message) {
      return;
    }

    const messageBytes =
      byteLength(message);

    if (
      this.entries.length >=
        this.maxLogEntries ||
      messageBytes >
        this.maxLogBytes -
          this.totalBytes
    ) {
      this.markTruncated();

      return;
    }

    this.sequence += 1;

    this.totalBytes += messageBytes;

    this.entries.push({
      sequence: this.sequence,

      stream,

      message,
    });
  }

  // ================================================
  // Truncation
  // ================================================

  private markTruncated(): void {
    if (this.truncated) {
      return;
    }

    this.truncated = true;

    /**
     * Never retain raw incomplete lines after
     * the collection budget is exhausted.
     */
    for (const state of Object.values(
      this.streams,
    )) {
      state.pending = "";

      state.pendingBytes = 0;

      state.discardingOversizedLine = false;
    }

    const markerBytes =
      byteLength(TRUNCATION_MESSAGE);

    if (
      this.entries.length <
        this.maxLogEntries &&
      markerBytes <=
        this.maxLogBytes -
          this.totalBytes
    ) {
      this.sequence += 1;

      this.totalBytes += markerBytes;

      this.entries.push({
        sequence: this.sequence,

        stream: "system",

        message: TRUNCATION_MESSAGE,
      });
    }
  }

  // ================================================
  // Stream finalization
  // ================================================

  /**
   * Flush final unterminated lines after the
   * executor has finished emitting logs.
   *
   * Safe to call more than once.
   */
  finalize(): void {
    if (this.finalized) {
      return;
    }

    if (!this.truncated) {
      const streams: BuildLogStream[] = [
        "stdout",
        "stderr",
        "system",
      ];

      for (const stream of streams) {
        if (this.truncated) {
          break;
        }

        this.completeLine(
          stream,
          this.streams[stream],
        );
      }
    }

    for (const state of Object.values(
      this.streams,
    )) {
      state.pending = "";

      state.pendingBytes = 0;

      state.insidePrivateKey = false;

      state.discardingOversizedLine = false;
    }

    this.finalized = true;
  }

  // ================================================
  // Snapshot
  // ================================================

  /**
   * Return an independent copy of completed,
   * sanitized log entries.
   *
   * Pending raw chunks are never exposed.
   */
  snapshot(): BuildLogEntry[] {
    return this.entries.map(
      (entry) => ({ ...entry }),
    );
  }
}
