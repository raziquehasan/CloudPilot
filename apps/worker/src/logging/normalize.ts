
/**
 * CloudPilot — Log Normalization
 *
 * Phase 5.6 — Real-Time Deployment Logs & Streaming Engine
 *
 * Converts raw process/container output into normalized,
 * sanitized and bounded log records.
 *
 * Responsibilities:
 *
 * - Decode UTF-8 across chunk boundaries.
 * - Preserve independent stream buffers.
 * - Handle CRLF and LF line endings.
 * - Remove unsafe terminal and formatting controls.
 * - Apply secret redaction before output truncation.
 * - Bound input chunks and pending-line accumulation.
 * - Infer severity independently of the original stream.
 * - Emit explicit collection truncation notifications.
 * - Flush decoder state before finalizing trailing lines.
 * - Prevent malformed input from crashing collection.
 *
 * All log content is untrusted and must never be rendered
 * as raw HTML or executed by consumers.
 */

import { StringDecoder } from "node:string_decoder";

import type {
  CollectedLogRecord,
  DeploymentLogLevel,
  DeploymentLogSource,
} from "@cloudpilot/queue";

import {
  SecretRedactor,
} from "./redact.js";

// ==================================================
// Constants
// ==================================================

const DEFAULT_MAX_LINE_BYTES = 16 * 1024;

const DEFAULT_MAX_PENDING_LINE_BYTES = 64 * 1024;

const HARD_MAX_LINE_BYTES = 16 * 1024;

const HARD_MAX_PENDING_LINE_BYTES = 256 * 1024;

const MAX_DECODE_CHUNK_BYTES = 16 * 1024;

const MIN_LINE_BYTES = 64;

const OVERSIZED_LINE_MESSAGE =
  "[Oversized log line omitted]";

const TRUNCATION_MESSAGE =
  "[Log line limit reached; collection truncated]";

const COLLECTION_FAILURE_MESSAGE =
  "[Log collection stopped due to invalid input]";

const STREAM_NAMES = [
  "stdout",
  "stderr",
  "system",
] as const;

// ==================================================
// Terminal sanitization
// ==================================================

/**
 * Terminal sequences are processed using a bounded
 * character scanner instead of complex escape regexes.
 *
 * State is retained across logical lines so multiline
 * terminal control strings cannot expose their payloads.
 */

type TerminalState =
  | "TEXT"
  | "ESCAPE"
  | "ESCAPE_INTERMEDIATE"
  | "CSI"
  | "OSC"
  | "STRING"
  | "OSC_ESCAPE"
  | "STRING_ESCAPE";

const FORMAT_CONTROL_PATTERN =
  /[\u061c\u200b-\u200f\u202a-\u202e\u2060-\u206f\uFEFF]/gu;

class TerminalSanitizer {
  private state: TerminalState = "TEXT";

  sanitize(value: string): string {
    const output: string[] = [];

    for (const character of value) {
      const code = character.codePointAt(0)!;

      switch (this.state) {
        case "TEXT": {
          if (code === 0x1b) {
            this.state = "ESCAPE";
            break;
          }

          // Eight-bit CSI.
          if (code === 0x9b) {
            this.state = "CSI";
            break;
          }

          // Eight-bit OSC.
          if (code === 0x9d) {
            this.state = "OSC";
            break;
          }

          // DCS, SOS, PM and APC.
          if (
            code === 0x90 ||
            code === 0x98 ||
            code === 0x9e ||
            code === 0x9f
          ) {
            this.state = "STRING";
            break;
          }

          // Preserve tabs only among C0 controls.
          if (
            (code < 0x20 && code !== 0x09) ||
            (code >= 0x7f && code <= 0x9f)
          ) {
            break;
          }

          output.push(character);
          break;
        }

        case "ESCAPE": {
          if (character === "[") {
            this.state = "CSI";
          } else if (character === "]") {
            this.state = "OSC";
          } else if (
            character === "P" ||
            character === "X" ||
            character === "^" ||
            character === "_"
          ) {
            this.state = "STRING";
          } else if (code === 0x1b) {
            this.state = "ESCAPE";
          } else if (
            code >= 0x20 &&
            code <= 0x2f
          ) {
            this.state = "ESCAPE_INTERMEDIATE";
          } else {
            this.state = "TEXT";
          }

          break;
        }

        case "ESCAPE_INTERMEDIATE": {
          if (code === 0x1b) {
            this.state = "ESCAPE";
          } else if (
            code < 0x20 ||
            code > 0x2f
          ) {
            this.state = "TEXT";
          }

          break;
        }

        case "CSI": {
          if (code === 0x1b) {
            this.state = "ESCAPE";
          } else if (
            code === 0x18 ||
            code === 0x1a ||
            (code >= 0x40 && code <= 0x7e)
          ) {
            this.state = "TEXT";
          }

          break;
        }

        case "OSC": {
          if (
            code === 0x07 ||
            code === 0x9c ||
            code === 0x18 ||
            code === 0x1a
          ) {
            this.state = "TEXT";
          } else if (code === 0x1b) {
            this.state = "OSC_ESCAPE";
          }

          break;
        }

        case "STRING": {
          if (
            code === 0x9c ||
            code === 0x18 ||
            code === 0x1a
          ) {
            this.state = "TEXT";
          } else if (code === 0x1b) {
            this.state = "STRING_ESCAPE";
          }

          break;
        }

        case "OSC_ESCAPE": {
          if (
            character === "\\" ||
            code === 0x07 ||
            code === 0x9c
          ) {
            this.state = "TEXT";
          } else if (code !== 0x1b) {
            this.state = "OSC";
          }

          break;
        }

        case "STRING_ESCAPE": {
          if (
            character === "\\" ||
            code === 0x9c
          ) {
            this.state = "TEXT";
          } else if (code !== 0x1b) {
            this.state = "STRING";
          }

          break;
        }
      }
    }

    return output
      .join("")
      .replace(FORMAT_CONTROL_PATTERN, "");
  }

  reset(): void {
    this.state = "TEXT";
  }
}

// ==================================================
// Level inference
// ==================================================

const ERROR_MARKER =
  /\b(?:error|fatal|panic|exception|traceback|failed|failure)\b/iu;

const WARN_MARKER =
  /\b(?:warn|warning|deprecated|retrying)\b/iu;

const DEBUG_MARKER =
  /\b(?:debug|trace|verbose)\b/iu;

function inferLevel(
  message: string,
  _stream: "stdout" | "stderr" | "system",
): DeploymentLogLevel {
  if (!message.trim()) {
    return "UNKNOWN";
  }

  if (ERROR_MARKER.test(message)) {
    return "ERROR";
  }

  if (WARN_MARKER.test(message)) {
    return "WARN";
  }

  if (DEBUG_MARKER.test(message)) {
    return "DEBUG";
  }

  return "INFO";
}

/**
 * Infer severity from content rather than assuming
 * every stderr message represents an error.
 */

export function inferLogLevel(
  message: string,
  stream: "stdout" | "stderr" | "system",
): DeploymentLogLevel {
  if (typeof message !== "string") {
    return "UNKNOWN";
  }

  return inferLevel(message, stream);
}

// ==================================================
// Validation helpers
// ==================================================

function assertBoundedInteger(
  value: number,
  name: string,
  minimum: number,
  maximum: number,
): void {
  if (
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    throw new RangeError(
      `${name} must be an integer between ` +
        `${minimum} and ${maximum}.`,
    );
  }
}

function isRawLogStream(
  value: unknown,
): value is RawLogStream {
  return (
    value === "stdout" ||
    value === "stderr" ||
    value === "system"
  );
}

function isLogSource(
  value: unknown,
): value is DeploymentLogSource {
  return (
    value === "BUILD" ||
    value === "RUNTIME" ||
    value === "SYSTEM"
  );
}

// ==================================================
// UTF-8 byte helpers
// ==================================================

function byteLength(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

/**
 * Truncate without splitting Unicode code points.
 *
 * This function is applied only after sanitization
 * and secret redaction.
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
// Public contracts
// ==================================================

export type RawLogStream =
  | "stdout"
  | "stderr"
  | "system";

export interface LogNormalizerOptions {
  source: DeploymentLogSource;

  /**
   * Backward-compatible shared redactor.
   *
   * A dedicated redactor per normalizer is required.
   */

  redactor: SecretRedactor;

  /**
   * Optional independent redactors for each stream.
   *
   * Each instance must contain the appropriate
   * registered secrets.
   */

  streamRedactors?: Readonly<
    Record<RawLogStream, SecretRedactor>
  >;

  maxLineBytes?: number;

  maxPendingLineBytes?: number;

  now?: () => Date;

  onRecord: (record: CollectedLogRecord) => void;
}

// ==================================================
// Internal stream state
// ==================================================

interface StreamState {
  decoder: StringDecoder;

  sanitizer: TerminalSanitizer;

  pending: string;

  pendingBytes: number;

  inputMode: "buffer" | "string" | null;

  redactor: SecretRedactor;
}

// ==================================================
// Normalizer
// ==================================================

export class LogNormalizer {
  private readonly source: DeploymentLogSource;

  private readonly maxLineBytes: number;

  private readonly maxPendingLineBytes: number;

  private readonly now: () => Date;

  private readonly onRecord:
    (record: CollectedLogRecord) => void;

  private readonly streams:
    Record<RawLogStream, StreamState>;

  private readonly hasIndependentRedactors: boolean;

  private truncated = false;

  private finalized = false;

  private finalizing = false;

  private processing = false;

  constructor(options: LogNormalizerOptions) {
    if (!isLogSource(options.source)) {
      throw new TypeError(
        "Invalid deployment log source.",
      );
    }

    if (
      typeof options.onRecord !== "function"
    ) {
      throw new TypeError(
        "onRecord must be a function.",
      );
    }

    if (
      !options.redactor ||
      typeof options.redactor.redact !== "function"
    ) {
      throw new TypeError(
        "A valid secret redactor is required.",
      );
    }

    if (
      options.now !== undefined &&
      typeof options.now !== "function"
    ) {
      throw new TypeError(
        "now must be a function.",
      );
    }

    this.source = options.source;

    this.maxLineBytes =
      options.maxLineBytes ??
      DEFAULT_MAX_LINE_BYTES;

    this.maxPendingLineBytes =
      options.maxPendingLineBytes ??
      DEFAULT_MAX_PENDING_LINE_BYTES;

    assertBoundedInteger(
      this.maxLineBytes,
      "maxLineBytes",
      MIN_LINE_BYTES,
      HARD_MAX_LINE_BYTES,
    );

    assertBoundedInteger(
      this.maxPendingLineBytes,
      "maxPendingLineBytes",
      this.maxLineBytes,
      HARD_MAX_PENDING_LINE_BYTES,
    );

    this.now =
      options.now ?? (() => new Date());

    this.onRecord = options.onRecord;

    const redactors =
      options.streamRedactors;

    if (redactors) {
      const instances = STREAM_NAMES.map(
        (stream) => redactors[stream],
      );

      for (const redactor of instances) {
        if (
          !redactor ||
          typeof redactor.redact !== "function"
        ) {
          throw new TypeError(
            "Invalid stream redactor.",
          );
        }
      }

      if (new Set(instances).size !== 3) {
        throw new TypeError(
          "Stream redactors must be independent.",
        );
      }
    }

    this.hasIndependentRedactors =
      redactors !== undefined;

    this.streams = {
      stdout: this.createState(
        redactors?.stdout ??
          options.redactor,
      ),

      stderr: this.createState(
        redactors?.stderr ??
          options.redactor,
      ),

      system: this.createState(
        redactors?.system ??
          options.redactor,
      ),
    };
  }

  // ================================================
  // Public collection status
  // ================================================

  get isTruncated(): boolean {
    return this.truncated;
  }

  // ================================================
  // Stream initialization
  // ================================================

  private createState(
    redactor: SecretRedactor,
  ): StreamState {
    return {
      decoder: new StringDecoder("utf8"),

      sanitizer: new TerminalSanitizer(),

      pending: "",

      pendingBytes: 0,

      inputMode: null,

      redactor,
    };
  }

  // ================================================
  // Raw input processing
  // ================================================

  /**
   * Process a raw chunk in bounded slices.
   *
   * Mixing string and Buffer chunks on the same stream
   * is rejected because incomplete decoder state could
   * otherwise change the original byte ordering.
   */

  push(
    stream: RawLogStream,
    chunk: Buffer | string,
  ): void {
    if (
      this.finalized ||
      this.finalizing ||
      this.truncated ||
      this.processing
    ) {
      return;
    }

    if (
      !isRawLogStream(stream) ||
      (
        typeof chunk !== "string" &&
        !Buffer.isBuffer(chunk)
      )
    ) {
      this.markTruncated(
        COLLECTION_FAILURE_MESSAGE,
      );

      return;
    }

    if (chunk.length === 0) {
      return;
    }

    this.processing = true;

    try {
      const state = this.streams[stream];

      const mode =
        typeof chunk === "string"
          ? "string"
          : "buffer";

      if (
        state.inputMode !== null &&
        state.inputMode !== mode
      ) {
        this.markTruncated(
          COLLECTION_FAILURE_MESSAGE,
        );

        return;
      }

      state.inputMode = mode;

      if (typeof chunk === "string") {
        this.consumeString(
          stream,
          state,
          chunk,
        );
      } else {
        this.consumeBuffer(
          stream,
          state,
          chunk,
        );
      }
    } catch {
      this.markTruncated(
        COLLECTION_FAILURE_MESSAGE,
      );
    } finally {
      this.processing = false;
    }
  }

  // ================================================
  // Bounded Buffer decoding
  // ================================================

  private consumeBuffer(
    stream: RawLogStream,
    state: StreamState,
    chunk: Buffer,
  ): void {
    for (
      let offset = 0;
      offset < chunk.length;
      offset += MAX_DECODE_CHUNK_BYTES
    ) {
      if (this.truncated) {
        return;
      }

      const end = Math.min(
        offset + MAX_DECODE_CHUNK_BYTES,
        chunk.length,
      );

      const text = state.decoder.write(
        chunk.subarray(offset, end),
      );

      this.consumeText(
        stream,
        state,
        text,
      );
    }
  }

  // ================================================
  // Bounded string processing
  // ================================================

  private consumeString(
    stream: RawLogStream,
    state: StreamState,
    chunk: string,
  ): void {
    let offset = 0;

    while (
      offset < chunk.length &&
      !this.truncated
    ) {
      let end = Math.min(
        offset + MAX_DECODE_CHUNK_BYTES,
        chunk.length,
      );

      // Avoid splitting a surrogate pair when slicing
      // an existing JavaScript string.

      if (end < chunk.length) {
        const previous =
          chunk.charCodeAt(end - 1);

        if (
          previous >= 0xd800 &&
          previous <= 0xdbff
        ) {
          end -= 1;
        }
      }

      this.consumeText(
        stream,
        state,
        chunk.slice(offset, end),
      );

      offset = end;
    }
  }

  // ================================================
  // Logical line extraction
  // ================================================

  private consumeText(
    stream: RawLogStream,
    state: StreamState,
    text: string,
  ): void {
    let offset = 0;

    while (
      offset < text.length &&
      !this.truncated
    ) {
      const newlineIndex =
        text.indexOf("\n", offset);

      const hasNewline =
        newlineIndex !== -1;

      const end = hasNewline
        ? newlineIndex
        : text.length;

      this.appendSegment(
        state,
        text.slice(offset, end),
      );

      if (this.truncated) {
        return;
      }

      if (!hasNewline) {
        break;
      }

      this.completeLine(
        stream,
        state,
      );

      offset = newlineIndex + 1;
    }
  }

  // ================================================
  // Bounded pending-line accumulation
  // ================================================

  private appendSegment(
    state: StreamState,
    segment: string,
  ): void {
    if (
      this.truncated ||
      segment.length === 0
    ) {
      return;
    }

    const remainingBytes =
      this.maxPendingLineBytes -
      state.pendingBytes;

    if (
      segment.length > remainingBytes ||
      byteLength(segment) > remainingBytes
    ) {
      // Never persist a partial oversized record.
      //
      // Collection stops because discarded content
      // could contain a secret or private key marker.

      this.markTruncated(
        TRUNCATION_MESSAGE,
        true,
      );

      return;
    }

    state.pending += segment;

    state.pendingBytes +=
      byteLength(segment);
  }

  // ================================================
  // Complete logical line
  // ================================================

  private completeLine(
    stream: RawLogStream,
    state: StreamState,
  ): void {
    if (this.truncated) {
      return;
    }

    const rawLine = state.pending;

    state.pending = "";

    state.pendingBytes = 0;

    if (rawLine.length === 0) {
      return;
    }

    const line = state.sanitizer.sanitize(
      rawLine.replace(/\r$/u, ""),
    );

    if (!line.trim()) {
      return;
    }

    this.emit(
      stream,
      state,
      line,
    );
  }

  // ================================================
  // Secret redaction and normalized output
  // ================================================

  private emit(
    stream: RawLogStream,
    state: StreamState,
    rawLine: string,
  ): void {
    if (this.truncated) {
      return;
    }

    /**
     * The legacy shared-redactor mode cannot safely
     * isolate multiline private key state.
     *
     * Stop collection conservatively if a private
     * key marker appears in shared mode.
     */

    if (
      !this.hasIndependentRedactors &&
      /-----BEGIN (?:[A-Z0-9 ]{1,64} )?PRIVATE KEY-----/u.test(
        rawLine,
      )
    ) {
      this.deliverRecord(
        stream,
        "[REDACTED]",
        "WARN",
      );

      this.markTruncated(
        COLLECTION_FAILURE_MESSAGE,
      );

      return;
    }

    const redaction =
      state.redactor.redact(rawLine);

    if (
      !redaction ||
      typeof redaction.message !== "string"
    ) {
      this.markTruncated(
        COLLECTION_FAILURE_MESSAGE,
      );

      return;
    }

    const message = truncateUtf8(
      redaction.message,
      this.maxLineBytes,
    );

    if (!message.trim()) {
      return;
    }

    const level = inferLevel(
      message,
      stream,
    );

    if (
      !this.deliverRecord(
        stream,
        message,
        level,
      )
    ) {
      this.markTruncated(
        COLLECTION_FAILURE_MESSAGE,
      );
    }
  }

  // ================================================
  // Record delivery
  // ================================================

  private deliverRecord(
    stream: RawLogStream,
    message: string,
    level: DeploymentLogLevel,
    source: DeploymentLogSource = this.source,
  ): boolean {
    try {
      const timestamp = this.now();

      if (
        !(timestamp instanceof Date) ||
        !Number.isFinite(timestamp.getTime())
      ) {
        return false;
      }

      const record: CollectedLogRecord = {
        source,

        stream:
          stream === "system"
            ? "SYSTEM"
            : stream === "stderr"
              ? "STDERR"
              : "STDOUT",

        level,

        message,

        eventTimestamp: new Date(
          timestamp.getTime(),
        ),
      };

      this.onRecord(record);

      return true;
    } catch {
      // Never log raw input or sink exception details.
      return false;
    }
  }

  // ================================================
  // Trusted internal notifications
  // ================================================

  private emitSystemMarker(
    message: string,
  ): void {
    /**
     * Only internally generated constant messages
     * are passed through this method.
     *
     * This bypasses the truncated-state guard and
     * secret redaction so collection failure markers
     * can still be delivered.
     */

    this.deliverRecord(
      "system",
      truncateUtf8(
        message,
        this.maxLineBytes,
      ),
      "WARN",
      "SYSTEM",
    );
  }

  // ================================================
  // Fail-closed collection termination
  // ================================================

  private markTruncated(
    reason: string = TRUNCATION_MESSAGE,
    oversized = false,
  ): void {
    if (this.truncated) {
      return;
    }

    this.truncated = true;

    this.clearPendingState();

    if (oversized) {
      this.emitSystemMarker(
        OVERSIZED_LINE_MESSAGE,
      );
    }

    this.emitSystemMarker(reason);
  }

  // ================================================
  // Pending state cleanup
  // ================================================

  private clearPendingState(): void {
    for (const state of Object.values(
      this.streams,
    )) {
      state.pending = "";

      state.pendingBytes = 0;

      state.inputMode = null;

      state.decoder =
        new StringDecoder("utf8");

      state.sanitizer.reset();
    }
  }

  // ================================================
  // Finalization
  // ================================================

  /**
   * Flush decoder state before completing trailing
   * unterminated lines.
   *
   * Safe to call repeatedly.
   *
   * Redactors remain caller-owned and are not
   * automatically disposed by the normalizer.
   */

  finalize(): void {
    if (
      this.finalized ||
      this.finalizing ||
      this.processing
    ) {
      return;
    }

    this.finalizing = true;

    try {
      if (!this.truncated) {
        for (const stream of STREAM_NAMES) {
          if (this.truncated) {
            break;
          }

          const state =
            this.streams[stream];

          // Flush incomplete UTF-8 decoder state
          // before emitting the trailing line.

          const trailingText =
            state.decoder.end();

          if (trailingText.length > 0) {
            this.consumeText(
              stream,
              state,
              trailingText,
            );
          }

          if (!this.truncated) {
            this.completeLine(
              stream,
              state,
            );
          }
        }
      }
    } catch {
      this.markTruncated(
        COLLECTION_FAILURE_MESSAGE,
      );
    } finally {
      this.clearPendingState();

      this.finalized = true;

      this.finalizing = false;
    }
  }
}
