
/**
 * CloudPilot — Centralized Secret Redaction
 *
 * Phase 5.6 — Real-Time Deployment Logs & Streaming Engine
 *
 * Best-effort, defense-in-depth redaction applied before
 * log persistence, publication, export or display.
 *
 * Security properties:
 *
 * - Registered secrets are matched literally.
 * - No regular expressions are constructed from secrets.
 * - Input size, registry capacity and secret length are bounded.
 * - Overlapping literal secrets are merged before replacement.
 * - Sensitive headers and assignments use conservative masking.
 * - Multiline private key blocks are suppressed.
 * - Oversized inputs are replaced rather than partially exposed.
 * - Internal failures return a safe replacement marker.
 *
 * Limitations:
 *
 * Redaction cannot guarantee detection of every arbitrary,
 * transformed, encoded or fragmented secret.
 *
 * A redactor must be scoped to an individual logical log stream
 * when multiline private key detection is required.
 *
 * Infrastructure secrets must never be passed to untrusted
 * builds or containers.
 */

export const REDACTED_MARKER = "[REDACTED]";

// ==================================================
// Default configuration
// ==================================================

const DEFAULT_MAX_REGISTERED_SECRETS = 64;

const DEFAULT_MAX_SECRET_LENGTH = 4_096;

const DEFAULT_MAX_INPUT_BYTES = 64 * 1_024;

// ==================================================
// Hard security limits
// ==================================================

const HARD_MAX_REGISTERED_SECRETS = 256;

const HARD_MAX_SECRET_LENGTH = 16_384;

const HARD_MAX_INPUT_BYTES = 256 * 1_024;

const HARD_MAX_REGISTERED_BYTES = 512 * 1_024;

const HARD_MAX_LITERAL_MATCHES = 16_384;

const MIN_SECRET_LENGTH = 4;

const UTF8_ENCODER = new TextEncoder();

// ==================================================
// Pattern-based token detection
// ==================================================

/**
 * These expressions are fixed and never constructed from
 * registered secret values.
 *
 * Input limits provide an additional bound on regex work.
 * No unconditional linear-time guarantee is assumed.
 */

const TOKEN_PATTERNS: readonly RegExp[] = Object.freeze([
  // GitHub classic and installation tokens.
  /\bgh[pousr]_[A-Za-z0-9_]{20,}/gu,

  // GitHub fine-grained personal access tokens.
  /\bgithub_pat_[A-Za-z0-9_]{20,}/gu,

  // AWS access key identifiers.
  /\b(?:AKIA|ASIA)[A-Z0-9]{16}/gu,

  // OpenAI-style API keys.
  /\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{16,}/gu,

  // Google API keys.
  /\bAIza[A-Za-z0-9_-]{30,}/gu,

  // Slack tokens.
  /\bxox[baprs]-[A-Za-z0-9-]{10,}/gu,

  // JWT-like compact tokens.
  /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*/gu,

  // Stripe secret and restricted API keys.
  /\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}/gu,

  // npm access tokens.
  /\bnpm_[A-Za-z0-9]{20,}/gu,

  // GitLab personal access tokens.
  /\bglpat-[A-Za-z0-9_-]{16,}/gu,
]);

// ==================================================
// Sensitive headers and assignments
// ==================================================

/**
 * Sensitive headers are handled conservatively.
 *
 * Redacting the entire logical line prevents leakage from
 * multiple cookies, escaped values and authentication
 * parameters that cannot be parsed reliably as plain text.
 */

const SENSITIVE_HEADER_PATTERN =
  /\b(?:authorization|proxy-authorization|cookie|set-cookie|x-api-key|x-auth-token)["']?\s*[:=]/iu;

/**
 * Standalone authentication credentials.
 */

const AUTHORIZATION_PATTERN =
  /\b(?:Bearer|Basic)[ \t]+[^\s"',;]+/giu;

/**
 * Detect URLs containing user information.
 *
 * The complete matching URL is removed rather than trying
 * to preserve a potentially sensitive hostname or path.
 */

const URL_CANDIDATE_PATTERN =
  /\b[A-Za-z][A-Za-z0-9+.-]{0,31}:\/\/[^\s"'<>]+/gu;

/**
 * Detect potentially sensitive key assignments.
 *
 * Key length is bounded to avoid unbounded key scanning.
 * Values are intentionally not parsed with a complex regex.
 */

const ASSIGNMENT_KEY_PATTERN =
  /(?:^|[^A-Za-z0-9_.-])["']?([A-Za-z_][A-Za-z0-9_.-]{0,255})["']?[ \t]*[:=]/gu;

const SENSITIVE_KEY_PATTERN =
  /(?:token|password|passwd|passphrase|secret|credential|api[-_.]?key|access[-_.]?key|private[-_.]?key|client[-_.]?key|signing[-_.]?key|encryption[-_.]?key|session[-_.]?id)/iu;

const SENSITIVE_EXACT_KEYS: ReadonlySet<string> = new Set([
  "database_url",
  "databaseurl",
  "redis_url",
  "redisurl",
  "connection_string",
  "connectionstring",
  "authorization",
  "proxy-authorization",
  "cookie",
  "set-cookie",
  "x-api-key",
  "x-auth-token",
]);

// ==================================================
// Private key detection
// ==================================================

const PRIVATE_KEY_START_PATTERN =
  /-----BEGIN (?:[A-Z0-9 ]{1,64} )?PRIVATE KEY-----/u;

const PRIVATE_KEY_END_PATTERN =
  /-----END (?:[A-Z0-9 ]{1,64} )?PRIVATE KEY-----/u;

// ==================================================
// Public contracts
// ==================================================

export interface SecretRedactorOptions {
  maxRegisteredSecrets?: number;

  maxSecretLength?: number;

  maxInputBytes?: number;
}

export interface RedactionResult {
  message: string;

  redacted: boolean;
}

// ==================================================
// Internal contracts
// ==================================================

interface LiteralMatch {
  start: number;

  end: number;
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

function utf8ByteLength(value: string): number {
  return UTF8_ENCODER.encode(value).byteLength;
}

function fullyRedacted(): RedactionResult {
  return {
    message: REDACTED_MARKER,
    redacted: true,
  };
}

function isSensitiveKey(key: string): boolean {
  return (
    SENSITIVE_EXACT_KEYS.has(key.toLowerCase()) ||
    SENSITIVE_KEY_PATTERN.test(key)
  );
}

// ==================================================
// Sensitive assignment detection
// ==================================================

function containsSensitiveAssignment(
  message: string,
): boolean {
  // Use a fresh regex instance so lastIndex cannot leak
  // across calls or independent redactor instances.

  const pattern = new RegExp(
    ASSIGNMENT_KEY_PATTERN.source,
    ASSIGNMENT_KEY_PATTERN.flags,
  );

  for (const match of message.matchAll(pattern)) {
    const key = match[1];

    if (key && isSensitiveKey(key)) {
      return true;
    }
  }

  return false;
}

// ==================================================
// URL credential detection
// ==================================================

function redactCredentialUrls(
  message: string,
): string {
  return message.replace(
    URL_CANDIDATE_PATTERN,
    (candidate: string): string => {
      const schemeEnd = candidate.indexOf("://");

      if (schemeEnd < 0) {
        return candidate;
      }

      const remainder = candidate.slice(
        schemeEnd + 3,
      );

      const authorityEnd = remainder.search(
        /[/?#]/u,
      );

      const authority =
        authorityEnd < 0
          ? remainder
          : remainder.slice(0, authorityEnd);

      // Any user information is considered sensitive,
      // even when no explicit password is present.

      if (authority.includes("@")) {
        return REDACTED_MARKER;
      }

      // Mask URLs containing common sensitive query
      // parameter names.

      if (
        /[?&](?:token|access_token|refresh_token|api_key|apikey|password|secret|signature|sig|x-amz-signature|x-amz-credential)=/iu.test(
          candidate,
        )
      ) {
        return REDACTED_MARKER;
      }

      return candidate;
    },
  );
}

// ==================================================
// Literal matching
// ==================================================

/**
 * Collect matching ranges against the original input.
 *
 * Replacing against the original input avoids situations
 * where replacing a shorter secret destroys the matching
 * representation of a longer overlapping secret.
 */

function collectLiteralMatches(
  message: string,
  secrets: readonly string[],
): LiteralMatch[] | null {
  const matches: LiteralMatch[] = [];

  for (const secret of secrets) {
    let offset = 0;

    while (offset < message.length) {
      const index = message.indexOf(
        secret,
        offset,
      );

      if (index < 0) {
        break;
      }

      if (
        matches.length >=
        HARD_MAX_LITERAL_MATCHES
      ) {
        // Excessive matches must not result in
        // partially redacted output.

        return null;
      }

      matches.push({
        start: index,
        end: index + secret.length,
      });

      // Advance one character to detect overlapping
      // occurrences of the same secret.

      offset = index + 1;
    }
  }

  return matches;
}

// ==================================================
// Merge overlapping literal ranges
// ==================================================

function mergeLiteralMatches(
  matches: LiteralMatch[],
): LiteralMatch[] {
  if (matches.length === 0) {
    return [];
  }

  matches.sort(
    (left, right) =>
      left.start - right.start ||
      right.end - left.end,
  );

  const merged: LiteralMatch[] = [];

  for (const match of matches) {
    const previous =
      merged[merged.length - 1];

    if (
      previous &&
      match.start <= previous.end
    ) {
      previous.end = Math.max(
        previous.end,
        match.end,
      );

      continue;
    }

    merged.push({
      start: match.start,
      end: match.end,
    });
  }

  return merged;
}

// ==================================================
// Apply literal replacements
// ==================================================

function applyLiteralMatches(
  message: string,
  matches: readonly LiteralMatch[],
): string {
  if (matches.length === 0) {
    return message;
  }

  const parts: string[] = [];

  let offset = 0;

  for (const match of matches) {
    parts.push(
      message.slice(offset, match.start),
      REDACTED_MARKER,
    );

    offset = match.end;
  }

  parts.push(message.slice(offset));

  return parts.join("");
}

// ==================================================
// Secret redactor
// ==================================================

export class SecretRedactor {
  private readonly maxRegisteredSecrets: number;

  private readonly maxSecretLength: number;

  private readonly maxInputBytes: number;

  private readonly secrets: string[] = [];

  private readonly secretSet: Set<string> =
    new Set();

  private registeredBytes = 0;

  /**
   * Stateful multiline private key suppression.
   *
   * Instances should not be shared across unrelated
   * deployments or independent stdout/stderr streams.
   */

  private insidePrivateKey = false;

  constructor(options?: SecretRedactorOptions) {
    this.maxRegisteredSecrets =
      options?.maxRegisteredSecrets ??
      DEFAULT_MAX_REGISTERED_SECRETS;

    this.maxSecretLength =
      options?.maxSecretLength ??
      DEFAULT_MAX_SECRET_LENGTH;

    this.maxInputBytes =
      options?.maxInputBytes ??
      DEFAULT_MAX_INPUT_BYTES;

    assertBoundedInteger(
      this.maxRegisteredSecrets,
      "maxRegisteredSecrets",
      1,
      HARD_MAX_REGISTERED_SECRETS,
    );

    assertBoundedInteger(
      this.maxSecretLength,
      "maxSecretLength",
      MIN_SECRET_LENGTH,
      HARD_MAX_SECRET_LENGTH,
    );

    assertBoundedInteger(
      this.maxInputBytes,
      "maxInputBytes",
      1,
      HARD_MAX_INPUT_BYTES,
    );
  }

  // ================================================
  // Registry metadata
  // ================================================

  get registeredCount(): number {
    return this.secrets.length;
  }

  // ================================================
  // Register a known sensitive literal
  // ================================================

  /**
   * Register a literal secret without constructing a regex.
   *
   * The original value is preserved rather than silently
   * trimming potentially meaningful whitespace.
   *
   * Whitespace-only values and unsupported multiline
   * values are rejected.
   *
   * Callers must handle registration failure explicitly
   * when protection depends on a known secret.
   */

  registerSecret(value: unknown): boolean {
    if (typeof value !== "string") {
      return false;
    }

    if (
      value.length < MIN_SECRET_LENGTH ||
      value.length > this.maxSecretLength
    ) {
      return false;
    }

    if (
      value.trim().length === 0 ||
      /[\u0000\r\n]/u.test(value)
    ) {
      return false;
    }

    if (this.secretSet.has(value)) {
      return false;
    }

    if (
      this.secrets.length >=
      this.maxRegisteredSecrets
    ) {
      return false;
    }

    const bytes = utf8ByteLength(value);

    if (bytes > this.maxSecretLength) {
      return false;
    }

    if (
      this.registeredBytes + bytes >
      HARD_MAX_REGISTERED_BYTES
    ) {
      return false;
    }

    this.secrets.push(value);

    this.secretSet.add(value);

    this.registeredBytes += bytes;

    return true;
  }

  // ================================================
  // Registry cleanup
  // ================================================

  /**
   * Release registered secret references.
   *
   * JavaScript strings are immutable. Clearing references
   * does not guarantee immediate memory zeroization.
   */

  clearSecrets(): void {
    this.secrets.length = 0;

    this.secretSet.clear();

    this.registeredBytes = 0;
  }

  // ================================================
  // Stream lifecycle
  // ================================================

  /**
   * Reset multiline state only when the logical log
   * stream has ended or been replaced.
   *
   * Do not call this between individual log records.
   */

  resetStreamState(): void {
    this.insidePrivateKey = false;
  }

  /**
   * Release registry references and reset stream state.
   */

  dispose(): void {
    this.clearSecrets();

    this.resetStreamState();
  }

  // ================================================
  // Private key suppression
  // ================================================

  private redactPrivateKey(
    message: string,
  ): RedactionResult | null {
    const containsStart =
      PRIVATE_KEY_START_PATTERN.test(message);

    const containsEnd =
      PRIVATE_KEY_END_PATTERN.test(message);

    if (this.insidePrivateKey) {
      if (containsEnd && !containsStart) {
        this.insidePrivateKey = false;
      }

      return fullyRedacted();
    }

    if (containsStart) {
      // Suppress the current record and subsequent
      // records until the closing marker is observed.
      //
      // A complete key on one record is also removed.

      this.insidePrivateKey = !containsEnd;

      return fullyRedacted();
    }

    if (containsEnd) {
      return fullyRedacted();
    }

    return null;
  }

  // ================================================
  // Main redaction pipeline
  // ================================================

  /**
   * Redact a complete logical log record.
   *
   * Redaction must run before persistence and before
   * truncation that could remove identifying prefixes.
   */

  redact(line: string): RedactionResult {
    try {
      // Defensive runtime validation.

      if (typeof line !== "string") {
        return fullyRedacted();
      }

      // Check character length before UTF-8 allocation.
      //
      // Oversized records are discarded conservatively.
      // Keep multiline suppression enabled because an
      // oversized record could contain a private key
      // opening marker.

      if (
        line.length > this.maxInputBytes ||
        utf8ByteLength(line) >
          this.maxInputBytes
      ) {
        this.insidePrivateKey = true;

        return fullyRedacted();
      }

      // Suppress multiline private key material before
      // any other transformations.

      const privateKeyResult =
        this.redactPrivateKey(line);

      if (privateKeyResult) {
        return privateKeyResult;
      }

      // Sensitive headers may contain multiple values
      // and complex authentication parameters.

      if (
        SENSITIVE_HEADER_PATTERN.test(line)
      ) {
        return fullyRedacted();
      }

      // Conservatively redact complete records containing
      // sensitive assignments rather than risking partial
      // exposure through quoted or escaped values.

      if (
        containsSensitiveAssignment(line)
      ) {
        return fullyRedacted();
      }

      // Detect credential-bearing URLs against the
      // original input before literal replacements
      // can destroy URL parsing boundaries.

      const urlRedacted =
        redactCredentialUrls(line);

      if (urlRedacted !== line) {
        return fullyRedacted();
      }

      // Remove standalone authentication credentials
      // conservatively.

      const authRedacted = line.replace(
        AUTHORIZATION_PATTERN,
        REDACTED_MARKER,
      );

      if (authRedacted !== line) {
        return fullyRedacted();
      }

      // Detect known token formats against original
      // input so literal replacement cannot conceal
      // the original token structure.

      for (const pattern of TOKEN_PATTERNS) {
        const next = line.replace(
          pattern,
          REDACTED_MARKER,
        );

        if (next !== line) {
          return fullyRedacted();
        }
      }

      // Match registered literals against the original
      // record and merge overlapping ranges.

      const matches = collectLiteralMatches(
        line,
        this.secrets,
      );

      if (matches === null) {
        return fullyRedacted();
      }

      if (matches.length === 0) {
        return {
          message: line,
          redacted: false,
        };
      }

      const merged =
        mergeLiteralMatches(matches);

      const message = applyLiteralMatches(
        line,
        merged,
      );

      // Replacement markers may increase output size.
      // Never return an unbounded expanded record.

      if (
        message.length >
          this.maxInputBytes ||
        utf8ByteLength(message) >
          this.maxInputBytes
      ) {
        return fullyRedacted();
      }

      return {
        message,
        redacted: true,
      };
    } catch {
      // Never include the original record or exception
      // details in a redaction failure result.

      return fullyRedacted();
    }
  }
}
