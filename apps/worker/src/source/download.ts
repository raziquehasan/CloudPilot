/**
 * CloudPilot — Secure GitHub Archive Downloader
 *
 * Downloads repository archives at immutable commit SHAs.
 *
 * Security guarantees:
 * - Strict HTTPS destination allowlist
 * - Manual redirect validation
 * - Authorization stripping after redirects
 * - Bounded streaming downloads
 * - Exclusive filesystem writes
 * - Cancellation-aware retries
 * - Sanitized errors and URLs
 *
 * Repository content must never be executed here.
 */

import { open, rm } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";

import {
  SourcePreparationError,
  type SourceFailureCode,
} from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const INTERNAL_APPROVED_HOSTS = new Set([
  "api.github.com",
  "github.com",
  "codeload.github.com",
]);

// Export a separate copy so external mutation cannot
// modify the downloader's internal security allowlist.

export const APPROVED_HOSTS: ReadonlySet<string> =
  new Set(INTERNAL_APPROVED_HOSTS);

const REDIRECT_STATUSES = new Set([
  301,
  302,
  303,
  307,
  308,
]);

const MAX_REDIRECTS = 5;

const WRITE_CHUNK = 64 * 1024;

const MAX_RETRIES = 10;

const MAX_TIMEOUT_MS = 2_147_483_647;

const INITIAL_BACKOFF_MS = 500;

const MAX_BACKOFF_MS = 8_000;

const MAX_RETRY_WAIT_MS = 60_000;

const FILE_MODE = 0o600;

const OWNER_PATTERN =
  /^[A-Za-z0-9-]{1,39}$/u;

const REPO_PATTERN =
  /^[A-Za-z0-9._-]{1,100}$/u;

const SHA_PATTERN =
  /^[0-9a-f]{40}$/u;

// ==================================================
// Public contracts
// ==================================================

export interface DownloadOptions {
  token: string;

  owner: string;

  repo: string;

  sha: string;

  destinationPath: string;

  maxArchiveBytes: number;

  timeoutMs: number;

  maxRetries: number;

  signal?: AbortSignal;

  fetchImpl?: typeof fetch;

  sleep?: (ms: number) => Promise<void>;
}

export interface DownloadResult {
  downloadedBytes: number;

  /**
   * Sanitized URL.
   *
   * Query parameters and fragments are intentionally
   * excluded to prevent temporary credential exposure.
   */
  finalUrl: string;
}

// ==================================================
// Internal error contract
// ==================================================

class ArchiveDownloadError
  extends SourcePreparationError {
  constructor(
    code: SourceFailureCode,
    message: string,
    readonly retryAfterMs?: number,
  ) {
    super(code, message);
  }
}

// ==================================================
// Input validation
// ==================================================

function assertSafeRef(
  owner: string,
  repo: string,
  sha: string,
): void {
  if (
    typeof owner !== "string" ||
    !OWNER_PATTERN.test(owner) ||
    owner.startsWith("-") ||
    owner.endsWith("-")
  ) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Invalid repository owner.",
    );
  }

  if (
    typeof repo !== "string" ||
    !REPO_PATTERN.test(repo) ||
    repo === "." ||
    repo === ".."
  ) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Invalid repository name.",
    );
  }

  if (
    typeof sha !== "string" ||
    !SHA_PATTERN.test(sha)
  ) {
    throw new SourcePreparationError(
      "COMMIT_NOT_FOUND",
      "Invalid immutable commit SHA.",
    );
  }
}

function validateOptions(
  options: DownloadOptions,
): void {
  assertSafeRef(
    options.owner,
    options.repo,
    options.sha,
  );

  if (
    typeof options.token !== "string" ||
    options.token.length === 0 ||
    /[\s\u0000-\u001f\u007f]/u.test(
      options.token,
    )
  ) {
    throw new SourcePreparationError(
      "INSTALLATION_UNAVAILABLE",
      "Invalid GitHub installation token.",
    );
  }

  if (
    typeof options.destinationPath !== "string" ||
    options.destinationPath.length === 0 ||
    options.destinationPath.includes("\0")
  ) {
    throw new SourcePreparationError(
      "WORKSPACE_UNAVAILABLE",
      "Invalid archive destination.",
    );
  }

  if (
    !Number.isSafeInteger(
      options.maxArchiveBytes,
    ) ||
    options.maxArchiveBytes < 1
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Invalid archive size limit.",
    );
  }

  if (
    !Number.isSafeInteger(options.timeoutMs) ||
    options.timeoutMs < 1 ||
    options.timeoutMs > MAX_TIMEOUT_MS
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Invalid archive download timeout.",
    );
  }

  if (
    !Number.isSafeInteger(options.maxRetries) ||
    options.maxRetries < 0 ||
    options.maxRetries > MAX_RETRIES
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Invalid archive retry limit.",
    );
  }
}

// ==================================================
// Cancellation and timeout handling
// ==================================================

function assertNotCancelled(
  signal?: AbortSignal,
): void {
  if (signal?.aborted) {
    throw new SourcePreparationError(
      "SOURCE_PREPARATION_CANCELLED",
      "Source download was cancelled.",
    );
  }
}

function assertDownloadActive(
  externalSignal: AbortSignal | undefined,
  timeoutSignal: AbortSignal,
): void {
  assertNotCancelled(externalSignal);

  if (timeoutSignal.aborted) {
    throw new SourcePreparationError(
      "DOWNLOAD_TIMEOUT",
      "Source download timed out.",
    );
  }
}

// ==================================================
// SSRF and redirect validation
// ==================================================

export function isApprovedTarget(
  rawUrl: string,
  baseUrl?: string,
): URL {
  let url: URL;

  try {
    url = baseUrl
      ? new URL(rawUrl, baseUrl)
      : new URL(rawUrl);
  } catch {
    throw new SourcePreparationError(
      "UNSAFE_ARCHIVE",
      "Redirect target is invalid.",
    );
  }

  if (url.protocol !== "https:") {
    throw new SourcePreparationError(
      "UNSAFE_ARCHIVE",
      "Redirect target must use HTTPS.",
    );
  }

  if (
    url.username !== "" ||
    url.password !== ""
  ) {
    throw new SourcePreparationError(
      "UNSAFE_ARCHIVE",
      "Redirect target contains embedded credentials.",
    );
  }

  if (
    url.port !== "" &&
    url.port !== "443"
  ) {
    throw new SourcePreparationError(
      "UNSAFE_ARCHIVE",
      "Redirect target uses an unauthorized port.",
    );
  }

  if (
    !INTERNAL_APPROVED_HOSTS.has(
      url.hostname,
    )
  ) {
    throw new SourcePreparationError(
      "UNSAFE_ARCHIVE",
      "Redirect target is not an approved GitHub host.",
    );
  }

  if (url.hash !== "") {
    throw new SourcePreparationError(
      "UNSAFE_ARCHIVE",
      "Redirect target contains an unexpected fragment.",
    );
  }

  return url;
}

function sanitizeFinalUrl(url: URL): string {
  // Return only the origin. Temporary credentials
  // may appear in query parameters or URL paths.

  return url.origin;
}

// ==================================================
// HTTP status classification
// ==================================================

function classifyStatus(
  status: number,
  headers: Headers,
): SourceFailureCode {
  if (status === 429) {
    return "GITHUB_RATE_LIMITED";
  }

  if (status === 403) {
    const remaining = headers.get(
      "x-ratelimit-remaining",
    );

    const retryAfter = headers.get(
      "retry-after",
    );

    if (
      remaining === "0" ||
      retryAfter !== null
    ) {
      return "GITHUB_RATE_LIMITED";
    }

    return "REPOSITORY_ACCESS_DENIED";
  }

  if (status === 401) {
    return "REPOSITORY_ACCESS_DENIED";
  }

  if (status === 404 || status === 410) {
    return "COMMIT_NOT_FOUND";
  }

  if (status === 408) {
    return "DOWNLOAD_TIMEOUT";
  }

  if (status >= 500 && status <= 599) {
    return "GITHUB_UNAVAILABLE";
  }

  if (
    status === 400 ||
    status === 405 ||
    status === 422
  ) {
    return "REPOSITORY_ACCESS_DENIED";
  }

  return "UNEXPECTED_FAILURE";
}

// ==================================================
// Rate-limit handling
// ==================================================

function parseRetryAfter(
  headers: Headers,
): number | undefined {
  const retryAfter = headers.get(
    "retry-after",
  );

  if (retryAfter !== null) {
    const value = retryAfter.trim();

    if (/^[0-9]+$/u.test(value)) {
      const seconds = Number(value);

      if (
        Number.isSafeInteger(seconds) &&
        seconds >= 0 &&
        seconds <= Number.MAX_SAFE_INTEGER / 1000
      ) {
        return seconds * 1000;
      }
    }

    const timestamp = Date.parse(value);

    if (Number.isFinite(timestamp)) {
      return Math.max(
        0,
        timestamp - Date.now(),
      );
    }
  }

  const remaining = headers.get(
    "x-ratelimit-remaining",
  );

  const reset = headers.get(
    "x-ratelimit-reset",
  );

  if (
    remaining === "0" &&
    reset !== null &&
    /^[0-9]+$/u.test(reset)
  ) {
    const resetSeconds = Number(reset);

    if (
      Number.isSafeInteger(resetSeconds) &&
      resetSeconds <= Number.MAX_SAFE_INTEGER / 1000
    ) {
      return Math.max(
        0,
        resetSeconds * 1000 -
          Date.now(),
      );
    }
  }

  return undefined;
}

// ==================================================
// Content-Length validation
// ==================================================

function validateContentLength(
  headers: Headers,
  maxArchiveBytes: number,
): number | undefined {
  const contentLength = headers.get(
    "content-length",
  );

  if (contentLength === null) {
    return undefined;
  }

  if (
    !/^[0-9]+$/u.test(
      contentLength.trim(),
    )
  ) {
    throw new SourcePreparationError(
      "DOWNLOAD_INTERRUPTED",
      "Archive content length is invalid.",
    );
  }

  const size = Number(contentLength);

  if (
    !Number.isSafeInteger(size) ||
    size < 0
  ) {
    throw new SourcePreparationError(
      "ARCHIVE_SIZE_EXCEEDED",
      "Archive content length exceeds safe bounds.",
    );
  }

  if (size > maxArchiveBytes) {
    throw new SourcePreparationError(
      "ARCHIVE_SIZE_EXCEEDED",
      "Archive exceeds the compressed size limit.",
    );
  }

  if (size === 0) {
    throw new SourcePreparationError(
      "DOWNLOAD_INTERRUPTED",
      "GitHub returned an empty archive.",
    );
  }

  return size;
}

// ==================================================
// Sanitized error normalization
// ==================================================

function normalizeDownloadError(
  error: unknown,
  externalSignal: AbortSignal | undefined,
  timeoutSignal: AbortSignal,
): SourcePreparationError {
  if (error instanceof SourcePreparationError) {
    return error;
  }

  if (externalSignal?.aborted) {
    return new SourcePreparationError(
      "SOURCE_PREPARATION_CANCELLED",
      "Source download was cancelled.",
    );
  }

  if (timeoutSignal.aborted) {
    return new SourcePreparationError(
      "DOWNLOAD_TIMEOUT",
      "Source download timed out.",
    );
  }

  if (
    typeof error === "object" &&
    error !== null &&
    "code" in error
  ) {
    const code = error.code;

    if (
      code === "ENOSPC" ||
      code === "EDQUOT"
    ) {
      return new SourcePreparationError(
        "INSUFFICIENT_DISK_SPACE",
        "Insufficient disk space for archive download.",
      );
    }

    if (
      code === "EACCES" ||
      code === "EPERM" ||
      code === "EROFS" ||
      code === "ENOENT" ||
      code === "EISDIR" ||
      code === "EMFILE" ||
      code === "ENFILE" ||
      code === "EIO"
    ) {
      return new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Archive destination is unavailable.",
      );
    }

    if (code === "EEXIST") {
      return new SourcePreparationError(
        "WORKSPACE_UNAVAILABLE",
        "Archive destination already exists.",
      );
    }
  }

  if (
    error instanceof TypeError ||
    (
      error instanceof Error &&
      (
        error.name === "AbortError" ||
        error.name === "TimeoutError"
      )
    )
  ) {
    return new SourcePreparationError(
      "DOWNLOAD_INTERRUPTED",
      "GitHub archive connection was interrupted.",
    );
  }

  return new SourcePreparationError(
    "UNEXPECTED_FAILURE",
    "Source archive download failed unexpectedly.",
  );
}

// ==================================================
// Cancellation-aware retry waiting
// ==================================================

async function waitForRetry(
  ms: number,
  options: DownloadOptions,
): Promise<void> {
  assertNotCancelled(options.signal);

  if (!options.sleep) {
    try {
      await delay(ms, undefined, {
        ...(options.signal
          ? { signal: options.signal }
          : {}),
      });
    } catch (error) {
      assertNotCancelled(options.signal);

      throw error;
    }

    return;
  }

  // Preserve injected sleep support for tests while
  // allowing cancellation during the waiting period.

  const signal = options.signal;

  if (!signal) {
    await options.sleep(ms);
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const onAbort = (): void => {
      reject(
        new SourcePreparationError(
          "SOURCE_PREPARATION_CANCELLED",
          "Source download retry was cancelled.",
        ),
      );
    };

    signal.addEventListener(
      "abort",
      onAbort,
      { once: true },
    );

    if (signal.aborted) {
      signal.removeEventListener(
        "abort",
        onAbort,
      );

      onAbort();
      return;
    }

    Promise.resolve()
      .then(() => options.sleep!(ms))
      .then(resolve, reject)
      .finally(() => {
        signal.removeEventListener(
          "abort",
          onAbort,
        );
      });
  });

  assertNotCancelled(signal);
}

// ==================================================
// Safe response cleanup
// ==================================================

async function cancelResponse(
  response: Response,
): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // Cleanup errors must not replace the original
    // classified HTTP or security failure.
  }
}

// ==================================================
// Single download attempt
// ==================================================

async function downloadOnce(
  initialUrl: string,
  options: DownloadOptions,
  fetchImpl: typeof fetch,
): Promise<DownloadResult> {
  // One timeout covers the complete HTTP attempt,
  // including redirects and response streaming.

  const timeoutSignal = AbortSignal.timeout(
    options.timeoutMs,
  );

  const signal = options.signal
    ? AbortSignal.any([
        options.signal,
        timeoutSignal,
      ])
    : timeoutSignal;

  let currentUrl = isApprovedTarget(
    initialUrl,
  );

  let redirects = 0;

  let createdArtifact = false;

  let completed = false;

  let response: Response | undefined;

  try {
    for (;;) {
      assertDownloadActive(
        options.signal,
        timeoutSignal,
      );

      const headers: Record<string, string> = {
        Accept: "application/vnd.github+json",

        "X-GitHub-Api-Version": "2022-11-28",

        "User-Agent": "CloudPilot-SourceRetrieval",

        "Accept-Encoding": "identity",
      };

      // Credentials are only sent on the original
      // api.github.com request.
      //
      // Never restore authorization after a redirect,
      // even when redirected back to api.github.com.

      if (
        redirects === 0 &&
        currentUrl.origin ===
          "https://api.github.com"
      ) {
        headers.Authorization =
          `Bearer ${options.token}`;
      }

      try {
        response = await fetchImpl(
          currentUrl.toString(),
          {
            method: "GET",
            headers,
            signal,
            redirect: "manual",
            credentials: "omit",
            cache: "no-store",
            referrerPolicy: "no-referrer",
          },
        );
      } catch (error) {
        assertDownloadActive(
          options.signal,
          timeoutSignal,
        );

        throw new SourcePreparationError(
          "DOWNLOAD_INTERRUPTED",
          "GitHub archive request was interrupted.",
        );
      }

      assertDownloadActive(
        options.signal,
        timeoutSignal,
      );

      // ============================================
      // Redirect handling
      // ============================================

      if (
        REDIRECT_STATUSES.has(
          response.status,
        )
      ) {
        const location = response.headers.get(
          "location",
        );

        await cancelResponse(response);

        response = undefined;

        if (!location) {
          throw new SourcePreparationError(
            "UNSAFE_ARCHIVE",
            "GitHub redirect target is missing.",
          );
        }

        if (redirects >= MAX_REDIRECTS) {
          throw new SourcePreparationError(
            "UNSAFE_ARCHIVE",
            "GitHub redirect limit exceeded.",
          );
        }

        currentUrl = isApprovedTarget(
          location,
          currentUrl.toString(),
        );

        redirects++;

        continue;
      }

      // ============================================
      // HTTP error handling
      // ============================================

      if (response.status !== 200) {
        const code = classifyStatus(
          response.status,
          response.headers,
        );

        const retryAfterMs = parseRetryAfter(
          response.headers,
        );

        await cancelResponse(response);

        response = undefined;

        throw new ArchiveDownloadError(
          code,
          "GitHub archive request failed.",
          retryAfterMs,
        );
      }

      if (!response.body) {
        throw new SourcePreparationError(
          "DOWNLOAD_INTERRUPTED",
          "GitHub returned an empty archive stream.",
        );
      }

      // Reject HTTP-level compression so the byte
      // limit applies to the actual archive payload.

      const contentEncoding =
        response.headers.get(
          "content-encoding",
        );

      if (
        contentEncoding !== null &&
        contentEncoding
          .trim()
          .toLowerCase() !== "identity"
      ) {
        throw new SourcePreparationError(
          "MALFORMED_ARCHIVE",
          "Unexpected archive content encoding.",
        );
      }

      const expectedBytes =
        validateContentLength(
          response.headers,
          options.maxArchiveBytes,
        );

      // ============================================
      // Exclusive archive creation
      // ============================================

      const handle = await open(
        options.destinationPath,
        "wx",
        FILE_MODE,
      );

      createdArtifact = true;

      let downloadedBytes = 0;

      let reader:
        | ReadableStreamDefaultReader<Uint8Array>
        | undefined;

      let streamCompleted = false;

      try {
        reader = response.body.getReader();

        for (;;) {
          assertDownloadActive(
            options.signal,
            timeoutSignal,
          );

          let chunk:
            ReadableStreamReadResult<Uint8Array>;

          try {
            chunk = await reader.read();
          } catch {
            assertDownloadActive(
              options.signal,
              timeoutSignal,
            );

            throw new SourcePreparationError(
              "DOWNLOAD_INTERRUPTED",
              "Archive stream was interrupted.",
            );
          }

          if (chunk.done) {
            streamCompleted = true;
            break;
          }

          const value = chunk.value;

          if (
            !value ||
            value.byteLength === 0
          ) {
            continue;
          }

          // Validate before writing any part of
          // the incoming chunk to disk.

          if (
            value.byteLength >
            options.maxArchiveBytes -
              downloadedBytes
          ) {
            throw new SourcePreparationError(
              "ARCHIVE_SIZE_EXCEEDED",
              "Archive exceeds the compressed size limit.",
            );
          }

          let offset = 0;

          while (offset < value.byteLength) {
            assertDownloadActive(
              options.signal,
              timeoutSignal,
            );

            const length = Math.min(
              WRITE_CHUNK,
              value.byteLength - offset,
            );

            const { bytesWritten } =
              await handle.write(
                value,
                offset,
                length,
                null,
              );

            if (bytesWritten <= 0) {
              throw new SourcePreparationError(
                "WORKSPACE_UNAVAILABLE",
                "Archive could not be written.",
              );
            }

            offset += bytesWritten;

            downloadedBytes += bytesWritten;
          }
        }

        assertDownloadActive(
          options.signal,
          timeoutSignal,
        );

        if (downloadedBytes === 0) {
          throw new SourcePreparationError(
            "DOWNLOAD_INTERRUPTED",
            "GitHub returned an empty archive.",
          );
        }

        if (
          expectedBytes !== undefined &&
          downloadedBytes !== expectedBytes
        ) {
          throw new SourcePreparationError(
            "DOWNLOAD_INTERRUPTED",
            "Archive download size does not match the expected length.",
          );
        }
      } finally {
        try {
          if (reader) {
            if (!streamCompleted) {
              await reader
                .cancel()
                .catch(() => undefined);
            }

            reader.releaseLock();
          }
        } finally {
          await handle.close();
        }
      }

      assertDownloadActive(
        options.signal,
        timeoutSignal,
      );

      completed = true;

      return {
        downloadedBytes,

        finalUrl: sanitizeFinalUrl(
          currentUrl,
        ),
      };
    }
  } catch (error) {
    throw normalizeDownloadError(
      error,
      options.signal,
      timeoutSignal,
    );
  } finally {
    if (response && !completed) {
      await cancelResponse(response);
    }

    // Only remove an artifact created by this attempt.
    // Never delete a pre-existing destination.

    if (createdArtifact && !completed) {
      try {
        await removeDownloadArtifact(
          options.destinationPath,
        );
      } catch {
        // Do not retry into a potentially inconsistent
        // workspace when partial-file cleanup fails.

        throw new SourcePreparationError(
          "WORKSPACE_UNAVAILABLE",
          "Partial archive cleanup failed.",
        );
      }
    }
  }
}

// ==================================================
// Public download orchestration
// ==================================================

export async function downloadGitHubArchive(
  options: DownloadOptions,
): Promise<DownloadResult> {
  validateOptions(options);

  assertNotCancelled(options.signal);

  const fetchImpl =
    options.fetchImpl ?? fetch;

  const initialUrl =
    `https://api.github.com/repos/` +
    `${encodeURIComponent(options.owner)}/` +
    `${encodeURIComponent(options.repo)}/` +
    `tarball/${options.sha}`;

  let attempt = 0;

  for (;;) {
    assertNotCancelled(options.signal);

    try {
      return await downloadOnce(
        initialUrl,
        options,
        fetchImpl,
      );
    } catch (error) {
      assertNotCancelled(options.signal);

      if (
        !(error instanceof SourcePreparationError) ||
        !error.retryable ||
        attempt >= options.maxRetries
      ) {
        throw error;
      }

      // Every retry starts at the original GitHub
      // API endpoint, obtaining a fresh redirect URL.

      attempt++;

      const exponentialBackoff = Math.min(
        MAX_BACKOFF_MS,
        INITIAL_BACKOFF_MS *
          2 ** (attempt - 1),
      );

      const jitter = Math.floor(
        Math.random() * 250,
      );

      const retryAfterMs =
        error instanceof ArchiveDownloadError
          ? error.retryAfterMs
          : undefined;

      const minimumRateLimitDelay =
        error.code === "GITHUB_RATE_LIMITED"
          ? 60_000
          : 0;

      const waitMs = Math.max(
        exponentialBackoff + jitter,
        retryAfterMs ?? minimumRateLimitDelay,
      );

      // Do not hold a worker indefinitely when GitHub
      // requests a long rate-limit waiting period.
      //
      // The deployment queue must handle the deferred
      // retry without retrying earlier than requested.

      if (waitMs > MAX_RETRY_WAIT_MS) {
        throw error;
      }

      await waitForRetry(
        waitMs,
        options,
      );
    }
  }
}

// ==================================================
// Archive cleanup
// ==================================================

export async function removeDownloadArtifact(
  path: string,
): Promise<void> {
  // This path must originate from the trusted
  // WorkspaceManager, never from repository content.

  await rm(path, {
    force: true,
    recursive: false,
  });
}