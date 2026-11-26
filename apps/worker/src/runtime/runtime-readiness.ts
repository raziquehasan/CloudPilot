/**
 * CloudPilot — Startup Readiness
 *
 * Verifies basic startup readiness against the verified local
 * runtime endpoint only.
 *
 * Security properties:
 * - Targets explicit numeric loopback endpoints only.
 * - Rejects arbitrary external hosts and unsupported protocols.
 * - Validates ports, paths, and connection deadlines.
 * - Supports cooperative cancellation through AbortSignal.
 * - Cleans up sockets, timers, and abort listeners.
 * - Does not perform HTTP requests or follow redirects.
 *
 * Security limitations:
 * - A reachable TCP port establishes basic TCP connectivity,
 *   not full application health.
 * - Loopback validation does not establish container ownership.
 * - The caller must verify Docker container identity and the
 *   actual published port before performing readiness checks.
 * - Continuous monitoring and rollback belong to later phases.
 */

import { Socket } from "node:net";

import {
  RuntimeDeploymentError,
} from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

/**
 * Numeric loopback addresses only.
 *
 * Hostnames are intentionally excluded to avoid
 * DNS resolution and maintain consistency with
 * runtime-config.ts.
 */
const LOOPBACK_HOSTS: ReadonlySet<string> =
  new Set([
    "127.0.0.1",
    "::1",
  ]);

const MIN_PORT = 1;

const MAX_PORT = 65_535;

const MAX_PATH_LENGTH = 256;

const MIN_PROBE_TIMEOUT_MS = 1;

const MAX_PROBE_TIMEOUT_MS = 60_000;

const CONTROL_CHARACTER_PATTERN =
  /[\u0000-\u001f\u007f-\u009f]/u;

const UNSAFE_PATH_CHARACTER_PATTERN =
  /[\\@#\s]/u;

const INVALID_PERCENT_ENCODING_PATTERN =
  /%(?![a-fA-F0-9]{2})/u;

/**
 * Reject encoded control characters and encoded
 * backslashes in readiness paths.
 */
const UNSAFE_ENCODED_CHARACTER_PATTERN =
  /%(?:0[0-9a-f]|1[0-9a-f]|7f|5c)/iu;

// ==================================================
// Public target contract
// ==================================================

export interface ReadinessTarget {
  host: string;
  port: number;
  path?: string | null;
}

// ==================================================
// Error handling
// ==================================================

/**
 * Return a sanitized runtime configuration error.
 *
 * Do not include untrusted endpoint values or
 * underlying network errors in public messages.
 */
function invalid(
  message: string,
): RuntimeDeploymentError {
  return new RuntimeDeploymentError(
    "INVALID_RUNTIME_CONFIGURATION",
    message,
  );
}

// ==================================================
// Host validation
// ==================================================

/**
 * Accept only explicitly supported numeric
 * loopback addresses.
 *
 * Reject hostnames, wildcard addresses, public IPs,
 * private LAN addresses, and IPv4-mapped IPv6 forms.
 */
function isLoopbackHost(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    LOOPBACK_HOSTS.has(value)
  );
}

// ==================================================
// Port validation
// ==================================================

function isValidPort(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= MIN_PORT &&
    value <= MAX_PORT
  );
}

// ==================================================
// Timeout validation
// ==================================================

/**
 * Reject invalid, unbounded, fractional, negative,
 * and non-finite timeout values.
 */
function isValidTimeout(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= MIN_PROBE_TIMEOUT_MS &&
    value <= MAX_PROBE_TIMEOUT_MS
  );
}

// ==================================================
// Readiness path validation
// ==================================================

/**
 * Validate an origin-relative readiness path.
 *
 * The path must not introduce an alternate origin,
 * credentials, fragments, backslashes, raw whitespace,
 * or malformed percent encoding.
 *
 * Query parameters are supported.
 */
function resolveReadinessPath(
  value: unknown,
): string {
  if (value == null) {
    return "/";
  }

  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > MAX_PATH_LENGTH ||
    !value.startsWith("/") ||
    value.startsWith("//") ||
    value.includes("://") ||
    CONTROL_CHARACTER_PATTERN.test(value) ||
    UNSAFE_PATH_CHARACTER_PATTERN.test(value) ||
    INVALID_PERCENT_ENCODING_PATTERN.test(value) ||
    UNSAFE_ENCODED_CHARACTER_PATTERN.test(value)
  ) {
    throw invalid(
      "Readiness path is invalid.",
    );
  }

  return value;
}

// ==================================================
// Readiness URL generation
// ==================================================

/**
 * Build a validated loopback-only readiness URL.
 *
 * Only HTTP is supported.
 *
 * This function constructs a URL but does not
 * perform a network request.
 */
export function buildReadinessUrl(
  target: ReadinessTarget,
): string {
  // --------------------------------------------------
  // Validate target shape
  // --------------------------------------------------

  if (
    !target ||
    typeof target !== "object" ||
    Array.isArray(target)
  ) {
    throw invalid(
      "Readiness target is invalid.",
    );
  }

  const {
    host,
    port,
  } = target;

  // --------------------------------------------------
  // Validate loopback host
  // --------------------------------------------------

  if (!isLoopbackHost(host)) {
    throw invalid(
      "Readiness checks must target a numeric loopback address.",
    );
  }

  // --------------------------------------------------
  // Validate port
  // --------------------------------------------------

  if (!isValidPort(port)) {
    throw invalid(
      "Readiness target port is invalid.",
    );
  }

  // --------------------------------------------------
  // Validate readiness path
  // --------------------------------------------------

  const path =
    resolveReadinessPath(
      target.path,
    );

  // --------------------------------------------------
  // Format IPv6 address correctly
  // --------------------------------------------------

  const displayHost =
    host === "::1"
      ? "[::1]"
      : host;

  const expectedOrigin =
    `http://${displayHost}:${port}`;

  let url: URL;

  let baseUrl: URL;

  // --------------------------------------------------
  // Parse using the standard URL implementation
  // --------------------------------------------------

  try {
    baseUrl = new URL(
      `${expectedOrigin}/`,
    );

    url = new URL(
      path,
      baseUrl,
    );
  } catch {
    throw invalid(
      "Readiness URL is invalid.",
    );
  }

  // --------------------------------------------------
  // Verify final parsed URL
  // --------------------------------------------------

  if (
    url.protocol !== "http:" ||
    url.origin !== baseUrl.origin ||
    url.hostname !== baseUrl.hostname ||
    url.port !== baseUrl.port ||
    url.username !== "" ||
    url.password !== "" ||
    url.hash !== ""
  ) {
    throw invalid(
      "Readiness URL violates the runtime endpoint policy.",
    );
  }

  // Preserve the explicit port and validated path
  // for compatibility with existing callers.
  return `${expectedOrigin}${path}`;
}

// ==================================================
// TCP readiness probe
// ==================================================

/**
 * Attempt a bounded TCP connection to a verified
 * numeric loopback endpoint.
 *
 * Returns:
 * - true when the TCP connection succeeds.
 * - false for invalid configuration, connection
 *   refusal, timeout, cancellation, or socket errors.
 *
 * Existing three-argument callers remain compatible.
 *
 * Callers may optionally provide an AbortSignal
 * associated with worker shutdown, deployment
 * cancellation, execution deadlines, or lease loss.
 *
 * This probe does not send application data.
 */
export function probeTcp(
  host: string,
  port: number,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<boolean> {
  // --------------------------------------------------
  // Validate probe configuration
  // --------------------------------------------------

  if (
    !isLoopbackHost(host) ||
    !isValidPort(port) ||
    !isValidTimeout(timeoutMs)
  ) {
    return Promise.resolve(false);
  }

  // --------------------------------------------------
  // Reject already cancelled probes
  // --------------------------------------------------

  if (signal?.aborted) {
    return Promise.resolve(false);
  }

  return new Promise<boolean>(
    (resolve) => {
      let settled = false;

      let deadline:
        ReturnType<typeof setTimeout> |
        undefined;

      let socket: Socket;

      // ----------------------------------------------
      // Create the socket without connecting yet
      // ----------------------------------------------

      try {
        socket = new Socket();
      } catch {
        resolve(false);
        return;
      }

      // ----------------------------------------------
      // Centralized completion and cleanup
      // ----------------------------------------------

      function finish(
        result: boolean,
      ): void {
        if (settled) {
          return;
        }

        settled = true;

        // --------------------------------------------
        // Clear the absolute connection deadline
        // --------------------------------------------

        if (deadline !== undefined) {
          clearTimeout(deadline);

          deadline = undefined;
        }

        // --------------------------------------------
        // Remove cancellation listener
        // --------------------------------------------

        signal?.removeEventListener(
          "abort",
          onAbort,
        );

        // --------------------------------------------
        // Remove operational listeners
        // --------------------------------------------

        socket.removeListener(
          "connect",
          onConnect,
        );

        socket.removeListener(
          "timeout",
          onTimeout,
        );

        // Keep error and close handlers attached
        // until the socket has fully closed.
        //
        // This prevents late socket errors from
        // becoming unhandled error events.

        // --------------------------------------------
        // Destroy the socket
        // --------------------------------------------

        try {
          if (!socket.destroyed) {
            socket.destroy();
          }
        } catch {
          // Cleanup is best-effort.
          // Never expose raw socket errors.
        }

        resolve(result);
      }

      // ----------------------------------------------
      // Successful connection
      // ----------------------------------------------

      function onConnect(): void {
        if (signal?.aborted) {
          finish(false);
          return;
        }

        finish(true);
      }

      // ----------------------------------------------
      // Socket inactivity timeout
      // ----------------------------------------------

      function onTimeout(): void {
        finish(false);
      }

      // ----------------------------------------------
      // Socket connection error
      // ----------------------------------------------

      function onError(): void {
        finish(false);
      }

      // ----------------------------------------------
      // Socket closed before or after completion
      // ----------------------------------------------

      function onClose(): void {
        finish(false);

        socket.removeListener(
          "error",
          onError,
        );

        socket.removeListener(
          "close",
          onClose,
        );
      }

      // ----------------------------------------------
      // Cooperative cancellation
      // ----------------------------------------------

      function onAbort(): void {
        finish(false);
      }

      // ----------------------------------------------
      // Register handlers before connecting
      // ----------------------------------------------

      try {
        socket.once(
          "connect",
          onConnect,
        );

        socket.once(
          "timeout",
          onTimeout,
        );

        socket.on(
          "error",
          onError,
        );

        socket.once(
          "close",
          onClose,
        );

        signal?.addEventListener(
          "abort",
          onAbort,
          {
            once: true,
          },
        );

        // Recheck cancellation after registering
        // the abort listener.

        if (signal?.aborted) {
          finish(false);
          return;
        }

        // --------------------------------------------
        // Independent absolute connection deadline
        // --------------------------------------------

        deadline = setTimeout(
          () => {
            finish(false);
          },
          timeoutMs,
        );

        // --------------------------------------------
        // Secondary socket inactivity timeout
        // --------------------------------------------

        socket.setTimeout(
          timeoutMs,
        );

        // --------------------------------------------
        // Connect directly to a numeric address
        // --------------------------------------------

        socket.connect({
          host,
          port,
          family:
            host === "::1"
              ? 6
              : 4,
        });
      } catch {
        // Handles synchronous connection failures
        // and unexpected socket setup errors.

        finish(false);
      }
    },
  );
}