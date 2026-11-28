// ==================================================
// Scheduler diagnostics (Phase 5.7.9, Stage J)
// ==================================================
//
// Pure, deterministic helpers for the health
// scheduler's backoff and lag calculations so the
// behavior can be unit tested without timers or
// infrastructure. No policy is changed here: the
// backoff formula matches the existing scheduler
// (min(cap, pollMs * 2 ** failures)).
// ==================================================

export const DEFAULT_BACKOFF_CAP_MS = 30_000;
export const MAX_CONSECUTIVE_FAILURES = 10;

export function computeBackoffMs(
  pollMs: number,
  failures: number,
  capMs: number = DEFAULT_BACKOFF_CAP_MS,
): number {
  if (!Number.isInteger(pollMs) || pollMs < 1 || pollMs > 30_000) {
    throw new RangeError("Invalid poll interval");
  }
  if (!Number.isInteger(failures) || failures < 0 || failures > MAX_CONSECUTIVE_FAILURES) {
    throw new RangeError("Invalid failure count");
  }
  if (!Number.isInteger(capMs) || capMs < pollMs) {
    throw new RangeError("Invalid backoff cap");
  }
  return Math.min(capMs, pollMs * 2 ** failures);
}

/**
 * Scheduling lag is the amount by which the actual wait
 * exceeded the intended backoff. Negative values (woke
 * early) are clamped to zero.
 */
export function schedulingLagMs(actualWaitMs: number, intendedWaitMs: number): number {
  if (!Number.isFinite(actualWaitMs) || !Number.isFinite(intendedWaitMs)) return 0;
  return Math.max(0, actualWaitMs - intendedWaitMs);
}