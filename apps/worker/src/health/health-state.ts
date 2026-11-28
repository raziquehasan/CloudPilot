import type { DeploymentHealthStatus, HealthFailureReason, HealthScope } from "@cloudpilot/database";

export const healthDefaults = Object.freeze({
  checkIntervalMs: 15_000,
  httpTimeoutMs: 3_000,
  consecutiveFailureLimit: 3,
  recoveryAttemptLimit: 3,
  recoveryWindowMs: 15 * 60_000,
});

export interface HealthState {
  scope: HealthScope;
  status: DeploymentHealthStatus;
  consecutiveSuccesses: number;
  consecutiveFailures: number;
  lastFailureReason: HealthFailureReason | null;
}

export type HealthCheckFailure = Exclude<HealthFailureReason, `RECOVERY_${string}`>;

export type HealthEvent = { scope: HealthScope } & (
  | { kind: "STARTED" | "CHECK_PASSED" | "RECOVERY_VERIFIED" }
  | { kind: "CHECK_FAILED"; reason: HealthCheckFailure }
  | { kind: "RECOVERY_STARTED" | "RECOVERY_FAILED" | "RECOVERY_EXHAUSTED"; attemptsInWindow: number }
);

export type HealthTransition =
  | { applied: true; state: HealthState }
  | { applied: false; reason: "STALE_GENERATION" | "INVALID_TRANSITION" };

const maxCounter = 2_147_483_647;
const increment = (value: number) => Math.min(value + 1, maxCounter);
const invalid = { applied: false, reason: "INVALID_TRANSITION" } as const;

export function createHealthState(scope: HealthScope): HealthState {
  return { scope: { ...scope }, status: "UNKNOWN", consecutiveSuccesses: 0, consecutiveFailures: 0, lastFailureReason: null };
}

export function transitionHealth(
  state: HealthState,
  event: HealthEvent,
  policy: Pick<typeof healthDefaults, "consecutiveFailureLimit" | "recoveryAttemptLimit"> = healthDefaults,
): HealthTransition {
  const current = state.scope;
  const observed = event.scope;
  if (
    current.workspaceId !== observed.workspaceId || current.projectId !== observed.projectId ||
    current.deploymentId !== observed.deploymentId || current.runtimeInstanceId !== observed.runtimeInstanceId ||
    current.generation !== observed.generation
  ) return { applied: false, reason: "STALE_GENERATION" };

  for (const limit of [policy.consecutiveFailureLimit, policy.recoveryAttemptLimit]) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > maxCounter) {
      throw new RangeError("Invalid health threshold");
    }
  }
  if ("attemptsInWindow" in event && (!Number.isSafeInteger(event.attemptsInWindow) || event.attemptsInWindow < 0)) return invalid;

  const apply = (change: Partial<Omit<HealthState, "scope">>): HealthTransition =>
    ({ applied: true, state: { ...state, ...change } });
  const healthy = () => apply({
    status: "HEALTHY", consecutiveSuccesses: increment(state.consecutiveSuccesses),
    consecutiveFailures: 0, lastFailureReason: null,
  });
  const exhausted = () => apply({ status: "FAILED", lastFailureReason: "RECOVERY_EXHAUSTED" });

  switch (event.kind) {
    case "STARTED":
      return state.status === "UNKNOWN" ? apply({ status: "STARTING" }) : invalid;
    case "CHECK_PASSED":
      return state.status === "RECOVERING" || state.status === "FAILED" ? invalid : healthy();
    case "CHECK_FAILED": {
      if (state.status === "RECOVERING" || state.status === "FAILED") return invalid;
      const consecutiveFailures = increment(state.consecutiveFailures);
      return apply({
        status: state.status === "UNHEALTHY" || consecutiveFailures >= policy.consecutiveFailureLimit ? "UNHEALTHY" : "DEGRADED",
        consecutiveSuccesses: 0, consecutiveFailures, lastFailureReason: event.reason,
      });
    }
    case "RECOVERY_STARTED":
      return state.status === "UNHEALTHY" && event.attemptsInWindow < policy.recoveryAttemptLimit
        ? apply({ status: "RECOVERING" }) : invalid;
    case "RECOVERY_VERIFIED":
      return state.status === "RECOVERING" ? healthy() : invalid;
    case "RECOVERY_FAILED":
      if (state.status !== "RECOVERING" || event.attemptsInWindow === 0) return invalid;
      return event.attemptsInWindow >= policy.recoveryAttemptLimit
        ? exhausted() : apply({ status: "UNHEALTHY", lastFailureReason: "RECOVERY_EXECUTION_FAILED" });
    case "RECOVERY_EXHAUSTED":
      return state.status === "UNHEALTHY" && event.attemptsInWindow >= policy.recoveryAttemptLimit
        ? exhausted() : invalid;
  }
}
