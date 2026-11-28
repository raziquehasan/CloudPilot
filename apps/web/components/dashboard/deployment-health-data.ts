import { ApiError } from "@/lib/api";

export type HealthStatus =
  | "UNKNOWN" | "STARTING" | "HEALTHY" | "DEGRADED" | "UNHEALTHY" | "RECOVERING" | "FAILED";
export type ProbeResult = "PASSED" | "FAILED" | "SKIPPED";

export type HealthCheck = {
  id: string;
  generation: number;
  dockerResult: ProbeResult;
  dockerState: string | null;
  httpResult: ProbeResult;
  httpStatus: number | null;
  responseLatencyMs: number | null;
  failureReason: string | null;
  checkedAt: string;
};

export type RecoveryBlock =
  | "FORBIDDEN" | "DISABLED" | "SCHEMA_UNAVAILABLE" | "RUNTIME_NOT_RUNNING"
  | "NOT_UNHEALTHY" | "ACTIVE" | "COOLDOWN" | "EXHAUSTED";

export type MonitoringStatus =
  | "ENABLED" | "DISABLED" | "UNAVAILABLE" | "STALE" | "UNKNOWN";

export type MonitoringReport = {
  status: MonitoringStatus;
  workerCount: number;
  reportingCount: number;
  availableCount: number;
  enabledCount: number;
  lastHeartbeatAt: string | null;
};

export type AutomaticRecovery = {
  configured: boolean;
  workerAvailability: "DISABLED" | "AVAILABLE" | "UNAVAILABLE" | "STALE" | "UNKNOWN" | "CONFLICTING";
  circuitState: "CLOSED" | "OPEN" | "HALF_OPEN" | null;
  eligible: boolean;
  rejectionReason: string | null;
  remainingBudget: number | null;
  maxBudget: number | null;
  consecutiveFailures: number | null;
  cooldownRemainingMs: number | null;
  nextEligibleAt: string | null;
  manualInterventionRequired: boolean | null;
  latestOutcome: Pick<Recovery, "status" | "trigger" | "failureReason" | "finishedAt"> | null;
  snapshotAt: string | null;
};

export type DeploymentHealth = {
  schemaAvailable: boolean;
  runtimeStatus: string | null;
  state: {
    status: HealthStatus;
    generation: number;
    consecutiveFailures: number;
    lastCheckedAt: string | null;
    lastHealthyAt: string | null;
    nextCheckAt: string | null;
    lastFailureReason: string | null;
  } | null;
  checks: HealthCheck[];
  monitoring: MonitoringReport | null;
  automaticRecovery?: AutomaticRecovery;
  recovery: { allowed: boolean; reason: RecoveryBlock | null };
};

export type Recovery = {
  id: string;
  runtimeInstanceId: string;
  generation: number;
  status: "REQUESTED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
  trigger: "MANUAL" | "AUTOMATIC";
  action: "RESTART";
  attemptCount: number;
  failureReason: string | null;
  failureMessage: string | null;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
};

export function isRecoveryActive(attempt?: Recovery | null) {
  return attempt?.status === "REQUESTED" || attempt?.status === "RUNNING";
}

export function recoveryBlockMessage(reason: RecoveryBlock | null | undefined) {
  switch (reason) {
    case "FORBIDDEN": return "Manual recovery requires workspace owner or administrator access.";
    case "DISABLED": return "Manual recovery is disabled.";
    case "SCHEMA_UNAVAILABLE": return "Recovery is unavailable because the recovery schema is missing.";
    case "RUNTIME_NOT_RUNNING": return "Recovery is available only for a running runtime.";
    case "NOT_UNHEALTHY": return "The backend has not identified an eligible unhealthy runtime.";
    case "ACTIVE": return "A recovery is already in progress.";
    case "COOLDOWN": return "The deployment is in its recovery cooldown. Check again in a moment.";
    case "EXHAUSTED": return "The deployment has exhausted its recovery budget.";
    default: return "Recovery availability could not be confirmed.";
  }
}

const failureMessages: Record<string, string> = {
  RECOVERY_STALE: "The runtime generation changed before this recovery could finish.",
  RECOVERY_IDENTITY_MISMATCH: "The runtime container or image identity could not be verified.",
  RECOVERY_PORT_CHANGED: "The original loopback port could not be verified. Public routing needs repair.",
  RECOVERY_DOCKER_UNAVAILABLE: "Docker was unavailable during recovery.",
  RECOVERY_TIMEOUT: "Recovery exceeded its time limit.",
  RECOVERY_RESTART_UNCERTAIN: "The worker could not verify a restart. Check the runtime before requesting another recovery.",
  RECOVERY_EXHAUSTED: "The deployment has exhausted its recovery budget.",
  RECOVERY_EXECUTION_FAILED: "The recovery operation did not finish successfully.",
  CONTAINER_UNAVAILABLE: "The container could not be inspected.",
  CONTAINER_NOT_RUNNING: "The container was not running.",
  CONTAINER_IDENTITY_MISMATCH: "The inspected container did not match this deployment.",
  HTTP_TIMEOUT: "The HTTP probe timed out.",
  HTTP_UNREACHABLE: "The HTTP probe could not reach the runtime.",
  HTTP_CONNECTION_REFUSED: "The runtime refused the HTTP connection.",
  HTTP_REDIRECT: "The runtime returned an HTTP redirect.",
  HTTP_INVALID_RESPONSE: "The runtime returned an invalid or oversized HTTP response.",
  HTTP_STATUS: "The application returned a non-success HTTP status.",
};

export function failureMessage(code: string) {
  return Object.hasOwn(failureMessages, code) ? failureMessages[code] : "The backend reported a failure.";
}

export function checkOutcome(check: HealthCheck): ProbeResult {
  if (check.failureReason || check.httpResult === "FAILED" || check.dockerResult === "FAILED") return "FAILED";
  return check.httpResult === "PASSED" && check.dockerResult === "PASSED" ? "PASSED" : "SKIPPED";
}

const displayTime = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" });

export function formatHealthTime(value: string | number | null | undefined) {
  const time = typeof value === "number" ? value : value ? Date.parse(value) : NaN;
  return Number.isFinite(time) ? displayTime.format(time) : "Unavailable";
}

/**
 * Format the latency chart tooltip label from a recharts
 * payload row. The chart's x-axis value is a numeric
 * timestamp, but recharts passes the series config label
 * (e.g. "Passed") to labelFormatter, which previously
 * produced a misleading "Unavailable" header. Reading the
 * timestamp from the payload row restores the correct time
 * whenever valid health-check data exists.
 */
export function formatChartTooltipTime(
  payload: ReadonlyArray<{ payload?: { time?: unknown } }> | undefined,
): string {
  const time = payload?.[0]?.payload?.time;
  return typeof time === "number" ? formatHealthTime(time) : "Unavailable";
}

export function monitoringMessage(report: MonitoringReport | null | undefined) {
  switch (report?.status) {
    case "ENABLED":
      return "Health monitoring is active on a connected worker.";
    case "DISABLED":
      return "A connected worker is not performing health monitoring (monitoring is disabled).";
    case "STALE":
      return "Health monitoring may be stale: no connected worker has a recent heartbeat.";
    case "UNKNOWN":
      return "Health monitoring status is unknown: the connected worker does not report monitoring capability.";
    case "UNAVAILABLE":
    default:
      return "Health monitoring availability could not be confirmed from worker heartbeats.";
  }
}

export function formatRecoveryDuration(start: string | null, finish: string | null) {
  if (!start || !finish) return "Unavailable";
  const ms = Date.parse(finish) - Date.parse(start);
  if (!Number.isFinite(ms) || ms < 0) return "Unavailable";
  if (ms < 1000) return `${ms} ms`;
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  return minutes === 0 ? `${(ms / 1000).toFixed(1)} s` : `${minutes} m ${seconds % 60} s`;
}

export function recoverySubmissionError(error: unknown) {
  if (!(error instanceof ApiError)) {
    return { ambiguous: true, message: "The request outcome is unknown. Retry the pending request to reuse its idempotency key." };
  }
  switch (error.code) {
    case "RECOVERY_DISABLED": return { ambiguous: false, message: recoveryBlockMessage("DISABLED") };
    case "RECOVERY_COOLDOWN": return { ambiguous: false, message: recoveryBlockMessage("COOLDOWN") };
    case "RECOVERY_EXHAUSTED": return { ambiguous: false, message: recoveryBlockMessage("EXHAUSTED") };
    case "RECOVERY_BUSY": return { ambiguous: false, message: recoveryBlockMessage("ACTIVE") };
    case "RECOVERY_INELIGIBLE": return { ambiguous: false, message: "The runtime is no longer eligible. Refresh health and try again." };
  }
  if (error.status === 401 || error.status === 403) return { ambiguous: false, message: "Your session or permissions do not allow this recovery." };
  if (error.status === 429) return { ambiguous: false, message: "Recovery requests are temporarily rate limited. Try again later." };
  if (error.status >= 500 || error.status === 408) {
    return { ambiguous: true, message: "The request outcome is unknown. Retry the pending request to reuse its idempotency key." };
  }
  return { ambiguous: false, message: "Recovery could not be requested. Refresh this deployment and try again." };
}


export const automaticWorkerLabels: Record<AutomaticRecovery["workerAvailability"], string> = {
  AVAILABLE: "Available",
  DISABLED: "Disabled",
  UNAVAILABLE: "Unavailable",
  STALE: "Stale heartbeat",
  UNKNOWN: "Capability unknown",
  CONFLICTING: "Conflicting worker capabilities",
};

const automaticRejections: Record<string, string> = {
  AUTO_DISABLED: "Automatic recovery is disabled.",
  WORKER_UNAVAILABLE: "No available worker confirms automatic recovery.",
  WORKER_STALE: "Automatic recovery worker heartbeats are stale.",
  WORKER_UNKNOWN: "The workers have not reported their automatic recovery capability.",
  WORKER_CONFLICTING: "Workers report different automatic recovery capabilities.",
  SCHEMA_UNAVAILABLE: "The recovery schema is unavailable.",
  CIRCUIT_UNAVAILABLE: "The recovery circuit could not be read.",
  INFRASTRUCTURE_UNAVAILABLE: "Recovery infrastructure is unavailable.",
  DEPLOYMENT_NOT_RUNNING: "The deployment is not running.",
  RUNTIME_NOT_RUNNING: "No running runtime is available to recover.",
  RUNTIME_MISSING: "This deployment has no runtime.",
  RUNTIME_INELIGIBLE: "The runtime is ineligible for trusted local recovery.",
  HEALTH_NOT_UNHEALTHY: "Recovery requires an unhealthy runtime.",
  HEALTH_UNAVAILABLE: "No current runtime health observation is available.",
  HEALTH_STATE_MISSING: "No current runtime health observation is available.",
  INSUFFICIENT_FAILURES: "Too few consecutive health checks have failed.",
  STALE_HEALTH: "The latest health check is stale.",
  ACTIVE_RECOVERY: "A recovery attempt is already active.",
  CONFLICTING_OPERATION: "The runtime is busy with another operation.",
  RESTART_UNCERTAIN: "A prior restart could not be verified.",
  CIRCUIT_OPEN: "The circuit is open and automatic recovery is blocked.",
  HALF_OPEN_IN_PROGRESS: "A half-open recovery trial is already active.",
  LEGACY_TRIAL_UNCLASSIFIED: "A prior recovery trial could not be classified.",
  BUDGET_EXHAUSTED: "The shared restart budget is exhausted.",
  COOLDOWN: "The shared recovery cooldown is active.",
};

export function automaticRejectionMessage(reason: string | null) {
  return reason && Object.hasOwn(automaticRejections, reason)
    ? automaticRejections[reason]
    : "Automatic recovery eligibility could not be confirmed.";
}

export function recoveryCooldownSeconds(remainingMs: number | null, elapsedMs: number) {
  return remainingMs === null ? null : Math.max(0, Math.ceil((remainingMs - Math.max(0, elapsedMs)) / 1000));
}
