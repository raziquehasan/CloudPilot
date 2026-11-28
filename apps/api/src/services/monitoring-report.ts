// ==================================================
// Monitoring report derivation (Phase 5.7.8)
// ==================================================
//
// Pure, side-effect-free derivation of an accurate
// health-monitoring availability report from
// worker-reported heartbeats.
//
// Availability is established from the workers' own
// reported capability and heartbeat freshness. It must
// NOT be inferred from API environment variables or from
// scheduled check timestamps (nextCheckAt), which only
// reflect the database schedule, not whether a worker is
// actively monitoring.
// ==================================================

import type { WorkerHeartbeat } from "@cloudpilot/queue";

export type MonitoringStatus =
  | "ENABLED"
  | "DISABLED"
  | "UNAVAILABLE"
  | "STALE"
  | "UNKNOWN";

export interface MonitoringReport {
  status: MonitoringStatus;
  workerCount: number;
  reportingCount: number;
  availableCount: number;
  enabledCount: number;
  lastHeartbeatAt: string | null;
}

export interface DeriveMonitoringInput {
  workers: WorkerHeartbeat[];
  now: number;
  staleAfterMs: number;
  reachable: boolean;
}

/**
 * Derive the monitoring status from worker heartbeats.
 *
 * States:
 * - UNAVAILABLE: Redis unreachable or no deployment workers.
 * - UNKNOWN: workers exist but none report the capability
 *   (backward-compatible older workers).
 * - STALE: workers exist and report, but none are fresh and
 *   available (all draining or heartbeat too old).
 * - ENABLED: at least one fresh, available worker reports
 *   monitoring active.
 * - DISABLED: fresh, available workers exist but all report
 *   monitoring inactive.
 */
export function deriveMonitoringReport(
  input: DeriveMonitoringInput,
): MonitoringReport {
  const { workers, now, staleAfterMs, reachable } = input;

  const reporting = workers.filter(
    (worker) => typeof worker.capabilities.healthMonitoring === "boolean",
  );

  const isFresh = (worker: WorkerHeartbeat): boolean => {
    const last = Date.parse(worker.lastHeartbeatAt);
    const age = now - last;
    return (
      Number.isFinite(last) &&
      age <= staleAfterMs &&
      age >= -staleAfterMs
    );
  };

  const available = workers.filter(
    (worker) =>
      worker.available && !worker.draining && isFresh(worker),
  );

  const enabled = available.filter(
    (worker) => worker.capabilities.healthMonitoring === true,
  );

  const lastHeartbeatAt =
    workers
      .map((worker) => worker.lastHeartbeatAt)
      .filter((value): value is string => typeof value === "string")
      .sort()
      .at(-1) ?? null;

  const base = {
    workerCount: workers.length,
    reportingCount: reporting.length,
    availableCount: available.length,
    enabledCount: enabled.length,
    lastHeartbeatAt,
  };

  if (!reachable || workers.length === 0) {
    return { status: "UNAVAILABLE", ...base };
  }

  if (reporting.length === 0) {
    return { status: "UNKNOWN", ...base };
  }

  if (available.length === 0) {
    return { status: "STALE", ...base };
  }

  if (enabled.length > 0) {
    return { status: "ENABLED", ...base };
  }

  return { status: "DISABLED", ...base };
}