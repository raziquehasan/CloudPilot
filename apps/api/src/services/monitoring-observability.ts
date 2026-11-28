// ==================================================
// Monitoring observability (Phase 5.7.8)
// ==================================================
//
// Reads live worker heartbeats and derives an accurate
// health-monitoring availability report. The pure
// derivation lives in monitoring-report.ts so it can be
// unit tested without side effects.
// ==================================================

import {
  listWorkerHeartbeats,
  type WorkerHeartbeat,
} from "@cloudpilot/queue";

import { queueEnv, redisProducerConnection } from "./queue-producer.js";
import {
  deriveMonitoringReport,
  type MonitoringReport,
} from "./monitoring-report.js";

export type { MonitoringReport, MonitoringStatus } from "./monitoring-report.js";
export { deriveMonitoringReport } from "./monitoring-report.js";

/**
 * Read the current monitoring report from live worker
 * heartbeats. Never throws: an unreachable Redis or a
 * registry error yields an UNAVAILABLE report.
 */
export async function readMonitoringReport(): Promise<MonitoringReport> {
  const staleAfterMs = queueEnv.WORKER_HEARTBEAT_TTL_SECONDS * 1000;

  let workers: WorkerHeartbeat[] = [];

  let reachable = false;

  try {
    workers = await listWorkerHeartbeats(
      redisProducerConnection,
      {
        prefix: queueEnv.QUEUE_PREFIX,
        ttlSeconds: queueEnv.WORKER_HEARTBEAT_TTL_SECONDS,
      },
      "deployment",
    );

    reachable = true;
  } catch {
    reachable = false;
  }

  return deriveMonitoringReport({
    workers,
    now: Date.now(),
    staleAfterMs,
    reachable,
  });
}