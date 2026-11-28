import { listWorkerHeartbeats } from "@cloudpilot/queue";
import { queueEnv, redisProducerConnection } from "./queue-producer.js";
import type { DeriveMonitoringInput } from "./monitoring-report.js";

async function fetchHeartbeats(): Promise<DeriveMonitoringInput> {
  const staleAfterMs = queueEnv.WORKER_HEARTBEAT_TTL_SECONDS * 1000;
  try {
    const workers = await listWorkerHeartbeats(redisProducerConnection, {
      prefix: queueEnv.QUEUE_PREFIX,
      ttlSeconds: queueEnv.WORKER_HEARTBEAT_TTL_SECONDS,
    }, "deployment", { cleanupExpired: false });
    return { workers, now: Date.now(), staleAfterMs, reachable: true };
  } catch {
    return { workers: [], now: Date.now(), staleAfterMs, reachable: false };
  }
}

let pending: Promise<DeriveMonitoringInput> | undefined;

export async function readMonitoringHeartbeats(): Promise<DeriveMonitoringInput> {
  if (!pending) pending = fetchHeartbeats().finally(() => { pending = undefined; });
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      pending,
      new Promise<DeriveMonitoringInput>((resolve) => {
        timer = setTimeout(() => resolve({
          workers: [], now: Date.now(), staleAfterMs: 0, reachable: false,
        }), 1500);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
export { deriveMonitoringReport } from "./monitoring-report.js";
