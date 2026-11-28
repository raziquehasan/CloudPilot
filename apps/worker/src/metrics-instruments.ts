import { Counter, Gauge, Histogram, latencyBuckets, observe, type Registry } from "@cloudpilot/metrics";
import { QUEUE_NAMES, type QueueJob, type QueueName, type QueueManager } from "@cloudpilot/queue";
import type { HealthFailureReason } from "@cloudpilot/database";

const queues = new Set<string>(Object.values(QUEUE_NAMES));
const actions = new Set(["start", "stop", "restart", "recover"]);
const jobOutcomes = new Map([
  ["succeeded", "succeeded"], ["prepared", "succeeded"], ["failed", "domain_failed"],
  ["cancelled", "cancelled"], ["skipped", "skipped"],
]);
const failureReasons = new Set<string>([
  "CONTAINER_UNAVAILABLE", "CONTAINER_NOT_RUNNING", "CONTAINER_IDENTITY_MISMATCH",
  "HTTP_TIMEOUT", "HTTP_UNREACHABLE", "HTTP_STATUS", "HTTP_CONNECTION_REFUSED", "HTTP_INVALID_RESPONSE", "HTTP_REDIRECT",
  "RECOVERY_EXECUTION_FAILED", "RECOVERY_EXHAUSTED", "RECOVERY_STALE", "RECOVERY_IDENTITY_MISMATCH",
  "RECOVERY_PORT_CHANGED", "RECOVERY_DOCKER_UNAVAILABLE", "RECOVERY_TIMEOUT", "RECOVERY_RESTART_UNCERTAIN",
]);

const reason = (value: HealthFailureReason | null) => value === null ? "none" : failureReasons.has(value) ? value : "other";
const safeQueue = (queue: string) => queues.has(queue) ? queue : "other";

const instruments = new WeakMap<Registry, ReturnType<typeof registerWorkerMetrics>>();

export function createWorkerMetrics(registry: Registry) {
  const existing = instruments.get(registry);
  if (existing) return existing;
  const metrics = registerWorkerMetrics(registry);
  instruments.set(registry, metrics);
  return metrics;
}

function registerWorkerMetrics(registry: Registry) {
  const options = { registers: [registry] };
  const buckets = { ...options, buckets: latencyBuckets };
  const jobLabels = ["queue", "job_type", "attempt"] as const;
  const jobTerminal = [...jobLabels, "outcome"] as const;
  const healthLabels = ["outcome", "reason"] as const;
  const recoveryLabels = ["trigger", "action", "outcome", "reason"] as const;
  const totals = new Counter({ ...options, name: "cloudpilot_worker_jobs_total", help: "Worker processor execution attempts", labelNames: jobTerminal });
  const durations = new Histogram({ ...buckets, name: "cloudpilot_worker_job_duration_seconds", help: "Worker processor execution time", labelNames: jobTerminal });
  const inflight = new Gauge({ ...options, name: "cloudpilot_worker_jobs_in_flight", help: "Worker processor executions in flight", labelNames: jobLabels });
  const stalls = new Counter({ ...options, name: "cloudpilot_worker_job_stalls_total", help: "BullMQ stalled events observed by the worker", labelNames: ["queue"] });
  const checks = new Counter({ ...options, name: "cloudpilot_health_checks_total", help: "Durably recorded scheduled runtime health checks", labelNames: healthLabels });
  const checkTimes = new Histogram({ ...buckets, name: "cloudpilot_health_check_duration_seconds", help: "Scheduled check time through durable recording", labelNames: healthLabels });
  const batches = new Counter({ ...options, name: "cloudpilot_health_monitor_batches_total", help: "Scheduler batch observations", labelNames: ["outcome"] });
  const batchTimes = new Histogram({ ...buckets, name: "cloudpilot_health_monitor_batch_duration_seconds", help: "Scheduler batch duration, including backoff on infrastructure failure", labelNames: ["outcome"] });
  const recoveries = new Counter({ ...options, name: "cloudpilot_recovery_attempts_total", help: "Recoveries observed after durable processor settlement", labelNames: recoveryLabels });
  const recoveryTimes = new Histogram({ ...buckets, name: "cloudpilot_recovery_duration_seconds", help: "Claim-to-settlement execution duration", labelNames: recoveryLabels });
  let readinessRegistered = false;
  type Processor = Parameters<QueueManager["createWorker"]>[1];

  return {
    wrap(queue: QueueName, processor: Processor): Processor {
      return async (job, token, signal) => {
        const data: QueueJob = job.data;
        const jobType = data.kind === "runtime" ? (actions.has(data.action) ? data.action : "other") : safeQueue(data.kind);
        const labels = { queue: safeQueue(queue), job_type: jobType, attempt: job.attemptsStarted > 1 ? "subsequent" : "first" };
        const start = performance.now();
        let outcome = "error";
        observe(() => inflight.inc(labels));
        try {
          const result: unknown = await processor(job, token, signal);
          observe(() => {
            const status = typeof result === "object" && result !== null && "status" in result ? result.status : null;
            outcome = typeof status === "string" ? (jobOutcomes.get(status) ?? "other") :
              queue === QUEUE_NAMES.diagnostic ? "succeeded" : "other";
          });
          return result;
        } catch (error) {
          if (signal?.aborted) outcome = "interrupted";
          throw error;
        } finally {
          observe(() => inflight.dec(labels));
          observe(() => totals.inc({ ...labels, outcome }));
          observe(() => durations.observe({ ...labels, outcome }, (performance.now() - start) / 1000));
        }
      };
    },
    stalled(queue: QueueName) { observe(() => stalls.inc({ queue: safeQueue(queue) })); },
    healthCheck(failure: HealthFailureReason | null, seconds: number) {
      const labels = { outcome: failure ? "failed" : "passed", reason: reason(failure) };
      observe(() => checks.inc(labels));
      observe(() => checkTimes.observe(labels, seconds));
    },
    healthBatch(outcome: "completed" | "infrastructure_error", seconds: number) {
      observe(() => batches.inc({ outcome }));
      observe(() => batchTimes.observe({ outcome }, seconds));
    },
    recovery(trigger: "MANUAL" | "AUTOMATIC", status: "SUCCEEDED" | "FAILED", failure: HealthFailureReason | null, seconds: number) {
      const labels = { trigger, action: "RESTART", outcome: status, reason: reason(failure) };
      observe(() => recoveries.inc(labels));
      observe(() => recoveryTimes.observe(labels, seconds));
    },
    readiness(read: () => { available: boolean; monitoring: boolean; automatic: boolean }) {
      if (readinessRegistered) return;
      readinessRegistered = true;
      for (const [name, selector, help] of [
        ["cloudpilot_worker_available", "available", "Deployment queue consumer readiness"],
        ["cloudpilot_worker_health_monitoring_enabled", "monitoring", "Configured health monitor is running"],
        ["cloudpilot_worker_automatic_recovery_enabled", "automatic", "Automatic recovery configured and local workers ready"],
      ] as const) {
        new Gauge({ ...options, name, help, collect() { observe(() => this.set(Number(read()[selector]))); } });
      }
    },
  };
}
