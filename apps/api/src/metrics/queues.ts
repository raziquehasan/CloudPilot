import { Gauge, observe, type Registry } from "@cloudpilot/metrics";
import { QUEUE_NAMES, type QueueName } from "@cloudpilot/queue";

export const queueStates = ["waiting", "active", "delayed", "failed", "completed", "prioritized", "waiting-children"] as const;
const queueNames = Object.values(QUEUE_NAMES);

export function createQueueCollector(
  registry: Registry,
  read: (queue: QueueName) => Promise<Record<string, number>>,
  { intervalMs = 15_000, timeoutMs = 2_000 } = {},
) {
  const jobs = new Gauge({
    name: "cloudpilot_queue_jobs", help: "Retained BullMQ jobs in the last successful collection",
    labelNames: ["queue", "state"], registers: [registry],
  });
  const success = new Gauge({
    name: "cloudpilot_queue_collection_success", help: "Whether the last BullMQ collection succeeded",
    registers: [registry],
  });
  const duration = new Gauge({
    name: "cloudpilot_queue_collection_duration_seconds", help: "Duration of the last BullMQ collection in seconds",
    registers: [registry],
  });
  const timestamp = new Gauge({
    name: "cloudpilot_queue_last_success_timestamp_seconds", help: "Unix timestamp of the last successful BullMQ collection",
    labelNames: ["source"], registers: [registry],
  });
  // Absence means no successful sample, including when every queue is empty.
  let pending: Promise<unknown> | undefined;
  let collecting = false;
  let stopped = false;
  let timer: ReturnType<typeof setInterval> | undefined;
  let deadline: ReturnType<typeof setTimeout> | undefined;

  async function sample() {
    if (stopped || pending || collecting) return;
    collecting = true;
    const start = performance.now();
    const work = Promise.allSettled(queueNames.map((queue) => Promise.resolve().then(() => read(queue))));
    pending = work;
    void work.then(() => { pending = undefined; });
    try {
      const collected = await Promise.race([work, new Promise<null>((resolve) => {
        deadline = setTimeout(() => resolve(null), timeoutMs);
      })]);
      if (stopped) return;
      const rows = collected?.flatMap((result, index) => {
        const queue = queueNames[index]!;
        if (result.status !== "fulfilled") return [];
        return queueStates.map((state) => ({ queue, state, value: result.value[state] }));
      });
      if (!rows || rows.length !== queueNames.length * queueStates.length ||
          rows.some(({ value }) => typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)) {
        observe(() => success.set(0));
        return;
      }
      observe(() => {
        for (const { queue, state, value } of rows) jobs.set({ queue, state }, value!);
        timestamp.set({ source: "bullmq" }, Date.now() / 1000);
        success.set(1);
      });
    } finally {
      collecting = false;
      clearTimeout(deadline);
      if (!stopped) observe(() => duration.set((performance.now() - start) / 1000));
    }
  }

  return {
    sample,
    start() {
      if (timer || stopped) return;
      void sample();
      timer = setInterval(() => { void sample(); }, intervalMs);
      timer.unref();
    },
    stop() {
      stopped = true;
      clearInterval(timer);
      // A timed-out Redis operation is never replaced until all reads settle.
      observe(() => success.set(0));
    },
  };
}
