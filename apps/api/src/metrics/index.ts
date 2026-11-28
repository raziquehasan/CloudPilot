import { createMetricsRegistry, metricsOptions, openMetrics } from "@cloudpilot/metrics";
import { logger } from "../config/logger.js";
import { queueManager } from "../services/queue-producer.js";
import { httpMetrics } from "./http.js";
import { createQueueCollector, queueStates } from "./queues.js";

export const apiMetrics = (() => {
  try {
    const options = metricsOptions(process.env, "API");
    if (!options) return null;
    const registry = createMetricsRegistry();
    const requests = httpMetrics(registry);
    const queues = createQueueCollector(registry, (queue) => queueManager.getQueue(queue).getJobCounts(...queueStates));
    return {
      requests,
      start() {
        const close = openMetrics(options, registry, () => logger.error("API metrics listener unavailable"));
        queues.start();
        return async () => { queues.stop(); await close(); };
      },
    };
  } catch {
    logger.error("API metrics initialization failed. Check metrics configuration and credential file.");
    return null;
  }
})();
