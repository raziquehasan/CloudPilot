import { createMetricsRegistry, metricsOptions, openMetrics } from "@cloudpilot/metrics";
import { workerLogger } from "./config/logger.js";
import { createWorkerMetrics } from "./metrics-instruments.js";

export const workerMetrics = (() => {
  try {
    const options = metricsOptions(process.env, "WORKER");
    if (!options) return null;
    const registry = createMetricsRegistry();
    return {
      ...createWorkerMetrics(registry),
      registry,
      start: () => openMetrics(options, registry, () => workerLogger.error("Worker metrics listener unavailable")),
    };
  } catch {
    workerLogger.error("Worker metrics initialization failed. Check metrics configuration and credential file.");
    return null;
  }
})();
