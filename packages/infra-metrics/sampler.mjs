import { Counter, Gauge, createMetricsRegistry } from "@cloudpilot/metrics";

export function collectPeriodically({ registry, backend, prefix, types, slotted = false, read, intervalMs = 10_000 }) {
  const metricOptions = { registers: [registry] };
  const status = new Gauge({ ...metricOptions, name: "cloudpilot_infra_backend_up", help: "Underlying source was successfully collected" });
  const updated = new Gauge({
    ...metricOptions, name: "cloudpilot_infra_last_success_timestamp_seconds",
    help: "Unix timestamp of last successful infrastructure collection", labelNames: ["backend"],
  });
  const duration = new Gauge({ ...metricOptions, name: "cloudpilot_infra_collection_duration_seconds", help: "Duration of the last infrastructure collection", labelNames: ["backend"] });
  const families = Object.entries(types).map(([key, kind]) => {
    const Constructor = kind === "counter" ? Counter : Gauge;
    return [key, kind, new Constructor({
      ...metricOptions, name: `${prefix}_${key}`, help: `${backend} ${key.replaceAll("_", " ")}`,
      labelNames: slotted ? ["slot"] : ["backend"],
    })];
  });
  // Store only a complete batch. Retain the last valid samples on failure with a
  // failed source status and unchanged freshness. An empty row removes old slot
  // series instead of extrapolating utilization after a stop or replacement.
  let inFlight;
  let timer;
  let stopped = false;
  function sample() {
    if (inFlight || stopped) return inFlight ?? Promise.resolve();
    const start = performance.now();
    const work = Promise.resolve().then(read).then((rows) => {
      if (stopped) return;
      for (const [key, kind, metric] of families) {
        metric.reset();
        for (const { slot, values } of rows) {
          const value = values[key];
          if (typeof value !== "number" || !Number.isFinite(value) || value < 0) continue;
          const labels = slotted ? { slot } : { backend };
          if (kind === "counter") {
            // Publish the cumulative value supplied by the infrastructure. A
            // lower source value is preserved as a counter reset for rate().
            if (!slotted) metric.reset();
            metric.inc(labels, value);
          } else metric.set(labels, value);
        }
      }
      updated.set({ backend }, Date.now() / 1000);
      status.set(1);
    }).catch(() => { if (!stopped) status.set(0); }).finally(() => {
      if (!stopped) duration.set({ backend }, (performance.now() - start) / 1000);
      inFlight = undefined;
    });
    inFlight = work;
    return work;
  }
  return {
    registry, sample,
    start() {
      if (timer || stopped) return;
      void sample();
      timer = setInterval(() => void sample(), intervalMs);
      timer.unref();
    },
    async stop() {
      stopped = true;
      clearInterval(timer);
      await inFlight;
    },
  };
}

export const infrastructureRegistry = () => createMetricsRegistry(false);
