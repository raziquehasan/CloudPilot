import { setTimeout as delay } from "node:timers/promises";
import type { HealthScope } from "@cloudpilot/database";

export interface HealthMonitorOptions {
  enabled: boolean;
  concurrency: number;
  batchSize: number;
  signal?: AbortSignal;
  ready: () => Promise<boolean>;
  discover: (limit: number) => Promise<HealthScope[]>;
  check: (scope: HealthScope, signal: AbortSignal) => Promise<void>;
  retain?: () => Promise<number>;
  emit: (event: "started" | "stopped" | "infrastructure_error" | "retained", details?: Record<string, number | string>) => void;
  pollMs?: number;
}

export function startHealthMonitor(options: HealthMonitorOptions) {
  if (!options.enabled || options.signal?.aborted) return null;
  const { concurrency, batchSize } = options;
  const pollMs = options.pollMs ?? 1_000;
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 16 ||
    !Number.isInteger(batchSize) || batchSize < 1 || batchSize > 128 ||
    !Number.isInteger(pollMs) || pollMs < 1 || pollMs > 30_000) {
    throw new RangeError("Invalid health monitor limits");
  }
  const controller = new AbortController();
  const signal = options.signal ? AbortSignal.any([options.signal, controller.signal]) : controller.signal;

  async function monitor() {
    let failures = 0;
    let nextRetentionAt = 0;
    options.emit("started", { concurrency, batchSize });
    try {
      while (!signal.aborted) {
        let operation = "schema readiness";
        try {
          if (!await options.ready()) throw new Error("Health persistence is unavailable");
          if (signal.aborted) break;
          operation = "due-check discovery";
          const scopes = (await options.discover(batchSize)).slice(0, batchSize);
          operation = "health check";
          for (let offset = 0; offset < scopes.length && !signal.aborted; offset += concurrency) {
            const results = await Promise.allSettled(scopes.slice(offset, offset + concurrency).map(
              async (scope) => { if (!signal.aborted) await options.check(scope, signal); },
            ));
            if (results.some((result) => result.status === "rejected")) throw new Error("Health checks failed");
          }
          if (options.retain && !signal.aborted && performance.now() >= nextRetentionAt) {
            operation = "history retention";
            const deleted = await options.retain();
            nextRetentionAt = performance.now() + 60_000;
            if (deleted > 0) options.emit("retained", { deleted });
          }
          failures = 0;
        } catch {
          if (signal.aborted) break;
          failures = Math.min(failures + 1, 10);
          options.emit("infrastructure_error", { operation, consecutiveFailures: failures });
        }
        if (!signal.aborted) {
          await delay(Math.min(30_000, pollMs * 2 ** failures), undefined, { signal }).catch((error: unknown) => {
            if (!signal.aborted) throw error;
          });
        }
      }
    } finally {
      options.emit("stopped");
    }
  }

  const done = monitor();
  return { done, stop: () => { controller.abort(); return done; } };
}
