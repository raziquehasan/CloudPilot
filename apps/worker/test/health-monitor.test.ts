import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { startHealthMonitor, type HealthMonitorOptions } from "../src/health/health-monitor.js";

const scope = { workspaceId: "workspace", projectId: "project", deploymentId: "deployment", runtimeInstanceId: "runtime", generation: 0 };
const defaults: HealthMonitorOptions = {
  enabled: true, concurrency: 2, batchSize: 4, pollMs: 2,
  ready: async () => true, discover: async () => [], check: async () => {}, emit: () => {},
};

test("disabled monitoring performs no readiness check, discovery, or retention", async () => {
  let calls = 0;
  assert.equal(startHealthMonitor({
    ...defaults, enabled: false,
    ready: async () => { calls++; return true; },
    discover: async () => { calls++; return []; },
    retain: async () => { calls++; return 0; },
    emit: () => { calls++; },
  }), null);
  await delay(10);
  assert.equal(calls, 0);
});

test("scheduler caps concurrency and batch size and waits for cancelled probes on shutdown", async () => {
  const abort = new AbortController();
  let active = 0;
  let maximum = 0;
  let started = 0;
  let scans = 0;
  const scheduler = startHealthMonitor({
    ...defaults, signal: abort.signal,
    discover: async (limit) => {
      assert.equal(limit, 4);
      assert.equal(active, 0);
      scans++;
      return Array.from({ length: 8 }, (_, generation) => ({ ...scope, generation }));
    },
    check: async (_scope, signal) => {
      active++;
      started++;
      maximum = Math.max(maximum, active);
      try {
        if (scans > 1 && active === 2) abort.abort();
        await delay(10, undefined, { signal });
      } catch (error) {
        assert.ok(signal.aborted, String(error));
      } finally {
        active--;
      }
    },
  });
  assert.ok(scheduler);
  await scheduler.done;
  assert.deepEqual({ active, maximum, started, scans }, { active: 0, maximum: 2, started: 6, scans: 2 });
});

test("database outage backs off before retrying, then resumes checks and retention", async () => {
  const abort = new AbortController();
  const attempts: number[] = [];
  const failureCounts: number[] = [];
  let checks = 0;
  let retention = 0;
  const scheduler = startHealthMonitor({
    ...defaults, pollMs: 10, signal: abort.signal,
    discover: async () => {
      attempts.push(performance.now());
      if (attempts.length <= 2) throw new Error("private database error");
      return [scope];
    },
    check: async () => { checks++; },
    retain: async () => { retention++; abort.abort(); return 0; },
    emit(event, detail) {
      if (event === "infrastructure_error") failureCounts.push(Number(detail?.consecutiveFailures));
    },
  });
  assert.ok(scheduler);
  await scheduler.done;
  assert.deepEqual(failureCounts, [1, 2]);
  assert.ok(attempts[1]! - attempts[0]! >= 18);
  assert.ok(attempts[2]! - attempts[1]! >= 38);
  assert.equal(checks, 1);
  assert.equal(retention, 1);
});
