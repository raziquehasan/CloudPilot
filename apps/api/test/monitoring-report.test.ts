import assert from "node:assert/strict";
import test from "node:test";

import { deriveMonitoringReport } from "../src/services/monitoring-report.ts";
import type { WorkerHeartbeat } from "@cloudpilot/queue";

const NOW = Date.parse("2026-11-28T12:00:00Z");
const FRESH = new Date(NOW - 1_000).toISOString();
const STALE = new Date(NOW - 60_000).toISOString();
const STALE_AFTER_MS = 20_000;

function heartbeat(overrides: Partial<WorkerHeartbeat> = {}): WorkerHeartbeat {
  return {
    workerId: "worker-1",
    role: "deployment",
    version: "0.1.0",
    capabilities: {
      diagnostic: false,
      deploymentQueue: true,
      sourceRetrieval: true,
      workspaceStorage: true,
      dockerBuild: true,
      containerExecution: true,
      containerRuntime: true,
      healthMonitoring: true,
    },
    startupAt: new Date(NOW - 120_000).toISOString(),
    lastHeartbeatAt: FRESH,
    available: true,
    draining: false,
    concurrency: 1,
    ...overrides,
  };
}

function report(workers: WorkerHeartbeat[], reachable = true) {
  return deriveMonitoringReport({ workers, now: NOW, staleAfterMs: STALE_AFTER_MS, reachable });
}

test("no workers or unreachable Redis is UNAVAILABLE", () => {
  assert.equal(report([]).status, "UNAVAILABLE");
  assert.equal(report([heartbeat()], false).status, "UNAVAILABLE");
});

test("workers that do not report the capability are UNKNOWN (backward compatible)", () => {
  const legacy = heartbeat();
  delete (legacy.capabilities as { healthMonitoring?: boolean }).healthMonitoring;
  assert.equal(report([legacy]).status, "UNKNOWN");
});

test("fresh available worker reporting monitoring is ENABLED", () => {
  const result = report([heartbeat()]);
  assert.equal(result.status, "ENABLED");
  assert.equal(result.enabledCount, 1);
  assert.equal(result.availableCount, 1);
  assert.equal(result.reportingCount, 1);
});

test("fresh available worker reporting disabled is DISABLED", () => {
  const result = report([heartbeat({ capabilities: { ...heartbeat().capabilities, healthMonitoring: false } })]);
  assert.equal(result.status, "DISABLED");
  assert.equal(result.enabledCount, 0);
});

test("stale heartbeat (too old) is STALE", () => {
  const result = report([heartbeat({ lastHeartbeatAt: STALE })]);
  assert.equal(result.status, "STALE");
  assert.equal(result.availableCount, 0);
});

test("draining worker is STALE", () => {
  const result = report([heartbeat({ draining: true })]);
  assert.equal(result.status, "STALE");
});

test("multiple workers: one enabled among many is ENABLED", () => {
  const workers = [
    heartbeat({ workerId: "a", capabilities: { ...heartbeat().capabilities, healthMonitoring: false } }),
    heartbeat({ workerId: "b", lastHeartbeatAt: STALE }),
    heartbeat({ workerId: "c" }),
  ];
  const result = report(workers);
  assert.equal(result.status, "ENABLED");
  assert.equal(result.workerCount, 3);
  assert.equal(result.enabledCount, 1);
});

test("lastHeartbeatAt is the most recent across workers", () => {
  const result = report([
    heartbeat({ workerId: "a", lastHeartbeatAt: new Date(NOW - 5_000).toISOString() }),
    heartbeat({ workerId: "b", lastHeartbeatAt: FRESH }),
  ]);
  assert.equal(result.lastHeartbeatAt, FRESH);
});