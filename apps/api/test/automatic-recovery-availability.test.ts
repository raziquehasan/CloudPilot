import assert from "node:assert/strict";
import test from "node:test";
import type { WorkerHeartbeat } from "@cloudpilot/queue";
import { deriveAutomaticRecoveryAvailability } from "../src/services/automatic-recovery-availability.ts";

const now = Date.parse("2026-11-28T12:00:00Z");
const repo = "owner/repo";
function worker(configured: boolean, available = configured): WorkerHeartbeat {
  return {
    workerId: "worker", role: "deployment", version: "0.1.0",
    available: true, draining: false, concurrency: 1,
    startupAt: new Date(now - 10_000).toISOString(), lastHeartbeatAt: new Date(now).toISOString(),
    capabilities: {
      diagnostic: false, deploymentQueue: true, sourceRetrieval: true, workspaceStorage: true,
      dockerBuild: true, containerExecution: true, containerRuntime: true,
      healthMonitoring: true, automaticRecovery: { configured, available, trustedRepositories: [repo] },
    },
  };
}
const availability = (workers: WorkerHeartbeat[], reachable = true) =>
  deriveAutomaticRecoveryAvailability({ workers, now, staleAfterMs: 20_000, reachable }, repo);

test("automatic recovery availability uses fresh worker configuration and repository scope", () => {
  assert.equal(availability([worker(false)]), "DISABLED");
  assert.equal(availability([worker(true)]), "AVAILABLE");
  assert.equal(availability([worker(true, false)]), "UNAVAILABLE");
  const other = worker(true);
  other.capabilities.automaticRecovery!.trustedRepositories = ["different/repo"];
  assert.equal(availability([other]), "UNAVAILABLE");
  assert.equal(availability([worker(true), other]), "CONFLICTING");
});

test("automatic recovery availability distinguishes stale, missing and conflicting capabilities", () => {
  const legacy = worker(true);
  delete legacy.capabilities.automaticRecovery;
  assert.equal(availability([legacy]), "UNKNOWN");
  assert.equal(availability([worker(true), legacy]), "UNKNOWN");
  const stale = { ...worker(true), lastHeartbeatAt: new Date(now - 30_000).toISOString() };
  assert.equal(availability([stale]), "STALE");
  const missed = { ...worker(true), lastHeartbeatAt: new Date(now - 16_000).toISOString() };
  missed.capabilities.automaticRecovery!.heartbeatIntervalMs = 5000;
  assert.equal(availability([missed]), "STALE");
  assert.equal(availability([{ ...worker(true), draining: true }]), "UNAVAILABLE");
  assert.equal(availability([worker(true)], false), "UNAVAILABLE");
  assert.equal(availability([]), "UNAVAILABLE");
  assert.equal(availability([worker(true), worker(false)]), "CONFLICTING");
  assert.equal(availability([worker(true), worker(true, false)]), "CONFLICTING");
});
