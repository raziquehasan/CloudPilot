import assert from "node:assert/strict";
import test from "node:test";
import { evaluateAutoRecovery, type AutoRecoveryInput } from "../src/health/auto-recovery-policy.ts";

const NOW = 1_700_000_000_000;

function base(overrides: Partial<AutoRecoveryInput> = {}): AutoRecoveryInput {
  return {
    autoRecoveryEnabled: true,
    persistenceEnabled: true,
    monitoringAvailable: true,
    infrastructureReady: true,
    deploymentStatus: "RUNNING",
    runtimeStatus: "RUNNING",
    runtimeEligible: true,
    healthStatus: "UNHEALTHY",
    consecutiveFailures: 3,
    lastCheckedAt: NOW - 1_000,
    now: NOW,
    freshnessMs: 30_000,
    generationMatches: true,
    restartUncertain: false,
    activeRecovery: false,
    conflictingOperation: false,
    attemptsInWindow: 0,
    recoveryLimit: 3,
    cooldownRemainingMs: 0,
    ...overrides,
  };
}

function reason(overrides: Partial<AutoRecoveryInput>) {
  const decision = evaluateAutoRecovery(base(overrides));
  assert.equal(decision.eligible, false);
  return decision.eligible === false ? decision.reason : "ELIGIBLE";
}

test("eligible when every safeguard passes", () => {
  assert.deepEqual(evaluateAutoRecovery(base()), { eligible: true });
});

test("feature flag disabled rejects", () => {
  assert.equal(reason({ autoRecoveryEnabled: false }), "AUTO_DISABLED");
});

test("infrastructure/persistence/monitoring unavailability rejects", () => {
  assert.equal(reason({ infrastructureReady: false }), "INFRASTRUCTURE_UNAVAILABLE");
  assert.equal(reason({ persistenceEnabled: false }), "PERSISTENCE_UNAVAILABLE");
  assert.equal(reason({ monitoringAvailable: false }), "MONITORING_UNAVAILABLE");
});

test("deployment/runtime not running or ineligible rejects", () => {
  assert.equal(reason({ deploymentStatus: "STOPPED" }), "DEPLOYMENT_NOT_RUNNING");
  assert.equal(reason({ runtimeStatus: "STOPPED" }), "RUNTIME_NOT_RUNNING");
  assert.equal(reason({ runtimeEligible: false }), "RUNTIME_INELIGIBLE");
});

test("healthy runtime rejects", () => {
  assert.equal(reason({ healthStatus: "HEALTHY" }), "HEALTH_NOT_UNHEALTHY");
});

test("insufficient consecutive failures rejects", () => {
  assert.equal(reason({ consecutiveFailures: 2 }), "INSUFFICIENT_FAILURES");
});

test("stale health observation rejects", () => {
  assert.equal(reason({ lastCheckedAt: NOW - 60_000 }), "STALE_HEALTH");
  assert.equal(reason({ lastCheckedAt: null }), "STALE_HEALTH");
});

test("generation mismatch rejects", () => {
  assert.equal(reason({ generationMatches: false }), "GENERATION_MISMATCH");
});

test("unresolved restart uncertainty rejects", () => {
  assert.equal(reason({ restartUncertain: true }), "RESTART_UNCERTAIN");
});

test("active manual or automatic recovery rejects", () => {
  assert.equal(reason({ activeRecovery: true }), "ACTIVE_RECOVERY");
});

test("conflicting operation rejects", () => {
  assert.equal(reason({ conflictingOperation: true }), "CONFLICTING_OPERATION");
});

test("exhausted budget and cooldown reject", () => {
  assert.equal(reason({ attemptsInWindow: 3 }), "BUDGET_EXHAUSTED");
  assert.equal(reason({ cooldownRemainingMs: 1 }), "COOLDOWN");
});

test("evaluation order is deterministic (flag precedes everything)", () => {
  // Even with every other condition failing, the disabled flag wins.
  const decision = evaluateAutoRecovery(base({
    autoRecoveryEnabled: false, infrastructureReady: false, healthStatus: "HEALTHY",
  }));
  assert.equal(decision.eligible, false);
  assert.equal(decision.eligible === false && decision.reason, "AUTO_DISABLED");
});