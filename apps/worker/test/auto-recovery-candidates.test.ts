import assert from "node:assert/strict";
import test from "node:test";
import {
  evaluateCandidateRows,
  type AutoRecoveryCandidateRow,
  type EvaluationOptions,
} from "../src/health/auto-recovery-candidates.ts";

const NOW = 1_700_000_000_000;

// Default evaluation options that enable every gate so that per-row
// rejection reasons can be exercised in isolation.
const ON: EvaluationOptions = {
  autoRecoveryEnabled: true,
  persistenceEnabled: true,
  monitoringAvailable: true,
  infrastructureReady: true,
  recoveryLimit: 3,
};

function row(overrides: Partial<AutoRecoveryCandidateRow> = {}): AutoRecoveryCandidateRow {
  return {
    workspaceId: "ws",
    projectId: "pr",
    deploymentId: "dp",
    runtimeInstanceId: "rt",
    generation: 1,
    version: 0,
    deploymentStatus: "RUNNING",
    runtimeStatus: "RUNNING",
    runtimeEligible: true,
    healthStatus: "UNHEALTHY",
    consecutiveFailures: 3,
    lastCheckedAt: new Date(NOW - 1_000),
    generationMatches: true,
    restartUncertain: false,
    activeRecovery: false,
    conflictingOperation: false,
    attemptsInWindow: 0,
    lastFailedFinishedAt: null,
    recentFailureCount: 0,
    now: new Date(NOW),
    ...overrides,
  };
}

function reason(r: AutoRecoveryCandidateRow, options: EvaluationOptions = ON) {
  const { results } = evaluateCandidateRows([r], options);
  const decision = results[0]!.decision;
  return decision.eligible ? "ELIGIBLE" : decision.reason;
}

test("eligible when every safeguard passes", () => {
  assert.equal(reason(row()), "ELIGIBLE");
});

test("auto disabled rejects all rows", () => {
  assert.equal(reason(row(), { ...ON, autoRecoveryEnabled: false }), "AUTO_DISABLED");
});

test("infrastructure unavailable rejects", () => {
  assert.equal(reason(row(), { ...ON, infrastructureReady: false }), "INFRASTRUCTURE_UNAVAILABLE");
});

test("persistence unavailable rejects", () => {
  assert.equal(reason(row(), { ...ON, persistenceEnabled: false }), "PERSISTENCE_UNAVAILABLE");
});

test("monitoring unavailable rejects", () => {
  assert.equal(reason(row(), { ...ON, monitoringAvailable: false }), "MONITORING_UNAVAILABLE");
});

test("deployment not running rejects", () => {
  assert.equal(reason(row({ deploymentStatus: "STOPPED" })), "DEPLOYMENT_NOT_RUNNING");
});

test("runtime not running rejects", () => {
  assert.equal(reason(row({ runtimeStatus: "STOPPED" })), "RUNTIME_NOT_RUNNING");
});

test("runtime ineligible rejects", () => {
  assert.equal(reason(row({ runtimeEligible: false })), "RUNTIME_INELIGIBLE");
});

test("healthy runtime rejects", () => {
  assert.equal(reason(row({ healthStatus: "HEALTHY" })), "HEALTH_NOT_UNHEALTHY");
});

test("insufficient failures rejects", () => {
  assert.equal(reason(row({ consecutiveFailures: 2 })), "INSUFFICIENT_FAILURES");
});

test("stale health observation rejects", () => {
  assert.equal(reason(row({ lastCheckedAt: new Date(NOW - 60_000) })), "STALE_HEALTH");
});

test("null lastCheckedAt rejects as stale", () => {
  assert.equal(reason(row({ lastCheckedAt: null })), "STALE_HEALTH");
});

test("generation mismatch rejects", () => {
  assert.equal(reason(row({ generationMatches: false })), "GENERATION_MISMATCH");
});

test("restart uncertain rejects", () => {
  assert.equal(reason(row({ restartUncertain: true })), "RESTART_UNCERTAIN");
});

test("active recovery rejects", () => {
  assert.equal(reason(row({ activeRecovery: true })), "ACTIVE_RECOVERY");
});

test("conflicting operation rejects", () => {
  assert.equal(reason(row({ conflictingOperation: true })), "CONFLICTING_OPERATION");
});

test("budget exhausted rejects", () => {
  assert.equal(reason(row({ attemptsInWindow: 3 })), "BUDGET_EXHAUSTED");
});

test("cooldown rejects while cooling down", () => {
  assert.equal(reason(row({ lastFailedFinishedAt: new Date(NOW - 1_000), recentFailureCount: 1 })), "COOLDOWN");
});

test("cooldown elapsed permits eligibility", () => {
  assert.equal(
    reason(row({ lastFailedFinishedAt: new Date(NOW - 60_000), recentFailureCount: 1 })),
    "ELIGIBLE",
  );
});

test("empty input yields a zeroed batch", () => {
  const { results, batch } = evaluateCandidateRows([], ON);
  assert.equal(results.length, 0);
  assert.deepEqual(batch, { evaluated: 0, eligible: 0, rejected: 0, reasons: {} });
});

test("batch aggregates eligible and rejected counts", () => {
  const { batch } = evaluateCandidateRows(
    [row(), row(), row({ healthStatus: "HEALTHY" }), row({ consecutiveFailures: 1 })],
    ON,
  );
  assert.equal(batch.evaluated, 4);
  assert.equal(batch.eligible, 2);
  assert.equal(batch.rejected, 2);
  assert.deepEqual(batch.reasons, { HEALTH_NOT_UNHEALTHY: 1, INSUFFICIENT_FAILURES: 1 });
});
