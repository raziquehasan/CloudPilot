import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { evaluateCircuit } from "../src/health/auto-recovery-circuit.js";

function base(overrides: Partial<any> = {}) {
  return {
    consecutiveFailures: 0,
    attemptsInWindow: 0,
    recoveryLimit: 5,
    cooldownRemainingMs: 0,
    restartUncertain: false,
    verifiedRecoverySinceOpen: false,
    halfOpenInProgress: false,
    failedHalfOpenTrial: false,
    failureThreshold: 3,
    failedAttemptsInWindow: 0,
    now: 1000,
    ...overrides,
  };
}

describe("auto-recovery-circuit", () => {
  it("CLOSED when below threshold", () => {
    const r = evaluateCircuit(base({ consecutiveFailures: 2 }));
    assert.equal(r.state, "CLOSED");
    assert.equal(r.reason, null);
  });
  it("opens after threshold", () => {
    const r = evaluateCircuit(base({ consecutiveFailures: 3 }));
    assert.equal(r.state, "HALF_OPEN");
  });
  it("OPEN blocks when half-open in progress already? no, distinct", () => {
    const r = evaluateCircuit(base({ consecutiveFailures: 3, halfOpenInProgress: true }));
    assert.equal(r.state, "HALF_OPEN");
    assert.equal(r.reason, "HALF_OPEN_IN_PROGRESS");
  });
  it("rolling window expiry does not close unresolved circuit", () => {
    const r = evaluateCircuit(base({ consecutiveFailures: 3, failedAttemptsInWindow: 0 }));
    assert.notEqual(r.state, "CLOSED");
  });
  it("verified recovery closes", () => {
    const r = evaluateCircuit(base({ consecutiveFailures: 3, verifiedRecoverySinceOpen: true }));
    assert.equal(r.state, "CLOSED");
  });
  it("failed half-open trial reopens + manual", () => {
    const r = evaluateCircuit(base({ consecutiveFailures: 3, failedHalfOpenTrial: true }));
    assert.equal(r.state, "OPEN");
    assert.equal(r.manualInterventionRequired, true);
  });
  it("restart uncertainty blocks", () => {
    const r = evaluateCircuit(base({ restartUncertain: true }));
    assert.equal(r.reason, "RESTART_UNCERTAIN");
  });
  it("budget exhaustion", () => {
    const r = evaluateCircuit(base({ attemptsInWindow: 5 }));
    assert.equal(r.reason, "BUDGET_EXHAUSTED");
  });
  it("cooldown", () => {
    const r = evaluateCircuit(base({ cooldownRemainingMs: 1000 }));
    assert.equal(r.reason, "COOLDOWN");
  });
});
