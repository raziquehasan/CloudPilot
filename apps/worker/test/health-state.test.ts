import assert from "node:assert/strict";
import test from "node:test";
import { createHealthState, transitionHealth, type HealthEvent, type HealthState } from "../src/health/health-state.js";

const scope = {
  workspaceId: "workspace", projectId: "project", deploymentId: "deployment",
  runtimeInstanceId: "runtime", generation: 0,
};
const failure = { scope, kind: "CHECK_FAILED", reason: "HTTP_TIMEOUT" } as const;

function step(state: HealthState, event: HealthEvent) {
  const result = transitionHealth(state, event);
  assert.ok(result.applied);
  return result.state;
}

test("startup and consecutive failures degrade health until the threshold", () => {
  const initial = createHealthState(scope);
  assert.equal(initial.status, "UNKNOWN");
  const starting = step(initial, { scope, kind: "STARTED" });
  assert.equal(starting.status, "STARTING");
  const healthy = step(starting, { scope, kind: "CHECK_PASSED" });
  const first = step(healthy, failure);
  const second = step(first, failure);
  assert.equal(first.status, "DEGRADED");
  assert.equal(first.consecutiveSuccesses, 0);
  assert.equal(second.status, "DEGRADED");
  const third = step(second, failure);
  assert.equal(third.status, "UNHEALTHY");
  assert.equal(third.consecutiveFailures, 3);
  assert.equal(third.lastFailureReason, "HTTP_TIMEOUT");
  const recovered = step(second, { scope, kind: "CHECK_PASSED" });
  assert.equal(recovered.status, "HEALTHY");
  assert.equal(recovered.consecutiveFailures, 0);
  assert.equal(recovered.lastFailureReason, null);
});

test("recovery requires explicit verification and stops when the budget is exhausted", () => {
  const unhealthy = [failure, failure, failure].reduce<HealthState>(step, createHealthState(scope));
  const recovering = step(unhealthy, { scope, kind: "RECOVERY_STARTED", attemptsInWindow: 0 });
  assert.equal(recovering.status, "RECOVERING");
  assert.deepEqual(transitionHealth(recovering, { scope, kind: "CHECK_PASSED" }), {
    applied: false, reason: "INVALID_TRANSITION",
  });
  assert.equal(step(recovering, { scope, kind: "RECOVERY_VERIFIED" }).status, "HEALTHY");
  assert.equal(step(recovering, { scope, kind: "RECOVERY_FAILED", attemptsInWindow: 1 }).status, "UNHEALTHY");
  const failed = step(recovering, { scope, kind: "RECOVERY_FAILED", attemptsInWindow: 3 });
  assert.equal(failed.status, "FAILED");
  assert.equal(failed.lastFailureReason, "RECOVERY_EXHAUSTED");
  assert.equal(transitionHealth(failed, { scope, kind: "CHECK_PASSED" }).applied, false);
  assert.equal(transitionHealth(unhealthy, { scope, kind: "RECOVERY_STARTED", attemptsInWindow: 3 }).applied, false);
  assert.equal(step(unhealthy, { scope, kind: "RECOVERY_EXHAUSTED", attemptsInWindow: 3 }).status, "FAILED");
  assert.equal(transitionHealth(createHealthState(scope), { scope, kind: "RECOVERY_STARTED", attemptsInWindow: 0 }).applied, false);
});

test("an old generation or a different runtime cannot update current health", () => {
  const initial = createHealthState({ ...scope, generation: 1 });
  for (const stale of [scope, { ...initial.scope, runtimeInstanceId: "previous-runtime" }, { ...initial.scope, workspaceId: "other" }]) {
    assert.deepEqual(transitionHealth(initial, { scope: stale, kind: "CHECK_PASSED" }), {
      applied: false, reason: "STALE_GENERATION",
    });
  }
  assert.equal(initial.status, "UNKNOWN");
});
