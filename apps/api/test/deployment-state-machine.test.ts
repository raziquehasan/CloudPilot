import assert from "node:assert/strict";
import test from "node:test";
import {
  canTransitionDeployment,
  isTerminalDeploymentStatus,
} from "../src/services/deployment-state-machine.ts";

test("deployment lifecycle allows only ordered preparation and health-verified running", () => {
  assert.equal(canTransitionDeployment("QUEUED", "PREPARING"), true);
  assert.equal(canTransitionDeployment("PREPARING", "CLONING"), true);
  assert.equal(canTransitionDeployment("CLONING", "BUILDING"), true);
  assert.equal(canTransitionDeployment("BUILDING", "DEPLOYING"), true);
  assert.equal(canTransitionDeployment("DEPLOYING", "HEALTH_CHECKING"), true);
  assert.equal(canTransitionDeployment("HEALTH_CHECKING", "RUNNING"), true);
  assert.equal(canTransitionDeployment("BUILDING", "RUNNING"), false);
  assert.equal(canTransitionDeployment("PREPARING", "DEPLOYING"), false);
});

test("phase 5.4 build lifecycle reaches BUILD_SUCCEEDED without deploying", () => {
  assert.equal(canTransitionDeployment("SOURCE_PREPARED", "BUILDING"), true);
  assert.equal(canTransitionDeployment("BUILDING", "BUILD_SUCCEEDED"), true);
  // A successful build must not be treated as a live deployment.
  assert.equal(canTransitionDeployment("BUILD_SUCCEEDED", "RUNNING"), false);
  assert.equal(canTransitionDeployment("BUILD_SUCCEEDED", "SUCCEEDED"), false);
  // No build before source preparation.
  assert.equal(canTransitionDeployment("PREPARING", "BUILDING"), false);
  // No build for cancelled deployments.
  assert.equal(canTransitionDeployment("CANCELLED", "BUILDING"), false);
});

test("deployment cancellation and terminal states cannot be overwritten", () => {
  assert.equal(canTransitionDeployment("QUEUED", "CANCELLED"), true);
  assert.equal(canTransitionDeployment("CANCELLING", "CANCELLED"), true);
  assert.equal(canTransitionDeployment("RUNNING", "CANCELLED"), false);
  for (const status of ["SUCCEEDED", "FAILED", "CANCELLED", "STOPPED", "ROLLED_BACK"] as const) {
    assert.equal(isTerminalDeploymentStatus(status), true);
    assert.equal(canTransitionDeployment(status, "PREPARING"), false);
  }
  assert.equal(isTerminalDeploymentStatus("RUNNING"), false);
});
