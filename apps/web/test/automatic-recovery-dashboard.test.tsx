import assert from "node:assert/strict";
import test from "node:test";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderToStaticMarkup } from "react-dom/server";
import DeploymentHealthPanel from "../components/dashboard/deployment-health";
import {
  type AutomaticRecovery, type DeploymentHealth, type Recovery,
  recoveryCooldownSeconds,
} from "../components/dashboard/deployment-health-data";
import { TooltipProvider } from "../components/ui/tooltip";
import { ApiError } from "../lib/api";

const scope = ["deployment-health", "workspace", "project", "deployment"] as const;
const recordedAt = "2026-11-28T12:00:00.000Z";
const attempt: Recovery = {
  id: "recovery", runtimeInstanceId: "runtime", generation: 2,
  status: "FAILED", trigger: "AUTOMATIC", action: "RESTART", attemptCount: 1,
  failureReason: "RECOVERY_TIMEOUT", failureMessage: null, createdAt: recordedAt,
  updatedAt: recordedAt, startedAt: recordedAt, finishedAt: recordedAt,
};
const automatic: AutomaticRecovery = {
  configured: false, workerAvailability: "DISABLED", circuitState: "OPEN", eligible: false,
  rejectionReason: "AUTO_DISABLED", remainingBudget: 1, maxBudget: 3, consecutiveFailures: 4,
  cooldownRemainingMs: 5000, nextEligibleAt: null, manualInterventionRequired: true,
  latestOutcome: attempt, snapshotAt: recordedAt,
};
const health: DeploymentHealth = {
  schemaAvailable: true, runtimeStatus: "RUNNING", state: null, checks: [], monitoring: null,
  recovery: { allowed: true, reason: null }, automaticRecovery: automatic,
};

function render(client: QueryClient, canManage = true) {
  return renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <TooltipProvider>
        <DeploymentHealthPanel workspaceId="workspace" projectId="project" deploymentId="deployment" canManage={canManage} />
      </TooltipProvider>
    </QueryClientProvider>,
  );
}

test("the Health tab shows the disabled worker, real circuit and both history triggers", () => {
  const client = new QueryClient();
  client.setQueryData([...scope, "summary"], health);
  client.setQueryData([...scope, "recoveries"], [attempt, { ...attempt, id: "manual", trigger: "MANUAL" }]);
  const html = render(client);
  assert.match(html, /Automatic recovery is disabled/);
  assert.match(html, />open</);
  assert.match(html, /1 of 3 remaining/);
  assert.match(html, />5 s</);
  assert.match(html, /Recovery exceeded its time limit/);
  assert.match(html, />automatic</);
  assert.match(html, />manual</);
  assert.match(html, /Restart runtime/);
  assert.doesNotMatch(render(client, false), /Restart runtime/);
});

test("closed and half-open snapshots render without changing the worker configuration", () => {
  for (const circuitState of ["CLOSED", "HALF_OPEN"] as const) {
    const client = new QueryClient();
    client.setQueryData([...scope, "summary"], {
      ...health, automaticRecovery: { ...automatic, circuitState, workerAvailability: "STALE" },
    });
    const html = render(client);
    assert.match(html, new RegExp(">" + circuitState.toLowerCase().replace("_", " ") + "<"));
    assert.match(html, /stale heartbeat/);
    assert.match(html, /Automatic recovery is disabled/);
  }
});

test("loading and unavailable health do not manufacture circuit data", () => {
  const client = new QueryClient();
  const loading = render(client);
  assert.match(loading, /Loading automatic recovery/);
  assert.match(loading, /Loading recovery history/);

  client.setQueryData([...scope, "summary"], {
    ...health, automaticRecovery: { ...automatic, circuitState: null, workerAvailability: "UNKNOWN",
      rejectionReason: "SCHEMA_UNAVAILABLE", remainingBudget: null, maxBudget: null, consecutiveFailures: null,
      cooldownRemainingMs: null, manualInterventionRequired: null, latestOutcome: null },
  });
  const unavailable = render(client);
  assert.match(unavailable, /capability unknown/);
  assert.match(unavailable, /recovery schema is unavailable/);
  assert.doesNotMatch(unavailable, /0 of 3 remaining|>closed</);
});

test("a workspace authorization error hides a previously cached automatic snapshot", () => {
  const client = new QueryClient();
  client.setQueryData([...scope, "summary"], health);
  client.getQueryCache().find({ queryKey: [...scope, "summary"] })?.setState({
    status: "error", error: new ApiError(403, "Forbidden"), errorUpdatedAt: Date.now(),
  });
  const html = render(client);
  assert.match(html, /This deployment is unavailable in your current workspace/);
  assert.match(html, /Automatic recovery data is unavailable/);
  assert.doesNotMatch(html, /1 of 3 remaining/);
});

test("cooldown uses elapsed time from the snapshot and stops at zero", () => {
  assert.equal(recoveryCooldownSeconds(5000, 1200), 4);
  assert.equal(recoveryCooldownSeconds(5000, 20_000), 0);
  assert.equal(recoveryCooldownSeconds(5000, -1000), 5);
  assert.equal(recoveryCooldownSeconds(null, 1000), null);
});
