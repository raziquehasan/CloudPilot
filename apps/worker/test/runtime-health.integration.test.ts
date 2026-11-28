import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import test from "node:test";
import { disconnectDatabase, prisma } from "@cloudpilot/database";
import {
  healthPersistenceReady, retireRuntimeHealth, runRuntimeHealthCheck, syncRuntimeHealthAfterStart,
} from "../src/health/runtime-health.js";
import type { HealthInspector } from "../src/health/probe-runtime.js";
import { discoverDueHealthChecks, pruneHealthHistory } from "../src/health/health-monitor-runtime.js";

const enabled = process.env.HEALTH_PERSISTENCE_ENABLED === "true" &&
  /^\/cloudpilot_health_test(?:_[a-z0-9]+)?$/.test(new URL(process.env.DATABASE_URL ?? "http://localhost").pathname);

test("scoped runtime health records one check, rejecting obsolete generations, expired leases and interruptions", {
  skip: !enabled,
}, async () => {
  assert.equal(await healthPersistenceReady(), true);
  let requests = 0;
  let responseStatus = 200;
  const server = createServer((_req, res) => { requests++; res.writeHead(responseStatus).end("probe"); });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const workspace = await prisma.workspace.create({ data: { name: "Health test", slug: `health-${randomUUID()}` } });
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: "Health test", slug: "health" } });
    await prisma.repository.create({
      data: {
        workspaceId: workspace.id, projectId: project.id, provider: "github", externalId: randomUUID(),
        name: "health", fullName: "example/health-probe-test",
      },
    });
    const deployment = await prisma.deployment.create({
      data: {
        workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT",
        triggerSource: "MANUAL", status: "RUNNING",
      },
    });
    const parent = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
    const build = await prisma.deploymentBuild.create({
      data: { ...parent, sourcePreparationId: randomUUID(), resolvedCommitSha: "c".repeat(40) },
    });
    const containerId = "a".repeat(64);
    const imageId = `sha256:${"b".repeat(64)}`;
    const readyAt = new Date();
    const runtime = await prisma.deploymentRuntimeInstance.create({
      data: {
        ...parent, buildId: build.id, status: "RUNNING", readyAt, containerId, imageId,
        hostPort: address.port, hostBindingAddress: "127.0.0.1", containerPort: 3000,
      },
    });
    const identity = { ...parent, runtimeInstanceId: runtime.id };
    const initial = { ...identity, generation: 0 };
    const owner = randomUUID();
    const inspect: HealthInspector = async (id, labels, containerPort, host) => {
      assert.equal(id, containerId);
      assert.equal(labels["cloudpilot.runtimeInstanceId"], runtime.id);
      assert.equal(containerPort, 3000);
      assert.equal(host, "127.0.0.1");
      return {
        ownershipVerified: true,
        inspection: {
          exists: true, running: true, containerId, imageId, state: "running", restartCount: 0,
          exitCode: null, startedAt: readyAt.toISOString(), hostPort: address.port,
        },
      };
    };
    const state = (generation: number) => prisma.deploymentHealthState.findFirstOrThrow({ where: { ...identity, generation } });
    await syncRuntimeHealthAfterStart(identity, readyAt, inspect);
    assert.equal((await state(0)).status, "STARTING");
    assert.deepEqual(await discoverDueHealthChecks(4, deployment.id), [initial]);
    assert.ok((await discoverDueHealthChecks(4)).some((candidate) => candidate.runtimeInstanceId === runtime.id));
    const concurrent = await Promise.all([
      runRuntimeHealthCheck(initial, owner, inspect),
      runRuntimeHealthCheck(initial, randomUUID(), inspect),
    ]);
    assert.equal(concurrent.filter(Boolean).length, 1);
    assert.equal(requests, 1);
    const healthy = await state(0);
    assert.equal(healthy.status, "HEALTHY");
    assert.equal(healthy.version, 1);
    assert.equal(healthy.consecutiveSuccesses, 1);
    assert.ok(healthy.lastCheckedAt && healthy.lastHealthyAt);
    const persisted = await prisma.deploymentHealthCheck.findFirstOrThrow({ where: initial });
    assert.equal(persisted.httpStatus, 200);
    assert.equal(persisted.dockerState, "running");
    assert.deepEqual(await discoverDueHealthChecks(4, deployment.id), []);

    for (const expected of ["DEGRADED", "DEGRADED", "UNHEALTHY", "HEALTHY"]) {
      responseStatus = expected === "HEALTHY" ? 200 : 500;
      await prisma.deploymentHealthState.updateMany({ where: initial, data: { nextCheckAt: new Date(0) } });
      assert.equal(await runRuntimeHealthCheck(initial, owner, inspect), true);
      assert.equal((await state(0)).status, expected);
    }
    assert.equal((await state(0)).consecutiveFailures, 0);
    assert.equal((await state(0)).consecutiveSuccesses, 1);

    const old = { ...initial, dockerResult: "PASSED", httpResult: "PASSED", checkedAt: new Date(0) } as const;
    await prisma.deploymentHealthCheck.createMany({ data: [old, old, old] });
    assert.equal(await pruneHealthHistory(2), 2);
    assert.equal(await prisma.deploymentHealthCheck.count({ where: { ...initial, checkedAt: new Date(0) } }), 1);
    assert.equal(await pruneHealthHistory(2), 1);
    assert.equal((await state(0)).status, "HEALTHY");

    await retireRuntimeHealth(identity);
    await syncRuntimeHealthAfterStart(identity, readyAt, inspect);
    assert.equal(await runRuntimeHealthCheck(initial, owner, inspect), false);
    assert.equal((await state(0)).nextCheckAt, null);
    const second = { ...identity, generation: 1 };
    assert.deepEqual(await discoverDueHealthChecks(4, deployment.id), [second]);
    await prisma.deploymentRuntimeInstance.update({ where: { id: runtime.id }, data: { status: "STOPPED" } });
    assert.deepEqual(await discoverDueHealthChecks(4, deployment.id), []);
    await prisma.deploymentRuntimeInstance.update({ where: { id: runtime.id }, data: { status: "RUNNING", hostBindingAddress: "0.0.0.0" } });
    assert.deepEqual(await discoverDueHealthChecks(4, deployment.id), []);
    await prisma.deploymentRuntimeInstance.update({ where: { id: runtime.id }, data: { hostBindingAddress: "127.0.0.1" } });
    const expire: HealthInspector = async (...args) => {
      await prisma.deploymentHealthState.updateMany({ where: second, data: { leaseExpiresAt: new Date(0) } });
      return inspect(...args);
    };
    assert.equal(await runRuntimeHealthCheck(second, owner, expire), false);
    assert.equal((await state(1)).version, 0);

    const controller = new AbortController();
    const interrupt: HealthInspector = async (...args) => {
      controller.abort();
      return inspect(...args);
    };
    assert.equal(await runRuntimeHealthCheck(second, owner, interrupt, { signal: controller.signal }), false);
    assert.equal((await state(1)).leaseToken, null);
    assert.equal(await prisma.deploymentHealthCheck.count({ where: identity }), 5);

    let retiredDuringProbe = false;
    const replace: HealthInspector = async (...args) => {
      if (!retiredDuringProbe) {
        retiredDuringProbe = true;
        await retireRuntimeHealth(identity);
        await syncRuntimeHealthAfterStart(identity, readyAt, inspect);
      }
      return inspect(...args);
    };
    assert.equal(await runRuntimeHealthCheck(second, owner, replace), false);
    assert.equal((await state(1)).nextCheckAt, null);
    assert.equal((await state(2)).status, "STARTING");
    assert.equal(await prisma.deploymentHealthCheck.count({ where: identity }), 5);
  } finally {
    await prisma.workspace.delete({ where: { id: workspace.id } });
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await disconnectDatabase();
  }
});
