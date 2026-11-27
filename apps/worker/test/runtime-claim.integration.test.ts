import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { createServer } from "node:net";
import type { Job } from "bullmq";
import { prisma } from "@cloudpilot/database";
import type { QueueJob, RuntimeJob } from "@cloudpilot/queue";
import { workerEnv } from "../src/config/env.js";
import { claimRuntime, processRuntimeJob } from "../src/processors/runtime.processor.js";
import { DockerRuntimeExecutor, readDockerRuntimeCapabilities } from "../src/runtime/docker-runtime-client.js";

async function withRuntimeFixture(run: (fixture: {
  payload: RuntimeJob;
  job: Job<QueueJob>;
  executor: DockerRuntimeExecutor;
  imageId: string;
}) => Promise<void>) {
  const workspace = await prisma.workspace.create({
    data: { name: "Runtime claim test", slug: `runtime-test-${randomUUID()}` },
  });
  let deploymentId: string | undefined;
  try {
    const project = await prisma.project.create({
      data: { workspaceId: workspace.id, name: "Runtime claim test", slug: "runtime-test" },
    });
    const deployment = await prisma.deployment.create({
      data: {
        workspaceId: workspace.id, projectId: project.id,
        environment: "DEVELOPMENT", triggerSource: "MANUAL", status: "BUILD_SUCCEEDED",
      },
    });
    deploymentId = deployment.id;
    const scope = { workspaceId: workspace.id, projectId: project.id, deploymentId };
    const imageId = `sha256:${"a".repeat(64)}`;
    const build = await prisma.deploymentBuild.create({
      data: {
        ...scope, sourcePreparationId: randomUUID(), resolvedCommitSha: "a".repeat(40),
        status: "SUCCEEDED", imageId,
      },
    });
    const instance = await prisma.deploymentRuntimeInstance.create({
      data: { ...scope, buildId: build.id, imageId },
    });
    const jobId = `runtime-${deploymentId}`;
    await prisma.deploymentOutboxEvent.create({
      data: { ...scope, id: instance.id, eventType: "RUNTIME_REQUESTED", jobId, status: "PUBLISHED" },
    });
    const payload: RuntimeJob = {
      ...scope, kind: "runtime", action: "deploy", runtimeInstanceId: instance.id,
      correlationId: instance.id, buildId: build.id,
    };
    const job = { id: jobId, attemptsMade: 0, opts: { attempts: 3 } } as Job<QueueJob>;
    const executor = new DockerRuntimeExecutor();
    executor.inspectImage = async () => ({ exists: true, imageId });
    await run({ payload, job, executor, imageId });
  } finally {
    if (deploymentId) {
      await prisma.deploymentLogEntry.deleteMany({ where: { workspaceId: workspace.id, deploymentId } });
      await prisma.auditLog.deleteMany({ where: { workspaceId: workspace.id, entityId: deploymentId } });
      await prisma.deploymentRuntimeInstance.deleteMany({ where: { workspaceId: workspace.id, deploymentId } });
    }
    await prisma.workspace.delete({ where: { id: workspace.id } });
  }
}
test.after(() => prisma.$disconnect());

test("an unavailable runtime can retry its reserved attempt and only one worker wins", {
  skip: !workerEnv.RUNTIME_ENABLED,
}, () => withRuntimeFixture(async ({ payload, job, executor }) => {
  const { workspaceId, projectId, deploymentId, runtimeInstanceId } = payload;
  executor.probe = async () => readDockerRuntimeCapabilities(null);

  await assert.rejects(
    processRuntimeJob(job, payload, new AbortController().signal, { executor }),
    /Container deployment failed: RUNTIME_UNAVAILABLE/,
  );
  assert.equal((await prisma.deployment.findUniqueOrThrow({ where: { id: deploymentId } })).status, "DEPLOYING");
  const released = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: { id: runtimeInstanceId } });
  assert.equal(released.status, "QUEUED");
  assert.equal(released.executionLeaseToken, null);
  assert.equal(released.executionLeaseExpiresAt, null);
  assert.equal(await claimRuntime({ ...payload, workspaceId: randomUUID() }), null);
  assert.equal(await claimRuntime({ ...payload, runtimeInstanceId: randomUUID() }), null);
  assert.equal(await claimRuntime({ ...payload, buildId: randomUUID() }), null);

  const results = await Promise.all([claimRuntime(payload), claimRuntime(payload)]);
  const [winner] = results.filter((claim) => claim !== null);
  assert.equal(results.filter((claim) => claim !== null).length, 1);
  assert.ok(winner);
  assert.equal(winner.runtimeInstanceId, runtimeInstanceId);
  assert.equal(winner.runtimeAttempt, released.runtimeAttempt);
  const claimed = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: { id: runtimeInstanceId } });
  assert.equal(claimed.status, "CREATING");
  assert.equal(claimed.executionLeaseToken, winner.leaseToken);
  assert.equal(await prisma.deploymentRuntimeInstance.count({ where: { workspaceId, projectId, deploymentId } }), 1);
}));

test("a Docker-assigned host port is persisted only after startup and inspection", {
  skip: !workerEnv.RUNTIME_ENABLED,
}, () => withRuntimeFixture(async ({ payload, job, executor, imageId }) => {
  const server = createServer((socket) => socket.end());
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, workerEnv.RUNTIME_BIND_HOST, resolve);
  });

  try {
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const { port } = address;
    const containerId = "b".repeat(64);
    const instanceWhere = { id: payload.runtimeInstanceId };
    executor.probe = async () => ({
      available: true, runtimeType: "docker", supportsResourceLimits: true,
      supportsNetworkIsolation: true, supportsStrongIsolation: false,
    });
    executor.ensureNetwork = async () => true;
    executor.createContainer = async (request) => ({
      containerId, containerName: request.name, networkName: request.networkName, hostPort: 0,
    });
    executor.startContainer = async () => {
      const starting = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: instanceWhere });
      assert.equal(starting.status, "STARTING");
      assert.equal(starting.containerId, containerId);
      assert.equal(starting.hostPort, null);
    };
    executor.inspectContainer = async () => ({
      exists: true, containerId, imageId, state: "running", running: true,
      exitCode: null, startedAt: new Date().toISOString(), hostPort: port,
    });

    assert.equal(
      (await processRuntimeJob(job, payload, new AbortController().signal, { executor })).status,
      "succeeded",
    );
    const running = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: instanceWhere });
    assert.equal(running.status, "RUNNING");
    assert.equal(running.hostPort, port);
    assert.equal((await prisma.deployment.findUniqueOrThrow({ where: { id: payload.deploymentId } })).status, "RUNNING");
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}));

test("unexpected runtime failures persist the operation and code without raw errors", {
  skip: !workerEnv.RUNTIME_ENABLED || !workerEnv.DEPLOYMENT_LOGS_ENABLED,
}, () => withRuntimeFixture(async ({ payload, job, executor }) => {
  executor.probe = async () => { throw new Error("credential-shaped internal error"); };

  await assert.rejects(
    processRuntimeJob(job, payload, new AbortController().signal, { executor }),
    /Container deployment failed: UNEXPECTED_RUNTIME_FAILURE/,
  );
  const logs = await prisma.deploymentLogEntry.findMany({
    where: { deploymentId: payload.deploymentId, runtimeInstanceId: payload.runtimeInstanceId },
    select: { message: true, level: true },
  });
  assert.deepEqual(logs, [{
    message: "Runtime failed during Docker capability probe (UNEXPECTED_RUNTIME_FAILURE).",
    level: "ERROR",
  }]);
}));
