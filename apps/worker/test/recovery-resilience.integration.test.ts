import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import test from "node:test";
import {
  prisma,
  disconnectDatabase,
  recoverySchemaReady,
  reserveManualRecovery,
  type RecoveryScope,
} from "@cloudpilot/database";
import { type RuntimeJob } from "@cloudpilot/queue";
import { processRecoveryJob } from "../src/health/recovery-processor.js";
import { DockerRuntimeExecutor } from "../src/runtime/docker-runtime-client.js";

// Isolated resilience suite (Phase 5.7.9). Runs only against a
// dedicated test database and never against primary infrastructure.
const database = new URL(process.env.DATABASE_URL ?? "http://localhost");
const enabled =
  process.env.MANUAL_RECOVERY_ENABLED === "true" &&
  /^\/cloudpilot_health_test_[a-z0-9]+$/.test(database.pathname);

const sourceEpoch = "2026-11-27T01:00:00.123456789Z";
const restartedEpoch = "2026-11-27T01:01:00.987654321Z";
const containerId = "a".repeat(64);
const imageId = `sha256:${"b".repeat(64)}`;

type Fixture = {
  scope: RecoveryScope;
  actorId: string;
  runtimeId: string;
  buildId: string;
  port: number;
  executor: DockerRuntimeExecutor;
  calls: () => number;
  setStarted: (value: string) => void;
};

async function withFixture(run: (f: Fixture) => Promise<void>) {
  assert.equal(await recoverySchemaReady(prisma), true);
  const suffix = randomUUID();
  const actor = await prisma.user.create({ data: { email: `${suffix}@example.invalid` } });
  const workspace = await prisma.workspace.create({ data: { name: "Resilience", slug: `resilience-${suffix}` } });
  let http = 200;
  let restarts = 0;
  let startedAt = sourceEpoch;
  const server = createServer((_req, res) => res.writeHead(http).end("probe"));
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const port = address.port;
    const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: "Resilience", slug: "resilience" } });
    await prisma.repository.create({ data: {
      workspaceId: workspace.id, projectId: project.id, provider: "github", externalId: randomUUID(),
      name: "health", fullName: "example/health-probe-test",
    } });
    const deployment = await prisma.deployment.create({ data: {
      workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT", triggerSource: "MANUAL", status: "RUNNING",
    } });
    const scope = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
    const build = await prisma.deploymentBuild.create({ data: {
      ...scope, sourcePreparationId: randomUUID(), resolvedCommitSha: "c".repeat(40), status: "SUCCEEDED", imageId,
    } });
    const runtime = await prisma.deploymentRuntimeInstance.create({ data: {
      ...scope, buildId: build.id, status: "RUNNING", readyAt: new Date(sourceEpoch), containerId, imageId,
      hostPort: port, hostBindingAddress: "127.0.0.1", containerPort: 3000,
    } });
    await prisma.deploymentHealthState.create({ data: {
      ...scope, runtimeInstanceId: runtime.id, generation: 0,
      status: "UNHEALTHY", consecutiveFailures: 3, nextCheckAt: new Date(),
    } });
    const executor = new DockerRuntimeExecutor();
    executor.inspectOwnedContainer = async (id, labels, containerPort, host) => {
      assert.equal(id, containerId);
      assert.equal(labels["cloudpilot.runtimeInstanceId"], runtime.id);
      assert.equal(containerPort, 3000);
      assert.equal(host, "127.0.0.1");
      return { ownershipVerified: true, inspection: {
        exists: true, running: true, containerId, imageId, state: "running", startedAt,
        restartCount: restarts, exitCode: null, hostPort: port,
      } };
    };
    executor.restartOwnedContainer = async (id, labels, target) => {
      assert.equal(id, containerId);
      assert.equal(target.hostPort, port);
      assert.equal(target.startedAt, sourceEpoch);
      restarts += 1;
      startedAt = restartedEpoch;
    };
    await run({ scope, actorId: actor.id, runtimeId: runtime.id, buildId: build.id, port, executor,
      calls: () => restarts, setStarted: (value) => { startedAt = value; } });
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await prisma.auditLog.deleteMany({ where: { workspaceId: workspace.id } });
    await prisma.workspace.delete({ where: { id: workspace.id } });
    await prisma.user.delete({ where: { id: actor.id } });
  }
}

function jobFor(f: Fixture, id: string): RuntimeJob {
  return { ...f.scope, kind: "runtime", action: "recover", correlationId: id, buildId: f.buildId, runtimeInstanceId: f.runtimeId };
}
const runJob = (job: RuntimeJob, executor: DockerRuntimeExecutor) =>
  processRecoveryJob(`recovery-${job.correlationId}`, job, executor, new AbortController().signal);

test("D4: expired-lease takeover reconciles an already-issued restart without a second restart", { skip: !enabled }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const attempt = reserve.attempt;
    // Simulate a crashed worker that already persisted restart intent and
    // issued the Docker restart, then died before finalizing. Its lease is
    // now expired and a new owner must reconcile safely.
    const past = new Date(Date.now() - 60_000);
    await prisma.deploymentRecoveryAttempt.update({ where: { id: attempt.id }, data: {
      status: "RUNNING", attemptCount: 1, restartIssuedAt: new Date(), restartSourceStartedAt: sourceEpoch,
      leaseOwnerId: randomUUID(), leaseToken: randomUUID(), leaseExpiresAt: past, startedAt: new Date(),
    } });
    await prisma.deploymentRuntimeInstance.update({ where: { id: f.runtimeId }, data: {
      executionLeaseToken: randomUUID(), executionLeaseExpiresAt: past,
    } });
    // The crashed worker already restarted the container; reflect the
    // advanced source start time so reconciliation verifies the restart.
    f.setStarted(restartedEpoch);

    const outcome = await runJob(jobFor(f, attempt.id), f.executor);
    assert.equal(outcome.status, "succeeded");
    assert.equal(f.calls(), 0, "takeover must not issue a second restart");
    const settled = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: attempt.id } });
    assert.equal(settled.status, "SUCCEEDED");
    assert.equal(settled.leaseToken, null);
    const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
    assert.equal(latest.generation, 1);
    assert.equal(latest.status, "HEALTHY");
    assert.equal(await prisma.deploymentHealthCheck.count({ where: f.scope }), 1);
  }));

test("D1/D2: duplicate concurrent finalization increments generation exactly once", { skip: !enabled }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    const outcomes = await Promise.all([runJob(job, f.executor), runJob(job, f.executor)]);
    assert.deepEqual(outcomes.map((o) => o.status).sort(), ["skipped", "succeeded"]);
    assert.equal(f.calls(), 1);
    const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
    assert.equal(latest.generation, 1);
    assert.equal(await prisma.deploymentHealthState.count({ where: f.scope }), 2);
    assert.equal(await prisma.deploymentHealthCheck.count({ where: f.scope }), 1);
    const record = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(record.status, "SUCCEEDED");
    assert.equal(record.attemptCount, 1);
  }));

test.after(async () => { if (enabled) await disconnectDatabase(); });