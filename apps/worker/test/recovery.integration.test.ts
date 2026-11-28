import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import test from "node:test";
import { disconnectDatabase, prisma, recoverySchemaReady, reserveManualRecovery, type RecoveryScope } from "@cloudpilot/database";
import { type RuntimeJob } from "@cloudpilot/queue";
import { processRecoveryJob, withRuntimeLifecycleLease } from "../src/health/recovery-processor.js";
import { DockerRuntimeExecutor } from "../src/runtime/docker-runtime-client.js";

const database = new URL(process.env.DATABASE_URL ?? "http://localhost");
const enabled = process.env.MANUAL_RECOVERY_ENABLED === "true" && /^\/cloudpilot_health_test_[a-z0-9]+$/.test(database.pathname);
const sourceEpoch = "2026-11-27T01:00:00.123456789Z";
const restartedEpoch = "2026-11-27T01:01:00.987654321Z";
const containerId = "a".repeat(64);
const imageId = `sha256:${"b".repeat(64)}`;

async function fixture(run: (f: {
  scope: RecoveryScope; actorId: string; runtimeId: string; buildId: string;
  executor: DockerRuntimeExecutor; port: number; calls: () => number;
  setHttp: (value: number) => void; setPort: (value: number) => void;
}) => Promise<void>) {
  assert.equal(await recoverySchemaReady(prisma), true);
  const suffix = randomUUID();
  const actor = await prisma.user.create({ data: { email: `${suffix}@example.invalid` } });
  const workspace = await prisma.workspace.create({ data: { name: "Recovery test", slug: `recovery-${suffix}` } });
  let http = 200;
  const server = createServer((_req, res) => res.writeHead(http).end("probe"));
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const port = address.port;
    const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: "Recovery test", slug: "recovery" } });
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
    let startedAt = sourceEpoch;
    let restarts = 0;
    let exposedPort = port;
    const executor = new DockerRuntimeExecutor();
    executor.inspectOwnedContainer = async (id, labels, containerPort, host) => {
      assert.equal(id, containerId);
      assert.equal(labels["cloudpilot.runtimeInstanceId"], runtime.id);
      assert.equal(containerPort, 3000);
      assert.equal(host, "127.0.0.1");
      return { ownershipVerified: true, inspection: {
        exists: true, running: true, containerId, imageId, state: "running", startedAt,
        restartCount: restarts, exitCode: null, hostPort: exposedPort,
      } };
    };
    executor.restartOwnedContainer = async (id, labels, target) => {
      assert.equal(id, containerId);
      assert.equal(labels["cloudpilot.runtimeInstanceId"], runtime.id);
      assert.equal(target.hostPort, port);
      assert.equal(target.startedAt, sourceEpoch);
      restarts++;
      startedAt = restartedEpoch;
    };
    await run({
      scope, actorId: actor.id, runtimeId: runtime.id, buildId: build.id, executor,
      port, calls: () => restarts, setHttp: (value) => { http = value; }, setPort: (value) => { exposedPort = value; },
    });
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await prisma.auditLog.deleteMany({ where: { workspaceId: workspace.id } });
    await prisma.workspace.delete({ where: { id: workspace.id } });
    await prisma.user.delete({ where: { id: actor.id } });
  }
}

function jobFor(f: { scope: RecoveryScope; runtimeId: string; buildId: string }, id: string): RuntimeJob {
  return { ...f.scope, kind: "runtime", action: "recover", recoveryTrigger: "MANUAL", correlationId: id, buildId: f.buildId, runtimeInstanceId: f.runtimeId };
}

const runJob = (job: RuntimeJob, executor: DockerRuntimeExecutor, signal = new AbortController().signal) =>
  processRecoveryJob(`recovery-${job.correlationId}`, job, executor, signal);

test("manual recovery serializes reservations and workers, verifies HTTP and preserves the binding", { skip: !enabled }, () =>
  fixture(async (f) => {
    const key = randomUUID();
    const replay = await Promise.all([
      reserveManualRecovery(prisma, f.scope, f.actorId, key),
      reserveManualRecovery(prisma, f.scope, f.actorId, key),
    ]);
    assert.deepEqual(replay.map((value) => value.kind).sort(), ["replay", "reserved"]);
    const [first] = replay;
    assert.ok(first?.kind === "reserved" || first?.kind === "replay");
    assert.equal((await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID())).kind, "busy");
    assert.equal((await reserveManualRecovery(prisma, { ...f.scope, workspaceId: randomUUID() }, f.actorId, randomUUID())).kind, "not_found");
    const job = jobFor(f, first.attempt.id);
    await assert.rejects(withRuntimeLifecycleLease({ ...job, action: "stop" }, new AbortController().signal, async () => {
      assert.fail("A competing stop cannot run during recovery");
    }), /operation is already in progress/);
    const outcomes = await Promise.all([runJob(job, f.executor), runJob(job, f.executor)]);
    assert.deepEqual(outcomes.map((value) => value.status).sort(), ["skipped", "succeeded"]);
    assert.equal(f.calls(), 1);
    const record = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: first.attempt.id } });
    assert.equal(record.status, "SUCCEEDED");
    assert.equal(record.attemptCount, 1);
    assert.equal(record.restartSourceStartedAt, sourceEpoch);
    assert.ok(record.startedAt && record.finishedAt);
    assert.equal(record.leaseToken, null);
    const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
    assert.equal(latest.generation, 1);
    assert.equal(latest.status, "HEALTHY");
    assert.equal((await prisma.deploymentHealthCheck.findFirstOrThrow({ where: f.scope })).httpStatus, 200);
    const runtime = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: { id: f.runtimeId } });
    assert.equal(runtime.hostPort, f.port);
    assert.equal(runtime.containerId, containerId);
    assert.equal(runtime.executionLeaseToken, null);
    assert.equal(await prisma.auditLog.count({ where: { entityId: f.scope.deploymentId, action: "DEPLOYMENT_RECOVERY_SUCCEEDED" } }), 1);
  }));

test("database settlement failure after restart reconciles once from the persisted epoch", { skip: !enabled }, () =>
  fixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    assert.equal(reserve.kind, "reserved");
    if (reserve.kind !== "reserved") return;
    const job = jobFor(f, reserve.attempt.id);
    await prisma.$executeRawUnsafe(`CREATE FUNCTION recovery_test_fail_settlement() RETURNS trigger LANGUAGE plpgsql
      AS $$ BEGIN IF NEW.status = 'SUCCEEDED' THEN RAISE EXCEPTION 'simulated settlement outage'; END IF; RETURN NEW; END; $$`);
    await prisma.$executeRawUnsafe(`CREATE TRIGGER recovery_test_fail_settlement BEFORE UPDATE ON deployment_recovery_attempts
      FOR EACH ROW EXECUTE FUNCTION recovery_test_fail_settlement()`);
    try {
      await assert.rejects(runJob(job, f.executor), /settlement unavailable/);
      assert.equal(f.calls(), 1);
      const uncertain = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: job.correlationId } });
      assert.equal(uncertain.status, "RUNNING");
      assert.equal(uncertain.restartSourceStartedAt, sourceEpoch);
      assert.equal(await prisma.deploymentHealthCheck.count({ where: f.scope }), 0);
    } finally {
      await prisma.$executeRawUnsafe("DROP TRIGGER recovery_test_fail_settlement ON deployment_recovery_attempts");
      await prisma.$executeRawUnsafe("DROP FUNCTION recovery_test_fail_settlement()");
    }
    assert.equal((await runJob(job, f.executor)).status, "succeeded");
    assert.equal(f.calls(), 1);
    assert.equal(await prisma.deploymentHealthCheck.count({ where: f.scope }), 1);
  }));

test("failed HTTP is not success, exhaustion is terminal, and port drift leaves the saved port intact", { skip: !enabled }, async () => {
  await fixture(async (f) => {
    for (let index = 0; index < 2; index++) {
      await prisma.deploymentRecoveryAttempt.create({ data: {
        ...f.scope, runtimeInstanceId: f.runtimeId, generation: 0, action: "RESTART", trigger: "MANUAL",
        idempotencyKey: randomUUID(), status: "FAILED", attemptCount: 1,
        startedAt: new Date(Date.now() - 120_000), finishedAt: new Date(Date.now() - 120_000),
        createdAt: new Date(Date.now() - 120_000), failureReason: "HTTP_STATUS",
      } });
    }
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    f.setHttp(500);
    assert.equal((await runJob(jobFor(f, reserve.attempt.id), f.executor)).status, "failed");
    assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).failureReason, "HTTP_STATUS");
    const health = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope });
    assert.equal(health.status, "FAILED");
    assert.equal(health.lastFailureReason, "RECOVERY_EXHAUSTED");
    assert.equal((await prisma.deployment.findUniqueOrThrow({ where: { id: f.scope.deploymentId } })).status, "RUNNING");
  });
  await fixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const restart = f.executor.restartOwnedContainer.bind(f.executor);
    f.executor.restartOwnedContainer = async (...args) => { await restart(...args); f.setPort(f.port === 65535 ? 65534 : f.port + 1); };
    assert.equal((await runJob(jobFor(f, reserve.attempt.id), f.executor)).status, "failed");
    assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).failureReason, "RECOVERY_PORT_CHANGED");
    assert.equal((await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: { id: f.runtimeId } })).hostPort, f.port);
  });
});

test("stopped, stale, and interrupted attempts never restart a replacement", { skip: !enabled }, () =>
  fixture(async (f) => {
    await prisma.deploymentRuntimeInstance.update({ where: { id: f.runtimeId }, data: { status: "STOPPED" } });
    assert.equal((await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID())).kind, "ineligible");
    await prisma.deploymentRuntimeInstance.update({ where: { id: f.runtimeId }, data: { status: "RUNNING" } });
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    const abort = new AbortController();
    const inspect = f.executor.inspectOwnedContainer.bind(f.executor);
    f.executor.inspectOwnedContainer = async (...args) => { abort.abort(); return inspect(...args); };
    await assert.rejects(runJob(job, f.executor, abort.signal), /interrupted/);
    assert.equal(f.calls(), 0);
    const interrupted = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(interrupted.status, "RUNNING");
    assert.ok(interrupted.leaseExpiresAt && interrupted.leaseExpiresAt <= new Date());
    await prisma.deploymentHealthState.updateMany({ where: f.scope, data: { version: { increment: 1 } } });
    assert.equal((await runJob(job, f.executor)).status, "skipped");
    assert.equal(f.calls(), 0);
    assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).failureReason, "RECOVERY_STALE");
  }));

test.after(async () => { if (enabled) await disconnectDatabase(); });
