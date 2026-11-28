import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
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

// Real worker-subprocess crash (Phase 5.7.9, Stage E). Opt-in, isolated only.
const database = new URL(process.env.DATABASE_URL ?? "http://localhost");
const enabled =
  process.env.CLOUDPILOT_RESILIENCE_FAULT === "1" &&
  process.env.MANUAL_RECOVERY_ENABLED === "true" &&
  /^\/cloudpilot_health_test_[a-z0-9]+$/.test(database.pathname);

const sourceEpoch = "2026-11-27T01:00:00.123456789Z";
const restartedEpoch = "2026-11-27T01:01:00.987654321Z";
const containerId = "a".repeat(64);
const imageId = `sha256:${"b".repeat(64)}`;

function runChild(input: unknown, crashAfter: string, nodeEnv = "test", timeoutMs = 60_000, maxRunMs = 12_000): Promise<number> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--import", "tsx", "test/helpers/crash-child.ts"], {
      cwd: process.cwd(),
      env: { ...process.env, NODE_ENV: nodeEnv, CLOUDPILOT_RECOVERY_CRASH_TEST: "1", CLOUDPILOT_RECOVERY_CRASH_AFTER: crashAfter, CLOUDPILOT_CRASH_MAX_MS: String(maxRunMs), CLOUDPILOT_CRASH_INPUT: JSON.stringify(input) },
      stdio: "ignore",
    });
    const timer = setTimeout(() => { child.kill(); resolve(124); }, timeoutMs);
    child.once("error", (error) => { clearTimeout(timer); reject(error); });
    child.once("exit", (code) => { clearTimeout(timer); resolve(code ?? -1); });
  });
}

type Fixture = { scope: RecoveryScope; actorId: string; runtimeId: string; buildId: string; port: number };

async function withFixture(run: (f: Fixture) => Promise<void>) {
  assert.equal(await recoverySchemaReady(prisma), true);
  const suffix = randomUUID();
  const actor = await prisma.user.create({ data: { email: `${suffix}@example.invalid` } });
  const workspace = await prisma.workspace.create({ data: { name: "Crash", slug: `crash-${suffix}` } });
  const server = createServer((_req, res) => res.writeHead(200).end("probe"));
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const port = address.port;
    const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: "Crash", slug: "crash" } });
    await prisma.repository.create({ data: { workspaceId: workspace.id, projectId: project.id, provider: "github", externalId: randomUUID(), name: "health", fullName: "example/health-probe-test" } });
    const deployment = await prisma.deployment.create({ data: { workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT", triggerSource: "MANUAL", status: "RUNNING" } });
    const scope = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
    const build = await prisma.deploymentBuild.create({ data: { ...scope, sourcePreparationId: randomUUID(), resolvedCommitSha: "c".repeat(40), status: "SUCCEEDED", imageId } });
    const runtime = await prisma.deploymentRuntimeInstance.create({ data: { ...scope, buildId: build.id, status: "RUNNING", readyAt: new Date(sourceEpoch), containerId, imageId, hostPort: port, hostBindingAddress: "127.0.0.1", containerPort: 3000 } });
    await prisma.deploymentHealthState.create({ data: { ...scope, runtimeInstanceId: runtime.id, generation: 0, status: "UNHEALTHY", consecutiveFailures: 3, nextCheckAt: new Date() } });
    await run({ scope, actorId: actor.id, runtimeId: runtime.id, buildId: build.id, port });
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await prisma.auditLog.deleteMany({ where: { workspaceId: workspace.id } });
    await prisma.workspace.delete({ where: { id: workspace.id } });
    await prisma.user.delete({ where: { id: actor.id } });
  }
}

function jobFor(f: Fixture, id: string): RuntimeJob {
  return { ...f.scope, kind: "runtime", action: "recover", recoveryTrigger: "MANUAL", correlationId: id, buildId: f.buildId, runtimeInstanceId: f.runtimeId };
}

test("E: crash after durable intent (before restart) reconciles conservatively with no second restart", { skip: !enabled, timeout: 90000 }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    const code = await runChild({ job, containerId, imageId, port: f.port, restartedEpoch }, "after-intent");
    assert.equal(code, 1, "child must crash at the after-intent boundary");

    // Durable intent survived the abrupt termination.
    const crashed = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.ok(crashed.restartIssuedAt, "restart intent persisted before the crash");
    assert.equal(crashed.restartSourceStartedAt, sourceEpoch);
    assert.equal(crashed.status, "RUNNING");

    // Simulate the 120s lease elapsing, then a replacement worker reconciles.
    const past = new Date(Date.now() - 1_000);
    await prisma.deploymentRecoveryAttempt.update({ where: { id: reserve.attempt.id }, data: { leaseExpiresAt: past } });
    await prisma.deploymentRuntimeInstance.update({ where: { id: f.runtimeId }, data: { executionLeaseExpiresAt: past } });

    let restarts = 0;
    const executor = new DockerRuntimeExecutor();
    executor.inspectOwnedContainer = async () => ({ ownershipVerified: true, inspection: {
      exists: true, running: true, containerId, imageId, state: "running", startedAt: sourceEpoch,
      restartCount: 0, exitCode: null, hostPort: f.port } });
    executor.restartOwnedContainer = async () => { restarts += 1; };

    const outcome = await processRecoveryJob(`recovery-${job.correlationId}`, job, executor, new AbortController().signal);
    assert.equal(outcome.status, "failed", "uncertain restart must not be reported as success");
    assert.equal(restarts, 0, "a persisted intent must never trigger a second restart");
    const settled = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(settled.status, "FAILED");
    assert.equal(settled.failureReason, "RECOVERY_RESTART_UNCERTAIN");
    assert.equal(await prisma.deploymentHealthCheck.count({ where: f.scope }), 0, "no false health generation advancement");
    const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
    assert.equal(latest.generation, 0);
  }));

test.after(async () => { if (enabled) await disconnectDatabase(); });

async function expireLease(runtimeId: string, attemptId: string) {
  const past = new Date(Date.now() - 1_000);
  await prisma.deploymentRecoveryAttempt.update({ where: { id: attemptId }, data: { leaseExpiresAt: past } });
  await prisma.deploymentRuntimeInstance.update({ where: { id: runtimeId }, data: { executionLeaseExpiresAt: past } });
}

function replacementExecutor(port: number, startedAt: string, onRestart: () => void) {
  let current = startedAt;
  const executor = new DockerRuntimeExecutor();
  executor.inspectOwnedContainer = async () => ({ ownershipVerified: true, inspection: {
    exists: true, running: true, containerId, imageId, state: "running", startedAt: current,
    restartCount: 0, exitCode: null, hostPort: port } });
  executor.restartOwnedContainer = async () => { current = restartedEpoch; onRestart(); };
  return executor;
}

test("E2: crash after lease acquisition (before intent) → replacement issues exactly one restart", { skip: !enabled, timeout: 90000 }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    assert.equal(await runChild({ job, containerId, imageId, port: f.port, restartedEpoch }, "after-claim"), 1);
    const crashed = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(crashed.restartIssuedAt, null, "no intent persisted before the crash");
    await expireLease(f.runtimeId, reserve.attempt.id);
    let restarts = 0;
    const outcome = await processRecoveryJob(`recovery-${job.correlationId}`, job, replacementExecutor(f.port, sourceEpoch, () => { restarts += 1; }), new AbortController().signal);
    assert.equal(outcome.status, "succeeded");
    assert.equal(restarts, 1);
    assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).status, "SUCCEEDED");
    assert.equal((await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } })).generation, 1);
  }));

test("E3: crash after Docker restart (before finalize) → replacement does not restart twice", { skip: !enabled, timeout: 90000 }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    assert.equal(await runChild({ job, containerId, imageId, port: f.port, restartedEpoch }, "after-restart"), 1);
    const crashed = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.ok(crashed.restartIssuedAt, "intent persisted before the restart");
    await expireLease(f.runtimeId, reserve.attempt.id);
    let restarts = 0;
    // The container was already restarted by the crashed worker.
    const outcome = await processRecoveryJob(`recovery-${job.correlationId}`, job, replacementExecutor(f.port, restartedEpoch, () => { restarts += 1; }), new AbortController().signal);
    assert.equal(outcome.status, "succeeded");
    assert.equal(restarts, 0, "no unsafe duplicate restart");
    assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).status, "SUCCEEDED");
    assert.equal((await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } })).generation, 1);
  }));

test("E4: crash after finalization (before ack) → redelivery is a no-op", { skip: !enabled, timeout: 90000 }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    assert.equal(await runChild({ job, containerId, imageId, port: f.port, restartedEpoch }, "after-finish"), 1);
    assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).status, "SUCCEEDED");
    let restarts = 0;
    const outcome = await processRecoveryJob(`recovery-${job.correlationId}`, job, replacementExecutor(f.port, restartedEpoch, () => { restarts += 1; }), new AbortController().signal);
    assert.equal(outcome.status, "skipped");
    assert.equal(restarts, 0);
    assert.equal(await prisma.deploymentHealthState.count({ where: f.scope }), 2, "no duplicate generation");
  }));

test("M1: sustained multi-worker contention yields one success, no duplicate generation", { skip: !enabled, timeout: 120000 }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    const input = { job, containerId, imageId, port: f.port, restartedEpoch };
    const codes = await Promise.all([
      runChild(input, "none", "test", 45_000),
      runChild(input, "none", "test", 45_000),
    ]);
    assert.ok(codes.every((c) => c === 0 || c === 1), `workers exited cleanly: ${codes}`);
    const record = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(record.status, "SUCCEEDED");
    assert.equal(record.attemptCount, 1);
    assert.equal(await prisma.deploymentHealthState.count({ where: f.scope }), 2);
    assert.equal(await prisma.deploymentHealthCheck.count({ where: f.scope }), 1);
    const runtime = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: { id: f.runtimeId } });
    assert.equal(runtime.restartCount, 1, "exactly one restart across all workers");
    assert.equal(runtime.executionLeaseToken, null);
  }));

test("crash hook is inert outside NODE_ENV=test even with both crash env vars set", { skip: !enabled, timeout: 90000 }, async () => {
  for (const nodeEnv of ["production", "development"]) {
    await withFixture(async (f) => {
      const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
      if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
      const job = jobFor(f, reserve.attempt.id);
      // Both crash-test variables are set, but NODE_ENV is not "test":
      // the hook must not terminate the worker.
      const code = await runChild({ job, containerId, imageId, port: f.port, restartedEpoch }, "after-intent", nodeEnv);
      assert.equal(code, 0, `worker must not crash under NODE_ENV=${nodeEnv}`);
      const settled = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
      assert.equal(settled.status, "SUCCEEDED");
      const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
      assert.equal(latest.generation, 1);
    });
  }
});