import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import test, { after } from "node:test";
import { Queue } from "bullmq";
import {
  prisma, disconnectDatabase, recoverySchemaReady, reserveManualRecovery, reserveAutomaticRecovery,
  type RecoveryScope,
} from "@cloudpilot/database";
import { type RuntimeJob } from "@cloudpilot/queue";
import { processRecoveryJob } from "../src/health/recovery-processor.js";
import { DockerRuntimeExecutor } from "../src/runtime/docker-runtime-client.js";
import {
  publishRecoveryJob, reconcileUnpublishedAutoRecoveries, type RecoveryJobQueue,
} from "../src/health/auto-recovery-publisher.js";

// Isolated automatic-recovery scheduling (Phase 5.8.3).
// Docker operations are MOCKED (no real containers are created or
// restarted); PostgreSQL and Redis/BullMQ are real but isolated to a
// uniquely named test database and queue namespace.
const database = new URL(process.env.DATABASE_URL ?? "http://localhost");
const dbEnabled = /^\/cloudpilot_health_test_[a-z0-9]+$/.test(database.pathname);
const redisEnabled = dbEnabled && !!process.env.REDIS_URL;
const autoExecEnabled = redisEnabled && process.env.AUTO_RECOVERY_ENABLED === "true";
const autoDisabled = dbEnabled && process.env.AUTO_RECOVERY_ENABLED !== "true";
const manualEnabled = dbEnabled && process.env.MANUAL_RECOVERY_ENABLED === "true";

const sourceEpoch = "2026-11-27T01:00:00.123456789Z";
const restartedEpoch = "2026-11-27T01:01:00.987654321Z";
const containerId = "a".repeat(64);
const imageId = `sha256:${"b".repeat(64)}`;

function redisConnection() {
  const url = new URL(process.env.REDIS_URL!);
  return {
    host: url.hostname, port: Number(url.port),
    ...(url.password ? { password: decodeURIComponent(url.password) } : {}),
    maxRetriesPerRequest: null as null,
  };
}

type Fixture = {
  scope: RecoveryScope; actorId: string; runtimeId: string; buildId: string; port: number;
  executor: DockerRuntimeExecutor; calls: () => number;
  setHttp: (value: number) => void; setPort: (value: number) => void;
  queue: Queue; recoveryQueue: RecoveryJobQueue; suffix: string;
};

async function withFixture(run: (f: Fixture) => Promise<void>, withQueue: boolean) {
  assert.equal(await recoverySchemaReady(prisma), true);
  const suffix = randomUUID();
  const actor = await prisma.user.create({ data: { email: `${suffix}@example.invalid` } });
  const workspace = await prisma.workspace.create({ data: { name: "Auto recovery", slug: `auto-${suffix}` } });
  let http = 200;
  const server = createServer((_req, res) => res.writeHead(http).end("probe"));
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const queue = withQueue
    ? new Queue(`auto-recovery-${suffix}`, { connection: redisConnection(), prefix: `cp583:${suffix}` })
    : null;
  const recoveryQueue: RecoveryJobQueue | null = queue
    ? {
        getJob: (id) => queue.getJob(id).then((job) => job ?? null),
        add: (name, payload, opts) => queue.add(name, payload, opts),
      }
    : null;
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const port = address.port;
    const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: "Auto", slug: "auto" } });
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
      ...scope, runtimeInstanceId: runtime.id, generation: 0, status: "UNHEALTHY",
      consecutiveFailures: 3, lastCheckedAt: new Date(), nextCheckAt: new Date(),
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
    assert.ok(queue || recoveryQueue === null);
    await run({
      scope, actorId: actor.id, runtimeId: runtime.id, buildId: build.id, port, executor,
      calls: () => restarts, setHttp: (value) => { http = value; }, setPort: (value) => { exposedPort = value; },
      queue: queue!, recoveryQueue: recoveryQueue!, suffix,
    });
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    if (queue) await queue.close();
    await prisma.auditLog.deleteMany({ where: { workspaceId: workspace.id } });
    await prisma.workspace.delete({ where: { id: workspace.id } });
    await prisma.user.delete({ where: { id: actor.id } });
  }
}

function autoJob(f: { scope: RecoveryScope; runtimeId: string; buildId: string }, id: string, trigger: "MANUAL" | "AUTOMATIC"): RuntimeJob {
  return { ...f.scope, kind: "runtime", action: "recover", recoveryTrigger: trigger, correlationId: id, buildId: f.buildId, runtimeInstanceId: f.runtimeId };
}
const runJob = (job: RuntimeJob, executor: DockerRuntimeExecutor) => processRecoveryJob(`recovery-${job.correlationId}`, job, executor, new AbortController().signal);

test("eligible unhealthy runtime creates a durable AUTOMATIC reservation (idempotent, concurrent-safe)", { skip: !dbEnabled }, () =>
  withFixture(async (f) => {
    // Concurrent reservations create exactly one; the other is rejected.
    const concurrent = await Promise.all([
      reserveAutomaticRecovery(prisma, f.scope, f.runtimeId),
      reserveAutomaticRecovery(prisma, f.scope, f.runtimeId),
    ]);
    assert.equal(concurrent.filter((value) => value.kind === "reserved").length, 1);
    const winner = concurrent.find((value) => value.kind === "reserved");
    assert.ok(winner && (winner.kind === "reserved" || winner.kind === "replay"));
    const id = (winner as { attempt: { id: string } }).attempt.id;
    const reserved = concurrent.find((value) => value.kind === "reserved") as { attempt: { trigger: string; status: string } };
    assert.equal(reserved.attempt.trigger, "AUTOMATIC");
    assert.equal(reserved.attempt.status, "REQUESTED");
    const health = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope });
    assert.equal(health.status, "RECOVERING");

    // Duplicate evaluation replays the same reservation.
    const replay = await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId);
    assert.ok(replay.kind === "busy" || replay.kind === "replay");
    if (replay.kind === "replay") assert.equal(replay.attempt.id, id);
  }, false));

test("healthy, stale, stopped or wrong-generation runtimes are rejected", { skip: !dbEnabled }, () =>
  withFixture(async (f) => {
    await prisma.deploymentHealthState.updateMany({ where: f.scope, data: { status: "HEALTHY" } });
    assert.equal((await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId)).kind, "ineligible");
    await prisma.deploymentHealthState.updateMany({ where: f.scope, data: { status: "UNHEALTHY", lastCheckedAt: new Date(Date.now() - 120_000) } });
    assert.equal((await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId)).kind, "ineligible", "stale observation rejected");
    await prisma.deploymentHealthState.updateMany({ where: f.scope, data: { lastCheckedAt: new Date() } });
    await prisma.deploymentRuntimeInstance.update({ where: { id: f.runtimeId }, data: { status: "STOPPED" } });
    assert.equal((await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId)).kind, "ineligible");
    await prisma.deploymentRuntimeInstance.update({ where: { id: f.runtimeId }, data: { status: "RUNNING" } });
    // A newer runtime attempt makes the candidate's runtime id stale.
    const build = await prisma.deploymentBuild.findFirstOrThrow({ where: f.scope });
    const replacement = await prisma.deploymentRuntimeInstance.create({ data: {
      ...f.scope, buildId: build.id, runtimeAttempt: 2, status: "RUNNING", readyAt: new Date(), containerId, imageId,
      hostPort: f.port, hostBindingAddress: "127.0.0.1", containerPort: 3000,
    } });
    assert.notEqual(replacement.id, f.runtimeId);
    assert.equal((await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId)).kind, "ineligible");
  }, false));

test("manual and automatic reservations contend on the shared budget", { skip: !dbEnabled }, () =>
  withFixture(async (f) => {
    assert.equal((await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID())).kind, "reserved");
    assert.equal((await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId)).kind, "busy");
  }, false));

test("shared budget exhaustion and cooldown reject automatic recovery", { skip: !dbEnabled }, () =>
  withFixture(async (f) => {
    for (let index = 0; index < 3; index++) {
      await prisma.deploymentRecoveryAttempt.create({ data: {
        ...f.scope, runtimeInstanceId: f.runtimeId, generation: 0, action: "RESTART", trigger: "AUTOMATIC",
        idempotencyKey: randomUUID(), status: "FAILED", attemptCount: 1,
        startedAt: new Date(Date.now() - 120_000), finishedAt: new Date(Date.now() - 120_000),
        createdAt: new Date(Date.now() - 120_000), failureReason: "HTTP_STATUS",
      } });
    }
    assert.equal((await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId)).kind, "exhausted");
  }, false));

test("cooldown after a recent failure rejects automatic recovery", { skip: !dbEnabled }, () =>
  withFixture(async (f) => {
    await prisma.deploymentRecoveryAttempt.create({ data: {
      ...f.scope, runtimeInstanceId: f.runtimeId, generation: 0, action: "RESTART", trigger: "AUTOMATIC",
      idempotencyKey: randomUUID(), status: "FAILED", attemptCount: 1,
      startedAt: new Date(), finishedAt: new Date(), createdAt: new Date(), failureReason: "HTTP_STATUS",
    } });
    assert.equal((await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId)).kind, "cooldown");
  }, false));

test("queue publication is idempotent and reconciles after a Redis outage", { skip: !redisEnabled }, () =>
  withFixture(async (f) => {
    const reserve = await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId);
    assert.equal(reserve.kind, "reserved");
    if (reserve.kind !== "reserved") return;
    const input = { ...f.scope, runtimeInstanceId: f.runtimeId, buildId: f.buildId };

    // Redis unavailable: publication fails but the reservation is durable.
    const broken: RecoveryJobQueue = { getJob: async () => { throw new Error("redis down"); }, add: async () => { throw new Error("redis down"); } };
    assert.equal(await publishRecoveryJob(broken, input, reserve.attempt.id, "AUTOMATIC"), false);
    assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).status, "REQUESTED");

    // Idempotent publish (twice) yields a single job.
    assert.equal(await publishRecoveryJob(f.recoveryQueue, input, reserve.attempt.id, "AUTOMATIC"), true);
    assert.equal(await publishRecoveryJob(f.recoveryQueue, input, reserve.attempt.id, "AUTOMATIC"), true);
    assert.ok(await f.queue.getJob(`recovery-${reserve.attempt.id}`));

    // Reconciliation re-publishes any REQUESTED automatic attempt.
    const result = await reconcileUnpublishedAutoRecoveries(f.recoveryQueue, 16);
    assert.ok(result.published >= 1);
    assert.ok(await f.queue.getJob(`recovery-${reserve.attempt.id}`));
  }, true));

test("automatic recovery is cancelled without a restart when disabled before execution", { skip: !autoDisabled || !manualEnabled }, () =>
  withFixture(async (f) => {
    const reserve = await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId);
    assert.equal(reserve.kind, "reserved");
    if (reserve.kind !== "reserved") return;
    const outcome = await runJob(autoJob(f, reserve.attempt.id, "AUTOMATIC"), f.executor);
    assert.equal(outcome.status, "skipped");
    assert.equal(f.calls(), 0, "no Docker restart when auto is disabled");
    const settled = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(settled.status, "CANCELLED");
    const health = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope });
    assert.equal(health.status, "UNHEALTHY");
    assert.ok(health.nextCheckAt);
  }, false));

test("successful automatic recovery uses the existing processor and advances generation", { skip: !autoExecEnabled }, () =>
  withFixture(async (f) => {
    const reserve = await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId);
    assert.equal(reserve.kind, "reserved");
    if (reserve.kind !== "reserved") return;
    const outcome = await runJob(autoJob(f, reserve.attempt.id, "AUTOMATIC"), f.executor);
    assert.equal(outcome.status, "succeeded");
    assert.equal(f.calls(), 1);
    const settled = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(settled.status, "SUCCEEDED");
    assert.equal(settled.trigger, "AUTOMATIC");
    const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
    assert.equal(latest.generation, 1);
    assert.equal(latest.status, "HEALTHY");
  }, false));

test("failed automatic recovery is not reported as success", { skip: !autoExecEnabled }, () =>
  withFixture(async (f) => {
    const reserve = await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId);
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    f.setHttp(500);
    const outcome = await runJob(autoJob(f, reserve.attempt.id, "AUTOMATIC"), f.executor);
    assert.equal(outcome.status, "failed");
    const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
    assert.equal(latest.generation, 0, "no false generation advancement");
    const settled = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(settled.status, "FAILED");
    assert.equal(settled.failureReason, "HTTP_STATUS");
  }, false));

test("ambiguous automatic restart is never repeated and never reported as success", { skip: !autoExecEnabled }, () =>
  withFixture(async (f) => {
    const reserve = await reserveAutomaticRecovery(prisma, f.scope, f.runtimeId);
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    // Restart does not change the container epoch -> ambiguous.
    f.executor.restartOwnedContainer = async () => { /* epoch unchanged */ };
    const outcome = await runJob(autoJob(f, reserve.attempt.id, "AUTOMATIC"), f.executor);
    assert.equal(outcome.status, "failed");
    const settled = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
    assert.equal(settled.failureReason, "RECOVERY_RESTART_UNCERTAIN");
    const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
    assert.equal(latest.generation, 0);
  }, false));

test("stale queue payload is skipped without touching the runtime", { skip: !manualEnabled }, () =>
  withFixture(async (f) => {
    const unknown = randomUUID();
    const outcome = await runJob(autoJob(f, unknown, "MANUAL"), f.executor);
    assert.equal(outcome.status, "skipped");
    assert.equal(f.calls(), 0);
    const wrongId = await processRecoveryJob("not-a-recovery-job", autoJob(f, unknown, "MANUAL"), f.executor, new AbortController().signal);
    assert.equal(wrongId.status, "skipped");
  }, false));

test("existing manual recovery remains compatible", { skip: !manualEnabled }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const outcome = await runJob(autoJob(f, reserve.attempt.id, "MANUAL"), f.executor);
    assert.equal(outcome.status, "succeeded");
    assert.equal(f.calls(), 1);
  }, false));

after(async () => { await disconnectDatabase(); });