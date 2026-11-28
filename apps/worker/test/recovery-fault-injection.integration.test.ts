import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { createServer } from "node:http";
import test from "node:test";
import { Queue, Worker } from "bullmq";
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

// Real fault injection (Phase 5.7.9). Opt-in, isolated only.
const database = new URL(process.env.DATABASE_URL ?? "http://localhost");
const enabled =
  process.env.CLOUDPILOT_RESILIENCE_FAULT === "1" &&
  process.env.MANUAL_RECOVERY_ENABLED === "true" &&
  /^\/cloudpilot_health_test_[a-z0-9]+$/.test(database.pathname) &&
  !!process.env.REDIS_URL;

const PG_CONTAINER = "cloudpilot-resilience-579-postgres";
const REDIS_CONTAINER = "cloudpilot-resilience-579-redis";
const sourceEpoch = "2026-11-27T01:00:00.123456789Z";
const restartedEpoch = "2026-11-27T01:01:00.987654321Z";
const containerId = "a".repeat(64);
const imageId = `sha256:${"b".repeat(64)}`;

const docker = (...args: string[]) =>
  execFileSync("docker", args, { encoding: "utf8", timeout: 60000 }).trim();

function redisConnection() {
  const url = new URL(process.env.REDIS_URL!);
  return { host: url.hostname, port: Number(url.port), maxRetriesPerRequest: null as null };
}

type Fixture = {
  scope: RecoveryScope; actorId: string; runtimeId: string; buildId: string; port: number;
  executor: DockerRuntimeExecutor; calls: () => number;
};

async function withFixture(run: (f: Fixture) => Promise<void>) {
  assert.equal(await recoverySchemaReady(prisma), true);
  const suffix = randomUUID();
  const actor = await prisma.user.create({ data: { email: `${suffix}@example.invalid` } });
  const workspace = await prisma.workspace.create({ data: { name: "Fault", slug: `fault-${suffix}` } });
  let restarts = 0;
  let startedAt = sourceEpoch;
  const server = createServer((_req, res) => res.writeHead(200).end("probe"));
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const port = address.port;
    const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: "Fault", slug: "fault" } });
    await prisma.repository.create({ data: { workspaceId: workspace.id, projectId: project.id, provider: "github", externalId: randomUUID(), name: "health", fullName: "example/health-probe-test" } });
    const deployment = await prisma.deployment.create({ data: { workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT", triggerSource: "MANUAL", status: "RUNNING" } });
    const scope = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
    const build = await prisma.deploymentBuild.create({ data: { ...scope, sourcePreparationId: randomUUID(), resolvedCommitSha: "c".repeat(40), status: "SUCCEEDED", imageId } });
    const runtime = await prisma.deploymentRuntimeInstance.create({ data: { ...scope, buildId: build.id, status: "RUNNING", readyAt: new Date(sourceEpoch), containerId, imageId, hostPort: port, hostBindingAddress: "127.0.0.1", containerPort: 3000 } });
    await prisma.deploymentHealthState.create({ data: { ...scope, runtimeInstanceId: runtime.id, generation: 0, status: "UNHEALTHY", consecutiveFailures: 3, nextCheckAt: new Date() } });
    const executor = new DockerRuntimeExecutor();
    executor.inspectOwnedContainer = async (id, labels, containerPort, host) => {
      assert.equal(id, containerId);
      assert.equal(labels["cloudpilot.runtimeInstanceId"], runtime.id);
      assert.equal(containerPort, 3000);
      assert.equal(host, "127.0.0.1");
      return { ownershipVerified: true, inspection: { exists: true, running: true, containerId, imageId, state: "running", startedAt, restartCount: restarts, exitCode: null, hostPort: port } };
    };
    executor.restartOwnedContainer = async (id, _labels, target) => {
      assert.equal(id, containerId);
      assert.equal(target.hostPort, port);
      assert.equal(target.startedAt, sourceEpoch);
      restarts += 1; startedAt = restartedEpoch;
    };
    await run({ scope, actorId: actor.id, runtimeId: runtime.id, buildId: build.id, port, executor, calls: () => restarts });
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

// Postgres interruption runs first, while the pool is fresh.
test("PostgreSQL interruption prevents lease acquisition; reconnect reconciles safely", { skip: !enabled, timeout: 60000 }, () =>
  withFixture(async (f) => {
    docker("pause", PG_CONTAINER);
    let blocked = false;
    try {
      await Promise.race([
        prisma.$queryRaw`SELECT 1`,
        new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 6_000)),
      ]);
    } catch { blocked = true; }
    assert.equal(blocked, true, "database must be unreachable while paused");
    assert.equal(f.calls(), 0, "no restart without durable authorization");
    docker("unpause", PG_CONTAINER);
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    assert.equal(reserve.kind, "reserved");
    if (reserve.kind !== "reserved") return;
    const outcome = await processRecoveryJob(`recovery-${reserve.attempt.id}`, jobFor(f, reserve.attempt.id), f.executor, new AbortController().signal);
    assert.equal(outcome.status, "succeeded");
    assert.equal(f.calls(), 1);
    assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).status, "SUCCEEDED");
  }));

test("Redis interruption blocks publication; reconnect restores durable processing", { skip: !enabled, timeout: 60000 }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const queueName = `recovery-redis-${randomUUID()}`;
    const connection = redisConnection();
    const queue = new Queue(queueName, { connection });
    try {
      docker("pause", REDIS_CONTAINER);
      let blocked = false;
      try {
        await Promise.race([
          queue.add("recover", jobFor(f, reserve.attempt.id), { jobId: `recovery-${reserve.attempt.id}` }),
          new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 6_000)),
        ]);
      } catch { blocked = true; }
      assert.equal(blocked, true, "publication must fail while Redis is paused");
      const attempt = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
      assert.equal(attempt.status, "REQUESTED");
      assert.equal(f.calls(), 0);
    } finally {
      docker("unpause", REDIS_CONTAINER);
      await queue.close().catch(() => {});
    }
    const queue2 = new Queue(queueName, { connection });
    const worker = new Worker(queueName, async (bullJob) => {
      await processRecoveryJob(bullJob.id, bullJob.data as RuntimeJob, f.executor, new AbortController().signal);
    }, { connection, concurrency: 1 });
    try {
      await queue2.add("recover", jobFor(f, reserve.attempt.id), { jobId: `recovery-${reserve.attempt.id}` });
      await new Promise((resolve) => setTimeout(resolve, 1_500));
      await worker.close();
      await queue2.close();
      assert.equal(f.calls(), 1);
      assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).status, "SUCCEEDED");
    } finally {
      await worker.close().catch(() => {});
      await queue2.close().catch(() => {});
    }
  }));

test("real BullMQ duplicate delivery yields one restart and one generation increment", { skip: !enabled, timeout: 60000 }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    const queueName = `recovery-dup-${randomUUID()}`;
    const connection = redisConnection();
    const queue = new Queue(queueName, { connection });
    const worker = new Worker(queueName, async (bullJob) => {
      await processRecoveryJob(bullJob.id, bullJob.data as RuntimeJob, f.executor, new AbortController().signal);
    }, { connection, concurrency: 1 });
    try {
      await queue.add("recover", job, { jobId: `recovery-${job.correlationId}` });
      await queue.add("recover", job, { jobId: `recovery-${job.correlationId}` });
      const counts = await queue.getJobCounts();
      assert.equal(counts.completed + counts.failed + counts.active + counts.waiting + counts.delayed, 1, "BullMQ dedups by job id");
      await new Promise((resolve) => setTimeout(resolve, 1_500));
      assert.equal(f.calls(), 1);
      const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
      assert.equal(latest.generation, 1);
      assert.equal((await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } })).status, "SUCCEEDED");
    } finally {
      await worker.close().catch(() => {});
      await queue.close().catch(() => {});
    }
  }));

test.after(async () => { if (enabled) await disconnectDatabase(); });