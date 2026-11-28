import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
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

// Real BullMQ stalled-job redelivery (Phase 5.7.10). Opt-in, isolated only.
const database = new URL(process.env.DATABASE_URL ?? "http://localhost");
const enabled =
  process.env.CLOUDPILOT_RESILIENCE_FAULT === "1" &&
  process.env.MANUAL_RECOVERY_ENABLED === "true" &&
  /^\/cloudpilot_health_test_[a-z0-9]+$/.test(database.pathname) &&
  !!process.env.REDIS_URL;

const sourceEpoch = "2026-11-27T01:00:00.123456789Z";
const restartedEpoch = "2026-11-27T01:01:00.987654321Z";
const containerId = "a".repeat(64);
const imageId = `sha256:${"b".repeat(64)}`;

function redisConnection() {
  const url = new URL(process.env.REDIS_URL!);
  return { host: url.hostname, port: Number(url.port), maxRetriesPerRequest: null as null };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function withFixture(run: (f: { scope: RecoveryScope; actorId: string; runtimeId: string; buildId: string; port: number }) => Promise<void>) {
  assert.equal(await recoverySchemaReady(prisma), true);
  const suffix = randomUUID();
  const actor = await prisma.user.create({ data: { email: `${suffix}@example.invalid` } });
  const workspace = await prisma.workspace.create({ data: { name: "Stalled", slug: `stalled-${suffix}` } });
  const server = createServer((_req, res) => res.writeHead(200).end("probe"));
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const port = address.port;
    const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: "Stalled", slug: "stalled" } });
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

function jobFor(f: { scope: RecoveryScope; buildId: string; runtimeId: string }, id: string): RuntimeJob {
  return { ...f.scope, kind: "runtime", action: "recover", recoveryTrigger: "MANUAL", correlationId: id, buildId: f.buildId, runtimeInstanceId: f.runtimeId };
}

function parentExecutor(port: number) {
  let startedAt = sourceEpoch;
  const executor = new DockerRuntimeExecutor();
  executor.inspectOwnedContainer = async () => ({ ownershipVerified: true, inspection: {
    exists: true, running: true, containerId, imageId, state: "running", startedAt, restartCount: 0, exitCode: null, hostPort: port } });
  executor.restartOwnedContainer = async () => { startedAt = restartedEpoch; };
  return executor;
}

test("BullMQ stalled-job redelivery reconciles once with no unsafe duplicate restart", { skip: !enabled, timeout: 120000 }, () =>
  withFixture(async (f) => {
    const reserve = await reserveManualRecovery(prisma, f.scope, f.actorId, randomUUID());
    if (reserve.kind !== "reserved") return assert.fail(reserve.kind);
    const job = jobFor(f, reserve.attempt.id);
    const queueName = `recovery-stalled-${randomUUID()}`;
    const connection = redisConnection();
    const queue = new Queue(queueName, { connection, lockDuration: 3_000, stalledInterval: 1_000, maxStalledCount: 1 });

    let child: ChildProcess | undefined;
    let worker: Worker | undefined;
    try {
      await queue.add("recover", job, { jobId: `recovery-${job.correlationId}` });

      // Start a worker that will exceed the lock and be killed → stalled.
      child = spawn(process.execPath, ["--import", "tsx", "test/helpers/stalled-worker-child.ts"], {
        cwd: process.cwd(),
        env: { ...process.env, CLOUDPILOT_STALLED_INPUT: JSON.stringify({ queueName, connection, containerId, imageId, port: f.port, restartedEpoch }) },
        stdio: "ignore",
      });
      await sleep(1_500);
      child.kill();

      // Replacement worker processes the redelivered job.
      worker = new Worker(queueName, async (bullJob) => {
        await processRecoveryJob(bullJob.id, bullJob.data as RuntimeJob, parentExecutor(f.port), new AbortController().signal);
      }, { connection, lockDuration: 3_000 });

      let settled = false;
      for (let i = 0; i < 40; i++) {
        const attempt = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
        if (attempt.status === "SUCCEEDED" || attempt.status === "FAILED") { settled = true; break; }
        await sleep(500);
      }
      assert.equal(settled, true, "redelivered job must settle");
      const record = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: reserve.attempt.id } });
      assert.equal(record.status, "SUCCEEDED");
      const latest = await prisma.deploymentHealthState.findFirstOrThrow({ where: f.scope, orderBy: { generation: "desc" } });
      assert.equal(latest.generation, 1, "exactly one generation increment");
      const runtime = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: { id: f.runtimeId } });
      assert.equal(runtime.restartCount, 1, "no unsafe duplicate restart after redelivery");
    } finally {
      child?.kill();
      await worker?.close().catch(() => {});
      await queue.close().catch(() => {});
    }
  }));

test.after(async () => { if (enabled) await disconnectDatabase(); });