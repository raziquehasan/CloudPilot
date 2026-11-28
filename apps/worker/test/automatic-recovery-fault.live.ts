import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { parseEnv } from "node:util";

// Phase 5.8.6.B live fault injection. Reuses the proven 5.8.6.A harness
// isolation and adds a single, reversible interruption of the disposable
// Redis (b1) or PostgreSQL (b2) service at a real recovery boundary.
const mode = process.argv[2];
assert.ok(mode === "b1" || mode === "b2", "Pass b1 or b2 explicitly");
assert.equal(process.env.CLOUDPILOT_RUN_LIVE_RECOVERY, "1");
const boundary = mode === "b1" ? "after-intent" : "after-restart";
const local = parseEnv(readFileSync(".env", "utf8"));
assert.equal(local.AUTO_RECOVERY_ENABLED, "false");
const repository = local.BUILD_TRUSTED_REPOSITORIES?.split(",").map((name) => name.trim().toLowerCase()).find((name) => name.endsWith("/cloudpilot-recovery-validation"));
assert.match(repository ?? "", /^[a-z0-9_.-]+\/[a-z0-9_.-]+$/);
const run = `cp586b-${mode}-${randomUUID()}`;
const database = `cloudpilot_health_test_${run.replaceAll("-", "")}`;
const workspaceRoot = mkdtempSync(resolve(tmpdir(), `${run}-`));
const containers: string[] = [];
const docker = (...args: string[]) => execFileSync("docker", args, {
  encoding: "utf8", timeout: 60_000, stdio: ["ignore", "pipe", "pipe"],
}).trim();
function runContainer(...args: string[]) {
  const id = docker("run", "-d", "--label", `cloudpilot.validation=${run}`, ...args);
  assert.match(id, /^[a-f0-9]{64}$/);
  containers.push(id);
  return id;
}
const poll = async <T>(label: string, timeout: number, check: () => Promise<T | undefined>, interval = 500): Promise<T> => {
  const start = performance.now();
  while (performance.now() - start < timeout) {
    const result = await check();
    if (result !== undefined) return result;
    await delay(interval);
  }
  throw new Error(`Timed out waiting for ${label}`);
};
function port(id: string, internal: number) {
  const binding = docker("port", id, `${internal}/tcp`);
  assert.match(binding, /^127\.0\.0\.1:\d+$/);
  const value = Number(binding.split(":")[1]);
  assert.ok(![3000, 8000, 5433, 6379, 53143, 63299].includes(value));
  return value;
}
async function http(hostPort: number, path = "/health", method = "GET") {
  try {
    const response = await fetch(`http://127.0.0.1:${hostPort}${path}`, { method, signal: AbortSignal.timeout(1500) });
    await response.text();
    return response.status;
  } catch { return undefined; }
}
async function unusedPort() {
  const socket = createServer();
  await new Promise<void>((resolve, reject) => socket.once("error", reject).listen(0, "127.0.0.1", resolve));
  const address = socket.address();
  assert.ok(address && typeof address !== "string");
  await new Promise<void>((resolve, reject) => socket.close((error) => error ? reject(error) : resolve()));
  assert.ok(![3000, 8000, 5433, 6379, 53143, 63299].includes(address.port));
  return address.port;
}
const inspect = (id: string) => JSON.parse(docker("inspect", id))[0] as {
  Id: string; Image: string; State: { StartedAt: string };
  NetworkSettings: { Ports: Record<string, { HostIp: string; HostPort: string }[]> };
};
const protectedSnapshot = () => {
  const rows = docker("ps", "--format", "{{.Names}} {{.RunningFor}}").split(/\r?\n/).filter(Boolean);
  return rows.filter((row) => /^(cloudpilot-postgres|cloudpilot-redis)\b/.test(row));
};
const workerLogs: Array<Record<string, unknown>> = [];
let worker: ReturnType<typeof spawn> | undefined;
let disconnect: (() => Promise<void>) | undefined;
let closeQueue: (() => Promise<void>) | undefined;
console.log(JSON.stringify({ run, mode, event: "start", boundary }));
try {
  const protectedBefore = protectedSnapshot();
  const password = randomBytes(24).toString("hex");
  process.env.POSTGRES_PASSWORD = password;
  const pg = runContainer("--name", `${run}-pg`, "-e", "POSTGRES_PASSWORD", "-e", `POSTGRES_DB=${database}`, "-p", "127.0.0.1::5432", "postgres:16");
  delete process.env.POSTGRES_PASSWORD;
  const redis = runContainer("--name", `${run}-redis`, "-p", "127.0.0.1::6379", "redis:7");
  const pgPort = port(pg, 5432);
  const redisPort = port(redis, 6379);
  const databaseUrl = new URL(`postgresql://postgres:${password}@127.0.0.1:${pgPort}/${database}`);
  const redisUrl = new URL(`redis://127.0.0.1:${redisPort}/0`);
  assert.equal(databaseUrl.host, docker("port", pg, "5432/tcp"));
  assert.equal(redisUrl.host, docker("port", redis, "6379/tcp"));
  assert.equal(databaseUrl.pathname, `/${database}`);
  process.env.DATABASE_URL = databaseUrl.href;
  process.env.REDIS_URL = redisUrl.href;
  await poll("disposable database readiness", 30_000, async () => {
    try { docker("exec", pg, "pg_isready", "-U", "postgres", "-d", database); return true; }
    catch { return undefined; }
  });
  execFileSync(process.execPath, ["../../node_modules/prisma/build/index.js", "migrate", "deploy"], {
    cwd: "packages/database", timeout: 90_000, stdio: ["ignore", "pipe", "pipe"],
  });
  const { prisma, disconnectDatabase, recoverySchemaReady, inspectAutomaticRecoveryCircuit, recoveryNow } = await import("@cloudpilot/database");
  disconnect = disconnectDatabase;
  assert.ok(await recoverySchemaReady(prisma));
  const { buildOwnershipLabels } = await import("@cloudpilot/queue");
  const { Queue } = await import("bullmq");
  const queue = new Queue("runtime", { prefix: run, connection: { host: "127.0.0.1", port: redisPort } });
  closeQueue = () => queue.close();
  const workspace = await prisma.workspace.create({ data: { name: run, slug: run } });
  const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: run, slug: run } });
  await prisma.repository.create({ data: {
    workspaceId: workspace.id, projectId: project.id, provider: "github", externalId: run, name: run, fullName: repository!,
  } });
  const deployment = await prisma.deployment.create({ data: {
    workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT", triggerSource: "MANUAL", status: "RUNNING",
  } });
  const scope = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
  const image = docker("image", "inspect", "--format", "{{.Id}}", "node:22-alpine");
  const build = await prisma.deploymentBuild.create({ data: {
    ...scope, sourcePreparationId: randomUUID(), resolvedCommitSha: "c".repeat(40), status: "SUCCEEDED", imageId: image,
  } });
  const runtimeId = randomUUID();
  const runtimeScope = { ...scope, runtimeInstanceId: runtimeId };
  const labels = buildOwnershipLabels({ ...runtimeScope, attempt: 1 });
  const hostPort = await unusedPort();
  const app = runContainer("--name", `cloudpilot-${project.id}-${deployment.id}-1`,
    "--restart", "no", "--memory", "512m", "--cpus", "1", "--pids-limit", "128", "--security-opt", "no-new-privileges",
    "--cap-drop", "ALL", "-p", `127.0.0.1:${hostPort}:3000`, ...Object.entries(labels).flatMap(([key, value]) => ["--label", `${key}=${value}`]),
    image, "node", "-e", `let healthy=true; require('node:http').createServer((req,res)=>{
      if(req.method==='POST' && req.url==='/fail') { healthy=false; return res.writeHead(204).end(); }
      return res.writeHead(req.url==='/health' ? (healthy?200:503) : 404).end();
    }).listen(3000,'0.0.0.0')`);
  assert.equal(port(app, 3000), hostPort);
  const before = inspect(app);
  const eventStart = Math.floor(Date.now() / 1000);
  await poll("application ready", 20_000, async () => await http(hostPort) === 200 ? true : undefined);
  await prisma.deploymentRuntimeInstance.create({ data: {
    id: runtimeId, ...scope, buildId: build.id, status: "RUNNING", imageId: image, containerId: app,
    hostPort, hostBindingAddress: "127.0.0.1", containerPort: 3000, readyAt: new Date(before.State.StartedAt),
  } });
  await prisma.deploymentHealthState.create({ data: { ...runtimeScope, generation: 0, status: "STARTING", nextCheckAt: new Date() } });
  console.log(JSON.stringify({ run, mode, event: "fixture", pgPort, redisPort, hostPort, container: app, image, startedAt: before.State.StartedAt, runtimeId, ...scope }));

  worker = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "--eval",
    "const r = await import('./apps/worker/src/health/recovery-processor.ts'); process.on('message', (m) => { if (m === 'shutdown') process.emit('SIGTERM'); if (m === 'release-fault') r.releaseRecoveryFault(); }); await import('./apps/worker/src/worker.ts');"],
  { stdio: ["ignore", "pipe", "pipe", "ipc"], env: {
    ...process.env, NODE_ENV: "test", QUEUE_PREFIX: run, SOURCE_WORKSPACE_ROOT: workspaceRoot,
    RUNTIME_ENABLED: "true", RUNTIME_TRUSTED_LOCAL_ONLY: "true", RUNTIME_BIND_HOST: "127.0.0.1",
    BUILD_TRUSTED_REPOSITORIES: repository!, BUILD_ENABLED: "false",
    HEALTH_PERSISTENCE_ENABLED: "true", HEALTH_MONITORING_ENABLED: "true", MANUAL_RECOVERY_ENABLED: "true",
    AUTO_RECOVERY_ENABLED: "true", DEPLOYMENT_LOGS_ENABLED: "false", LOG_RUNTIME_COLLECTION_ENABLED: "false",
    LOG_STREAMING_ENABLED: "false", LOG_LEVEL: "debug",
    CLOUDPILOT_RECOVERY_FAULT_TEST: "1", CLOUDPILOT_RECOVERY_FAULT_BOUNDARY: boundary,
  } });
  const exit = new Promise<number | null>((resolve) => worker!.once("exit", resolve));
  for (const stream of [worker.stdout, worker.stderr]) {
    let remaining = "";
    stream?.setEncoding("utf8").on("data", (part: string) => {
      const lines = (remaining + part).split("\n");
      remaining = lines.pop() ?? "";
      for (const line of lines) {
        try { workerLogs.push(JSON.parse(line)); }
        catch { /* structured only */ }
      }
    });
  }
  const checkWorker = () => { if (worker!.exitCode !== null) throw new Error(`Worker exited early with code ${worker!.exitCode}`); };
  const healthy = (generation: number) => prisma.deploymentHealthCheck.count({ where: { ...runtimeScope, generation, httpStatus: 200, httpResult: "PASSED" } });
  await poll("worker baseline healthy probe", 70_000, async () => { checkWorker(); return await healthy(0) >= 1 ? true : undefined; });
  assert.equal(await http(hostPort, "/fail", "POST"), 204);
  assert.equal(await http(hostPort), 503);
  const injected = Date.now();
  const failedChecks = () => prisma.deploymentHealthCheck.count({ where: { ...runtimeScope, generation: 0, httpStatus: 503 } });

  const waitForPause = () => poll("worker fault pause", 90_000, async () => {
    checkWorker();
    return workerLogs.find((log) => log.msg === "Recovery fault pause (test only)" && log.stage === boundary) ? true : undefined;
  }, 100);

  let faultStart = 0;
  let faultEnd = 0;
  let intentAt: string | null = null;
  let afterRestart: ReturnType<typeof inspect> | undefined;

  if (mode === "b1") {
    await waitForPause();
    const attempt = await prisma.deploymentRecoveryAttempt.findFirstOrThrow({ where: scope, orderBy: { createdAt: "desc" } });
    assert.ok(attempt.restartIssuedAt, "durable restart intent must exist before the fault");
    intentAt = attempt.restartIssuedAt.toISOString();
    docker("pause", redis);
    faultStart = Date.now();
    await delay(2_000);
    worker!.send("release-fault");
    // Docker restart now runs while Redis is paused; wait for DB settlement.
    await poll("automatic recovery settlement (b1)", 90_000, async () => {
      checkWorker();
      const a = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: attempt.id } });
      return a.status === "SUCCEEDED" || a.status === "FAILED" || a.status === "CANCELLED" ? a : undefined;
    }, 200);
    afterRestart = inspect(app);
    await delay(2_000);
    docker("unpause", redis);
    faultEnd = Date.now();
  } else {
    await waitForPause();
    afterRestart = inspect(app);
    assert.ok(Date.parse(afterRestart.State.StartedAt) > Date.parse(before.State.StartedAt), "Docker restart must have completed before the fault");
    const attempt = await prisma.deploymentRecoveryAttempt.findFirstOrThrow({ where: scope, orderBy: { createdAt: "desc" } });
    assert.ok(attempt.restartIssuedAt, "durable restart intent must exist before the fault");
    intentAt = attempt.restartIssuedAt.toISOString();
    docker("pause", pg);
    faultStart = Date.now();
    await delay(2_000);
    worker!.send("release-fault");
    // PostgreSQL is paused; the worker must not restart again or fabricate success.
    await delay(3_000);
    assert.equal(inspect(app).State.StartedAt, afterRestart.State.StartedAt, "no second Docker restart while PostgreSQL is paused");
    docker("unpause", pg);
    faultEnd = Date.now();
  }

  const attempt = await poll("automatic recovery settlement (final)", 120_000, async () => {
    checkWorker();
    const a = await prisma.deploymentRecoveryAttempt.findUniqueOrThrow({ where: { id: (await prisma.deploymentRecoveryAttempt.findFirstOrThrow({ where: scope, orderBy: { createdAt: "desc" } })).id } });
    return a.status === "SUCCEEDED" || a.status === "FAILED" || a.status === "CANCELLED" ? a : undefined;
  }, 200);
  assert.equal(attempt.status, "SUCCEEDED", `recovery must succeed after restore, got ${attempt.status}/${attempt.failureReason}`);
  assert.equal(attempt.trigger, "AUTOMATIC");
  assert.equal(attempt.attemptCount, 1);
  assert.ok(attempt.restartIssuedAt && attempt.restartSourceStartedAt);
  assert.equal(attempt.restartSourceStartedAt, before.State.StartedAt);
  assert.equal(attempt.leaseToken, null);
  assert.equal(attempt.leaseExpiresAt, null);
  assert.equal(attempt.generation, 0);
  const attemptsAll = await prisma.deploymentRecoveryAttempt.findMany({ where: scope });
  assert.equal(attemptsAll.length, 1, "no duplicate reservation");
  const health = await prisma.deploymentHealthState.findMany({ where: runtimeScope, orderBy: { generation: "desc" } });
  assert.deepEqual(health.map((h) => h.generation), [1, 0]);
  assert.equal(health[0]!.status, "HEALTHY");
  assert.ok(health.every((h) => h.leaseToken === null && h.leaseExpiresAt === null));
  const runtime = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: { id: runtimeId } });
  assert.equal(runtime.executionLeaseToken, null);
  assert.equal(runtime.containerId, before.Id);
  assert.equal(runtime.imageId, before.Image);
  assert.equal(runtime.hostPort, hostPort);
  const after = inspect(app);
  assert.ok(Date.parse(after.State.StartedAt) > Date.parse(before.State.StartedAt));
  assert.equal(after.Id, before.Id);
  assert.equal(after.Image, before.Image);
  assert.deepEqual(after.NetworkSettings.Ports, before.NetworkSettings.Ports);
  assert.equal(await http(hostPort), 200);

  const restartEvents = docker("events", "--since", String(eventStart), "--until", String(Math.ceil(Date.now() / 1000)),
    "--filter", `container=${app}`, "--filter", "event=restart", "--format", "{{.Time}} {{.Action}}")
    .split(/\r?\n/).filter(Boolean).map((line) => Number(line.split(" ")[0]));
  assert.equal(restartEvents.length, 1, "exactly one Docker restart event");
  const restartAt = restartEvents[0]! * 1000;
  // Mode-specific overlap proof.
  if (mode === "b1") {
    // Redis paused before issuance and held through completion: the single
    // Docker restart must fall inside the Redis fault window.
    assert.ok(restartAt >= faultStart && restartAt <= faultEnd, "Docker restart must occur within the Redis fault window");
  } else {
    // PostgreSQL paused only after the restart completed and before
    // finalization committed.
    assert.ok(restartAt < faultStart, "Docker restart must complete before the PostgreSQL fault");
    assert.ok(attempt.finishedAt!.getTime() > faultEnd, "finalization must commit after PostgreSQL restore");
  }

  const jobId = `recovery-${attempt.id}`;
  const job = await poll("BullMQ terminal state", 30_000, async () => {
    const j = await queue.getJob(jobId);
    const state = await j?.getState();
    return j && (state === "completed" || state === "failed") ? { j, state } : undefined;
  }, 300);
  const circuit = await inspectAutomaticRecoveryCircuit(prisma, scope, await recoveryNow(prisma));
  const protectedAfter = protectedSnapshot();
  assert.deepEqual(protectedAfter, protectedBefore, "protected Redis/Postgres containers must be unchanged");

  const redisState = docker("inspect", "--format", "{{.State.Status}}", redis);
  const pgState = docker("inspect", "--format", "{{.State.Status}}", pg);
  assert.equal(redisState, "running");
  assert.equal(pgState, "running");

  console.log(JSON.stringify({ run, mode, event: "passed", recoveryId: attempt.id, jobId,
    jobState: job.state, attemptsStarted: job.j.attemptsStarted, attemptsMade: job.j.attemptsMade,
    faultStart: new Date(faultStart).toISOString(), faultEnd: new Date(faultEnd).toISOString(),
    faultWindowMs: faultEnd - faultStart, restartAt: new Date(restartAt).toISOString(),
    finishedAt: attempt.finishedAt!.toISOString(),
    intentAt, beforeStartedAt: before.State.StartedAt, afterStartedAt: after.State.StartedAt,
    sameContainer: after.Id === before.Id, sameImage: after.Image === before.Image, hostPort,
    generation: [0, 1], restartEvents: restartEvents.length, failureReason: attempt.failureReason,
    remainingBudget: circuit.remainingBudget, circuit: circuit.state, health: health[0]!.status,
    redisState, pgState, protectedUnchanged: true, httpStatus: 200,
    injectionToSettlementMs: attempt.finishedAt!.getTime() - injected,
  }));

  await new Promise<void>((resolve, reject) => worker!.send("shutdown", (error) => error ? reject(error) : resolve()));
  assert.equal(await Promise.race([exit, delay(20_000, "timeout", { ref: false })]), 0);
} catch (error) {
  const message = error instanceof Error ? error.message.split("\n")[0]! : "Unknown failure";
  console.error(JSON.stringify({ run, mode, event: "failed", message: message.replace(/[a-z]+:\/\/[^\s]+/gi, "[endpoint]"),
    worker: workerLogs.slice(-40).map(({ msg, event, reserved, published, reasons, failureReason, code, stage }) =>
      ({ msg, event, reserved, published, reasons, failureReason, code, stage })),
  }));
  process.exitCode = 1;
} finally {
  // Always restore any paused disposable service before removal.
  for (const id of containers) {
    try { docker("unpause", id); } catch { /* not paused */ }
  }
  if (worker && worker.exitCode === null && worker.signalCode === null) {
    const exited = new Promise<void>((resolve) => worker!.once("exit", () => resolve()));
    if (worker.connected) worker.send("shutdown", () => undefined);
    if (await Promise.race([exited.then(() => false), delay(20_000, true, { ref: false })])) {
      worker.kill();
      await exited;
    }
  }
  await closeQueue?.();
  await disconnect?.();
  for (const id of containers.reverse()) {
    assert.equal(docker("inspect", "--format", '{{index .Config.Labels "cloudpilot.validation"}}', id), run);
    docker("rm", "--force", "--volumes", id);
  }
  assert.equal(dirname(workspaceRoot), resolve(tmpdir()));
  rmSync(workspaceRoot, { force: true, recursive: true });
  console.log(JSON.stringify({ run, mode, event: "cleanup", workerExit: worker?.exitCode, containersRemaining: docker("ps", "-aq", "--filter", `label=cloudpilot.validation=${run}`) }));
}