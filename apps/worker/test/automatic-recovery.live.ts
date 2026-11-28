import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { parseEnv } from "node:util";

const mode = process.argv[2];
assert.ok(mode === "enabled" || mode === "disabled", "Pass enabled or disabled explicitly");
assert.equal(process.env.CLOUDPILOT_RUN_LIVE_RECOVERY, "1");
const enabled = mode === "enabled";
const local = parseEnv(readFileSync(".env", "utf8"));
assert.equal(local.AUTO_RECOVERY_ENABLED, "false");
const repository = local.BUILD_TRUSTED_REPOSITORIES?.split(",").map((name) => name.trim().toLowerCase()).find((name) => name.endsWith("/cloudpilot-recovery-validation"));
assert.match(repository ?? "", /^[a-z0-9_.-]+\/[a-z0-9_.-]+$/);
const run = `cp586a-${randomUUID()}`;
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
const workerLogs: Array<Record<string, unknown>> = [];
let worker: ReturnType<typeof spawn> | undefined;
let disconnect: (() => Promise<void>) | undefined;
let closeQueue: (() => Promise<void>) | undefined;
console.log(JSON.stringify({ run, mode, event: "start" }));
try {
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

  // Only the child receives the enabled switch. Its IPC handler invokes the worker's existing graceful shutdown path.
  assert.equal(databaseUrl.host, docker("port", pg, "5432/tcp"));
  assert.equal(redisUrl.host, docker("port", redis, "6379/tcp"));
  worker = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "--eval",
    "process.on('message', (message) => { if (message === 'shutdown') process.emit('SIGTERM'); }); await import('./apps/worker/src/worker.ts');"],
  { stdio: ["ignore", "pipe", "pipe", "ipc"], env: {
    ...process.env, NODE_ENV: "test", QUEUE_PREFIX: run, SOURCE_WORKSPACE_ROOT: workspaceRoot,
    RUNTIME_ENABLED: "true", RUNTIME_TRUSTED_LOCAL_ONLY: "true", RUNTIME_BIND_HOST: "127.0.0.1",
    BUILD_TRUSTED_REPOSITORIES: repository!, BUILD_ENABLED: "false",
    HEALTH_PERSISTENCE_ENABLED: "true", HEALTH_MONITORING_ENABLED: "true", MANUAL_RECOVERY_ENABLED: "true",
    AUTO_RECOVERY_ENABLED: String(enabled), DEPLOYMENT_LOGS_ENABLED: "false", LOG_RUNTIME_COLLECTION_ENABLED: "false",
    LOG_STREAMING_ENABLED: "false", LOG_LEVEL: "debug",
  } });
  const exit = new Promise<number | null>((resolve) => worker!.once("exit", resolve));
  for (const stream of [worker.stdout, worker.stderr]) {
    let remaining = "";
    stream?.setEncoding("utf8").on("data", (part: string) => {
      const lines = (remaining + part).split("\n");
      remaining = lines.pop() ?? "";
      for (const line of lines) {
        try { workerLogs.push(JSON.parse(line)); }
        catch { /* Only structured worker evidence is emitted. */ }
      }
    });
  }
  const healthy = (generation: number) => prisma.deploymentHealthCheck.count({
    where: { ...runtimeScope, generation, httpStatus: 200, httpResult: "PASSED" },
  });
  const checkWorker = () => {
    if (worker!.exitCode !== null) throw new Error(`Worker exited early with code ${worker!.exitCode}`);
  };
  await poll("worker baseline healthy probe", 70_000, async () => {
    checkWorker(); return await healthy(0) >= 1 ? true : undefined;
  });
  assert.equal(await http(hostPort, "/fail", "POST"), 204);
  assert.equal(await http(hostPort), 503);
  const injected = Date.now();
  const failedChecks = () => prisma.deploymentHealthCheck.count({ where: { ...runtimeScope, generation: 0, httpStatus: 503 } });
  const seen = new Set<string>();
  const sample = async () => {
    checkWorker();
    const [health, attempts] = await Promise.all([
      prisma.deploymentHealthState.findMany({ where: runtimeScope, orderBy: { generation: "desc" } }),
      prisma.deploymentRecoveryAttempt.findMany({ where: scope }),
    ]);
    if (health[0]) seen.add(health[0].status);
    if (attempts[0]) seen.add(attempts[0].status);
    return { health, attempts };
  };
  if (enabled) {
    const result = await poll("automatic recovery settlement", 120_000, async () => {
      const sampleResult = await sample();
      const attempt = sampleResult.attempts[0];
      if (attempt?.status === "FAILED" || attempt?.status === "CANCELLED") {
        throw new Error(`Automatic recovery ${attempt.status} ${attempt.failureReason}`);
      }
      return attempt?.status === "SUCCEEDED" ? sampleResult : undefined;
    }, 100);
    assert.ok(await failedChecks() >= 3);
    const attempt = result.attempts[0]!;
    assert.equal(result.attempts.length, 1);
    assert.equal(attempt.trigger, "AUTOMATIC");
    assert.equal(attempt.action, "RESTART");
    assert.equal(attempt.attemptCount, 1);
    assert.ok(attempt.restartIssuedAt && attempt.restartSourceStartedAt);
    assert.equal(attempt.restartSourceStartedAt, before.State.StartedAt);
    assert.equal(attempt.leaseToken, null);
    assert.equal(attempt.leaseExpiresAt, null);
    assert.equal(attempt.leaseOwnerId, null);
    assert.equal(attempt.generation, 0);
    assert.ok(attempt.healthVersion !== null);
    assert.deepEqual(result.health.map((state) => state.generation), [1, 0]);
    assert.ok(result.health.every((state) => state.leaseToken === null && state.leaseExpiresAt === null));
    assert.equal(result.health[0]!.status, "HEALTHY");
    assert.equal(result.health[0]!.consecutiveFailures, 0);
    const runtime = await prisma.deploymentRuntimeInstance.findUniqueOrThrow({ where: { id: runtimeId } });
    assert.equal(runtime.executionLeaseToken, null);
    assert.equal(runtime.executionLeaseExpiresAt, null);
    assert.equal(runtime.containerId, before.Id);
    assert.equal(runtime.imageId, before.Image);
    assert.equal(runtime.hostPort, hostPort);
    const circuit = await inspectAutomaticRecoveryCircuit(prisma, scope, await recoveryNow(prisma));
    assert.equal(circuit.remainingBudget, 2);
    const audit = await prisma.auditLog.findFirstOrThrow({
      where: { workspaceId: workspace.id, entityId: deployment.id, action: "DEPLOYMENT_RECOVERY_REQUESTED" },
    });
    assert.equal((audit.metadata as { automaticCircuitState: string }).automaticCircuitState, "CLOSED");
    const jobId = `recovery-${attempt.id}`;
    const job = await poll("BullMQ completion", 15_000, async () => {
      const job = await queue.getJob(jobId);
      return await job?.getState() === "completed" ? job : undefined;
    });
    assert.equal(job.data.recoveryTrigger, "AUTOMATIC");
    const after = inspect(app);
    assert.ok(Date.parse(after.State.StartedAt) > Date.parse(before.State.StartedAt));
    assert.equal(after.Id, before.Id);
    assert.equal(after.Image, before.Image);
    assert.deepEqual(after.NetworkSettings.Ports, before.NetworkSettings.Ports);
    assert.equal(await http(hostPort), 200);
    await poll("two subsequent healthy scheduler cycles", 55_000, async () => {
      checkWorker(); return await healthy(1) >= 3 ? true : undefined;
    });
    assert.equal((await sample()).attempts.length, 1);
    assert.equal(inspect(app).State.StartedAt, after.State.StartedAt);
    const restartEvents = docker("events", "--since", String(eventStart), "--until", String(Math.ceil(Date.now() / 1000)),
      "--filter", `container=${app}`, "--filter", "event=restart", "--format", "{{.Action}}");
    assert.deepEqual(restartEvents.split(/\r?\n/).filter(Boolean), ["restart"]);
    const publication = workerLogs.find((log) => log.event === "auto_recovery" && log.reserved === 1 && log.published === 1);
    assert.ok(publication);
    console.log(JSON.stringify({ run, mode, event: "passed", recoveryId: attempt.id, healthVersion: attempt.healthVersion,
      observedStates: [...seen], failedChecks: await failedChecks(), generation: [0, 1], healthyNewGeneration: await healthy(1),
      publication: { evaluated: publication.evaluated, eligible: publication.eligible, reserved: publication.reserved, published: publication.published },
      jobId, jobState: await job.getState(), attemptsStarted: job.attemptsStarted, circuit: "CLOSED", remainingBudget: circuit.remainingBudget,
      restartIssuedAt: attempt.restartIssuedAt, restartSourceStartedAt: attempt.restartSourceStartedAt,
      afterStartedAt: after.State.StartedAt, sameContainer: after.Id === before.Id, sameImage: after.Image === before.Image,
      hostPort, httpStatus: 200, dockerRestartEvents: 1, injectionToSettlementMs: attempt.finishedAt!.getTime() - injected,
      claimToSettlementMs: attempt.finishedAt!.getTime() - attempt.startedAt!.getTime(),
    }));
  } else {
    await poll("disabled control threshold plus two monitoring cycles", 125_000, async () => {
      const result = await sample();
      assert.equal(result.attempts.length, 0);
      assert.equal(await queue.count(), 0);
      assert.equal(inspect(app).State.StartedAt, before.State.StartedAt);
      return result.health[0]?.status === "UNHEALTHY" && await failedChecks() >= 5 ? true : undefined;
    }, 1000);
    assert.equal(await http(hostPort), 503);
    assert.deepEqual(await queue.getJobs(["waiting", "active", "completed", "failed", "delayed"]), []);
    console.log(JSON.stringify({ run, mode, event: "passed", observedStates: [...seen], failedChecks: await failedChecks(),
      attempts: 0, jobs: 0, generation: 0, hostPort, httpStatus: 503, startedAt: before.State.StartedAt, observationMs: Date.now() - injected,
    }));
  }

  await new Promise<void>((resolve, reject) => worker!.send("shutdown", (error) => error ? reject(error) : resolve()));
  assert.equal(await Promise.race([exit, delay(20_000, "timeout", { ref: false })]), 0);
} catch (error) {
  // Error messages are deliberately bounded and omit command output and connection strings.
  const message = error instanceof Error ? error.message.split("\n")[0]! : "Unknown failure";
  console.error(JSON.stringify({ run, mode, event: "failed", message: message.replace(/[a-z]+:\/\/[^\s]+/gi, "[endpoint]"),
    worker: workerLogs.slice(-40).map(({ msg, event, reserved, published, reasons, failureReason, code }) =>
      ({ msg, event, reserved, published, reasons, failureReason, code })),
  }));
  process.exitCode = 1;
} finally {
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
