import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { buildOwnershipLabels } from "@cloudpilot/queue";
import pg from "pg";
import { Redis } from "ioredis";
import { readPostgres, readRedis } from "../collectors.mjs";

assert.equal(process.env.CLOUDPILOT_RUN_INFRA_VALIDATION, "1");
const run = "cp593-" + randomUUID();
const db = "cp593_" + randomBytes(8).toString("hex");
const root = mkdtempSync(join(tmpdir(), run));
const tokenFile = join(root, "infra.token");
const appTokenFile = join(root, "app.token");
const configFile = join(root, "config.json");
const secret = () => randomBytes(32).toString("base64url");
const token = secret();
const postgresPassword = secret();
const redisPassword = secret();
const adminPassword = secret();
const tracked = [];
const docker = (args, options = {}) => execFileSync("docker", args, {
  encoding: "utf8", timeout: 120_000, stdio: ["pipe", "pipe", "pipe"], ...options,
}).trim();
const composeFiles = ["-p", run, "-f", "infrastructure/monitoring/docker-compose.yml"];
const applicationOverlay = ["-f", "infrastructure/monitoring/docker-compose.metrics.yml"];
const infraOverlay = ["-f", "infrastructure/monitoring/docker-compose.infrastructure.yml"];
const composeEnv = { ...process.env, CLOUDPILOT_INFRA_TOKEN_FILE: tokenFile, CLOUDPILOT_METRICS_TOKEN_FILE: appTokenFile, CLOUDPILOT_PROMETHEUS_PORT: "19093" };
const compose = (overlays, ...args) => docker(["compose", ...composeFiles, ...overlays, ...args], { env: composeEnv });
const runContainer = (...args) => {
  const id = docker(["run", "-d", "--label", `cloudpilot.validation=${run}`, ...args], {
    env: { ...process.env, POSTGRES_PASSWORD: adminPassword },
  });
  assert.match(id, /^[a-f0-9]{64}$/);
  tracked.push(id);
  return id;
};
function mapped(id, internalPort) {
  const binding = docker(["port", id, `${internalPort}/tcp`]);
  assert.match(binding, /^127\.0\.0\.1:\d+$/);
  const port = Number(binding.split(":")[1]);
  assert.ok(![3000, 8000, 5433, 6379, 53143, 63299, 9090, 9464, 9465, 9470, 9471, 9472, 19093].includes(port));
  return port;
}
async function poll(label, predicate, seconds = 45) {
  for (let i = 0; i < seconds * 2; i++) {
    const value = await predicate();
    if (value) return value;
    await sleep(500);
  }
  throw Error(`Timed out: ${label}`);
}
async function http(url, init) {
  return fetch(url, { signal: AbortSignal.timeout(1500), ...init });
}
const apps = new Map();
let observer;
let redisAdmin;
let collector;
let usedCompose = false;

try {
  for (const port of [9470, 9471, 9472, 19093]) {
    const { createServer } = await import("node:net");
    const probe = createServer();
    await new Promise((resolve, reject) => probe.once("error", reject).listen(port, "127.0.0.1", resolve));
    await new Promise((resolve) => probe.close(resolve));
  }
  for (const [file, value] of [[tokenFile, token], [appTokenFile, secret()]]) writeFileSync(file, value, { mode: 0o600 });
  const pgImage = docker(["image", "inspect", "--format", "{{.Id}}", "postgres:15-alpine"]);
  const redisImage = docker(["image", "inspect", "--format", "{{.Id}}", "redis:7-alpine"]);
  const appImage = docker(["image", "inspect", "--format", "{{.Id}}", "node:22-alpine"]);
  const p = runContainer("--name", run + "-pg", "-e", "POSTGRES_PASSWORD", "-e", `POSTGRES_DB=${db}`, "-p", "127.0.0.1::5432", pgImage);
  const r = runContainer("--name", run + "-redis", "-p", "127.0.0.1::6379", redisImage);
  const pgPort = mapped(p, 5432), redisPort = mapped(r, 6379);
  const pgConfig = { port: pgPort, database: db, user: "cloudpilot_observer", password: postgresPassword };
  const redisConfig = { port: redisPort, username: "cloudpilot_observer", password: redisPassword };
  const sql = (query) => docker(["exec", "-i", p, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db],
    { input: query });
  // Use TCP to ensure initialization's temporary server has finished.
  await poll("PostgreSQL ready", () => {
    try { return docker(["exec", p, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", db]); } catch { return false; }
  });
  sql(`CREATE ROLE cloudpilot_observer LOGIN PASSWORD '${postgresPassword}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    REVOKE CONNECT ON DATABASE ${db} FROM PUBLIC;
    GRANT CONNECT ON DATABASE ${db} TO cloudpilot_observer;
    CREATE TABLE private_fixture (id integer); INSERT INTO private_fixture VALUES (1);`);
  observer = new pg.Client({ ...pgConfig, host: "127.0.0.1" });
  await observer.connect();
  assert.equal((await observer.query("SELECT 1")).rowCount, 1);
  await assert.rejects(observer.query("INSERT INTO private_fixture VALUES (2)"), { code: "42501" });
  await assert.rejects(observer.query("SELECT * FROM private_fixture"), { code: "42501" });
  await assert.rejects(observer.query("CREATE SCHEMA unauthorized"), { code: "42501" });
  await observer.end();
  observer = undefined;

  const grant = ["+info|server", "+info|clients", "+info|memory", "+info|stats"];
  docker(["exec", "-i", r, "redis-cli", "-x", "ACL", "SETUSER", redisConfig.username, "reset", "on", ...grant],
    { input: ">" + redisPassword });
  const redisCheck = new Redis({ ...redisConfig, host: "127.0.0.1", enableReadyCheck: false, disableClientInfo: true, lazyConnect: true });
  redisCheck.on("error", () => {});
  try {
    await redisCheck.connect();
    await assert.rejects(redisCheck.set("private-data", "test"), /NOPERM/);
    await assert.rejects(redisCheck.config("SET", "maxmemory", "0"), /NOPERM/);
    await assert.rejects(redisCheck.info("commandstats"), /NOPERM/);
  } finally { redisCheck.disconnect(); }
  await assert.rejects(() => readRedis({ ...redisConfig, password: secret() }));
  await assert.rejects(() => readPostgres({ ...pgConfig, password: secret() }));

  const scope = { workspaceId: randomUUID(), projectId: randomUUID(), deploymentId: randomUUID(), runtimeInstanceId: randomUUID(), attempt: 1 };
  const labels = buildOwnershipLabels(scope);
  function app(withLabels, suffix, memory = "128m") {
    const id = runContainer("--name", suffix, "--memory", memory, "--cpus", "1", "--pids-limit", "64",
      "-p", "127.0.0.1::3000", ...(withLabels ? Object.entries(labels).flatMap(([key, val]) => ["--label", `${key}=${val}`]) : []),
      appImage, "node", "-e", "require('http').createServer((q,s)=>{for(let i=0;i<1000000;i++) Math.sqrt(i);s.end(Buffer.alloc(65536,65))}).listen(3000,'0.0.0.0')");
    apps.set(id, mapped(id, 3000));
    return id;
  }
  const managed = app(true, `cloudpilot-${scope.projectId}-${scope.deploymentId}-1`);
  const unrelated = app(false, run + "-unrelated");
  writeFileSync(configFile, JSON.stringify({
    containers: [{ slot: 1, ...scope, containerId: managed, imageId: appImage }],
    postgres: pgConfig, redis: redisConfig,
  }), { mode: 0o600 });
  if (process.platform === "win32") {
    const identity = execFileSync("whoami", { encoding: "utf8" }).trim();
    for (const file of [tokenFile, appTokenFile, configFile]) {
      execFileSync("icacls", [file, "/inheritance:r", "/grant:r", `${identity}:(R,W)`], { stdio: "pipe" });
    }
  } else for (const file of [tokenFile, appTokenFile, configFile]) chmodSync(file, 0o600);
  console.log(JSON.stringify({ run, pgPort, redisPort, appPort: apps.get(managed), pgImage, redisImage,
    postgresVersion: docker(["exec", p, "postgres", "--version"]), redisVersion: docker(["exec", r, "redis-server", "--version"]) }));

  usedCompose = true;
  const both = [...applicationOverlay, ...infraOverlay];
  compose(both, "config", "--quiet");
  console.log(compose(both, "run", "--rm", "--no-deps", "--entrypoint", "/bin/promtool", "prometheus", "check", "config", "/etc/prometheus/prometheus.yml"));
  collector = spawn(process.execPath, ["--input-type=module", "--eval",
    "process.on('message', () => process.emit('SIGTERM')); await import('./packages/infra-metrics/server.mjs');"], {
    env: { ...process.env, CLOUDPILOT_INFRA_CONFIG: configFile, CLOUDPILOT_INFRA_TOKEN_FILE: tokenFile },
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  const headers = { Authorization: `Bearer ${token}` };
  const readPort = async (port) => (await http(`http://127.0.0.1:${port}/metrics`, { headers })).text();
  const metric = (text, expr) => {
    const line = text.split("\n").find((row) => row.startsWith(expr + " "));
    return line ? Number(line.slice(expr.length + 1)) : undefined;
  };
  const up = async (port, value) => metric(await readPort(port), "cloudpilot_infra_backend_up") === value;
  for (const port of [9470, 9471, 9472]) {
    await poll(`backend ${port}`, async () => {
      if (collector.exitCode !== null) throw Error("Collector exited before readiness");
      try { return await up(port, 1); } catch { return false; }
    });
    const endpoint = `http://127.0.0.1:${port}`;
    assert.equal((await http(endpoint + "/metrics")).status, 401);
    assert.equal((await http(endpoint + "/metrics", { headers: { Authorization: "Bearer invalid" } })).status, 401);
    assert.equal((await http(endpoint + "/metrics", { headers, method: "POST" })).status, 405);
    assert.equal((await http(endpoint + "/containers/test/stop", { headers, method: "POST" })).status, 404);
    const body = await readPort(port);
    for (const privateValue of [token, postgresPassword, redisPassword, managed, unrelated, scope.workspaceId, scope.runtimeInstanceId]) {
      assert.ok(!body.includes(privateValue));
    }
    console.log(JSON.stringify({ port, bytes: Buffer.byteLength(body), series: body.split("\n").filter((line) => line && !line.startsWith("#")).length }));
  }
  compose(infraOverlay, "up", "-d", "--wait", "--wait-timeout", "60");
  const prom = "http://127.0.0.1:19093";
  const targets = await poll("isolated scrape targets", async () => {
    const data = (await (await http(prom + "/api/v1/targets")).json()).data.activeTargets;
    return data.length === 4 && data.every((target) => target.health === "up") && data;
  });
  console.log(JSON.stringify({ targets: targets.map(({ scrapePool, health }) => ({ scrapePool, health })) }));
  const initial = await readPort(9470);
  const initialCpu = metric(initial, 'cloudpilot_container_cpu_usage_seconds_total{slot="1"}');
  const initialNet = metric(initial, 'cloudpilot_container_network_transmit_bytes_total{slot="1"}');
  assert.ok(initialCpu >= 0 && initialNet >= 0);
  const fixture = `http://127.0.0.1:${apps.get(managed)}`;
  for (let i = 0; i < 24; i++) await (await http(fixture)).arrayBuffer();
  const pgBefore = (await readPostgres(pgConfig))[0].values;
  sql("BEGIN; INSERT INTO private_fixture VALUES (3); COMMIT; BEGIN; SELECT 1; ROLLBACK;");
  const pgAfter = await poll("Postgres counters", async () => {
    const current = (await readPostgres(pgConfig))[0].values;
    return current.transactions_committed_total > pgBefore.transactions_committed_total &&
      current.transactions_rolled_back_total > pgBefore.transactions_rolled_back_total && current;
  });
  redisAdmin = new Redis({ host: "127.0.0.1", port: redisPort });
  const redisBefore = (await readRedis(redisConfig))[0].values;
  await redisAdmin.set("cp593-hit", "value");
  await redisAdmin.get("cp593-hit");
  await redisAdmin.get("cp593-miss");
  const redisAfter = (await readRedis(redisConfig))[0].values;
  assert.ok(redisAfter.keyspace_hits_total > redisBefore.keyspace_hits_total);
  assert.ok(redisAfter.keyspace_misses_total > redisBefore.keyspace_misses_total);
  assert.ok(redisAfter.commands_processed_total > redisBefore.commands_processed_total);
  const appStats = await poll("real container CPU and network", async () => {
    const b = await readPort(9470);
    return metric(b, 'cloudpilot_container_cpu_usage_seconds_total{slot="1"}') > initialCpu &&
      metric(b, 'cloudpilot_container_network_transmit_bytes_total{slot="1"}') > initialNet && b;
  });
  assert.equal(metric(appStats, 'cloudpilot_container_running{slot="1"}'), 1);
  assert.equal(metric(appStats, 'cloudpilot_container_memory_limit_bytes{slot="1"}'), 134217728);
  assert.ok(metric(appStats, 'cloudpilot_container_memory_usage_bytes{slot="1"}') > 0);
  const inspection = JSON.parse(docker(["inspect", "--format", "{{json .State}}", managed]));
  assert.equal(metric(appStats, 'cloudpilot_container_start_timestamp_seconds{slot="1"}'), Date.parse(inspection.StartedAt) / 1000);
  const cliStats = JSON.parse(docker(["stats", "--no-stream", "--format", "{{json .}}", managed]));
  console.log(JSON.stringify({ dockerAuthoritative: { running: inspection.Running, start: inspection.StartedAt,
    memory: cliStats.MemUsage, network: cliStats.NetIO },
    dockerExposition: appStats.split("\n").filter((line) => line.startsWith("cloudpilot_container_")), postgres: pgAfter, redis: redisAfter }));
  const query = async (expr) => {
    const reply = await (await http(prom + "/api/v1/query?query=" + encodeURIComponent(expr))).json();
    if (reply.status !== "success") throw Error("Invalid PromQL");
    return reply.data.result;
  };
  await poll("two scrapes for collector targets", async () => {
    const result = await query('count_over_time(up{job=~"cloudpilot-containers|cloudpilot-postgres|cloudpilot-redis"}[1m])');
    return result.length === 3 && result.every(({ value }) => Number(value[1]) >= 2);
  });
  for (const expr of [
    "100 * cloudpilot_container_cpu_utilization_ratio", "cloudpilot_container_memory_usage_bytes / 1024^2",
    "100 * cloudpilot_container_memory_usage_bytes / cloudpilot_container_memory_limit_bytes",
    "rate(cloudpilot_container_network_receive_bytes_total[2m])", "rate(cloudpilot_container_network_transmit_bytes_total[2m])",
    "cloudpilot_container_uptime_seconds", "cloudpilot_postgres_connections",
    "rate(cloudpilot_postgres_transactions_committed_total[2m])",
    "cloudpilot_postgres_blocks_hit_total / (cloudpilot_postgres_blocks_hit_total + cloudpilot_postgres_blocks_read_total)",
    "cloudpilot_redis_memory_used_bytes", "cloudpilot_redis_connected_clients", "rate(cloudpilot_redis_commands_processed_total[2m])",
    "cloudpilot_redis_keyspace_hits_total / (cloudpilot_redis_keyspace_hits_total + cloudpilot_redis_keyspace_misses_total)",
    'up{job=~"cloudpilot-containers|cloudpilot-postgres|cloudpilot-redis"}', "cloudpilot_infra_backend_up",
    "time() - cloudpilot_infra_last_success_timestamp_seconds",
  ]) {
    const result = await poll(expr, async () => {
      const rows = await query(expr);
      return rows.length && rows.every(({ value }) => Number.isFinite(Number(value[1]))) && rows;
    });
    console.log(JSON.stringify({ expr, samples: result.map(({ metric, value }) => ({ job: metric.job, value })) }));
  }

  const beforeSeries = (await query("prometheus_tsdb_head_series"))[0]?.value[1];
  docker(["stop", managed]);
  await poll("stopped container", async () => {
    const data = await readPort(9470);
    return metric(data, 'cloudpilot_container_running{slot="1"}') === 0 && !data.includes('cloudpilot_container_memory_usage_bytes{');
  });
  docker(["start", managed]);
  await poll("container returns with a reset CPU counter", async () => {
    const data = await readPort(9470);
    const cpu = metric(data, 'cloudpilot_container_cpu_usage_seconds_total{slot="1"}');
    return metric(data, 'cloudpilot_container_running{slot="1"}') === 1 && cpu !== undefined && cpu < metric(appStats, 'cloudpilot_container_cpu_usage_seconds_total{slot="1"}');
  });
  docker(["rm", "-f", managed]);
  await poll("removed container", async () => metric(await readPort(9470), 'cloudpilot_container_present{slot="1"}') === 0);
  const afterSeries = (await query("prometheus_tsdb_head_series"))[0]?.value[1];
  console.log(JSON.stringify({ beforeSeries, afterSeries, reusedSlot: true }));

  sql(`REVOKE CONNECT ON DATABASE ${db} FROM cloudpilot_observer;`);
  await poll("Postgres permission failure", () => up(9471, 0));
  sql(`GRANT CONNECT ON DATABASE ${db} TO cloudpilot_observer;`);
  await poll("Postgres permission recovery", () => up(9471, 1));
  await redisAdmin.acl("SETUSER", redisConfig.username, "-info");
  await poll("Redis permission failure", () => up(9472, 0));
  await redisAdmin.acl("SETUSER", redisConfig.username, ...grant);
  await poll("Redis permission recovery", () => up(9472, 1));

  for (const [id, port] of [[p, 9471], [r, 9472]]) {
    docker(["pause", id]);
    try { await poll(`unavailable backend ${port}`, () => up(port, 0)); }
    finally { docker(["unpause", id]); }
    await poll(`recovered backend ${port}`, () => up(port, 1));
  }
  const stats = docker(["stats", "--no-stream", "--format", "{{.CPUPerc}} {{.MemUsage}}", `${run}-prometheus-1`]);
  console.log(JSON.stringify({ event: "passed", collectorPid: collector.pid, prometheusCpuAndMemory: stats }));
} catch (error) {
  console.error("Isolated validation failed:", error?.stack ?? error);
  process.exitCode = 1;
} finally {
  redisAdmin?.disconnect();
  await observer?.end();
  if (collector && collector.exitCode === null && collector.signalCode === null) {
    const exit = new Promise((resolve) => collector.once("exit", resolve));
    collector.send("shutdown");
    if (await Promise.race([exit.then(() => false), sleep(8_000, true, { ref: false })])) collector.kill();
    await exit;
  }
  if (usedCompose) compose(infraOverlay, "down", "--volumes");
  for (const id of tracked.reverse()) {
    const actual = docker(["ps", "-aq", "--no-trunc", "--filter", `id=${id}`, "--filter", `label=cloudpilot.validation=${run}`]);
    if (actual === id) docker(["rm", "-f", "-v", id]);
  }
  assert.equal(dirname(root), resolve(tmpdir()));
  rmSync(root, { recursive: true, force: true });
}
