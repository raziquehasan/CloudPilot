import { request } from "node:http";
import { buildOwnershipLabels } from "@cloudpilot/queue";
import { Redis } from "ioredis";
import pg from "pg";

export const containerMetricTypes = {
  present: "gauge", running: "gauge", start_timestamp_seconds: "gauge", uptime_seconds: "gauge",
  restarts_total: "counter", cpu_usage_seconds_total: "counter", cpu_utilization_ratio: "gauge",
  memory_usage_bytes: "gauge", memory_limit_bytes: "gauge",
  network_receive_bytes_total: "counter", network_transmit_bytes_total: "counter",
};

export const postgresMetricTypes = {
  connections: "gauge", max_connections: "gauge", database_connection_limit: "gauge",
  database_size_bytes: "gauge", transactions_committed_total: "counter", transactions_rolled_back_total: "counter",
  blocks_hit_total: "counter", blocks_read_total: "counter", deadlocks_total: "counter",
  temp_files_total: "counter", temp_bytes_total: "counter", uptime_seconds: "gauge",
};

const redisFields = {
  used_memory: ["memory_used_bytes", "gauge"],
  maxmemory: ["max_memory_bytes", "gauge"],
  connected_clients: ["connected_clients", "gauge"],
  total_commands_processed: ["commands_processed_total", "counter"],
  instantaneous_ops_per_sec: ["operations_per_second", "gauge"],
  keyspace_hits: ["keyspace_hits_total", "counter"],
  keyspace_misses: ["keyspace_misses_total", "counter"],
  evicted_keys: ["evicted_keys_total", "counter"],
  expired_keys: ["expired_keys_total", "counter"],
  uptime_in_seconds: ["uptime_seconds", "gauge"],
};
export const redisMetricTypes = Object.fromEntries(Object.values(redisFields));

const number = (value) => typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
const epoch = (value) => typeof value === "string" && !value.startsWith("0001-") && Number.isFinite(Date.parse(value))
  ? Date.parse(value) / 1000 : undefined;
const decimal = (value) => typeof value === "string" && /^\d+(\.\d+)?$/.test(value) ? number(Number(value)) : number(value);

export function validateInventory(rows) {
  if (!Array.isArray(rows) || rows.length > 8) throw Error("Invalid Docker slot inventory");
  const slots = new Set();
  const ids = new Set();
  return rows.map(({ slot, containerId, imageId, workspaceId, projectId, deploymentId, runtimeInstanceId, attempt }) => {
    if (!Number.isInteger(slot) || slot < 1 || slot > 8 || slots.has(slot) ||
        typeof containerId !== "string" || !/^[a-f0-9]{64}$/.test(containerId) || ids.has(containerId) ||
        typeof imageId !== "string" || !/^sha256:[a-f0-9]{64}$/.test(imageId) ||
        ![workspaceId, projectId, deploymentId, runtimeInstanceId].every((value) =>
          typeof value === "string" && /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value))) throw Error("Invalid Docker slot identity");
    slots.add(slot);
    ids.add(containerId);
    return {
      slot: String(slot), containerId, imageId,
      name: `/cloudpilot-${projectId}-${deploymentId}-${attempt}`,
      labels: buildOwnershipLabels({ workspaceId, projectId, deploymentId, runtimeInstanceId, attempt }),
    };
  });
}

// Only fixed GETs to the existing local engine. No caller supplies a path or method.
function dockerRead(containerId, stats, signal) {
  if (!/^[a-f0-9]{64}$/.test(containerId)) throw Error("Invalid Docker identity");
  const socketPath = process.platform === "win32" ? "//./pipe/dockerDesktopLinuxEngine" : "/var/run/docker.sock";
  const path = `/v1.51/containers/${containerId}/${stats ? "stats?stream=false&one-shot=true" : "json"}`;
  return new Promise((resolve, reject) => {
    const req = request({ method: "GET", socketPath, path, signal, agent: false }, (res) => {
      res.on("error", reject);
      if (res.statusCode === 404) { res.resume(); resolve(null); return; }
      if (res.statusCode !== 200) { res.resume(); reject(Error("Docker request failed")); return; }
      const chunks = [];
      let size = 0;
      res.on("data", (chunk) => {
        size += chunk.length;
        if (size > 2 * 1024 * 1024) { req.destroy(Error("Docker response too large")); return; }
        chunks.push(chunk);
      });
      res.on("end", () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8"))); } catch (err) { reject(err); }
      });
    });
    req.on("error", reject);
    req.end();
  });
}

function sumNetwork(networks, field) {
  if (!networks || typeof networks !== "object") return undefined;
  const values = Object.values(networks).map((entry) => number(entry?.[field]));
  return values.length && values.every((value) => value !== undefined)
    ? number(values.reduce((total, value) => total + value, 0)) : undefined;
}

export function createDockerCollector(inventory, { read = dockerRead, deadlineMs = 4_000 } = {}) {
  let previous = new Map();
  return async () => {
    const signal = AbortSignal.timeout(deadlineMs);
    const next = new Map();
    async function collect(target) {
      const { slot, containerId, imageId, labels, name } = target;
      const info = await read(containerId, false, signal);
      if (!info || info.Id !== containerId || info.Image !== imageId || info.Name !== name ||
          !Object.entries(labels).every(([key, value]) => info.Config?.Labels?.[key] === value)) {
        return { slot, values: { present: 0, running: 0 } };
      }
      if (typeof info.State?.Running !== "boolean") throw Error("Invalid Docker state");
      const running = info.State.Running;
      const started = epoch(info.State.StartedAt);
      const limit = number(info.HostConfig?.Memory);
      const values = {
        present: 1, running: Number(running), start_timestamp_seconds: started,
        restarts_total: number(info.RestartCount), memory_limit_bytes: limit > 0 ? limit : undefined,
      };
      if (!running) return { slot, values };
      const stats = await read(containerId, true, signal);
      if (!stats) return { slot, values: { present: 0, running: 0 } };
      if (stats.id !== containerId) throw Error("Unexpected Docker stats identity");
      const sampledAt = epoch(stats.read);
      const rawCpu = number(stats.cpu_stats?.cpu_usage?.total_usage);
      const cpu = rawCpu === undefined ? undefined : rawCpu / 1e9;
      const prior = previous.get(slot);
      const elapsed = sampledAt !== undefined && prior ? sampledAt - prior.sampledAt : 0;
      if (started !== undefined && cpu !== undefined && sampledAt !== undefined) {
        next.set(slot, { startedAt: info.State.StartedAt, cpu, sampledAt });
      }
      return { slot, values: {
        ...values,
        uptime_seconds: started !== undefined && sampledAt !== undefined ? number(sampledAt - started) : undefined,
        cpu_usage_seconds_total: cpu,
        cpu_utilization_ratio: prior?.startedAt === info.State.StartedAt && elapsed > 0 && cpu !== undefined && cpu >= prior.cpu
          ? (cpu - prior.cpu) / elapsed : undefined,
        memory_usage_bytes: number(stats.memory_stats?.usage),
        network_receive_bytes_total: sumNetwork(stats.networks, "rx_bytes"),
        network_transmit_bytes_total: sumNetwork(stats.networks, "tx_bytes"),
      } };
    }
    const rows = [];
    try {
      for (let i = 0; i < inventory.length; i += 2) {
        const batch = await Promise.allSettled(inventory.slice(i, i + 2).map(collect));
        for (const result of batch) {
          if (result.status === "rejected") throw result.reason;
          rows.push(result.value);
        }
      }
      previous = next;
      return rows;
    } catch (error) {
      previous.clear();
      throw error;
    }
  };
}

export async function readPostgres(config) {
  const client = new pg.Client({
    ...config, host: "127.0.0.1", ssl: false, application_name: "cloudpilot-infra-metrics",
    connectionTimeoutMillis: 1_500, statement_timeout: 2_000, query_timeout: 2_500,
    options: "-c default_transaction_read_only=on -c search_path=pg_catalog",
  });
  client.on("error", () => {});
  try {
    await client.connect();
    const { rows } = await client.query(`
      SELECT s.numbackends AS connections,
        current_setting('max_connections')::integer AS max_connections,
        CASE WHEN d.datconnlimit >= 0 THEN d.datconnlimit END AS database_connection_limit,
        pg_database_size(current_database()) AS database_size_bytes,
        s.xact_commit AS transactions_committed_total, s.xact_rollback AS transactions_rolled_back_total,
        s.blks_hit AS blocks_hit_total, s.blks_read AS blocks_read_total, s.deadlocks AS deadlocks_total,
        s.temp_files AS temp_files_total, s.temp_bytes AS temp_bytes_total,
        extract(epoch FROM clock_timestamp() - pg_postmaster_start_time()) AS uptime_seconds
      FROM pg_stat_database s JOIN pg_database d ON d.oid = s.datid WHERE s.datname = current_database()
    `);
    if (rows.length !== 1) throw Error("Missing PostgreSQL statistics");
    return [{ values: Object.fromEntries(Object.keys(postgresMetricTypes).map((key) => [key, decimal(rows[0][key])])) }];
  } finally { await client.end(); }
}

export function redisObservations(info) {
  const values = {};
  for (const line of info.split(/\r?\n/)) {
    const index = line.indexOf(":");
    const field = line.slice(0, index);
    if (index > 0 && Object.hasOwn(redisFields, field)) {
      const output = decimal(line.slice(index + 1));
      if (output !== undefined) values[redisFields[field][0]] = output;
    }
  }
  if (values.memory_used_bytes === undefined || values.connected_clients === undefined ||
      values.commands_processed_total === undefined || values.uptime_seconds === undefined) throw Error("Missing Redis statistics");
  return [{ values }];
}

export async function readRedis(config) {
  const redis = new Redis({
    ...config, host: "127.0.0.1", lazyConnect: true, enableReadyCheck: false, disableClientInfo: true,
    connectTimeout: 1_500, commandTimeout: 1_500, enableOfflineQueue: false,
    maxRetriesPerRequest: 0, retryStrategy: () => null,
  });
  redis.on("error", () => {});
  const timer = setTimeout(() => redis.disconnect(), 3_500);
  try {
    await redis.connect();
    const sections = await Promise.all(["server", "clients", "memory", "stats"].map((section) => redis.info(section)));
    return redisObservations(sections.join("\r\n"));
  } finally {
    clearTimeout(timer);
    redis.disconnect();
  }
}
