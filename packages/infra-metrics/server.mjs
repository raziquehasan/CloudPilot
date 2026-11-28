import { readFileSync } from "node:fs";
import { createMetricsServer } from "@cloudpilot/metrics";
import {
  containerMetricTypes, postgresMetricTypes, redisMetricTypes, validateInventory,
  createDockerCollector, readPostgres, readRedis,
} from "./collectors.mjs";
import { collectPeriodically, infrastructureRegistry } from "./sampler.mjs";

function port(value) {
  if (!Number.isInteger(value) || value < 1024 || value > 65535) throw Error("Invalid infrastructure port");
  return value;
}

function credentials(value, type) {
  if (!value || typeof value !== "object") throw Error("Missing backend configuration");
  const user = type === "postgres" ? value.user : value.username;
  if (typeof user !== "string" || !/^[a-z_][a-z0-9_]{0,62}$/.test(user) ||
      ["postgres", "default", "root"].includes(user) ||
      typeof value.password !== "string" || value.password.length < 16 || value.password.length > 256) {
    throw Error("Invalid observer identity");
  }
  if (type === "redis") return { username: user, password: value.password, port: port(value.port), db: 0 };
  if (typeof value.database !== "string" || !/^[a-z_][a-z0-9_]{0,62}$/.test(value.database)) throw Error("Invalid database name");
  return { user, password: value.password, port: port(value.port), database: value.database };
}

async function run() {
  const config = JSON.parse(readFileSync(process.env.CLOUDPILOT_INFRA_CONFIG, "utf8"));
  const token = readFileSync(process.env.CLOUDPILOT_INFRA_TOKEN_FILE, "utf8").trim();
  if (!/^[a-zA-Z0-9_-]{43,128}$/.test(token)) throw Error("Invalid bearer credential");
  const inventory = validateInventory(config.containers);
  if (!inventory.length) throw Error("Empty Docker inventory");
  const postgres = credentials(config.postgres, "postgres");
  const redis = credentials(config.redis, "redis");
  const sources = [
    { backend: "docker", prefix: "cloudpilot_container", types: containerMetricTypes, read: createDockerCollector(inventory), port: 9470, slotted: true },
    { backend: "postgres", prefix: "cloudpilot_postgres", types: postgresMetricTypes, read: () => readPostgres(postgres), port: 9471 },
    { backend: "redis", prefix: "cloudpilot_redis", types: redisMetricTypes, read: () => readRedis(redis), port: 9472 },
  ].map((source) => {
    const registry = infrastructureRegistry();
    return { ...source, sampler: collectPeriodically({ ...source, registry }), server: createMetricsServer(registry, token) };
  });

  let stopping;
  const stop = () => stopping ??= Promise.all(sources.map(async ({ server, sampler }) => {
    const closing = new Promise((resolve) => server.close(resolve));
    server.closeAllConnections();
    await Promise.all([sampler.stop(), closing]);
  }));
  process.once("SIGINT", () => { void stop(); });
  process.once("SIGTERM", () => { void stop(); });
  try {
    for (const { server, port: listenPort } of sources) {
      await new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(listenPort, "127.0.0.1", () => {
          server.removeListener("error", reject);
          server.on("error", () => { console.error("Infrastructure metrics listener failed"); process.exitCode = 1; void stop(); });
          resolve();
        });
      });
      if (stopping) break;
    }
    if (!stopping) {
      for (const { sampler } of sources) sampler.start();
      console.log("Infrastructure metrics listeners ready on loopback ports 9470–9472");
    }
  } catch {
    await stop();
    throw Error("Metrics listener startup failed");
  }
}

try { await run(); }
catch {
  console.error("Infrastructure metrics unavailable; check private configuration and loopback listeners");
  process.exitCode = 1;
}
