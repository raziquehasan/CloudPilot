import { timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { collectDefaultMetrics, Registry } from "prom-client";

export { Counter, Gauge, Histogram, Registry } from "prom-client";

export const latencyBuckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120];

export function createMetricsRegistry(defaults = true) {
  const registry = new Registry();
  if (defaults) collectDefaultMetrics({ register: registry });
  return registry;
}

type MetricsOptions = { host: "127.0.0.1"; port: number; token: string };
type Environment = Record<string, string | undefined>;

export function metricsOptions(source: Environment, processName: "API" | "WORKER"): MetricsOptions | null {
  const enabled = source.METRICS_ENABLED ?? "false";
  if (enabled === "false") return null;
  if (enabled !== "true") throw new Error("METRICS_ENABLED must be true or false");
  if ((source.METRICS_HOST ?? "127.0.0.1") !== "127.0.0.1") throw new Error("METRICS_HOST must be 127.0.0.1");
  const portValue = source[`${processName}_METRICS_PORT`] ?? (processName === "API" ? "9464" : "9465");
  const port = Number(portValue);
  if (!/^\d+$/.test(portValue) || !Number.isInteger(port) || port < 1024 || port > 65535) {
    throw new Error("Invalid metrics port");
  }
  let token: string;
  try { token = readFileSync(source.METRICS_TOKEN_FILE ?? "", "utf8").trim(); }
  catch { throw new Error("Metrics credentials file is unreadable"); }
  if (!/^[A-Za-z0-9_-]{43,128}$/.test(token)) throw new Error("Invalid metrics credentials file contents");
  return { host: "127.0.0.1", port, token };
}

// Telemetry observers must never take ownership of application control flow.
export function observe(operation: () => void) {
  try { operation(); } catch { /* Metrics are best effort. */ }
}

export function createMetricsServer(registry: Registry, token: string): Server {
  const expected = Buffer.from(`Bearer ${token}`);
  let collecting = false;
  const server = createServer({ requestTimeout: 5_000, headersTimeout: 3_000, maxHeaderSize: 4096 },
    async (request, response) => {
      response.setHeader("Cache-Control", "no-store");
      if (request.url !== "/metrics") { response.writeHead(404).end(); return; }
      if (request.method !== "GET") { response.writeHead(405, { Allow: "GET" }).end(); return; }
      const provided = Buffer.from(request.headers.authorization ?? "");
      if (provided.length !== expected.length || !timingSafeEqual(provided, expected)) {
        response.writeHead(401, { "WWW-Authenticate": "Bearer" }).end();
        return;
      }
      if (collecting) { response.writeHead(503).end(); return; }
      collecting = true;
      try {
        const output = await registry.metrics();
        response.writeHead(200, { "Content-Type": registry.contentType }).end(output);
      } catch {
        response.writeHead(503).end();
      } finally {
        collecting = false;
      }
    });
  server.maxConnections = 16;
  server.maxRequestsPerSocket = 20;
  server.setTimeout(4500, (socket) => socket.destroy());
  return server;
}

export function openMetrics(options: MetricsOptions, registry: Registry, onError: () => void) {
  const server = createMetricsServer(registry, options.token);
  server.on("error", onError);
  server.listen(options.port, options.host);
  return () => new Promise<void>((resolve) => {
    server.close(() => resolve());
    server.closeAllConnections();
  });
}
