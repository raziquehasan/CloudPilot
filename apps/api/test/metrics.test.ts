import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, request as send } from "node:http";
import { randomBytes } from "node:crypto";
import test from "node:test";
import express from "express";
import { Counter, createMetricsRegistry, createMetricsServer, metricsOptions } from "@cloudpilot/metrics";
import { httpMetrics, metricsRoutePrefix } from "../src/metrics/http.js";
import { createQueueCollector, queueStates } from "../src/metrics/queues.js";

test("registries are isolated and dedicated metrics listener authenticates and excludes business routes", async () => {
  assert.equal(metricsOptions({}, "API"), null);
  assert.throws(() => metricsOptions({ METRICS_ENABLED: "true", METRICS_HOST: "0.0.0.0" }, "API"));
  assert.throws(() => metricsOptions({ METRICS_ENABLED: "true" }, "WORKER"), /unreadable/);
  const first = createMetricsRegistry(false);
  const second = createMetricsRegistry(false);
  for (const registry of [first, second]) {
    new Counter({ name: "metrics_test_total", help: "Metrics test", registers: [registry] }).inc(registry === first ? 2 : 1);
  }
  assert.match(await second.metrics(), /metrics_test_total 1/);
  const token = randomBytes(32).toString("base64url");
  const server = createMetricsServer(first, token).listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const base = `http://127.0.0.1:${address.port}`;
    assert.equal((await fetch(base + "/metrics")).status, 401);
    assert.equal((await fetch(base + "/metrics", { headers: { Authorization: "Bearer wrong" } })).status, 401);
    const headers = { Authorization: `Bearer ${token}` };
    assert.equal((await fetch(base + "/health", { headers })).status, 404);
    assert.equal((await fetch(base + "/metrics", { headers, method: "POST" })).status, 405);
    const valid = await fetch(base + "/metrics", { headers });
    assert.equal(valid.status, 200);
    assert.match(valid.headers.get("content-type") ?? "", /^text\/plain/);
    const output = await valid.text();
    assert.match(output, /# TYPE metrics_test_total counter\nmetrics_test_total 2/);
    assert.ok(!output.includes(token));
  } finally { server.close(); server.closeAllConnections(); }
});

test("HTTP records terminal outcomes once with static mounted routes and no identifiers or queries", async () => {
  const app = express();
  const registry = createMetricsRegistry(false);
  const middleware = httpMetrics(registry);
  assert.equal(httpMetrics(registry), middleware);
  app.use(middleware);
  const router = express.Router();
  router.get("/deployments/:id", (_req, res) => res.sendStatus(200));
  router.get("/error", (_req, res) => res.sendStatus(500));
  router.get("/auth", (_req, res) => res.sendStatus(401));
  let arrival!: () => void;
  const arrived = new Promise<void>((resolve) => { arrival = resolve; });
  let close!: () => void;
  const closed = new Promise<void>((resolve) => { close = resolve; });
  router.get("/stream/:id", (_req, res) => { res.on("close", close); arrival(); });
  app.use("/api/v1/projects", metricsRoutePrefix("/api/v1/projects"), router);
  const server = createServer(app).listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const base = `http://127.0.0.1:${address.port}`;
    for (const [path, status] of [
      ["/api/v1/projects/deployments/private-id?token=private-query", 200],
      ["/api/v1/projects/deployments/another-id", 200],
      ["/api/v1/projects/error", 500], ["/api/v1/projects/auth", 401],
      ["/private-unmatched", 404], ["/metrics", 404],
    ] as const) {
      const response = await fetch(base + path);
      assert.equal(response.status, status);
      await response.text();
    }
    const pending = send(base + "/api/v1/projects/stream/private-id");
    pending.on("error", () => undefined);
    pending.end();
    await arrived;
    pending.destroy();
    await closed;
    const output = await registry.metrics();
    assert.match(output, /cloudpilot_http_requests_total\{method="GET",route="\/api\/v1\/projects\/deployments\/:id",status="200"\} 2/);
    assert.match(output, /route="\/api\/v1\/projects\/error",status="500"\} 1/);
    assert.match(output, /route="\/api\/v1\/projects\/auth",status="401"\} 1/);
    assert.match(output, /route="unmatched",status="404"\} 1/);
    assert.match(output, /route="\/api\/v1\/projects\/stream\/:id",status="aborted"\} 1/);
    assert.match(output, /cloudpilot_http_requests_in_flight 0/);
    assert.match(output, /# TYPE cloudpilot_http_request_duration_seconds histogram/);
    assert.doesNotMatch(output, /private-|another-id/);
  } finally { server.close(); server.closeAllConnections(); }
});

test("queue collection is single flight and marks retained data stale after rejection or timeout", async () => {
  const registry = createMetricsRegistry(false);
  let mode = "success";
  let calls = 0;
  const releases: Array<() => void> = [];
  const values = Object.fromEntries(queueStates.map((state) => [state, 7]));
  const collection = createQueueCollector(registry, async () => {
    calls++;
    if (mode === "reject") throw new Error("Redis down");
    if (mode === "timeout") await new Promise<void>((resolve) => releases.push(resolve));
    return values;
  }, { timeoutMs: 20 });
  const before = await registry.metrics();
  assert.doesNotMatch(before, /^cloudpilot_queue_jobs\{/m);
  assert.doesNotMatch(before, /^cloudpilot_queue_last_success_timestamp_seconds\{/m);
  await collection.sample();
  const fresh = await registry.metrics();
  assert.match(fresh, /cloudpilot_queue_collection_success 1/);
  assert.match(fresh, /cloudpilot_queue_jobs\{queue="diagnostic",state="waiting"\} 7/);
  assert.match(fresh, /cloudpilot_queue_last_success_timestamp_seconds\{source="bullmq"\} [1-9]/);
  mode = "reject";
  await collection.sample();
  assert.match(await registry.metrics(), /cloudpilot_queue_collection_success 0/);
  mode = "timeout";
  await collection.sample();
  const timedOut = calls;
  await collection.sample();
  assert.equal(calls, timedOut);
  const stale = await registry.metrics();
  assert.match(stale, /cloudpilot_queue_collection_success 0/);
  assert.match(stale, /cloudpilot_queue_jobs\{queue="diagnostic",state="waiting"\} 7/);
  collection.stop();
  for (const release of releases) release();
  await collection.sample();
  assert.equal(calls, timedOut);
});
