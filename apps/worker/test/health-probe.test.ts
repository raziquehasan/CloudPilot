import assert from "node:assert/strict";
import { createServer, type RequestListener } from "node:http";
import test from "node:test";
import { probeHttpHealth } from "../src/health/http-health.js";
import { probeRuntime, type HealthInspector } from "../src/health/probe-runtime.js";

const host = "127.0.0.1";
const scope = {
  workspaceId: "10000000-0000-4000-8000-000000000001",
  projectId: "10000000-0000-4000-8000-000000000002",
  deploymentId: "10000000-0000-4000-8000-000000000003",
  runtimeInstanceId: "10000000-0000-4000-8000-000000000004",
  generation: 1,
};
const runtime = { host, port: 8080, containerPort: 3000, runtimeAttempt: 1, containerId: "a".repeat(64), imageId: `sha256:${"b".repeat(64)}` };

async function serve(handler: RequestListener, testServer: (port: number) => Promise<void>) {
  const server = createServer(handler);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, host, resolve);
  });
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    await testServer(address.port);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

test("HTTP health accepts 200, records 500 and missing endpoints, and never follows redirects", async () => {
  let requests = 0;
  await serve((req, res) => {
    requests++;
    assert.equal(req.headers.cookie, undefined);
    assert.equal(req.headers.authorization, undefined);
    const status = req.url === "/health" ? 200 : req.url === "/fault" ? 500 : req.url === "/redirect" ? 302 : 404;
    if (status === 302) res.setHeader("Location", "http://169.254.169.254/");
    res.writeHead(status).end("status");
  }, async (port) => {
    const healthy = await probeHttpHealth(host, port);
    assert.equal(healthy?.status, 200);
    assert.equal(healthy.failureReason, null);
    for (const [path, status, code] of [
      ["/fault", 500, "HTTP_STATUS"], ["/missing", 404, "HTTP_STATUS"], ["/redirect", 302, "HTTP_REDIRECT"],
    ] as const) {
      const result = await probeHttpHealth(host, port, path);
      assert.equal(result?.status, status);
      assert.equal(result.failureReason, code);
    }
    assert.equal(requests, 4);
  });
});

test("HTTP health bounds responses, aborts on timeout or worker interruption, and distinguishes refusal", async () => {
  let closedPort = 0;
  await serve((req, res) => {
    if (req.url === "/big") res.end(Buffer.alloc(4097));
  }, async (port) => {
    closedPort = port;
    assert.equal((await probeHttpHealth(host, port, "/big"))?.failureReason, "HTTP_INVALID_RESPONSE");
    assert.equal((await probeHttpHealth(host, port, "/wait", undefined, 50))?.failureReason, "HTTP_TIMEOUT");
    const controller = new AbortController();
    const result = probeHttpHealth(host, port, "/wait", controller.signal);
    controller.abort();
    assert.equal(await result, null);
  });
  assert.equal((await probeHttpHealth(host, closedPort))?.failureReason, "HTTP_CONNECTION_REFUSED");
});

test("HTTP health rejects arbitrary addresses, ports, and paths", async () => {
  for (const [address, port, path] of [
    ["localhost", 3000, "/health"], ["192.168.1.1", 3000, "/health"],
    [host, 0, "/health"], [host, 65536, "/health"],
    [host, 3000, "//example.org"], [host, 3000, "/../health"], [host, 3000, "http://example.org/"],
  ] as const) await assert.rejects(probeHttpHealth(address, port, path), RangeError);
});

test("Docker failures and stopped or unowned containers skip HTTP", async () => {
  const exited: HealthInspector = async () => ({
    ownershipVerified: true,
    inspection: {
      exists: true, state: "exited", running: false, containerId: runtime.containerId,
      imageId: runtime.imageId, exitCode: 137, restartCount: 2, startedAt: null, hostPort: null,
    },
  });
  const stopped = await probeRuntime(scope, runtime, exited);
  assert.equal(stopped?.failureReason, "CONTAINER_NOT_RUNNING");
  assert.equal(stopped?.httpResult, "SKIPPED");
  assert.equal(stopped?.docker?.exitCode, 137);
  assert.equal(stopped?.docker?.restartCount, 2);
  const unavailable = await probeRuntime(scope, runtime, async () => { throw new Error("Docker unavailable"); });
  assert.equal(unavailable?.failureReason, "CONTAINER_UNAVAILABLE");
  const foreign = await probeRuntime(scope, runtime, async (...args) => ({ ...await exited(...args), ownershipVerified: false }));
  assert.equal(foreign?.failureReason, "CONTAINER_IDENTITY_MISMATCH");
  assert.equal(foreign?.docker, null);
});
