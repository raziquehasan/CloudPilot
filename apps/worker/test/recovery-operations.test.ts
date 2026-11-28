import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { workerEnv } from "../src/config/env.js";
import { verifyRecoveryRestart } from "../src/health/recovery-operations.js";
import type { RuntimeExecutor } from "../src/runtime/runtime-executor.js";

const scope = { workspaceId: randomUUID(), projectId: randomUUID(), deploymentId: randomUUID(), runtimeInstanceId: randomUUID(), generation: 0 };
const runtime = { runtimeAttempt: 1, containerId: "a".repeat(64), imageId: `sha256:${"b".repeat(64)}`, containerPort: 3000, host: "127.0.0.1", port: 3001 };
const startedAt = "2026-11-27T01:00:00.123456789Z";

function inspection(id = runtime.containerId) {
  return { ownershipVerified: true, inspection: {
    exists: true, running: true, state: "running" as const, containerId: id, imageId: runtime.imageId,
    startedAt, exitCode: null, restartCount: 0, hostPort: runtime.port,
  } };
}

test("identity mismatch and unavailable Docker fail before writing an intent", async () => {
  for (const [inspectOwnedContainer, reason] of [
    [async () => inspection("c".repeat(64)), "RECOVERY_IDENTITY_MISMATCH"],
    [async () => { throw new Error("Docker down"); }, "RECOVERY_DOCKER_UNAVAILABLE"],
  ] as const) {
    const executor: Pick<RuntimeExecutor, "inspectOwnedContainer" | "restartOwnedContainer"> = {
      inspectOwnedContainer, restartOwnedContainer: async () => assert.fail("Must not restart"),
    };
    const result = await verifyRecoveryRestart({
      scope, runtime, executor, signal: new AbortController().signal, sourceEpoch: null, timeoutMs: 1000,
      markIntent: async () => assert.fail("Must not reserve an external action"),
    });
    assert.deepEqual(result, { ok: false, reason });
  }
});

test("timeout and replay of an uncertain intent never send a second restart", async () => {
  const abort = new AbortController();
  let calls = 0;
  const executor = {
    inspectOwnedContainer: async () => inspection(),
    restartOwnedContainer: async () => { calls++; abort.abort(); throw new Error("Docker timed out"); },
  };
  const options = { scope, runtime, executor, timeoutMs: 1000, markIntent: async () => true };
  assert.deepEqual(await verifyRecoveryRestart({ ...options, signal: abort.signal, sourceEpoch: null }),
    { ok: false, reason: "RECOVERY_TIMEOUT" });
  assert.deepEqual(await verifyRecoveryRestart({ ...options, signal: new AbortController().signal, sourceEpoch: startedAt }),
    { ok: false, reason: "RECOVERY_RESTART_UNCERTAIN" });
  assert.equal(calls, 1);
  assert.equal(workerEnv.AUTO_RECOVERY_ENABLED, false);
});
