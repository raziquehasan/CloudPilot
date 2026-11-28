import assert from "node:assert/strict";
import test from "node:test";
import { runtimeJobSchema } from "@cloudpilot/queue";
import { assertValidRequestedHostPort } from "../src/runtime/runtime-config.ts";

const base = {
  kind: "runtime" as const,
  workspaceId: "10000000-0000-4000-8000-000000000001",
  projectId: "10000000-0000-4000-8000-000000000002",
  deploymentId: "10000000-0000-4000-8000-000000000003",
  runtimeInstanceId: "10000000-0000-4000-8000-000000000004",
  buildId: "10000000-0000-4000-8000-000000000005",
  action: "deploy" as const,
  correlationId: "job-1",
};

test("requested host port validator preserves dynamic default and rejects unsafe ports", () => {
  assert.doesNotThrow(() => assertValidRequestedHostPort(0));
  assert.doesNotThrow(() => assertValidRequestedHostPort(3000));
  assert.doesNotThrow(() => assertValidRequestedHostPort(1024));
  assert.doesNotThrow(() => assertValidRequestedHostPort(65535));
  assert.throws(() => assertValidRequestedHostPort(80));
  assert.throws(() => assertValidRequestedHostPort(65536));
  assert.throws(() => assertValidRequestedHostPort(-1));
  assert.throws(() => assertValidRequestedHostPort(1.5));
  assert.throws(() => assertValidRequestedHostPort("3000"));
});

test("runtime job schema leaves requestedHostPort optional and validates range", () => {
  assert.equal(runtimeJobSchema.parse(base).requestedHostPort, undefined);
  assert.equal(
    runtimeJobSchema.parse({ ...base, requestedHostPort: 3000 }).requestedHostPort,
    3000,
  );
  assert.throws(() => runtimeJobSchema.parse({ ...base, requestedHostPort: 70000 }));
  assert.throws(() => runtimeJobSchema.parse({ ...base, requestedHostPort: 1.5 }));
});