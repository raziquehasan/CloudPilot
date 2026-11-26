import assert from "node:assert/strict";
import {
  createCipheriv,
  randomBytes,
  randomUUID,
} from "node:crypto";
import test from "node:test";

import {
  RuntimeDeploymentError,
  buildOwnershipLabels,
  generateContainerName,
  getRuntimeFailureCode,
  isManagedContainerName,
  isRetryableRuntimeFailure,
  isRuntimeFailureCode,
  normalizeRuntimeFailureCode,
  runtimeJobSchema,
  runtimeOwnershipLabels,
  toRuntimeFailureMetadata,
} from "@cloudpilot/queue";

import {
  assertLoopbackBind,
  assertValidContainerPort,
  assertValidImageId,
  assertValidResourceLimits,
  assertValidUuid,
  generateNetworkName,
} from "../src/runtime/runtime-config.js";

import {
  resolveRuntimeEnvironment,
  type StoredEnvironmentVariable,
} from "../src/runtime/runtime-env.js";

import {
  buildReadinessUrl,
  probeTcp,
} from "../src/runtime/runtime-readiness.js";

// ==================================================
// Managed container identity
// ==================================================

test("generateContainerName produces a managed, validated name", () => {
  const projectId = randomUUID();
  const deploymentId = randomUUID();

  const name = generateContainerName({
    projectId,
    deploymentId,
    attempt: 3,
  });

  assert.equal(
    name,
    `cloudpilot-${projectId}-${deploymentId}-3`,
  );

  assert.equal(isManagedContainerName(name), true);
});

test("generateContainerName rejects invalid attempts", () => {
  assert.throws(
    () =>
      generateContainerName({
        projectId: randomUUID(),
        deploymentId: randomUUID(),
        attempt: 0,
      }),
    RuntimeDeploymentError,
  );
});

test("isManagedContainerName rejects foreign names", () => {
  assert.equal(isManagedContainerName("nginx"), false);
  assert.equal(
    isManagedContainerName("cloudpilot-not-a-uuid"),
    false,
  );
});

// ==================================================
// Ownership labels
// ==================================================

test("buildOwnershipLabels stamps the authoritative boundary", () => {
  const labels = buildOwnershipLabels({
    workspaceId: randomUUID(),
    projectId: randomUUID(),
    deploymentId: randomUUID(),
    runtimeInstanceId: randomUUID(),
    attempt: 2,
  });

  assert.equal(labels[runtimeOwnershipLabels.managed], "true");
  assert.equal(
    labels[runtimeOwnershipLabels.attempt],
    "2",
  );
});

// ==================================================
// Failure classification
// ==================================================

test("only RUNTIME_UNAVAILABLE is retryable", () => {
  assert.equal(
    isRetryableRuntimeFailure("RUNTIME_UNAVAILABLE"),
    true,
  );

  assert.equal(
    isRetryableRuntimeFailure("READINESS_FAILED"),
    false,
  );

  assert.equal(
    isRetryableRuntimeFailure("STARTUP_TIMEOUT"),
    false,
  );
});

test("RuntimeDeploymentError carries a normalized retryable flag", () => {
  const transient = new RuntimeDeploymentError(
    "RUNTIME_UNAVAILABLE",
  );

  assert.equal(transient.retryable, true);
  assert.equal(
    getRuntimeFailureCode(transient),
    "RUNTIME_UNAVAILABLE",
  );

  const permanent = new RuntimeDeploymentError(
    "CONTAINER_CREATION_FAILED",
  );

  assert.equal(permanent.retryable, false);
});

test("unknown errors normalize to UNEXPECTED_RUNTIME_FAILURE", () => {
  assert.equal(
    getRuntimeFailureCode(new Error("boom")),
    "UNEXPECTED_RUNTIME_FAILURE",
  );

  assert.equal(
    normalizeRuntimeFailureCode("nope"),
    "UNEXPECTED_RUNTIME_FAILURE",
  );

  assert.equal(isRuntimeFailureCode("READINESS_FAILED"), true);
});

test("toRuntimeFailureMetadata never leaks raw messages", () => {
  const metadata = toRuntimeFailureMetadata(
    new RuntimeDeploymentError(
      "IMAGE_OWNERSHIP_FAILED",
      "secret internal detail",
    ),
  );

  assert.equal(metadata.code, "IMAGE_OWNERSHIP_FAILED");
  assert.equal(metadata.retryable, false);
  assert.equal(
    metadata.message.includes("secret"),
    false,
  );
});

// ==================================================
// Runtime job payload
// ==================================================

test("runtimeJobSchema defaults action to deploy", () => {
  const parsed = runtimeJobSchema.parse({
    kind: "runtime",
    workspaceId: randomUUID(),
    projectId: randomUUID(),
    deploymentId: randomUUID(),
    runtimeInstanceId: randomUUID(),
    buildId: randomUUID(),
    correlationId: "job-1",
  });

  assert.equal(parsed.action, "deploy");
});

test("runtimeJobSchema is strict and validates identifiers", () => {
  assert.throws(() =>
    runtimeJobSchema.parse({
      kind: "runtime",
      workspaceId: "not-a-uuid",
      projectId: randomUUID(),
      deploymentId: randomUUID(),
      runtimeInstanceId: randomUUID(),
      buildId: randomUUID(),
      correlationId: "job-1",
    }),
  );

  assert.throws(() =>
    runtimeJobSchema.parse({
      kind: "runtime",
      workspaceId: randomUUID(),
      projectId: randomUUID(),
      deploymentId: randomUUID(),
      runtimeInstanceId: randomUUID(),
      buildId: randomUUID(),
      correlationId: "job-1",
      extra: true,
    }),
  );
});

// ==================================================
// Runtime configuration validation
// ==================================================

test("assertValidImageId requires an immutable sha256 id", () => {
  const digest = "a".repeat(64);

  assertValidImageId(`sha256:${digest}`);

  assert.throws(
    () => assertValidImageId("cloudpilot/project:latest"),
    RuntimeDeploymentError,
  );
});

test("assertValidContainerPort enforces the valid range", () => {
  assertValidContainerPort(8080);

  assert.throws(
    () => assertValidContainerPort(0),
    RuntimeDeploymentError,
  );

  assert.throws(
    () => assertValidContainerPort(70_000),
    RuntimeDeploymentError,
  );
});

test("assertValidResourceLimits rejects out-of-range values", () => {
  assertValidResourceLimits({
    cpuLimit: 2,
    memoryLimitMb: 512,
    pidsLimit: 256,
  });

  assert.throws(
    () =>
      assertValidResourceLimits({
        cpuLimit: 0,
        memoryLimitMb: 512,
        pidsLimit: 256,
      }),
    RuntimeDeploymentError,
  );
});

test("generateNetworkName is derived from a trusted project id", () => {
  const projectId = randomUUID();

  assert.equal(
    generateNetworkName("cloudpilot", projectId),
    `cloudpilot-net-${projectId}`,
  );

  assert.throws(
    () => generateNetworkName("cloudpilot", "nope"),
    RuntimeDeploymentError,
  );
});

test("assertLoopbackBind rejects non-loopback hosts", () => {
  assertLoopbackBind("127.0.0.1");

  assert.throws(
    () => assertLoopbackBind("0.0.0.0"),
    RuntimeDeploymentError,
  );
});

test("assertValidUuid validates identity strings", () => {
  assertValidUuid(randomUUID(), "Project");

  assert.throws(
    () => assertValidUuid("x", "Project"),
    RuntimeDeploymentError,
  );
});

// ==================================================
// Secret resolution
// ==================================================

function encryptForTest(input: {
  keyHex: string;
  workspaceId: string;
  projectId: string;
  environment: string;
  name: string;
  value: string;
}): StoredEnvironmentVariable {
  const nonce = randomBytes(12);

  const cipher = createCipheriv(
    "aes-256-gcm",
    Buffer.from(input.keyHex, "hex"),
    nonce,
  );

  cipher.setAAD(
    Buffer.from(
      JSON.stringify([
        input.workspaceId,
        input.projectId,
        input.environment,
        input.name,
      ]),
      "utf8",
    ),
  );

  const ciphertext = Buffer.concat([
    cipher.update(input.value, "utf8"),
    cipher.final(),
  ]);

  const tag = cipher.getAuthTag();

  const encode = (buffer: Buffer): string =>
    buffer.toString("base64url");

  return {
    key: input.name,
    encryptedValue: [
      "v1",
      encode(nonce),
      encode(tag),
      encode(ciphertext),
    ].join("."),
  };
}

test("resolveRuntimeEnvironment decrypts with matching context", () => {
  const keyHex = "b".repeat(64);

  const workspaceId = randomUUID();
  const projectId = randomUUID();
  const environment = "production";

  const stored = encryptForTest({
    keyHex,
    workspaceId,
    projectId,
    environment,
    name: "DATABASE_URL",
    value: "postgres://secret",
  });

  const resolved = resolveRuntimeEnvironment(
    {
      workspaceId,
      projectId,
      environment,
      encryptionKey: keyHex,
    },
    [stored],
  );

  assert.deepEqual(resolved, {
    DATABASE_URL: "postgres://secret",
  });
});

test("resolveRuntimeEnvironment fails closed on a wrong key", () => {
  const workspaceId = randomUUID();
  const projectId = randomUUID();
  const environment = "production";

  const stored = encryptForTest({
    keyHex: "b".repeat(64),
    workspaceId,
    projectId,
    environment,
    name: "TOKEN",
    value: "value",
  });

  assert.throws(
    () =>
      resolveRuntimeEnvironment(
        {
          workspaceId,
          projectId,
          environment,
          encryptionKey: "c".repeat(64),
        },
        [stored],
      ),
    RuntimeDeploymentError,
  );
});

test("resolveRuntimeEnvironment rejects an invalid key format", () => {
  assert.throws(
    () =>
      resolveRuntimeEnvironment(
        {
          workspaceId: randomUUID(),
          projectId: randomUUID(),
          environment: "production",
          encryptionKey: "not-hex",
        },
        [],
      ),
    RuntimeDeploymentError,
  );
});

// ==================================================
// Readiness
// ==================================================

test("buildReadinessUrl permits loopback targets only", () => {
  assert.equal(
    buildReadinessUrl({ host: "127.0.0.1", port: 8080 }),
    "http://127.0.0.1:8080/",
  );

  assert.throws(
    () => buildReadinessUrl({ host: "example.com", port: 80 }),
    RuntimeDeploymentError,
  );
});

test("buildReadinessUrl rejects path traversal tricks", () => {
  assert.throws(
    () =>
      buildReadinessUrl({
        host: "127.0.0.1",
        port: 8080,
        path: "http://evil",
      }),
    RuntimeDeploymentError,
  );

  assert.throws(
    () =>
      buildReadinessUrl({
        host: "127.0.0.1",
        port: 8080,
        path: "/@evil",
      }),
    RuntimeDeploymentError,
  );
});

test("probeTcp resolves false for a closed loopback port", async () => {
  const reachable = await probeTcp(
    "127.0.0.1",
    59_999,
    200,
  );

  assert.equal(reachable, false);
});