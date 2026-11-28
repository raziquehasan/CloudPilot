import assert from "node:assert/strict";
import test from "node:test";

import {
  createDeploymentSchema,
  requestedHostPortSchema,
} from "../src/validators/deployment.validator.ts";

import {
  isFixedPortAuthorized,
  normalizeRequestedHostPort,
} from "../src/services/deployment-host-port.ts";

import {
  matchesDeploymentRequest,
} from "../src/services/deployment-identity.ts";

const TRUSTED_REPO = "raziquehasan/cloudpilot-demo-app";
const OTHER_REPO = "someone/untrusted";

function baseDeployment(overrides: Record<string, unknown> = {}) {
  return {
    environment: "DEVELOPMENT" as const,
    branch: "main",
    ...overrides,
  };
}

// ==================================================
// Schema: omitted and zero
// ==================================================

test("omitted requestedHostPort leaves it undefined (dynamic)", () => {
  const parsed = createDeploymentSchema.safeParse(baseDeployment());
  assert.equal(parsed.success, true);
  assert.equal(parsed.success && parsed.data.requestedHostPort, undefined);
});

test("requestedHostPort 0 is accepted (dynamic)", () => {
  const parsed = createDeploymentSchema.safeParse(
    baseDeployment({ requestedHostPort: 0 }),
  );
  assert.equal(parsed.success, true);
  assert.equal(parsed.success && parsed.data.requestedHostPort, 0);
});

// ==================================================
// Schema: valid fixed port
// ==================================================

test("valid fixed port 63299 is accepted", () => {
  const parsed = createDeploymentSchema.safeParse(
    baseDeployment({ requestedHostPort: 63_299 }),
  );
  assert.equal(parsed.success, true);
  assert.equal(parsed.success && parsed.data.requestedHostPort, 63_299);
});

test("lower bound 1024 and upper bound 65535 are accepted", () => {
  assert.equal(
    requestedHostPortSchema.safeParse(1_024).success,
    true,
  );
  assert.equal(
    requestedHostPortSchema.safeParse(65_535).success,
    true,
  );
});

// ==================================================
// Schema: invalid, fractional and out-of-range
// ==================================================

test("invalid, fractional and out-of-range ports are rejected", () => {
  for (const value of [
    1,
    1_023,
    65_536,
    70_000,
    -1,
    3.5,
    63_299.1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
  ]) {
    assert.equal(
      requestedHostPortSchema.safeParse(value).success,
      false,
      `expected ${String(value)} to be rejected`,
    );
  }
});

test("non-number port types are rejected", () => {
  for (const value of ["63299", "0", null, {}, true]) {
    assert.equal(
      requestedHostPortSchema.safeParse(value).success,
      false,
      `expected ${JSON.stringify(value)} to be rejected`,
    );
  }
});

test("unknown keys remain rejected by strict deployment schema", () => {
  const parsed = createDeploymentSchema.safeParse(
    baseDeployment({ requestedHostPort: 63_299, unexpected: true }),
  );
  assert.equal(parsed.success, false);
});

// ==================================================
// Authorization
// ==================================================

function authorize(
  role: string,
  opts: {
    port?: number;
    trustedLocal?: boolean;
    repos?: readonly string[];
    repo?: string;
  } = {},
) {
  return isFixedPortAuthorized({
    requestedHostPort: opts.port ?? 63_299,
    role,
    trustedLocalMode: opts.trustedLocal ?? true,
    trustedRepositories: opts.repos ?? [TRUSTED_REPO],
    repositoryFullName: opts.repo ?? TRUSTED_REPO,
  });
}

test("OWNER and ADMIN may request a fixed port when trusted", () => {
  assert.equal(authorize("OWNER"), true);
  assert.equal(authorize("ADMIN"), true);
});

test("unauthorized MEMBER requests are rejected", () => {
  assert.equal(authorize("MEMBER"), false);
  assert.equal(authorize("VIEWER"), false);
});

test("untrusted repository is rejected", () => {
  assert.equal(authorize("OWNER", { repo: OTHER_REPO }), false);
});

test("disabled trusted local mode is rejected", () => {
  assert.equal(authorize("OWNER", { trustedLocal: false }), false);
});

test("dynamic port (0) is always authorized", () => {
  assert.equal(authorize("MEMBER", { port: 0 }), true);
  assert.equal(authorize("OWNER", { port: 0, trustedLocal: false }), true);
});

// ==================================================
// Snapshot normalization / propagation
// ==================================================

test("snapshot with fixed port propagates unchanged (no silent fallback)", () => {
  assert.equal(
    normalizeRequestedHostPort({ requestedHostPort: 63_299 }),
    63_299,
  );
});

test("occupied fixed port is preserved, not coerced to dynamic", () => {
  // The value the outbox forwards must remain the fixed port so
  // an occupied port fails authoritatively downstream.
  const forwarded = normalizeRequestedHostPort({
    requestedHostPort: 63_299,
  });
  assert.equal(forwarded, 63_299);
  assert.notEqual(forwarded, 0);
});

test("absent or invalid snapshot yields dynamic (0)", () => {
  assert.equal(normalizeRequestedHostPort(null), 0);
  assert.equal(normalizeRequestedHostPort(undefined), 0);
  assert.equal(normalizeRequestedHostPort({}), 0);
  assert.equal(
    normalizeRequestedHostPort({ requestedHostPort: "63299" }),
    0,
  );
  assert.equal(
    normalizeRequestedHostPort({ requestedHostPort: 70_000 }),
    0,
  );
  assert.equal(
    normalizeRequestedHostPort({ requestedHostPort: 0 }),
    0,
  );
});

// ==================================================
// Idempotency conflict for different ports
// ==================================================

test("same key with a different requested port conflicts", () => {
  const existing = {
    requestedBranch: "main",
    commitSha: "a".repeat(40),
    buildConfigurationSnapshot: { requestedHostPort: 63_299 },
  };

  assert.equal(
    matchesDeploymentRequest(existing, {
      branch: "main",
      requestedHostPort: 63_299,
    }),
    true,
  );

  assert.equal(
    matchesDeploymentRequest(existing, {
      branch: "main",
      requestedHostPort: 0,
    }),
    false,
  );

  assert.equal(
    matchesDeploymentRequest(existing, {
      branch: "main",
      requestedHostPort: 63_300,
    }),
    false,
  );
});

test("omitted port on request matches a dynamic existing deployment", () => {
  const existing = {
    requestedBranch: "main",
    commitSha: "b".repeat(40),
    buildConfigurationSnapshot: null,
  };

  assert.equal(
    matchesDeploymentRequest(existing, { branch: "main" }),
    true,
  );

  assert.equal(
    matchesDeploymentRequest(existing, {
      branch: "main",
      requestedHostPort: 63_299,
    }),
    false,
  );
});

// ==================================================
// Existing deployment compatibility
// ==================================================

test("existing deployments without a snapshot remain dynamic", () => {
  const existing = {
    requestedBranch: "main",
    commitSha: "c".repeat(40),
    buildConfigurationSnapshot: null,
  };

  assert.equal(
    matchesDeploymentRequest(existing, {
      branch: "main",
      requestedHostPort: 0,
    }),
    true,
  );
});