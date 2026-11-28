import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test, { after } from "node:test";
import { disconnectDatabase, prisma } from "@cloudpilot/database";
import { workerEnv } from "../src/config/env.js";
import { discoverAutoRecoveryCandidates } from "../src/health/auto-recovery-candidates.js";

const enabled = process.env.HEALTH_PERSISTENCE_ENABLED === "true" &&
  /^\/cloudpilot_health_test(?:_[a-z0-9]+)?$/.test(new URL(process.env.DATABASE_URL ?? "http://localhost").pathname);

const trusted = workerEnv.BUILD_TRUSTED_REPOSITORIES.map((name) => name.trim().toLowerCase());
const trustedLocal = workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY && trusted.length > 0;

after(async () => {
  await disconnectDatabase();
});

async function seed(fullName: string) {
  const workspace = await prisma.workspace.create({ data: { name: "Auto-recovery test", slug: `auto-${randomUUID()}` } });
  const project = await prisma.project.create({ data: { workspaceId: workspace.id, name: "Auto", slug: "auto" } });
  await prisma.repository.create({
    data: {
      workspaceId: workspace.id, projectId: project.id, provider: "github", externalId: randomUUID(),
      name: "auto", fullName,
    },
  });
  const deployment = await prisma.deployment.create({
    data: { workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT", triggerSource: "MANUAL", status: "RUNNING" },
  });
  const parent = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
  const build = await prisma.deploymentBuild.create({
    data: { ...parent, sourcePreparationId: randomUUID(), resolvedCommitSha: "c".repeat(40) },
  });
  const runtime = await prisma.deploymentRuntimeInstance.create({
    data: {
      ...parent, buildId: build.id, status: "RUNNING", readyAt: new Date(),
      containerId: "a".repeat(64), imageId: `sha256:${"b".repeat(64)}`,
      hostPort: 32000 + Math.floor(Math.random() * 1000), hostBindingAddress: "127.0.0.1", containerPort: 3000,
    },
  });
  const identity = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id, runtimeInstanceId: runtime.id };
  const now = new Date();
  await prisma.deploymentHealthState.create({
    data: {
      ...identity, generation: runtime.runtimeAttempt, status: "UNHEALTHY",
      consecutiveFailures: 3, lastCheckedAt: now, nextCheckAt: now,
    },
  });
  return { workspace, identity };
}

test("discovery is read-only and ignores untrusted repositories", {
  skip: !enabled,
}, async () => {
  assert.equal(await discoverAutoRecoveryCandidates(16).then((r) => r.length >= 0), true);
  const { workspace, identity } = await seed("example/untrusted-repo");
  try {
    const before = await prisma.deploymentHealthState.findFirstOrThrow({ where: identity });
    const attemptsBefore = await prisma.deploymentRecoveryAttempt.count({ where: identity });
    const rows = await discoverAutoRecoveryCandidates(16);
    assert.equal(rows.some((row) => row.runtimeInstanceId === identity.runtimeInstanceId), false);
    const after = await prisma.deploymentHealthState.findFirstOrThrow({ where: identity });
    assert.equal(after.version, before.version);
    assert.equal(after.status, before.status);
    assert.equal(after.consecutiveFailures, before.consecutiveFailures);
    assert.equal(await prisma.deploymentRecoveryAttempt.count({ where: identity }), attemptsBefore);
  } finally {
    await prisma.workspace.delete({ where: { id: workspace.id } });
  }
});

test("discovery surfaces an eligible trusted candidate without side effects", {
  skip: !enabled || !trustedLocal,
}, async () => {
  const { workspace, identity } = await seed(trusted[0]!);
  try {
    const before = await prisma.deploymentHealthState.findFirstOrThrow({ where: identity });
    const attemptsBefore = await prisma.deploymentRecoveryAttempt.count({ where: identity });
    const rows = await discoverAutoRecoveryCandidates(16);
    const match = rows.find((row) => row.runtimeInstanceId === identity.runtimeInstanceId);
    assert.ok(match, "expected the trusted unhealthy runtime to be discovered");
    assert.equal(match!.healthStatus, "UNHEALTHY");
    assert.equal(match!.generation, before.generation);
    assert.equal(match!.activeRecovery, false);
    const after = await prisma.deploymentHealthState.findFirstOrThrow({ where: identity });
    assert.equal(after.version, before.version);
    assert.equal(after.status, "UNHEALTHY");
    assert.equal(await prisma.deploymentRecoveryAttempt.count({ where: identity }), attemptsBefore);
  } finally {
    await prisma.workspace.delete({ where: { id: workspace.id } });
  }
});
