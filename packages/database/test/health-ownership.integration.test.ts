import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../src/generated/prisma/client.js";
import { claimHealthCheck, releaseHealthCheck, renewHealthCheck } from "../src/health-ownership.js";

const databaseUrl = process.env.CLOUDPILOT_HEALTH_TEST_DATABASE_URL;

test("health migration enforces tenant scope, lease fencing, ranges and recovery idempotency", {
  skip: !databaseUrl,
}, async () => {
  assert.ok(databaseUrl);
  const url = new URL(databaseUrl);
  assert.match(url.pathname, /^\/cloudpilot_health_test(?:_[a-z0-9]+)?$/);
  const db = new PrismaClient({ adapter: new PrismaPg({ connectionString: databaseUrl }) });
  const runId = randomUUID();

  async function createRuntime(label: string) {
    const workspace = await db.workspace.create({ data: { name: label, slug: `${label}-${runId}` } });
    const project = await db.project.create({ data: { workspaceId: workspace.id, name: label, slug: label } });
    const deployment = await db.deployment.create({
      data: { workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT", triggerSource: "MANUAL" },
    });
    const scope = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
    const build = await db.deploymentBuild.create({
      data: { ...scope, sourcePreparationId: randomUUID(), resolvedCommitSha: "a".repeat(40) },
    });
    const runtime = await db.deploymentRuntimeInstance.create({ data: { ...scope, buildId: build.id } });
    return { ...scope, runtimeInstanceId: runtime.id, generation: 0 };
  }

  try {
    const scope = await createRuntime("health-owner");
    const foreignScope = await createRuntime("health-foreign");
    const due = new Date(0);
    await db.deploymentHealthState.create({ data: { ...scope, nextCheckAt: due } });
    await assert.rejects(db.deploymentHealthState.create({ data: scope }), { code: "P2002" });
    await assert.rejects(db.deploymentHealthState.create({
      data: { ...foreignScope, runtimeInstanceId: scope.runtimeInstanceId },
    }), { code: "P2003" });
    await assert.rejects(db.deploymentHealthState.create({
      data: { ...scope, generation: 1, consecutiveFailures: -1 },
    }), /dhs_counters_chk/);
    await assert.rejects(db.deploymentHealthState.create({
      data: { ...scope, generation: 1, leaseOwnerId: randomUUID() },
    }), /dhs_lease_chk/);
    const nextGeneration = { ...scope, generation: 1 };
    await db.deploymentHealthState.create({ data: nextGeneration });

    const probe = { ...scope, dockerResult: "PASSED", httpResult: "PASSED" } as const;
    await assert.rejects(db.deploymentHealthCheck.create({
      data: { ...probe, workspaceId: foreignScope.workspaceId },
    }), { code: "P2003" });
    await assert.rejects(db.deploymentHealthCheck.create({
      data: { ...probe, responseLatencyMs: -1 },
    }), /dhc_measurements_chk/);
    await assert.rejects(db.deploymentHealthCheck.create({
      data: { ...probe, httpStatus: 600 },
    }), /dhc_measurements_chk/);
    await db.deploymentHealthCheck.create({ data: { ...probe, responseLatencyMs: 30, httpStatus: 200 } });

    const ownerIds = [randomUUID(), randomUUID()];
    const claims = await Promise.all(ownerIds.map((owner) => claimHealthCheck(db, scope, owner, 1_000)));
    const [winner] = claims.filter(Boolean);
    assert.equal(claims.filter(Boolean).length, 1);
    assert.ok(winner);
    assert.equal(await releaseHealthCheck(db, scope, { ...winner, ownerId: randomUUID() }), false);
    assert.equal(await renewHealthCheck(db, nextGeneration, winner, 1_000), null);
    assert.equal(await claimHealthCheck(db, nextGeneration, randomUUID(), 1_000), null);
    assert.equal(await claimHealthCheck(db, foreignScope, randomUUID(), 1_000), null);
    await assert.rejects(claimHealthCheck(db, scope, randomUUID(), 120_001), RangeError);
    const renewed = await renewHealthCheck(db, scope, winner, 1_000);
    assert.ok(renewed);
    assert.equal(renewed.token, winner.token);
    assert.ok(renewed.expiresAt >= winner.expiresAt);
    await db.$queryRaw`SELECT 1 AS waited FROM pg_sleep(1.1)`;
    assert.equal(await renewHealthCheck(db, scope, winner, 1_000), null);
    assert.equal(await releaseHealthCheck(db, scope, winner), false);
    const replacement = await claimHealthCheck(db, scope, randomUUID(), 5_000);
    assert.ok(replacement);
    assert.notEqual(replacement.token, winner.token);
    assert.equal(await releaseHealthCheck(db, scope, winner), false);
    assert.equal(await releaseHealthCheck(db, scope, replacement), true);

    const recovery = { ...scope, action: "RESTART", trigger: "MANUAL", idempotencyKey: randomUUID() } as const;
    await assert.rejects(db.deploymentRecoveryAttempt.create({
      data: { ...recovery, attemptCount: -1 },
    }), /dra_attempt_chk/);
    const active = await db.deploymentRecoveryAttempt.create({ data: recovery });
    await assert.rejects(db.deploymentRecoveryAttempt.create({
      data: { ...recovery, ...nextGeneration, idempotencyKey: randomUUID() },
    }), { code: "P2002" });
    await db.deploymentRecoveryAttempt.update({
      where: { id: active.id }, data: { status: "CANCELLED", finishedAt: new Date() },
    });
    await assert.rejects(db.deploymentRecoveryAttempt.create({ data: recovery }), { code: "P2002" });
    await db.deploymentRecoveryAttempt.create({ data: { ...recovery, idempotencyKey: randomUUID() } });
    await db.deploymentRuntimeInstance.delete({ where: { id: scope.runtimeInstanceId } });
    assert.equal(await db.deploymentHealthState.count({ where: { runtimeInstanceId: scope.runtimeInstanceId } }), 0);
    assert.equal(await db.deploymentHealthCheck.count({ where: scope }), 0);
    assert.equal(await db.deploymentRecoveryAttempt.count({ where: scope }), 0);
  } finally {
    await db.workspace.deleteMany({ where: { slug: { endsWith: runId } } });
    await db.$disconnect();
  }
});
