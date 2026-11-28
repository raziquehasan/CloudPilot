import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import {
  disconnectDatabase, prisma, recoveryPolicy, recoverySchemaReady,
  reserveAutomaticRecovery, reserveManualRecovery, type HealthFailureReason,
} from "@cloudpilot/database";

const database = new URL(process.env.DATABASE_URL ?? "http://localhost");
const isolated = /^\/cloudpilot_health_test_[a-z0-9]+$/.test(database.pathname);

async function fixture(run: (f: {
  seedFailure: (minutesAgo: number, reason?: HealthFailureReason) => Promise<unknown>;
  automatic: () => ReturnType<typeof reserveAutomaticRecovery>;
  manual: () => ReturnType<typeof reserveManualRecovery>;
  scope: { workspaceId: string; projectId: string; deploymentId: string; runtimeInstanceId: string; generation: number };
}) => Promise<void>) {
  assert.equal(await recoverySchemaReady(prisma), true);
  const suffix = randomUUID();
  const user = await prisma.user.create({ data: { email: `${suffix}@example.invalid` } });
  const workspace = await prisma.workspace.create({ data: { name: "Circuit test", slug: `circuit-${suffix}` } });
  try {
    const project = await prisma.project.create({ data: {
      workspaceId: workspace.id, name: "Circuit", slug: "circuit",
    } });
    await prisma.repository.create({ data: {
      workspaceId: workspace.id, projectId: project.id, provider: "github",
      externalId: suffix, name: "circuit", fullName: "example/circuit-test",
    } });
    const deployment = await prisma.deployment.create({ data: {
      workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT",
      triggerSource: "MANUAL", status: "RUNNING",
    } });
    const deploymentScope = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
    const imageId = `sha256:${"b".repeat(64)}`;
    const build = await prisma.deploymentBuild.create({ data: {
      ...deploymentScope, sourcePreparationId: randomUUID(), resolvedCommitSha: "c".repeat(40),
      imageId, status: "SUCCEEDED",
    } });
    const runtime = await prisma.deploymentRuntimeInstance.create({ data: {
      ...deploymentScope, buildId: build.id, status: "RUNNING", readyAt: new Date(),
      containerId: "a".repeat(64), imageId, hostBindingAddress: "127.0.0.1", hostPort: 3100, containerPort: 3000,
    } });
    const scope = { ...deploymentScope, runtimeInstanceId: runtime.id, generation: 0 };
    await prisma.deploymentHealthState.create({ data: {
      ...scope, status: "UNHEALTHY", consecutiveFailures: 3, lastCheckedAt: new Date(), nextCheckAt: new Date(),
    } });
    await run({
      scope,
      seedFailure(minutesAgo, failureReason = "HTTP_STATUS") {
        const finishedAt = new Date(Date.now() - minutesAgo * 60_000);
        const createdAt = new Date(finishedAt.getTime() - 1_000);
        return prisma.deploymentRecoveryAttempt.create({ data: {
          ...scope, action: "RESTART", trigger: "AUTOMATIC", idempotencyKey: randomUUID(), status: "FAILED",
          attemptCount: 1, createdAt, startedAt: createdAt, finishedAt, failureReason,
        } });
      },
      automatic: () => reserveAutomaticRecovery(prisma, deploymentScope, runtime.id),
      manual: () => reserveManualRecovery(prisma, deploymentScope, user.id, randomUUID()),
    });
  } finally {
    await prisma.auditLog.deleteMany({ where: { workspaceId: workspace.id } });
    await prisma.workspace.delete({ where: { id: workspace.id } });
    await prisma.user.delete({ where: { id: user.id } });
  }
}

test("an uncertain restart keeps automatic recovery OPEN after the rolling budget expires", { skip: !isolated }, () =>
  fixture(async ({ seedFailure, automatic, scope }) => {
    await seedFailure(60, "RECOVERY_RESTART_UNCERTAIN");
    assert.equal((await automatic()).kind, "ineligible");
    assert.equal(await prisma.deploymentRecoveryAttempt.count({ where: { ...scope, status: "REQUESTED" } }), 0);
  }));

test("concurrent HALF_OPEN reservations authorize one trial whose failure survives the budget window", { skip: !isolated }, () =>
  fixture(async ({ seedFailure, automatic, scope }) => {
    for (const minutes of [60, 55, 50]) await seedFailure(minutes);
    const attempts = await Promise.all(Array.from({ length: 5 }, () => automatic()));
    const admitted = attempts.filter((attempt) => attempt.kind === "reserved");
    assert.equal(admitted.length, 1);
    const trial = admitted[0]?.attempt;
    assert.ok(trial);
    const audit = await prisma.auditLog.findFirstOrThrow({
      where: { workspaceId: scope.workspaceId, entityId: scope.deploymentId, action: "DEPLOYMENT_RECOVERY_REQUESTED" },
    });
    assert.equal((audit.metadata as { automaticCircuitState?: string }).automaticCircuitState, "HALF_OPEN");

    const failedAt = new Date(Date.now() - recoveryPolicy.windowMs - 60_000);
    await prisma.deploymentRecoveryAttempt.update({ where: { id: trial.id }, data: {
      status: "FAILED", failureReason: "HTTP_STATUS", attemptCount: 1,
      createdAt: new Date(failedAt.getTime() - 1_000), startedAt: failedAt, finishedAt: failedAt,
    } });
    await prisma.deploymentHealthState.updateMany({ where: scope, data: {
      status: "UNHEALTHY", lastCheckedAt: new Date(), nextCheckAt: new Date(), version: { increment: 1 },
    } });
    assert.equal((await automatic()).kind, "ineligible");
    assert.equal(await prisma.deploymentRecoveryAttempt.count({ where: { ...scope, status: "REQUESTED" } }), 0);
  }));

test("manual recovery bypasses the open circuit and still respects the shared budget", { skip: !isolated }, async () => {
  await fixture(async ({ seedFailure, automatic, manual }) => {
    await seedFailure(60, "RECOVERY_RESTART_UNCERTAIN");
    assert.equal((await automatic()).kind, "ineligible");
    assert.equal((await manual()).kind, "reserved");
  });
  await fixture(async ({ seedFailure, manual }) => {
    for (const minutes of [5, 4, 3]) await seedFailure(minutes);
    assert.equal((await manual()).kind, "exhausted");
  });
});

test("verified success closes the old circuit, but ambiguous unmarked legacy trials fail closed", { skip: !isolated }, async () => {
  await fixture(async ({ seedFailure, automatic, scope }) => {
    await seedFailure(60, "RECOVERY_RESTART_UNCERTAIN");
    const finishedAt = new Date(Date.now() - 30 * 60_000);
    await prisma.deploymentRecoveryAttempt.create({ data: {
      ...scope, trigger: "MANUAL", action: "RESTART", status: "SUCCEEDED", attemptCount: 1,
      idempotencyKey: randomUUID(), createdAt: finishedAt, startedAt: finishedAt, finishedAt,
    } });
    assert.equal((await automatic()).kind, "reserved");
  });
  await fixture(async ({ seedFailure, automatic }) => {
    for (const minutes of [60, 55, 50, 45]) await seedFailure(minutes);
    assert.equal((await automatic()).kind, "ineligible");
  });
});

test.after(async () => { if (isolated) await disconnectDatabase(); });
