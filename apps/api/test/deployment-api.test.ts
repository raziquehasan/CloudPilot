import assert from "node:assert/strict";
import { createServer } from "node:http";
import { randomBytes, randomUUID } from "node:crypto";
import test from "node:test";
import { publishWorkerHeartbeat } from "@cloudpilot/queue";
import { disconnectDatabase, prisma, recoveryPolicy } from "@cloudpilot/database";
import { createApp } from "../src/app.ts";
import { hashSecret } from "../src/services/session.service.ts";
import { disconnectQueueProducer, queueEnv, queueManager, redisProducerConnection } from "../src/services/queue-producer.ts";

const recoveryIntegration = process.env.MANUAL_RECOVERY_ENABLED === "true" &&
  /^\/cloudpilot_health_test_574(?:_[a-z0-9]+)?$/.test(new URL(process.env.DATABASE_URL ?? "http://localhost").pathname) &&
  process.env.CLOUDPILOT_RECOVERY_TEST_REDIS === process.env.REDIS_URL &&
  !!process.env.CLOUDPILOT_RECOVERY_TEST_REDIS;

type TestSession = {
  cookie: string;
  csrf: string;
};

async function createTestSession(userId: string, workspaceId: string): Promise<TestSession> {
  const token = randomBytes(32).toString("base64url");
  const csrf = randomBytes(32).toString("base64url");
  await prisma.session.create({
    data: {
      userId,
      activeWorkspaceId: workspaceId,
      tokenHash: hashSecret(token),
      csrfHash: hashSecret(csrf),
      expiresAt: new Date(Date.now() + 60 * 60 * 1000),
    },
  });
  return {
    cookie: `cloudpilot_session=${token}; cloudpilot_csrf=${csrf}`,
    csrf,
  };
}

test("deployment API enforces workspace, role, CSRF, worker readiness, and atomic cancellation", async () => {
  const suffix = randomUUID();
  const createdUsers: string[] = [];
  const createdWorkspaces: string[] = [];
  const server = createServer(createApp());

  try {
    const owner = await prisma.user.create({
      data: { email: `phase5-owner-${suffix}@example.invalid`, name: "Phase 5 test owner" },
    });
    createdUsers.push(owner.id);
    const member = await prisma.user.create({
      data: { email: `phase5-member-${suffix}@example.invalid`, name: "Phase 5 test member" },
    });
    createdUsers.push(member.id);

    const workspace = await prisma.workspace.create({
      data: { name: "Phase 5 test workspace", slug: `phase5-${suffix}` },
    });
    createdWorkspaces.push(workspace.id);
    await prisma.workspaceMember.createMany({
      data: [
        { workspaceId: workspace.id, userId: owner.id, role: "OWNER" },
        { workspaceId: workspace.id, userId: member.id, role: "MEMBER" },
      ],
    });
    const project = await prisma.project.create({
      data: { workspaceId: workspace.id, name: "Test project", slug: `test-${suffix}` },
    });
    const ownerSession = await createTestSession(owner.id, workspace.id);
    const memberSession = await createTestSession(member.id, workspace.id);

    const otherWorkspace = await prisma.workspace.create({
      data: { name: "Other workspace", slug: `other-${suffix}` },
    });
    createdWorkspaces.push(otherWorkspace.id);
    const otherProject = await prisma.project.create({
      data: { workspaceId: otherWorkspace.id, name: "Other project", slug: `other-${suffix}` },
    });

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const apiUrl = `http://127.0.0.1:${address.port}/api/v1`;
    const baseUrl = `${apiUrl}/projects/${project.id}/deployments`;
    const csrfHeaders = (session: TestSession) => ({
      Cookie: session.cookie,
      "Content-Type": "application/json",
      "X-CSRF-Token": session.csrf,
    });

    const unauthenticated = await fetch(baseUrl);
    assert.equal(unauthenticated.status, 401);
    const recoveryPath = `${baseUrl}/${randomUUID()}/recoveries`;
    assert.equal((await fetch(recoveryPath)).status, 401);
    assert.equal((await fetch(recoveryPath, { method: "POST", headers: { Cookie: ownerSession.cookie } })).status, 403);
    assert.equal((await fetch(recoveryPath, { method: "POST", headers: {
      ...csrfHeaders(memberSession), "Idempotency-Key": randomUUID(),
    } })).status, 403);
    assert.equal((await fetch(`${apiUrl}/projects/${otherProject.id}/deployments/${randomUUID()}/recoveries`, {
      headers: { Cookie: ownerSession.cookie },
    })).status, 404);

    const listResponse = await fetch(`${baseUrl}?page=1&perPage=10`, {
      headers: { Cookie: ownerSession.cookie },
    });
    assert.equal(listResponse.status, 200);
    assert.deepEqual((await listResponse.json() as { data: { items: unknown[] } }).data.items, []);

    const crossWorkspaceResponse = await fetch(
      `http://127.0.0.1:${address.port}/api/v1/projects/${otherProject.id}/deployments`,
      { headers: { Cookie: ownerSession.cookie } },
    );
    assert.equal(crossWorkspaceResponse.status, 404);

    const requestBody = JSON.stringify({ environment: "DEVELOPMENT", branch: "main" });
    const missingCsrf = await fetch(baseUrl, {
      method: "POST",
      headers: { Cookie: ownerSession.cookie, "Content-Type": "application/json" },
      body: requestBody,
    });
    assert.equal(missingCsrf.status, 403);

    const forbiddenCreate = await fetch(baseUrl, {
      method: "POST",
      headers: { ...csrfHeaders(memberSession), "Idempotency-Key": `member-${suffix}` },
      body: requestBody,
    });
    assert.equal(forbiddenCreate.status, 403);

    const missingKey = await fetch(baseUrl, {
      method: "POST",
      headers: csrfHeaders(ownerSession),
      body: requestBody,
    });
    assert.equal(missingKey.status, 400);

    const blockedCreate = await fetch(baseUrl, {
      method: "POST",
      headers: { ...csrfHeaders(ownerSession), "Idempotency-Key": `owner-${suffix}` },
      body: requestBody,
    });
    assert.equal(blockedCreate.status, 409);
    assert.equal(
      (await blockedCreate.json() as { error: { code: string } }).error.code,
      "REPOSITORY_NOT_CONNECTED",
    );
    assert.equal(await prisma.deployment.count({ where: { workspaceId: workspace.id } }), 0);

    const idempotentDeployment = await prisma.deployment.create({
      data: {
        workspaceId: workspace.id,
        projectId: project.id,
        environment: "DEVELOPMENT",
        triggerSource: "MANUAL",
        idempotencyKey: `replay-${suffix}`,
        requestedBranch: "main",
      },
    });
    const workspaceList = await fetch(`${apiUrl}/deployments`, {
      headers: { Cookie: ownerSession.cookie },
    });
    assert.equal(workspaceList.status, 200);
    const { data: workspaceDeployments } = await workspaceList.json() as {
      data: Array<{ id: string; commitSha: unknown; build: unknown; runtime: unknown; project: { id: string } }>;
    };
    assert.equal(workspaceDeployments.length, 1);
    const [listed] = workspaceDeployments;
    assert.ok(listed);
    assert.equal(listed.id, idempotentDeployment.id);
    assert.equal(listed.project.id, project.id);
    assert.equal(listed.commitSha, null);
    assert.equal(listed.build, null);
    assert.equal(listed.runtime, null);

    const replay = await fetch(baseUrl, {
      method: "POST",
      headers: { ...csrfHeaders(ownerSession), "Idempotency-Key": `replay-${suffix}` },
      body: requestBody,
    });
    assert.equal(replay.status, 200);
    assert.equal((await replay.json() as { data: { id: string }; idempotent: boolean }).data.id, idempotentDeployment.id);

    const conflictingReplay = await fetch(baseUrl, {
      method: "POST",
      headers: { ...csrfHeaders(ownerSession), "Idempotency-Key": `replay-${suffix}` },
      body: JSON.stringify({ environment: "DEVELOPMENT", branch: "different-branch" }),
    });
    assert.equal(conflictingReplay.status, 409);

    const queued = await prisma.deployment.create({
      data: {
        workspaceId: workspace.id,
        projectId: project.id,
        environment: "DEVELOPMENT",
        triggerSource: "MANUAL",
      },
    });
    const cancelUrl = `${baseUrl}/${queued.id}/cancel`;
    const cancel = () => fetch(cancelUrl, {
      method: "POST",
      headers: csrfHeaders(ownerSession),
    });
    const cancelResponses = await Promise.all([cancel(), cancel()]);
    assert.deepEqual(cancelResponses.map((response) => response.status).sort(), [200, 409]);
    assert.equal((await cancel()).status, 409);
    assert.equal(
      (await prisma.deployment.findUnique({ where: { id: queued.id } }))?.status,
      "CANCELLED",
    );
    assert.equal(
      await prisma.auditLog.count({
        where: { workspaceId: workspace.id, entityId: queued.id, action: "DEPLOYMENT_CANCELLATION_REQUESTED" },
      }),
      1,
    );
    if (recoveryIntegration) {
      const deployment = await prisma.deployment.create({
        data: { workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT", triggerSource: "MANUAL", status: "RUNNING" },
      });
      const recoveryScope = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id };
      await prisma.repository.create({ data: {
        workspaceId: workspace.id, projectId: project.id, provider: "github", externalId: randomUUID(),
        name: "fixture", fullName: "example/health-probe-test",
      } });
      const build = await prisma.deploymentBuild.create({ data: {
        ...recoveryScope, sourcePreparationId: randomUUID(), resolvedCommitSha: "a".repeat(40), status: "SUCCEEDED",
      } });
      const runtime = await prisma.deploymentRuntimeInstance.create({ data: {
        ...recoveryScope, buildId: build.id, status: "RUNNING", readyAt: new Date(),
        containerId: "a".repeat(64), imageId: `sha256:${"b".repeat(64)}`, hostPort: 3001, hostBindingAddress: "127.0.0.1",
      } });
      await prisma.deploymentHealthState.create({ data: {
        ...recoveryScope, runtimeInstanceId: runtime.id, generation: 0, status: "UNHEALTHY",
        consecutiveFailures: 3, nextCheckAt: new Date(),
      } });
      const path = `${baseUrl}/${deployment.id}/recoveries`;
      const headers = { ...csrfHeaders(ownerSession), "Idempotency-Key": randomUUID() };
      const results = await Promise.all([fetch(path, { method: "POST", headers }), fetch(path, { method: "POST", headers })]);
      assert.deepEqual(results.map((result) => result.status), [202, 202]);
      const accepted = await Promise.all(results.map(async (result) => (await result.json() as { data: { id: string } }).data.id));
      const [id] = accepted;
      assert.ok(id);
      assert.equal(accepted[1], id);
      const job = await queueManager.getQueue("runtime").getJob(`recovery-${id}`);
      assert.ok(job && job.data.kind === "runtime");
      assert.equal(job.data.action, "recover");
      assert.equal(job.data.runtimeInstanceId, runtime.id);
      const history = await fetch(path, { headers: { Cookie: memberSession.cookie } });
      assert.equal(history.status, 200);
      assert.equal((await history.json() as { data: unknown[] }).data.length, 1);
      const status = await fetch(`${path}/${id}`, { headers: { Cookie: memberSession.cookie } });
      const payload = await status.json() as { data: { status: string; leaseToken?: string } };
      assert.equal(status.status, 200);
      assert.equal(payload.data.status, "REQUESTED");
      assert.equal(payload.data.leaseToken, undefined);
    }
  } finally {
    if (server.listening) {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
      });
    }
    if (createdWorkspaces.length) {
      await prisma.auditLog.deleteMany({ where: { workspaceId: { in: createdWorkspaces } } });
      await prisma.workspace.deleteMany({ where: { id: { in: createdWorkspaces } } });
    }
    if (createdUsers.length) {
      await prisma.user.deleteMany({ where: { id: { in: createdUsers } } });
    }

  }
});

test("deployment health endpoint enforces access, scope, read-only behavior, and recovery availability", async () => {
  const suffix = randomUUID();
  const createdUsers: string[] = [];
  const createdWorkspaces: string[] = [];
  const server = createServer(createApp());

  try {
    const owner = await prisma.user.create({
      data: { email: `health-owner-${suffix}@example.invalid`, name: "Health test owner" },
    });
    createdUsers.push(owner.id);
    const member = await prisma.user.create({
      data: { email: `health-member-${suffix}@example.invalid`, name: "Health test member" },
    });
    createdUsers.push(member.id);

    const workspace = await prisma.workspace.create({
      data: { name: "Health test workspace", slug: `health-${suffix}` },
    });
    createdWorkspaces.push(workspace.id);
    await prisma.workspaceMember.createMany({
      data: [
        { workspaceId: workspace.id, userId: owner.id, role: "OWNER" },
        { workspaceId: workspace.id, userId: member.id, role: "MEMBER" },
      ],
    });
    const project = await prisma.project.create({
      data: { workspaceId: workspace.id, name: "Health project", slug: `health-proj-${suffix}` },
    });
    const deployment = await prisma.deployment.create({
      data: { workspaceId: workspace.id, projectId: project.id, environment: "DEVELOPMENT", triggerSource: "MANUAL" },
    });
    const ownerSession = await createTestSession(owner.id, workspace.id);
    const memberSession = await createTestSession(member.id, workspace.id);

    const otherWorkspace = await prisma.workspace.create({
      data: { name: "Health other workspace", slug: `health-other-${suffix}` },
    });
    createdWorkspaces.push(otherWorkspace.id);
    const otherProject = await prisma.project.create({
      data: { workspaceId: otherWorkspace.id, name: "Health other project", slug: `health-other-proj-${suffix}` },
    });

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const healthUrl = (projectId: string, deploymentId: string) =>
      `http://127.0.0.1:${address.port}/api/v1/projects/${projectId}/deployments/${deploymentId}/health`;
    const ownHealth = healthUrl(project.id, deployment.id);

    // Unauthenticated access is rejected.
    assert.equal((await fetch(ownHealth)).status, 401);

    // Cross-workspace access is rejected as not found.
    assert.equal((await fetch(healthUrl(otherProject.id, randomUUID()), { headers: { Cookie: ownerSession.cookie } })).status, 404);

    // Invalid deployment/project combinations return 404.
    assert.equal((await fetch(healthUrl(project.id, randomUUID()), { headers: { Cookie: ownerSession.cookie } })).status, 404);
    assert.equal((await fetch(healthUrl(randomUUID(), deployment.id), { headers: { Cookie: ownerSession.cookie } })).status, 404);

    // Authorized workspace members can read health information.
    const memberRead = await fetch(ownHealth, { headers: { Cookie: memberSession.cookie } });
    assert.equal(memberRead.status, 200);
    const memberBody = await memberRead.json() as { data: { schemaAvailable: boolean; checks: unknown[]; recovery: { allowed: boolean; reason: string | null } } };
    assert.equal(typeof memberBody.data.schemaAvailable, "boolean");
    assert.ok(Array.isArray(memberBody.data.checks));
    assert.equal(typeof memberBody.data.recovery.allowed, "boolean");

    const missingRuntime = (memberBody.data as typeof memberBody.data & {
      automaticRecovery: { configured: boolean; circuitState: string | null; consecutiveFailures: number | null; rejectionReason: string };
    }).automaticRecovery;
    assert.equal(missingRuntime.configured, false);
    assert.equal(missingRuntime.circuitState, null);
    assert.equal(missingRuntime.consecutiveFailures, null);
    assert.equal(missingRuntime.rejectionReason, "RUNTIME_MISSING");

    // GET requests do not mutate database state.
    const before = {
      states: await prisma.deploymentHealthState.count({ where: { deploymentId: deployment.id } }),
      checks: await prisma.deploymentHealthCheck.count({ where: { deploymentId: deployment.id } }),
      recoveries: await prisma.deploymentRecoveryAttempt.count({ where: { deploymentId: deployment.id } }),
    };
    const ownerRead = await fetch(ownHealth, { headers: { Cookie: ownerSession.cookie } });
    assert.equal(ownerRead.status, 200);
    const after = {
      states: await prisma.deploymentHealthState.count({ where: { deploymentId: deployment.id } }),
      checks: await prisma.deploymentHealthCheck.count({ where: { deploymentId: deployment.id } }),
      recoveries: await prisma.deploymentRecoveryAttempt.count({ where: { deploymentId: deployment.id } }),
    };
    assert.deepEqual(after, before);

    // Non-owner/admin users cannot initiate recovery.
    const memberRecover = await fetch(`${ownHealth.replace(/\/health$/, "")}/recoveries`, {
      method: "POST",
      headers: { Cookie: memberSession.cookie, "Content-Type": "application/json", "X-CSRF-Token": memberSession.csrf, "Idempotency-Key": randomUUID() },
    });
    assert.equal(memberRecover.status, 403);

    // Manual recovery disabled state is respected (only observable when the
    // isolated recovery integration is not enabled).
    if (!recoveryIntegration) {
      const ownerBody = await ownerRead.json() as { data: { recovery: { reason: string | null } } };
      assert.equal(ownerBody.data.recovery.reason, "DISABLED");
      assert.equal(ownerBody.data.recovery.allowed, false);
    }

    if (recoveryIntegration) {
      // Build an eligible, running runtime with a repository so recovery
      // availability can be exercised against real fixtures.
      await prisma.repository.create({ data: {
        workspaceId: workspace.id, projectId: project.id, provider: "github", externalId: randomUUID(),
        name: "fixture", fullName: "example/health-endpoint-test",
      } });
      const build = await prisma.deploymentBuild.create({ data: {
        workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id,
        sourcePreparationId: randomUUID(), resolvedCommitSha: "a".repeat(40), status: "SUCCEEDED",
      } });
      // Two runtime attempts: only the current (highest attempt) runtime's
      // checks must be returned.
      const previousRuntime = await prisma.deploymentRuntimeInstance.create({ data: {
        workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id, buildId: build.id,
        runtimeAttempt: 1, status: "STOPPED",
      } });
      const currentRuntime = await prisma.deploymentRuntimeInstance.create({ data: {
        workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id, buildId: build.id,
        runtimeAttempt: 2, status: "RUNNING", readyAt: new Date(),
        containerId: "a".repeat(64), imageId: `sha256:${"b".repeat(64)}`, hostPort: 3001, hostBindingAddress: "127.0.0.1",
      } });
      await prisma.deployment.update({ where: { id: deployment.id }, data: { status: "RUNNING" } });
      const healthScope = { workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id, runtimeInstanceId: currentRuntime.id, generation: 0 };
      await prisma.deploymentHealthState.create({ data: {
        ...healthScope, status: "UNHEALTHY", consecutiveFailures: 3, nextCheckAt: new Date(),
      } });
      // 60 checks on the current runtime; history must be capped at 50.
      await prisma.deploymentHealthCheck.createMany({ data: Array.from({ length: 60 }, (_, index) => ({
        workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id,
        runtimeInstanceId: currentRuntime.id, generation: 0,
        dockerResult: "PASSED" as const, httpResult: "PASSED" as const, httpStatus: 200,
        responseLatencyMs: index, checkedAt: new Date(Date.now() - index * 1000),
      })) });
      // Create the required health state for the previous runtime.
      await prisma.deploymentHealthState.create({ data: {
        ...healthScope,
        runtimeInstanceId: previousRuntime.id,
        status: "HEALTHY",
        nextCheckAt: null,
      } });

      // A check on the previous runtime must never surface.
      const previousCheck = await prisma.deploymentHealthCheck.create({ data: {
        workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id,
        runtimeInstanceId: previousRuntime.id, generation: 0,
        dockerResult: "PASSED" as const, httpResult: "PASSED" as const, httpStatus: 200, checkedAt: new Date(),
      } });

      const currentRead = await fetch(ownHealth, { headers: { Cookie: ownerSession.cookie } });
      assert.equal(currentRead.status, 200);
      const currentBody = await currentRead.json() as {
        data: { runtimeStatus: string | null; checks: Array<{ id: string; generation: number }> };
      };
      assert.equal(currentBody.data.runtimeStatus, "RUNNING");
      assert.equal(currentBody.data.checks.length, 50);
      assert.ok(currentBody.data.checks.every((check) => check.id !== previousCheck.id));
      assert.ok(currentBody.data.checks.every((check) => check.generation === 0));

      const automaticSnapshot = async () => {
        const response = await fetch(ownHealth, { headers: { Cookie: ownerSession.cookie } });
        assert.equal(response.status, 200);
        const body = await response.json() as { data: { automaticRecovery: {
          configured: boolean; workerAvailability: string; circuitState: string | null; eligible: boolean;
          rejectionReason: string | null; remainingBudget: number | null; maxBudget: number | null;
          consecutiveFailures: number | null; cooldownRemainingMs: number | null; nextEligibleAt: string | null;
          manualInterventionRequired: boolean | null; latestOutcome: { trigger: string } | null; snapshotAt: string;
        } } };
        return body.data.automaticRecovery;
      };
      const stamp = new Date().toISOString();
      await publishWorkerHeartbeat(redisProducerConnection, {
        prefix: queueEnv.QUEUE_PREFIX, ttlSeconds: queueEnv.WORKER_HEARTBEAT_TTL_SECONDS,
      }, {
        workerId: randomUUID(), role: "deployment", version: "0.1.0", available: true, draining: false,
        concurrency: 1, startupAt: stamp, lastHeartbeatAt: stamp,
        capabilities: {
          diagnostic: false, deploymentQueue: true, sourceRetrieval: true, workspaceStorage: true,
          dockerBuild: true, containerExecution: true, containerRuntime: true, healthMonitoring: true,
          automaticRecovery: { configured: false, available: false, trustedRepositories: ["example/health-endpoint-test"] },
        },
      });
      const baseline = await automaticSnapshot();
      assert.equal(baseline.configured, false);
      assert.equal(baseline.workerAvailability, "DISABLED");
      assert.equal(baseline.eligible, false);
      assert.equal(baseline.rejectionReason, "AUTO_DISABLED");
      assert.equal(baseline.circuitState, "CLOSED");
      assert.equal(baseline.remainingBudget, recoveryPolicy.limit);
      assert.equal(baseline.maxBudget, recoveryPolicy.limit);

      const admin = await prisma.user.create({ data: { email: `health-admin-${suffix}@example.invalid` } });
      createdUsers.push(admin.id);
      await prisma.workspaceMember.create({ data: { workspaceId: workspace.id, userId: admin.id, role: "ADMIN" } });
      const adminSession = await createTestSession(admin.id, workspace.id);
      const adminRead = await fetch(ownHealth, { headers: { Cookie: adminSession.cookie } });
      const adminBody = await adminRead.json() as { data: { recovery: { allowed: boolean } } };
      assert.equal(adminRead.status, 200);
      assert.equal(adminBody.data.recovery.allowed, true);
      const memberRuntimeRead = await fetch(ownHealth, { headers: { Cookie: memberSession.cookie } });
      const memberRuntimeBody = await memberRuntimeRead.json() as { data: { recovery: { allowed: boolean; reason: string } } };
      assert.equal(memberRuntimeRead.status, 200);
      assert.equal(memberRuntimeBody.data.recovery.reason, "FORBIDDEN");
      assert.equal(memberBody.data.recovery.allowed, false);

      // Active recovery is reflected in availability.
      await prisma.deploymentRecoveryAttempt.create({ data: {
        ...healthScope, action: "RESTART", trigger: "MANUAL", idempotencyKey: randomUUID(), status: "REQUESTED",
      } });
      const activeBody = await (await fetch(ownHealth, { headers: { Cookie: ownerSession.cookie } })).json() as { data: { recovery: { allowed: boolean; reason: string | null } } };
      assert.equal(activeBody.data.recovery.reason, "ACTIVE");
      assert.equal(activeBody.data.recovery.allowed, false);

      // Cooldown is reflected after a recent failed attempt.
      await prisma.deploymentRecoveryAttempt.deleteMany({ where: { deploymentId: deployment.id } });
      await prisma.deploymentRecoveryAttempt.create({ data: {
        ...healthScope, action: "RESTART", trigger: "MANUAL", idempotencyKey: randomUUID(), status: "FAILED",
        attemptCount: 1, startedAt: new Date(Date.now() - 1000), finishedAt: new Date(), failureReason: "RECOVERY_EXECUTION_FAILED",
      } });
      const cooldownBody = await (await fetch(ownHealth, { headers: { Cookie: ownerSession.cookie } })).json() as { data: { recovery: { reason: string | null } } };
      assert.equal(cooldownBody.data.recovery.reason, "COOLDOWN");

      const cooling = await automaticSnapshot();
      assert.equal(cooling.circuitState, "OPEN");
      assert.equal(cooling.remainingBudget, recoveryPolicy.limit - 1);
      assert.equal(cooling.consecutiveFailures, 1);
      assert.equal(cooling.latestOutcome?.trigger, "MANUAL");
      assert.ok(cooling.cooldownRemainingMs && cooling.cooldownRemainingMs <= recoveryPolicy.cooldownMs);
      assert.equal(Date.parse(cooling.nextEligibleAt!), Date.parse(cooling.snapshotAt) + cooling.cooldownRemainingMs);

      // Exhausted budget is reflected once the window is full.
      await prisma.deploymentRecoveryAttempt.deleteMany({ where: { deploymentId: deployment.id } });
      await prisma.deploymentRecoveryAttempt.createMany({ data: Array.from({ length: recoveryPolicy.limit }, () => ({
        workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id, runtimeInstanceId: currentRuntime.id,
        generation: 0, action: "RESTART" as const, trigger: "MANUAL" as const, idempotencyKey: randomUUID(),
        status: "FAILED" as const, attemptCount: 1, startedAt: new Date(Date.now() - 61_000), finishedAt: new Date(Date.now() - 60_000), failureReason: "RECOVERY_EXECUTION_FAILED" as const,
      })) });
      const exhaustedBody = await (await fetch(ownHealth, { headers: { Cookie: ownerSession.cookie } })).json() as { data: { recovery: { reason: string | null } } };
      assert.equal(exhaustedBody.data.recovery.reason, "EXHAUSTED");

      assert.equal((await automaticSnapshot()).remainingBudget, 0);
      await prisma.deploymentRecoveryAttempt.updateMany({
        where: { deploymentId: deployment.id }, data: { createdAt: new Date(Date.now() - recoveryPolicy.windowMs - 5_000) },
      });
      const halfOpen = await automaticSnapshot();
      assert.equal(halfOpen.circuitState, "HALF_OPEN");
      assert.equal(halfOpen.consecutiveFailures, 3);
      assert.equal(halfOpen.remainingBudget, recoveryPolicy.limit);

      const authorization = await prisma.deploymentRecoveryAttempt.create({ data: {
        ...healthScope, action: "RESTART", trigger: "AUTOMATIC", status: "REQUESTED", idempotencyKey: randomUUID(),
      } });
      await prisma.auditLog.create({ data: {
        workspaceId: workspace.id, entityId: deployment.id, entityType: "Deployment", action: "DEPLOYMENT_RECOVERY_REQUESTED",
        metadata: { recoveryId: authorization.id, automaticCircuitState: "HALF_OPEN" },
      } });
      const reserved = await automaticSnapshot();
      assert.equal(reserved.circuitState, "HALF_OPEN");
      assert.equal(reserved.nextEligibleAt, null);
      const history = await fetch(ownHealth.replace(/\/health$/, "/recoveries"), { headers: { Cookie: memberSession.cookie } });
      assert.equal(history.status, 200);
      const historyBody = await history.json() as { data: Array<{ trigger: string; status: string }> };
      assert.ok(historyBody.data.some(({ trigger, status }) => trigger === "AUTOMATIC" && status === "REQUESTED"));
      assert.ok(historyBody.data.some(({ trigger, status }) => trigger === "MANUAL" && status === "FAILED"));
      assert.equal(await queueManager.getQueue("runtime").getJob(`recovery-${authorization.id}`), undefined);

      // A healthy runtime is not eligible for recovery.
      await prisma.deploymentRecoveryAttempt.deleteMany({ where: { deploymentId: deployment.id } });
      await prisma.deploymentHealthState.update({ where: {
        workspaceId_projectId_deploymentId_runtimeInstanceId_generation: {
          workspaceId: workspace.id, projectId: project.id, deploymentId: deployment.id, runtimeInstanceId: currentRuntime.id, generation: 0,
        },
      }, data: { status: "HEALTHY", nextCheckAt: new Date() } });
      const healthyBody = await (await fetch(ownHealth, { headers: { Cookie: ownerSession.cookie } })).json() as { data: { recovery: { reason: string | null } } };
      assert.equal(healthyBody.data.recovery.reason, "NOT_UNHEALTHY");
    }
  } finally {
    if (server.listening) {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
      });
    }
    if (createdWorkspaces.length) {
      await prisma.auditLog.deleteMany({ where: { workspaceId: { in: createdWorkspaces } } });
      await prisma.workspace.deleteMany({ where: { id: { in: createdWorkspaces } } });
    }
    if (createdUsers.length) {
      await prisma.user.deleteMany({ where: { id: { in: createdUsers } } });
    }

  }
});
test.after(async () => {
  await disconnectQueueProducer();
  redisProducerConnection.disconnect();
  await disconnectDatabase();
});
