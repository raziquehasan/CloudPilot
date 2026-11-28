import assert from "node:assert/strict";
import { createServer } from "node:http";
import { randomBytes, randomUUID } from "node:crypto";
import test from "node:test";
import { prisma } from "@cloudpilot/database";
import { createApp } from "../src/app.ts";
import { hashSecret } from "../src/services/session.service.ts";
import { disconnectQueueProducer, queueManager } from "../src/services/queue-producer.ts";

const recoveryIntegration = process.env.MANUAL_RECOVERY_ENABLED === "true" &&
  /^\/cloudpilot_health_test_574$/.test(new URL(process.env.DATABASE_URL ?? "http://localhost").pathname) &&
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
    if (recoveryIntegration) await disconnectQueueProducer();
  }
});
