import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import {
  prisma,
  disconnectDatabase,
  recoverySchemaReady,
  reserveManualRecovery
} from "@cloudpilot/database";
import { buildOwnershipLabels } from "@cloudpilot/queue";
import { processRecoveryJob } from "../src/health/recovery-processor.js";
import { DockerRuntimeExecutor } from "../src/runtime/docker-runtime-client.js";

const docker = (...args: string[]) =>
  execFileSync("docker", args, {
    encoding: "utf8",
    timeout: 120000
  }).trim();

test("real Docker manual recovery", { timeout: 240000 }, async () => {
  const db = new URL(process.env.DATABASE_URL ?? "");

  assert.equal(db.hostname, "127.0.0.1");
  assert.equal(db.port, "63479");
  assert.equal(db.pathname, "/cloudpilot_health_test_574");
  assert.equal(process.env.MANUAL_RECOVERY_ENABLED, "true");
  assert.equal(await recoverySchemaReady(prisma), true);

  const suffix = randomUUID();
  const actor = await prisma.user.create({
    data: { email: `${suffix}@example.invalid` }
  });

  let workspaceId: string | undefined;
  let containerId: string | undefined;

  try {
    const workspace = await prisma.workspace.create({
      data: {
        name: "Live recovery test",
        slug: `live-recovery-${suffix}`
      }
    });

    workspaceId = workspace.id;

    const project = await prisma.project.create({
      data: {
        workspaceId,
        name: "Live recovery",
        slug: "live-recovery"
      }
    });

    await prisma.repository.create({
      data: {
        workspaceId,
        projectId: project.id,
        provider: "github",
        externalId: randomUUID(),
        name: "live-recovery",
        fullName: "example/live-recovery"
      }
    });

    const deployment = await prisma.deployment.create({
      data: {
        workspaceId,
        projectId: project.id,
        environment: "DEVELOPMENT",
        triggerSource: "MANUAL",
        status: "RUNNING"
      }
    });

    const scope = {
      workspaceId,
      projectId: project.id,
      deploymentId: deployment.id
    };

    const image = "node:22-alpine";
    docker("pull", image);

    const imageId = docker(
      "image", "inspect", "--format", "{{.Id}}", image
    );

    const build = await prisma.deploymentBuild.create({
      data: {
        ...scope,
        sourcePreparationId: randomUUID(),
        resolvedCommitSha: "c".repeat(40),
        status: "SUCCEEDED",
        imageId
      }
    });

    const runtimeId = randomUUID();

    const labels = buildOwnershipLabels({
      ...scope,
      buildId: build.id,
      runtimeInstanceId: runtimeId,
      attempt: 1
    });

    const labelArgs = Object.entries(labels).flatMap(
      ([key, value]) => ["--label", `${key}=${value}`]
    );

    const name = `cloudpilot-${project.id}-${deployment.id}-1`;

    containerId = docker(
      "run", "-d",
      "--name", name,
      "--restart", "no",
      "--memory", "128m",
      "--cpus", "0.5",
      "--pids-limit", "64",
      "-p", "127.0.0.1:63299:3000",
      ...labelArgs,
      imageId,
      "node", "-e",
      "require('http').createServer((q,s)=>s.writeHead(200).end('OK')).listen(3000,'0.0.0.0')"
    );

    const inspect = () =>
      JSON.parse(docker("inspect", containerId!))[0];

    const before = inspect();
    const port = Number(
      before.NetworkSettings.Ports["3000/tcp"][0].HostPort
    );

    const url = `http://127.0.0.1:${port}/health`;

    let ready = false;

    for (let i = 0; i < 30; i++) {
      try {
        const response = await fetch(url, {
          signal: AbortSignal.timeout(1000)
        });

        await response.text();

        if (response.status === 200) {
          ready = true;
          break;
        }
      } catch {}

      await delay(500);
    }

    assert.ok(ready, "Test container did not become ready");

    await prisma.deploymentRuntimeInstance.create({
      data: {
        id: runtimeId,
        ...scope,
        buildId: build.id,
        status: "RUNNING",
        readyAt: new Date(before.State.StartedAt),
        containerId,
        imageId,
        hostPort: port,
        hostBindingAddress: "127.0.0.1",
        containerPort: 3000
      }
    });

    await prisma.deploymentHealthState.create({
      data: {
        ...scope,
        runtimeInstanceId: runtimeId,
        generation: 0,
        status: "UNHEALTHY",
        consecutiveFailures: 3,
        nextCheckAt: new Date()
      }
    });

    const reservation = await reserveManualRecovery(
      prisma, scope, actor.id, randomUUID()
    );

    assert.equal(reservation.kind, "reserved");

    if (reservation.kind !== "reserved") {
      throw new Error(reservation.kind);
    }

    const recoveryId = reservation.attempt.id;

    const job = {
      ...scope,
      kind: "runtime" as const,
action: "recover" as const,
      recoveryTrigger: "MANUAL" as const,
      correlationId: recoveryId,
      buildId: build.id,
      runtimeInstanceId: runtimeId
    };

    const executor = new DockerRuntimeExecutor();

    const result = await processRecoveryJob(
      `recovery-${recoveryId}`,
      job,
      executor,
      new AbortController().signal
    );

    const after = inspect();

    const recovery = await prisma.deploymentRecoveryAttempt
      .findUniqueOrThrow({ where: { id: recoveryId } });

    const health = await prisma.deploymentHealthState
      .findFirstOrThrow({
        where: scope,
        orderBy: { generation: "desc" }
      });

    const check = await prisma.deploymentHealthCheck
      .findFirst({ where: scope });

    console.log(JSON.stringify({
      recoveryId,
      runtimeId,
      result,
      beforeStartedAt: before.State.StartedAt,
      afterStartedAt: after.State.StartedAt,
      sameContainer: before.Id === after.Id,
      sameImage: before.Image === after.Image,
      portBefore: port,
      portAfter: after.NetworkSettings.Ports["3000/tcp"][0].HostPort,
      recoveryStatus: recovery.status,
      failureReason: recovery.failureReason,
      restartIssuedAt: recovery.restartIssuedAt,
      restartSourceStartedAt: recovery.restartSourceStartedAt,
      healthStatus: health.status,
      generation: health.generation,
      httpStatus: check?.httpStatus
    }, null, 2));

    assert.equal(result.status, "succeeded");
    assert.notEqual(before.State.StartedAt, after.State.StartedAt);
    assert.equal(before.Id, after.Id);
    assert.equal(before.Image, after.Image);
    assert.equal(
      Number(after.NetworkSettings.Ports["3000/tcp"][0].HostPort),
      port
    );
    assert.equal(recovery.status, "SUCCEEDED");
    assert.equal(recovery.attemptCount, 1);
    assert.ok(recovery.restartIssuedAt);
    assert.equal(recovery.restartSourceStartedAt, before.State.StartedAt);
    assert.equal(health.status, "HEALTHY");
    assert.equal(health.generation, 1);
    assert.equal(check?.httpStatus, 200);

    console.log("REAL DOCKER RECOVERY PASSED");

  } finally {
    try {
      if (containerId) docker("rm", "-f", containerId);
    } finally {
      if (workspaceId) {
        await prisma.auditLog.deleteMany({
          where: { workspaceId }
        });

        await prisma.workspace.delete({
          where: { id: workspaceId }
        });
      }

      await prisma.user.delete({
        where: { id: actor.id }
      });
    }
  }
});

test.after(async () => {
  await disconnectDatabase();
});

