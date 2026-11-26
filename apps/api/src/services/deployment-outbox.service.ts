import { randomUUID } from "node:crypto";
import { Prisma, prisma } from "@cloudpilot/database";
import {
  QueueManager,
  type BuildJob,
  type DeploymentJob,
} from "@cloudpilot/queue";

export interface DeploymentOutboxPolicy {
  maxAttempts: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
  leaseMs: number;
}

interface ClaimedOutboxEvent {
  id: string;
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  eventType: "DEPLOYMENT_REQUESTED" | "BUILD_REQUESTED";
  jobId: string;
  attemptCount: number;
  leaseToken: string;
}

const defaultPolicy: DeploymentOutboxPolicy = {
  maxAttempts: 10,
  baseBackoffMs: 1000,
  maxBackoffMs: 60_000,
  leaseMs: 30_000,
};

export async function recordDeploymentOutboxEvent(
  tx: Prisma.TransactionClient,
  input: { workspaceId: string; projectId: string; deploymentId: string },
) {
  const jobId = QueueManager.deterministicJobId("deployment", [input.deploymentId]);
  return tx.deploymentOutboxEvent.create({
    data: {
      ...input,
      eventType: "DEPLOYMENT_REQUESTED",
      jobId,
    },
  });
}

export async function createDeploymentWithOutbox(
  tx: Prisma.TransactionClient,
  input: {
    workspaceId: string;
    projectId: string;
    environment: "DEVELOPMENT" | "STAGING" | "PRODUCTION" | "PREVIEW";
    triggerSource?: "MANUAL" | "GITHUB_PUSH" | "GITHUB_PULL_REQUEST" | "SCHEDULE" | "API" | "ROLLBACK";
    idempotencyKey?: string;
    requestedBranch?: string;
    gitRef?: string;
    commitSha?: string;
    commitMessage?: string;
    triggeredByUserId?: string;
    buildConfigurationSnapshot?: Prisma.InputJsonValue;
  },
) {
  const deploymentId = randomUUID();
  const jobId = QueueManager.deterministicJobId("deployment", [deploymentId]);
  const deployment = await tx.deployment.create({
    data: {
      ...input,
      id: deploymentId,
      queueJobId: jobId,
    },
  });
  await recordDeploymentOutboxEvent(tx, {
    workspaceId: input.workspaceId,
    projectId: input.projectId,
    deploymentId,
  });
  return deployment;
}

export class DeploymentOutboxPublisher {
  constructor(
    private readonly queueManager: QueueManager,
    private readonly policy: DeploymentOutboxPolicy = defaultPolicy,
    private readonly database: typeof prisma = prisma,
    private readonly clock: () => Date = () => new Date(),
    private readonly random: () => number = Math.random,
  ) {
    if (!Number.isInteger(policy.maxAttempts) || policy.maxAttempts < 1 || policy.maxAttempts > 20) {
      throw new Error("Outbox attempts must be between 1 and 20");
    }
    if (!Number.isInteger(policy.leaseMs) || policy.leaseMs < 1000 || policy.leaseMs > 300_000) {
      throw new Error("Outbox lease must be between 1000 and 300000 ms");
    }
    if (!Number.isInteger(policy.baseBackoffMs) || policy.baseBackoffMs < 1 || policy.baseBackoffMs > 60_000) {
      throw new Error("Outbox base backoff must be between 1 and 60000 ms");
    }
    if (!Number.isInteger(policy.maxBackoffMs) || policy.maxBackoffMs < policy.baseBackoffMs || policy.maxBackoffMs > 3_600_000) {
      throw new Error("Outbox maximum backoff must be between the base backoff and 3600000 ms");
    }
  }

  async publishBatch(limit = 10): Promise<{ attempted: number; published: number; deferred: number }> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
      throw new Error("Outbox batch size must be between 1 and 100");
    }
    let attempted = 0;
    let published = 0;
    let deferred = 0;
    while (attempted < limit) {
      const event = await this.claimNextEvent();
      if (!event) break;
      attempted += 1;
      const result = await this.publishClaimedEvent(event);
      if (result === "published") published += 1;
      else deferred += 1;
    }
    return { attempted, published, deferred };
  }

  private async claimNextEvent(): Promise<ClaimedOutboxEvent | null> {
    const now = this.clock();
    const leaseToken = randomUUID();
    const leaseExpiresAt = new Date(now.getTime() + this.policy.leaseMs);
    return this.database.$transaction(async (tx) => {
      const rows = await tx.$queryRaw<ClaimedOutboxEvent[]>`
        WITH candidate AS (
          SELECT "id"
          FROM "deployment_outbox_events"
          WHERE (
            "status" = 'PENDING'::"DeploymentOutboxStatus"
            AND "nextAttemptAt" <= ${now}
          ) OR (
            "status" = 'PUBLISHING'::"DeploymentOutboxStatus"
            AND "leaseExpiresAt" <= ${now}
          )
          ORDER BY "createdAt" ASC, "id" ASC
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        UPDATE "deployment_outbox_events" AS event
        SET "status" = 'PUBLISHING'::"DeploymentOutboxStatus",
            "attemptCount" = event."attemptCount" + 1,
            "leaseToken" = ${leaseToken}::uuid,
            "leaseExpiresAt" = ${leaseExpiresAt},
            "updatedAt" = ${now}
        FROM candidate
        WHERE event."id" = candidate."id"
        RETURNING event."id", event."workspaceId", event."projectId",
                  event."deploymentId", event."eventType", event."jobId",
                  event."attemptCount", event."leaseToken"
      `;
      return rows[0] ?? null;
    });
  }

  private async publishClaimedEvent(
    event: ClaimedOutboxEvent,
  ): Promise<"published" | "deferred"> {
    const deployment = await this.database.deployment.findFirst({
      where: {
        id: event.deploymentId,
        workspaceId: event.workspaceId,
        projectId: event.projectId,
      },
      select: { status: true },
    });
    if (!deployment || deployment.status === "CANCELLED" || deployment.status === "CANCELLING") {
      await this.database.deploymentOutboxEvent.updateMany({
        where: { id: event.id, status: "PUBLISHING", leaseToken: event.leaseToken },
        data: {
          status: "DEAD",
          leaseToken: null,
          leaseExpiresAt: null,
          lastErrorCode: "DEPLOYMENT_NOT_PUBLISHABLE",
        },
      });
      return "deferred";
    }

    try {
      if (event.eventType === "BUILD_REQUESTED") {
        await this.publishBuildEvent(event);
      } else {
        const payload: DeploymentJob = {
          kind: "deployment",
          workspaceId: event.workspaceId,
          projectId: event.projectId,
          deploymentId: event.deploymentId,
          eventId: event.id,
          correlationId: event.id,
          idempotencyKey: event.id,
        };
        await this.queueManager.addJob("deployment", payload, event.jobId);
      }
      await this.database.deploymentOutboxEvent.updateMany({
        where: { id: event.id, status: "PUBLISHING", leaseToken: event.leaseToken },
        data: {
          status: "PUBLISHED",
          publishedAt: this.clock(),
          leaseToken: null,
          leaseExpiresAt: null,
          lastErrorCode: null,
        },
      });
      return "published";
    } catch {
      const terminal = event.attemptCount >= this.policy.maxAttempts;
      const delay = Math.min(
        this.policy.maxBackoffMs,
        this.policy.baseBackoffMs * 2 ** Math.min(event.attemptCount - 1, 16),
      );
      const jitteredDelay = Math.max(1, Math.round(delay * (0.8 + this.random() * 0.4)));
      await this.database.deploymentOutboxEvent.updateMany({
        where: { id: event.id, status: "PUBLISHING", leaseToken: event.leaseToken },
        data: {
          status: terminal ? "DEAD" : "PENDING",
          nextAttemptAt: new Date(this.clock().getTime() + jitteredDelay),
          leaseToken: null,
          leaseExpiresAt: null,
          lastErrorCode: "QUEUE_PUBLISH_FAILED",
        },
      });
      return "deferred";
    }
  }

  /**
   * Publish a build job. The payload is derived from
   * authoritative database state and contains identifiers
   * only — never credentials or filesystem paths.
   */
  private async publishBuildEvent(
    event: ClaimedOutboxEvent,
  ): Promise<void> {
    const source = await this.database.deploymentSourcePreparation.findFirst({
      where: {
        workspaceId: event.workspaceId,
        projectId: event.projectId,
        deploymentId: event.deploymentId,
        status: "READY",
      },
      select: { id: true, resolvedCommitSha: true },
    });

    if (!source) {
      // No READY source: leave the event for retry rather
      // than publishing an invalid build.
      throw new Error("Build source preparation is not ready");
    }

    const payload: BuildJob = {
      kind: "build",
      workspaceId: event.workspaceId,
      projectId: event.projectId,
      deploymentId: event.deploymentId,
      buildId: event.id,
      sourcePreparationId: source.id,
      resolvedCommitSha: source.resolvedCommitSha,
      correlationId: event.id,
    };

    await this.queueManager.addJob("build", payload, event.jobId);
  }
}
