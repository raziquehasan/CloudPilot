import { Router } from "express";
import type { Request, Response } from "express";
import {
  currentRecoveryRuntime, isRecoveryRuntimeEligible, prisma, publicRecoverySelect,
  recoveryNow, recoveryPolicy, recoverySchemaReady, reserveManualRecovery,
} from "@cloudpilot/database";
import { env } from "../config/env.js";
import { requireAuth, requireCsrf } from "../middlewares/auth.middleware.js";
import { enqueueRecoveryJob } from "../services/queue-producer.js";
import { deploymentIdSchema } from "../validators/deployment.validator.js";
import { canManage, resolveProject, sendError } from "./project-build-configuration.controller.js";

function recoveryResponse<T extends { failureReason: string | null }>(attempt: T) {
  return { ...attempt, failureMessage: attempt.failureReason === "RECOVERY_PORT_CHANGED"
    ? "The original loopback port could not be verified. Public routing needs repair." : null };
}

async function getScope(req: Request, res: Response) {
  res.set("Cache-Control", "no-store");
  const project = await resolveProject(req, res);
  if (!project) return null;
  const deploymentId = deploymentIdSchema.safeParse(req.params.deploymentId);
  if (!deploymentId.success) {
    sendError(res, 404, "DEPLOYMENT_NOT_FOUND", "Deployment not found in this project.");
    return null;
  }
  return { project, scope: { workspaceId: project.workspaceId, projectId: project.projectId, deploymentId: deploymentId.data } };
}

async function requestRecovery(req: Request, res: Response) {
  const target = await getScope(req, res);
  if (!target) return;
  const { scope, project } = target;
  if (!req.userId || !canManage(project.role)) {
    sendError(res, 403, "FORBIDDEN", "Workspace administrator access is required.");
    return;
  }
  const key = deploymentIdSchema.safeParse(req.get("Idempotency-Key"));
  if (!key.success) {
    sendError(res, 400, "INVALID_IDEMPOTENCY_KEY", "A UUID Idempotency-Key is required.");
    return;
  }
  try {
    if (!env.MANUAL_RECOVERY_ENABLED || !await recoverySchemaReady(prisma)) {
      sendError(res, 503, "RECOVERY_DISABLED", "Manual recovery is not enabled.");
      return;
    }
    const result = await reserveManualRecovery(prisma, scope, req.userId, key.data);
    if (result.kind !== "reserved" && result.kind !== "replay") {
      const messages = {
        not_found: "Deployment not found in this project.",
        ineligible: "Only a currently running, unhealthy runtime can be recovered.",
        busy: "A runtime operation is already in progress.",
        cooldown: "The deployment is in its recovery cooldown.",
        exhausted: "The deployment has exhausted its recovery budget.",
      };
      sendError(res, result.kind === "not_found" ? 404 : 409, `RECOVERY_${result.kind.toUpperCase()}`, messages[result.kind]);
      return;
    }
    const { healthState, ...data } = result.attempt;
    if (data.status === "REQUESTED" || data.status === "RUNNING") {
      await enqueueRecoveryJob({ ...scope, runtimeInstanceId: data.runtimeInstanceId, buildId: healthState.runtime.buildId }, data.id);
      res.status(202).json({ success: true, data: recoveryResponse(data) });
    } else res.json({ success: true, data: recoveryResponse(data) });
  } catch {
    sendError(res, 503, "RECOVERY_UNAVAILABLE", "Recovery could not be submitted. Retry with the same Idempotency-Key.");
  }
}

async function readRecovery(req: Request, res: Response) {
  const target = await getScope(req, res);
  if (!target) return;
  const { scope } = target;
  try {
    const deployment = await prisma.deployment.findFirst({
      where: { id: scope.deploymentId, workspaceId: scope.workspaceId, projectId: scope.projectId }, select: { id: true },
    });
    if (!deployment) {
      sendError(res, 404, "DEPLOYMENT_NOT_FOUND", "Deployment not found in this project.");
      return;
    }
    if (!await recoverySchemaReady(prisma)) {
      sendError(res, 503, "RECOVERY_UNAVAILABLE", "Recovery history is unavailable.");
      return;
    }
    if (req.params.recoveryId === undefined) {
      const data = await prisma.deploymentRecoveryAttempt.findMany({
        where: scope, select: publicRecoverySelect, orderBy: [{ createdAt: "desc" }, { id: "desc" }], take: 100,
      });
      res.json({ success: true, data: data.map(recoveryResponse) });
      return;
    }
    const id = deploymentIdSchema.safeParse(req.params.recoveryId);
    const data = id.success ? await prisma.deploymentRecoveryAttempt.findFirst({ where: { ...scope, id: id.data }, select: publicRecoverySelect }) : null;
    if (!data) {
      sendError(res, 404, "RECOVERY_NOT_FOUND", "Recovery attempt not found in this deployment.");
      return;
    }
    res.json({ success: true, data: recoveryResponse(data) });
  } catch {
    sendError(res, 503, "RECOVERY_UNAVAILABLE", "Recovery history is unavailable.");
  }
}

export const deploymentRecoveryRouter = Router();
const path = "/:projectId/deployments/:deploymentId/recoveries";
deploymentRecoveryRouter.post(path, requireAuth, requireCsrf, requestRecovery);
deploymentRecoveryRouter.get(path, requireAuth, readRecovery);
deploymentRecoveryRouter.get(`${path}/:recoveryId`, requireAuth, readRecovery);

async function readDeploymentHealth(req: Request, res: Response) {
  const target = await getScope(req, res);
  if (!target) return;
  const { scope, project } = target;
  try {
    const deployment = await prisma.deployment.findFirst({
      where: { id: scope.deploymentId, workspaceId: scope.workspaceId, projectId: scope.projectId },
      select: { id: true },
    });
    if (!deployment) {
      sendError(res, 404, "DEPLOYMENT_NOT_FOUND", "Deployment not found in this project.");
      return;
    }
    const [runtime, [schema]] = await Promise.all([
      currentRecoveryRuntime(prisma, scope),
      prisma.$queryRaw<Array<{ available: boolean }>>`
        SELECT to_regclass('public.deployment_health_states') IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM pg_attribute
            WHERE attrelid = to_regclass('public.deployment_health_checks')
              AND attname = 'dockerState' AND NOT attisdropped
          ) AS available
      `,
    ]);
    const schemaAvailable = schema?.available === true;
    const runtimeStatus = runtime?.status ?? null;
    const [state, checks] = schemaAvailable && runtime ? await Promise.all([
      prisma.deploymentHealthState.findFirst({
        where: { ...scope, runtimeInstanceId: runtime.id }, orderBy: { generation: "desc" },
        select: {
          status: true, generation: true, consecutiveFailures: true, lastCheckedAt: true,
          lastHealthyAt: true, nextCheckAt: true, lastFailureReason: true,
        },
      }),
      prisma.deploymentHealthCheck.findMany({
        where: { ...scope, runtimeInstanceId: runtime.id }, orderBy: [{ checkedAt: "desc" }, { id: "desc" }], take: 50,
        select: {
          id: true, generation: true, dockerResult: true, dockerState: true, httpResult: true,
          httpStatus: true, responseLatencyMs: true, failureReason: true, checkedAt: true,
        },
      }),
    ]) : [null, []];

    const reason = await recoveryAvailability();
    res.json({
      success: true,
      data: { schemaAvailable, runtimeStatus, state, checks, recovery: { allowed: reason === null, reason } },
    });

    async function recoveryAvailability() {
      if (!canManage(project.role)) return "FORBIDDEN";
      if (!env.MANUAL_RECOVERY_ENABLED) return "DISABLED";
      if (!schemaAvailable || !await recoverySchemaReady(prisma)) return "SCHEMA_UNAVAILABLE";
      if (!isRecoveryRuntimeEligible(runtime)) return "RUNTIME_NOT_RUNNING";

      const now = await recoveryNow(prisma);
      if (runtime.executionLeaseExpiresAt && runtime.executionLeaseExpiresAt > now) return "ACTIVE";
      const active = await prisma.deploymentRecoveryAttempt.findFirst({
        where: { ...scope, status: { in: ["REQUESTED", "RUNNING"] } }, select: { id: true },
      });
      if (active) return "ACTIVE";
      if (state?.status === "FAILED" && state.lastFailureReason === "RECOVERY_EXHAUSTED") return "EXHAUSTED";
      if (state?.status !== "UNHEALTHY" || !state.nextCheckAt) return "NOT_UNHEALTHY";
      const recent = await prisma.deploymentRecoveryAttempt.findMany({
        where: { ...scope, createdAt: { gt: new Date(now.getTime() - recoveryPolicy.windowMs) } },
        orderBy: { createdAt: "desc" }, take: recoveryPolicy.limit,
        select: { status: true, finishedAt: true },
      });
      if (recent.length >= recoveryPolicy.limit) return "EXHAUSTED";
      const last = recent[0];
      if (last?.status === "FAILED" && last.finishedAt &&
        now.getTime() < last.finishedAt.getTime() + recoveryPolicy.cooldownMs * 2 ** (recent.length - 1)) return "COOLDOWN";
      return null;
    }
  } catch {
    sendError(res, 503, "DEPLOYMENT_HEALTH_UNAVAILABLE", "Deployment health is temporarily unavailable.");
  }
}

deploymentRecoveryRouter.get("/:projectId/deployments/:deploymentId/health", requireAuth, readDeploymentHealth);
