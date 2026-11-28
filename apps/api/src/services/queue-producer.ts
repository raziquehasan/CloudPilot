import {
  QueueManager,
  createProducerConnection,
  parseQueueEnv,
  type DiagnosticJob,
  type RuntimeJob,
} from "@cloudpilot/queue";
import { env } from "../config/env.js";

export const queueEnv = parseQueueEnv();
export const redisProducerConnection = createProducerConnection(env.REDIS_URL);

export const queueManager = new QueueManager(
  redisProducerConnection,
  redisProducerConnection,
  {
    attempts: queueEnv.QUEUE_JOB_ATTEMPTS,
    backoffMs: queueEnv.QUEUE_BACKOFF_MS,
    completedRetention: queueEnv.QUEUE_COMPLETED_RETENTION,
    failedRetention: queueEnv.QUEUE_FAILED_RETENTION,
    executionTimeoutMs: queueEnv.QUEUE_EXECUTION_TIMEOUT_MS,
    backoffJitter: queueEnv.QUEUE_BACKOFF_JITTER,
  },
  true,
  queueEnv.QUEUE_PREFIX,
);

export async function enqueueDiagnosticJob(
  payload: DiagnosticJob,
  jobId: string,
) {
  return queueManager.addJob("diagnostic", payload, QueueManager.validateJobId(jobId));
}

export async function removeQueuedDeploymentJob(jobId: string): Promise<boolean> {
  const job = await queueManager.getQueue("deployment").getJob(QueueManager.validateJobId(jobId));
  if (!job) return false;
  const state = await job.getState();
  if (!["waiting", "delayed", "paused", "prioritized"].includes(state)) return false;
  await job.remove();
  return true;
}

export async function enqueueRuntimeLifecycleJob(input: {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  runtimeInstanceId: string;
  buildId: string;
  action: "start" | "stop" | "restart";
  jobId: string;
}): Promise<void> {
  const payload: RuntimeJob = {
    kind: "runtime",
    workspaceId: input.workspaceId,
    projectId: input.projectId,
    deploymentId: input.deploymentId,
    runtimeInstanceId: input.runtimeInstanceId,
    buildId: input.buildId,
    action: input.action,
    correlationId: input.jobId,
  };
  await queueManager.addJob("runtime", payload, QueueManager.validateJobId(input.jobId));
}

export async function disconnectQueueProducer(): Promise<void> {
  await queueManager.close();
}

export async function enqueueRecoveryJob(input: Omit<RuntimeJob, "kind" | "action" | "correlationId">, recoveryId: string) {
  const jobId = QueueManager.validateJobId(`recovery-${recoveryId}`);
  const queue = queueManager.getQueue("runtime");
  const existing = await queue.getJob(jobId);
  if (!existing) {
    await queueManager.addJob("runtime", { ...input, kind: "runtime", action: "recover", correlationId: recoveryId }, jobId);
  } else if (await existing.getState() === "failed") {
    try {
      await existing.retry("failed", { resetAttemptsMade: true, resetAttemptsStarted: true });
    } catch (error) {
      const state = await existing.getState();
      if (state !== "waiting" && state !== "active" && state !== "delayed") throw error;
    }
  }
}
