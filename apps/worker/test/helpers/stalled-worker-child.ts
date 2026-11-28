// Test-only stalled worker (Phase 5.7.10). Processes a recovery job but
// deliberately exceeds the queue lock without acknowledging, so BullMQ
// marks the job stalled and redelivers it. The parent kills this process.
import { Worker } from "bullmq";
import { processRecoveryJob } from "../../src/health/recovery-processor.js";
import { DockerRuntimeExecutor } from "../../src/runtime/docker-runtime-client.js";

type Input = {
  queueName: string;
  connection: { host: string; port: number; maxRetriesPerRequest: null };
  containerId: string;
  imageId: string;
  port: number;
  restartedEpoch: string;
};

const input = JSON.parse(process.env.CLOUDPILOT_STALLED_INPUT ?? "{}") as Input;
let startedAt = "2026-11-27T01:00:00.123456789Z";

const executor = new DockerRuntimeExecutor();
executor.inspectOwnedContainer = async () => ({
  ownershipVerified: true,
  inspection: { exists: true, running: true, containerId: input.containerId, imageId: input.imageId, state: "running", startedAt, restartCount: 0, exitCode: null, hostPort: input.port },
});
executor.restartOwnedContainer = async () => { startedAt = input.restartedEpoch; };

const worker = new Worker(input.queueName, async (job) => {
  // Exceed the (test-specific) lock duration without acking.
  await new Promise((resolve) => setTimeout(resolve, 6_000));
  await processRecoveryJob(job.id, job.data as never, executor, new AbortController().signal);
}, { connection: input.connection, lockDuration: 3_000 });

process.on("SIGTERM", () => { void worker.close(); process.exit(0); });