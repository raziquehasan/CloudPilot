// Test-only child process (Phase 5.7.9). Runs a single recovery job
// with a mock executor and crashes at the stage named in the input.
// Invoked only by recovery-crash.integration.test.ts.
import { processRecoveryJob } from "../../src/health/recovery-processor.js";
import { DockerRuntimeExecutor } from "../../src/runtime/docker-runtime-client.js";

type Input = {
  job: Parameters<typeof processRecoveryJob>[1];
  containerId: string;
  imageId: string;
  port: number;
  restartedEpoch: string;
};

const input = JSON.parse(process.env.CLOUDPILOT_CRASH_INPUT ?? "{}") as Input;
// Preserve the exact nanosecond source epoch string (no Date truncation).
let startedAt = "2026-11-27T01:00:00.123456789Z";

const executor = new DockerRuntimeExecutor();
executor.inspectOwnedContainer = async () => ({
  ownershipVerified: true,
  inspection: {
    exists: true, running: true, containerId: input.containerId, imageId: input.imageId,
    state: "running", startedAt, restartCount: 0, exitCode: null, hostPort: input.port,
  },
});
executor.restartOwnedContainer = async () => { startedAt = input.restartedEpoch; };

const maxMs = Number(process.env.CLOUDPILOT_CRASH_MAX_MS ?? "0");
const signal = maxMs > 0 ? AbortSignal.timeout(maxMs) : new AbortController().signal;

await processRecoveryJob(
  `recovery-${input.job.correlationId}`,
  input.job,
  executor,
  signal,
);