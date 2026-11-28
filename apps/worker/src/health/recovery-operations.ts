import { setTimeout as delay } from "node:timers/promises";
import { buildOwnershipLabels } from "@cloudpilot/queue";
import type { HealthFailureReason, HealthScope } from "@cloudpilot/database";
import type { RuntimeExecutor } from "../runtime/runtime-executor.js";
import { inspectHealthRuntime, probeRuntime, type HealthRuntime, type RuntimeHealthObservation } from "./probe-runtime.js";

type Executor = Pick<RuntimeExecutor, "inspectOwnedContainer" | "restartOwnedContainer">;
export type RecoveryOperationResult =
  | { ok: true; observation: RuntimeHealthObservation; startedAt: Date }
  | { ok: false; reason: HealthFailureReason };

const fail = (reason: HealthFailureReason): RecoveryOperationResult => ({ ok: false, reason });

function validEpoch(value: string | null): value is string {
  return !!value && value.length <= 40 && !value.startsWith("0001-") &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/.test(value) && Number.isFinite(Date.parse(value));
}

export async function verifyRecoveryRestart(options: {
  scope: HealthScope; runtime: HealthRuntime; executor: Executor; signal: AbortSignal;
  sourceEpoch: string | null; timeoutMs: number;
  markIntent: (epoch: string) => Promise<boolean>;
}): Promise<RecoveryOperationResult> {
  const { scope, runtime, executor, signal } = options;
  if (!executor.inspectOwnedContainer || !executor.restartOwnedContainer) return fail("RECOVERY_DOCKER_UNAVAILABLE");
  const inspect = executor.inspectOwnedContainer.bind(executor);
  async function snapshot() {
    try {
      return await inspectHealthRuntime(scope, runtime, inspect, signal);
    } catch {
      return null;
    }
  }
  function check(result: Awaited<ReturnType<typeof snapshot>>) {
    if (signal.aborted) return "RECOVERY_TIMEOUT";
    if (!result) return "RECOVERY_DOCKER_UNAVAILABLE";
    const value = result.inspection;
    if (!result.ownershipVerified || !value.exists || value.containerId !== runtime.containerId ||
        value.imageId !== runtime.imageId || !value.running || value.state !== "running" ||
        !validEpoch(value.startedAt)) return "RECOVERY_IDENTITY_MISMATCH";
    if (value.hostPort !== runtime.port) return "RECOVERY_PORT_CHANGED";
    return null;
  }
  const before = await snapshot();
  const initialFailure = check(before);
  const observedEpoch = before?.inspection.startedAt ?? null;
  if (initialFailure || !validEpoch(observedEpoch)) return fail(initialFailure ?? "RECOVERY_IDENTITY_MISMATCH");
  const sourceEpoch = options.sourceEpoch ?? observedEpoch;
  let restartTimedOut = false;
  if (!options.sourceEpoch) {
    if (signal.aborted) return fail("RECOVERY_TIMEOUT");
    if (!await options.markIntent(observedEpoch)) return fail("RECOVERY_STALE");
    if (signal.aborted) return fail("RECOVERY_TIMEOUT");
    const restartDeadline = AbortSignal.timeout(options.timeoutMs + 10_000);
    try {
      await executor.restartOwnedContainer(runtime.containerId, buildOwnershipLabels({
        ...scope, attempt: runtime.runtimeAttempt,
      }), {
        imageId: runtime.imageId, host: runtime.host, hostPort: runtime.port,
        containerPort: runtime.containerPort, startedAt: observedEpoch,
      }, options.timeoutMs, AbortSignal.any([signal, restartDeadline]));
    } catch {
      // Docker might have restarted the container even if the command failed.
      if (signal.aborted) return fail("RECOVERY_TIMEOUT");
      restartTimedOut = restartDeadline.aborted;
    }
  }

  const after = options.sourceEpoch ? before : await snapshot();
  const finalFailure = check(after);
  const verifiedEpoch = after?.inspection.startedAt ?? null;
  if (finalFailure || !validEpoch(verifiedEpoch)) return fail(finalFailure ?? "RECOVERY_IDENTITY_MISMATCH");
  // The persisted epoch is precise even when both starts occupy the same millisecond.
  if (verifiedEpoch === sourceEpoch || Date.parse(verifiedEpoch) < Date.parse(sourceEpoch)) {
    return fail(restartTimedOut ? "RECOVERY_TIMEOUT" : "RECOVERY_RESTART_UNCERTAIN");
  }

  let reason: HealthFailureReason = "RECOVERY_EXECUTION_FAILED";
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt > 0) await delay(1_000 * attempt, undefined, { signal }).catch(() => {});
    if (signal.aborted) return fail("RECOVERY_TIMEOUT");
    const observation = await probeRuntime(scope, runtime, inspect, undefined, signal);
    if (signal.aborted) return fail("RECOVERY_TIMEOUT");
    if (!observation) return fail("RECOVERY_RESTART_UNCERTAIN");
    if (observation.docker?.startedAt !== verifiedEpoch) return fail("RECOVERY_RESTART_UNCERTAIN");
    if (!observation.failureReason) return { ok: true, observation, startedAt: new Date(verifiedEpoch) };
    reason = observation.failureReason;
  }
  return fail(reason);
}
