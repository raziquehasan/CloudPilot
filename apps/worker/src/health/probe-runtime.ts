import { buildOwnershipLabels } from "@cloudpilot/queue";
import type { HealthProbeStatus, HealthScope } from "@cloudpilot/database";
import type { HealthCheckFailure } from "./health-state.js";
import type { ContainerInspection, RuntimeExecutor } from "../runtime/runtime-executor.js";
import { defaultHealthPath, isHealthBinding, isHealthPath, probeHttpHealth } from "./http-health.js";

export interface HealthRuntime {
  runtimeAttempt: number;
  containerId: string;
  imageId: string;
  containerPort: number;
  host: string;
  port: number;
}

export interface RuntimeHealthObservation {
  scope: HealthScope;
  docker: ContainerInspection | null;
  dockerResult: HealthProbeStatus;
  httpResult: HealthProbeStatus;
  httpStatus: number | null;
  responseLatencyMs: number | null;
  failureReason: HealthCheckFailure | null;
}

export type HealthInspector = NonNullable<RuntimeExecutor["inspectOwnedContainer"]>;

export function inspectHealthRuntime(
  scope: Omit<HealthScope, "generation">, runtime: HealthRuntime, inspect: HealthInspector, signal?: AbortSignal,
) {
  return inspect(runtime.containerId, buildOwnershipLabels({
    ...scope, attempt: runtime.runtimeAttempt,
  }), runtime.containerPort, runtime.host, signal);
}

export function matchesHealthRuntime(
  snapshot: Awaited<ReturnType<HealthInspector>>, runtime: HealthRuntime,
): boolean {
  return snapshot.ownershipVerified && snapshot.inspection.exists && snapshot.inspection.running &&
    snapshot.inspection.state === "running" &&
    snapshot.inspection.containerId === runtime.containerId && snapshot.inspection.imageId === runtime.imageId &&
    snapshot.inspection.hostPort === runtime.port;
}

export async function probeRuntime(
  scope: HealthScope, runtime: HealthRuntime, inspect: HealthInspector,
  path = defaultHealthPath, signal?: AbortSignal,
  onDockerError?: (phase: "initial_inspection" | "revalidation") => void,
): Promise<RuntimeHealthObservation | null> {
  if (!isHealthBinding(runtime.host, runtime.port) || !isHealthBinding(runtime.host, runtime.containerPort) ||
    !/^[a-f0-9]{64}$/.test(runtime.containerId) || !/^sha256:[a-f0-9]{64}$/.test(runtime.imageId) ||
    !isHealthPath(path)) {
    throw new RangeError("Invalid managed runtime health target");
  }
  if (signal?.aborted) return null;
  const result: RuntimeHealthObservation = {
    scope, docker: null, dockerResult: "FAILED", httpResult: "SKIPPED",
    httpStatus: null, responseLatencyMs: null, failureReason: "CONTAINER_UNAVAILABLE",
  };

  let snapshot: Awaited<ReturnType<HealthInspector>>;
  try {
    snapshot = await inspectHealthRuntime(scope, runtime, inspect, signal);
  } catch {
    if (!signal?.aborted) onDockerError?.("initial_inspection");
    return signal?.aborted ? null : result;
  }
  if (signal?.aborted) return null;
  if (!snapshot.inspection.exists) return result;
  if (!snapshot.ownershipVerified || snapshot.inspection.containerId !== runtime.containerId ||
    snapshot.inspection.imageId !== runtime.imageId) {
    return { ...result, failureReason: "CONTAINER_IDENTITY_MISMATCH" };
  }
  result.docker = snapshot.inspection;
  if (!snapshot.inspection.running || snapshot.inspection.state !== "running") {
    return { ...result, failureReason: "CONTAINER_NOT_RUNNING" };
  }
  if (snapshot.inspection.hostPort !== runtime.port) {
    return { ...result, failureReason: "CONTAINER_IDENTITY_MISMATCH" };
  }

  const http = await probeHttpHealth(runtime.host, runtime.port, path, signal);
  if (!http) return null;
  try {
    const after = await inspectHealthRuntime(scope, runtime, inspect, signal);
    if (signal?.aborted || !matchesHealthRuntime(after, runtime) ||
      after.inspection.startedAt !== snapshot.inspection.startedAt ||
      after.inspection.restartCount !== snapshot.inspection.restartCount) return null;
  } catch {
    if (!signal?.aborted) onDockerError?.("revalidation");
    return null;
  }
  return {
    ...result, dockerResult: "PASSED", httpResult: http.failureReason ? "FAILED" : "PASSED",
    httpStatus: http.status, responseLatencyMs: http.latencyMs, failureReason: http.failureReason,
  };
}
