export const deploymentStatuses = [
  "QUEUED",
  "PENDING",
  "PREPARING",
  "SOURCE_PREPARED",
  "CLONING",
  "BUILDING",
  "BUILD_SUCCEEDED",
  "DEPLOYING",
  "HEALTH_CHECKING",
  "RUNNING",
  "SUCCEEDED",
  "FAILED",
  "CANCELLING",
  "CANCELLED",
  "STOPPED",
  "ROLLED_BACK",
] as const;

export type DeploymentLifecycleStatus = (typeof deploymentStatuses)[number];

const transitions: Record<DeploymentLifecycleStatus, readonly DeploymentLifecycleStatus[]> = {
  QUEUED: ["PREPARING", "CANCELLING", "CANCELLED", "FAILED"],
  PENDING: ["PREPARING", "CANCELLING", "CANCELLED", "FAILED"],
  PREPARING: ["SOURCE_PREPARED", "CLONING", "FAILED", "CANCELLING"],
  SOURCE_PREPARED: ["BUILDING", "FAILED", "CANCELLING"],
  CLONING: ["BUILDING", "FAILED", "CANCELLING"],
  BUILDING: ["BUILD_SUCCEEDED", "DEPLOYING", "FAILED", "CANCELLING"],
  BUILD_SUCCEEDED: ["DEPLOYING", "FAILED", "CANCELLING"],
  DEPLOYING: ["HEALTH_CHECKING", "FAILED", "CANCELLING"],
  HEALTH_CHECKING: ["RUNNING", "FAILED", "CANCELLING"],
  RUNNING: ["FAILED", "STOPPED"],
  SUCCEEDED: [],
  FAILED: [],
  CANCELLING: ["CANCELLED", "FAILED"],
  CANCELLED: [],
  STOPPED: [],
  ROLLED_BACK: [],
};

const terminalStatuses = new Set<DeploymentLifecycleStatus>([
  "SUCCEEDED",
  "FAILED",
  "CANCELLED",
  "STOPPED",
  "ROLLED_BACK",
]);

export function canTransitionDeployment(
  from: DeploymentLifecycleStatus,
  to: DeploymentLifecycleStatus,
): boolean {
  return transitions[from].includes(to);
}

export function isTerminalDeploymentStatus(status: DeploymentLifecycleStatus): boolean {
  return terminalStatuses.has(status);
}
