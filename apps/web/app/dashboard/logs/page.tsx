import DeploymentLogsClient from "./deployment-logs-client";

// The dashboard renders this segment after its client-side session check.
export const instant = false;

export default function LogsPage() {
  return <DeploymentLogsClient />;
}
