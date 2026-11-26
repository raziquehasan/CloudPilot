"use client";

import { useCallback, useEffect, useState } from "react";
import { Rocket, RefreshCw } from "lucide-react";
import { DashboardPageShell } from "@/components/dashboard/page-shell";
import { apiFetch } from "@/lib/api";
import { useDashboard } from "@/components/dashboard/dashboard-provider";

type BuildStatus = "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";

type DeploymentBuild = {
  id: string;
  buildAttempt: number;
  status: BuildStatus;
  dockerfilePath: string | null;
  imageTag: string | null;
  imageSizeBytes: string | null;
  failureCode: string | null;
  failureMessage: string | null;
  durationMs: number | null;
} | null;

type Deployment = {
  id: string;
  status: string;
  environment: string;
  triggerSource: string;
  commitSha: string | null;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  failureCode: string | null;
  build: DeploymentBuild;
  project: { id: string; name: string };
};

function formatDuration(ms: number | null): string {
  if (ms === null || ms < 0) return "—";
  if (ms < 1000) return `${ms} ms`;
  return `${Math.round(ms / 1000)} sec`;
}

function buildStatusLabel(status: string): string {
  return status.toLowerCase().replaceAll("_", " ");
}

export default function DeploymentsPage() {
  const { activeWorkspace } = useDashboard();
  const [items, setItems] = useState<Deployment[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setItems(await apiFetch<Deployment[]>("/deployments"));
      setError("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not load deployments.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => void load(), 0);
    return () => window.clearTimeout(timer);
  }, [load, activeWorkspace?.id]);

  return (
    <DashboardPageShell
      breadcrumb="Deployments"
      title="Deployments"
      description={`Actual deployment records for ${activeWorkspace?.name ?? "your active workspace"}.`}
    >
      {error && (
        <div
          role="alert"
          className="mb-4 flex justify-between rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-800"
        >
          {error}
          <button aria-label="Retry" onClick={() => void load()}>
            <RefreshCw className="size-4" />
          </button>
        </div>
      )}

      {loading ? (
        <div className="h-48 animate-pulse rounded-xl border bg-zinc-50 dark:bg-zinc-900" />
      ) : items.length ? (
        <div className="overflow-x-auto rounded-xl border bg-white dark:bg-zinc-950">
          <table className="w-full min-w-[760px] text-left text-xs">
            <thead className="border-b text-zinc-500">
              <tr>
                {["Project", "Status", "Build", "Commit", "Image", "Duration"].map((header) => (
                  <th key={header} className="px-4 py-3 font-medium">
                    {header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y">
              {items.map((item) => (
                <tr key={item.id}>
                  <td className="px-4 py-3 font-medium">{item.project.name}</td>
                  <td className="px-4 py-3">
                    <span className="rounded-full bg-zinc-100 px-2 py-1 dark:bg-zinc-800">
                      {buildStatusLabel(item.status)}
                    </span>
                    {item.failureCode && (
                      <span className="ml-2 text-red-600">
                        {item.failureCode.toLowerCase()}
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    {item.build ? (
                      <span className="rounded-full bg-zinc-100 px-2 py-1 dark:bg-zinc-800">
                        {buildStatusLabel(item.build.status)}
                      </span>
                    ) : (
                      <span className="text-zinc-400">—</span>
                    )}
                  </td>
                  <td className="px-4 py-3 font-mono">
                    {item.commitSha ? item.commitSha.slice(0, 7) : "—"}
                  </td>
                  <td className="px-4 py-3">
                    {item.build?.imageTag ? (
                      <span title={item.build.imageTag}>
                        {item.build.imageSizeBytes
                          ? `${(Number(item.build.imageSizeBytes) / (1024 * 1024)).toFixed(1)} MiB`
                          : "built"}
                      </span>
                    ) : item.build?.failureMessage ? (
                      <span className="text-red-600">{item.build.failureMessage}</span>
                    ) : (
                      <span className="text-zinc-400">—</span>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    {item.build?.durationMs != null
                      ? formatDuration(item.build.durationMs)
                      : item.startedAt && item.finishedAt
                        ? formatDuration(
                            new Date(item.finishedAt).getTime() - new Date(item.startedAt).getTime(),
                          )
                        : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="rounded-xl border border-dashed py-14 text-center">
          <Rocket className="mx-auto size-6 text-zinc-400" />
          <p className="mt-3 text-sm font-medium">No deployment records</p>
          <p className="mt-1 text-xs text-zinc-500">
            This workspace has no deployments. Execution is not available yet.
          </p>
        </div>
      )}

      {items.some((item) => item.status === "BUILD_SUCCEEDED") && (
        <p className="mt-4 rounded-lg border border-dashed p-3 text-xs text-zinc-500">
          Image built successfully. Application deployment will be available in Phase 5.5.
        </p>
      )}

      <p className="mt-4 rounded-lg border border-dashed p-3 text-xs text-zinc-500">
        CloudPilot builds a verified image artifact only. It does not start or host the application container.
      </p>
    </DashboardPageShell>
  );
}