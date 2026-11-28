"use client";

// ==================================================
// CloudPilot — Deployment Logs Page
// Phase 5.6 — Realtime Deployment Logs
// ==================================================

import { Suspense } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Activity, Loader2, Terminal } from "lucide-react";
import { useQuery } from "@tanstack/react-query";

import { useDashboard } from "@/components/dashboard/dashboard-provider";
import { DashboardPageShell } from "@/components/dashboard/page-shell";
import DeploymentHealthPanel from "@/components/dashboard/deployment-health";
import { ApiError, apiFetch } from "@/lib/api";

import LogTerminal from "./log-terminal";

// ==================================================
// Constants
// ==================================================

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// ==================================================
// Query parameter validation
// ==================================================

function isValidUuid(
  value: string | null,
): value is string {
  return (
    typeof value === "string" &&
    value.length === 36 &&
    UUID_PATTERN.test(value)
  );
}

function readUniqueUuid(
  values: readonly string[],
): string | null {
  // Reject missing or ambiguous query parameters.
  if (values.length !== 1) {
    return null;
  }

  const value = values[0] ?? null;

  if (!isValidUuid(value)) {
    return null;
  }

  return value.toLowerCase();
}

// ==================================================
// Loading fallback
// ==================================================

function LogsPageLoading() {
  return (
    <DashboardPageShell
      breadcrumb="Logs"
      title="Deployment Logs"
      description="View and monitor deployment logs."
    >
      <div
        role="status"
        aria-live="polite"
        aria-busy="true"
        className="flex min-h-[280px] min-w-0 flex-col items-center justify-center gap-3 rounded-xl border bg-white p-6 dark:bg-zinc-950"
      >
        <Loader2
          aria-hidden="true"
          className="size-6 animate-spin text-zinc-500 motion-reduce:animate-none dark:text-zinc-400"
        />

        <p className="text-center text-sm font-medium">
          Loading deployment logs...
        </p>

        <p className="max-w-sm text-center text-xs leading-relaxed text-zinc-500 dark:text-zinc-400">
          Preparing the deployment log terminal.
        </p>
      </div>
    </DashboardPageShell>
  );
}

function DeploymentStatus({ projectId, deploymentId }: { projectId: string; deploymentId: string }) {
  const { activeWorkspace } = useDashboard();
  const status = useQuery({
    queryKey: ["deployment-status", activeWorkspace?.id, projectId, deploymentId],
    queryFn: ({ signal }) => apiFetch<{ status: string; failureMessage: string | null }>(
      `/projects/${projectId}/deployments/${deploymentId}`,
      { signal },
    ),
    enabled: Boolean(activeWorkspace),
    retry: false,
    refetchInterval: ({ state }) => {
      if (state.error instanceof ApiError && [401, 403, 404].includes(state.error.status)) return false;
      if (state.data?.status === "RUNNING" || state.error) return 30_000;
      return ["SUCCEEDED", "FAILED", "CANCELLED", "STOPPED", "ROLLED_BACK"].includes(state.data?.status ?? "")
        ? false
        : 12_000;
    },
  });

  return (
    <div role="status" className="mb-3 rounded-lg border bg-card px-4 py-3 text-sm">
      {status.isError ? "Could not load deployment status." : status.data ? (
        <>
          <span className="font-medium">Deployment status</span>{" "}
          <span className="capitalize">{status.data.status.toLowerCase().replaceAll("_", " ")}</span>
          {status.data.failureMessage && <p className="mt-1 text-destructive">{status.data.failureMessage}</p>}
        </>
      ) : "Loading deployment status…"}
    </div>
  );
}

// ==================================================
// URL-aware deployment log terminal
// ==================================================

function LogsPageContent() {
  const searchParams = useSearchParams();
  const { activeWorkspace } = useDashboard();

  const deploymentId = readUniqueUuid(
    searchParams?.getAll("deployment") ?? [],
  );

  const projectId = readUniqueUuid(
    searchParams?.getAll("project") ?? [],
  );

  // Both identifiers must be valid before
  // a deployment log session can be opened.

  const hasValidSelection =
    deploymentId !== null &&
    projectId !== null;

  if (!hasValidSelection) {
    return (
      <LogTerminal
        key="no-deployment-selected"
        deploymentId={null}
        projectId={null}
      />
    );
  }

  const healthSelected = searchParams?.get("tab") === "health";
  const base = new URLSearchParams({ project: projectId, deployment: deploymentId });
  const health = new URLSearchParams(base);
  health.set("tab", "health");
  const detailsNavigation = <nav aria-label="Deployment details" className="mb-4 flex w-fit max-w-full items-center gap-1 rounded-lg border bg-muted/30 p-1">
    <Link prefetch={false} href={`/dashboard/logs?${base.toString()}`} aria-current={!healthSelected ? "page" : undefined}
      className="inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground aria-[current=page]:bg-background aria-[current=page]:font-medium aria-[current=page]:text-foreground aria-[current=page]:shadow-xs">
      <Terminal className="size-4" aria-hidden="true" /> Logs
    </Link>
    <Link prefetch={false} href={`/dashboard/logs?${health.toString()}`} aria-current={healthSelected ? "page" : undefined}
      className="inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground aria-[current=page]:bg-background aria-[current=page]:font-medium aria-[current=page]:text-foreground aria-[current=page]:shadow-xs">
      <Activity className="size-4" aria-hidden="true" /> Health
    </Link>
  </nav>;

  if (healthSelected) {
    return <DashboardPageShell breadcrumb="Deployment Health">
      {detailsNavigation}
      <h1 className="mb-4 text-xl font-semibold tracking-tight">Deployment health</h1>
      {activeWorkspace ? <DeploymentHealthPanel
        key={`${activeWorkspace.id}:${projectId}:${deploymentId}`}
        workspaceId={activeWorkspace.id}
        projectId={projectId}
        deploymentId={deploymentId}
        canManage={activeWorkspace.role === "OWNER" || activeWorkspace.role === "ADMIN"}
      /> : <p className="text-sm text-muted-foreground">Select a workspace to view deployment health.</p>}
    </DashboardPageShell>;
  }

  // Remount the terminal when the selected
  // deployment or project changes.
  //
  // This prevents previous terminal state
  // from being reused for another deployment.
  //
  // LogTerminal remains responsible for
  // workspace scoping and connection cleanup.
  //
  // Backend authorization remains mandatory.

  return (
    <>
      <div className="mx-4 mt-4 sm:mx-6 lg:mx-8">{detailsNavigation}</div>
      <DeploymentStatus projectId={projectId} deploymentId={deploymentId} />
      <LogTerminal
        key={`${projectId}:${deploymentId}`}
        deploymentId={deploymentId}
        projectId={projectId}
      />
    </>
  );
}

// ==================================================
// Main logs page
// ==================================================

export default function DeploymentLogsClient() {
  return (
    <Suspense fallback={<LogsPageLoading />}>
      <LogsPageContent />
    </Suspense>
  );
}
