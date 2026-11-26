"use client";

import { EnvironmentVariables } from "./project-environment-variables";
import { useEffect, useRef, useState, type ComponentType, type ReactNode } from "react";
import {
  Activity, AlertCircle, ArrowDownRight, ArrowUpRight, Check, Circle, Clock3,
  Copy, ExternalLink, File, Folder, GitBranch, GitCommitHorizontal,
  Radio, RefreshCw, Settings2, ShieldCheck, Sparkles,
  TerminalSquare, Webhook, Workflow,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { GitHubIcon } from "@/components/shared/github-icon";
import { apiFetch } from "@/lib/api";
import type { Project, ProjectDeployment } from "./project-types";
import { useDashboard } from "@/components/dashboard/dashboard-provider";
import { useBuildConfiguration, useBranches, useChangeProjectBranch, useCommitDetail, useCommits, useLatestCommit, useRefreshRepository, useRepository, useRepositoryDetection, useRepositoryFiles, useRepositoryLanguages, useResetBuildConfiguration, useSaveBuildConfiguration, type BuildConfiguration, type Commit, type RepositoryDetection } from "@/lib/repository-intelligence";

export const projectSections = [
  { id: "overview", label: "Overview", icon: Activity },
  { id: "deployments", label: "Deployments", icon: RocketIcon },
  { id: "build-configuration", label: "Build Configuration", icon: Settings2 },
  { id: "source", label: "Source", icon: Folder },
  { id: "commits", label: "Commits", icon: GitCommitHorizontal },
  { id: "environment-variables", label: "Environment Variables", icon: ShieldCheck },
  { id: "logs", label: "Logs", icon: TerminalSquare },
  { id: "monitoring", label: "Monitoring", icon: Radio },
  { id: "git-integration", label: "Git Integration", icon: GitBranch },
  { id: "settings", label: "Settings", icon: Settings2 },
] as const;

function RocketIcon(props: React.ComponentProps<typeof Activity>) {
  return <ArrowUpRight {...props} />;
}

function StatusBadge({ status }: { status: string }) {
  const success = ["SUCCEEDED", "SUCCESS", "COMPLETED"].includes(status.toUpperCase());
  const failed = ["FAILED", "ERROR"].includes(status.toUpperCase());
  return <Badge variant="outline" className={success ? "border-emerald-600/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300" : failed ? "border-red-600/30 bg-red-500/10 text-red-700 dark:text-red-300" : "capitalize"}>{status.toLowerCase().replaceAll("_", " ")}</Badge>;
}

function EmptyPanel({ icon: Icon, title, description, action }: { icon: ComponentType<{ className?: string }>; title: string; description: string; action?: ReactNode }) {
  return <div className="flex min-h-48 flex-col items-center justify-center px-5 py-10 text-center"><span className="grid size-10 place-items-center rounded-xl border bg-muted/50"><Icon className="size-5 text-muted-foreground" /></span><h3 className="mt-4 text-sm font-semibold">{title}</h3><p className="mt-1 max-w-md text-sm leading-6 text-muted-foreground">{description}</p>{action && <div className="mt-4">{action}</div>}</div>;
}

function CommitAvatar({ commit }: { commit: Commit }) {
  if (commit.authorAvatar?.startsWith("https://")) {
    // GitHub supplies the author's avatar, including for private repositories.
    // eslint-disable-next-line @next/next/no-img-element
    return <img src={commit.authorAvatar} alt="" referrerPolicy="no-referrer" loading="lazy" className="size-8 shrink-0 rounded-full border bg-muted" />;
  }
  return <span className="grid size-8 shrink-0 place-items-center rounded-full border bg-muted"><GitCommitHorizontal className="size-4 text-muted-foreground" /></span>;
}

function Timestamp({ value, relative = false }: { value: string; relative?: boolean }) {
  const date = new Date(value);
  const exact = date.toLocaleString();
  const [now] = useState(() => Date.now());
  const seconds = Math.round((date.getTime() - now) / 1000);
  const [unit, divisor] = Math.abs(seconds) >= 86400 ? ["day", 86400] as const : Math.abs(seconds) >= 3600 ? ["hour", 3600] as const : ["minute", 60] as const;
  const label = relative
    ? new Intl.RelativeTimeFormat(undefined, { numeric: "auto" }).format(Math.round(seconds / divisor), unit)
    : exact;
  return <time dateTime={value} title={exact}>{label}</time>;
}

interface ProjectDeploymentReadiness {
  deploymentQueue: { available: boolean };
  workers: { deployment: { connected: boolean; available: boolean } };
  repository: { connected: boolean };
  commitResolution: { available: boolean };
  sourceRetrieval: { available: boolean; workerCapable: boolean; workspaceStorage: boolean };
  workspaceStorage: { available: boolean };
  buildConfiguration: { configured: boolean };
  execution: { capabilitiesAvailable: boolean; available: boolean };
}

function Readiness({ project, workspaceId, branch, branchStatus, framework, saved, configPending }: {
  workspaceId?: string;
  project: Project; branch: string; branchStatus: string;
  framework?: string | null; saved?: boolean; configPending: boolean;
}) {
  const [readiness, setReadiness] = useState<ProjectDeploymentReadiness | null>(null);
  const [readinessLoading, setReadinessLoading] = useState(true);
  useEffect(() => {
    let active = true;
    void apiFetch<ProjectDeploymentReadiness>(`/projects/${encodeURIComponent(project.id)}/deployment-readiness`)
      .then((result) => { if (active) setReadiness(result); })
      .catch(() => { if (active) setReadiness(null); })
      .finally(() => { if (active) setReadinessLoading(false); });
    return () => { active = false; };
  }, [project.id, workspaceId]);

  const rows = [
    { label: "Repository connection", value: readinessLoading ? "Checking connection" : readiness?.repository.connected ? "Connected" : "Not connected", ready: readiness?.repository.connected ?? false },
    { label: "Selected branch", value: branchStatus === "success" ? branch : branchStatus === "error" ? "Repository access failed" : branchStatus === "missing" ? "No commit available on this branch" : branch ? "Checking access" : "Select a branch", ready: branchStatus === "success" },
    { label: "Framework", value: framework ?? "Detect or configure a framework", ready: Boolean(framework) },
    { label: "Build configuration", value: readinessLoading ? (configPending ? "Checking configuration" : "Checking readiness") : readiness?.buildConfiguration.configured ? "Configured" : saved ? "Saved but incomplete" : "Missing or incomplete", ready: readiness?.buildConfiguration.configured ?? false },
    { label: "Environment variables", value: project._count.environmentVariables ? `${project._count.environmentVariables} configured · review application requirements` : "Configure only if your application requires them", ready: null },
    { label: "Deployment queue", value: readinessLoading ? "Checking availability" : readiness?.deploymentQueue.available ? "Available" : "Unavailable", ready: readiness?.deploymentQueue.available ?? false },
    { label: "Deployment worker", value: readinessLoading ? "Checking connection" : readiness?.workers.deployment.connected ? readiness.workers.deployment.available ? "Connected and available" : "Connected but unavailable" : "Disconnected", ready: readiness?.workers.deployment.available ?? false },
    { label: "Immutable commit resolution", value: readinessLoading ? "Checking capability" : readiness?.commitResolution.available ? "Available" : "Unavailable", ready: readiness?.commitResolution.available ?? false },
    { label: "Source retrieval", value: readinessLoading ? "Checking capability" : readiness?.sourceRetrieval.available ? "Available" : readiness?.sourceRetrieval.workerCapable ? "Worker ready, storage unavailable" : "Unavailable", ready: readiness?.sourceRetrieval.available ?? false },
    { label: "Workspace storage", value: readinessLoading ? "Checking storage" : readiness?.workspaceStorage.available ? "Available" : "Unavailable", ready: readiness?.workspaceStorage.available ?? false },
    { label: "Deployment engine", value: readinessLoading ? "Checking capabilities" : readiness?.execution.capabilitiesAvailable ? "Configured" : "Not configured", ready: readiness?.execution.capabilitiesAvailable ?? false },
    { label: "Execution capability", value: readinessLoading ? "Checking availability" : readiness?.execution.available ? "Available" : "Unavailable (build/runtime disabled)", ready: readiness?.execution.available ?? false },
  ];
  return <Card size="sm">
    <CardHeader className="border-b"><CardTitle className="text-sm">Deployment readiness</CardTitle></CardHeader>
    <CardContent><ul className="divide-y">
      {rows.map(({ label, value, ready }) => <li key={label} className="flex min-w-0 items-start gap-2 py-2.5 first:pt-0 last:pb-0">
        {ready ? <Check aria-hidden="true" className="mt-0.5 size-4 shrink-0 text-emerald-600 dark:text-emerald-400" /> : <Circle aria-hidden="true" className="mt-0.5 size-4 shrink-0 text-muted-foreground" />}
        <div className="min-w-0 [overflow-wrap:anywhere]"><p className="text-sm font-medium">{label}<span className="sr-only">, {ready === null ? "review needed" : ready ? "ready" : "pending"}</span></p><p className="text-xs leading-5 text-muted-foreground">{value}</p></div>
      </li>)}
    </ul></CardContent>
  </Card>;
}

function DeploymentRows({ rows }: { rows: ProjectDeployment[] }) {
  if (!rows.length) return <EmptyPanel icon={Workflow} title="No deployment activity yet" description="Deployment runs will appear here once a deployment engine is available. Your project and repository are ready to configure." />;
  return <div className="divide-y">{rows.map((deployment) => <div key={deployment.id} className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 py-3 first:pt-0 last:pb-0">
    <div className="flex min-w-0 items-center gap-3"><span className="grid size-8 shrink-0 place-items-center rounded-lg border bg-muted/40"><GitCommitHorizontal className="size-4 text-muted-foreground" /></span><div className="min-w-0"><p className="truncate font-mono text-xs">{deployment.id.slice(0, 8)}</p><p className="mt-1 text-xs text-muted-foreground">{deployment.environment.toLowerCase()} · {deployment.triggerSource.toLowerCase().replaceAll("_", " ")}{deployment.commitSha ? ` · ${deployment.commitSha.slice(0, 7)}` : ""}{deployment.status === "SOURCE_PREPARED" ? " · source ready" : ""}</p></div></div>
    <div className="flex items-center gap-3"><StatusBadge status={deployment.status} /><time className="whitespace-nowrap text-xs text-muted-foreground" dateTime={deployment.createdAt}>{new Date(deployment.createdAt).toLocaleString()}</time></div>
  </div>)}</div>;
}

function Overview({ project }: { project: Project }) {
  const { activeWorkspace } = useDashboard();
  const workspaceId = activeWorkspace?.id;
  const repository = useRepository(project.id, project.repository ? workspaceId : undefined);
  const languages = useRepositoryLanguages(project.id, project.repository ? workspaceId : undefined);
  const branch = repository.data?.selectedBranch ?? project.repository?.selectedBranch ?? repository.data?.defaultBranch ?? project.repository?.defaultBranch ?? "";
  const latest = useLatestCommit(project.id, branch, workspaceId);
  const detection = useRepositoryDetection(project.id, branch, workspaceId);
  const refresh = useRefreshRepository(project.id, workspaceId);
  const build = useBuildConfiguration(project.id, workspaceId);
  const commit = latest.data;
  const canRefresh = activeWorkspace?.role === "OWNER" || activeWorkspace?.role === "ADMIN";

  return <div className="space-y-4">
    {project.repository && <Card size="sm">
      <CardHeader className="border-b">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="min-w-0">
            <CardTitle className="text-sm">Latest commit</CardTitle>
            {branch && <CardDescription className="break-all font-mono">{branch}</CardDescription>}
          </div>
          {canRefresh && <Button size="sm" variant="outline" disabled={refresh.isPending} onClick={() => refresh.mutate()}><RefreshCw className={refresh.isPending ? "animate-spin" : ""} />{refresh.isPending ? "Refreshing…" : "Refresh repository"}</Button>}
        </div>
      </CardHeader>
      <CardContent className="space-y-2">
        {refresh.isError && <p role="alert" className="text-sm text-destructive">{refresh.error.message}</p>}
        {refresh.isSuccess && <p role="status" className="text-xs text-muted-foreground">Repository refreshed.</p>}
        {commit ? <div className="flex min-w-0 items-start gap-3">
          <CommitAvatar commit={commit} />
          <div className="min-w-0 flex-1 [overflow-wrap:anywhere]">
            <a href={commit.url} target="_blank" rel="noreferrer" className="text-sm font-semibold hover:underline">{commit.message.split("\n")[0]} <ExternalLink className="inline size-3" aria-label="Open on GitHub" /></a>
            <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
              <span>{commit.authorName ?? commit.githubUsername ?? "Unknown author"}</span>
              <code>{commit.shortSha}</code>
              {commit.timestamp && <Timestamp value={commit.timestamp} />}
            </div>
          </div>
        </div> : <p role="status" className="text-sm text-muted-foreground">{!branch ? "No branch selected." : latest.isPending ? "Loading latest commit…" : latest.isError ? latest.error.message : "No commits on this branch."}</p>}
      </CardContent>
    </Card>}
    <div className="grid min-w-0 items-start gap-4 xl:grid-cols-2">
      {project.repository && <Card size="sm">
        <CardHeader className="border-b"><CardTitle className="text-sm">Repository profile</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          <dl className="grid gap-2 text-sm sm:grid-cols-[auto_minmax(0,1fr)]">
            <dt className="text-muted-foreground">Framework</dt><dd className="break-words">{detection.data?.primaryFramework ?? (detection.isFetching ? "Detecting…" : detection.isError ? "Detection unavailable" : "Undetected")}</dd>
            <dt className="text-muted-foreground">Runtime</dt><dd className="break-words">{detection.data?.runtime ?? (detection.isFetching ? "Detecting…" : detection.isError ? "Detection unavailable" : "Undetected")}</dd>
            <dt className="text-muted-foreground">Last push</dt><dd>{repository.data?.lastPushedAt ? <Timestamp value={repository.data.lastPushedAt} /> : repository.isPending ? "Loading…" : "Unavailable"}</dd>
          </dl>
          <div className="space-y-2 border-t pt-3">
            <p className="text-xs font-medium text-muted-foreground">Repository languages</p>
            {languages.data?.languages.length ? languages.data.languages.map((item) => <div key={item.name} className="grid grid-cols-[minmax(0,1fr)_minmax(0,2fr)_3.5rem] items-center gap-2 text-xs">
              <span className="break-words">{item.name}</span><span className="h-1.5 overflow-hidden rounded-full bg-muted" aria-hidden="true"><span className="block h-full rounded-full bg-blue-600" style={{ width: `${item.percentage}%` }} /></span><span className="text-right tabular-nums text-muted-foreground">{item.percentage}%</span>
            </div>) : <p className="text-xs text-muted-foreground">{languages.isPending ? "Loading statistics…" : languages.isError ? "Language statistics unavailable." : "GitHub returned no language statistics."}</p>}
          </div>
        </CardContent>
      </Card>}
      <Readiness project={project} workspaceId={workspaceId} branch={branch} branchStatus={latest.isSuccess && !latest.data ? "missing" : latest.status} framework={build.data?.configuration?.framework ?? detection.data?.primaryFramework} saved={build.data?.saved} configPending={build.isPending} />
    </div>
    <Card size="sm">
      <CardHeader className="border-b"><CardTitle className="text-sm">Deployment activity</CardTitle></CardHeader>
      <CardContent>{project.deployments.length ? <DeploymentRows rows={project.deployments.slice(0, 5)} /> : <p className="text-sm text-muted-foreground">No deployments recorded.</p>}</CardContent>
    </Card>
  </div>;
}

function Deployments({ project }: { project: Project }) {
  return <Card><CardHeader className="border-b"><div className="flex flex-wrap items-start justify-between gap-3"><div><CardTitle>Deployment history</CardTitle><CardDescription>Only deployments recorded for this project are shown.</CardDescription></div><Badge variant="outline">{project._count.deployments} total</Badge></div></CardHeader><CardContent className="pt-4"><DeploymentRows rows={project.deployments} /></CardContent></Card>;
}

function BuildConfiguration({ project }: { project: Project }) {
  const { activeWorkspace } = useDashboard();
  const saved = useBuildConfiguration(project.id, activeWorkspace?.id);
  const repository = useRepository(project.repository ? project.id : "", activeWorkspace?.id);
  const detect = useRepositoryDetection(project.id, repository.data?.selectedBranch ?? project.repository?.selectedBranch ?? project.repository?.defaultBranch ?? "", activeWorkspace?.id);
  if (saved.isLoading) return <div className="h-72 animate-pulse rounded-xl border bg-muted/30" aria-label="Loading saved build configuration" />;
  if (saved.isError) return <Card><CardContent className="space-y-3 py-8 text-center"><p role="alert" className="text-sm text-destructive">Could not load saved configuration. {saved.error.message}</p><Button variant="outline" onClick={() => void saved.refetch()}>Retry</Button></CardContent></Card>;
  if (!saved.data) return <div className="h-72 animate-pulse rounded-xl border bg-muted/30" aria-label="Loading saved build configuration" />;
  return <BuildConfigurationEditor key={`${project.id}:${saved.data.configuration?.updatedAt ?? "new"}`} project={project} workspaceId={activeWorkspace?.id} saved={saved.data.saved} configuration={saved.data.configuration} suggestion={detect.data?.suggestions} detectedFramework={detect.data?.primaryFramework ?? null} detectedRuntime={detect.data?.runtime ?? null} detectedManager={detect.data?.packageManager ?? null} monorepo={detect.data?.monorepo ?? false} detectionError={detect.isError} />;
}

function BuildConfigurationEditor({ project, workspaceId, saved, configuration, suggestion, detectedFramework, detectedRuntime, detectedManager, monorepo, detectionError }: { project: Project; workspaceId?: string; saved: boolean; configuration: BuildConfiguration | null; suggestion?: RepositoryDetection["suggestions"]; detectedFramework: string | null; detectedRuntime: string | null; detectedManager: string | null; monorepo: boolean; detectionError: boolean }) {
  const save = useSaveBuildConfiguration(project.id, workspaceId);
  const reset = useResetBuildConfiguration(project.id, workspaceId);
  const values = Object.fromEntries(Object.keys(emptyBuildConfig).map((key) => [key, configuration?.[key as keyof BuildConfiguration] ?? null])) as BuildConfiguration;
  const [form, setForm] = useState<BuildConfiguration>(values);
  const [confirmReset, setConfirmReset] = useState(false);
  const dirty = Object.keys(values).some((key) => values[key as keyof BuildConfiguration] !== form[key as keyof BuildConfiguration]);
  const canManage = ["OWNER", "ADMIN"].includes(useDashboard().activeWorkspace?.role ?? "");
  const busy = save.isPending || reset.isPending;
  useEffect(() => {
    if (!dirty) return;
    const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); };
    const warnOnNavigation = (event: MouseEvent) => {
      const link = event.target instanceof Element ? event.target.closest("a[href]") : null;
      if (link && !link.hasAttribute("target") && !event.ctrlKey && !event.metaKey && !event.shiftKey && !window.confirm("Leave without saving build configuration?")) {
        event.preventDefault();
        event.stopPropagation();
      }
    };
    window.addEventListener("beforeunload", warn);
    document.addEventListener("click", warnOnNavigation, true);
    return () => {
      window.removeEventListener("beforeunload", warn);
      document.removeEventListener("click", warnOnNavigation, true);
    };
  }, [dirty]);
  const applySuggestions = () => setForm((current) => ({
    ...current,
    ...Object.fromEntries(Object.entries({ ...suggestion, framework: detectedFramework, runtime: detectedRuntime, packageManager: detectedManager }).filter(([, value]) => value != null)),
  }));
  const update = (field: keyof BuildConfiguration, value: string | number | null) => setForm((current) => ({ ...current, [field]: value }));
  const textField = (field: keyof BuildConfiguration, label: string, hint?: string) => <label className="block min-w-0 space-y-1.5 text-xs font-medium" key={field}>{label}{hint && <span className="ml-2 font-normal text-muted-foreground">{hint}</span>}<input value={String(form[field] ?? "")} onChange={(event) => update(field, event.target.value || null)} className="h-10 w-full min-w-0 rounded-md border bg-background px-3 text-sm font-normal" /></label>;
  return <div className="space-y-5">
    <div className="rounded-lg border border-blue-600/20 bg-blue-500/5 p-4"><div className="flex gap-3"><Sparkles className="mt-0.5 size-4 shrink-0 text-blue-600" /><div className="min-w-0"><p className="text-sm font-medium">Suggestions are read-only analysis</p><p className="mt-1 text-sm leading-6 text-muted-foreground">Detected settings are based on repository files. Review and save values explicitly. Saving stores configuration only; it does not execute commands or deploy.</p>{suggestion && <div className="mt-3 flex flex-wrap gap-2 text-xs">{detectedFramework && <Badge variant="secondary">Detected: {detectedFramework}</Badge>}{detectedRuntime && <Badge variant="outline">Runtime: {detectedRuntime}</Badge>}{detectedManager && <Badge variant="outline">{detectedManager}</Badge>}{monorepo && <Badge variant="outline">Monorepo</Badge>}</div>}</div></div></div>
    {suggestion && <div className="flex flex-wrap items-center gap-3"><Button type="button" variant="outline" disabled={!canManage || busy || ![...Object.values(suggestion), detectedFramework, detectedRuntime, detectedManager].some(Boolean)} onClick={applySuggestions}>Apply Detected Configuration</Button>{monorepo && !suggestion.rootDirectory && <p className="text-sm text-muted-foreground">Enter the application directory and its commands for this monorepo.</p>}</div>}
    {detectionError && <p role="status" className="text-sm text-muted-foreground">Repository suggestions are temporarily unavailable.</p>}
    {reset.isError && <p role="alert" className="text-sm text-destructive">{reset.error.message}</p>}
    {save.isError && <p role="alert" className="text-sm text-destructive">{save.error.message}</p>}
    <Card><CardHeader className="border-b"><div className="flex flex-wrap items-start justify-between gap-3"><div><CardTitle>Build settings</CardTitle><CardDescription>{saved ? `Saved ${configuration?.updatedAt ? new Date(configuration.updatedAt).toLocaleString() : "configuration"}` : "No saved configuration yet. Suggestions are never saved automatically."}</CardDescription></div><Badge variant={saved ? "secondary" : "outline"}>{dirty ? "Unsaved changes" : saved ? "Saved" : "Unsaved"}</Badge></div></CardHeader>
      <form onSubmit={(event) => { event.preventDefault(); if (dirty && canManage && !busy) save.mutate(form); }}><fieldset disabled={!canManage || busy}><CardContent className="grid gap-4 pt-4 sm:grid-cols-2">
        <label className="block space-y-1.5 text-xs font-medium">Framework<input maxLength={80} value={form.framework ?? ""} onChange={(event) => update("framework", event.target.value || null)} className="h-10 w-full rounded-md border bg-background px-3 text-sm font-normal" />{detectedFramework && <Suggestion onClick={() => update("framework", detectedFramework)} value={detectedFramework} />}</label>
        <label className="block space-y-1.5 text-xs font-medium">Runtime<input value={form.runtime ?? ""} onChange={(event) => update("runtime", event.target.value || null)} placeholder={detectedRuntime ?? "e.g. Node.js 22"} className="h-10 w-full rounded-md border bg-background px-3 text-sm font-normal" />{detectedRuntime && <Suggestion onClick={() => update("runtime", detectedRuntime)} value={detectedRuntime} />}</label>
        {textField("packageManager", "Package manager", detectedManager ?? undefined)}
        {textField("rootDirectory", "Root directory", suggestion?.rootDirectory ? `Detected: ${suggestion.rootDirectory}` : undefined)}
        {textField("installCommand", "Install command", suggestion?.installCommand ? `Suggested: ${suggestion.installCommand}` : undefined)}
        {textField("buildCommand", "Build command", suggestion?.buildCommand ? `Suggested: ${suggestion.buildCommand}` : undefined)}
        {textField("startCommand", "Start command", suggestion?.startCommand ? `Suggested: ${suggestion.startCommand}` : undefined)}
        {textField("dockerfilePath", "Dockerfile path", suggestion?.dockerfilePath ? `Detected: ${suggestion.dockerfilePath}` : undefined)}
        <label className="block space-y-1.5 text-xs font-medium">Application port<input type="number" min={1} max={65535} value={form.applicationPort ?? ""} onChange={(event) => update("applicationPort", event.target.value ? Number(event.target.value) : null)} placeholder="e.g. 3000" className="h-10 w-full rounded-md border bg-background px-3 text-sm font-normal" />{suggestion?.applicationPort && <Suggestion value={String(suggestion.applicationPort)} onClick={() => update("applicationPort", suggestion.applicationPort)} />}</label>
        <label className="block space-y-1.5 text-xs font-medium">Build timeout in seconds<input type="number" min={30} max={3600} value={form.buildTimeoutSeconds ?? ""} onChange={(event) => update("buildTimeoutSeconds", event.target.value ? Number(event.target.value) : null)} placeholder="600" className="h-10 w-full rounded-md border bg-background px-3 text-sm font-normal" /></label>
        <label className="block space-y-1.5 text-xs font-medium">Deployment environment<select value={form.environment ?? ""} onChange={(event) => update("environment", event.target.value || null)} className="h-10 w-full rounded-md border bg-background px-3 text-sm font-normal"><option value="">Not configured</option><option value="DEVELOPMENT">Development</option><option value="STAGING">Staging</option><option value="PRODUCTION">Production</option><option value="PREVIEW">Preview</option></select></label>
      </CardContent></fieldset><div className="flex flex-wrap justify-end gap-2 border-t px-4 py-3">{dirty && <Button type="button" variant="ghost" disabled={busy} onClick={() => setForm(values)}>Discard edits</Button>}<Button type="button" variant="outline" disabled={!canManage || !saved || busy} onClick={() => { reset.reset(); setConfirmReset(true); }}>Reset saved settings</Button><Button type="submit" disabled={!canManage || !dirty || busy}>{save.isPending ? "Saving…" : "Save configuration"}</Button></div></form></Card>
    <Dialog open={confirmReset} onOpenChange={(open) => { if (!reset.isPending) setConfirmReset(open); }}>
      <DialogContent>
        <DialogTitle>Reset build configuration?</DialogTitle>
        <DialogDescription>This removes the saved configuration and discards any current edits.</DialogDescription>
        {reset.isError && <p role="alert" className="text-sm text-destructive">{reset.error.message}</p>}
        <div className="flex flex-wrap justify-end gap-2">
          <Button variant="outline" disabled={reset.isPending} onClick={() => setConfirmReset(false)}>Cancel</Button>
          <Button variant="destructive" disabled={reset.isPending} onClick={() => reset.mutate(undefined, { onSuccess: () => setConfirmReset(false) })}>{reset.isPending ? "Resetting…" : "Reset configuration"}</Button>
        </div>
      </DialogContent>
    </Dialog>
  </div>;
}

const emptyBuildConfig: BuildConfiguration = { framework: null, runtime: null, packageManager: null, rootDirectory: null, installCommand: null, buildCommand: null, startCommand: null, dockerfilePath: null, applicationPort: null, buildTimeoutSeconds: null, environment: null };
function Suggestion({ value, onClick }: { value?: string | null; onClick: () => void }) { return value ? <button type="button" onClick={onClick} className="block max-w-full break-words text-left font-normal text-blue-700 hover:underline dark:text-blue-300">Use detected: {value}</button> : null; }

function RepositorySource({ project }: { project: Project }) {
  const { activeWorkspace } = useDashboard();
  const repository = useRepository(project.id, activeWorkspace?.id);
  const branchList = useBranches(project.id, activeWorkspace?.id);
  const currentBranch = repository.data?.selectedBranch ?? project.repository?.selectedBranch ?? repository.data?.defaultBranch ?? project.repository?.defaultBranch ?? "";
  const [path, setPath] = useState("");
  const [browseBranch, setBrowseBranch] = useState("");
  const branchValue = browseBranch || currentBranch;
  const files = useRepositoryFiles(project.id, branchValue, path, activeWorkspace?.id);
  const breadcrumbs = path.split("/").filter(Boolean).map((name, index, parts) => ({
    name,
    path: parts.slice(0, index + 1).join("/"),
  }));
  if (!project.repository) return <EmptyPanel icon={Folder} title="No GitHub source connected" description="Import a repository to browse its files and branches." />;
  return <div className="space-y-4">
    <Card><CardHeader className="border-b"><div className="flex flex-wrap items-center justify-between gap-3"><div className="min-w-0"><CardTitle className="flex items-center gap-2"><Folder className="size-4 text-blue-600" />Source browser</CardTitle><CardDescription className="mt-1 [overflow-wrap:anywhere]">{repository.data?.fullName ?? project.repository.fullName ?? project.repository.name}</CardDescription></div><label className="flex w-full min-w-0 items-center gap-2 text-xs text-muted-foreground sm:w-auto">Branch<select aria-label="Select repository branch" value={branchValue} onChange={(event) => { setBrowseBranch(event.target.value); setPath(""); }} disabled={branchList.isLoading} className="h-9 min-w-0 max-w-full sm:max-w-[220px] rounded-md border bg-background px-2 font-mono text-xs text-foreground">{branchValue && !branchList.data?.items.some((item) => item.name === branchValue) && <option value={branchValue}>{branchValue}</option>}{(branchList.data?.items ?? []).map((item) => <option key={item.name} value={item.name}>{item.name}{item.isDefault ? " · default" : ""}</option>)}</select></label></div></CardHeader>
      {branchList.isError && <p role="alert" className="border-b px-4 py-2 text-xs text-destructive">Could not load other branches. <button type="button" className="underline" onClick={() => void branchList.refetch()}>Retry</button></p>}
      <CardContent className="p-0">
        <div className="flex flex-wrap items-center gap-1 border-b px-4 py-3 text-sm"><button type="button" onClick={() => setPath("")} className="min-w-0 max-w-full rounded px-1.5 py-1 text-left font-medium hover:bg-muted [overflow-wrap:anywhere]">{repository.data?.name ?? project.repository.name}</button>{breadcrumbs.map((part) => <span key={part.path} className="flex items-center gap-1 text-muted-foreground"><span>/</span><button type="button" onClick={() => setPath(part.path)} className="max-w-[200px] text-left [overflow-wrap:anywhere] rounded px-1 py-1 hover:bg-muted hover:text-foreground">{part.name}</button></span>)}</div>
        {files.isLoading ? <div className="space-y-2 p-4" aria-label="Loading repository files">{[1,2,3,4].map((n) => <div key={n} className="h-10 animate-pulse rounded-md bg-muted/60" />)}</div> : files.isError ? <div className="p-8 text-center"><p role="alert" className="text-sm text-destructive">Could not load this repository path. {files.error.message}</p><Button variant="outline" className="mt-3" onClick={() => void files.refetch()}>Retry</Button></div> : files.data?.kind === "directory" ? files.data.entries.length ? <div className="divide-y">{files.data.entries.map((entry) => <div key={entry.path} className="flex min-w-0 items-center gap-3 px-4 py-2.5 hover:bg-muted/30"><span className="text-blue-600 dark:text-blue-400">{entry.type === "directory" ? <Folder className="size-4" /> : <File className="size-4" />}</span>{entry.type === "directory" ? <button type="button" onClick={() => setPath(entry.path)} className="min-w-0 flex-1 text-left text-sm [overflow-wrap:anywhere] font-medium hover:underline">{entry.name}</button> : entry.previewable ? <button type="button" onClick={() => setPath(entry.path)} className="min-w-0 flex-1 text-left text-sm [overflow-wrap:anywhere] hover:underline">{entry.name}</button> : <span className="min-w-0 flex-1 [overflow-wrap:anywhere] text-sm">{entry.name}</span>}<span className="shrink-0 text-right text-xs text-muted-foreground sm:w-20">{entry.type === "directory" ? "Folder" : formatBytes(entry.size)}</span>{entry.url && <a href={entry.url} target="_blank" rel="noreferrer" aria-label={`Open ${entry.name} on GitHub`} className="rounded p-1 text-muted-foreground hover:text-foreground"><ExternalLink className="size-3.5" /></a>}</div>)}</div> : <EmptyPanel icon={Folder} title="Empty directory" description="This directory has no visible files." /> : files.data ? <div className="min-w-0"><div className="flex flex-wrap items-center justify-between gap-2 border-b bg-muted/20 px-4 py-2"><span className="[overflow-wrap:anywhere] font-mono text-xs">{files.data.path}</span><div className="flex items-center gap-2">{files.data.size != null && <span className="text-xs text-muted-foreground">{formatBytes(files.data.size)}</span>}{files.data.url && <a href={files.data.url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-xs text-blue-700 hover:underline dark:text-blue-300">Open on GitHub <ExternalLink className="size-3" /></a>}</div></div>{files.data.preview != null ? <SourcePreview path={files.data.path} content={files.data.preview} /> : <div className="p-8 text-center text-sm text-muted-foreground">Preview is unavailable for this file{files.data.previewUnavailableReason ? ` (${files.data.previewUnavailableReason.replaceAll("_", " ")})` : ""}. It remains available on GitHub.</div>}</div> : null}
      </CardContent>
    </Card>
  </div>;
}

function formatBytes(bytes: number): string { if (bytes < 1024) return `${bytes} B`; if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`; return `${(bytes / (1024 * 1024)).toFixed(1)} MB`; }

function SourcePreview({ path, content }: { path: string; content: string }) {
  const extension = path.split(".").pop()?.toLowerCase() ?? "";
  const supported = /^(?:[cm]?js|jsx|tsx?|json|py|go|java|sh|ps1|ya?ml|toml|sql|tf|hcl)$/u.test(extension) || /(?:^|\/)Dockerfile$/u.test(path);
  const tokens = supported
    ? content.split(/("(?:[^"\\\r\n]|\\[^\r\n])*"|'(?:[^'\\\r\n]|\\[^\r\n])*'|\/\/[^\r\n]*|#[^\r\n]*|\b(?:async|await|break|class|const|def|else|export|extends|false|for|from|func|function|if|import|interface|let|new|null|package|return|struct|true|type|var|while|FROM|RUN|COPY|WORKDIR|CMD|ENTRYPOINT|SELECT|WHERE|JOIN|INSERT|UPDATE|DELETE)\b|\b\d+(?:\.\d+)?\b)/u)
    : [content];
  return <pre className="max-h-[70vh] overflow-auto whitespace-pre-wrap p-4 font-mono text-xs leading-5 [overflow-wrap:anywhere]"><code>
    {tokens.map((token, index) => {
      if (index % 2 === 0) return token;
      const color = token.startsWith("//") || token.startsWith("#")
        ? "text-muted-foreground"
        : token.startsWith('"') || token.startsWith("'")
          ? "text-emerald-700 dark:text-emerald-300"
          : /^\d/u.test(token) ? "text-violet-700 dark:text-violet-300" : "text-blue-700 dark:text-blue-300";
      return <span key={index} className={color}>{token}</span>;
    })}
  </code></pre>;
}

function Commits({ project }: { project: Project }) {
  const { activeWorkspace } = useDashboard();
  const repository = useRepository(project.id, activeWorkspace?.id);
  const branches = useBranches(project.id, activeWorkspace?.id);
  const [branch, setBranch] = useState("");
  const [copyStatus, setCopyStatus] = useState("");
  const [page, setPage] = useState(1);
  const [selectedSha, setSelectedSha] = useState("");
  const commitTrigger = useRef<HTMLButtonElement | null>(null);
  const refresh = useRefreshRepository(project.id, activeWorkspace?.id);
  const canRefresh = activeWorkspace?.role === "OWNER" || activeWorkspace?.role === "ADMIN";
  const branchName = branch || repository.data?.selectedBranch || project.repository?.selectedBranch || repository.data?.defaultBranch || project.repository?.defaultBranch || "";
  const history = useCommits(project.id, branchName, page, activeWorkspace?.id);
  const detail = useCommitDetail(project.id, selectedSha, branchName, activeWorkspace?.id);
  async function copySha(commit: Commit) {
    try {
      await navigator.clipboard.writeText(commit.sha);
      setCopyStatus(`Copied ${commit.shortSha}`);
    } catch {
      setCopyStatus("Could not copy the SHA. Open the commit details to select it.");
    }
  }

  if (!project.repository) return <EmptyPanel icon={GitCommitHorizontal} title="No repository connected" description="Commit history becomes available after a GitHub repository is imported." />;
  return <div className="space-y-4">
    <Card><CardHeader className="border-b"><div className="flex flex-wrap items-center justify-between gap-3"><div><CardTitle>Commit history</CardTitle><CardDescription>Commit data is retrieved from GitHub for the selected branch.</CardDescription></div><div className="flex min-w-0 flex-wrap items-center gap-2"><select aria-label="Commit branch" value={branchName} onChange={(event) => { setBranch(event.target.value); setPage(1); setSelectedSha(""); }} className="h-9 min-w-0 max-w-full sm:max-w-[240px] rounded-md border bg-background px-2 font-mono text-xs">{branchName && !branches.data?.items.some((item) => item.name === branchName) && <option value={branchName}>{branchName}</option>}{(branches.data?.items ?? []).map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}</select><Button variant="outline" size="sm" disabled={refresh.isPending || history.isFetching || !branchName} onClick={() => canRefresh ? refresh.mutate() : void history.refetch()}><RefreshCw className={refresh.isPending || history.isFetching ? "animate-spin" : ""} />Refresh</Button></div></div>
      <CardDescription>Verified means GitHub validated the commit signature. Unverified includes unsigned commits and signatures GitHub could not validate.</CardDescription>
      {branches.isError && <p role="alert" className="text-xs text-destructive">Other branches could not be loaded. <button type="button" className="underline" onClick={() => void branches.refetch()}>Retry</button></p>}
      {refresh.isError && <p role="alert" className="text-xs text-destructive">{refresh.error.message}</p>}
      {copyStatus && <p role="status" className="text-xs text-muted-foreground">{copyStatus}</p>}
    </CardHeader>
      {history.isLoading ? <div className="space-y-3 p-4" aria-label="Loading commit history">{[1,2,3,4,5].map((n) => <div key={n} className="h-16 animate-pulse rounded-md bg-muted/60" />)}</div> : history.isError ? <div className="p-8 text-center"><p role="alert" className="text-sm text-destructive">Commit history could not be loaded. {history.error.message}</p><Button variant="outline" className="mt-3" onClick={() => void history.refetch()}>Retry</Button></div> : history.data?.items.length ? <div className="divide-y">{history.data.items.map((commit) => <article key={commit.sha} className="flex min-w-0 flex-col gap-2 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"><div className="flex min-w-0 gap-3"><CommitAvatar commit={commit} /><div className="min-w-0"><button type="button" onClick={(event) => { commitTrigger.current = event.currentTarget; setSelectedSha(commit.sha); }} aria-haspopup="dialog" className="text-left [overflow-wrap:anywhere] text-sm font-medium hover:text-blue-700 dark:hover:text-blue-300">{commit.message.split("\n")[0]}</button><p className="mt-1 flex flex-wrap gap-x-2 text-xs text-muted-foreground"><span className="[overflow-wrap:anywhere]">{commit.authorName ?? commit.githubUsername ?? "Unknown author"}</span>{commit.timestamp && <Timestamp value={commit.timestamp} relative />}</p></div></div><div className="flex shrink-0 flex-wrap items-center gap-2 pl-11 sm:pl-0"><code className="text-xs text-muted-foreground">{commit.shortSha}</code><button type="button" aria-label={`Copy commit ${commit.shortSha}`} onClick={() => void copySha(commit)} className="rounded p-1 text-muted-foreground hover:bg-muted hover:text-foreground"><Copy className="size-3.5" /></button><a href={commit.url} target="_blank" rel="noreferrer" aria-label="Open commit on GitHub" className="rounded p-1 text-muted-foreground hover:bg-muted hover:text-foreground"><ExternalLink className="size-3.5" /></a><Badge variant="outline" title={commit.verificationReason?.replaceAll("_", " ")}>{commit.verified === true ? "Verified" : commit.verified === false ? "Unverified" : "Signature unknown"}</Badge></div></article>)}</div> : <EmptyPanel icon={GitCommitHorizontal} title="No commits on this branch" description="GitHub returned no commits for this branch." />}
      {history.data && (page > 1 || history.data.hasNextPage) ? <div className="flex flex-wrap items-center justify-between gap-2 border-t px-4 py-3"><span className="min-w-0 text-xs text-muted-foreground [overflow-wrap:anywhere]">Page {page} · {branchName}</span><div className="flex gap-2"><Button size="sm" variant="outline" disabled={page <= 1} onClick={() => setPage((current) => current - 1)}>Previous</Button><Button size="sm" variant="outline" disabled={!history.data.hasNextPage} onClick={() => setPage((current) => current + 1)}>Next</Button></div></div> : null}
    </Card>
    <Dialog open={Boolean(selectedSha)} onOpenChange={(open) => { if (!open) setSelectedSha(""); }}><DialogContent onCloseAutoFocus={(event) => { event.preventDefault(); commitTrigger.current?.focus(); }} className="max-h-[85dvh] min-w-0 overflow-y-auto sm:max-w-2xl"><DialogHeader><DialogTitle>Commit details</DialogTitle><DialogDescription className="break-all font-mono">{selectedSha}</DialogDescription></DialogHeader>{detail.isLoading ? <div className="h-40 animate-pulse bg-muted/30" /> : detail.isError ? <p role="alert" className="p-4 text-sm text-destructive">Could not load commit details: {detail.error.message}</p> : detail.data && <div className="min-w-0 space-y-4"><p className="whitespace-pre-wrap text-sm [overflow-wrap:anywhere]">{detail.data.message}</p><div className="flex flex-wrap gap-2">{detail.data.stats?.additions !== undefined && <Badge variant="secondary">+{detail.data.stats.additions}</Badge>}{detail.data.stats?.deletions !== undefined && <Badge variant="outline">−{detail.data.stats.deletions}</Badge>}<Badge variant="outline">{detail.data.files.length} {detail.data.files.length === 1 ? "file" : "files"} returned</Badge>{detail.data.filesTruncated && <p className="text-xs text-muted-foreground">More files are available on GitHub.</p>}</div><div className="divide-y rounded-md border">{detail.data.files.map((file) => <div key={file.filename} className="flex flex-wrap items-center justify-between gap-2 px-3 py-2 text-xs"><span className="min-w-0 flex-1 break-all font-mono">{file.filename}</span><span className="text-muted-foreground">{file.status} · +{file.additions} / −{file.deletions}</span>{file.url && <a href={file.url} target="_blank" rel="noreferrer" aria-label={`Open ${file.filename} on GitHub`}><ExternalLink className="size-3.5" /></a>}</div>)}</div></div>}</DialogContent></Dialog>
  </div>;
}

function GitIntegration({ project }: { project: Project }) {
  const { activeWorkspace } = useDashboard();
  const repository = useRepository(project.id, activeWorkspace?.id);
  const branches = useBranches(project.id, activeWorkspace?.id);
  const languages = useRepositoryLanguages(project.id, activeWorkspace?.id);
  const detection = useRepositoryDetection(project.id, repository.data?.selectedBranch ?? project.repository?.selectedBranch ?? project.repository?.defaultBranch ?? "", activeWorkspace?.id);
  const refresh = useRefreshRepository(project.id, activeWorkspace?.id);
  const changeBranch = useChangeProjectBranch(project.id, activeWorkspace?.id);
  const repo = project.repository;
  const canManage = activeWorkspace?.role === "OWNER" || activeWorkspace?.role === "ADMIN";
  const connectedBranch = repository.data?.selectedBranch ?? repo?.selectedBranch ?? repository.data?.defaultBranch ?? repo?.defaultBranch ?? "";
  return <div className="space-y-5"><Card><CardHeader className="border-b"><div className="flex flex-wrap items-start justify-between gap-3"><div><CardTitle>GitHub connection</CardTitle><CardDescription>Live repository data is verified through the authorized GitHub App installation.</CardDescription></div>{repo && canManage && <Button variant="outline" size="sm" disabled={refresh.isPending} onClick={() => refresh.mutate()}><RefreshCw className={`size-3.5 ${refresh.isPending ? "animate-spin" : ""}`} />{refresh.isPending ? "Refreshing…" : "Refresh from GitHub"}</Button>}</div></CardHeader>{refresh.isError && <p role="alert" className="border-b px-4 py-2 text-sm text-destructive">{refresh.error.message}</p>}{repo ? <CardContent className="pt-4"><div className="grid min-w-0 gap-5 sm:grid-cols-2 xl:grid-cols-3"><Info label="GitHub account" value={repository.data?.installation.accountLogin ?? repo.installation?.accountLogin ?? "Account unavailable"} /><Info label="Repository" value={repository.data?.fullName ?? repo.fullName ?? repo.name} href={repository.data?.repositoryUrl ?? (repo.fullName ? `https://github.com/${repo.fullName}` : undefined)} />{canManage ? <div className="min-w-0"><p className="text-xs text-muted-foreground">Connected branch</p><select aria-label="Set connected branch" value={connectedBranch} disabled={changeBranch.isPending || branches.isLoading} onChange={(event) => changeBranch.mutate(event.target.value)} className="mt-1 h-9 max-w-full rounded-md border bg-background px-2 font-mono text-sm">{connectedBranch && !branches.data?.items.some((item) => item.name === connectedBranch) && <option value={connectedBranch}>{connectedBranch}</option>}{(branches.data?.items ?? []).map((branch) => <option key={branch.name} value={branch.name}>{branch.name}{branch.isDefault ? " · default" : ""}</option>)}</select></div> : <Info label="Connected branch" value={connectedBranch || "Unknown"} mono />}<Info label="Default branch" value={repository.data?.defaultBranch ?? repo.defaultBranch ?? "Unknown"} mono /><Info label="Visibility" value={repository.data?.visibility ?? (repo.isPrivate ? "private" : "public")} /><Info label="Primary language" value={repository.data?.language ?? repo.language ?? "Not reported by GitHub"} /><Info label="Stars / forks" value={repository.data ? `${repository.data.stars.toLocaleString()} / ${repository.data.forks.toLocaleString()}` : "Loading live statistics…"} /><Info label="Open issues" value={repository.data ? repository.data.openIssuesCount.toLocaleString() : "—"} /><Info label="Owner type" value={repository.data?.owner.type ?? "Unknown"} /><Info label="Installation status" value={repository.data?.installation.status ?? repo.installation?.status ?? "Unavailable"} /><Info label="Repository access" value={repository.data?.installation.repositorySelection.toLowerCase() ?? repo.installation?.repositorySelection?.toLowerCase() ?? "Unknown"} /><Info label="Last pushed" value={repository.data?.lastPushedAt ? new Date(repository.data.lastPushedAt).toLocaleString() : "Not reported"} /><Info label="Last refreshed" value={repository.data?.lastRefreshedAt ? new Date(repository.data.lastRefreshedAt).toLocaleString() : "Not refreshed yet"} /></div>{repository.isError && <p role="alert" className="mt-4 text-sm text-destructive">Live GitHub metadata is unavailable. {repository.error.message}</p>}{languages.data?.languages.length ? <div className="mt-6 border-t pt-4"><p className="mb-3 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Language breakdown</p><div className="flex flex-wrap gap-2">{languages.data.languages.map((item) => <Badge variant="outline" key={item.name}>{item.name} · {item.percentage}%</Badge>)}</div></div> : null}{detection.data && <div className="mt-5 border-t pt-4"><p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Read-only framework analysis</p><p className="mt-2 text-sm">{detection.data.frameworks.length ? detection.data.frameworks.map((item) => item.name).join(", ") : "No supported framework evidence found."}</p></div>}{changeBranch.isError && <p role="alert" className="mt-4 text-sm text-destructive">Branch was not changed: {changeBranch.error.message}</p>}</CardContent> : <CardContent className="pt-4"><EmptyPanel icon={GitHubIcon} title="Repository not connected" description="Import a GitHub repository to attach a source connection to this project." /></CardContent>}</Card><div className="grid min-w-0 gap-3 md:grid-cols-3"><FutureControl icon={RefreshCw} title="Automatic deployments" detail="Not enabled" /><FutureControl icon={Webhook} title="Webhook delivery" detail="Not configured" /><FutureControl icon={GitBranch} title="Branch rules" detail="Not configured" /></div></div>;
}

function Info({ label, value, href, mono }: { label: string; value: string; href?: string; mono?: boolean }) {
  return <div className="min-w-0"><p className="text-xs text-muted-foreground">{label}</p>{href ? <a href={href} target="_blank" rel="noreferrer" className={`mt-1 block [overflow-wrap:anywhere] text-sm font-medium underline-offset-4 hover:underline ${mono ? "font-mono" : ""}`}>{value}<ArrowUpRight className="ml-1 inline size-3" /></a> : <p className={`mt-1 [overflow-wrap:anywhere] text-sm font-medium ${mono ? "font-mono" : ""}`} title={value}>{value}</p>}</div>;
}

function FutureControl({ icon: Icon, title, detail }: { icon: ComponentType<{ className?: string }>; title: string; detail: string }) {
  return <Card size="sm"><CardContent className="flex items-start gap-3"><Icon className="mt-0.5 size-4 text-muted-foreground" /><div><p className="text-sm font-medium">{title}</p><p className="mt-1 text-xs text-muted-foreground">{detail} · Future control</p></div></CardContent></Card>;
}

function Logs() {
  return <Card><CardHeader className="border-b"><CardTitle>Project logs</CardTitle><CardDescription>Build and runtime output for this project.</CardDescription></CardHeader><CardContent><EmptyPanel icon={TerminalSquare} title="Project log stream unavailable" description="CloudPilot does not yet provide a project-scoped log viewer. No log content is being simulated here." /></CardContent></Card>;
}

function Monitoring() {
  return <div className="space-y-5"><div className="rounded-lg border border-amber-600/20 bg-amber-500/5 p-4"><div className="flex gap-3"><AlertCircle className="mt-0.5 size-4 shrink-0 text-amber-600" /><div><p className="text-sm font-medium">Runtime monitoring is not connected</p><p className="mt-1 text-sm leading-6 text-muted-foreground">No health checks, uptime, latency, or resource metrics are available for this project yet.</p></div></div></div><Card><CardHeader className="border-b"><CardTitle>Monitoring sources</CardTitle><CardDescription>Telemetry appears after an application is deployed and connected.</CardDescription></CardHeader><CardContent className="grid gap-3 pt-4 md:grid-cols-3"><FutureControl icon={Activity} title="Health checks" detail="No endpoint configured" /><FutureControl icon={ArrowDownRight} title="Request metrics" detail="No traffic data" /><FutureControl icon={Clock3} title="Uptime history" detail="No monitoring source" /></CardContent></Card></div>;
}

function Settings({ project, onEdit, onDelete }: { project: Project; onEdit: () => void; onDelete: () => void }) {
  return <div className="space-y-5"><Card><CardHeader className="border-b"><CardTitle>Project details</CardTitle><CardDescription>These changes are persisted to the active workspace.</CardDescription></CardHeader><CardContent className="flex flex-wrap items-center justify-between gap-4 pt-4"><div><p className="text-sm font-medium">Name and description</p><p className="mt-1 max-w-lg text-sm text-muted-foreground">{project.description || "No description provided."}</p></div><Button variant="outline" onClick={onEdit}><Settings2 />Edit details</Button></CardContent></Card><Card className="border-red-600/20"><CardHeader className="border-b"><CardTitle className="text-destructive">Danger zone</CardTitle><CardDescription>Deleting a project also removes its project-scoped records.</CardDescription></CardHeader><CardContent className="flex flex-wrap items-center justify-between gap-4 pt-4"><div><p className="text-sm font-medium">Delete this project</p><p className="mt-1 text-sm text-muted-foreground">This action cannot be undone.</p></div><Button variant="destructive" onClick={onDelete}>Delete project</Button></CardContent></Card></div>;
}

export function ProjectSectionView({ section, project, onEdit, onDelete }: { section: string; project: Project; onEdit: () => void; onDelete: () => void }) {
  switch (section) {
    case "deployments": return <Deployments project={project} />;
    case "build-configuration": return <BuildConfiguration project={project} />;
    case "source": return <RepositorySource project={project} />;
    case "commits": return <Commits project={project} />;
    case "environment-variables": return <EnvironmentVariables key={project.id} projectId={project.id} />;
    case "logs": return <Logs />;
    case "monitoring": return <Monitoring />;
    case "git-integration": return <GitIntegration project={project} />;
    case "settings": return <Settings project={project} onEdit={onEdit} onDelete={onDelete} />;
    default: return <Overview project={project} />;
  }
}
