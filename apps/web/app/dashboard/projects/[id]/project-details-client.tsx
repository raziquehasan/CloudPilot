"use client";

import { useCallback, useEffect, useRef, useState, useSyncExternalStore, type FormEvent } from "react";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import {
  ArrowLeft, CheckCircle2, ChevronDown,
  CircleAlert, Code2, ExternalLink, FolderKanban, GitBranch,
  Globe2, LockKeyhole, Pencil, Rocket, Settings2, Trash2,
} from "lucide-react";
import { DashboardPageShell } from "@/components/dashboard/page-shell";
import { GitHubIcon } from "@/components/shared/github-icon";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, apiFetch } from "@/lib/api";
import { useBuildConfiguration, useRepository } from "@/lib/repository-intelligence";
import { useDashboard } from "@/components/dashboard/dashboard-provider";
import { ProjectSectionView, projectSections } from "./project-section-view";
import type { Project } from "./project-types";

const wideNavigation = "(min-width: 48rem)";

function subscribeToNavigationWidth(listener: () => void) {
  const query = window.matchMedia(wideNavigation);
  query.addEventListener("change", listener);
  return () => query.removeEventListener("change", listener);
}

interface DeploymentReadiness {
  repository: { connected: boolean; trusted: boolean };
  buildConfiguration: { configured: boolean };
  execution: { available: boolean };
}

type DeployEnvironment = "DEVELOPMENT" | "STAGING" | "PRODUCTION" | "PREVIEW";

function DeployControl({ project, branch, canManage }: { project: Project; branch: string; canManage: boolean }) {
  const router = useRouter();
  const queryClient = useQueryClient();
  const { activeWorkspace } = useDashboard();
  const workspaceId = activeWorkspace?.id;
  const build = useBuildConfiguration(project.id, workspaceId);
  const readinessQuery = useQuery({
    queryKey: ["deployment-readiness", workspaceId, project.id, branch, build.data?.configuration?.updatedAt],
    queryFn: ({ signal }) => apiFetch<DeploymentReadiness>(`/projects/${encodeURIComponent(project.id)}/deployment-readiness`, { signal }),
    enabled: Boolean(project.id && workspaceId && canManage && build.isSuccess),
    refetchInterval: 20_000,
  });
  const [open, setOpen] = useState(false);
  const [environment, setEnvironment] = useState<DeployEnvironment>("PRODUCTION");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const inFlight = useRef(false);
  const request = useRef<{ body: string; key: string } | null>(null);

  const readiness = readinessQuery.data;
  const configured = readiness?.buildConfiguration.configured ?? false;
  const executionAvailable = readiness?.execution.available ?? false;
  const repositoryConnected = readiness?.repository.connected ?? false;
  const repositoryTrusted = readiness?.repository.trusted ?? false;
  const configSaved = build.isSuccess && build.data.saved && Boolean(build.data.configuration);

  // Eligible only when the project is manageable, the repository is
  // connected, a build configuration is saved, and the backend reports
  // execution readiness. The server re-validates all of these regardless.
  const eligible = canManage && readinessQuery.isSuccess && repositoryConnected && repositoryTrusted
    && configured && configSaved && executionAvailable && Boolean(branch);
  const checking = readinessQuery.isPending || build.isPending;

  const reason = !canManage
    ? "Workspace administrator access is required to deploy."
    : checking
      ? "Checking deployment readiness…"
      : readinessQuery.isError || build.isError
        ? "Deployment readiness could not be verified."
      : !repositoryConnected
        ? "Connect an authorized GitHub repository before deploying."
        : !repositoryTrusted
          ? "This repository is not authorized for local builds."
        : !branch
          ? "Select a branch before deploying."
          : !configSaved || !configured
            ? "Save a complete build configuration before deploying."
            : !executionAvailable
              ? "A deployment engine is not available right now."
              : "";

  function openDialog() {
    if (!eligible || submitting) return;
    const savedEnvironment = build.data?.configuration?.environment;
    setEnvironment(savedEnvironment ?? "PRODUCTION");
    setError("");
    setOpen(true);
  }

  async function submit() {
    if (!eligible || inFlight.current) return;
    inFlight.current = true;
    setSubmitting(true);
    setError("");
    let created = false;
    try {
      const latest = await readinessQuery.refetch({ throwOnError: true });
      if (!latest.data?.execution.available) {
        setError("Deployment readiness changed. Check the project configuration and try again.");
        return;
      }
      const body = JSON.stringify({ branch, environment });
      if (request.current?.body !== body) request.current = { body, key: crypto.randomUUID() };
      const result = await apiFetch<{ id: string; status: string; failureMessage?: string | null }>(`/projects/${encodeURIComponent(project.id)}/deployments`, {
        method: "POST",
        headers: { "Idempotency-Key": request.current.key },
        body,
      });
      created = true;
      queryClient.setQueryData(["deployment-status", workspaceId, project.id, result.id], {
        status: result.status,
        failureMessage: result.failureMessage ?? null,
      });
      setOpen(false);
      const query = new URLSearchParams({ deployment: result.id, project: project.id });
      router.push(`/dashboard/logs?${query.toString()}`);
    } catch (cause) {
      if (cause instanceof ApiError && cause.status === 403) {
        setError(`Deployment request denied. ${cause.message}`);
      } else {
        setError(cause instanceof Error ? cause.message : "The deployment could not be created.");
      }
    } finally {
      if (!created) {
        inFlight.current = false;
        setSubmitting(false);
      }
    }
  }

  const configuration = build.data?.configuration;

  return <>
    <span title={!eligible ? reason : undefined}>
      <Button variant="outline" size="sm" disabled={!eligible || submitting} onClick={openDialog} aria-label={eligible ? undefined : `Deploy. ${reason}`}><Rocket />Deploy</Button>
    </span>
    <Dialog open={open} onOpenChange={(next) => { if (!submitting) setOpen(next); }}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Deploy {project.name}</DialogTitle>
          <DialogDescription>Confirm the deployment target. A new deployment will be queued on the connected deployment worker.</DialogDescription>
        </DialogHeader>
        <dl className="grid gap-3 text-sm sm:grid-cols-[auto_minmax(0,1fr)]">
          <dt className="text-muted-foreground">Repository</dt>
          <dd className="min-w-0 [overflow-wrap:anywhere] font-medium">{project.repository?.fullName ?? "Connected repository"}</dd>
          <dt className="text-muted-foreground">Branch</dt>
          <dd className="min-w-0 [overflow-wrap:anywhere] font-mono">{branch}</dd>
          <dt className="text-muted-foreground">Environment</dt>
          <dd><select aria-label="Deployment environment" value={environment} disabled={submitting} onChange={(event) => setEnvironment(event.target.value as DeployEnvironment)} className="h-9 w-full rounded-md border bg-background px-2 text-sm sm:w-auto">{(["DEVELOPMENT", "STAGING", "PRODUCTION", "PREVIEW"] as const).map((value) => <option key={value} value={value}>{value.charAt(0) + value.slice(1).toLowerCase()}</option>)}</select></dd>
          <dt className="text-muted-foreground">Build configuration</dt>
          <dd className="min-w-0 space-y-1 [overflow-wrap:anywhere]">
            {configuration ? <><p className="font-medium">{configuration.framework ?? "Custom build"}{configuration.runtime ? ` · ${configuration.runtime}` : ""}</p>{configuration.dockerfilePath ? <p className="text-xs text-muted-foreground">Dockerfile: {configuration.dockerfilePath}</p> : configuration.buildCommand ? <p className="text-xs text-muted-foreground">Build: {configuration.buildCommand}</p> : null}</> : <span className="text-muted-foreground">Saved configuration</span>}
          </dd>
        </dl>
        {(error || !eligible) && <p role="alert" className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-sm text-destructive">{error || reason}</p>}
        <DialogFooter>
          <Button type="button" variant="outline" disabled={submitting} onClick={() => setOpen(false)}>Cancel</Button>
          <Button type="button" disabled={!eligible || submitting} onClick={() => void submit()}>{submitting ? "Deploying…" : "Deploy now"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </>;
}

export default function ProjectDetailsPage() {
  const params = useParams<{ id: string; section?: string }>();
  const router = useRouter();
  const { activeWorkspace } = useDashboard();
  const [project, setProject] = useState<Project | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [notice, setNotice] = useState("");
  const section = params.section ?? "overview";

  const loadProject = useCallback(async () => {
    setLoading(true);
    setProject(null);
    try {
      const result = await apiFetch<Project>(`/projects/${encodeURIComponent(params.id)}`);
      setProject(result);
      setError("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Project unavailable.");
    } finally {
      setLoading(false);
    }
  }, [params.id]);

  useEffect(() => {
    const timer = window.setTimeout(() => { void loadProject(); }, 0);
    return () => window.clearTimeout(timer);
  }, [loadProject, activeWorkspace?.id]);

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!project) return;
    const form = new FormData(event.currentTarget);
    setSaving(true);
    try {
      const updated = await apiFetch<Project>(`/projects/${encodeURIComponent(project.id)}`, {
        method: "PATCH",
        body: JSON.stringify({ name: String(form.get("name")), description: String(form.get("description") || "") }),
      });
      setProject(updated);
      setEditing(false);
      setError("");
      setNotice("Project details saved.");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not update project.");
    } finally {
      setSaving(false);
    }
  }

  async function remove() {
    if (!project || !window.confirm(`Delete “${project.name}” and its project records?`)) return;
    try {
      await apiFetch(`/projects/${encodeURIComponent(project.id)}`, { method: "DELETE" });
      router.replace("/dashboard/projects");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not delete project.");
    }
  }

  const storedRepository = project?.repository;
  const liveRepository = useRepository(params.id, storedRepository ? activeWorkspace?.id : undefined);
  const repository = liveRepository.data ?? storedRepository;
  const repositoryUrl = liveRepository.data?.repositoryUrl ?? (repository?.fullName ? `https://github.com/${repository.fullName}` : null);
  const visibleSectionCount = useSyncExternalStore(subscribeToNavigationWidth, () => window.matchMedia(wideNavigation).matches ? 5 : 2, () => 2);
  const currentSection = projectSections.find((item) => item.id === section) ?? projectSections[0];
  const activeSection = currentSection.id;

  return <DashboardPageShell breadcrumb={project ? `Projects / ${project.name}` : "Projects / Details"}>
    <div className="mx-auto w-full max-w-[1320px] min-w-0">
      {notice && <div role="status" className="mb-4 flex items-center justify-between gap-3 rounded-lg border border-emerald-600/20 bg-emerald-500/5 px-3 py-2.5 text-sm"><span className="flex items-center gap-2"><CheckCircle2 className="size-4 text-emerald-600" />{notice}</span><button onClick={() => setNotice("")} aria-label="Dismiss notice" className="text-muted-foreground">×</button></div>}
      {error && <div role="alert" className="mb-4 flex items-center justify-between gap-3 rounded-lg border border-destructive/30 bg-destructive/5 px-3 py-2.5 text-sm"><span className="flex items-center gap-2"><CircleAlert className="size-4 text-destructive" />{error}</span><button onClick={() => setError("")} aria-label="Dismiss error" className="text-muted-foreground">×</button></div>}
      {loading && !project ? <div className="space-y-4" aria-label="Loading project"><div className="h-32 animate-pulse rounded-xl border bg-muted/40" /><div className="h-12 animate-pulse rounded-lg border bg-muted/40" /><div className="h-80 animate-pulse rounded-xl border bg-muted/40" /></div>
        : !project ? <div className="rounded-xl border border-dashed p-8 text-center"><FolderKanban className="mx-auto size-7 text-muted-foreground" /><p className="mt-3 text-sm font-semibold">Project unavailable</p><p className="mt-1 text-sm text-muted-foreground">This project may have been removed or may not belong to the active workspace.</p><Button className="mt-4" variant="outline" asChild><Link href="/dashboard/projects"><ArrowLeft />Back to projects</Link></Button></div>
          : <>
            <section className="mb-3 rounded-xl border bg-card">
              <div className="flex flex-col gap-3 p-3 sm:p-4 md:flex-row md:items-start md:justify-between">
                <div className="flex min-w-0 gap-3 sm:gap-4">
                  <span className="grid size-9 shrink-0 place-items-center rounded-xl border bg-muted/50"><FolderKanban className="size-5 text-blue-600 dark:text-blue-400" /></span>
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2"><h1 className="max-w-full text-lg font-semibold tracking-tight [overflow-wrap:anywhere] sm:text-xl">{project.name}</h1></div>
                    {project.description && <p className="mt-1 max-w-3xl text-sm text-muted-foreground [overflow-wrap:anywhere]">{project.description}</p>}
                    <div className="mt-2 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                      <span className="inline-flex min-w-0 items-center gap-1.5"><GitHubIcon className="size-3.5 shrink-0" />{repository ? <a href={repositoryUrl ?? undefined} target="_blank" rel="noreferrer" className="min-w-0 font-medium [overflow-wrap:anywhere] text-foreground underline-offset-4 hover:underline">{repository.fullName ?? repository.name}<ExternalLink className="ml-1 inline size-3" /></a> : <span>No repository connected</span>}</span>
                      {repository && <><span className="inline-flex items-center gap-1.5">{repository.isPrivate ? <LockKeyhole className="size-3.5" /> : <Globe2 className="size-3.5" />}{repository.isPrivate ? "Private" : "Public"}</span>{repository.language && <span className="inline-flex items-center gap-1.5"><Code2 className="size-3.5" />{repository.language}</span>}{repository.selectedBranch && <span className="inline-flex max-w-full items-center gap-1.5"><GitBranch className="size-3.5 shrink-0" /><span className="min-w-0 font-mono [overflow-wrap:anywhere]">{repository.selectedBranch}</span></span>}</>}
                    </div>
                  </div>
                </div>
                <div className="flex shrink-0 flex-wrap items-center gap-2">
                  <DeployControl key={`${activeWorkspace?.id}:${project.id}`} project={project} branch={repository?.selectedBranch ?? project.repository?.selectedBranch ?? ""} canManage={activeWorkspace?.role === "OWNER" || activeWorkspace?.role === "ADMIN"} />
                  <DropdownMenu><DropdownMenuTrigger asChild><Button variant="outline" size="sm" aria-label="Project actions"><span className="hidden sm:inline">Actions</span><ChevronDown /></Button></DropdownMenuTrigger><DropdownMenuContent align="end" className="w-48"><DropdownMenuItem onSelect={() => setEditing(true)}><Pencil />Edit project</DropdownMenuItem><DropdownMenuItem onSelect={() => router.push(`/dashboard/projects/${project.id}/settings`)}><Settings2 />Settings</DropdownMenuItem><DropdownMenuSeparator /><DropdownMenuItem variant="destructive" onSelect={() => void remove()}><Trash2 />Delete project</DropdownMenuItem></DropdownMenuContent></DropdownMenu>
                </div>
              </div>
            </section>

            <nav aria-label="Project navigation" className="mb-4 flex flex-wrap items-center gap-1 border-b">
              {projectSections.slice(0, visibleSectionCount).map(({ id, label, icon: Icon }) => <Link
                key={id}
                href={id === "overview" ? `/dashboard/projects/${project.id}` : `/dashboard/projects/${project.id}/${id}`}
                aria-current={activeSection === id ? "page" : undefined}
                className={`inline-flex min-h-9 items-center gap-1.5 rounded-t-md border-b-2 px-2 text-xs font-medium transition-colors hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${activeSection === id ? "border-blue-600 text-foreground" : "border-transparent text-muted-foreground"}`}
              ><Icon className="size-3.5 shrink-0" />{label}</Link>)}
              <DropdownMenu>
                <DropdownMenuTrigger asChild><Button variant="ghost" size="sm" aria-label={`More project sections. Current section: ${currentSection.label}`}>More <ChevronDown /></Button></DropdownMenuTrigger>
                <DropdownMenuContent align="start" className="w-52 max-w-[calc(100vw-1rem)]">
                  {projectSections.slice(visibleSectionCount).map(({ id, label, icon: Icon }) => <DropdownMenuItem key={id} asChild>
                    <Link href={`/dashboard/projects/${project.id}/${id}`} aria-current={activeSection === id ? "page" : undefined}><Icon />{label}{activeSection === id && <CheckCircle2 className="ml-auto" />}</Link>
                  </DropdownMenuItem>)}
                </DropdownMenuContent>
              </DropdownMenu>
            </nav>
            <div className="min-w-0"><ProjectSectionView section={activeSection} project={project} onEdit={() => setEditing(true)} onDelete={() => void remove()} /></div>
          </>}
    </div>

    {project && <Dialog open={editing} onOpenChange={setEditing}><DialogContent className="max-w-lg"><DialogHeader><DialogTitle>Edit project</DialogTitle><DialogDescription>Update the project name and description for {activeWorkspace?.name ?? "this workspace"}.</DialogDescription></DialogHeader><form id="project-edit-form" onSubmit={save} className="space-y-4"><label className="block text-xs font-medium">Project name<input name="name" required maxLength={80} defaultValue={project.name} className="mt-2 h-10 w-full rounded-md border bg-background px-3 text-sm" /></label><label className="block text-xs font-medium">Description<textarea name="description" maxLength={500} defaultValue={project.description ?? ""} className="mt-2 min-h-24 w-full resize-y rounded-md border bg-background p-3 text-sm" /></label></form><DialogFooter><Button type="button" variant="outline" onClick={() => setEditing(false)}>Cancel</Button><Button form="project-edit-form" disabled={saving}>{saving ? "Saving…" : "Save changes"}</Button></DialogFooter></DialogContent></Dialog>}
  </DashboardPageShell>;
}
