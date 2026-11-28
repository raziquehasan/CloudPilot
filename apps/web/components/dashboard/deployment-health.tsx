"use client";

import { useRef, useState, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity, ChevronRight, RefreshCw, RotateCcw } from "lucide-react";
import { CartesianGrid, Line, LineChart, XAxis, YAxis } from "recharts";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { ApiError, apiFetch } from "@/lib/api";
import {
  checkOutcome, failureMessage, formatChartTooltipTime, formatHealthTime, formatRecoveryDuration,
  isRecoveryActive, monitoringMessage, recoveryBlockMessage, recoverySubmissionError,
  type DeploymentHealth, type HealthCheck, type Recovery,
} from "./deployment-health-data";

const statusColors = {
  good: "border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
  bad: "border-red-500/25 bg-red-500/10 text-red-700 dark:text-red-300",
  warning: "border-amber-500/25 bg-amber-500/10 text-amber-800 dark:text-amber-300",
  active: "border-blue-500/25 bg-blue-500/10 text-blue-700 dark:text-blue-300",
  neutral: "border-border bg-muted/70 text-muted-foreground",
};

function Status({ value }: { value: string }) {
  let tone: keyof typeof statusColors = "neutral";
  if (["HEALTHY", "SUCCEEDED", "PASSED"].includes(value)) tone = "good";
  if (["UNHEALTHY", "FAILED"].includes(value)) tone = "bad";
  if (value === "DEGRADED") tone = "warning";
  if (["RECOVERING", "REQUESTED", "RUNNING", "STARTING"].includes(value)) tone = "active";
  return <Badge variant="outline" className={`capitalize ${statusColors[tone]}`}>{value.toLowerCase().replaceAll("_", " ")}</Badge>;
}

function Metric({ name, children }: { name: string; children: ReactNode }) {
  return <div className="min-w-0">
    <dt className="text-xs text-muted-foreground">{name}</dt>
    <dd className="mt-1.5 break-words text-sm font-medium tabular-nums">{children}</dd>
  </div>;
}

function Failure({ code }: { code: string }) {
  return <div className="space-y-1 break-words">
    <p className="text-sm">{failureMessage(code)}</p>
    {/^[A-Z][A-Z0-9_]{0,79}$/.test(code) && <code className="break-all text-xs text-muted-foreground">{code}</code>}
  </div>;
}

function QueryProblem({ message, retry }: { message: string; retry: () => void }) {
  return <div role="alert" className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-sm">
    <p>{message}</p><Button type="button" size="sm" variant="outline" onClick={retry}>Try again</Button>
  </div>;
}

function scopedError(error: unknown) {
  if (!(error instanceof ApiError)) return false;
  return error.status === 401 || error.status === 403 || error.status === 404;
}

function recentRecovery(attempts: Recovery[]) {
  return attempts.some((attempt) => isRecoveryActive(attempt) && Date.now() - Date.parse(attempt.createdAt) < 120_000);
}

function LoadingRows({ label }: { label: string }) {
  return <div role="status" aria-busy="true" className="space-y-3">
    <span className="sr-only">{label}</span>
    <Skeleton className="h-7 w-1/2" />
    <Skeleton className="h-7 w-full" />
    <Skeleton className="h-7 w-3/4" />
  </div>;
}

const chartConfig = {
  passed: { label: "Passed", theme: { light: "#2563eb", dark: "#60a5fa" } },
  failed: { label: "Failed", theme: { light: "#dc2626", dark: "#f87171" } },
};

function LatencyChart({ checks }: { checks: HealthCheck[] }) {
  const data = [...checks].reverse().map((check) => ({
    time: Date.parse(check.checkedAt),
    passed: checkOutcome(check) === "PASSED" ? check.responseLatencyMs : null,
    failed: checkOutcome(check) === "FAILED" ? check.responseLatencyMs : null,
  })).filter(({ time }) => Number.isFinite(time));
  const hasMeasurements = data.some(({ passed, failed }) => passed !== null || failed !== null);
  const shortTime = (time: number) => new Date(time).toLocaleTimeString(undefined, {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
  });
  return <Card>
    <CardHeader>
      <CardTitle><h2>Response latency</h2></CardTitle>
      <CardDescription>Recorded HTTP measurements from the latest {checks.length} checks.</CardDescription>
    </CardHeader>
    <CardContent>
      {hasMeasurements ? <>
        <div className="mb-3 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
          <span className="flex items-center gap-1.5"><span className="size-2 rounded-full bg-blue-600 dark:bg-blue-400" />Passed</span>
          <span className="flex items-center gap-1.5"><span className="size-2 rounded-full bg-red-600 dark:bg-red-400" />Failed</span>
          <span>Checks without a measurement leave a gap.</span>
        </div>
        <ChartContainer config={chartConfig} className="h-56 w-full min-w-0 sm:h-64" aria-label="HTTP latency trend in milliseconds">
          <LineChart accessibilityLayer data={data} margin={{ left: 0, right: 14, top: 8, bottom: 2 }}>
            <CartesianGrid vertical={false} />
            <XAxis dataKey="time" tick={{ fill: "var(--muted-foreground)" }} tickFormatter={shortTime} minTickGap={32} tickLine={false} axisLine={false} />
            <YAxis unit=" ms" width={56} tick={{ fill: "var(--muted-foreground)" }} tickLine={false} axisLine={false} allowDecimals={false} />
            <ChartTooltip content={<ChartTooltipContent labelFormatter={(_, items) => formatChartTooltipTime(items)} />} />
            <Line type="linear" dataKey="passed" stroke="var(--color-passed)" strokeWidth={2} dot={{ r: 2 }} connectNulls={false} isAnimationActive={false} />
            <Line dataKey="failed" stroke="none" dot={{ r: 4, fill: "var(--color-failed)" }} connectNulls={false} isAnimationActive={false} />
          </LineChart>
        </ChartContainer>
      </> : <p className="rounded-md border border-dashed p-4 text-sm text-muted-foreground">No latency measurements recorded.</p>}
    </CardContent>
  </Card>;
}

const checkColumns = ["Checked", "Docker", "HTTP", "Latency", "Outcome", "Generation", "Failure reason"];

function CheckHistory({ checks }: { checks: HealthCheck[] }) {
  return <Card>
    <CardHeader><CardTitle><h2>Health check history</h2></CardTitle><CardDescription>Latest {checks.length} recorded checks for this runtime.</CardDescription></CardHeader>
    <CardContent className="space-y-4">
      {!checks.length ? <p className="rounded-md border border-dashed p-4 text-sm text-muted-foreground">No health checks have been recorded.</p> : <>
        <div className="space-y-2 lg:hidden">
          {checks.map((check) => <div key={check.id} className="rounded-md border p-3">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
              <span className="font-mono text-xs">{formatHealthTime(check.checkedAt)}</span><Status value={checkOutcome(check)} />
            </div>
            <dl className="grid grid-cols-2 gap-3">
              <Metric name="Docker">{check.dockerState ?? check.dockerResult.toLowerCase()}</Metric>
              <Metric name="HTTP">{check.httpStatus ?? check.httpResult.toLowerCase()}</Metric>
              <Metric name="Latency">{check.responseLatencyMs === null ? "Unavailable" : `${check.responseLatencyMs} ms`}</Metric>
              <Metric name="Generation">{check.generation}</Metric>
            </dl>
            {check.failureReason && <div className="mt-3 border-t pt-3"><Failure code={check.failureReason} /></div>}
          </div>)}
        </div>
        <div className="hidden max-w-full overflow-x-auto lg:block">
          <table className="w-full text-left text-sm">
            <thead className="border-b text-xs text-muted-foreground"><tr>{checkColumns.map((column) => <th scope="col" key={column} className="px-2 py-2.5 font-medium">{column}</th>)}</tr></thead>
            <tbody className="divide-y">{checks.map((check) => <tr key={check.id}>
              <td className="px-2 py-3 font-mono text-xs">{formatHealthTime(check.checkedAt)}</td>
              <td className="px-2 py-3 capitalize">{check.dockerState ?? check.dockerResult.toLowerCase()}</td>
              <td className="px-2 py-3 capitalize tabular-nums">{check.httpStatus ?? check.httpResult.toLowerCase()}</td>
              <td className="px-2 py-3 tabular-nums">{check.responseLatencyMs === null ? "Unavailable" : `${check.responseLatencyMs} ms`}</td>
              <td className="px-2 py-3"><Status value={checkOutcome(check)} /></td>
              <td className="px-2 py-3 tabular-nums">{check.generation}</td>
              <td className="max-w-56 px-2 py-3">{check.failureReason ? <Failure code={check.failureReason} /> : "—"}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </>}
    </CardContent>
  </Card>;
}

function RecoveryFields({ attempt }: { attempt: Recovery }) {
  return <dl className="grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-3">
    <Metric name="Trigger"><span className="capitalize">{attempt.trigger.toLowerCase()}</span></Metric>
    <Metric name="Action"><span className="capitalize">{attempt.action.toLowerCase()}</span></Metric>
    <Metric name="Generation">{attempt.generation}</Metric>
    <Metric name="Requested">{formatHealthTime(attempt.createdAt)}</Metric>
    <Metric name="Started">{formatHealthTime(attempt.startedAt)}</Metric>
    <Metric name="Finished">{formatHealthTime(attempt.finishedAt)}</Metric>
    <Metric name="Duration">{formatRecoveryDuration(attempt.startedAt, attempt.finishedAt)}</Metric>
    {attempt.failureReason && <div className="col-span-full"><dt className="mb-1 text-xs text-muted-foreground">Failure reason</dt><dd><Failure code={attempt.failureReason} /></dd></div>}
  </dl>;
}

function RecoveryDetails({ prefix, scope, attempt }: {
  prefix: string; scope: readonly string[]; attempt: Recovery;
}) {
  const [open, setOpen] = useState(false);
  const details = useQuery({
    queryKey: [...scope, "recovery", attempt.id],
    queryFn: ({ signal }) => apiFetch<Recovery>(`${prefix}/recoveries/${encodeURIComponent(attempt.id)}`, { signal }),
    enabled: open,
    retry: false,
  });
  const latest = details.data && Date.parse(details.data.updatedAt) >= Date.parse(attempt.updatedAt) ? details.data : attempt;
  return <Dialog open={open} onOpenChange={setOpen}>
    <DialogTrigger asChild>
      <Button variant="ghost" size="sm" aria-label={`View recovery ${attempt.id}`}>Details <ChevronRight aria-hidden="true" /></Button>
    </DialogTrigger>
    <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-xl">
      <DialogHeader>
        <DialogTitle>Recovery details</DialogTitle>
        <DialogDescription className="break-all font-mono text-xs">{attempt.id}</DialogDescription>
      </DialogHeader>
      {details.isPending ? <LoadingRows label="Loading recovery details" /> :
        details.isError ? <QueryProblem message="Recovery details could not be loaded." retry={() => void details.refetch()} /> :
          <><div><Status value={latest.status} /></div><RecoveryFields attempt={latest} /></>}
      <DialogFooter showCloseButton />
    </DialogContent>
  </Dialog>;
}

function readRecoveryKey(key: string) {
  try {
    const value = sessionStorage.getItem(key);
    return value && /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value) ? value : null;
  } catch { return null; }
}

export default function DeploymentHealthPanel({ workspaceId, projectId, deploymentId, canManage }: {
  workspaceId: string; projectId: string; deploymentId: string; canManage: boolean;
}) {
  const queryClient = useQueryClient();
  const scope = ["deployment-health", workspaceId, projectId, deploymentId] as const;
  const healthKey = [...scope, "summary"] as const;
  const recoveriesKey = [...scope, "recoveries"] as const;
  const prefix = `/projects/${encodeURIComponent(projectId)}/deployments/${encodeURIComponent(deploymentId)}`;
  const keyStorage = scope.join(":") + ":pending-recovery";
  const [pendingKey, setPendingKey] = useState(() => readRecoveryKey(keyStorage));
  const [submitted, setSubmitted] = useState<Recovery | null>(null);
  const [open, setOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const sending = useRef(false);

  const health = useQuery({
    queryKey: healthKey,
    queryFn: ({ signal }) => apiFetch<DeploymentHealth>(`${prefix}/health`, { signal }),
    retry: false,
    refetchInterval: ({ state }) => {
      if (scopedError(state.error)) return false;
      if (recentRecovery(queryClient.getQueryData<Recovery[]>(recoveriesKey) ?? [])) return 12_000;
      return state.data?.runtimeStatus === "RUNNING" || state.error ? 30_000 : false;
    },
    refetchIntervalInBackground: false,
  });
  const recoveries = useQuery({
    queryKey: recoveriesKey,
    queryFn: ({ signal }) => apiFetch<Recovery[]>(`${prefix}/recoveries`, { signal }),
    retry: false,
    refetchInterval: ({ state }) => {
      if (scopedError(state.error) || health.data?.recovery.reason === "SCHEMA_UNAVAILABLE") return false;
      const attempts = state.data ?? [];
      if (submitted && !attempts.some((attempt) => attempt.id === submitted.id)) {
        if (isRecoveryActive(submitted)) return recentRecovery([submitted]) ? 5_000 : 30_000;
      }
      if (attempts.some(isRecoveryActive)) return recentRecovery(attempts) ? 5_000 : 30_000;
      return health.data?.recovery.reason === "ACTIVE" ? 30_000 : false;
    },
    refetchIntervalInBackground: false,
  });
  const listed = recoveries.data ?? [];
  const tracked = submitted ? listed.find((attempt) => attempt.id === submitted.id) ?? submitted : null;
  const active = listed.some(isRecoveryActive) || isRecoveryActive(tracked);
  const recoveryRunning = listed.some((attempt) => attempt.status === "RUNNING") || tracked?.status === "RUNNING";
  const capabilities = health.data?.recovery;
  const roleAllowsRecovery = canManage && capabilities?.reason !== "FORBIDDEN";
  const retryable = pendingKey !== null && capabilities !== undefined &&
    capabilities.reason !== "DISABLED" && capabilities.reason !== "SCHEMA_UNAVAILABLE" && capabilities.reason !== "FORBIDDEN";
  const mutation = useMutation({
    mutationFn: async (idempotencyKey: string) => {
      const result = await apiFetch<Recovery>(`${prefix}/recoveries`, {
        method: "POST", headers: { "Idempotency-Key": idempotencyKey }, signal: AbortSignal.timeout(15_000),
      });
      if (!result?.id) throw new Error("Unknown recovery response");
      return result;
    },
  });
  const canSubmit = roleAllowsRecovery && !health.isError && !recoveries.isError && !recoveries.isPending &&
    !mutation.isPending && (pendingKey ? retryable && !recoveryRunning : capabilities?.allowed === true && !active);

  function remember(key: string | null) {
    setPendingKey(key);
    try {
      if (key) sessionStorage.setItem(keyStorage, key);
      else sessionStorage.removeItem(keyStorage);
    } catch { /* The in-memory key still protects retries in this view. */ }
  }

  async function submit() {
    if (!canSubmit || sending.current) return;
    sending.current = true;
    setError(null);
    try {
      const key = pendingKey ?? crypto.randomUUID();
      remember(key);
      const attempt = await mutation.mutateAsync(key);
      setSubmitted(attempt);
      remember(null);
      queryClient.setQueryData<Recovery[]>(recoveriesKey, (previous) =>
        [attempt, ...(previous ?? []).filter((item) => item.id !== attempt.id)].slice(0, 100));
      setOpen(false);
    } catch (cause) {
      const failure = recoverySubmissionError(cause);
      if (!pendingKey && !failure.ambiguous) remember(null);
      setError(failure.message);
      setOpen(false);
    } finally {
      sending.current = false;
      void queryClient.invalidateQueries({ queryKey: scope });
    }
  }

  const summary = health.data;
  const state = summary?.state;
  const checks = summary?.checks ?? [];
  const latestCheck = state ? checks.find((check) => check.generation === state.generation) : undefined;
  const lastChecked = state?.lastCheckedAt ? Date.parse(state.lastCheckedAt) : NaN;
  const healthStale = Number.isFinite(lastChecked) && health.dataUpdatedAt - lastChecked > 300_000;
  const blockedReason = !roleAllowsRecovery ? recoveryBlockMessage("FORBIDDEN") :
    active && (!retryable || recoveryRunning) ? recoveryBlockMessage("ACTIVE") :
      recoveries.isError && capabilities?.allowed ? "Recovery history could not be verified." :
        !capabilities?.allowed && !retryable ? recoveryBlockMessage(capabilities?.reason) : null;

  return <div className="min-w-0 space-y-5">
    <Card>
      <CardHeader className="flex flex-col items-start gap-3 sm:flex-row sm:justify-between">
        <div className="space-y-1.5">
          <CardTitle className="text-base"><h2 className="flex items-center gap-2"><Activity className="size-4 text-blue-600 dark:text-blue-400" aria-hidden="true" />Health overview</h2></CardTitle>
          <CardDescription>Health checks and runtime recovery for this deployment.</CardDescription>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" variant="outline" size="sm" onClick={() => void queryClient.invalidateQueries({ queryKey: scope })} disabled={health.isFetching || recoveries.isFetching}>
            <RefreshCw aria-hidden="true" /> Refresh
          </Button>
          {roleAllowsRecovery && <Dialog open={open} onOpenChange={(next) => { if (!sending.current) { setOpen(next); if (next) setError(null); } }}>
            <DialogTrigger asChild>
              <Button type="button" size="sm" disabled={!canSubmit}><RotateCcw aria-hidden="true" />{pendingKey ? "Retry pending request" : "Restart runtime"}</Button>
            </DialogTrigger>
            <DialogContent showCloseButton={!mutation.isPending}>
              <DialogHeader>
                <DialogTitle>{pendingKey ? "Retry recovery request?" : "Restart this runtime?"}</DialogTitle>
                <DialogDescription>{pendingKey
                  ? "This retries the original recovery request using the same idempotency key."
                  : "Recovery restarts the existing runtime container and may briefly interrupt service. The existing build and deployment stay in place. Backend safety checks and rate limits still apply."}</DialogDescription>
              </DialogHeader>
              <DialogFooter>
                <DialogClose asChild><Button type="button" variant="outline" disabled={mutation.isPending}>Cancel</Button></DialogClose>
                <Button type="button" variant="destructive" disabled={!canSubmit} onClick={() => void submit()}>{mutation.isPending ? "Submitting…" : pendingKey ? "Retry request" : "Confirm restart"}</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {health.isPending ? <LoadingRows label="Loading deployment health" /> : health.isError ?
          <QueryProblem message={scopedError(health.error) ? "This deployment is unavailable in your current workspace." : "Deployment health could not be loaded."} retry={() => void health.refetch()} /> :
          <>
            {!summary?.schemaAvailable ? <p className="rounded-md border border-dashed p-4 text-sm text-muted-foreground">Health data is unavailable because the health schema is missing.</p> : <>
              <dl className="grid grid-cols-2 gap-x-4 gap-y-5 rounded-lg border bg-muted/20 p-4 sm:grid-cols-3 xl:grid-cols-4">
                <Metric name="Health status"><Status value={state?.status ?? "UNKNOWN"} /></Metric>
                <Metric name="Docker state"><span className="capitalize">{latestCheck?.dockerState ?? "Unavailable"}</span></Metric>
                <Metric name="Latest HTTP status">{latestCheck?.httpStatus ?? "Unavailable"}</Metric>
                <Metric name="Response latency">{latestCheck?.responseLatencyMs == null ? "Unavailable" : `${latestCheck.responseLatencyMs} ms`}</Metric>
                <Metric name="Last check">{formatHealthTime(state?.lastCheckedAt)}</Metric>
                <Metric name="Next scheduled check">{formatHealthTime(state?.nextCheckAt)}</Metric>
                <Metric name="Consecutive failures">{state?.consecutiveFailures ?? "Unavailable"}</Metric>
                <Metric name="Health generation">{state?.generation ?? "Unavailable"}</Metric>
              </dl>
              {state?.lastFailureReason && <div className="rounded-md border p-3"><p className="mb-1 text-xs text-muted-foreground">Latest failure</p><Failure code={state.lastFailureReason} /></div>}
              <p className="text-xs text-muted-foreground">{monitoringMessage(summary?.monitoring)}</p>
              {healthStale && <p role="status" className="rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-xs text-amber-800 dark:text-amber-300">The most recent health check is older than expected. Monitoring may be delayed or stale.</p>}
            </>}
            {blockedReason && <p className="border-t pt-3 text-sm text-muted-foreground">{blockedReason}</p>}
          </>}
        {pendingKey && !error && <p role="status" className="rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-sm">A previous recovery request has an unknown outcome. Retrying will reuse its request key.</p>}
        {error && <p role="alert" className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm">{error}</p>}
        {tracked && <div role="status" className="flex flex-wrap items-center gap-2 rounded-md border p-3 text-sm">
          <Status value={tracked.status} />
          <p>{tracked.status === "SUCCEEDED" ? "Runtime recovery succeeded." : tracked.status === "FAILED" ? "Runtime recovery failed." : tracked.status === "CANCELLED" ? "Runtime recovery was cancelled." : "Recovery request accepted."}</p>
          {tracked.failureReason && <div className="basis-full"><Failure code={tracked.failureReason} /></div>}
        </div>}
      </CardContent>
    </Card>

    {summary?.schemaAvailable && !health.isError && <>
      {checks.length > 0 && <LatencyChart checks={checks} />}
      <CheckHistory checks={checks} />
    </>}

    <Card>
      <CardHeader><CardTitle><h2>Recovery history</h2></CardTitle><CardDescription>Recent recovery attempts for this deployment.</CardDescription></CardHeader>
      <CardContent className="space-y-3">
        {recoveries.isPending ? <LoadingRows label="Loading recovery history" /> : recoveries.isError ?
          <QueryProblem message={capabilities?.reason === "SCHEMA_UNAVAILABLE" ? "Recovery history is unavailable until the recovery schema is ready." : "Recovery history could not be loaded."} retry={() => void recoveries.refetch()} /> :
          listed.length === 0 ? <p className="rounded-md border border-dashed p-4 text-sm text-muted-foreground">No recovery attempts have been recorded.</p> :
            <ul className="divide-y rounded-lg border">
              {listed.map((attempt) => <li key={attempt.id} className="min-w-0 p-3 sm:p-4">
                <div className="mb-3 flex flex-wrap items-start justify-between gap-2">
                  <div className="min-w-0 space-y-1.5">
                    <Status value={attempt.status} /><p className="break-all font-mono text-xs text-muted-foreground">{attempt.id}</p>
                  </div>
                  <RecoveryDetails prefix={prefix} scope={scope} attempt={attempt} />
                </div>
                <RecoveryFields attempt={attempt} />
              </li>)}
            </ul>}
      </CardContent>
    </Card>
  </div>;
}
