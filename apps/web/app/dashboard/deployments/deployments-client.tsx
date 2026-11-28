"use client";

// ==================================================
// CloudPilot — Deployments Dashboard
// Phase 5.6 — Realtime Logs Integration
// ==================================================

import {
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";

import Link from "next/link";

import {
  Activity,
  AlertCircle,
  CheckCircle2,
  Clock3,
  Container,
  ExternalLink,
  GitCommitHorizontal,
  Layers3,
  Loader2,
  Package,
  Play,
  RefreshCw,
  Rocket,
  RotateCcw,
  Square,
  Terminal,
  XCircle,
} from "lucide-react";

import { DashboardPageShell } from "@/components/dashboard/page-shell";
import { apiFetch, ApiError } from "@/lib/api";
import { useDashboard } from "@/components/dashboard/dashboard-provider";

// ==================================================
// Type definitions
// ==================================================

type BuildStatus =
  | "QUEUED"
  | "RUNNING"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED";

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

type RuntimeStatus =
  | "QUEUED"
  | "CREATING"
  | "STARTING"
  | "RUNNING"
  | "STOPPED"
  | "FAILED"
  | "CANCELLED";

type DeploymentRuntime = {
  id: string;
  runtimeAttempt: number;
  status: RuntimeStatus;
  containerName: string | null;
  containerPort: number | null;
  hostPort: number | null;
  hostBindingAddress: string | null;
  cpuLimit: number | null;
  memoryLimitMb: number | null;
  pidsLimit: number | null;
  restartCount: number;
  failureCode: string | null;
  failureMessage: string | null;
  applicationUrl: string | null;
} | null;

type Deployment = {
  id: string;
  status: string;
  environment: string;
  triggerSource: string;
  // The workspace list always includes the commit and latest attempt summaries.
  // An unresolved commit or a missing attempt is represented by null.
  commitSha: string | null;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  failureCode: string | null;
  build: DeploymentBuild;
  runtime: DeploymentRuntime;
  project: {
    id: string;
    name: string;
  };
};

const runtimeActions = [
  "start",
  "stop",
  "restart",
] as const;

type RuntimeAction =
  (typeof runtimeActions)[number];

type ActionState = Record<
  string,
  RuntimeAction | undefined
>;

type PendingAction = {
  action: RuntimeAction;
  runtimeId: string;
  previousStatus: RuntimeStatus;
  previousRestartCount: number;
  requestedAt: number;
  timeoutReported: boolean;
};

type PendingActionState = Record<
  string,
  PendingAction | undefined
>;

type DeploymentSnapshot = {
  workspaceId: string;
  items: Deployment[];
  loaded: boolean;
  loading: boolean;
  refreshing: boolean;
  error: string;
  lastUpdatedAt: number | null;
};

type RuntimeActionError = {
  deploymentId: string;
  message: string;
};

// ==================================================
// Constants
// ==================================================

const POLLING_INTERVAL_MS = 5_000;

const IDLE_POLLING_INTERVAL_MS = 30_000;

const REQUEST_TIMEOUT_MS = 15_000;

const ACTION_REQUEST_TIMEOUT_MS = 20_000;

const ACTION_CONFIRMATION_TIMEOUT_MS = 60_000;

const MAX_SNAPSHOT_AGE_MS = 45_000;

const MAX_DEPLOYMENTS = 10_000;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const BUILD_STATUSES = new Set<string>([
  "QUEUED",
  "RUNNING",
  "SUCCEEDED",
  "FAILED",
  "CANCELLED",
]);

const RUNTIME_STATUSES = new Set<string>([
  "QUEUED",
  "CREATING",
  "STARTING",
  "RUNNING",
  "STOPPED",
  "FAILED",
  "CANCELLED",
]);

const ACTIVE_DEPLOYMENT_STATUSES =
  new Set<string>([
    "PENDING",
    "QUEUED",
    "PREPARING",
    "SOURCE_PREPARING",
    "SOURCE_PREPARED",
    "BUILDING",
    "BUILD_SUCCEEDED",
    "DEPLOYING",
    "HEALTH_CHECKING",
    "CANCELLING",
    "ROLLING_BACK",
  ]);

const ACTIVE_RUNTIME_STATUSES =
  new Set<RuntimeStatus>([
    "QUEUED",
    "CREATING",
    "STARTING",
  ]);

const FAILED_STATUSES = new Set([
  "FAILED",
  "BUILD_FAILED",
  "RUNTIME_FAILED",
  "HEALTH_CHECK_FAILED",
]);

const SUCCESS_STATUSES = new Set([
  "SUCCEEDED",
  "RUNNING",
  "BUILD_SUCCEEDED",
  "SOURCE_PREPARED",
  "ROLLED_BACK",
]);

const ACTION_LABELS: Record<
  RuntimeAction,
  string
> = {
  start: "Start",
  stop: "Stop",
  restart: "Restart",
};

const ACTION_ICONS = {
  start: Play,
  stop: Square,
  restart: RotateCcw,
} as const;

const BUTTON_CLASS = [
  "inline-flex min-h-9 items-center",
  "justify-center gap-1.5 rounded-md border",
  "px-3 py-1.5 text-xs font-medium",
  "transition-colors",
  "hover:bg-zinc-100 dark:hover:bg-zinc-800",
  "focus-visible:outline-none",
  "focus-visible:ring-2 focus-visible:ring-blue-500",
  "disabled:cursor-not-allowed disabled:opacity-50",
].join(" ");

// ==================================================
// API response validation
// ==================================================

function isObject(
  value: unknown,
): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

function isUuid(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    value.length === 36 &&
    UUID_PATTERN.test(value)
  );
}

function isText(
  value: unknown,
  maxLength = 4096,
): value is string {
  return (
    typeof value === "string" &&
    value.length <= maxLength &&
    !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/u.test(
      value,
    )
  );
}

function isNullableText(
  value: unknown,
  maxLength = 4096,
): boolean {
  return (
    value === null ||
    isText(value, maxLength)
  );
}

function isStatus(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    /^[A-Z][A-Z0-9_]{0,79}$/.test(value)
  );
}

function isNonNegativeInteger(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 0
  );
}

function isPositiveInteger(
  value: unknown,
): value is number {
  return (
    isNonNegativeInteger(value) &&
    value > 0
  );
}

function isNullableInteger(
  value: unknown,
): boolean {
  return (
    value === null ||
    isNonNegativeInteger(value)
  );
}

function isNullablePositiveNumber(
  value: unknown,
): boolean {
  return (
    value === null ||
    (
      typeof value === "number" &&
      Number.isFinite(value) &&
      value > 0
    )
  );
}

function isNullablePort(
  value: unknown,
): boolean {
  return (
    value === null ||
    (
      isNonNegativeInteger(value) &&
      value <= 65535
    )
  );
}

function isDateString(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    value.length <= 64 &&
    Number.isFinite(Date.parse(value))
  );
}

function isNullableDate(
  value: unknown,
): boolean {
  return (
    value === null ||
    isDateString(value)
  );
}

function isBuild(
  value: unknown,
): value is DeploymentBuild {
  if (value === null) {
    return true;
  }

  if (!isObject(value)) {
    return false;
  }

  return (
    isUuid(value.id) &&
    isPositiveInteger(value.buildAttempt) &&
    typeof value.status === "string" &&
    BUILD_STATUSES.has(value.status) &&
    isNullableText(value.dockerfilePath) &&
    isNullableText(value.imageTag, 1024) &&
    (
      value.imageSizeBytes === null ||
      (
        typeof value.imageSizeBytes === "string" &&
        /^[0-9]{1,30}$/.test(
          value.imageSizeBytes,
        )
      )
    ) &&
    isNullableText(value.failureCode, 128) &&
    isNullableText(value.failureMessage) &&
    isNullableInteger(value.durationMs)
  );
}

function isRuntime(
  value: unknown,
): value is DeploymentRuntime {
  if (value === null) {
    return true;
  }

  if (!isObject(value)) {
    return false;
  }

  return (
    isUuid(value.id) &&
    isPositiveInteger(value.runtimeAttempt) &&
    typeof value.status === "string" &&
    RUNTIME_STATUSES.has(value.status) &&
    isNullableText(value.containerName, 512) &&
    isNullablePort(value.containerPort) &&
    isNullablePort(value.hostPort) &&
    isNullableText(value.hostBindingAddress, 128) &&
    isNullablePositiveNumber(value.cpuLimit) &&
    isNullableInteger(value.memoryLimitMb) &&
    isNullableInteger(value.pidsLimit) &&
    isNonNegativeInteger(value.restartCount) &&
    isNullableText(value.failureCode, 128) &&
    isNullableText(value.failureMessage) &&
    isNullableText(value.applicationUrl, 2048)
  );
}

function isDeployment(
  value: unknown,
): value is Deployment {
  if (!isObject(value)) {
    return false;
  }

  const project = value.project;

  return (
    isUuid(value.id) &&
    isStatus(value.status) &&
    isText(value.environment, 128) &&
    isText(value.triggerSource, 128) &&
    (
      value.commitSha === null ||
      (
        typeof value.commitSha === "string" &&
        /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/i.test(
          value.commitSha,
        )
      )
    ) &&
    isDateString(value.createdAt) &&
    isNullableDate(value.startedAt) &&
    isNullableDate(value.finishedAt) &&
    isNullableText(value.failureCode, 128) &&
    isBuild(value.build) &&
    isRuntime(value.runtime) &&
    isObject(project) &&
    isUuid(project.id) &&
    isText(project.name, 512) &&
    project.name.trim().length > 0
  );
}

function validateDeployments(
  value: unknown,
): Deployment[] {
  if (
    !Array.isArray(value) ||
    value.length > MAX_DEPLOYMENTS
  ) {
    throw new Error(
      "Invalid deployment response.",
    );
  }

  const ids = new Set<string>();

  const deployments: Deployment[] = [];

  for (const item of value) {
    if (!isDeployment(item)) {
      throw new Error(
        "Invalid deployment record.",
      );
    }

    const id = item.id.toLowerCase();

    if (ids.has(id)) {
      throw new Error(
        "Duplicate deployment identity.",
      );
    }

    ids.add(id);

    deployments.push(item);
  }

  return deployments;
}

// ==================================================
// Formatting helpers
// ==================================================

function formatDuration(
  ms: number | null,
): string {
  if (
    ms === null ||
    !Number.isFinite(ms) ||
    ms < 0
  ) {
    return "—";
  }

  if (ms < 1000) {
    return `${Math.round(ms)} ms`;
  }

  const seconds = Math.round(ms / 1000);

  if (seconds < 60) {
    return `${seconds} sec`;
  }

  const minutes = Math.floor(seconds / 60);

  const remainingSeconds = seconds % 60;

  if (minutes < 60) {
    return `${minutes}m ${remainingSeconds}s`;
  }

  const hours = Math.floor(minutes / 60);

  return `${hours}h ${minutes % 60}m`;
}

function formatDeploymentDuration(
  item: Deployment,
): string {
  if (item.build?.durationMs != null) {
    return formatDuration(
      item.build.durationMs,
    );
  }

  if (
    !item.startedAt ||
    !item.finishedAt
  ) {
    return "—";
  }

  return formatDuration(
    Date.parse(item.finishedAt) -
      Date.parse(item.startedAt),
  );
}

function formatImageSize(
  value: string | null,
): string {
  if (
    value === null ||
    !/^[0-9]{1,30}$/.test(value)
  ) {
    return "—";
  }

  try {
    const bytes = BigInt(value);

    const units = [
      "B",
      "KiB",
      "MiB",
      "GiB",
      "TiB",
      "PiB",
    ];

    let divisor = BigInt(1);

    let unitIndex = 0;

    while (
      unitIndex < units.length - 1 &&
      bytes >= divisor * BigInt(1024)
    ) {
      divisor *= BigInt(1024);

      unitIndex += 1;
    }

    if (unitIndex === 0) {
      return `${bytes.toString()} B`;
    }

    const rounded =
      (
        bytes * BigInt(10) +
        divisor / BigInt(2)
      ) / divisor;

    return (
      `${rounded / BigInt(10)}.` +
      `${rounded % BigInt(10)} ` +
      units[unitIndex]
    );
  } catch {
    return "—";
  }
}

function formatDate(
  value: string | null,
): string {
  if (!value) {
    return "—";
  }

  const timestamp = Date.parse(value);

  if (!Number.isFinite(timestamp)) {
    return "—";
  }

  return new Intl.DateTimeFormat(
    undefined,
    {
      dateStyle: "medium",
      timeStyle: "short",
    },
  ).format(timestamp);
}

function formatLastUpdated(
  timestamp: number | null,
): string {
  if (
    timestamp === null ||
    !Number.isFinite(timestamp)
  ) {
    return "Not refreshed yet";
  }

  return new Intl.DateTimeFormat(
    undefined,
    {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    },
  ).format(timestamp);
}

function buildStatusLabel(
  status: string,
): string {
  return status
    .toLowerCase()
    .replaceAll("_", " ")
    .replace(
      /\b\w/g,
      (character) =>
        character.toUpperCase(),
    );
}

function getErrorMessage(
  cause: unknown,
  fallback: string,
): string {
  if (cause instanceof ApiError) {
    switch (cause.status) {
      case 400:
      case 422:
        return "The request was rejected as invalid.";

      case 401:
        return "Your session has expired. Please sign in again.";

      case 403:
        return "You do not have permission to perform this operation.";

      case 404:
        return "The requested deployment is no longer available.";

      case 409:
        return "The deployment state has changed. Refresh and try again.";

      case 429:
        return "Too many requests. Please wait before trying again.";

      default:
        return fallback;
    }
  }

  return fallback;
}

function isPermissionError(
  cause: unknown,
): boolean {
  return (
    cause instanceof ApiError &&
    (
      cause.status === 401 ||
      cause.status === 403
    )
  );
}

// ==================================================
// Runtime lifecycle helpers
// ==================================================

function getAllowedRuntimeActions(
  runtime: DeploymentRuntime,
): readonly RuntimeAction[] {
  if (!runtime) {
    return [];
  }

  switch (runtime.status) {
    case "RUNNING":
      return ["stop", "restart"];

    case "STOPPED":
      return ["start"];

    default:
      return [];
  }
}

function isRuntimeActionAllowed(
  runtime: DeploymentRuntime,
  action: RuntimeAction,
): boolean {
  return getAllowedRuntimeActions(
    runtime,
  ).includes(action);
}

function isDeploymentActive(
  item: Deployment,
): boolean {
  if (
    item.status === "BUILD_SUCCEEDED" &&
    !item.runtime
  ) {
    return false;
  }

  return (
    ACTIVE_DEPLOYMENT_STATUSES.has(
      item.status,
    ) ||
    item.build?.status === "QUEUED" ||
    item.build?.status === "RUNNING" ||
    (
      item.runtime !== null &&
      ACTIVE_RUNTIME_STATUSES.has(
        item.runtime.status,
      )
    )
  );
}

function isDeploymentFailed(
  item: Deployment,
): boolean {
  return (
    FAILED_STATUSES.has(item.status) ||
    item.build?.status === "FAILED" ||
    item.runtime?.status === "FAILED"
  );
}

function isActionConfirmed(
  item: Deployment | undefined,
  pending: PendingAction,
): boolean {
  if (
    !item?.runtime ||
    item.runtime.id !== pending.runtimeId
  ) {
    return false;
  }

  const runtime = item.runtime;

  if (
    runtime.status === "FAILED" ||
    runtime.status === "CANCELLED"
  ) {
    return true;
  }

  switch (pending.action) {
    case "start":
      return runtime.status === "RUNNING";

    case "stop":
      return runtime.status === "STOPPED";

    case "restart":
      return (
        runtime.status === "RUNNING" &&
        runtime.restartCount >
          pending.previousRestartCount
      );

    default:
      return false;
  }
}

// ==================================================
// Application URL security
// ==================================================

/**
 * CloudPilot currently supports trusted local
 * runtime execution with numeric loopback binding.
 *
 * Arbitrary API-provided URLs must not be rendered
 * as application links.
 */
function getSafeApplicationUrl(
  runtime: DeploymentRuntime,
): string | null {
  if (
    !runtime ||
    runtime.status !== "RUNNING" ||
    !runtime.applicationUrl ||
    runtime.applicationUrl.length > 2048
  ) {
    return null;
  }

  const rawUrl = runtime.applicationUrl;

  if (
    /[\u0000-\u0020\u007f-\u009f\\]/u.test(
      rawUrl,
    )
  ) {
    return null;
  }

  const port = runtime.hostPort;

  if (
    port === null ||
    !Number.isSafeInteger(port) ||
    port < 1 ||
    port > 65535
  ) {
    return null;
  }

  const binding =
    runtime.hostBindingAddress;

  if (
    binding !== "127.0.0.1" &&
    binding !== "::1"
  ) {
    return null;
  }

  const hostname =
    binding === "::1"
      ? "[::1]"
      : binding;

  const expectedAuthority =
    `http://${hostname}:${port}`;

  // Reject alternate host representations before
  // browser URL normalization.
  if (
    rawUrl !== expectedAuthority &&
    !rawUrl.startsWith(
      `${expectedAuthority}/`,
    )
  ) {
    return null;
  }

  try {
    const parsed = new URL(rawUrl);

    if (
      parsed.protocol !== "http:" ||
      parsed.hostname !== hostname ||
      parsed.username !== "" ||
      parsed.password !== "" ||
      parsed.hash !== ""
    ) {
      return null;
    }

    const actualPort =
      parsed.port === ""
        ? 80
        : Number(parsed.port);

    if (actualPort !== port) {
      return null;
    }

    return parsed.href;
  } catch {
    return null;
  }
}

// ==================================================
// Logs navigation
// ==================================================

function ViewLogsLink({
  item,
}: {
  item: Deployment;
}) {
  const query = new URLSearchParams({
    deployment: item.id,
    project: item.project.id,
  });

  return (
    <Link
      href={`/dashboard/logs?${query.toString()}`}
      prefetch={false}
      aria-label={`View logs for ${item.project.name}`}
      className={`${BUTTON_CLASS} w-fit max-w-full`}
    >
      <Terminal
        aria-hidden="true"
        className="size-3.5 shrink-0"
      />

      View Logs
    </Link>
  );
}

// ==================================================
// Status presentation
// ==================================================

function getStatusClasses(
  status: string,
): string {
  if (FAILED_STATUSES.has(status)) {
    return [
      "border-red-200 bg-red-50 text-red-700",
      "dark:border-red-900/60",
      "dark:bg-red-950/40 dark:text-red-400",
    ].join(" ");
  }

  if (SUCCESS_STATUSES.has(status)) {
    return [
      "border-emerald-200 bg-emerald-50",
      "text-emerald-700",
      "dark:border-emerald-900/60",
      "dark:bg-emerald-950/40",
      "dark:text-emerald-400",
    ].join(" ");
  }

  if (
    [
      "BUILDING",
      "DEPLOYING",
      "STARTING",
      "CREATING",
      "HEALTH_CHECKING",
      "SOURCE_PREPARING",
    ].includes(status)
  ) {
    return [
      "border-blue-200 bg-blue-50 text-blue-700",
      "dark:border-blue-900/60",
      "dark:bg-blue-950/40 dark:text-blue-400",
    ].join(" ");
  }

  if (
    [
      "QUEUED",
      "PENDING",
      "CANCELLING",
    ].includes(status)
  ) {
    return [
      "border-amber-200 bg-amber-50",
      "text-amber-700",
      "dark:border-amber-900/60",
      "dark:bg-amber-950/40",
      "dark:text-amber-400",
    ].join(" ");
  }

  return [
    "border-zinc-200 bg-zinc-100",
    "text-zinc-700",
    "dark:border-zinc-800",
    "dark:bg-zinc-900 dark:text-zinc-300",
  ].join(" ");
}

function StatusBadge({
  status,
}: {
  status: string;
}) {
  return (
    <span
      className={[
        "inline-flex max-w-full",
        "items-center gap-1.5",
        "rounded-full border",
        "px-2.5 py-1",
        "text-[11px] font-medium",
        getStatusClasses(status),
      ].join(" ")}
    >
      <span
        aria-hidden="true"
        className="size-1.5 shrink-0 rounded-full bg-current"
      />

      <span className="min-w-0 break-words">
        {buildStatusLabel(status)}
      </span>
    </span>
  );
}

// ==================================================
// Summary cards
// ==================================================

function DeploymentSummary({
  items,
}: {
  items: Deployment[];
}) {
  const statistics = [
    {
      label: "Total deployments",
      value: items.length,
      icon: Layers3,
      color:
        "text-zinc-600 dark:text-zinc-300",
      background:
        "bg-zinc-100 dark:bg-zinc-800",
    },
    {
      label: "Running applications",
      value: items.filter(
        (item) =>
          item.runtime?.status === "RUNNING",
      ).length,
      icon: CheckCircle2,
      color:
        "text-emerald-600 dark:text-emerald-400",
      background:
        "bg-emerald-50 dark:bg-emerald-950/40",
    },
    {
      label: "In progress",
      value: items.filter(
        isDeploymentActive,
      ).length,
      icon: Activity,
      color:
        "text-blue-600 dark:text-blue-400",
      background:
        "bg-blue-50 dark:bg-blue-950/40",
    },
    {
      label: "Failed deployments",
      value: items.filter(
        isDeploymentFailed,
      ).length,
      icon: XCircle,
      color:
        "text-red-600 dark:text-red-400",
      background:
        "bg-red-50 dark:bg-red-950/40",
    },
  ];

  return (
    <div className="grid min-w-0 grid-cols-2 gap-3 xl:grid-cols-4">
      {statistics.map((statistic) => {
        const Icon = statistic.icon;

        return (
          <div
            key={statistic.label}
            className="min-w-0 rounded-xl border bg-white p-3 shadow-sm dark:bg-zinc-950 sm:p-4"
          >
            <div className="flex flex-wrap items-start justify-between gap-2">
              <p className="min-w-0 break-words text-xs font-medium text-zinc-500 dark:text-zinc-400">
                {statistic.label}
              </p>

              <div
                className={[
                  "flex size-8 shrink-0",
                  "items-center justify-center",
                  "rounded-lg",
                  statistic.background,
                  statistic.color,
                ].join(" ")}
              >
                <Icon
                  aria-hidden="true"
                  className="size-4"
                />
              </div>
            </div>

            <p className="mt-2 text-2xl font-semibold tracking-tight tabular-nums sm:text-3xl">
              {statistic.value}
            </p>
          </div>
        );
      })}
    </div>
  );
}

// ==================================================
// Runtime controls
// ==================================================

interface RuntimeControlsProps {
  item: Deployment;
  acting: RuntimeAction | undefined;
  pending: PendingAction | undefined;
  controlsDisabled: boolean;
  onAction: (
    deploymentId: string,
    action: RuntimeAction,
  ) => void;
}

function RuntimeControls({
  item,
  acting,
  pending,
  controlsDisabled,
  onAction,
}: RuntimeControlsProps) {
  const runtime = item.runtime;

  if (!runtime) {
    return (
      <span className="text-xs text-zinc-400">
        —
      </span>
    );
  }

  const applicationUrl =
    controlsDisabled
      ? null
      : getSafeApplicationUrl(runtime);

  const allowedActions =
    getAllowedRuntimeActions(runtime);

  const busy = Boolean(
    acting || pending,
  );

  return (
    <div className="flex min-w-0 flex-col items-start gap-2">
      <StatusBadge
        status={runtime.status}
      />

      {runtime.hostPort !== null &&
        runtime.hostBindingAddress && (
          <span className="max-w-full break-all font-mono text-[11px] text-zinc-500 dark:text-zinc-400">
            {runtime.hostBindingAddress ===
            "::1"
              ? "[::1]"
              : runtime.hostBindingAddress}
            :{runtime.hostPort}
          </span>
        )}

      {applicationUrl && (
        <a
          href={applicationUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex min-h-8 max-w-full items-center gap-1.5 rounded-md text-xs font-medium text-blue-600 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:text-blue-400"
        >
          Open application

          <ExternalLink
            aria-hidden="true"
            className="size-3 shrink-0"
          />
        </a>
      )}

      {runtime.failureCode && (
        <span className="max-w-full break-all text-[11px] font-medium text-red-600 dark:text-red-400">
          {buildStatusLabel(
            runtime.failureCode,
          )}
        </span>
      )}

      {runtime.failureMessage && (
        <p className="max-w-full break-words text-xs leading-relaxed text-red-600 dark:text-red-400">
          Runtime execution reported an
          error. Check deployment logs for
          details.
        </p>
      )}

      {runtime.restartCount > 0 && (
        <span className="text-[11px] text-zinc-500 dark:text-zinc-400">
          Restarts: {runtime.restartCount}
        </span>
      )}

      {(allowedActions.length > 0 ||
        busy) && (
        <div
          className="flex w-full min-w-0 flex-wrap gap-1.5"
          aria-label={`Runtime controls for ${item.project.name}`}
        >
          {runtimeActions.map((action) => {
            const allowed =
              allowedActions.includes(
                action,
              );

            const isActing =
              acting === action;

            const isPending =
              pending?.action === action;

            if (
              !allowed &&
              !isActing &&
              !isPending
            ) {
              return null;
            }

            const Icon =
              ACTION_ICONS[action];

            return (
              <button
                key={action}
                type="button"
                disabled={
                  controlsDisabled ||
                  busy ||
                  !allowed
                }
                aria-label={`${ACTION_LABELS[action]} ${item.project.name}`}
                aria-busy={
                  isActing || isPending
                }
                onClick={() =>
                  onAction(
                    item.id,
                    action,
                  )
                }
                className={[
                  BUTTON_CLASS,
                  action === "stop"
                    ? "border-red-200 text-red-600 dark:border-red-900/60 dark:text-red-400"
                    : "",
                ].join(" ")}
              >
                {isActing || isPending ? (
                  <Loader2
                    aria-hidden="true"
                    className="size-3 shrink-0 animate-spin motion-reduce:animate-none"
                  />
                ) : (
                  <Icon
                    aria-hidden="true"
                    className="size-3 shrink-0"
                  />
                )}

                {isActing
                  ? "Sending..."
                  : isPending
                    ? "Pending..."
                    : ACTION_LABELS[action]}
              </button>
            );
          })}
        </div>
      )}

      {pending && (
        <p
          role="status"
          className="text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400"
        >
          {pending.timeoutReported
            ? "Confirmation delayed. Controls remain locked to prevent duplicate operations."
            : "Waiting for the worker to confirm the runtime action."}
        </p>
      )}

      {controlsDisabled &&
        !busy &&
        allowedActions.length > 0 && (
          <p className="text-[11px] text-amber-600 dark:text-amber-400">
            Refresh deployment data before
            performing runtime actions.
          </p>
        )}
    </div>
  );
}

// ==================================================
// Build information
// ==================================================

function BuildInformation({
  build,
}: {
  build: DeploymentBuild;
}) {
  if (!build) {
    return (
      <span className="text-zinc-400">
        —
      </span>
    );
  }

  return (
    <div className="flex min-w-0 flex-col items-start gap-1.5">
      <StatusBadge
        status={build.status}
      />

      <span className="text-[11px] text-zinc-500 dark:text-zinc-400">
        Attempt #{build.buildAttempt}
      </span>

      {build.dockerfilePath && (
        <span
          title={build.dockerfilePath}
          className="max-w-full break-all font-mono text-[10px] text-zinc-500 dark:text-zinc-400"
        >
          {build.dockerfilePath}
        </span>
      )}

      {build.failureCode && (
        <span className="max-w-full break-all text-[11px] text-red-600 dark:text-red-400">
          {buildStatusLabel(
            build.failureCode,
          )}
        </span>
      )}
    </div>
  );
}

function ImageInformation({
  build,
}: {
  build: DeploymentBuild;
}) {
  if (!build) {
    return (
      <span className="text-zinc-400">
        —
      </span>
    );
  }

  const imageSize = formatImageSize(
    build.imageSizeBytes,
  );

  return (
    <div className="flex min-w-0 flex-col gap-1">
      {build.imageTag && (
        <>
          <span className="inline-flex flex-wrap items-center gap-1.5 font-medium">
            <Package
              aria-hidden="true"
              className="size-3.5 shrink-0 text-zinc-500"
            />

            {imageSize === "—"
              ? "Image built"
              : imageSize}
          </span>

          <span
            title={build.imageTag}
            className="max-w-full break-all font-mono text-[10px] text-zinc-500 dark:text-zinc-400"
          >
            {build.imageTag}
          </span>
        </>
      )}

      {build.failureMessage && (
        <span className="break-words text-xs leading-relaxed text-red-600 dark:text-red-400">
          Build execution reported an
          error. Check deployment logs.
        </span>
      )}

      {!build.imageTag &&
        !build.failureMessage && (
          <span className="text-zinc-400">
            —
          </span>
        )}
    </div>
  );
}

// ==================================================
// Commit information
// ==================================================

function CommitInformation({
  commitSha,
}: {
  commitSha: string | null;
}) {
  return (
    <span className="inline-flex min-w-0 items-center gap-1.5">
      <GitCommitHorizontal
        aria-hidden="true"
        className="size-3.5 shrink-0 text-zinc-500"
      />

      <span
        title={commitSha ?? undefined}
        className="min-w-0 break-all font-mono"
      >
        {commitSha
          ? commitSha.slice(0, 7)
          : "—"}
      </span>
    </span>
  );
}

// ==================================================
// Runtime resource information
// ==================================================

function RuntimeResources({
  runtime,
}: {
  runtime: DeploymentRuntime;
}) {
  if (!runtime) {
    return null;
  }

  return (
    <div className="grid min-w-0 grid-cols-3 gap-2 rounded-lg bg-zinc-50 p-3 dark:bg-zinc-900">
      <div className="min-w-0">
        <p className="text-[10px] text-zinc-500 dark:text-zinc-400">
          CPU
        </p>

        <p className="mt-1 break-words text-xs font-medium">
          {runtime.cpuLimit !== null
            ? `${runtime.cpuLimit} CPU`
            : "—"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[10px] text-zinc-500 dark:text-zinc-400">
          Memory
        </p>

        <p className="mt-1 break-words text-xs font-medium">
          {runtime.memoryLimitMb !== null
            ? `${runtime.memoryLimitMb} MB`
            : "—"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[10px] text-zinc-500 dark:text-zinc-400">
          PID limit
        </p>

        <p className="mt-1 break-words text-xs font-medium">
          {runtime.pidsLimit ?? "—"}
        </p>
      </div>
    </div>
  );
}

// ==================================================
// Mobile deployment card
// ==================================================

function MobileDeploymentCard({
  item,
  acting,
  pending,
  controlsDisabled,
  onAction,
}: RuntimeControlsProps) {
  return (
    <article className="min-w-0 overflow-hidden rounded-xl border bg-white p-3 shadow-sm dark:bg-zinc-950 sm:p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex min-w-0 flex-1 items-start gap-3">
          <div className="flex size-10 shrink-0 items-center justify-center rounded-lg border bg-zinc-50 dark:bg-zinc-900">
            <Rocket
              aria-hidden="true"
              className="size-4 text-zinc-600 dark:text-zinc-300"
            />
          </div>

          <div className="min-w-0">
            <h3 className="break-all text-sm font-semibold">
              {item.project.name}
            </h3>

            <p className="mt-1 break-words text-[11px] text-zinc-500 dark:text-zinc-400">
              {buildStatusLabel(
                item.environment,
              )}

              {" · "}

              {buildStatusLabel(
                item.triggerSource,
              )}
            </p>
          </div>
        </div>

        <StatusBadge
          status={item.status}
        />
      </div>

      <div className="mt-3">
        <ViewLogsLink item={item} />
      </div>

      {item.failureCode && (
        <div className="mt-3 break-all rounded-md border border-red-200 bg-red-50 p-2 text-xs text-red-700 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-400">
          {buildStatusLabel(
            item.failureCode,
          )}
        </div>
      )}

      <div className="mt-4 grid min-w-0 grid-cols-2 gap-3 border-y py-4 sm:gap-4">
        <div className="min-w-0">
          <p className="mb-2 text-[11px] font-medium text-zinc-500 dark:text-zinc-400">
            Build
          </p>

          <BuildInformation
            build={item.build}
          />
        </div>

        <div className="min-w-0">
          <p className="mb-2 text-[11px] font-medium text-zinc-500 dark:text-zinc-400">
            Image
          </p>

          <ImageInformation
            build={item.build}
          />
        </div>

        <div className="min-w-0">
          <p className="mb-1 text-[11px] font-medium text-zinc-500 dark:text-zinc-400">
            Commit
          </p>

          <div className="text-xs">
            <CommitInformation
              commitSha={item.commitSha}
            />
          </div>
        </div>

        <div className="min-w-0">
          <p className="mb-1 text-[11px] font-medium text-zinc-500 dark:text-zinc-400">
            Duration
          </p>

          <div className="flex items-center gap-1.5 text-xs">
            <Clock3
              aria-hidden="true"
              className="size-3.5 shrink-0 text-zinc-500"
            />

            {formatDeploymentDuration(
              item,
            )}
          </div>
        </div>
      </div>

      <div className="mt-4">
        <p className="mb-2 flex items-center gap-1.5 text-xs font-medium text-zinc-500 dark:text-zinc-400">
          <Container
            aria-hidden="true"
            className="size-3.5"
          />

          Runtime
        </p>

        <RuntimeControls
          item={item}
          acting={acting}
          pending={pending}
          controlsDisabled={
            controlsDisabled
          }
          onAction={onAction}
        />
      </div>

      {item.runtime && (
        <div className="mt-4">
          <RuntimeResources
            runtime={item.runtime}
          />
        </div>
      )}

      <p className="mt-4 border-t pt-3 text-[11px] text-zinc-500 dark:text-zinc-400">
        Created {formatDate(item.createdAt)}
      </p>
    </article>
  );
}

// ==================================================
// Desktop deployments table
// ==================================================

interface DeploymentTableProps {
  items: Deployment[];
  acting: ActionState;
  pending: PendingActionState;
  controlsDisabled: boolean;
  onAction: (
    deploymentId: string,
    action: RuntimeAction,
  ) => void;
}

function DeploymentTable({
  items,
  acting,
  pending,
  controlsDisabled,
  onAction,
}: DeploymentTableProps) {
  const headers = [
    "Project",
    "Status",
    "Build",
    "Runtime",
    "Commit",
    "Image",
    "Duration",
  ];

  return (
    <div className="hidden min-w-0 overflow-hidden rounded-xl border bg-white shadow-sm dark:bg-zinc-950 xl:block">
      <div className="max-w-full overflow-x-auto">
        <table className="w-full min-w-[1000px] table-fixed text-left text-xs">
          <caption className="sr-only">
            Deployment records and
            runtime controls
          </caption>

          <colgroup>
            <col className="w-[16%]" />
            <col className="w-[12%]" />
            <col className="w-[12%]" />
            <col className="w-[23%]" />
            <col className="w-[9%]" />
            <col className="w-[19%]" />
            <col className="w-[9%]" />
          </colgroup>

          <thead className="border-b bg-zinc-50/80 text-zinc-500 dark:bg-zinc-900/60 dark:text-zinc-400">
            <tr>
              {headers.map((header) => (
                <th
                  key={header}
                  scope="col"
                  className="px-3 py-3.5 font-medium"
                >
                  {header}
                </th>
              ))}
            </tr>
          </thead>

          <tbody className="divide-y">
            {items.map((item) => (
              <tr
                key={item.id}
                className="align-top transition-colors hover:bg-zinc-50/60 dark:hover:bg-zinc-900/40"
              >
                <td className="px-3 py-4">
                  <div className="flex min-w-0 flex-col gap-1.5">
                    <span className="break-all font-semibold">
                      {item.project.name}
                    </span>

                    <span className="break-words text-[11px] text-zinc-500 dark:text-zinc-400">
                      {buildStatusLabel(
                        item.environment,
                      )}
                    </span>

                    <span className="break-words text-[10px] text-zinc-500 dark:text-zinc-400">
                      {formatDate(
                        item.createdAt,
                      )}
                    </span>

                    <ViewLogsLink
                      item={item}
                    />
                  </div>
                </td>

                <td className="px-3 py-4">
                  <div className="flex min-w-0 flex-col items-start gap-2">
                    <StatusBadge
                      status={item.status}
                    />

                    {item.failureCode && (
                      <span className="max-w-full break-all text-[11px] text-red-600 dark:text-red-400">
                        {buildStatusLabel(
                          item.failureCode,
                        )}
                      </span>
                    )}
                  </div>
                </td>

                <td className="px-3 py-4">
                  <BuildInformation
                    build={item.build}
                  />
                </td>

                <td className="px-3 py-4">
                  <RuntimeControls
                    item={item}
                    acting={
                      acting[item.id]
                    }
                    pending={
                      pending[item.id]
                    }
                    controlsDisabled={
                      controlsDisabled
                    }
                    onAction={onAction}
                  />
                </td>

                <td className="px-3 py-4">
                  <CommitInformation
                    commitSha={
                      item.commitSha
                    }
                  />
                </td>

                <td className="px-3 py-4">
                  <ImageInformation
                    build={item.build}
                  />
                </td>

                <td className="px-3 py-4">
                  <span className="tabular-nums">
                    {formatDeploymentDuration(
                      item,
                    )}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ==================================================
// Loading state
// ==================================================

function DeploymentSkeleton() {
  return (
    <div
      role="status"
      aria-label="Loading deployments"
      className="space-y-4"
    >
      <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">
        {Array.from(
          { length: 4 },
          (_, index) => (
            <div
              key={index}
              className="h-24 animate-pulse rounded-xl border bg-zinc-50 motion-reduce:animate-none dark:bg-zinc-900"
            />
          ),
        )}
      </div>

      <div className="space-y-3 xl:hidden">
        {Array.from(
          { length: 3 },
          (_, index) => (
            <div
              key={index}
              className="h-48 animate-pulse rounded-xl border bg-zinc-50 motion-reduce:animate-none dark:bg-zinc-900"
            />
          ),
        )}
      </div>

      <div className="hidden h-64 animate-pulse rounded-xl border bg-zinc-50 motion-reduce:animate-none dark:bg-zinc-900 xl:block" />

      <span className="sr-only">
        Loading deployment records...
      </span>
    </div>
  );
}

// ==================================================
// Empty state
// ==================================================

function EmptyDeployments() {
  return (
    <div className="rounded-xl border border-dashed px-5 py-14 text-center">
      <div className="mx-auto flex size-12 items-center justify-center rounded-xl bg-zinc-100 dark:bg-zinc-900">
        <Rocket
          aria-hidden="true"
          className="size-6 text-zinc-500"
        />
      </div>

      <h3 className="mt-4 text-sm font-semibold">
        No deployment records
      </h3>

      <p className="mx-auto mt-2 max-w-sm text-xs leading-relaxed text-zinc-500 dark:text-zinc-400">
        This workspace has no deployments
        yet. Deployment records will appear
        here when a project deployment is
        created.
      </p>
    </div>
  );
}

// ==================================================
// Workspace-scoped deployment page
// ==================================================

function WorkspaceDeployments({
  workspaceId,
}: {
  workspaceId: string;
}) {
  const [snapshot, setSnapshot] =
    useState<DeploymentSnapshot>({
      workspaceId,
      items: [],
      loaded: false,
      loading: true,
      refreshing: false,
      error: "",
      lastUpdatedAt: null,
    });

  const [acting, setActing] =
    useState<ActionState>({});

  const [pending, setPending] =
    useState<PendingActionState>({});

  const [
    actionErrors,
    setActionErrors,
  ] = useState<RuntimeActionError[]>(
    [],
  );

  const [
    snapshotStale,
    setSnapshotStale,
  ] = useState(true);

  // ================================================
  // Request and action references
  // ================================================

  const mountedRef = useRef(false);

  const generationRef = useRef(0);

  const requestIdRef = useRef(0);

  const itemsRef =
    useRef<Deployment[]>([]);

  const loadedRef = useRef(false);

  const lastUpdatedRef =
    useRef<number | null>(null);

  const staleRef = useRef(true);

  const permissionDeniedRef =
    useRef(false);

  const requestRef =
    useRef<Promise<void> | null>(null);

  const requestControllerRef =
    useRef<AbortController | null>(null);

  const actionControllersRef = useRef(
    new Map<string, AbortController>(),
  );

  const actionLocksRef = useRef(
    new Set<string>(),
  );

  const pendingRef =
    useRef<PendingActionState>({});

  // ================================================
  // Action error management
  // ================================================

  const reportActionError =
    useCallback(
      (
        deploymentId: string,
        message: string,
      ) => {
        if (!mountedRef.current) {
          return;
        }

        setActionErrors((previous) => [
          ...previous.filter(
            (error) =>
              error.deploymentId !==
              deploymentId,
          ),
          {
            deploymentId,
            message,
          },
        ]);
      },
      [],
    );

  const clearActionError =
    useCallback(
      (deploymentId: string) => {
        setActionErrors((previous) =>
          previous.filter(
            (error) =>
              error.deploymentId !==
              deploymentId,
          ),
        );
      },
      [],
    );

  // ================================================
  // Permission failure handling
  // ================================================

  const invalidateAccess =
    useCallback(
      (message: string) => {
        permissionDeniedRef.current = true;

        staleRef.current = true;

        itemsRef.current = [];

        loadedRef.current = false;

        lastUpdatedRef.current = null;

        // Keep unresolved operation identities
        // internally until access is restored.
        // Never retry mutations automatically.

        setSnapshotStale(true);

        setActionErrors([]);

        setSnapshot({
          workspaceId,
          items: [],
          loaded: false,
          loading: false,
          refreshing: false,
          error: message,
          lastUpdatedAt: null,
        });
      },
      [workspaceId],
    );

  // ================================================
  // Reconcile accepted runtime actions
  // ================================================

  const reconcilePendingActions =
    useCallback(
      (nextItems: Deployment[]) => {
        const nextPending = {
          ...pendingRef.current,
        };

        const deploymentMap = new Map(
          nextItems.map((item) => [
            item.id,
            item,
          ]),
        );

        let changed = false;

        for (const [
          deploymentId,
          operation,
        ] of Object.entries(
          nextPending,
        )) {
          if (!operation) {
            continue;
          }

          const item =
            deploymentMap.get(
              deploymentId,
            );

          // A different runtime identity is not
          // proof of successful execution.
          if (
            item?.runtime &&
            item.runtime.id !==
              operation.runtimeId
          ) {
            delete nextPending[
              deploymentId
            ];

            changed = true;

            reportActionError(
              deploymentId,
              "The runtime instance changed before the previous action was confirmed. Review the new runtime state.",
            );

            continue;
          }

          if (
            !isActionConfirmed(
              item,
              operation,
            )
          ) {
            continue;
          }

          delete nextPending[
            deploymentId
          ];

          changed = true;

          if (
            item?.runtime?.status ===
              "FAILED" ||
            item?.runtime?.status ===
              "CANCELLED"
          ) {
            reportActionError(
              deploymentId,
              "The runtime action did not complete successfully. Check deployment logs.",
            );
          } else {
            clearActionError(
              deploymentId,
            );
          }
        }

        if (changed) {
          pendingRef.current =
            nextPending;

          setPending(nextPending);
        }
      },
      [
        reportActionError,
        clearActionError,
      ],
    );

  // ================================================
  // Workspace-safe deployment fetching
  // ================================================

  const load = useCallback(
    async (): Promise<void> => {
      if (!mountedRef.current) {
        return;
      }

      // Polling and manual refresh share
      // the same in-flight request.
      if (requestRef.current) {
        return requestRef.current;
      }

      const generation =
        generationRef.current;

      const requestId =
        ++requestIdRef.current;

      const controller =
        new AbortController();

      requestControllerRef.current =
        controller;

      const isCurrentRequest =
        (): boolean =>
          mountedRef.current &&
          generationRef.current ===
            generation &&
          requestIdRef.current ===
            requestId;

      const timeout = window.setTimeout(
        () => {
          controller.abort();
        },
        REQUEST_TIMEOUT_MS,
      );

      setSnapshot((previous) => ({
        ...previous,
        loading: !loadedRef.current,
        refreshing: loadedRef.current,
      }));

      const task = (async () => {
        try {
          const response =
            await apiFetch<unknown>(
              "/deployments",
              {
                signal: controller.signal,
              },
            );

          if (!isCurrentRequest()) {
            return;
          }

          const deployments =
            validateDeployments(response);

          const updatedAt = Date.now();

          itemsRef.current =
            deployments;

          loadedRef.current = true;

          staleRef.current = false;

          permissionDeniedRef.current =
            false;

          lastUpdatedRef.current =
            updatedAt;

          setSnapshotStale(false);

          setSnapshot({
            workspaceId,
            items: deployments,
            loaded: true,
            loading: false,
            refreshing: false,
            error: "",
            lastUpdatedAt: updatedAt,
          });

          reconcilePendingActions(
            deployments,
          );
        } catch (cause) {
          if (!isCurrentRequest()) {
            return;
          }

          if (isPermissionError(cause)) {
            invalidateAccess(
              getErrorMessage(
                cause,
                "Deployment access is unavailable.",
              ),
            );

            return;
          }

          staleRef.current = true;

          setSnapshotStale(true);

          // Preserve the last successful
          // response for temporary failures,
          // but disable runtime controls.

          setSnapshot((previous) => ({
            ...previous,
            loading: false,
            refreshing: false,
            error: controller.signal.aborted
              ? "The deployment request timed out."
              : getErrorMessage(
                  cause,
                  "Could not load deployments. Please try again.",
                ),
          }));
        } finally {
          window.clearTimeout(timeout);

          if (isCurrentRequest()) {
            requestRef.current = null;

            requestControllerRef.current =
              null;
          }
        }
      })();

      requestRef.current = task;

      return task;
    },
    [
      workspaceId,
      reconcilePendingActions,
      invalidateAccess,
    ],
  );

  // ================================================
  // Component lifecycle
  // ================================================

  useEffect(() => {
    mountedRef.current = true;

    generationRef.current += 1;

    const actionControllers =
      actionControllersRef.current;

    const actionLocks = actionLocksRef.current;

    const timer = window.setTimeout(
      () => {
        void load();
      },
      0,
    );

    return () => {
      window.clearTimeout(timer);

      mountedRef.current = false;

      generationRef.current += 1;

      requestIdRef.current += 1;

      requestControllerRef.current?.abort();

      requestControllerRef.current = null;

      requestRef.current = null;

      for (const controller of
        actionControllers.values()) {
        controller.abort();
      }

      actionControllers.clear();

      actionLocks.clear();

      pendingRef.current = {};

      itemsRef.current = [];

      loadedRef.current = false;

      staleRef.current = true;

      lastUpdatedRef.current = null;
    };
  }, [load]);

  // ================================================
  // Adaptive automatic polling
  // ================================================

  useEffect(() => {
    let stopped = false;

    let timer: number | undefined;

    const schedule = () => {
      if (stopped) {
        return;
      }

      const hasActiveDeployments =
        itemsRef.current.some(
          isDeploymentActive,
        );

      const hasRunningApplications =
        itemsRef.current.some(
          (item) =>
            item.runtime?.status ===
            "RUNNING",
        );

      const hasPendingActions =
        Object.values(
          pendingRef.current,
        ).some(Boolean);

      const requiresFastPolling =
        !loadedRef.current ||
        staleRef.current ||
        hasActiveDeployments ||
        hasRunningApplications ||
        hasPendingActions;

      const interval =
        requiresFastPolling
          ? POLLING_INTERVAL_MS
          : IDLE_POLLING_INTERVAL_MS;

      timer = window.setTimeout(
        async () => {
          if (stopped) {
            return;
          }

          if (
            document.visibilityState ===
              "visible" &&
            !permissionDeniedRef.current
          ) {
            await load();
          }

          schedule();
        },
        interval,
      );
    };

    const refreshVisible = () => {
      if (
        document.visibilityState ===
          "visible" &&
        !permissionDeniedRef.current
      ) {
        void load();
      }
    };

    schedule();

    document.addEventListener(
      "visibilitychange",
      refreshVisible,
    );

    window.addEventListener(
      "focus",
      refreshVisible,
    );

    window.addEventListener(
      "online",
      refreshVisible,
    );

    return () => {
      stopped = true;

      if (timer !== undefined) {
        window.clearTimeout(timer);
      }

      document.removeEventListener(
        "visibilitychange",
        refreshVisible,
      );

      window.removeEventListener(
        "focus",
        refreshVisible,
      );

      window.removeEventListener(
        "online",
        refreshVisible,
      );
    };
  }, [load]);

  // ================================================
  // Snapshot freshness and action timeout
  // ================================================

  useEffect(() => {
    const timer = window.setInterval(
      () => {
        const now = Date.now();

        const lastUpdated =
          lastUpdatedRef.current;

        if (
          lastUpdated === null ||
          now - lastUpdated >
            MAX_SNAPSHOT_AGE_MS
        ) {
          staleRef.current = true;

          setSnapshotStale(true);
        }

        if (
          permissionDeniedRef.current
        ) {
          return;
        }

        const nextPending = {
          ...pendingRef.current,
        };

        let changed = false;

        for (const [
          deploymentId,
          operation,
        ] of Object.entries(
          nextPending,
        )) {
          if (
            !operation ||
            operation.timeoutReported ||
            now - operation.requestedAt <
              ACTION_CONFIRMATION_TIMEOUT_MS
          ) {
            continue;
          }

          nextPending[deploymentId] = {
            ...operation,
            timeoutReported: true,
          };

          changed = true;

          // Report once. Timeout does not
          // prove that the operation failed.

          reportActionError(
            deploymentId,
            "The runtime action has not been confirmed yet. Check worker status and deployment logs. Automatic retries are disabled.",
          );
        }

        if (changed) {
          pendingRef.current =
            nextPending;

          setPending(nextPending);
        }
      },
      1_000,
    );

    return () => {
      window.clearInterval(timer);
    };
  }, [reportActionError]);

  // ================================================
  // Runtime action execution
  // ================================================

  const runAction = useCallback(
    async (
      deploymentId: string,
      action: RuntimeAction,
    ): Promise<void> => {
      if (!mountedRef.current) {
        return;
      }

      if (
        !isUuid(deploymentId) ||
        !runtimeActions.includes(action)
      ) {
        return;
      }

      // Synchronous locking prevents rapid
      // double-clicks before React re-renders.
      if (
        actionLocksRef.current.has(
          deploymentId,
        ) ||
        pendingRef.current[deploymentId]
      ) {
        return;
      }

      const lastUpdated =
        lastUpdatedRef.current;

      if (
        permissionDeniedRef.current ||
        staleRef.current ||
        lastUpdated === null ||
        Date.now() - lastUpdated >
          MAX_SNAPSHOT_AGE_MS
      ) {
        reportActionError(
          deploymentId,
          "Deployment information is stale. Refresh before performing a runtime action.",
        );

        return;
      }

      const item =
        itemsRef.current.find(
          (deployment) =>
            deployment.id ===
            deploymentId,
        );

      if (
        !item?.runtime ||
        !isRuntimeActionAllowed(
          item.runtime,
          action,
        )
      ) {
        reportActionError(
          deploymentId,
          "This action is not available for the current runtime status.",
        );

        return;
      }

      const generation =
        generationRef.current;

      const isCurrentAction =
        (): boolean =>
          mountedRef.current &&
          generationRef.current ===
            generation;

      const operation: PendingAction = {
        action,
        runtimeId: item.runtime.id,
        previousStatus:
          item.runtime.status,
        previousRestartCount:
          item.runtime.restartCount,
        requestedAt: Date.now(),
        timeoutReported: false,
      };

      const controller =
        new AbortController();

      actionControllersRef.current.set(
        deploymentId,
        controller,
      );

      actionLocksRef.current.add(
        deploymentId,
      );

      setActing((previous) => ({
        ...previous,
        [deploymentId]: action,
      }));

      clearActionError(deploymentId);

      const timeout = window.setTimeout(
        () => {
          controller.abort();
        },
        ACTION_REQUEST_TIMEOUT_MS,
      );

      let requiresConfirmation = false;

      try {
        await apiFetch(
          `/deployments/${encodeURIComponent(
            deploymentId,
          )}/runtime/${action}`,
          {
            method: "POST",
            signal: controller.signal,
          },
        );

        if (!isCurrentAction()) {
          return;
        }

        // A successful HTTP response may only
        // acknowledge queue acceptance.
        requiresConfirmation = true;
      } catch (cause) {
        if (!isCurrentAction()) {
          return;
        }

        if (isPermissionError(cause)) {
          // Preserve uncertainty until the
          // API authorization contract has
          // been verified end-to-end.
          requiresConfirmation = true;

          invalidateAccess(
            getErrorMessage(
              cause,
              "Runtime access is unavailable.",
            ),
          );
        } else {
          // HTTP errors, network failures,
          // timeouts and invalid responses
          // may have ambiguous side effects.
          //
          // Do not automatically retry
          // lifecycle mutations.
          requiresConfirmation = true;

          reportActionError(
            deploymentId,
            `${getErrorMessage(
              cause,
              "The runtime request could not be completed.",
            )} The action outcome could not be confirmed.`,
          );
        }
      } finally {
        window.clearTimeout(timeout);

        if (isCurrentAction()) {
          actionControllersRef.current.delete(
            deploymentId,
          );

          if (requiresConfirmation) {
            const nextPending = {
              ...pendingRef.current,
              [deploymentId]: operation,
            };

            pendingRef.current =
              nextPending;

            setPending(nextPending);
          }

          actionLocksRef.current.delete(
            deploymentId,
          );

          setActing((previous) => {
            const next = {
              ...previous,
            };

            delete next[deploymentId];

            return next;
          });
        }
      }

      if (
        !isCurrentAction() ||
        permissionDeniedRef.current
      ) {
        return;
      }

      // An existing GET may have started
      // before the mutation completed.
      // Wait for it, then request fresh data.

      const inFlight =
        requestRef.current;

      if (inFlight) {
        await inFlight;
      }

      if (
        isCurrentAction() &&
        !permissionDeniedRef.current
      ) {
        await load();
      }
    },
    [
      load,
      reportActionError,
      clearActionError,
      invalidateAccess,
    ],
  );

  // ================================================
  // Derived presentation state
  // ================================================

  const items =
    snapshot.workspaceId === workspaceId
      ? snapshot.items
      : [];

  const hasActiveDeployments =
    items.some(isDeploymentActive);

  const hasRunningApplications =
    items.some(
      (item) =>
        item.runtime?.status === "RUNNING",
    );

  const hasPendingActions =
    Object.values(pending).some(Boolean);

  const fastPolling =
    hasActiveDeployments ||
    hasRunningApplications ||
    hasPendingActions ||
    snapshotStale;

  const controlsDisabled =
    snapshotStale ||
    !snapshot.loaded ||
    Boolean(snapshot.error);

  const showInitialLoading =
    snapshot.loading &&
    !snapshot.loaded;

  const showEmptyState =
    snapshot.loaded &&
    !snapshot.loading &&
    items.length === 0;

  const handleRuntimeAction = (
    deploymentId: string,
    action: RuntimeAction,
  ) => {
    void runAction(
      deploymentId,
      action,
    );
  };

  // ================================================
  // Render workspace deployments
  // ================================================

  return (
    <div className="min-w-0 space-y-5">
      {/* Fetch error */}

      {snapshot.error && (
        <div
          role="alert"
          className="flex min-w-0 flex-col gap-3 rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-800 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300 sm:flex-row sm:items-center sm:justify-between"
        >
          <div className="flex min-w-0 items-start gap-2">
            <AlertCircle
              aria-hidden="true"
              className="mt-0.5 size-4 shrink-0"
            />

            <div className="min-w-0">
              <p className="break-words font-medium">
                {snapshot.error}
              </p>

              {snapshot.loaded && (
                <p className="mt-1 text-xs opacity-80">
                  Showing the last
                  successfully loaded
                  records. Runtime controls
                  are temporarily disabled.
                </p>
              )}
            </div>
          </div>

          <button
            type="button"
            disabled={
              snapshot.loading ||
              snapshot.refreshing
            }
            onClick={() => void load()}
            className={`${BUTTON_CLASS} shrink-0 border-red-200 dark:border-red-900`}
          >
            <RefreshCw
              aria-hidden="true"
              className={[
                "size-3.5",
                snapshot.loading ||
                snapshot.refreshing
                  ? "animate-spin motion-reduce:animate-none"
                  : "",
              ].join(" ")}
            />

            Retry loading
          </button>
        </div>
      )}

      {/* Runtime action errors */}

      {actionErrors.map((error) => (
        <div
          key={error.deploymentId}
          role="alert"
          className="flex min-w-0 items-start justify-between gap-3 rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-800 dark:border-amber-900/60 dark:bg-amber-950/40 dark:text-amber-300"
        >
          <div className="flex min-w-0 items-start gap-2">
            <AlertCircle
              aria-hidden="true"
              className="mt-0.5 size-4 shrink-0"
            />

            <div className="min-w-0">
              <p className="font-semibold">
                Runtime action notice
              </p>

              <p className="mt-1 break-words leading-relaxed">
                {error.message}
              </p>
            </div>
          </div>

          <button
            type="button"
            aria-label="Dismiss runtime action notice"
            onClick={() =>
              clearActionError(
                error.deploymentId,
              )
            }
            className="shrink-0 rounded-md p-1 hover:bg-amber-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-500 dark:hover:bg-amber-950"
          >
            <XCircle
              aria-hidden="true"
              className="size-4"
            />
          </button>
        </div>
      ))}

      {/* Initial loading */}

      {showInitialLoading ? (
        <DeploymentSkeleton />
      ) : (
        <>
          {snapshot.loaded && (
            <>
              {/* Deployment statistics */}

              <DeploymentSummary
                items={items}
              />

              {/* Deployment toolbar */}

              <div className="flex min-w-0 flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <div className="min-w-0">
                  <h2 className="text-sm font-semibold">
                    Deployment history
                  </h2>

                  <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-zinc-500 dark:text-zinc-400">
                    <span>
                      {items.length}{" "}
                      {items.length === 1
                        ? "deployment"
                        : "deployments"}
                    </span>

                    <span className="inline-flex items-center gap-1.5">
                      <span
                        className={[
                          "size-1.5 rounded-full",
                          snapshotStale
                            ? "bg-amber-500"
                            : "bg-emerald-500",
                        ].join(" ")}
                      />

                      {snapshotStale
                        ? "Refresh required"
                        : fastPolling
                          ? "Auto-refresh · 5s"
                          : "Auto-refresh · 30s"}
                    </span>

                    {snapshot.lastUpdatedAt !==
                      null && (
                      <span>
                        Updated{" "}
                        {formatLastUpdated(
                          snapshot.lastUpdatedAt,
                        )}
                      </span>
                    )}
                  </div>
                </div>

                <button
                  type="button"
                  aria-label="Refresh deployments"
                  aria-busy={
                    snapshot.refreshing
                  }
                  disabled={
                    snapshot.refreshing ||
                    snapshot.loading
                  }
                  onClick={() =>
                    void load()
                  }
                  className={`${BUTTON_CLASS} w-full shrink-0 bg-white shadow-sm dark:bg-zinc-950 sm:w-auto`}
                >
                  <RefreshCw
                    aria-hidden="true"
                    className={[
                      "size-3.5",
                      snapshot.refreshing
                        ? "animate-spin motion-reduce:animate-none"
                        : "",
                    ].join(" ")}
                  />

                  {snapshot.refreshing
                    ? "Refreshing..."
                    : "Refresh"}
                </button>
              </div>

              {/* Deployment records */}

              {items.length > 0 && (
                <>
                  {/* Mobile and tablet */}

                  <div className="grid min-w-0 grid-cols-1 gap-3 md:grid-cols-2 xl:hidden">
                    {items.map((item) => (
                      <MobileDeploymentCard
                        key={item.id}
                        item={item}
                        acting={
                          acting[item.id]
                        }
                        pending={
                          pending[item.id]
                        }
                        controlsDisabled={
                          controlsDisabled
                        }
                        onAction={
                          handleRuntimeAction
                        }
                      />
                    ))}
                  </div>

                  {/* Laptop and desktop */}

                  <DeploymentTable
                    items={items}
                    acting={acting}
                    pending={pending}
                    controlsDisabled={
                      controlsDisabled
                    }
                    onAction={
                      handleRuntimeAction
                    }
                  />
                </>
              )}

              {showEmptyState && (
                <EmptyDeployments />
              )}
            </>
          )}
        </>
      )}

      {/* Build completed without runtime */}

      {items.some(
        (item) =>
          item.status ===
            "BUILD_SUCCEEDED" &&
          !item.runtime,
      ) && (
        <div className="flex min-w-0 items-start gap-3 rounded-xl border border-dashed p-4">
          <Package
            aria-hidden="true"
            className="mt-0.5 size-4 shrink-0 text-emerald-600 dark:text-emerald-400"
          />

          <div className="min-w-0">
            <p className="text-xs font-medium">
              Build artifact available
            </p>

            <p className="mt-1 text-xs leading-relaxed text-zinc-500 dark:text-zinc-400">
              An image has been built
              successfully. A runtime
              instance may be created
              when container execution
              is enabled and the runtime
              request is processed.
            </p>
          </div>
        </div>
      )}

      {/* Runtime security information */}

      <div className="flex min-w-0 items-start gap-3 rounded-xl border border-dashed p-4">
        <Container
          aria-hidden="true"
          className="mt-0.5 size-4 shrink-0 text-zinc-500"
        />

        <div className="min-w-0">
          <p className="text-xs font-medium">
            Local runtime execution
          </p>

          <p className="mt-1 text-xs leading-relaxed text-zinc-500 dark:text-zinc-400">
            CloudPilot is designed to run
            trusted applications using
            loopback-bound Docker containers,
            configured resource limits,
            read-only filesystems and
            restricted capabilities. These
            controls do not provide strong
            multi-tenant isolation.
          </p>

          <p className="mt-2 text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400">
            Application links are available
            only for validated numeric
            loopback addresses. The
            application must be accessible
            from the machine running your
            browser.
          </p>
        </div>
      </div>
    </div>
  );
}

// ==================================================
// Main deployments page
// ==================================================

export default function DeploymentsPage() {
  const { activeWorkspace } =
    useDashboard();

  const candidateWorkspaceId =
    activeWorkspace?.id ?? null;

  const workspaceId =
    isUuid(candidateWorkspaceId)
      ? candidateWorkspaceId
      : null;

  return (
    <DashboardPageShell
      breadcrumb="Deployments"
      title="Deployments"
      description={`Manage deployment builds and runtime instances for ${
        activeWorkspace?.name ??
        "your active workspace"
      }.`}
    >
      {workspaceId ? (
        // Remount workspace-scoped state
        // whenever the active workspace
        // changes. Old requests and records
        // cannot update the new component.

        <WorkspaceDeployments
          key={workspaceId}
          workspaceId={workspaceId}
        />
      ) : (
        <div className="rounded-xl border border-dashed px-5 py-14 text-center">
          <div className="mx-auto flex size-12 items-center justify-center rounded-xl bg-zinc-100 dark:bg-zinc-900">
            <Layers3
              aria-hidden="true"
              className="size-6 text-zinc-500"
            />
          </div>

          <h3 className="mt-4 text-sm font-semibold">
            No active workspace
          </h3>

          <p className="mx-auto mt-2 max-w-sm text-xs leading-relaxed text-zinc-500 dark:text-zinc-400">
            Select a valid workspace to
            view deployment records and
            manage runtime instances.
          </p>
        </div>
      )}
    </DashboardPageShell>
  );
}
