
"use client";

import {
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";

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
  XCircle,
} from "lucide-react";

import { DashboardPageShell } from "@/components/dashboard/page-shell";
import { apiFetch } from "@/lib/api";
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
};

type PendingActionState = Record<
  string,
  PendingAction | undefined
>;

type DeploymentSnapshot = {
  workspaceId: string | null;
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

const ACTION_CONFIRMATION_TIMEOUT_MS = 60_000;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

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

  const remainingMinutes = minutes % 60;

  return `${hours}h ${remainingMinutes}m`;
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

  const startedAt = Date.parse(
    item.startedAt,
  );

  const finishedAt = Date.parse(
    item.finishedAt,
  );

  if (
    !Number.isFinite(startedAt) ||
    !Number.isFinite(finishedAt)
  ) {
    return "—";
  }

  return formatDuration(
    finishedAt - startedAt,
  );
}

function formatImageSize(
  value: string | null,
): string {
  if (
    value === null ||
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 30 ||
    /[^0-9]/u.test(value)
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

    const whole = rounded / BigInt(10);

    const fraction =
      rounded % BigInt(10);

    return (
      `${whole.toString()}.` +
      `${fraction.toString()} ` +
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
  if (
    cause instanceof Error &&
    cause.message.trim().length > 0
  ) {
    return cause.message.slice(0, 500);
  }

  return fallback;
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
 * Do not render arbitrary URLs returned by the API.
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

  /**
   * Reject alternate IP representations,
   * external hosts and unexpected authorities
   * before URL normalization.
   */
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
// Status presentation
// ==================================================

function getStatusClasses(
  status: string,
): string {
  if (FAILED_STATUSES.has(status)) {
    return [
      "border-red-200",
      "bg-red-50",
      "text-red-700",
      "dark:border-red-900/60",
      "dark:bg-red-950/40",
      "dark:text-red-400",
    ].join(" ");
  }

  if (SUCCESS_STATUSES.has(status)) {
    return [
      "border-emerald-200",
      "bg-emerald-50",
      "text-emerald-700",
      "dark:border-emerald-900/60",
      "dark:bg-emerald-950/40",
      "dark:text-emerald-400",
    ].join(" ");
  }

  if (
    status === "BUILDING" ||
    status === "DEPLOYING" ||
    status === "STARTING" ||
    status === "CREATING" ||
    status === "HEALTH_CHECKING" ||
    status === "SOURCE_PREPARING"
  ) {
    return [
      "border-blue-200",
      "bg-blue-50",
      "text-blue-700",
      "dark:border-blue-900/60",
      "dark:bg-blue-950/40",
      "dark:text-blue-400",
    ].join(" ");
  }

  if (
    status === "QUEUED" ||
    status === "PENDING" ||
    status === "CANCELLING"
  ) {
    return [
      "border-amber-200",
      "bg-amber-50",
      "text-amber-700",
      "dark:border-amber-900/60",
      "dark:bg-amber-950/40",
      "dark:text-amber-400",
    ].join(" ");
  }

  return [
    "border-zinc-200",
    "bg-zinc-100",
    "text-zinc-700",
    "dark:border-zinc-800",
    "dark:bg-zinc-900",
    "dark:text-zinc-300",
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

      <span className="break-words">
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
          item.runtime?.status ===
          "RUNNING",
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
    <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">
      {statistics.map((statistic) => {
        const Icon = statistic.icon;

        return (
          <div
            key={statistic.label}
            className="min-w-0 rounded-xl border bg-white p-3 shadow-sm dark:bg-zinc-950 sm:p-4"
          >
            <div className="flex items-start justify-between gap-2">
              <p className="min-w-0 text-xs font-medium text-zinc-500 dark:text-zinc-400">
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
  onAction: (
    deploymentId: string,
    action: RuntimeAction,
  ) => void;
}

function RuntimeControls({
  item,
  acting,
  pending,
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
    getSafeApplicationUrl(runtime);

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
          className="inline-flex min-h-8 items-center gap-1.5 rounded-md text-xs font-medium text-blue-600 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:text-blue-400"
        >
          Open application

          <ExternalLink
            aria-hidden="true"
            className="size-3"
          />
        </a>
      )}

      {runtime.failureCode && (
        <span className="max-w-full break-words text-[11px] font-medium text-red-600 dark:text-red-400">
          {buildStatusLabel(
            runtime.failureCode,
          )}
        </span>
      )}

      {runtime.failureMessage && (
        <p className="max-w-full break-words text-xs leading-relaxed text-red-600 dark:text-red-400">
          {runtime.failureMessage}
        </p>
      )}

      {runtime.restartCount > 0 && (
        <span className="text-[11px] text-zinc-500 dark:text-zinc-400">
          Restarts:{" "}
          {runtime.restartCount}
        </span>
      )}

      {(allowedActions.length > 0 ||
        busy) && (
        <div
          className="flex w-full flex-wrap gap-1.5"
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
                  busy || !allowed
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
                  "inline-flex min-h-9",
                  "items-center justify-center",
                  "gap-1.5 rounded-md border",
                  "px-2.5 py-1.5",
                  "text-xs font-medium",
                  "transition-colors",
                  "focus-visible:outline-none",
                  "focus-visible:ring-2",
                  "focus-visible:ring-blue-500",
                  "disabled:cursor-not-allowed",
                  "disabled:opacity-50",
                  action === "stop"
                    ? "border-red-200 text-red-600 hover:bg-red-50 dark:border-red-900/60 dark:text-red-400 dark:hover:bg-red-950/40"
                    : "hover:bg-zinc-100 dark:hover:bg-zinc-800",
                ].join(" ")}
              >
                {isActing ||
                isPending ? (
                  <Loader2
                    aria-hidden="true"
                    className="size-3 animate-spin"
                  />
                ) : (
                  <Icon
                    aria-hidden="true"
                    className="size-3"
                  />
                )}

                {isActing
                  ? "Sending..."
                  : isPending
                    ? "Pending..."
                    : ACTION_LABELS[
                        action
                      ]}
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
          Waiting for the worker to
          confirm the runtime action.
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
        <span className="break-words text-[11px] text-red-600 dark:text-red-400">
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
          <span
            title={build.imageTag}
            className="inline-flex items-center gap-1.5 font-medium"
          >
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
          {build.failureMessage}
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
// Mobile deployment card
// ==================================================

function MobileDeploymentCard({
  item,
  acting,
  pending,
  onAction,
}: RuntimeControlsProps) {
  return (
    <article className="min-w-0 overflow-hidden rounded-xl border bg-white p-4 shadow-sm dark:bg-zinc-950">
      <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 items-start gap-3">
          <div className="flex size-10 shrink-0 items-center justify-center rounded-lg border bg-zinc-50 dark:bg-zinc-900">
            <Rocket
              aria-hidden="true"
              className="size-4 text-zinc-600 dark:text-zinc-300"
            />
          </div>

          <div className="min-w-0">
            <h3 className="break-words text-sm font-semibold">
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

        <div className="shrink-0">
          <StatusBadge
            status={item.status}
          />
        </div>
      </div>

      {item.failureCode && (
        <div className="mt-3 rounded-md border border-red-200 bg-red-50 p-2 text-xs text-red-700 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-400">
          {buildStatusLabel(
            item.failureCode,
          )}
        </div>
      )}

      <div className="mt-4 grid grid-cols-2 gap-4 border-y py-4">
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

          <div className="flex items-center gap-1.5 text-xs">
            <GitCommitHorizontal
              aria-hidden="true"
              className="size-3.5 shrink-0 text-zinc-500"
            />

            <span
              title={
                item.commitSha ??
                undefined
              }
              className="font-mono"
            >
              {item.commitSha
                ? item.commitSha.slice(
                    0,
                    7,
                  )
                : "—"}
            </span>
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
          onAction={onAction}
        />
      </div>

      {item.runtime && (
        <div className="mt-4 grid grid-cols-3 gap-2 rounded-lg bg-zinc-50 p-3 dark:bg-zinc-900">
          <div className="min-w-0">
            <p className="text-[10px] text-zinc-500 dark:text-zinc-400">
              CPU
            </p>

            <p className="mt-1 break-words text-xs font-medium">
              {item.runtime.cpuLimit !==
              null
                ? `${item.runtime.cpuLimit} CPU`
                : "—"}
            </p>
          </div>

          <div className="min-w-0">
            <p className="text-[10px] text-zinc-500 dark:text-zinc-400">
              Memory
            </p>

            <p className="mt-1 break-words text-xs font-medium">
              {item.runtime
                .memoryLimitMb !== null
                ? `${item.runtime.memoryLimitMb} MB`
                : "—"}
            </p>
          </div>

          <div className="min-w-0">
            <p className="text-[10px] text-zinc-500 dark:text-zinc-400">
              PID limit
            </p>

            <p className="mt-1 text-xs font-medium">
              {item.runtime.pidsLimit ??
                "—"}
            </p>
          </div>
        </div>
      )}

      <p className="mt-4 border-t pt-3 text-[11px] text-zinc-500 dark:text-zinc-400">
        Created{" "}
        {formatDate(item.createdAt)}
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
  onAction: (
    deploymentId: string,
    action: RuntimeAction,
  ) => void;
}

function DeploymentTable({
  items,
  acting,
  pending,
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
    <div className="hidden overflow-hidden rounded-xl border bg-white shadow-sm dark:bg-zinc-950 xl:block">
      <div className="overflow-x-auto">
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
                    <span className="break-words font-semibold">
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
                  </div>
                </td>

                <td className="px-3 py-4">
                  <div className="flex min-w-0 flex-col items-start gap-2">
                    <StatusBadge
                      status={item.status}
                    />

                    {item.failureCode && (
                      <span className="break-words text-[11px] text-red-600 dark:text-red-400">
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
                    onAction={onAction}
                  />
                </td>

                <td className="px-3 py-4">
                  <span
                    title={
                      item.commitSha ??
                      undefined
                    }
                    className="font-mono"
                  >
                    {item.commitSha
                      ? item.commitSha.slice(
                          0,
                          7,
                        )
                      : "—"}
                  </span>
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
        This workspace has no
        deployments yet. Deployment
        records will appear here when
        a project deployment is
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

  // ================================================
  // Request and action references
  // ================================================

  const mountedRef = useRef(false);

  const generationRef = useRef(0);

  const requestIdRef = useRef(0);

  const itemsRef = useRef<
    Deployment[]
  >([]);

  const loadedRef = useRef(false);

  const requestRef = useRef<
    Promise<void> | null
  >(null);

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

        setActionErrors(
          (previous) => [
            ...previous.filter(
              (error) =>
                error.deploymentId !==
                deploymentId,
            ),
            {
              deploymentId,
              message,
            },
          ],
        );
      },
      [],
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

          const item = nextItems.find(
            (deployment) =>
              deployment.id ===
              deploymentId,
          );

          if (
            isActionConfirmed(
              item,
              operation,
            )
          ) {
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
                item.runtime
                  .failureMessage ??
                  "The runtime action did not complete successfully.",
              );
            }
          }
        }

        if (changed) {
          pendingRef.current =
            nextPending;

          setPending(nextPending);
        }
      },
      [reportActionError],
    );

  // ================================================
  // Workspace-safe deployment fetching
  // ================================================

  const load = useCallback(
    async (): Promise<void> => {
      if (!mountedRef.current) {
        return;
      }

      /**
       * Reuse the in-flight request.
       *
       * Polling and manual refresh must not
       * create overlapping list requests.
       */
      if (requestRef.current) {
        return requestRef.current;
      }

      const generation =
        generationRef.current;

      const requestId =
        ++requestIdRef.current;

      const isCurrentRequest =
        (): boolean =>
          mountedRef.current &&
          generationRef.current ===
            generation &&
          requestIdRef.current ===
            requestId;

      setSnapshot((previous) => ({
        ...previous,

        loading: !loadedRef.current,

        refreshing:
          loadedRef.current,
      }));

      const task = (async () => {
        try {
          const response =
            await apiFetch<
              Deployment[]
            >("/deployments");

          if (!isCurrentRequest()) {
            return;
          }

          if (
            !Array.isArray(response)
          ) {
            throw new Error(
              "Invalid deployment response.",
            );
          }

          itemsRef.current = response;

          loadedRef.current = true;

          setSnapshot({
            workspaceId,

            items: response,

            loaded: true,

            loading: false,

            refreshing: false,

            error: "",

            lastUpdatedAt: Date.now(),
          });

          reconcilePendingActions(
            response,
          );
        } catch (cause) {
          if (!isCurrentRequest()) {
            return;
          }

          /**
           * Preserve the last successful
           * response during refresh failures.
           */
          setSnapshot(
            (previous) => ({
              ...previous,

              loading: false,

              refreshing: false,

              error: getErrorMessage(
                cause,
                "Could not load deployments.",
              ),
            }),
          );
        } finally {
          if (isCurrentRequest()) {
            requestRef.current = null;
          }
        }
      })();

      requestRef.current = task;

      return task;
    },
    [
      workspaceId,
      reconcilePendingActions,
    ],
  );

  // ================================================
  // Component lifecycle
  // ================================================

  useEffect(() => {
    mountedRef.current = true;

    generationRef.current += 1;

    /**
     * Delay the first request until the
     * workspace-scoped component is mounted.
     */
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

      requestRef.current = null;

      actionLocksRef.current.clear();

      pendingRef.current = {};
    };
  }, [load]);

  // ================================================
  // Automatic polling
  // ================================================

  useEffect(() => {
    let stopped = false;

    let timer: number | undefined;

    const schedule = () => {
      if (stopped) {
        return;
      }

      timer = window.setTimeout(
        async () => {
          if (stopped) {
            return;
          }

          const hasActiveDeployments =
            itemsRef.current.some(
              isDeploymentActive,
            );

          const hasPendingActions =
            Object.values(
              pendingRef.current,
            ).some(Boolean);

          if (
            document.visibilityState ===
              "visible" &&
            (
              hasActiveDeployments ||
              hasPendingActions
            )
          ) {
            await load();
          }

          schedule();
        },
        POLLING_INTERVAL_MS,
      );
    };

    const handleVisibilityChange =
      () => {
        if (
          document.visibilityState ===
          "visible"
        ) {
          void load();
        }
      };

    const handleFocus = () => {
      if (
        document.visibilityState ===
        "visible"
      ) {
        void load();
      }
    };

    schedule();

    document.addEventListener(
      "visibilitychange",
      handleVisibilityChange,
    );

    window.addEventListener(
      "focus",
      handleFocus,
    );

    return () => {
      stopped = true;

      if (timer !== undefined) {
        window.clearTimeout(timer);
      }

      document.removeEventListener(
        "visibilitychange",
        handleVisibilityChange,
      );

      window.removeEventListener(
        "focus",
        handleFocus,
      );
    };
  }, [load]);

  // ================================================
  // Pending action confirmation timeout
  // ================================================

  useEffect(() => {
    const timer = window.setInterval(
      () => {
        const now = Date.now();

        for (const [
          deploymentId,
          operation,
        ] of Object.entries(
          pendingRef.current,
        )) {
          if (!operation) {
            continue;
          }

          if (
            now -
              operation.requestedAt <
            ACTION_CONFIRMATION_TIMEOUT_MS
          ) {
            continue;
          }

          /**
           * A timeout is not proof that the
           * operation failed.
           *
           * Keep the operation locked instead
           * of automatically submitting it again.
           */
          reportActionError(
            deploymentId,
            "The runtime action has not been confirmed yet. Refresh the deployment and check worker status before attempting another operation.",
          );
        }
      },
      POLLING_INTERVAL_MS,
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
        deploymentId.length !== 36 ||
        !UUID_PATTERN.test(
          deploymentId,
        )
      ) {
        reportActionError(
          deploymentId,
          "Invalid deployment identity.",
        );

        return;
      }

      /**
       * Synchronous locking prevents rapid
       * double-clicks before React re-renders.
       */
      if (
        actionLocksRef.current.has(
          deploymentId,
        ) ||
        pendingRef.current[
          deploymentId
        ]
      ) {
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

      const operation: PendingAction =
        {
          action,

          runtimeId:
            item.runtime.id,

          previousStatus:
            item.runtime.status,

          previousRestartCount:
            item.runtime.restartCount,

          requestedAt: Date.now(),
        };

      actionLocksRef.current.add(
        deploymentId,
      );

      setActing((previous) => ({
        ...previous,

        [deploymentId]: action,
      }));

      setActionErrors((previous) =>
        previous.filter(
          (error) =>
            error.deploymentId !==
            deploymentId,
        ),
      );

      try {
        await apiFetch(
          `/deployments/${encodeURIComponent(
            deploymentId,
          )}/runtime/${action}`,
          {
            method: "POST",
          },
        );

        if (!isCurrentAction()) {
          return;
        }

        /**
         * The API may acknowledge queue
         * acceptance before execution completes.
         *
         * Keep lifecycle controls locked until
         * the worker outcome is observed.
         */
        const nextPending = {
          ...pendingRef.current,

          [deploymentId]: operation,
        };

        pendingRef.current =
          nextPending;

        setPending(nextPending);

        /**
         * An existing GET request may have
         * started before the POST completed.
         * Wait for it before requesting a fresh
         * deployment snapshot.
         */
        const inFlight =
          requestRef.current;

        if (inFlight) {
          await inFlight;
        }

        if (isCurrentAction()) {
          await load();
        }
      } catch (cause) {
        if (!isCurrentAction()) {
          return;
        }

        /**
         * A network failure can occur after
         * the server accepts a lifecycle job.
         *
         * Treat the outcome as uncertain rather
         * than immediately allowing duplicate
         * runtime operations.
         */
        const nextPending = {
          ...pendingRef.current,

          [deploymentId]: operation,
        };

        pendingRef.current =
          nextPending;

        setPending(nextPending);

        reportActionError(
          deploymentId,
          `${getErrorMessage(
            cause,
            "Runtime action failed.",
          )} The action outcome could not be confirmed.`,
        );

        const inFlight =
          requestRef.current;

        if (inFlight) {
          await inFlight;
        }

        if (isCurrentAction()) {
          await load();
        }
      } finally {
        if (isCurrentAction()) {
          actionLocksRef.current.delete(
            deploymentId,
          );

          setActing((previous) => {
            const next = {
              ...previous,
            };

            delete next[
              deploymentId
            ];

            return next;
          });
        }
      }
    },
    [load, reportActionError],
  );

  // ================================================
  // Derived presentation state
  // ================================================

  const items =
    snapshot.workspaceId ===
    workspaceId
      ? snapshot.items
      : [];

  const hasActiveDeployments =
    items.some(isDeploymentActive);

  const hasPendingActions =
    Object.values(pending).some(
      Boolean,
    );

  const isPolling =
    hasActiveDeployments ||
    hasPendingActions;

  const showInitialLoading =
    snapshot.loading &&
    !snapshot.loaded;

  const showEmptyState =
    snapshot.loaded &&
    !snapshot.loading &&
    items.length === 0;

  // ================================================
  // Render workspace deployments
  // ================================================

  return (
    <div className="min-w-0 space-y-5">
      {/* Fetch error */}

      {snapshot.error && (
        <div
          role="alert"
          className="flex flex-col gap-3 rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-800 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300 sm:flex-row sm:items-center sm:justify-between"
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
                  deployment records.
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
            className="inline-flex min-h-9 shrink-0 items-center justify-center gap-2 rounded-md border border-red-200 px-3 text-xs font-medium hover:bg-red-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-500 disabled:opacity-50 dark:border-red-900 dark:hover:bg-red-950"
          >
            <RefreshCw
              aria-hidden="true"
              className={[
                "size-3.5",
                snapshot.loading ||
                snapshot.refreshing
                  ? "animate-spin"
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
          className="flex items-start justify-between gap-3 rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-800 dark:border-amber-900/60 dark:bg-amber-950/40 dark:text-amber-300"
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
              setActionErrors(
                (previous) =>
                  previous.filter(
                    (entry) =>
                      entry.deploymentId !==
                      error.deploymentId,
                  ),
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

              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
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

                    {isPolling && (
                      <span className="inline-flex items-center gap-1.5">
                        <span className="size-1.5 animate-pulse rounded-full bg-emerald-500 motion-reduce:animate-none" />

                        Auto-refresh
                        enabled
                      </span>
                    )}

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
                  className="inline-flex min-h-9 w-full shrink-0 items-center justify-center gap-2 rounded-lg border bg-white px-3 text-xs font-medium shadow-sm transition-colors hover:bg-zinc-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 disabled:cursor-not-allowed disabled:opacity-60 dark:bg-zinc-950 dark:hover:bg-zinc-900 sm:w-auto"
                >
                  <RefreshCw
                    aria-hidden="true"
                    className={[
                      "size-3.5",
                      snapshot.refreshing
                        ? "animate-spin"
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
                    {items.map(
                      (item) => (
                        <MobileDeploymentCard
                          key={item.id}
                          item={item}
                          acting={
                            acting[
                              item.id
                            ]
                          }
                          pending={
                            pending[
                              item.id
                            ]
                          }
                          onAction={(
                            deploymentId,
                            action,
                          ) => {
                            void runAction(
                              deploymentId,
                              action,
                            );
                          }}
                        />
                      ),
                    )}
                  </div>

                  {/* Laptop and desktop */}

                  <DeploymentTable
                    items={items}
                    acting={acting}
                    pending={pending}
                    onAction={(
                      deploymentId,
                      action,
                    ) => {
                      void runAction(
                        deploymentId,
                        action,
                      );
                    }}
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
        <div className="flex items-start gap-3 rounded-xl border border-dashed p-4">
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

      <div className="flex items-start gap-3 rounded-xl border border-dashed p-4">
        <Container
          aria-hidden="true"
          className="mt-0.5 size-4 shrink-0 text-zinc-500"
        />

        <div className="min-w-0">
          <p className="text-xs font-medium">
            Local runtime execution
          </p>

          <p className="mt-1 text-xs leading-relaxed text-zinc-500 dark:text-zinc-400">
            CloudPilot is designed to
            run trusted applications
            using loopback-bound Docker
            containers, configured
            resource limits, read-only
            filesystems and restricted
            capabilities. These
            controls do not provide
            strong multi-tenant
            isolation.
          </p>

          <p className="mt-2 text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400">
            Application links are
            available only for verified
            numeric loopback addresses.
            The application must be
            accessible from the machine
            running your browser.
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

  const workspaceId =
    activeWorkspace?.id ?? null;

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
        /**
         * Remount all workspace-scoped state
         * when the active workspace changes.
         *
         * Old deployment records, errors,
         * requests and runtime action states
         * cannot carry into the new component.
         */
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
            Select a workspace to view
            deployment records and
            manage runtime instances.
          </p>
        </div>
      )}
    </DashboardPageShell>
  );
}
