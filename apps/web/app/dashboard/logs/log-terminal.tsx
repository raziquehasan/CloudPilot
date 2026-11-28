"use client";

import {
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { io, type Socket } from "socket.io-client";

import {
  ArrowDownToLine,
  ArrowDown,
  ClipboardCopy,
  Pause,
  Play,
  RefreshCw,
  Search,
  Terminal,
  Trash2,
  Check,
  AlertTriangle,
} from "lucide-react";

import { DashboardPageShell } from "@/components/dashboard/page-shell";
import { useDashboard } from "@/components/dashboard/dashboard-provider";
import { API_ORIGIN, apiFetch } from "@/lib/api";

// ==================================================
// Types — mirror deployment log contracts
// ==================================================

type Source = "BUILD" | "RUNTIME" | "SYSTEM";

type Stream = "STDOUT" | "STDERR" | "SYSTEM";

type Level =
  | "DEBUG"
  | "INFO"
  | "WARN"
  | "ERROR"
  | "UNKNOWN";

type Connection =
  | "connecting"
  | "live"
  | "reconnecting"
  | "disconnected";

interface LogEntry {
  id: string;
  workspaceId: string;
  projectId: string;
  deploymentId: string;
  buildId: string | null;
  runtimeInstanceId: string | null;
  source: Source;
  stream: Stream;
  level: Level;
  message: string;
  sequence: string;
  eventTimestamp: string;
  persistedAt: string;
}

interface LogHistory {
  items: LogEntry[];
  nextCursor: string | null;
  previousCursor: string | null;
  hasMore: boolean;
  oldestSequence: string | null;
  newestSequence: string | null;
  retentionTruncated: boolean;
}

interface LogScope {
  workspaceId: string;
  projectId: string;
  deploymentId: string;
}

interface LogBatch {
  deploymentId: string;
  entries: LogEntry[];
  highWatermark: string;
  gapDetected: boolean;
}

interface LogBuffer {
  entries: LogEntry[];
  bytes: number;
  evicted: boolean;
}

// ==================================================
// Safety limits
// ==================================================

const MAX_ENTRIES = 5_000;

const MAX_BYTES = 16 * 1024 * 1024;

const MAX_ENTRY_BYTES = 16 * 1024;

const MAX_BATCH_ENTRIES = 1_000;

const MAX_BATCH_BYTES = 512 * 1024;

const MAX_HISTORY_BYTES = 8 * 1024 * 1024;

const MAX_CURSOR = BigInt("9223372036854775807");

const HISTORY_PAGE_SIZE = 200;

const MAX_CATCHUP_PAGES = 25;

const MAX_SEARCH_LENGTH = 256;

const RENDER_INTERVAL_MS = 100;

const SOCKET_TIMEOUT_MS = 10_000;

const RECONNECT_DELAY_MS = 1_000;

const MAX_RECONNECT_DELAY_MS = 15_000;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const CURSOR_PATTERN = /^(0|[1-9][0-9]{0,18})$/;

const UNSAFE_CONTROLS =
  /[\u0000-\u0008\u000B-\u001F\u007F-\u009F]/;

const encoder = new TextEncoder();

const SOURCES = new Set<string>([
  "BUILD",
  "RUNTIME",
  "SYSTEM",
]);

const STREAMS = new Set<string>([
  "STDOUT",
  "STDERR",
  "SYSTEM",
]);

const LEVELS = new Set<string>([
  "DEBUG",
  "INFO",
  "WARN",
  "ERROR",
  "UNKNOWN",
]);

const SOURCE_TABS: Array<{
  key: "ALL" | Source;
  label: string;
}> = [
  { key: "ALL", label: "All Logs" },
  { key: "BUILD", label: "Build" },
  { key: "RUNTIME", label: "Runtime" },
  { key: "SYSTEM", label: "System" },
];

const CONNECTION_LABELS: Record<Connection, string> = {
  connecting: "Connecting…",
  live: "Live",
  reconnecting: "Reconnecting…",
  disconnected: "Disconnected",
};

const CONNECTION_COLORS: Record<Connection, string> = {
  connecting: "bg-amber-500",
  live: "bg-emerald-500",
  reconnecting: "bg-amber-500",
  disconnected: "bg-zinc-400",
};

const ACTION_CLASS =
  "inline-flex h-9 w-9 shrink-0 items-center justify-center " +
  "rounded-md border border-zinc-200 bg-white transition-colors " +
  "hover:bg-zinc-100 focus-visible:outline-none " +
  "focus-visible:ring-2 focus-visible:ring-blue-500 " +
  "disabled:cursor-not-allowed disabled:opacity-40 " +
  "dark:border-zinc-800 dark:bg-zinc-950 dark:hover:bg-zinc-800";

// ==================================================
// Validation helpers
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

function isUuid(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length === 36 &&
    UUID_PATTERN.test(value)
  );
}

function sameUuid(
  left: string,
  right: string,
): boolean {
  return left.toLowerCase() === right.toLowerCase();
}

function isNullableUuid(
  value: unknown,
): value is string | null {
  return value === null || isUuid(value);
}

function parseCursor(value: unknown): bigint | null {
  if (
    typeof value !== "string" ||
    value.length > 19 ||
    !CURSOR_PATTERN.test(value)
  ) {
    return null;
  }

  try {
    const cursor = BigInt(value);

    if (cursor < BigInt(0) || cursor > MAX_CURSOR) {
      return null;
    }

    return cursor;
  } catch {
    return null;
  }
}

function isPositiveCursor(
  value: unknown,
): value is string {
  const cursor = parseCursor(value);

  return cursor !== null && cursor > BigInt(0);
}

function isNullableCursor(
  value: unknown,
): value is string | null {
  return value === null || isPositiveCursor(value);
}

function compareCursor(
  left: string,
  right: string,
): number {
  const a = BigInt(left);
  const b = BigInt(right);

  return a < b ? -1 : a > b ? 1 : 0;
}

function isTimestamp(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= 64 &&
    Number.isFinite(Date.parse(value))
  );
}

function utf8Bytes(value: string): number {
  return encoder.encode(value).byteLength;
}

function createEmptyBuffer(): LogBuffer {
  return {
    entries: [],
    bytes: 0,
    evicted: false,
  };
}

function validateEntry(
  value: unknown,
  scope: LogScope,
): value is LogEntry {
  if (!isObject(value)) {
    return false;
  }

  if (
    !isUuid(value.id) ||
    !isUuid(value.workspaceId) ||
    !isUuid(value.projectId) ||
    !isUuid(value.deploymentId)
  ) {
    return false;
  }

  if (
    !sameUuid(value.workspaceId, scope.workspaceId) ||
    !sameUuid(value.projectId, scope.projectId) ||
    !sameUuid(value.deploymentId, scope.deploymentId)
  ) {
    return false;
  }

  if (
    !isNullableUuid(value.buildId) ||
    !isNullableUuid(value.runtimeInstanceId)
  ) {
    return false;
  }

  if (
    typeof value.source !== "string" ||
    !SOURCES.has(value.source) ||
    typeof value.stream !== "string" ||
    !STREAMS.has(value.stream) ||
    typeof value.level !== "string" ||
    !LEVELS.has(value.level)
  ) {
    return false;
  }

  if (
    typeof value.message !== "string" ||
    value.message.length === 0 ||
    value.message.length > MAX_ENTRY_BYTES ||
    UNSAFE_CONTROLS.test(value.message) ||
    utf8Bytes(value.message) > MAX_ENTRY_BYTES
  ) {
    return false;
  }

  return (
    isPositiveCursor(value.sequence) &&
    isTimestamp(value.eventTimestamp) &&
    isTimestamp(value.persistedAt)
  );
}

function validateEntries(
  value: unknown,
  scope: LogScope,
  maxBytes: number,
): value is LogEntry[] {
  if (
    !Array.isArray(value) ||
    value.length > MAX_BATCH_ENTRIES
  ) {
    return false;
  }

  const ids = new Set<string>();
  const sequences = new Set<string>();

  let bytes = 0;

  for (const entry of value) {
    if (!validateEntry(entry, scope)) {
      return false;
    }

    const id = entry.id.toLowerCase();

    if (
      ids.has(id) ||
      sequences.has(entry.sequence)
    ) {
      return false;
    }

    ids.add(id);
    sequences.add(entry.sequence);

    bytes += utf8Bytes(entry.message);

    if (bytes > maxBytes) {
      return false;
    }
  }

  return true;
}

function validateHistory(
  value: unknown,
  scope: LogScope,
): value is LogHistory {
  if (!isObject(value)) {
    return false;
  }

  if (
    !validateEntries(
      value.items,
      scope,
      MAX_HISTORY_BYTES,
    ) ||
    !isNullableCursor(value.nextCursor) ||
    !isNullableCursor(value.previousCursor) ||
    !isNullableCursor(value.oldestSequence) ||
    !isNullableCursor(value.newestSequence) ||
    typeof value.hasMore !== "boolean" ||
    typeof value.retentionTruncated !== "boolean"
  ) {
    return false;
  }

  const oldest = value.oldestSequence;
  const newest = value.newestSequence;

  if ((oldest === null) !== (newest === null)) {
    return false;
  }

  if (
    oldest !== null &&
    newest !== null &&
    compareCursor(oldest, newest) > 0
  ) {
    return false;
  }

  if (value.items.length > 0) {
    if (oldest === null || newest === null) {
      return false;
    }

    for (const entry of value.items) {
      if (
        compareCursor(entry.sequence, oldest) < 0 ||
        compareCursor(entry.sequence, newest) > 0
      ) {
        return false;
      }
    }
  }

  return true;
}

function validateBatch(
  value: unknown,
  scope: LogScope,
): value is LogBatch {
  if (!isObject(value)) {
    return false;
  }

  if (
    !isUuid(value.deploymentId) ||
    !sameUuid(value.deploymentId, scope.deploymentId) ||
    !validateEntries(
      value.entries,
      scope,
      MAX_BATCH_BYTES,
    ) ||
    typeof value.highWatermark !== "string" ||
    parseCursor(value.highWatermark) === null ||
    typeof value.gapDetected !== "boolean"
  ) {
    return false;
  }

  let previous = "0";

  for (const entry of value.entries) {
    if (
      compareCursor(entry.sequence, previous) <= 0 ||
      compareCursor(
        entry.sequence,
        value.highWatermark,
      ) > 0
    ) {
      return false;
    }

    previous = entry.sequence;
  }

  return true;
}

// ==================================================
// Bounded log buffer
// ==================================================

function mergeLogBuffer(
  previous: LogBuffer,
  incoming: readonly LogEntry[],
): {
  buffer: LogBuffer;
  added: number;
} {
  if (incoming.length === 0) {
    return {
      buffer: previous,
      added: 0,
    };
  }

  const ids = new Set(
    previous.entries.map((entry) =>
      entry.id.toLowerCase(),
    ),
  );

  const sequences = new Set(
    previous.entries.map((entry) => entry.sequence),
  );

  const entries = previous.entries.slice();

  let bytes = previous.bytes;
  let added = 0;

  for (const entry of incoming) {
    const id = entry.id.toLowerCase();

    if (
      ids.has(id) ||
      sequences.has(entry.sequence)
    ) {
      continue;
    }

    ids.add(id);
    sequences.add(entry.sequence);

    entries.push(entry);

    bytes += utf8Bytes(entry.message);

    added += 1;
  }

  if (added === 0) {
    return {
      buffer: previous,
      added: 0,
    };
  }

  entries.sort((left, right) =>
    compareCursor(left.sequence, right.sequence),
  );

  let removeCount = 0;

  while (
    removeCount < entries.length &&
    (
      entries.length - removeCount > MAX_ENTRIES ||
      bytes > MAX_BYTES
    )
  ) {
    const entry = entries[removeCount];

    if (!entry) {
      break;
    }

    bytes -= utf8Bytes(entry.message);

    removeCount += 1;
  }

  return {
    buffer: {
      entries:
        removeCount > 0
          ? entries.slice(removeCount)
          : entries,

      bytes: Math.max(0, bytes),

      evicted:
        previous.evicted || removeCount > 0,
    },

    added,
  };
}

// ==================================================
// Presentation helpers
// ==================================================

function levelClass(level: Level): string {
  switch (level) {
    case "ERROR":
      return "text-red-400";

    case "WARN":
      return "text-amber-400";

    case "INFO":
      return "text-sky-400";

    case "DEBUG":
      return "text-zinc-400";

    default:
      return "text-zinc-500";
  }
}

function formatTimestamp(value: string): string {
  const timestamp = new Date(value);

  if (!Number.isFinite(timestamp.getTime())) {
    return "--:--:--.---";
  }

  return timestamp.toISOString().slice(11, 23);
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }

  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KiB`;
  }

  return `${(
    bytes /
    (1024 * 1024)
  ).toFixed(1)} MiB`;
}

function formatLogText(
  entries: readonly LogEntry[],
): string {
  return entries
    .map(
      (entry) =>
        `${entry.eventTimestamp} ` +
        `[${entry.source}:${entry.stream}:${entry.level}] ` +
        entry.message,
    )
    .join("\n");
}

// ==================================================
// Public terminal component
//
// The scope-specific key prevents another workspace
// or deployment from reusing old terminal state.
// ==================================================

export default function LogTerminal({
  deploymentId,
  projectId,
}: {
  deploymentId: string | null;
  projectId: string | null;
}) {
  const { activeWorkspace } = useDashboard();

  const workspaceId = activeWorkspace?.id ?? null;

  if (
    !isUuid(workspaceId) ||
    !isUuid(projectId) ||
    !isUuid(deploymentId)
  ) {
    return (
      <DashboardPageShell breadcrumb="Logs">
        <div className="flex min-h-64 flex-col items-center justify-center gap-3 rounded-xl border p-6 text-center">
          <Terminal className="size-8 text-zinc-400" />

          <h2 className="text-sm font-semibold">
            No deployment selected
          </h2>

          <p className="max-w-sm text-sm text-zinc-500 dark:text-zinc-400">
            Select a deployment and choose View Logs to
            inspect its build, runtime and system output.
          </p>
        </div>
      </DashboardPageShell>
    );
  }

  return (
    <ScopedLogTerminal
      key={`${workspaceId}:${projectId}:${deploymentId}`}
      workspaceId={workspaceId}
      projectId={projectId}
      deploymentId={deploymentId}
    />
  );
}

// ==================================================
// Scoped terminal
// ==================================================

function ScopedLogTerminal({
  workspaceId,
  projectId,
  deploymentId,
}: LogScope) {
  const scope = useMemo<LogScope>(
    () => ({
      workspaceId,
      projectId,
      deploymentId,
    }),
    [workspaceId, projectId, deploymentId],
  );

  const historyPath =
    `/projects/${encodeURIComponent(projectId)}` +
    `/deployments/${encodeURIComponent(deploymentId)}/logs`;

  // ==================================================
  // UI state
  // ==================================================

  const [display, setDisplay] =
    useState<LogBuffer>(createEmptyBuffer);

  const [connection, setConnection] =
    useState<Connection>("disconnected");

  const [loading, setLoading] = useState(false);

  const [error, setError] = useState("");

  const [notice, setNotice] = useState("");

  const [tab, setTab] =
    useState<"ALL" | Source>("ALL");

  const [level, setLevel] =
    useState<"ALL" | Level>("ALL");

  const [search, setSearch] = useState("");

  const [autoScroll, setAutoScroll] =
    useState(true);

  const [paused, setPaused] = useState(false);

  const [pendingCount, setPendingCount] =
    useState(0);

  const [retentionTruncated, setRetentionTruncated] =
    useState(false);

  const [gapDetected, setGapDetected] =
    useState(false);

  const [copied, setCopied] = useState(false);

  const [accessDenied, setAccessDenied] =
    useState(false);

  const deferredSearch = useDeferredValue(search);

  // ==================================================
  // Mutable ingestion state
  // ==================================================

  const socketRef = useRef<Socket | null>(null);

  const scrollRef =
    useRef<HTMLDivElement | null>(null);

  const bufferRef =
    useRef<LogBuffer>(createEmptyBuffer());

  const cursorRef = useRef<string | null>(null);

  const clearCursorRef =
    useRef<string | null>(null);

  const pausedRef = useRef(false);

  const pendingCountRef = useRef(0);

  const autoScrollRef = useRef(true);

  const generationRef = useRef(0);

  const accessDeniedRef = useRef(false);

  const refreshRef =
    useRef<(() => Promise<void>) | null>(null);

  const renderTimerRef =
    useRef<ReturnType<typeof setTimeout> | null>(
      null,
    );

  const copyTimerRef =
    useRef<ReturnType<typeof setTimeout> | null>(
      null,
    );

  const noticeTimerRef =
    useRef<ReturnType<typeof setTimeout> | null>(
      null,
    );

  // ==================================================
  // Batched display publication
  // ==================================================

  const publishDisplay = useCallback(() => {
    if (
      renderTimerRef.current !== null ||
      accessDeniedRef.current
    ) {
      return;
    }

    const generation = generationRef.current;

    renderTimerRef.current = setTimeout(() => {
      renderTimerRef.current = null;

      if (
        generation !== generationRef.current ||
        accessDeniedRef.current
      ) {
        return;
      }

      if (pausedRef.current) {
        setPendingCount(
          pendingCountRef.current,
        );

        return;
      }

      setDisplay(bufferRef.current);

      pendingCountRef.current = 0;

      setPendingCount(0);
    }, RENDER_INTERVAL_MS);
  }, []);

  // ==================================================
  // Merge validated entries
  // ==================================================

  const mergeEntries = useCallback(
    (incoming: readonly LogEntry[]) => {
      if (accessDeniedRef.current) {
        return;
      }

      const clearCursor = clearCursorRef.current;

      const accepted =
        clearCursor === null
          ? incoming
          : incoming.filter(
              (entry) =>
                compareCursor(
                  entry.sequence,
                  clearCursor,
                ) > 0,
            );

      const result = mergeLogBuffer(
        bufferRef.current,
        accepted,
      );

      bufferRef.current = result.buffer;

      if (pausedRef.current) {
        pendingCountRef.current = Math.min(
          Number.MAX_SAFE_INTEGER,
          pendingCountRef.current + result.added,
        );
      }

      if (result.added > 0) {
        publishDisplay();
      }
    },
    [publishDisplay],
  );

  // ==================================================
  // Cursor management
  //
  // Only advance to entries actually received.
  // A server high watermark alone is not proof that
  // every preceding entry was delivered.
  // ==================================================

  const advanceCursor = useCallback(
    (candidate: string | null) => {
      if (
        candidate === null ||
        parseCursor(candidate) === null
      ) {
        return;
      }

      const current = cursorRef.current;

      if (
        current === null ||
        compareCursor(candidate, current) > 0
      ) {
        cursorRef.current = candidate;
      }
    },
    [],
  );

  // ==================================================
  // Socket lifecycle and REST history recovery
  // ==================================================

  useEffect(() => {
    const generation = ++generationRef.current;

    let cancelled = false;

    let recovering = false;

    let initialized = false;

    let recoveryRequired = false;

    let recoveryTask: Promise<void> | null = null;

    let socket: Socket | null = null;

    const isCurrent = () =>
      !cancelled &&
      generation === generationRef.current &&
      !accessDeniedRef.current;

    // ==================================================
    // Fetch bounded retained history
    // ==================================================

    async function loadHistory(
      initial: boolean,
    ): Promise<boolean> {
      if (!isCurrent()) {
        return false;
      }

      setLoading(true);

      try {
        let after = initial
          ? null
          : cursorRef.current;

        for (
          let page = 0;
          page < MAX_CATCHUP_PAGES;
          page += 1
        ) {
          if (!isCurrent()) {
            return false;
          }

          const params = new URLSearchParams({
            limit: String(HISTORY_PAGE_SIZE),
          });

          if (after !== null) {
            params.set("after", after);
          }

          const raw = await apiFetch<unknown>(
            `${historyPath}?${params.toString()}`,
          );

          if (!isCurrent()) {
            return false;
          }

          if (!validateHistory(raw, scope)) {
            throw new Error(
              "Invalid log history response.",
            );
          }

          const ordered = raw.items
            .slice()
            .sort((left, right) =>
              compareCursor(
                left.sequence,
                right.sequence,
              ),
            );

          if (
            after !== null &&
            ordered.some(
              (entry) =>
                compareCursor(
                  entry.sequence,
                  after!,
                ) <= 0,
            )
          ) {
            throw new Error(
              "Invalid history cursor progression.",
            );
          }

          if (raw.retentionTruncated) {
            setRetentionTruncated(true);
          }

          mergeEntries(ordered);

          const last =
            ordered[ordered.length - 1];

          if (last) {
            advanceCursor(last.sequence);
          }

          // Initial load uses the API's default page.
          // Socket replay continues from its last entry.
          if (initial) {
            setError("");

            return true;
          }

          if (!raw.hasMore) {
            setError("");

            return true;
          }

          const next = raw.nextCursor;

          if (
            !last ||
            next === null ||
            compareCursor(
              next,
              last.sequence,
            ) !== 0 ||
            (
              after !== null &&
              compareCursor(next, after) <= 0
            )
          ) {
            throw new Error(
              "Invalid log pagination cursor.",
            );
          }

          after = next;
        }

        if (isCurrent()) {
          setGapDetected(true);

          setNotice(
            "History catch-up reached its safety limit. " +
            "Refresh to continue loading retained logs.",
          );
        }

        return false;
      } catch {
        if (isCurrent()) {
          setError(
            "Could not load deployment logs. " +
            "Check your connection and access permissions.",
          );
        }

        return false;
      } finally {
        if (isCurrent()) {
          setLoading(false);
        }
      }
    }

    // ==================================================
    // Subscribe from the last received cursor
    // ==================================================

    function subscribe() {
      if (
        !isCurrent() ||
        !socket?.connected ||
        recovering
      ) {
        return;
      }

      const lastCursor = cursorRef.current;

      setConnection("connecting");

      socket.emit(
        "deployment:logs:subscribe",
        {
          deploymentId,
          projectId,

          ...(lastCursor !== null
            ? { lastCursor }
            : {}),
        },
      );
    }

    // ==================================================
    // Serialized history recovery
    // ==================================================

    function recoverHistory(): Promise<void> {
      if (!isCurrent()) {
        return Promise.resolve();
      }

      if (recoveryTask) {
        return recoveryTask;
      }

      recovering = true;

      recoveryRequired = true;

      setConnection("reconnecting");

      if (socket?.connected) {
        socket.emit(
          "deployment:logs:unsubscribe",
          {
            deploymentId,
            projectId,
          },
        );
      }

      const task = Promise.resolve()
        .then(async () => {
          const recovered = await loadHistory(false);

          if (!isCurrent()) {
            return;
          }

          recoveryRequired = !recovered;

          if (recovered) {
            setGapDetected(false);
          } else {
            setConnection("disconnected");
          }
        })
        .finally(() => {
          recovering = false;

          if (recoveryTask === task) {
            recoveryTask = null;
          }

          if (
            isCurrent() &&
            !recoveryRequired &&
            socket?.connected
          ) {
            subscribe();
          }
        });

      recoveryTask = task;

      return task;
    }

    // ==================================================
    // Clear data when server reports access denial
    // ==================================================

    function denyAccess() {
      accessDeniedRef.current = true;

      bufferRef.current = createEmptyBuffer();

      cursorRef.current = null;

      clearCursorRef.current = null;

      pendingCountRef.current = 0;

      pausedRef.current = false;

      if (renderTimerRef.current !== null) {
        clearTimeout(renderTimerRef.current);

        renderTimerRef.current = null;
      }

      setDisplay(createEmptyBuffer());

      setPendingCount(0);

      setPaused(false);

      setLoading(false);

      setCopied(false);

      setNotice("");

      setRetentionTruncated(false);

      setGapDetected(false);

      setAccessDenied(true);

      setConnection("disconnected");

      setError(
        "Access to deployment logs is no longer available.",
      );

      socket?.disconnect();
    }

    // ==================================================
    // Manual refresh callback
    // ==================================================

    refreshRef.current = async () => {
      if (!isCurrent() || !initialized) {
        return;
      }

      await recoverHistory();

      if (
        isCurrent() &&
        socket &&
        !socket.connected &&
        !recoveryRequired
      ) {
        socket.connect();
      }
    };

    // ==================================================
    // Initialize history before opening live stream
    // ==================================================

    async function initialize() {
      if (!isCurrent()) {
        return;
      }

      setConnection("connecting");

      setError("");

      const loaded = await loadHistory(true);

      if (!isCurrent()) {
        return;
      }

      recoveryRequired = !loaded;

      const client = io(API_ORIGIN, {
        withCredentials: true,

        transports: ["websocket"],

        autoConnect: false,

        reconnection: true,

        reconnectionDelay: RECONNECT_DELAY_MS,

        reconnectionDelayMax:
          MAX_RECONNECT_DELAY_MS,

        randomizationFactor: 0.5,

        timeout: SOCKET_TIMEOUT_MS,

        forceNew: true,
      });

      socket = client;

      socketRef.current = client;

      initialized = true;

      // ==================================================
      // Connection events
      // ==================================================

      client.on("connect", () => {
        if (!isCurrent()) {
          return;
        }

        if (recovering) {
          return;
        }

        if (recoveryRequired) {
          void recoverHistory();

          return;
        }

        subscribe();
      });

      client.on("disconnect", () => {
        if (!isCurrent()) {
          return;
        }

        setConnection("disconnected");
      });

      client.on("connect_error", () => {
        if (!isCurrent()) {
          return;
        }

        setConnection(
          client.active
            ? "reconnecting"
            : "disconnected",
        );

        setError(
          "Live log connection unavailable. " +
          "Previously loaded logs remain visible.",
        );
      });

      client.io.on("reconnect_attempt", () => {
        if (isCurrent()) {
          setConnection("reconnecting");
        }
      });

      client.io.on("reconnect_failed", () => {
        if (isCurrent()) {
          setConnection("disconnected");
        }
      });

      // ==================================================
      // Validated live log batches
      // ==================================================

      client.on(
        "deployment:logs:batch",
        (payload: unknown) => {
          if (
            !isCurrent() ||
            recovering ||
            recoveryRequired
          ) {
            return;
          }

          if (!validateBatch(payload, scope)) {
            setError(
              "An invalid live log batch was rejected.",
            );

            setGapDetected(true);

            void recoverHistory();

            return;
          }

          const current = cursorRef.current;

          const fresh =
            current === null
              ? payload.entries
              : payload.entries.filter(
                  (entry) =>
                    compareCursor(
                      entry.sequence,
                      current,
                    ) > 0,
                );

          let sequenceGap = false;

          if (
            current !== null &&
            fresh.length > 0
          ) {
            let expected =
              BigInt(current) + BigInt(1);

            for (const entry of fresh) {
              if (
                BigInt(entry.sequence) !== expected
              ) {
                sequenceGap = true;

                break;
              }

              expected += BigInt(1);
            }
          }

          if (
            payload.gapDetected ||
            sequenceGap
          ) {
            setGapDetected(true);

            void recoverHistory();

            return;
          }

          mergeEntries(fresh);

          const last =
            fresh[fresh.length - 1];

          if (last) {
            advanceCursor(last.sequence);
          }

          setConnection("live");

          setError("");
        },
      );

      // ==================================================
      // Subscription errors
      // ==================================================

      client.on(
        "deployment:logs:error",
        (payload: unknown) => {
          if (
            !isCurrent() ||
            !isObject(payload)
          ) {
            return;
          }

          if (
            payload.deploymentId !== null &&
            (
              !isUuid(payload.deploymentId) ||
              !sameUuid(
                payload.deploymentId,
                deploymentId,
              )
            )
          ) {
            return;
          }

          const code =
            typeof payload.code === "string" &&
            payload.code.length <= 64
              ? payload.code
              : "";

          const permissionError =
            /FORBIDDEN|UNAUTHORIZED|ACCESS_DENIED|PERMISSION|AUTH_REQUIRED|ACCESS_REVOKED/.test(
              code,
            );

          if (permissionError) {
            denyAccess();

            return;
          }

          setError(
            "The live log subscription encountered an error.",
          );

          recoveryRequired = true;

          setConnection("disconnected");
        },
      );

      // ==================================================
      // Server-requested resynchronization
      // ==================================================

      client.on(
        "deployment:logs:resync",
        (payload: unknown) => {
          if (
            !isCurrent() ||
            !isObject(payload) ||
            !isUuid(payload.deploymentId) ||
            !sameUuid(
              payload.deploymentId,
              deploymentId,
            )
          ) {
            return;
          }

          setGapDetected(true);

          void recoverHistory();
        },
      );

      client.connect();
    }

    void initialize().catch(() => {
      if (isCurrent()) {
        setConnection("disconnected");

        setError(
          "Could not initialize the live log terminal.",
        );
      }
    });

    // ==================================================
    // Cleanup
    // ==================================================

    return () => {
      cancelled = true;

      generationRef.current += 1;

      refreshRef.current = null;

      if (socket) {
        if (socket.connected) {
          socket.emit(
            "deployment:logs:unsubscribe",
            {
              deploymentId,
              projectId,
            },
          );
        }

        socket.removeAllListeners();

        socket.io.removeAllListeners();

        socket.disconnect();
      }

      socketRef.current = null;

      if (renderTimerRef.current !== null) {
        clearTimeout(renderTimerRef.current);

        renderTimerRef.current = null;
      }

      if (copyTimerRef.current !== null) {
        clearTimeout(copyTimerRef.current);

        copyTimerRef.current = null;
      }

      if (noticeTimerRef.current !== null) {
        clearTimeout(noticeTimerRef.current);

        noticeTimerRef.current = null;
      }
    };
  }, [
    scope,
    deploymentId,
    projectId,
    historyPath,
    mergeEntries,
    advanceCursor,
  ]);

  // ==================================================
  // Pause and resume display
  // ==================================================

  const togglePause = useCallback(() => {
    if (accessDeniedRef.current) {
      return;
    }

    const next = !pausedRef.current;

    pausedRef.current = next;

    setPaused(next);

    if (!next) {
      setDisplay(bufferRef.current);

      pendingCountRef.current = 0;

      setPendingCount(0);
    }
  }, []);

  // ==================================================
  // Auto-scroll
  // ==================================================

  useEffect(() => {
    if (!autoScroll || paused) {
      return;
    }

    const node = scrollRef.current;

    if (node) {
      node.scrollTop = node.scrollHeight;
    }
  }, [display.entries, autoScroll, paused]);

  const onScroll = useCallback(() => {
    const node = scrollRef.current;

    if (!node || pausedRef.current) {
      return;
    }

    const atBottom =
      node.scrollHeight -
        node.scrollTop -
        node.clientHeight <
      40;

    if (autoScrollRef.current !== atBottom) {
      autoScrollRef.current = atBottom;

      setAutoScroll(atBottom);
    }
  }, []);

  const jumpToLatest = useCallback(() => {
    if (accessDeniedRef.current) {
      return;
    }

    pausedRef.current = false;

    autoScrollRef.current = true;

    pendingCountRef.current = 0;

    setPaused(false);

    setAutoScroll(true);

    setPendingCount(0);

    setDisplay(bufferRef.current);

    const node = scrollRef.current;

    if (node) {
      node.scrollTop = node.scrollHeight;
    }
  }, []);

  // ==================================================
  // Local filters and deferred search
  // ==================================================

  const filtered = useMemo(() => {
    const query = deferredSearch
      .trim()
      .toLowerCase();

    return display.entries.filter((entry) => {
      if (
        tab !== "ALL" &&
        entry.source !== tab
      ) {
        return false;
      }

      if (
        level !== "ALL" &&
        entry.level !== level
      ) {
        return false;
      }

      if (
        query &&
        !entry.message.toLowerCase().includes(query)
      ) {
        return false;
      }

      return true;
    });
  }, [
    display.entries,
    tab,
    level,
    deferredSearch,
  ]);

  // ==================================================
  // Temporary action feedback
  // ==================================================

  const showNotice = useCallback(
    (message: string) => {
      if (accessDeniedRef.current) {
        return;
      }

      if (noticeTimerRef.current !== null) {
        clearTimeout(noticeTimerRef.current);
      }

      setNotice(message);

      const generation = generationRef.current;

      noticeTimerRef.current = setTimeout(() => {
        noticeTimerRef.current = null;

        if (
          generation === generationRef.current &&
          !accessDeniedRef.current
        ) {
          setNotice("");
        }
      }, 4_000);
    },
    [],
  );

  // ==================================================
  // Clipboard
  // ==================================================

  const copyAll = useCallback(async () => {
    if (
      filtered.length === 0 ||
      accessDeniedRef.current
    ) {
      return;
    }

    const generation = generationRef.current;

    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error(
          "Clipboard API unavailable.",
        );
      }

      await navigator.clipboard.writeText(
        formatLogText(filtered),
      );

      if (
        generation !== generationRef.current ||
        accessDeniedRef.current
      ) {
        return;
      }

      setCopied(true);

      showNotice(
        `${filtered.length.toLocaleString()} visible logs copied.`,
      );

      if (copyTimerRef.current !== null) {
        clearTimeout(copyTimerRef.current);
      }

      copyTimerRef.current = setTimeout(() => {
        copyTimerRef.current = null;

        if (
          generation === generationRef.current &&
          !accessDeniedRef.current
        ) {
          setCopied(false);
        }
      }, 2_000);
    } catch {
      if (
        generation === generationRef.current &&
        !accessDeniedRef.current
      ) {
        showNotice(
          "Clipboard access failed. Download the logs instead.",
        );
      }
    }
  }, [filtered, showNotice]);

  // ==================================================
  // Download visible filtered logs
  //
  // Local text export avoids depending on an
  // unverified server download endpoint.
  // ==================================================

  const download = useCallback(() => {
    if (
      filtered.length === 0 ||
      accessDeniedRef.current
    ) {
      return;
    }

    let url: string | null = null;

    let anchor: HTMLAnchorElement | null = null;

    try {
      const blob = new Blob(
        [formatLogText(filtered)],
        {
          type: "text/plain;charset=utf-8",
        },
      );

      url = URL.createObjectURL(blob);

      anchor = document.createElement("a");

      anchor.href = url;

      anchor.download =
        `cloudpilot-${deploymentId.slice(0, 8)}-logs.txt`;

      document.body.appendChild(anchor);

      anchor.click();

      showNotice(
        "Visible filtered logs exported.",
      );
    } catch {
      showNotice(
        "Could not export the visible logs.",
      );
    } finally {
      anchor?.remove();

      if (url !== null) {
        const objectUrl = url;

        setTimeout(() => {
          URL.revokeObjectURL(objectUrl);
        }, 1_000);
      }
    }
  }, [
    filtered,
    deploymentId,
    showNotice,
  ]);

  // ==================================================
  // Clear local display
  //
  // Persisted database logs are not deleted.
  // ==================================================

  const clearLocal = useCallback(() => {
    if (accessDeniedRef.current) {
      return;
    }

    clearCursorRef.current =
      cursorRef.current;

    bufferRef.current =
      createEmptyBuffer();

    pendingCountRef.current = 0;

    setDisplay(createEmptyBuffer());

    setPendingCount(0);

    showNotice(
      "Local display cleared. Stored logs were not deleted.",
    );
  }, [showNotice]);

  // ==================================================
  // Manual history refresh
  // ==================================================

  const reloadHistory = useCallback(async () => {
    if (accessDeniedRef.current) {
      return;
    }

    await refreshRef.current?.();
  }, []);

  // ==================================================
  // Responsive terminal interface
  // ==================================================

  return (
    <DashboardPageShell
      breadcrumb="Logs"
      title="Deployment logs"
      description={
        `Build, runtime and system output for deployment ` +
        `${deploymentId.slice(0, 8)}.`
      }
    >
      <div className="flex w-full min-w-0 flex-col gap-4">

        {/* Toolbar */}

        <div className="flex min-w-0 flex-col gap-3 rounded-xl border bg-white p-3 dark:bg-zinc-950 sm:p-4">

          {/* Connection status */}

          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex min-w-0 items-center gap-2">
              <Terminal className="size-4 shrink-0 text-zinc-500" />

              <span className="text-sm font-semibold">
                Log terminal
              </span>
            </div>

            <div
              role="status"
              className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400"
            >
              <span
                className={
                  `size-2 shrink-0 rounded-full ` +
                  CONNECTION_COLORS[connection]
                }
              />

              {CONNECTION_LABELS[connection]}
            </div>
          </div>

          {/* Source filters */}

          <div
            role="group"
            aria-label="Log source filters"
            className="grid grid-cols-2 gap-1 rounded-lg border p-1 sm:flex sm:w-fit sm:flex-wrap"
          >
            {SOURCE_TABS.map((item) => (
              <button
                key={item.key}
                type="button"
                aria-pressed={tab === item.key}
                onClick={() => setTab(item.key)}
                className={
                  "min-h-9 rounded-md px-3 py-1.5 " +
                  "text-xs font-medium transition-colors " +
                  (
                    tab === item.key
                      ? "bg-zinc-900 text-white dark:bg-zinc-100 dark:text-zinc-900"
                      : "text-zinc-600 hover:bg-zinc-100 dark:text-zinc-300 dark:hover:bg-zinc-800"
                  )
                }
              >
                {item.label}
              </button>
            ))}
          </div>

          {/* Search and action controls */}

          <div className="flex min-w-0 flex-col gap-2 lg:flex-row lg:items-center">

            <div className="flex min-w-0 flex-col gap-2 sm:flex-row">

              <div className="relative min-w-0 flex-1">
                <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-zinc-400" />

                <input
                  aria-label="Search visible logs"
                  value={search}
                  maxLength={MAX_SEARCH_LENGTH}
                  onChange={(event) =>
                    setSearch(event.target.value)
                  }
                  placeholder="Search logs..."
                  className="h-9 w-full min-w-0 rounded-md border bg-white pl-9 pr-3 text-xs outline-none focus:ring-2 focus:ring-blue-500 dark:bg-zinc-950 sm:w-56"
                />
              </div>

              <select
                aria-label="Filter by log level"
                value={level}
                onChange={(event) => {
                  const value =
                    event.target.value;

                  if (
                    value === "ALL" ||
                    LEVELS.has(value)
                  ) {
                    setLevel(
                      value as "ALL" | Level,
                    );
                  }
                }}
                className="h-9 w-full rounded-md border bg-white px-3 text-xs dark:bg-zinc-950 sm:w-32"
              >
                <option value="ALL">
                  All levels
                </option>

                <option value="ERROR">
                  Error
                </option>

                <option value="WARN">
                  Warning
                </option>

                <option value="INFO">
                  Info
                </option>

                <option value="DEBUG">
                  Debug
                </option>

                <option value="UNKNOWN">
                  Unknown
                </option>
              </select>
            </div>

            {/* Terminal actions */}

            <div className="flex flex-wrap items-center gap-2 lg:ml-auto">

              <button
                type="button"
                aria-label={
                  paused
                    ? "Resume display"
                    : "Pause display"
                }
                aria-pressed={paused}
                title={
                  paused
                    ? "Resume display"
                    : "Pause display"
                }
                disabled={accessDenied}
                onClick={togglePause}
                className={ACTION_CLASS}
              >
                {paused ? (
                  <Play className="size-4" />
                ) : (
                  <Pause className="size-4" />
                )}
              </button>

              <button
                type="button"
                aria-label="Jump to latest log"
                title="Jump to latest"
                disabled={accessDenied}
                onClick={jumpToLatest}
                className={ACTION_CLASS}
              >
                <ArrowDown className="size-4" />
              </button>

              <button
                type="button"
                aria-label="Copy visible logs"
                title="Copy visible logs"
                disabled={
                  filtered.length === 0 ||
                  accessDenied
                }
                onClick={() => void copyAll()}
                className={ACTION_CLASS}
              >
                {copied ? (
                  <Check className="size-4 text-emerald-500" />
                ) : (
                  <ClipboardCopy className="size-4" />
                )}
              </button>

              <button
                type="button"
                aria-label="Download visible logs"
                title="Download visible logs"
                disabled={
                  filtered.length === 0 ||
                  accessDenied
                }
                onClick={download}
                className={ACTION_CLASS}
              >
                <ArrowDownToLine className="size-4" />
              </button>

              <button
                type="button"
                aria-label="Clear local log display"
                title="Clear local display"
                disabled={accessDenied}
                onClick={clearLocal}
                className={ACTION_CLASS}
              >
                <Trash2 className="size-4" />
              </button>

              <button
                type="button"
                aria-label="Refresh retained logs"
                title="Refresh retained logs"
                disabled={
                  loading ||
                  accessDenied
                }
                onClick={() =>
                  void reloadHistory()
                }
                className={ACTION_CLASS}
              >
                <RefreshCw
                  className={
                    `size-4 ${
                      loading
                        ? "animate-spin"
                        : ""
                    }`
                  }
                />
              </button>
            </div>
          </div>
        </div>

        {/* Pause notification */}

        {paused && (
          <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200">
            <span>
              Display paused.

              {pendingCount > 0 &&
                ` ${pendingCount.toLocaleString()} new logs received.`}
            </span>

            <button
              type="button"
              onClick={jumpToLatest}
              className="font-semibold underline underline-offset-2"
            >
              Resume live display
            </button>
          </div>
        )}

        {/* Server retention warning */}

        {retentionTruncated && (
          <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200">
            <AlertTriangle className="mt-0.5 size-4 shrink-0" />

            <span>
              Some earlier logs are no longer available
              under the server retention policy.
            </span>
          </div>
        )}

        {/* Local memory warning */}

        {display.evicted && (
          <div className="rounded-lg border px-3 py-2 text-xs text-zinc-500 dark:text-zinc-400">
            Older entries were removed from the local
            display to keep browser memory bounded.
            This does not delete stored logs.
          </div>
        )}

        {/* Sequence gap warning */}

        {gapDetected && (
          <div
            role="status"
            className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200"
          >
            A log sequence gap was detected.
            Retained history may need to be refreshed.
          </div>
        )}

        {/* Error messages */}

        {error && (
          <div
            role="alert"
            className="break-words rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200"
          >
            {error}
          </div>
        )}

        {/* Action feedback */}

        {notice && (
          <div
            role="status"
            className="rounded-lg border px-3 py-2 text-xs text-zinc-600 dark:text-zinc-300"
          >
            {notice}
          </div>
        )}

        {/* Terminal viewport */}

        <div className="relative min-w-0 overflow-hidden rounded-xl border border-zinc-800 bg-zinc-950">

          {/* Terminal header */}

          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-zinc-800 bg-zinc-900 px-3 py-2">

            <div className="flex items-center gap-1.5">
              <span className="size-2 rounded-full bg-red-500" />

              <span className="size-2 rounded-full bg-amber-500" />

              <span className="size-2 rounded-full bg-emerald-500" />

              <span className="ml-2 font-mono text-[11px] text-zinc-400">
                cloudpilot / logs
              </span>
            </div>

            <span className="font-mono text-[10px] text-zinc-400">
              {accessDenied
                ? "ACCESS DENIED"
                : paused
                  ? "PAUSED"
                  : autoScroll
                    ? "FOLLOWING"
                    : "SCROLL LOCK"}
            </span>
          </div>

          {/* Log output */}

          <div
            ref={scrollRef}
            onScroll={onScroll}
            role="region"
            aria-label="Deployment log output"
            tabIndex={0}
            className="h-[55vh] min-h-64 max-h-[75vh] overflow-auto overscroll-contain p-3 font-mono text-[11px] leading-5 text-zinc-100 outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-blue-500 sm:h-[60vh] sm:text-xs"
          >
            {accessDenied ? (
              <div className="flex h-full flex-col items-center justify-center gap-3 px-4 text-center text-zinc-400">
                <AlertTriangle className="size-8 text-amber-400" />

                <span>
                  You no longer have access to these logs.
                </span>
              </div>
            ) : loading && filtered.length === 0 ? (
              <div className="flex h-full items-center justify-center gap-2 text-zinc-400">
                <RefreshCw className="size-4 animate-spin" />

                Loading logs…
              </div>
            ) : filtered.length === 0 ? (
              <div className="flex h-full flex-col items-center justify-center gap-3 px-4 text-center text-zinc-500">
                <Terminal className="size-8" />

                <span>
                  {display.entries.length > 0
                    ? "No logs match the current filters."
                    : "No log entries yet."}
                </span>

                {display.entries.length > 0 && (
                  <button
                    type="button"
                    onClick={() => {
                      setSearch("");

                      setTab("ALL");

                      setLevel("ALL");
                    }}
                    className="text-sky-400 underline underline-offset-2"
                  >
                    Reset filters
                  </button>
                )}
              </div>
            ) : (
              filtered.map((entry) => (
                <div
                  key={entry.id}
                  className="flex min-w-0 flex-wrap items-baseline gap-x-2 rounded px-1 py-0.5 hover:bg-white/5 sm:flex-nowrap"
                >
                  <span
                    title={entry.eventTimestamp}
                    className="shrink-0 select-none text-zinc-500"
                  >
                    {formatTimestamp(
                      entry.eventTimestamp,
                    )}
                  </span>

                  <span
                    className={
                      `w-14 shrink-0 font-medium ` +
                      levelClass(entry.level)
                    }
                  >
                    {entry.level}
                  </span>

                  <span
                    title={
                      `${entry.source} / ${entry.stream}`
                    }
                    className="shrink-0 text-zinc-500"
                  >
                    [{entry.source[0]}
                    {entry.stream[0]}]
                  </span>

                  <span className="min-w-0 basis-full whitespace-pre-wrap break-words text-zinc-200 [overflow-wrap:anywhere] sm:flex-1 sm:basis-auto">
                    {entry.message}
                  </span>
                </div>
              ))
            )}
          </div>

          {/* Floating jump-to-latest control */}

          {!autoScroll &&
            !paused &&
            !accessDenied && (
              <button
                type="button"
                onClick={jumpToLatest}
                className="absolute bottom-4 right-4 inline-flex items-center gap-2 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-2 text-xs font-medium text-zinc-100 shadow-lg hover:bg-zinc-800"
              >
                <ArrowDown className="size-3.5" />

                Latest logs
              </button>
            )}
        </div>

        {/* Terminal statistics */}

        <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 text-[11px] text-zinc-500 dark:text-zinc-400">

          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <span>
              {filtered.length.toLocaleString()} visible
            </span>

            <span>
              {display.entries.length.toLocaleString()} buffered
            </span>

            <span>
              {formatBytes(display.bytes)}
            </span>
          </div>

          <span>
            Limit: {MAX_ENTRIES.toLocaleString()} entries
            {" / "}
            {formatBytes(MAX_BYTES)}
          </span>
        </div>
      </div>
    </DashboardPageShell>
  );
}