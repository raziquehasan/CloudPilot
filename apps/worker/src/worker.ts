import { randomUUID } from "node:crypto";

import {
  QUEUE_NAMES,
  QueueManager,
  createWorkerConnection,
  publishWorkerHeartbeat,
  type DiagnosticJob,
  type QueueJob,
  queuePayloadSchemas,
} from "@cloudpilot/queue";

import {
  UnrecoverableError,
  type Job,
} from "bullmq";

import { workerEnv } from "./config/env.js";
import { workerMetrics } from "./metrics.js";
import { workerLogger } from "./config/logger.js";

import {
  processDiagnosticJob,
} from "./processors/diagnostic.processor.js";

import {
  createWorkerGitHubAuth,
  createWorkspaceManager,
  processDeploymentJob,
} from "./processors/deployment.processor.js";

import {
  processBuildJob,
} from "./processors/build.processor.js";

import {
  processRuntimeJob,
  reconcileRuntimeInstances,
} from "./processors/runtime.processor.js";

import {
  BuildKitExecutor,
} from "./build/buildkit-client.js";

import {
  DockerRuntimeExecutor,
} from "./runtime/docker-runtime-client.js";

import {
  RuntimeLogCollectorManager,
} from "./logging/runtime-log-manager.js";

import { runLogRetention } from "./logging/retention.js";
import { startDeploymentHealthMonitor } from "./health/health-monitor-runtime.js";
import type { RecoveryJobQueue } from "./health/auto-recovery-publisher.js";

// ==================================================
// Constants
// ==================================================

const WORKER_VERSION = "0.1.0";

const SHUTDOWN_TIMEOUT_MS = 30_000;

const HEARTBEAT_SHUTDOWN_TIMEOUT_MS = 2_000;

/**
 * Startup and maintenance safety limits.
 *
 * Timeout wrappers bound waiting, but do not guarantee
 * cancellation of the underlying asynchronous operation.
 */
const STARTUP_TIMEOUT_MS = 60_000;

const REDIS_SHUTDOWN_TIMEOUT_MS = 3_000;

const BACKGROUND_SHUTDOWN_TIMEOUT_MS = 8_000;

/**
 * Runtime collector memory budgets.
 *
 * These limits apply to each worker process, not the
 * entire distributed CloudPilot installation.
 */
const RUNTIME_LOG_MAX_COLLECTORS = 16;

const RUNTIME_LOG_MAX_BUFFER_BYTES = 8 * 1024 * 1024;

const RUNTIME_LOG_TOTAL_BUFFER_BYTES = 128 * 1024 * 1024;

const RUNTIME_LOG_MAX_BUFFERED_ENTRIES = 20_000;

type WorkerRole = "diagnostic" | "deployment";

type Timer = ReturnType<typeof setTimeout>;

// ==================================================
// Worker identity and lifecycle
// ==================================================

const workerId = `worker-${randomUUID()}`;

const startupAt = new Date().toISOString();

const shutdownController = new AbortController();

let shuttingDown = false;

let startupCompleted = false;

let shutdownExitCode = 0;

let closeMetrics: (() => Promise<void>) | undefined;
let shutdownPromise: Promise<void> | null = null;

let heartbeatTimer: Timer | undefined;

let cleanupTimer: Timer | undefined;

let logReconcileTimer: Timer | undefined;

let logRetentionTimer: Timer | undefined;

let heartbeatTask: Promise<void> | null = null;

let maintenanceTask: Promise<void> | null = null;

let logReconcileTask: Promise<void> | null = null;

let logRetentionTask: Promise<void> | null = null;

let workspaceStorageAvailable = false;

async function refreshWorkspaceStorage(): Promise<void> {
  try {
    await workspaceManager.ensureRoot();

    workspaceStorageAvailable = true;
  } catch {
    workspaceStorageAvailable = false;
  }
}

// ==================================================
// Shared dependencies
// ==================================================

const workerConnection = createWorkerConnection(
  workerEnv.REDIS_URL,
);

const sourceAuth = createWorkerGitHubAuth();

const workspaceManager = createWorkspaceManager();

const sourceRetrievalConfigured =
  sourceAuth !== null;

const heartbeatOptions = {
  prefix: workerEnv.QUEUE_PREFIX,

  ttlSeconds:
    workerEnv.WORKER_HEARTBEAT_TTL_SECONDS,
};

const queueManager = new QueueManager(
  workerConnection,

  workerConnection,

  {
    attempts:
      workerEnv.QUEUE_JOB_ATTEMPTS,

    backoffMs:
      workerEnv.QUEUE_BACKOFF_MS,

    completedRetention:
      workerEnv.QUEUE_COMPLETED_RETENTION,

    failedRetention:
      workerEnv.QUEUE_FAILED_RETENTION,

    executionTimeoutMs:
      workerEnv.QUEUE_EXECUTION_TIMEOUT_MS,

    backoffJitter:
      workerEnv.QUEUE_BACKOFF_JITTER,
  },

  false,

  workerEnv.QUEUE_PREFIX,
);

// ==================================================
// Build queue manager
// ==================================================
//
// Builds are long-running and use a dedicated execution
// timeout derived from BUILD_TIMEOUT_MS. The build queue
// is only created when build execution is enabled.
// ==================================================

const BUILD_QUEUE_TIMEOUT_MARGIN_MS = 120_000;

const buildExecutionTimeoutMs = Math.max(
  workerEnv.QUEUE_EXECUTION_TIMEOUT_MS,

  workerEnv.BUILD_TIMEOUT_MS +
    BUILD_QUEUE_TIMEOUT_MARGIN_MS,
);

const buildQueueManager = workerEnv.BUILD_ENABLED
  ? new QueueManager(
      workerConnection,

      workerConnection,

      {
        attempts:
          workerEnv.QUEUE_JOB_ATTEMPTS,

        backoffMs:
          workerEnv.QUEUE_BACKOFF_MS,

        completedRetention:
          workerEnv.QUEUE_COMPLETED_RETENTION,

        failedRetention:
          workerEnv.QUEUE_FAILED_RETENTION,

        executionTimeoutMs:
          buildExecutionTimeoutMs,

        backoffJitter:
          workerEnv.QUEUE_BACKOFF_JITTER,
      },

      false,

      workerEnv.QUEUE_PREFIX,
    )
  : null;

const buildExecutor = workerEnv.BUILD_ENABLED
  ? new BuildKitExecutor()
  : null;

// ==================================================
// Runtime queue manager
// ==================================================
//
// Container execution is long-running and uses a dedicated
// execution timeout derived from RUNTIME_STARTUP_TIMEOUT_MS.
// The runtime queue is only created when runtime execution
// is enabled.
// ==================================================

const RUNTIME_QUEUE_TIMEOUT_MARGIN_MS = 120_000;

const runtimeExecutionTimeoutMs = Math.max(
  workerEnv.QUEUE_EXECUTION_TIMEOUT_MS,

  workerEnv.RUNTIME_STARTUP_TIMEOUT_MS +
    RUNTIME_QUEUE_TIMEOUT_MARGIN_MS,
);

const runtimeQueueManager = workerEnv.RUNTIME_ENABLED
  ? new QueueManager(
      workerConnection,

      workerConnection,

      {
        attempts:
          workerEnv.QUEUE_JOB_ATTEMPTS,

        backoffMs:
          workerEnv.QUEUE_BACKOFF_MS,

        completedRetention:
          workerEnv.QUEUE_COMPLETED_RETENTION,

        failedRetention:
          workerEnv.QUEUE_FAILED_RETENTION,

        executionTimeoutMs:
          runtimeExecutionTimeoutMs,

        backoffJitter:
          workerEnv.QUEUE_BACKOFF_JITTER,
      },

      false,

      workerEnv.QUEUE_PREFIX,
    )
  : null;

const runtimeExecutor = workerEnv.RUNTIME_ENABLED
  ? new DockerRuntimeExecutor()
  : null;
let healthMonitor: ReturnType<typeof startDeploymentHealthMonitor> = null;

// ==================================================
// Runtime log collector manager (Phase 5.6)
// ==================================================
//
// Collects container logs for RUNNING runtime instances.
// Independent of the deployment execution lease. Only one
// worker collects a given stream (Redis ownership lock).
// Disabled unless runtime logging is explicitly enabled.
// ==================================================

const runtimeLogManager =
  runtimeExecutor &&
  workerEnv.DEPLOYMENT_LOGS_ENABLED &&
  workerEnv.LOG_RUNTIME_COLLECTION_ENABLED
    ? new RuntimeLogCollectorManager({
        executor: runtimeExecutor,

        redis: workerConnection,

        signal: shutdownController.signal,

        config: {
          maxEntryBytes:
            workerEnv.LOG_MAX_ENTRY_BYTES,

          batchSize: workerEnv.LOG_BATCH_SIZE,

          flushIntervalMs:
            workerEnv.LOG_FLUSH_INTERVAL_MS,

          maxBufferedBytes: Math.min(
            workerEnv.LOG_MAX_DEPLOYMENT_BYTES,
            RUNTIME_LOG_MAX_BUFFER_BYTES,
          ),

          maxBufferedEntries:
            RUNTIME_LOG_MAX_BUFFERED_ENTRIES,

          maxActiveCollectors:
            RUNTIME_LOG_MAX_COLLECTORS,

          maxTotalBufferedBytes:
            RUNTIME_LOG_TOTAL_BUFFER_BYTES,

          reconcilePageSize: 100,

          maxScanPerReconcile: 500,

          maxStartsPerReconcile: 8,

          lockTtlMs:
            workerEnv.LOG_COLLECTOR_LOCK_TTL_MS,

          renewIntervalMs:
            workerEnv.LOG_COLLECTOR_RENEW_MS,

          maxHistoricalMs:
            workerEnv.LOG_RUNTIME_HISTORICAL_MS,
        },
      })
    : null;

// ==================================================
// Sanitized error utilities
// ==================================================

function getErrorName(error: unknown): string {
  if (error instanceof Error) {
    const name = error.name;

    if (/^[A-Za-z0-9_-]{1,80}$/u.test(name)) {
      return name;
    }
  }

  return "UnknownError";
}

function getErrorCode(
  error: unknown,
): string | undefined {
  if (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    typeof error.code === "string" &&
    /^[A-Za-z0-9_-]{1,80}$/u.test(error.code)
  ) {
    return error.code;
  }

  return undefined;
}

function sanitizedError(error: unknown) {
  return {
    errorName: getErrorName(error),

    errorCode: getErrorCode(error),
  };
}

// ==================================================
// Bounded asynchronous operations
// ==================================================

async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  message: string,
): Promise<T> {
  let timer: Timer | undefined;

  try {
    return await Promise.race([
      operation,

      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(message));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

// ==================================================
// Cancellation propagation
// ==================================================

function createJobSignal(
  jobSignal?: AbortSignal,
): AbortSignal {
  if (!jobSignal) {
    return shutdownController.signal;
  }

  return AbortSignal.any([
    jobSignal,

    shutdownController.signal,
  ]);
}

// ==================================================
// Redis connection monitoring
// ==================================================

workerConnection.on("error", (error: unknown) => {
  workerLogger.error(
    {
      workerId,

      ...sanitizedError(error),
    },

    "Worker Redis connection error",
  );
});

workerConnection.on("ready", () => {
  workerLogger.info(
    {
      workerId,
    },

    "Worker Redis connection ready",
  );

  if (startupCompleted && !shuttingDown) {
    void refreshHeartbeats();
  }
});

workerConnection.on("reconnecting", () => {
  workerLogger.warn(
    {
      workerId,
    },

    "Worker Redis connection reconnecting",
  );
});

workerConnection.on("end", () => {
  if (!shuttingDown) {
    workerLogger.warn(
      {
        workerId,
      },

      "Worker Redis connection ended",
    );
  }
});

// ==================================================
// Diagnostic queue worker
// ==================================================

function createObservedWorker(manager: QueueManager, ...args: Parameters<QueueManager["createWorker"]>) {
  const [queue, processor, concurrency] = args;
  const consumer = manager.createWorker(queue, workerMetrics?.wrap(queue, processor) ?? processor, concurrency);
  if (workerMetrics) consumer.on("stalled", () => workerMetrics?.stalled(queue));
  return consumer;
}

const worker = createObservedWorker(queueManager,
  QUEUE_NAMES.diagnostic,

  async (
    job: Job<QueueJob>,
    _token,
    signal,
  ) => {
    const parsed =
      queuePayloadSchemas.diagnostic.safeParse(
        job.data,
      );

    if (!parsed.success) {
      workerLogger.warn(
        {
          workerId,

          jobId: job.id,

          issueCount:
            parsed.error.issues.length,
        },

        "Invalid diagnostic job payload",
      );

      throw new UnrecoverableError(
        "Invalid diagnostic job payload.",
      );
    }

    const jobSignal = createJobSignal(signal);

    if (jobSignal.aborted) {
      throw new Error(
        "Diagnostic execution was interrupted.",
      );
    }

    const result = await processDiagnosticJob(
      job as Job<DiagnosticJob>,

      jobSignal,
    );

    workerLogger.info(
      {
        workerId,

        jobId: job.id,

        correlationId:
          parsed.data.correlationId,

        attempts: job.attemptsStarted,
      },

      "Diagnostic job completed",
    );

    return result;
  },

  workerEnv.WORKER_CONCURRENCY,
);

// ==================================================
// Deployment queue worker
// ==================================================

const deploymentWorker =
  createObservedWorker(queueManager,
    QUEUE_NAMES.deployment,

    async (
      job: Job<QueueJob>,
      _token,
      signal,
    ) => {
      const parsed =
        queuePayloadSchemas.deployment.safeParse(
          job.data,
        );

      if (!parsed.success) {
        workerLogger.warn(
          {
            workerId,

            jobId: job.id,

            issueCount:
              parsed.error.issues.length,
          },

          "Invalid deployment job payload",
        );

        throw new UnrecoverableError(
          "Invalid deployment job payload.",
        );
      }

      const jobSignal =
        createJobSignal(signal);

      workerLogger.info(
        {
          workerId,

          jobId: job.id,

          deploymentId:
            parsed.data.deploymentId,

          attempts: job.attemptsStarted,
        },

        "Deployment infrastructure job started",
      );

      return processDeploymentJob(
        job,

        parsed.data,

        jobSignal,

        {
          auth: sourceAuth,

          workspace: workspaceManager,
        },
      );
    },

    workerEnv.DEPLOYMENT_WORKER_CONCURRENCY,
  );

// ==================================================
// Build queue worker
// ==================================================

const buildWorker =
  buildQueueManager && buildExecutor
    ? createObservedWorker(buildQueueManager,
        QUEUE_NAMES.build,

        async (
          job: Job<QueueJob>,
          _token,
          signal,
        ) => {
          const parsed =
            queuePayloadSchemas.build.safeParse(
              job.data,
            );

          if (!parsed.success) {
            workerLogger.warn(
              {
                workerId,

                jobId: job.id,

                issueCount:
                  parsed.error.issues.length,
              },

              "Invalid build job payload",
            );

            throw new UnrecoverableError(
              "Invalid build job payload.",
            );
          }

          const jobSignal =
            createJobSignal(signal);

          workerLogger.info(
            {
              workerId,

              jobId: job.id,

              deploymentId:
                parsed.data.deploymentId,

              attempts: job.attemptsStarted,
            },

            "Deployment build job started",
          );

          return processBuildJob(
            job,

            parsed.data,

            jobSignal,

            {
              executor: buildExecutor,

              workspace: workspaceManager,
            },
          );
        },

        workerEnv.BUILD_MAX_CONCURRENCY,
      )
    : null;

// ==================================================
// Runtime queue worker
// ==================================================

const runtimeWorker =
  runtimeQueueManager && runtimeExecutor
    ? createObservedWorker(runtimeQueueManager,
        QUEUE_NAMES.runtime,

        async (
          job: Job<QueueJob>,
          _token,
          signal,
        ) => {
          const parsed =
            queuePayloadSchemas.runtime.safeParse(
              job.data,
            );

          if (!parsed.success) {
            workerLogger.warn(
              {
                workerId,

                jobId: job.id,

                issueCount:
                  parsed.error.issues.length,
              },

              "Invalid runtime job payload",
            );

            throw new UnrecoverableError(
              "Invalid runtime job payload.",
            );
          }

          const jobSignal =
            createJobSignal(signal);

          workerLogger.info(
            {
              workerId,

              jobId: job.id,

              deploymentId:
                parsed.data.deploymentId,

              attempts: job.attemptsStarted,
            },

            "Container runtime job started",
          );

          return processRuntimeJob(
            job,

            parsed.data,

            jobSignal,

            {
              executor: runtimeExecutor,
            },
          );
        },

        workerEnv.RUNTIME_MAX_CONCURRENCY,
      )
    : null;

// ==================================================
// Worker event handlers
// ==================================================

worker.on("completed", (job) => {
  workerLogger.info(
    {
      workerId,

      jobId: job.id,

      queue: QUEUE_NAMES.diagnostic,
    },

    "Diagnostic queue job completed",
  );
});

worker.on("failed", (job, error) => {
  workerLogger.warn(
    {
      workerId,

      jobId: job?.id,

      queue: QUEUE_NAMES.diagnostic,

      attemptsMade: job?.attemptsMade,

      ...sanitizedError(error),
    },

    "Diagnostic queue job failed",
  );
});

worker.on("stalled", (jobId) => {
  workerLogger.warn(
    {
      workerId,

      jobId,

      queue: QUEUE_NAMES.diagnostic,
    },

    "Diagnostic queue job stalled",
  );
});

worker.on("error", (error) => {
  workerLogger.error(
    {
      workerId,

      queue: QUEUE_NAMES.diagnostic,

      ...sanitizedError(error),
    },

    "Diagnostic worker error",
  );
});

// ==================================================
// Deployment worker event handlers
// ==================================================

deploymentWorker.on("completed", (job) => {
  workerLogger.info(
    {
      workerId,

      jobId: job.id,

      queue: QUEUE_NAMES.deployment,
    },

    "Deployment infrastructure job completed",
  );
});

deploymentWorker.on("failed", (job, error) => {
  workerLogger.warn(
    {
      workerId,

      jobId: job?.id,

      queue: QUEUE_NAMES.deployment,

      attemptsMade: job?.attemptsMade,

      configuredAttempts:
        job?.opts.attempts,

      ...sanitizedError(error),
    },

    "Deployment infrastructure job failed",
  );
});

deploymentWorker.on("stalled", (jobId) => {
  workerLogger.warn(
    {
      workerId,

      jobId,

      queue: QUEUE_NAMES.deployment,
    },

    "Deployment queue job stalled",
  );
});

deploymentWorker.on("error", (error) => {
  workerLogger.error(
    {
      workerId,

      queue: QUEUE_NAMES.deployment,

      ...sanitizedError(error),
    },

    "Deployment worker error",
  );
});

// ==================================================
// Build worker event handlers
// ==================================================

buildWorker?.on("completed", (job) => {
  workerLogger.info(
    {
      workerId,

      jobId: job.id,

      queue: QUEUE_NAMES.build,
    },

    "Deployment build job completed",
  );
});

buildWorker?.on("failed", (job, error) => {
  workerLogger.warn(
    {
      workerId,

      jobId: job?.id,

      queue: QUEUE_NAMES.build,

      attemptsMade: job?.attemptsMade,

      ...sanitizedError(error),
    },

    "Deployment build job failed",
  );
});

buildWorker?.on("stalled", (jobId) => {
  workerLogger.warn(
    {
      workerId,

      jobId,

      queue: QUEUE_NAMES.build,
    },

    "Build queue job stalled",
  );
});

buildWorker?.on("error", (error) => {
  workerLogger.error(
    {
      workerId,

      queue: QUEUE_NAMES.build,

      ...sanitizedError(error),
    },

    "Build worker error",
  );
});

// ==================================================
// Runtime worker event handlers
// ==================================================

runtimeWorker?.on("completed", (job) => {
  workerLogger.info(
    {
      workerId,

      jobId: job.id,

      queue: QUEUE_NAMES.runtime,
    },

    "Container runtime job completed",
  );
});

runtimeWorker?.on("failed", (job, error) => {
  workerLogger.warn(
    {
      workerId,

      jobId: job?.id,

      queue: QUEUE_NAMES.runtime,

      attemptsMade: job?.attemptsMade,

      ...sanitizedError(error),
    },

    "Container runtime job failed",
  );
});

runtimeWorker?.on("stalled", (jobId) => {
  workerLogger.warn(
    {
      workerId,

      jobId,

      queue: QUEUE_NAMES.runtime,
    },

    "Runtime queue job stalled",
  );
});

runtimeWorker?.on("error", (error) => {
  workerLogger.error(
    {
      workerId,

      queue: QUEUE_NAMES.runtime,

      ...sanitizedError(error),
    },

    "Runtime worker error",
  );
});

// ==================================================
// Worker readiness
// ==================================================

function isWorkerAvailable(
  role: WorkerRole,
): boolean {
  if (
    shuttingDown ||
    !startupCompleted ||
    workerConnection.status !== "ready"
  ) {
    return false;
  }

  const currentWorker =
    role === "deployment"
      ? deploymentWorker
      : worker;

  return (
    currentWorker.isRunning() &&
    !currentWorker.isPaused()
  );
}

function isBuildAvailable(): boolean {
  if (
    !workerEnv.BUILD_ENABLED ||
    !buildWorker ||
    shuttingDown ||
    !startupCompleted ||
    workerConnection.status !== "ready"
  ) {
    return false;
  }

  return (
    buildWorker.isRunning() &&
    !buildWorker.isPaused()
  );
}

function isRuntimeAvailable(): boolean {
  if (
    !workerEnv.RUNTIME_ENABLED ||
    !runtimeWorker ||
    shuttingDown ||
    !startupCompleted ||
    workerConnection.status !== "ready"
  ) {
    return false;
  }

  return (
    runtimeWorker.isRunning() &&
    !runtimeWorker.isPaused()
  );
}

// ==================================================
// Worker heartbeat publication
// ==================================================

async function publishHeartbeat(
  role: WorkerRole,
  available: boolean,
  draining: boolean,
): Promise<void> {
  const isDeploymentWorker =
    role === "deployment";

  // Effective health-monitoring state: the worker only
  // reports monitoring as active when the feature is fully
  // configured (monitoring + persistence + runtime + trusted
  // local) and the monitor has actually started. This is the
  // worker's own authoritative report; readers must not infer
  // availability from API environment variables or scheduled
  // check timestamps.
  const healthMonitoring =
    isDeploymentWorker &&
    workerEnv.HEALTH_MONITORING_ENABLED &&
    workerEnv.HEALTH_PERSISTENCE_ENABLED &&
    workerEnv.RUNTIME_ENABLED &&
    workerEnv.RUNTIME_TRUSTED_LOCAL_ONLY &&
    healthMonitor !== null;

  try {
    await publishWorkerHeartbeat(
      workerConnection,

      heartbeatOptions,

      {
        workerId,

        role,

        version: WORKER_VERSION,

        capabilities: {
          diagnostic:
            !isDeploymentWorker,

          deploymentQueue:
            isDeploymentWorker,

          sourceRetrieval:
            isDeploymentWorker &&
            sourceRetrievalConfigured,

          workspaceStorage:
            isDeploymentWorker &&
            workspaceStorageAvailable,

          // Phase 5.4 adds isolated Docker image builds.

          dockerBuild:
            isDeploymentWorker &&
            isBuildAvailable(),

          // Phase 5.5 adds secure container execution.
          // Disabled by default; untrusted workloads remain
          // unsupported.

          containerExecution:
            isDeploymentWorker &&
            isRuntimeAvailable(),

          containerRuntime:
            isDeploymentWorker &&
            isRuntimeAvailable(),

          // Phase 5.7.8 health-monitoring observability.
          healthMonitoring,
          ...(isDeploymentWorker && {
            automaticRecovery: {
              configured: workerEnv.AUTO_RECOVERY_ENABLED,
              available: workerEnv.AUTO_RECOVERY_ENABLED && healthMonitoring &&
                isRuntimeAvailable() && runtimeQueueManager !== null && workerEnv.BUILD_TRUSTED_REPOSITORIES.length > 0,
              trustedRepositories: workerEnv.BUILD_TRUSTED_REPOSITORIES.map((repository) => repository.trim().toLowerCase()),
              heartbeatIntervalMs: workerEnv.WORKER_HEARTBEAT_INTERVAL_MS,
            },
          }),
        },

        startupAt,

        lastHeartbeatAt:
          new Date().toISOString(),

        available,

        draining,

        concurrency: isDeploymentWorker
          ? workerEnv.DEPLOYMENT_WORKER_CONCURRENCY
          : workerEnv.WORKER_CONCURRENCY,
      },
    );
  } catch (error) {
    workerLogger.warn(
      {
        workerId,

        role,

        ...sanitizedError(error),
      },

      "Worker heartbeat update failed",
    );
  }
}

// ==================================================
// Non-overlapping heartbeat execution
// ==================================================

function refreshHeartbeats(): Promise<void> {
  if (shuttingDown) {
    return Promise.resolve();
  }

  if (heartbeatTask) {
    return heartbeatTask;
  }

  heartbeatTask = (async () => {
    if (workerConnection.status !== "ready") {
      return;
    }

    await refreshWorkspaceStorage();

    if (
      shuttingDown ||
      workerConnection.status !== "ready"
    ) {
      return;
    }

    await Promise.all([
      publishHeartbeat(
        "diagnostic",

        isWorkerAvailable("diagnostic"),

        false,
      ),

      publishHeartbeat(
        "deployment",

        isWorkerAvailable("deployment"),

        false,
      ),
    ]);
  })().finally(() => {
    heartbeatTask = null;
  });

  return heartbeatTask;
}

function scheduleHeartbeat(): void {
  if (shuttingDown || heartbeatTimer) {
    return;
  }

  heartbeatTimer = setTimeout(() => {
    heartbeatTimer = undefined;

    void refreshHeartbeats()
      .catch((error: unknown) => {
        workerLogger.warn(
          {
            workerId,

            ...sanitizedError(error),
          },

          "Worker heartbeat refresh failed",
        );
      })
      .finally(() => {
        scheduleHeartbeat();
      });
  }, workerEnv.WORKER_HEARTBEAT_INTERVAL_MS);

  heartbeatTimer.unref();
}

// ==================================================
// Workspace maintenance
// ==================================================

type MaintenanceMode =
  | "recovery"
  | "expiration";

function runWorkspaceMaintenance(
  mode: MaintenanceMode,
): Promise<void> {
  if (shuttingDown) {
    return Promise.resolve();
  }

  if (maintenanceTask) {
    return maintenanceTask;
  }

  maintenanceTask = (async () => {
    try {
      const result =
        mode === "recovery"
          ? await workspaceManager.recoverOrphaned()
          : await workspaceManager.cleanupExpired();

      if (result.removed > 0) {
        workerLogger.info(
          {
            workerId,

            mode,

            removed: result.removed,
          },

          "Source workspace maintenance completed",
        );
      }
    } catch (error) {
      workerLogger.warn(
        {
          workerId,

          mode,

          ...sanitizedError(error),
        },

        "Source workspace maintenance failed",
      );
    }
  })().finally(() => {
    maintenanceTask = null;
  });

  return maintenanceTask;
}

function scheduleWorkspaceCleanup(): void {
  if (shuttingDown || cleanupTimer) {
    return;
  }

  cleanupTimer = setTimeout(() => {
    cleanupTimer = undefined;

    void runWorkspaceMaintenance(
      "expiration",
    ).finally(() => {
      scheduleWorkspaceCleanup();
    });
  }, workerEnv.SOURCE_CLEANUP_INTERVAL_SECONDS * 1000);

  cleanupTimer.unref();
}

// ==================================================
// Runtime log collector reconciliation
// ==================================================

const LOG_RECONCILE_INTERVAL_MS = 15_000;

/**
 * Reconciliation is serialized at the worker lifecycle
 * level in addition to manager-level protection.
 *
 * Failed reconciliation must not permanently disable
 * future collector discovery.
 */
function runLogReconcile(): Promise<void> {
  const manager = runtimeLogManager;

  if (
    shuttingDown ||
    shutdownController.signal.aborted ||
    !manager
  ) {
    return Promise.resolve();
  }

  if (logReconcileTask) {
    return logReconcileTask;
  }

  const task = Promise.resolve()
    .then(async () => {
      if (
        shuttingDown ||
        shutdownController.signal.aborted
      ) {
        return;
      }

      await manager.reconcile();
    })
    .catch((error: unknown) => {
      if (!shuttingDown) {
        workerLogger.warn(
          {
            workerId,

            ...sanitizedError(error),
          },

          "Runtime log collector reconciliation failed",
        );
      }
    })
    .finally(() => {
      if (logReconcileTask === task) {
        logReconcileTask = null;
      }
    });

  logReconcileTask = task;

  return task;
}

function scheduleLogReconcile(): void {
  if (
    shuttingDown ||
    !runtimeLogManager ||
    logReconcileTimer
  ) {
    return;
  }

  logReconcileTimer = setTimeout(() => {
    logReconcileTimer = undefined;

    void runLogReconcile().finally(() => {
      scheduleLogReconcile();
    });
  }, LOG_RECONCILE_INTERVAL_MS);

  logReconcileTimer.unref();
}

// ==================================================
// Log retention cleanup
// ==================================================

const LOG_RETENTION_INTERVAL_MS = 60 * 60_000;

/**
 * Track retention so shutdown can wait for active
 * database maintenance before terminating the process.
 */
function runTrackedLogRetention(): Promise<void> {
  if (
    shuttingDown ||
    shutdownController.signal.aborted ||
    !workerEnv.DEPLOYMENT_LOGS_ENABLED
  ) {
    return Promise.resolve();
  }

  if (logRetentionTask) {
    return logRetentionTask;
  }

  const task = Promise.resolve()
    .then(async () => {
      if (
        shuttingDown ||
        shutdownController.signal.aborted
      ) {
        return;
      }

      const { deletedEntries } = await runLogRetention();

      if (deletedEntries > 0) {
        workerLogger.info(
          {
            workerId,

            deletedEntries,
          },

          "Expired deployment logs removed",
        );
      }
    })
    .catch((error: unknown) => {
      workerLogger.warn(
        {
          workerId,

          ...sanitizedError(error),
        },

        "Log retention cleanup failed",
      );
    })
    .finally(() => {
      if (logRetentionTask === task) {
        logRetentionTask = null;
      }
    });

  logRetentionTask = task;

  return task;
}

function scheduleLogRetention(): void {
  if (
    shuttingDown ||
    !workerEnv.DEPLOYMENT_LOGS_ENABLED ||
    logRetentionTimer
  ) {
    return;
  }

  logRetentionTimer = setTimeout(() => {
    logRetentionTimer = undefined;

    void runTrackedLogRetention().finally(() => {
      scheduleLogRetention();
    });
  }, LOG_RETENTION_INTERVAL_MS);

  logRetentionTimer.unref();
}

// ==================================================
// Startup validation
// ==================================================

function validateWorkerConfiguration(): void {
  const heartbeatTtlMs =
    workerEnv.WORKER_HEARTBEAT_TTL_SECONDS *
    1000;

  const heartbeatIntervalMs =
    workerEnv.WORKER_HEARTBEAT_INTERVAL_MS;

  if (
    !Number.isSafeInteger(heartbeatTtlMs) ||
    heartbeatTtlMs <= 0 ||
    !Number.isSafeInteger(heartbeatIntervalMs) ||
    heartbeatIntervalMs <= 0
  ) {
    throw new Error(
      "Invalid worker heartbeat configuration.",
    );
  }

  if (heartbeatIntervalMs >= heartbeatTtlMs) {
    throw new Error(
      "Worker heartbeat interval must be shorter than its TTL.",
    );
  }

  if (
    heartbeatIntervalMs * 2 >= heartbeatTtlMs
  ) {
    workerLogger.warn(
      {
        workerId,

        heartbeatIntervalMs,

        heartbeatTtlMs,
      },

      "Worker heartbeat TTL has limited recovery margin",
    );
  }

  const cleanupIntervalMs =
    workerEnv.SOURCE_CLEANUP_INTERVAL_SECONDS *
    1000;

  if (
    !Number.isSafeInteger(cleanupIntervalMs) ||
    cleanupIntervalMs <= 0 ||
    cleanupIntervalMs > 2_147_483_647
  ) {
    throw new Error(
      "Invalid workspace cleanup interval.",
    );
  }

  if (
    workerEnv.BUILD_ENABLED &&
    (
      !Number.isSafeInteger(buildExecutionTimeoutMs) ||
      buildExecutionTimeoutMs <= 0 ||
      buildExecutionTimeoutMs > 2_147_483_647
    )
  ) {
    throw new Error(
      "Invalid build execution timeout.",
    );
  }

  if (
    workerEnv.RUNTIME_ENABLED &&
    (
      !Number.isSafeInteger(runtimeExecutionTimeoutMs) ||
      runtimeExecutionTimeoutMs <= 0 ||
      runtimeExecutionTimeoutMs > 2_147_483_647
    )
  ) {
    throw new Error(
      "Invalid runtime execution timeout.",
    );
  }

  if (
    workerEnv.LOG_RUNTIME_COLLECTION_ENABLED &&
    !workerEnv.DEPLOYMENT_LOGS_ENABLED
  ) {
    throw new Error(
      "Runtime log collection requires deployment logging.",
    );
  }
}

// ==================================================
// Worker startup
// ==================================================

async function start(): Promise<void> {
  validateWorkerConfiguration();

  workerLogger.info(
    {
      workerId,

      version: WORKER_VERSION,

      diagnosticConcurrency:
        workerEnv.WORKER_CONCURRENCY,

      deploymentConcurrency:
        workerEnv.DEPLOYMENT_WORKER_CONCURRENCY,

      sourceRetrievalConfigured,

      dockerBuild: isBuildAvailable(),

      containerExecution: isRuntimeAvailable(),
    },

    "CloudPilot worker starting",
  );

  // Wait until all registered BullMQ workers have
  // established their required Redis connections.

  await withTimeout(
    Promise.all([
      worker.waitUntilReady(),

      deploymentWorker.waitUntilReady(),

      ...(buildWorker
        ? [buildWorker.waitUntilReady()]
        : []),

      ...(runtimeWorker
        ? [runtimeWorker.waitUntilReady()]
        : []),
    ]),

    STARTUP_TIMEOUT_MS,

    "Worker Redis readiness timed out.",
  );

  if (shuttingDown) {
    return;
  }

  startupCompleted = true;

  await withTimeout(
    refreshHeartbeats(),

    STARTUP_TIMEOUT_MS,

    "Initial worker heartbeat timed out.",
  );

  if (shuttingDown) {
    return;
  }

  scheduleHeartbeat();

  // Maintenance operations are serialized to prevent
  // startup recovery and periodic cleanup overlapping.

  await runWorkspaceMaintenance("recovery");

  if (shuttingDown) {
    return;
  }

  scheduleWorkspaceCleanup();

  if (runtimeWorker) {
    try {
      const result =
        await reconcileRuntimeInstances();

      if (result.recovered > 0) {
        workerLogger.info(
          {
            workerId,

            recovered: result.recovered,
          },

          "Runtime reconciliation recovered abandoned instances",
        );
      }
    } catch (error) {
      workerLogger.warn(
        {
          workerId,

          ...sanitizedError(error),
        },

        "Runtime reconciliation failed",
      );
    }
  }

  if (!sourceRetrievalConfigured) {
    workerLogger.warn(
      {
        workerId,
      },

      "GitHub App source retrieval is not configured",
    );
  }

  if (shuttingDown) {
    return;
  }

  /**
   * Logging is auxiliary infrastructure.
   *
   * Collector discovery must not block worker startup.
   * Schedule the next reconciliation even if the first
   * reconciliation fails.
   */
  if (runtimeLogManager) {
    void runLogReconcile().finally(() => {
      scheduleLogReconcile();
    });
  }

  if (workerEnv.DEPLOYMENT_LOGS_ENABLED) {
    void runTrackedLogRetention().finally(() => {
      scheduleLogRetention();
    });
  }

  if (runtimeExecutor && workerEnv.HEALTH_MONITORING_ENABLED) {
    const recoveryQueue: RecoveryJobQueue | null = runtimeQueueManager
      ? {
          getJob: (id) => runtimeQueueManager.getQueue("runtime").getJob(id).then((job) => job ?? null),
          add: (name, payload, opts) => runtimeQueueManager.getQueue("runtime").add(name, payload, opts),
        }
      : null;
    healthMonitor = startDeploymentHealthMonitor(
      runtimeExecutor.inspectOwnedContainer.bind(runtimeExecutor), shutdownController.signal, undefined, recoveryQueue,
    );
  }

  if (shuttingDown) {
    return;
  }

  if (workerMetrics) {
    try {
      workerMetrics.readiness(() => {
        const available = isWorkerAvailable("deployment");
        const monitoring = healthMonitor !== null && !shutdownController.signal.aborted;
        const automatic = available && monitoring && workerEnv.AUTO_RECOVERY_ENABLED &&
          isRuntimeAvailable() && runtimeQueueManager !== null && workerEnv.BUILD_TRUSTED_REPOSITORIES.length > 0;
        return { available, monitoring, automatic };
      });
      closeMetrics = workerMetrics.start();
    } catch {
      workerLogger.error("Worker metrics startup failed");
    }
  }

  workerLogger.info(
    {
      workerId,

      diagnosticAvailable:
        isWorkerAvailable("diagnostic"),

      deploymentAvailable:
        isWorkerAvailable("deployment"),

      sourceRetrievalConfigured,

      dockerBuild: isBuildAvailable(),

      containerRuntime: isRuntimeAvailable(),
    },

    "CloudPilot worker startup completed",
  );
}

// ==================================================
// Shutdown utilities
// ==================================================

function stopBackgroundTimers(): void {
  if (heartbeatTimer) {
    clearTimeout(heartbeatTimer);

    heartbeatTimer = undefined;
  }

  if (cleanupTimer) {
    clearTimeout(cleanupTimer);

    cleanupTimer = undefined;
  }

  if (logReconcileTimer) {
    clearTimeout(logReconcileTimer);

    logReconcileTimer = undefined;
  }

  if (logRetentionTimer) {
    clearTimeout(logRetentionTimer);

    logRetentionTimer = undefined;
  }
}

async function publishDrainingHeartbeats():
  Promise<void> {
  if (workerConnection.status !== "ready") {
    return;
  }

  try {
    // Ensure an older available heartbeat cannot
    // overwrite the final draining heartbeat.

    if (heartbeatTask) {
      try {
        await withTimeout(
          heartbeatTask,

          HEARTBEAT_SHUTDOWN_TIMEOUT_MS,

          "Pending heartbeat did not finish.",
        );
      } catch (error) {
        workerLogger.warn(
          {
            workerId,

            ...sanitizedError(error),
          },

          "Pending heartbeat could not be confirmed before draining",
        );
      }
    }

    if (workerConnection.status !== "ready") {
      return;
    }

    await withTimeout(
      Promise.all([
        publishHeartbeat(
          "diagnostic",
          false,
          true,
        ),

        publishHeartbeat(
          "deployment",
          false,
          true,
        ),
      ]),

      HEARTBEAT_SHUTDOWN_TIMEOUT_MS,

      "Draining heartbeat publication timed out.",
    );
  } catch (error) {
    workerLogger.warn(
      {
        workerId,

        ...sanitizedError(error),
      },

      "Final worker heartbeat could not be confirmed",
    );
  }
}

async function shutdownStep(
  name: string,
  operation: () => Promise<unknown>,
): Promise<void> {
  try {
    await operation();
  } catch (error) {
    shutdownExitCode = 1;

    workerLogger.error(
      {
        workerId,

        step: name,

        ...sanitizedError(error),
      },

      "Worker shutdown step failed",
    );
  }
}

// ==================================================
// Graceful shutdown
// ==================================================

function shutdown(
  reason: string,
  exitCode = 0,
): Promise<void> {
  // A fatal error during an existing graceful shutdown
  // must still result in a non-zero process exit.

  shutdownExitCode = Math.max(
    shutdownExitCode,

    exitCode,
  );

  if (shutdownPromise) {
    return shutdownPromise;
  }

  shuttingDown = true;

  startupCompleted = false;

  stopBackgroundTimers();

  workerLogger.info(
    {
      workerId,

      reason,

      exitCode: shutdownExitCode,
    },

    "Worker graceful shutdown started",
  );

  // Keep this timer referenced. It must enforce
  // the deadline even if other handles disappear.

  const deadline = setTimeout(() => {
    workerLogger.error(
      {
        workerId,

        reason,

        timeoutMs: SHUTDOWN_TIMEOUT_MS,
      },

      "Worker graceful shutdown deadline exceeded",
    );

    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS);

  shutdownPromise = (async () => {
    try {
      // Signal active processors so they can stop
      // downloads and extraction cooperatively.

      shutdownController.abort();
      await shutdownStep("close-metrics-listener", async () => { await closeMetrics?.(); });

      /**
       * Start collector shutdown immediately.
       *
       * Keep Redis available until collector locks
       * and buffered writes have been handled.
       */
      const collectorShutdownTask = shutdownStep(
        "stop-runtime-log-collectors",

        async () => {
          if (runtimeLogManager) {
            await runtimeLogManager.stopAll();
          }
        },
      );

      // Stop accepting new work without waiting
      // for current jobs at this stage.

      await shutdownStep(
        "pause-workers",

        async () => {
          const results =
            await Promise.allSettled([
              worker.pause(true),

              deploymentWorker.pause(true),

              ...(buildWorker
                ? [buildWorker.pause(true)]
                : []),

              ...(runtimeWorker
                ? [runtimeWorker.pause(true)]
                : []),
            ]);

          if (
            results.some(
              (result) =>
                result.status === "rejected",
            )
          ) {
            throw new Error(
              "One or more workers could not be paused.",
            );
          }
        },
      );

      // Publish unavailable/draining state before
      // closing Redis-dependent infrastructure.

      await publishDrainingHeartbeats();

      // QueueManager should wait for active processors
      // to settle before closing worker connections.

      await shutdownStep(
        "close-queue-manager",

        async () => {
          const managers = [
            queueManager,

            ...(buildQueueManager
              ? [buildQueueManager]
              : []),

            ...(runtimeQueueManager
              ? [runtimeQueueManager]
              : []),
          ];

          const results = await Promise.allSettled(
            managers.map((manager) =>
              Promise.resolve().then(() =>
                manager.close(),
              ),
            ),
          );

          if (
            results.some(
              (result) => result.status === "rejected",
            )
          ) {
            throw new Error(
              "One or more queue managers failed to close.",
            );
          }
        },
      );

      /**
       * Wait for background tasks and collector shutdown.
       *
       * The outer process deadline remains authoritative.
       * If this shorter waiting budget expires, report
       * incomplete shutdown instead of claiming success.
       */
      await shutdownStep(
        "finish-background-maintenance",

        async () => {
          const pendingTasks: Promise<unknown>[] = [
            collectorShutdownTask,
          ];

          if (healthMonitor) pendingTasks.push(healthMonitor.stop());

          if (maintenanceTask) {
            pendingTasks.push(maintenanceTask);
          }

          if (logReconcileTask) {
            pendingTasks.push(logReconcileTask);
          }

          if (logRetentionTask) {
            pendingTasks.push(logRetentionTask);
          }

          if (heartbeatTask) {
            pendingTasks.push(heartbeatTask);
          }

          const results = await withTimeout(
            Promise.allSettled(pendingTasks),

            BACKGROUND_SHUTDOWN_TIMEOUT_MS,

            "Background maintenance shutdown timed out.",
          );

          if (
            results.some(
              (result) => result.status === "rejected",
            )
          ) {
            throw new Error(
              "One or more background tasks failed to finish.",
            );
          }
        },
      );

      await shutdownStep(
        "close-redis-connection",

        async () => {
          if (workerConnection.status === "end") {
            return;
          }

          try {
            await withTimeout(
              workerConnection.quit(),

              REDIS_SHUTDOWN_TIMEOUT_MS,

              "Redis graceful disconnect timed out.",
            );
          } catch {
            workerConnection.disconnect();

            throw new Error(
              "Redis graceful disconnect failed.",
            );
          }
        },
      );

      if (shutdownExitCode === 0) {
        workerLogger.info(
          {
            workerId,

            reason,

            exitCode: shutdownExitCode,
          },

          "Worker graceful shutdown completed",
        );
      } else {
        workerLogger.error(
          {
            workerId,

            reason,

            exitCode: shutdownExitCode,
          },

          "Worker shutdown completed with errors or incomplete cleanup",
        );
      }
    } catch (error) {
      shutdownExitCode = 1;

      workerLogger.error(
        {
          workerId,

          reason,

          ...sanitizedError(error),
        },

        "Worker graceful shutdown failed",
      );
    } finally {
      clearTimeout(deadline);

      process.exit(shutdownExitCode);
    }
  })();

  return shutdownPromise;
}

// ==================================================
// Process lifecycle handlers
// ==================================================

process.on("SIGINT", () => {
  void shutdown("SIGINT");
});

process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});

process.on("uncaughtException", (error) => {
  workerLogger.fatal(
    {
      workerId,

      ...sanitizedError(error),
    },

    "Uncaught worker exception",
  );

  void shutdown(
    "uncaughtException",

    1,
  );
});

process.on("unhandledRejection", (reason) => {
  workerLogger.fatal(
    {
      workerId,

      ...sanitizedError(reason),
    },

    "Unhandled worker rejection",
  );

  void shutdown(
    "unhandledRejection",

    1,
  );
});

// ==================================================
// Application entry point
// ==================================================

void start().catch((error: unknown) => {
  workerLogger.fatal(
    {
      workerId,

      ...sanitizedError(error),
    },

    "CloudPilot worker startup failed",
  );

  void shutdown(
    "startupFailure",

    1,
  );
});
