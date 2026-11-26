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
  BuildKitExecutor,
} from "./build/buildkit-client.js";

// ==================================================
// Constants
// ==================================================

const WORKER_VERSION = "0.1.0";

const SHUTDOWN_TIMEOUT_MS = 30_000;

const HEARTBEAT_SHUTDOWN_TIMEOUT_MS = 2_000;

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

let shutdownPromise: Promise<void> | null = null;

let heartbeatTimer: Timer | undefined;

let cleanupTimer: Timer | undefined;

let heartbeatTask: Promise<void> | null = null;

let maintenanceTask: Promise<void> | null = null;

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

const worker = queueManager.createWorker(
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
  queueManager.createWorker(
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
    ? buildQueueManager.createWorker(
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

          // Phase 5.4 adds isolated Docker image builds.
          // Container execution remains disabled.

          dockerBuild:
            isDeploymentWorker &&
            isBuildAvailable(),

          containerExecution: false,
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
  if (shuttingDown) {
    return;
  }

  heartbeatTimer = setTimeout(() => {
    void refreshHeartbeats().finally(() => {
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
  if (shuttingDown) {
    return;
  }

  cleanupTimer = setTimeout(() => {
    void runWorkspaceMaintenance(
      "expiration",
    ).finally(() => {
      scheduleWorkspaceCleanup();
    });
  }, workerEnv.SOURCE_CLEANUP_INTERVAL_SECONDS * 1000);

  cleanupTimer.unref();
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

      containerExecution: false,
    },

    "CloudPilot worker starting",
  );

  // Wait until all registered BullMQ workers have
  // established their required Redis connections.

  await Promise.all([
    worker.waitUntilReady(),

    deploymentWorker.waitUntilReady(),

    ...(buildWorker
      ? [buildWorker.waitUntilReady()]
      : []),
  ]);

  if (shuttingDown) {
    return;
  }

  startupCompleted = true;

  await refreshHeartbeats();

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

  if (!sourceRetrievalConfigured) {
    workerLogger.warn(
      {
        workerId,
      },

      "GitHub App source retrieval is not configured",
    );
  }

  workerLogger.info(
    {
      workerId,

      diagnosticAvailable:
        isWorkerAvailable("diagnostic"),

      deploymentAvailable:
        isWorkerAvailable("deployment"),

      sourceRetrievalConfigured,
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
      await withTimeout(
        heartbeatTask,

        HEARTBEAT_SHUTDOWN_TIMEOUT_MS,

        "Pending heartbeat did not finish.",
      );
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
          await queueManager.close();

          if (buildQueueManager) {
            await buildQueueManager.close();
          }
        },
      );

      // Do not disconnect Redis while workspace
      // maintenance may still be using dependencies.

      await shutdownStep(
        "finish-workspace-maintenance",

        async () => {
          if (maintenanceTask) {
            await maintenanceTask;
          }
        },
      );

      // Redis is closed only after workers finish.

      await shutdownStep(
        "close-redis-connection",

        async () => {
          if (workerConnection.status === "end") {
            return;
          }

          try {
            await workerConnection.quit();
          } catch {
            workerConnection.disconnect();

            throw new Error(
              "Redis graceful disconnect failed.",
            );
          }
        },
      );

      workerLogger.info(
        {
          workerId,

          reason,

          exitCode: shutdownExitCode,
        },

        "Worker graceful shutdown completed",
      );
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