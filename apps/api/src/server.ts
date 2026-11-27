
import type { Server } from "node:http";
import { createServer } from "node:http";

import { disconnectDatabase } from "@cloudpilot/database";

import { createRedisConnection } from "@cloudpilot/queue";

import {
  disconnectQueueProducer,
  queueEnv,
} from "./services/queue-producer.js";

import { deploymentOutboxRuntime } from "./services/deployment-outbox-runtime.js";
import { LogDistributionRuntime } from "./services/log-distribution.service.js";
import { LogGateway } from "./realtime/log-gateway.js";
import { createApp } from "./app.js";
import { env } from "./config/env.js";
import { logger } from "./config/logger.js";

// --------------------------------------------------
// Application and HTTP server
// --------------------------------------------------

const app = createApp();
const httpServer: Server = createServer(app);

// --------------------------------------------------
// Real-time log distribution (Phase 5.6)
// --------------------------------------------------
//
// A dedicated subscriber connection is required because a
// Redis client in subscriber mode cannot issue other
// commands. Distribution and the Socket.IO gateway are only
// created when deployment logging is enabled.
// --------------------------------------------------

const logsEnabled = env.DEPLOYMENT_LOGS_ENABLED;

const logSubscriber = logsEnabled
  ? createRedisConnection({
      url: env.REDIS_URL,
      timeoutMs: 5_000,
      maxRetriesPerRequest: null,
      name: "cloudpilot-log-subscriber",
    })
  : null;

const logDistribution = logsEnabled
  ? new LogDistributionRuntime()
  : null;

const logGateway =
  logSubscriber && logsEnabled
    ? new LogGateway(httpServer, logSubscriber)
    : null;

const SHUTDOWN_TIMEOUT_MS = 10_000;

let isShuttingDown = false;
let shutdownExitCode = 0;

// --------------------------------------------------
// Graceful shutdown
// --------------------------------------------------

async function shutdown(
  exitCode: number,
  signal: string,
): Promise<void> {
  shutdownExitCode = Math.max(shutdownExitCode, exitCode);

  if (isShuttingDown) {
    return;
  }

  isShuttingDown = true;

  logger.info(
    { signal, exitCode: shutdownExitCode },
    "Graceful shutdown started",
  );

  // Keep the timeout active throughout the entire shutdown.
  const timeout = setTimeout(() => {
    logger.error(
      {
        signal,
        timeoutMs: SHUTDOWN_TIMEOUT_MS,
      },
      "Graceful shutdown timeout exceeded",
    );

    httpServer.closeAllConnections();

    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS);

  // Attempt every cleanup operation even if one fails.
  async function cleanup(
    resource: string,
    operation: () => Promise<unknown>,
  ): Promise<void> {
    try {
      await operation();

      logger.info(
        { resource, signal },
        "Resource shutdown completed",
      );
    } catch (error) {
      shutdownExitCode = 1;

      logger.error(
        {
          err: error,
          resource,
          signal,
        },
        "Resource shutdown failed",
      );
    }
  }

  try {
    // 1. Stop accepting new HTTP requests.
    await cleanup("HTTP server", async () => {
      if (!httpServer.listening) {
        return;
      }

      await new Promise<void>((resolve, reject) => {
        httpServer.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });

        httpServer.closeIdleConnections();
      });
    });

    // 2. Stop the deployment outbox publisher before
    // disconnecting its database and queue dependencies.
    await cleanup("Deployment outbox runtime", async () => {
      await deploymentOutboxRuntime.stop();
    });

    // 2b. Stop real-time log distribution and the Socket.IO
    // gateway before disconnecting Redis.
    await cleanup("Log gateway", async () => {
      if (logGateway) {
        await logGateway.close();
      }
    });

    await cleanup("Log distribution runtime", async () => {
      if (logDistribution) {
        await logDistribution.stop();
      }
    });

    await cleanup("Log subscriber", async () => {
      if (logSubscriber) {
        try {
          await logSubscriber.quit();
        } catch {
          logSubscriber.disconnect();
        }
      }
    });

    // 3. Disconnect BullMQ queue producer and Redis.
    await cleanup("Queue producer", async () => {
      await disconnectQueueProducer();
    });

    // 4. Disconnect PostgreSQL.
    await cleanup("PostgreSQL database", async () => {
      await disconnectDatabase();
    });

    if (shutdownExitCode === 0) {
      logger.info(
        { signal, exitCode: shutdownExitCode },
        "Graceful shutdown completed successfully",
      );
    } else {
      logger.error(
        { signal, exitCode: shutdownExitCode },
        "Shutdown completed with errors",
      );
    }
  } catch (error) {
    shutdownExitCode = 1;

    logger.fatal(
      { err: error, signal },
      "Unexpected shutdown failure",
    );
  } finally {
    clearTimeout(timeout);

    process.exit(shutdownExitCode);
  }
}

// --------------------------------------------------
// HTTP server error handling
// --------------------------------------------------

httpServer.on("error", (error) => {
  logger.fatal(
    {
      err: error,
      host: env.HOST,
      port: env.PORT,
    },
    "HTTP server error",
  );

  void shutdown(1, "httpServerError");
});

// --------------------------------------------------
// Process signal handling
// --------------------------------------------------

process.on("SIGINT", () => {
  void shutdown(0, "SIGINT");
});

process.on("SIGTERM", () => {
  void shutdown(0, "SIGTERM");
});

// --------------------------------------------------
// Global process error handling
// --------------------------------------------------

process.on("unhandledRejection", (reason) => {
  logger.fatal(
    { err: reason },
    "Unhandled promise rejection",
  );

  void shutdown(1, "unhandledRejection");
});

process.on("uncaughtException", (error) => {
  logger.fatal(
    { err: error },
    "Uncaught exception",
  );

  void shutdown(1, "uncaughtException");
});

// --------------------------------------------------
// HTTP server startup
// --------------------------------------------------

httpServer.listen(env.PORT, env.HOST, () => {
  if (isShuttingDown) {
    return;
  }

  try {
    deploymentOutboxRuntime.start(
      queueEnv.DEPLOYMENT_OUTBOX_PUBLISH_INTERVAL_MS,
    );

    if (logDistribution) {
      logDistribution.start(
        env.LOG_NOTIFICATION_PUBLISH_INTERVAL_MS,
      );
    }

    logger.info(
      {
        host: env.HOST,
        port: env.PORT,
        environment: env.NODE_ENV,
        apiVersion: env.API_VERSION,
        outboxPublishIntervalMs:
          queueEnv.DEPLOYMENT_OUTBOX_PUBLISH_INTERVAL_MS,
      },
      "CloudPilot API started",
    );
  } catch (error) {
    logger.fatal(
      { err: error },
      "CloudPilot API initialization failed",
    );

    void shutdown(1, "startupFailure");
  }
});
