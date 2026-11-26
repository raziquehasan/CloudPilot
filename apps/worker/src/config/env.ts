import { config } from "dotenv";
import { isAbsolute, resolve, parse } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";

// ==================================================
// Constants
// ==================================================

const KiB = 1024;
const MiB = 1024 * KiB;
const GiB = 1024 * MiB;

const SECOND = 1000;
const MINUTE = 60 * SECOND;

const MAX_TIMER_MS = 2_147_483_647;

// ==================================================
// Stable project paths
// ==================================================

const PROJECT_ROOT = fileURLToPath(
  new URL("../../../../", import.meta.url),
);

const WORKER_ROOT = fileURLToPath(
  new URL("../../", import.meta.url),
);

// ==================================================
// Environment loading
// ==================================================
//
// Configuration precedence:
//
// 1. Existing process environment
// 2. Root .env
// 3. Worker-specific .env
//
// Worker-specific configuration only fills variables
// that have not already been defined.
//
// Never overwrite environment variables injected by
// Docker, CI/CD or production secret managers.
// ==================================================

config({
  path: resolve(PROJECT_ROOT, ".env"),
  override: false,
  quiet: true,
});

config({
  path: resolve(WORKER_ROOT, ".env"),
  override: false,
  quiet: true,
});

// ==================================================
// Validation helpers
// ==================================================

function integerEnv(
  minimum: number,
  maximum: number,
  defaultValue: number,
) {
  return z.preprocess(
    (value) => {
      if (typeof value !== "string") {
        return value;
      }

      const normalized = value.trim();

      if (!/^[0-9]+$/u.test(normalized)) {
        return value;
      }

      return Number(normalized);
    },

    z
      .number()
      .int()
      .min(minimum)
      .max(maximum)
      .default(defaultValue),
  );
}

function decimalEnv(
  minimum: number,
  maximum: number,
  defaultValue: number,
) {
  return z.preprocess(
    (value) => {
      if (typeof value !== "string") {
        return value;
      }

      const normalized = value.trim();

      if (
        !/^(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$/u.test(
          normalized,
        )
      ) {
        return value;
      }

      return Number(normalized);
    },

    z
      .number()
      .finite()
      .min(minimum)
      .max(maximum)
      .default(defaultValue),
  );
}

function optionalString() {
  return z.preprocess(
    (value) => {
      if (typeof value !== "string") {
        return value;
      }

      const normalized = value.trim();

      return normalized.length > 0
        ? normalized
        : undefined;
    },

    z.string().min(1).optional(),
  );
}

function resolveProjectPath(value: string): string {
  return isAbsolute(value)
    ? resolve(value)
    : resolve(PROJECT_ROOT, value);
}

function isFilesystemRoot(value: string): boolean {
  return resolve(value) === parse(resolve(value)).root;
}

function isValidRedisUrl(value: string): boolean {
  try {
    const url = new URL(value);

    if (
      url.protocol !== "redis:" &&
      url.protocol !== "rediss:"
    ) {
      return false;
    }

    if (!url.hostname || url.hash !== "") {
      return false;
    }

    if (
      url.pathname !== "" &&
      url.pathname !== "/" &&
      !/^\/[0-9]+$/u.test(url.pathname)
    ) {
      return false;
    }

    if (url.port !== "") {
      const port = Number(url.port);

      if (
        !Number.isInteger(port) ||
        port < 1 ||
        port > 65535
      ) {
        return false;
      }
    }

    return true;
  } catch {
    return false;
  }
}

function isValidGitHubAppId(value: string): boolean {
  if (!/^[1-9][0-9]*$/u.test(value)) {
    return false;
  }

  const numeric = Number(value);

  return (
    Number.isSafeInteger(numeric) &&
    numeric > 0
  );
}

// ==================================================
// Environment schema
// ==================================================

const envSchema = z
  .object({
    // ==============================================
    // Application environment
    // ==============================================

    NODE_ENV: z
      .enum([
        "development",
        "test",
        "production",
      ])
      .default("development"),

    LOG_LEVEL: z
      .enum([
        "fatal",
        "error",
        "warn",
        "info",
        "debug",
        "trace",
        "silent",
      ])
      .default("info"),

    // ==============================================
    // Redis configuration
    // ==============================================

    REDIS_URL: z
      .string()
      .trim()
      .min(1)
      .refine(
        isValidRedisUrl,
        "Must be a valid redis:// or rediss:// URL.",
      ),

    QUEUE_PREFIX: z
      .string()
      .trim()
      .min(1)
      .max(100)
      .regex(
        /^[A-Za-z0-9_-]+$/u,
        "Must contain only letters, numbers, underscores or hyphens.",
      )
      .default("cloudpilot"),

    // ==============================================
    // Worker concurrency
    // ==============================================

    WORKER_CONCURRENCY: integerEnv(
      1,
      100,
      5,
    ),

    DEPLOYMENT_WORKER_CONCURRENCY: integerEnv(
      1,
      20,
      2,
    ),

    // ==============================================
    // Queue retry configuration
    // ==============================================

    QUEUE_JOB_ATTEMPTS: integerEnv(
      1,
      20,
      3,
    ),

    QUEUE_BACKOFF_MS: integerEnv(
      100,
      60 * MINUTE,
      SECOND,
    ),

    QUEUE_BACKOFF_JITTER: decimalEnv(
      0,
      1,
      0.25,
    ),

    // ==============================================
    // Queue retention
    // ==============================================

    QUEUE_COMPLETED_RETENTION: integerEnv(
      1,
      100_000,
      1_000,
    ),

    QUEUE_FAILED_RETENTION: integerEnv(
      1,
      100_000,
      5_000,
    ),

    // ==============================================
    // Execution timeout
    // ==============================================
    //
    // Source retrieval requires more time than a
    // simple diagnostic queue operation.
    //
    // This is a shared queue execution timeout until
    // QueueManager supports per-queue timeouts.
    // ==============================================

    QUEUE_EXECUTION_TIMEOUT_MS: integerEnv(
      100,
      60 * MINUTE,
      5 * MINUTE,
    ),

    // ==============================================
    // Deployment execution lease
    // ==============================================
    //
    // The deployment processor renews this lease
    // periodically while source preparation runs.
    //
    // Lease duration is not the total job duration.
    // ==============================================

    DEPLOYMENT_EXECUTION_LEASE_MS: integerEnv(
      1_000,
      5 * MINUTE,
      MINUTE,
    ),

    // ==============================================
    // Worker registry heartbeat
    // ==============================================

    WORKER_HEARTBEAT_INTERVAL_MS: integerEnv(
      1_000,
      MINUTE,
      5 * SECOND,
    ),

    WORKER_HEARTBEAT_TTL_SECONDS: integerEnv(
      2,
      300,
      20,
    ),

    // ==============================================
    // GitHub App authentication
    // ==============================================

    GITHUB_APP_ID: optionalString().refine(
      (value) =>
        value === undefined ||
        isValidGitHubAppId(value),

      "Must be a positive GitHub App identifier.",
    ),

    GITHUB_APP_PRIVATE_KEY_PATH: optionalString()
      .refine(
        (value) =>
          value === undefined ||
          !value.includes("\0"),

        "Private key path is invalid.",
      )
      .transform((value) =>
        value === undefined
          ? undefined
          : resolveProjectPath(value),
      ),

    // ==============================================
    // Source workspace configuration
    // ==============================================

    SOURCE_WORKSPACE_ROOT: z
      .string()
      .trim()
      .min(1)
      .refine(
        (value) => !value.includes("\0"),

        "Workspace path is invalid.",
      )
      .default("runtime/workspaces")
      .transform(resolveProjectPath)
      .refine(
        (value) => !isFilesystemRoot(value),

        "Workspace directory cannot be a filesystem root.",
      ),

    // ==============================================
    // Archive download timeout
    // ==============================================

    SOURCE_DOWNLOAD_TIMEOUT_MS: integerEnv(
      SECOND,
      10 * MINUTE,
      MINUTE,
    ),

    // ==============================================
    // Archive resource limits
    // ==============================================

    SOURCE_MAX_ARCHIVE_BYTES: integerEnv(
      KiB,
      2 * GiB,
      100 * MiB,
    ),

    SOURCE_MAX_EXTRACTED_BYTES: integerEnv(
      KiB,
      8 * GiB,
      500 * MiB,
    ),

    SOURCE_MAX_FILE_BYTES: integerEnv(
      KiB,
      2 * GiB,
      50 * MiB,
    ),

    SOURCE_MAX_FILE_COUNT: integerEnv(
      1,
      1_000_000,
      20_000,
    ),

    // ==============================================
    // Source download retry configuration
    // ==============================================

    SOURCE_MAX_RETRIES: integerEnv(
      0,
      10,
      3,
    ),

    // ==============================================
    // Workspace retention and cleanup
    // ==============================================

    SOURCE_WORKSPACE_TTL_SECONDS: integerEnv(
      60,
      86_400,
      3_600,
    ),

    SOURCE_CLEANUP_INTERVAL_SECONDS: integerEnv(
      10,
      3_600,
      300,
    ),

    // ==============================================
    // Storage safety threshold
    // ==============================================

    SOURCE_MIN_FREE_BYTES: integerEnv(
      0,
      8 * GiB,
      100 * MiB,
    ),
  })

  // ==================================================
  // Cross-field validation
  // ==================================================

  .superRefine((value, context) => {
    // ==============================================
    // GitHub App configuration consistency
    // ==============================================

    const hasAppId =
      value.GITHUB_APP_ID !== undefined;

    const hasPrivateKey =
      value.GITHUB_APP_PRIVATE_KEY_PATH !== undefined;

    if (hasAppId !== hasPrivateKey) {
      context.addIssue({
        code: "custom",

        path: [
          hasAppId
            ? "GITHUB_APP_PRIVATE_KEY_PATH"
            : "GITHUB_APP_ID",
        ],

        message:
          "GitHub App ID and private key path must be configured together.",
      });
    }

    // ==============================================
    // Heartbeat TTL safety
    // ==============================================

    const heartbeatTtlMs =
      value.WORKER_HEARTBEAT_TTL_SECONDS *
      SECOND;

    if (
      value.WORKER_HEARTBEAT_INTERVAL_MS * 2 >=
      heartbeatTtlMs
    ) {
      context.addIssue({
        code: "custom",

        path: [
          "WORKER_HEARTBEAT_TTL_SECONDS",
        ],

        message:
          "Heartbeat TTL must exceed twice the heartbeat interval.",
      });
    }

    // ==============================================
    // Execution lease safety
    // ==============================================
    //
    // deployment.processor.ts renews the execution
    // lease while the deployment is active.
    //
    // Therefore, the lease does not need to exceed
    // the entire queue execution timeout.
    // ==============================================

    if (
      value.DEPLOYMENT_EXECUTION_LEASE_MS <
      10 * SECOND
    ) {
      context.addIssue({
        code: "custom",

        path: [
          "DEPLOYMENT_EXECUTION_LEASE_MS",
        ],

        message:
          "Execution lease must be at least 10 seconds to provide renewal margin.",
      });
    }

    // ==============================================
    // Queue and download timeout compatibility
    // ==============================================

    if (
      value.QUEUE_EXECUTION_TIMEOUT_MS <=
      value.SOURCE_DOWNLOAD_TIMEOUT_MS
    ) {
      context.addIssue({
        code: "custom",

        path: [
          "QUEUE_EXECUTION_TIMEOUT_MS",
        ],

        message:
          "Queue execution timeout must exceed the source download timeout.",
      });
    }

    // ==============================================
    // File extraction limits
    // ==============================================

    if (
      value.SOURCE_MAX_FILE_BYTES >
      value.SOURCE_MAX_EXTRACTED_BYTES
    ) {
      context.addIssue({
        code: "custom",

        path: [
          "SOURCE_MAX_FILE_BYTES",
        ],

        message:
          "Maximum file size must not exceed the total extraction size limit.",
      });
    }

    // ==============================================
    // Workspace cleanup interval
    // ==============================================

    if (
      value.SOURCE_CLEANUP_INTERVAL_SECONDS >
      value.SOURCE_WORKSPACE_TTL_SECONDS
    ) {
      context.addIssue({
        code: "custom",

        path: [
          "SOURCE_CLEANUP_INTERVAL_SECONDS",
        ],

        message:
          "Workspace cleanup interval must not exceed workspace TTL.",
      });
    }

    // ==============================================
    // Workspace location safety
    // ==============================================

    if (
      value.SOURCE_WORKSPACE_ROOT ===
        resolve(PROJECT_ROOT) ||
      value.SOURCE_WORKSPACE_ROOT ===
        resolve(WORKER_ROOT)
    ) {
      context.addIssue({
        code: "custom",

        path: [
          "SOURCE_WORKSPACE_ROOT",
        ],

        message:
          "Source workspace must use a dedicated runtime directory.",
      });
    }

    // ==============================================
    // Private key location safety
    // ==============================================

    if (value.GITHUB_APP_PRIVATE_KEY_PATH) {
      const workspaceRoot = resolve(
        value.SOURCE_WORKSPACE_ROOT,
      );

      const keyPath = resolve(
        value.GITHUB_APP_PRIVATE_KEY_PATH,
      );

      const relativeKeyPath = resolve(
        workspaceRoot,
        keyPath,
      );

      const workspacePrefix =
        workspaceRoot.endsWith("/")
          ? workspaceRoot
          : `${workspaceRoot}/`;

      const normalizedKeyPath =
        relativeKeyPath
          .replace(/\\/gu, "/");

      const normalizedWorkspacePrefix =
        workspacePrefix
          .replace(/\\/gu, "/");

      const comparableKeyPath =
        process.platform === "win32"
          ? normalizedKeyPath.toLowerCase()
          : normalizedKeyPath;

      const comparableWorkspacePrefix =
        process.platform === "win32"
          ? normalizedWorkspacePrefix.toLowerCase()
          : normalizedWorkspacePrefix;

      if (
        comparableKeyPath.startsWith(
          comparableWorkspacePrefix,
        )
      ) {
        context.addIssue({
          code: "custom",

          path: [
            "GITHUB_APP_PRIVATE_KEY_PATH",
          ],

          message:
            "GitHub App private key must not be stored inside the source workspace.",
        });
      }
    }

    // ==============================================
    // Node.js timer safety
    // ==============================================

    const timerValues = [
      {
        name: "QUEUE_EXECUTION_TIMEOUT_MS",
        value: value.QUEUE_EXECUTION_TIMEOUT_MS,
      },

      {
        name: "QUEUE_BACKOFF_MS",
        value: value.QUEUE_BACKOFF_MS,
      },

      {
        name: "SOURCE_DOWNLOAD_TIMEOUT_MS",
        value: value.SOURCE_DOWNLOAD_TIMEOUT_MS,
      },

      {
        name: "WORKER_HEARTBEAT_INTERVAL_MS",
        value: value.WORKER_HEARTBEAT_INTERVAL_MS,
      },

      {
        name: "SOURCE_CLEANUP_INTERVAL_SECONDS",
        value:
          value.SOURCE_CLEANUP_INTERVAL_SECONDS *
          SECOND,
      },
    ];

    for (const timer of timerValues) {
      if (
        !Number.isSafeInteger(timer.value) ||
        timer.value <= 0 ||
        timer.value > MAX_TIMER_MS
      ) {
        context.addIssue({
          code: "custom",

          path: [timer.name],

          message:
            "Configured duration exceeds supported timer limits.",
        });
      }
    }
  });

// ==================================================
// Public environment type
// ==================================================

export type WorkerEnv = Readonly<
  z.infer<typeof envSchema>
>;

// ==================================================
// Environment parsing
// ==================================================

export function parseWorkerEnv(
  environment: Record<string, unknown>,
): WorkerEnv {
  const parsed =
    envSchema.safeParse(environment);

  if (!parsed.success) {
    // Never serialize the complete Zod error,
    // process.env or raw configuration values.
    //
    // Redis URLs may contain credentials and
    // GitHub configuration may contain sensitive paths.

    const details = parsed.error.issues
      .map((issue) => {
        const field =
          issue.path.length > 0
            ? issue.path.join(".")
            : "environment";

        if (issue.code === "custom") {
          return `${field}: ${issue.message}`;
        }

        return `${field}: Invalid configuration value.`;
      })
      .join("; ");

    throw new Error(
      `Invalid worker environment configuration: ${details}`,
    );
  }

  return Object.freeze(parsed.data);
}

// ==================================================
// Validated worker configuration
// ==================================================

export const workerEnv: WorkerEnv =
  parseWorkerEnv(process.env);