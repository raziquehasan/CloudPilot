/**
 * CloudPilot — API Environment Configuration
 *
 * Phase 5.6 — Realtime Deployment Logs
 *
 * Responsibilities:
 *
 * - Load environment configuration.
 * - Validate API and infrastructure settings.
 * - Validate authentication configuration.
 * - Validate GitHub integration credentials.
 * - Validate deployment logging configuration.
 * - Restrict Redis notification namespaces.
 * - Validate CORS origins.
 * - Prevent accidental configuration mutation.
 * - Avoid exposing sensitive environment values.
 */

import { config } from "dotenv";

import { fileURLToPath } from "node:url";

import { resolve } from "node:path";

import { z } from "zod";

// ==================================================
// Environment loading
// ==================================================

config({
  path: fileURLToPath(
    new URL(
      "../../../../.env",
      import.meta.url,
    ),
  ),
});

config({
  path: fileURLToPath(
    new URL(
      "../../.env",
      import.meta.url,
    ),
  ),
});

const PROJECT_ROOT = fileURLToPath(
  new URL(
    "../../../../",
    import.meta.url,
  ),
);

// ==================================================
// Constants
// ==================================================

const MAX_TIMER_MS = 2_147_483_647;

const MAX_LOG_HISTORY_PAGE_SIZE = 1_000;

const MAX_LOG_SOCKET_SUBSCRIPTIONS = 20;

const MAX_CORS_ORIGINS = 20;

const MAX_CORS_ORIGIN_LENGTH = 512;

const MAX_REDIS_PREFIX_LENGTH = 100;

const REDIS_PREFIX_PATTERN =
  /^[a-zA-Z0-9_-]+(?::[a-zA-Z0-9_-]+)*$/;

const POSITIVE_INTEGER_PATTERN =
  /^[1-9][0-9]*$/;

// ==================================================
// Environment preprocessing
// ==================================================

/**
 * Treat empty optional configuration values
 * as undefined.
 */
function emptyToUndefined(
  value: unknown,
): unknown {
  if (
    typeof value === "string" &&
    value.trim().length === 0
  ) {
    return undefined;
  }

  return value;
}

/**
 * Parse integers without accepting hexadecimal,
 * exponential notation, decimals or booleans.
 */
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

      if (
        !POSITIVE_INTEGER_PATTERN.test(
          normalized,
        )
      ) {
        return value;
      }

      return Number(normalized);
    },

    z.number()
      .int()
      .min(minimum)
      .max(maximum)
      .default(defaultValue),
  );
}

/**
 * Explicit boolean parsing.
 *
 * Never use Boolean("false") because it
 * evaluates to true.
 */
function booleanEnv(
  defaultValue = false,
) {
  return z.preprocess(
    (value) => {
      const normalized =
        emptyToUndefined(value);

      return typeof normalized === "string"
        ? normalized.trim().toLowerCase()
        : normalized;
    },

    z.enum([
      "true",
      "false",
      "1",
      "0",
    ])
      .default(
        defaultValue ? "true" : "false",
      )
      .transform(
        (value) =>
          value === "true" ||
          value === "1",
      ),
  );
}

// ==================================================
// Optional configuration helpers
// ==================================================

function optionalString(
  maximumLength = 4_096,
) {
  return z.preprocess(
    emptyToUndefined,

    z.string()
      .trim()
      .min(1)
      .max(maximumLength)
      .optional(),
  );
}

function optionalSecret(
  minimumLength = 1,
  maximumLength = 4_096,
) {
  return z.preprocess(
    emptyToUndefined,

    z.string()
      .min(minimumLength)
      .max(maximumLength)
      .refine(
        (value) =>
          value === value.trim() &&
          !/[\u0000-\u001f\u007f]/u.test(
            value,
          ),

        {
          message:
            "Invalid secret configuration.",
        },
      )
      .optional(),
  );
}

// ==================================================
// URL validation
// ==================================================

function isInfrastructureUrl(
  value: string,
  protocols: readonly string[],
): boolean {
  if (
    value.length === 0 ||
    value.length > 8_192 ||
    /[\u0000-\u0020\u007f]/u.test(value)
  ) {
    return false;
  }

  try {
    const url = new URL(value);

    return (
      protocols.includes(url.protocol) &&
      url.hostname.length > 0 &&
      url.hash.length === 0
    );
  } catch {
    return false;
  }
}

function isHttpUrl(
  value: string,
): boolean {
  if (
    !isInfrastructureUrl(
      value,
      ["http:", "https:"],
    )
  ) {
    return false;
  }

  try {
    const url = new URL(value);

    return (
      url.username.length === 0 &&
      url.password.length === 0
    );
  } catch {
    return false;
  }
}

function optionalCallbackUrl() {
  return z.preprocess(
    emptyToUndefined,

    z.string()
      .trim()
      .max(2_048)
      .refine(
        isHttpUrl,

        {
          message:
            "Invalid HTTP callback URL.",
        },
      )
      .optional(),
  );
}

// ==================================================
// CORS validation
// ==================================================

function parseCorsOrigins(
  value: string,
): string[] | null {
  const parts = value
    .split(",")
    .map((origin) => origin.trim());

  if (
    parts.length === 0 ||
    parts.length > MAX_CORS_ORIGINS
  ) {
    return null;
  }

  const origins = new Set<string>();

  for (const origin of parts) {
    if (
      origin.length === 0 ||
      origin.length >
        MAX_CORS_ORIGIN_LENGTH ||
      origin === "*" ||
      origin === "null" ||
      !isHttpUrl(origin)
    ) {
      return null;
    }

    try {
      const url = new URL(origin);

      if (
        url.pathname !== "/" ||
        url.search.length > 0 ||
        url.hash.length > 0
      ) {
        return null;
      }

      // Normalize trailing slashes, hostname
      // casing and default protocol ports.

      origins.add(url.origin);
    } catch {
      return null;
    }
  }

  return Array.from(origins);
}

// ==================================================
// Environment schema
// ==================================================

const envSchema = z.object({

  // ------------------------------------------------
  // API runtime
  // ------------------------------------------------

  NODE_ENV: z.enum([
    "development",
    "test",
    "production",
  ]).default("development"),

  PORT: integerEnv(
    1,
    65_535,
    8_000,
  ),

  HOST: z.string()
    .trim()
    .min(1)
    .max(253)
    .refine(
      (value) =>
        !/[\s\u0000-\u001f\u007f/\\]/u.test(
          value,
        ),

      {
        message: "Invalid API host.",
      },
    )
    .default("127.0.0.1"),

  API_VERSION: z.string()
    .trim()
    .regex(/^v[1-9][0-9]{0,3}$/)
    .default("v1"),

  // ------------------------------------------------
  // CORS
  // ------------------------------------------------

  CORS_ORIGIN: z.string()
    .trim()
    .min(1)
    .max(
      MAX_CORS_ORIGINS *
        (MAX_CORS_ORIGIN_LENGTH + 1),
    )
    .refine(
      (value) =>
        parseCorsOrigins(value) !== null,

      {
        message:
          "CORS_ORIGIN must contain valid HTTP or HTTPS origins.",
      },
    )
    .default("http://localhost:3000"),

  // ------------------------------------------------
  // Logging
  // ------------------------------------------------

  LOG_LEVEL: z.enum([
    "fatal",
    "error",
    "warn",
    "info",
    "debug",
    "trace",
    "silent",
  ]).default("info"),

  // ------------------------------------------------
  // Rate limiting
  // ------------------------------------------------

  RATE_LIMIT_WINDOW_MS: integerEnv(
    1_000,
    MAX_TIMER_MS,
    900_000,
  ),

  RATE_LIMIT_MAX: integerEnv(
    1,
    100_000,
    100,
  ),

  // ------------------------------------------------
  // PostgreSQL
  // ------------------------------------------------

  DATABASE_URL: z.string()
    .trim()
    .min(1)
    .max(8_192)
    .refine(
      (value) =>
        isInfrastructureUrl(
          value,
          ["postgresql:"],
        ),

      {
        message:
          "Invalid PostgreSQL connection URL.",
      },
    ),

  // ------------------------------------------------
  // Redis
  // ------------------------------------------------

  REDIS_URL: z.string()
    .trim()
    .min(1)
    .max(8_192)
    .refine(
      (value) =>
        isInfrastructureUrl(
          value,
          ["redis:"],
        ),

      {
        message:
          "Invalid Redis connection URL.",
      },
    ),

  // ------------------------------------------------
  // GitHub OAuth
  // ------------------------------------------------

  GITHUB_CLIENT_ID:
    optionalString(256),

  GITHUB_CLIENT_SECRET:
    optionalSecret(),

  GITHUB_CALLBACK_URL:
    optionalCallbackUrl(),

  // ------------------------------------------------
  // GitHub App
  // ------------------------------------------------

  GITHUB_APP_ID: z.preprocess(
    emptyToUndefined,

    z.string()
      .trim()
      .regex(/^[1-9][0-9]{0,19}$/)
      .optional(),
  ),

  GITHUB_APP_SLUG: z.preprocess(
    emptyToUndefined,

    z.string()
      .trim()
      .min(1)
      .max(100)
      .regex(/^[a-zA-Z0-9-]+$/)
      .optional(),
  ),

  GITHUB_APP_PRIVATE_KEY_PATH:
    optionalString(),

  // ------------------------------------------------
  // GitHub App user authorization
  // ------------------------------------------------

  GITHUB_APP_CLIENT_ID:
    optionalString(256),

  GITHUB_APP_CLIENT_SECRET:
    optionalSecret(),

  GITHUB_INSTALLATION_OAUTH_CALLBACK_URL:
    optionalCallbackUrl(),

  // ------------------------------------------------
  // Authentication
  // ------------------------------------------------

  AUTH_SECRET:
    optionalSecret(32),

  // ------------------------------------------------
  // Environment variable encryption
  // ------------------------------------------------

  ENVIRONMENT_VARIABLE_ENCRYPTION_KEY:
    z.preprocess(
      emptyToUndefined,

      z.string()
        .regex(/^[a-f0-9]{64}$/i)
        .optional(),
    ),

  // ------------------------------------------------
  // Source workspace
  // ------------------------------------------------

  SOURCE_WORKSPACE_ROOT: z.string()
    .trim()
    .min(1)
    .max(4_096)
    .refine(
      (value) =>
        !/[\u0000-\u001f\u007f]/u.test(
          value,
        ),

      {
        message:
          "Invalid source workspace path.",
      },
    )
    .default("runtime/workspaces")
    .transform(
      (value) =>
        resolve(PROJECT_ROOT, value),
    ),

  // ------------------------------------------------
  // Deployment logging feature flag
  // ------------------------------------------------

  MANUAL_RECOVERY_ENABLED: booleanEnv(false),
  DEPLOYMENT_LOGS_ENABLED:
    booleanEnv(false),

  // ------------------------------------------------
  // Historical log pagination
  // ------------------------------------------------

  LOG_HISTORY_PAGE_SIZE: integerEnv(
    1,
    MAX_LOG_HISTORY_PAGE_SIZE,
    200,
  ),

  LOG_HISTORY_MAX_PAGE_SIZE: integerEnv(
    1,
    MAX_LOG_HISTORY_PAGE_SIZE,
    1_000,
  ),

  // ------------------------------------------------
  // Socket.IO subscription limits
  // ------------------------------------------------

  LOG_SOCKET_MAX_SUBSCRIPTIONS: integerEnv(
    1,
    MAX_LOG_SOCKET_SUBSCRIPTIONS,
    10,
  ),

  // ------------------------------------------------
  // Redis notification publishing
  // ------------------------------------------------

  LOG_NOTIFICATION_PUBLISH_INTERVAL_MS:
    integerEnv(
      100,
      60_000,
      1_000,
    ),

  // ------------------------------------------------
  // Permission revalidation
  // ------------------------------------------------

  LOG_PERMISSION_REVALIDATE_MS:
    integerEnv(
      1_000,
      600_000,
      30_000,
    ),

  // ------------------------------------------------
  // Redis log notification namespace
  // ------------------------------------------------

  LOG_REDIS_PREFIX: z.string()
    .trim()
    .min(1)
    .max(MAX_REDIS_PREFIX_LENGTH)
    .regex(
      REDIS_PREFIX_PATTERN,

      {
        message:
          "Invalid Redis log namespace.",
      },
    )
    .default("cloudpilot:logs"),

}).superRefine((values, context) => {

  // ------------------------------------------------
  // History pagination consistency
  // ------------------------------------------------

  if (
    values.LOG_HISTORY_PAGE_SIZE >
    values.LOG_HISTORY_MAX_PAGE_SIZE
  ) {
    context.addIssue({
      code: "custom",

      path: ["LOG_HISTORY_PAGE_SIZE"],

      message:
        "LOG_HISTORY_PAGE_SIZE must not exceed LOG_HISTORY_MAX_PAGE_SIZE.",
    });
  }

  // ------------------------------------------------
  // Production authentication
  // ------------------------------------------------

  if (
    values.NODE_ENV === "production" &&
    !values.AUTH_SECRET
  ) {
    context.addIssue({
      code: "custom",

      path: ["AUTH_SECRET"],

      message:
        "AUTH_SECRET is required in production.",
    });
  }

  // ------------------------------------------------
  // GitHub OAuth configuration
  // ------------------------------------------------

  const oauthFields = [
    values.GITHUB_CLIENT_ID,
    values.GITHUB_CLIENT_SECRET,
  ];

  if (
    oauthFields.some(Boolean) &&
    !oauthFields.every(Boolean)
  ) {
    context.addIssue({
      code: "custom",

      path: ["GITHUB_CLIENT_ID"],

      message:
        "GitHub OAuth credentials must be configured together.",
    });
  }

  if (
    oauthFields.every(Boolean) &&
    !values.GITHUB_CALLBACK_URL
  ) {
    context.addIssue({
      code: "custom",

      path: ["GITHUB_CALLBACK_URL"],

      message:
        "GITHUB_CALLBACK_URL is required with GitHub OAuth credentials.",
    });
  }

  // ------------------------------------------------
  // GitHub App configuration
  // ------------------------------------------------

  const appFields = [
    values.GITHUB_APP_ID,

    values.GITHUB_APP_SLUG,

    values.GITHUB_APP_PRIVATE_KEY_PATH,
  ];

  if (
    appFields.some(Boolean) &&
    !appFields.every(Boolean)
  ) {
    context.addIssue({
      code: "custom",

      path: ["GITHUB_APP_ID"],

      message:
        "GITHUB_APP_ID, GITHUB_APP_SLUG, and GITHUB_APP_PRIVATE_KEY_PATH must be configured together.",
    });
  }

  // ------------------------------------------------
  // GitHub App OAuth configuration
  // ------------------------------------------------

  const appOAuthFields = [
    values.GITHUB_APP_CLIENT_ID,

    values.GITHUB_APP_CLIENT_SECRET,
  ];

  if (
    appOAuthFields.some(Boolean) &&
    !appOAuthFields.every(Boolean)
  ) {
    context.addIssue({
      code: "custom",

      path: ["GITHUB_APP_CLIENT_ID"],

      message:
        "GITHUB_APP_CLIENT_ID and GITHUB_APP_CLIENT_SECRET must be configured together.",
    });
  }

  if (
    appOAuthFields.every(Boolean) &&
    !values.GITHUB_INSTALLATION_OAUTH_CALLBACK_URL
  ) {
    context.addIssue({
      code: "custom",

      path: [
        "GITHUB_INSTALLATION_OAUTH_CALLBACK_URL",
      ],

      message:
        "GitHub App user authorization requires a callback URL.",
    });
  }

});

// ==================================================
// Environment validation
// ==================================================

const parsed = envSchema.safeParse(
  process.env,
);

if (!parsed.success) {
  // Report configuration field names only.
  //
  // Never include raw environment values,
  // credentials, connection strings or
  // potentially sensitive validator output.

  const invalidFields = Array.from(
    new Set(
      parsed.error.issues.map(
        (issue) =>
          issue.path.join(".") ||
          "environment",
      ),
    ),
  );

  throw new Error(
    "Invalid environment configuration. " +
    `Check the following fields: ${invalidFields.join(", ")}`,
  );
}

// ==================================================
// Normalized CORS origins
// ==================================================

const corsOrigins = parseCorsOrigins(
  parsed.data.CORS_ORIGIN,
);

if (
  corsOrigins === null ||
  corsOrigins.length === 0
) {
  throw new Error(
    "Invalid CORS_ORIGIN configuration.",
  );
}

// ==================================================
// Immutable environment export
// ==================================================

export const env = Object.freeze({
  ...parsed.data,

  corsOrigins: Object.freeze(
    corsOrigins,
  ),
});
