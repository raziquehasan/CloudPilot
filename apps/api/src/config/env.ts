import { config } from "dotenv";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { z } from "zod";

config({ path: fileURLToPath(new URL("../../../../.env", import.meta.url)) });
config({ path: fileURLToPath(new URL("../../.env", import.meta.url)) });

const PROJECT_ROOT = fileURLToPath(new URL("../../../../", import.meta.url));

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: z.coerce.number().int().min(1).max(65535).default(8000),
  HOST: z.string().min(1).default("127.0.0.1"),
  API_VERSION: z.string().trim().min(1).default("v1"),
  CORS_ORIGIN: z.string().trim().min(1).default("http://localhost:3000"),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
  RATE_LIMIT_WINDOW_MS: z.coerce.number().int().min(1000).default(900000),
  RATE_LIMIT_MAX: z.coerce.number().int().min(1).default(100),
  DATABASE_URL: z.string().trim().min(1).startsWith("postgresql://"),
  REDIS_URL: z.string().trim().min(1).startsWith("redis://"),
  GITHUB_CLIENT_ID: z.string().trim().min(1).optional(),
  GITHUB_CLIENT_SECRET: z.string().trim().min(1).optional(),
  GITHUB_CALLBACK_URL: z.string().trim().url().optional(),
  GITHUB_APP_ID: z.string().trim().regex(/^\d+$/).optional(),
  GITHUB_APP_SLUG: z.string().trim().regex(/^[a-zA-Z0-9-]+$/).optional(),
  GITHUB_APP_PRIVATE_KEY_PATH: z.string().trim().min(1).optional(),
  GITHUB_APP_CLIENT_ID: z.string().trim().min(1).optional(),
  GITHUB_APP_CLIENT_SECRET: z.string().trim().min(1).optional(),
  GITHUB_INSTALLATION_OAUTH_CALLBACK_URL: z.string().trim().url().optional(),
  AUTH_SECRET: z.string().trim().min(32).optional(),
  ENVIRONMENT_VARIABLE_ENCRYPTION_KEY: z.preprocess((value) => value === "" ? undefined : value, z.string().regex(/^[a-f0-9]{64}$/i).optional()),
  SOURCE_WORKSPACE_ROOT: z.string().trim().min(1).default("runtime/workspaces").transform((value) => resolve(PROJECT_ROOT, value)),
}).superRefine((values, context) => {
  const appFields = [values.GITHUB_APP_ID, values.GITHUB_APP_SLUG, values.GITHUB_APP_PRIVATE_KEY_PATH];
  if (appFields.some(Boolean) && !appFields.every(Boolean)) {
    context.addIssue({ code: "custom", message: "GITHUB_APP_ID, GITHUB_APP_SLUG, and GITHUB_APP_PRIVATE_KEY_PATH must be configured together." });
  }
  const appOAuthFields = [values.GITHUB_APP_CLIENT_ID, values.GITHUB_APP_CLIENT_SECRET];
  if (appOAuthFields.some(Boolean) && !appOAuthFields.every(Boolean)) {
    context.addIssue({ code: "custom", message: "GITHUB_APP_CLIENT_ID and GITHUB_APP_CLIENT_SECRET must be configured together." });
  }
  if (appOAuthFields.every(Boolean) && !values.GITHUB_INSTALLATION_OAUTH_CALLBACK_URL) {
    context.addIssue({ code: "custom", message: "GITHUB_INSTALLATION_OAUTH_CALLBACK_URL is required with GitHub App user authorization credentials." });
  }
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  const details = parsed.error.issues
    .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
    .join("; ");
  throw new Error(`Invalid environment configuration: ${details}`);
}

export const env = Object.freeze({
  ...parsed.data,
  corsOrigins: parsed.data.CORS_ORIGIN.split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0),
});
