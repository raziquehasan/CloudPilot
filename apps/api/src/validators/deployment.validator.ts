import { z } from "zod";
import { deploymentStatuses } from "../services/deployment-state-machine.js";
import { branchNameSchema, commitShaSchema } from "./project-intelligence.validator.js";

export const deploymentEnvironmentSchema = z.enum([
  "DEVELOPMENT",
  "STAGING",
  "PRODUCTION",
  "PREVIEW",
]);

export const deploymentStatusSchema = z.enum(deploymentStatuses);

// A fixed host port is either 0 (dynamic allocation) or an
// integer in the non-privileged range. Values 1-1023 are
// rejected so a privileged port cannot be requested.
export const requestedHostPortSchema = z.number().int().refine(
  (value) =>
    value === 0 ||
    (value >= 1_024 && value <= 65_535),
  {
    message:
      "requestedHostPort must be 0 or an integer between 1024 and 65535.",
  },
);

export const createDeploymentSchema = z.object({
  branch: branchNameSchema.optional(),
  commit: commitShaSchema.optional(),
  environment: deploymentEnvironmentSchema,
  requestedHostPort: requestedHostPortSchema.optional(),
}).strict().refine(
  (value) => value.branch !== undefined || value.commit !== undefined,
  { message: "A branch or commit must be provided." },
);

export const deploymentListQuerySchema = z.object({
  page: z.coerce.number().int().min(1).max(10_000).default(1),
  perPage: z.coerce.number().int().min(1).max(100).default(20),
  status: deploymentStatusSchema.optional(),
}).strict();

export const idempotencyKeySchema = z.string()
  .trim()
  .min(8)
  .max(128)
  .regex(/^[A-Za-z0-9._:-]+$/u);

export const deploymentIdSchema = z.string().uuid();
