
/**
 * CloudPilot — Queue Job Contracts
 *
 * Shared runtime validation and TypeScript contracts
 * for all supported CloudPilot background jobs.
 *
 * Queue payloads must contain identifiers and trusted
 * metadata only. Credentials, private keys, secrets,
 * and unrestricted filesystem paths must never be
 * included in job payloads.
 */

import { z } from "zod";

// ==================================================
// Shared validation schemas
// ==================================================

/**
 * Reject control characters that could interfere with
 * structured logging, tracing, or identifier handling.
 */
const controlCharacterPattern =
  /[\u0000-\u001f\u007f-\u009f]/u;

/**
 * Bounded identifiers used for correlation and
 * idempotency tracking.
 */
const identifierSchema = z
  .string()
  .refine(
    (value) => !controlCharacterPattern.test(value),
    {
      message: "Identifier contains invalid control characters.",
    },
  )
  .pipe(
    z.string().trim().min(1).max(128),
  );

/**
 * Canonical database identifier validation.
 */
const uuidSchema = z.uuid();

/**
 * Immutable Git commit SHA.
 *
 * Only full lowercase 40-character Git SHA-1 commit
 * identifiers are supported by the current pipeline.
 *
 * Syntax validation does not establish that a commit
 * exists or belongs to an authorized repository.
 */
export const commitShaSchema = z
  .string()
  .length(40)
  .regex(/^[0-9a-f]{40}$/u, {
    message: "Invalid resolved commit SHA.",
  });

// ==================================================
// Shared deployment identity
// ==================================================

/**
 * Every deployment-related job must carry its tenant
 * and deployment identity.
 *
 * These identifiers must still be verified against
 * authoritative database records by the processor.
 */
const deploymentIdentitySchema = z.object({
  workspaceId: uuidSchema,
  projectId: uuidSchema,
  deploymentId: uuidSchema,
});

// ==================================================
// Diagnostic job
// ==================================================

export const diagnosticJobSchema = z
  .object({
    kind: z.literal("diagnostic"),

    correlationId: identifierSchema,

    message: z
      .string()
      .trim()
      .min(1)
      .max(500)
      .optional(),

    transientFailureOnce: z.boolean().optional(),

    permanentFailure: z.boolean().optional(),

    blockForMs: z
      .number()
      .int()
      .min(1)
      .max(30_000)
      .optional(),
  })
  .strict()
  .refine(
    (job) =>
      !(
        job.transientFailureOnce === true &&
        job.permanentFailure === true
      ),
    {
      message:
        "Transient and permanent failure simulation cannot both be enabled.",
      path: ["permanentFailure"],
    },
  );

// ==================================================
// Deployment source preparation job
// ==================================================

export const deploymentJobSchema = z
  .object({
    ...deploymentIdentitySchema.shape,

    kind: z.literal("deployment"),

    eventId: uuidSchema,

    correlationId: identifierSchema,

    idempotencyKey: identifierSchema,
  })
  .strict();

// ==================================================
// Docker build job
// ==================================================

/**
 * Phase 5.4 build job.
 *
 * The processor must verify:
 *
 * - Workspace and project ownership
 * - Deployment identity
 * - Build record identity
 * - Source preparation ownership
 * - Resolved commit consistency
 * - Source preparation readiness
 * - Build execution policy
 * - Execution lease and fencing token
 *
 * Queue payload validation alone does not authorize
 * build execution.
 */
export const buildJobSchema = z
  .object({
    ...deploymentIdentitySchema.shape,

    kind: z.literal("build"),

    buildId: uuidSchema,

    sourcePreparationId: uuidSchema,

    resolvedCommitSha: commitShaSchema,

    correlationId: identifierSchema,
  })
  .strict();

// ==================================================
// Rollback job
// ==================================================

/**
 * Reserved rollback job contract.
 *
 * The existence of this schema does not imply that
 * rollback execution is currently implemented.
 */
export const rollbackJobSchema = z
  .object({
    ...deploymentIdentitySchema.shape,

    kind: z.literal("rollback"),

    rollbackOfId: uuidSchema,

    correlationId: identifierSchema,

    idempotencyKey: identifierSchema,
  })
  .strict();

// ==================================================
// Supported queue job kinds
// ==================================================

export const queueJobKinds = Object.freeze([
  "diagnostic",
  "deployment",
  "build",
  "rollback",
] as const);

export type QueueJobKind =
  (typeof queueJobKinds)[number];

const queueJobKindSet: ReadonlySet<string> =
  new Set(queueJobKinds);

/**
 * Validate a job kind received at runtime.
 */
export function isQueueJobKind(
  value: unknown,
): value is QueueJobKind {
  return (
    typeof value === "string" &&
    queueJobKindSet.has(value)
  );
}

// ==================================================
// Discriminated queue payload union
// ==================================================

/**
 * All supported queue payloads are distinguished by
 * their literal kind property.
 *
 * Unknown fields and unsupported job kinds are
 * rejected by the corresponding strict schemas.
 */
export const queueJobSchema = z.discriminatedUnion(
  "kind",
  [
    diagnosticJobSchema,
    deploymentJobSchema,
    buildJobSchema,
    rollbackJobSchema,
  ],
);

// ==================================================
// Inferred TypeScript contracts
// ==================================================

export type DiagnosticJob =
  z.infer<typeof diagnosticJobSchema>;

export type DeploymentJob =
  z.infer<typeof deploymentJobSchema>;

export type BuildJob =
  z.infer<typeof buildJobSchema>;

export type RollbackJob =
  z.infer<typeof rollbackJobSchema>;

export type QueueJob =
  z.infer<typeof queueJobSchema>;

// ==================================================
// Queue payload schema registry
// ==================================================

/**
 * Immutable registry of supported payload schemas.
 *
 * The queue manager and worker registry can use this
 * mapping to validate payloads against their expected
 * job kind.
 */
export const queuePayloadSchemas = Object.freeze({
  diagnostic: diagnosticJobSchema,
  deployment: deploymentJobSchema,
  build: buildJobSchema,
  rollback: rollbackJobSchema,
} satisfies Record<QueueJobKind, z.ZodType>);

// ==================================================
// Runtime parsing utilities
// ==================================================

/**
 * Parse an unknown queue payload.
 *
 * Throws a Zod validation error when the payload is
 * invalid. Callers must sanitize validation failures
 * before exposing them to external systems.
 */
export function parseQueueJob(
  payload: unknown,
): QueueJob {
  return queueJobSchema.parse(payload);
}

/**
 * Safely validate an unknown queue payload without
 * throwing a validation error.
 *
 * Workers should treat invalid payloads as permanent
 * failures rather than repeatedly retrying them.
 */
export function safeParseQueueJob(
  payload: unknown,
) {
  return queueJobSchema.safeParse(payload);
}

/**
 * Validate a payload against its expected job kind.
 *
 * This prevents a valid payload for one processor
 * from being accidentally accepted by another.
 */
export function safeParseQueueJobByKind<
  TKind extends QueueJobKind,
>(
  kind: TKind,
  payload: unknown,
) {
  return queuePayloadSchemas[kind].safeParse(payload);
}

// ==================================================
// Diagnostic result
// ==================================================

export interface DiagnosticResult {
  kind: "diagnostic";
  correlationId: string;
  processedAt: string;
}
