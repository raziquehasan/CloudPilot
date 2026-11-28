/**
 * CloudPilot — Worker Registry
 *
 * Redis-backed worker discovery, heartbeat publication,
 * capability reporting, and worker availability tracking.
 *
 * Heartbeats provide advisory worker availability.
 * They do not authorize deployment execution or replace
 * database-backed execution leases and fencing.
 */

import { z } from "zod";
import type { Redis } from "ioredis";

// ==================================================
// Constants
// ==================================================

const MAX_IDENTIFIER_LENGTH = 128;

const MAX_HEARTBEAT_BYTES = 8 * 1024;

const HEARTBEAT_SCAN_BATCH_SIZE = 100;

const MIN_HEARTBEAT_TTL_SECONDS = 2;

const MAX_HEARTBEAT_TTL_SECONDS = 300;

const INVALID_IDENTIFIER_PATTERN =
  /[^A-Za-z0-9_-]/u;

const CONTROL_CHARACTER_PATTERN =
  /[\u0000-\u001f\u007f-\u009f]/u;

// ==================================================
// Shared validation schemas
// ==================================================

const workerIdentifierSchema = z
  .string()
  .min(1)
  .max(MAX_IDENTIFIER_LENGTH)
  .refine(
    (value) =>
      !INVALID_IDENTIFIER_PATTERN.test(value),
    {
      message: "Worker identifier is invalid",
    },
  );

const workerRoleSchema = z.enum([
  "diagnostic",
  "deployment",
]);

const workerVersionSchema = z
  .string()
  .min(1)
  .max(40)
  .refine(
    (value) =>
      value.trim().length > 0 &&
      !CONTROL_CHARACTER_PATTERN.test(value),
    {
      message: "Worker version is invalid",
    },
  );

// ==================================================
// Worker capabilities
// ==================================================

/**
 * Capabilities describe the services and execution
 * facilities currently reported by a worker.
 *
 * Container runtime availability must be established
 * by the worker's actual runtime health checks.
 */

const workerCapabilitiesSchema = z
  .object({
    diagnostic: z.boolean(),

    deploymentQueue: z.boolean(),

    sourceRetrieval: z.boolean(),

    workspaceStorage: z.boolean(),

    dockerBuild: z.boolean(),

    containerExecution: z.boolean(),

    containerRuntime: z.boolean(),

    // Phase 5.7.8 reports whether this worker is actively
    // performing deployment health monitoring. Optional for
    // backward compatibility with older workers that do not
    // report it; absence is treated as "unknown" by readers.
    healthMonitoring: z.boolean().optional(),
    automaticRecovery: z.object({
      configured: z.boolean(),
      available: z.boolean(),
      trustedRepositories: z.array(z.string()),
      heartbeatIntervalMs: z.number().int().positive().optional(),
    }).optional(),
  })
  .strict();

// ==================================================
// Worker heartbeat schema
// ==================================================

export const workerHeartbeatSchema = z
  .object({
    workerId: workerIdentifierSchema,

    role: workerRoleSchema,

    version: workerVersionSchema,

    capabilities: workerCapabilitiesSchema,

    startupAt: z.string().datetime(),

    lastHeartbeatAt: z.string().datetime(),

    available: z.boolean(),

    draining: z.boolean(),

    concurrency: z
      .number()
      .int()
      .min(1)
      .max(100),
  })
  .strict()
  .superRefine((heartbeat, context) => {
    const expectedCapability =
      heartbeat.role === "diagnostic"
        ? "diagnostic"
        : "deploymentQueue";

    if (
      !heartbeat.capabilities[
        expectedCapability
      ]
    ) {
      context.addIssue({
        code: "custom",

        path: [
          "capabilities",
          expectedCapability,
        ],

        message:
          "Capability does not match worker role",
      });
    }

    const startupTime = Date.parse(
      heartbeat.startupAt,
    );

    const heartbeatTime = Date.parse(
      heartbeat.lastHeartbeatAt,
    );

    if (
      !Number.isFinite(startupTime) ||
      !Number.isFinite(heartbeatTime)
    ) {
      context.addIssue({
        code: "custom",

        path: ["lastHeartbeatAt"],

        message:
          "Worker heartbeat timestamp is invalid",
      });

      return;
    }

    if (heartbeatTime < startupTime) {
      context.addIssue({
        code: "custom",

        path: ["lastHeartbeatAt"],

        message:
          "Heartbeat cannot precede worker startup",
      });
    }
  });

// ==================================================
// Public TypeScript contracts
// ==================================================

export type WorkerHeartbeat =
  z.infer<typeof workerHeartbeatSchema>;

export interface WorkerRegistryOptions {
  prefix: string;

  ttlSeconds: number;
}

type WorkerRole = WorkerHeartbeat["role"];

// ==================================================
// Redis atomic operation scripts
// ==================================================

/**
 * Publish the heartbeat and register its key
 * within one atomic Redis operation.
 *
 * Existing Redis key formats are preserved.
 *
 * Redis Lua scripts provide atomic execution,
 * but do not roll back commands after a script
 * encounters a runtime error.
 *
 * Validate the registry index type before writing.
 */

const PUBLISH_HEARTBEAT_SCRIPT = `
local indexType = redis.call("TYPE", KEYS[2]).ok

if indexType ~= "none" and indexType ~= "set" then
  return redis.error_reply("Invalid worker registry index")
end

redis.call(
  "SET",
  KEYS[1],
  ARGV[1],
  "EX",
  ARGV[2]
)

redis.call(
  "SADD",
  KEYS[2],
  KEYS[1]
)

return 1
`;

/**
 * Remove expired index entries only when their
 * heartbeat keys are still absent.
 *
 * This prevents removing an active worker that
 * renewed its heartbeat after a previous read.
 */

const REMOVE_EXPIRED_HEARTBEAT_SCRIPT = `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return redis.call(
    "SREM",
    KEYS[2],
    KEYS[1]
  )
end

return 0
`;

/**
 * Remove a worker heartbeat and its index entry
 * within one atomic Redis operation.
 *
 * The caller must own the worker identity.
 * Worker IDs must be unique per worker process
 * to avoid deleting another process's heartbeat.
 */

const REMOVE_HEARTBEAT_SCRIPT = `
local indexType = redis.call("TYPE", KEYS[2]).ok

if indexType ~= "none" and indexType ~= "set" then
  return redis.error_reply("Invalid worker registry index")
end

redis.call("DEL", KEYS[1])

redis.call(
  "SREM",
  KEYS[2],
  KEYS[1]
)

return 1
`;

// ==================================================
// Registry validation
// ==================================================

function validateOptions(
  options: WorkerRegistryOptions,
): void {
  if (
    !options ||
    typeof options !== "object"
  ) {
    throw new Error(
      "Worker registry options are invalid",
    );
  }

  if (
    typeof options.prefix !== "string" ||
    options.prefix.length === 0 ||
    options.prefix.length >
      MAX_IDENTIFIER_LENGTH ||
    INVALID_IDENTIFIER_PATTERN.test(
      options.prefix,
    )
  ) {
    throw new Error(
      "Worker registry prefix is invalid",
    );
  }

  if (
    !Number.isSafeInteger(
      options.ttlSeconds,
    ) ||
    options.ttlSeconds <
      MIN_HEARTBEAT_TTL_SECONDS ||
    options.ttlSeconds >
      MAX_HEARTBEAT_TTL_SECONDS
  ) {
    throw new Error(
      "Worker heartbeat TTL must be between " +
      `${MIN_HEARTBEAT_TTL_SECONDS} and ` +
      `${MAX_HEARTBEAT_TTL_SECONDS} seconds`,
    );
  }
}

function validateRole(
  role: unknown,
): WorkerRole {
  const parsed =
    workerRoleSchema.safeParse(role);

  if (!parsed.success) {
    throw new Error(
      "Worker registry role is invalid",
    );
  }

  return parsed.data;
}

function validateWorkerId(
  workerId: unknown,
): string {
  const parsed =
    workerIdentifierSchema.safeParse(
      workerId,
    );

  if (!parsed.success) {
    throw new Error(
      "Worker identifier is invalid",
    );
  }

  return parsed.data;
}

// ==================================================
// Redis key generation
// ==================================================

function workerIndexKey(
  prefix: string,
  role: WorkerRole,
): string {
  return `${prefix}:worker-heartbeats:${role}`;
}

function workerKey(
  prefix: string,
  role: WorkerRole,
  workerId: string,
): string {
  return (
    `${prefix}:worker-heartbeat:` +
    `${role}:${workerId}`
  );
}

/**
 * Validate that a registry index entry belongs
 * to the expected namespace and worker role.
 */

function validateIndexedWorkerKey(
  prefix: string,
  role: WorkerRole,
  key: string,
): string {
  const expectedPrefix =
    `${prefix}:worker-heartbeat:${role}:`;

  if (
    typeof key !== "string" ||
    !key.startsWith(expectedPrefix)
  ) {
    throw new Error(
      "Worker registry contains an invalid key",
    );
  }

  const workerId = key.slice(
    expectedPrefix.length,
  );

  return validateWorkerId(workerId);
}

// ==================================================
// Heartbeat serialization
// ==================================================

function serializeHeartbeat(
  heartbeat: WorkerHeartbeat,
): string {
  const serialized =
    JSON.stringify(heartbeat);

  if (
    Buffer.byteLength(
      serialized,
      "utf8",
    ) > MAX_HEARTBEAT_BYTES
  ) {
    throw new Error(
      "Worker heartbeat exceeds the allowed size",
    );
  }

  return serialized;
}

/**
 * Parse heartbeat data without exposing raw Redis
 * values or detailed validation errors.
 */

function parseHeartbeat(
  raw: string,
): WorkerHeartbeat {
  if (
    Buffer.byteLength(
      raw,
      "utf8",
    ) > MAX_HEARTBEAT_BYTES
  ) {
    throw new Error(
      "Worker heartbeat data exceeds the allowed size",
    );
  }

  let value: unknown;

  try {
    value = JSON.parse(raw);
  } catch {
    throw new Error(
      "Worker heartbeat data is invalid",
    );
  }

  const parsed =
    workerHeartbeatSchema.safeParse(value);

  if (!parsed.success) {
    throw new Error(
      "Worker heartbeat data is invalid",
    );
  }

  return parsed.data;
}

// ==================================================
// Heartbeat publication
// ==================================================

/**
 * Publish a validated worker heartbeat.
 *
 * The heartbeat value and its registry membership
 * are updated atomically.
 *
 * Redis expiration remains the primary mechanism
 * for detecting expired heartbeat records.
 */

export async function publishWorkerHeartbeat(
  connection: Redis,

  options: WorkerRegistryOptions,

  heartbeat: WorkerHeartbeat,
): Promise<void> {
  validateOptions(options);

  const result =
    workerHeartbeatSchema.safeParse(
      heartbeat,
    );

  if (!result.success) {
    throw new Error(
      "Worker heartbeat data is invalid",
    );
  }

  const parsed = result.data;

  const key = workerKey(
    options.prefix,
    parsed.role,
    parsed.workerId,
  );

  const indexKey = workerIndexKey(
    options.prefix,
    parsed.role,
  );

  const serialized =
    serializeHeartbeat(parsed);

  await connection.eval(
    PUBLISH_HEARTBEAT_SCRIPT,

    2,

    key,

    indexKey,

    serialized,

    String(options.ttlSeconds),
  );
}

// ==================================================
// Expired heartbeat cleanup
// ==================================================

/**
 * Remove expired index entries without racing
 * concurrent heartbeat publication.
 *
 * Redis checks heartbeat existence and removes
 * the corresponding index entry atomically.
 */

async function cleanupExpiredHeartbeats(
  connection: Redis,

  indexKey: string,

  expiredKeys: readonly string[],
): Promise<void> {
  if (expiredKeys.length === 0) {
    return;
  }

  const pipeline = connection.pipeline();

  for (const key of expiredKeys) {
    pipeline.eval(
      REMOVE_EXPIRED_HEARTBEAT_SCRIPT,

      2,

      key,

      indexKey,
    );
  }

  const results = await pipeline.exec();

  if (!results) {
    throw new Error(
      "Worker heartbeat cleanup did not complete",
    );
  }

  for (const [error] of results) {
    if (error) {
      throw new Error(
        "Worker heartbeat cleanup failed",
        {
          cause: error,
        },
      );
    }
  }
}

// ==================================================
// Heartbeat discovery
// ==================================================

/**
 * Discover live worker heartbeat records.
 *
 * SSCAN avoids loading the complete Redis index
 * in one command.
 *
 * MGET requests are processed in bounded batches.
 *
 * Discovery is not a transactional snapshot.
 * Workers may publish, expire, or shut down
 * while discovery is in progress.
 *
 * Callers must treat these records as advisory
 * availability information.
 */

export async function listWorkerHeartbeats(
  connection: Redis,

  options: WorkerRegistryOptions,

  role: WorkerHeartbeat["role"],
  { cleanupExpired = true } = {},
): Promise<WorkerHeartbeat[]> {
  validateOptions(options);

  const validatedRole =
    validateRole(role);

  const indexKey = workerIndexKey(
    options.prefix,
    validatedRole,
  );

  const live = new Map<
    string,
    WorkerHeartbeat
  >();

  let cursor = "0";

  do {
    const [nextCursor, keys] =
      await connection.sscan(
        indexKey,

        cursor,

        "COUNT",

        HEARTBEAT_SCAN_BATCH_SIZE,
      );

    cursor = nextCursor;

    if (keys.length === 0) {
      continue;
    }

    /**
     * SSCAN COUNT is a hint rather than a strict
     * maximum. Explicitly split returned keys
     * into bounded MGET batches.
     */

    for (
      let offset = 0;
      offset < keys.length;
      offset += HEARTBEAT_SCAN_BATCH_SIZE
    ) {
      const batch = keys.slice(
        offset,

        offset +
          HEARTBEAT_SCAN_BATCH_SIZE,
      );

      /**
       * Validate index membership before reading
       * any referenced Redis heartbeat keys.
       */

      for (const key of batch) {
        validateIndexedWorkerKey(
          options.prefix,

          validatedRole,

          key,
        );
      }

      const values =
        await connection.mget(...batch);

      if (values.length !== batch.length) {
        throw new Error(
          "Worker heartbeat lookup returned invalid results",
        );
      }

      const expired: string[] = [];

      for (
        let index = 0;
        index < values.length;
        index++
      ) {
        const key = batch[index];

        const raw = values[index];

        if (!key) {
          throw new Error(
            "Worker registry key is missing",
          );
        }

        if (raw === null) {
          expired.push(key);

          live.delete(key);

          continue;
        }

        if (typeof raw !== "string") {
          throw new Error(
            "Worker heartbeat data is invalid",
          );
        }

        const parsed =
          parseHeartbeat(raw);

        if (
          parsed.role !== validatedRole
        ) {
          throw new Error(
            "Worker heartbeat role does not match its registry",
          );
        }

        const expectedKey = workerKey(
          options.prefix,

          parsed.role,

          parsed.workerId,
        );

        if (expectedKey !== key) {
          throw new Error(
            "Worker heartbeat identity does not match its registry key",
          );
        }

        /**
         * SSCAN may return duplicate members.
         * A Map prevents duplicate worker records.
         */

        live.set(
          key,
          parsed,
        );
      }

      if (cleanupExpired) await cleanupExpiredHeartbeats(connection, indexKey, expired);
    }
  } while (cursor !== "0");

  return [...live.values()];
}

// ==================================================
// Heartbeat removal
// ==================================================

/**
 * Remove a worker heartbeat during graceful
 * shutdown.
 *
 * Heartbeat publishers must stop and await any
 * in-flight publication before calling removal.
 *
 * Worker identities must not be shared by
 * concurrently running worker processes.
 *
 * If graceful removal fails, Redis TTL provides
 * eventual heartbeat expiration.
 */

export async function removeWorkerHeartbeat(
  connection: Redis,

  options: WorkerRegistryOptions,

  heartbeat: Pick<
    WorkerHeartbeat,
    "workerId" | "role"
  >,
): Promise<void> {
  validateOptions(options);

  const workerId = validateWorkerId(
    heartbeat.workerId,
  );

  const role = validateRole(
    heartbeat.role,
  );

  const key = workerKey(
    options.prefix,

    role,

    workerId,
  );

  const indexKey = workerIndexKey(
    options.prefix,

    role,
  );

  await connection.eval(
    REMOVE_HEARTBEAT_SCRIPT,

    2,

    key,

    indexKey,
  );
}