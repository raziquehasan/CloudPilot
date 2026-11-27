
-- ==================================================
-- CloudPilot Phase 5.6
-- Real-Time Deployment Logs & Streaming Engine
-- ==================================================
--
-- Additive, backward-compatible migration.
--
-- Design guarantees:
--   1. Existing database tables and records are preserved.
--   2. Legacy deployment and build logs remain untouched.
--   3. Log records are scoped to their workspace,
--      project and deployment.
--   4. Optional build and runtime references are
--      constrained to the same deployment.
--   5. Persisted log sequences are strictly positive.
--   6. Log messages have bounded UTF-8 byte sizes.
--   7. Notification ranges are valid and unique.
--   8. Migration execution is transactional.
--
-- Operational requirements:
--   - Validate against a disposable PostgreSQL database.
--   - Keep Prisma schema mappings synchronized.
--   - Never modify this migration after it has been
--     applied to a shared or primary database.
--   - Apply to the primary database only after
--     explicit approval.
--
-- Important:
-- Sequence uniqueness does not independently guarantee
-- idempotent ingestion. The writer must preserve stable
-- record identities across ambiguous transaction retries.
-- ==================================================

BEGIN;

-- ==================================================
-- Deployment logging enumerations
-- ==================================================

CREATE TYPE "DeploymentLogSource" AS ENUM (
    'BUILD',
    'RUNTIME',
    'SYSTEM'
);

CREATE TYPE "DeploymentLogStream" AS ENUM (
    'STDOUT',
    'STDERR',
    'SYSTEM'
);

CREATE TYPE "DeploymentLogLevel" AS ENUM (
    'DEBUG',
    'INFO',
    'WARN',
    'ERROR',
    'UNKNOWN'
);

-- ==================================================
-- Unified authoritative log history
-- ==================================================

CREATE TABLE "deployment_log_entries" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),

    "workspaceId" UUID NOT NULL,

    "projectId" UUID NOT NULL,

    "deploymentId" UUID NOT NULL,

    "buildId" UUID,

    "runtimeInstanceId" UUID,

    "source" "DeploymentLogSource" NOT NULL,

    "stream" "DeploymentLogStream" NOT NULL,

    "level" "DeploymentLogLevel" NOT NULL,

    "message" TEXT NOT NULL,

    "sequence" BIGINT NOT NULL,

    "eventTimestamp" TIMESTAMP(3) NOT NULL,

    "persistedAt" TIMESTAMP(3) NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    "createdAt" TIMESTAMP(3) NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "deployment_log_entries_pkey"
        PRIMARY KEY ("id"),

    -- Zero is reserved for the initial cursor.
    CONSTRAINT "dle_sequence_positive_chk"
        CHECK ("sequence" > 0),

    -- Empty log messages are not persisted.
    CONSTRAINT "dle_message_nonempty_chk"
        CHECK (length("message") > 0),

    -- Enforce the shared protocol message limit.
    -- PostgreSQL text length alone is insufficient
    -- because Unicode characters may occupy multiple
    -- UTF-8 bytes.
    CONSTRAINT "dle_message_size_chk"
        CHECK (
            octet_length(
                convert_to("message", 'UTF8')
            ) <= 16384
        )
);

-- ==================================================
-- Log history indexes
-- ==================================================

-- Enforces unique sequences within a deployment.
-- Also supports forward and backward cursor pagination.
--
-- No additional index on the same columns is required.

CREATE UNIQUE INDEX "dle_ws_dep_seq_key"
ON "deployment_log_entries" (
    "workspaceId",
    "deploymentId",
    "sequence"
);

-- Source-filtered historical log queries.

CREATE INDEX "dle_ws_dep_source_seq_idx"
ON "deployment_log_entries" (
    "workspaceId",
    "deploymentId",
    "source",
    "sequence"
);

-- Level-filtered historical log queries.

CREATE INDEX "dle_ws_dep_level_seq_idx"
ON "deployment_log_entries" (
    "workspaceId",
    "deploymentId",
    "level",
    "sequence"
);

-- Stream-filtered historical log queries.

CREATE INDEX "dle_ws_dep_stream_seq_idx"
ON "deployment_log_entries" (
    "workspaceId",
    "deploymentId",
    "stream",
    "sequence"
);

-- Retention and bounded cleanup operations.

CREATE INDEX "dle_created_idx"
ON "deployment_log_entries" (
    "createdAt"
);

-- Support scoped build foreign-key checks.

CREATE INDEX "dle_ws_proj_dep_build_idx"
ON "deployment_log_entries" (
    "workspaceId",
    "projectId",
    "deploymentId",
    "buildId"
);

-- Support scoped runtime foreign-key checks.

CREATE INDEX "dle_ws_proj_dep_runtime_idx"
ON "deployment_log_entries" (
    "workspaceId",
    "projectId",
    "deploymentId",
    "runtimeInstanceId"
);

-- ==================================================
-- Tenant-scoped deployment relationship
-- ==================================================

ALTER TABLE "deployment_log_entries"
ADD CONSTRAINT "dle_deployment_fkey"
FOREIGN KEY (
    "workspaceId",
    "projectId",
    "deploymentId"
)
REFERENCES "deployments" (
    "workspaceId",
    "projectId",
    "id"
)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- ==================================================
-- Optional tenant-scoped build relationship
-- ==================================================
--
-- MATCH SIMPLE permits a null buildId for records that
-- are not associated with a build.
--
-- When buildId is present, the referenced build must
-- belong to the same workspace, project and deployment.
--
-- NO ACTION prevents independent deletion of referenced
-- build metadata while retained log records exist.

ALTER TABLE "deployment_log_entries"
ADD CONSTRAINT "dle_build_fkey"
FOREIGN KEY (
    "workspaceId",
    "projectId",
    "deploymentId",
    "buildId"
)
REFERENCES "deployment_builds" (
    "workspaceId",
    "projectId",
    "deploymentId",
    "id"
)
MATCH SIMPLE
ON DELETE NO ACTION
ON UPDATE CASCADE;

-- ==================================================
-- Optional tenant-scoped runtime relationship
-- ==================================================
--
-- Runtime records may be absent for build and system
-- logs. When present, runtime identity must belong to
-- the same deployment.
--
-- Independent runtime metadata deletion is prevented
-- while retained log records reference that runtime.

ALTER TABLE "deployment_log_entries"
ADD CONSTRAINT "dle_runtime_fkey"
FOREIGN KEY (
    "workspaceId",
    "projectId",
    "deploymentId",
    "runtimeInstanceId"
)
REFERENCES "deployment_runtime_instances" (
    "workspaceId",
    "projectId",
    "deploymentId",
    "id"
)
MATCH SIMPLE
ON DELETE NO ACTION
ON UPDATE CASCADE;

-- ==================================================
-- Per-deployment concurrency-safe sequence allocator
-- ==================================================
--
-- The counter starts at zero.
--
-- The writer must allocate sequences atomically inside
-- the same transaction that persists log entries and
-- their notification outbox records.
--
-- Sequence allocation must never be implemented using
-- an unprotected MAX(sequence) + 1 query.
--
-- Retention must not reset or delete the allocator
-- while the associated deployment continues to exist.

CREATE TABLE "deployment_log_sequences" (
    "workspaceId" UUID NOT NULL,

    "deploymentId" UUID NOT NULL,

    "value" BIGINT NOT NULL DEFAULT 0,

    "updatedAt" TIMESTAMP(3) NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "dls_ws_dep_pk"
        PRIMARY KEY (
            "workspaceId",
            "deploymentId"
        ),

    CONSTRAINT "dls_value_nonnegative_chk"
        CHECK ("value" >= 0)
);

-- The allocator does not store a separate projectId.
-- Deployment identity is already uniquely scoped by
-- workspaceId and deploymentId.

ALTER TABLE "deployment_log_sequences"
ADD CONSTRAINT "dls_deployment_fkey"
FOREIGN KEY (
    "workspaceId",
    "deploymentId"
)
REFERENCES "deployments" (
    "workspaceId",
    "id"
)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- ==================================================
-- Batched committed-cursor notification outbox
-- ==================================================
--
-- Notifications contain committed sequence ranges,
-- never raw log message bodies.
--
-- Outbox records must be inserted atomically with
-- their associated log entries.
--
-- Redis delivery remains at-least-once.
-- Consumers must tolerate duplicate notifications.

CREATE TABLE "deployment_log_notification_outbox" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),

    "workspaceId" UUID NOT NULL,

    "projectId" UUID NOT NULL,

    "deploymentId" UUID NOT NULL,

    "fromSequence" BIGINT NOT NULL,

    "toSequence" BIGINT NOT NULL,

    "status" "DeploymentOutboxStatus" NOT NULL
        DEFAULT 'PENDING',

    "attemptCount" INTEGER NOT NULL
        DEFAULT 0,

    "nextAttemptAt" TIMESTAMP(3) NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    "leaseToken" UUID,

    "leaseExpiresAt" TIMESTAMP(3),

    "publishedAt" TIMESTAMP(3),

    "lastErrorCode" VARCHAR(80),

    "createdAt" TIMESTAMP(3) NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    "updatedAt" TIMESTAMP(3) NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "deployment_log_notification_outbox_pkey"
        PRIMARY KEY ("id"),

    CONSTRAINT "dlno_attempt_count_nonnegative_chk"
        CHECK ("attemptCount" >= 0),

    CONSTRAINT "dlno_from_sequence_positive_chk"
        CHECK ("fromSequence" > 0),

    CONSTRAINT "dlno_sequence_order_chk"
        CHECK ("toSequence" >= "fromSequence")
);

-- ==================================================
-- Notification outbox indexes
-- ==================================================

-- Prevent duplicate notification rows for an identical
-- committed range.
--
-- This does not prevent overlapping ranges or provide
-- exactly-once Redis delivery.

CREATE UNIQUE INDEX "dlno_ws_dep_range_key"
ON "deployment_log_notification_outbox" (
    "workspaceId",
    "deploymentId",
    "fromSequence",
    "toSequence"
);

-- Pending notification discovery and retry scheduling.

CREATE INDEX "dlno_status_next_idx"
ON "deployment_log_notification_outbox" (
    "status",
    "nextAttemptAt",
    "createdAt"
);

-- Expired publication lease recovery.

CREATE INDEX "dlno_status_lease_idx"
ON "deployment_log_notification_outbox" (
    "status",
    "leaseExpiresAt"
);

-- Deployment-scoped notification history.

CREATE INDEX "dlno_ws_dep_created_idx"
ON "deployment_log_notification_outbox" (
    "workspaceId",
    "deploymentId",
    "createdAt"
);

-- Published and dead notification retention.

CREATE INDEX "dlno_status_created_idx"
ON "deployment_log_notification_outbox" (
    "status",
    "createdAt"
);

-- ==================================================
-- Tenant-scoped outbox relationship
-- ==================================================

ALTER TABLE "deployment_log_notification_outbox"
ADD CONSTRAINT "dlno_deployment_fkey"
FOREIGN KEY (
    "workspaceId",
    "projectId",
    "deploymentId"
)
REFERENCES "deployments" (
    "workspaceId",
    "projectId",
    "id"
)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- ==================================================
-- Migration completion
-- ==================================================

COMMIT;
