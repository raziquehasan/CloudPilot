BEGIN;

CREATE TYPE "DeploymentHealthStatus" AS ENUM ('UNKNOWN', 'STARTING', 'HEALTHY', 'DEGRADED', 'UNHEALTHY', 'RECOVERING', 'FAILED');
CREATE TYPE "HealthProbeStatus" AS ENUM ('PASSED', 'FAILED', 'SKIPPED');
CREATE TYPE "HealthFailureReason" AS ENUM (
  'CONTAINER_UNAVAILABLE', 'CONTAINER_NOT_RUNNING', 'HTTP_TIMEOUT', 'HTTP_UNREACHABLE',
  'HTTP_STATUS', 'RECOVERY_EXECUTION_FAILED', 'RECOVERY_EXHAUSTED'
);
CREATE TYPE "RecoveryAction" AS ENUM ('RESTART');
CREATE TYPE "RecoveryTrigger" AS ENUM ('MANUAL', 'AUTOMATIC');
CREATE TYPE "RecoveryStatus" AS ENUM ('REQUESTED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED');

CREATE TABLE "deployment_health_states" (
  "workspaceId" UUID NOT NULL,
  "projectId" UUID NOT NULL,
  "deploymentId" UUID NOT NULL,
  "runtimeInstanceId" UUID NOT NULL,
  "generation" INTEGER NOT NULL,
  "status" "DeploymentHealthStatus" NOT NULL DEFAULT 'UNKNOWN',
  "consecutiveSuccesses" INTEGER NOT NULL DEFAULT 0,
  "consecutiveFailures" INTEGER NOT NULL DEFAULT 0,
  "lastCheckedAt" TIMESTAMP(3),
  "lastHealthyAt" TIMESTAMP(3),
  "nextCheckAt" TIMESTAMP(3),
  "lastFailureReason" "HealthFailureReason",
  "leaseOwnerId" UUID,
  "leaseToken" UUID,
  "leaseExpiresAt" TIMESTAMP(3),
  "version" INTEGER NOT NULL DEFAULT 0,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "dhs_pkey" PRIMARY KEY ("workspaceId", "projectId", "deploymentId", "runtimeInstanceId", "generation"),
  CONSTRAINT "dhs_runtime_fkey" FOREIGN KEY ("workspaceId", "projectId", "deploymentId", "runtimeInstanceId")
    REFERENCES "deployment_runtime_instances" ("workspaceId", "projectId", "deploymentId", "id") ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT "dhs_counters_chk" CHECK (
    "generation" >= 0 AND "version" >= 0 AND "consecutiveSuccesses" >= 0 AND "consecutiveFailures" >= 0
    AND ("consecutiveSuccesses" = 0 OR "consecutiveFailures" = 0)
  ),
  CONSTRAINT "dhs_healthy_time_chk" CHECK (
    "lastHealthyAt" IS NULL OR ("lastCheckedAt" IS NOT NULL AND "lastHealthyAt" <= "lastCheckedAt")
  ),
  CONSTRAINT "dhs_lease_chk" CHECK (
    ("leaseOwnerId" IS NULL AND "leaseToken" IS NULL AND "leaseExpiresAt" IS NULL)
    OR ("leaseOwnerId" IS NOT NULL AND "leaseToken" IS NOT NULL AND "leaseExpiresAt" IS NOT NULL)
  )
);

CREATE UNIQUE INDEX "dhs_lease_token_key" ON "deployment_health_states" ("leaseToken");
CREATE INDEX "dhs_due_idx" ON "deployment_health_states" ("nextCheckAt");

CREATE TABLE "deployment_health_checks" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "workspaceId" UUID NOT NULL,
  "projectId" UUID NOT NULL,
  "deploymentId" UUID NOT NULL,
  "runtimeInstanceId" UUID NOT NULL,
  "generation" INTEGER NOT NULL,
  "dockerResult" "HealthProbeStatus" NOT NULL,
  "httpResult" "HealthProbeStatus" NOT NULL,
  "responseLatencyMs" INTEGER,
  "httpStatus" INTEGER,
  "failureReason" "HealthFailureReason",
  "checkedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "deployment_health_checks_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "dhc_state_fkey" FOREIGN KEY ("workspaceId", "projectId", "deploymentId", "runtimeInstanceId", "generation")
    REFERENCES "deployment_health_states" ("workspaceId", "projectId", "deploymentId", "runtimeInstanceId", "generation")
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT "dhc_measurements_chk" CHECK (
    ("responseLatencyMs" IS NULL OR "responseLatencyMs" BETWEEN 0 AND 60000)
    AND ("httpStatus" IS NULL OR "httpStatus" BETWEEN 100 AND 599)
    AND ("httpResult" <> 'SKIPPED' OR ("responseLatencyMs" IS NULL AND "httpStatus" IS NULL))
  ),
  CONSTRAINT "dhc_failure_reason_chk" CHECK (
    ("dockerResult" = 'FAILED' OR "httpResult" = 'FAILED') = ("failureReason" IS NOT NULL)
  )
);

CREATE INDEX "dhc_state_time_idx" ON "deployment_health_checks"
  ("workspaceId", "projectId", "deploymentId", "runtimeInstanceId", "generation", "checkedAt");
CREATE INDEX "dhc_retention_idx" ON "deployment_health_checks" ("checkedAt");

CREATE TABLE "deployment_recovery_attempts" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "workspaceId" UUID NOT NULL,
  "projectId" UUID NOT NULL,
  "deploymentId" UUID NOT NULL,
  "runtimeInstanceId" UUID NOT NULL,
  "generation" INTEGER NOT NULL,
  "action" "RecoveryAction" NOT NULL,
  "trigger" "RecoveryTrigger" NOT NULL,
  "idempotencyKey" UUID NOT NULL,
  "status" "RecoveryStatus" NOT NULL DEFAULT 'REQUESTED',
  "attemptCount" INTEGER NOT NULL DEFAULT 0,
  "leaseOwnerId" UUID,
  "leaseToken" UUID,
  "leaseExpiresAt" TIMESTAMP(3),
  "startedAt" TIMESTAMP(3),
  "finishedAt" TIMESTAMP(3),
  "failureReason" "HealthFailureReason",
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "deployment_recovery_attempts_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "dra_state_fkey" FOREIGN KEY ("workspaceId", "projectId", "deploymentId", "runtimeInstanceId", "generation")
    REFERENCES "deployment_health_states" ("workspaceId", "projectId", "deploymentId", "runtimeInstanceId", "generation")
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT "dra_lease_chk" CHECK (
    ("status" = 'RUNNING' AND "leaseOwnerId" IS NOT NULL AND "leaseToken" IS NOT NULL AND "leaseExpiresAt" IS NOT NULL)
    OR ("status" <> 'RUNNING' AND "leaseOwnerId" IS NULL AND "leaseToken" IS NULL AND "leaseExpiresAt" IS NULL)
  ),
  CONSTRAINT "dra_attempt_chk" CHECK (
    "attemptCount" >= 0 AND ("status" NOT IN ('RUNNING', 'SUCCEEDED', 'FAILED') OR "attemptCount" > 0)
  ),
  CONSTRAINT "dra_lifecycle_chk" CHECK (
    ("status" = 'REQUESTED' AND "startedAt" IS NULL AND "finishedAt" IS NULL)
    OR ("status" = 'RUNNING' AND "startedAt" IS NOT NULL AND "finishedAt" IS NULL)
    OR ("status" IN ('SUCCEEDED', 'FAILED') AND "startedAt" IS NOT NULL AND "finishedAt" >= "startedAt" AND "finishedAt" IS NOT NULL)
    OR ("status" = 'CANCELLED' AND "finishedAt" IS NOT NULL AND ("startedAt" IS NULL OR "finishedAt" >= "startedAt"))
  )
);

CREATE UNIQUE INDEX "dra_lease_token_key" ON "deployment_recovery_attempts" ("leaseToken");
CREATE UNIQUE INDEX "dra_deployment_idempotency_key" ON "deployment_recovery_attempts"
  ("workspaceId", "projectId", "deploymentId", "idempotencyKey");
CREATE UNIQUE INDEX "dra_active_deployment_key" ON "deployment_recovery_attempts" ("workspaceId", "projectId", "deploymentId")
  WHERE "status" IN ('REQUESTED', 'RUNNING');
CREATE INDEX "dra_state_time_idx" ON "deployment_recovery_attempts"
  ("workspaceId", "projectId", "deploymentId", "runtimeInstanceId", "generation", "createdAt");

COMMIT;
