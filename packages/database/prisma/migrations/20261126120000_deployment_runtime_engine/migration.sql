BEGIN;

ALTER TYPE "DeploymentFailureCode"
ADD VALUE IF NOT EXISTS 'RUNTIME_FAILED' BEFORE 'TIMEOUT';

ALTER TYPE "DeploymentOutboxEventType"
ADD VALUE IF NOT EXISTS 'RUNTIME_REQUESTED';

CREATE TYPE "DeploymentRuntimeStatus" AS ENUM (
    'QUEUED',
    'CREATING',
    'STARTING',
    'RUNNING',
    'STOPPED',
    'FAILED',
    'CANCELLED'
);

CREATE TABLE "deployment_runtime_instances" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "workspaceId" UUID NOT NULL,
    "projectId" UUID NOT NULL,
    "deploymentId" UUID NOT NULL,
    "buildId" UUID NOT NULL,

    "runtimeAttempt" INTEGER NOT NULL DEFAULT 1,
    "status" "DeploymentRuntimeStatus" NOT NULL DEFAULT 'QUEUED',

    "containerId" VARCHAR(255),
    "containerName" VARCHAR(255),

    "imageId" VARCHAR(255),
    "imageTag" VARCHAR(255),

    "containerPort" INTEGER,
    "hostPort" INTEGER,
    "hostBindingAddress" VARCHAR(64),
    "networkName" VARCHAR(255),

    "cpuLimit" INTEGER,
    "memoryLimitMb" INTEGER,
    "pidsLimit" INTEGER,

    "restartCount" INTEGER NOT NULL DEFAULT 0,

    "failureCode" VARCHAR(80),
    "failureMessage" TEXT,

    "executionLeaseToken" UUID,
    "executionLeaseExpiresAt" TIMESTAMP(3),

    "startedAt" TIMESTAMP(3),
    "readyAt" TIMESTAMP(3),
    "stoppedAt" TIMESTAMP(3),

    "exitCode" INTEGER,

    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "deployment_runtime_instances_pkey"
        PRIMARY KEY ("id"),

    CONSTRAINT "dri_runtime_attempt_positive_chk"
        CHECK ("runtimeAttempt" >= 1),

    CONSTRAINT "dri_restart_count_nonnegative_chk"
        CHECK ("restartCount" >= 0),

    CONSTRAINT "dri_container_port_range_chk"
        CHECK (
            "containerPort" IS NULL OR
            "containerPort" BETWEEN 1 AND 65535
        ),

    CONSTRAINT "dri_host_port_range_chk"
        CHECK (
            "hostPort" IS NULL OR
            "hostPort" BETWEEN 1 AND 65535
        ),

    CONSTRAINT "dri_cpu_limit_positive_chk"
        CHECK ("cpuLimit" IS NULL OR "cpuLimit" > 0),

    CONSTRAINT "dri_memory_limit_positive_chk"
        CHECK ("memoryLimitMb" IS NULL OR "memoryLimitMb" > 0),

    CONSTRAINT "dri_pids_limit_positive_chk"
        CHECK ("pidsLimit" IS NULL OR "pidsLimit" > 0)
);

CREATE UNIQUE INDEX "dri_ws_proj_dep_attempt_key"
ON "deployment_runtime_instances" (
    "workspaceId",
    "projectId",
    "deploymentId",
    "runtimeAttempt"
);

CREATE UNIQUE INDEX "dri_ws_proj_dep_id_key"
ON "deployment_runtime_instances" (
    "workspaceId",
    "projectId",
    "deploymentId",
    "id"
);

CREATE INDEX "dri_ws_dep_created_idx"
ON "deployment_runtime_instances" (
    "workspaceId",
    "deploymentId",
    "createdAt"
);

CREATE INDEX "dri_status_created_idx"
ON "deployment_runtime_instances" (
    "status",
    "createdAt"
);

CREATE INDEX "dri_status_lease_expires_idx"
ON "deployment_runtime_instances" (
    "status",
    "executionLeaseExpiresAt"
);

CREATE INDEX "dri_ws_proj_dep_build_idx"
ON "deployment_runtime_instances" (
    "workspaceId",
    "projectId",
    "deploymentId",
    "buildId"
);

ALTER TABLE "deployment_runtime_instances"
ADD CONSTRAINT "dri_deployment_fkey"
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

ALTER TABLE "deployment_runtime_instances"
ADD CONSTRAINT "dri_build_fkey"
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
ON DELETE NO ACTION
ON UPDATE CASCADE;

COMMIT;
