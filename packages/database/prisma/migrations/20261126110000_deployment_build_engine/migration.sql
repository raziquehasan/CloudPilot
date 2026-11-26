-- Phase 5.4: Secure & Isolated Docker Build Engine
-- Additive migration: build lifecycle state, build metadata,
-- and bounded build log records.
--
-- This migration is additive and safe to re-run. It does not
-- rewrite or drop any existing object.

-- 1. Extend the deployment lifecycle with a build-succeeded stage.
ALTER TYPE "DeploymentStatus" ADD VALUE IF NOT EXISTS 'BUILD_SUCCEEDED';

-- 2. Extend the outbox event types with a build request.
ALTER TYPE "DeploymentOutboxEventType" ADD VALUE IF NOT EXISTS 'BUILD_REQUESTED';

-- 3. New build status and artifact type enums.
DO $$ BEGIN
  CREATE TYPE "DeploymentBuildStatus" AS ENUM ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED');
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE "DeploymentBuildArtifactType" AS ENUM ('DOCKER_IMAGE_STORE', 'OCI_ARCHIVE');
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

-- 4. Dedicated deployment build metadata table.
CREATE TABLE IF NOT EXISTS "deployment_builds" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "workspaceId" UUID NOT NULL,
  "projectId" UUID NOT NULL,
  "deploymentId" UUID NOT NULL,
  "sourcePreparationId" UUID NOT NULL,
  "resolvedCommitSha" VARCHAR(40) NOT NULL,
  "buildAttempt" INTEGER NOT NULL DEFAULT 1,
  "status" "DeploymentBuildStatus" NOT NULL DEFAULT 'QUEUED',
  "builderType" VARCHAR(40),
  "dockerfilePath" VARCHAR(500),
  "buildContextPath" VARCHAR(500),
  "imageTag" VARCHAR(255),
  "imageId" VARCHAR(255),
  "imageDigest" VARCHAR(255),
  "artifactType" "DeploymentBuildArtifactType",
  "artifactLocation" VARCHAR(500),
  "platform" VARCHAR(40),
  "imageSizeBytes" BIGINT,
  "failureCode" VARCHAR(80),
  "failureMessage" TEXT,
  "executionLeaseToken" UUID,
  "executionLeaseExpiresAt" TIMESTAMP(3),
  "startedAt" TIMESTAMP(3),
  "completedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,

  CONSTRAINT "deployment_builds_pkey" PRIMARY KEY ("id")
);

-- 5. Composite tenant foreign key to the deployment (cascade on delete).
DO $$ BEGIN
  ALTER TABLE "deployment_builds"
    ADD CONSTRAINT "deployment_builds_workspaceId_projectId_deploymentId_fkey"
    FOREIGN KEY ("workspaceId", "projectId", "deploymentId")
    REFERENCES "deployments"("workspaceId", "projectId", "id")
    ON DELETE CASCADE ON UPDATE CASCADE;
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

-- 6. Prevent duplicate build attempts per deployment.
CREATE UNIQUE INDEX IF NOT EXISTS "deployment_builds_workspaceId_projectId_deploymentId_buildAttempt_key"
  ON "deployment_builds"("workspaceId", "projectId", "deploymentId", "buildAttempt");

CREATE INDEX IF NOT EXISTS "deployment_builds_workspaceId_deploymentId_createdAt_idx"
  ON "deployment_builds"("workspaceId", "deploymentId", "createdAt");

CREATE INDEX IF NOT EXISTS "deployment_builds_status_createdAt_idx"
  ON "deployment_builds"("status", "createdAt");

-- 6b. Unique key matching the composite build-log foreign key target.
CREATE UNIQUE INDEX IF NOT EXISTS "deployment_builds_workspaceId_projectId_deploymentId_id_key"
  ON "deployment_builds"("workspaceId", "projectId", "deploymentId", "id");

-- 7. Bounded build log records.
CREATE TABLE IF NOT EXISTS "deployment_build_logs" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "workspaceId" UUID NOT NULL,
  "projectId" UUID NOT NULL,
  "deploymentId" UUID NOT NULL,
  "buildId" UUID NOT NULL,
  "sequence" BIGINT NOT NULL,
  "stream" VARCHAR(16) NOT NULL,
  "message" TEXT NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "deployment_build_logs_pkey" PRIMARY KEY ("id")
);

-- 8. Composite tenant foreign key to the build (cascade on delete).
DO $$ BEGIN
  ALTER TABLE "deployment_build_logs"
    ADD CONSTRAINT "deployment_build_logs_workspaceId_projectId_deploymentId_buildId_fkey"
    FOREIGN KEY ("workspaceId", "projectId", "deploymentId", "buildId")
    REFERENCES "deployment_builds"("workspaceId", "projectId", "deploymentId", "id")
    ON DELETE CASCADE ON UPDATE CASCADE;
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS "deployment_build_logs_workspaceId_projectId_deploymentId_buildId_sequence_key"
  ON "deployment_build_logs"("workspaceId", "projectId", "deploymentId", "buildId", "sequence");

CREATE INDEX IF NOT EXISTS "deployment_build_logs_workspaceId_projectId_deploymentId_buildId_createdAt_idx"
  ON "deployment_build_logs"("workspaceId", "projectId", "deploymentId", "buildId", "createdAt");