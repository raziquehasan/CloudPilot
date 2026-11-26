-- Phase 5.3: Secure source retrieval
-- Additive migration: new deployment source preparation metadata and source-prepared lifecycle state.

-- 1. Extend the deployment lifecycle with a source-prepared stage.
ALTER TYPE "DeploymentStatus" ADD VALUE IF NOT EXISTS 'SOURCE_PREPARED';

-- 2. New source preparation status enum.
DO $$ BEGIN
  CREATE TYPE "DeploymentSourceStatus" AS ENUM ('PENDING', 'DOWNLOADING', 'EXTRACTING', 'READY', 'FAILED', 'CANCELLED');
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

-- 3. Source preparation metadata table.
CREATE TABLE IF NOT EXISTS "deployment_source_preparations" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "workspaceId" UUID NOT NULL,
  "projectId" UUID NOT NULL,
  "deploymentId" UUID NOT NULL,
  "repositoryFullName" VARCHAR(255) NOT NULL,
  "requestedBranch" VARCHAR(255),
  "resolvedCommitSha" VARCHAR(40) NOT NULL,
  "status" "DeploymentSourceStatus" NOT NULL DEFAULT 'PENDING',
  "attempt" INTEGER NOT NULL DEFAULT 0,
  "sourceWorkspaceKey" VARCHAR(64),
  "downloadedBytes" BIGINT NOT NULL DEFAULT 0,
  "extractedBytes" BIGINT NOT NULL DEFAULT 0,
  "fileCount" INTEGER NOT NULL DEFAULT 0,
  "failureCode" VARCHAR(80),
  "expiresAt" TIMESTAMP(3),
  "startedAt" TIMESTAMP(3),
  "completedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,

  CONSTRAINT "deployment_source_preparations_pkey" PRIMARY KEY ("id")
);

-- 4. Composite tenant foreign key to the deployment (cascade on delete).
DO $$ BEGIN
  ALTER TABLE "deployment_source_preparations"
    ADD CONSTRAINT "deployment_source_preparations_workspaceId_projectId_deploymentId_fkey"
    FOREIGN KEY ("workspaceId", "projectId", "deploymentId")
    REFERENCES "deployments"("workspaceId", "projectId", "id")
    ON DELETE CASCADE ON UPDATE CASCADE;
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

-- 5. Indexes and uniqueness.
CREATE UNIQUE INDEX IF NOT EXISTS "deployment_source_preparations_workspaceId_projectId_deploymentId_key"
  ON "deployment_source_preparations"("workspaceId", "projectId", "deploymentId");

CREATE INDEX IF NOT EXISTS "deployment_source_preparations_status_expiresAt_idx"
  ON "deployment_source_preparations"("status", "expiresAt");

CREATE INDEX IF NOT EXISTS "deployment_source_preparations_workspaceId_projectId_createdAt_idx"
  ON "deployment_source_preparations"("workspaceId", "projectId", "createdAt");