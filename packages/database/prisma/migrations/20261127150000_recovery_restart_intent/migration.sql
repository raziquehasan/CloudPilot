BEGIN;
ALTER TYPE "HealthFailureReason" ADD VALUE 'RECOVERY_STALE';
ALTER TYPE "HealthFailureReason" ADD VALUE 'RECOVERY_IDENTITY_MISMATCH';
ALTER TYPE "HealthFailureReason" ADD VALUE 'RECOVERY_PORT_CHANGED';
ALTER TYPE "HealthFailureReason" ADD VALUE 'RECOVERY_DOCKER_UNAVAILABLE';
ALTER TYPE "HealthFailureReason" ADD VALUE 'RECOVERY_TIMEOUT';
ALTER TYPE "HealthFailureReason" ADD VALUE 'RECOVERY_RESTART_UNCERTAIN';

ALTER TABLE "deployment_recovery_attempts"
  ADD COLUMN "healthVersion" INTEGER,
  ADD COLUMN "restartIssuedAt" TIMESTAMP(3),
  ADD COLUMN "restartSourceStartedAt" VARCHAR(40),
  ADD CONSTRAINT "dra_restart_fence_chk" CHECK (
    ("healthVersion" IS NULL OR "healthVersion" >= 0)
    AND (("restartIssuedAt" IS NULL) = ("restartSourceStartedAt" IS NULL))
    AND ("restartIssuedAt" IS NULL OR ("healthVersion" IS NOT NULL AND "attemptCount" > 0))
  );
COMMIT;
