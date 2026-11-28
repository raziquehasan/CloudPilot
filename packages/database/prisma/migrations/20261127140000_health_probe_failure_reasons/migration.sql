BEGIN;
ALTER TYPE "HealthFailureReason" ADD VALUE 'CONTAINER_IDENTITY_MISMATCH';
ALTER TYPE "HealthFailureReason" ADD VALUE 'HTTP_CONNECTION_REFUSED';
ALTER TYPE "HealthFailureReason" ADD VALUE 'HTTP_INVALID_RESPONSE';
ALTER TYPE "HealthFailureReason" ADD VALUE 'HTTP_REDIRECT';
CREATE TYPE "HealthDockerState" AS ENUM ('created', 'running', 'exited', 'restarting', 'paused', 'dead', 'unknown');
ALTER TABLE "deployment_health_checks" ADD COLUMN "dockerState" "HealthDockerState";
COMMIT;
