# CloudPilot — Phase 5.4: Secure & Isolated Docker Build Engine

## 1. Overview

Phase 5.4 adds a secure, asynchronous Docker image build
pipeline on top of Phase 5.3 source retrieval. The phase
produces a **verified image artifact** associated with a
specific workspace, project, deployment, and immutable Git
commit.

Phase 5.4 does **not** start or deploy the application
container. Container execution remains disabled.

Build execution is **disabled by default** (`BUILD_ENABLED=false`).

## 2. Build lifecycle

```text
SOURCE_PREPARED
      |
      v
BUILDING
      |
      v
BUILD_SUCCEEDED
```

Failure and cancellation paths:

```text
BUILDING -> FAILED
BUILDING -> CANCELLING -> CANCELLED
```

`BUILD_SUCCEEDED` is a terminal state for Phase 5.4. There is
no transition to `DEPLOYING`, `RUNNING`, or `LIVE`.

The state machine (`apps/api/src/services/deployment-state-machine.ts`)
is authoritative for permitted transitions.

## 3. Execution flow

```text
Deployment worker (Phase 5.3)
  -> SOURCE_PREPARED committed
  -> BUILD_REQUESTED outbox event (same transaction)
  -> Outbox publisher (API)
  -> deployment-build queue (BullMQ)
  -> Build worker (fenced claim)
  -> resolve + validate source workspace
  -> validate build context + Dockerfile + policy
  -> BuildKit executor (docker buildx)
  -> verify image artifact
  -> persist build metadata + logs
  -> BUILD_SUCCEEDED
```

The `SOURCE_PREPARED` transition and build scheduling share one
database transaction, so there is no database-to-queue gap.

## 4. Database schema

Additive migration:

`20261126110000_deployment_build_engine`

Adds:

- `BUILD_SUCCEEDED` deployment status.
- `BUILD_REQUESTED` outbox event type.
- `DeploymentBuildStatus` and `DeploymentBuildArtifactType` enums.
- `deployment_builds` table (tenant-scoped, composite FK,
  unique per `(workspace, project, deployment, buildAttempt)`).
- `deployment_build_logs` table (bounded, ordered build logs).

A database backup was taken before applying the migration
(`/tmp/cloudpilot-pre-5.4.dump`).

## 5. Queue contracts

`packages/queue/src/contracts/build.ts` defines:

- `buildFailureCodes` and `BuildFailureCode`.
- `BuildError` with retryable/permanent classification.
- Sanitized user-facing messages.
- Build artifact metadata types.

`buildJobSchema` carries identifiers only:

```typescript
{
  kind: "build",
  workspaceId, projectId, deploymentId,
  buildId, sourcePreparationId,
  resolvedCommitSha, correlationId
}
```

No credentials, secrets, or filesystem paths are placed in the
payload.

## 6. Build executor interface

`apps/worker/src/build/build-executor.ts` defines a
`BuildExecutor` interface supporting local development, dedicated
remote builders, and future cloud execution:

- `probe()` — detect availability and isolation capabilities.
- `build(request)` — execute a build, return artifact metadata.
- `inspectImage(tag)` — verify the artifact exists.
- `removeImage(tag)` — attempt-scoped cleanup.

`BuildKitExecutor` (`buildkit-client.ts`) implements the
interface with Docker Buildx. External commands are invoked with
**argument arrays** and `shell: false`. User-controlled input is
never interpolated into a shell.

## 7. Build subsystem modules

```text
apps/worker/src/build/
  build-engine.ts        orchestration
  build-context.ts       context validation + size limits
  dockerfile-validator.ts Dockerfile structure validation
  build-policy.ts        entitlement/capability policy
  buildkit-client.ts     BuildKit executor
  build-executor.ts      executor interface
  build-logs.ts          bounded, sanitized log capture
  image-artifact.ts      trusted image tag generation
  build-cleanup.ts       attempt-scoped artifact cleanup
```

## 8. Isolation model

The default Docker `docker` driver does **not** provide strong
multi-tenant isolation (no enforced per-build network or
resource policies). Therefore:

- Untrusted repository-controlled builds remain **disabled**.
- Local builds are allowed only for explicitly authorized
  trusted repositories (`BUILD_TRUSTED_LOCAL_ONLY=true` plus
  `BUILD_TRUSTED_REPOSITORIES`) behind `BUILD_ENABLED=true`.
- The executor never receives the host Docker socket, host
  credentials, SSH agents, or cloud credentials.
- CloudPilot never runs global Docker cleanup; cleanup is scoped
  to a single validated image tag.

The builder reports `supportsStrongIsolation=false` for the
local driver. A production multi-tenant deployment requires a
dedicated, isolated builder (e.g. a hardened `docker-container`
or remote builder with enforced network/resource policies),
which is **not** established in this phase.

## 9. Resource limits

Configurable via environment variables. The selected builder
determines which limits are actually enforced. The local `docker`
driver does **not** enforce per-build CPU/memory/network limits;
those limits are validated and passed where supported but are
**not** claimed as enforced on the local driver. Build timeout
and context-size limits are enforced by the worker.

## 10. Build context and Dockerfile validation

- Context and Dockerfile must resolve inside the immutable
  Phase 5.3 source workspace (traversal, absolute paths, and
  symlink escapes rejected).
- Context size and file count are bounded.
- Special files and symlinks in the context are rejected.
- Dockerfile must exist and contain a `FROM` instruction.
- Build policy rejects privileged/insecure entitlements, host
  networking, SSH/secret/host-bind mounts, remote `ADD`
  sources, and custom Dockerfile frontends.

Dockerfile validation is defense in depth, not a sandbox.

## 11. Image artifact

- Tag format: `cloudpilot/project-<projectId>:<sha>-<attempt>`.
- Generated from trusted identifiers only.
- The artifact is verified to exist before success publication.
- Images are loaded into the local builder image store; nothing
  is pushed to a public registry.
- The manifest digest is recorded only when available; the
  image configuration ID is not treated as a registry digest.

## 12. Build logs

- Bounded by `BUILD_MAX_LOG_BYTES`.
- ANSI/control characters stripped; common credential patterns
  redacted (best effort).
- Persisted to `deployment_build_logs` with sequence numbers.
- Exposed via a tenant-scoped endpoint:
  `GET /api/v1/projects/:projectId/deployments/:deploymentId/build-logs`.

Real-time WebSocket streaming remains Phase 5.6.

## 13. Cancellation, retry, recovery

- Cancellation is cooperative and fenced by the build lease.
- The executor is signalled to terminate the build process.
- Retryable failures (builder unavailable, timeout, resource
  limit) are retried within budget; permanent failures are not.
- Restart recovery and reconciliation handle abandoned attempts.
- Stale workers cannot overwrite a newer build result (fencing
  tokens + build attempt).

## 14. API changes

- Deployment responses include a `build` object (latest
  attempt) with status, image metadata, and sanitized failure.
- `imageSizeBytes` is serialized as a string (BigInt safety).
- Readiness distinguishes source retrieval, build capability,
  and container deployment capability.
- New build-logs endpoint (above).

## 15. Frontend changes

The deployments dashboard shows genuine build status, commit
SHA, image size/tag, build failure reason, and duration. It
shows: “Image built successfully. Application deployment will be
available in Phase 5.5.” No fabricated progress percentages.

## 16. Configuration

See `.env.example`. Key variables:

```env
BUILD_ENABLED=false
BUILD_EXECUTOR=buildkit
BUILD_TRUSTED_LOCAL_ONLY=true
BUILD_TRUSTED_REPOSITORIES=
BUILD_PLATFORM=linux/amd64
BUILD_MAX_CONCURRENCY=1
BUILD_TIMEOUT_MS=900000
BUILD_CPU_LIMIT=2
BUILD_MEMORY_LIMIT_MB=2048
BUILD_MAX_CONTEXT_BYTES=524288000
BUILD_MAX_LOG_BYTES=10485760
BUILD_MAX_RETRIES=1
BUILD_ARTIFACT_TTL_SECONDS=86400
```

The build queue uses a dedicated execution timeout derived from
`BUILD_TIMEOUT_MS` so long builds are not cut off by the shared
queue timeout.

## 17. Testing

- Worker unit tests (`test/build-engine.test.ts`) with a mock
  executor: context validation, Dockerfile validation, policy,
  image tag, log sanitization, success, builder-unavailable,
  policy violation, missing artifact, cancellation.
- API state-machine tests extended for `BUILD_SUCCEEDED`.
- Real BuildKit execution verified with a controlled fixture
  (image built and inspected). Full worker-orchestrated live
  builds require enabling builds and a READY source workspace.

## 18. Known limitations

- Untrusted multi-tenant builds are disabled (no strong
  isolation on the local driver).
- Resource/network limits are not enforced by the local driver.
- Cancellation is cooperative; killing the local CLI is not
  proof a remote build stopped.
- Prisma/Effect advisory (`GHSA-7589-6fvf-x4pw`) remains.
- Container deployment is not implemented (Phase 5.5).

## 19. Phase 5.5 integration contract

Phase 5.5 consumes the verified image artifact via:

- `deployment_builds.imageTag` / `imageId` / `imageDigest`.
- `artifactType = DOCKER_IMAGE_STORE`.
- Immutable `resolvedCommitSha` association.

The image is present in the builder image store and has not
been started. Phase 5.5 must add runtime isolation, network
policy, and secret injection before executing it.