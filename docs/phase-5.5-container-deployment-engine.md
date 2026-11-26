# CloudPilot — Phase 5.5: Secure Container Deployment & Runtime Engine

## 1. Overview

Phase 5.5 converts a **verified image artifact** produced by the
Phase 5.4 build engine into a running, isolated application
container. It introduces a durable runtime instance model, a
fenced runtime queue, a hardened local Docker runtime client, and
tenant-scoped lifecycle controls.

Container execution is **disabled by default**
(`RUNTIME_ENABLED=false`). The local Docker driver cannot enforce
strong isolation for untrusted, multi-tenant workloads; enable the
runtime only on a trusted, single-tenant host.

## 2. Runtime lifecycle

Runtime state is tracked on a dedicated
`deployment_runtime_instances` record and reuses the existing
deployment state machine (no new deployment states).

```text
QUEUED -> CREATING -> STARTING -> RUNNING
                                  |
                                  v
                               STOPPED
```

Failure and cancellation paths:

```text
CREATING/STARTING -> FAILED
QUEUED            -> CANCELLED
```

Lifecycle actions (`start`, `stop`, `restart`) operate on an
existing managed instance and never rebuild the image.

## 3. Execution flow

```text
Build worker (Phase 5.4)
  -> BUILD_SUCCEEDED
  -> RUNTIME_REQUESTED outbox event (same transaction)
  -> Outbox publisher (API)
  -> runtime queue (BullMQ)
  -> Runtime worker (fenced claim + lease)
  -> verify build artifact + image id ownership
  -> resolve + decrypt environment (worker-side)
  -> allocate loopback host port
  -> ensure per-project managed network
  -> create hardened container (read-only, cap-dropped)
  -> persist container identity
  -> start + inspect + readiness probe
  -> RUNNING
```

The `RUNTIME_REQUESTED` event is committed in the same transaction
as `BUILD_SUCCEEDED`, so there is no database-to-queue gap.

## 4. Data model

`DeploymentRuntimeInstance` (`deployment_runtime_instances`) is
tenant-scoped by `(workspaceId, projectId, deploymentId)` with a
composite foreign key to `deployments`. A unique constraint on
`(workspaceId, projectId, deploymentId, runtimeAttempt)` guarantees
one instance per attempt.

New enum values:

- `DeploymentOutboxEventType.RUNTIME_REQUESTED`
- `DeploymentFailureCode.RUNTIME_FAILED`
- `DeploymentRuntimeStatus` (`QUEUED`, `CREATING`, `STARTING`,
  `RUNNING`, `STOPPED`, `FAILED`, `CANCELLED`)

Migration `20261126120000_deployment_runtime_engine` is additive
only. It adds the table, enum, and enum values without modifying or
dropping existing data.

## 5. Container hardening

Every managed container is created with:

- `--read-only` with a `--tmpfs /tmp`
- `--cap-drop ALL`
- `--security-opt no-new-privileges`
- `--cpus`, `--memory`, `--pids-limit` resource caps
- loopback-only published port (`-p 127.0.0.1::<port>`)
- secrets injected via `--env-file`, never on the command line
- `shell: false` execution (no shell interpolation)

Ownership labels (`cloudpilot.managed`, `cloudpilot.workspaceId`,
`cloudpilot.projectId`, `cloudpilot.deploymentId`,
`cloudpilot.runtimeInstanceId`, `cloudpilot.attempt`) are the
authoritative boundary checked before any lifecycle or cleanup
operation.

## 6. Secret handling

Stored environment variables are encrypted with AES-256-GCM using
associated data `JSON.stringify([workspaceId, projectId,
environment, key])`. The worker decrypts inside the authorized
execution boundary only, using `ENVIRONMENT_VARIABLE_ENCRYPTION_KEY`.
Plaintext never enters queue payloads or logs, and any decryption
failure fails closed.

## 7. Failure classification

Runtime failures use a conservative, fixed code set
(`packages/queue/src/contracts/runtime.ts`). Only
`RUNTIME_UNAVAILABLE` is retryable. Timeouts, readiness failures,
and resource-limit violations are permanent by default. A retryable
classification never by itself authorizes a retry; the orchestrator
also verifies retry budget, execution ownership, cancellation, and
prior termination.

## 8. API surface

`GET /deployments` now includes a `runtime` object (status, ports,
limits, `applicationUrl`, failure detail). Lifecycle endpoints:

```text
POST /:projectId/deployments/:deploymentId/runtime/start
POST /:projectId/deployments/:deploymentId/runtime/stop
POST /:projectId/deployments/:deploymentId/runtime/restart
```

All lifecycle endpoints require authentication, CSRF, and workspace
management permission, and return `503 RUNTIME_DISABLED` when the
runtime is unavailable.

## 9. Configuration

See `.env.example`. Key variables:

- `RUNTIME_ENABLED` (default `false`)
- `RUNTIME_TRUSTED_LOCAL_ONLY` (default `true`)
- `RUNTIME_BIND_HOST` (default `127.0.0.1`)
- `RUNTIME_CONTAINER_PORT`, `RUNTIME_CPU_LIMIT`,
  `RUNTIME_MEMORY_LIMIT_MB`, `RUNTIME_PIDS_LIMIT`
- `RUNTIME_STARTUP_TIMEOUT_MS`,
  `RUNTIME_HEALTH_CHECK_INTERVAL_MS`, `RUNTIME_STOP_TIMEOUT_MS`
- `RUNTIME_MAX_CONCURRENCY`, `RUNTIME_MAX_RETRIES`,
  `RUNTIME_NETWORK_PREFIX`
- `ENVIRONMENT_VARIABLE_ENCRYPTION_KEY`

## 10. Rollout

The migration must be applied before enabling the runtime. Apply the
migration and enable live execution only after explicit approval.
Until then the runtime remains disabled and no containers are
started.