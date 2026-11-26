
# CloudPilot — Phase 5 Architecture and Security Notes

## 1. Overview

Phase 5 establishes CloudPilot's deployment orchestration
architecture, including deployment lifecycle management,
durable queue processing, secure GitHub source retrieval,
isolated builds, container execution, and operational recovery.

The architecture follows a security-first, milestone-based
implementation strategy.

Each milestone must pass its own functional, reliability,
and security acceptance gates before subsequent execution
capabilities are enabled.

## 2. Historical audit baseline

The Phase 4 baseline is commit:

`5a4d095341bf9b5690100cf85282b7904e397318`

At the initial Phase 5 audit:

- The Git worktree was clean.
- The API responded on port 8000.
- The web application responded on port 3000.
- PostgreSQL and Redis readiness checks passed.
- Existing Prisma migrations were applied.
- Only the diagnostic queue processor was registered.
- No source retrieval or isolated execution engine existed.

These observations describe the historical baseline,
not the current Phase 5.3 implementation.

The Phase 5.2 checkpoint is:

`a872eb598f73cdf8a8924ff32e71205e2f019036`

## 3. Current implementation status

| Milestone | Status |
|---|---|
| Phase 5.1 — Deployment API | Implemented |
| Phase 5.2 — Queue and worker | Implemented |
| Phase 5.3 — Source retrieval | Implemented; final review pending |
| Phase 5.4 — Isolated build | Implemented; disabled by default |
| Phase 5.5 — Container runtime | Not enabled |
| Phase 5.6 — Real-time logs | Planned |
| Phase 5.7 — Health and rollback | Planned |
| Phase 5.8 — GitHub webhooks | Planned |
| Phase 5.9 — End-to-end verification | Planned |

Implementation status does not imply production readiness.

Phase 5.3 permits authorized source preparation when the
required capabilities are available.

Repository-controlled code execution remains disabled.

## 4. High-level architecture

```text
Browser / CloudPilot Dashboard
              |
              v
      Authenticated API
              |
              v
   Workspace Authorization
              |
              v
    GitHub Commit Resolution
              |
              v
         PostgreSQL
              |
     Transactional Outbox
              |
              v
        Redis / BullMQ
              |
              v
      Deployment Worker
              |
       Execution Lease
              |
              v
       GitHub App Auth
              |
              v
     SHA-pinned Download
              |
              v
     Secure TAR Extraction
              |
              v
      Isolated Workspace
              |
              v
       SOURCE_PREPARED
              |
              v
      Future Build Engine
              |
              v
     Future Runtime Engine
```

PostgreSQL is the authoritative state store.

Redis provides asynchronous job delivery and coordination.
Redis job state must never replace authoritative deployment
state in PostgreSQL.

## 5. Architectural principles

- Fail closed when authorization or readiness is uncertain.
- Treat repository source and configuration as untrusted.
- Enforce workspace and project isolation.
- Resolve immutable source identity before queue publication.
- Keep credentials outside job payloads and deployment snapshots.
- Use conditional state transitions and fencing leases.
- Bound retries, timeouts, and resource consumption.
- Preserve auditability of deployment lifecycle changes.
- Separate source preparation from code execution.
- Never enable execution before isolation gates pass.

## 6. Trust boundaries

### Browser to API

Authentication uses server-side sessions.

Session bearer tokens are stored as hashes.

Mutating requests require CSRF validation.

Deployment mutations require OWNER or ADMIN authorization.

Read operations require authenticated workspace membership.

API responses must not expose credentials, environment
variable values, ciphertext, or internal filesystem paths.

### API to PostgreSQL

PostgreSQL stores authoritative:

- Deployment lifecycle state.
- Immutable source identity.
- Build configuration snapshots.
- Idempotency records.
- Transactional outbox events.
- Execution lease information.
- Source preparation metadata.
- Audit events.

Lifecycle updates must use conditional database operations.

Related state changes and audit events should be committed
within the same transaction where required.

### API to GitHub

GitHub authorization is performed server-side using the
existing GitHub App installation infrastructure.

Repository access must be scoped to the authorized workspace
and project.

Branch references must resolve to an immutable commit SHA.

An explicitly requested SHA must be validated against the
authorized repository before it becomes deployment source.

### API to Redis and worker

Queue payloads must contain identifiers and non-sensitive
metadata only.

Deployment creation must fail closed when no compatible
deployment worker is available.

The transactional outbox coordinates durable database
acceptance with asynchronous Redis publication.

Duplicate delivery must not produce duplicate deployment
execution.

### Worker to GitHub

The worker revalidates repository and installation access.

Installation tokens are short-lived and must not be persisted
in database records, queue payloads, or logs.

Repository archives are untrusted input.

### Worker to filesystem

Every source preparation attempt must use an isolated
server-managed workspace.

Archive extraction must enforce filesystem containment,
entry validation, and resource limits.

Cleanup must not delete unrelated or actively owned workspaces.

### Worker to build and runtime engines

Phase 5.4 introduces an isolated Docker build engine that
produces a verified image artifact. It is disabled by default
and only builds explicitly trusted repositories locally.

Detailed implementation documentation:

`docs/phase-5.4-docker-build-engine.md`

The runtime boundary remains disabled in Phase 5.4.

Future repository-controlled execution must not receive:

- Host Docker socket access.
- Host credentials.
- Unrestricted host filesystem mounts.
- Unrestricted internal service access.
- Unrelated workspace data.

## 7. Database architecture

Existing workspace-scoped models include:

- Deployment.
- Build.
- DeploymentLog.
- Project.
- Repository.
- ProjectBuildConfiguration.
- EnvironmentVariable.
- AuditLog.

Phase 5.2 introduces durable deployment outbox records
and execution lease metadata.

Phase 5.3 introduces source preparation records.

The migration:

`20261126090000_deployment_source_retrieval`

adds:

- SOURCE_PREPARED deployment status.
- DeploymentSourceStatus enum.
- deployment_source_preparations table.

Composite tenant relationships must prevent records from
referencing deployments in unrelated workspaces or projects.

Database migrations must remain additive where possible.

Already-applied migrations must not be rewritten.

Back up existing data before applying schema changes.

## 8. Deployment lifecycle

The current successful Phase 5.3 path is:

```text
QUEUED
   |
   v
PREPARING
   |
   v
SOURCE_PREPARED
```

The planned execution lifecycle extends this path:

```text
SOURCE_PREPARED
   |
   v
BUILDING
   |
   v
DEPLOYING
   |
   v
HEALTH_CHECKING
   |
   v
RUNNING
```

The planned execution stages are not enabled in Phase 5.3.

Existing compatibility states, including PENDING, CLONING,
SUCCEEDED, and ROLLED_BACK, must be handled according to
the actual deployment state machine.

Do not infer allowed transitions solely from this diagram.

The state-machine implementation is authoritative for
permitted transitions.

### Lifecycle invariants

- Terminal deployments must not be overwritten by stale workers.
- State changes require the expected prior state.
- Worker-owned changes require a valid execution lease.
- Cancellation must not be overwritten by successful preparation.
- Source preparation success does not imply deployment success.
- RUNNING requires successful runtime health verification.
- Build configuration snapshots remain immutable after creation.

## 9. Deployment API

### List deployments

`GET /api/v1/projects/:projectId/deployments`

Returns workspace-scoped deployment records with pagination
and optional lifecycle status filtering.

### Deployment details

`GET /api/v1/projects/:projectId/deployments/:deploymentId`

Returns deployment metadata only when the record belongs to
the authenticated workspace and requested project.

### Create deployment

`POST /api/v1/projects/:projectId/deployments`

Requires:

- Authenticated OWNER or ADMIN session.
- Valid CSRF protection.
- Authorized project.
- Valid deployment configuration.
- Idempotency-Key.
- Connected repository.
- Valid source reference.
- Immutable commit resolution.
- Compatible worker readiness.

Matching idempotent requests should replay the existing
deployment without creating duplicate work.

Conflicting reuse of an idempotency key must be rejected.

The deployment and its outbox event must be persisted
atomically.

Phase 5.3 acceptance enables source preparation only.

### Cancel deployment

`POST /api/v1/projects/:projectId/deployments/:deploymentId/cancel`

Requires OWNER or ADMIN authorization and CSRF validation.

Queued work may be cancelled directly where permitted.

Active source preparation uses cooperative cancellation.

Concurrent requests must not create inconsistent lifecycle
states or duplicate cancellation effects.

### Deployment readiness

`GET /api/v1/projects/:projectId/deployment-readiness`

Reports safe readiness information for the deployment
pipeline, including source preparation capabilities.

Readiness must distinguish source retrieval availability
from Docker build and runtime availability.

## 10. Transactional outbox

Phase 5.2 introduces durable publication between PostgreSQL
and Redis.

```text
Database transaction
  -> Deployment record
  -> Outbox event
  -> Commit

Outbox publisher
  -> Claim pending event
  -> Publish deterministic job
  -> Record publication result
```

The publisher must support:

- Bounded retries.
- Deterministic job identifiers.
- Conditional event claims.
- Recovery of expired publication claims.
- Duplicate publication handling.
- Sanitized failure information.
- Graceful shutdown.

A temporary Redis outage must not silently discard an
accepted deployment.

Publication recovery must preserve idempotency.

## 11. Worker execution leases

Deployment workers claim execution ownership using
fencing lease tokens.

A lease represents temporary execution ownership.

Worker operations must account for:

- Lease acquisition.
- Lease expiration.
- Lease renewal.
- Worker heartbeat.
- Attempt ownership.
- Cancellation.
- Retry exhaustion.
- Stale worker recovery.

A worker that loses its lease must not publish successful
source preparation metadata.

Redis locks alone are insufficient to authorize changes to
authoritative PostgreSQL deployment state.

Lease recovery must prevent stale attempts from overwriting
newer execution results.

## 12. Secure GitHub source retrieval

Phase 5.3 introduces:

- Immutable commit resolution.
- GitHub App installation revalidation.
- Short-lived installation tokens.
- SHA-pinned archive retrieval.
- Restricted redirect handling.
- Streaming download limits.
- Hardened TAR.GZ extraction.
- Isolated source workspaces.
- Source preparation metadata.
- Cooperative cancellation.
- Bounded retries.

Detailed implementation documentation:

`docs/phase-5.3-source-retrieval.md`

### Source preparation invariants

- Retrieve only the authorized immutable commit.
- Never substitute a newer branch revision.
- Never execute downloaded source.
- Reject unsafe archive paths and entry types.
- Apply resource limits during streaming.
- Validate metadata-derived archive paths.
- Do not expose installation credentials.
- Publish READY only after successful preparation.
- Require current lease ownership when publishing results.

## 13. Workspace lifecycle

Each source preparation attempt uses an isolated workspace.

Conceptual layout:

```text
runtime/
  deployments/
    <deployment-id>/
      <attempt-id>/
        source/
        metadata.json
```

The database stores an opaque source workspace key.

The workspace manager must validate identifiers and resolve
paths within the configured workspace root.

Required lifecycle behavior:

- Unique attempt directories.
- Restrictive filesystem permissions.
- Disk capacity checks.
- Partial download cleanup.
- Failed extraction cleanup.
- Cancellation cleanup.
- Expiration handling.
- Restart recovery.
- Active workspace protection.

A READY workspace must not be deleted while it is actively
owned by an authorized execution attempt.

Local filesystem storage does not automatically support
distributed workers or remote builders.

A future distributed architecture must define trusted
artifact transfer, integrity validation, and storage ownership.

## 14. Retry and cancellation strategy

Retryable failures are explicitly classified.

Examples include:

- GitHub rate limiting.
- Temporary GitHub unavailability.
- Interrupted downloads.
- Download timeouts.
- Temporary storage unavailability.

Retries must respect configured attempt budgets and backoff.

Authorization failures, unsafe archives, malformed input,
and missing commits must not be retried indefinitely.

Cancellation must be checked during long-running operations.

AbortSignal should interrupt supported network and extraction
operations.

Cancellation remains cooperative in Phase 5.3.

Actual termination latency must be verified under blocked
network and filesystem conditions.

## 15. Isolation and execution security

Phase 5.3 does not execute repository-controlled code.

Before Phase 5.4 enables isolated builds, the execution
environment must enforce:

- CPU limits.
- Memory limits.
- Process limits.
- Disk limits.
- Execution timeouts.
- Filesystem boundaries.
- Network restrictions.
- Credential isolation.
- Controlled cleanup.

The host Docker socket must never be exposed to untrusted
repository-controlled processes.

Docker Desktop availability does not establish sufficient
execution isolation.

The execution security model must be explicitly designed,
implemented, and tested.

If the required isolation cannot be established, execution
must remain disabled.

## 16. Environment variable security

Environment variable values are stored separately from
deployment records.

The existing encryption design uses AES-256-GCM with
workspace, project, environment, and key-associated data.

The ciphertext format is versioned.

Deployment configuration snapshots must never include
environment variable values.

Secrets must not enter:

- Queue payloads.
- Deployment snapshots.
- Source metadata.
- URLs.
- Build arguments.
- Container image layers.
- Process arguments.
- Application logs.
- Audit metadata.

Future secret injection must occur through a trusted
execution service with explicit access controls.

## 17. Encryption-key rotation

The current application uses a single configured active
encryption key.

Do not replace the active encryption key in place.

A future rotation mechanism must:

1. Introduce a versioned key registry.
2. Retain the previous key for decryption.
3. Use the new key for new encryption operations.
4. Re-encrypt existing values in bounded batches.
5. Preserve associated-data context.
6. Use conditional updates during migration.
7. Verify record counts and decryptability.
8. Maintain a tested backup and recovery path.
9. Retire old keys only after successful verification.

Never invalidate existing ciphertext as a shortcut.

## 18. Credential rotation

Credentials exposed outside their intended secret store
must be treated as compromised.

Review and rotate affected credentials, including:

- PostgreSQL credentials.
- Redis credentials.
- GitHub App client secrets.
- GitHub App private keys.
- OAuth client secrets.
- Session and authentication secrets.
- Future deployment credentials.

Update dependent services securely.

Verify replacement credentials before revoking old credentials
where the provider permits safe overlap.

Review session invalidation behavior before rotating
authentication material.

Encryption-key rotation requires the separate ciphertext
migration strategy described above.

Never commit actual credential values.

## 19. Logging and observability

Operational logs should contain structured, sanitized
deployment information.

Useful fields include:

- Workspace ID.
- Project ID.
- Deployment ID.
- Job identifier.
- Execution attempt.
- Lifecycle status.
- Resolved commit SHA.
- Downloaded bytes.
- Extracted bytes.
- File count.
- Operation duration.
- Sanitized failure code.

Authentication cookies, CSRF tokens, authorization headers,
private keys, and environment variable values must be redacted.

Future monitoring should integrate Prometheus and Grafana.

Planned metrics include:

- Queue depth.
- Outbox publication latency.
- Worker heartbeat availability.
- Deployment preparation duration.
- Source retrieval failure rate.
- Retry counts.
- Lease expiration counts.
- Workspace storage usage.

These metrics are roadmap items unless their implementation
is independently confirmed.

## 20. Dependency security

The Phase 5.3 implementation report identifies three
high-severity npm audit findings associated with the
Prisma/Effect dependency chain.

Reported advisory:

`GHSA-7589-6fvf-x4pw`

The documented dependency tree includes:

```text
prisma@7.10.1
  -> @prisma/config@7.10.1
    -> effect@3.20.0
```

No successful remediation has been established.

Do not:

- Run forced npm audit fixes without review.
- Introduce an unverified dependency override.
- Downgrade Prisma across major versions without testing.
- Adopt an unstable release solely to silence audit output.
- Claim that a vulnerability is fixed without evidence.

Recheck the actual installed dependency tree and current
advisory before choosing a compatible remediation.

Document accepted development risks separately from
production release requirements.

## 21. Testing and acceptance evidence

### Phase 5.2 baseline

The implementation agent reported:

- API tests: 7 of 7 passed.
- Database, queue, worker, and API checks passed.

### Phase 5.3 verification

The implementation agent reported:

- Worker tests: 17 of 17 passed.
- Full monorepo build passed.
- TypeScript checks passed.
- ESLint passed.
- Live GitHub source retrieval succeeded.

Live retrieval details:

- Repository: raziquehasan/CloudPilot.
- Branch: main.
- Commit:
  a872eb598f73cdf8a8924ff32e71205e2f019036.
- Compressed archive: 269,241 bytes.
- Extracted data: 1,194,024 bytes.
- Extracted files: 179.

These results are reported implementation evidence.

They must not be represented as independently reproduced
verification until the corresponding tests are rerun.

### Outstanding verification

- Exact extracted file-content comparison.
- Authorized private repository retrieval.
- Unauthorized repository rejection.
- Stale-worker fencing under concurrent execution.
- Worker restart and workspace recovery.
- Cancellation during blocked network operations.
- Responsive dashboard visual verification.
- Final dependency audit.
- Final regression checks after code enhancements.

## 22. Milestone roadmap

### Phase 5.1 — Deployment API and state machine

- Additive database schema.
- Authenticated deployment endpoints.
- Workspace authorization.
- CSRF protection.
- Idempotency.
- Conditional state transitions.
- Audit events.

### Phase 5.2 — Queue and worker reliability

- BullMQ deployment processing.
- Transactional outbox.
- Deterministic job identifiers.
- Execution leases.
- Worker heartbeat.
- Bounded retries.
- Reconciliation.
- Graceful shutdown.

### Phase 5.3 — Secure source retrieval

- Immutable commit resolution.
- GitHub installation authorization.
- Secure archive download.
- Hardened extraction.
- Isolated source workspace.
- Source metadata persistence.
- SOURCE_PREPARED lifecycle state.

### Phase 5.4 — Isolated Docker build engine

- Disposable isolated Linux builder.
- Dockerfile-based builds first.
- Resource and network restrictions.
- Read-only source access.
- Build timeout and cancellation.
- Controlled image output.
- Build isolation verification.

### Phase 5.5 — Container runtime

- Authorized image execution.
- Container lifecycle management.
- Runtime resource restrictions.
- Network isolation.
- Controlled port allocation.
- Secure runtime secret injection.

### Phase 5.6 — Real-time logs

- Ordered build and runtime logs.
- Bounded log retention.
- Secret redaction.
- Authorized live delivery.

### Phase 5.7 — Health and recovery

- Health checks.
- Stop and restart.
- Redeployment.
- Rollback.
- Resource cleanup.

### Phase 5.8 — GitHub webhooks

- Raw-body signature verification.
- Delivery idempotency.
- Repository authorization.
- Explicit auto-deployment configuration.

### Phase 5.9 — End-to-end verification

- Trusted disposable repository tests.
- Successful deployment lifecycle.
- Failed build handling.
- Unhealthy runtime handling.
- Cancellation.
- Rollback.
- Resource cleanup.
- Security regression testing.

## 23. Phase 5.4 entry gate

The isolated build engine must not start until:

- Deployment state is SOURCE_PREPARED.
- Source preparation status is READY.
- Source identity matches the immutable deployment SHA.
- Tenant ownership is verified.
- Source workspace ownership is verified.
- The source workspace is available and unexpired.
- A fresh execution lease is acquired.
- Build configuration is valid.
- Required execution isolation is available.
- Relevant Phase 5.3 regression tests pass.

The transition from SOURCE_PREPARED to BUILDING must be
conditional and protected by execution ownership.

A prepared source workspace is not authorization to execute
repository-controlled code on the host.

## 24. Known limitations

- Docker build execution remains disabled.
- Container runtime execution remains disabled.
- Cancellation remains cooperative.
- Prisma/Effect advisory remains unresolved.
- Single active encryption-key configuration remains.
- Distributed source workspace storage is not implemented.
- Multi-region deployment support is not implemented.
- Deployment endpoint rate limiting requires verification.
- Production-grade execution isolation is not established.

## 25. Release policy

Every milestone requires:

- Functional verification.
- Security review.
- Database consistency review.
- Authorization testing.
- Failure recovery testing.
- Dependency review.
- Documentation updates.

Do not enable production or public multi-tenant execution
until the required isolation and security controls have
been implemented and independently verified.

CloudPilot must prioritize secure and recoverable deployment
orchestration over premature execution capabilities.
