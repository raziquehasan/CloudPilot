
# CloudPilot — Phase 5.3: Secure GitHub Source Retrieval Engine

## 1. Overview

Phase 5.3 introduces a secure, commit-pinned GitHub source
retrieval pipeline for CloudPilot.

The pipeline retrieves authorized repository source code and
prepares an isolated workspace for the future build engine.

It does not:

- Execute repository-controlled code.
- Run package managers or lifecycle scripts.
- Execute Dockerfiles.
- Build container images.
- Start application containers.
- Perform production deployments.

The Docker build and runtime engines remain separate release
gates in Phases 5.4 and 5.5.

## 2. Core security principles

1. PostgreSQL is the authoritative deployment state store.
2. Every deployment is bound to an immutable commit SHA.
3. GitHub authorization is checked before source retrieval.
4. Access tokens remain in worker memory, never in job payloads.
5. Repository archives and metadata are untrusted input.
6. Source files are never executed during Phase 5.3.
7. Each retrieval attempt uses an isolated workspace.
8. Worker state changes require a valid execution lease.
9. Temporary artifacts must have bounded lifetimes.
10. Full deployment execution remains disabled.

## 3. System architecture

```text
Authenticated deployment request
                |
                v
        Workspace authorization
                |
                v
        GitHub repository check
                |
                v
        Immutable SHA resolution
                |
                v
     PostgreSQL deployment record
          + outbox event
                |
                v
     Transactional outbox publisher
                |
                v
          BullMQ job
                |
                v
      Worker execution lease
                |
                v
      GitHub installation check
                |
                v
      Short-lived access token
                |
                v
     SHA-pinned archive download
                |
                v
        Safe TAR.GZ extraction
                |
                v
       Isolated source workspace
                |
                v
        Source metadata write
                |
                v
          SOURCE_PREPARED
                |
                v
      Phase 5.4 build gate
         (not enabled yet)
```

Important components:

| Component | Responsibility |
|---|---|
| commit-resolution.service.ts | Resolve exact commit |
| deployment-outbox.service.ts | Durable publication |
| deployment.processor.ts | Worker lifecycle |
| source/github-auth.ts | GitHub App authentication |
| source/download.ts | Archive download |
| source/extract.ts | Secure extraction |
| source/workspace.ts | Workspace lifecycle |
| source/prepare.ts | Source preparation orchestration |

The database remains authoritative throughout the pipeline.

## 4. Immutable commit resolution

The API resolves the requested GitHub branch or explicitly
selected commit before accepting the deployment.

Requirements:

- Validate the repository against the connected installation.
- Verify active workspace and project ownership.
- Validate the requested Git reference.
- Resolve an exact 40-character commit SHA.
- Persist the resolved SHA before queue publication.
- Never substitute a different commit when retrieval fails.

Once persisted, the resolved SHA must not change because the
remote branch moves.

The worker retrieves the exact persisted commit.

A missing branch or inaccessible commit must fail safely.

## 5. GitHub App authentication

CloudPilot reuses its existing GitHub App integration.

The worker:

1. Loads the authorized installation reference.
2. Checks installation availability and repository access.
3. Rejects suspended installations.
4. Mints a short-lived installation token.
5. Downloads source using that token.
6. Discards the token when no longer required.

Personal access tokens are not used as fallback credentials.

Installation tokens must never enter:

- Deployment records.
- BullMQ payloads.
- Frontend state.
- Audit metadata.
- URLs.
- Source workspace metadata.
- Application logs.

Authorization must also be rechecked at retrieval time,
because repository permissions can change after the original
deployment request.

## 6. Secure archive download

The source downloader retrieves the exact commit archive from
GitHub's authenticated repository archive API.

Required controls:

- Validate owner, repository and commit identifiers.
- Construct the initial URL from trusted components.
- Require HTTPS.
- Limit redirect count.
- Allow only explicitly approved redirect destinations.
- Strip authorization headers on cross-origin redirects.
- Reject arbitrary user-supplied download URLs.
- Bound download time and compressed archive size.
- Support AbortSignal cancellation.
- Use bounded retries for transient failures.

The redirect allowlist is necessary but is not a complete SSRF
defense by itself. Redirect handling must also reject unexpected
ports, embedded credentials, malformed URLs and unsafe protocols.

The implementation must not forward GitHub credentials to a
redirect target merely because its hostname is approved.

A failed or interrupted download must not become a READY source.

## 7. Secure TAR.GZ extraction

Repository archives are untrusted.

The extractor must enforce path, entry-type and resource
validation before making filesystem changes.

### Path security

Reject:

- Absolute paths.
- Parent directory traversal.
- Windows drive-letter paths.
- UNC paths.
- Backslash-based escape attempts.
- Windows alternate data streams.
- Reserved Windows filenames.
- Unsafe trailing spaces or periods.
- Invalid or conflicting destination paths.
- Duplicate entries.
- Case-insensitive filename collisions.
- File-directory conflicts.

Every effective path must remain inside the authorized
destination after canonicalization.

### TAR metadata

Support valid repository archive metadata, including:

- Per-entry PAX headers.
- Bounded global PAX metadata.
- GNU long filenames where supported.

Validate metadata-derived effective names using the same path
rules as ordinary TAR entry names.

Reject unsupported links, special files, unsafe metadata
overrides and malformed extended headers.

Parsing an extended header must not bypass resource limits.

### Resource limits

Enforce bounded:

- Compressed archive bytes.
- Total decompressed stream bytes.
- Individual file size.
- Total extracted file bytes.
- File count.
- Directory and metadata entries.
- Metadata size.
- Extraction duration.

Limits must apply during processing, not only afterward.

A valid TAR termination and complete gzip integrity check are
required before publishing source readiness.

### Execution restrictions

Do not execute:

- Git hooks.
- npm, pnpm or yarn commands.
- Package lifecycle scripts.
- Shell scripts.
- Dockerfiles.
- Git submodules automatically.

Preserve source file content without trusting repository-defined
permissions or privileged filesystem metadata.

## 8. Isolated source workspaces

Source workspaces are managed by the worker.

Conceptual layout:

```text
SOURCE_WORKSPACE_ROOT/
  deployments/
    <deployment-id>/
      <attempt-id>/
        source/
        metadata.json
```

Deployment and attempt identifiers must be generated or
validated by the server.

The workspace manager must provide:

- Unique workspace per attempt.
- Filesystem containment.
- Restrictive permissions where supported.
- Safe directory creation.
- Protection against symlink and junction traversal.
- Storage capacity checks.
- Partial-attempt cleanup.
- Expired workspace cleanup.
- Protection of active workspaces.
- Atomic publication of completed metadata.

Only an opaque source workspace key is persisted in deployment
metadata.

Absolute filesystem paths must not be exposed through the
public API.

### Recovery and cleanup

A crashed worker may leave incomplete workspaces.

Recovery must distinguish:

- Active source preparations.
- Expired execution leases.
- Failed attempts.
- Cancelled attempts.
- Successfully prepared sources awaiting future builds.

Cleanup must not delete a workspace simply because it is old
if it is still owned by an active execution attempt.

A failed attempt must never cause deletion outside the
configured runtime workspace root.

## 9. Database architecture

The additive migration:

`20261126090000_deployment_source_retrieval`

introduces:

- Deployment lifecycle status `SOURCE_PREPARED`.
- `DeploymentSourceStatus` enum.
- `deployment_source_preparations` table.

The source preparation record includes:

- Workspace ID.
- Project ID.
- Deployment ID.
- Repository identity.
- Requested branch.
- Resolved commit SHA.
- Preparation status.
- Attempt count.
- Opaque workspace key.
- Downloaded and extracted byte counts.
- File count.
- Sanitized failure code.
- Creation, update and lifecycle timestamps.

A composite foreign key ties source preparation metadata to
the corresponding workspace, project and deployment.

The unique deployment relationship allows one authoritative
source preparation record per deployment.

Do not modify the already-applied migration in place.

Any later database integrity constraints must be introduced
through a new additive migration after existing records are
verified against the proposed constraints.

## 10. Worker integration

The deployment worker reuses the existing:

- BullMQ deployment queue.
- Transactional outbox.
- Deployment state machine.
- Execution leases.
- Worker heartbeats.
- Retry policy.
- Reconciliation mechanism.
- Cancellation mechanism.

A worker must hold the current execution lease before
publishing source preparation results.

State changes must use conditional database updates that
reject outdated fencing tokens.

Expected successful progression:

```text
QUEUED
  |
  v
PREPARING
  |
  v
SOURCE_PREPARED
```

Source preparation status progresses independently:

```text
PENDING
  |
  v
DOWNLOADING
  |
  v
EXTRACTING
  |
  v
READY
```

Failure and cancellation paths must remain compatible with
the deployment state machine.

Successfully retrieving a source archive must never mark
the deployment as successfully deployed.

## 11. Retry and failure policy

Source preparation errors use sanitized failure codes.

Retryable failures include:

- GitHub rate limiting.
- Temporary GitHub service unavailability.
- Download timeouts.
- Interrupted downloads.
- Insufficient disk space.
- Temporary workspace unavailability.

Permanent failures include invalid authorization, missing
commits, malformed archives and unsafe archive entries.

Retries must remain bounded by the configured job policy.

The `retryable` error attribute indicates eligibility, not
permission to retry indefinitely.

Before each retry, the worker must recheck:

- Deployment cancellation.
- Execution lease validity.
- Retry budget.
- Repository access.
- Workspace state.

An expired or lost lease must not permit stale metadata writes.

Cancellation should interrupt active operations through
AbortSignal wherever supported.

## 12. Deployment readiness API

Endpoint:

`GET /api/v1/projects/:projectId/deployment-readiness`

The API exposes safe capability information for:

- Connected repository.
- Commit resolution.
- Source retrieval.
- Workspace storage.
- Queue and worker availability.
- Docker build capability.
- Container execution capability.

A source-prepared deployment is not a running application.

Docker build and runtime execution must remain disabled until
their separate security gates are implemented and tested.

Do not expose credentials, internal source paths or sensitive
worker infrastructure details in readiness responses.

## 13. Frontend integration

The project dashboard presents available source preparation
information, including:

- Connected repository.
- Requested branch.
- Resolved commit SHA.
- Source retrieval readiness.
- Source preparation status.

Use actual API data.

Do not display fabricated progress percentages, deployment
URLs, runtime logs or successful deployment results.

Preserve existing CloudPilot styling.

Responsive layouts must be visually verified separately at
375px, 768px, 1024px and 1440px.

Full deployment execution remains disabled.

## 14. Configuration

Non-secret configuration keys:

| Variable | Purpose |
|---|---|
| SOURCE_WORKSPACE_ROOT | Runtime source directory |
| SOURCE_DOWNLOAD_TIMEOUT_MS | Download timeout |
| SOURCE_MAX_ARCHIVE_BYTES | Compressed size limit |
| SOURCE_MAX_EXTRACTED_BYTES | Extracted data limit |
| SOURCE_MAX_FILE_BYTES | Per-file size limit |
| SOURCE_MAX_FILE_COUNT | File count limit |
| SOURCE_MAX_RETRIES | Download retry limit |
| SOURCE_WORKSPACE_TTL_SECONDS | Workspace lifetime |
| SOURCE_CLEANUP_INTERVAL_SECONDS | Cleanup frequency |
| SOURCE_MIN_FREE_BYTES | Minimum available storage |

Configuration is validated on startup.

Runtime source workspaces must be excluded from version control.

The real .env file and credential material must never be
committed.

## 15. Testing strategy

### Deterministic unit tests

Test:

- Valid repository archive extraction.
- PAX and GNU filename compatibility.
- Malformed TAR metadata.
- Path traversal.
- Duplicate and conflicting paths.
- Symbolic and hard links.
- Special device files.
- File size and count limits.
- Decompression resource limits.
- Unsafe redirect rejection.
- Authorization header stripping.
- Incorrect or missing commits.
- Workspace containment.
- Cleanup protection.

### Integration tests

Verify:

- PostgreSQL transaction and outbox consistency.
- Worker execution lease ownership.
- Stale worker rejection.
- Concurrent preparation behavior.
- Cancellation and retry recovery.
- Tenant isolation.
- Metadata consistency.
- Worker restart recovery.

### Live smoke test

The controlled GitHub retrieval test should:

1. Verify the authorized repository.
2. Resolve the main branch to an immutable commit SHA.
3. Retrieve the archive for that exact SHA.
4. Extract into a disposable isolated workspace.
5. Confirm expected repository files exist.
6. Compare at least one extracted file against the same
   GitHub commit using byte comparison or a cryptographic hash.
7. Verify stored metadata refers to the resolved commit.
8. Delete only the disposable test workspace.

No repository code may be executed during the smoke test.

### Recorded Phase 5.3 results

The implementation agent reported:

- API baseline: 7 of 7 tests passed.
- Worker tests: 17 of 17 passed.
- Full monorepo build: passed.
- API, worker, queue and database typechecks: passed.
- Frontend TypeScript and ESLint: passed.
- Live retrieval: 269,241 compressed bytes.
- Extracted data: 1,194,024 bytes.
- Extracted files: 179.
- Resolved commit:
  a872eb598f73cdf8a8924ff32e71205e2f019036.

These results are agent-reported and should be reproducible
from recorded commands and test output.

The report confirms expected file existence and commit metadata
binding but does not provide independent evidence of exact
file-content comparison.

Private repository retrieval and visual responsive testing
also require separate verification.

## 16. Observability

Structured logs may contain:

- Workspace ID.
- Project ID.
- Deployment ID.
- Execution attempt.
- Resolved commit SHA.
- Downloaded byte count.
- Extracted byte count.
- File count.
- Duration.
- Sanitized failure code.

Never log:

- GitHub App private keys.
- Installation access tokens.
- Authorization headers.
- Database credentials.
- Environment variable values.
- Retrieved repository file content.

Operational metrics should be designed for future integration
with Prometheus and Grafana.

## 17. Known limitations

### Dependency advisory

The Phase 5.3 report identifies three high-severity npm audit
findings associated with the Prisma/Effect dependency chain.

The report does not claim remediation.

Recheck the actual dependency tree and advisory status before
choosing a stable, compatible fix.

Do not apply forced downgrades or unstable releases without
independent compatibility and security verification.

### Source workspace portability

The current approach relies on local worker filesystem paths.

That does not automatically make source workspaces available
to a remote Phase 5.4 builder.

For distributed workers, a trusted artifact-transfer mechanism
or explicit storage architecture will be required.

### Cancellation

Cancellation is cooperative.

The worker must check cancellation during long operations and
ensure that network reads, archive extraction and filesystem
writes have bounded termination behavior.

### Cleanup and crash recovery

Crash recovery and cleanup must be validated under actual
interrupted worker scenarios.

A source marked READY must remain available long enough for
the authorized build attempt to consume it.

### Execution isolation

Phase 5.3 prepares untrusted source files but does not execute
them.

Source readiness is not evidence of safe code execution.

The production Docker build security boundary remains
unimplemented.

## 18. Phase 5.4 integration contract

Phase 5.4 must verify all of the following before starting
an isolated build:

- Deployment state is SOURCE_PREPARED.
- Source preparation status is READY.
- Workspace, project and deployment identities match.
- Resolved commit SHA matches the deployment record.
- Source workspace exists and is accessible.
- Source workspace ownership is valid.
- Execution lease is acquired successfully.
- Build configuration is authorized and valid.
- Required isolation capabilities are available.

The build engine must not trust a sourceWorkspaceKey without
resolving and validating it through the workspace manager.

The eventual source mount should be read-only, and the
builder must not receive access to host secrets, the host
Docker socket, or unrelated deployment workspaces.

The state transition:

`SOURCE_PREPARED -> BUILDING`

must be conditional and protected by a valid execution lease.

Phase 5.4 must also establish CPU, memory, process, disk,
filesystem, network and execution-time restrictions before
enabling repository-controlled builds.

Do not treat a basic Docker container as sufficient isolation
without validating the intended security model.

## 19. Phase acceptance checklist

Phase 5.3 can be considered accepted when:

- Immutable commit resolution is verified.
- Authorized repository retrieval works.
- Unauthorized repository access is rejected.
- Safe extraction tests pass.
- Real GitHub archive compatibility is verified.
- Resource limits are enforced.
- Workspace isolation is tested.
- Cancellation and retries are tested.
- Execution lease fencing is verified.
- Source metadata is committed consistently.
- No downloaded code is executed.
- Build and lint checks pass.
- Full deployment execution remains disabled.
- Remaining security risks are documented.

## 20. Future roadmap

- Phase 5.4 — Isolated Docker Build Engine.
- Phase 5.5 — Container Runtime and Deployment.
- Phase 5.6 — Real-time Build and Runtime Logs.
- Phase 5.7 — Health Checks, Recovery and Rollback.
- Phase 5.8 — GitHub Webhooks and Auto Deployment.
- Phase 5.9 — End-to-End Deployment Verification.

Phase 5.3 establishes the source preparation boundary.
It does not authorize subsequent build or runtime execution
without additional isolation and security verification.
