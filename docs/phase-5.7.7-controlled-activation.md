# Phase 5.7.7 — Controlled health monitoring and manual recovery activation

Status: **Step 1 (migration), Stages A/B/C complete and verified. The disposable validation
deployment is BLOCKED pending approval** (fixed-port provisioning unsupported + untrusted/
non-existent validation repo). No deployment, code, `.env`, or DB records were created/changed
for the validation app. PostgreSQL/Redis and the demo container were not restarted. Writes so far:
read-only backup file(s) outside the repo (§2), the approved migration (§5.1), the Stage A/B/C
`.env` flag blocks (§5.3/§5.4/§5.5), and controlled API/worker restarts (§5.3/§5.4/§5.5).

## 1. Preflight findings

### Endpoints (credentials omitted)

| Component | Endpoint / locator | State |
| --- | --- | --- |
| PostgreSQL | `127.0.0.1:5433`, database `cloudpilot`, schema `public` (container `cloudpilot-postgres`, `postgres:15-alpine`) | healthy (up 40h) |
| Redis | `127.0.0.1:6379` (container `cloudpilot-redis`, `redis:7-alpine`) | healthy; `PING` → `PONG` |
| API | `http://127.0.0.1:8000` (`tsx watch src/server.ts`) | running |
| Web | `http://localhost:3000` | running |
| Worker | `npm run dev -w @cloudpilot/worker` (`tsx watch src/worker.ts`) | running |

### Migration status

- 16 migrations present in `packages/database/prisma/migrations`; **15 applied, 1 pending**.
- Pending: `20261127150000_recovery_restart_intent`.
- The applied health-domain migrations already created `deployment_health_states`,
  `deployment_health_checks`, and `deployment_recovery_attempts` (live counts:
  1 health state, 3 health checks, 0 recovery attempts — all for the demo deployment).
- `recoverySchemaReady()` checks `_prisma_migrations` for the pending migration name;
  it currently returns **false**, so manual recovery is blocked at the schema layer even
  if `MANUAL_RECOVERY_ENABLED` were set true.

### Pending migration inventory

`20261127150000_recovery_restart_intent/migration.sql` (additive, wrapped in `BEGIN/COMMIT`):

- `ALTER TYPE "HealthFailureReason" ADD VALUE` × 6 (`RECOVERY_STALE`,
  `RECOVERY_IDENTITY_MISMATCH`, `RECOVERY_PORT_CHANGED`, `RECOVERY_DOCKER_UNAVAILABLE`,
  `RECOVERY_TIMEOUT`, `RECOVERY_RESTART_UNCERTAIN`).
- `ALTER TABLE "deployment_recovery_attempts"` add nullable columns `healthVersion`,
  `restartIssuedAt`, `restartSourceStartedAt` and check constraint `dra_restart_fence_chk`.
- No data backfill, no destructive DDL. PostgreSQL 15 permits `ADD VALUE` inside a transaction.
- Dependency: requires the two prior health migrations (already applied).

### API / worker compatibility

- Same repository and commit; worker runs live via `tsx watch` (no stale build).
- Worker capabilities (from live readiness): `dockerBuild=true`, `containerRuntime=true`,
  `containerExecution=true`, `sourceRetrieval=true`, deployment queue available.
- Schema is exactly one migration behind the code's recovery expectations.

### Feature flags (current values)

| Flag | Scope | Current | Source |
| --- | --- | --- | --- |
| `HEALTH_PERSISTENCE_ENABLED` | worker | unset → `false` | `apps/worker/src/config/env.ts:610` |
| `HEALTH_MONITORING_ENABLED` | worker | unset → `false` | `apps/worker/src/config/env.ts:611` |
| `MANUAL_RECOVERY_ENABLED` | API + worker | unset → `false` | `apps/api/.../env.ts:581`, worker `:612` |
| `AUTO_RECOVERY_ENABLED` | worker | unset → `false` (no trigger) | worker `:613` |
| `RUNTIME_TRUSTED_LOCAL_ONLY` | worker | unset → `true` | worker env |
| `BUILD_TRUSTED_REPOSITORIES` | worker | `raziquehasan/cloudpilot-demo-app` | `.env` |
| `BUILD_ENABLED` / `RUNTIME_ENABLED` | worker | `true` / `true` | `.env` |
| `DEPLOYMENT_LOGS_ENABLED`, `LOG_*` | worker | `true` | `.env` |

### Existing runtime deployments (preserve)

- `RUNNING`: deployment `8b55c7b0-c37e-4b7c-afc7-aa9a73df75a7` (project `cloudpilot-demo-app`),
  container `f9144585…`, image `sha256:15b896427fd3…`, host `127.0.0.1:53143`, ready.
  **Do not restart or modify.**
- Other: 4 `FAILED`, 1 `DEPLOYING`.

### Deployment-level scoping

`discoverDueHealthChecks(limit, deploymentId?)` supports per-deployment scoping, but the
production worker start (`apps/worker/src/worker.ts:1769`) calls it **without** a
`deploymentId`, so enabling monitoring is global but constrained to
`BUILD_TRUSTED_REPOSITORIES` + `RUNTIME_TRUSTED_LOCAL_ONLY`. Probes are read-only
(docker inspect + HTTP); with `AUTO_RECOVERY_ENABLED=false` no restart can occur.

## 2. Backup and restoration procedure

Backup created (read-only `pg_dump` executed inside the container; no host tooling required):

```
Backup file: C:\Users\asus\OneDrive\Desktop\CloudPilot\backups\cloudpilot-pre-5.7.7-20261128-121754.dump
Format: custom (-Fc), size 561,591 bytes
Integrity: pg_restore --list exit 0; 235 TOC entries; 27 TABLE DATA sections
```

Reproduce / refresh:

```powershell
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
$dir = "C:\Users\asus\OneDrive\Desktop\CloudPilot\backups"
New-Item -ItemType Directory -Path $dir -Force | Out-Null
docker exec cloudpilot-postgres pg_dump -U cloudpilot -d cloudpilot -Fc -f "/tmp/cloudpilot-$ts.dump"
docker cp "cloudpilot-postgres:/tmp/cloudpilot-$ts.dump" "$dir\cloudpilot-$ts.dump"
docker exec cloudpilot-postgres pg_restore --list "/tmp/cloudpilot-$ts.dump"
```

Restoration (only if a rollback is ever approved; **not** part of this phase):

```powershell
# Stop writers first (API + worker), then restore into the same database.
docker cp "$dir\cloudpilot-<ts>.dump" "cloudpilot-postgres:/tmp/restore.dump"
docker exec cloudpilot-postgres pg_restore -U cloudpilot -d cloudpilot --clean --if-exists --no-owner /tmp/restore.dump
```

Risk: `pg_restore --clean` drops and recreates objects; only use against the disposable
or, with explicit approval, the primary database after stopping writers.

## 3. Activation sequence (prepared, not executed)

Stage A — persistence only, all execution/recovery off:

```
HEALTH_PERSISTENCE_ENABLED=true
HEALTH_MONITORING_ENABLED=false
MANUAL_RECOVERY_ENABLED=false
AUTO_RECOVERY_ENABLED=false
```

Stage B — enable monitoring for the trusted worker configuration; verify scheduler
readiness, runtime eligibility, lease ownership, and health-check persistence.

```
HEALTH_MONITORING_ENABLED=true
```

Stage C — enable manual recovery in **both** API and worker after the migration is applied:

```
MANUAL_RECOVERY_ENABLED=true
```

`AUTO_RECOVERY_ENABLED` stays `false` throughout. These are worker/API env values; the
worker and API must be restarted to pick them up (requires approval).

## 4. Exact commands (to run only after approval)

```powershell
# 0. Stop writers before backup/migration (approval required).
# 1. Apply the single pending migration (primary DB) — APPROVAL REQUIRED.
npx prisma migrate deploy            # from packages/database (applies 20261127150000_recovery_restart_intent)

# 2. Confirm status is clean.
npx prisma migrate status

# 3. Stage A/B/C: set the flags in .env (approval required), then restart worker + API.
#    (No command executed in this phase.)

# 4. End-to-end dashboard recovery is performed manually via the Health tab after activation.
```

## 5. Actual verification evidence (this phase)

### 5.1 Approved migration (executed)

Command (from `packages/database`, repo-installed Prisma, existing config):

```
npx prisma migrate deploy
```

Result: **success** — `Applying migration 20261127150000_recovery_restart_intent` …
`All migrations have been successfully applied.` (exit 0).

Post-migration `npx prisma migrate status` → `Database schema is up to date!`
(16 migrations found, **0 pending**). `_prisma_migrations` row for
`20261127150000_recovery_restart_intent`: `finished = t`, `rolled_back = null`
(so `recoverySchemaReady()` now returns true).

Schema verification (via `docker exec cloudpilot-postgres psql`):

- New `HealthFailureReason` enum values present: `RECOVERY_STALE`,
  `RECOVERY_IDENTITY_MISMATCH`, `RECOVERY_PORT_CHANGED`, `RECOVERY_DOCKER_UNAVAILABLE`,
  `RECOVERY_TIMEOUT`, `RECOVERY_RESTART_UNCERTAIN` (plus pre-existing
  `RECOVERY_EXECUTION_FAILED`, `RECOVERY_EXHAUSTED`).
- New columns on `deployment_recovery_attempts`: `healthVersion`, `restartIssuedAt`,
  `restartSourceStartedAt`.
- Check constraint `dra_restart_fence_chk` present.

Data preservation (before → after, identical):

| Table | Before | After |
| --- | --- | --- |
| deployments | 6 | 6 |
| deployment_runtime_instances | 3 | 3 |
| deployment_health_states | 1 | 1 |
| deployment_health_checks | 3 | 3 |
| deployment_recovery_attempts | 0 | 0 |

Demo container unchanged: runtime `8c47b816-e7b3-49e7-baf5-efab3168e8ef`, container
`f9144585eb879e81ec21402a7a2f24b2759fa6a7cf441ad368cbce70b19da472`, `127.0.0.1:53143`,
docker `Up 14 hours`, `53143->3000`. API `/health` → `200`; 5 worker/api node processes
alive. Feature flags remain **unset/false** (no `.env` change, no restart).

### 5.2 Preflight (read-only)

- `npx prisma migrate status` (pre) → 1 pending (`20261127150000_recovery_restart_intent`).
- `docker ps` → demo container + postgres + redis healthy; demo host port `53143`.
- Redis `PING` → `PONG`; read-only DB query → 1 RUNNING (demo), 4 FAILED, 1 DEPLOYING;
  health tables present (1 state, 3 checks, 0 recoveries).
- Backup + `pg_restore --list` integrity check passed (see §2).

### 5.3 Stage A — health persistence activation (executed)

Pre-activation safety:

- DEPLOYING deployment `23aede51-3aa8-4880-bc62-eb35b39409c1` (created `2026-11-27 16:04:35`,
  no execution lease): build `a0c29be9` `SUCCEEDED`, runtime `48a84f49` `QUEUED`, neither with
  a lease ⇒ **stale, not actively executing**. Left untouched (not cancelled/repaired).
- No active build/runtime execution leases (`executionLeaseExpiresAt > now` → none).
- BullMQ: only `bull:diagnostic:*` keys; **no** `bull:runtime:*` / `bull:build:*` queues ⇒ no
  active deployment/recovery jobs.
- Duplicate API processes were present (two `npm run dev -w @cloudpilot/api` + two
  `tsx watch src/server.ts`); all API/worker node processes were stopped and restarted once each.

Configuration (`.env` backed up to `…\backups\.env.pre-stageA-20261128-122507.bak`, outside the
repo; `.gitignore` covers `.env`/`.env.*`). Appended block:

```
HEALTH_PERSISTENCE_ENABLED=true
HEALTH_MONITORING_ENABLED=false
MANUAL_RECOVERY_ENABLED=false
AUTO_RECOVERY_ENABLED=false
```

Effective config (verified by importing the same env modules):

- Worker: `HEALTH_PERSISTENCE_ENABLED=true`, `HEALTH_MONITORING_ENABLED=false`,
  `MANUAL_RECOVERY_ENABLED=false`, `AUTO_RECOVERY_ENABLED=false`, `RUNTIME_ENABLED=true`,
  `BUILD_TRUSTED_REPOSITORIES=["raziquehasan/cloudpilot-demo-app"]`.
- API: `MANUAL_RECOVERY_ENABLED=false`.

Restart (controlled, one process each; PostgreSQL/Redis/demo untouched):

- API `npm run dev -w @cloudpilot/api` → `CloudPilot API started` on `127.0.0.1:8000`.
- Worker `npm run dev -w @cloudpilot/worker` → `startup completed` with `dockerBuild=true`,
  `containerRuntime=true`, `deploymentAvailable=true`; heartbeat registered for
  `worker-b33cecf7…` (deployment + diagnostic).

Verification:

- API `/health` → `200`.
- Counts unchanged (no scheduler/recovery activity): `health_checks=3`,
  `recovery_attempts=0`, `health_states=1`.
- No `bull:runtime:*` / `bull:build:*` keys (no jobs enqueued).
- Health endpoint readable: `GET …/deployments/8b55c7b0…/health` → `200`,
  `schemaAvailable=true`, `runtimeStatus=RUNNING`, state `HEALTHY`.
- Recovery schema ready (migration applied, §5.1); monitoring/manual/auto all disabled.
- Demo preserved: Id `f9144585…`, Image `sha256:15b896427fd3…`, `StartedAt 2026-11-27T16:36:43Z`,
  `127.0.0.1:53143` (unchanged).

### 5.4 Stage B — live health monitoring activation (executed)

Pre-activation: API `/health` 200; exactly one API + one worker process (no duplicates);
no build/runtime leases; no `bull:runtime:*`/`bull:build:*` keys; stale DEPLOYING
`23aede51…` left untouched. Eligible-runtime inventory (trusted repo + local-only + RUNNING +
ready + valid container/image + 127.0.0.1/::1): **exactly one** — the demo runtime
`8c47b816…` (container `f9144585…`, image `sha256:15b896427fd3…`, `127.0.0.1:53143`,
repo `raziquehasan/cloudpilot-demo-app`). No unexpected/untrusted runtime eligible.

Configuration (`.env` backed up to `…\backups\.env.pre-stageB-20261128-123323.bak`, outside the
repo). `HEALTH_MONITORING_ENABLED` changed `false → true` in place (single definition, no
duplicate). Final flags: persistence `true`, monitoring `true`, manual `false`, auto `false`;
concurrency `4`, batch `16` (unchanged). Effective worker env verified via env module.

Worker restart (only the worker; API not required, left running):

- Old worker stopped; new `npm run dev -w @cloudpilot/worker` started `worker-c49b0fbb…`.
- `Deployment health monitor … event:started concurrency:4 batchSize:16`.
- `startup completed` with `dockerBuild=true, containerRuntime=true, deploymentAvailable=true`.

Live monitoring evidence — new persisted checks for the demo (gen 0, all `PASSED`/`running`,
HTTP `200`, latency persisted), ~16s interval:

| checkedAt | docker | http | latency |
| --- | --- | --- | --- |
| 2026-11-28 07:04:57 | PASSED/running | 200 | 10 ms |
| 2026-11-28 07:05:12 | PASSED/running | 200 | 8 ms |
| 2026-11-28 07:05:29 | PASSED/running | 200 | 9 ms |
| 2026-11-28 07:05:45 | PASSED/running | 200 | 8 ms |
| 2026-11-28 07:06:01 | PASSED/running | 200 | 9 ms |
| 2026-11-28 07:06:17 | PASSED/running | 200 | 29 ms |

Before → after: `deployment_health_checks` 3 → 12 (9 new, no duplicates); health state
`HEALTHY`, generation `0`, `lastCheckedAt` advanced to `07:06:01`, `nextCheckAt` `07:06:16`;
`deployment_recovery_attempts` `0` (unchanged); no `bull:runtime:*`/`bull:build:*` jobs;
no automatic restart/recovery.

Demo preservation (before == after): Id `f9144585…`, Image `sha256:15b896427fd3…`,
`StartedAt 2026-11-27T16:36:43Z`, `127.0.0.1:53143`. Monitoring was read-only (docker inspect +
HTTP probe); the container was not restarted.

Dashboard verification: the Health tab is served by the existing frontend and the backing API
returns the real persisted data above (`GET …/health` → 200, real checks/latency/state).
Automated in-browser DOM verification was **not** performed (it requires the operator's
authenticated dashboard session); this is reported as a limitation, not a mocked result.

### 5.5 Stage C — manual recovery activation (executed)

Pre-activation: API `/health` 200; exactly one API + one worker process; recovery schema ready
(`_prisma_migrations` `finished=t`, `rolled_back=null`); no build/runtime leases, no active
recovery; stale DEPLOYING `23aede51…` untouched. Running-runtime eligibility: only the demo
(`8c47b816…`, HEALTHY) — not eligible; no unexpected runtime eligible.

Configuration (`.env` backed up to `…\backups\.env.pre-stageC-20261128-124138.bak`, outside the
repo). `MANUAL_RECOVERY_ENABLED` changed `false → true` in place (single definition). Final flags:
persistence `true`, monitoring `true`, manual `true`, auto `false`. Effective config verified in
both processes: worker manual `true`, API manual `true`, auto `false` everywhere.

Restart (both required; PostgreSQL/Redis/demo untouched): API `npm run dev -w @cloudpilot/api`
→ started `127.0.0.1:8000`; worker `npm run dev -w @cloudpilot/worker` → `worker-5b106bb4…`,
`Deployment health monitor … event:started`, `startup completed` (dockerBuild/containerRuntime/
deploymentAvailable true). Recovery consumer is the worker's runtime-queue consumer (active;
`bull:runtime:*` empty because no jobs are enqueued merely by enabling the feature).

Verification:

- Health endpoint (owner) → `200`, `schemaAvailable=true`, `runtimeStatus=RUNNING`, 36 checks.
- Recovery availability (authorized owner) → `{allowed:false, reason:"NOT_UNHEALTHY"}` — the
  healthy demo is **not eligible**; no recovery submitted against the demo.
- `deployment_recovery_attempts` = `0` (unchanged) — enabling the feature created no attempts/jobs.
- Health monitoring continuity: `deployment_health_checks` 12 → 35, latest `07:12:39`, state
  `HEALTHY` gen `0`, `nextCheckAt` advancing (~16s interval).
- No MEMBER fixture exists in this workspace; the member-denied (403) path is covered by the
  existing API integration tests (`canManage`).
- Demo preserved: Id `f9144585…`, Image `sha256:15b896427fd3…`, `StartedAt 2026-11-27T16:36:43Z`,
  `127.0.0.1:53143` (unchanged).

### 5.6 Disposable validation deployment plan (not executed)

Goal: a dedicated deployment (separate project + deployment identity, trusted repo, dedicated
container, stable host port, controllable HTTP health, deterministic unhealthy→healthy-after-restart)
with no shared mutable state with the demo.

Host-port stability (inspected): the runtime provisioning request carries a requested
`hostPort` where `0` means Docker-managed **dynamic** allocation
(`apps/worker/src/runtime/runtime-executor.ts:111`, `docker-runtime-client.ts:2039`). Recovery
fences on port identity: `recovery-operations.ts:41` returns `RECOVERY_PORT_CHANGED` if the
post-restart `hostPort` differs from the recorded `runtime.port`; the health probe also requires
`inspection.hostPort === runtime.port` (`probe-runtime.ts:42,78`). Therefore the validation
deployment **must request a fixed, non-zero host port** (not `0`) so the port is stable across
restart; dynamic allocation would break recovery. Port-safety checks (`assertValidContainerPort`,
`isValidPort`, loopback `RUNTIME_BIND_HOST` assertion) must remain in force.

Planned (not created): new project + deployment under the trusted repo
`raziquehasan/cloudpilot-demo-app` (or another approved trusted repo), a fixed loopback host port
(e.g. a dedicated `127.0.0.1:PORT` distinct from the demo's `53143`), an app exposing a
controllable health endpoint (e.g. `/healthz` toggled via a file/flag inside the container), and a
restart that preserves container/image identity and host port. No changes to the demo, recovery
policies, or port-safety code. Creation/deploy awaits separate approval.

### 5.7 Disposable validation deployment — BLOCKED (preflight + blocker report)

Preflight (read-only): API `/health` 200; flags unchanged (persistence/monitoring/manual `true`,
auto `false`, trusted repo `raziquehasan/cloudpilot-demo-app`); exactly one API + one worker; no
build/runtime leases, no active recovery; stale DEPLOYING `23aede51…` untouched; demo unchanged
(runtime `8c47b816…`, container `f9144585…`, image `sha256:15b896427fd3…`, `127.0.0.1:53143`,
HEALTHY gen 0, 0 failures, 0 recovery attempts).

Proposed validation app (not created): minimal Node HTTP server, `GET /healthz` → 200 healthy /
503 unhealthy, in-memory failure flag toggled by a local-only control (e.g. `POST /__fail` bound
to loopback, or a file touch inside the container), resets to healthy on process restart, no shared
state with the demo, no external services.

**Blocker 1 — fixed host port provisioning is unsupported.** The runtime provision path hardcodes
`hostPort: 0` (dynamic) at `apps/worker/src/processors/runtime.processor.ts:1029`; there is no
config/env to request a fixed port (grep for `RUNTIME_HOST_PORT`/`FIXED_PORT`/`hostPort: [1-9]` →
none). So a *fixed* host port cannot be requested through the normal workflow.

Mitigating fact: recovery restarts the **same** container by id
(`executor.restartOwnedContainer`, `recovery-operations.ts:56`), and `docker restart` preserves
the published port mapping. The dynamic port is therefore **stable across the recovery restart**,
and the `RECOVERY_PORT_CHANGED` fence (`recovery-operations.ts:41`) will not fire. So a fixed port
is not strictly required for recovery to succeed.

**Blocker 2 — validation repo is not trusted and cannot be created here.** The app must be cloned
from a trusted GitHub repo; `BUILD_TRUSTED_REPOSITORIES` contains only
`raziquehasan/cloudpilot-demo-app`. A new repo `raziquehasan/cloudpilot-recovery-validation` would
require (a) a real, clonable GitHub repo with the app (cannot be created/pushed from here) and
(b) a `.env` trusted-repo addition.

**Exact proposed changes (require approval before proceeding):**

1. Core runtime (only if a fixed port is still mandated): thread a validated `requestedHostPort`
   (default `0` = dynamic, backward-compatible) from the deployment/build configuration through
   the runtime job payload to `runtime.processor.ts:1029` (`hostPort: requestedHostPort`), with a
   pre-provision availability check and existing `isValidPort`/loopback assertions intact.
2. Trusted repo: add `raziquehasan/cloudpilot-recovery-validation` to
   `BUILD_TRUSTED_REPOSITORIES` in `.env` (additive; does not weaken restrictions).
3. Provide the validation app in that GitHub repo (operator action).

No deployment, DB record, code, or `.env` change was made. Awaiting approval for the above.

### 5.8 Fixed host port support + validation app (implemented, not published)

Fixed host port provisioning is now supported (backward-compatible, no DB migration):

- `packages/queue/src/contracts/runtime.ts`: added optional `requestedHostPort`
  (integer 0–65535; `0`/absent = dynamic, preserving existing behavior) to the strict
  runtime job schema. Rebuilt `@cloudpilot/queue` (`npm run build -w @cloudpilot/queue`).
- `apps/api/src/services/deployment-outbox.service.ts`: the deploy job now reads
  `requestedHostPort` from the deployment's existing `buildConfigurationSnapshot` JSON
  (no schema change) and includes it in the payload; absent/invalid → `0`.
- `apps/worker/src/runtime/runtime-config.ts`: added `assertValidRequestedHostPort`
  (0 allowed; non-zero must be an integer in the non-privileged range 1024–65535).
- `apps/worker/src/processors/runtime.processor.ts`: validates the requested port, honors it
  **only** when `RUNTIME_TRUSTED_LOCAL_ONLY` and the deployment's repository is in
  `BUILD_TRUSTED_REPOSITORIES` (else forces `0`), and passes it to `createContainer`
  (`hostPort: requestedHostPort`). Docker bind failure remains authoritative; loopback bind,
  ownership, identity, and `RECOVERY_PORT_CHANGED` fencing are unchanged.

Why dynamic allocation is unsuitable: Phase 5.7.5 observed `RECOVERY_PORT_CHANGED` with a
dynamic port; a fixed port made the same recovery succeed. The validation therefore requires a
fixed port.

Targeted tests (`apps/worker/test/requested-host-port.test.ts`): **2 passed, 0 failed** —
validator accepts 0/1024/3000/65535 and rejects 80/65536/-1/1.5/"3000"; job schema leaves the
field optional and rejects out-of-range/non-integer. Typecheck: worker **pass**, API **pass**.

Validation app prepared locally (outside the repo) at
`C:\Users\asus\OneDrive\Desktop\CloudPilot\recovery-validation`: `package.json`, `server.js`
(GET `/health` 200/503, loopback-only `POST /__fail`, in-memory state, resets on restart),
`Dockerfile` (node:20-alpine, non-root `node` user, port 3000), `.dockerignore`, `.gitignore`,
`README.md`. Local smoke test: `/health` 200 → `POST /__fail` 200 → `/health` 503.

Not yet done (awaiting operator): create/push the GitHub repo
`raziquehasan/cloudpilot-recovery-validation`, add it to `BUILD_TRUSTED_REPOSITORIES`, restart
services, then create the validation deployment with a fixed `requestedHostPort` in its build
configuration snapshot.

### 5.9 Authorized deployment-level fixed host port (implemented, not deployed)

The deployment API now accepts an optional fixed host port so the disposable validation deployment
can be created through the normal authorized workflow. No DB migration and no frontend change.

Schema (`apps/api/src/validators/deployment.validator.ts`):

- `createDeploymentSchema` gains optional `requestedHostPort` validated by `requestedHostPortSchema`
  (`0` = dynamic, or an integer in `1024–65535`; `1–1023`, fractional, negative, non-number and
  out-of-range values are rejected; `.strict()` still rejects unknown keys).
- `buildConfigurationSchema` is intentionally **not** extended: it maps to persisted
  `ProjectBuildConfiguration` columns, so a new field would require a migration (forbidden). The
  fixed port is a per-deployment property carried in the deployment's `buildConfigurationSnapshot`
  JSON, set only by the authorized deployment API.

Authorization (`apps/api/src/services/deployment-host-port.ts`, `isFixedPortAuthorized`):

- A non-zero fixed port is honored only when the actor is `OWNER`/`ADMIN`, trusted local runtime
  mode is enabled (`RUNTIME_TRUSTED_LOCAL_ONLY`, default `true`), and the resolved repository
  `fullName` is in `BUILD_TRUSTED_REPOSITORIES`. Otherwise the API returns
  `403 FIXED_PORT_NOT_AUTHORIZED`.
- Dynamic allocation (`0`/omitted) is always permitted under the existing `canManage` gate.
- Existing authentication (`requireAuth`), CSRF (`requireCsrf`), workspace isolation
  (`resolveProject`) and repository authorization (installation `ACTIVE`, immutable commit
  resolution) are unchanged and still enforced before any port handling.

API env (`apps/api/src/config/env.ts`): added `RUNTIME_TRUSTED_LOCAL_ONLY` (boolean, default
`true`) and `BUILD_TRUSTED_REPOSITORIES` (CSV, default empty) mirroring the worker so the API can
authorize before persisting. The worker independently re-validates and enforces trust.

Persistence + propagation:

- `createDeployment` passes `buildConfigurationSnapshot: { requestedHostPort }` to
  `createDeploymentWithOutbox` only when the field is provided; existing/omitted deployments keep a
  `null` snapshot (dynamic).
- `deployment-outbox.service.ts` forwards the persisted value via the shared
  `normalizeRequestedHostPort` (unchanged fixed port, `0` for absent/invalid) into the runtime job
  payload — no silent dynamic fallback; an occupied fixed port fails authoritatively in the worker.

Idempotency (`apps/api/src/services/deployment-identity.ts`, `matchesDeploymentRequest`):

- The requested port is part of the request identity. Reusing an `Idempotency-Key` with a different
  `requestedHostPort` returns `409 IDEMPOTENCY_KEY_REUSED` (checked on the initial lookup, inside the
  transaction, and on `P2002` recovery).

Retry/redeploy safety: the outbox retry re-reads the persisted snapshot each attempt, so the fixed
port is never lost; there is no separate redeploy endpoint that creates deployments, so the single
authorized `createDeployment` path cannot be bypassed.

Tests (`apps/api/test/deployment-host-port.test.ts`): **18 passed, 0 failed** — omitted/`0`, valid
`63299`, bounds `1024`/`65535`, invalid/fractional/out-of-range/non-number rejection, strict unknown
key, MEMBER/untrusted-repo/disabled-local rejection, dynamic always allowed, snapshot→queue
propagation with no silent fallback, idempotency conflict for different ports, and existing-deployment
compatibility. Typecheck: API **pass**, worker **pass**. Not committed or pushed; validation project
not created or deployed.

## 6. Recovery identifiers and state transitions (planned; actuals recorded post-approval)

Target: a disposable deployment (not the demo). Expected transition:
`REQUESTED → RUNNING → SUCCEEDED`, with a new health `generation` reaching `HEALTHY`,
same `containerId`, `imageId`, and `hostPort` after restart. Recovery id, idempotency key,
BullMQ job id, and before/after health generation will be recorded here after execution.

## 7. Remaining limitations

- Recovery migration not yet applied; `recoverySchemaReady()` is false.
- Health monitoring/persistence/recovery flags all false; no scheduler or recovery processor active.
- Host lacks `psql`/`pg_dump`/`redis-cli`; DB/Redis operations go through containers.
- Production monitoring is global (trusted-repo scoped), not per-deployment.
- The demo deployment already has health rows; enabling monitoring will also probe it (read-only).

## 8. Deactivation procedure

Set `HEALTH_MONITORING_ENABLED=false`, `MANUAL_RECOVERY_ENABLED=false`,
`HEALTH_PERSISTENCE_ENABLED=false`, `AUTO_RECOVERY_ENABLED=false` and restart worker + API.
No schema rollback is required to disable; the additive migration can remain applied.
Cleanup of the disposable validation deployment affects only that deployment's records.

## 9. Readiness assessment for Phase 5.8

Not yet ready. Blocked on the disposable validation deployment (§5.7): fixed-port provisioning is
unsupported (dynamic port is stable across recovery, so not strictly required) and the validation
repo is neither trusted nor creatable here. Migration (§5.1) and Stages A/B/C (§5.3–§5.5) are
complete and verified; manual recovery is enabled and the healthy demo is correctly `NOT_UNHEALTHY`.
After approval of the §5.7 changes (trusted repo + validation app, and optionally fixed-port support)
and a real recovery reaching `SUCCEEDED` with a new HEALTHY generation, Phase 5.7.7 is complete and
Phase 5.8 may begin.
## Final end-to-end manual recovery validation

Date: 2026-11-28
Result: PASSED

### Validation deployment

- Repository: raziquehasan/cloudpilot-recovery-validation
- Branch: main
- Commit: 34d9c8e
- Project ID: 92965047-d6d9-4512-81f6-3c8286240fa5
- Deployment ID: 31198994-8d7d-4c70-81d5-af2a0c5c2839
- Runtime ID: 8a842ccb-ffd3-4a6b-95ff-e625c9eee724
- Container ID: 64376e6449f1
- Fixed host port: 63299
- Container port: 3000

### Recovery audit

- Recovery ID: 5edf953f-31ec-4e61-ac20-a35eb6eba1d6
- Action: RESTART
- Trigger: MANUAL
- Status: SUCCEEDED
- Attempt count: 1
- Recovery duration: 13.186 seconds
- Health generation: 0 -> 1
- Health version at reservation: 23
- HTTP status: 503 -> 200
- Durable restart intent: 2026-11-28 08:30:11.748 UTC
- Previous StartedAt: 2026-11-28T08:24:28.723658833Z
- Recovered StartedAt: 2026-11-28T08:30:23.100977192Z
- Recovery lease released successfully.
- No recovery failure reason recorded.

### Verified safety guarantees

- Container identity preserved.
- Docker image identity preserved.
- Fixed loopback host port preserved.
- Existing demo container was not restarted or replaced.
- Existing demo continued returning HTTP 200.
- PostgreSQL and Redis remained healthy.
- Scheduled monitoring continued after recovery.
- Recovery was initiated through the authenticated dashboard.
- No direct Docker restart was used.

### Active local feature configuration

- HEALTH_PERSISTENCE_ENABLED=true
- HEALTH_MONITORING_ENABLED=true
- MANUAL_RECOVERY_ENABLED=true
- AUTO_RECOVERY_ENABLED=false

Feature defaults remain disabled in source configuration.

### Known limitations

- Backend does not report effective worker monitoring-enabled status.
- Some dashboard chart tooltip metadata remains unavailable.
- Automatic recovery remains disabled.
- Backup restore procedure has not been exercised.
- Validation covers the controlled local Docker environment.

The controlled end-to-end manual recovery validation passed.
