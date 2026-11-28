# Phase 5.7.6 deployment health and manual recovery

The existing deployment detail view at `/dashboard/logs?project=…&deployment=…` now includes a Health tab. Use `tab=health` for a direct link. The Logs tab continues to use the existing authenticated Socket.IO stream. Health and recovery use TanStack Query with workspace, project, and deployment scope.

## API

- `GET /api/v1/projects/:projectId/deployments/:deploymentId/health` is a read-only, authenticated workspace endpoint. It returns the latest runtime's health state, up to 50 checks across the runtime's generations, and advisory recovery availability. It returns an unavailable state when the health schema is absent.
- `GET /api/v1/projects/:projectId/deployments/:deploymentId/recoveries` lists the existing bounded recovery history. Append `/:recoveryId` to read an attempt.
- `POST /api/v1/projects/:projectId/deployments/:deploymentId/recoveries` uses the existing owner/admin authorization, CSRF protection, UUID `Idempotency-Key`, and server-side recovery rules. The confirmation step precedes submission. A retry of an ambiguous result reuses its original key. The UI only reports success after the backend returns `SUCCEEDED`.

The read endpoint reports the API's manual recovery setting and checks the schema, role, runtime, active operation, cooldown, and budget. This is advisory. The POST endpoint remains authoritative, and a worker must independently be configured and available to execute a request. The current worker heartbeat does not report whether monitoring is enabled. A saved due time is shown as a scheduled time and is not proof that polling is active. Missing timestamps and measurements remain unavailable.

## Verification results (Phase 5.7.6)

### Task 1 — Health API tests

Command (from `apps/api`):

```
node --import tsx --test test/deployment-api.test.ts
```

Result: **2 passed, 0 failed, 0 skipped** (`# pass 2 / # fail 0 / # skipped 0`).

`apps/api/test/deployment-api.test.ts` now contains a second test,
`deployment health endpoint enforces access, scope, read-only behavior, and recovery availability`,
reusing the existing in-file server/session/fixture helpers. It creates isolated
fixtures (owner, member, workspace, project, deployment, and a foreign
workspace/project) and removes them in `finally`; the primary database is never
modified destructively.

Assertions that run against the configured (non-isolated) database:

- Unauthenticated request returns `401`.
- Cross-workspace access returns `404`.
- Invalid deployment/project combinations return `404`.
- An authorized workspace member can read health (`200`, `schemaAvailable` boolean, `checks` array, `recovery.allowed` boolean).
- `GET` does not mutate database state (health states, checks, and recovery attempts counts are identical before and after).
- A non-owner/admin member cannot initiate recovery (`POST …/recoveries` returns `403`).
- Manual recovery disabled state is respected (`recovery.reason === "DISABLED"`, `allowed === false`) when the isolated recovery integration is not enabled.

Assertions gated behind the isolated recovery integration
(`MANUAL_RECOVERY_ENABLED=true`, `DATABASE_URL` path `/cloudpilot_health_test_574`,
and `CLOUDPILOT_RECOVERY_TEST_REDIS === REDIS_URL`), which were **skipped** in
this run because that environment was not provisioned:

- Only the current runtime's checks are returned (two runtime attempts; the previous attempt's check never surfaces).
- Health history is limited to 50 records (60 checks created, 50 returned).
- Recovery availability reflects `ACTIVE`, `COOLDOWN`, `EXHAUSTED`, and `NOT_UNHEALTHY` using real fixtures and `recoveryPolicy`.

### Task 2 — Mocked recovery UI verification

No component/DOM test harness is configured in `apps/web` (no Vitest/Jest/jsdom),
and the dashboard was not rebuilt. Verification was performed by code review of
`apps/web/components/dashboard/deployment-health.tsx` against each item, plus the
existing data-layer unit tests.

Command (from `apps/web`):

```
node --import tsx --test test/deployment-health.test.ts
```

Result: **2 passed, 0 failed, 0 skipped**.

Checklist mapped to `deployment-health.tsx`:

- Confirmation dialog opens correctly — `Dialog`/`DialogTrigger`/`DialogContent` (lines 317–333).
- Cancellation sends no POST — Cancel is a `DialogClose` (line 329); `submit()` is wired only to the confirm button (line 330).
- Confirmation submits exactly one request — `submit()` calls `mutation.mutateAsync` once (line 280), guarded by `sending.current` (lines 217, 274).
- Duplicate submissions prevented — `sending.current` guard plus `canSubmit` includes `!mutation.isPending` (lines 262–263, 274); buttons disabled via `disabled={!canSubmit}` (lines 319, 330).
- UUID Idempotency-Key included — `crypto.randomUUID()` (line 278) sent as `Idempotency-Key` header (line 256); stored key validated as UUID (lines 197–202).
- Ambiguous retries reuse the original key — key retained when `failure.ambiguous` (line 288), `retryable` (lines 251–252), reused at line 278.
- Recovery status refreshes correctly — `refetchInterval` (lines 234–243) and `invalidateQueries` (line 293).
- Success shown only after `SUCCEEDED` — status message gated on `tracked.status === "SUCCEEDED"` (lines 358–362).
- Failed recovery shows a safe message — `Failure`/`failureMessage` (lines 43–48, 361) and `recoverySubmissionError` (data module).
- Unauthorized/disabled controls cannot submit — `roleAllowsRecovery` (line 250), `canSubmit` (lines 262–263), `blockedReason` (lines 301–304).
- Mobile layout remains usable — card list vs. table switch `lg:hidden` / `hidden lg:block` (lines 123, 137) and responsive grids.

### Task 3 — Unrelated frontend lint issues (documented, not suppressed)

Command (from `apps/web`):

```
npx eslint app/dashboard/logs/log-terminal.tsx app/dashboard/deployments/deployments-client.tsx
```

Result: **1 error, 2 warnings** (pre-existing, unrelated to Phase 5.7.6; no ESLint
rules were suppressed and no Socket.IO logic was refactored):

- `app/dashboard/logs/log-terminal.tsx:961:5` — `react-hooks/set-state-in-effect` (error): synchronous `setConnection("connecting")` inside an effect.
- `app/dashboard/deployments/deployments-client.tsx:2375:30` — `react-hooks/exhaustive-deps` (warning): `actionControllersRef.current` used in effect cleanup.
- `app/dashboard/deployments/deployments-client.tsx:2381:22` — `react-hooks/exhaustive-deps` (warning): `actionLocksRef.current` used in effect cleanup.

## Phase 5.7.7 prerequisites

Apply and verify the pending recovery migration only after approval. Confirm the API and all runtime workers run compatible builds, then explicitly verify the scoped persistence, monitoring, and manual recovery settings before controlled activation. Keep automatic recovery and rollback disabled. Verify a disposable or dedicated unhealthy deployment through the dashboard before making recovery available to operators.

## Remaining limitations

- The deep recovery-availability assertions (current-runtime isolation, 50-record cap, `ACTIVE`/`COOLDOWN`/`EXHAUSTED`/`NOT_UNHEALTHY`) are present but require the isolated recovery database/Redis environment to execute; they were skipped in this run.
- No automated DOM/component test harness exists for the recovery UI; Task 2 was verified by code review plus data-layer unit tests.
### Final isolated API integration verification

Date: 2026-11-28

- Deployment API integration test: PASS.
- Deployment health API integration test: PASS.
- Deep recovery availability assertions executed using isolated PostgreSQL and Redis.
- Runtime isolation, history limit, active recovery, cooldown,
  exhausted budget and healthy runtime protection verified.
- Corrected test fixtures to satisfy health-state foreign keys,
  recovery attempt counts and lifecycle constraints.
- No production database migration or feature activation performed.
- Automated DOM tests and live dashboard recovery remain unverified.
- Pre-existing frontend lint issues remain documented.
