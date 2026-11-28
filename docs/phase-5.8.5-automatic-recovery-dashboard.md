# Phase 5.8.5 Automatic Recovery Dashboard

The existing deployment **Health** tab provides a read-only view of automatic recovery. Application health, latency, check history, manual restart and recovery details remain in the same tab.

## Health API

Authenticated GET /api/v1/projects/:projectId/deployments/:deploymentId/health now returns data.automaticRecovery.

| Fields | Meaning |
| --- | --- |
| configured, workerAvailability | API configuration and effective worker availability. Workers can report DISABLED, AVAILABLE, UNAVAILABLE, STALE, UNKNOWN, or CONFLICTING. |
| circuitState, eligible, rejectionReason | Persisted circuit state and advisory deployment eligibility. A null circuit state means the API could not establish it. |
| remainingBudget, maxBudget, consecutiveFailures | Shared manual and automatic budget and durable recovery failures. Zero is returned only when established from available data. |
| cooldownRemainingMs, nextEligibleAt | PostgreSQL-clock cooldown and the circuit's earliest eligible time, subject to all other admission checks. |
| manualInterventionRequired, latestOutcome, snapshotAt | Circuit intervention requirement, most recent finished recovery with its trigger and failure code, and PostgreSQL snapshot time. |

The snapshot uses a read-only, repeatable-read PostgreSQL transaction and the shared circuit evaluator. Companion reads use the deployment, project and workspace scope. Missing runtime, health state or schema produces unavailable values. A circuit read failure leaves the existing health response usable. Reading health neither creates recovery work nor cleans the Redis heartbeat index. The snapshot is advisory. The worker still fences and revalidates every restart.

## Workers and permissions

The optional deployment heartbeat capability reports configured and effective automatic recovery, with the worker's trusted repository scope used internally for the named deployment. A legacy heartbeat is accepted and has unknown automatic recovery capability. Mixed reports that disagree on configuration, readiness or trust for this deployment are reported as conflicting. A heartbeat can become stale after three missed intervals within the registry TTL. An unreachable registry is unavailable. Updated API readers should be rolled out before updated workers because older readers strictly validate their known heartbeat fields.

Workspace members can read scoped data. Only owners and admins can initiate manual recovery. The existing CSRF token, confirmation dialog, server validation and idempotency key remain in use. Automatic configuration has no activation control.

## Dashboard

The recovery section shows configuration, worker availability, circuit state, restart budget, cooldown, eligibility, intervention and latest recorded outcome. The countdown uses elapsed time against the server-provided duration, and a stale snapshot is identified when refresh fails or the data exceeds its freshness window. Existing scoped TanStack Query keys are retained. Recovery history distinguishes manual and automatic triggers with status, timestamps, duration, generation and safe failure details.

## Verification and activation

Use disposable PostgreSQL and Redis for integration testing. The focused tests exercise worker availability, scoped access, owner/admin/member permissions, read-only health snapshots, circuit states, budget, cooldown and recovery history. The backend and circuit suite passed 25 tests, and the frontend suite passed 10 tests, with zero failures or skips. Shared package builds, API/worker/web TypeScript checks, frontend lint, and the Next.js production build passed. Chromium verified the sign-in redirect at 390 x 844 and 1280 x 800. Authenticated responsive layout remains unverified because no signed-in browser session was available. The disposable PostgreSQL and Redis containers and their volumes were removed after testing.

Automatic recovery stays disabled. Phase 5.8.6 still requires live Redis interruption during restart, live PostgreSQL interruption before finalization, real worker termination during a Docker operation and real end-to-end automatic Docker recovery. Docker restarts have no exactly-once guarantee.

## Authenticated browser verification

Verified on November 28, 2026 using the running local CloudPilot dashboard.

- Desktop health overview and automatic recovery snapshot rendered successfully.
- Automatic recovery configuration and worker capability displayed Disabled.
- Circuit state CLOSED, restart budget 3/3, zero consecutive failed recoveries, and no cooldown.
- Health monitoring active, HTTP 200, and health generation 1.
- Response latency chart and health check history rendered successfully.
- Existing manual recovery history displayed SUCCEEDED, one execution attempt, generation 0, and duration 13.2 seconds.
- Mobile Chromium emulation at 412x924 verified navigation, health overview, buttons, text wrapping, and the visible portion of the automatic recovery card.
- Full mobile recovery history and remaining automatic recovery fields were not visually verified.
- Protected demo and validation runtime health endpoints returned HTTP 200.
- Automatic recovery remains disabled.
- Live automatic recovery activation remains gated by Phase 5.8.6.
