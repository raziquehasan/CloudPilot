# Phase 5.7.8 — Monitoring Observability & UI Hardening

Baseline: `main` @ `812abbd0bfcbe708f55d07f8db6949aa6421dee7` (clean tree).
Phase 5.7.7 completed; real Docker manual recovery passed in 13.186 s with container
identity, image identity, fixed host port and the existing demo runtime preserved.

No database migration was required. No feature flags were changed. The demo and
validation containers were not restarted or modified. No recovery requests were
submitted. Nothing was committed or pushed.

## 1. Worker monitoring observability

### Architecture (inspected)
- Workers publish a Redis heartbeat (`packages/queue/src/workers/worker-registry.ts`)
  with a strict `capabilities` object. Heartbeats are advisory availability only;
  they never authorize execution.
- The API reads heartbeats via `listWorkerHeartbeats` in
  `deployment-readiness.service.ts` and (new) the per-deployment health endpoint.
- The worker starts the health monitor only when
  `runtimeExecutor && HEALTH_MONITORING_ENABLED` (`worker.ts`), and the monitor's
  own effective enablement is
  `HEALTH_MONITORING_ENABLED && HEALTH_PERSISTENCE_ENABLED && RUNTIME_ENABLED &&
  RUNTIME_TRUSTED_LOCAL_ONLY` (`health-monitor-runtime.ts`).

### Changes
- `packages/queue/src/workers/worker-registry.ts`: added optional
  `capabilities.healthMonitoring` (boolean) for backward compatibility. Older
  workers that omit it parse unchanged; absence is treated as "unknown".
- `apps/worker/src/worker.ts`: the deployment heartbeat now reports
  `healthMonitoring` = deployment role AND the full effective configuration
  (monitoring + persistence + runtime + trusted local) AND the monitor actually
  started (`healthMonitor !== null`). This is the worker's own authoritative
  report.
- `apps/api/src/services/monitoring-report.ts` (new, pure): `deriveMonitoringReport`
  maps worker heartbeats to one of five states using the reported capability and
  heartbeat freshness only:
  - `UNAVAILABLE` — Redis unreachable or no deployment workers.
  - `UNKNOWN` — workers exist but none report the capability (old workers).
  - `STALE` — workers report but none are fresh/available (draining or old).
  - `ENABLED` — a fresh, available worker reports monitoring active.
  - `DISABLED` — fresh, available workers report monitoring inactive.
- `apps/api/src/services/monitoring-observability.ts` (new):
  `readMonitoringReport()` reads live deployment heartbeats and derives the report;
  it never throws (Redis/registry failure → `UNAVAILABLE`).
- `deployment-readiness.service.ts`: readiness now includes a `monitoring` report
  derived from the already-fetched deployment workers (no extra Redis round trip).
- `deployment-recovery.controller.ts`: the per-deployment `/health` response now
  includes `monitoring`.

Design decision: availability is derived from worker-reported capability +
heartbeat freshness, never from API environment variables or `nextCheckAt`
scheduled timestamps. Multiple workers are aggregated (any enabled ⇒ ENABLED);
backward compatibility is preserved via the optional capability (⇒ UNKNOWN).

## 2. Health dashboard improvements
- `deployment-health-data.ts`: added `MonitoringStatus`/`MonitoringReport` types,
  `monitoringMessage()` (accurate backend-reported text per state), and
  `formatChartTooltipTime()`.
- `deployment-health.tsx`:
  - Replaced the generic "The backend does not report whether monitoring is
    enabled." with `monitoringMessage(summary.monitoring)`.
  - Fixed the latency chart tooltip: `labelFormatter` previously received the
    series config label ("Passed") and rendered a misleading "Unavailable"
    header; it now reads the numeric timestamp from the payload row via
    `formatChartTooltipTime`.
  - Added a stale hint when the latest check is older than 5 minutes (using
    React Query `dataUpdatedAt`, not `Date.now()`, to keep render pure).
  - Loading/empty/unavailable states and authenticated query scoping are
    preserved; responsive layout unchanged.
- Historical health checks across runtime generations are preserved: the backend
  returns all checks for the runtime (all generations) and the UI renders them.

## 3. Recovery history improvements
- The recovery list and detail dialog already display status, action, trigger,
  generation, requested/started/finished timestamps, duration and failure reason
  (`RecoveryFields`). Backend-authoritative eligibility (`recovery.reason`) is
  preserved; the success banner is driven strictly by `status === "SUCCEEDED"`.
- Idempotency (pending key reuse) and all recovery safety guarantees are
  unchanged. No recovery requests were submitted during this phase.

## 4. Fixed-port hardening
- Reviewed normalization across API validation
  (`requestedHostPortSchema`: 0 or 1024–65535), snapshot
  (`normalizeRequestedHostPort`), queue contract (optional 0–65535) and worker
  (`assertValidRequestedHostPort`: 0 or 1024–65535).
- `normalizeRequestedHostPort` now rejects privileged (1–1023), negative,
  fractional and out-of-range values (→ 0 dynamic), matching the validator and
  the worker. An invalid/privileged port can no longer be forwarded to the worker.
- Dynamic allocation remains the default; trusted-local + trusted-repository
  authorization is unchanged.

## 5. Frontend lint cleanup
- `log-terminal.tsx`: moved the synchronous `setConnection("connecting")` /
  `setError("")` resets out of the effect body into the async `initialize()`
  (fixes `react-hooks/set-state-in-effect`).
- `deployments-client.tsx`: captured `actionControllersRef.current` and
  `actionLocksRef.current` into locals inside the effect and used them in the
  cleanup (fixes the two `react-hooks/exhaustive-deps` ref-cleanup warnings).
- No lint rules were suppressed; no unrelated refactoring.

## 6. Verification (actual results)
- API tests (`monitoring-report.test.ts`, `deployment-host-port.test.ts`):
  **27 pass, 0 fail**.
- Web tests (`deployment-health.test.ts`): **5 pass, 0 fail**.
- Existing API test (`project-intelligence.test.ts`): **4 pass, 0 fail**.
- Worker fixed-port test (`requested-host-port.test.ts`): **2 pass, 0 fail**.
- TypeScript: API **pass**, worker **pass**, web **pass**.
- Frontend lint (`eslint`): **clean (0 problems)**.
- Frontend production build (`next build`): **success** (19 routes generated).

## 7. Remaining limitations
- Monitoring status is global (worker-level) and is surfaced in the per-deployment
  health payload; it is not per-deployment.
- `STALE` uses the heartbeat TTL as the freshness boundary; a present key older
  than the TTL cannot occur, so `STALE` effectively triggers near expiry / when
  workers are draining.
- No automated browser/E2E test was added; tooltip and state logic are covered by
  unit tests over the pure helpers.
- The monitoring capability is additive; very old workers report `UNKNOWN` until
  upgraded.

## 8. Git status
Modified: `apps/api/src/controllers/deployment-recovery.controller.ts`,
`apps/api/src/services/deployment-host-port.ts`,
`apps/api/src/services/deployment-readiness.service.ts`,
`apps/api/test/deployment-host-port.test.ts`,
`apps/web/app/dashboard/deployments/deployments-client.tsx`,
`apps/web/app/dashboard/logs/log-terminal.tsx`,
`apps/web/components/dashboard/deployment-health-data.ts`,
`apps/web/components/dashboard/deployment-health.tsx`,
`apps/web/test/deployment-health.test.ts`, `apps/worker/src/worker.ts`,
`packages/queue/src/workers/worker-registry.ts`.
New: `apps/api/src/services/monitoring-observability.ts`,
`apps/api/src/services/monitoring-report.ts`,
`apps/api/test/monitoring-report.test.ts`,
`docs/phase-5.7.8-monitoring-observability.md`.
Not committed or pushed (awaiting approval).
## Live verification - November 28, 2026

- Queue package rebuilt successfully.
- API and worker restarted successfully.
- Duplicate old worker process tree removed.
- Single active worker verified.
- PostgreSQL and Redis remained healthy.
- Existing demo and validation containers preserved.
- Dashboard reported health monitoring active on a connected worker.
- Validation runtime remained healthy with HTTP 200 and generation 1.
- Consecutive failures remained zero.
- Next health check scheduled after 15 seconds.
- Chart tooltip displayed a valid timestamp and latency value.
- Existing manual recovery details displayed Succeeded, Restart,
  Manual trigger, generation 0, and duration 13.2 seconds.
- Healthy runtime restart action remained disabled.
- No new recovery operation or failure injection was performed.

### Known limitations

- Monitoring availability represents connected worker capability.
- Live stale-heartbeat scenarios were not exercised.
- An intermittent health-check scheduling gap was observed;
  its cause was not established.
- Chart tooltip latency value does not explicitly include the ms suffix.
- No production readiness claim is made.
