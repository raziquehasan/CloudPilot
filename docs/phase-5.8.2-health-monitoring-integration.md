# Phase 5.8.2 — Read-Only Automatic Recovery Candidate Discovery & Scheduler Integration

## 1. Phase objectives
Introduce a **bounded, strictly read-only** stage that, after each successful health
monitoring batch, discovers unhealthy runtimes that *might* be eligible for automatic
recovery and evaluates them against the existing safety policy. This stage **never**
creates recovery attempts, enqueues jobs, invokes Docker, or mutates any row. It only
emits advisory diagnostics. Automatic recovery itself remains **disabled**
(`AUTO_RECOVERY_ENABLED=false`).

## 2. Baseline
- Branch `main`, building on Phase 5.8.1 (`auto-recovery-policy.ts`) and the
  5.7.7–5.7.9 hardening work. No commit/push performed in this subphase.

## 3. New module
- `apps/worker/src/health/auto-recovery-candidates.ts`
  - `discoverAutoRecoveryCandidates(limit)` — single `$queryRaw` (no N+1) over
    `deployment_health_states` joined to runtime instances, deployments and
    repositories. Pre-filters to: `UNHEALTHY`, `consecutiveFailures >= limit`,
    fresh `lastCheckedAt` (`checkIntervalMs*2`), deployment+runtime `RUNNING`,
    trusted repository (`BUILD_TRUSTED_REPOSITORIES` + `RUNTIME_TRUSTED_LOCAL_ONLY`),
    loopback host binding, valid container/image identity, no active
    (`REQUESTED`/`RUNNING`) recovery, and no live execution lease. Deterministic
    `ORDER BY`, `LIMIT 1..128`. Returns `[]` when trusted-local is off.
  - `evaluateCandidateRows(rows, options?)` — pure evaluator delegating to
    `evaluateAutoRecovery` (5.8.1). Uses the database `now` from each row so
    freshness/cooldown share the PostgreSQL clock. Aggregates an `EvaluationBatch`
    (`evaluated`/`eligible`/`rejected`/`reasons`). Feature inputs are overridable for
    deterministic tests; defaults come from `workerEnv`.

## 4. Scheduler integration
- `health-monitor.ts`: added optional `afterBatch` hook (runs only after a successful,
  non-aborted batch) and an `auto_recovery` emit event. Hook failures are caught and
  reported as diagnostics without affecting health persistence.
- `health-monitor-runtime.ts`: `afterBatch` short-circuits (no DB query) when
  `AUTO_RECOVERY_ENABLED` is false; otherwise it discovers, evaluates and logs an
  `auto_recovery` record (`evaluated`/`eligible`/`rejected`/`reasons`/`durationMs`).

## 5. Safety properties (preserved)
- No writes: discovery is a single read; evaluation is pure. No recovery attempt,
  lease, job, or Docker call is produced.
- Default-off: with `AUTO_RECOVERY_ENABLED=false` the hook performs **no** query.
- Trust boundary unchanged: only `RUNTIME_TRUSTED_LOCAL_ONLY` + explicit
  `BUILD_TRUSTED_REPOSITORIES`; loopback-only; identity/port checks mirror the
  read-only due-check discovery.
- Advisory only: results are logged, never acted upon. Any future reservation stage
  must revalidate all conditions atomically.

## 6. Tests
- `test/auto-recovery-candidates.test.ts` — 21 pure unit tests: eligible path, every
  rejection reason, cooldown elapsed/active, empty batch, and batch aggregation.
- `test/auto-recovery-candidates.integration.test.ts` — 2 DB tests (isolated
  `cloudpilot_health_test_579`): discovery ignores untrusted repos and is read-only
  (version/status/failures unchanged, no attempts created); surfaces an eligible
  trusted candidate with no side effects.
- Worker `tsc --noEmit`: clean. Existing `health-monitor` tests: 3 pass.

## 7. Rollback
Revert the `afterBatch` hook and delete `auto-recovery-candidates.ts` plus its tests.
No schema, migration, or configuration change is required.