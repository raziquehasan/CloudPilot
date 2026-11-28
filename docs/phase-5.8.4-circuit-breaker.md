# Phase 5.8.4 — Circuit Breaker and Execution Safety

Baseline commit: `751b07c73cab9db81ff4ce1070e9d1fff81dc5d5`

## Disablement safety fix

`cancelUnstartedAutomatic` now cancels a RUNNING attempt that has no persisted restart intent (`restartIssuedAt === null`) in addition to REQUESTED attempts. Attempts with `restartIssuedAt` set are never cancelled and are reconciled by the normal processor path. This prevents a disabled automatic recovery from authorizing a new Docker restart while preserving already-issued restart outcomes.

## Circuit breaker

The rolling budget and cooldown apply to both triggers. Automatic reservation separately reads failures since the most recent verified successful recovery, with no rolling cutoff. An uncertain restart remains unresolved until a verified recovery. Timeout, stale-ownership, or Docker-unavailable failures with a persisted restart intent provide no proof that the restart did not occur, so they also block automatic execution. Manual recovery remains subject to the shared budget and cooldown.

## Circuit breaker implementation

`packages/database/src/auto-recovery-circuit.ts` is the authoritative evaluator, exported for worker reuse. Reservation evaluates it under the existing runtime lock after checking for active attempts. The same transaction writes the automatic admission state to the existing recovery-request audit event. A failed marked HALF_OPEN trial remains consumed after the budget window. An old failed automatic attempt can lack this admission marker. If its preceding durable failures could have made it a trial, automatic recovery fails closed until a verified recovery. No schema migration is required.

## Changed files

- apps/worker/src/health/recovery-processor.ts (disablement safety)
- apps/worker/src/health/auto-recovery-circuit.ts (new)
- docs/phase-5.8.4-circuit-breaker.md


Live Redis/PostgreSQL interruption, worker termination during Docker ops, and real end-to-end automatic Docker recovery remain Phase 5.8.6 activation gates. Exactly-once Docker execution is not claimed.

## Test results
- auto-recovery-policy.test.ts: 13 pass, 0 fail
- auto-recovery-candidates.test.ts: 21 pass, 0 fail
- auto-recovery-reservation.test.ts: 3 pass, 0 fail
- API tsc: pass
- worker tsc: pass

## Phase 5.8.4.C worker integration

The worker uses the database circuit evaluator during candidate discovery, locked reservation and immediately before recording restart intent. Discovery includes the circuit state, rejection reason, remaining budget, consecutive failures, cooldown, next eligible time, manual-intervention requirement and latest outcome. Reservation fences the observed health generation and version. Restart authorization shares a runtime lock with cancellation. If cancellation wins, the worker cannot persist restart intent. If authorization wins, reconciliation preserves that intent and verifies the outcome without issuing another restart.

The health monitor runs reconciliation separately from candidate evaluation, including while automatic recovery is disabled. Reconciliation cancels expired or ineligible reservations without restart intent, retires orphaned RECOVERING states under the runtime lock and uses deterministic BullMQ IDs to republish missing jobs or retry terminal jobs for recoveries that remain active. Manual recovery continues to share budget and cooldown.

The focused suite ran against disposable PostgreSQL and Redis with automatic execution disabled. It passed 50 tests, with zero failures and three automatic-execution tests skipped. It exercised OPEN and HALF_OPEN discovery, stale snapshot rejection, cancellation and authorization ordering, persisted intent, queue reconciliation and manual recovery. Live infrastructure interruption, real worker termination during Docker execution and end-to-end automatic Docker recovery remain Phase 5.8.6 activation gates.

## 5.8.4.D — Final circuit breaker safety audit

Baseline: `96073e8`. No source changes were required; the audit found no defects.

### Source audit findings (file:line)
- Single authoritative evaluator: `packages/database/src/auto-recovery-circuit.ts:34` (`evaluateCircuit`), consumed only through `inspectAutomaticRecoveryCircuit` (`recovery-store.ts:154`) by discovery (`auto-recovery-candidates.ts:205`), reservation (`recovery-store.ts:303`) and pre-intent authorization (`recovery-processor.ts:153`).
- Atomic reservation under runtime lock: `recovery-store.ts:257` (`lockRecoveryRuntime` … `FOR UPDATE`) precedes health/circuit reads and the RECOVERING write (`recovery-store.ts:309`).
- Durable OPEN/HALF_OPEN history: admission state persisted to the request audit event (`recovery-store.ts:320-326`) and reclassified from `audit_logs` (`recovery-store.ts:104-114`).
- One HALF_OPEN trial: active-attempt check (`recovery-store.ts:267`) plus `failedHalfOpenTrial` reopen (`auto-recovery-circuit.ts:45`).
- Budget expiry cannot reset an unresolved circuit: failures counted since last SUCCEEDED with no rolling cutoff (`recovery-store.ts:93-103`).
- Stale generation/version rejection: reservation fence (`recovery-store.ts:274`), claim (`recovery-processor.ts:91`), revalidate (`recovery-processor.ts:137`).
- Cancellation vs authorization share the runtime lock and `restartIssuedAt` fence (`recovery-processor.ts:141-166`, `257-294`).
- Authorized attempt excludes itself pre-intent (`recovery-processor.ts:153`, `excludeAttemptId`).
- Persisted intent prevents a second ambiguous restart (`recovery-processor.ts:160-163`; `recovery-operations.ts` source-epoch gate).
- Manual recovery shares budget/cooldown (`recovery-store.ts:193-197`, `292-300`).

### Queue reconciliation audit
- Missing/terminal jobs reconciled; active jobs untouched (`auto-recovery-publisher.ts:76-90`).
- Runs independently of candidate evaluation via `maintenance()` (`health-monitor-runtime.ts:76-86`), including while auto is disabled.
- Expired leases selected (`auto-recovery-publisher.ts:130-134`); orphaned RECOVERING repaired under lock without overwriting newer generations (`auto-recovery-publisher.ts:169-187`); bounded with `afterId` cursor (`health-monitor-runtime.ts:65-84`); deployment-scoped (`auto-recovery-publisher.ts:125`).

### Defects discovered and fixed
None. No source changes made.

### Exact test results (disposable DB `cloudpilot_health_test_584d`, real Redis/BullMQ, Docker mocked; auto enabled only in the test process)
- `auto-recovery-scheduling.integration.test.ts` (AUTO=true): 12 pass, 0 fail, 4 skipped (the 4 skips are the auto-disabled-only cases). The three previously skipped execution tests now pass: successful automatic recovery (generation→1, HEALTHY), failed recovery (no false success), ambiguous restart (no repeat, `RECOVERY_RESTART_UNCERTAIN`).
- `auto-recovery-circuit.integration.test.ts`: 4 pass, 0 fail, 0 skipped.
- `auto-recovery-candidates.integration.test.ts` + `recovery.integration.test.ts`: 6 pass, 0 fail (manual compatibility intact).
- Pure suites (policy, candidates, reservation, health-monitor): 40 pass, 0 fail.
- Worker `tsc --noEmit`: pass. API `tsc --noEmit`: pass. No build changes (no source edits).

### Protected infrastructure & cleanup
Primary API/worker/Postgres(5433)/Redis(6379)/demo(53143)/validation(63299) untouched. Disposable DB dropped and `cp583:*` test queue keys purged.

### Frontend integration contract (Phase 5.8.5 — backend only, no UI yet)
Authoritative sources:
- Enabled/disabled: `workerEnv.AUTO_RECOVERY_ENABLED` (worker). The API does not expose it today — backend addition required.
- Circuit state, rejection reason, next eligible time, manual-intervention flag, remaining budget, consecutive failures, cooldown, latest outcome: `inspectAutomaticRecoveryCircuit` (`recovery-store.ts:154`).
- Recovery history: existing `GET …/recoveries` and `GET …/recoveries/:id`.

Existing authenticated endpoints reusable: `GET /:projectId/deployments/:deploymentId/health` (state, checks, monitoring, manual recovery availability) and the recoveries list/detail endpoints. Workspace isolation via `resolveProject`; `requireAuth` present.

Required backend additions (5.8.5): extend the health response with an `automaticRecovery` snapshot (`state`, `reason`, `nextEligibleAt`, `manualInterventionRequired`, `remainingBudget`, `consecutiveFailures`, `cooldownRemainingMs`, `latestOutcome`) sourced from `inspectAutomaticRecoveryCircuit`, plus an `autoRecoveryEnabled` flag; keep workspace scoping and gate any config view behind `canManage`.

### Readiness
Phase 5.8.5 (automatic recovery dashboard) may proceed against the contract above.

### Phase 5.8.6 activation blockers (unchanged)
1. Live Redis interruption during an in-flight Docker restart.
2. Live PostgreSQL interruption after restart but before finalization.
3. Real worker termination during an in-flight Docker operation.
4. Real end-to-end automatic Docker recovery.
Exactly-once Docker execution is not claimed; production readiness is not asserted.
