# Phase 5.7.9 — Recovery Resilience, Fault Injection & Safety Validation

## 1. Phase objectives
Validate that manual runtime recovery remains correct, idempotent, bounded and safe under
worker crashes, duplicate delivery, concurrency, lease expiry, Redis/Postgres interruption,
Docker failures, stale identity, restart uncertainty, host-port changes, stale heartbeats and
scheduler delays — **before** introducing automatic recovery. Automatic recovery remains
disabled.

## 2. Baseline
- Commit `f3204f756ad6781ea2d647e70b9a86ad7a7da39d`
  (`feat: report worker monitoring availability and harden health dashboard`).
- Branch `main`, clean tree, `origin/main` in sync at start.

## 3. Repository audit findings
- State machine: `apps/worker/src/health/health-state.ts` (`transitionHealth`).
- Recovery processor: `apps/worker/src/health/recovery-processor.ts`
  (claim → markRestart → verify → finishRecovery; lease + generation fencing).
- Restart intent: `markRestart` persists `restartIssuedAt` + `restartSourceStartedAt`
  **before** Docker restart, inside a `FOR UPDATE` lease-fenced transaction.
- Operations: `apps/worker/src/health/recovery-operations.ts` (`verifyRecoveryRestart`).
- Store/policy: `packages/database/src/recovery-store.ts`
  (`recoveryPolicy` = window 15 min, limit 3, cooldown 15 s ×2^n, lease 120 s;
  `reserveManualRecovery` idempotent reservation).
- Schema: `DeploymentRecoveryAttempt` with `dra_attempt_chk`, `dra_lifecycle_chk`,
  `dra_restart_fence_chk`, unique idempotency key, unique lease token.
- Scheduler: `apps/worker/src/health/health-monitor.ts` (poll 1 s, backoff
  `min(30 s, 1 s·2^failures)`, bounded concurrency, 60 s retention).
- Monitoring: `apps/api/src/services/monitoring-report.ts` (5.7.8).

## 4. Existing safety guarantees (verified)
Durable restart intent before side effect; source start-time fencing; lease ownership
validated before every authoritative mutation; generation fencing; conservative
`RECOVERY_RESTART_UNCERTAIN`; identity/port verification; idempotent reservation;
deterministic job id `recovery-${correlationId}`; bounded budget/cooldown; healthy-runtime
protection; automatic recovery disabled.

## 5. Isolated test architecture
- Dedicated containers `cloudpilot-resilience-579-postgres` (dynamic host port, DB
  `cloudpilot_health_test_579`) and `cloudpilot-resilience-579-redis` (dynamic host port).
- Queue prefix `cloudpilot_resilience_579`.
- Migrations applied **only** to the isolated DB via `prisma migrate deploy`.
- Disposable, labeled runtime containers for the live test.

## 6. Test environment guardrails
- Integration tests gate on `MANUAL_RECOVERY_ENABLED=true` **and** DB path matching
  `/cloudpilot_health_test_[a-z0-9]+`.
- Live test gates on `CLOUDPILOT_RESILIENCE_LIVE=1`, uses label
  `cloudpilot.resilience579=true`, unique names, and cleans up in `finally`.
- No primary DB/Redis/container was touched; no primary records modified.

## 7. Fault-injection test matrix
| Stage | Scenario | Test | Result |
|---|---|---|---|
| C | State transitions / generation fencing | `health-state.test.ts` (existing) | pass |
| D1 | Duplicate recovery job | `recovery.integration.test.ts`, `recovery-resilience.integration.test.ts` | pass |
| D2 | Concurrent workers | same | pass |
| D3 | Concurrent API reservations | `recovery.integration.test.ts` | pass |
| D4 | Lease-expiry takeover, no 2nd restart | `recovery-resilience.integration.test.ts` | pass |
| E | Crash before/after intent; settlement failure | `recovery.integration.test.ts`, `recovery-operations.test.ts` | pass |
| F | Postgres failure during finalize (trigger) | `recovery.integration.test.ts` | pass |
| G | Identity/port drift, failed HTTP, stopped/stale | `recovery.integration.test.ts` | pass |
| G | Occupied fixed port (live Docker) | `recovery-resilience.live.test.ts` | pass |
| H | Exhaustion terminal, cooldown | `recovery.integration.test.ts`, `health-state.test.ts` | pass |
| I | Monitoring states | `monitoring-report.test.ts` | pass |

## 8. Recovery crash-boundary results
- Before lease acquisition / before intent: no restart issued (`calls()===0`), attempt
  remains REQUESTED/RUNNING with no intent (`recovery-operations.test.ts`).
- After intent, before restart: persisted epoch fences; uncertain outcome never re-issues.
- After restart, before finalize: settlement-failure trigger → attempt stays RUNNING with
  persisted epoch; a later run reconciles once (`calls()===1`), generation +1 exactly once.
- Takeover with expired lease: new owner reconciles the already-issued restart with **no**
  second restart; generation +1; one health check.
- Duplicate concurrent finalization: one `succeeded`, one `skipped`; one restart; one
  generation increment; two health states (gen 0 + gen 1); one check.

## 9. Duplicate delivery & concurrency
Duplicate and concurrent workers resolve to a single restart and single finalization;
competing `withRuntimeLifecycleLease` stop is rejected (`operation is already in progress`);
concurrent reservations yield `reserved`+`replay` and a third is `busy`.

## 10. Redis interruption
Not exercised live (no isolated Redis fault harness in this run). Design relies on durable DB
state as authoritative and bounded outbox/queue retry; **unverified live** — see limitations.

## 11. PostgreSQL interruption
Simulated a finalize-time DB failure via a temporary trigger; restart uncertainty preserved,
no false success, reconciliation safe after recovery. Primary DB never touched.

## 12. Docker failure results
Identity mismatch, unavailable Docker, port drift, failed HTTP, stopped/stale runtime all
classified correctly (`RECOVERY_*`) with the saved port preserved (mocked executor).

## 13. Fixed-port collision (live)
Second bind on an occupied loopback port fails; original owner stays running on the same port;
no unintended running container remains; no silent fallback.

## 14. Recovery budget & cooldown
Exhaustion is terminal (`RECOVERY_EXHAUSTED`, deployment stays RUNNING); cooldown/backoff and
generation fencing verified via existing unit + integration tests. Policy values confirmed
from source (limit 3 / 15 min, cooldown 15 s ×2^n, lease 120 s).

## 15. Monitoring heartbeat resilience
`monitoring-report.test.ts` covers ENABLED/DISABLED/UNAVAILABLE/STALE/UNKNOWN, mixed workers,
legacy workers without the field, and stale/draining. Availability is never inferred from API
env or `nextCheckAt`.

## 16. Scheduler delay investigation
`health-monitor.ts` uses poll 1 s with exponential backoff `min(30 s, 1 s·2^failures)` after a
failed batch, plus batch execution time (Docker inspect + 3 s HTTP probe, bounded concurrency).
A ~72 s gap is consistent with a transient batch failure (e.g., inspect/HTTP timeout) followed
by backoff plus the next batch. No code change made (would not hide delays by changing
interval/concurrency). **Root cause not definitively reproduced in isolation** — remaining
uncertainty documented.

## 17. Defects discovered & corrections
None required. No source changes; only additive tests. No schema migration.

## 18. Automated validation results
| Command | Exit | Pass | Fail | Skip | Infra |
|---|---|---|---|---|---|
| queue build | 0 | — | — | — | — |
| worker tsc | 0 | — | — | — | — |
| api tsc | 0 | — | — | — | — |
| worker unit (health-state, recovery-operations, requested-host-port) | 0 | 7 | 0 | 0 | mocked |
| api (monitoring-report, deployment-host-port) | 0 | 27 | 0 | 0 | pure |
| recovery.integration (isolated DB) | 0 | 4 | 0 | 0 | real DB, mocked Docker |
| recovery-resilience.integration (isolated DB) | 0 | 2 | 0 | 0 | real DB, mocked Docker |
| recovery-resilience.live (opt-in) | 0 | 1 | 0 | 0 | real Docker, disposable |

## 19. Protected infrastructure verification
Before/after unchanged: demo `f9144585eb87` :53143 (image `sha256:15b89642…`), validation
`64376e6449f1` :63299 (image `sha256:ec6f7b2f…`), postgres :5433 healthy, redis :6379 healthy.
Feature flags unchanged (persistence/monitoring/manual=true, auto=false).

## 20. Test cleanup
Isolated Postgres/Redis containers removed; live test containers removed in `finally`; no
resilience containers remain. Temp env file deleted.

## 21. Known limitations
- Redis interruption (Stage F) not exercised live.
- Live Docker tests limited to occupied-port; no live crash of a real worker subprocess.
- Scheduler 72 s gap root cause not reproduced in isolation.
- Exactly-once external Docker execution is **not** claimed; conservative uncertainty retained.

## 22. Remaining risks before automatic recovery
Redis-failure reconciliation under live outage; real worker-subprocess crash at each boundary;
definitive scheduler-gap root cause; sustained multi-worker contention.

## 23. Recommended Phase 5.8 prerequisites
Add live Redis-failure and worker-subprocess crash harnesses; add bounded scheduler diagnostics;
then design automatic recovery with the same fencing, budget and uncertainty guarantees.

## 24. Git status
Untracked (new tests only): `apps/worker/test/recovery-resilience.integration.test.ts`,
`apps/worker/test/recovery-resilience.live.test.ts`, and this doc. No source modified. Not
committed or pushed.

---

# Addendum — Real infrastructure fault injection (Checkpoint 5b)

## A1. Isolated infrastructure (current run)
`cloudpilot-resilience-579-postgres` (dynamic host port, DB `cloudpilot_health_test_579`) and
`cloudpilot-resilience-579-redis` (dynamic host port), queue prefix `cloudpilot_resilience_579`,
migrations applied only to the isolated DB. All removed after the run. Primary infra untouched.

## A2. Real Redis interruption (actual BullMQ)
`apps/worker/test/recovery-fault-injection.integration.test.ts` (opt-in
`CLOUDPILOT_RESILIENCE_FAULT=1`): paused the isolated Redis container → `queue.add` fails
(publication blocked), durable attempt stays `REQUESTED`, zero restarts; after `unpause` the job
publishes and processes to `SUCCEEDED` with exactly one restart. **Result: pass.**

## A3. Real PostgreSQL interruption
Same file: paused the isolated Postgres → `SELECT 1` unreachable within timeout, zero restarts
(no durable authorization); after `unpause`, reservation succeeds and recovery completes with one
restart and `SUCCEEDED`. **Result: pass.**

## A4. Real BullMQ duplicate delivery
Same file: the same deterministic job id added twice → BullMQ dedups to a single job; one restart,
one generation increment, attempt `SUCCEEDED`. This is **real BullMQ** delivery/ack, not a mocked
processor replay. **Result: pass.**

## A5. Worker subprocess crash (real process)
`apps/worker/test/recovery-crash.integration.test.ts` + `test/helpers/crash-child.ts`: a spawned
`tsx` child crashes (process.exit) at the **after-intent** boundary via a strictly double-gated
test hook (`CLOUDPILOT_RECOVERY_CRASH_TEST=1` + `CLOUDPILOT_RECOVERY_CRASH_AFTER`). Evidence: the
child exited 1; `restartIssuedAt` + `restartSourceStartedAt` persisted; attempt `RUNNING`. After
simulated lease expiry, a replacement worker reconciles with **no second restart** and reports
`FAILED / RECOVERY_RESTART_UNCERTAIN` — no false success, no health generation advancement
(generation stays 0, zero checks). Exactly-once Docker execution is **not** claimed. **Result: pass.**

## A6. Scheduler diagnostics (Stage J)
- `apps/worker/src/health/scheduler-diagnostics.ts` (new, pure): `computeBackoffMs`,
  `schedulingLagMs`; formula unchanged (`min(30s, poll·2^failures)`).
- `health-monitor.ts` now emits bounded `batch` diagnostics (dueCount, batchDurationMs,
  consecutiveFailures, backoffMs, schedulingLagMs) and includes `backoffMs` on
  `infrastructure_error`; `health-monitor-runtime.ts` logs `batch` at debug. No interval/concurrency
  change.
- `apps/worker/test/scheduler-diagnostics.test.ts`: 4 pass — backoff doubling/cap, a 61 s backoff
  sequence + two ~5 s batches reproduces the ~72 s gap, invalid-input rejection, non-negative lag.
- The original 72 s gap is **consistent with** transient-failure backoff + batch duration; not
  definitively reproduced live. Root cause remains uncertain (documented, not invented).

## A7. Additional validation (actual)
| Command | Exit | Pass | Fail | Skip | Infra |
|---|---|---|---|---|---|
| worker tsc | 0 | — | — | — | — |
| api tsc | 0 | — | — | — | — |
| web tsc | 0 | — | — | — | — |
| web eslint | 0 | — | — | — | — |
| web next build | 0 | — | — | — | — |
| queue build | 0 | — | — | — | — |
| scheduler-diagnostics | 0 | 4 | 0 | 0 | pure |
| recovery-fault-injection (opt-in) | 0 | 3 | 0 | 0 | real Redis+Postgres+BullMQ |
| recovery-crash (opt-in) | 0 | 1 | 0 | 0 | real subprocess |

## A8. Source changes (this addendum)
- `apps/worker/src/health/scheduler-diagnostics.ts` (new)
- `apps/worker/src/health/health-monitor.ts` (diagnostics emit + computeBackoffMs)
- `apps/worker/src/health/health-monitor-runtime.ts` (batch log at debug)
- `apps/worker/src/health/recovery-processor.ts` (double-gated test-only crash hook)
- New tests: `scheduler-diagnostics.test.ts`, `recovery-fault-injection.integration.test.ts`,
  `recovery-crash.integration.test.ts`, `helpers/crash-child.ts`.
No schema migration. Protected infra unchanged; flags unchanged (auto=false). Not committed/pushed.

## A9. Updated limitations
Live Redis/Postgres interruption and real subprocess crash are now verified in isolation. Still not
covered: crash at every boundary in one run, sustained multi-worker contention, definitive
scheduler-gap root cause. Exactly-once external Docker execution is not claimed.