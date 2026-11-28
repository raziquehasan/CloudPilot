# Phase 5.7.10 — Final Recovery Safety (Stabilization)

## Baseline
`0590465` (`test: validate recovery resilience and harden failure handling`), branch `main`.

## Flaky-test root cause (established)
The multi-worker subprocess scenario (M1) and, intermittently, the after-intent scenario (E)
depended on child workers that called `processRecoveryJob` with an **unbounded** abort signal.
Losing workers that could not acquire the lease busy-looped until the processor's internal 90 s
deadline, so under repeated runs a child could outlive the test window (hang / timeout / 124 exit).
This was a harness timing defect, not a recovery-logic defect.

## Stabilization changes
- `apps/worker/test/helpers/crash-child.ts`: the child now uses `AbortSignal.timeout(maxMs)` when
  `CLOUDPILOT_CRASH_MAX_MS` is set, so a losing worker aborts deterministically instead of looping
  to the 90 s deadline.
- `apps/worker/test/recovery-crash.integration.test.ts`: `runChild` passes a bounded `maxRunMs`
  (12 s) and a hard `timeoutMs` kill; M1 uses fresh fixtures and asserts durable DB state
  (single `SUCCEEDED`, one generation, `restartCount === 1`) rather than child exit codes.
- No assertions were weakened, no retries added, no arbitrary sleeps introduced. The triple-gated
  crash hook (`NODE_ENV=test` + `CLOUDPILOT_RECOVERY_CRASH_TEST=1` + matching stage) is unchanged.

## Consecutive-round results (10 independent rounds, fresh DB state each)
`--test-name-pattern="E:|E2:|E3:|E4:|M1:"` — **10/10 rounds, 5/5 pass each, 0 failures.**
Scenarios: E after-intent (conservative uncertainty, no 2nd restart), E2 after-claim (exactly one
restart), E3 after-restart (no duplicate restart), E4 after-finish (redelivery no-op), M1 two
subprocess workers (single lease, one success, one generation, `restartCount=1`).

## Multi-worker contention evidence
Two isolated worker subprocesses per round × 10 rounds: exactly one `SUCCEEDED`, `attemptCount=1`,
two health states (gen 0 + gen 1), one health check, runtime `restartCount=1`, lease cleared.

## Actual BullMQ stalled-job redelivery
`recovery-stalled.integration.test.ts` (real BullMQ + isolated Redis, test-only `lockDuration=3s`):
a child worker exceeds the lock and is killed → BullMQ marks the job stalled and redelivers to a
replacement worker → one `SUCCEEDED`, one generation increment, `restartCount=1` (no unsafe
duplicate restart). **Pass.**

## Dependency interruption evidence
- Redis pause before publication → `queue.add` blocked, attempt stays `REQUESTED`, 0 restarts;
  after unpause → `SUCCEEDED` (real Redis).
- PostgreSQL pause before reservation → DB unreachable, 0 restarts; after unpause → `SUCCEEDED`.
- PostgreSQL finalize-time failure (trigger, 5.7.9) → uncertainty preserved, no false success.
- Completed-recovery redelivery (E4 + stalled) → no-op, no duplicate generation.

## Scheduler
Existing `scheduler-diagnostics.ts` + tests retained (backoff doubling/cap, ~72 s gap reproducible
from a 61 s backoff sequence + batch durations). Historical gap not definitively reproduced; current
diagnostics (batch duration, due count, backoff, scheduling lag) make future occurrences diagnosable.
No interval/concurrency/backoff policy change.

## Coverage audit (remaining gaps)
Untested in this phase: live Redis pause **during** an in-flight Docker restart; live PostgreSQL
pause **after** Docker restart but before finalization (covered indirectly by the finalize-failure
trigger + E3). Crash hook covers after-claim/after-intent/after-restart/after-finish; "during Docker
restart / during health verification" are represented by the uncertain-restart unit tests, not a
live mid-call kill. These are documented limitations, not claimed coverage.

## Validation
- worker `tsc --noEmit`: exit 0.
- Crash scenarios: 10/10 rounds (above).
- Stalled redelivery: 1/1. Non-fault worker group: 17/17. Fault injection: 3/3 (prior runs).
- Web/API/queue unchanged this phase (no edits); previously green.

## Protected infrastructure
Demo `f9144585…` :53143, validation `64376e6449f1` :63299, postgres :5433, redis :6379 — unchanged.
`AUTO_RECOVERY_ENABLED=false`, `MANUAL_RECOVERY_ENABLED=true`. Isolated containers removed.

## Phase 5.8 readiness verdict
**Ready to begin automatic-recovery development** (critical crash boundaries, stalled redelivery,
sustained contention, dependency interruptions, durable intent/lease fencing, budget/cooldown all
validated; no false success; no unsafe duplicate restart in tested scenarios). **Not ready to enable
in production** until the documented live mid-restart interruption gaps are covered and the scheduler
gap is either reproduced or accepted as diagnosable. Automatic recovery remains disabled.

## Git
Modified: `apps/worker/test/helpers/crash-child.ts`, `apps/worker/test/recovery-crash.integration.test.ts`.
New: `apps/worker/test/recovery-stalled.integration.test.ts`, `apps/worker/test/helpers/stalled-worker-child.ts`,
this doc. No source/production changes, no schema migration. Not committed/pushed.