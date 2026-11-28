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
