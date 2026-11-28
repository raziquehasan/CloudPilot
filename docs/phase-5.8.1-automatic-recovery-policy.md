# Phase 5.8.1 — Automatic Recovery Policy Foundation

## Baseline
`366d67f` (`test: complete recovery crash and distributed resilience validation`), branch `main`.

## Objective
Deterministic, independently testable automatic-recovery **eligibility policy** reusing the existing
health monitoring and manual recovery architecture. **No automatic Docker restart is implemented or
activated.** Manual recovery is unchanged.

## Policy architecture
- New pure module `apps/worker/src/health/auto-recovery-policy.ts`:
  `evaluateAutoRecovery(input) → { eligible: true } | { eligible: false, reason }`.
- No side effects, no DB/queue access, not wired to any live trigger.
- Reuses `healthDefaults.consecutiveFailureLimit` (3) and the shared recovery budget/cooldown
  semantics; it cannot bypass the manual/automatic budget, lease fencing, or durable restart intent
  enforced by the recovery processor.

## Eligibility rules (fixed evaluation order → stable rejection reason)
1. `AUTO_DISABLED` — `AUTO_RECOVERY_ENABLED` not explicitly enabled.
2. `INFRASTRUCTURE_UNAVAILABLE` — DB/Redis unreachable.
3. `PERSISTENCE_UNAVAILABLE` — `HEALTH_PERSISTENCE_ENABLED` off.
4. `MONITORING_UNAVAILABLE` — no worker actively monitoring.
5. `DEPLOYMENT_NOT_RUNNING`.
6. `RUNTIME_NOT_RUNNING`.
7. `RUNTIME_INELIGIBLE` — not trusted local / trusted repo / eligible identity.
8. `HEALTH_NOT_UNHEALTHY`.
9. `INSUFFICIENT_FAILURES` — fewer than 3 consecutive failures.
10. `STALE_HEALTH` — observation missing or older than `freshnessMs`.
11. `GENERATION_MISMATCH`.
12. `RESTART_UNCERTAIN` — unresolved ambiguous restart.
13. `ACTIVE_RECOVERY` — a REQUESTED/RUNNING attempt exists.
14. `CONFLICTING_OPERATION` — conflicting deployment/runtime lifecycle op.
15. `BUDGET_EXHAUSTED` — `attemptsInWindow >= recoveryLimit`.
16. `COOLDOWN` — cooldown remaining > 0.
17. Otherwise `eligible: true`.

## Shared safeguards
Manual and automatic recovery draw from the same budget/cooldown and the same lease + durable-intent
fencing; the policy refuses to act on stale health, replaced/stopped runtimes, active/conflicting
operations, or unresolved restart uncertainty — preventing loops, stale/replaced recovery, duplicate
reservations, shutdown-time recovery, and automatic retries of ambiguous restarts. No rollback,
redeployment, container recreation, or destructive remediation is introduced.

## Feature flags
Unchanged and verified: `HEALTH_PERSISTENCE_ENABLED=true`, `HEALTH_MONITORING_ENABLED=true`,
`MANUAL_RECOVERY_ENABLED=true`, `AUTO_RECOVERY_ENABLED=false`. Root `.env` untouched. The policy is
not connected to any live trigger.

## Test results
`apps/worker/test/auto-recovery-policy.test.ts` — **13 pass, 0 fail** (pure unit): eligible case;
flag disabled; infra/persistence/monitoring unavailable; deployment/runtime not running; ineligible
runtime; healthy; insufficient failures; stale/missing observation; generation mismatch; restart
uncertain; active recovery; conflicting op; budget exhausted; cooldown; deterministic order.

## TypeScript / regression
- worker `tsc --noEmit`: exit 0.
- api `tsc --noEmit`: exit 0.
- Manual recovery regression (`health-state`, `recovery-operations`): 5 pass, 0 fail — unchanged.

## Remaining limitations / activation blockers (carried from 5.7.10)
- Live Redis interruption during in-flight Docker restart — not verified.
- Live PostgreSQL interruption after restart but before finalization — not verified.
- Actual worker termination during an in-flight Docker operation — not verified.
These remain mandatory activation-gate scenarios. Exactly-once Docker execution is not claimed.

## Next integration steps (Phase 5.8.2)
Wire the evaluator into a bounded automatic-recovery scheduler behind `AUTO_RECOVERY_ENABLED` (still
off), reusing `reserveManualRecovery`-style reservation with `trigger=AUTOMATIC`, the shared budget,
and the existing processor; add the missing live interruption tests before any production enablement.

## Git
New: `apps/worker/src/health/auto-recovery-policy.ts`, `apps/worker/test/auto-recovery-policy.test.ts`,
this doc. No source/production behavior change, no schema migration. Not committed/pushed.