# Phase 5.8.3 — Automatic Recovery Reservation & Durable Job Scheduling

## 1. Objective
Connect Phase 5.8.2 read-only candidate discovery to a **durable, atomic
reservation**, **idempotent BullMQ publication**, and the **existing recovery
processor** — reusing the manual recovery architecture rather than building a
separate restart engine. Automatic recovery remains **disabled by default**
(`AUTO_RECOVERY_ENABLED=false`); all execution validation runs only in isolated
test environments.

## 2. Baseline
- Commit `26df84f` (`feat: add read-only automatic recovery candidate discovery`).
- Branch `main`. No commit/push performed in this subphase.

## 3. Reservation architecture
- `packages/database/src/recovery-store.ts`
  - Extracted a shared `reserveRecovery(db, scope, { trigger, actorId, idempotencyKey, expectedRuntimeId? })`
    used by both `reserveManualRecovery` (unchanged external behavior) and the new
    `reserveAutomaticRecovery(db, scope, runtimeInstanceId)`.
  - Revalidates **inside one transaction**: PostgreSQL `clock_timestamp()` time,
    `FOR UPDATE` runtime lock, ownership, current runtime identity, deployment/runtime
    `RUNNING`, eligible container/image/loopback identity, `UNHEALTHY` health with a
    **fresh** `lastCheckedAt` (automatic only, ≤ 30 s), shared manual+automatic budget
    (`recoveryPolicy.limit`), exponential cooldown, active/conflicting operation
    rejection, and unresolved-restart rejection.
  - Caller advisory candidates never authorize Docker; the reservation is the only
    authority and is re-checked atomically.
- **Idempotency**: `automaticRecoveryKey(runtimeInstanceId, generation, healthVersion)`
  is a deterministic UUIDv5-style key derived from the eligibility epoch. Duplicate
  scheduler evaluation and concurrent workers collapse to a single reservation
  (unique `dra_deployment_idempotency_key` + `P2002` replay). A success advances the
  generation and any settlement advances the health version, so a legitimate future
  recovery is never permanently blocked.

## 4. Transaction boundaries
1. Reservation transaction commits the `REQUESTED` attempt + `RECOVERING` health
   transition (durable before any queue call).
2. Publication is a separate, idempotent step (`recovery-<id>` job id).
3. Execution (claim → intent → restart → verify → settle) runs in the existing
   processor with its own lease-fenced transactions.

A crash or Redis outage **after** the reservation commit leaves a durable `REQUESTED`
attempt; it is never lost. A crash **before** commit leaves nothing to reconcile.

## 5. Queue publication & reconciliation
- `apps/worker/src/health/auto-recovery-publisher.ts`
  - `publishRecoveryJob(queue, input, recoveryId, trigger)` — idempotent add-by-job-id;
    returns `false` on failure and logs, leaving the reservation durable.
  - `reconcileUnpublishedAutoRecoveries(queue, limit)` — bounded scan of `REQUESTED`
    `AUTOMATIC` attempts, re-publishing any whose job is missing.
- Reuses the existing runtime queue and deterministic `recovery-<id>` job ids; no new
  queue, no direct Docker call from the scheduler.

## 6. Recovery processor integration
- `claimRecovery` filters the attempt by `job.recoveryTrigger`, so a job can only
  claim a reservation of the same trigger.
- `processRecoveryJob` enforces flags **independently per trigger**: `MANUAL` requires
  `MANUAL_RECOVERY_ENABLED`, `AUTOMATIC` requires `AUTO_RECOVERY_ENABLED` (plus
  `RUNTIME_ENABLED` + schema readiness).
- **Disabled-before-execution**: `cancelUnstartedAutomatic` closes a still-`REQUESTED`
  attempt as `CANCELLED` (no restart) and restores health to `UNHEALTHY` with a fresh
  `nextCheckAt`. A restart already issued (`RUNNING` or persisted intent) is **never**
  cancelled; its outcome is reconciled through the normal verify/settle path.
- All existing safety is preserved: durable restart intent before the side effect,
  source-epoch verification, lease/generation fencing, image/host-port checks,
  verified-health success, generation advance only via the verified transition, and no
  repeated ambiguous restart.

## 7. Scheduler integration
- `health-monitor.ts` `afterBatch` hook (5.8.2) now, when `AUTO_RECOVERY_ENABLED` and a
  queue are present: discovers → evaluates → reserves each eligible candidate (bounded
  by batch size) → publishes → reconciles. When the flag is off it performs **no** DB
  query, reservation, or publication. A null queue (runtime disabled) skips reservation
  to avoid orphaned attempts.
- Bounded, non-overlapping (single monitor loop), no unbounded queries, no Docker,
  graceful shutdown via the existing abort signal, and normal monitoring cadence,
  retention and backoff are unchanged.

## 8. Feature flags (unchanged)
`HEALTH_PERSISTENCE_ENABLED=true`, `HEALTH_MONITORING_ENABLED=true`,
`MANUAL_RECOVERY_ENABLED=true`, `AUTO_RECOVERY_ENABLED=false`. Root `.env` untouched;
automatic recovery enabled only via isolated test env.

## 9. Test evidence (isolated DB `cloudpilot_health_test_583`, real Redis/BullMQ,
## Docker mocked)
- `test/auto-recovery-reservation.test.ts` — 3 pure key tests (deterministic, epoch
  rotation, UUID format).
- `test/auto-recovery-scheduling.integration.test.ts` — 12 tests:
  - Auto **disabled**: 9 pass, 3 skipped (execution) — reservation idempotency/concurrency,
    healthy/stale/stopped/wrong-generation rejection, manual↔automatic contention, budget
    exhaustion, cooldown, idempotent publish + reconcile after Redis outage,
    cancel-without-restart when disabled, stale payload skip, manual compatibility.
  - Auto **enabled**: 11 pass, 1 skipped — adds successful automatic recovery (generation
    → 1, HEALTHY), failed recovery (no false success, generation stays 0), ambiguous
    restart (no repeat, `RECOVERY_RESTART_UNCERTAIN`).
- Regression: `test/recovery.integration.test.ts` 4/4 pass (manual compatibility);
  pure suites (policy, candidates, health-monitor, reservation) 40/40 pass.
- Worker `tsc --noEmit` and API `tsc --noEmit`: clean. Queue + database packages rebuilt.

## 10. Remaining limitations / activation blockers (unverified)
- Live Redis interruption during an in-flight Docker restart.
- Live PostgreSQL interruption after restart but before finalization.
- Actual worker termination during an in-flight Docker operation.
- Exactly-once Docker execution is **not** claimed. Production activation is outside
  this subphase.

## 11. Phase 5.8.4 readiness
Reservation, durable publication, reconciliation, and processor integration are
implemented and validated in isolation with automatic recovery disabled by default.
5.8.4 may proceed to controlled activation planning once the blockers above are
exercised against real fault injection.