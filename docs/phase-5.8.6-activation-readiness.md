# Phase 5.8.6.D — Automatic Recovery Activation Readiness

Baseline commit: `6f58bae` (`test: validate live worker crash resilience and BullMQ stalled recovery`) on `main`. Working tree was clean at the start of this phase. This is a read-only audit and documentation checkpoint; no source, schema, migration, or primary configuration was changed, and primary automatic recovery remains disabled.

## 1. Architecture summary

Automatic recovery is a six-stage pipeline layered on the existing manual recovery machinery. Every stage is durable-first and revalidates all safety conditions inside a PostgreSQL transaction before any Docker call.

1. **Eligibility policy** — `apps/worker/src/health/auto-recovery-policy.ts`. Pure, ordered evaluator; first failing rule yields a stable rejection reason (`evaluateAutoRecovery`, lines 87–121).
2. **Health monitoring scheduler** — `apps/worker/src/health/health-monitor.ts` drives due checks; `health-monitor-runtime.ts` runs the read-only `afterBatch` discovery/reservation hook (lines 87–148) and the reconciliation `maintenance` hook (lines 76–86).
3. **Candidate discovery** — `apps/worker/src/health/auto-recovery-candidates.ts`. Bounded, read-only SQL (lines 149–219) pre-filters to current, trusted, eligible, unhealthy runtimes; `evaluateCandidateRows` (lines 77–140) makes the advisory decision.
4. **Durable reservation** — `packages/database/src/recovery-store.ts` `reserveAutomaticRecovery`/`reserveRecovery` (lines 214–335). Revalidates runtime, health generation/version, freshness, budget, cooldown, circuit, and active/conflicting operations inside one transaction; writes a `REQUESTED` attempt plus `RECOVERING` health state and a `DEPLOYMENT_RECOVERY_REQUESTED` audit row carrying the circuit state.
5. **Circuit breaker** — `packages/database/src/auto-recovery-circuit.ts` via `inspectAutomaticRecoveryCircuit` (`recovery-store.ts:154–178`). CLOSED / OPEN / HALF_OPEN with shared budget, cooldown, and `manualInterventionRequired`.
6. **Queue publisher + reconciliation** — `apps/worker/src/health/auto-recovery-publisher.ts`. Idempotent add-by-job-id (`publishRecoveryJob`, lines 56–108) and bounded reconciliation of missing/terminal jobs and orphaned `RECOVERING` states (`reconcileUnpublishedAutoRecoveries`, lines 115–189).
7. **Recovery processor** — `apps/worker/src/health/recovery-processor.ts`. Claims the durable attempt under a runtime lease, persists restart intent, and executes Docker only through `verifyRecoveryRestart`.
8. **Docker recovery operations** — `apps/worker/src/health/recovery-operations.ts` `verifyRecoveryRestart` (lines 19–90). Identity/port fencing, durable-intent fence, authoritative Docker + HTTP verification.
9. **Leases** — recovery-attempt lease and runtime execution lease, both `recoveryPolicy.leaseMs` (120 s, `recovery-store.ts:6`), acquired/released under `lockRecoveryRuntime` (`recovery-store.ts:36–43`).
10. **Capability reporting / API / dashboard** — worker heartbeat reports `automaticRecovery` capability (`worker.ts:1190–1198`); API derives availability (`apps/api/src/services/automatic-recovery-availability.ts`) and snapshot (`automatic-recovery-snapshot.ts`).

The job contract (`packages/queue/src/contracts/runtime.ts`) carries `recoveryTrigger` (`MANUAL` default, `AUTOMATIC` set only by the scheduler; lines 354–393). Payloads carry identifiers and trusted metadata only — never secrets.

## 2. Safety invariant audit

Each invariant lists source references and supporting test/live evidence. "Unit" = `apps/worker/test/*.test.ts`; "Integration" = `*.integration.test.ts` (opt-in, disposable DB/Redis); "Live" = `*.live.ts` (real Docker).

| # | Invariant | Source references | Evidence |
| --- | --- | --- | --- |
| 1 | Automatic recovery disabled by default | `env.ts:613` (`booleanEnv(false)`); `auto-recovery-policy.ts:90`; `health-monitor-runtime.ts:90` (early return) | Unit "flag precedes everything"; Gate A disabled control (no reservation, no restart) |
| 2 | Worker config independently controls execution authorization | `recovery-processor.ts:330–339` (AUTOMATIC needs `RUNTIME_ENABLED`+schema; disabled cancels unstarted), `:176` (`markRestart` re-checks flag); `worker.ts:1191–1194` | Integration scheduling disablement (CANCELLED, 0 Docker calls); `recovery-fault-gate.test.ts` |
| 3 | Candidate discovery uses current health generation and version | `auto-recovery-candidates.ts:170–184` (NOT EXISTS newer generation/attempt), `:208–209`; `recovery-store.ts:274–275` (`expectedHealth`) | Unit/integration `auto-recovery-candidates.*` |
| 4 | Stale candidates cannot authorize recovery | `auto-recovery-policy.ts:104–105`; `recovery-store.ts:278–282` (`automaticFreshnessMs`, failure threshold); `auto-recovery-candidates.ts:184` | Unit "stale health observation rejects"; reservation integration |
| 5 | Ownership, repo trust, Docker identity, image, host-port fencing enforced | `recovery-processor.ts:77–87` (`runtimeTarget`), `recovery-store.ts:61–70` (`isRecoveryRuntimeEligible`), `recovery-operations.ts:38–42` (`RECOVERY_IDENTITY_MISMATCH`/`RECOVERY_PORT_CHANGED`), `probe-runtime.ts:50–80` | Unit `recovery-operations.test.ts`; integration `recovery.integration.test.ts` (hostPort) |
| 6 | Manual and automatic cannot execute conflicting operations | `recovery-store.ts:267–269` (busy on active), `recovery-processor.ts:384–390` (`withRuntimeLifecycleLease` busy), `claimRecovery` lock | Integration `recovery.integration.test.ts` busy; scheduling auto-busy-when-manual; crash `M1` one success |
| 7 | Durable restart intent persisted before Docker issuance | `recovery-operations.ts:50–52` (`markIntent` before restart), `recovery-processor.ts:189–193` (sets `restartIssuedAt`+`restartSourceStartedAt`), `:357` | Integration crash `E`/`E2`/`E3`; Live C1/C3 `intentAt` persisted pre-restart |
| 8 | Persisted intent prevents blind repeated Docker execution | `recovery-operations.ts:48,50,69,74` (sourceEpoch fence; unchanged epoch → uncertain) | Integration crash `E`/`E3` (0 restarts); Live C2 (0 restarts) |
| 9 | Success requires authoritative Docker + HTTP verification | `recovery-operations.ts:69–89` (after-snapshot + 3 probes, `startedAt` match), `probe-runtime.ts:82–96` | Unit `recovery-operations.test.ts`; integration `recovery.integration.test.ts` |
| 10 | Generation advances only after valid success | `recovery-processor.ts:219–241` (`RECOVERY_VERIFIED`, new generation only on `result.ok`) | Integration crash `E` (gen stays 0), `E4` (no duplicate); Live C2 (gen 0) |
| 11 | Budget and cooldown enforced atomically | `recovery-store.ts:292–300` (exhausted/cooldown inside tx), `:147–152` | Unit "exhausted budget and cooldown reject"; scheduling integration |
| 12 | OPEN/HALF_OPEN circuit safe across workers | `recovery-store.ts:154–178`, reservation `:302–308`, `recovery-processor.ts:182–185` (re-check + authorization marker) | Unit/integration `auto-recovery-circuit.*` |
| 13 | Uncertain outcomes require safe intervention | `recovery-operations.ts:74–75`; `recovery-processor.ts:225–234` (unsafe binding → FAILED, no next check); `recovery-store.ts:167–169` | Integration crash `E` (`RECOVERY_RESTART_UNCERTAIN`, FAILED); Live C2 (OPEN, manual required) |
| 14 | Missing/terminal queue jobs reconciled | `auto-recovery-publisher.ts:115–189` (republish + orphan repair), `:81–88` (retry terminal) | Integration scheduling reconciliation; `recovery-stalled.integration.test.ts` |
| 15 | Disabling safely handles unstarted and already-claimed work | `recovery-processor.ts:286–323` (`cancelUnstartedAutomatic`; never cancels issued intent), `:330–336`; publisher `:133,159–161` | Integration scheduling disablement; crash `E` (issued intent reconciled, not cancelled) |
| 16 | Expired leases cannot authorize stale writes | `recovery-processor.ts:149–153` (`ownedAttempt` gt now), `:114,132` (busy), `:160–166` (`revalidate`), `:202–208` | Integration `recovery.integration.test.ts` (expired → skipped); `recovery-resilience.integration.test.ts`; crash |
| 17 | Manual recovery compatibility preserved | `recovery-store.ts:193–197`; `recovery-processor.ts:337–339`; `contracts/runtime.ts:391–393` (default MANUAL) | Integration `recovery.integration.test.ts`, crash, stalled (all MANUAL) |
| 18 | No test-only crash/fault instrumentation in prod/dev | `recovery-processor.ts:39–46` (`maybeCrash`), `:60–75` (`maybeFault`/`recoveryFaultArmed`) — all require `NODE_ENV==="test"` + flag + stage | Integration crash "inert outside NODE_ENV=test" (production/development exit 0); `recovery-fault-gate.test.ts` |

All 18 invariants are satisfied in source and covered by committed tests. No production defect was identified.

## 3. Activation gate results (committed evidence)

Full detail is in `docs/phase-5.8.6-live-automatic-recovery.md`. Summary:

### Gate A — real scheduler-triggered automatic Docker recovery
- Three independent successful recoveries (`cp586a-00f2ec97`, `cp586a-6f4d0127`, `cp586a-e7ea7905`), each one Docker restart, unchanged container ID/image/host port, generation 0→1, budget 2/3, circuit CLOSED.
- Disabled control (`cp586a-9fdbaac8`): reached UNHEALTHY, five HTTP 503 probes, no reservation, no BullMQ job, unchanged Docker StartedAt, final HTTP 503.

### Gate B — live Redis and PostgreSQL interruption
- Three Redis interruptions during the Docker restart (`cp586b-b1-*`): single restart inside the fault window, `attemptsStarted=1` (no redelivery), generation 0→1, CLOSED.
- Three PostgreSQL interruptions after restart before finalization (`cp586b-b2-*`): no second restart while paused, intent reused, SUCCEEDED only after authoritative verification, `attemptsStarted=1`, CLOSED.

### Gate C — real worker termination + BullMQ stalled redelivery
- Three independent C1 in-flight termination runs (`cp586-c1-8fd79b43`, `cp586-c1-e589caa7`, `cp586-c1-ac677de7`): real SIGKILL, BullMQ `stalled` redelivery, replacement lease after expiry, one restart, SUCCEEDED, generation 1, CLOSED.
- One C2 pre-issuance termination (`cp586-c2-772e8424`): 0 restarts, FAILED / `RECOVERY_RESTART_UNCERTAIN`, generation 0, circuit OPEN, manual intervention required, `originalStartedAt == finalStartedAt`.
- One C3 post-restart/pre-finalization termination (`cp586-c3-ebaf4353`): restart completed before crash, 1 restart, SUCCEEDED, generation 1, CLOSED.

### Evidence classification
- **Observed**: Docker StartedAt, container ID/image/port, HTTP status, BullMQ job state/attempts, stalled stream IDs, recovery attempt status/failureReason, health generation, circuit state, worker PIDs, timestamps read directly from Docker/Redis/PostgreSQL.
- **Derived (exact)**: the replacement-claim time is computed as `leaseExpiresAt − 120000 ms` (`automatic-recovery-fault.live.ts:353`). This is an exact reconstruction, not an approximation, because the claim transaction sets `leaseExpiresAt = recoveryNow(tx) + recoveryPolicy.leaseMs` (`recovery-processor.ts:134`, `recovery-store.ts:6`, 120 s). It is not a directly observed wall-clock event; the assertion `replacementClaim.at >= original.leaseExpiresAt` (`:360`) is therefore meaningful.
- **Not claimed**: exactly-once Docker execution. Redelivery after lease expiry re-verifies and finalizes but never re-issues a restart when durable intent exists.

## 4. Regression evidence (this phase)

- Focused unit suite (`auto-recovery-policy`, `auto-recovery-candidates`, `auto-recovery-circuit`, `recovery-operations`, `recovery-fault-gate`): **48 pass / 0 fail**, exit 0.
- Worker typecheck (`npm run worker:typecheck`): exit 0.
- Existing committed integration/live evidence reused per phase instructions (no long-running live scenario repeated).

## 5. Known limitations and severity

| Limitation | Severity | Notes |
| --- | --- | --- |
| Exactly-once Docker execution not guaranteed | Medium | Durable intent prevents a second issuance; uncertain outcomes fail closed to manual intervention. Acceptable for local activation with monitoring. |
| Early BullMQ redelivery (before lease expiry) not exercised live | Low | Covered by integration: `claimRecovery` returns `busy` (`recovery-processor.ts:114,132`), processor retries within a 90 s deadline (`:341–349`), never issuing an unauthorized restart; `M1` multi-worker contention yields one success. No orphaned `RECOVERING` (reconciler repairs, publisher `:169–187`). |
| Replacement-claim timestamp is derived | Low | Exact reconstruction from `leaseExpiresAt − 120 s`; documented above. Not a directly observed event. |
| Parallel multi-file integration flake | Low | One transient auto-exec failure from shared Redis/DB in a single parallel run; deterministic and green in the intended per-file sequential mode. Test interference, not a product defect. |
| Configuration-dependent skips | Low | Enabled (12 pass / 4 skip) + disabled (13 pass / 3 skip) scheduling runs collectively exercise all 16 cases; the 4 enabled-mode skips are disablement-path cases intentionally exercised in disabled mode. |
| Activation is global, not deployment-scoped | High (scope) | See §6. There is no per-deployment canary; the flag applies to all eligible trusted deployments at once. |

## 6. Configuration requirements and activation blast radius

Required effective primary configuration (verified read-only this phase):

```
RUNTIME_ENABLED=true
HEALTH_PERSISTENCE_ENABLED=true
HEALTH_MONITORING_ENABLED=true
MANUAL_RECOVERY_ENABLED=true
AUTO_RECOVERY_ENABLED=false        # remains disabled
RUNTIME_TRUSTED_LOCAL_ONLY         # unset → defaults true
BUILD_TRUSTED_REPOSITORIES=raziquehasan/cloudpilot-demo-app,raziquehasan/cloudpilot-recovery-validation
```

Infrastructure verified read-only: demo runtime `127.0.0.1:53143` HTTP 200; recovery-validation runtime `127.0.0.1:63299` HTTP 200; primary PostgreSQL `127.0.0.1:5433` (healthy); primary Redis `127.0.0.1:6379` (healthy); API `127.0.0.1:8000` HTTP 200; frontend `127.0.0.1:3000` HTTP 200; primary worker PID 46964 and API/frontend processes up. Read-only DB counts: `auto_active=0`, `recovering=0`, `auto_total=0` (no pending automatic reservations or orphaned RECOVERING states). Effective automatic-recovery capability reports `configured=false` → `DISABLED`.

**Activation scope (explicit):** `AUTO_RECOVERY_ENABLED` is a single global worker flag. When enabled, automatic recovery applies to **every** currently-eligible trusted deployment simultaneously — i.e. all `RUNNING` trusted-local runtimes whose repository is in `BUILD_TRUSTED_REPOSITORIES`. It is **not** a deployment-scoped canary. A deployment-scoped canary would require new scope-control features (per-deployment authorization/allow-list and selective scheduling) that are **not implemented**; that is reported as a blocker for canary-style rollout, not silently added here.

**Blast radius:** all eligible trusted deployments across the configured trusted repositories; each unhealthy generation can trigger at most one automatic restart per the shared budget (3 per 15 min) and circuit. Untrusted repositories and non-loopback runtimes are never eligible.

## 7. Controlled activation runbook (do not execute in this phase)

1. **Preflight**: confirm the §6 configuration; confirm protected infra healthy (ports 53143/63299/5433/6379/8000/3000); confirm `auto_active=0` and `recovering=0`; confirm the trusted repository set is minimal and correct.
2. **Backup**: snapshot the primary database (and note current health generations/versions) before any change.
4. **Enable**: set `AUTO_RECOVERY_ENABLED=true` in the worker environment and restart **only the worker** so the capability is re-reported. Do not restart protected services.
3. **Eligibility/blast-radius review**: enumerate the trusted repositories and the deployments that would become eligible; confirm the operator accepts global scope.
5. **Initial observation**: watch worker `auto_recovery` logs (`evaluated/eligible/reserved/published/publicationFailed`), recovery attempts, circuit state, and health generations for a defined window.
6. **Monitoring**: track recovery history, circuit OPEN/HALF_OPEN transitions, and any `RECOVERY_RESTART_UNCERTAIN` (manual intervention required).
7. **Abort conditions**: unexpected restart of a non-target deployment, repeated uncertain outcomes, circuit flapping, or any protected-infra impact → disable per §8.
8. **Post-change verification**: confirm capability reports `AVAILABLE` for the intended repositories and no unintended reservations.

## 8. Safe disablement and rollback runbook

1. Set `AUTO_RECOVERY_ENABLED=false` and restart only the worker.
2. On the next reconciliation pass, unstarted `REQUESTED`/unleased `RUNNING` automatic attempts are cancelled (`cancelUnstartedAutomatic`, `recovery-processor.ts:286–323`) and their `RECOVERING` health states are returned to `UNHEALTHY` with a fresh `nextCheckAt`.
3. **Already-persisted restart intents are never cancelled**; an in-flight recovery is allowed to reconcile to a terminal state (SUCCEEDED/FAILED) by the replacement worker. Do not force lease expiry.
4. **Uncertain outcomes** (`RECOVERY_RESTART_UNCERTAIN`): circuit goes OPEN with `manualInterventionRequired=true`; a human verifies the container and uses manual recovery. Do not re-enable automatic recovery until the uncertainty is resolved.
5. **Post-disable verification**: capability reports `DISABLED`; `auto_active=0`; no new automatic reservations; protected infra unchanged.

## 9. Final readiness decision

**READY WITH CONDITIONS.**

The implementation is safe, durable-first, and validated by Gates A/B/C plus the 18-invariant audit and the fresh 48-test unit run. Conditions for controlled **local** activation:

1. Activation is **global across all eligible trusted deployments**, not a deployment-scoped canary. The operator must explicitly accept this blast radius. If a per-deployment canary is required, that is a **blocker** needing new scope-control features (not implemented in this phase).
2. The trusted repository set must be minimal and correct (`BUILD_TRUSTED_REPOSITORIES`) before enabling.
3. Exactly-once Docker execution is not claimed; monitoring and the abort/disable runbook (§7–§8) must be in place, and uncertain outcomes require manual intervention.
4. A database backup and the §7 preflight must be completed first.

Production readiness is outside the scope of this phase.

## 10. Required user approvals before activation

- Explicit authorization to set `AUTO_RECOVERY_ENABLED=true` on the worker (global scope accepted).
- Confirmation of the minimal trusted repository set.
- Approval of the observation window and abort thresholds.

## 11. Primary automatic recovery status

Primary `AUTO_RECOVERY_ENABLED` remains **false**. No primary data, configuration, or protected service was modified during this phase. No commit or push was performed.