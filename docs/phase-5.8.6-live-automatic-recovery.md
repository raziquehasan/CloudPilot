# Phase 5.8.6.A Live Automatic Docker Recovery

Baseline was clean at `5488913` on `main`. The only changes for this phase are the live validation harness and this report.

## Isolation

Each invocation creates a uniquely named PostgreSQL 16 container and database, a Redis 7 container and BullMQ prefix, a workspace/project/deployment/runtime fixture, a dedicated application container, a temporary workspace directory, and a child running the normal CloudPilot worker entry point. Docker 29.9.3 supplies dynamically assigned loopback database and Redis ports. The harness checks the connection targets against the exact disposable Docker port bindings and rejects protected ports before launching the worker. Existing Prisma migrations run only against the disposable database.

The in-memory validation fixture uses the existing `raziquehasan/cloudpilot-recovery-validation` trust entry and the production ownership label builder. The fixture binds its fixed, per-run host port only on loopback. `GET /health` initially returns 200, a local `POST /fail` changes it to 503, and a process restart restores 200. The phase enables automatic recovery only for enabled child workers. Health persistence, monitoring, and manual recovery are enabled in both control and enabled children. No manual attempt reservation, queue publication, processor invocation, or external Docker restart is used by the harness.

## Commands

From the repository root, with Docker available and the local PostgreSQL, Redis and Node images installed:

```powershell
$env:CLOUDPILOT_RUN_LIVE_RECOVERY='1'
node --import tsx apps/worker/test/automatic-recovery.live.ts enabled
node --import tsx apps/worker/test/automatic-recovery.live.ts enabled
node --import tsx apps/worker/test/automatic-recovery.live.ts enabled
node --import tsx apps/worker/test/automatic-recovery.live.ts disabled
npm run worker:typecheck
node node_modules/typescript/bin/tsc --noEmit --module NodeNext --target ES2023 --strict --noUncheckedIndexedAccess --exactOptionalPropertyTypes --esModuleInterop --skipLibCheck apps/worker/test/automatic-recovery.live.ts
node --import tsx --test --test-reporter=spec apps/worker/test/auto-recovery-circuit.test.ts apps/worker/test/auto-recovery-candidates.test.ts apps/worker/test/health-monitor.test.ts
```

The opt-in variable arms the harness. It does not enable automatic recovery in the calling shell or primary worker.

## Execution and assertions

The harness waits for a generation-zero HTTP 200 probe from the real scheduler, injects the fixture failure, and checks HTTP 503. The scheduler detects the third failure, evaluates eligibility, and calls the existing locked circuit reservation. PostgreSQL commits the automatic restart reservation and RECOVERING state. Worker logs confirm eligibility and publication. BullMQ records the deterministic recovery job, and the runtime worker claims it. The processor commits restart intent and the source Docker epoch before calling Docker. The harness checks actual Docker start-time advancement, a Docker restart event filtered to the fixture ID, and an unchanged container ID, image and port binding. It then verifies the succeeded record, preserved health history, exactly one generation advance, reset failures, cleared leases, shared budget consumption, and continued generation-one probes. Fast states are evidenced by durable records and worker logs when polling misses the transition.

The disabled control reached UNHEALTHY and recorded five HTTP 503 probes over 80.329 seconds. It stayed at generation 0 with no reservations or BullMQ jobs, unchanged Docker StartedAt, and a final HTTP 503. It exited 0 and removed its resources.

## Harness corrections

A startup pilot exited 1 before its first probe. The harness override misspelled LOG_STREAMING_ENABLED, leaving the inherited log streaming setting on alongside disabled deployment logs. The corrected harness uses the actual variable and emits structured worker logs. Three subsequent enabled pilots all reached worker recovery settlement and BullMQ completion, but the harness exited 1 in its source-epoch assertion because it called getTime on a persisted Docker timestamp string. The assertion now compares Docker's exact nanosecond timestamp string. A separate harness TypeScript check also identified and removed an unused buildId field passed to the runtime label helper. These were validation harness defects. Runtime, scheduler and schema code were unchanged.

## Enabled results

All three independent final invocations exited 0. Each recorded three HTTP 503 checks and observed HEALTHY, DEGRADED, UNHEALTHY, RECOVERING, REQUESTED, RUNNING, and SUCCEEDED. Worker logs recorded one evaluated candidate, one eligible candidate, one durable reservation, and one published job. Each job ID was recovery- followed by the listed recovery ID. BullMQ reported completed with attemptsStarted=1. Each stored attempt was AUTOMATIC / RESTART / SUCCEEDED with one claim and committed health version 5. The generation advanced 0 to 1, and the rolling budget had 2 of 3 attempts remaining. There were three successful generation-one health records per run, including two subsequent scheduled probes. Runtime and recovery ownership leases were cleared.

| Run identifier | Recovery ID | PostgreSQL / Redis / app loopback ports | Failure to settlement | Claim to settlement |
| --- | --- | --- | --- | --- |
| cp586a-00f2ec97-a2d9-448c-851d-9684ce37fbc9 | 9d69ac99-c353-40f4-8512-0a2533a6590b | 61690 / 63406 / 56975 | 62.551 s | 13.643 s |
| cp586a-6f4d0127-7ab4-445c-b12b-5e3871ecadfb | 07b3233e-66e5-4408-a302-481c46ddb039 | 61694 / 63409 / 56976 | 62.248 s | 13.644 s |
| cp586a-e7ea7905-24bb-4a1e-9eec-109931cf911b | 94c357bd-e8b3-4fc1-9c97-ad2e1e7b3c0f | 52196 / 50589 / 56977 | 62.004 s | 13.555 s |

| App port | Runtime ID | Container ID prefix | Persisted source StartedAt | Persisted restart intent | Final StartedAt |
| --- | --- | --- | --- | --- | --- |
| 56975 | e2140422-d80d-4302-9ad4-292ad14411de | 614955a783c8 | 2026-11-28T15:10:42.655066705Z | 2026-11-28T15:11:38.187Z | 2026-11-28T15:11:49.950698145Z |
| 56976 | 632b6ddd-8112-4b7e-811d-651cac20cd22 | 9900180baf3c | 2026-11-28T15:10:42.540514317Z | 2026-11-28T15:11:38.264Z | 2026-11-28T15:11:50.114155431Z |
| 56977 | 39f843a0-bd3a-4f8a-9f59-0674af9c8015 | 9561db5ba59c | 2026-11-28T15:10:42.585376966Z | 2026-11-28T15:11:38.176Z | 2026-11-28T15:11:49.763353039Z |

The full per-run container IDs and the image ID were captured from Docker. The image was sha256:1314fb30ec37592d5845e5a160d522c0e680972aa534549940b8d73bde88481a in all runs. Every successful run preserved the container ID, image, fixed host binding, and returned HTTP 200. Exactly one Docker restart event appeared for each container within its recorded observation window. StartedAt stayed fixed through the two follow-up probes, and PostgreSQL still held one recovery row per deployment. These measurements establish one observed restart per run in the tested window.

The disabled control used queue prefix cp586a-9fdbaac8-a94d-4105-8a49-302badf0e53e and loopback ports 52320 / 62381 / 53254. Docker StartedAt remained 2026-11-28T15:08:26.17788213Z.

Worker TypeScript and the final standalone harness TypeScript check exited 0. The focused circuit/candidate/monitor suite exited 0 with 33 passes, zero failures and zero skips. The four fully validated scenarios exited 0. The four preliminary harness failures exited 1 and are described above.

## Protected resources and cleanup

| Protected service | Port | Docker container ID prefix | Image ID prefix | StartedAt | Final evidence |
| --- | --- | --- | --- | --- | --- |
| Demo runtime | 53143 | f9144585eb87 | 15b896427fd3 | 2026-11-27T16:36:43.784631096Z | HTTP 200 |
| Recovery validation runtime | 63299 | 64376e6449f1 | ec6f7b2f9d1a | 2026-11-28T08:30:23.100977192Z | HTTP 200 |
| Primary PostgreSQL | 5433 | f887ca72cb1b | 5fe8ca7fc662 | 2026-11-26T14:42:40.970153667Z | Exact container, image, start and port match |
| Primary Redis | 6379 | 3e42c98d87c9 | 02f2cc4882f8 | 2026-11-27T11:18:18.685280548Z | Exact container, image, start and port match |

Before and after comparison used full Docker identities and exact loopback mappings. API and web listeners retained their original processes on 8000 and 3000. The primary worker PIDs and start times remained unchanged. The root environment file hash matched and AUTO_RECOVERY_ENABLED remained false.

Each of the four validated workers shut down gracefully with exit 0. The tagged-container inventory was empty after cleanup. All six anonymous volumes recorded for the final enabled runs were absent, and all temporary workspaces were absent. The disposable Redis instances and their per-run queue prefixes were removed with the containers and volumes.

## Remaining activation gates

- Live Redis interruption during the Docker restart.
- Live PostgreSQL interruption after restart and before finalization.
- Real worker termination during an in-flight Docker operation.

## Phase 5.8.6.B — Live infrastructure fault injection

Baseline `7bb40ab`. This section validates the first two activation gates above with real Docker, PostgreSQL, Redis, BullMQ and isolated worker processes. The third gate (worker termination) remains 5.8.6.C.

### Isolation configuration
`apps/worker/test/automatic-recovery-fault.live.ts` reuses the 5.8.6.A harness isolation: a uniquely named PostgreSQL 16 container + database, a Redis 7 container, a per-run BullMQ prefix, a dedicated workspace/project/deployment/runtime, a dedicated application container on a fixed unused loopback port, and a child running the normal worker entry point. Automatic recovery is enabled only in the child environment (`AUTO_RECOVERY_ENABLED=true`); the caller shell and primary worker remain disabled. The harness asserts every connection target equals the disposable Docker port binding, rejects protected ports (3000/8000/5433/6379/53143/63299), and snapshots the protected `cloudpilot-postgres`/`cloudpilot-redis` containers before and after (must be identical).

### Fault injection boundaries (test-only, strictly gated)
`recovery-processor.ts` adds `maybeFault(stage)` + `releaseRecoveryFault()` + `recoveryFaultArmed(stage)`. It is inert unless `NODE_ENV==="test"` AND `CLOUDPILOT_RECOVERY_FAULT_TEST==="1"` AND `CLOUDPILOT_RECOVERY_FAULT_BOUNDARY===stage`; it only holds execution at an existing boundary and never bypasses identity, ownership, trust or restart fencing. The child's IPC handler calls `releaseRecoveryFault()` on `release-fault`.
- B1 pauses at `after-intent` (durable `restartIssuedAt` committed, before the Docker restart call). The harness pauses the disposable Redis, then releases; the real Docker restart runs and completes while Redis is down.
- B2 pauses at `after-restart` (Docker restart completed, before finalization). The harness pauses the disposable PostgreSQL, then releases; finalization blocks until restore.

### Scenario B1 — Redis interruption during Docker restart (3 runs, all pass)
Redis was paused before Docker issuance and held through completion; the single Docker restart event timestamp falls inside the Redis fault window in every run. BullMQ reported `completed` with `attemptsStarted=1`/`attemptsMade=1` — redelivery did **not** occur (Redis was restored before the stalled-job threshold; the already-active job completed on first attempt after restore). No duplicate reservation, no second restart, identity/image/port unchanged, generation 0→1, HEALTHY, budget 2/3, circuit CLOSED.

| Run | Recovery ID | Redis fault window (ms) | Docker restart at | Intent at | Final StartedAt | Restarts | Job |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cp586b-b1-7d676717 | 6e833729-cb47-4a9c-947f-fd802fc62ab7 | 17852 | 15:21:54.000 (inside) | 15:21:39.480 | 15:21:53.363 | 1 | completed/1 |
| cp586b-b1-03ae395a | 2ad13c19-8a14-49bc-bc00-3cf89dd43254 | 17025 | 15:23:59.000 (inside) | 15:23:44.748 | 15:23:58.381 | 1 | completed/1 |
| cp586b-b1-a13baae4 | 16dee004-717c-4493-a027-1e55768fa015 | 17327 | 15:25:51.000 (inside) | 15:25:37.153 | 15:25:50.915 | 1 | completed/1 |

### Scenario B2 — PostgreSQL interruption after restart, before finalization (3 runs, all pass)
PostgreSQL was paused only after the Docker restart completed (`restartAt < faultStart`) and before finalization committed (`finishedAt > faultEnd`). While paused the worker issued no second restart (StartedAt unchanged) and fabricated no success. After restore the existing intent was reused, the source epoch was preserved, and the attempt reached SUCCEEDED only after authoritative verification. `attemptsStarted=1` (no redelivery). Identity/image/port unchanged, generation 0→1, HEALTHY, budget 2/3, circuit CLOSED.

| Run | Recovery ID | PG fault window (ms) | Restart at (< fault) | Finalized at (> restore) | Intent at | Restarts | Job |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cp586b-b2-ebd9e89e | e8aa5b1e-6c33-4a6c-9895-3700aad2dc46 | 5370 | 15:30:50.000 | 15:31:33.544 | 15:30:38.918 | 1 | completed/1 |
| cp586b-b2-43afd766 | dd8a2888-89fe-49b4-b93f-df4d9d93cbcc | 6753 | 15:34:10.000 | 15:34:53.700 | 15:33:58.738 | 1 | completed/1 |
| cp586b-b2-1e390921 | db9d410d-33e9-4a2a-b31c-aa1e76bfb941 | 5367 | 15:36:43.000 | 15:37:25.677 | 15:36:31.738 | 1 | completed/1 |

### Defects and fixes
One preliminary B2 run (`cp586b-b2-4b450625`) exited 1 on a **harness assertion** that required the Docker restart to fall inside the fault window; for B2 the PostgreSQL window is after the restart, so the correct proof is `restartAt < faultStart` and `finishedAt > faultEnd`. The recovery itself succeeded (worker logged `Automatic recovery settled`, `failureReason:null`). The harness assertion was corrected to be mode-specific and the scenario re-run. No product/runtime/schema code defect was found; the only source change is the test-only fault gate.

### Commands and exit codes
```powershell
$env:CLOUDPILOT_RUN_LIVE_RECOVERY='1'
node --import tsx apps/worker/test/automatic-recovery-fault.live.ts b1   # x3, exit 0
node --import tsx apps/worker/test/automatic-recovery-fault.live.ts b2   # x3, exit 0
npm run worker:typecheck                                                 # exit 0
node node_modules/typescript/bin/tsc --noEmit --module NodeNext --target ES2023 --strict --noUncheckedIndexedAccess --exactOptionalPropertyTypes --esModuleInterop --skipLibCheck apps/worker/test/automatic-recovery-fault.live.ts  # exit 0
node --import tsx --test apps/worker/test/recovery-fault-gate.test.ts    # 3 pass, exit 0
# Focused integration (disposable DB, sequential per file):
node --import tsx --test apps/worker/test/auto-recovery-scheduling.integration.test.ts   # 12 pass / 4 skip, exit 0
node --import tsx --test apps/worker/test/auto-recovery-circuit.integration.test.ts      # 4 pass, exit 0
node --import tsx --test apps/worker/test/auto-recovery-candidates.integration.test.ts apps/worker/test/recovery.integration.test.ts  # 6 pass, exit 0
node --import tsx --test apps/worker/test/recovery-fault-gate.test.ts apps/worker/test/auto-recovery-reservation.test.ts apps/worker/test/auto-recovery-policy.test.ts apps/worker/test/auto-recovery-candidates.test.ts apps/worker/test/health-monitor.test.ts  # 43 pass, exit 0
```

A single parallel multi-file integration run showed one transient failure in the auto-exec success test caused by concurrent files sharing one Redis/DB; the same test passes deterministically in isolation and in the intended per-file sequential mode (documented, not a product defect).

### Protected infrastructure and cleanup
Before/after snapshots of `cloudpilot-postgres` (127.0.0.1:5433) and `cloudpilot-redis` (127.0.0.1:6379) were identical; demo (53143), validation (63299), API (8000) and frontend (3000) stayed reachable; root `.env` unchanged with `AUTO_RECOVERY_ENABLED=false`; primary worker untouched. Each run's `finally` unpaused any paused disposable service, shut down the child (exit 0), closed the queue, disconnected Prisma, removed all `cloudpilot.validation=cp586b-*` containers/volumes (label-asserted) and the temp workspace. Post-run inventory of disposable containers was 0; the regression database was dropped and test queue keys purged.

### Remaining limitations
Exactly-once Docker execution is not claimed. BullMQ redelivery was not exercised because bounded interruptions restored the service before the stalled-job threshold; reconciliation of a truly stalled/terminal job remains covered by the 5.8.4 integration tests, not by a live stall in this phase. Real worker termination during an in-flight Docker operation (5.8.6.C) and final activation readiness (5.8.6.D) remain open.

## Phase 5.8.6.C.1 — BullMQ stalled redelivery diagnosis

Baseline HEAD was 554f710. The existing uncommitted C1/C2/C3 harness work was preserved. The diagnostic and acceptance code is test-only. No runtime code, package policy, schema, or migration changed.

### Minimal reproduction

BullMQ 6.7.9 ran against a disposable Redis 7 instance on a unique loopback port and prefix. Worker A started the only job, held a 1,600 ms lock, and was hard-killed. The initial probe incorrectly used the unavailable queue.client handle for raw Redis reads. The dedicated read-only ioredis observer corrected that harness error. With a 700 ms stalled interval and one permitted stall, TTL declined from 1,560 ms to 9 ms, then Redis returned -2. The job stayed active briefly until BullMQ emitted waiting and stalled, then Worker B activated and completed the original job. The final attempt counters were 2 started and 1 made, and the Redis stall counter was 1. No observer manipulated any queue key.

### Effective options and root cause

A live diagnostic explicitly recorded runtime consumer readiness from both actual workers on the same runtime queue, disposable prefix, 127.0.0.1:63095, Redis database 0. Both had autorun enabled, concurrency 1, no pause, lock renewal and stalled detection enabled, a 30 s stalled interval, and maxStalledCount 1. Both reported a 600 s lock and 300 s renewal period. Immediately after Worker A exited, the original job was still in the active list with a 597.934 s lock TTL and attemptsStarted=1. Subsequent TTLs included 593.458 s, 588.416 s, and 497.521 s. This is a positive, decreasing lock TTL that outlasted the 540 s observation deadline. It was never eligible to be moved under those deadlines.

The worker computes its runtime policy from the larger of the shared queue timeout and the runtime startup timeout plus 120 s. QueueManager also applies the 180 s runtime execution floor and then doubles the effective timeout to obtain the lock. The shared queue timeout defaults to 300 s. The old startup-only override left that default in control. The corrected harness sets both inputs for disposable crash children, asserts the actual 360 s lock and 180 s renewal period, and gives the original lock plus two stall-check intervals room in the 540 s bound. An earlier corrected preflight expected 250 s and exited before injection when it measured the 360 s runtime floor. The acceptance harness explicitly accounts for that floor.

PostgreSQL uses separate recovery and runtime execution leases, both expiring 120 s after the original claim. The recovery processor waits on a busy lease and retries its claim while its queue execution remains valid. Reconciliation can also look up an expired durable reservation and its current queue job. A reconciliation publication count by itself does not establish redelivery, since publication treats an existing active job as already present.

### Commands and fresh regressions

```powershell
$env:CLOUDPILOT_RUN_LIVE_RECOVERY='1'
node --import tsx apps/worker/test/bullmq-stalled.live.ts
node --import tsx apps/worker/test/automatic-recovery-fault.live.ts c1
npm run worker:typecheck
node node_modules/typescript/bin/tsc --noEmit --module NodeNext --target ES2023 --strict --noUncheckedIndexedAccess --exactOptionalPropertyTypes --esModuleInterop --skipLibCheck apps/worker/test/automatic-recovery-fault.live.ts apps/worker/test/bullmq-stalled.live.ts
node --import tsx --test --test-reporter=spec apps/worker/test/recovery-fault-gate.test.ts apps/worker/test/auto-recovery-circuit.test.ts apps/worker/test/auto-recovery-candidates.test.ts
```

The minimal reproduction, worker TypeScript and harness TypeScript checks exited 0. The focused unit suite exited 0 with 33 passes. On a separate disposable PostgreSQL/Redis pair, the scheduling integration file ran sequentially under both flag settings and exited 0 each time. Enabled yielded 12 passes and 4 skips, disabled yielded 13 passes and 3 skips. Together they exercised all 16 cases. The four enabled-mode skips were disabled-before-execution cancellation, disabled terminal-job reconciliation, disabled claimed-work reconciliation, and the disablement-versus-authorization race. The disposable circuit integration file exited 0 with 4 passes. The commands for those runs were node --import tsx --test --test-reporter=spec apps/worker/test/auto-recovery-scheduling.integration.test.ts and node --import tsx --test --test-reporter=spec apps/worker/test/auto-recovery-circuit.integration.test.ts.

### Single C1 acceptance result

The corrected scenario exited 0. The disposable run was cp586-c1-8fd79b43-d075-4c38-9e13-4e2faf6690f0 with PostgreSQL, Redis, and application loopback ports 54060, 53726, and 51974. Both actual runtime consumers reported the same prefix, Redis database 0, a 360,000 ms lock, 180,000 ms renewal, 30,000 ms stall interval, maxStalledCount 1, concurrency 1, autorun enabled, and no pause or disabled checks. Worker A PID 25084 and Worker B PID 35944 proved their identities through the parent-child IPC channel before the crash and replacement observation.

Recovery d0d24569-537c-4a6a-97a6-6a6fdba4c211 used job recovery-d0d24569-537c-4a6a-97a6-6a6fdba4c211. All following times are UTC on 2026-11-28.

| Event | Time or value |
| --- | --- |
| Original Docker StartedAt | 16:41:32.833449506 |
| PostgreSQL restart intent persisted | 16:42:29.275 |
| Docker daemon sent signal 15 to the fixture and the app logged the signal | 16:42:29.813753351 |
| Hard termination requested and child exit confirmed | 16:42:31.215 / 16:42:31.253 |
| Final Docker StartedAt and daemon restart event | 16:42:38.374514603 / 16:42:38.988490449 |
| Original recovery and runtime leases expired | 16:44:28.933 |
| Redis lock TTL progression | 357628, 10440, 5439, 427, then -2 ms |
| BullMQ stalled stream ID | 1795884515481-1 |
| Replacement PostgreSQL claim | 16:48:35.578 |
| Reconciled settlement | 16:48:36.489 |

The Redis stalled-check key had a positive TTL and the job was in the stalled candidate set while the original lock remained positive. At 16:48:33 Redis returned -2 for the job lock while the job was still active. The next stalled check moved the same job through waiting and active before it completed with attemptsStarted=2, attemptsMade=1. Worker B replaced the expired database lease and verified the original restart intent against Docker and HTTP. The recovery settled SUCCEEDED with one Docker restart event, an unchanged container ID/image/port, generation 0 to 1, a CLOSED circuit, two attempts remaining, and cleared leases. The app returned HTTP 200. Redelivery followed lease expiry in this run, so the early-redelivery busy path was not exercised by this C1 scenario.

The earlier instrumented diagnostic exited 1 at its deadline while the original job was still active and its latest sampled TTL was 53.959 s. It shut down its replacement worker and removed its containers. The accepted run also shut down Worker B gracefully and removed the uniquely tagged containers, anonymous volumes, Redis queue data, and temporary workspace. The separate minimal Redis and regression fixtures were removed. Protected container IDs, image IDs, StartedAt and port bindings matched before and after. Both protected apps returned HTTP 200, the API and frontend retained their listeners, and primary worker PID 46964 retained its start time. The root environment hash matched and AUTO_RECOVERY_ENABLED remained false.

## Phase 5.8.6.C — Live worker-termination acceptance (C1/C2/C3)

Baseline HEAD was `554f710`. This section closes the third activation gate (real worker termination during an in-flight Docker operation) with two independent C1 repetitions plus one C2 and one C3 run, all on real Docker, PostgreSQL, Redis, BullMQ and isolated worker processes. The C1/C2/C3 harness and the `bullmq-stalled.live.ts` diagnostic are test-only; no runtime, package-policy, schema or migration code changed in this phase. Automatic recovery stayed disabled in the caller shell and primary worker; it was enabled only inside each disposable child.

### Crash and replacement mechanics
Each run starts Worker A on the normal worker entry point, drives one automatic recovery to the durable-intent boundary, verifies container labels/ownership and identity (child PID ≠ self, parent = harness PID, run match), then hard-kills Worker A with `SIGKILL` and confirms the persisted attempt stays `RUNNING` with `finishedAt:null` and the original lease unchanged. Worker B (`startWorker(false)`) then waits for BullMQ stalled redelivery plus recovery-attempt lease expiry, claims the expired lease, and settles the original job. The replacement claim is asserted to land at or after the original `leaseExpiresAt`, with `attemptsStarted≥2`, a captured `stalled` stream event, reused `restartSourceStartedAt`, cleared leases and a single settled attempt. The 360 s runtime lock (180 s renewal) and the 120 s recovery/runtime leases come from the corrected effective-timeout configuration; the original lock plus two stall-check intervals fit inside the 540 s bound.

Mode semantics follow `verifyRecoveryRestart` (`recovery-operations.ts:50`): when a durable `sourceEpoch` exists it never re-issues Docker, an unchanged epoch yields `RECOVERY_RESTART_UNCERTAIN`, and a fixed non-zero host port is required (`RECOVERY_PORT_CHANGED`).
- C1 (`armed=false`): the Docker restart runs to completion; expect 1 restart, SUCCEEDED, generation 1, circuit CLOSED. Timing fence `issuedAt < crashRequestedAt < crashObservedAt < completedAt`.
- C2 (`armed=true`, boundary `after-intent`): no Docker restart is issued; expect 0 restarts, FAILED / `RECOVERY_RESTART_UNCERTAIN`, generation 0, circuit OPEN, manual intervention required, `originalStartedAt == finalStartedAt`.
- C3 (`armed=true`, boundary `after-restart`): the restart completed before the crash; expect 1 restart, SUCCEEDED, generation 1, circuit CLOSED. Timing fence `issuedAt < completedAt < crashRequestedAt`.

### Results (all pass, exit 0)
All four runs captured a BullMQ `stalled` stream event, settled with `attemptsStarted=2`/`attemptsMade=1`, replaced the lease only after `leaseExpiresAt`, preserved container ID/image/fixed host port, and cleaned up (child exit 0, no leftover containers). All times UTC on 2026-11-28.

| Mode | Run | Recovery ID | Host port | Worker A / B PID | issuedAt | completedAt | crashRequestedAt | Original StartedAt | Final StartedAt | Restarts | Status | Gen | Circuit |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C1 | cp586-c1-e589caa7-baa8-4b7f-a8f5-ddfe89b09453 | f2030fc6-0179-4fce-a9c5-3b1b4e1d1c6a | 57930 | 10316 / 9984 | 16:52:56.178 | 16:53:06.050 | 16:52:57.632 | 16:51:58.358355434 | 16:53:05.203468829 | 1 | SUCCEEDED | 1 | CLOSED |
| C1 | cp586-c1-ac677de7-31cf-427c-bee2-2db6272fde62 | 4629c11a-adbf-4f9d-a920-773ce478b90d | 63439 | 25112 / 26176 | 17:01:30.775 | 17:01:41.032 | 17:01:32.339 | 17:00:33.460233640 | 17:01:40.232408696 | 1 | SUCCEEDED | 1 | CLOSED |
| C2 | cp586-c2-772e8424-dce2-4d2e-a0d8-461f4b9df9ba | f0b449b6-e19c-4bf1-9455-ca38f83b3c1a | 55483 | 35324 / 16796 | null | null | 17:10:01.626 | 17:09:03.346212630 | 17:09:03.346212630 | 0 | FAILED / RECOVERY_RESTART_UNCERTAIN | 0 | OPEN |
| C3 | cp586-c3-ebaf4353-32a5-4ebb-a29b-393473c629cd | 04ebb26a-7998-4f47-8e57-59b8b009316e | 55708 | 9608 / 44916 | 17:17:42.723 | 17:17:53.812 | 17:17:57.998 | 17:16:48.099730426 | 17:17:53.301627438 | 1 | SUCCEEDED | 1 | CLOSED |

Replacement claims landed after lease expiry in every run: C1 #2 at 16:59:01.799 (lease 16:54:55.309), C1 #3 at 17:07:38.099 (lease 17:03:29.732), C2 at 17:16:06.337 (lease 17:11:59.722), C3 at 17:23:59.761 (lease 17:19:42.002). C2 left the attempt FAILED with manual intervention required and the circuit OPEN; C1/C3 each produced exactly one generation advance and a CLOSED circuit. The C1 runs confirm reproducibility of the crash/restart path; C2 confirms the epoch fence suppresses any second Docker issuance and surfaces `RECOVERY_RESTART_UNCERTAIN`; C3 confirms an already-completed restart is verified and finalized despite the worker crash.

### Commands and exit codes
```powershell
$env:CLOUDPILOT_RUN_LIVE_RECOVERY='1'
node --import tsx apps/worker/test/automatic-recovery-fault.live.ts c1   # x2, exit 0
node --import tsx apps/worker/test/automatic-recovery-fault.live.ts c2   # exit 0
node --import tsx apps/worker/test/automatic-recovery-fault.live.ts c3   # exit 0
npm run worker:typecheck                                                 # exit 0
node node_modules/typescript/bin/tsc --noEmit --module NodeNext --target ES2023 --strict --noUncheckedIndexedAccess --exactOptionalPropertyTypes --esModuleInterop --skipLibCheck apps/worker/test/automatic-recovery-fault.live.ts  # exit 0
```

### Protected infrastructure and cleanup
Each run's `finally` shut down Worker B (exit 0), closed the queue, disconnected Prisma, removed all `cloudpilot.validation=cp586-*` containers/volumes (label-asserted) and the temp workspace; post-run disposable-container inventory was empty. The demo (53143), validation (63299), API (8000) and frontend (3000) stayed reachable; root `.env` was unchanged with `AUTO_RECOVERY_ENABLED=false`; the primary worker was untouched.

### Remaining limitations
Exactly-once Docker execution is not claimed; redelivery followed lease expiry in these runs, so the early-redelivery busy path remains covered by the 5.8.4 integration tests rather than a live stall. Final activation readiness (5.8.6.D) remains a separate decision. No commit or push was performed; the harness and this report are left uncommitted pending explicit approval.
