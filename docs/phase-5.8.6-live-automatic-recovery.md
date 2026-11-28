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
