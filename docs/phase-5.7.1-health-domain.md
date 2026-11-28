# Phase 5.7.1 health domain

Application health is tracked separately from deployment and runtime execution status. A health state is identified by the workspace, project, deployment, runtime instance, and a nonnegative generation. The generation is an epoch for observations of that runtime instance. The future lifecycle integration must advance it when previously issued observations become obsolete, including a restart that reuses the same runtime instance ID. It must retire the old schedule and publish the new generation in the same transaction. Phase 5.7.1 does not create health state for existing runtimes.

## Persistence

- `deployment_health_states` has one row per scoped generation. Its due-time index supports scheduling, while timestamps, consecutive counters, a normalized failure reason, and an optimistic version support future result recording.
- `deployment_health_checks` references that exact generation and stores Docker and HTTP outcomes, optional HTTP status and latency, and a check timestamp. Per-generation history and the global retention index support bounded history reads and future cleanup.
- `deployment_recovery_attempts` references the same generation. A caller-provided UUID is idempotent within a deployment, and a partial unique index permits at most one requested or running recovery across that deployment's generations.

All scope relationships use composite foreign keys. Deleting a runtime cascades through its health states to checks and recovery attempts. PostgreSQL checks enforce nonnegative counters, valid HTTP measurements, consistent health timestamps, all-or-none health leases, and recovery lease and lifecycle invariants. Failure reasons are enums. The retention index does not delete history on its own.

## Transitions and policy

The pure worker domain creates an `UNKNOWN` state and accepts an explicit `STARTING` event. Normal checks produce `HEALTHY` on success, `DEGRADED` before the consecutive failure limit, and `UNHEALTHY` at the limit. Success clears the failure streak. Recovery starts explicitly from `UNHEALTHY`; only an explicit verification event can move `RECOVERING` to `HEALTHY`. A failed recovery returns to `UNHEALTHY` while budget remains, and exhaustion ends the generation in `FAILED`. Every event must match the current scope and generation.

The domain defaults are a 15-second monitoring interval, 3-second HTTP timeout, three consecutive failures, and three recovery attempts per 15-minute window. The transition function accepts threshold overrides. Scheduling, timeout enforcement, recovery window accounting, and deployment-level configuration belong to the future service layer.

## Ownership

Claims lock one scoped state row and sample the PostgreSQL clock under that lock. A due row is claimable when its lease is absent or expired. Every successful claim issues a new UUID token. Leases are bounded to 1–120 seconds. Renew and release require the same owner and token on an unexpired lease. An expired token cannot change a replacement lease. The helpers own only the health-check lease and do not increment the state version.

Future result writes must lock the current state row, verify the exact scope, generation, owner, live token, and expected version, and atomically append history and increment the version. Future operations that also lock lifecycle or recovery rows should use runtime instance, then health state, then recovery attempt order. Docker and HTTP operations occur between short database transactions. The migration defines recovery lease fields and constraints. Recovery claim and execution helpers are outside this phase.

Validate the migration against a disposable PostgreSQL database. The integration test runs when `CLOUDPILOT_HEALTH_TEST_DATABASE_URL` points at a disposable database named `cloudpilot_health_test` or `cloudpilot_health_test_<suffix>`. Apply the migration to the primary database only with explicit approval.

Run the following from the repository root after generating the Prisma client and migrating the disposable database.

```sh
node --import tsx --test apps/worker/test/health-state.test.ts packages/database/test/health-ownership.integration.test.ts
```
