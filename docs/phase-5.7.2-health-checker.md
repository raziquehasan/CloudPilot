# Phase 5.7.2 health checker

The worker provides an on-demand `runRuntimeHealthCheck` function for a scoped runtime generation. Scheduling and recovery belong to later phases. Health persistence defaults to disabled. Runtime lifecycle hooks and the callable checker require explicit `HEALTH_PERSISTENCE_ENABLED=true`, enabled runtime execution, and successful application of both health migrations. Apply neither migration to the primary database without approval.

## Probe behavior

The checker uses a trusted repository and the saved runtime's immutable container ID, image ID, container port and loopback binding. Docker inspection checks ownership labels for the workspace, project, deployment, instance, and attempt. It collects container state, exit code, start time, and restart count. An exited, missing, unowned, or unverifiable runtime does not receive an HTTP request. A second Docker inspection discards results if the container or its start epoch changed during HTTP.

The default target is `GET /health`. A caller may provide an origin-relative path consisting of simple path characters. Hosts must be literal `127.0.0.1` or `::1`, and ports must match Docker's verified binding. The request has a three-second timeout and a 4 KiB body limit. It follows no redirects and sends no authentication or cookies. A complete 2xx response passes. A missing endpoint (404) is an HTTP failure and counts toward the normal three-failure threshold. Timeouts, connection refusals, invalid responses and redirects have separate reason codes. Worker cancellation discards the observation.

## Persistence and lifecycle

After deployment, start or restart, the worker verifies the running identity and binding before initializing a `STARTING` generation. Lifecycle operations retire the old generation before changing the container. A failed lifecycle operation leaves health unscheduled until a verified start. A newer verified runtime attempt retires the older deployment generation. Runtime deletion removes the state and history through Phase 5.7.1 foreign keys.

No transaction spans a Docker or HTTP operation. Result recording locks the runtime then the health state, checks the current runtime identity and ready timestamp, verifies the generation, version, owner, token, and database-time expiry, then atomically appends history and sets the next due time. Check history stores the normalized Docker state and result, HTTP outcome and measurements, and a bounded failure code. Docker exit code and restart count are observation metadata.

Disposable PostgreSQL integration tests require a database named `cloudpilot_health_test` or `cloudpilot_health_test_<suffix>` with both migrations applied. Enable persistence for that test process and allowlist its fixture repository, `example/health-probe-test`. A read-only demo probe can call `probeRuntime` independently of persistence.

Run the probe unit tests from the repository root with `node --import tsx --test apps/worker/test/health-probe.test.ts`. With the disposable database in `DATABASE_URL` and the test flags described above, run `node --import tsx --test apps/worker/test/runtime-health.integration.test.ts`.
