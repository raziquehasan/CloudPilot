# Phase 5.7.3 distributed health monitoring

The worker polls for due PostgreSQL health rows once per second and uses the existing checker to schedule each successful result write 15 seconds after completion. PostgreSQL time determines eligibility and expiry. Only active health generations for running, trusted-local deployments with verified loopback bindings are eligible. An older runtime attempt or generation is never selected. Newly started runtimes use the Phase 5.7.2 lifecycle hooks to initialize health. Runtimes already running before health activation need an explicit verified initialization.

The worker processes at most one batch per iteration and waits for its probes to finish before polling again. Multiple workers may discover the same candidate. Only the owner of its PostgreSQL lease can probe and record a result. The probe deadline is 80 seconds, including the lease claim round trip, and the lease lasts 90 seconds. Shutdown aborts pending probe work and waits for it to drain. A crashed worker's lease becomes claimable after expiry. Repeated database or Docker infrastructure errors increase the poll delay exponentially up to 30 seconds.

Each worker prunes up to 500 history entries older than seven days once per minute, using row locks with `SKIP LOCKED`. The pruning statement touches only check history. Worker logs report lifecycle and state transitions at info, infrastructure failures at warn, and individual check results, Docker inspection errors, lease conflicts, and rejected stale observations at debug. Deployment application logs do not carry scheduler events.

| Setting | Default | Behavior |
| --- | --- | --- |
| `HEALTH_MONITORING_ENABLED` | `false` | Starts the scheduler when the persistence and trusted-runtime prerequisites are also enabled. |
| `HEALTH_MONITORING_CONCURRENCY` | `4` | Active checks per worker, 1–16. |
| `HEALTH_MONITORING_BATCH_SIZE` | `16` | Due candidates per iteration, 1–128. |

The existing health migrations must already be complete. The scheduler does not restart, replace, or roll back unhealthy deployments. A deployment-scoped monitor can run in a separate, explicitly enabled test process. It skips global retention.

Run the scheduler unit tests with `node --import tsx --test apps/worker/test/health-monitor.test.ts`. The disposable PostgreSQL suite in `apps/worker/test/runtime-health.integration.test.ts` exercises discovery, state transitions, fencing, and bounded retention using the setup in [Phase 5.7.2](phase-5.7.2-health-checker.md).
