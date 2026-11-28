# Phase 5.7.4 recovery

Manual recovery reserves the existing recovery row and dispatches a deterministic job on the runtime queue. Its database intent is durable. A caller retries the same idempotency key if the reservation succeeded but queue submission was interrupted. The attempt, lease, and health state remain scoped to the exact runtime generation.

## Additive schema changes

The existing recovery table receives nullable `healthVersion`, `restartIssuedAt`, and `restartSourceStartedAt` fields. The version ties a reservation to its health-state mutation. The timestamp pair records a durable restart intent and the exact Docker start epoch preceding it, including Docker's submillisecond precision. The migration also extends the normalized failure enum. Existing rows and execution history are preserved.

## API and rollout

- `POST /api/v1/projects/:projectId/deployments/:deploymentId/recoveries` requires an authenticated workspace owner or administrator, a CSRF token, and a UUID `Idempotency-Key`. Repeat that key after an interrupted submission.
- `GET` on the same route returns up to 100 scoped attempts. Append `/:recoveryId` to inspect one attempt. Both routes require workspace membership.
- `MANUAL_RECOVERY_ENABLED` defaults to false in the API and worker. Enable it only after all runtime workers have the lifecycle interlock and the recovery migration has been verified. `AUTO_RECOVERY_ENABLED` defaults to false and has no automatic trigger in this phase.

The budget is three recovery reservations per rolling 15-minute window. Failed attempts impose a 15-second base cooldown, doubled for each subsequent failure in the window. The attempt and runtime share a 120-second lease, with a 90-second action deadline. A resumed attempt with a durable restart intent verifies the start epoch instead of issuing another restart. If the epoch did not advance, it fails as uncertain. A verified recovery uses the original immutable container, image and loopback port, then requires a passing HTTP health check before publishing a new health generation. Port drift leaves the saved binding unchanged and reports that public routing needs repair.

## Rollback prerequisites

Restarting a container starts the same immutable image again on its existing binding. Rollback needs an immutable previous successful release, verified image and target eligibility, deployment-level routing, a stable public endpoint, atomic traffic switching, and a rollback audit trail. Container replacement or routing changes are outside this phase.
