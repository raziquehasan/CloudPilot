# CloudPilot — Phase 5.6: Real-Time Deployment Logs & Streaming Engine

This document describes the implemented Phase 5.6 logging and
streaming engine. It reflects what is actually implemented in
this repository, not planned functionality.

## 1. High-Level Architecture

```
Docker BuildKit          Docker Containers
      |                         |
      | Build Output            | Runtime Output
      +------------+------------+
                   |
           CloudPilot Worker
                   |
           Log Collection Layer  (normalize + redact)
                   |
           Bounded Batch Buffer (DeploymentLogWriter)
                   |
           PostgreSQL  (deployment_log_entries)   <-- authoritative
                   |
          Committed Log Outbox (deployment_log_notification_outbox)
                   |
             Redis Pub/Sub  (committed cursor ranges)
                   |
       Express API + Socket.IO  (re-authorizes, replays from PG)
                   |
         Next.js Log Terminal
```

PostgreSQL is the authoritative source of log history. Redis
Pub/Sub is used only for real-time notification of committed
cursor ranges. Persistence happens **before** publication; a
Redis outage never loses committed logs.

## 2. Log Collection Flow

1. The worker collects build output (BuildKit `--progress=plain`)
   and runtime output (`docker logs --timestamps --follow`).
2. Output is normalized (UTF-8 safe, partial-line buffering,
   CRLF/LF, ANSI/control stripping) and redacted.
3. Records are enqueued into a bounded per-deployment
   `DeploymentLogWriter` buffer.
4. The writer flushes batches to PostgreSQL inside a
   transaction that also allocates the sequence and writes a
   batched notification-outbox row.
5. The API's `LogDistributionRuntime` publishes committed
   cursor ranges to Redis.
6. The API's Socket.IO `LogGateway` fans out to authorized
   subscribers, which fetch the authoritative rows from
   PostgreSQL.

## 3. Database Design

Additive migration:
`20261127120000_deployment_realtime_logs`.

New tables (existing tables are untouched):

- `deployment_log_entries` — unified authoritative history.
  Columns: id, workspaceId, projectId, deploymentId, buildId?,
  runtimeInstanceId?, source, stream, level, message, sequence
  (BIGINT), eventTimestamp, persistedAt, createdAt.
  Unique `(workspaceId, deploymentId, sequence)` guarantees
  idempotent retried writes. Indexes cover
  workspace/deployment/sequence, source and level queries, and
  createdAt for retention.
- `deployment_log_sequences` — per-deployment concurrency-safe
  sequence allocator (`value` BIGINT).
- `deployment_log_notification_outbox` — one bounded row per
  committed batch (never one per line), driving Redis
  publication.

New enums: `DeploymentLogSource`, `DeploymentLogStream`,
`DeploymentLogLevel` (includes `UNKNOWN`).

The legacy `deployment_logs` and `deployment_build_logs` tables
remain in place for backward compatibility.

## 4. Ordering, Delivery and Cursor Semantics

- **Sequence allocation** uses
  `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` on the
  per-deployment counter row inside the write transaction. The
  row lock is held until commit, so sequence order matches
  commit order. There is no unprotected `MAX(sequence)+1`.
- **Idempotency**: retried batch writes conflict on the unique
  `(workspace, deployment, sequence)` key and are skipped.
- **Cursor**: the decimal string of a log entry's `sequence`.
  `after` returns ascending entries with sequence > cursor;
  `before` returns descending entries with sequence < cursor,
  reversed to ascending. `nextCursor`/`previousCursor` are the
  last/first returned sequence when more remain.
- **Filtered requests** page strictly by sequence, so
  filtered-out rows never create false gaps.
- **Delivery guarantee**: at-least-once persistence; clients
  de-duplicate by entry id and reconcile by sequence. Exactly
  once is not claimed (Docker logs provide no unique event
  identity).

## 5. Replay / Synchronization Protocol

On subscribe the gateway:

1. Authorizes the socket (session + workspace membership +
   project + deployment ownership).
2. Replays history from the database (`after=lastCursor`).
3. Sends an initial `deployment:logs:batch` with a
   `highWatermark`.
4. Joins the tenant-scoped room for live batches.

Clients de-duplicate by entry id and drop entries at or below
the high-watermark, preventing the history-to-live race from
losing or duplicating committed logs. `deployment:logs:resync`
prompts a full reload after a detected gap.

## 6. Redis Publication

`LogDistributionRuntime` claims due outbox rows with
`FOR UPDATE SKIP LOCKED`, publishes a lightweight
`LogNotification` (`workspaceId, projectId, deploymentId,
fromSequence, toSequence, committedAt`) to
`{prefix}:deployment:{workspaceId}:{deploymentId}`, and marks
them PUBLISHED. Failures retry with bounded exponential backoff
and jitter; after the attempt budget a row becomes DEAD.
Published rows are cleaned up after a retention window.

## 7. Socket.IO Authentication & Authorization

- Authenticated handshake using the existing
  `cloudpilot_session` cookie (SHA-256 token hash lookup,
  expiry/revocation checks).
- Explicit origin validation via CORS origins.
- Workspace membership, project and deployment ownership are
  re-checked against the database on every subscribe; client
  supplied workspace IDs are never trusted.
- Tenant-scoped rooms; channel names are not used for
  authorization.
- Bounded subscriptions per socket
  (`LOG_SOCKET_MAX_SUBSCRIPTIONS`) and a 1 MB
  `maxHttpBufferSize` for slow-client protection.
- Permissions are revalidated periodically
  (`LOG_PERMISSION_REVALIDATE_MS`); revoked subscriptions are
  removed and an error is emitted.

Events:

- Client → server: `deployment:logs:subscribe`,
  `deployment:logs:unsubscribe`.
- Server → client: `deployment:logs:batch`,
  `deployment:logs:error`, `deployment:logs:resync`,
  `deployment:status:changed` (reserved).

## 8. Secret Redaction

`SecretRedactor` performs best-effort redaction before
persistence, publication, export or display:

- Registered deployment-specific secrets (bounded count and
  size) replaced via index-based splitting (no regex built
  from untrusted values).
- Pattern detection for GitHub/AWS/OpenAI/Google/Slack tokens,
  JWTs, bearer/basic auth, URL credentials, sensitive
  assignments and private-key blocks.

Redaction is defense in depth and cannot guarantee detection
of every arbitrary or encoded secret. Infrastructure secrets
must never be passed to builds or containers.

## 9. Backpressure and Resource Limits

`DeploymentLogWriter` bounds both entry count and bytes. Under
overload the **newest** records are dropped (the producer is
never blocked, so a noisy application cannot deadlock a Docker
process) and an explicit SYSTEM truncation marker is persisted.
Dropped entries are counted. Graceful shutdown flushes the
buffer with bounded retries.

Defaults (configurable): max entry 16 KiB, batch 100, flush
250 ms, max deployment 50 MiB, history page 200 (max 1000),
client buffer 5000 entries.

## 10. Retention

`runLogRetention` deletes expired `deployment_log_entries` in
bounded batches (default 7 days) and cleans obsolete
notification-outbox rows. It never deletes deployments,
builds, runtime instances, containers or audit records.
Retention gaps are reported to clients via history bounds.

## 11. Environment Variables

Worker / API (see `.env.example`):

```
DEPLOYMENT_LOGS_ENABLED=false
LOG_STREAMING_ENABLED=false
LOG_RUNTIME_COLLECTION_ENABLED=false
LOG_MAX_ENTRY_BYTES=16384
LOG_BATCH_SIZE=100
LOG_FLUSH_INTERVAL_MS=250
LOG_MAX_DEPLOYMENT_BYTES=52428800
LOG_RETENTION_DAYS=7
LOG_HISTORY_PAGE_SIZE=200
LOG_HISTORY_MAX_PAGE_SIZE=1000
LOG_SOCKET_MAX_SUBSCRIPTIONS=10
LOG_CLIENT_MAX_ENTRIES=5000
LOG_NOTIFICATION_PUBLISH_INTERVAL_MS=1000
LOG_PERMISSION_REVALIDATE_MS=30000
LOG_REDIS_PREFIX=cloudpilot:logs
LOG_RUNTIME_HISTORICAL_MS=300000
LOG_COLLECTOR_LOCK_TTL_MS=15000
LOG_COLLECTOR_RENEW_MS=5000
```

`BUILD_ENABLED` and `RUNTIME_ENABLED` are unchanged and remain
disabled by default. Enabling logging does not enable build or
runtime execution.

## 12. REST API

Mounted under the existing `/projects` router (auth +
workspace authorization required):

```
GET /api/v1/projects/:projectId/deployments/:deploymentId/logs
GET .../logs?after=<cursor>
GET .../logs?before=<cursor>
GET .../logs?source=BUILD|RUNTIME|SYSTEM
GET .../logs?stream=STDOUT|STDERR|SYSTEM
GET .../logs?level=ERROR|WARN|INFO|DEBUG|UNKNOWN
GET .../logs?limit=<n>
GET .../logs/download
```

Response: `{ items, nextCursor, previousCursor, hasMore,
oldestSequence, newestSequence, retentionTruncated }`.

## 13. Frontend

A dedicated log terminal at `/dashboard/logs` (opened via the
"View Logs" action on each deployment in
`/dashboard/deployments`). Features: All/Build/Runtime/System
tabs, live connection indicator, history loading, auto
reconnect + replay, search, level filter, timestamps, copy,
download, auto-scroll toggle, pause/resume, jump to latest,
clear local display, truncation/retention notices, empty /
loading / error states, monospace terminal styling with
level-based colors, and a bounded client buffer (5000 entries
+ byte cap). Workspace/deployment switching unsubscribes and
clears state.

## 14. Failure Recovery

- API restart: clients reconnect and resubscribe with their
  last cursor; history is replayed from PostgreSQL.
- Worker restart: `RuntimeLogCollectorManager.reconcile`
  restarts collectors for RUNNING instances (bounded
  historical recovery via `--since`).
- Redis outage: committed logs remain in PostgreSQL; outbox
  rows retry; clients resync on reconnect.
- Database outage: the writer retries bounded then persists a
  drop marker; no Docker retry is triggered by log failures.
- Browser disconnect: Socket.IO reconnection + resubscribe.
- Container restart/replacement: reconcile stops the old
  collector (container id change) and starts a new one.

## 15. Testing

- Unit tests: `apps/worker/test/logging.test.ts` (redaction,
  UTF-8 chunk boundaries, partial lines, CRLF, ANSI, level
  inference, oversized lines, cursor round-trip, buffer
  bounds). Run: `npm run test:logging -w @cloudpilot/worker`.
- Typecheck: all workspaces. Build: all workspaces.
- Migration: validate against a disposable PostgreSQL
  database; do not reset the primary database.

Real Docker end-to-end testing requires an explicitly approved
trusted environment and is not performed by default.

## 16. Known Limitations

- Redaction is best-effort.
- Runtime collection is at-least-once; Docker provides no
  unique event identity, so duplicates are possible and
  de-duplicated client-side.
- Docker log replay/timestamps/rotation are bounded by
  `--since` and `--tail` semantics.
- Permission revalidation is periodic, not instantaneous.
- The frontend uses bounded rendering (client buffer cap)
  rather than full windowing virtualization.
- Prometheus/Grafana monitoring is Phase 6; only internal
  counters are prepared here.

## 17. Phase 6 Integration

`LogMetrics` exposes low-cardinality internal counters
(collected/persisted/dropped/redacted entries, buffered bytes,
batch write duration, DB/Redis failures, active collectors and
subscriptions, reconnect attempts, retention deletions) ready
for a future Prometheus exporter.