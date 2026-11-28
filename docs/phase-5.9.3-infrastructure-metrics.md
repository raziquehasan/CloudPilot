# Phase 5.9.3 — Docker Container and Infrastructure Metrics

Repository baseline discovered at the start of this phase: HEAD `527fdeb` (`docs: record phase 5.9.2.B primary metrics activation`), synchronized with `origin/main` (0 ahead / 0 behind). The working tree already contained an in-progress implementation (an `@cloudpilot/infra-metrics` package, an infrastructure Compose overlay, an infrastructure targets file, a `prometheus.yml` `scrape_config_files` entry, and lockfile additions); this phase completed, validated and documented it. No primary infrastructure, configuration, database record or protected service was changed, and no primary activation was performed.

## 1. Architecture and design decisions

Infrastructure metrics are produced by a dedicated, host-run Node collector, `@cloudpilot/infra-metrics` (`packages/infra-metrics`), that exposes three loopback-only Prometheus listeners:

| Listener | Job | Backend | Port |
| --- | --- | --- | --- |
| Containers | `cloudpilot-containers` | Docker Engine (read-only) | 9470 |
| PostgreSQL | `cloudpilot-postgres` | PostgreSQL (read-only) | 9471 |
| Redis | `cloudpilot-redis` | Redis (read-only) | 9472 |

Prometheus scrapes `host.docker.internal:9470/9471/9472` over the existing monitoring network with a bearer token, exactly like the Phase 5.9.2 application targets. The collector reuses the existing `@cloudpilot/metrics` registry/server primitives and the existing `@cloudpilot/queue` ownership-label builder.

### Chosen approach and rationale
- **No Docker socket is mounted into any container.** The collector runs as a host process (like the API/worker metrics) and talks to the existing Docker Desktop Linux engine through the named pipe `//./pipe/dockerDesktopLinuxEngine` (Windows) or `/var/run/docker.sock` (Linux). It issues only fixed `GET /v1.51/containers/{id}/json` and `.../stats?stream=false&one-shot=true` requests for validated 64-hex container IDs. No caller supplies a path or method, and the exposed interface cannot create, start, stop, restart, remove or modify containers (POST returns 405; unknown paths return 404).
- **Least-privilege native clients.** PostgreSQL is read through `pg` with a dedicated `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS` role granted only `CONNECT`, plus a read-only transaction and a locked `pg_catalog` search path. Redis is read through `ioredis` with an ACL user limited to `+info|server +info|clients +info|memory +info|stats`.
- **Bounded, identity-only discovery.** The Docker inventory is a fixed, validated list of at most 8 CloudPilot-managed containers described only by slot, container/image id and the trusted ownership labels. Arbitrary workloads, environment variables, inspect payloads, command arguments and raw labels are never read or exposed.

### Rejected alternatives
- **cAdvisor**: requires privileged/host-filesystem and cgroup assumptions that do not hold on the Docker Desktop Linux VM, has a large default metric/cardinality surface, and would add a privileged container. Rejected for compatibility and security.
- **Prometheus `postgres_exporter` / `redis_exporter` images**: add third-party images and separate credential/secret handling. The custom collector reuses the existing `pg`/`ioredis` clients, the existing metrics library and the same least-privilege identity model, with a smaller and auditable surface. Rejected to avoid new images and duplicate secret plumbing.
- **Mounting the Docker socket into Prometheus/Grafana or a public exporter**: explicitly prohibited; not used.

### Versions (pinned, no `latest`)
- `postgres:15-alpine` (PostgreSQL 15.16), `redis:7-alpine` (Redis 7.4.7), `node:22-alpine` for disposable workloads; `prom/prometheus:v3.13.5` (existing base). Collector dependencies: `ioredis 6.1.0`, `pg 8.23.1` (exact pins in `packages/infra-metrics/package.json`).

## 2. Security model
- All three listeners bind to `127.0.0.1` only; the token is a 32-byte base64url secret loaded from a private file (`CLOUDPILOT_INFRA_TOKEN_FILE`) and checked with the shared timing-safe comparison. Missing/invalid bearer → 401; non-GET → 405; unknown path → 404.
- Backend credentials come from a private JSON config (`CLOUDPILOT_INFRA_CONFIG`, mode 0600 / ACL-restricted) and are never logged, labelled or returned. The collector rejects weak/short passwords and reserved usernames (`postgres`/`default`/`root`).
- The Docker access is the host process's existing engine permission; no new container privilege, no TCP daemon exposure, no privileged container, no host filesystem mount.

## 3. Metric inventory (types and units)

Containers (`cloudpilot_container_*`, label `slot`):
- `present` gauge, `running` gauge, `start_timestamp_seconds` gauge (Unix seconds), `uptime_seconds` gauge, `restarts_total` counter, `cpu_usage_seconds_total` counter (seconds), `cpu_utilization_ratio` gauge (cores used, 1.0 = one core; normalize by core count for percent), `memory_usage_bytes` gauge, `memory_limit_bytes` gauge (absent when no finite limit), `network_receive_bytes_total` / `network_transmit_bytes_total` counters (summed across interfaces).

PostgreSQL (`cloudpilot_postgres_*`, label `backend`):
- `connections`, `max_connections`, `database_connection_limit` gauges; `database_size_bytes` gauge; `transactions_committed_total`, `transactions_rolled_back_total`, `blocks_hit_total`, `blocks_read_total`, `deadlocks_total`, `temp_files_total`, `temp_bytes_total` counters; `uptime_seconds` gauge.

Redis (`cloudpilot_redis_*`, label `backend`):
- `memory_used_bytes`, `max_memory_bytes` (0 means no configured limit), `connected_clients`, `operations_per_second` gauges; `commands_processed_total`, `keyspace_hits_total`, `keyspace_misses_total`, `evicted_keys_total`, `expired_keys_total` counters; `uptime_seconds` gauge.

Reliability (`cloudpilot_infra_*`): `backend_up` gauge, `last_success_timestamp_seconds` gauge (label `backend`), `collection_duration_seconds` gauge (label `backend`).

## 4. Label strategy and cardinality
Containers use a single bounded `slot` label (1–8); PostgreSQL and Redis use a single `backend` label. No workspace/project/deployment/runtime/user/job identifiers, URLs, paths or raw Docker labels are emitted. Container churn reuses slots and removes stale series rather than accumulating them. Future workspace-scoped Monitoring APIs will map an authorized runtime record to a slot server-side; raw infrastructure metrics are never exposed directly to tenant users.

## 5. Collection reliability
10 s collection interval; single-flight (overlapping scrapes never start a second collection); a timed-out Docker collection aborts within a 4 s deadline and the next cycle proceeds; PostgreSQL/Redis use 1.5–2.5 s connect/command timeouts; only a complete batch is published; on failure the last valid samples are retained with `backend_up 0` and unchanged freshness; on container stop/removal stale slot series are dropped. Graceful SIGINT/SIGTERM closes listeners and stops sampling. Missing data, unavailable data (`backend_up 0`) and genuine zeros are distinct.

## 6. Credential requirements (primary activation)
- PostgreSQL: a dedicated login role with `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, `CONNECT` on the monitored database only, no table privileges. No superuser.
- Redis: an ACL user with only `+info|server +info|clients +info|memory +info|stats`, `reset` otherwise; no `KEYS`, `MONITOR`, `FLUSH*`, `CONFIG SET`, or write commands.
- Secrets live in private files outside the repository (`CLOUDPILOT_INFRA_CONFIG`, `CLOUDPILOT_INFRA_TOKEN_FILE`), ACL-restricted; the Compose overlay mounts only the bearer token read-only as a secret.

## 7. Isolated validation (actual results)
Run `cp593-14ac7d85-f1a2-4e67-adc7-1106bf05df08` against disposable `postgres:15-alpine` (PostgreSQL 15.16), `redis:7-alpine` (Redis 7.4.7), `node:22-alpine` workloads and a disposable Prometheus project on port 19093. Command: `CLOUDPILOT_RUN_INFRA_VALIDATION=1 node packages/infra-metrics/test/infrastructure.live.mjs` → exit 0, `event: passed`.

- Unit suite `node --test packages/infra-metrics/test/collectors.test.mjs` → 5 pass / 0 fail.
- Authentication: authorized 200; missing and invalid bearer 401; POST 405; unknown path 404; no token, passwords, container IDs or scope UUIDs appeared in any exposition.
- Least privilege: the PostgreSQL observer could `SELECT 1` but `INSERT`/`SELECT` on a private table and `CREATE SCHEMA` failed with `42501`; the Redis ACL user could not `SET`, `CONFIG SET maxmemory`, or `INFO commandstats` (`NOPERM`); wrong passwords were rejected.
- Targets: `cloudpilot-containers`, `cloudpilot-postgres`, `cloudpilot-redis`, `prometheus` all `up`; two scrapes observed per collector target.
- Genuine Docker metrics matched authoritative `docker inspect`/`docker stats`: memory `13.27MiB / 128MiB`, exact `StartedAt`, network `9.89kB / 1.58MB`; CPU and network counters advanced after real workload traffic.
- PostgreSQL counters advanced after a committed and a rolled-back transaction; Redis keyspace hits/misses and command counters advanced after real GET/SET.
- Lifecycle: stop → `running 0` and memory series removed; start → counter reset and slot reused; `rm` → `present 0`. Head series 812 → 872 after churn (bounded).
- Failure/recovery: revoking `CONNECT` drove `cloudpilot_infra_backend_up 0` then `1` on restore; removing the Redis `info` grant likewise; pausing/unpausing PostgreSQL and Redis drove `up 0` then `1`.
- Resources: Prometheus 0.58% CPU, 29.93 MiB / 512 MiB; collector responses ~2.4–2.9 KB, 13–14 series each.

All disposable containers, the disposable Compose project and volume, and the temp directory were removed; no protected resource was touched.

## 8. PromQL examples (verified against collected samples)
```promql
100 * cloudpilot_container_cpu_utilization_ratio                       # CPU % of one core (divide by cores for host %)
cloudpilot_container_memory_usage_bytes / 1024^2                       # memory MiB
100 * cloudpilot_container_memory_usage_bytes / cloudpilot_container_memory_limit_bytes  # % of limit (when present)
rate(cloudpilot_container_network_receive_bytes_total[2m])             # rx bytes/s
rate(cloudpilot_container_network_transmit_bytes_total[2m])            # tx bytes/s
cloudpilot_container_uptime_seconds
cloudpilot_postgres_connections
rate(cloudpilot_postgres_transactions_committed_total[2m])
cloudpilot_postgres_blocks_hit_total / (cloudpilot_postgres_blocks_hit_total + cloudpilot_postgres_blocks_read_total)  # cache hit ratio
cloudpilot_redis_memory_used_bytes
cloudpilot_redis_connected_clients
rate(cloudpilot_redis_commands_processed_total[2m])
cloudpilot_redis_keyspace_hits_total / (cloudpilot_redis_keyspace_hits_total + cloudpilot_redis_keyspace_misses_total)
up{job=~"cloudpilot-containers|cloudpilot-postgres|cloudpilot-redis"}
cloudpilot_infra_backend_up
time() - cloudpilot_infra_last_success_timestamp_seconds
```
An empty result is not a zero value; `max_memory_bytes 0` and an absent `memory_limit_bytes` mean "no configured limit", not zero.

## 9. Resource usage and cardinality
Isolated: Prometheus 0.58% CPU, 29.93 MiB / 512 MiB, head series 812→872 after churn. These are local snapshots, not production benchmarks. The Phase 5.9.2 primary snapshot (1,373 series, ~31 MiB, ~0.59% CPU) is the historical reference; adding three low-cardinality jobs is expected to add only a few hundred series.

## 10. Known limitations
- `restarts_total` is Docker's restart counter and may not represent every explicit restart operation.
- `memory_limit_bytes` is absent when the container has no finite limit (host fallback); `max_memory_bytes 0` means no Redis limit.
- `cpu_utilization_ratio` needs two samples (it is undefined on the first) and is per-core; normalize by core count for host percentage.
- Docker access relies on the host process's existing Docker Desktop named-pipe permission; on a different host/trust boundary, private connectivity and transport protection must be re-verified.
- Activation is process-local and does not survive a collector restart unless the environment is persisted (a separately approved change).

## 11. Primary activation procedure (requires explicit approval)
1. Create a private 32-byte base64url scrape token file and a private JSON config (`containers` inventory, `postgres`, `redis`) outside the repository; ACL-restrict both.
2. Create the least-privilege PostgreSQL role (`CONNECT` only) and the Redis ACL user (`+info` only) using separately approved statements.
3. Start the collector with `CLOUDPILOT_INFRA_CONFIG` and `CLOUDPILOT_INFRA_TOKEN_FILE` set; confirm 9470–9472 bound to loopback and 401/200 behavior.
4. In the monitoring shell set `CLOUDPILOT_INFRA_TOKEN_FILE` and apply the combined overlays:
   `docker compose -f infrastructure/monitoring/docker-compose.yml -f infrastructure/monitoring/docker-compose.metrics.yml -f infrastructure/monitoring/docker-compose.infrastructure.yml up -d --wait prometheus` after `config --quiet` and `promtool check config`.
5. Verify the original three targets remain UP plus the three new targets; verify real metrics and multiple historical samples.

## 12. Rollback
`docker compose -f infrastructure/monitoring/docker-compose.yml -f infrastructure/monitoring/docker-compose.metrics.yml up -d --force-recreate --wait prometheus` removes only the infrastructure overlay while retaining the application overlay and the TSDB volume. Stop the collector and remove its private files. Removing the monitoring identities is a separate, explicitly authorized step (revoke the PostgreSQL role's `CONNECT`, `ACL DELUSER` the Redis user). Do not run `compose down --volumes` on the primary project; verify the original three targets remain UP after rollback.

## 13. Protected infrastructure comparison
Primary activation was performed under explicit approval. After-comparison against the fresh pre-activation baseline:

| Item | Before | After | Note |
| --- | --- | --- | --- |
| Demo container | `f9144585eb87` / `2026-11-27T16:36:43.784631096Z` / `53143` | identical | unchanged |
| Validation container | `64376e6449f1` / `2026-11-28T08:30:23.100977192Z` / `63299` | identical | unchanged |
| PostgreSQL | `f887ca72cb1b` healthy | identical | unchanged |
| Redis | `3e42c98d87c9` healthy | identical | unchanged |
| Frontend / API PIDs | 8460 / 26756 | identical | not restarted |
| Prometheus container | `978cc183fe6d` | `872fdaf97c7d` | recreated by the approved overlay (expected) |
| Prometheus volume | `cloudpilot-monitoring_prometheus_data` | identical | preserved |
| HTTP health (demo/validation/api/frontend) | 200 | 200 | unchanged |
| `AUTO_RECOVERY_ENABLED` | false | false | unchanged |

The only difference is the Prometheus container id, recreated by the approved infrastructure overlay; the persistent volume and all application targets/history were preserved. No protected service was restarted and no primary data was modified.

## 14. Primary activation — live evidence
Activated under explicit approval. Steps: created the least-privilege `cloudpilot_observer` PostgreSQL role (`NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, `CONNECT` only) and the `cloudpilot_observer` Redis ACL user (`+info|server +info|clients +info|memory +info|stats` only); wrote private ACL-restricted `infra-config.json` and `infra.token`; started the host collector (loopback 9470–9472); applied the combined base+application+infrastructure overlay to the `cloudpilot-monitoring` project.

- Effective privileges: observer `rolsuper=f`, `CONNECT=t`; `SELECT` on `deployment_health_states` → permission denied; `PUBLIC` has 0 table grants in schema `public` (no inherited table reads); application role `cloudpilot` (superuser) permissions untouched.
- Redis restrictions: `SET`/`CONFIG SET`/`INFO commandstats` → `NOPERM`; `INFO memory` allowed; default application user unchanged (`PING`→`PONG`).
- Collector: listeners on `127.0.0.1` only; authorized 200, missing/invalid bearer 401, `POST /containers/.../stop` 404 (no mutation route); `cloudpilot_infra_backend_up=1` for all three backends (collection success independent of Prometheus reachability). Named-pipe privilege limitation: the collector uses the host process's existing Docker Desktop engine permission via fixed read-only GETs on validated managed IDs; no socket is mounted into any container.
- Six targets UP: `cloudpilot-api`, `cloudpilot-worker`, `prometheus` (preserved) plus `cloudpilot-containers`, `cloudpilot-postgres`, `cloudpilot-redis`.
- Authoritative comparison: container `StartedAt` exact (slot1 `1795797403.784`, slot2 `1795854623.1`); `running=1` both. Container `memory_usage_bytes` is raw cgroup `memory_stats.usage` (includes reclaimable page cache) and therefore reads higher than `docker stats` MemUsage (which subtracts inactive cache) — a metric-definition difference, not an error. PostgreSQL `connections` 4 vs 5 and `database_size_bytes` 36,216,167 vs 36,224,359 differ only by the sampling window and the extra `psql` connection. Redis `memory_used_bytes` 2,992,128 vs 3,008,568 and `connected_clients` 9 = 9 differ only by sampling window.
- History/freshness/cardinality: each infra job produced 6–7 UP samples over 90 s; the original three targets continued sampling (history preserved); freshness 3.3–13.3 s; collection durations 17–71 ms; head series 1,373 → 1,571 (bounded, +~200 for three low-cardinality jobs); Prometheus 0.49% CPU, 35.12 MiB / 512 MiB.
- No artificial primary workload was generated and no protected service was failed or restarted; `AUTO_RECOVERY_ENABLED` stayed false; root `.env` was not modified.

## 15. Final decision
PHASE 5.9.3 PASSED — implementation, isolated validation and approved primary activation are complete; all six Prometheus targets are UP with genuine metrics matching authoritative sources, authentication and least-privilege identities are enforced, protected infrastructure is preserved, and primary automatic recovery remains disabled.

## 16. Future Grafana integration requirements
A later phase must add a least-privilege Grafana datasource, bounded dashboards, and workspace-scoped mapping from authorized runtime records to slots; it must not expose raw infrastructure metrics to tenants and must preserve the loopback-only, authenticated boundary established here.