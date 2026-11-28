# Phase 5.9.1 Prometheus foundation

## Repository and infrastructure baseline

Validated on main at 82848c0, with an initially clean working tree. The root Compose project cloudpilot contains PostgreSQL 15 and Redis 7, attached to cloudpilot-network and using cloudpilot_postgres_data and cloudpilot_redis_data. Docker also had the default bridge, host, and none networks, two project-specific runtime bridges, and an unrelated skillmitra_default bridge. Port 9090 was available. Docker Desktop used WSL 2, Docker Engine 29.9.3, and Compose 5.7.0.

The npm workspace layout covers apps and packages. The root package listed monitoring as keywords, with no Prometheus client or Grafana dependency found in workspace manifests. The API already provides /health and versioned liveness/readiness routes. The worker publishes Redis heartbeats and has no HTTP metrics listener. Existing flags and their validation were left in place. The root gitignore already excludes environment, runtime, logs and generated data. The established docs convention is one report per phase.

## Architecture and security

The monitoring Compose file declares an independent cloudpilot-monitoring project. It creates container cloudpilot-monitoring-prometheus-1, bridge cloudpilot-monitoring_monitoring, and named data volume cloudpilot-monitoring_prometheus_data. No primary network is attached. The only published address is 127.0.0.1:9090. Set CLOUDPILOT_PROMETHEUS_PORT for a different local port.

The official prom/prometheus:v3.13.5 image is the patched Prometheus 3.13 LTS release listed at validation time. The pulled manifest digest was sha256:aafa9d694e6a883c02db516d9f6e5b84e14195f49a2a6456a4cca815edda72b9. [Prometheus downloads](https://prometheus.io/download/) · [LTS support cycle](https://prometheus.io/docs/introduction/release-cycle/)

The service uses the image's nobody user, verified as UID/GID 65534. The volume is owned by 65534:65534 and mounted read/write at /prometheus. The configuration file is the sole host bind mount and is read-only. The container root filesystem is read-only. All capabilities are dropped, no-new-privileges is enabled, and the container uses neither privileged mode, host networking nor a Docker socket mount. The admin API and lifecycle endpoints were verified disabled. The scrape configuration contains no credentials.

Development settings are one CPU, 512 MiB memory, 128 PIDs, a 30-second graceful stop, and unless-stopped restart policy. Prometheus scrapes and evaluates every 15 seconds, with a 5-second scrape timeout. TSDB retention is 7 days or approximately 1 GiB, as reported by the running configuration. Retention is not a disk quota or an observed history length. These settings are for local development and are not production sizing recommendations. See the [operator README](../infrastructure/monitoring/README.md) for PowerShell commands, persistent shutdown, scratch cleanup, and troubleshooting.

## Validation results

All validations ran only against the monitoring project except the explicitly read-only protected probes. The Compose variable in the table refers to infrastructure/monitoring/docker-compose.yml.

| Method | Expected | Observed | Exit / result |
| --- | --- | --- | --- |
| docker compose -f $monitor config | Resolve an isolated project and loopback bind | Correct project, mounts, network, limits, and 127.0.0.1 | 0 / PASS |
| docker compose -f $monitor run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml | Validate mounted config with pinned image | Valid configuration | 0 / PASS |
| Pinned promtool check config against a temporary fixture | Reject a 2 s timeout on a 1 s interval | Rejected with the expected scrape-timeout diagnostic. Fixture removed. | 1 expected / PASS |
| docker compose -f $monitor up -d --wait --wait-timeout 90 | Start only the new service and pass the container health check | Running and healthy | 0 / PASS |
| docker inspect and container id/stat commands | Verify mounts, loopback, limits and non-root data access | Verified UID/GID, writable data mount, read-only config, read-only root and no privileged access | 0 / PASS |
| GET /-/healthy and /-/ready | HTTP 200 | HTTP 200 for both | Probe 0 / PASS |
| GET /api/v1/status/config and /api/v1/status/flags | Confirm effective retention and disabled administrative endpoints | 1w/1GiB, 15s/5s scrape, both admin and lifecycle flags false | Probe 0 / PASS |
| GET /api/v1/targets and instant PromQL | One UP target with genuine samples | One UP target without scrape errors, four instant queries returned finite values | Probe 0 / PASS |
| GET /api/v1/query_range | Multiple real increasing scrape timestamps | Three distinct scrape epochs across six evaluation points | Probe 0 / PASS |
| docker compose -f $monitor stop prometheus | Monitoring unavailable, CloudPilot remains available | Prometheus readiness unreachable. Four protected HTTP URLs still returned 200. | Stop 0, negative probe observed / PASS |
| docker compose -f $monitor restart prometheus | Readiness restored, retained history and new samples | Same container and volume, new StartedAt, identical historical points, then a new UP scrape | 0 / PASS |
| In-container DNS and HTTP probes | Monitoring service resolves on its own network and Docker Desktop host alias works | Service alias and host.docker.internal resolved. The existing API health route on port 8000 was reachable. | 0 / PASS |
| Worker listener audit | Identify whether a scrape endpoint is available | Source confirms Redis heartbeat and no worker HTTP listener. Worker HTTP probing is inapplicable. | Source audit / PASS |
| docker stats, du and TSDB head metric | Obtain local usage observation | 0.73% CPU, 29.1 MiB / 512 MiB, six PIDs, 104 KiB TSDB directory, 795 head series | 0 / PASS |

## Actual metrics and persistence

The self-scrape target was http://127.0.0.1:9090/metrics, with job prometheus, instance 127.0.0.1:9090 and health up. Representative pre-restart query results were collected around epoch 1795887929.

| Query | Sample |
| --- | --- |
| up{job="prometheus"} | 1 |
| prometheus_build_info | 1, version label 3.13.5 |
| process_resident_memory_bytes | 89,780,224 bytes |
| prometheus_tsdb_head_series | 680 |

The range query used timestamp(up{job="prometheus"}) with start 1795887933, end 1795887959 and a 5 s step. It returned six evaluation points whose values identified these three distinct, increasing scrape epochs: 1795887927.363, 1795887942.363, and 1795887957.366. Using timestamp distinguishes actual scrape timestamps from range evaluation timestamps.

The same range returned the same six timestamp/value pairs after restart. The container ID stayed 92890d52f328ffec9c8b2ba239c94aced4d9195d733eace60c2c2df1cfa50155 and StartedAt advanced from 2026-11-28T17:44:34.636020074Z to 2026-11-28T17:46:04.074900752Z. The post-restart scrape epoch 1795887972.364 returned up=1. The usage snapshot above was taken at 2026-11-28 23:16:13 +05:30 with only self-scraping active, roughly 12 seconds after restart. Process RSS and Docker memory accounting are different measurements. This snapshot is not a production benchmark.

## Protected infrastructure before and after

Full container IDs, image IDs, start timestamps, port mappings and Docker status matched. IDs are shortened below for readability.

| Service | Host port | Container / image ID prefix | StartedAt UTC | Before and after |
| --- | --- | --- | --- | --- |
| Primary PostgreSQL | 5433 | f887ca72cb1b / 5fe8ca7fc662 | 2026-11-26T14:42:40.970153667Z | Healthy |
| Primary Redis | 6379 | 3e42c98d87c9 / 02f2cc4882f8 | 2026-11-27T11:18:18.685280548Z | Healthy |
| Demo | 53143 | f9144585eb87 / 15b896427fd3 | 2026-11-27T16:36:43.784631096Z | Running, HTTP 200 |
| Recovery validation | 63299 | 64376e6449f1 / ec6f7b2f9d1a | 2026-11-28T08:30:23.100977192Z | Running, HTTP 200 |
| API | 8000 | PID 43192 | — | HTTP 200, same listener |
| Frontend | 3000 | PID 8460 | — | HTTP 200, same listener |
| Worker process observations | — | PIDs 46964 and 45284 | Same creation times | Both process identities unchanged |

No primary data-write commands were issued. The root environment file hash matched before and after, and health persistence, monitoring, and manual recovery remained true with automatic recovery false.

## Metric contracts for future phases

Use the cloudpilot_ prefix and Prometheus base units. Suffix cumulative counters with _total and durations with _seconds.

| Proposed metrics | Type |
| --- | --- |
| cloudpilot_http_requests_total, cloudpilot_worker_jobs_total, cloudpilot_health_checks_total, cloudpilot_recovery_attempts_total | Counter |
| cloudpilot_queue_jobs, cloudpilot_worker_available | Gauge |
| cloudpilot_http_request_duration_seconds, cloudpilot_worker_job_duration_seconds, cloudpilot_health_check_duration_seconds, cloudpilot_recovery_duration_seconds | Histogram |

Use bounded labels such as normalized route templates, HTTP methods and status classes, known queues and enumerated outcomes. Exclude raw URLs, user/session IDs, job/recovery IDs, commit hashes, unrestricted container IDs, error messages, credentials and environment values. Deployment-level labels require an explicit cardinality budget, retention policy, and tenant authorization design.

## Integration boundaries and acceptance

- **5.9.2** adds prom-client, secure internal API and worker metrics endpoints, HTTP/job timing and counts, BullMQ statistics, health, and recovery metrics. Reserve job names cloudpilot-api, cloudpilot-worker, and cloudpilot-queues. Verify host listener reachability for the actual protected metrics routes. The Docker Desktop alias worked for the existing API health route. The worker currently has no HTTP listener.
- **5.9.3** evaluates Docker Stats versus cAdvisor and PostgreSQL/Redis exporters with least privilege and explicit runtime identity mapping. Reserve cloudpilot-containers, cloudpilot-postgres and cloudpilot-redis. Socket access requires separate security design.
- **5.9.4** adds operator Grafana, its own durable storage and provisioned datasource, with secure credentials and anonymous access disabled.
- **5.9.5** adds authenticated, workspace-scoped monitoring APIs, explicit platform authorization, bounded PromQL, validated query windows, timeout and caching.
- **5.9.6** builds the backend-driven Monitoring dashboard across overview, containers, services, queues and alerts with responsive and unavailable states.
- **5.9.7** adds evaluated alert rules, Alertmanager, deduplication, history and authorized notifications.
- **5.9.8** validates metrics, tenant isolation, failures, cardinality, resource usage and operations end to end.

Only self-scrape is configured. Operator access is local and has no Prometheus authentication layer. Future tenant metrics go through authenticated CloudPilot APIs with explicit authorization for platform metrics. There are no frontend connections to Prometheus and no unrestricted PromQL API in this phase.

**PHASE 5.9.1 PASSED.** The independent Prometheus service and its preserved volume were left running. Phase 5.9.2 can begin with the endpoint and access design above.
