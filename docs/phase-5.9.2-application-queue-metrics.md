# Phase 5.9.2 Application, worker and queue metrics

## Baseline and topology

Work began on main at a4f96ba with a clean tree synchronized with origin/main. Express middleware, the route/auth/CSRF chain, health/readiness routes and deployment outbox were reviewed before instrumentation. The queue producers and runtime consumers reuse the repository's BullMQ QueueManager and Redis configuration. The worker has deployment, diagnostic, optional build, and optional runtime consumers. The authoritative queue contract also has a rollback queue name. Manual and automatic recovery are runtime actions on the physical runtime queue. Existing health persistence, scheduler leases and recovery fencing remain in place.

The API and worker each own a private prom-client registry. The new @cloudpilot/metrics workspace shares configuration, runtime defaults, the listener and non-throwing observer. Each process initializes its registry once. A given HTTP or worker registry reuses its instrument definitions on repeat initialization. The API producer owns queue observations, so increasing the number of workers does not duplicate queue gauges. prom-client 15.1.3 is the sole new direct runtime dependency. npm marks that package deprecated in favor of its successor. Keep a library migration separate from activating this implementation.

## Metric inventory

The table lists custom families. Counter values and all histogram counts/buckets start over when the owning process restarts. No observation is emitted for a scope that has never been observed unless the metric is an unlabelled gauge with an explicit zero state. The default runtime families cover process CPU, RSS and start time plus Node.js heap, event loop lag and garbage collection. OS-specific default families may be absent on Windows. Scrape job labels identify the API and worker.

| Metric | Owner / type / unit | Labels and collection semantics |
| --- | --- | --- |
| cloudpilot_http_requests_total | API counter, requests | method, route, status. One observation per response finish or premature close. |
| cloudpilot_http_request_duration_seconds | API histogram, seconds | Same labels, monotonic time to finish or close. |
| cloudpilot_http_requests_in_flight | API gauge, requests | Unlabelled, decremented once on finish or close. |
| cloudpilot_queue_jobs | API gauge, jobs | queue and state. Last successful all-queue snapshot from BullMQ getJobCounts. |
| cloudpilot_queue_collection_success | API gauge, boolean | Unlabelled, one after successful collection, zero when absent or last collection failed. |
| cloudpilot_queue_collection_duration_seconds | API gauge, seconds | Unlabelled, duration of last collection, including deadline when a read times out. |
| cloudpilot_queue_last_success_timestamp_seconds | API gauge, Unix seconds | source=bullmq, absent before first successful collection; advances on successful snapshots only. |
| cloudpilot_worker_jobs_total | Worker counter, executions | queue, job_type, attempt and outcome. Processor invocations after queue payload validation. |
| cloudpilot_worker_job_duration_seconds | Worker histogram, seconds | Same labels, time in the processor including DB and runtime operations. |
| cloudpilot_worker_jobs_in_flight | Worker gauge, executions | queue, job_type, attempt. Counts current invocations. |
| cloudpilot_worker_job_stalls_total | Worker counter, events | queue. Stalled events observed by the consumer. |
| cloudpilot_worker_available | Worker gauge, boolean | Unlabelled, existing deployment consumer readiness and Redis connection state. |
| cloudpilot_worker_health_monitoring_enabled | Worker gauge, boolean | Unlabelled, configured monitor has started and shutdown has not started. Schema failures appear separately in batch metrics. |
| cloudpilot_worker_automatic_recovery_enabled | Worker gauge, boolean | Unlabelled, configured automatic recovery, active monitor, trusted repositories and ready deployment/runtime consumers. This describes process capability. Admission still depends on deployment state and circuit policy. |
| cloudpilot_health_checks_total | Worker counter, checks | outcome and reason. Only scheduled health checks recorded durably. |
| cloudpilot_health_check_duration_seconds | Worker histogram, seconds | Same labels, time through durable recording, including inspection and HTTP. |
| cloudpilot_health_monitor_batches_total | Worker counter, batches | outcome=completed or infrastructure_error. Includes empty successful polls without counting a check. |
| cloudpilot_health_monitor_batch_duration_seconds | Worker histogram, seconds | Same labels. The existing failure diagnostic duration can include scheduler backoff. |
| cloudpilot_recovery_attempts_total | Worker counter, settlements | trigger, action, outcome, reason. Emits after the recovery processor's claim verifies and commits terminal settlement. |
| cloudpilot_recovery_duration_seconds | Worker histogram, seconds | Same labels, monotonic time from claim to settlement as observed by that process. |

HTTP methods are GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS or OTHER. The route uses a static mounted Express prefix and declared route template, or unmatched. Status uses the HTTP status code or aborted. Health and readiness traffic is included. Metrics listener traffic is excluded. The API preserves normal request/auth middleware behavior. Derive percentiles with histogram_quantile.

Queue comes from the configured queue enum. States are waiting, active, delayed, failed, completed, prioritized and waiting-children. BullMQ 6.7.9's getJobCounts type excludes paused as a state. The collector runs every 15 s, with a 2 s collection deadline and at most one outstanding snapshot. It uses the existing producer clients and discards results that arrive past the deadline. Last values may remain visible on failure. Consumers must check collection_success and the age of last_success_timestamp_seconds. Retained failed/completed counts are finite queue contents, not cumulative totals.

Worker runtime job types are start, stop, restart, and recover. The other job types are the validated queue kinds. Unknown labels map to other. attempt is first or subsequent, which includes retry and redelivery. outcome is succeeded, domain_failed, cancelled, skipped, interrupted, error, or other. A return value carrying a failed domain outcome is domain_failed even if BullMQ completes the transport job. Polling retries, stalled events and subsequent processor invocations have different semantics.

Health and recovery reason labels are the bounded persisted HealthFailureReason enum, none or other. Health outcomes are passed and failed. A skipped probe or a Docker/DB infrastructure error does not count as a passed check. Recovery labels use MANUAL or AUTOMATIC, RESTART, and SUCCEEDED or FAILED. A skipped or cancelled dispatch does not count as a verification settlement. Measurements are process-local observations. PostgreSQL remains the durable audit source, and redelivery or crashes may affect counter coverage.

No user, tenant, resource, job or recovery IDs, raw URLs, credentials or free-text errors are used as labels. Prometheus up measures scrape reachability separately from worker application readiness.

## Security and configuration

The listeners bind only to IPv4 loopback, on API_METRICS_PORT=9464 and WORKER_METRICS_PORT=9465 by default. METRICS_ENABLED defaults false. When enabled, a metrics token file is required and all inputs are validated. A missing, unreadable or invalid token prevents the listener from starting, with a sanitized startup message. Listener startup failures do not stop the application. GET /metrics requires a bearer header checked with a timing-safe comparison. The listener serves no business routes, adds no CORS permission and bounds connections, headers and concurrent collections. Shutdown closes the private listener and stops the queue sampler.

Keep a 32-byte randomly generated base64url token in a private file outside the repo, readable only by the account running the processes and Docker Desktop's mount context. Avoid logging the token or putting it into URLs. The opt-in overlay mounts the file read-only as a Compose secret. Traffic is HTTP over loopback and the local Docker Desktop host bridge. For a different host or trust boundary, verify private connectivity and transport protection before activating scrape targets.

The base Prometheus config includes an optional scrape file path. The base service has no such mount, so it remains self-scrape only. The overlay supplies cloudpilot-api and cloudpilot-worker using host.docker.internal at ports 9464 and 9465. The existing polling interval, scrape timeout, retention and persistent volume stay in effect. Custom listener ports require a corresponding target edit in a private local overlay.

## Controlled primary activation and rollback

**Requires explicit approval.** This implementation did not modify root .env, restart primary consumers or activate the primary Prometheus overlay.

1. Confirm ports 9464 and 9465 are unused. Create a private token file such as %LOCALAPPDATA%\CloudPilot\metrics.token. Use the commands below from PowerShell 7. In both application terminals, use the proposed settings below or add them to the ignored root .env only if that edit is explicitly approved.
2. Gracefully stop the existing API and worker from their own terminals with Ctrl+C. Wait for their shutdown messages and confirm their listeners/processes are stopped before restarting. Do not terminate another Node process. The new listener closes before shared Redis resources are disconnected.
3. From the repository root run npm run metrics:build. Start the API with npm run dev -w @cloudpilot/api and the worker with npm run worker:dev in their respective configured shells. Keep the existing primary health/recovery settings. Verify authorized metrics and 401 without credentials for both listeners.
4. Validate and apply only the monitoring project overlay from the Compose shell. Self-scrape and both authenticated targets must report UP. Confirm no new settings for primary Docker, PostgreSQL, Redis, frontend, or deployment runtimes.

```powershell
# One-time token creation in a private directory. This emits no credential.
$tokenPath = Join-Path $env:LOCALAPPDATA 'CloudPilot/metrics.token'
New-Item -ItemType Directory -Force (Split-Path $tokenPath) | Out-Null
if (Test-Path -LiteralPath $tokenPath) { throw 'Token file already exists' }
$bytes = [Security.Cryptography.RandomNumberGenerator]::GetBytes(32)
$tokenText = [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+','-').Replace('/','_')
[IO.File]::WriteAllText($tokenPath, $tokenText)
Remove-Variable tokenText,bytes
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
icacls $tokenPath /inheritance:r /grant:r "${identity}:(R,W)" | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not restrict the token ACL' }

# Set in each application shell before restarting that process.
$env:METRICS_ENABLED = 'true'
$env:METRICS_HOST = '127.0.0.1'
$env:API_METRICS_PORT = '9464'
$env:WORKER_METRICS_PORT = '9465'
$env:METRICS_TOKEN_FILE = Join-Path $env:LOCALAPPDATA 'CloudPilot/metrics.token'

# Set in the monitoring shell only after both listeners pass local checks.
$env:CLOUDPILOT_METRICS_TOKEN_FILE = Join-Path $env:LOCALAPPDATA 'CloudPilot/metrics.token'
$base = 'infrastructure/monitoring/docker-compose.yml'
$overlay = 'infrastructure/monitoring/docker-compose.metrics.yml'
docker compose -f $base -f $overlay config --quiet
docker compose -f $base -f $overlay run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
docker compose -f $base -f $overlay up -d --wait --wait-timeout 90 prometheus
```

For rollback use docker compose -f $base up -d --force-recreate --wait prometheus. This removes the opt-in mount while retaining the TSDB volume. With approval for application restarts, set METRICS_ENABLED=false in the application launch environments and gracefully restart just the API and worker. Remove token-file settings only from environments where they were added. Check primary service identities, frontend/API and runtime HTTP health, the Prometheus self-target and recovery flags again. Do not run Compose down --volumes on the monitoring project.

Use real queries after primary activation. Examples include up{job="cloudpilot-api"}, up{job="cloudpilot-worker"}, cloudpilot_queue_jobs, cloudpilot_queue_collection_success, cloudpilot_health_checks_total, and cloudpilot_recovery_attempts_total. Allow enough history for sum(rate(cloudpilot_http_requests_total[5m])) and histogram_quantile(0.95, sum by (le) (rate(cloudpilot_http_request_duration_seconds_bucket[5m]))). An empty result is not a zero metric value.

Phase 5.9.3 requires verified private scrape connectivity, a least-privilege exporter design, bounded labels, and explicit mapping for container identity. Grafana and workspace metrics access are later work.

## Executed verification

| Command or method | Expected | Observed | Exit / status |
| --- | --- | --- | --- |
| npm run metrics:build | Shared metrics package compiles | TypeScript emitted JS and declarations | 0 / PASS |
| npm run typecheck -w @cloudpilot/api and npm run worker:typecheck | Both application typechecks pass | Both passed after mapping the observed runtime settlement type and dropping unsupported paused queue counts | 0 / PASS |
| node --import tsx --test --test-reporter=spec apps/api/test/metrics.test.ts apps/worker/test/worker-metrics.test.ts apps/worker/test/health-monitor.test.ts apps/worker/test/auto-recovery-circuit.test.ts apps/worker/test/recovery-fault-gate.test.ts | Metrics, fault gates and scheduler pass | 19 passed, zero failed or skipped | 0 / PASS |
| Base and overlay Compose config plus pinned promtool check config | Both configurations valid | Valid, with absent application targets under base and authenticated app targets under overlay | 0 / PASS |
| Disposable Compose, actual API and worker entry points, HTTP and PromQL probes | Both authorized scrapes plus self-scrape UP with genuine samples | All three UP. Both missing and invalid credentials returned 401 on both listeners. Authorized responses were 200 in Prometheus text format. | 0 / PASS |
| node --import tsx --test --test-concurrency=1 apps/api/test/deployment-api.test.ts using disposable API fixture | Authorization, CSRF, scope, deployment and manual recovery API regression | Both tests passed with the isolated branch enabled | 0 / PASS |
| Live primary activation and scrape history | Approval and authenticated primary app targets, then live samples | Primary Prometheus still has self-scrape only. Primary activation awaits approval. | Not run / NOT VERIFIED |

The initial combined regression command used a random database name outside the existing API test's guarded fixture branch and hit its DISABLED assertion. The API guard now permits a unique suffix in its dedicated disposable name. The recovery integration files have their own naming guard, so they run separately against a different disposable database. This changes only the test gate.

The isolated stack ran PostgreSQL 16 and Redis 7 with dynamically published loopback ports and a unique queue prefix. It used independent API business port 18000, API and worker metrics ports 9464 and 9465, and a Prometheus project on port 19090 with its own volume. A disposable Docker app and owned runtime fixture provided a genuine scheduled HTTP 200 check followed by HTTP 503. No primary recovery was triggered. At observed query times 1795889158 to 1795889161, Prometheus reported:

| Isolated observation | Value |
| --- | --- |
| API GET /health, status 200 | Request counter 1, duration histogram count 1 |
| Isolated diagnostic job | First error 1, subsequent success 1 |
| Diagnostic completed queue gauge | 1, matching authoritative BullMQ |
| Queue collection success and duration | 1 and approximately 5.15 ms |
| Worker readiness and automatic recovery | Available 1, automatic recovery 0 |
| Recorded health checks | Passed 1, failed HTTP_STATUS 1 |
| Samples over the last minute | 3 UP samples for each application target |

At the isolated listener pre-workload snapshot the API response was 12,960 bytes, about 11.1 ms and 136 exposed samples. The worker response was 11,876 bytes, about 4.9 ms and 109 exposed samples. Later Node RSS was 170,684,416 bytes for the API and 179,302,400 bytes for the worker. Disposable Prometheus reported 1,051 head series, 0.43% CPU and 31 MiB of its 512 MiB limit. Phase 5.9.1 self-scrape-only measurements were 795 head series and 29.1 MiB. These are local process snapshots, and the workloads and process ages differ.
