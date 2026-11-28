# CloudPilot monitoring

Prometheus is a local operator service at http://127.0.0.1:9090. It collects its own real metrics. CloudPilot application and infrastructure scrape targets will be added as those services gain dedicated metrics endpoints.

Run these PowerShell commands from the repository root. The Compose file declares its own project, cloudpilot-monitoring.

```powershell
$monitor = 'infrastructure/monitoring/docker-compose.yml'

docker compose -f $monitor config
docker compose -f $monitor run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml

docker compose -f $monitor up -d --wait --wait-timeout 90
docker compose -f $monitor ps
docker compose -f $monitor logs --tail 100 prometheus

$base = 'http://127.0.0.1:9090'
Invoke-WebRequest "$base/-/ready"
(Invoke-RestMethod "$base/api/v1/targets").data.activeTargets
$q = [Uri]::EscapeDataString('up{job="prometheus"}')
(Invoke-RestMethod "$base/api/v1/query?query=$q").data.result

docker compose -f $monitor restart prometheus
docker compose -f $monitor down
```

Normal shutdown retains the metrics volume. Use an alternate port by setting `$env:CLOUDPILOT_PROMETHEUS_PORT = '19090'` before running Compose and updating `$base`. The bind address remains loopback.

For throwaway testing, choose an unused host port and a distinct project, such as a generated `$scratch = "cloudpilot-monitoring-test-$([guid]::NewGuid().ToString('N'))"`. Start it with `docker compose -p $scratch -f $monitor up -d`. **Destructive cleanup** for that exact scratch project is `docker compose -p $scratch -f $monitor down --volumes`. Check the project name first. Routine shutdown uses the commands above.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Port already in use | Inspect `Get-NetTCPConnection -State Listen -LocalPort 9090` and set the alternate loopback port. |
| Compose YAML error | Run `docker compose -f $monitor config` and correct the reported line. |
| Invalid scrape configuration | Run the pinned image's `promtool check config` command above before starting the service. |
| TSDB permission error | Inspect the Compose-created volume mounted at `/prometheus` and check its ownership against the image's `nobody` user. Avoid broad host permission changes or running the service as root. |
| Docker Desktop host connectivity | Inside the monitoring network, use service DNS for Compose services. Verify `host.docker.internal` and the intended host listener before adding a host target. |
| Target down | Inspect `/api/v1/targets` for the target's last error and check the address from inside the monitoring network. |
| Missing samples | Wait for a scrape, verify target status and query labels, and distinguish an empty result from a numeric zero. |
| Startup failure | Check Compose status and logs, then verify Docker is running, the image is available and the mounted configuration passes promtool. |

Development defaults are a 15-second scrape and evaluation interval, a 5-second timeout, and retention up to 7 days or approximately 1 GiB of retained storage. The size setting governs TSDB retention and is not a hard volume quota. The operator endpoint has no application-level authentication. Keep it on loopback and do not proxy it to the public internet.

See the [Phase 5.9.1 validation report](../../docs/phase-5.9.1-prometheus-setup.md) for measured results and future integration boundaries.

## Application and worker metrics

Phase 5.9.2 adds an optional Compose overlay. Leave the base stack on self-scrape until both dedicated metrics listeners are enabled and verified. The overlay adds authenticated targets for host.docker.internal:9464 and :9465 without changing the monitoring project or data volume.

Both application processes default to metrics disabled. When approval is granted, use one randomly generated bearer token stored outside the repository. Start the API and worker with METRICS_ENABLED=true and METRICS_TOKEN_FILE pointing to the private file. Pass the same path as CLOUDPILOT_METRICS_TOKEN_FILE in the Compose shell. See the [Phase 5.9.2 activation and rollback runbook](../../docs/phase-5.9.2-application-queue-metrics.md).

## Container and infrastructure metrics

Phase 5.9.3 adds an optional infrastructure overlay (`docker-compose.infrastructure.yml`) that mounts `prometheus/infrastructure-targets.yml` and a read-only bearer-token secret, adding three authenticated jobs: `cloudpilot-containers` (host.docker.internal:9470), `cloudpilot-postgres` (:9471) and `cloudpilot-redis` (:9472). It coexists with the application overlay and preserves the base self-scrape, retention and data volume.

The targets are served by the host-run `@cloudpilot/infra-metrics` collector (loopback-only, bearer-authenticated). It reads managed containers through the existing Docker Desktop engine with fixed read-only GETs (no socket mounted into any container), and PostgreSQL/Redis through dedicated least-privilege identities. Configure it with private `CLOUDPILOT_INFRA_CONFIG` (JSON inventory + backend credentials) and `CLOUDPILOT_INFRA_TOKEN_FILE` files, then pass the token path as `CLOUDPILOT_INFRA_TOKEN_FILE` in the Compose shell. Apply all three overlays together:

```powershell
$base = 'infrastructure/monitoring/docker-compose.yml'
$app  = 'infrastructure/monitoring/docker-compose.metrics.yml'
$infra = 'infrastructure/monitoring/docker-compose.infrastructure.yml'
docker compose -f $base -f $app -f $infra config --quiet
docker compose -f $base -f $app -f $infra run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
docker compose -f $base -f $app -f $infra up -d --wait --wait-timeout 90 prometheus
```

Rollback removes only the infrastructure overlay: `docker compose -f $base -f $app up -d --force-recreate --wait prometheus` (retains the application overlay and the TSDB volume). Do not use `down --volumes` on the primary project. See the [Phase 5.9.3 infrastructure metrics report](../../docs/phase-5.9.3-infrastructure-metrics.md) for the security model, metric inventory, PromQL and activation/rollback procedure. Primary activation requires explicit approval.
