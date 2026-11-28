import type { DeriveMonitoringInput } from "./monitoring-report.js";

export function deriveAutomaticRecoveryAvailability(input: DeriveMonitoringInput, repository: string | null) {
  if (!input.reachable || input.workers.length === 0) return "UNAVAILABLE";
  const fresh = input.workers.filter((worker) => {
    const interval = worker.capabilities.automaticRecovery?.heartbeatIntervalMs;
    const freshForMs = interval === undefined ? input.staleAfterMs : Math.min(input.staleAfterMs, interval * 3);
    return Math.abs(input.now - Date.parse(worker.lastHeartbeatAt)) <= freshForMs;
  });
  if (!fresh.length) return "STALE";

  const ready = fresh.filter((worker) => worker.available && !worker.draining);
  if (!ready.length) return "UNAVAILABLE";

  const capabilities = ready.map((worker) => worker.capabilities.automaticRecovery);
  const reported = capabilities.filter((capability) => capability !== undefined);
  const supportsRepository = (capability: (typeof reported)[number]) =>
    repository !== null && capability.trustedRepositories.includes(repository.trim().toLowerCase());
  const configurations = new Set(reported.map((capability) => capability.configured));
  const scopes = new Set(reported.filter((capability) => capability.configured).map(supportsRepository));
  const readiness = new Set(reported.filter((capability) => capability.configured).map((capability) => capability.available));
  if (configurations.size > 1 || scopes.size > 1 || readiness.size > 1) return "CONFLICTING";
  if (reported.length !== capabilities.length) return "UNKNOWN";
  if (configurations.has(false)) return "DISABLED";
  return reported.some((capability) => capability.available && supportsRepository(capability))
    ? "AVAILABLE" : "UNAVAILABLE";
}
