// ==================================================
// Automatic recovery eligibility policy (Phase 5.8.1)
// ==================================================
//
// Pure, deterministic evaluator that decides whether an
// automatic recovery is currently eligible. It performs
// NO side effects and is NOT wired to any live restart
// trigger in this subphase. Manual recovery is unchanged.
//
// The evaluator reuses the existing recovery budget and
// the health failure threshold; it never bypasses the
// shared manual/automatic budget, lease fencing, or the
// durable restart-intent guarantees enforced by the
// recovery processor.
// ==================================================

import type { DeploymentHealthStatus } from "@cloudpilot/database";
import { healthDefaults } from "./health-state.js";

export type AutoRecoveryRejection =
  | "AUTO_DISABLED"
  | "INFRASTRUCTURE_UNAVAILABLE"
  | "PERSISTENCE_UNAVAILABLE"
  | "MONITORING_UNAVAILABLE"
  | "DEPLOYMENT_NOT_RUNNING"
  | "RUNTIME_NOT_RUNNING"
  | "RUNTIME_INELIGIBLE"
  | "HEALTH_NOT_UNHEALTHY"
  | "INSUFFICIENT_FAILURES"
  | "STALE_HEALTH"
  | "GENERATION_MISMATCH"
  | "RESTART_UNCERTAIN"
  | "ACTIVE_RECOVERY"
  | "CONFLICTING_OPERATION"
  | "BUDGET_EXHAUSTED"
  | "COOLDOWN";

export type AutoRecoveryDecision =
  | { eligible: true }
  | { eligible: false; reason: AutoRecoveryRejection };

export interface AutoRecoveryInput {
  /** AUTO_RECOVERY_ENABLED — must be explicitly enabled. */
  autoRecoveryEnabled: boolean;
  /** HEALTH_PERSISTENCE_ENABLED — durable state available. */
  persistenceEnabled: boolean;
  /** A connected worker is actively monitoring (5.7.8 report). */
  monitoringAvailable: boolean;
  /** Isolated infrastructure (DB/Redis) reachable. */
  infrastructureReady: boolean;

  deploymentStatus: string;
  runtimeStatus: string;
  /** Trusted local runtime + trusted repository + eligible identity. */
  runtimeEligible: boolean;

  healthStatus: DeploymentHealthStatus;
  consecutiveFailures: number;
  /** Epoch ms of the last health observation, or null. */
  lastCheckedAt: number | null;
  now: number;
  /** Maximum age (ms) for a health observation to be considered fresh. */
  freshnessMs: number;

  /** Health generation matches the current runtime generation. */
  generationMatches: boolean;

  /** An unresolved ambiguous restart condition exists. */
  restartUncertain: boolean;
  /** A manual/automatic recovery is REQUESTED or RUNNING. */
  activeRecovery: boolean;
  /** A conflicting deployment or runtime lifecycle operation is in progress. */
  conflictingOperation: boolean;

  attemptsInWindow: number;
  recoveryLimit: number;
  /** Remaining cooldown (ms); > 0 means still cooling down. */
  cooldownRemainingMs: number;
}

/**
 * Evaluate automatic recovery eligibility. The first failing
 * rule (in a fixed, documented order) yields a stable
 * rejection reason. Eligibility is returned only when every
 * safeguard passes.
 */
export function evaluateAutoRecovery(
  input: AutoRecoveryInput,
): AutoRecoveryDecision {
  if (!input.autoRecoveryEnabled) return { eligible: false, reason: "AUTO_DISABLED" };
  if (!input.infrastructureReady) return { eligible: false, reason: "INFRASTRUCTURE_UNAVAILABLE" };
  if (!input.persistenceEnabled) return { eligible: false, reason: "PERSISTENCE_UNAVAILABLE" };
  if (!input.monitoringAvailable) return { eligible: false, reason: "MONITORING_UNAVAILABLE" };

  if (input.deploymentStatus !== "RUNNING") return { eligible: false, reason: "DEPLOYMENT_NOT_RUNNING" };
  if (input.runtimeStatus !== "RUNNING") return { eligible: false, reason: "RUNTIME_NOT_RUNNING" };
  if (!input.runtimeEligible) return { eligible: false, reason: "RUNTIME_INELIGIBLE" };

  if (input.healthStatus !== "UNHEALTHY") return { eligible: false, reason: "HEALTH_NOT_UNHEALTHY" };
  if (input.consecutiveFailures < healthDefaults.consecutiveFailureLimit) {
    return { eligible: false, reason: "INSUFFICIENT_FAILURES" };
  }

  const age = input.lastCheckedAt === null ? Number.POSITIVE_INFINITY : input.now - input.lastCheckedAt;
  if (age < 0 || age > input.freshnessMs) return { eligible: false, reason: "STALE_HEALTH" };

  if (!input.generationMatches) return { eligible: false, reason: "GENERATION_MISMATCH" };

  // Never act on an unresolved ambiguous restart.
  if (input.restartUncertain) return { eligible: false, reason: "RESTART_UNCERTAIN" };

  // Never create a conflicting or duplicate recovery operation.
  if (input.activeRecovery) return { eligible: false, reason: "ACTIVE_RECOVERY" };
  if (input.conflictingOperation) return { eligible: false, reason: "CONFLICTING_OPERATION" };

  // Shared recovery budget and cooldown (manual + automatic).
  if (input.attemptsInWindow >= input.recoveryLimit) return { eligible: false, reason: "BUDGET_EXHAUSTED" };
  if (input.cooldownRemainingMs > 0) return { eligible: false, reason: "COOLDOWN" };

  return { eligible: true };
}