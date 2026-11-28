export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export type CircuitRejection =
  | "CIRCUIT_OPEN"
  | "HALF_OPEN_IN_PROGRESS"
  | "RESTART_UNCERTAIN"
  | "BUDGET_EXHAUSTED"
  | "COOLDOWN";

export interface CircuitHistory {
  consecutiveFailures: number;
  attemptsInWindow: number;
  recoveryLimit: number;
  cooldownRemainingMs: number;
  restartUncertain: boolean;
  verifiedRecoverySinceOpen: boolean;
  halfOpenInProgress: boolean;
  failedHalfOpenTrial: boolean;
}

export interface CircuitInput extends CircuitHistory {
  failureThreshold: number;
  failedAttemptsInWindow: number;
  now: number;
}

export interface CircuitResult {
  state: CircuitState;
  reason: CircuitRejection | null;
  nextEligibleAt: number | null;
  manualInterventionRequired: boolean;
}

export function evaluateCircuit(input: CircuitInput): CircuitResult {
  if (input.restartUncertain) {
    return { state: "OPEN", reason: "RESTART_UNCERTAIN", nextEligibleAt: null, manualInterventionRequired: true };
  }
  if (input.cooldownRemainingMs > 0) {
    return { state: "OPEN", reason: "COOLDOWN", nextEligibleAt: input.now + input.cooldownRemainingMs, manualInterventionRequired: false };
  }
  if (input.attemptsInWindow >= input.recoveryLimit) {
    return { state: "OPEN", reason: "BUDGET_EXHAUSTED", nextEligibleAt: null, manualInterventionRequired: false };
  }
  // Failed half-open trial reopens and requires manual intervention.
  if (input.failedHalfOpenTrial) {
    return { state: "OPEN", reason: "CIRCUIT_OPEN", nextEligibleAt: null, manualInterventionRequired: true };
  }
  const open = input.consecutiveFailures >= input.failureThreshold;
  if (!open) {
    return { state: "CLOSED", reason: null, nextEligibleAt: input.now, manualInterventionRequired: false };
  }
  // Open circuit. Verified recovery closes it.
  if (input.verifiedRecoverySinceOpen) {
    return { state: "CLOSED", reason: null, nextEligibleAt: input.now, manualInterventionRequired: false };
  }
  // Bounded half-open trial.
  if (input.halfOpenInProgress) {
    return { state: "HALF_OPEN", reason: "HALF_OPEN_IN_PROGRESS", nextEligibleAt: null, manualInterventionRequired: false };
  }
  return { state: "HALF_OPEN", reason: null, nextEligibleAt: input.now, manualInterventionRequired: false };
}
