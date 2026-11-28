import assert from "node:assert/strict";
import test from "node:test";
import { ApiError } from "../lib/api";
import {
  checkOutcome, failureMessage, formatChartTooltipTime, formatHealthTime, formatRecoveryDuration,
  isRecoveryActive, monitoringMessage, recoveryBlockMessage, recoverySubmissionError,
  type HealthCheck, type MonitoringReport, type Recovery,
} from "../components/dashboard/deployment-health-data";

const check: HealthCheck = {
  id: "check", generation: 0, dockerResult: "PASSED", dockerState: "running",
  httpResult: "PASSED", httpStatus: 200, responseLatencyMs: 0,
  failureReason: null, checkedAt: "2026-11-28T00:00:00Z",
};

test("health measurements distinguish missing values from zero and failed checks", () => {
  assert.equal(checkOutcome(check), "PASSED");
  assert.equal(checkOutcome({ ...check, httpResult: "FAILED", httpStatus: 500 }), "FAILED");
  assert.equal(checkOutcome({ ...check, httpResult: "SKIPPED", httpStatus: null, responseLatencyMs: null }), "SKIPPED");
  assert.equal(formatHealthTime(null), "Unavailable");
  assert.notEqual(formatHealthTime(0), "Unavailable");
  assert.equal(formatRecoveryDuration(check.checkedAt, check.checkedAt), "0 ms");
  assert.equal(formatRecoveryDuration(null, check.checkedAt), "Unavailable");
});

test("recovery failures use safe messages and distinguish definitive blocks from ambiguous requests", () => {
  assert.equal(recoverySubmissionError(new TypeError("connection lost")).ambiguous, true);
  assert.equal(recoverySubmissionError(new ApiError(503, "private error", "RECOVERY_UNAVAILABLE")).ambiguous, true);
  assert.equal(recoverySubmissionError(new ApiError(503, "private error", "RECOVERY_DISABLED")).ambiguous, false);
  assert.equal(recoverySubmissionError(new ApiError(409, "private error", "RECOVERY_COOLDOWN")).message, recoveryBlockMessage("COOLDOWN"));
  assert.equal(recoverySubmissionError(new ApiError(429, "private error")).ambiguous, false);
  assert.match(failureMessage("RECOVERY_PORT_CHANGED"), /routing needs repair/);
  assert.equal(failureMessage("PRIVATE INTERNAL DATA"), "The backend reported a failure.");
});

test("chart tooltip shows the check time, not a misleading Unavailable", () => {
  const time = Date.parse("2026-11-28T12:34:56Z");
  const label = formatChartTooltipTime([{ payload: { time } }]);
  assert.notEqual(label, "Unavailable");
  assert.equal(label, formatHealthTime(time));
  // Missing or non-numeric time still degrades safely.
  assert.equal(formatChartTooltipTime(undefined), "Unavailable");
  assert.equal(formatChartTooltipTime([]), "Unavailable");
  assert.equal(formatChartTooltipTime([{ payload: { time: "123" } }]), "Unavailable");
});

test("monitoring message reflects backend-reported status", () => {
  const base: MonitoringReport = {
    status: "ENABLED", workerCount: 1, reportingCount: 1, availableCount: 1, enabledCount: 1,
    lastHeartbeatAt: "2026-11-28T12:00:00Z",
  };
  assert.match(monitoringMessage(base), /active/);
  assert.match(monitoringMessage({ ...base, status: "DISABLED" }), /disabled/);
  assert.match(monitoringMessage({ ...base, status: "STALE" }), /stale/);
  assert.match(monitoringMessage({ ...base, status: "UNKNOWN" }), /unknown/);
  assert.match(monitoringMessage({ ...base, status: "UNAVAILABLE" }), /could not be confirmed/);
  assert.match(monitoringMessage(null), /could not be confirmed/);
});

test("recovery history never reports success before SUCCEEDED", () => {
  const base: Recovery = {
    id: "r", runtimeInstanceId: "rt", generation: 1, status: "SUCCEEDED", trigger: "MANUAL",
    action: "RESTART", attemptCount: 1, failureReason: null, failureMessage: null,
    createdAt: "2026-11-28T12:00:00Z", updatedAt: "2026-11-28T12:00:05Z",
    startedAt: "2026-11-28T12:00:01Z", finishedAt: "2026-11-28T12:00:05Z",
  };
  assert.equal(isRecoveryActive(base), false);
  assert.equal(isRecoveryActive({ ...base, status: "RUNNING" }), true);
  assert.equal(isRecoveryActive({ ...base, status: "REQUESTED" }), true);
  assert.equal(isRecoveryActive({ ...base, status: "FAILED" }), false);
  assert.equal(isRecoveryActive({ ...base, status: "CANCELLED" }), false);
  assert.equal(formatRecoveryDuration(base.startedAt, base.finishedAt), "4.0 s");
});
