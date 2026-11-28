import assert from "node:assert/strict";
import test from "node:test";
import { ApiError } from "../lib/api";
import {
  checkOutcome, failureMessage, formatHealthTime, formatRecoveryDuration,
  recoveryBlockMessage, recoverySubmissionError, type HealthCheck,
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
