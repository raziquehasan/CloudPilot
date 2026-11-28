import assert from "node:assert/strict";
import test from "node:test";
import {
  computeBackoffMs,
  schedulingLagMs,
  DEFAULT_BACKOFF_CAP_MS,
} from "../src/health/scheduler-diagnostics.ts";

test("backoff doubles from the poll interval and caps at 30s", () => {
  assert.equal(computeBackoffMs(1_000, 0), 1_000);
  assert.equal(computeBackoffMs(1_000, 1), 2_000);
  assert.equal(computeBackoffMs(1_000, 2), 4_000);
  assert.equal(computeBackoffMs(1_000, 5), 32_000 > DEFAULT_BACKOFF_CAP_MS ? DEFAULT_BACKOFF_CAP_MS : 32_000);
  assert.equal(computeBackoffMs(1_000, 10), DEFAULT_BACKOFF_CAP_MS);
});

test("a transient failure sequence can approach the observed ~72s gap", () => {
  // failures 0..5 with poll 1s => 1+2+4+8+16+30 = 61s of backoff,
  // plus two batch durations (~5s each) reproduces ~72s without
  // changing the interval or concurrency policy.
  let total = 0;
  for (let failures = 0; failures <= 5; failures++) total += computeBackoffMs(1_000, failures);
  assert.equal(total, 61_000);
  assert.ok(total + 2 * 5_000 >= 70_000 && total + 2 * 5_000 <= 75_000);
});

test("invalid inputs are rejected", () => {
  assert.throws(() => computeBackoffMs(0, 0), RangeError);
  assert.throws(() => computeBackoffMs(1_000, -1), RangeError);
  assert.throws(() => computeBackoffMs(1_000, 11), RangeError);
  assert.throws(() => computeBackoffMs(1_000, 1, 500), RangeError);
});

test("scheduling lag is non-negative and finite-safe", () => {
  assert.equal(schedulingLagMs(1_500, 1_000), 500);
  assert.equal(schedulingLagMs(900, 1_000), 0);
  assert.equal(schedulingLagMs(Number.NaN, 1_000), 0);
});