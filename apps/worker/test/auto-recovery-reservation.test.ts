import assert from "node:assert/strict";
import test from "node:test";
import { automaticRecoveryKey } from "@cloudpilot/database";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-a[0-9a-f]{3}-[0-9a-f]{12}$/;

test("automatic recovery key is deterministic for the same eligibility epoch", () => {
  const a = automaticRecoveryKey("rt-1", 3, 7);
  const b = automaticRecoveryKey("rt-1", 3, 7);
  assert.equal(a, b);
  assert.match(a, UUID);
});

test("automatic recovery key changes with runtime, generation or health version", () => {
  const base = automaticRecoveryKey("rt-1", 3, 7);
  assert.notEqual(base, automaticRecoveryKey("rt-2", 3, 7));
  assert.notEqual(base, automaticRecoveryKey("rt-1", 4, 7));
  assert.notEqual(base, automaticRecoveryKey("rt-1", 3, 8));
});

test("automatic recovery key is a valid canonical UUID", () => {
  assert.match(automaticRecoveryKey("00000000-0000-0000-0000-000000000000", 0, 0), UUID);
});