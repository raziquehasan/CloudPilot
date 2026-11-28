import assert from "node:assert/strict";
import test from "node:test";
import { recoveryFaultArmed, releaseRecoveryFault } from "../src/health/recovery-processor.js";

const BOUNDARY = "after-intent";

function withEnv(env: Record<string, string | undefined>, run: () => void) {
  const keys = ["NODE_ENV", "CLOUDPILOT_RECOVERY_FAULT_TEST", "CLOUDPILOT_RECOVERY_FAULT_BOUNDARY"] as const;
  const saved = keys.map((key) => [key, process.env[key]] as const);
  try {
    for (const key of keys) delete process.env[key];
    for (const [key, value] of Object.entries(env)) if (value !== undefined) process.env[key] = value;
    run();
  } finally {
    for (const [key, value] of saved) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test("fault gate is inert outside NODE_ENV=test", () => {
  withEnv({ NODE_ENV: "production", CLOUDPILOT_RECOVERY_FAULT_TEST: "1", CLOUDPILOT_RECOVERY_FAULT_BOUNDARY: BOUNDARY }, () => {
    assert.equal(recoveryFaultArmed(BOUNDARY), false);
  });
  withEnv({ NODE_ENV: "development", CLOUDPILOT_RECOVERY_FAULT_TEST: "1", CLOUDPILOT_RECOVERY_FAULT_BOUNDARY: BOUNDARY }, () => {
    assert.equal(recoveryFaultArmed(BOUNDARY), false);
  });
});

test("fault gate is inert without the arm flag or with a different boundary", () => {
  withEnv({ NODE_ENV: "test", CLOUDPILOT_RECOVERY_FAULT_BOUNDARY: BOUNDARY }, () => {
    assert.equal(recoveryFaultArmed(BOUNDARY), false);
  });
  withEnv({ NODE_ENV: "test", CLOUDPILOT_RECOVERY_FAULT_TEST: "1", CLOUDPILOT_RECOVERY_FAULT_BOUNDARY: "after-restart" }, () => {
    assert.equal(recoveryFaultArmed(BOUNDARY), false);
    assert.equal(recoveryFaultArmed("after-restart"), true);
  });
});

test("release is a safe no-op when no pause is active", () => {
  withEnv({}, () => {
    assert.doesNotThrow(() => releaseRecoveryFault());
  });
});