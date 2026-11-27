import assert from "node:assert/strict";
import { test } from "node:test";

import {
  formatCursor,
  parseCursor,
} from "@cloudpilot/queue";

import {
  SecretRedactor,
  REDACTED_MARKER,
} from "../src/logging/redact.ts";

import {
  LogNormalizer,
  inferLogLevel,
} from "../src/logging/normalize.ts";

import { DeploymentLogWriter } from "../src/logging/log-writer.ts";

import type { CollectedLogRecord } from "@cloudpilot/queue";

// ==================================================
// Secret redaction
// ==================================================

test("redacts GitHub tokens", () => {
  const redactor = new SecretRedactor();
  const result = redactor.redact(
    "token ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 end",
  );
  assert.equal(result.redacted, true);
  assert.ok(result.message.includes(REDACTED_MARKER));
  assert.ok(!result.message.includes("ghp_"));
});

test("redacts bearer authorization headers", () => {
  const redactor = new SecretRedactor();
  const result = redactor.redact(
    "Authorization: Bearer abc123def456ghi789",
  );
  assert.equal(result.redacted, true);
  assert.ok(!result.message.includes("abc123def456"));
});

test("redacts registered literal secrets with special characters", () => {
  const redactor = new SecretRedactor();
  const secret = "p@ssw0rd/with+special=chars";
  assert.equal(redactor.registerSecret(secret), true);
  const result = redactor.redact(`value is ${secret}!`);
  assert.equal(result.redacted, true);
  assert.ok(!result.message.includes(secret));
});

test("bounds the number of registered secrets", () => {
  const redactor = new SecretRedactor({
    maxRegisteredSecrets: 2,
  });
  assert.equal(redactor.registerSecret("secret-one"), true);
  assert.equal(redactor.registerSecret("secret-two"), true);
  assert.equal(redactor.registerSecret("secret-three"), false);
  assert.equal(redactor.registeredCount, 2);
});

test("does not redact ordinary content", () => {
  const redactor = new SecretRedactor();
  const result = redactor.redact(
    "Build step 3 of 5 completed successfully",
  );
  assert.equal(result.redacted, false);
  assert.equal(
    result.message,
    "Build step 3 of 5 completed successfully",
  );
});

test("redacts private key blocks", () => {
  const redactor = new SecretRedactor();
  const result = redactor.redact(
    "-----BEGIN RSA PRIVATE KEY-----",
  );
  assert.equal(result.message, REDACTED_MARKER);
});

// ==================================================
// Normalization
// ==================================================

function collect(
  source: "BUILD" | "RUNTIME" | "SYSTEM",
): {
  normalizer: LogNormalizer;
  records: CollectedLogRecord[];
} {
  const records: CollectedLogRecord[] = [];
  const normalizer = new LogNormalizer({
    source,
    redactor: new SecretRedactor(),
    onRecord: (record) => records.push(record),
  });
  return { normalizer, records };
}

test("decodes UTF-8 across chunk boundaries", () => {
  const { normalizer, records } = collect("RUNTIME");
  const bytes = Buffer.from("héllo\n", "utf8");
  // Split in the middle of the multi-byte é.
  const split = bytes.indexOf(0xc3) + 1;
  normalizer.push("stdout", bytes.subarray(0, split));
  normalizer.push("stdout", bytes.subarray(split));
  normalizer.finalize();
  assert.equal(records.length, 1);
  assert.equal(records[0].message, "héllo");
});

test("buffers partial lines until a newline arrives", () => {
  const { normalizer, records } = collect("BUILD");
  normalizer.push("stdout", "partial");
  assert.equal(records.length, 0);
  normalizer.push("stdout", " line\n");
  normalizer.finalize();
  assert.equal(records.length, 1);
  assert.equal(records[0].message, "partial line");
});

test("handles CRLF line endings", () => {
  const { normalizer, records } = collect("BUILD");
  normalizer.push("stdout", "one\r\ntwo\r\n");
  normalizer.finalize();
  assert.equal(records.length, 2);
  assert.equal(records[0].message, "one");
  assert.equal(records[1].message, "two");
});

test("strips ANSI escape sequences", () => {
  const { normalizer, records } = collect("BUILD");
  normalizer.push("stdout", "\u001b[31mred\u001b[0m text\n");
  normalizer.finalize();
  assert.equal(records[0].message, "red text");
});

test("does not classify every stderr line as an error", () => {
  const { normalizer, records } = collect("RUNTIME");
  normalizer.push("stderr", "a normal diagnostic line\n");
  normalizer.finalize();
  assert.equal(records[0].stream, "STDERR");
  assert.notEqual(records[0].level, "ERROR");
});

test("infers error level from content", () => {
  assert.equal(
    inferLogLevel("fatal: boom", "stdout"),
    "ERROR",
  );
  assert.equal(
    inferLogLevel("all good", "stdout"),
    "INFO",
  );
});

test("truncates oversized lines with a marker", () => {
  const { normalizer, records } = collect("BUILD");
  const huge = "x".repeat(200_000);
  normalizer.push("stdout", `${huge}\n`);
  normalizer.finalize();
  const joined = records.map((r) => r.message).join("|");
  assert.ok(joined.includes("Oversized log line omitted"));
});

// ==================================================
// Cursor serialization
// ==================================================

test("cursor round-trips through decimal strings", () => {
  assert.equal(parseCursor("1042"), 1042n);
  assert.equal(formatCursor(1042n), "1042");
  assert.equal(parseCursor("-1"), null);
  assert.equal(parseCursor("abc"), null);
  assert.equal(parseCursor(""), null);
});

// ==================================================
// Durable writer buffer bounds (no DB required)
// ==================================================

test("writer bounds the buffer and drops under overload", () => {
  const writer = new DeploymentLogWriter({
    workspaceId:
      "00000000-0000-0000-0000-000000000001",
    projectId:
      "00000000-0000-0000-0000-000000000002",
    deploymentId:
      "00000000-0000-0000-0000-000000000003",
    maxBufferedEntries: 3,
    maxBufferedBytes: 1_000_000,
    // Large flush interval so no DB write is attempted
    // during the test.
    flushIntervalMs: 60_000,
    maxRetries: 0,
  });

  for (let index = 0; index < 10; index += 1) {
    writer.enqueue({
      source: "SYSTEM",
      stream: "SYSTEM",
      level: "INFO",
      message: `entry ${index}`,
      eventTimestamp: new Date(),
    });
  }

  assert.ok(writer.bufferedEntryCount <= 4);
  assert.ok(writer.bufferedByteCount <= 1_000_000);
});