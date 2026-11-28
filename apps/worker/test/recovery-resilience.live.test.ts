import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createServer } from "node:http";
import test from "node:test";

// Live Docker fault injection (Phase 5.7.9, Stage G). Explicitly
// opt-in: normal test runs never start destructive infrastructure.
// Uses only disposable, labeled containers and cleans them up.
const live = process.env.CLOUDPILOT_RESILIENCE_LIVE === "1";
const LABEL = "cloudpilot.resilience579=true";
const IMAGE = process.env.RESILIENCE_TEST_IMAGE ?? "node:20-alpine";

const docker = (...args: string[]) =>
  execFileSync("docker", args, { encoding: "utf8", timeout: 120000 }).trim();

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = address && typeof address === "object" ? address.port : 0;
      server.close(() => resolve(port));
    });
  });
}

function exists(name: string): boolean {
  try {
    docker("inspect", name);
    return true;
  } catch {
    return false;
  }
}

test("G: an occupied fixed host port fails authoritatively without a silent fallback", { skip: !live, timeout: 180000 }, async () => {
  const port = await freePort();
  const first = `cloudpilot-resilience-579-occ-${port}`;
  const second = `cloudpilot-resilience-579-occ2-${port}`;
  try {
    docker("run", "-d", "--name", first, "--label", LABEL, "-p", `127.0.0.1:${port}:3000`, IMAGE, "sleep", "300");
    assert.equal(docker("inspect", first, "--format", "{{.State.Running}}"), "true");

    let bindFailed = false;
    try {
      docker("run", "-d", "--name", second, "--label", LABEL, "-p", `127.0.0.1:${port}:3000`, IMAGE, "sleep", "300");
    } catch {
      bindFailed = true;
    }
    assert.equal(bindFailed, true, "second bind on the occupied port must fail");

    // Original owner is unaffected and still bound to the same port.
    assert.equal(docker("inspect", first, "--format", "{{.State.Running}}"), "true");
    assert.match(docker("port", first, "3000/tcp"), new RegExp(`127.0.0.1:${port}`));

    // The failed deployment must not leave an unintended running container.
    if (exists(second)) {
      assert.notEqual(docker("inspect", second, "--format", "{{.State.Running}}"), "true");
    }
  } finally {
    for (const name of [first, second]) {
      if (exists(name)) docker("rm", "-f", name);
    }
  }
});