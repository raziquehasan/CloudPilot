import assert from "node:assert/strict";
import { execFileSync, fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { Redis } from "ioredis";
import { Queue, QueueEvents, Worker } from "bullmq";
import { fileURLToPath } from "node:url";

if (process.env.CLOUDPILOT_STALLED_CHILD) {
  const { prefix, port, role } = JSON.parse(process.env.CLOUDPILOT_STALLED_CHILD);
  const worker = new Worker("runtime", async () => {
    if (role === "A") await new Promise(() => {});
    return { pid: process.pid };
  }, {
    prefix, connection: { host: "127.0.0.1", port, db: 0 },
    lockDuration: 1600, stalledInterval: 700, maxStalledCount: 1,
  });
  await worker.waitUntilReady();
  process.send?.({ pid: process.pid, prefix, port, role });
  process.on("message", async () => { await worker.close(); process.disconnect?.(); });
} else {
  assert.equal(process.env.CLOUDPILOT_RUN_LIVE_RECOVERY, "1");
  const prefix = `cp586c-bull-${randomUUID()}`;
  const docker = (...args: string[]) => execFileSync("docker", args, { encoding: "utf8", timeout: 30_000 }).trim();
  const id = docker("run", "-d", "--name", prefix, "-p", "127.0.0.1::6379", "redis:7");
  const children: ReturnType<typeof fork>[] = [];
  let queue: Queue | undefined;
  let events: QueueEvents | undefined;
  let redis: Redis | undefined;
  try {
    const binding = docker("port", id, "6379/tcp");
    assert.match(binding, /^127\.0\.0\.1:\d+$/);
    const port = Number(binding.split(":")[1]);
    assert.ok(![6379, 5433, 53143, 63299, 8000, 3000].includes(port));
    const connection = { host: "127.0.0.1", port, db: 0 };
    queue = new Queue("runtime", { connection, prefix });
    events = new QueueEvents("runtime", { connection, prefix });
    await events.waitUntilReady();
    let stalled = false;
    events.on("stalled", ({ jobId }) => { if (jobId === "probe") stalled = true; });
    for (const name of ["active", "stalled", "waiting", "completed"] as const) {
      events.on(name, (job: unknown, streamId: string) => console.log(JSON.stringify({ name, job, streamId })));
    }
    const start = async (role: string) => {
      const child = fork(fileURLToPath(import.meta.url), [], { execArgv: ["--import", "tsx"], env: {
        ...process.env, CLOUDPILOT_STALLED_CHILD: JSON.stringify({ prefix, port, role }),
      } });
      children.push(child);
      const ready = await Promise.race([new Promise((resolve) => child.once("message", resolve)), delay(8000, null, { ref: false })]);
      assert.deepEqual(ready, { pid: child.pid, prefix, port, role });
      return child;
    };
    const a = await start("A");
    const job = await queue.add("probe", {}, { jobId: "probe" });
    redis = new Redis(connection);
    const lock = queue.toKey("probe:lock");
    const deadline = Date.now() + 8000;
    while (await redis.pttl(lock) <= 0 && Date.now() < deadline) await delay(100);
    assert.equal(await job.getState(), "active");
    assert.ok(await redis.pttl(lock) > 0);
    const stopped = new Promise((resolve) => a.once("exit", (_code, signal) => resolve(signal)));
    assert.ok(a.kill("SIGKILL"));
    assert.equal(await stopped, "SIGKILL");
    console.log(JSON.stringify({ killedAt: Date.now(), pid: a.pid, ttl: await redis.pttl(lock) }));
    const bReady = start("B");
    let done = false;
    for (let i = 0; i < 55; i++) {
      const current = await queue.getJob("probe");
      const state = await current?.getState();
      console.log(JSON.stringify({ at: Date.now(), state, ttl: await redis.pttl(lock), attemptsStarted: current?.attemptsStarted,
        attemptsMade: current?.attemptsMade, stalledCounter: await redis.hget(queue.toKey("probe"), "stc") }));
      if (state === "completed") {
        assert.equal(current?.returnvalue.pid, (await bReady).pid);
        assert.equal(current?.attemptsStarted, 2);
        done = true;
        break;
      }
      await delay(200);
    }
    assert.ok(done);
    assert.ok(stalled, "BullMQ must emit stalled for the original job");
  } finally {
    for (const child of children) {
      if (child.exitCode === null && child.signalCode === null) {
        const stopped = new Promise<void>((resolve) => child.once("exit", () => resolve()));
        child.send("shutdown");
        if (await Promise.race([stopped.then(() => false), delay(4000, true, { ref: false })])) child.kill("SIGKILL");
        await stopped;
      }
    }
    redis?.disconnect();
    await events?.close();
    await queue?.close();
    docker("rm", "-f", "-v", id);
  }
}
