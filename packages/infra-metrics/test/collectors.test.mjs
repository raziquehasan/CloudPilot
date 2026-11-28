import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { createDockerCollector, validateInventory, redisObservations } from "../collectors.mjs";
import { collectPeriodically, infrastructureRegistry } from "../sampler.mjs";

const raw = () => ({
  slot: 1, containerId: "a".repeat(64), imageId: "sha256:" + "b".repeat(64), attempt: 1,
  workspaceId: randomUUID(), projectId: randomUUID(), deploymentId: randomUUID(), runtimeInstanceId: randomUUID(),
});

test("inventory rejects invalid, duplicate and unbounded slot or container identities", () => {
  const item = raw();
  assert.throws(() => validateInventory([{ ...item, slot: 9 }]));
  assert.throws(() => validateInventory([item, { ...item, slot: 2 }]));
  assert.throws(() => validateInventory([{ ...item, containerId: "a/../stop" }]));
  assert.throws(() => validateInventory(Array(9).fill(item)));
});

test("Docker stats require complete ownership; resets, stop and absence omit old stats", async () => {
  const targets = validateInventory([raw()]);
  const { containerId, imageId, labels, name } = targets[0];
  const info = { Id: containerId, Image: imageId, Name: name, Config: { Labels: labels },
    State: { Running: true, StartedAt: "2026-01-01T00:00:00Z" }, HostConfig: { Memory: 0 }, RestartCount: 0 };
  let source = info;
  const stats = { id: containerId, read: "2026-01-01T00:00:10Z",
    cpu_stats: { cpu_usage: { total_usage: 2e9 } }, memory_stats: { usage: 1234 },
    networks: { first: { rx_bytes: 22, tx_bytes: 5 }, second: { rx_bytes: 7, tx_bytes: 11 } } };
  let statsReads = 0;
  const collect = createDockerCollector(targets, { read: async (_id, wantsStats) => {
    if (wantsStats) { statsReads++; return stats; }
    return source;
  } });
  const first = (await collect())[0].values;
  assert.equal(first.cpu_usage_seconds_total, 2);
  assert.equal(first.cpu_utilization_ratio, undefined);
  assert.equal(first.memory_limit_bytes, undefined);
  assert.equal(first.network_receive_bytes_total, 29);
  assert.equal(first.network_transmit_bytes_total, 16);
  stats.read = "2026-01-01T00:00:20Z";
  stats.cpu_stats.cpu_usage.total_usage = 17e9;
  assert.equal((await collect())[0].values.cpu_utilization_ratio, 1.5);
  info.State.StartedAt = "2026-01-01T00:00:21Z";
  stats.read = "2026-01-01T00:00:22Z";
  stats.cpu_stats.cpu_usage.total_usage = 1e9;
  assert.equal((await collect())[0].values.cpu_utilization_ratio, undefined);
  info.State.Running = false;
  assert.equal((await collect())[0].values.running, 0);
  assert.deepEqual((await collect())[0].values.cpu_usage_seconds_total, undefined);
  source = { ...info, Config: { Labels: {} } };
  assert.deepEqual((await collect())[0].values, { present: 0, running: 0 });
  source = null;
  assert.deepEqual((await collect())[0].values, { present: 0, running: 0 });
  assert.equal(statsReads, 3);
});

test("sampler distinguishes missing, fresh, failed and recovered samples without overlapping reads", async () => {
  const registry = infrastructureRegistry();
  let attempts = 0;
  let release;
  const stalled = new Promise((resolve) => { release = resolve; });
  const collector = collectPeriodically({ registry, backend: "test", prefix: "cloudpilot_test", types: { gauge: "gauge" },
    read: async () => {
      attempts++;
      if (attempts === 1) await stalled;
      if (attempts === 2) throw Error("internal credential");
      return [{ values: { gauge: attempts } }];
    },
  });
  assert.doesNotMatch(await registry.metrics(), /^cloudpilot_test_gauge\{/m);
  const first = collector.sample();
  assert.equal(collector.sample(), first);
  release();
  await first;
  assert.match(await registry.metrics(), /cloudpilot_test_gauge\{backend="test"\} 1/);
  await collector.sample();
  assert.match(await registry.metrics(), /cloudpilot_infra_backend_up 0/);
  assert.match(await registry.metrics(), /cloudpilot_test_gauge\{backend="test"\} 1/);
  await collector.sample();
  assert.match(await registry.metrics(), /cloudpilot_infra_backend_up 1/);
  assert.match(await registry.metrics(), /cloudpilot_test_gauge\{backend="test"\} 3/);
  await collector.stop();
  await collector.sample();
  assert.equal(attempts, 3);
});

test("Docker timeout waits for aborted operation to settle, then admits a new sample", async () => {
  const registry = infrastructureRegistry();
  let timeout = true;
  const read = createDockerCollector(validateInventory([raw()]), { deadlineMs: 30,
    read: async (_id, _stats, signal) => {
      if (!timeout) return null;
      await new Promise((_, reject) => {
        const keepAlive = setTimeout(() => reject(Error("test deadline")), 2_000);
        signal.addEventListener("abort", () => { clearTimeout(keepAlive); reject(signal.reason); }, { once: true });
      });
    },
  });
  const collector = collectPeriodically({ registry, backend: "docker", prefix: "cloudpilot_container",
    slotted: true, types: { present: "gauge" }, read });
  await collector.sample();
  assert.match(await registry.metrics(), /cloudpilot_infra_backend_up 0/);
  assert.doesNotMatch(await registry.metrics(), /^cloudpilot_container_present\{/m);
  timeout = false;
  await collector.sample();
  assert.match(await registry.metrics(), /cloudpilot_infra_backend_up 1/);
  assert.match(await registry.metrics(), /cloudpilot_container_present\{slot="1"\} 0/);
  await collector.stop();
});

test("Redis observations accept zeros and omit unsupported and private fields", () => {
  const data = redisObservations("used_memory:100\r\nconnected_clients:1\r\ntotal_commands_processed:0\r\nuptime_in_seconds:3\r\nsecret:private");
  assert.deepEqual(data, [{ values: { memory_used_bytes: 100, connected_clients: 1, commands_processed_total: 0, uptime_seconds: 3 } }]);
  assert.throws(() => redisObservations("used_memory:100"));
});
