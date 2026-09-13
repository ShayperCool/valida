import { expect, test } from "bun:test";
import { parseCpuUsage, parseMemoryStat, summarizeResources,
  type ContainerSpec, type Sample } from "./cgroup_sampler.ts";

test("cgroup v2 sampler reports one-core CPU percentages and app/stack memory separately", () => {
  expect(parseCpuUsage("usage_usec 12345\nuser_usec 10000\n")).toBe(12345);
  expect(parseMemoryStat("anon 200\nfile 300\n")).toEqual({ anon: 200, file: 300 });
  const containers: ContainerSpec[] = [
    { name: "api", role: "app", id: "api-id", cgroupPath: "/test/api" },
    { name: "pg", role: "postgres", id: "pg-id", cgroupPath: "/test/pg" },
    { name: "redis", role: "redis", id: "redis-id", cgroupPath: "/test/redis" },
  ];
  const samples: Sample[] = [
    { at_epoch_ms: 0, elapsed_ms: 0, interval_ms: null, containers: {
      api: { cpu_usage_usec: 0, memory_current_bytes: 100, memory_anon_bytes: 70,
        memory_file_bytes: 20, cpu_percent_one_core: null },
      pg: { cpu_usage_usec: 0, memory_current_bytes: 40, memory_anon_bytes: 20,
        memory_file_bytes: 15, cpu_percent_one_core: null },
      redis: { cpu_usage_usec: 0, memory_current_bytes: 20, memory_anon_bytes: 10,
        memory_file_bytes: 5, cpu_percent_one_core: null },
    } },
    { at_epoch_ms: 110, elapsed_ms: 110, interval_ms: 110, containers: {
      api: { cpu_usage_usec: 55_000, memory_current_bytes: 120, memory_anon_bytes: 75,
        memory_file_bytes: 35, cpu_percent_one_core: 50 },
      pg: { cpu_usage_usec: 11_000, memory_current_bytes: 45, memory_anon_bytes: 20,
        memory_file_bytes: 20, cpu_percent_one_core: 10 },
      redis: { cpu_usage_usec: 5_500, memory_current_bytes: 20, memory_anon_bytes: 10,
        memory_file_bytes: 5, cpu_percent_one_core: 5 },
    } },
    { at_epoch_ms: 220, elapsed_ms: 220, interval_ms: 110, containers: {
      api: { cpu_usage_usec: 165_000, memory_current_bytes: 140, memory_anon_bytes: 80,
        memory_file_bytes: 50, cpu_percent_one_core: 100 },
      pg: { cpu_usage_usec: 33_000, memory_current_bytes: 50, memory_anon_bytes: 20,
        memory_file_bytes: 25, cpu_percent_one_core: 20 },
      redis: { cpu_usage_usec: 11_000, memory_current_bytes: 20, memory_anon_bytes: 10,
        memory_file_bytes: 5, cpu_percent_one_core: 5 },
    } },
  ];
  const [result] = summarizeResources(samples, [{ kind: "idle", start_epoch_ms: 0,
    end_epoch_ms: 220 }], containers);
  expect(result?.app.cpu_avg_percent_one_core).toBe(75);
  expect(result?.app.cpu_peak_percent_one_core_100ms).toBe(100);
  expect(result?.app.memory_median_bytes).toBe(120);
  expect(result?.stack.cpu_peak_percent_one_core_100ms).toBe(125);
  expect(result?.stack.memory_peak_bytes).toBe(210);
  expect(result?.app.memory_anon_peak_bytes).toBe(80);
  expect(result?.stack.memory_file_median_bytes).toBe(60);
  expect(result?.containers.pg.cpu_avg_percent_one_core).toBe(15);
});
